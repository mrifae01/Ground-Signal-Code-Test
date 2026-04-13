"""
Tests for inventory reconciliation logic.
Run with: pytest tests/
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from reconcile import normalize_sku, normalize_date, reconcile, load_and_clean


# ---------------------------------------------------------------------------
# normalize_sku
# ---------------------------------------------------------------------------

class TestNormalizeSku:
    def test_standard_sku_is_unchanged(self):
        sku, issue = normalize_sku("SKU-001")
        assert sku == "SKU-001"
        assert issue is None

    def test_missing_dash_is_fixed(self):
        sku, issue = normalize_sku("SKU001")
        assert sku == "SKU-001"
        assert issue is not None

    def test_lowercase_is_uppercased(self):
        sku, issue = normalize_sku("sku-008")
        assert sku == "SKU-008"
        assert issue is not None

    def test_lowercase_missing_dash(self):
        sku, issue = normalize_sku("sku008")
        assert sku == "SKU-008"
        assert issue is not None

    def test_leading_trailing_whitespace_stripped(self):
        sku, issue = normalize_sku("  SKU-005  ")
        assert sku == "SKU-005"

    def test_zero_padding_applied(self):
        sku, issue = normalize_sku("SKU-5")
        assert sku == "SKU-005"

    def test_three_digit_sku_unchanged(self):
        sku, issue = normalize_sku("SKU-080")
        assert sku == "SKU-080"
        assert issue is None

    def test_unrecognized_format_returns_issue(self):
        sku, issue = normalize_sku("INVALID-123")
        assert issue is not None
        assert "Unrecognized" in issue


# ---------------------------------------------------------------------------
# normalize_date
# ---------------------------------------------------------------------------

class TestNormalizeDate:
    def test_iso_format_unchanged(self):
        date, issue = normalize_date("2024-01-15")
        assert date == "2024-01-15"
        assert issue is None

    def test_us_format_normalized(self):
        date, issue = normalize_date("01/15/2024")
        assert date == "2024-01-15"
        assert issue is not None

    def test_unrecognized_format_returns_issue(self):
        date, issue = normalize_date("not-a-date")
        assert issue is not None

    def test_whitespace_stripped_before_parsing(self):
        date, issue = normalize_date("  2024-01-15  ")
        assert date == "2024-01-15"


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------

def _make_df(rows: list[tuple]) -> pd.DataFrame:
    """Helper: build a minimal snapshot DataFrame."""
    df = pd.DataFrame(rows, columns=["sku", "name", "quantity", "location", "last_counted"])
    df["quantity"] = pd.array(df["quantity"].tolist(), dtype="Int64")
    return df


class TestReconcile:
    def test_unchanged_item(self):
        df1 = _make_df([("SKU-001", "Widget A", 100, "Warehouse A", "2024-01-08")])
        df2 = _make_df([("SKU-001", "Widget A", 100, "Warehouse A", "2024-01-15")])
        result = reconcile(df1, df2)
        assert result.iloc[0]["status"] == "unchanged"
        assert result.iloc[0]["qty_delta"] == 0

    def test_quantity_decreased(self):
        df1 = _make_df([("SKU-001", "Widget A", 100, "Warehouse A", "2024-01-08")])
        df2 = _make_df([("SKU-001", "Widget A", 80, "Warehouse A", "2024-01-15")])
        result = reconcile(df1, df2)
        assert result.iloc[0]["status"] == "quantity_changed"
        assert result.iloc[0]["qty_delta"] == -20

    def test_quantity_increased(self):
        df1 = _make_df([("SKU-001", "Widget A", 50, "Warehouse A", "2024-01-08")])
        df2 = _make_df([("SKU-001", "Widget A", 75, "Warehouse A", "2024-01-15")])
        result = reconcile(df1, df2)
        assert result.iloc[0]["status"] == "quantity_changed"
        assert result.iloc[0]["qty_delta"] == 25

    def test_removed_item(self):
        df1 = _make_df([("SKU-001", "Widget A", 100, "Warehouse A", "2024-01-08")])
        df2 = _make_df([])
        result = reconcile(df1, df2)
        assert result.iloc[0]["status"] == "removed"
        assert result.iloc[0]["qty_snapshot_2"] is None

    def test_added_item(self):
        df1 = _make_df([])
        df2 = _make_df([("SKU-099", "New Item", 50, "Warehouse A", "2024-01-15")])
        result = reconcile(df1, df2)
        assert result.iloc[0]["status"] == "added"
        assert result.iloc[0]["qty_snapshot_1"] is None

    def test_all_four_statuses(self):
        df1 = _make_df([
            ("SKU-001", "Widget A", 100, "Warehouse A", "2024-01-08"),  # unchanged
            ("SKU-002", "Widget B", 50,  "Warehouse B", "2024-01-08"),  # changed
            ("SKU-003", "Widget C", 30,  "Warehouse C", "2024-01-08"),  # removed
        ])
        df2 = _make_df([
            ("SKU-001", "Widget A", 100, "Warehouse A", "2024-01-15"),  # unchanged
            ("SKU-002", "Widget B", 45,  "Warehouse B", "2024-01-15"),  # changed
            ("SKU-004", "Widget D", 20,  "Warehouse D", "2024-01-15"),  # added
        ])
        result = reconcile(df1, df2)
        statuses = result.set_index("sku")["status"].to_dict()
        assert statuses["SKU-001"] == "unchanged"
        assert statuses["SKU-002"] == "quantity_changed"
        assert statuses["SKU-003"] == "removed"
        assert statuses["SKU-004"] == "added"

    def test_output_sorted_removed_before_added(self):
        df1 = _make_df([("SKU-010", "Item 10", 10, "WH A", "2024-01-08")])
        df2 = _make_df([("SKU-020", "Item 20", 20, "WH A", "2024-01-15")])
        result = reconcile(df1, df2)
        statuses = result["status"].tolist()
        assert statuses.index("removed") < statuses.index("added")

    def test_empty_snapshots(self):
        df1 = _make_df([])
        df2 = _make_df([])
        result = reconcile(df1, df2)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# load_and_clean
# ---------------------------------------------------------------------------

class TestLoadAndClean:
    def _write_csv(self, tmp_path, filename, content):
        p = tmp_path / filename
        p.write_text(content)
        return p

    def test_sku_normalization(self, tmp_path):
        p = self._write_csv(tmp_path, "snap.csv",
            "sku,name,quantity,location,last_counted\n"
            "SKU001,Widget,10,WH A,2024-01-08\n"
        )
        df, issues = load_and_clean(p)
        assert df.iloc[0]["sku"] == "SKU-001"
        assert any("SKU001" in str(i["raw_value"]) for i in issues)

    def test_lowercase_sku_normalized(self, tmp_path):
        p = self._write_csv(tmp_path, "snap.csv",
            "sku,name,quantity,location,last_counted\n"
            "sku-008,Widget,10,WH A,2024-01-08\n"
        )
        df, issues = load_and_clean(p)
        assert df.iloc[0]["sku"] == "SKU-008"

    def test_float_quantity_converted_and_flagged(self, tmp_path):
        p = self._write_csv(tmp_path, "snap.csv",
            "sku,name,quantity,location,last_counted\n"
            "SKU-001,Widget,70.0,WH A,2024-01-08\n"
        )
        df, issues = load_and_clean(p)
        assert int(df.iloc[0]["quantity"]) == 70
        assert any("float" in i["issue"].lower() for i in issues)

    def test_negative_quantity_excluded(self, tmp_path):
        p = self._write_csv(tmp_path, "snap.csv",
            "sku,name,quantity,location,last_counted\n"
            "SKU-001,Widget,-5,WH A,2024-01-08\n"
        )
        df, issues = load_and_clean(p)
        assert len(df) == 0
        assert any("negative" in i["issue"].lower() for i in issues)

    def test_duplicate_sku_deduped_and_flagged(self, tmp_path):
        p = self._write_csv(tmp_path, "snap.csv",
            "sku,name,quantity,location,last_counted\n"
            "SKU-001,Widget,10,WH A,2024-01-08\n"
            "SKU-001,Widget Dupe,20,WH B,2024-01-08\n"
        )
        df, issues = load_and_clean(p)
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Widget"
        assert any("duplicate" in i["issue"].lower() for i in issues)

    def test_whitespace_stripped_from_name(self, tmp_path):
        p = self._write_csv(tmp_path, "snap.csv",
            "sku,name,quantity,location,last_counted\n"
            "SKU-001, Widget ,10,WH A,2024-01-08\n"
        )
        df, issues = load_and_clean(p)
        assert df.iloc[0]["name"] == "Widget"
        assert any("whitespace" in i["issue"].lower() for i in issues)

    def test_nonstandard_date_normalized(self, tmp_path):
        p = self._write_csv(tmp_path, "snap.csv",
            "sku,name,quantity,location,last_counted\n"
            "SKU-001,Widget,10,WH A,01/15/2024\n"
        )
        df, issues = load_and_clean(p)
        assert df.iloc[0]["last_counted"] == "2024-01-15"
        assert any("date" in i["issue"].lower() for i in issues)

    def test_column_rename_applied(self, tmp_path):
        p = self._write_csv(tmp_path, "snap.csv",
            "sku,product_name,qty,warehouse,updated_at\n"
            "SKU-001,Widget,10,WH A,2024-01-15\n"
        )
        rename = {"product_name": "name", "qty": "quantity", "warehouse": "location", "updated_at": "last_counted"}
        df, issues = load_and_clean(p, rename=rename)
        assert "name" in df.columns
        assert "quantity" in df.columns

    def test_valid_rows_preserved_with_no_issues(self, tmp_path):
        p = self._write_csv(tmp_path, "snap.csv",
            "sku,name,quantity,location,last_counted\n"
            "SKU-001,Widget A,100,WH A,2024-01-08\n"
            "SKU-002,Widget B,200,WH B,2024-01-08\n"
        )
        df, issues = load_and_clean(p)
        assert len(df) == 2
        assert len(issues) == 0
