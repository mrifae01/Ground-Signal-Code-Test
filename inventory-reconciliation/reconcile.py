"""
Inventory Reconciliation Script

Compares two weekly warehouse inventory snapshots and produces a structured
report identifying changes, additions, removals, and data quality issues.

Usage:
    python reconcile.py
"""

from __future__ import annotations

import re
import pandas as pd
from pathlib import Path
from datetime import datetime


# ---------------------------------------------------------------------------
# snapshot_2 uses different column names — map them to canonical names
# ---------------------------------------------------------------------------
SNAPSHOT_2_RENAME = {
    "product_name": "name",
    "qty": "quantity",
    "warehouse": "location",
    "updated_at": "last_counted",
}

DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize_sku(raw: str) -> tuple[str, str | None]:
    """
    Normalize SKU to uppercase SKU-NNN format.
    Returns (normalized_sku, issue_description_or_None).
    """
    s = str(raw).strip()
    upper = s.upper()
    match = re.match(r"^SKU-?(\d+)$", upper)
    if match:
        normalized = f"SKU-{match.group(1).zfill(3)}"
        if normalized != s:
            return normalized, f"Malformed SKU '{s}' normalized to '{normalized}'"
        return normalized, None
    return upper, f"Unrecognized SKU format: '{s}'"


def normalize_date(raw: str) -> tuple[str, str | None]:
    """
    Normalize date string to ISO format YYYY-MM-DD.
    Returns (normalized_date, issue_description_or_None).
    """
    s = str(raw).strip()
    for fmt in DATE_FORMATS:
        try:
            parsed = datetime.strptime(s, fmt)
            normalized = parsed.strftime("%Y-%m-%d")
            if normalized != s:
                return normalized, f"Non-standard date '{s}' normalized to '{normalized}'"
            return normalized, None
        except ValueError:
            continue
    return s, f"Unrecognized date format: '{s}'"


# ---------------------------------------------------------------------------
# Loading and cleaning
# ---------------------------------------------------------------------------

def _make_issue(source: str, row, field: str, raw, normalized, issue: str) -> dict:
    return {
        "source": source,
        "row": row,
        "field": field,
        "raw_value": raw,
        "normalized_value": normalized,
        "issue": issue,
    }


def load_and_clean(
    path: str | Path,
    rename: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    Load a snapshot CSV, rename columns to canonical names, normalize all
    fields, and collect data quality issues.

    Returns (cleaned_df, issues_list).
    """
    path = Path(path)
    source = path.name
    df = pd.read_csv(path, dtype=str)  # load everything as str to capture raw values

    if rename:
        df = df.rename(columns=rename)

    issues: list[dict] = []

    # --- 1. Normalize SKUs ---
    for i, raw in enumerate(df["sku"]):
        norm, issue = normalize_sku(raw)
        df.at[i, "sku"] = norm
        if issue:
            issues.append(_make_issue(source, i + 2, "sku", raw, norm, issue))

    # --- 2. Strip whitespace from string fields ---
    for col in ["name", "location"]:
        if col not in df.columns:
            continue
        for i, raw in enumerate(df[col]):
            stripped = str(raw).strip()
            df.at[i, col] = stripped
            if stripped != raw:
                issues.append(_make_issue(
                    source, i + 2, col, repr(raw), repr(stripped),
                    f"Whitespace stripped from '{col}'"
                ))

    # --- 3. Normalize quantities ---
    valid_quantities: list[int | None] = []
    for i, raw in enumerate(df["quantity"]):
        s = str(raw).strip()
        try:
            as_float = float(s)
        except ValueError:
            issues.append(_make_issue(
                source, i + 2, "quantity", s, None,
                f"Non-numeric quantity '{s}' — row excluded"
            ))
            valid_quantities.append(None)
            continue

        if as_float < 0:
            issues.append(_make_issue(
                source, i + 2, "quantity", s, None,
                f"Negative quantity '{s}' — row excluded"
            ))
            valid_quantities.append(None)
            continue

        as_int = int(as_float)
        if "." in s:
            issues.append(_make_issue(
                source, i + 2, "quantity", s, as_int,
                f"Float quantity '{s}' converted to int"
            ))
        valid_quantities.append(as_int)

    df["quantity"] = pd.array(valid_quantities, dtype="Int64")

    # --- 4. Normalize date formats ---
    for i, raw in enumerate(df["last_counted"]):
        norm, issue = normalize_date(raw)
        df.at[i, "last_counted"] = norm
        if issue:
            issues.append(_make_issue(source, i + 2, "last_counted", raw, norm, issue))

    # --- 5. Flag and deduplicate duplicate SKUs ---
    sku_counts = df["sku"].value_counts()
    dupes = sku_counts[sku_counts > 1].index.tolist()
    for sku in dupes:
        dupe_rows = df[df["sku"] == sku].index.tolist()
        issues.append(_make_issue(
            source, [r + 2 for r in dupe_rows], "sku", sku, None,
            f"Duplicate SKU '{sku}' appears {len(dupe_rows)} times — keeping first occurrence"
        ))
    df = df.drop_duplicates(subset=["sku"], keep="first").reset_index(drop=True)

    # Drop rows with invalid (None) quantity
    df = df[df["quantity"].notna()].reset_index(drop=True)

    return df, issues


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def reconcile(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Merge two normalized snapshots on SKU and classify each item as:
      - unchanged         : in both snapshots, quantity the same
      - quantity_changed  : in both snapshots, quantity differs
      - removed           : only in snapshot_1 (sold out / delisted)
      - added             : only in snapshot_2 (new product)
    """
    cols = ["sku", "name", "quantity", "location", "last_counted"]
    merged = pd.merge(
        df1[cols], df2[cols],
        on="sku",
        how="outer",
        suffixes=("_s1", "_s2"),
    )

    records = []
    for _, row in merged.iterrows():
        in_s1 = pd.notna(row["quantity_s1"])
        in_s2 = pd.notna(row["quantity_s2"])

        if in_s1 and in_s2:
            qty_s1 = int(row["quantity_s1"])
            qty_s2 = int(row["quantity_s2"])
            delta = qty_s2 - qty_s1
            status = "quantity_changed" if delta != 0 else "unchanged"
        elif in_s1:
            qty_s1, qty_s2, delta = int(row["quantity_s1"]), None, None
            status = "removed"
        else:
            qty_s1, qty_s2, delta = None, int(row["quantity_s2"]), None
            status = "added"

        # Prefer snapshot_2 values for matched items (most current)
        name = row["name_s2"] if pd.notna(row.get("name_s2")) else row["name_s1"]
        location = row["location_s2"] if pd.notna(row.get("location_s2")) else row["location_s1"]

        records.append({
            "sku": row["sku"],
            "name": name,
            "location": location,
            "status": status,
            "qty_snapshot_1": qty_s1,
            "qty_snapshot_2": qty_s2,
            "qty_delta": delta,
            "last_counted_s1": row.get("last_counted_s1"),
            "last_counted_s2": row.get("last_counted_s2"),
        })

    if not records:
        return pd.DataFrame(columns=["sku", "name", "location", "status",
                                     "qty_snapshot_1", "qty_snapshot_2", "qty_delta",
                                     "last_counted_s1", "last_counted_s2"])

    result = pd.DataFrame(records)
    sort_order = {"removed": 0, "added": 1, "quantity_changed": 2, "unchanged": 3}
    result["_sort"] = result["status"].map(sort_order)
    result = (
        result.sort_values(["_sort", "sku"])
        .drop(columns="_sort")
        .reset_index(drop=True)
    )
    return result


# ---------------------------------------------------------------------------
# HTML Report
# ---------------------------------------------------------------------------

def generate_html_report(report: pd.DataFrame, issues: list[dict], out_path: Path) -> None:
    """Generate a self-contained HTML report with summary, reconciliation table, and issues."""

    STATUS_COLORS = {
        "removed":          ("#fdecea", "#c62828"),
        "added":            ("#e8f5e9", "#2e7d32"),
        "quantity_changed": ("#fff8e1", "#f57f17"),
        "unchanged":        ("#f5f5f5", "#616161"),
    }
    STATUS_LABELS = {
        "removed":          "Removed",
        "added":            "Added",
        "quantity_changed": "Qty Changed",
        "unchanged":        "Unchanged",
    }

    counts = report["status"].value_counts()

    # --- Summary cards ---
    cards_html = ""
    for status, label in STATUS_LABELS.items():
        bg, fg = STATUS_COLORS[status]
        n = counts.get(status, 0)
        cards_html += f"""
        <div class="card" style="background:{bg}; border-left: 4px solid {fg};">
            <div class="card-count" style="color:{fg};">{n}</div>
            <div class="card-label">{label}</div>
        </div>"""

    # --- Reconciliation table rows ---
    report_rows_html = ""
    for _, row in report.iterrows():
        bg, fg = STATUS_COLORS.get(row["status"], ("#fff", "#000"))
        delta = row["qty_delta"]
        if delta is None or (isinstance(delta, float) and pd.isna(delta)):
            delta_str = "—"
        elif delta > 0:
            delta_str = f'<span style="color:#2e7d32;">+{int(delta)}</span>'
        elif delta < 0:
            delta_str = f'<span style="color:#c62828;">{int(delta)}</span>'
        else:
            delta_str = "0"

        qty_s1 = "—" if pd.isna(row["qty_snapshot_1"]) else int(row["qty_snapshot_1"])
        qty_s2 = "—" if pd.isna(row["qty_snapshot_2"]) else int(row["qty_snapshot_2"])
        lc_s1  = "—" if pd.isna(row["last_counted_s1"]) else row["last_counted_s1"]
        lc_s2  = "—" if pd.isna(row["last_counted_s2"]) else row["last_counted_s2"]

        badge = f'<span class="badge" style="background:{fg}; color:#fff;">{STATUS_LABELS[row["status"]]}</span>'

        report_rows_html += f"""
        <tr style="background:{bg};">
            <td><code>{row["sku"]}</code></td>
            <td>{row["name"]}</td>
            <td>{row["location"]}</td>
            <td>{badge}</td>
            <td style="text-align:right;">{qty_s1}</td>
            <td style="text-align:right;">{qty_s2}</td>
            <td style="text-align:right;">{delta_str}</td>
            <td>{lc_s1}</td>
            <td>{lc_s2}</td>
        </tr>"""

    # --- Issues table rows ---
    issues_rows_html = ""
    for issue in issues:
        issues_rows_html += f"""
        <tr>
            <td><code>{issue["source"]}</code></td>
            <td style="text-align:center;">{issue["row"]}</td>
            <td><code>{issue["field"]}</code></td>
            <td><code>{issue["raw_value"]}</code></td>
            <td><code>{issue["normalized_value"] if issue["normalized_value"] is not None else "—"}</code></td>
            <td>{issue["issue"]}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Inventory Reconciliation Report</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #fafafa; color: #212121; padding: 32px; }}
  h1 {{ font-size: 1.6rem; margin-bottom: 4px; }}
  .subtitle {{ color: #757575; font-size: 0.9rem; margin-bottom: 28px; }}
  h2 {{ font-size: 1.1rem; margin: 32px 0 12px; color: #424242; border-bottom: 1px solid #e0e0e0; padding-bottom: 6px; }}
  .cards {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 8px; }}
  .card {{ padding: 16px 24px; border-radius: 8px; min-width: 140px; }}
  .card-count {{ font-size: 2rem; font-weight: 700; }}
  .card-label {{ font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 2px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
  th {{ background: #424242; color: #fff; padding: 10px 12px; text-align: left; font-weight: 600; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.04em; white-space: nowrap; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid rgba(0,0,0,0.06); vertical-align: middle; }}
  tr:last-child td {{ border-bottom: none; }}
  code {{ font-size: 0.82rem; background: rgba(0,0,0,0.06); padding: 1px 5px; border-radius: 3px; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; white-space: nowrap; }}
</style>
</head>
<body>

<h1>Inventory Reconciliation Report</h1>
<p class="subtitle">snapshot_1.csv (2024-01-08) vs snapshot_2.csv (2024-01-15)</p>

<h2>Summary</h2>
<div class="cards">{cards_html}
</div>

<h2>Reconciliation ({len(report)} items)</h2>
<table>
  <thead>
    <tr>
      <th>SKU</th><th>Name</th><th>Location</th><th>Status</th>
      <th style="text-align:right;">Qty S1</th>
      <th style="text-align:right;">Qty S2</th>
      <th style="text-align:right;">Delta</th>
      <th>Last Counted S1</th><th>Last Counted S2</th>
    </tr>
  </thead>
  <tbody>{report_rows_html}
  </tbody>
</table>

<h2>Data Quality Issues ({len(issues)} found)</h2>
<table>
  <thead>
    <tr>
      <th>Source</th><th>Row</th><th>Field</th>
      <th>Raw Value</th><th>Normalized</th><th>Issue</th>
    </tr>
  </thead>
  <tbody>{issues_rows_html}
  </tbody>
</table>

</body>
</html>"""

    out_path.write_text(html, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main — load, normalize, reconcile, and write output files
# ---------------------------------------------------------------------------

def main() -> None:
    base = Path(__file__).parent
    out_dir = base / "output"
    out_dir.mkdir(exist_ok=True)

    print("Loading snapshot 1...")
    df1, issues1 = load_and_clean(base / "data" / "snapshot_1.csv")
    print(f"  {len(df1)} valid rows, {len(issues1)} issues found")

    print("Loading snapshot 2...")
    df2, issues2 = load_and_clean(base / "data" / "snapshot_2.csv", rename=SNAPSHOT_2_RENAME)
    print(f"  {len(df2)} valid rows, {len(issues2)} issues found")

    print("\nReconciling...")
    report = reconcile(df1, df2)
    all_issues = issues1 + issues2

    # Write output files
    report_path = out_dir / "reconciliation_report.csv"
    issues_path = out_dir / "data_quality_issues.csv"

    html_path = out_dir / "reconciliation_report.html"

    report.to_csv(report_path, index=False)
    pd.DataFrame(all_issues).to_csv(issues_path, index=False)
    generate_html_report(report, all_issues, html_path)

    # Print summary
    counts = report["status"].value_counts()
    print("\n=== Reconciliation Summary ===")
    for status in ["removed", "added", "quantity_changed", "unchanged"]:
        print(f"  {status:<22}: {counts.get(status, 0)}")
    print(f"\n  Total data quality issues : {len(all_issues)}")
    print(f"\nOutputs written to: {out_dir}")
    print(f"  - {report_path.name}")
    print(f"  - {issues_path.name}")
    print(f"  - {html_path.name}")


if __name__ == "__main__":
    main()
