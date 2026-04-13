# Reconciliation Notes

## How I Approached This

Before writing any code I opened both CSVs and read through them manually. The first thing I noticed was that the two files don't share the same column names — `name` in snapshot_1 becomes `product_name` in snapshot_2, `quantity` becomes `qty`, `location` becomes `warehouse`, and `last_counted` becomes `updated_at`. That told me normalization had to happen before any comparison could be meaningful.

I then went row by row through both files looking for anything that stood out. I flagged a list of issues myself before touching the code. Once I had a clear picture of the problems, I used Claude Code to validate my findings, challenge my assumptions, and help architect the solution. After the script was written and outputs generated, I manually cross-referenced the reconciliation report against the raw CSVs to verify the results were correct.

## Data Quality Issues Found

Most of these I spotted during the manual review pass:

**SKU formatting inconsistencies (snapshot_2):**
- `SKU005` — missing the dash, should be `SKU-005`
- `SKU018` — same issue, should be `SKU-018`
- `sku-008` — lowercase, should be `SKU-008`

**Whitespace in string fields:**
- snapshot_1, row 36: `"Cable Ties 100pk "` — trailing space in name
- snapshot_1, row 53: `" Compressed Air Can"` — leading space in name
- snapshot_2: `" Widget B"`, `"Mounting Bracket Large "`, `" HDMI Cable 3ft "` — various leading/trailing spaces in name

**Quantity stored as float instead of int (snapshot_2):**
- `SKU-002`: `70.0`
- `SKU-007`: `80.00`

**Duplicate SKU with conflicting data (snapshot_2):**
- `SKU-045` appears twice — once as `Multimeter Professional` with qty `23`, and again as `Multimeter Pro` with qty `-5`. The negative quantity is clearly a data entry error. The first occurrence was kept and the duplicate row excluded.

**Non-standard date format (snapshot_2):**
- `SKU-035` has `01/15/2024` instead of `2024-01-15` — the only row in either file using MM/DD/YYYY format.

## Key Decisions

**SKU as the only join key.** I considered using product name as a fallback but ruled it out quickly. `SKU-045` appears as both `Multimeter Pro` and `Multimeter Professional` across the two snapshots, which would have caused a false mismatch. SKU is the stable identifier.

**Negative quantities are excluded entirely.** A qty of `-5` has no valid meaning in an inventory context. The row is dropped and flagged in the issues log rather than trying to interpret or correct it.

**Float quantities are accepted and converted.** `70.0` and `80.00` are clearly integers stored with unnecessary decimal precision. Dropping these rows would have been too aggressive since they carry valid data.

**Column rename happens before any normalization.** Since the two files use different names for the same fields, renaming to a shared canonical schema (`name`, `quantity`, `location`, `last_counted`) is the first step so all downstream logic can be written once.

**First occurrence wins on duplicates.** When a SKU appears more than once in the same snapshot, the first row is kept. This is a conservative default, without knowing which row is authoritative, earlier is safer than later.

**Definition of quantity change.** Quantity comparisons are performed after normalization and data cleaning. A SKU is classified as “quantity changed” when the quantity differs between snapshot_1 and snapshot_2 after normalization. For example, if snapshot_1 has qty = 150 and snapshot_2 has qty = 145, the delta is -5 and the SKU is marked as changed.

## Results

After normalization and reconciliation across 80 total SKUs:

| Status | Count |
|---|---|
| Removed (only in snapshot_1) | 2 |
| Added (only in snapshot_2) | 5 |
| Quantity changed | 71 |
| Unchanged | 2 |

The two removed items are `SKU-025` (VGA Cable) and `SKU-026` (DVI Cable). The five new items are `SKU-076` through `SKU-080` (Stream Deck Mini, Stream Deck XL, Capture Card, USB-C Hub, Thunderbolt Cable).

The high number of quantity changes (71 out of 80 SKUs) reflects real differences between the two snapshots, not issues caused by data cleaning.

**NOTE**: I also created a simple HTML report to visualize the reconciliation results (outputs/reconciliation_report.html). 
