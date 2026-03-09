"""
Replace specific values with new values (e.g. "n/a" -> null, "y" -> "yes").
"""

import pandas as pd

from cleaner.report import CleaningReport


def _to_replacement(v):
    """None -> pd.NA for pandas; other values unchanged."""
    if v is None:
        return pd.NA
    return v


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Apply value replacements. config["options"] may contain:
    - columns: list of column names (default: all columns).
    - mapping: dict of { old_value: new_value }. new_value can be null/None for NaN.
    - mappings: optional per-column overrides, { "column_name": { old_value: new_value } }.
    """
    options = config.get("options", {})
    columns = options.get("columns")
    mapping = options.get("mapping") or {}
    per_col = options.get("mappings") or {}

    if columns is not None:
        cols = [c for c in columns if c in df.columns]
    else:
        cols = list(df.columns)

    if not cols or (not mapping and not per_col):
        report.record_module(
            config["module_id"],
            {"columns_processed": 0, "replacements": 0},
        )
        return df

    global_map = {k: _to_replacement(v) for k, v in mapping.items()}
    total_replaced = 0
    df = df.copy()

    for col in cols:
        col_map = {
            k: _to_replacement(v)
            for k, v in (per_col.get(col) or global_map).items()
        }
        if not col_map:
            continue
        for old_val, new_val in col_map.items():
            mask = df[col] == old_val
            count = mask.sum()
            if count > 0:
                df.loc[mask, col] = new_val
                total_replaced += count

    report.record_module(
        config["module_id"],
        {"columns_processed": len(cols), "replacements": int(total_replaced)},
    )
    return df
