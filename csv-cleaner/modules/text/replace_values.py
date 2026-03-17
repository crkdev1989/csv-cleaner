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
    raw_mapping = options.get("mapping")
    mapping = raw_mapping if isinstance(raw_mapping, dict) else {}
    raw_per = options.get("mappings")
    per_col = raw_per if isinstance(raw_per, dict) else {}

    if columns is not None:
        cols = [c for c in (list(columns) if hasattr(columns, "__iter__") and not isinstance(columns, str) else [columns]) if c in df.columns]
    else:
        cols = list(df.columns)

    if len(cols) == 0 or (len(mapping) == 0 and len(per_col) == 0):
        report.record_module(
            config["module_id"],
            {"columns_processed": 0, "replacements": 0},
        )
        return df

    global_map = {k: _to_replacement(v) for k, v in mapping.items()}
    total_replaced = 0
    df = df.copy()

    for col in cols:
        per_col_map = per_col.get(col)
        col_map = {
            k: _to_replacement(v)
            for k, v in (per_col_map if isinstance(per_col_map, dict) else global_map).items()
        }
        if len(col_map) == 0:
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
