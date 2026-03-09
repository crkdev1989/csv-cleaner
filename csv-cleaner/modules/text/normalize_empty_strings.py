"""
Replace empty and whitespace-only strings with NaN for consistent missing-data handling.
"""

import pandas as pd

from cleaner.report import CleaningReport


def _is_empty(val, empty_values):
    """True if value should be treated as null (already null, "", whitespace-only, or in empty_values)."""
    if pd.isna(val):
        return True
    s = str(val).strip()
    if s == "":
        return True
    if s in empty_values:
        return True
    return False


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Treat empty strings and optional custom values as null. config["options"] may contain:
    - columns: list of column names (default: all string/object columns).
    - empty_values: list of extra strings to treat as null (e.g. ["n/a", "NA", "-"]).
    values_replaced counts only non-null values that were converted to null (not already-null cells).
    """
    options = config.get("options", {})
    columns = options.get("columns")
    empty_values = options.get("empty_values") or []

    if columns is not None:
        cols = [c for c in columns if c in df.columns]
    else:
        cols = [
            c
            for c in df.columns
            if pd.api.types.is_string_dtype(df[c])
        ]

    if not cols:
        report.record_module(
            config["module_id"],
            {"columns_processed": 0, "values_replaced": 0},
        )
        return df

    total_replaced = 0
    df = df.copy()

    for col in cols:
        mask = df[col].apply(lambda v: _is_empty(v, empty_values))
        already_null = df[col].isna()
        # Only count when a non-null value was converted to null
        newly_replaced = mask & ~already_null
        count = newly_replaced.sum()
        if mask.any():
            df.loc[mask, col] = pd.NA
        total_replaced += count

    report.record_module(
        config["module_id"],
        {"columns_processed": len(cols), "values_replaced": int(total_replaced)},
    )
    return df
