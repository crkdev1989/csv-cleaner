"""
Replace empty and whitespace-only strings with NaN for consistent missing-data handling.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Treat empty strings and optional custom values as null. config["options"] may contain:
    - columns: list of column names (default: all string/object columns).
    - empty_values: list of extra strings to treat as null (e.g. ["n/a", "NA", "-"]).
    """
    options = config.get("options", {})
    columns = options.get("columns")
    empty_values = options.get("empty_values") or []

    if columns is not None:
        cols = [c for c in columns if c in df.columns]
    else:
        cols = [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]

    if not cols:
        report.record_module(config["module_id"], {"columns_processed": 0, "values_replaced": 0})
        return df

    # Standard empties: "" and whitespace-only
    def is_empty(val):
        if pd.isna(val):
            return True
        s = str(val).strip()
        if s == "":
            return True
        if s in empty_values:
            return True
        return False

    total_replaced = 0
    df = df.copy()
    for col in cols:
        mask = df[col].apply(is_empty)
        count = mask.sum()
        if count > 0:
            df.loc[mask, col] = pd.NA
            total_replaced += count

    report.record_module(
        config["module_id"],
        {"columns_processed": len(cols), "values_replaced": int(total_replaced)},
    )
    return df
