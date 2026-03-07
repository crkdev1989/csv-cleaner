"""
Fill null/NaN values with a constant or per-column values.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Fill missing values. config["options"] may contain:
    - value: scalar to use for all nulls (applied to columns if columns is set).
    - values: dict of { column_name: fill_value } for per-column fill (overrides value).
    - columns: list of columns to fill (default: all). If only value is set, fill only these.
    """
    options = config.get("options", {})
    single_value = options.get("value")
    per_col_values = options.get("values") or {}
    columns = options.get("columns")

    if per_col_values:
        cols = [c for c in per_col_values if c in df.columns]
    elif columns is not None:
        cols = [c for c in columns if c in df.columns]
        if single_value is None:
            report.record_module(
                config["module_id"],
                {"columns_filled": 0, "nulls_filled": 0},
            )
            return df
    else:
        cols = list(df.columns)
        if single_value is None and not per_col_values:
            report.record_module(
                config["module_id"],
                {"columns_filled": 0, "nulls_filled": 0},
            )
            return df

    if not cols:
        report.record_module(
            config["module_id"],
            {"columns_filled": 0, "nulls_filled": 0},
        )
        return df

    total_filled = 0
    df = df.copy()

    for col in cols:
        fill_val = (
            per_col_values.get(col, single_value)
            if per_col_values
            else single_value
        )
        if fill_val is None:
            continue
        null_count = df[col].isna().sum()
        if null_count > 0:
            df[col] = df[col].fillna(fill_val)
            total_filled += null_count

    report.record_module(
        config["module_id"],
        {"columns_filled": len(cols), "nulls_filled": int(total_filled)},
    )
    return df
