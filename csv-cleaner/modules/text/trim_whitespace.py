"""
Trim leading and trailing whitespace from string columns.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Strip whitespace from string cells. config["options"] may contain:
    - columns: list of column names to trim (default: all string/object columns).
    Nulls are left unchanged (not converted to the string "nan").
    """
    options = config.get("options", {})
    columns = options.get("columns")

    if columns is not None:
        cols = [c for c in columns if c in df.columns]
    else:
        cols = [
            c
            for c in df.columns
            if pd.api.types.is_string_dtype(df[c])
        ]

    if not cols:
        report.record_module(config["module_id"], {"columns_trimmed": 0})
        return df

    df = df.copy()
    changed = 0

    for col in cols:
        non_null = df[col].notna()
        if not non_null.any():
            continue
        before = df.loc[non_null, col].astype(str)
        after = before.str.strip()
        if not before.equals(after):
            df.loc[non_null, col] = after
            changed += (before != after).sum()

    report.record_module(
        config["module_id"],
        {"columns_trimmed": len(cols), "cells_changed": int(changed)},
    )
    return df
