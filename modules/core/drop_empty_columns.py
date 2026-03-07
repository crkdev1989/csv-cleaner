"""
Drop columns where all values are null or (optionally) blank.
"""

import pandas as pd

from cleaner.report import CleaningReport


def _is_empty(val, treat_blank_as_empty):
    """True if value is null or (if treat_blank_as_empty) blank string."""
    if pd.isna(val):
        return True
    if treat_blank_as_empty and isinstance(val, str) and val.strip() == "":
        return True
    return False


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Drop columns that are entirely null/empty. config["options"] may contain:
    - subset: list of column names to evaluate (default: all columns). Only these
      can be dropped; missing columns are skipped.
    - treat_blank_strings_as_empty: treat "" and whitespace-only as empty (default: True).
    """
    options = config.get("options", {})
    subset = options.get("subset")
    treat_blank_as_empty = options.get("treat_blank_strings_as_empty", True)

    if subset is not None:
        to_check = [c for c in subset if c in df.columns]
    else:
        to_check = list(df.columns)

    if not to_check:
        report.record_module(
            config["module_id"],
            {"columns_dropped": 0, "dropped_columns": []},
        )
        return df

    drop = []
    for col in to_check:
        if df[col].apply(lambda v: _is_empty(v, treat_blank_as_empty)).all():
            drop.append(col)

    if not drop:
        report.record_module(
            config["module_id"],
            {"columns_dropped": 0, "dropped_columns": []},
        )
        return df

    df = df.drop(columns=drop)
    report.record_module(
        config["module_id"],
        {"columns_dropped": len(drop), "dropped_columns": drop},
    )
    return df
