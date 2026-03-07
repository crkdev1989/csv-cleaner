"""
Drop duplicate rows, optionally by subset of columns.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Remove duplicate rows. config["options"] may contain:
    - subset: list of columns to consider for duplicates (default: all)
    - keep: "first" | "last" | False (default "first")
    """
    options = config.get("options", {})
    subset = options.get("subset")
    keep = options.get("keep", "first")
    rows_before = len(df)

    if subset is not None:
        df = df.drop_duplicates(subset=subset, keep=keep)
    else:
        df = df.drop_duplicates(keep=keep)

    removed = rows_before - len(df)
    report.duplicates_removed += removed
    report.record_module(config["module_id"], {"duplicates_removed": removed})

    return df
