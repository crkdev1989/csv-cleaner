"""
Drop rows that are entirely empty (all columns NaN or empty string).
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Remove rows where every cell is missing or empty.
    config["options"] may contain: subset (list of columns to check; default all).
    """
    options = config.get("options", {})
    subset = options.get("subset")
    rows_before = len(df)

    if subset is not None:
        df = df.dropna(how="all", subset=subset)
    else:
        df = df.dropna(how="all")

    dropped = rows_before - len(df)
    report.rows_dropped += dropped
    report.record_module(config["module_id"], {"dropped_empty": dropped})

    return df
