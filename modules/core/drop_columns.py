"""
Drop specified columns from the DataFrame.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Remove columns. config["options"] may contain:
    - columns: list of column names to drop. Missing columns are skipped.
    """
    options = config.get("options", {})
    to_drop = options.get("columns") or []

    cols_present = [c for c in to_drop if c in df.columns]
    if not cols_present:
        report.record_module(config["module_id"], {"columns_dropped": 0})
        return df

    df = df.drop(columns=cols_present)
    report.record_module(
        config["module_id"],
        {"columns_dropped": len(cols_present), "dropped_columns": cols_present},
    )
    return df
