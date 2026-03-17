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
    raw_drop = options.get("columns")
    if raw_drop is None:
        to_drop = []
    elif isinstance(raw_drop, (list, tuple)):
        to_drop = list(raw_drop)
    elif hasattr(raw_drop, "__iter__") and not isinstance(raw_drop, str):
        to_drop = list(raw_drop)
    else:
        to_drop = [raw_drop]

    cols_present = [c for c in to_drop if c in df.columns]
    if len(cols_present) == 0:
        report.record_module(config["module_id"], {"columns_dropped": 0})
        return df

    df = df.drop(columns=cols_present)
    report.record_module(
        config["module_id"],
        {"columns_dropped": len(cols_present), "dropped_columns": cols_present},
    )
    return df
