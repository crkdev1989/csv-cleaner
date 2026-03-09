"""
Drop rows that have missing (null) values in required columns.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Remove rows with missing required fields. config["options"] may contain:
    - columns: list of column names that must be non-null (default: []). Missing
      columns are skipped; only existing columns are checked.
    - how: "any" | "all" — drop row if any required column is null (default), or
      only if all are null.
    """
    options = config.get("options", {})
    required = options.get("columns") or []
    how = (options.get("how") or "any").lower()

    if how not in ("any", "all"):
        how = "any"

    cols = [c for c in required if c in df.columns]
    if not cols:
        report.record_module(config["module_id"], {"rows_dropped": 0})
        return df

    rows_before = len(df)
    if how == "any":
        mask = df[cols].isna().any(axis=1)
    else:
        mask = df[cols].isna().all(axis=1)
    df = df.loc[~mask].copy()
    dropped = rows_before - len(df)

    report.rows_dropped += dropped
    report.record_module(
        config["module_id"],
        {"rows_dropped": dropped, "required_columns": cols, "how": how},
    )
    return df
