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

    Strict/email-ready export uses this step; typically columns are [website, email, phone]
    with how="all" so we only drop when all are missing. We do not require firm_name or
    contact_name so leads with real emails are not excluded when firm extraction is inconsistent.
    """
    options = config.get("options", {})
    raw_required = options.get("columns")
    if raw_required is None:
        required = []
    elif isinstance(raw_required, (list, tuple)):
        required = list(raw_required)
    elif hasattr(raw_required, "__iter__") and not isinstance(raw_required, str):
        required = list(raw_required)
    else:
        required = [raw_required]
    how_raw = options.get("how")
    how = str(how_raw).lower() if how_raw is not None else "any"

    if how not in ("any", "all"):
        how = "any"

    cols = [c for c in required if c in df.columns]
    if len(cols) == 0:
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
    report.rows_dropped_required += dropped
    report.record_module(
        config["module_id"],
        {"rows_dropped": dropped, "required_columns": cols, "how": how},
    )
    return df
