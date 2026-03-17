"""
Drop rows where website or input_url (or other URL columns) contain junk substrings
(e.g. google.com/maps, amazon.com). Removes the row entirely instead of just blanking the URL.
"""

import pandas as pd

from cleaner.report import CleaningReport


DEFAULT_JUNK_SUBSTRINGS = ("google.com/maps", "amazon.com")


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Drop rows where any of the configured URL columns contains any of the junk substrings.
    config["options"] may contain:
    - url_columns: list of column names to check (default ["website", "input_url"]).
    - substrings: list of substrings that indicate a junk row (default google.com/maps, amazon.com).
    """
    options = config.get("options", {})
    url_columns = options.get("url_columns", ["website", "input_url"])
    substrings = options.get("substrings", list(DEFAULT_JUNK_SUBSTRINGS))

    cols = [c for c in url_columns if c in df.columns]
    if not cols or not substrings:
        report.record_module(config["module_id"], {"rows_dropped": 0})
        return df

    mask = pd.Series(False, index=df.index)
    for col in cols:
        for sub in substrings:
            mask = mask | df[col].astype(str).str.contains(sub, na=False, regex=False)

    rows_before = len(df)
    df = df.loc[~mask].copy()
    dropped = rows_before - len(df)

    report.rows_dropped += dropped
    report.rows_dropped_junk += dropped
    report.record_module(
        config["module_id"],
        {"rows_dropped": dropped, "url_columns": cols, "substrings": list(substrings)},
    )
    return df
