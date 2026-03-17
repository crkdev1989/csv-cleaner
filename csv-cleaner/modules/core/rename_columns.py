"""
Rename columns by a mapping of old name -> new name.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Rename columns. config["options"] may contain:
    - mapping: dict of { old_name: new_name }. Columns not present are skipped.
    """
    options = config.get("options", {})
    raw_mapping = options.get("mapping")
    mapping = raw_mapping if isinstance(raw_mapping, dict) else {}

    if len(mapping) == 0:
        report.record_module(config["module_id"], {"renamed": []})
        return df

    rename = {
        old: new
        for old, new in mapping.items()
        if old in df.columns
    }
    if len(rename) == 0:
        report.record_module(config["module_id"], {"renamed": []})
        return df

    df = df.rename(columns=rename)
    report.record_module(
        config["module_id"],
        {"renamed": [{"old": o, "new": n} for o, n in rename.items()]},
    )
    return df
