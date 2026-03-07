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
    mapping = options.get("mapping") or {}

    if not mapping:
        report.record_module(config["module_id"], {"renamed": []})
        return df

    # Only rename columns that exist
    rename = {old: new for old, new in mapping.items() if old in df.columns}
    if not rename:
        report.record_module(config["module_id"], {"renamed": []})
        return df

    df = df.rename(columns=rename)
    report.record_module(
        config["module_id"],
        {"renamed": [{"old": o, "new": n} for o, n in rename.items()]},
    )
    return df
