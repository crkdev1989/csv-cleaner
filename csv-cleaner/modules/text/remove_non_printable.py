"""
Remove non-printable and control characters from string columns.
"""

import re

import pandas as pd

from cleaner.report import CleaningReport

# Control and other non-printable: 0x00-0x1f and 0x7f
_NON_PRINTABLE_RE = re.compile(r"[\x00-\x1f\x7f]")


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Remove non-printable/control characters from string cells. config["options"] may contain:
    - columns: list of column names (default: all string/object columns).
    - collapse_whitespace: replace runs of whitespace with single space (default: False).
    - strip: strip leading/trailing whitespace after cleaning (default: False).
    Only non-null values are modified; nulls are preserved.
    """
    options = config.get("options", {})
    columns = options.get("columns")
    collapse_whitespace = options.get("collapse_whitespace", False)
    strip = options.get("strip", False)

    if columns is not None:
        cols = [c for c in columns if c in df.columns]
    else:
        cols = [
            c
            for c in df.columns
            if pd.api.types.is_string_dtype(df[c])
        ]

    if not cols:
        report.record_module(
            config["module_id"],
            {"columns_processed": 0, "values_changed": 0},
        )
        return df

    total_changed = 0
    df = df.copy()

    for col in cols:
        non_null = df[col].notna()
        if not non_null.any():
            continue

        raw = df.loc[non_null, col].astype(str)
        cleaned = raw.str.replace(_NON_PRINTABLE_RE, "", regex=True)
        if collapse_whitespace:
            cleaned = cleaned.str.replace(r"\s+", " ", regex=True)
        if strip:
            cleaned = cleaned.str.strip()

        changed = (raw != cleaned).sum()
        total_changed += changed
        df.loc[non_null, col] = cleaned

    report.record_module(
        config["module_id"],
        {"columns_processed": len(cols), "values_changed": int(total_changed)},
    )
    return df
