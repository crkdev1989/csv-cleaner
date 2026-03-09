"""
Normalize text for matching/deduplication (lowercase, trim, punctuation, whitespace).
"""

import re

import pandas as pd

from cleaner.report import CleaningReport


def _normalize_cell(
    s: str,
    remove_punctuation: bool,
    collapse_whitespace: bool,
) -> str:
    s = s.strip().lower()
    if remove_punctuation:
        s = re.sub(r"[^\w\s]", "", s, flags=re.UNICODE)
    if collapse_whitespace:
        s = re.sub(r"\s+", " ", s).strip()
    return s


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Prepare text for matching/dedupe: lowercase, trim, optional punctuation removal
    and whitespace collapse. config["options"] may contain:
    - columns: list of column names (default: all string/object columns).
    - remove_punctuation: remove non-word chars except spaces (default: True).
    - collapse_whitespace: replace runs of whitespace with single space (default: True).
    Operates on non-null values only.
    """
    options = config.get("options", {})
    columns = options.get("columns")
    remove_punctuation = options.get("remove_punctuation", True)
    collapse_whitespace = options.get("collapse_whitespace", True)

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
        cleaned = raw.apply(
            lambda v: _normalize_cell(v, remove_punctuation, collapse_whitespace)
        )
        changed = (raw != cleaned).sum()
        total_changed += changed
        df.loc[non_null, col] = cleaned

    report.record_module(
        config["module_id"],
        {"columns_processed": len(cols), "values_changed": int(total_changed)},
    )
    return df
