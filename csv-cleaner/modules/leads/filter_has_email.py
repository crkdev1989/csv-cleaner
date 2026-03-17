"""
Keep only rows that have a non-empty, valid email for the strict (email-ready) path.

Run this BEFORE dedupe so that:
1. Strict pipeline filters to rows with email first
2. Dedupe then runs on that subset with email as the key (no fallback needed)
Different emails survive; same email dedupes to one row.
"""

from typing import Any

import pandas as pd

from cleaner.report import CleaningReport


def _normalize_email(val: Any) -> str:
    """Return normalized email for key/validation; empty string if missing or invalid."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip().lower()
    if not s or "@" not in s:
        return ""
    return s


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Keep only rows where the email column is non-empty and valid (contains @).
    config["options"] may contain:
    - email_column: column name (default "email").
    """
    options = config.get("options", {})
    email_col = str(options.get("email_column") or "email").strip() or "email"

    if email_col not in df.columns:
        report.record_module(config["module_id"], {"rows_dropped": 0, "reason": "column_missing"})
        return df

    before = len(df)
    mask = df[email_col].apply(lambda v: bool(_normalize_email(v)))
    df = df.loc[mask].copy()
    dropped = before - len(df)

    report.rows_dropped += dropped
    report.rows_dropped_strict_filter += dropped
    report.record_module(
        config["module_id"],
        {"rows_dropped": dropped, "rows_before": before, "rows_after": len(df), "email_column": email_col},
    )
    return df
