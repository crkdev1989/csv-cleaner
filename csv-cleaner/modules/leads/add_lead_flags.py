"""
Add row-level boolean flags: has_email, has_contact_page, has_phone.
Used for master leads output so downstream can filter without dropping rows.
"""

import pandas as pd

from cleaner.report import CleaningReport


def _column_option(value: object, default: str) -> str:
    """Return a single column name string; avoid list/Series in df[col]."""
    if value is None:
        return default
    if isinstance(value, (list, tuple)) and len(value) > 0:
        return str(value[0]).strip() or default
    return str(value).strip() or default


def _has_value(series: pd.Series | pd.DataFrame) -> pd.Series:
    """True where value is non-null and (if str) non-empty after strip."""
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    out = series.notna()
    col_dtype = series.dtype
    if col_dtype == object or str(col_dtype) == "object":
        out = out & series.astype(str).str.strip().astype(bool)
    return out


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Add has_email, has_contact_page, has_phone. config["options"] may contain:
    - email_column, contact_page_column, phone_column (defaults: email, contact_page_url, phone).
    """
    options = config.get("options", {})
    email_col = _column_option(options.get("email_column"), "email")
    contact_page_col = _column_option(options.get("contact_page_column"), "contact_page_url")
    phone_col = _column_option(options.get("phone_column"), "phone")

    df = df.copy()
    stats = {}

    if email_col in df.columns:
        df["has_email"] = _has_value(df[email_col])
        stats["has_email_true"] = int(df["has_email"].sum())
    else:
        df["has_email"] = pd.Series(False, index=df.index)
        stats["has_email_missing_column"] = True

    if contact_page_col in df.columns:
        df["has_contact_page"] = _has_value(df[contact_page_col])
        stats["has_contact_page_true"] = int(df["has_contact_page"].sum())
    else:
        df["has_contact_page"] = pd.Series(False, index=df.index)
        stats["has_contact_page_missing_column"] = True

    if phone_col in df.columns:
        df["has_phone"] = _has_value(df[phone_col])
        stats["has_phone_true"] = int(df["has_phone"].sum())
    else:
        df["has_phone"] = pd.Series(False, index=df.index)
        stats["has_phone_missing_column"] = True

    report.record_module(config["module_id"], stats)
    return df
