"""
Drop duplicate rows, optionally by subset of columns.
Used for the strict (email-ready) output path.

Milestone 3: Supports email-first dedupe so rows with different valid emails survive
as separate strict leads; same email dedupes to one row; fallback dedupe only when email is missing.
Strict path: when all rows have email (e.g. after leads.filter_has_email), key = normalized email only.
"""

import logging
from typing import Any

import pandas as pd

from cleaner.report import CleaningReport

logger = logging.getLogger(__name__)

# Delimiter for composite keys (must not appear in normal data)
_KEY_SEP = "\x00"


def _normalize_email_for_key(val: Any) -> str:
    """Normalize email to a canonical string for dedupe key; empty if missing/invalid."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip().lower()
    if not s or "@" not in s:
        return ""
    return s


def _resolve_email_column(df: pd.DataFrame, option_name: str) -> str | None:
    """Return actual column name for email: exact match, then first case-insensitive match."""
    if option_name in df.columns:
        return option_name
    low = option_name.lower()
    for c in df.columns:
        if getattr(c, "lower", None) and c.lower() == low:
            return c
    return None


def _build_fallback_key(row: pd.Series, columns: list[str]) -> str:
    """Build a composite key from fallback columns for rows without email."""
    parts = []
    for col in columns:
        if col not in row.index:
            parts.append("")
            continue
        v = row[col]
        if v is None or (isinstance(v, float) and pd.isna(v)):
            parts.append("")
        else:
            parts.append(str(v).strip().lower())
    return _KEY_SEP.join(parts)


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Remove duplicate rows. config["options"] may contain:
    - mode: "subset" | "email_first" (default "subset"). When "email_first", one row per
      distinct normalized email; rows without email use fallback_subset to form a key.
    - email_column: column name for email (default "email"), used when mode is "email_first".
    - fallback_subset: list of columns for composite key when email is missing (default
      ["website", "profile_url", "input_url", "name", "law_firm", "city", "state"]).
    - subset: list of columns for duplicates when mode is "subset" (legacy).
    - keep: "first" | "last" (default "first").
    """
    options = config.get("options", {})
    mode = str(options.get("mode") or "subset").strip().lower()
    keep = options.get("keep", "first")
    rows_before = len(df)

    if mode == "email_first":
        email_col_opt = str(options.get("email_column") or "email").strip() or "email"
        email_col = _resolve_email_column(df, email_col_opt)
        fallback_cols = options.get("fallback_subset")
        if fallback_cols is None:
            fallback_cols = [
                "website",
                "profile_url",
                "input_url",
                "name",
                "law_firm",
                "city",
                "state",
            ]
        fallback_cols = [c for c in fallback_cols if isinstance(c, str) and c.strip()]
        if not fallback_cols:
            fallback_cols = ["website", "input_url"]

        # Build normalized email from dataframe column (vectorized) so we use the correct column
        if email_col is not None:
            email_series = df[email_col]
            normalized_emails = email_series.apply(_normalize_email_for_key)
            rows_with_email_before = int((normalized_emails != "").sum())
            unique_emails_before = int(normalized_emails[normalized_emails != ""].nunique())
        else:
            normalized_emails = pd.Series([""] * len(df), index=df.index)
            rows_with_email_before = 0
            unique_emails_before = 0

        # Debug: strict path pre-dedupe
        all_have_email = rows_with_email_before == rows_before and rows_before > 0
        key_mode = "email_only" if all_have_email else "email_then_fallback"
        dedupe_key_columns = [email_col] if (all_have_email and email_col) else ([email_col] if email_col else []) + fallback_cols
        logger.info(
            "strict_dedupe: rows_entering=%d non_empty_email_count=%d unique_emails=%d email_column=%s key_mode=%s dedupe_key_columns=%s",
            rows_before,
            rows_with_email_before,
            unique_emails_before,
            email_col or "(missing)",
            key_mode,
            dedupe_key_columns,
        )
        if rows_before > 0 and rows_before <= 20:
            logger.debug("strict_dedupe: columns=%s", list(df.columns))
            logger.debug("strict_dedupe: sample_emails=%s", normalized_emails.head(5).tolist())

        # Strict path: when ALL rows have non-empty email, use normalized email as the ONLY key (no fallback)
        if all_have_email and email_col is not None:
            key_series = normalized_emails
            df = df.copy()
            df["_strict_dedupe_key"] = key_series
            rows_before_dedupe = len(df)
            df = df.drop_duplicates(subset=["_strict_dedupe_key"], keep=keep)
            removed_total = rows_before_dedupe - len(df)
            removed_email_based = removed_total
            removed_fallback_based = 0
            logger.info(
                "strict_dedupe: email_only_key rows_removed=%d rows_after=%d unique_emails_used=%d",
                removed_total,
                len(df),
                int(key_series.nunique()),
            )
        else:
            # Mixed: email when present, else fallback (use vectorized email, apply only for fallback rows)
            key_series = pd.Series("", index=df.index, dtype=object)
            has_em = normalized_emails != ""
            key_series.loc[has_em] = "e" + _KEY_SEP + normalized_emails.loc[has_em]
            if not has_em.all():
                fallback_mask = ~has_em
                key_series.loc[fallback_mask] = df.loc[fallback_mask].apply(
                    lambda row: "f" + _KEY_SEP + _build_fallback_key(row, fallback_cols),
                    axis=1,
                )
            df = df.copy()
            df["_strict_dedupe_key"] = key_series
            rows_before_dedupe = len(df)
            df = df.drop_duplicates(subset=["_strict_dedupe_key"], keep=keep)
            removed_total = rows_before_dedupe - len(df)
            removed_indices = key_series.index.difference(df.index)
            keys_removed = key_series.loc[removed_indices] if len(removed_indices) > 0 else pd.Series(dtype=object)
            if len(keys_removed) > 0:
                removed_email_based = int((keys_removed.str.startswith("e" + _KEY_SEP)).sum())
                removed_fallback_based = int((keys_removed.str.startswith("f" + _KEY_SEP)).sum())
            else:
                removed_email_based = 0
                removed_fallback_based = 0
            logger.info(
                "strict_dedupe: rows_removed_by_email=%d rows_removed_by_fallback=%d total_removed=%d rows_after=%d",
                removed_email_based,
                removed_fallback_based,
                removed_total,
                len(df),
            )

        if "_strict_dedupe_key" in df.columns:
            df = df.drop(columns=["_strict_dedupe_key"])

        report.duplicates_removed += removed_total
        report.record_module(
            config["module_id"],
            {
                "duplicates_removed": removed_total,
                "rows_before_dedupe": rows_before,
                "rows_with_non_empty_email_before": rows_with_email_before,
                "unique_emails_before": unique_emails_before,
                "rows_after_dedupe": len(df),
                "rows_removed_by_email_dedupe": removed_email_based,
                "rows_removed_by_fallback_dedupe": removed_fallback_based,
                "strict_email_column_resolved": email_col,
                "strict_key_mode": key_mode,
                "strict_dedupe_key_columns": dedupe_key_columns,
            },
        )
        return df

    # Legacy subset-based dedupe
    raw_subset = options.get("subset")
    if raw_subset is None:
        subset_list = None
    elif isinstance(raw_subset, (list, tuple)):
        subset_list = list(raw_subset)
    elif hasattr(raw_subset, "__iter__") and not isinstance(raw_subset, str):
        subset_list = list(raw_subset)
    else:
        subset_list = [raw_subset]

    if subset_list is not None and len(subset_list) > 0:
        # Filter to columns that exist
        subset_list = [c for c in subset_list if c in df.columns]
    if subset_list is not None and len(subset_list) > 0:
        df = df.drop_duplicates(subset=subset_list, keep=keep)
    else:
        df = df.drop_duplicates(keep=keep)

    removed = rows_before - len(df)
    report.duplicates_removed += removed
    report.record_module(config["module_id"], {"duplicates_removed": removed})
    return df
