"""
Add lead-specific normalization, classification, and scoring columns.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from cleaner.lead_cleanup import (
    DEFAULT_ASSET_EXTENSIONS,
    DEFAULT_DIRECTORY_DOMAINS,
    DEFAULT_DIRECTORY_PATH_KEYWORDS,
    DEFAULT_FREE_EMAIL_DOMAINS,
    DEFAULT_GENERIC_EMAIL_PREFIXES,
    DEFAULT_SOCIAL_DOMAINS,
    DEFAULT_TEAM_PATH_KEYWORDS,
    DEFAULT_ATTORNEY_PATH_KEYWORDS,
    DEFAULT_VENDOR_EMAIL_DOMAINS,
    build_lead_score,
    classify_email_quality,
    classify_page_type,
    classify_phone_quality,
    normalize_root_domain,
)
from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict[str, Any],
    report: CleaningReport,
) -> pd.DataFrame:
    options = config.get("options", {})
    website_column = str(options.get("website_column") or "website")
    email_column = str(options.get("email_column") or "email")
    phone_column = str(options.get("phone_column") or "phone")
    firm_name_column = str(options.get("firm_name_column") or "firm_name")
    contact_name_column = str(options.get("contact_name_column") or "contact_name")

    normalized_domain_column = str(options.get("normalized_domain_column") or "normalized_domain")
    page_type_column = str(options.get("page_type_column") or "page_type")
    email_domain_column = str(options.get("email_domain_column") or "email_domain")
    email_quality_column = str(options.get("email_quality_column") or "email_quality")
    email_is_junk_column = str(options.get("email_is_junk_column") or "email_is_junk")
    email_is_free_provider_column = str(
        options.get("email_is_free_provider_column") or "email_is_free_provider"
    )
    phone_digits_column = str(options.get("phone_digits_column") or "phone_digits")
    phone_quality_column = str(options.get("phone_quality_column") or "phone_quality")
    phone_is_placeholder_column = str(
        options.get("phone_is_placeholder_column") or "phone_is_placeholder"
    )
    lead_score_column = str(options.get("lead_score_column") or "lead_score")
    lead_score_reason_column = str(
        options.get("lead_score_reason_column") or "lead_score_reason"
    )

    normalize_domain = bool(options.get("normalize_domain", True))
    classify_page_type_enabled = bool(options.get("classify_page_type", True))
    classify_email_quality_enabled = bool(options.get("classify_email_quality", True))
    classify_phone_quality_enabled = bool(options.get("classify_phone_quality", True))
    score_rows = bool(options.get("score_rows", True))
    include_score_reasons = bool(options.get("include_score_reasons", True))

    social_domains = _as_lower_set(options.get("social_domains"), DEFAULT_SOCIAL_DOMAINS)
    directory_domains = _as_lower_set(options.get("directory_domains"), DEFAULT_DIRECTORY_DOMAINS)
    free_email_domains = _as_lower_set(
        options.get("free_email_domains"),
        DEFAULT_FREE_EMAIL_DOMAINS,
    )
    vendor_email_domains = _as_lower_set(
        options.get("vendor_email_domains"),
        DEFAULT_VENDOR_EMAIL_DOMAINS,
    )
    generic_prefixes = _as_lower_set(
        options.get("generic_email_prefixes"),
        DEFAULT_GENERIC_EMAIL_PREFIXES,
    )
    asset_extensions = _as_lower_set(
        options.get("asset_extensions"),
        DEFAULT_ASSET_EXTENSIONS,
    )
    attorney_path_keywords = _as_lower_tuple(
        options.get("attorney_path_keywords"),
        DEFAULT_ATTORNEY_PATH_KEYWORDS,
    )
    team_path_keywords = _as_lower_tuple(
        options.get("team_path_keywords"),
        DEFAULT_TEAM_PATH_KEYWORDS,
    )
    directory_path_keywords = _as_lower_tuple(
        options.get("directory_path_keywords"),
        DEFAULT_DIRECTORY_PATH_KEYWORDS,
    )
    score_weights = options.get("score_weights")

    existing_columns = list(df.columns)
    rows = df.to_dict(orient="records")
    enriched_rows: list[dict[str, Any]] = []

    for row in rows:
        enriched = dict(row)
        website_value = enriched.get(website_column)
        email_value = enriched.get(email_column)
        phone_value = enriched.get(phone_column)

        normalized_domain = (
            normalize_root_domain(website_value)
            if normalize_domain
            else str(enriched.get(normalized_domain_column) or "")
        )
        enriched[normalized_domain_column] = normalized_domain

        if classify_page_type_enabled:
            enriched[page_type_column] = classify_page_type(
                website_value,
                social_domains=social_domains,
                directory_domains=directory_domains,
                directory_path_keywords=directory_path_keywords,
                attorney_path_keywords=attorney_path_keywords,
                team_path_keywords=team_path_keywords,
            )

        if classify_email_quality_enabled:
            email_result = classify_email_quality(
                email_value,
                normalized_domain=normalized_domain,
                free_email_domains=free_email_domains,
                vendor_email_domains=vendor_email_domains,
                generic_prefixes=generic_prefixes,
                asset_extensions=asset_extensions,
            )
            enriched[email_domain_column] = email_result["email_domain"]
            enriched[email_quality_column] = email_result["email_quality"]
            enriched[email_is_junk_column] = bool(email_result["email_is_junk"])
            enriched[email_is_free_provider_column] = bool(
                email_result["email_is_free_provider"]
            )

        if classify_phone_quality_enabled:
            phone_result = classify_phone_quality(phone_value)
            enriched[phone_digits_column] = phone_result["phone_digits"]
            enriched[phone_quality_column] = phone_result["phone_quality"]
            enriched[phone_is_placeholder_column] = bool(
                phone_result["phone_is_placeholder"]
            )

        if score_rows:
            lead_score, reasons = build_lead_score(
                enriched,
                weights=score_weights,
                normalized_domain_column=normalized_domain_column,
                page_type_column=page_type_column,
                email_quality_column=email_quality_column,
                phone_quality_column=phone_quality_column,
                contact_name_column=contact_name_column,
                firm_name_column=firm_name_column,
            )
            enriched[lead_score_column] = lead_score
            if include_score_reasons:
                enriched[lead_score_reason_column] = "; ".join(reasons)

        enriched_rows.append(enriched)

    output_columns = list(existing_columns)
    for row in enriched_rows:
        for key in row.keys():
            if key not in output_columns:
                output_columns.append(key)

    enriched_df = pd.DataFrame(enriched_rows)
    if not enriched_df.empty:
        enriched_df = enriched_df.reindex(columns=output_columns)

    report.record_module(
        config["module_id"],
        {
            "rows_processed": len(enriched_rows),
            "normalized_domains_found": int(
                enriched_df[normalized_domain_column].astype(str).str.strip().ne("").sum()
            )
            if normalized_domain_column in enriched_df.columns
            else 0,
            "page_type_counts": _value_counts(enriched_df, page_type_column),
            "email_quality_counts": _value_counts(enriched_df, email_quality_column),
            "phone_quality_counts": _value_counts(enriched_df, phone_quality_column),
        },
    )
    return enriched_df


def _as_lower_set(value: Any, default: set[str]) -> set[str]:
    if not isinstance(value, (list, tuple, set)):
        return set(default)
    return {str(item).strip().lower() for item in value if str(item).strip()}


def _as_lower_tuple(value: Any, default: tuple[str, ...]) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple, set)):
        return tuple(default)
    return tuple(str(item).strip().lower() for item in value if str(item).strip())


def _value_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
    if column not in df.columns:
        return {}
    counts = (
        df[column]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "<empty>")
        .value_counts()
        .to_dict()
    )
    return {str(key): int(value) for key, value in counts.items()}
