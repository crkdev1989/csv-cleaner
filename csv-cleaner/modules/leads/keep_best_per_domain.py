"""
Collapse lead rows to the best record per normalized domain.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import pandas as pd

from cleaner.lead_cleanup import (
    is_firm_domain_email_quality,
    is_valid_phone_quality,
    row_completeness,
)
from cleaner.report import CleaningReport


DEFAULT_COMPLETENESS_EXCLUDED_FIELDS = {
    "normalized_domain",
    "page_type",
    "email_domain",
    "email_quality",
    "email_is_junk",
    "email_is_free_provider",
    "phone_digits",
    "phone_quality",
    "phone_is_placeholder",
    "lead_score",
    "lead_score_reason",
}


def run(
    df: pd.DataFrame,
    config: dict[str, Any],
    report: CleaningReport,
) -> pd.DataFrame:
    options = config.get("options", {})
    domain_column = str(options.get("domain_column") or "normalized_domain")
    score_column = str(options.get("score_column") or "lead_score")
    page_type_column = str(options.get("page_type_column") or "page_type")
    email_quality_column = str(options.get("email_quality_column") or "email_quality")
    phone_quality_column = str(options.get("phone_quality_column") or "phone_quality")
    keep_empty_domain_rows = bool(options.get("keep_empty_domain_rows", True))
    excluded_fields = _as_string_set(
        options.get("completeness_excluded_fields"),
        DEFAULT_COMPLETENESS_EXCLUDED_FIELDS,
    )

    indexed_rows = list(enumerate(df.to_dict(orient="records")))
    grouped_rows: dict[str, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    kept_rows: list[tuple[int, dict[str, Any]]] = []
    empty_domain_rows_dropped = 0

    for index, row in indexed_rows:
        domain = str(row.get(domain_column) or "").strip().lower()
        if not domain:
            if keep_empty_domain_rows:
                kept_rows.append((index, row))
            else:
                empty_domain_rows_dropped += 1
            continue
        grouped_rows[domain].append((index, row))

    groups_collapsed = 0
    rows_removed = empty_domain_rows_dropped
    for domain_rows in grouped_rows.values():
        best = max(
            domain_rows,
            key=lambda item: _rank_row(
                item[1],
                item[0],
                score_column=score_column,
                page_type_column=page_type_column,
                email_quality_column=email_quality_column,
                phone_quality_column=phone_quality_column,
                excluded_fields=excluded_fields,
            ),
        )
        kept_rows.append(best)
        removed = max(0, len(domain_rows) - 1)
        if removed:
            groups_collapsed += 1
            rows_removed += removed

    kept_rows.sort(key=lambda item: item[0])
    deduped_rows = [row for _, row in kept_rows]
    deduped_df = pd.DataFrame(deduped_rows)
    if not deduped_df.empty:
        deduped_df = deduped_df.reindex(columns=list(df.columns))

    report.duplicates_removed += rows_removed
    report.record_module(
        config["module_id"],
        {
            "domain_groups": len(grouped_rows),
            "groups_collapsed": groups_collapsed,
            "rows_removed": rows_removed,
            "empty_domain_rows_kept": len(
                [1 for index, row in indexed_rows if not str(row.get(domain_column) or "").strip()]
            )
            if keep_empty_domain_rows
            else 0,
            "empty_domain_rows_dropped": empty_domain_rows_dropped,
        },
    )
    return deduped_df


def _rank_row(
    row: dict[str, Any],
    index: int,
    *,
    score_column: str,
    page_type_column: str,
    email_quality_column: str,
    phone_quality_column: str,
    excluded_fields: set[str],
) -> tuple[int, int, int, int, int, int]:
    page_type = str(row.get(page_type_column) or "")
    email_quality = row.get(email_quality_column)
    phone_quality = row.get(phone_quality_column)
    homepage_rank = 1 if page_type == "firm_homepage" else 0
    firm_email_rank = (
        2
        if str(email_quality or "") == "valid_firm_email"
        else 1
        if is_firm_domain_email_quality(email_quality)
        else 0
    )
    valid_phone_rank = 1 if is_valid_phone_quality(phone_quality) else 0
    completeness_rank = row_completeness(row, excluded_fields=excluded_fields)
    return (
        int(row.get(score_column) or 0),
        homepage_rank,
        firm_email_rank,
        valid_phone_rank,
        completeness_rank,
        -index,
    )


def _as_string_set(value: Any, default: set[str]) -> set[str]:
    if not isinstance(value, (list, tuple, set)):
        return set(default)
    return {str(item).strip() for item in value if str(item).strip()}
