"""
Pick best contact values from multi-value semicolon-delimited columns and apply fallbacks.
Used for raw website enrichment output: filter junk emails/phones, prefer contact URLs,
derive law_firm from page_title or domain when blank.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from cleaner.lead_cleanup import (
    classify_email_quality,
    classify_phone_quality,
    is_phone_acceptable,
    normalize_root_domain,
    phone_digits,
)
from cleaner.report import CleaningReport


def _is_null_like(value: Any) -> bool:
    """Treat None, NaN, pandas NA, string 'nan'/'NaN', empty/whitespace as empty."""
    if value is None:
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str):
        s = value.strip()
        return not s or s.lower() == "nan"
    return False


def _strip_null_like(value: Any) -> str:
    """Return non-empty string or ''; never return literal 'nan'."""
    if _is_null_like(value):
        return ""
    s = str(value).strip()
    return "" if s.lower() == "nan" else s


def _split_semicolon(value: Any) -> list[str]:
    if value is None or (isinstance(value, str) and not value.strip()):
        return []
    s = str(value).strip()
    return [p.strip() for p in s.split(";") if p.strip()]


def _pick_best_email(
    candidates: list[str],
    normalized_domain: str,
    existing: Any,
) -> str:
    """First non-junk, non-vendor email; else keep existing if it passes."""
    for addr in candidates:
        if not addr or "@" not in addr:
            continue
        r = classify_email_quality(addr, normalized_domain=normalized_domain)
        if r["email_is_junk"] or r["email_quality"] == "vendor_platform":
            continue
        return addr.strip().lower()
    if existing and isinstance(existing, str) and existing.strip() and "@" in existing:
        r = classify_email_quality(existing, normalized_domain=normalized_domain)
        if not r["email_is_junk"] and r["email_quality"] != "vendor_platform":
            return existing.strip().lower()
    return ""


def _normalize_phone_to_10_digits(value: Any) -> str:
    """Return 10-digit US form or empty string."""
    d = phone_digits(value)
    if not d:
        return ""
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return d if len(d) == 10 else ""


def _pick_best_phone(candidates: list[str], existing: Any) -> str:
    """
    Use only the single trusted contact_phone field when valid. Do not promote
    from aggregated phones list (no strong trust signal); leave blank rather
    than choosing a suspicious value from the list.
    """
    def _valid_raw(raw: Any) -> str:
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return ""
        s = str(raw).strip()
        digits = _normalize_phone_to_10_digits(s)
        if digits and is_phone_acceptable(digits):
            return s
        return ""

    existing_val = _valid_raw(existing)
    if existing_val:
        return existing_val
    return ""


def _pick_best_contact_url(candidates: list[str], keyword: str, existing: Any) -> str:
    """First URL containing keyword (e.g. 'contact'), else first URL, else existing."""
    with_keyword = [u for u in candidates if keyword in u.lower()]
    if with_keyword:
        return with_keyword[0].strip()
    if candidates:
        return candidates[0].strip()
    if existing and isinstance(existing, str) and existing.strip():
        return existing.strip()
    return ""


_FIRM_LIKE_KEYWORDS = ("law", "llc", "p.a.", "p.c.", "pc", "pa", "attorney", "attorneys", "&")


def _law_firm_from_page_title(value: Any, max_len: int = 80) -> str:
    """
    Prefer a segment that looks like a firm name (contains Law, LLC, P.A., etc.);
    else first segment before |. Cap length at max_len. Never return null-like.
    """
    if _is_null_like(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    segments = [seg.strip() for seg in s.split("|") if seg.strip() and seg.strip().lower() != "nan"]
    for seg in segments:
        seg_lower = seg.lower()
        if any(kw in seg_lower for kw in _FIRM_LIKE_KEYWORDS):
            return seg[:max_len]
    if segments:
        return segments[0][:max_len]
    return ""


def _law_firm_from_domain(domain: str) -> str:
    """
    Derive a readable firm name from root domain (e.g. stavroslawfirm.com -> Stavros Law Firm).
    Never return null-like.
    """
    if _is_null_like(domain):
        return ""
    d = str(domain).strip().lower()
    if not d:
        return ""
    parts = d.split(".")
    if not parts:
        return ""
    name = parts[0]
    if name in ("www", "mail", "ftp"):
        return ""
    for suffix in ("lawfirm", "lawfirms", "lawyer", "lawyers", "lawoffice", "law", "llc", "pa", "pc"):
        if name.endswith(suffix) and len(name) > len(suffix):
            base = name[: -len(suffix)]
            if base:
                return (base.title() + " " + suffix.replace("lawoffice", "Law Office").title()).strip()
    name_display = name.replace("-", " ").title()
    return name_display


def run(
    df: pd.DataFrame,
    config: dict[str, Any],
    report: CleaningReport,
) -> pd.DataFrame:
    options = config.get("options", {})
    emails_column = str(options.get("emails_column") or "emails")
    phones_column = str(options.get("phones_column") or "phones")
    contact_pages_column = str(options.get("contact_pages_column") or "contact_pages")
    website_column = str(options.get("website_column") or "website")
    email_column = str(options.get("email_column") or "email")
    phone_column = str(options.get("phone_column") or "contact_phone")
    contact_page_url_column = str(
        options.get("contact_page_url_column") or "contact_page_url"
    )
    law_firm_column = str(options.get("law_firm_column") or "law_firm")
    page_title_column = str(options.get("page_title_column") or "page_title")
    contact_keyword = str(options.get("contact_keyword") or "contact").lower()

    rows = df.to_dict(orient="records")
    out: list[dict[str, Any]] = []
    emails_set = 0
    phones_set = 0
    contact_url_set = 0
    law_firm_set = 0

    for row in rows:
        new_row = dict(row)
        website = new_row.get(website_column)
        normalized_domain = normalize_root_domain(website) if website else ""

        if emails_column in new_row:
            candidates = _split_semicolon(new_row.get(emails_column))
            existing = new_row.get(email_column)
            best = _pick_best_email(candidates, normalized_domain, existing)
            if best:
                if new_row.get(email_column) != best:
                    emails_set += 1
                new_row[email_column] = best
            elif candidates or existing:
                new_row[email_column] = best

        if phones_column in new_row:
            candidates = _split_semicolon(new_row.get(phones_column))
            existing = new_row.get(phone_column)
            best = _pick_best_phone(candidates, existing)
            if best:
                if new_row.get(phone_column) != best:
                    phones_set += 1
                new_row[phone_column] = best
            elif candidates or existing:
                new_row[phone_column] = best

        if contact_pages_column in new_row:
            candidates = _split_semicolon(new_row.get(contact_pages_column))
            existing = new_row.get(contact_page_url_column)
            best = _pick_best_contact_url(candidates, contact_keyword, existing)
            if best:
                if new_row.get(contact_page_url_column) != best:
                    contact_url_set += 1
                new_row[contact_page_url_column] = best

        if law_firm_column in new_row:
            current = new_row.get(law_firm_column)
            current_str = _strip_null_like(current)
            from_title = _law_firm_from_page_title(new_row.get(page_title_column))
            from_domain = _law_firm_from_domain(normalized_domain) if normalized_domain else ""
            best_firm = current_str or from_title or from_domain
            best_firm = _strip_null_like(best_firm)
            if best_firm:
                if not current_str and (from_title or from_domain):
                    law_firm_set += 1
                new_row[law_firm_column] = best_firm
            else:
                new_row[law_firm_column] = ""

        out.append(new_row)

    result = pd.DataFrame(out)
    if not result.empty:
        result = result.reindex(columns=list(df.columns))

    report.record_module(
        config["module_id"],
        {
            "rows_processed": len(out),
            "emails_replaced_from_multi": emails_set,
            "phones_replaced_from_multi": phones_set,
            "contact_page_url_replaced_from_multi": contact_url_set,
            "law_firm_filled_from_fallback": law_firm_set,
        },
    )
    return result
