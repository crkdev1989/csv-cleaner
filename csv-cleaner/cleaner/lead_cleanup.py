"""
Helpers for optional lead-specific cleanup and scoring.
"""

from __future__ import annotations

from collections import Counter
from typing import Any
from urllib.parse import urlsplit
import re

import pandas as pd


COMMON_SECOND_LEVEL_SUFFIXES = {
    "ac",
    "co",
    "com",
    "edu",
    "gov",
    "net",
    "org",
}
COMMON_COUNTRY_SUFFIXES = {
    "au",
    "jp",
    "nz",
    "uk",
}
DEFAULT_SOCIAL_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "x.com",
    "twitter.com",
    "youtube.com",
}
DEFAULT_DIRECTORY_DOMAINS = {
    "avvo.com",
    "findlaw.com",
    "justia.com",
    "martindale.com",
    "superlawyers.com",
    "thenationaltriallawyers.org",
}
DEFAULT_DIRECTORY_PATH_KEYWORDS = (
    "/directory",
    "/member-directory",
    "/members/",
    "/lawyers/",
    "/attorneys/",
)
DEFAULT_ATTORNEY_PATH_KEYWORDS = (
    "/attorney/",
    "/attorneys/",
    "/lawyer/",
    "/lawyers/",
)
DEFAULT_TEAM_PATH_KEYWORDS = (
    "/our-team/",
    "/team/",
    "/meet-the-team/",
    "/staff/",
)
DEFAULT_FREE_EMAIL_DOMAINS = {
    "aol.com",
    "gmail.com",
    "hotmail.com",
    "icloud.com",
    "live.com",
    "mac.com",
    "me.com",
    "msn.com",
    "outlook.com",
    "yahoo.com",
}
DEFAULT_VENDOR_EMAIL_DOMAINS = {
    "godaddy.com",
    "mopro.com",
    "sentry.com",
    "sentry.io",
    "squarespace.com",
    "webador.com",
    "weebly.com",
    "wix.com",
    "wixpress.com",
    "wordpress.com",
}
DEFAULT_GENERIC_EMAIL_PREFIXES = {
    "admin",
    "contact",
    "help",
    "info",
    "intake",
    "office",
    "referrals",
    "support",
}
DEFAULT_ASSET_EXTENSIONS = {
    "avif",
    "css",
    "gif",
    "ico",
    "jpeg",
    "jpg",
    "js",
    "pdf",
    "png",
    "svg",
    "webp",
}
DEFAULT_SCORE_WEIGHTS = {
    "page_type": {
        "firm_homepage": 4,
        "attorney_profile": -4,
        "team_page": -3,
        "social_profile": -6,
        "directory_page": -5,
        "unknown": 0,
    },
    "email_quality": {
        "valid_firm_email": 5,
        "generic_firm_email": 3,
        "generic_usable_email": 2,
        "free_provider": -2,
        "junk_asset": -6,
        "vendor_platform": -4,
        "other": 0,
        "empty": 0,
    },
    "phone_quality": {
        "valid": 2,
        "placeholder": -4,
        "suspicious": -1,
        "empty": 0,
    },
    "contact_name_present": 1,
    "firm_name_present": 2,
    "missing_normalized_domain": -8,
}
EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
NON_DIGIT_PATTERN = re.compile(r"\D+")

# Milestone 4: Conservative email validation. Only reject clearly bad; preserve generic role addresses.

# Domains that are always placeholder/fake (reject)
PLACEHOLDER_EMAIL_DOMAINS = frozenset({
    "example.com", "example.org", "example.net", "example.edu",
    "email.com", "test.com", "test.org", "foo.com", "bar.com",
    "domain.com", "sample.com", "invalid.com", "placeholder.com",
})
# Local parts that are non-outreach (noreply etc.); reject even at firm domain
NO_REPLY_LOCAL_PARTS = frozenset({
    "noreply", "no-reply", "donotreply", "do-not-reply", "no_reply",
})
# Generic role addresses we must NEVER reject (intake@, info@, contact@, etc.)
GENERIC_ROLE_LOCAL_PARTS_PRESERVE = frozenset({
    "admin", "contact", "help", "info", "intake", "office", "referrals", "support",
})
# Domain suffixes that indicate asset/file, not email (reject)
ASSET_DOMAIN_SUFFIXES = frozenset({"png", "jpg", "jpeg", "gif", "svg", "webp", "ico", "pdf", "css", "js"})


def normalize_email_for_validation(value: Any) -> str:
    """
    Safe normalization for validation: trim, lowercase, strip angle brackets and trailing punctuation.
    Preserves the actual address; does not over-transform.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).strip()
    if not s:
        return ""
    s = s.lower()
    if s.startswith("<") and ">" in s:
        s = s.split(">", 1)[0].lstrip("<").strip()
    if s and s[-1] in (",", ";", "."):
        s = s.rstrip(".,;").strip()
    return s


def is_email_clearly_junk(email: str) -> tuple[bool, str]:
    """
    Conservative: return (True, reason) only when email is clearly junk or placeholder.
    Preserves generic role addresses (info@, intake@, contact@, office@, help@, admin@).
    Returns (False, "") for any plausible real address.
    """
    if not email or "@" not in email:
        return (True, "malformed")
    parts = email.split("@", 1)
    local = (parts[0] or "").strip().lower()
    domain = (parts[1] or "").strip().lower()
    if not local or not domain:
        return (True, "malformed")
    if " " in email or "." not in domain:
        return (True, "malformed")
    # Plausible hostname: alphanumeric, dots, hyphens
    if not re.match(r"^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$", domain):
        return (True, "malformed")
    # Asset-like domain (e.g. x@2x.png)
    if domain.split(".")[-1].lower() in ASSET_DOMAIN_SUFFIXES:
        return (True, "placeholder_junk")
    if "/" in domain or "\\" in domain:
        return (True, "placeholder_junk")
    # Placeholder domains
    if domain in PLACEHOLDER_EMAIL_DOMAINS:
        return (True, "placeholder_junk")
    # Literal placeholder pairs
    if (local, domain) in (
        ("test", "test.com"), ("example", "example.com"), ("foo", "bar.com"),
        ("user", "example.com"), ("admin", "example.com"), ("info", "example.com"),
        ("sample", "example.com"), ("john", "doe.com"), ("johndoe", "email.com"),
    ):
        return (True, "placeholder_junk")
    if "johndoe" in local and "email.com" in domain:
        return (True, "placeholder_junk")
    # noreply / no-reply / donotreply: not useful for outreach
    if local in NO_REPLY_LOCAL_PARTS:
        return (True, "placeholder_junk")
    # Never reject generic role addresses (intake@, info@, contact@, office@, help@, admin@)
    if local in GENERIC_ROLE_LOCAL_PARTS_PRESERVE:
        return (False, "")
    # flags@2x.png style
    if "2x" in domain or "flags" in local and "png" in domain:
        return (True, "placeholder_junk")
    return (False, "")


def normalize_root_domain(value: Any) -> str:
    parsed = _safe_urlsplit(value)
    if parsed is None:
        return ""

    hostname = (parsed.hostname or "").strip().lower()
    if not hostname:
        return ""
    if hostname.startswith("www."):
        hostname = hostname[4:]
    if not _looks_like_hostname(hostname):
        return ""
    return _root_domain(hostname)


def classify_page_type(
    value: Any,
    *,
    social_domains: set[str] | None = None,
    directory_domains: set[str] | None = None,
    directory_path_keywords: tuple[str, ...] | None = None,
    attorney_path_keywords: tuple[str, ...] | None = None,
    team_path_keywords: tuple[str, ...] | None = None,
) -> str:
    parsed = _safe_urlsplit(value)
    if parsed is None:
        return "unknown"

    hostname = normalize_root_domain(value)
    path = (parsed.path or "/").strip().lower() or "/"

    social_domains = social_domains or set(DEFAULT_SOCIAL_DOMAINS)
    directory_domains = directory_domains or set(DEFAULT_DIRECTORY_DOMAINS)
    directory_path_keywords = directory_path_keywords or DEFAULT_DIRECTORY_PATH_KEYWORDS
    attorney_path_keywords = attorney_path_keywords or DEFAULT_ATTORNEY_PATH_KEYWORDS
    team_path_keywords = team_path_keywords or DEFAULT_TEAM_PATH_KEYWORDS

    if hostname in social_domains:
        return "social_profile"
    if hostname in directory_domains or any(keyword in path for keyword in directory_path_keywords):
        return "directory_page"
    if any(keyword in path for keyword in attorney_path_keywords):
        return "attorney_profile"
    if any(keyword in path for keyword in team_path_keywords):
        return "team_page"
    if path in {"", "/"}:
        return "firm_homepage"
    return "unknown"


def classify_email_quality(
    value: Any,
    *,
    normalized_domain: str = "",
    free_email_domains: set[str] | None = None,
    vendor_email_domains: set[str] | None = None,
    generic_prefixes: set[str] | None = None,
    asset_extensions: set[str] | None = None,
) -> dict[str, Any]:
    free_email_domains = free_email_domains or set(DEFAULT_FREE_EMAIL_DOMAINS)
    vendor_email_domains = vendor_email_domains or set(DEFAULT_VENDOR_EMAIL_DOMAINS)
    generic_prefixes = generic_prefixes or set(DEFAULT_GENERIC_EMAIL_PREFIXES)
    asset_extensions = asset_extensions or set(DEFAULT_ASSET_EXTENSIONS)

    email = normalize_email(value)
    email_domain = extract_email_domain(email)
    email_domain_root = _root_domain(email_domain) if email_domain else ""

    result = {
        "email": email,
        "email_domain": email_domain,
        "email_quality": "empty",
        "email_is_junk": False,
        "email_is_free_provider": False,
    }

    if not email:
        return result

    if not EMAIL_PATTERN.match(email) or _looks_like_asset_email(email, asset_extensions=asset_extensions):
        result["email_quality"] = "junk_asset"
        result["email_is_junk"] = True
        return result

    if email_domain_root in vendor_email_domains or email_domain in vendor_email_domains:
        result["email_quality"] = "vendor_platform"
        result["email_is_junk"] = True
        return result
    if "sentry" in email_domain:
        result["email_quality"] = "vendor_platform"
        result["email_is_junk"] = True
        return result

    if email_domain_root in free_email_domains or email_domain in free_email_domains:
        result["email_quality"] = "free_provider"
        result["email_is_free_provider"] = True
        return result

    local_part = email.split("@", 1)[0]
    local_base = local_part.split("+", 1)[0].strip().lower()
    generic_local = local_base in generic_prefixes

    if normalized_domain and email_domain_root == normalized_domain:
        result["email_quality"] = (
            "generic_firm_email" if generic_local else "valid_firm_email"
        )
        return result

    if generic_local:
        result["email_quality"] = "generic_usable_email"
        return result

    result["email_quality"] = "other"
    return result


def classify_phone_quality(value: Any) -> dict[str, Any]:
    digits = phone_digits(value)
    result = {
        "phone_digits": digits,
        "phone_quality": "empty",
        "phone_is_placeholder": False,
    }

    if not digits:
        return result

    normalized = digits[1:] if len(digits) == 11 and digits.startswith("1") else digits
    if len(normalized) != 10:
        result["phone_quality"] = "suspicious"
        return result

    if _is_placeholder_phone(normalized):
        result["phone_quality"] = "placeholder"
        result["phone_is_placeholder"] = True
        result["phone_digits"] = normalized
        return result

    if not is_phone_acceptable(normalized):
        result["phone_quality"] = "suspicious"
        result["phone_digits"] = normalized
        return result

    result["phone_quality"] = "valid"
    result["phone_digits"] = normalized
    return result


def build_lead_score(
    row: dict[str, Any],
    *,
    weights: dict[str, Any] | None = None,
    normalized_domain_column: str = "normalized_domain",
    page_type_column: str = "page_type",
    email_quality_column: str = "email_quality",
    phone_quality_column: str = "phone_quality",
    contact_name_column: str = "contact_name",
    firm_name_column: str = "firm_name",
) -> tuple[int, list[str]]:
    merged_weights = _merge_score_weights(weights)
    total = 0
    reasons: list[str] = []

    page_type = str(row.get(page_type_column) or "unknown")
    page_score = int(merged_weights["page_type"].get(page_type, 0))
    if page_score:
        total += page_score
        reasons.append(f"{page_type}:{page_score:+d}")

    email_quality = str(row.get(email_quality_column) or "empty")
    email_score = int(merged_weights["email_quality"].get(email_quality, 0))
    if email_score:
        total += email_score
        reasons.append(f"{email_quality}:{email_score:+d}")

    phone_quality = str(row.get(phone_quality_column) or "empty")
    phone_score = int(merged_weights["phone_quality"].get(phone_quality, 0))
    if phone_score:
        total += phone_score
        reasons.append(f"{phone_quality}:{phone_score:+d}")

    if _has_value(row.get(contact_name_column)):
        contact_score = int(merged_weights["contact_name_present"])
        total += contact_score
        reasons.append(f"contact_name:{contact_score:+d}")

    if _has_value(row.get(firm_name_column)):
        firm_score = int(merged_weights["firm_name_present"])
        total += firm_score
        reasons.append(f"firm_name:{firm_score:+d}")

    if not _has_value(row.get(normalized_domain_column)):
        missing_domain_score = int(merged_weights["missing_normalized_domain"])
        total += missing_domain_score
        reasons.append(f"missing_domain:{missing_domain_score:+d}")

    return total, reasons


def row_completeness(
    row: dict[str, Any],
    *,
    excluded_fields: set[str] | None = None,
) -> int:
    excluded_fields = excluded_fields or set()
    return sum(
        1
        for key, value in row.items()
        if key not in excluded_fields and _has_value(value)
    )


def phone_digits(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return NON_DIGIT_PATTERN.sub("", value)


def normalize_email(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def extract_email_domain(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.rsplit("@", 1)[1].strip().lower()


def is_firm_domain_email_quality(value: Any) -> bool:
    return str(value or "") in {"valid_firm_email", "generic_firm_email"}


def is_valid_phone_quality(value: Any) -> bool:
    return str(value or "") == "valid"


def _merge_score_weights(weights: dict[str, Any] | None) -> dict[str, Any]:
    merged = {
        "page_type": dict(DEFAULT_SCORE_WEIGHTS["page_type"]),
        "email_quality": dict(DEFAULT_SCORE_WEIGHTS["email_quality"]),
        "phone_quality": dict(DEFAULT_SCORE_WEIGHTS["phone_quality"]),
        "contact_name_present": DEFAULT_SCORE_WEIGHTS["contact_name_present"],
        "firm_name_present": DEFAULT_SCORE_WEIGHTS["firm_name_present"],
        "missing_normalized_domain": DEFAULT_SCORE_WEIGHTS["missing_normalized_domain"],
    }
    if not isinstance(weights, dict):
        return merged

    for key, value in weights.items():
        if key in {"page_type", "email_quality", "phone_quality"} and isinstance(value, dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


def _safe_urlsplit(value: Any):
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    try:
        return urlsplit(candidate)
    except ValueError:
        return None


def _looks_like_hostname(value: str) -> bool:
    if not value or " " in value:
        return False
    if "." not in value:
        return False
    return bool(re.fullmatch(r"[a-z0-9.-]+", value))


def _root_domain(hostname: str) -> str:
    labels = [label for label in hostname.split(".") if label]
    if len(labels) <= 2:
        return ".".join(labels)

    if (
        labels[-1] in COMMON_COUNTRY_SUFFIXES
        and labels[-2] in COMMON_SECOND_LEVEL_SUFFIXES
        and len(labels) >= 3
    ):
        return ".".join(labels[-3:])

    return ".".join(labels[-2:])


def _looks_like_asset_email(email: str, *, asset_extensions: set[str]) -> bool:
    lowered = email.strip().lower()
    if "/" in lowered or "\\" in lowered:
        return True
    for extension in asset_extensions:
        if lowered.endswith(f".{extension}"):
            return True
    asset_keywords = ("banner", "cover", "cta", "hero", "icon", "logo", "sprite", "transparent")
    return any(keyword in lowered for keyword in asset_keywords) and any(
        lowered.endswith(f".{extension}") for extension in asset_extensions
    )


def _is_placeholder_phone(value: str) -> bool:
    counts = Counter(value)
    return len(counts) == 1 or max(counts.values()) >= 9


def is_phone_acceptable(digits_10: str) -> bool:
    """
    Return False for junk 10-digit numbers: placeholders, invalid area codes,
    timestamp-like values. Use after normalizing to 10 digits (strip leading 1).
    """
    if not digits_10 or len(digits_10) != 10 or not digits_10.isdigit():
        return False
    if _is_placeholder_phone(digits_10):
        return False
    if digits_10 in ("0000000000", "9999999999"):
        return False
    area = digits_10[:3]
    if area[0] in ("0", "1"):
        return False
    if area in ("000", "666", "976"):
        return False
    if area >= "950" and area <= "959":
        return False
    try:
        n = int(digits_10)
        if 1_700_000_000 <= n <= 2_100_000_000:
            return False
    except ValueError:
        pass
    return True


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, pd.Series):
        non_empty = value.notna() & (value.astype(str).str.strip() != "")
        return bool(non_empty.any())
    try:
        if value != value:
            return False
    except (ValueError, TypeError):
        pass
    if isinstance(value, str):
        return bool(value.strip())
    return True
