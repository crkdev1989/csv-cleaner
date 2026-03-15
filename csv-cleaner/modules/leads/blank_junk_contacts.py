"""
Blank known junk values in website, email, and phone so downstream drop logic can remove rows.
Low-risk: only sets values to NA; does not drop rows.
"""

import re

import pandas as pd

from cleaner.report import CleaningReport


# Substrings that indicate a junk website (e.g. map links, retail)
JUNK_WEBSITE_SUBSTRINGS = ("google.com/maps", "amazon.com")

# Placeholder or fake email patterns (case-insensitive)
JUNK_EMAIL_PATTERNS = (
    r"johndoe@email\.com",
    r"flags@2x\.png",
    r"@example\.com",
    r"@email\.com",
    r"noreply@",
    r"no-reply@",
    r"donotreply@",
)
JUNK_EMAIL_RE = re.compile("|".join(f"({p})" for p in JUNK_EMAIL_PATTERNS), re.I)

# Placeholder phone patterns (digits only compared)
JUNK_PHONE_NORMALIZED = {"0000000000", "8005556666", "5555555555", "00000000000"}

# US NANP: area code = first 3 digits. Valid range 200-989 with gaps (555=test, 666=unassigned).
# Reject area codes < 200 or > 989 or in blocklist.
INVALID_US_AREA_CODES = {0, 555, 666}
US_AREA_CODE_MIN = 200
US_AREA_CODE_MAX = 989


def _normalize_phone_digits(s: str) -> str:
    """Extract digits from a phone string for comparison."""
    if pd.isna(s) or not isinstance(s, str):
        return ""
    return re.sub(r"\D", "", s.strip())


def _is_invalid_us_area_code(digits: str) -> bool:
    """True if the number has an invalid or junk US area code (first 3 digits)."""
    if len(digits) < 3:
        return True
    try:
        area = int(digits[:3])
    except ValueError:
        return True
    if area < US_AREA_CODE_MIN or area > US_AREA_CODE_MAX:
        return True
    if area in INVALID_US_AREA_CODES:
        return True
    # 800-555-xxxx is test
    if len(digits) >= 6 and digits[:3] == "800" and digits[3:6] == "555":
        return True
    return False


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Set website/email/phone to NA when they match known junk.
    config["options"] may contain:
    - website_column, email_column, phone_column: column names (defaults "website", "email", "phone").
    """
    options = config.get("options", {})
    website_col = options.get("website_column", "website")
    email_col = options.get("email_column", "email")
    phone_col = options.get("phone_column", "phone")
    df = df.copy()
    stats = {"website_blanked": 0, "email_blanked": 0, "phone_blanked": 0}

    if website_col in df.columns:
        for sub in JUNK_WEBSITE_SUBSTRINGS:
            mask = df[website_col].astype(str).str.contains(sub, na=False, regex=False)
            stats["website_blanked"] += int(mask.sum())
            df.loc[mask, website_col] = pd.NA

    if email_col in df.columns:
        def is_junk_email(val):
            if pd.isna(val) or not isinstance(val, str) or "@" not in val:
                return False
            return bool(JUNK_EMAIL_RE.search(val.strip()))
        mask = df[email_col].apply(is_junk_email)
        stats["email_blanked"] = int(mask.sum())
        df.loc[mask, email_col] = pd.NA

    if phone_col in df.columns:
        def is_junk_phone(val):
            digits = _normalize_phone_digits(str(val) if not pd.isna(val) else "")
            if not digits:
                return False
            # Normalize to 10 digits for US (strip leading 1)
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            if digits in JUNK_PHONE_NORMALIZED:
                return True
            if len(digits) >= 3 and _is_invalid_us_area_code(digits):
                return True
            return False
        mask = df[phone_col].apply(is_junk_phone)
        stats["phone_blanked"] = int(mask.sum())
        df.loc[mask, phone_col] = pd.NA

    report.record_module(config["module_id"], stats)
    return df
