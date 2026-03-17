"""
Blank known junk values in website, email, and phone so downstream drop logic can remove rows.
Low-risk: only sets values to NA; does not drop rows.

Milestone 4: Email validation is conservative and lead-preserving. We normalize (trim, lowercase),
then blank only clearly bad emails (placeholder domains, noreply, malformed, asset-like).
Generic role addresses (info@, intake@, contact@, office@, help@, admin@) are never rejected.
"""

import logging
import re

import pandas as pd

from cleaner.lead_cleanup import is_email_clearly_junk, normalize_email_for_validation
from cleaner.report import CleaningReport

logger = logging.getLogger(__name__)


# Substrings that indicate a junk website (e.g. map links, retail)
JUNK_WEBSITE_SUBSTRINGS = ("google.com/maps", "amazon.com")

# Placeholder phone patterns (digits only compared)
JUNK_PHONE_NORMALIZED = {"0000000000", "8005556666", "5555555555", "00000000000"}

# US NANP: area code = first 3 digits. Valid range 200-989 with gaps (555=test, 666=unassigned).
# Reject area codes < 200 or > 989 or in blocklist.
INVALID_US_AREA_CODES = {0, 555, 666}
US_AREA_CODE_MIN = 200
US_AREA_CODE_MAX = 989


def _normalize_phone_digits(s) -> str:
    """Extract digits from a phone string for comparison. Accepts scalar or Series (uses first element)."""
    if isinstance(s, pd.Series):
        s = s.iloc[0] if len(s) > 0 else pd.NA
    if pd.isna(s) or not isinstance(s, str):
        return ""
    return re.sub(r"\D", "", str(s).strip())


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
    def _col_name(val, default: str) -> str:
        while isinstance(val, pd.Series):
            val = val.iloc[0] if len(val) > 0 else None
        if val is None:
            return default
        if isinstance(val, float) and pd.isna(val):
            return default
        s = str(val).strip()
        return s if s else default
    website_col = _col_name(options.get("website_column"), "website")
    email_col = _col_name(options.get("email_column"), "email")
    phone_col = _col_name(options.get("phone_column"), "phone")
    df = df.copy()
    stats = {
        "website_blanked": 0,
        "email_blanked": 0,
        "phone_blanked": 0,
        "emails_inspected": 0,
        "emails_kept": 0,
        "emails_rejected_malformed": 0,
        "emails_rejected_placeholder_junk": 0,
        "emails_normalized": 0,
    }

    def _ensure_str_col(c):
        """Ensure column selector is a single string for loc (avoid Series)."""
        while isinstance(c, pd.Series):
            c = c.iloc[0] if len(c) > 0 else None
        if c is None or (isinstance(c, float) and pd.isna(c)):
            return None
        return str(c).strip() or None

    if website_col in df.columns:
        wcol = _ensure_str_col(website_col)
        if wcol:
            for sub in JUNK_WEBSITE_SUBSTRINGS:
                mask = df[wcol].astype(str).str.contains(sub, na=False, regex=False)
                stats["website_blanked"] += int(mask.sum())
                df.loc[mask, wcol] = pd.NA

    if email_col in df.columns:
        ecol = _ensure_str_col(email_col)
        if ecol:
            inspected = 0
            kept = 0
            rejected_malformed = 0
            rejected_junk = 0
            normalized_count = 0
            blank_mask = pd.Series(False, index=df.index)
            for idx in df.index:
                val = df.at[idx, ecol]
                if isinstance(val, pd.Series):
                    val = val.iloc[0] if len(val) > 0 else pd.NA
                if pd.isna(val) or not isinstance(val, str):
                    continue
                raw = str(val).strip()
                if not raw or "@" not in raw:
                    continue
                inspected += 1
                normalized = normalize_email_for_validation(val)
                if normalized != raw:
                    normalized_count += 1
                is_junk, reason = is_email_clearly_junk(normalized)
                if is_junk:
                    blank_mask.at[idx] = True
                    if reason == "malformed":
                        rejected_malformed += 1
                    else:
                        rejected_junk += 1
                else:
                    kept += 1
                    if normalized != raw:
                        df.at[idx, ecol] = normalized
            stats["emails_inspected"] = inspected
            stats["emails_kept"] = kept
            stats["emails_rejected_malformed"] = rejected_malformed
            stats["emails_rejected_placeholder_junk"] = rejected_junk
            stats["emails_normalized"] = normalized_count
            stats["email_blanked"] = int(blank_mask.sum())
            df.loc[blank_mask, ecol] = pd.NA
            if inspected > 0:
                logger.info(
                    "email_validation: inspected=%d kept=%d rejected_malformed=%d rejected_placeholder_junk=%d normalized=%d",
                    inspected, kept, rejected_malformed, rejected_junk, normalized_count,
                )

    if phone_col in df.columns:
        pcol = _ensure_str_col(phone_col)
        if pcol:
            # Use first matching column by index so we always have a single Series (avoid duplicate columns -> DataFrame)
            col_idxs = [i for i, c in enumerate(df.columns) if c == pcol]
            if col_idxs:
                col_idx = col_idxs[0]
                def is_junk_phone(val):
                    if isinstance(val, pd.Series):
                        val = val.iloc[0] if len(val) > 0 else pd.NA
                    if pd.isna(val):
                        digits = ""
                    else:
                        digits = _normalize_phone_digits(str(val))
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
                ser = df.iloc[:, col_idx]
                mask = ser.apply(is_junk_phone)
                if isinstance(mask, pd.DataFrame):
                    mask = mask.any(axis=1)
                stats["phone_blanked"] = int(mask.sum())
                df.iloc[mask, col_idx] = pd.NA

    report.record_module(config["module_id"], stats)
    return df
