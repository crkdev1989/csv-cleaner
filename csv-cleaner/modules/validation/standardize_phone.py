"""
Standardize US phone numbers to a consistent format.
"""

import re

import pandas as pd

from cleaner.report import CleaningReport

# US: 10 digits, or 11 with leading 1
US_PHONE_DIGITS = 10
US_COUNTRY_CODE = "1"


def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s)


def _format_us_phone(digits: str, keep_country_code: bool) -> str:
    """Format 10 digits as (XXX) XXX-XXXX; optional leading +1."""
    if keep_country_code:
        return f"+{US_COUNTRY_CODE} ({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


def _normalize_phone(raw: str, output: str, keep_country_code: bool) -> str | None:
    """
    Return formatted phone or None if invalid. output: digits | us | e164.
    Conservative: invalid length -> return None (caller keeps original).
    """
    digits = _digits_only(raw)
    # Accept 10 digits or 11 with leading 1
    if len(digits) == 11 and digits.startswith(US_COUNTRY_CODE):
        digits = digits[1:]
    if len(digits) != US_PHONE_DIGITS:
        return None

    if output == "digits":
        return digits
    if output == "e164":
        return f"+{US_COUNTRY_CODE}{digits}" if keep_country_code else digits
    if output == "us":
        return _format_us_phone(digits, keep_country_code)
    return digits


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Standardize US phone numbers. config["options"] may contain:
    - columns: list of column names (required).
    - output: "digits" | "us" | "e164" (default: "us").
    - keep_country_code: include +1 for US (default: True); applies to us/e164.
    Operates on non-null values only. Invalid length leaves value unchanged.
    """
    options = config.get("options", {})
    raw_cols = options.get("columns")
    columns = list(raw_cols) if raw_cols is not None and hasattr(raw_cols, "__iter__") and not isinstance(raw_cols, str) else ([] if raw_cols is None else [raw_cols])
    out_raw = options.get("output")
    if out_raw is None:
        output = "us"
    elif hasattr(out_raw, "iloc"):
        output = str(out_raw.iloc[0]).lower() if len(out_raw) > 0 else "us"
    else:
        output = str(out_raw).lower()
    raw_keep = options.get("keep_country_code")
    if isinstance(raw_keep, bool):
        keep_country_code = raw_keep
    elif raw_keep is None:
        keep_country_code = True
    elif hasattr(raw_keep, "iloc"):
        keep_country_code = bool(raw_keep.iloc[0]) if len(raw_keep) > 0 else True
    else:
        keep_country_code = bool(raw_keep)

    if output not in ("digits", "us", "e164"):
        output = "us"

    cols = [c for c in columns if c in df.columns]
    if len(cols) == 0:
        report.record_module(
            config["module_id"],
            {"columns_processed": 0, "values_changed": 0, "values_invalid": 0},
        )
        return df

    total_changed = 0
    total_invalid = 0
    df = df.copy()

    for col in cols:
        # Resolve to single column by index so duplicate column names give one Series, not DataFrame
        col_idxs = [i for i, c in enumerate(df.columns) if c == col]
        if len(col_idxs) == 0:
            continue
        col_idx = col_idxs[0]
        ser = df.iloc[:, col_idx]
        non_null = ser.notna()
        if not non_null.any():
            continue

        raw = ser.loc[non_null].astype(str)
        result = raw.apply(
            lambda v: _normalize_phone(v, output, keep_country_code)
        )
        # Invalid: normalized to None; leave original
        invalid = result.isna()
        total_invalid += int(invalid.sum())
        changed = (~invalid) & (raw != result)
        total_changed += int(changed.sum())
        df.iloc[non_null, col_idx] = raw.where(invalid, result).values

    report.record_module(
        config["module_id"],
        {
            "columns_processed": len(cols),
            "values_changed": int(total_changed),
            "values_invalid": int(total_invalid),
        },
    )
    return df
