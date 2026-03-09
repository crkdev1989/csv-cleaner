"""
Standardize US ZIP codes (ZIP5 or ZIP+4).
"""

import re

import pandas as pd

from cleaner.report import CleaningReport


def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s)


def _normalize_zip(raw: str, output: str) -> str | None:
    """
    Return zip5 (12345) or zip9 (12345-6789), or None if invalid.
    Preserve leading zeros by keeping as string. Invalid: wrong digit count.
    """
    digits = _digits_only(raw)
    if len(digits) == 5:
        return digits
    if len(digits) == 9:
        return f"{digits[:5]}-{digits[5:]}" if output == "zip9" else digits[:5]
    return None


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Standardize US ZIP codes. config["options"] may contain:
    - columns: list of column names (required).
    - output: "zip5" | "zip9" (default: "zip5"). zip9 outputs 12345-6789 when 9 digits.
    Operates on non-null values only. Leading zeros preserved. Invalid values left unchanged.
    """
    options = config.get("options", {})
    columns = options.get("columns") or []
    output = (options.get("output") or "zip5").lower()

    if output not in ("zip5", "zip9"):
        output = "zip5"

    cols = [c for c in columns if c in df.columns]
    if not cols:
        report.record_module(
            config["module_id"],
            {"columns_processed": 0, "values_changed": 0, "values_invalid": 0},
        )
        return df

    total_changed = 0
    total_invalid = 0
    df = df.copy()

    for col in cols:
        non_null = df[col].notna()
        if not non_null.any():
            continue

        raw = df.loc[non_null, col].astype(str).str.strip()
        result = raw.apply(lambda v: _normalize_zip(v, output))
        invalid = result.isna()
        total_invalid += invalid.sum()
        changed = (~invalid) & (raw != result)
        total_changed += changed.sum()
        df.loc[non_null, col] = raw.where(invalid, result)

    report.record_module(
        config["module_id"],
        {
            "columns_processed": len(cols),
            "values_changed": int(total_changed),
            "values_invalid": int(total_invalid),
        },
    )
    return df
