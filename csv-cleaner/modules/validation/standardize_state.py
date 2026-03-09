"""
Standardize US state names and abbreviations.
"""

import pandas as pd

from cleaner.report import CleaningReport

# US states + DC: abbreviation -> full name (title case). Case-insensitive lookup via normalized keys.
_US_STATES = {
    "al": "Alabama",
    "ak": "Alaska",
    "az": "Arizona",
    "ar": "Arkansas",
    "ca": "California",
    "co": "Colorado",
    "ct": "Connecticut",
    "de": "Delaware",
    "dc": "District of Columbia",
    "fl": "Florida",
    "ga": "Georgia",
    "hi": "Hawaii",
    "id": "Idaho",
    "il": "Illinois",
    "in": "Indiana",
    "ia": "Iowa",
    "ks": "Kansas",
    "ky": "Kentucky",
    "la": "Louisiana",
    "me": "Maine",
    "md": "Maryland",
    "ma": "Massachusetts",
    "mi": "Michigan",
    "mn": "Minnesota",
    "ms": "Mississippi",
    "mo": "Missouri",
    "mt": "Montana",
    "ne": "Nebraska",
    "nv": "Nevada",
    "nh": "New Hampshire",
    "nj": "New Jersey",
    "nm": "New Mexico",
    "ny": "New York",
    "nc": "North Carolina",
    "nd": "North Dakota",
    "oh": "Ohio",
    "ok": "Oklahoma",
    "or": "Oregon",
    "pa": "Pennsylvania",
    "ri": "Rhode Island",
    "sc": "South Carolina",
    "sd": "South Dakota",
    "tn": "Tennessee",
    "tx": "Texas",
    "ut": "Utah",
    "vt": "Vermont",
    "va": "Virginia",
    "wa": "Washington",
    "wv": "West Virginia",
    "wi": "Wisconsin",
    "wy": "Wyoming",
}

# Full name (normalized: lower, single spaces) -> abbreviation
_ABBR_BY_NAME = {v.lower().replace("-", " "): k for k, v in _US_STATES.items()}


def _normalize_state(raw: str, output: str) -> str | None:
    """
    Return abbr (2-letter) or full name, or None if unmapped (caller keeps original).
    Case-insensitive match.
    """
    s = str(raw).strip()
    if not s:
        return None
    key_lower = s.lower().replace("-", " ")
    # Try as abbreviation (e.g. "al", "dc")
    if len(key_lower) <= 3 and key_lower in _US_STATES:
        return _US_STATES[key_lower] if output == "name" else key_lower.upper()
    if key_lower in _ABBR_BY_NAME:
        abbr = _ABBR_BY_NAME[key_lower].upper()
        return _US_STATES[_ABBR_BY_NAME[key_lower]] if output == "name" else abbr
    return None


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Standardize US state names/abbreviations. config["options"] may contain:
    - columns: list of column names (required).
    - output: "abbr" | "name" (default: "abbr"). Output 2-letter code or full name.
    Operates on non-null values only. Unmapped values left unchanged.
    """
    options = config.get("options", {})
    columns = options.get("columns") or []
    output = (options.get("output") or "abbr").lower()

    if output not in ("abbr", "name"):
        output = "abbr"

    cols = [c for c in columns if c in df.columns]
    if not cols:
        report.record_module(
            config["module_id"],
            {"columns_processed": 0, "values_changed": 0, "values_unmapped": 0},
        )
        return df

    total_changed = 0
    total_unmapped = 0
    df = df.copy()

    for col in cols:
        non_null = df[col].notna()
        if not non_null.any():
            continue

        raw = df.loc[non_null, col].astype(str)
        result = raw.apply(lambda v: _normalize_state(v, output))
        unmapped = result.isna()
        total_unmapped += unmapped.sum()
        # Compare only where result is mapped; avoid .str on all-NaN series
        compare_to = result.fillna("").str.lower()
        changed = (~unmapped) & (raw.str.strip().str.lower() != compare_to)
        total_changed += changed.sum()
        df.loc[non_null, col] = raw.where(unmapped, result)

    report.record_module(
        config["module_id"],
        {
            "columns_processed": len(cols),
            "values_changed": int(total_changed),
            "values_unmapped": int(total_unmapped),
        },
    )
    return df
