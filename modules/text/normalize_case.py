"""
Normalize string case (lower, upper, or title) in specified columns.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Apply case normalization. config["options"] may contain:
    - columns: list of column names (default: all string/object columns).
    - case: "lower" | "upper" | "title" (default: "lower").
    """
    options = config.get("options", {})
    columns = options.get("columns")
    case = (options.get("case") or "lower").lower()

    if case not in ("lower", "upper", "title"):
        case = "lower"

    if columns is not None:
        cols = [c for c in columns if c in df.columns]
    else:
        cols = [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]

    if not cols:
        report.record_module(config["module_id"], {"columns_normalized": 0})
        return df

    if case == "lower":
        df = df.copy()
        for col in cols:
            df[col] = df[col].astype(str).str.lower()
    elif case == "upper":
        df = df.copy()
        for col in cols:
            df[col] = df[col].astype(str).str.upper()
    else:
        df = df.copy()
        for col in cols:
            df[col] = df[col].astype(str).str.title()

    report.record_module(
        config["module_id"],
        {"columns_normalized": len(cols), "case": case},
    )
    return df
