"""
Coerce string or mixed columns to datetime.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Convert target columns to datetime. config["options"] may contain:
    - columns: list of column names to coerce (required).
    - errors: "coerce" | "raise" | "ignore" (default: "coerce"). Coerce invalid to NaT.
    - format: optional str, e.g. "%Y-%m-%d" for faster parsing.
    - dayfirst: bool (default: False). Day before month in ambiguous dates.
    - utc: bool (default: False). Convert to UTC.
    Nulls are preserved (not converted to the string "nan").
    """
    options = config.get("options", {})
    columns = options.get("columns") or []
    errors = options.get("errors", "coerce")
    format_ = options.get("format")
    dayfirst = options.get("dayfirst", False)
    utc = options.get("utc", False)

    cols = [c for c in columns if c in df.columns]
    if not cols:
        report.record_module(
            config["module_id"],
            {
                "columns_processed": 0,
                "values_converted": 0,
                "values_failed": 0,
            },
        )
        return df

    total_converted = 0
    total_failed = 0
    df = df.copy()

    kwargs = {"errors": errors, "dayfirst": dayfirst}
    if format_ is not None:
        kwargs["format"] = format_
    if utc:
        kwargs["utc"] = True

    for col in cols:
        non_null = df[col].notna()
        if not non_null.any():
            continue

        raw = df.loc[non_null, col].astype(str)
        result = pd.to_datetime(raw, **kwargs)
        # Reindex to full index so column dtype can become datetime; missing -> NaT
        df[col] = result.reindex(df.index)

        still_valid = result.notna()
        total_converted += still_valid.sum()
        total_failed += (~still_valid).sum()

    report.record_module(
        config["module_id"],
        {
            "columns_processed": len(cols),
            "values_converted": int(total_converted),
            "values_failed": int(total_failed),
        },
    )
    return df
