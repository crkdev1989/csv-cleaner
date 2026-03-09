"""
Coerce string or mixed columns to numeric type (int/float).
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Convert target columns to numeric. config["options"] may contain:
    - columns: list of column names to coerce (required).
    - errors: "coerce" | "raise" | "ignore" (default: "coerce"). Coerce invalid to NaN.
      With "raise", the first invalid value in any processed column aborts the entire run.
    - downcast: optional, e.g. "integer" | "float" | "signed" | "unsigned".
    - strip_commas: remove commas before parsing (default: True).
    - strip_currency: remove leading $, £, etc. (default: False).
    Nulls are preserved (not converted to the string "nan").
    """
    options = config.get("options", {})
    columns = options.get("columns") or []
    errors = options.get("errors", "coerce")
    downcast = options.get("downcast")
    strip_commas = options.get("strip_commas", True)
    strip_currency = options.get("strip_currency", False)

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

    for col in cols:
        non_null = df[col].notna()
        if not non_null.any():
            continue

        raw = df.loc[non_null, col].astype(str)
        if strip_commas:
            raw = raw.str.replace(",", "", regex=False)
        if strip_currency:
            raw = raw.str.replace(r"^\s*[\$£€]|\s*[\$£€]\s*$", "", regex=True)
            raw = raw.str.strip()

        result = pd.to_numeric(raw, errors=errors, downcast=downcast)
        # Reindex to full index so column dtype can become numeric; missing -> NaN
        df[col] = result.reindex(df.index)

        still_numeric = result.notna()
        total_converted += still_numeric.sum()
        total_failed += (~still_numeric).sum()

    report.record_module(
        config["module_id"],
        {
            "columns_processed": len(cols),
            "values_converted": int(total_converted),
            "values_failed": int(total_failed),
        },
    )
    return df
