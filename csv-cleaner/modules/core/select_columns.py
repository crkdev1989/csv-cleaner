"""
Keep only specified columns in the given order; drop the rest.
"""

import pandas as pd

from cleaner.report import CleaningReport


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Select columns and optionally reorder. config["options"] may contain:
    - columns: list of column names to keep (order preserved). Missing columns
      are skipped unless strict is True (then KeyError).
    - strict: if True, raise if any requested column is missing (default: False).
    """
    options = config.get("options", {})
    wanted = options.get("columns") or []
    strict = options.get("strict", False)

    if not wanted:
        report.record_module(config["module_id"], {"columns_selected": 0})
        return df

    if strict:
        missing = [c for c in wanted if c not in df.columns]
        if missing:
            raise KeyError(f"select_columns (strict): missing columns {missing}")
        cols = wanted
    else:
        cols = [c for c in wanted if c in df.columns]

    dropped_count = len(df.columns) - len(cols)
    df = df[cols].copy()
    report.record_module(
        config["module_id"],
        {
            "columns_selected": len(cols),
            "columns_dropped": dropped_count,
            "selected": cols,
        },
    )
    return df
