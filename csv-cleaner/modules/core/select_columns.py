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
    raw_wanted = options.get("columns")
    if raw_wanted is None:
        wanted = []
    elif isinstance(raw_wanted, (list, tuple)):
        wanted = list(raw_wanted)
    elif hasattr(raw_wanted, "__iter__") and not isinstance(raw_wanted, str):
        wanted = list(raw_wanted)
    else:
        wanted = [raw_wanted]
    strict = options.get("strict", False)

    if len(wanted) == 0:
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
