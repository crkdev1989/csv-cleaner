"""
Drop rows where website/page content indicates a parked or for-sale domain.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from cleaner.report import CleaningReport

DEFAULT_PARKING_PATTERNS = [
    "for sale",
    "hugedomains",
    "domain for sale",
    "parked",
    "buy this domain",
    "this domain is for sale",
    "is for sale",
]


def run(
    df: pd.DataFrame,
    config: dict[str, Any],
    report: CleaningReport,
) -> pd.DataFrame:
    options = config.get("options", {})
    raw_cols = options.get("columns")
    if raw_cols is None:
        columns = [
            "page_title",
            "source_urls",
            "website",
            "contact_page_url",
        ]
    elif isinstance(raw_cols, (list, tuple)):
        columns = list(raw_cols)
    elif hasattr(raw_cols, "__iter__") and not isinstance(raw_cols, str):
        columns = list(raw_cols)
    else:
        columns = [raw_cols]
    raw_pat = options.get("patterns")
    patterns = list(raw_pat) if raw_pat is not None and hasattr(raw_pat, "__iter__") and not isinstance(raw_pat, str) else (DEFAULT_PARKING_PATTERNS if raw_pat is None else [raw_pat])

    cols = [c for c in columns if c in df.columns]
    if len(cols) == 0:
        report.record_module(
            config["module_id"],
            {"rows_dropped": 0, "reason": "no_columns_matched"},
        )
        return df

    patterns_lower = [p.lower() for p in patterns if p]

    def _is_parked(row: pd.Series) -> bool:
        text_parts = []
        for c in cols:
            v = row.get(c)
            if pd.isna(v):
                continue
            text_parts.append(str(v).strip())
        combined = " ".join(text_parts).lower()
        return any(p in combined for p in patterns_lower)

    mask = df.apply(_is_parked, axis=1)
    rows_before = len(df)
    df_out = df.loc[~mask].copy()
    dropped = rows_before - len(df_out)

    report.rows_dropped += dropped
    report.record_module(
        config["module_id"],
        {
            "rows_dropped": dropped,
            "columns_checked": cols,
            "patterns_count": len(patterns_lower),
        },
    )
    return df_out
