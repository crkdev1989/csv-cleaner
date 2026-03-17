"""
Emit preserved master leads to a separate file before strict drop/dedupe.
Master output: does not require email; uses identity-key dedupe (no domain-only collapse).
Prefers profile_url / input_url as strong identifiers; falls back to firm+city+state+website+name.
Includes has_email, has_contact_page, has_phone flags.
Returns df unchanged so strict pipeline continues.
"""

from pathlib import Path

import pandas as pd

from cleaner.report import CleaningReport
from cleaner.writers import write_data


MASTER_COLUMNS_ORDER = [
    "name",
    "law_firm",
    "website",
    "profile_url",
    "source_directory",
    "input_url",
    "contact_page_url",
    "email",
    "phone",
    "city",
    "state",
    "domain",
    "address",
    "zip",
    "practice_area",
    "scrape_date",
    "notes",
    "has_email",
    "has_contact_page",
    "has_phone",
]

# Delimiter for composite dedupe key (must not appear in normal data).
_KEY_SEP = "\t"


def _str(val) -> str:
    """Normalize value to string for key; empty/null -> ''. Never use val in boolean context (may be Series)."""
    if val is None:
        return ""
    if isinstance(val, pd.Series):
        val = val.iloc[0] if len(val) > 0 else None
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
    elif isinstance(val, float) and pd.isna(val):
        return ""
    s = str(val).strip()
    return s if s else ""


def _master_dedupe_key(row: pd.Series, profile_col: str, input_url_col: str, detail_url_col: str) -> str:
    """
    Single identity key for master dedupe. Prefer strong exact identifiers; else composite.
    - If profile_url (or detail_url) present: use it (one row per profile).
    - Else if input_url present: use it.
    - Else: firm|city|state|website|name so same firm+location+website+name = duplicate.
    Rows sharing only domain get different keys when profile/input_url or name differs.
    """
    profile = _str(row.get(profile_col)) if profile_col in row.index else ""
    input_url = _str(row.get(input_url_col)) if input_url_col in row.index else ""
    detail_url = _str(row.get(detail_url_col)) if detail_url_col in row.index else ""
    if profile:
        return profile
    if detail_url:
        return detail_url
    if input_url:
        return input_url
    law_firm = _str(row.get("law_firm")) if "law_firm" in row.index else ""
    city = _str(row.get("city")) if "city" in row.index else ""
    state = _str(row.get("state")) if "state" in row.index else ""
    website = _str(row.get("website")) if "website" in row.index else ""
    name = _str(row.get("name")) if "name" in row.index else ""
    return _KEY_SEP.join([law_firm, city, state, website, name])


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Write master leads CSV and set report.master_leads_count/path. Does not modify df.
    Master dedupe: identity key from profile_url > detail_url > input_url > composite
    (law_firm, city, state, website, name). Best-row selection by has_email, has_contact_page,
    has_phone before dropping duplicates. config["options"] may contain:
    - profile_url_column, input_url_column, detail_url_column (defaults: profile_url, input_url, detail_url).
    - dedupe_key_mode: "none", "exact" (default), "identity", or "legacy". none = no dedupe; exact = drop only identical rows; identity = one per profile/input_url/composite; legacy = subset-based dedupe.
    """
    full_config = config.get("config", {})
    output_master = full_config.get("output_master")
    if not output_master or not output_master.get("path"):
        report.record_module(config["module_id"], {"skipped": True, "reason": "no output_master.path"})
        return df

    # Independent snapshot: deep copy so later pipeline steps cannot mutate it.
    master_df = df.copy(deep=True)
    rows_before_dedupe = len(master_df)

    options = config.get("options", {})
    profile_col = str(options.get("profile_url_column") or "profile_url")
    input_url_col = str(options.get("input_url_column") or "input_url")
    detail_url_col = str(options.get("detail_url_column") or "detail_url")
    dedupe_key_mode = str(options.get("dedupe_key_mode") or "exact").strip().lower()

    if dedupe_key_mode == "none":
        # No dedupe: keep every row in the master snapshot.
        pass
    elif dedupe_key_mode == "exact":
        # Only drop rows that are exact duplicates (all columns identical). Preserves all distinct rows.
        master_df = master_df.drop_duplicates(subset=list(master_df.columns), keep="first")
    elif dedupe_key_mode == "legacy":
        # Legacy: drop exact duplicates on a fixed subset (old behavior).
        legacy_subset = options.get("dedupe_subset") or [
            "website", "profile_url", "input_url", "name", "law_firm", "email", "phone", "contact_page_url"
        ]
        subset = [c for c in legacy_subset if c in master_df.columns]
        if subset:
            master_df = master_df.drop_duplicates(subset=subset, keep="first")
    else:
        # Identity-key dedupe: one key per row; prefer profile_url/detail_url/input_url, else composite.
        master_df["_master_dedupe_key"] = master_df.apply(
            lambda r: _master_dedupe_key(r, profile_col, input_url_col, detail_url_col),
            axis=1,
        )
        # Best-row selection: sort so best is first, then keep first per key.
        for col, sort_col in [
            ("has_email", "_sort_email"),
            ("has_contact_page", "_sort_contact"),
            ("has_phone", "_sort_phone"),
        ]:
            if col in master_df.columns:
                master_df[sort_col] = master_df[col].fillna(False).astype(int)
            else:
                master_df[sort_col] = 0
        master_df["_sort_idx"] = range(len(master_df))
        master_df.sort_values(
            by=["_sort_email", "_sort_contact", "_sort_phone", "_sort_idx"],
            ascending=[False, False, False, True],
            inplace=True,
        )
        master_df = master_df.drop_duplicates(subset=["_master_dedupe_key"], keep="first")
        master_df = master_df.drop(
            columns=[c for c in ["_master_dedupe_key", "_sort_email", "_sort_contact", "_sort_phone", "_sort_idx"] if c in master_df.columns],
            errors="ignore",
        )

    master_rows = len(master_df)
    duplicates_removed = rows_before_dedupe - master_rows

    out_path = output_master.get("path", "").strip()
    file_name = (output_master.get("file_name") or "MASTER_LEADS.csv").strip()
    if not out_path:
        report.record_module(config["module_id"], {"skipped": True, "reason": "empty output_master.path"})
        return df

    path_resolved = Path(out_path).resolve()
    out_dir = path_resolved.parent if path_resolved.suffix else path_resolved
    master_path = str(out_dir / file_name)

    cols = [c for c in MASTER_COLUMNS_ORDER if c in master_df.columns]
    for c in ["has_email", "has_contact_page", "has_phone"]:
        if c in master_df.columns and c not in cols:
            cols.append(c)
    master_df = master_df[cols].copy()

    output_fmt = output_master.get("format") or "csv"
    write_data(master_df, master_path, format=output_fmt)

    report.master_leads_count = master_rows
    report.master_output_path = master_path
    report.record_module(
        config["module_id"],
        {
            "master_rows_written": master_rows,
            "master_output_path": master_path,
            "duplicates_removed_master": duplicates_removed,
        },
    )
    return df
