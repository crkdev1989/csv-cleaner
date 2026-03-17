"""
Merge identity fields (name, law_firm, etc.) from a second CSV into the main dataframe.
Used when enrichment output has empty name/law_firm because the enrichment step
preserves columns by exact name (input had attorney_name/firm_name, not name/law_firm).
Left-merge on a key column (e.g. website or input_url) and fill missing values from the identity file.
"""

from pathlib import Path

import pandas as pd

from cleaner.report import CleaningReport


def _normalize_url_for_merge(s: str) -> str:
    """Lowercase and strip trailing slash for consistent join."""
    if pd.isna(s) or not isinstance(s, str):
        return ""
    return s.strip().lower().rstrip("/")


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Merge identity columns from a CSV. config["options"] may contain:
    - identity_path: path to CSV with identity columns (e.g. FindLaw results).
    - merge_key: column name to join on (default "website"). Used in both df and identity.
    - fill_columns: dict of { target_column: source_column } (e.g. {"name": "attorney_name", "law_firm": "firm_name"}).
      Only fills when the target is missing (null/empty) in df.
    """
    options = config.get("options", {})
    identity_path = options.get("identity_path")
    merge_key = options.get("merge_key", "website")
    raw_fill = options.get("fill_columns")
    fill_columns = raw_fill if isinstance(raw_fill, dict) else {}

    if identity_path is not None and hasattr(identity_path, "iloc"):
        identity_path = str(identity_path.iloc[0]).strip() if len(identity_path) > 0 else ""
    elif identity_path is not None:
        identity_path = str(identity_path).strip()
    else:
        identity_path = ""
    if len(identity_path) == 0 or len(fill_columns) == 0:
        report.record_module(config["module_id"], {"rows_merged": 0, "cells_filled": 0})
        return df

    path = Path(identity_path)
    if not path.is_absolute():
        # Resolve relative to config or cwd
        path = path.resolve()
    if not path.exists():
        report.record_module(
            config["module_id"],
            {"error": f"identity_path not found: {path}", "rows_merged": 0},
        )
        return df

    if merge_key not in df.columns:
        report.record_module(
            config["module_id"],
            {"error": f"merge_key '{merge_key}' not in dataframe", "rows_merged": 0},
        )
        return df

    try:
        identity_df = pd.read_csv(path)
    except Exception as e:
        report.record_module(
            config["module_id"],
            {"error": str(e), "rows_merged": 0},
        )
        return df

    if merge_key not in identity_df.columns:
        report.record_module(
            config["module_id"],
            {"error": f"merge_key '{merge_key}' not in identity file", "rows_merged": 0},
        )
        return df

    # Normalize key column in both for robust join
    df_key = df[merge_key].astype(str).apply(_normalize_url_for_merge)
    identity_df = identity_df.copy()
    identity_df["_merge_key"] = identity_df[merge_key].astype(str).apply(_normalize_url_for_merge)

    # Build merge: keep first identity row per key (in case of duplicates)
    identity_df = identity_df.drop_duplicates(subset=["_merge_key"], keep="first")

    merged = df.copy()
    filled_total = 0
    for target_col, source_col in fill_columns.items():
        if source_col not in identity_df.columns or target_col not in merged.columns:
            continue
        # Map key -> value from identity
        key_to_val = identity_df.set_index("_merge_key")[source_col].to_dict()
        merged["_identity_val"] = df_key.map(key_to_val)
        # Fill only where current value is missing
        mask = merged[target_col].isna() | (merged[target_col].astype(str).str.strip() == "")
        mask = mask & merged["_identity_val"].notna()
        filled = int(mask.sum())
        if filled > 0:
            merged.loc[mask, target_col] = merged.loc[mask, "_identity_val"]
            filled_total += filled
    if "_identity_val" in merged.columns:
        merged = merged.drop(columns=["_identity_val"])

    report.record_module(
        config["module_id"],
        {"rows_merged": int((df_key.isin(identity_df["_merge_key"])).sum()), "cells_filled": filled_total},
    )
    return merged
