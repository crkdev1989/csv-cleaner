"""
Emit rows with website but missing strong contactability to a separate file for later review.
Runs after add_lead_flags. Does not remove rows from the pipeline (they still go to master).
"""

from pathlib import Path

import pandas as pd

from cleaner.report import CleaningReport
from cleaner.writers import write_data


def _has_value(series: pd.Series | pd.DataFrame) -> pd.Series:
    """True where value is non-null and (if str) non-empty after strip."""
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    out = series.notna()
    col_dtype = series.dtype
    if col_dtype == object or str(col_dtype) == "object":
        out = out & series.astype(str).str.strip().astype(bool)
    return out


def run(
    df: pd.DataFrame,
    config: dict,
    report: CleaningReport,
) -> pd.DataFrame:
    """
    Write rows with website but without email/contact_page/phone to output_review path.
    Requires add_lead_flags to have run (uses has_email, has_contact_page, has_phone).
    Does not modify df.
    """
    full_config = config.get("config", {})
    output_review = full_config.get("output_review")
    if not output_review or not output_review.get("path"):
        report.record_module(config["module_id"], {"skipped": True, "reason": "no output_review.path"})
        return df

    out_path = output_review.get("path", "").strip()
    if not out_path:
        report.record_module(config["module_id"], {"skipped": True, "reason": "empty output_review.path"})
        return df

    website_col = "website"
    if website_col not in df.columns:
        report.record_module(config["module_id"], {"skipped": True, "reason": "no website column"})
        return df

    has_website = _has_value(df[website_col])

    if "has_email" in df.columns and "has_contact_page" in df.columns and "has_phone" in df.columns:
        has_any_contact = df["has_email"] | df["has_contact_page"] | df["has_phone"]
    else:
        email_col = df.columns[df.columns.isin(["email", "contact_page_url", "phone"])]
        has_any_contact = pd.Series(False, index=df.index)
        for c in email_col:
            has_any_contact = has_any_contact | _has_value(df[c])

    review_mask = has_website & ~has_any_contact
    review_df = df.loc[review_mask].copy(deep=True)

    file_name = (output_review.get("file_name") or "REVIEW_NEEDED.csv").strip()
    path_resolved = Path(out_path).resolve()
    out_dir = path_resolved.parent if path_resolved.suffix else path_resolved
    review_path = str(out_dir / file_name)
    output_fmt = output_review.get("format") or "csv"
    write_data(review_df, review_path, format=output_fmt)

    report.review_needed_count = len(review_df)
    report.review_needed_output_path = review_path
    report.record_module(
        config["module_id"],
        {
            "review_needed_rows_written": len(review_df),
            "review_needed_output_path": review_path,
        },
    )
    return df
