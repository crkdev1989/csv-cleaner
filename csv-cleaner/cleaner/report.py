"""
Cleaning report: mutable container for run statistics and module-level metrics.

Used by the engine and by modules to record rows_loaded, rows_output,
duplicates_removed, rows_dropped, modules_executed, processing_time, and
arbitrary module-specific stats.
"""

import time
from typing import Any


class CleaningReport:
    """
    Mutable report object passed through the pipeline.
    Engine sets baseline fields; modules can update or add keys.
    """

    def __init__(self) -> None:
        self.rows_loaded: int = 0
        self.rows_output: int = 0
        self.duplicates_removed: int = 0
        self.rows_dropped: int = 0
        self.rows_dropped_junk: int = 0  # from drop_junk_website_rows (etc.)
        self.rows_dropped_required: int = 0  # from drop_rows_missing_required
        self.rows_dropped_strict_filter: int = 0  # from leads.filter_has_email (strict path)
        self.modules_executed: list[str] = []
        self.processing_time_seconds: float = 0.0
        # Module-specific metrics (e.g. {"core.drop_empty": {"dropped": 5}})
        self.module_stats: dict[str, dict[str, Any]] = {}
        self._start_time: float | None = None
        # Output paths (set by engine after run)
        self.output_path: str | None = None
        self.report_path: str | None = None
        self.input_path: str | None = None
        # Master leads (preserved dataset; set by leads.emit_master_leads when output_master is configured)
        self.master_leads_count: int | None = None
        self.master_output_path: str | None = None
        # Review-needed (rows with website but missing strong contact; set by leads.emit_review_needed)
        self.review_needed_count: int | None = None
        self.review_needed_output_path: str | None = None

    def start_timer(self) -> None:
        """Start the processing timer (called by engine at run start)."""
        self._start_time = time.perf_counter()

    def stop_timer(self) -> None:
        """Stop the timer and set processing_time_seconds."""
        if self._start_time is not None:
            self.processing_time_seconds = time.perf_counter() - self._start_time
            self._start_time = None

    def record_module(self, module_id: str, stats: dict[str, Any] | None = None) -> None:
        """Record that a module ran and optionally attach stats."""
        self.modules_executed.append(module_id)
        if stats is not None:
            self.module_stats[module_id] = stats

    def _stage_metrics(self) -> dict[str, Any]:
        """Aggregate stage/pipeline metrics for visibility (row loss between stages)."""
        m: dict[str, Any] = {
            "raw_input_rows": self.rows_loaded,
            "strict_email_ready_rows_written": self.rows_output,
            "rows_dropped_junk": self.rows_dropped_junk,
            "rows_dropped_required": self.rows_dropped_required,
            "rows_dropped_strict_filter": self.rows_dropped_strict_filter,
            "rows_dropped_total": self.rows_dropped,
            "rows_removed_by_dedupe": self.duplicates_removed,
        }
        if self.master_leads_count is not None:
            m["master_leads_rows_written"] = self.master_leads_count
        if self.review_needed_count is not None:
            m["review_needed_rows_written"] = self.review_needed_count
        return m

    def to_dict(self) -> dict[str, Any]:
        """Serialize report for JSON/file output."""
        out: dict[str, Any] = {
            "rows_loaded": self.rows_loaded,
            "rows_output": self.rows_output,
            "duplicates_removed": self.duplicates_removed,
            "rows_dropped": self.rows_dropped,
            "rows_dropped_junk": self.rows_dropped_junk,
            "rows_dropped_required": self.rows_dropped_required,
            "rows_dropped_strict_filter": self.rows_dropped_strict_filter,
            "modules_executed": self.modules_executed,
            "processing_time_seconds": round(self.processing_time_seconds, 4),
            "module_stats": self.module_stats,
            "stage_metrics": self._stage_metrics(),
        }
        if self.output_path is not None:
            out["output_path"] = self.output_path
        if self.report_path is not None:
            out["report_path"] = self.report_path
        if self.master_leads_count is not None:
            out["master_leads_count"] = self.master_leads_count
        if self.master_output_path is not None:
            out["master_output_path"] = self.master_output_path
        if self.review_needed_count is not None:
            out["review_needed_count"] = self.review_needed_count
        if self.review_needed_output_path is not None:
            out["review_needed_output_path"] = self.review_needed_output_path
        return out

    def format_summary(self) -> str:
        """Return a human-readable summary (for CLI and _summary.txt file)."""
        rows_removed = self.rows_loaded - self.rows_output
        lines = [
            "CSV Cleaner",
            "-----------",
            "",
            f"Input file:     {self.input_path or '(not set)'}",
            f"Output file:    {self.output_path or '(not set)'}",
            "",
            f"Rows loaded:    {self.rows_loaded}",
            f"Rows output:    {self.rows_output}",
            f"Rows removed:   {rows_removed}",
            "",
            "Stage / pipeline metrics:",
            f"  Raw input rows:                    {self.rows_loaded}",
            f"  Strict (email-ready) rows written:  {self.rows_output}",
            f"  Rows dropped (junk URLs):          {self.rows_dropped_junk}",
            f"  Rows dropped (required-field):    {self.rows_dropped_required}",
            f"  Rows dropped (strict email filter): {self.rows_dropped_strict_filter}",
            f"  Rows removed by dedupe:            {self.duplicates_removed}",
        ]
        if self.master_leads_count is not None:
            lines.append(f"  Master leads rows written:          {self.master_leads_count}")
        if self.review_needed_count is not None:
            lines.append(f"  Review-needed rows written:         {self.review_needed_count}")
        lines.extend([
            "",
            "Modules applied:",
        ])
        for mod in self.modules_executed:
            lines.append(f"  • {mod}")
        lines.extend([
            "",
            "Generated files (outputs by purpose):",
            f"  Email-ready (strict):    {self.output_path or '(none)'} ({self.rows_output} rows)",
            f"  Report:                  {self.report_path or '(none)'}",
        ])
        if self.master_output_path is not None:
            lines.append(f"  Master leads:         {self.master_output_path} ({self.master_leads_count or 0} rows)")
        if self.review_needed_output_path is not None:
            lines.append(f"  Review-needed:        {self.review_needed_output_path} ({self.review_needed_count or 0} rows)")
        return "\n".join(lines)
