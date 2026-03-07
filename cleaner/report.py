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
        self.modules_executed: list[str] = []
        self.processing_time_seconds: float = 0.0
        # Module-specific metrics (e.g. {"core.drop_empty": {"dropped": 5}})
        self.module_stats: dict[str, dict[str, Any]] = {}
        self._start_time: float | None = None

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

    def to_dict(self) -> dict[str, Any]:
        """Serialize report for JSON/file output."""
        return {
            "rows_loaded": self.rows_loaded,
            "rows_output": self.rows_output,
            "duplicates_removed": self.duplicates_removed,
            "rows_dropped": self.rows_dropped,
            "modules_executed": self.modules_executed,
            "processing_time_seconds": round(self.processing_time_seconds, 4),
            "module_stats": self.module_stats,
        }
