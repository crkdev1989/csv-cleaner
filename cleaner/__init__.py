"""
CSV Cleaner — config-driven, modular cleaning engine.

Public API:
    run_cleaner(config_path)  Run a single config or directory of configs.
"""

from cleaner.engine import run_cleaner

__all__ = ["run_cleaner"]
