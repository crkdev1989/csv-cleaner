"""
Module loader: resolve module id (e.g. core.drop_empty) to a run(df, config, report) callable.
Pipeline: run modules in order and pass DataFrame + config + report.
"""

from pathlib import Path
from typing import Any, Callable

import pandas as pd

from cleaner.report import CleaningReport


# Type for module run function
ModuleRun = Callable[[pd.DataFrame, dict[str, Any], CleaningReport], pd.DataFrame]


def get_modules_dir() -> Path:
    """Project root (folder containing 'cleaner/'). Modules live at project_root/modules."""
    # cleaner/pipeline.py -> parent=cleaner, parent.parent=project root
    return Path(__file__).resolve().parent.parent


def load_module(module_id: str, modules_root: Path | None = None) -> ModuleRun:
    """
    Load module by id (e.g. core.drop_empty). Returns the run(df, config, report) callable.
    Looks for modules/<group>/<name>.py and expects a top-level run function.
    """
    modules_root = modules_root or get_modules_dir() / "modules"
    if "." not in module_id:
        raise ValueError(f"Module id must be 'group.name': {module_id}")

    group, name = module_id.split(".", 1)
    module_path = modules_root / group / f"{name}.py"

    if not module_path.exists():
        raise FileNotFoundError(f"Module not found: {module_path}")

    # Dynamic import
    import importlib.util
    spec = importlib.util.spec_from_file_location(f"cleaner_modules.{module_id}", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module: {module_id}")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    if not hasattr(mod, "run"):
        raise AttributeError(f"Module {module_id} must define run(df, config, report)")

    return getattr(mod, "run")


def run_pipeline(
    df: pd.DataFrame,
    config: dict[str, Any],
    report: CleaningReport,
    modules_root: Path | None = None,
) -> pd.DataFrame:
    """
    Run the module pipeline in order. Each module receives (df, config, report)
    and returns transformed df. Report is updated with modules_executed and optional stats.
    """
    modules_root = modules_root or get_modules_dir() / "modules"
    module_specs = config.get("modules") or []

    for spec in module_specs:
        if isinstance(spec, str):
            module_id = spec
            module_options = {}
        elif isinstance(spec, dict) and "id" in spec:
            module_id = spec["id"]
            module_options = spec.get("options", {})
        else:
            continue

        run_fn = load_module(module_id, modules_root=modules_root)
        # Pass full config so modules can read input/output and their own options
        module_config = {"module_id": module_id, "options": module_options, "config": config}
        rows_before = len(df)
        df = run_fn(df, module_config, report)
        rows_after = len(df)

        # If module didn't call report.record_module, do a minimal record
        if module_id not in report.modules_executed:
            report.record_module(
                module_id,
                {"rows_before": rows_before, "rows_after": rows_after},
            )

    return df
