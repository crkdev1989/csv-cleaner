"""
Orchestration: load config, load data, run pipeline, write output and report.
Supports full load and chunked processing for large files.
"""

from pathlib import Path
from typing import Any

import pandas as pd

from cleaner.config import get_extension_for_format, load_config, infer_format
from cleaner.loaders import load_data, load_data_chunked
from cleaner.pipeline import run_pipeline
from cleaner.report import CleaningReport
from cleaner.writers import write_data, write_report


def run_cleaner(
    config_path: str | Path | None = None,
    *,
    config_dict: dict[str, Any] | None = None,
    config_overrides: dict[str, Any] | None = None,
) -> CleaningReport:
    """
    Run the cleaner for one config. Either config_path (file) or config_dict must be provided.
    config_overrides: optional dict merged into loaded config (e.g. for testing).
    """
    if config_dict is not None:
        from cleaner.config import DEFAULT_CONFIG, _deep_merge
        config = _deep_merge(DEFAULT_CONFIG.copy(), config_dict)
        # Ensure paths are absolute
        if config.get("input", {}).get("path"):
            config["input"]["path"] = str(Path(config["input"]["path"]).resolve())
        if config.get("output", {}).get("path"):
            config["output"]["path"] = str(Path(config["output"]["path"]).resolve())
        if config.get("report", {}).get("path"):
            config["report"]["path"] = str(Path(config["report"]["path"]).resolve())
        # Preset may use "pipeline" key; normalize to "modules"
        if "pipeline" in config:
            config["modules"] = config.pop("pipeline", [])
    elif config_path is not None:
        config = load_config(config_path)
    else:
        raise ValueError("Either config_path or config_dict is required")

    if config_overrides:
        for key, value in config_overrides.items():
            if isinstance(value, dict) and isinstance(config.get(key), dict):
                config[key] = {**config[key], **value}
            else:
                config[key] = value

    report = CleaningReport()
    report.start_timer()

    input_path = config["input"]["path"]
    if not (input_path and str(input_path).strip()):
        raise ValueError("config input.path is required and must be non-empty")
    input_fmt = config["input"].get("format") or infer_format(input_path)
    output_path_cfg = config["output"]["path"]
    if not (output_path_cfg and str(output_path_cfg).strip()):
        raise ValueError("config output.path is required and must be non-empty")
    output_fmt = config["output"].get("format") or infer_format(output_path_cfg)
    chunk_size = config.get("chunk_size")

    # Derive output paths from input filename: {stem}_cleaned.{ext}, {stem}_report.json
    input_stem = Path(input_path).stem
    output_path_resolved = Path(output_path_cfg).resolve()
    output_dir = output_path_resolved.parent if output_path_resolved.suffix else output_path_resolved
    output_ext = get_extension_for_format(output_fmt)
    output_path = str(output_dir / f"{input_stem}_cleaned{output_ext}")
    report_path = (
        str(output_dir / f"{input_stem}_report.json")
        if config.get("report", {}).get("path")
        else None
    )
    report.output_path = output_path
    report.report_path = report_path
    report.input_path = input_path

    if chunk_size and chunk_size > 0:
        _run_chunked(config, input_path, input_fmt, output_path, output_fmt, report, chunk_size)
    else:
        _run_full(config, input_path, input_fmt, output_path, output_fmt, report)

    report.stop_timer()

    if report_path:
        write_report(report.to_dict(), report_path)

    return report


def _run_full(
    config: dict[str, Any],
    input_path: str,
    input_fmt: str | None,
    output_path: str,
    output_fmt: str,
    report: CleaningReport,
) -> None:
    """Load full dataset, run pipeline once, write output."""
    df = load_data(input_path, format=input_fmt)
    report.rows_loaded = len(df)

    df = run_pipeline(df, config, report)
    report.rows_output = len(df)

    write_data(df, output_path, format=output_fmt)


def _run_chunked(
    config: dict[str, Any],
    input_path: str,
    input_fmt: str | None,
    output_path: str,
    output_fmt: str,
    report: CleaningReport,
    chunk_size: int,
) -> None:
    """
    Process in chunks: each chunk runs full pipeline; results accumulated and written once.
    Report aggregates rows_loaded and rows_output across chunks.
    """
    total_loaded = 0
    out_chunks: list[pd.DataFrame] = []

    for chunk in load_data_chunked(input_path, chunk_size=chunk_size, format=input_fmt):
        total_loaded += len(chunk)
        df = run_pipeline(chunk, config, report)
        if len(df) > 0:
            out_chunks.append(df)

    report.rows_loaded = total_loaded
    if out_chunks:
        combined = pd.concat(out_chunks, ignore_index=True)
        report.rows_output = len(combined)
        write_data(combined, output_path, format=output_fmt)
    else:
        report.rows_output = 0
        write_data(pd.DataFrame(), output_path, format=output_fmt)


def run_cleaner_batch(
    config_paths: str | Path,
) -> list[tuple[str, CleaningReport]]:
    """
    Run multiple configs. config_paths can be a single .json file or a directory
    containing .json files. Returns list of (config_path, report).
    """
    path = Path(config_paths)
    if path.is_file() and path.suffix.lower() == ".json":
        configs = [path]
    elif path.is_dir():
        configs = sorted(path.glob("*.json"))
    else:
        raise FileNotFoundError(f"Not a config file or directory: {config_paths}")

    results = []
    for p in configs:
        report = run_cleaner(p)
        results.append((str(p), report))
    return results
