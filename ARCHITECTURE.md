# CSV Cleaner — Architecture

## Purpose

Config-driven, modular pipeline for cleaning tabular data. The base engine is thin; all transformation logic lives in plugin-style modules under `modules/`. Designed for reuse across jobs and industries, local or VPS, with a path to productization.

## High-level flow

1. **Config** — Load JSON config (paths, module list, options).
2. **Load** — Read input file (CSV, XLSX, JSON, or YAML) into a pandas DataFrame (or chunked iterator for large CSV).
3. **Pipeline** — Run modules in config order; each receives `(df, config, report)` and returns the transformed DataFrame.
4. **Write output** — Write the final DataFrame to the path and format specified in config.
5. **Write report** — If `report.path` is set, write the cleaning report (rows_loaded, rows_output, modules_executed, timing, etc.) to JSON.

## Component responsibilities

| Component | Responsibility |
|-----------|----------------|
| **config.py** | Load and validate JSON config; deep-merge with defaults; resolve input/output/report paths relative to the config file directory; infer format from extension. |
| **engine.py** | Orchestrate a run: load config, load data (full or chunked), run pipeline, set report counts, write output and report; expose `run_cleaner(config_path)` and `run_cleaner_batch(config_paths)`. |
| **pipeline.py** | Resolve module id (e.g. `core.drop_empty`) to `modules/<group>/<name>.py`; load and run each module in order; pass `(df, config, report)`; optionally record module stats. |
| **report.py** | Mutable `CleaningReport`: rows_loaded, rows_output, duplicates_removed, rows_dropped, modules_executed, processing_time_seconds, module_stats; `start_timer`/`stop_timer`, `record_module`, `to_dict`. |
| **writers.py** | Write DataFrame to CSV/XLSX/JSON/YAML from path and format; write report dict to JSON; create output directories as needed. |
| **cli.py** | Parse CLI args (single config file or directory of `.json` configs); call `run_cleaner_batch`; print summary or errors; exit code. |
| **loaders/** | Normalize input to DataFrame: CSV (with optional chunked read), XLSX, JSON, YAML; factory dispatches by format. |
| **modules/** | Plugin directories (e.g. core, text, dedupe, validation, real_estate). Each module is a `.py` file with a `run(df, config, report)` function. |

## Module interface contract

- **Location**: `modules/<group>/<name>.py` (e.g. `modules/core/drop_empty.py`).
- **Signature**: `run(df: pd.DataFrame, config: dict, report: CleaningReport) -> pd.DataFrame`
- **Config** passed to the module: `{"module_id": "...", "options": {...}, "config": <full config>}`.
- **Report**: Modules may update `report` (e.g. `report.rows_dropped`, `report.record_module(module_id, stats)`).
- **Return**: Transformed DataFrame for the next module or final output.

## Config path resolution

- Paths in config (`input.path`, `output.path`, `report.path`) are resolved **relative to the directory of the config file**.
- Example: config at `configs/example.json` with `"path": "../data/sample.csv"` resolves to `configs/../data/sample.csv` (project root `data/sample.csv`).
- Absolute paths are left unchanged (after `Path.resolve()`).

## Supported input types

- **CSV** — `pd.read_csv`; supports chunked read when `chunk_size` is set in config.
- **XLSX** — First sheet via `pd.read_excel` (openpyxl).
- **JSON** — List of objects or dict; normalized to DataFrame.
- **YAML** — List of dicts or single dict; normalized to DataFrame.

Output supports the same formats (CSV, XLSX, JSON, YAML) via `writers.write_data`.

## Usage

**CLI**

```bash
python run.py config.json
python run.py configs/
```

**Library**

```python
from cleaner import run_cleaner

report = run_cleaner("config.json")
print(report.to_dict())
```

Batch (multiple configs):

```python
from cleaner.engine import run_cleaner_batch

for path, report in run_cleaner_batch("configs/"):
    print(path, report.rows_output)
```
