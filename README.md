# CSV Cleaner Engine

Config-driven, modular pipeline for cleaning tabular data. Thin base engine; all transformation logic lives in plugin-style modules.

## Setup

```bash
pip install -r requirements.txt
# or: pip install -e .
```

## Usage

**CLI (single config or directory of configs):**

```bash
python run.py config.json
python run.py configs/
```

**Python API:**

```python
from cleaner import run_cleaner

report = run_cleaner("config.json")
print(report.to_dict())
```

## Config (JSON)

- `input.path` — input file path (relative to config file directory)
- `input.format` — csv | xlsx | json | yaml (optional; inferred from extension)
- `output.path` — cleaned output path
- `output.format` — output format (default csv)
- `report.path` — optional path for cleaning report JSON
- `modules` — ordered list of module ids or `{"id": "...", "options": {...}}`
- `chunk_size` — optional; process input in chunks (CSV only for chunked read)

## Module contract

- Place modules under `modules/<group>/<name>.py` (e.g. `modules/core/drop_empty.py`).
- Each module must define: `run(df, config, report) -> DataFrame`.
- `config` passed to the module: `{"module_id": "...", "options": {...}, "config": <full config>}`.
- Use `report.record_module(module_id, stats)` to record metrics.

## Example

See `configs/example.json` and `data/sample.csv`. Run:

```bash
python run.py configs/example.json
```

Output files are named from the input (e.g. `sample_cleaned.csv`, `sample_report.json`) in the configured output directory. `configs/example_all_modules.json` demonstrates the full set of modules (text, core, dedupe).
