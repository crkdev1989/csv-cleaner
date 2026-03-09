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
- `output.path` — output path or directory; filenames are derived from input (e.g. `sample_cleaned.csv`, `sample_report.json`)
- `output.format` — output format (default csv)
- `report.path` — if set, write cleaning report JSON
- `modules` — ordered list of module ids or `{"id": "...", "options": {...}}`
- `chunk_size` — optional; process input in chunks (CSV only)

## Module contract

- Place modules under `modules/<group>/<name>.py` (e.g. `modules/core/drop_empty.py`).
- Each module must define: `run(df, config, report) -> DataFrame`.
- `config` passed to the module: `{"module_id": "...", "options": {...}, "config": <full config>}`.
- Use `report.record_module(module_id, stats)` to record metrics.

---

## Available Modules

### core

| Module ID | Purpose |
|-----------|---------|
| `core.drop_empty` | Drop rows that are entirely empty (all cells null/empty). |
| `core.drop_empty_columns` | Drop columns where every value is null (and optionally blank string). |
| `core.drop_columns` | Drop specified columns by name. |
| `core.drop_rows_missing_required` | Drop rows with null in any (or all) of the required columns. |
| `core.fill_nulls` | Fill nulls with a constant or per-column values. |
| `core.rename_columns` | Rename columns via a mapping. |
| `core.select_columns` | Keep only specified columns (and optionally reorder). |

**core.drop_empty**
- **Options:** `subset` (optional list of columns to check; default all).
- **Report:** `dropped_empty`
- **Example:**
```json
"core.drop_empty"
```

**core.drop_empty_columns**
- **Options:** `subset` (optional list of columns to evaluate; default all), `treat_blank_strings_as_empty` (default true).
- **Report:** `columns_dropped`, `dropped_columns`
- **Example:**
```json
{ "id": "core.drop_empty_columns", "options": { "treat_blank_strings_as_empty": true } }
```

**core.drop_columns**
- **Options:** `columns` (list of column names to drop).
- **Report:** `columns_dropped`, `dropped_columns`
- **Example:**
```json
{ "id": "core.drop_columns", "options": { "columns": ["remark", "temp"] } }
```

**core.drop_rows_missing_required**
- **Options:** `columns` (list of required column names), `how` ("any" | "all", default "any").
- **Report:** `rows_dropped`, `required_columns`, `how`
- **Example:**
```json
{ "id": "core.drop_rows_missing_required", "options": { "columns": ["name", "email"], "how": "any" } }
```

**core.fill_nulls**
- **Options:** `value` (scalar), `values` (per-column dict), `columns` (optional list to limit).
- **Report:** `columns_filled`, `nulls_filled`
- **Example:**
```json
{ "id": "core.fill_nulls", "options": { "value": 0, "columns": ["amount"] } }
```

**core.rename_columns**
- **Options:** `mapping` (dict of old_name → new_name).
- **Report:** `renamed` (list of {old, new})
- **Example:**
```json
{ "id": "core.rename_columns", "options": { "mapping": { "notes": "remark", "value": "amount" } } }
```

**core.select_columns**
- **Options:** `columns` (list to keep, order preserved), `strict` (default false; if true, raise on missing column).
- **Report:** `columns_selected`, `columns_dropped`, `selected`
- **Example:**
```json
{ "id": "core.select_columns", "options": { "columns": ["name", "amount"], "strict": false } }
```

---

### text

| Module ID | Purpose |
|-----------|---------|
| `text.trim_whitespace` | Strip leading/trailing whitespace from string columns. |
| `text.normalize_empty_strings` | Treat empty and whitespace-only strings as null; optional custom empty values. |
| `text.normalize_case` | Lowercase, uppercase, or title-case string columns. |
| `text.replace_values` | Replace specific values with new values (optionally per-column). |
| `text.remove_non_printable` | Remove control/non-printable characters from strings. |
| `text.normalize_for_matching` | Lowercase, trim, remove punctuation, collapse whitespace for dedupe/matching. |

**text.trim_whitespace**
- **Options:** `columns` (optional; default all string columns).
- **Report:** `columns_trimmed`, `cells_changed`
- **Example:**
```json
"text.trim_whitespace"
```

**text.normalize_empty_strings**
- **Options:** `columns` (optional), `empty_values` (list of extra strings to treat as null, e.g. ["n/a", "-"]).
- **Report:** `columns_processed`, `values_replaced`
- **Example:**
```json
{ "id": "text.normalize_empty_strings", "options": { "empty_values": ["n/a", "NA", "-"] } }
```

**text.normalize_case**
- **Options:** `columns` (optional), `case` ("lower" | "upper" | "title", default "lower").
- **Report:** `columns_normalized`, `case`
- **Example:**
```json
{ "id": "text.normalize_case", "options": { "case": "lower", "columns": ["name"] } }
```

**text.replace_values**
- **Options:** `columns` (optional), `mapping` (old → new; new can be null), `mappings` (per-column overrides).
- **Report:** `columns_processed`, `replacements`
- **Example:**
```json
{ "id": "text.replace_values", "options": { "mapping": { "first": "1st", "second": "2nd" } } }
```

**text.remove_non_printable**
- **Options:** `columns` (optional), `collapse_whitespace` (default false), `strip` (default false).
- **Report:** `columns_processed`, `values_changed`
- **Example:**
```json
{ "id": "text.remove_non_printable", "options": { "strip": true } }
```

**text.normalize_for_matching**
- **Options:** `columns` (optional), `remove_punctuation` (default true), `collapse_whitespace` (default true).
- **Report:** `columns_processed`, `values_changed`
- **Example:**
```json
{ "id": "text.normalize_for_matching", "options": { "remove_punctuation": true, "collapse_whitespace": true } }
```

---

### validation

| Module ID | Purpose |
|-----------|---------|
| `validation.coerce_numeric` | Coerce columns to numeric; strip commas/currency; invalid → NaN when errors="coerce". |
| `validation.coerce_datetime` | Coerce columns to datetime; optional format, dayfirst, utc. |
| `validation.standardize_phone` | US phone: digits, (XXX) XXX-XXXX, or E.164; invalid left unchanged. |
| `validation.standardize_zip` | US ZIP: zip5 or zip9; preserve leading zeros; invalid left unchanged. |
| `validation.standardize_state` | US state: map names ↔ abbreviations; unmapped left unchanged. |

**validation.coerce_numeric**
- **Options:** `columns` (required), `errors` ("coerce" | "raise" | "ignore", default "coerce"), `downcast` (optional), `strip_commas` (default true), `strip_currency` (default false).
- **Report:** `columns_processed`, `values_converted`, `values_failed`
- **Example:**
```json
{ "id": "validation.coerce_numeric", "options": { "columns": ["value"], "strip_commas": true } }
```

**validation.coerce_datetime**
- **Options:** `columns` (required), `errors` (default "coerce"), `format`, `dayfirst`, `utc`.
- **Report:** `columns_processed`, `values_converted`, `values_failed`
- **Example:**
```json
{ "id": "validation.coerce_datetime", "options": { "columns": ["date"], "dayfirst": false } }
```

**validation.standardize_phone**
- **Options:** `columns` (required), `output` ("digits" | "us" | "e164", default "us"), `keep_country_code` (default true).
- **Report:** `columns_processed`, `values_changed`, `values_invalid`
- **Example:**
```json
{ "id": "validation.standardize_phone", "options": { "columns": ["phone"], "output": "us" } }
```

**validation.standardize_zip**
- **Options:** `columns` (required), `output` ("zip5" | "zip9", default "zip5").
- **Report:** `columns_processed`, `values_changed`, `values_invalid`
- **Example:**
```json
{ "id": "validation.standardize_zip", "options": { "columns": ["zip"], "output": "zip5" } }
```

**validation.standardize_state**
- **Options:** `columns` (required), `output` ("abbr" | "name", default "abbr").
- **Report:** `columns_processed`, `values_changed`, `values_unmapped`
- **Example:**
```json
{ "id": "validation.standardize_state", "options": { "columns": ["state"], "output": "abbr" } }
```

---

### dedupe

| Module ID | Purpose |
|-----------|---------|
| `dedupe.drop_duplicates` | Remove duplicate rows; optional subset and keep first/last. |

**dedupe.drop_duplicates**
- **Options:** `subset` (optional list of columns), `keep` ("first" | "last" | false, default "first").
- **Report:** `duplicates_removed`
- **Example:**
```json
{ "id": "dedupe.drop_duplicates", "options": { "keep": "first" } }
```

---

## Example Pipelines

**Basic CSV cleanup** — trim, normalize empties, drop empty rows, dedupe:

```bash
python run.py configs/example.json
```

Config: `configs/example.json` — uses `core.drop_empty` and `dedupe.drop_duplicates` on `data/sample.csv`. Output: `output/sample_cleaned.csv`, `output/sample_report.json`.

**Contacts / lead cleanup** — text normalization, US phone/zip/state standardization:

```bash
python run.py configs/example_standardize_phone_zip_state.json
```

Config: `configs/example_standardize_phone_zip_state.json` — input `data/sample_contacts.csv`. Pipeline: trim, normalize_for_matching (e.g. on name), standardize_phone, standardize_zip, standardize_state. Good for leads or contact lists with mixed formats.

**Validation-focused pipeline** — coerce types, drop empty columns, then standard cleanup:

```bash
python run.py configs/example_validation_and_more.json
```

Config: `configs/example_validation_and_more.json` — input `data/sample.csv`. Pipeline: trim, remove_non_printable, coerce_numeric (e.g. `value`), coerce_datetime (e.g. `date` if present), drop_empty_columns, drop_empty, drop_duplicates. Use when you need type coercion and empty-column removal before other cleaning.

---

## Report keys (summary)

Report stats follow a consistent style across modules:

- **Counts:** `columns_processed`, `columns_dropped`, `columns_selected`, `columns_trimmed`, `columns_normalized`, `columns_filled`
- **Lists:** `dropped_columns`, `selected`, `renamed`
- **Values:** `values_changed`, `values_converted`, `values_replaced`, `values_failed`, `values_invalid`, `values_unmapped`, `replacements`, `cells_changed`
- **Rows:** `rows_dropped`, `dropped_empty`, `duplicates_removed`, `nulls_filled`

Engine-level report fields: `rows_loaded`, `rows_output`, `processing_time_seconds`, `modules_executed`, `module_stats`.

For a full pipeline that uses many modules, see `configs/example_all_modules.json`.
