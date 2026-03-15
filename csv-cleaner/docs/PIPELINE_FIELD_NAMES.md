# Pipeline field names (scrape → enrich → clean)

## Where name and law_firm are lost

1. **FindLaw scrape output** (`results.csv`) has `attorney_name` and `firm_name` populated (e.g. "Plaxen Adler Muncy, P.A.", "PLAXEN ADLER MUNCY, P.A. - Columbia, MD").
2. **Website enrichment** builds CONTACT_ENRICHED from the crawl and merges “preserved” columns from the **input CSV row** by **exact column name**. The config lists `preserve_columns: [name, law_firm, ...]`, so the runner does `output_row["name"] = input_row.get("name")` and `output_row["law_firm"] = input_row.get("law_firm")`. The FindLaw input has `attorney_name` and `firm_name`, not `name` and `law_firm`, so those keys are missing and the enriched row gets **empty** name and law_firm.
3. **Csv-cleaner** only renames columns that exist (e.g. `attorney_name` → `name`). CONTACT_ENRICHED has no `attorney_name`/`firm_name` columns, so nothing is renamed and name/law_firm stay empty.

**Fix**: Use **`leads.merge_identity`** in csv-cleaner with `identity_path` pointing at the FindLaw results CSV. The module left-joins on `website` and fills `name`/`law_firm` from the identity file’s `attorney_name`/`firm_name`.

**To run the FindLaw pipeline:** Edit `csv-cleaner/configs/law_firm_leads.json`: (1) Set `input.path` to your CONTACT_ENRICHED.csv path. (2) In the `leads.merge_identity` module options, set `identity_path` to your FindLaw results.csv path (e.g. `../scraper-engine/outputs/findlaw_annapolis_pw_test2_20260315_182355/results.csv`). Paths are relative to the current working directory when you run the cleaner (typically `csv-cleaner/`).

---

Canonical column names used across FindLaw → website enrichment → csv-cleaner:

| Canonical   | Description              | FindLaw/scraper alias   | Enrichment output |
|------------|---------------------------|-------------------------|-------------------|
| `name`     | Contact / attorney name   | `attorney_name`         | `name` (or preserve from input) |
| `law_firm` | Firm name                 | `firm_name`             | `law_firm` (or preserve from input) |
| `city`     | City                      | (from address)          | `city` |
| `state`    | State                     | (from address)         | `state` |
| `website`  | Firm website URL          | `website`              | `website` |
| `profile_url` | Directory profile URL  | `detail_url` / `profile_url` | `profile_url` |
| `source_directory` | Directory page URL  | `source_url` / `input_url` | `source_directory` |
| `input_url`| URL used as crawl target  | `input_url`            | `input_url` |
| `email`    | Best email                | `email`                | `email` |
| `phone`    | Best phone                | `phone`                | `contact_phone` → renamed to `phone` in law_firm_leads |
| `contact_page_url` | Contact page URL    | —                      | `contact_page_url` |

## Enrichment handoff

- **Input to enrichment**: FindLaw `results.csv` (or similar) has `attorney_name`, `firm_name`, `website`, `input_url`, `source_url`, `detail_url`, etc.
- **Enrichment output** should use canonical names so csv-cleaner does not need source-specific renames.
- If the enrichment step preserves columns by **exact name**, then either:
  1. Configure the enrichment to **map** input → canonical when preserving (e.g. `attorney_name` → `name`, `firm_name` → `law_firm`, `source_url` → `source_directory`, `detail_url` → `profile_url`), and keep `preserve_columns: [name, law_firm, city, state, website, profile_url, source_directory, input_url]`, or  
  2. Preserve source columns (`attorney_name`, `firm_name`, …) and rely on csv-cleaner’s `core.rename_columns` to normalize to `name` and `law_firm`.

**Suggested enrichment config** (when merging input CSV row into each output row). Use canonical output column names and, for FindLaw input, map from source columns:

```yaml
# In metadata.input_csv (or equivalent)
input_csv:
  url_column: website
  preserve_columns:
    - name
    - law_firm
    - city
    - state
    - website
    - profile_url
    - source_directory
    - input_url
  # Optional: when input CSV uses different names, map source column -> canonical output column
  input_column_mapping:
    attorney_name: name
    firm_name: law_firm
    detail_url: profile_url
    source_url: source_directory
```

When writing each enriched row, for each `preserve_columns` key (e.g. `name`), use `input_column_mapping` to read from the source column if the canonical key is missing in the input row (e.g. `name` ← `attorney_name`).

## Csv-cleaner configs

- **law_firm_leads.json**: Normalizes `contact_name`/`firm_name`/`attorney_name` → `name`/`law_firm`, then uses only canonical names in dedupe and select_columns.
- **website_enrichment_clean.json**: Renames `contact_name`→`name`, `firm_name`→`law_firm` if present, then runs lead-selection and domain dedupe.

## Junk handling (csv-cleaner)

- **`leads.drop_junk_website_rows`**: **Drops** rows where `website` or `input_url` contains `google.com/maps` or `amazon.com` (configurable).
- **`leads.blank_junk_contacts`** blanks:
  - **Website**: URLs containing `google.com/maps` or `amazon.com` (so downstream required-column drop can remove row if needed).
  - **Email**: Placeholder/fake patterns (e.g. `johndoe@email.com`, `flags@2x.png`, `@example.com`).
  - **Phone**: Placeholder numbers (`000-000-0000`, `800-555-6666`, `555-555-5555`) and numbers with **invalid US area codes** (e.g. 000, 555, 666, or first 3 digits &lt; 200 or &gt; 989, e.g. 177, 119, 126, 130).

Rows with all of website/email/phone blank can be dropped by `core.drop_rows_missing_required` (when configured with `how: "all"`).

## Rerun commands (WSL)

From repo root or `csv-cleaner/`:

1. **Edit config** for FindLaw pipeline: in `csv-cleaner/configs/law_firm_leads.json` set `input.path` to your CONTACT_ENRICHED.csv and `leads.merge_identity.options.identity_path` to your FindLaw results.csv.

2. **Run cleaner** (paths in config; run from `csv-cleaner/`):
   ```bash
   cd /mnt/c/Users/crk24/Craig/Dev/crk-dev/csv-cleaner
   python -m cleaner.cli configs/law_firm_leads.json
   ```
   Or with explicit input file (overrides config input.path):
   ```bash
   cd /mnt/c/Users/crk24/Craig/Dev/crk-dev/csv-cleaner
   python -m cleaner.cli ../scraper-engine/outputs/findlaw_annapolis_contact_enrich_20260315_182906/CONTACT_ENRICHED.csv --config configs/law_firm_leads.json
   ```
   When using `--config`, you must still set `identity_path` in the config for name/law_firm merge.
