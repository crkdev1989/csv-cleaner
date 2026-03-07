"""
Output writers: write DataFrame to CSV/XLSX/JSON/YAML and write report to JSON.
"""

from pathlib import Path
from typing import Any

import pandas as pd


def write_data(
    df: pd.DataFrame,
    path: str | Path,
    format: str | None = None,
    **kwargs: Any,
) -> None:
    """
    Write DataFrame to file. format: csv | xlsx | json | yaml.
    If None, inferred from path extension.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = (format or _infer_format(path)).lower()

    if fmt == "csv":
        df.to_csv(path, index=False, **kwargs)
    elif fmt == "xlsx":
        df.to_excel(path, index=False, engine="openpyxl", **kwargs)
    elif fmt == "json":
        df.to_json(path, orient="records", indent=2, **kwargs)
    elif fmt == "yaml":
        _write_yaml(df, path, **kwargs)
    else:
        raise ValueError(f"Unsupported output format: {format}")


def _write_yaml(df: pd.DataFrame, path: Path, **kwargs: Any) -> None:
    import yaml
    records = df.to_dict(orient="records")
    path.write_text(yaml.dump(records, default_flow_style=False, allow_unicode=True), encoding="utf-8")


def _infer_format(path: Path) -> str:
    ext = path.suffix.lower()
    mapping = {".csv": "csv", ".xlsx": "xlsx", ".json": "json", ".yaml": "yaml", ".yml": "yaml"}
    return mapping.get(ext, "csv")


def write_report(report_dict: dict[str, Any], path: str | Path) -> None:
    """Write report dict to JSON file."""
    import json
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report_dict, indent=2), encoding="utf-8")
