"""
Loader factory: dispatch by format and return a single DataFrame or chunked iterator.
All loaders normalize to pandas DataFrame.
"""

from pathlib import Path
from typing import Iterator

import pandas as pd

from cleaner.loaders.csv_loader import load_csv, load_csv_chunked
from cleaner.loaders.xlsx_loader import load_xlsx
from cleaner.loaders.json_loader import load_json
from cleaner.loaders.yaml_loader import load_yaml


def load_data(
    path: str | Path,
    format: str | None = None,
    **kwargs: object,
) -> pd.DataFrame:
    """
    Load file at path into a single DataFrame.
    format: csv | xlsx | json | yaml. If None, inferred from extension.
    """
    path = Path(path)
    fmt = (format or _infer_format(path)).lower()

    if fmt == "csv":
        return load_csv(path, **kwargs)
    if fmt == "xlsx":
        return load_xlsx(path, **kwargs)
    if fmt == "json":
        return load_json(path, **kwargs)
    if fmt == "yaml":
        return load_yaml(path, **kwargs)

    raise ValueError(f"Unsupported format: {format}")


def load_data_chunked(
    path: str | Path,
    chunk_size: int,
    format: str | None = None,
    **kwargs: object,
) -> Iterator[pd.DataFrame]:
    """
    Load file in chunks. Only CSV is chunked; other formats load fully.
    """
    path = Path(path)
    fmt = (format or _infer_format(path)).lower()

    if fmt == "csv":
        yield from load_csv_chunked(path, chunk_size=chunk_size, **kwargs)
    else:
        # Non-csv: load once and yield single chunk
        yield load_data(path, format=format, **kwargs)


def _infer_format(path: Path) -> str:
    ext = path.suffix.lower()
    mapping = {".csv": "csv", ".xlsx": "xlsx", ".xls": "xlsx", ".json": "json", ".yaml": "yaml", ".yml": "yaml"}
    return mapping.get(ext, "csv")
