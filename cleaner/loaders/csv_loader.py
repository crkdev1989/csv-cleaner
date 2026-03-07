"""
CSV loader: read CSV into DataFrame or as chunked iterator.
"""

from pathlib import Path
from typing import Any, Iterator

import pandas as pd


def load_csv(path: Path, **kwargs: Any) -> pd.DataFrame:
    """Load full CSV into DataFrame. kwargs passed to pandas read_csv."""
    return pd.read_csv(path, **kwargs)


def load_csv_chunked(
    path: Path,
    chunk_size: int,
    **kwargs: Any,
) -> Iterator[pd.DataFrame]:
    """Read CSV in chunks. kwargs passed to pandas read_csv."""
    for chunk in pd.read_csv(path, chunksize=chunk_size, **kwargs):
        yield chunk
