"""
XLSX loader: read first sheet into DataFrame. No chunking (pandas loads full sheet).
"""

from pathlib import Path
from typing import Any

import pandas as pd


def load_xlsx(path: Path, **kwargs: Any) -> pd.DataFrame:
    """Load first sheet of XLSX into DataFrame. kwargs passed to read_excel."""
    return pd.read_excel(path, engine="openpyxl", **kwargs)
