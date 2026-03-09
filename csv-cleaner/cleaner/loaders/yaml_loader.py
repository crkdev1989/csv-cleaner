"""
YAML loader: read YAML into DataFrame. Expects list of objects or single object.
"""

from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def load_yaml(path: Path, **kwargs: Any) -> pd.DataFrame:
    """
    Load YAML file into DataFrame. Expects list of dicts or single dict.
    kwargs not used for YAML parse but could be passed to DataFrame constructor.
    """
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text)

    if data is None:
        return pd.DataFrame()

    if isinstance(data, list):
        if len(data) == 0:
            return pd.DataFrame()
        if isinstance(data[0], dict):
            return pd.DataFrame(data)
        return pd.DataFrame(data)

    if isinstance(data, dict):
        return pd.json_normalize(data)

    return pd.DataFrame([data])
