"""
JSON loader: read JSON into DataFrame. Supports array of objects or nested paths.
"""

from pathlib import Path
from typing import Any

import pandas as pd


def load_json(path: Path, **kwargs: Any) -> pd.DataFrame:
    """
    Load JSON file into DataFrame. If top-level is a list of objects, uses it;
    if dict, uses first key that is a list or passes to pd.json_normalize.
    kwargs passed to pd.read_json or pd.json_normalize.
    """
    import json

    text = path.read_text(encoding="utf-8")
    data = json.loads(text)

    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return pd.DataFrame(data, **{k: v for k, v in kwargs.items() if k != "orient"})
    if isinstance(data, dict):
        # Single record or nested: normalize
        return pd.json_normalize(data, **kwargs)
    return pd.read_json(path, **kwargs)
