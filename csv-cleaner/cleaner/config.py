"""
Config load and validation. Reads JSON config and normalizes structure
so the engine and pipeline have a consistent shape.
"""

import json
from pathlib import Path
from typing import Any


# Default config shape; keys expected by engine/pipeline.
DEFAULT_CONFIG = {
    "input": {"path": "", "format": None},
    "output": {"path": "", "format": "csv"},
    "report": {"path": None},
    "modules": [],
    "chunk_size": None,
}


def load_config(path: str | Path) -> dict[str, Any]:
    """
    Load JSON config from path. Normalizes and fills defaults;
    does not validate paths exist.
    """
    path = Path(path)
    if not path.suffix.lower() == ".json":
        raise ValueError(f"Config must be a JSON file: {path}")

    text = path.read_text(encoding="utf-8")
    data = json.loads(text)

    # Deep merge with defaults so missing keys get defaults
    config = _deep_merge(DEFAULT_CONFIG.copy(), data)

    # Resolve paths relative to config file directory
    base = path.parent.resolve()
    if config.get("input", {}).get("path"):
        config["input"]["path"] = str((base / config["input"]["path"]).resolve())
    if config.get("output", {}).get("path"):
        config["output"]["path"] = str((base / config["output"]["path"]).resolve())
    if config.get("report", {}).get("path"):
        config["report"]["path"] = str((base / config["report"]["path"]).resolve())

    return config


def _deep_merge(base: dict, override: dict) -> dict:
    """Merge override into base recursively. base is mutated."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def infer_format(path: str | Path) -> str:
    """Infer format from file extension. Returns one of: csv, xlsx, json, yaml."""
    p = Path(path)
    ext = p.suffix.lower()
    mapping = {".csv": "csv", ".xlsx": "xlsx", ".xls": "xlsx", ".json": "json", ".yaml": "yaml", ".yml": "yaml"}
    return mapping.get(ext, "csv")


def get_extension_for_format(fmt: str) -> str:
    """Return file extension for a format (e.g. csv -> .csv). Used for derived output filenames."""
    mapping = {"csv": ".csv", "xlsx": ".xlsx", "json": ".json", "yaml": ".yaml"}
    return mapping.get((fmt or "csv").lower(), ".csv")
