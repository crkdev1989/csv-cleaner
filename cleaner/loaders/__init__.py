"""
Input loaders: read CSV, XLSX, JSON, YAML and normalize to pandas DataFrame.
"""

from cleaner.loaders.factory import load_data, load_data_chunked

__all__ = ["load_data", "load_data_chunked"]
