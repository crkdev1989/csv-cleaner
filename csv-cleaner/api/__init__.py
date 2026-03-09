"""
CSV Cleaner API — thin FastAPI layer over the existing cleaner engine.
Run with: uvicorn api.app:app --reload
"""

from api.app import app

__all__ = ["app"]
