"""
FastAPI app: upload file + preset -> run cleaner -> return paths and stats.

Flow: POST /clean receives file + preset name -> save to runs/<job_id>/input/
-> build config (input path, output dir = runs/<job_id>/output/) -> run_cleaner(config_dict)
-> write summary file -> return output_path, report_path, summary_path, rows_loaded, rows_output, modules.
Download endpoints serve files under runs/ only (path traversal safe).
"""

import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import shutil

from fastapi import APIRouter, FastAPI, File, Form, HTTPException, Query, UploadFile, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

# Project root (parent of api/)
ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = ROOT / "runs"
MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MB
STATS_PATH = ROOT / "data" / "stats.json"

# Reuse existing cleaner and CLI preset logic
from cleaner.cli import _list_presets, _write_summary_file, load_preset
from cleaner.engine import run_cleaner

# Build app and router
app = FastAPI(
    title="CSV Cleaner API",
    description="Upload a file, choose a preset, run the cleaner, download results.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://csv.crkdev.com",
        "https://crkdev.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
router = APIRouter()

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


def cleanup_old_runs() -> None:
    """Remove job folders under runs/ older than 24 hours. Ignores errors."""
    if not RUNS_DIR.is_dir():
        return
    threshold = (datetime.now() - timedelta(hours=24)).timestamp()
    for p in RUNS_DIR.iterdir():
        if not p.is_dir():
            continue
        if p.stat().st_mtime < threshold:
            try:
                shutil.rmtree(p)
            except Exception:
                pass


def _ensure_runs_dir() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)


def _path_under_runs(path: str | Path) -> Path:
    """Resolve path and require it to be under RUNS_DIR. Raise 400 if not."""
    resolved = Path(path).resolve()
    try:
        resolved.relative_to(RUNS_DIR)
    except ValueError:
        raise HTTPException(status_code=400, detail="Path must be under runs/")
    if not resolved.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return resolved


def _read_stats() -> dict:
    """Read persistent stats from data/stats.json. Return zeros if missing or invalid."""
    if not STATS_PATH.is_file():
        return {"files_cleaned": 0, "rows_processed": 0}
    try:
        data = json.loads(STATS_PATH.read_text(encoding="utf-8"))
        return {
            "files_cleaned": int(data.get("files_cleaned", 0)),
            "rows_processed": int(data.get("rows_processed", 0)),
        }
    except (json.JSONDecodeError, OSError):
        return {"files_cleaned": 0, "rows_processed": 0}


def _write_stats(stats: dict) -> None:
    """Write stats to data/stats.json using an atomic write (temp file then replace)."""
    STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = STATS_PATH.with_suffix(STATS_PATH.suffix + ".tmp")
    temp_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    temp_path.replace(STATS_PATH)


def update_stats_on_clean(rows_output: int) -> None:
    """Increment files_cleaned by 1 and rows_processed by rows_output, then persist."""
    stats = _read_stats()
    stats["files_cleaned"] += 1
    stats["rows_processed"] += rows_output
    _write_stats(stats)


@router.get("/health")
def health() -> dict:
    """Liveness check."""
    return {"status": "ok"}


@router.get("/presets")
def presets() -> dict:
    """List available preset names (from presets/ directory)."""
    names = _list_presets()
    return {"presets": names}


@router.get("/stats")
def stats() -> dict:
    """Return persistent usage counters (files_cleaned, rows_processed). Returns zeros if stats missing or invalid."""
    return _read_stats()


@limiter.limit("5/minute")
@router.post("/clean")
def clean(
    request: Request,
    file: UploadFile = File(...),
    preset: str = Form(...),
) -> dict:
    """
    Upload a file and run the cleaner with the given preset.
    Saves to runs/<job_id>/input/ and writes outputs to runs/<job_id>/output/.
    Returns paths and report stats.
    """
    cleanup_old_runs()

    if not file.filename or not file.filename.strip():
        raise HTTPException(status_code=400, detail="Upload a file (filename required)")

    try:
        preset_dict = load_preset(preset.strip())
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Read file and check size before saving to disk
    try:
        content = file.file.read()
    finally:
        file.file.close()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail="File too large. Maximum size is 10MB.",
        )

    job_id = str(uuid.uuid4())
    input_dir = RUNS_DIR / job_id / "input"
    output_dir = RUNS_DIR / job_id / "output"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save uploaded file
    input_path = input_dir / (file.filename or "uploaded")
    input_path.write_bytes(content)

    if not input_path.is_file():
        raise HTTPException(status_code=500, detail="Failed to save upload")

    # Build config: input = saved file, output/report = output_dir (engine derives stem_cleaned.csv etc.)
    config = {
        "input": {"path": str(input_path), "format": None},
        "output": {"path": str(output_dir / "cleaned.csv"), "format": "csv"},
        "report": {"path": str(output_dir / "report.json")},
        "pipeline": preset_dict.get("pipeline", preset_dict.get("modules", [])),
        "chunk_size": preset_dict.get("chunk_size"),
    }

    try:
        report = run_cleaner(config_dict=config)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleaning failed: {str(e)}")

    update_stats_on_clean(report.rows_output)

    summary_path = _write_summary_file(report)

    return {
        "job_id": job_id,
        "output_path": report.output_path,
        "report_path": report.report_path,
        "summary_path": summary_path,
        "rows_loaded": report.rows_loaded,
        "rows_output": report.rows_output,
        "modules": report.modules_executed,
    }


@router.get("/download/cleaned")
def download_cleaned(path: str = Query(..., description="Path from /clean response (under runs/)")) -> FileResponse:
    """Download the cleaned file. path must be under runs/ (e.g. from /clean response)."""
    _ensure_runs_dir()
    safe_path = _path_under_runs(path)
    return FileResponse(safe_path, filename=safe_path.name)


@router.get("/download/report")
def download_report(path: str = Query(..., description="Report path under runs/")) -> FileResponse:
    """Download the report JSON. path must be under runs/."""
    _ensure_runs_dir()
    safe_path = _path_under_runs(path)
    return FileResponse(safe_path, filename=safe_path.name, media_type="application/json")


@router.get("/download/summary")
def download_summary(path: str = Query(..., description="Summary path under runs/")) -> FileResponse:
    """Download the summary text file. path must be under runs/."""
    _ensure_runs_dir()
    safe_path = _path_under_runs(path)
    return FileResponse(safe_path, filename=safe_path.name, media_type="text/plain")


app.include_router(router)
