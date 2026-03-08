"""
FastAPI app: upload file + preset -> run cleaner -> return paths and stats.

Flow: POST /clean receives file + preset name -> save to runs/<job_id>/input/
-> build config (input path, output dir = runs/<job_id>/output/) -> run_cleaner(config_dict)
-> write summary file -> return output_path, report_path, summary_path, rows_loaded, rows_output, modules.
Download endpoints serve files under runs/ only (path traversal safe).
"""

import uuid
from datetime import datetime, timedelta
from pathlib import Path
import shutil

from fastapi import APIRouter, FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

# Project root (parent of api/)
ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = ROOT / "runs"
MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MB

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


@router.get("/health")
def health() -> dict:
    """Liveness check."""
    return {"status": "ok"}


@router.get("/presets")
def presets() -> dict:
    """List available preset names (from presets/ directory)."""
    names = _list_presets()
    return {"presets": names}


@router.post("/clean")
def clean(
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
