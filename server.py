"""
Simple single-process web app for the audit pipeline.

No database, no auth, no Docker, no job queue - one FastAPI process, one
background thread at a time, file-based state (same approach as the
original CLI scripts). Run with:

    uvicorn server:app --reload

Then open http://localhost:8000
"""
import json
import os
import threading
import traceback
import uuid
from datetime import datetime, timezone

import openpyxl
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from pipeline.fetch import run_fetch_phase
from pipeline.validate import run_validate_phase
from pipeline.writeback import run_writeback_phase

ROOT = os.path.dirname(os.path.abspath(__file__))
# Overridable so a throwaway test run never touches real uploads/jobs/settings.
STORAGE_DIR = os.environ.get("STORAGE_ROOT") or os.path.join(ROOT, "storage")
UPLOADS_DIR = os.path.join(STORAGE_DIR, "uploads")
CACHE_DIR = os.path.join(STORAGE_DIR, "cache")  # shared across every job - never re-fetched once cached
JOBS_DIR = os.path.join(STORAGE_DIR, "jobs")
SETTINGS_PATH = os.path.join(STORAGE_DIR, "settings.json")
for _d in (UPLOADS_DIR, CACHE_DIR, JOBS_DIR):
    os.makedirs(_d, exist_ok=True)

app = FastAPI(title="Audit Pipeline")
app.mount("/static", StaticFiles(directory=os.path.join(ROOT, "static")), name="static")

JOBS: dict[str, dict] = {}
_jobs_lock = threading.Lock()
_run_lock = threading.Lock()  # one pipeline run at a time, keeps resource usage predictable

# Fields that get persisted to each job's job.json (everything except the
# private, in-memory-only "_..." keys, which never touch disk).
_PERSISTED_JOB_FIELDS = (
    "id", "filename", "job_dir", "upload_path", "sheets", "sheet", "row_count",
    "pilot_size", "status", "phase", "log", "error", "stats_pilot", "stats_full",
    "pilot_path", "full_path", "manifests_dir", "results_dir", "created_at",
    "feedback_history",
)


def _combined_feedback(job: dict) -> str | None:
    """Every round of pilot-review feedback the user has given so far,
    threaded into later validate calls (both further pilot revisions AND the
    eventual full run) so the full dataset gets the same adjusted behavior
    the user actually approved, not the original unrefined rules."""
    history = job.get("feedback_history") or []
    if not history:
        return None
    if len(history) == 1:
        return history[0]
    return "\n\n".join(f"(Round {i + 1} feedback) {text}" for i, text in enumerate(history))


def _log(job_id: str, message: str) -> None:
    with _jobs_lock:
        job = JOBS.get(job_id)
        if job is None:
            return
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        job["log"].append(f"[{ts}] {message}")
        job["log"] = job["log"][-500:]


def _fail_job(job: dict, job_id: str, label: str, exc: Exception) -> None:
    """Marks a job failed with the FULL traceback, not just str(exc) - some
    exceptions stringify to an empty/uninformative message, which used to
    surface as a bare "Unknown error" in the UI with no way to diagnose it.
    Also fixes a real race: error is written before status flips to
    "failed", so a poll landing between the two can never see a failed
    status paired with a still-empty error message."""
    detail = traceback.format_exc()
    print(f"[{job_id}] {label} failed:\n{detail}")
    job["error"] = f"{exc}" if str(exc) else f"{type(exc).__name__} (see server log for the full traceback)"
    job["status"] = "failed"
    _log(job_id, f"{label} failed: {job['error']}")


def _save_job_meta(job: dict) -> None:
    """Persists everything about a job except its API keys, so job history
    (filename, status, stats, download availability) survives a restart."""
    data = {k: job.get(k) for k in _PERSISTED_JOB_FIELDS}
    path = os.path.join(job["job_dir"], "job.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1)
    os.replace(tmp, path)


def _load_job_history() -> None:
    """Called once at startup - rebuilds JOBS from each job's persisted
    job.json so /api/jobs (History) still shows past runs after a restart.
    A job that was actively running when the server stopped can't be
    resumed (its background thread is gone), so it's marked failed."""
    if not os.path.isdir(JOBS_DIR):
        return
    for job_id in os.listdir(JOBS_DIR):
        meta_path = os.path.join(JOBS_DIR, job_id, "job.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path, "r", encoding="utf-8") as f:
            job = json.load(f)
        job.setdefault("feedback_history", [])  # older jobs predate this field
        if job.get("status") in ("pilot_running", "full_running"):
            job["status"] = "failed"
            job["phase"] = None
            job["error"] = "Interrupted - the server was restarted while this was running. Please start again."
        job["_gemini_key"] = None
        job["_scrape_do_key"] = None
        job["_pilot_keys"] = None
        JOBS[job_id] = job


def load_settings() -> dict:
    if os.path.exists(SETTINGS_PATH):
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"scrape_do_key": None, "gemini_key": None}


def save_settings(scrape_do_key: str | None, gemini_key: str | None) -> None:
    tmp = SETTINGS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"scrape_do_key": scrape_do_key, "gemini_key": gemini_key}, f, indent=1)
    os.replace(tmp, SETTINGS_PATH)


_load_job_history()


@app.get("/")
def index():
    return FileResponse(os.path.join(ROOT, "static", "index.html"))


class SettingsRequest(BaseModel):
    scrape_do_key: str | None = None
    gemini_key: str | None = None


@app.get("/api/settings")
def get_settings():
    return load_settings()


@app.post("/api/settings")
def update_settings(body: SettingsRequest):
    save_settings(body.scrape_do_key or None, body.gemini_key or None)
    return {"status": "saved"}


@app.post("/api/upload")
async def upload(file: UploadFile):
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(400, "Only .xlsx files are supported")

    job_id = uuid.uuid4().hex[:12]
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    upload_path = os.path.join(job_dir, "input.xlsx")

    content = await file.read()
    with open(upload_path, "wb") as f:
        f.write(content)

    try:
        wb = openpyxl.load_workbook(upload_path, read_only=True, data_only=True)
    except Exception as e:
        raise HTTPException(400, f"Could not read workbook: {e}")

    sheets = wb.sheetnames
    ws = wb[sheets[0]]
    row_count = sum(1 for row in ws.iter_rows(min_row=2, values_only=True) if row[0] is not None)

    with _jobs_lock:
        job = {
            "id": job_id,
            "filename": file.filename,
            "job_dir": job_dir,
            "upload_path": upload_path,
            "sheets": sheets,
            "sheet": sheets[0],
            "row_count": row_count,
            "pilot_size": min(100, row_count),
            "status": "uploaded",
            "phase": None,
            "log": [],
            "error": None,
            "stats_pilot": None,
            "stats_full": None,
            "pilot_path": None,
            "full_path": None,
            "manifests_dir": os.path.join(job_dir, "manifests"),
            "results_dir": os.path.join(job_dir, "results"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "feedback_history": [],
            "_gemini_key": None,
            "_scrape_do_key": None,
            "_pilot_keys": None,
        }
        JOBS[job_id] = job
    _save_job_meta(job)

    return {"job_id": job_id, "filename": file.filename, "sheets": sheets, "row_count": row_count,
            "suggested_pilot_size": min(100, row_count)}


class PilotRequest(BaseModel):
    sheet: str
    pilot_size: int = 100
    scrape_do_key: str | None = None
    gemini_key: str


@app.post("/api/jobs/{job_id}/pilot")
def start_pilot(job_id: str, body: PilotRequest):
    with _jobs_lock:
        job = JOBS.get(job_id)
        if job is None:
            raise HTTPException(404, "Job not found")
        if job["status"] != "uploaded":
            raise HTTPException(400, f"Job already started (status={job['status']})")
        if _run_lock.locked():
            raise HTTPException(409, "Another run is already in progress - please wait for it to finish")
        job["sheet"] = body.sheet
        job["pilot_size"] = body.pilot_size
        job["_gemini_key"] = body.gemini_key
        job["_scrape_do_key"] = body.scrape_do_key or None
        job["status"] = "pilot_running"

    # Remember whatever keys were used so they're pre-filled next time -
    # this is what makes a saved Scrape.do key "just work" without having to
    # open Settings again for every new file.
    save_settings(body.scrape_do_key or None, body.gemini_key)
    _save_job_meta(job)

    threading.Thread(target=_run_pilot, args=(job_id,), daemon=True).start()
    return {"status": "started"}


def _run_pilot(job_id: str) -> None:
    with _run_lock:
        job = JOBS[job_id]
        try:
            def pilot_filter(product_key, ordinal):
                return ordinal < job["pilot_size"]

            job["phase"] = "fetch"
            run_fetch_phase(
                workbook_path=job["upload_path"], sheet=job["sheet"], product_filter=pilot_filter,
                manifests_dir=job["manifests_dir"], cache_dir=CACHE_DIR,
                scrape_do_key=job["_scrape_do_key"], log=lambda m: _log(job_id, m),
            )

            pilot_keys = {
                os.path.splitext(f)[0] for f in os.listdir(job["manifests_dir"]) if f.endswith(".json")
            }
            job["_pilot_keys"] = pilot_keys

            job["phase"] = "validate"
            run_validate_phase(
                manifests_dir=job["manifests_dir"], results_dir=job["results_dir"],
                product_filter=lambda k, i: k in pilot_keys, gemini_key=job["_gemini_key"],
                cache_dir=CACHE_DIR, log=lambda m: _log(job_id, m),
                extra_instructions=_combined_feedback(job),
            )

            job["phase"] = "writeback"
            stats = run_writeback_phase(
                input_path=job["upload_path"],
                output_path=os.path.join(job["job_dir"], "pilot_output.xlsx"),
                results_dir=job["results_dir"], manifests_dir=job["manifests_dir"], sheet=job["sheet"],
                product_keys=pilot_keys, log=lambda m: _log(job_id, m),
            )

            job["stats_pilot"] = stats
            job["pilot_path"] = os.path.join(job["job_dir"], "pilot_output.xlsx")
            job["status"] = "awaiting_approval"
            job["phase"] = None
            _log(job_id, "Pilot complete - review the download, then approve or leave feedback to refine it")
        except Exception as e:
            _fail_job(job, job_id, "Pilot", e)
        finally:
            _save_job_meta(job)


class FeedbackRequest(BaseModel):
    feedback: str
    scrape_do_key: str | None = None
    gemini_key: str


@app.post("/api/jobs/{job_id}/pilot/revise")
def revise_pilot(job_id: str, body: FeedbackRequest):
    feedback = body.feedback.strip()
    if not feedback:
        raise HTTPException(400, "Feedback text is required")

    with _jobs_lock:
        job = JOBS.get(job_id)
        if job is None:
            raise HTTPException(404, "Job not found")
        if job["status"] != "awaiting_approval":
            raise HTTPException(400, f"Job is not awaiting approval (status={job['status']})")
        if _run_lock.locked():
            raise HTTPException(409, "Another run is already in progress - please wait for it to finish")
        job.setdefault("feedback_history", []).append(feedback)
        job["_gemini_key"] = body.gemini_key
        job["_scrape_do_key"] = body.scrape_do_key or None
        job["status"] = "pilot_running"

    save_settings(body.scrape_do_key or None, body.gemini_key)
    _save_job_meta(job)

    threading.Thread(target=_run_pilot_revision, args=(job_id,), daemon=True).start()
    return {"status": "started"}


def _run_pilot_revision(job_id: str) -> None:
    with _run_lock:
        job = JOBS[job_id]
        try:
            # Recomputed from disk rather than trusted from an earlier
            # in-memory value - the pilot fetch phase only ever wrote
            # manifests for the pilot subset, so this is exactly the pilot's
            # product set regardless of whether the server restarted in
            # between (in-memory-only fields don't survive that).
            pilot_keys = {
                os.path.splitext(f)[0] for f in os.listdir(job["manifests_dir"]) if f.endswith(".json")
            }
            _log(job_id, f"Re-running the pilot on the same {len(pilot_keys)} products with your feedback")

            # Drop the previous validation result for just these products so
            # they're genuinely re-validated instead of skipped as "already done".
            for key in pilot_keys:
                result_path = os.path.join(job["results_dir"], f"{key}.json")
                if os.path.exists(result_path):
                    os.remove(result_path)

            job["phase"] = "validate"
            run_validate_phase(
                manifests_dir=job["manifests_dir"], results_dir=job["results_dir"],
                product_filter=lambda k, i: k in pilot_keys, gemini_key=job["_gemini_key"],
                cache_dir=CACHE_DIR, log=lambda m: _log(job_id, m),
                extra_instructions=_combined_feedback(job),
            )

            job["phase"] = "writeback"
            stats = run_writeback_phase(
                input_path=job["upload_path"],
                output_path=os.path.join(job["job_dir"], "pilot_output.xlsx"),
                results_dir=job["results_dir"], manifests_dir=job["manifests_dir"], sheet=job["sheet"],
                product_keys=pilot_keys, log=lambda m: _log(job_id, m),
            )

            job["stats_pilot"] = stats
            job["pilot_path"] = os.path.join(job["job_dir"], "pilot_output.xlsx")
            job["status"] = "awaiting_approval"
            job["phase"] = None
            _log(job_id, "Revised pilot complete - review the download again, approve, or leave more feedback")
        except Exception as e:
            _fail_job(job, job_id, "Pilot revision", e)
        finally:
            _save_job_meta(job)


@app.post("/api/jobs/{job_id}/approve")
def approve(job_id: str):
    with _jobs_lock:
        job = JOBS.get(job_id)
        if job is None:
            raise HTTPException(404, "Job not found")
        if job["status"] != "awaiting_approval":
            raise HTTPException(400, f"Job is not awaiting approval (status={job['status']})")
        if _run_lock.locked():
            raise HTTPException(409, "Another run is already in progress - please wait for it to finish")
        job["status"] = "full_running"

    threading.Thread(target=_run_full, args=(job_id,), daemon=True).start()
    return {"status": "started"}


def _run_full(job_id: str) -> None:
    with _run_lock:
        job = JOBS[job_id]
        try:
            def all_filter(product_key, ordinal):
                return True

            job["phase"] = "fetch"
            run_fetch_phase(
                workbook_path=job["upload_path"], sheet=job["sheet"], product_filter=all_filter,
                manifests_dir=job["manifests_dir"], cache_dir=CACHE_DIR,
                scrape_do_key=job["_scrape_do_key"], log=lambda m: _log(job_id, m),
            )

            job["phase"] = "validate"
            run_validate_phase(
                manifests_dir=job["manifests_dir"], results_dir=job["results_dir"],
                product_filter=all_filter, gemini_key=job["_gemini_key"],
                cache_dir=CACHE_DIR, log=lambda m: _log(job_id, m),
                extra_instructions=_combined_feedback(job),
            )

            job["phase"] = "writeback"
            stats = run_writeback_phase(
                input_path=job["upload_path"],
                output_path=os.path.join(job["job_dir"], "full_output.xlsx"),
                results_dir=job["results_dir"], manifests_dir=job["manifests_dir"], sheet=job["sheet"],
                product_keys=None, log=lambda m: _log(job_id, m),
            )

            job["stats_full"] = stats
            job["full_path"] = os.path.join(job["job_dir"], "full_output.xlsx")
            job["status"] = "completed"
            job["phase"] = None
            _log(job_id, "Full run complete")
        except Exception as e:
            _fail_job(job, job_id, "Full run", e)
        finally:
            job["_gemini_key"] = None
            job["_scrape_do_key"] = None
            _save_job_meta(job)


def _job_summary(job: dict) -> dict:
    return {
        "id": job["id"], "filename": job["filename"], "status": job["status"],
        "created_at": job.get("created_at"), "row_count": job.get("row_count"),
        "stats_pilot": job.get("stats_pilot"), "stats_full": job.get("stats_full"),
        "has_pilot_output": bool(job.get("pilot_path")) and os.path.exists(job["pilot_path"] or ""),
        "has_full_output": bool(job.get("full_path")) and os.path.exists(job["full_path"] or ""),
    }


@app.get("/api/jobs")
def list_jobs():
    jobs = sorted(JOBS.values(), key=lambda j: j.get("created_at") or "", reverse=True)
    return [_job_summary(j) for j in jobs]


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    job = JOBS.get(job_id)
    if job is None:
        raise HTTPException(404, "Job not found")
    return {
        "id": job["id"], "filename": job["filename"], "sheets": job["sheets"], "sheet": job["sheet"],
        "row_count": job["row_count"], "pilot_size": job["pilot_size"],
        "status": job["status"], "phase": job["phase"], "error": job["error"],
        "stats_pilot": job["stats_pilot"], "stats_full": job["stats_full"],
        "has_pilot_output": job["pilot_path"] is not None,
        "has_full_output": job["full_path"] is not None,
        "log": job["log"][-100:],
        "feedback_history": job.get("feedback_history") or [],
    }


@app.get("/api/jobs/{job_id}/pilot/download")
def download_pilot(job_id: str):
    job = JOBS.get(job_id)
    if job is None or not job["pilot_path"] or not os.path.exists(job["pilot_path"]):
        raise HTTPException(404, "Pilot output not available yet")
    return FileResponse(job["pilot_path"], filename=f"pilot_{job['filename']}")


@app.get("/api/jobs/{job_id}/full/download")
def download_full(job_id: str):
    job = JOBS.get(job_id)
    if job is None or not job["full_path"] or not os.path.exists(job["full_path"]):
        raise HTTPException(404, "Full output not available yet")
    return FileResponse(job["full_path"], filename=f"audited_{job['filename']}")
