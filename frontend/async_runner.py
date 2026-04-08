"""Background job execution for SOG pipeline runs.

Jobs are persisted to disk in phase2/.sog_jobs/{job_id}.json so they survive
Streamlit reruns. The background thread ONLY writes to JSON files — it never
touches st.session_state, which is unsafe from non-main threads.
"""

from __future__ import annotations

import json
import sys
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = PROJECT_ROOT / "phase2" / ".sog_jobs"

if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))
if str(PROJECT_ROOT / "frontend") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "frontend"))


@dataclass
class JobState:
    job_id: str
    status: str           # pending | running | completed | failed
    scenario_id: str
    scenario_yaml_path: str
    run_id: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    progress_percent: int = 0
    current_stage: str = ""   # starting | selection | truth | emission | quality | complete
    stages_completed: list[str] = field(default_factory=list)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_state(state: JobState) -> None:
    """Atomic write: write to .tmp then os.replace() — safe against thread interruption."""
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    job_file = JOBS_DIR / f"{state.job_id}.json"
    tmp_file = job_file.with_suffix(".tmp")
    tmp_file.write_text(json.dumps(asdict(state), indent=2), encoding="utf-8")
    # os.replace is atomic on POSIX and Windows (same-filesystem rename)
    import os as _os
    _os.replace(tmp_file, job_file)


def _read_state(job_id: str) -> JobState | None:
    """Read with retry to handle the brief window during atomic rename."""
    job_file = JOBS_DIR / f"{job_id}.json"
    for _attempt in range(3):
        if not job_file.exists():
            return None
        try:
            data = json.loads(job_file.read_text(encoding="utf-8"))
            return JobState(**data)
        except (json.JSONDecodeError, TypeError):
            # File may have been partially written; retry once
            import time as _time
            _time.sleep(0.05)
        except Exception:
            return None
    return None


def _run_pipeline_thread(job_id: str, scenario_yaml_path: Path, overwrite: bool) -> None:
    """Background thread — calls the pipeline and writes status to disk."""

    def _update(stage: str, pct: int) -> None:
        state = _read_state(job_id)
        if state is None:
            return
        state.status = "running"
        state.current_stage = stage
        state.progress_percent = pct
        if stage not in state.stages_completed:
            state.stages_completed.append(stage)
        _write_state(state)

    try:
        from pipeline_bridge import run_pipeline_sync

        _update("starting", 5)
        result = run_pipeline_sync(
            scenario_yaml_path=scenario_yaml_path,
            overwrite=overwrite,
        )
        state = _read_state(job_id)
        if state:
            state.status = "completed"
            state.run_id = result.get("run_id")
            state.progress_percent = 100
            state.current_stage = "complete"
            state.completed_at = _now_utc()
            _write_state(state)

    except FileExistsError as exc:
        state = _read_state(job_id)
        if state:
            state.status = "failed"
            state.error = f"Run already exists. Use overwrite=True to replace. ({exc})"
            state.completed_at = _now_utc()
            _write_state(state)

    except Exception as exc:
        state = _read_state(job_id)
        if state:
            state.status = "failed"
            state.error = str(exc)
            state.completed_at = _now_utc()
            _write_state(state)


def submit_run(
    scenario_yaml_path: Path,
    scenario_id: str,
    overwrite: bool = False,
) -> str:
    """Submit a pipeline run as a background job. Returns job_id immediately."""
    job_id = f"{scenario_id}_{uuid.uuid4().hex[:8]}"
    state = JobState(
        job_id=job_id,
        status="pending",
        scenario_id=scenario_id,
        scenario_yaml_path=str(scenario_yaml_path),
        started_at=_now_utc(),
        stages_completed=[],
    )
    _write_state(state)

    thread = threading.Thread(
        target=_run_pipeline_thread,
        args=(job_id, scenario_yaml_path, overwrite),
        daemon=True,
    )
    thread.start()
    return job_id


def poll_status(job_id: str) -> dict[str, Any]:
    """Return current job status dict."""
    state = _read_state(job_id)
    if state is None:
        return {"status": "unknown", "error": f"Job {job_id} not found"}

    result: dict[str, Any] = {
        "job_id": state.job_id,
        "status": state.status,
        "scenario_id": state.scenario_id,
        "progress_percent": state.progress_percent,
        "current_stage": state.current_stage,
    }
    if state.run_id:
        result["run_id"] = state.run_id
    if state.error:
        result["error"] = state.error
    if state.started_at:
        result["started_at"] = state.started_at
    if state.completed_at:
        result["completed_at"] = state.completed_at
    return result


def list_jobs(limit: int = 10) -> list[dict[str, Any]]:
    """List recent jobs sorted by modification time, newest first."""
    if not JOBS_DIR.exists():
        return []
    jobs = []
    paths = sorted(JOBS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in paths[:limit]:
        try:
            state = _read_state(p.stem)
            if state:
                jobs.append({
                    "job_id": state.job_id,
                    "scenario_id": state.scenario_id,
                    "status": state.status,
                    "run_id": state.run_id,
                    "started_at": state.started_at,
                })
        except Exception:
            pass
    return jobs


def cleanup_old_jobs(max_age_hours: int = 24) -> int:
    """Remove job files older than max_age_hours. Returns count removed."""
    import time
    if not JOBS_DIR.exists():
        return 0
    cutoff = time.time() - (max_age_hours * 3600)
    removed = 0
    for job_file in JOBS_DIR.glob("*.json"):
        try:
            if job_file.stat().st_mtime < cutoff:
                job_file.unlink()
                removed += 1
        except Exception:
            pass
    return removed
