"""Disk-based session persistence for the SOG chatbot.

Streamlit's st.session_state is in-memory only — it clears on every browser
refresh. This module persists the conversation context (last_run_id,
last_scenario_id, run_history) and chat messages to disk so users can
resume where they left off.

Sessions are stored in: phase2/.sog_sessions/{session_id}.json
They expire after SESSION_TTL_HOURS hours of inactivity.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SESSIONS_DIR = PROJECT_ROOT / "phase2" / ".sog_sessions"
SESSION_TTL_HOURS = 24


@dataclass
class SessionData:
    session_id: str
    context: dict[str, Any] = field(default_factory=lambda: {
        "last_run_id": None,
        "last_scenario_id": None,
        "run_history": [],
    })
    messages: list[dict[str, Any]] = field(default_factory=list)
    last_run_downloads: dict[str, str] = field(default_factory=dict)
    pending_job_id: str | None = None
    pending_charts: list[dict[str, Any]] = field(default_factory=list)
    updated_at: str = ""

    def __post_init__(self) -> None:
        self.context = {
            "last_run_id": None,
            "last_scenario_id": None,
            "run_history": [],
            **(self.context or {}),
        }
        self.messages = list(self.messages or [])
        self.last_run_downloads = dict(self.last_run_downloads or {})
        self.pending_charts = list(self.pending_charts or [])
        if not self.updated_at:
            self.updated_at = _now_utc()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_path(session_id: str) -> Path:
    return SESSIONS_DIR / f"{session_id}.json"


def load_session(session_id: str) -> SessionData:
    """Load session from disk, or return a fresh one if it doesn't exist."""
    path = _session_path(session_id)
    if not path.exists():
        return SessionData(session_id=session_id)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return SessionData(
            session_id=raw.get("session_id", session_id),
            context=raw.get("context", {}),
            messages=raw.get("messages", []),
            last_run_downloads=raw.get("last_run_downloads", {}),
            pending_job_id=raw.get("pending_job_id"),
            pending_charts=raw.get("pending_charts", []),
            updated_at=raw.get("updated_at", ""),
        )
    except Exception:
        return SessionData(session_id=session_id)


def save_session(data: SessionData) -> None:
    """Atomically persist session data to disk."""
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    data.updated_at = _now_utc()
    path = _session_path(data.session_id)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(asdict(data), indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def cleanup_expired_sessions(ttl_hours: int = SESSION_TTL_HOURS) -> int:
    """Remove session files older than ttl_hours. Returns count removed."""
    import time
    if not SESSIONS_DIR.exists():
        return 0
    cutoff = time.time() - (ttl_hours * 3600)
    removed = 0
    for f in SESSIONS_DIR.glob("*.json"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        except Exception:
            pass
    return removed
