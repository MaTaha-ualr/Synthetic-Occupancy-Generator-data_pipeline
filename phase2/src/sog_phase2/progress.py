"""Small stderr progress UI for Phase-2 command line scripts."""

from __future__ import annotations

import os
import sys
import threading
import time
from contextlib import AbstractContextManager
from types import TracebackType
from typing import TextIO


_DISABLED_VALUES = {"0", "false", "no", "off"}
_SPINNER = "|/-\\"


class ProgressReporter:
    """Render a stage progress bar without adding a runtime dependency.

    The reporter writes to stderr by default so stdout can remain machine-readable
    JSON for scripts that are consumed by other tools.
    """

    def __init__(
        self,
        *,
        total: int,
        label: str,
        enabled: bool = True,
        stream: TextIO | None = None,
        width: int = 24,
        refresh_seconds: float = 0.25,
    ) -> None:
        env_enabled = os.environ.get("SOG_PROGRESS", "1").strip().lower() not in _DISABLED_VALUES
        self.enabled = bool(enabled and env_enabled and total > 0)
        self.total = max(1, int(total))
        self.label = label
        self.stream = stream or sys.stderr
        self.width = max(10, int(width))
        self.refresh_seconds = max(0.05, float(refresh_seconds))

        self._completed = 0
        self._active_message = ""
        self._active_started_at = 0.0
        self._spinner_index = 0
        self._last_line_length = 0
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._dynamic = bool(self.enabled and hasattr(self.stream, "isatty") and self.stream.isatty())

    def step(self, message: str) -> "_ProgressStep":
        return _ProgressStep(self, message)

    def _start(self, message: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._active_message = message
            self._active_started_at = time.monotonic()
            self._spinner_index = 0
        if self._dynamic:
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._spin, daemon=True)
            self._thread.start()
            self._render(status="running")
        else:
            self.stream.write(f"{self.label}: [{self._completed + 1}/{self.total}] {message} ...\n")
            self.stream.flush()

    def _finish(self, *, failed: bool = False) -> None:
        if not self.enabled:
            if not failed:
                self._completed = min(self.total, self._completed + 1)
            return

        elapsed = max(0.0, time.monotonic() - self._active_started_at)
        if self._dynamic:
            self._stop_event.set()
            if self._thread is not None:
                self._thread.join(timeout=self.refresh_seconds * 2)
            if not failed:
                self._completed = min(self.total, self._completed + 1)
            self._render(status="failed" if failed else "done", elapsed=elapsed, final=True)
            self.stream.write("\n")
            self.stream.flush()
        else:
            if not failed:
                self._completed = min(self.total, self._completed + 1)
            status = "failed" if failed else "done"
            self.stream.write(
                f"{self.label}: [{self._completed}/{self.total}] "
                f"{self._active_message} {status} in {elapsed:.1f}s\n"
            )
            self.stream.flush()

        self._thread = None
        self._active_message = ""

    def _spin(self) -> None:
        while not self._stop_event.wait(self.refresh_seconds):
            self._render(status="running")

    def _render(self, *, status: str, elapsed: float | None = None, final: bool = False) -> None:
        with self._lock:
            message = self._active_message
            started_at = self._active_started_at
            spinner_index = self._spinner_index
            if not final:
                self._spinner_index = (self._spinner_index + 1) % len(_SPINNER)

        active_count = self._completed if final else min(self.total, self._completed + 1)
        filled = int(round((active_count / self.total) * self.width))
        bar = "#" * filled + "-" * (self.width - filled)
        symbol = " " if final else _SPINNER[spinner_index]
        if elapsed is None:
            elapsed = max(0.0, time.monotonic() - started_at)
        line = (
            f"\r{self.label} [{bar}] {active_count}/{self.total} "
            f"{symbol} {message} {status} ({elapsed:.1f}s)"
        )
        padding = " " * max(0, self._last_line_length - len(line))
        self.stream.write(line + padding)
        self.stream.flush()
        self._last_line_length = len(line)


class _ProgressStep(AbstractContextManager["_ProgressStep"]):
    def __init__(self, reporter: ProgressReporter, message: str) -> None:
        self.reporter = reporter
        self.message = message

    def __enter__(self) -> "_ProgressStep":
        self.reporter._start(self.message)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        self.reporter._finish(failed=exc_type is not None)
        return False
