"""Base class and shared utilities for all SOG agents."""

from __future__ import annotations

import json
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))
if str(PROJECT_ROOT / "frontend") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "frontend"))


@dataclass
class AgentResponse:
    """Standard response format for all SOG agents."""
    success: bool
    message: str                                    # Terse user-facing text
    data: dict[str, Any] = field(default_factory=dict)          # Structured downstream data
    session_updates: dict[str, Any] = field(default_factory=dict)  # Writes to session_state.context
    pending_job_id: str | None = None               # Set when async run was submitted
    charts: list[dict[str, Any]] = field(default_factory=list)  # [{path, insight, chart_type}]


class BaseAgent(ABC):
    """Base class for all SOG specialist agents."""

    def __init__(self, model: str, api_key: str | None = None):
        self.model = model
        self._api_key = api_key
        self._client = None

    def _get_client(self):
        """Lazy-initialize Anthropic client so we don't crash on missing key at import."""
        if self._client is None:
            import anthropic
            key = self._api_key or os.environ.get("ANTHROPIC_API_KEY")
            if not key:
                raise ValueError("ANTHROPIC_API_KEY not set")
            self._client = anthropic.Anthropic(api_key=key)
        return self._client

    @abstractmethod
    def get_system_prompt(self) -> str:
        """Return the system prompt for this agent."""

    @abstractmethod
    def get_tools(self) -> list[dict[str, Any]]:
        """Return the tool schemas for this agent."""

    @abstractmethod
    def dispatch_tool(self, name: str, inputs: dict[str, Any], session_id: str) -> dict[str, Any]:
        """Execute a tool call and return a JSON-serializable result."""

    def run_tool_loop(
        self,
        messages: list[dict[str, Any]],
        session_id: str,
        max_tokens: int = 2048,
        max_iterations: int = 20,
    ) -> tuple[str, dict[str, Any]]:
        """
        Standard agentic loop: call Claude → dispatch tools → repeat until end_turn.
        Returns (final_text, accumulated_data).

        accumulated_data collects structured data from tool results:
          download_paths, job_id, run_id, chart_path, charts.

        max_iterations caps the loop so a misbehaving model can't spin forever.
        Raises RuntimeError if the cap is hit.
        """
        import datetime

        def _json_serial(obj: Any) -> str:
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

        def _extract_text(response: Any) -> str:
            parts = []
            for block in response.content:
                if hasattr(block, "text"):
                    parts.append(block.text)
            return "\n".join(parts).strip()

        client = self._get_client()
        working = list(messages)
        tools = self.get_tools()
        accumulated: dict[str, Any] = {}
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            kwargs: dict[str, Any] = {
                "model": self.model,
                "max_tokens": max_tokens,
                "system": self.get_system_prompt(),
                "messages": working,
            }
            if tools:
                kwargs["tools"] = tools

            response = client.messages.create(**kwargs)

            if response.stop_reason == "end_turn":
                return _extract_text(response), accumulated

            if response.stop_reason == "tool_use":
                tool_results = []
                for block in response.content:
                    if block.type != "tool_use":
                        continue
                    try:
                        result = self.dispatch_tool(block.name, block.input, session_id)
                    except Exception as exc:
                        result = {"error": f"Tool dispatch error: {exc}"}

                    # Surface structured data so callers don't have to re-parse.
                    # Keep the latest value for each top-level key across tool calls.
                    if isinstance(result, dict):
                        accumulated.update(result)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, default=_json_serial),
                    })

                if not tool_results:
                    # Model said tool_use but produced no tool blocks — stop to avoid loop
                    return _extract_text(response), accumulated

                working.append({"role": "assistant", "content": response.content})
                working.append({"role": "user", "content": tool_results})
                continue

            # Any other stop reason (max_tokens, stop_sequence, …) — return what we have
            return _extract_text(response), accumulated

        # Iteration cap hit
        raise RuntimeError(
            f"{self.__class__.__name__} exceeded {max_iterations} tool-use iterations. "
            "The model may be stuck in a loop."
        )
