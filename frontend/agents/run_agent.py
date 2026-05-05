"""RunAgent — Async pipeline submission specialist.

Validates first, submits async, returns immediately with job_id.
Never interprets results.
"""

from __future__ import annotations

from typing import Any

from .base import AgentResponse, BaseAgent

_SYSTEM_PROMPT = """\
You are RunAgent, the execution specialist for SOG.

YOUR JOB: Submit pipeline runs and report status. Never interpret results.

WORKFLOW:
1. Call validate_scenario first. If errors exist, return them and stop.
2. Call submit_run_async. This returns immediately with a job_id.
3. Return ONE sentence: "Running [scenario_id] — job [job_id] submitted."
4. Never wait for completion. Never call get_run_results.

RESPONSE STYLE:
- "Running single_movers_hard_noise — job abc123 submitted."
- "Validation failed: emission.overlap_entity_pct must be ≤ 100."
- "Run already exists. Rerun with overwrite=True to replace."
- ONE sentence maximum.
"""

_TOOLS = [
    {
        "name": "validate_scenario",
        "description": "Validate scenario YAML before running.",
        "input_schema": {
            "type": "object",
            "properties": {
                "scenario_id": {"type": "string"},
                "session_id": {"type": "string"},
            },
            "required": ["scenario_id"],
        },
    },
    {
        "name": "submit_run_async",
        "description": "Submit a background pipeline run. Returns job_id immediately.",
        "input_schema": {
            "type": "object",
            "properties": {
                "scenario_id": {"type": "string"},
                "session_id": {"type": "string"},
                "overwrite": {"type": "boolean"},
            },
            "required": ["scenario_id"],
        },
    },
    {
        "name": "poll_run_status",
        "description": "Check current job status.",
        "input_schema": {
            "type": "object",
            "properties": {"job_id": {"type": "string"}},
            "required": ["job_id"],
        },
    },
    {
        "name": "list_recent_runs",
        "description": "List recently completed runs.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer"},
                "scenario_id_filter": {"type": "string"},
            },
        },
    },
]


class RunAgent(BaseAgent):
    """Submits async pipeline runs and reports job status."""

    def __init__(self, api_key: str | None = None, provider: str | None = None):
        super().__init__(api_key=api_key, provider=provider, model_role="fast")

    def get_system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def get_tools(self) -> list[dict[str, Any]]:
        return _TOOLS

    def dispatch_tool(self, name: str, inputs: dict[str, Any], session_id: str) -> dict[str, Any]:
        import sog_tools as t
        inputs_with_session = {**inputs, "session_id": session_id}
        try:
            if name == "validate_scenario":
                return t.validate_scenario(**inputs_with_session)
            if name == "submit_run_async":
                return t.submit_run_async(**inputs_with_session)
            if name == "poll_run_status":
                return t.poll_run_status(**inputs)
            if name == "list_recent_runs":
                return t.list_recent_runs(**inputs)
            return {"error": f"Unknown tool: {name}"}
        except Exception as exc:
            return {"error": str(exc)}

    def run(
        self,
        scenario_id: str,
        session_id: str,
        context: dict[str, Any],
        overwrite: bool = False,
    ) -> AgentResponse:
        """Submit a run for the given scenario_id."""
        user_msg = f"Run scenario '{scenario_id}'."
        if overwrite:
            user_msg += " Use overwrite=True."

        messages = [{"role": "user", "content": user_msg}]

        try:
            text, data = self.run_tool_loop(messages, session_id)
            job_id = data.get("job_id")
            return AgentResponse(
                success=True,
                message=text,
                data=data,
                session_updates={"last_scenario_id": scenario_id},
                pending_job_id=job_id,
            )
        except Exception as exc:
            return AgentResponse(
                success=False,
                message=f"Run submission failed: {exc}",
            )
