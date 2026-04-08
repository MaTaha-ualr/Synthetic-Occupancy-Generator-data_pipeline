from __future__ import annotations

from types import SimpleNamespace

import frontend.session_manager as session_manager
from frontend.agents.base import BaseAgent


class _FakeClient:
    def __init__(self, responses):
        self._responses = iter(responses)
        self.messages = self

    def create(self, **kwargs):
        return next(self._responses)


class _DummyAgent(BaseAgent):
    def __init__(self, responses):
        super().__init__(model="test-model")
        self._responses = responses

    def _get_client(self):
        return _FakeClient(self._responses)

    def get_system_prompt(self) -> str:
        return "test"

    def get_tools(self) -> list[dict[str, object]]:
        return [{"name": "fake_tool", "input_schema": {"type": "object", "properties": {}}}]

    def dispatch_tool(self, name: str, inputs: dict[str, object], session_id: str):
        return {
            "scenario_id": "single_movers_custom",
            "zip_path": "C:/tmp/export.zip",
            "job_id": "job-123",
        }


def test_session_manager_roundtrip_persists_frontend_state(monkeypatch, tmp_path):
    monkeypatch.setattr(session_manager, "SESSIONS_DIR", tmp_path)

    session = session_manager.SessionData(
        session_id="abc123",
        context={"last_run_id": "run-1"},
        messages=[{"role": "user", "content": "hello"}],
        last_run_downloads={"DatasetA": "C:/tmp/DatasetA.csv"},
        pending_job_id="job-123",
        pending_charts=[{"chart_path": "C:/tmp/chart.html", "insight": "test"}],
    )
    session_manager.save_session(session)

    loaded = session_manager.load_session("abc123")
    assert loaded.context["last_run_id"] == "run-1"
    assert loaded.messages == [{"role": "user", "content": "hello"}]
    assert loaded.last_run_downloads == {"DatasetA": "C:/tmp/DatasetA.csv"}
    assert loaded.pending_job_id == "job-123"
    assert loaded.pending_charts == [{"chart_path": "C:/tmp/chart.html", "insight": "test"}]


def test_base_agent_accumulates_tool_results_for_downstream_agents():
    responses = [
        SimpleNamespace(
            stop_reason="tool_use",
            content=[SimpleNamespace(type="tool_use", id="tool-1", name="fake_tool", input={})],
        ),
        SimpleNamespace(
            stop_reason="end_turn",
            content=[SimpleNamespace(text="done")],
        ),
    ]
    agent = _DummyAgent(responses)

    text, data = agent.run_tool_loop([{"role": "user", "content": "run"}], session_id="sess-1")

    assert text == "done"
    assert data["scenario_id"] == "single_movers_custom"
    assert data["zip_path"] == "C:/tmp/export.zip"
    assert data["job_id"] == "job-123"
