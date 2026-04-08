from __future__ import annotations

from frontend.agents.base import AgentResponse
from frontend.agents.orchestrator import Orchestrator


def test_orchestrator_infers_supported_scenario_ids_beyond_original_five(monkeypatch):
    orch = Orchestrator(api_key="test")
    monkeypatch.setattr(
        orch,
        "_known_runnable_scenario_ids",
        lambda: [
            "single_movers",
            "clean_baseline_linkage",
            "high_noise_identity_drift",
            "three_source_partial_overlap",
        ],
    )

    assert orch._infer_scenario_id("run clean_baseline_linkage now") == "clean_baseline_linkage"
    assert orch._infer_scenario_id("please execute high_noise_identity_drift") == "high_noise_identity_drift"
    assert orch._infer_scenario_id("launch three_source_partial_overlap next") == "three_source_partial_overlap"


def test_orchestrator_run_turn_uses_inferred_supported_scenario_without_config_roundtrip(monkeypatch):
    orch = Orchestrator(api_key="test")
    run_calls: list[tuple[str, str, dict[str, object]]] = []

    class _DummyRunAgent:
        def run(self, scenario_id: str, session_id: str, context: dict[str, object]) -> AgentResponse:
            run_calls.append((scenario_id, session_id, context))
            return AgentResponse(
                success=True,
                message=f"Queued {scenario_id}",
                session_updates={"last_scenario_id": scenario_id},
                pending_job_id="job-42",
            )

    def _unexpected_config():
        raise AssertionError("run intent should not fall back to ConfigAgent when scenario id is inferable")

    monkeypatch.setattr(orch, "classify_intent", lambda _text: "run")
    monkeypatch.setattr(orch, "_infer_scenario_id", lambda _text: "clean_baseline_linkage")
    monkeypatch.setattr(orch, "_run", lambda: _DummyRunAgent())
    monkeypatch.setattr(orch, "_config", _unexpected_config)

    result = orch.run_turn("run clean_baseline_linkage", "sess-1", {})

    assert run_calls == [("clean_baseline_linkage", "sess-1", {})]
    assert result["message"] == "Queued clean_baseline_linkage"
    assert result["pending_job_id"] == "job-42"
    assert result["session_updates"]["last_scenario_id"] == "clean_baseline_linkage"
