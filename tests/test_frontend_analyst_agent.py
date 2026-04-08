from __future__ import annotations

from frontend.agents.analyst_agent import AnalystAgent


def test_analyst_agent_explains_named_scenario_template_without_run_lookup(monkeypatch):
    agent = AnalystAgent(api_key="test")

    def _unexpected_run_loop(*args, **kwargs):
        raise AssertionError("scenario-template explanation should not call the LLM run loop")

    monkeypatch.setattr(agent, "run_tool_loop", _unexpected_run_loop)

    result = agent.run(
        "Open clean_baseline_linkage and explain it like I am evaluating an ER benchmark. Focus on topology, overlap, duplication, and noise.",
        "sess-1",
        {},
    )

    assert result.success is True
    assert "Scenario template: `clean_baseline_linkage`" in result.message
    assert "- Topology:" in result.message
    assert "- Match mode: `one_to_one`" in result.message
    assert result.session_updates["last_scenario_id"] == "clean_baseline_linkage"


def test_analyst_agent_run_id_request_still_uses_llm_tool_loop(monkeypatch):
    agent = AnalystAgent(api_key="test")
    called = {"value": False}

    def _fake_run_loop(messages, session_id):
        called["value"] = True
        return "used run loop", {}

    monkeypatch.setattr(agent, "run_tool_loop", _fake_run_loop)

    result = agent.run(
        "Summarize run 2026-04-07_single_movers_seed20260310.",
        "sess-2",
        {},
    )

    assert called["value"] is True
    assert result.success is True
    assert result.message == "used run loop"
