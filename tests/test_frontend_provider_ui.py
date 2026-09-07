from __future__ import annotations

import importlib
from pathlib import Path

import yaml
from streamlit.testing.v1 import AppTest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINT = PROJECT_ROOT / "frontend" / "chatbot_production.py"
CANONICAL_TEMPLATE = PROJECT_ROOT / "phase2" / "scenarios" / "clean_baseline_linkage.yaml"


def _app() -> AppTest:
    return AppTest.from_file(str(ENTRYPOINT)).run(timeout=30)


def test_nvidia_composer_is_available_without_anthropic(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    app = _app()

    assert list(app.exception) == []
    assert any(item.label == "Experiment requirements" for item in app.text_area)
    assert any(item.label == "NVIDIA API key" for item in app.text_input)
    assert any(item.label == "Generate structured proposal" for item in app.button)
    assert list(app.chat_input) == []

    optional_claude = next(
        item for item in app.expander if item.label == "Connect the optional Claude analyst"
    )
    assert optional_claude.proto.expanded is False


def test_app_recovers_from_stale_scenario_authoring_module(monkeypatch) -> None:
    runtime_authoring = importlib.import_module("scenario_authoring")
    monkeypatch.setattr(runtime_authoring, "SCENARIO_AUTHORING_API_VERSION", 2)
    monkeypatch.delattr(runtime_authoring, "REVIEW_NOISE_KEYS")
    monkeypatch.delattr(runtime_authoring, "summarize_candidate")

    app = _app()

    assert list(app.exception) == []
    assert runtime_authoring.SCENARIO_AUTHORING_API_VERSION == 3
    assert hasattr(runtime_authoring, "REVIEW_NOISE_KEYS")
    assert hasattr(runtime_authoring, "summarize_candidate")


def test_missing_nvidia_key_is_an_inline_error_not_an_app_crash(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    app = _app()
    next(
        item for item in app.text_area if item.key == "scenario_authoring_requirements"
    ).input("Create a clean linkage scenario.")
    next(item for item in app.button if item.key == "create_scenario_proposal").click()

    app.run(timeout=30)

    assert list(app.exception) == []
    assert any("NVIDIA_API_KEY is not configured" in item.value for item in app.error)


def test_review_panel_shows_runtime_resolved_targets_before_model_notes(monkeypatch) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    runtime_authoring = importlib.import_module("scenario_authoring")

    def fake_propose(self, requirements):
        candidate = yaml.safe_load(CANONICAL_TEMPLATE.read_text(encoding="utf-8"))
        candidate["scenario_id"] = "clean_linkage_10000"
        candidate["seed"] = 424242
        candidate["selection"]["sample"] = {"mode": "count", "value": 10_000}
        candidate["emission"].update(
            overlap_entity_pct=80.0,
            record_count_A=7_000,
            record_count_B=7_000,
        )
        candidate["emission"]["noise"]["A"].update(
            nickname_pct=2.0,
            name_typo_pct=0.5,
            address_missing_pct=1.0,
            dob_shift_pct=0.2,
        )
        candidate["emission"]["noise"]["B"].update(
            nickname_pct=5.0,
            name_typo_pct=1.0,
            address_missing_pct=2.0,
            dob_shift_pct=0.5,
        )
        proposal = {
            "template_id": "clean_baseline_linkage",
            "scenario_id": "clean_linkage_10000",
            "summary": "Model-written summary.",
            "assumptions": [],
            "warnings": [],
            "proposal_id": "test-proposal-id",
        }
        return {
            "proposal": proposal,
            "candidate": candidate,
            "candidate_yaml": yaml.safe_dump(candidate, sort_keys=False),
            "validation": {"valid": True, "errors": []},
        }

    monkeypatch.setattr(
        runtime_authoring.ScenarioAuthoringAgent,
        "propose",
        fake_propose,
    )
    app = _app()
    next(
        item for item in app.text_area if item.key == "scenario_authoring_requirements"
    ).input("Create the exact 10,000-person benchmark.")
    next(
        item for item in app.text_input if item.key == "nvidia_scenario_api_key"
    ).input("nvapi-test-only")
    next(item for item in app.button if item.key == "create_scenario_proposal").click()

    app.run(timeout=30)

    assert list(app.exception) == []
    rendered = "\n".join(item.value for item in app.markdown)
    assert "Resolved experiment targets" in rendered
    assert "10,000" in rendered
    assert "14,000" in rendered
    assert "80%" in rendered
    assert "Nickname" in rendered
    assert rendered.index("Resolved experiment targets") < rendered.index(
        "Model-written summary."
    )
