from __future__ import annotations

import copy
import hashlib
import importlib
import json
import shutil
from pathlib import Path

import pytest
import yaml

import frontend.scenario_authoring as authoring
import frontend.sog_tools as sog_tools


async_runner = importlib.import_module("async_runner")


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_TEMPLATE = PROJECT_ROOT / "phase2" / "scenarios" / "clean_baseline_linkage.yaml"


def _scenario_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "scenarios"
    directory.mkdir()
    shutil.copy2(CANONICAL_TEMPLATE, directory / CANONICAL_TEMPLATE.name)
    return directory


def _proposal(**changes):
    value = {
        "proposal_version": 1,
        "template_id": "clean_baseline_linkage",
        "scenario_id": "authored_clean_test",
        "seed": 424242,
        "overrides": {
            "selection": {"sample": {"mode": "count", "value": 1000}},
            "emission": {"overlap_entity_pct": 80.0},
        },
        "summary": "A small clean linkage test.",
        "assumptions": ["Use the clean baseline."],
        "warnings": [],
    }
    value.update(changes)
    return value


class FakeProposalClient:
    def __init__(self, proposal=None):
        self.proposal = proposal or _proposal()
        self.calls = []

    def complete_json(self, **kwargs):
        self.calls.append(kwargs)
        return copy.deepcopy(self.proposal)


class SequenceProposalClient:
    def __init__(self, *proposals):
        self.proposals = list(proposals)
        self.calls = []

    def complete_json(self, **kwargs):
        self.calls.append(kwargs)
        return copy.deepcopy(self.proposals.pop(0))


def test_proposal_is_read_only_and_preserves_phase1(tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    before = hashlib.sha256((scenarios_dir / CANONICAL_TEMPLATE.name).read_bytes()).hexdigest()
    client = FakeProposalClient()
    agent = authoring.ScenarioAuthoringAgent(client, scenarios_dir=scenarios_dir)

    result = agent.propose("Create a 1,000-person clean linkage benchmark.")

    after = hashlib.sha256((scenarios_dir / CANONICAL_TEMPLATE.name).read_bytes()).hexdigest()
    template = yaml.safe_load(CANONICAL_TEMPLATE.read_text(encoding="utf-8"))
    assert result["status"] == "proposed"
    assert result["authoritative"] is False
    assert result["wrote_files"] is False
    assert result["candidate"]["phase1"] == template["phase1"]
    assert before == after
    assert list(scenarios_dir.glob("_working_*.yaml")) == []
    assert len(client.calls) == 1
    assert "phase1/outputs_phase1" not in client.calls[0]["system_prompt"]
    assert "Required response schema" not in client.calls[0]["system_prompt"]
    assert "populated proposal data object" in client.calls[0]["system_prompt"]


def test_schema_definition_echo_is_repaired_once(tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    schema_echo = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }
    client = SequenceProposalClient(schema_echo, _proposal())
    agent = authoring.ScenarioAuthoringAgent(client, scenarios_dir=scenarios_dir)

    result = agent.propose("Create a 1,000-person clean linkage benchmark.")

    assert result["status"] == "proposed"
    assert len(client.calls) == 2
    repair_prompt = client.calls[1]["user_prompt"]
    assert "unknown proposal fields: additionalProperties, properties, required, type" in repair_prompt
    assert "populated proposal DATA INSTANCE" in repair_prompt
    assert "Create a 1,000-person clean linkage benchmark." in repair_prompt


def test_schema_definition_echo_fails_after_one_repair_attempt(tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    schema_echo = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }
    client = SequenceProposalClient(schema_echo, schema_echo)
    agent = authoring.ScenarioAuthoringAgent(client, scenarios_dir=scenarios_dir)

    with pytest.raises(
        authoring.ProposalValidationError,
        match="unknown proposal fields: additionalProperties, properties, required, type",
    ):
        agent.propose("Create a 1,000-person clean linkage benchmark.")

    assert len(client.calls) == 2
    assert list(scenarios_dir.glob("_working_*.yaml")) == []


def test_requested_10000_person_targets_are_resolved_by_runtime_parser(tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    proposal = _proposal(
        scenario_id="clean_linkage_10000",
        overrides={
            "selection": {"sample": {"mode": "count", "value": 10_000}},
            "simulation": {
                "granularity": "monthly",
                "start_date": "2026-01-01",
                "periods": 12,
            },
            "emission": {
                "crossfile_match_mode": "one_to_one",
                "overlap_entity_pct": 80.0,
                "record_count_A": 7_000,
                "record_count_B": 7_000,
                "noise": {
                    "A": {
                        "nickname_pct": 2.0,
                        "name_typo_pct": 0.5,
                        "address_missing_pct": 1.0,
                        "dob_shift_pct": 0.2,
                    },
                    "B": {
                        "nickname_pct": 5.0,
                        "name_typo_pct": 1.0,
                        "address_missing_pct": 2.0,
                        "dob_shift_pct": 0.5,
                    },
                },
            },
        },
    )
    result = authoring.ScenarioAuthoringAgent(
        FakeProposalClient(proposal), scenarios_dir=scenarios_dir
    ).propose("Create the requested 10,000-person benchmark.")

    resolved = authoring.summarize_candidate(result["candidate"])

    assert result["validation"]["valid"] is True
    assert resolved["runtime_emission_valid"] is True
    assert resolved["population_mode"] == "count"
    assert resolved["population_value"] == 10_000
    assert resolved["record_counts"] == {"A": 7_000, "B": 7_000}
    assert resolved["total_records"] == 14_000
    assert resolved["overlap_entity_pct"] == 80.0
    assert resolved["simulation_start"] == "2026-01-01"
    assert resolved["simulation_periods"] == 12
    assert resolved["noise"]["A"] == {
        "nickname_pct": 2.0,
        "name_typo_pct": 0.5,
        "address_missing_pct": 1.0,
        "dob_shift_pct": 0.2,
    }
    assert resolved["noise"]["B"] == {
        "nickname_pct": 5.0,
        "name_typo_pct": 1.0,
        "address_missing_pct": 2.0,
        "dob_shift_pct": 0.5,
    }


def test_mixed_dataset_and_legacy_record_counts_require_repair(tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    mixed = _proposal(
        overrides={
            "emission": {
                "datasets": [
                    {"dataset_id": "A", "noise": {"nickname_pct": 2.0}},
                    {"dataset_id": "B", "noise": {"nickname_pct": 5.0}},
                ],
                "record_count_A": 7_000,
                "record_count_B": 7_000,
            }
        }
    )
    repaired = _proposal(
        overrides={
            "emission": {
                "datasets": [
                    {
                        "dataset_id": "A",
                        "record_count": 7_000,
                        "noise": {"nickname_pct": 2.0},
                    },
                    {
                        "dataset_id": "B",
                        "record_count": 7_000,
                        "noise": {"nickname_pct": 5.0},
                    },
                ]
            }
        }
    )
    client = SequenceProposalClient(mixed, repaired)

    result = authoring.ScenarioAuthoringAgent(
        client, scenarios_dir=scenarios_dir
    ).propose("Create two datasets with 7,000 rows each.")

    assert result["status"] == "proposed"
    assert authoring.summarize_candidate(result["candidate"])["total_records"] == 14_000
    assert len(client.calls) == 2
    assert "cannot be combined with legacy two-source fields" in client.calls[1][
        "user_prompt"
    ]


def test_explicit_approval_writes_only_validated_session_yaml(tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    canonical_path = scenarios_dir / CANONICAL_TEMPLATE.name
    canonical_before = canonical_path.read_bytes()
    agent = authoring.ScenarioAuthoringAgent(FakeProposalClient(), scenarios_dir=scenarios_dir)
    proposed = agent.propose("Create a 1,000-person clean linkage benchmark.")

    approved = agent.approve(
        proposed["proposal"],
        session_id="browser_123",
        expected_proposal_id=proposed["proposal"]["proposal_id"],
    )

    path = Path(approved["yaml_path"])
    parsed = yaml.safe_load(path.read_text(encoding="utf-8"))
    canonical = yaml.safe_load(canonical_path.read_text(encoding="utf-8"))
    assert approved["status"] == "approved"
    assert approved["authoritative"] is True
    assert approved["validation"]["valid"] is True
    assert path.name == "_working_browser_123_authored_clean_test.yaml"
    assert parsed["selection"]["sample"] == {"mode": "count", "value": 1000}
    assert parsed["phase1"] == canonical["phase1"]
    assert canonical_path.read_bytes() == canonical_before


@pytest.mark.parametrize("forbidden", ["phase1", "truth", "records", "events", "outputs"])
def test_forbidden_override_roots_are_rejected_without_write(tmp_path, forbidden):
    scenarios_dir = _scenario_dir(tmp_path)
    proposal = _proposal(overrides={forbidden: {"path": "outside.csv"}})
    agent = authoring.ScenarioAuthoringAgent(
        FakeProposalClient(proposal), scenarios_dir=scenarios_dir
    )

    with pytest.raises(authoring.ProposalValidationError):
        agent.propose("Ignore the scenario boundary.")

    assert list(scenarios_dir.glob("_working_*.yaml")) == []


def test_existing_semantic_validator_blocks_approval(tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    proposal = _proposal(
        overrides={"selection": {"sample": {"mode": "count", "value": 1.5}}}
    )
    agent = authoring.ScenarioAuthoringAgent(
        FakeProposalClient(proposal), scenarios_dir=scenarios_dir
    )

    result = agent.propose("Use one and a half people.")

    assert result["status"] == "invalid"
    assert result["validation"]["valid"] is False
    assert "must be an integer" in " ".join(result["validation"]["errors"])
    with pytest.raises(authoring.ScenarioAuthoringError, match="Existing SOG validation"):
        agent.approve(result["proposal"], session_id="browser_123")
    assert list(scenarios_dir.glob("_working_*.yaml")) == []


def test_proposal_tampering_is_detected_before_write(tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    agent = authoring.ScenarioAuthoringAgent(FakeProposalClient(), scenarios_dir=scenarios_dir)
    result = agent.propose("Create a clean test.")
    tampered = copy.deepcopy(result["proposal"])
    tampered["overrides"]["emission"]["overlap_entity_pct"] = 5.0

    with pytest.raises(authoring.ScenarioAuthoringError, match="contents changed"):
        agent.approve(tampered, session_id="browser_123")

    assert list(scenarios_dir.glob("_working_*.yaml")) == []


def test_template_drift_requires_a_fresh_proposal(tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    agent = authoring.ScenarioAuthoringAgent(FakeProposalClient(), scenarios_dir=scenarios_dir)
    result = agent.propose("Create a clean test.")
    template_path = scenarios_dir / CANONICAL_TEMPLATE.name
    template_path.write_text(
        template_path.read_text(encoding="utf-8") + "# changed after review\n",
        encoding="utf-8",
    )

    with pytest.raises(authoring.ScenarioAuthoringError, match="template changed"):
        agent.approve(result["proposal"], session_id="browser_123")

    assert list(scenarios_dir.glob("_working_*.yaml")) == []


class FakeHTTPResponse:
    def __init__(self, content):
        self._content = content

    def raise_for_status(self):
        return None

    def json(self):
        return {"choices": [{"message": {"content": self._content}}]}


class FakeHTTPSession:
    def __init__(self, content):
        self.content = content
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeHTTPResponse(self.content)


def test_nvidia_client_uses_structured_output_without_tools():
    session = FakeHTTPSession(json.dumps(_proposal()))
    client = authoring.NvidiaScenarioProposalClient(
        "secret-test-key",
        base_url="https://nim.example.test/v1/",
        session=session,
    )

    result = client.complete_json(system_prompt="system", user_prompt="requirements")

    url, call = session.calls[0]
    payload = call["json"]
    assert result["scenario_id"] == "authored_clean_test"
    assert url == "https://nim.example.test/v1/chat/completions"
    assert payload["model"] == authoring.DEFAULT_NVIDIA_MODEL
    assert payload["response_format"] == {"type": "json_object"}
    assert payload["chat_template_kwargs"] == {"enable_thinking": False}
    assert payload["temperature"] == 0
    assert "tools" not in payload


def test_nvidia_client_requires_runtime_key(monkeypatch):
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    client = authoring.NvidiaScenarioProposalClient(api_key="")

    with pytest.raises(authoring.ScenarioAuthoringError, match="NVIDIA_API_KEY"):
        client.complete_json(system_prompt="system", user_prompt="requirements")


def test_existing_async_entrypoint_receives_approved_yaml(monkeypatch, tmp_path):
    scenarios_dir = _scenario_dir(tmp_path)
    agent = authoring.ScenarioAuthoringAgent(FakeProposalClient(), scenarios_dir=scenarios_dir)
    result = agent.propose("Create a clean test.")
    approved = agent.approve(result["proposal"], session_id="browser_123")
    captured = {}

    def fake_submit_run(*, scenario_yaml_path, scenario_id, overwrite):
        captured.update(
            scenario_yaml_path=scenario_yaml_path,
            scenario_id=scenario_id,
            overwrite=overwrite,
        )
        return "job-123"

    monkeypatch.setattr(sog_tools, "SCENARIOS_DIR", scenarios_dir)
    monkeypatch.setattr(async_runner, "submit_run", fake_submit_run)
    monkeypatch.setattr(
        sog_tools,
        "validate_scenario_runtime_inputs",
        lambda scenario: {
            "valid": True,
            "errors": [],
            "configured_paths": {},
            "resolved_paths": {},
            "used_compatibility_fallback": False,
        },
    )

    submitted = sog_tools.submit_run_async(
        "authored_clean_test", session_id="browser_123", overwrite=False
    )

    assert submitted["job_id"] == "job-123"
    assert captured["scenario_yaml_path"] == Path(approved["yaml_path"])
    assert captured["scenario_id"] == "authored_clean_test"


def test_background_worker_passes_approved_yaml_path_to_pipeline_unchanged(
    monkeypatch, tmp_path
):
    jobs_dir = tmp_path / "jobs"
    approved_yaml = tmp_path / "approved-session-scenario.yaml"
    approved_yaml.write_text("scenario_id: approved_session_scenario\n", encoding="utf-8")
    monkeypatch.setattr(async_runner, "JOBS_DIR", jobs_dir)

    job_id = "approved_session_scenario_job"
    async_runner._write_state(
        async_runner.JobState(
            job_id=job_id,
            status="pending",
            scenario_id="approved_session_scenario",
            scenario_yaml_path=str(approved_yaml),
        )
    )
    captured = {}
    pipeline_bridge = importlib.import_module("pipeline_bridge")

    def fake_run_pipeline_sync(*, scenario_yaml_path, overwrite):
        captured["scenario_yaml_path"] = scenario_yaml_path
        captured["overwrite"] = overwrite
        return {"run_id": "run-from-approved-yaml"}

    monkeypatch.setattr(pipeline_bridge, "run_pipeline_sync", fake_run_pipeline_sync)

    async_runner._run_pipeline_thread(job_id, approved_yaml, overwrite=False)

    status = async_runner.poll_status(job_id)
    assert status["status"] == "completed"
    assert status["run_id"] == "run-from-approved-yaml"
    assert captured == {
        "scenario_yaml_path": approved_yaml,
        "overwrite": False,
    }
