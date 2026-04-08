from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import frontend.sog_tools as sog_tools


def _seed_template(scenarios_dir: Path, scenario_id: str = "template") -> None:
    scenarios_dir.mkdir(parents=True, exist_ok=True)
    (scenarios_dir / f"{scenario_id}.yaml").write_text(
        dedent(
            f"""
            scenario_id: {scenario_id}
            seed: 20260310
            phase1:
              data_path: phase1/outputs_phase1/Phase1_people_addresses.csv
              manifest_path: phase1/outputs_phase1/Phase1_people_addresses.manifest.json
            parameters:
              move_rate_pct: 12.0
            simulation:
              granularity: monthly
              start_date: 2026-01-01
              periods: 12
            emission:
              crossfile_match_mode: one_to_one
              overlap_entity_pct: 70.0
              appearance_A_pct: 85.0
              appearance_B_pct: 90.0
              duplication_in_A_pct: 4.0
              duplication_in_B_pct: 6.0
              noise:
                A:
                  name_typo_pct: 1.0
                  dob_shift_pct: 0.4
                  ssn_mask_pct: 1.5
                  phone_mask_pct: 0.8
                  address_missing_pct: 0.8
                  middle_name_missing_pct: 20.0
                  phonetic_error_pct: 0.0
                  ocr_error_pct: 0.0
                  date_swap_pct: 0.0
                  zip_digit_error_pct: 0.0
                  nickname_pct: 0.0
                  suffix_missing_pct: 0.0
                B:
                  name_typo_pct: 2.5
                  dob_shift_pct: 1.2
                  ssn_mask_pct: 6.0
                  phone_mask_pct: 3.0
                  address_missing_pct: 2.2
                  middle_name_missing_pct: 30.0
                  phonetic_error_pct: 0.0
                  ocr_error_pct: 0.0
                  date_swap_pct: 0.0
                  zip_digit_error_pct: 2.0
                  nickname_pct: 0.0
                  suffix_missing_pct: 0.0
            quality:
              household_size_range:
                min: 1
                max: 8
            selection:
              sample:
                mode: pct
                value: 100.0
              filters:
                age_bins: []
                genders: []
                ethnicities: []
                residence_types: []
                redundancy_profiles: []
                mobility_propensity_buckets: []
              thresholds:
                mobility_low_max: 0.09
                mobility_high_min: 0.18
                trait_low_max: 0.33
                trait_high_min: 0.66
            constraints:
              min_marriage_age: 18
              max_partner_age_gap: 25
              fertility_age_range:
                min: 15
                max: 49
              allow_underage_marriage: false
              allow_child_lives_alone: false
              enforce_non_overlapping_residence_intervals: true
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )


def test_update_scenario_preserves_existing_working_copy(monkeypatch, tmp_path):
    scenarios_dir = tmp_path / "phase2" / "scenarios"
    _seed_template(scenarios_dir)
    monkeypatch.setattr(sog_tools, "SCENARIOS_DIR", scenarios_dir)

    session_id = "session-a"
    first = sog_tools.update_scenario(
        "template",
        {"emission.overlap_entity_pct": 50},
        session_id=session_id,
    )
    second = sog_tools.update_scenario(
        "template",
        {"emission.duplication_in_B_pct": 20},
        session_id=session_id,
    )

    assert first["validation_errors"] == []
    assert second["validation_errors"] == []

    parsed = sog_tools.read_scenario("template", session_id=session_id)["parsed"]
    assert parsed["emission"]["overlap_entity_pct"] == 50
    assert parsed["emission"]["duplication_in_B_pct"] == 20


def test_cloned_scenario_can_be_updated_again(monkeypatch, tmp_path):
    scenarios_dir = tmp_path / "phase2" / "scenarios"
    _seed_template(scenarios_dir)
    monkeypatch.setattr(sog_tools, "SCENARIOS_DIR", scenarios_dir)

    session_id = "session-b"
    created = sog_tools.create_scenario_from_template(
        "template",
        "template_clone",
        {"emission.overlap_entity_pct": 55},
        session_id=session_id,
    )
    updated = sog_tools.update_scenario(
        "template_clone",
        {"emission.duplication_in_A_pct": 11},
        session_id=session_id,
    )

    assert created["valid"] is True
    assert "error" not in updated

    parsed = sog_tools.read_scenario("template_clone", session_id=session_id)["parsed"]
    assert parsed["emission"]["overlap_entity_pct"] == 55
    assert parsed["emission"]["duplication_in_A_pct"] == 11


def test_update_scenario_supports_single_dataset_emission_schema(monkeypatch, tmp_path):
    scenarios_dir = tmp_path / "phase2" / "scenarios"
    _seed_template(scenarios_dir)
    monkeypatch.setattr(sog_tools, "SCENARIOS_DIR", scenarios_dir)

    session_id = "session-single"
    result = sog_tools.update_scenario(
        "template",
        {
            "emission.crossfile_match_mode": "single_dataset",
            "emission.datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 12.0,
                    "noise": {
                        "name_typo_pct": 1.0,
                        "dob_shift_pct": 0.0,
                        "ssn_mask_pct": 0.0,
                        "phone_mask_pct": 0.0,
                        "address_missing_pct": 0.0,
                        "middle_name_missing_pct": 0.0,
                        "phonetic_error_pct": 0.0,
                        "ocr_error_pct": 0.0,
                        "date_swap_pct": 0.0,
                        "zip_digit_error_pct": 0.0,
                        "nickname_pct": 0.0,
                        "suffix_missing_pct": 0.0,
                    },
                }
            ],
        },
        session_id=session_id,
    )

    assert result["validation_errors"] == []
    parsed = sog_tools.read_scenario("template", session_id=session_id)["parsed"]
    assert parsed["emission"]["crossfile_match_mode"] == "single_dataset"
    assert parsed["emission"]["datasets"][0]["dataset_id"] == "registry"
    assert parsed["emission"]["datasets"][0]["duplication_pct"] == 12.0


def test_list_scenarios_uses_catalog_metadata(monkeypatch, tmp_path):
    scenarios_dir = tmp_path / "phase2" / "scenarios"
    _seed_template(scenarios_dir, scenario_id="single_movers")
    monkeypatch.setattr(sog_tools, "SCENARIOS_DIR", scenarios_dir)

    result = sog_tools.list_scenarios()
    scenario = result["scenarios"][0]

    assert scenario["scenario_id"] == "single_movers"
    assert scenario["status"] == "supported"
    assert scenario["delivery_mode"] == "canonical_yaml"
    assert scenario["topology"] == "pairwise"
    assert scenario["description"] != ""
