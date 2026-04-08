"""Difficulty presets for SOG scenarios."""

from __future__ import annotations

from typing import Any

PRESETS: dict[str, dict[str, Any]] = {
    "baseline_easy": {
        "description": "85% overlap, minimal noise, good for algorithm sanity checks",
        "patches": {
            "emission.overlap_entity_pct": 85,
            "emission.crossfile_match_mode": "one_to_one",
            "emission.duplication_in_A_pct": 1,
            "emission.duplication_in_B_pct": 2,
            "emission.noise.A.name_typo_pct": 0.5,
            "emission.noise.A.dob_shift_pct": 0.2,
            "emission.noise.A.ssn_mask_pct": 0.5,
            "emission.noise.A.phone_mask_pct": 0.3,
            "emission.noise.A.address_missing_pct": 0.5,
            "emission.noise.A.middle_name_missing_pct": 10.0,
            "emission.noise.A.phonetic_error_pct": 0.0,
            "emission.noise.A.ocr_error_pct": 0.0,
            "emission.noise.A.nickname_pct": 0.0,
            "emission.noise.B.name_typo_pct": 1.0,
            "emission.noise.B.dob_shift_pct": 0.5,
            "emission.noise.B.ssn_mask_pct": 1.0,
            "emission.noise.B.phone_mask_pct": 0.5,
            "emission.noise.B.address_missing_pct": 0.8,
            "emission.noise.B.middle_name_missing_pct": 15.0,
            "emission.noise.B.phonetic_error_pct": 0.0,
            "emission.noise.B.ocr_error_pct": 0.0,
            "emission.noise.B.nickname_pct": 0.0,
        },
    },
    "realistic_medium": {
        "description": "65% overlap, census-like noise, simulates real production data",
        "patches": {
            "emission.overlap_entity_pct": 65,
            "emission.crossfile_match_mode": "one_to_many",
            "emission.duplication_in_A_pct": 3,
            "emission.duplication_in_B_pct": 8,
            "emission.noise.A.name_typo_pct": 1.0,
            "emission.noise.A.dob_shift_pct": 0.4,
            "emission.noise.A.ssn_mask_pct": 1.5,
            "emission.noise.A.phone_mask_pct": 0.8,
            "emission.noise.A.address_missing_pct": 0.8,
            "emission.noise.A.middle_name_missing_pct": 20.0,
            "emission.noise.A.phonetic_error_pct": 0.5,
            "emission.noise.A.ocr_error_pct": 0.2,
            "emission.noise.A.nickname_pct": 2.0,
            "emission.noise.B.name_typo_pct": 2.5,
            "emission.noise.B.dob_shift_pct": 1.2,
            "emission.noise.B.ssn_mask_pct": 6.0,
            "emission.noise.B.phone_mask_pct": 2.0,
            "emission.noise.B.address_missing_pct": 2.0,
            "emission.noise.B.middle_name_missing_pct": 30.0,
            "emission.noise.B.phonetic_error_pct": 1.5,
            "emission.noise.B.ocr_error_pct": 0.5,
            "emission.noise.B.nickname_pct": 5.0,
        },
    },
    "hard_noise": {
        "description": "50% overlap, significant name noise, challenges blocking methods",
        "patches": {
            "emission.overlap_entity_pct": 50,
            "emission.crossfile_match_mode": "many_to_many",
            "emission.duplication_in_A_pct": 8,
            "emission.duplication_in_B_pct": 15,
            "emission.noise.A.name_typo_pct": 3.0,
            "emission.noise.A.dob_shift_pct": 1.5,
            "emission.noise.A.ssn_mask_pct": 5.0,
            "emission.noise.A.phone_mask_pct": 2.0,
            "emission.noise.A.address_missing_pct": 2.5,
            "emission.noise.A.middle_name_missing_pct": 35.0,
            "emission.noise.A.phonetic_error_pct": 2.0,
            "emission.noise.A.ocr_error_pct": 1.0,
            "emission.noise.A.nickname_pct": 5.0,
            "emission.noise.B.name_typo_pct": 5.0,
            "emission.noise.B.dob_shift_pct": 2.5,
            "emission.noise.B.ssn_mask_pct": 10.0,
            "emission.noise.B.phone_mask_pct": 4.0,
            "emission.noise.B.address_missing_pct": 4.0,
            "emission.noise.B.middle_name_missing_pct": 50.0,
            "emission.noise.B.phonetic_error_pct": 4.0,
            "emission.noise.B.ocr_error_pct": 2.0,
            "emission.noise.B.nickname_pct": 10.0,
        },
    },
    "extreme_stress": {
        "description": "30% overlap, 10%+ noise, worst-case stress test",
        "patches": {
            "emission.overlap_entity_pct": 30,
            "emission.crossfile_match_mode": "many_to_many",
            "emission.duplication_in_A_pct": 15,
            "emission.duplication_in_B_pct": 25,
            "emission.noise.A.name_typo_pct": 5.0,
            "emission.noise.A.dob_shift_pct": 3.0,
            "emission.noise.A.ssn_mask_pct": 8.0,
            "emission.noise.A.phone_mask_pct": 5.0,
            "emission.noise.A.address_missing_pct": 5.0,
            "emission.noise.A.middle_name_missing_pct": 50.0,
            "emission.noise.A.phonetic_error_pct": 4.0,
            "emission.noise.A.ocr_error_pct": 2.0,
            "emission.noise.A.nickname_pct": 10.0,
            "emission.noise.B.name_typo_pct": 8.0,
            "emission.noise.B.dob_shift_pct": 5.0,
            "emission.noise.B.ssn_mask_pct": 15.0,
            "emission.noise.B.phone_mask_pct": 8.0,
            "emission.noise.B.address_missing_pct": 8.0,
            "emission.noise.B.middle_name_missing_pct": 70.0,
            "emission.noise.B.phonetic_error_pct": 7.0,
            "emission.noise.B.ocr_error_pct": 4.0,
            "emission.noise.B.nickname_pct": 15.0,
        },
    },
    "high_mobility": {
        "description": "25% move rate over 24 periods, tests address churn handling",
        "patches": {
            "parameters.move_rate_pct": 25.0,
            "simulation.periods": 24,
            "emission.overlap_entity_pct": 60,
            "emission.crossfile_match_mode": "one_to_many",
            "emission.noise.A.address_missing_pct": 2.0,
            "emission.noise.B.address_missing_pct": 5.0,
        },
    },
    "census_realistic": {
        "description": "ACS 2024 / CDC 2023 rates, closest to real US population dynamics",
        "patches": {
            "parameters.move_rate_pct": 11.8,
            "parameters.cohabit_rate_pct": 6.1,
            "parameters.divorce_rate_pct": 2.4,
            "parameters.birth_rate_pct": 11.0,
            "emission.overlap_entity_pct": 70,
            "emission.crossfile_match_mode": "one_to_many",
        },
    },
    "single_dataset_clean": {
        "description": "Single observed file with low duplication and minimal noise for dedup sanity checks",
        "patches": {
            "emission.crossfile_match_mode": "single_dataset",
            "emission.datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 2.0,
                    "noise": {
                        "name_typo_pct": 0.5,
                        "dob_shift_pct": 0.2,
                        "ssn_mask_pct": 0.5,
                        "phone_mask_pct": 0.3,
                        "address_missing_pct": 0.5,
                        "middle_name_missing_pct": 10.0,
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
    },
    "single_dataset_dedup": {
        "description": "Single observed file with meaningful duplication and moderate noise for within-file dedup benchmarking",
        "patches": {
            "emission.crossfile_match_mode": "single_dataset",
            "emission.datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 14.0,
                    "noise": {
                        "name_typo_pct": 2.5,
                        "dob_shift_pct": 1.0,
                        "ssn_mask_pct": 4.0,
                        "phone_mask_pct": 1.5,
                        "address_missing_pct": 1.5,
                        "middle_name_missing_pct": 25.0,
                        "phonetic_error_pct": 1.0,
                        "ocr_error_pct": 0.5,
                        "date_swap_pct": 0.2,
                        "zip_digit_error_pct": 0.5,
                        "nickname_pct": 3.0,
                        "suffix_missing_pct": 1.0,
                    },
                }
            ],
        },
    },
}


def get_preset(name: str) -> dict[str, Any]:
    """Return a preset by name, raising ValueError if not found."""
    if name not in PRESETS:
        available = list(PRESETS.keys())
        raise ValueError(f"Unknown preset {name!r}. Available: {available}")
    return PRESETS[name]


def list_presets() -> dict[str, str]:
    """Return {name: description} for all presets."""
    return {name: p["description"] for name, p in PRESETS.items()}
