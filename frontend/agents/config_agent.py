"""ConfigAgent - natural language to scenario configuration specialist."""

from __future__ import annotations

from typing import Any

from .base import AgentResponse, BaseAgent

_SYSTEM_PROMPT = """\
You are ConfigAgent, the scenario configuration specialist for SOG (Synthetic Occupancy Generator).

YOUR JOB
- Translate natural-language requests into exact scenario patches.
- Return a JSON object with keys: scenario_id, patches_applied, validation_errors, ready_to_run, summary.
- summary must be exactly one sentence.
- Never ask clarifying questions unless the request is genuinely ambiguous between different scenario families.
- If the request is ambiguous only about parameter values, choose the most common interpretation and proceed.

SCENARIO TEMPLATES
- single_movers: residential mobility, key parameter parameters.move_rate_pct
- couple_merge: cohabitation, key parameter parameters.cohabit_rate_pct
- family_birth: births in households, key parameter parameters.birth_rate_pct
- divorce_custody: divorce and custody, key parameter parameters.divorce_rate_pct
- roommates_split: household dissolution, key parameter parameters.split_rate_pct

NOISE FIELDS
These fields can appear under legacy emission.noise.A / emission.noise.B or under each entry in emission.datasets[*].noise.
- name_typo_pct
- dob_shift_pct
- ssn_mask_pct
- phone_mask_pct
- address_missing_pct
- middle_name_missing_pct
- phonetic_error_pct
- ocr_error_pct
- date_swap_pct
- zip_digit_error_pct
- nickname_pct
- suffix_missing_pct

OBSERVED DATASET TOPOLOGY
SOG now supports two emission shapes:

1. Legacy pairwise linkage mode
- Uses emission.crossfile_match_mode in one_to_one | one_to_many | many_to_one | many_to_many
- Usually keeps legacy fields:
  - emission.overlap_entity_pct
  - emission.appearance_A_pct
  - emission.appearance_B_pct
  - emission.duplication_in_A_pct
  - emission.duplication_in_B_pct
  - emission.noise.A.*
  - emission.noise.B.*
- Produces two observed datasets plus entity_record_map and truth_crosswalk.

2. Canonical dataset-list mode
- Uses emission.datasets as a list of one or two dataset configs.
- Each dataset config supports:
  - dataset_id
  - filename
  - snapshot: simulation_start | simulation_end
  - appearance_pct
  - duplication_pct
  - noise.<all noise fields>
- If crossfile_match_mode is single_dataset, configure exactly one entry in emission.datasets.
- Single-dataset mode produces one observed dataset plus entity_record_map only, with no truth_crosswalk.
- Pairwise dataset-list mode may still use A/B as dataset ids for backward compatibility.

MATCH MODES
- single_dataset: one observed file for deduplication benchmarking
- one_to_one: each overlapping entity links to one record per side
- one_to_many: one entity can map to multiple records in the second dataset
- many_to_one: multiple records in the first dataset can collapse to one in the second
- many_to_many: ambiguity on both sides

SIMULATION PARAMETERS
- simulation.granularity: monthly | daily
- simulation.start_date: YYYY-MM-DD
- simulation.periods: integer > 0

SELECTION PARAMETERS
- selection.sample.mode: all | count | pct
- selection.sample.value
- selection.filters.age_bins: ["age_0_17","age_18_34","age_35_64","age_65_plus"]
- selection.filters.mobility_propensity_buckets: ["low","medium","high"]
- selection.thresholds.mobility_low_max
- selection.thresholds.mobility_high_min

CONSTRAINT PARAMETERS
- constraints.min_marriage_age
- constraints.max_partner_age_gap
- constraints.fertility_age_range.min
- constraints.fertility_age_range.max
- constraints.allow_underage_marriage
- constraints.allow_child_lives_alone

INTENT TO PATCH RECIPES

"hard ER test" / "challenging match" / "difficult"
- If the user is in pairwise mode, set:
  - emission.crossfile_match_mode -> many_to_many
  - emission.overlap_entity_pct -> 40-55
  - emission.duplication_in_A_pct -> 10-15
  - emission.duplication_in_B_pct -> 15-20
  - make Dataset B noisier than Dataset A
- If the user is in single-dataset mode, keep single_dataset and increase duplication plus name noise in the single dataset.

"easy baseline" / "simple" / "clean"
- For pairwise mode:
  - emission.crossfile_match_mode -> one_to_one
  - emission.overlap_entity_pct -> 80-90
  - all noise fields <= 1.0
  - duplication <= 3
- For single-dataset mode:
  - emission.crossfile_match_mode -> single_dataset
  - single dataset appearance_pct -> 100
  - single dataset duplication_pct -> 1-3
  - all noise fields <= 1.0

"single file" / "single dataset" / "dedup only" / "within-file dedup"
- Set emission.crossfile_match_mode -> single_dataset
- Replace emission.datasets with exactly one dataset entry
- Preferred default dataset entry:
  {
    "dataset_id": "registry",
    "filename": "observed_registry.csv",
    "snapshot": "simulation_end",
    "appearance_pct": 100.0,
    "duplication_pct": 8.0,
    "noise": {
      "name_typo_pct": 1.0,
      "dob_shift_pct": 0.5,
      "ssn_mask_pct": 2.0,
      "phone_mask_pct": 1.0,
      "address_missing_pct": 1.0,
      "middle_name_missing_pct": 20.0,
      "phonetic_error_pct": 0.5,
      "ocr_error_pct": 0.2,
      "date_swap_pct": 0.0,
      "zip_digit_error_pct": 0.5,
      "nickname_pct": 1.0,
      "suffix_missing_pct": 0.5
    }
  }

"two file linkage" / "cross-file linkage" / "A and B"
- Use pairwise mode and two datasets or legacy A/B fields.
- If the user explicitly asks for A/B, keep dataset ids A and B.

"lots of moves" / "high mobility" / "address churn"
- parameters.move_rate_pct -> 20-30
- simulation.periods -> 18-24

"high duplication" / "many duplicates"
- In pairwise mode:
  - emission.duplication_in_A_pct -> 12-18
  - emission.duplication_in_B_pct -> 15-22
- In dataset-list mode:
  - increase each relevant emission.datasets[*].duplication_pct into the 12-22 range

"realistic" / "census rates"
- parameters.move_rate_pct -> 11.8
- parameters.cohabit_rate_pct -> 6.1
- parameters.divorce_rate_pct -> 2.4
- parameters.birth_rate_pct -> 11.0

"noisy Dataset B" / "B much noisier"
- In pairwise mode, make all B-side noise values 2x to 3x the A-side values.
- In dataset-list mode, make the second dataset 2x to 3x noisier than the first.

"more noise" / "increase noise"
- Multiply current noise values by about 1.5x to 2x after reading the current scenario.

PRESET NAMES
- baseline_easy
- realistic_medium
- hard_noise
- extreme_stress
- high_mobility
- census_realistic
- single_dataset_clean
- single_dataset_dedup

WORKFLOW
1. If the request names a preset, call apply_difficulty_preset.
2. If the request is custom, call create_scenario_from_template or update_scenario with exact patches.
3. If the user is modifying the last scenario, prefer update_scenario.
4. Always validate after patching and do not report ready_to_run=true if validation errors remain.
5. Return a one-sentence summary and no YAML dump.
"""

_TOOLS = [
    {
        "name": "list_scenarios",
        "description": "List available scenario templates.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "read_scenario",
        "description": "Read current config of a scenario (prefer working copy).",
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
        "name": "update_scenario",
        "description": "Apply dot-path patches to the working copy of a scenario.",
        "input_schema": {
            "type": "object",
            "properties": {
                "scenario_id": {"type": "string"},
                "patches": {
                    "type": "object",
                    "description": "Flat dict: dot-path to value. Example: {'emission.datasets': [{'dataset_id': 'registry', 'snapshot': 'simulation_end', 'appearance_pct': 100.0, 'duplication_pct': 8.0, 'noise': {'name_typo_pct': 1.0}}]}",
                },
                "session_id": {"type": "string"},
            },
            "required": ["scenario_id", "patches"],
        },
    },
    {
        "name": "validate_scenario",
        "description": "Validate a scenario YAML without running it.",
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
        "name": "create_scenario_from_template",
        "description": "Clone a template and apply patches to create a new scenario working copy.",
        "input_schema": {
            "type": "object",
            "properties": {
                "template_id": {"type": "string"},
                "new_id": {"type": "string"},
                "patches": {"type": "object"},
                "session_id": {"type": "string"},
            },
            "required": ["template_id", "new_id", "patches"],
        },
    },
    {
        "name": "get_schema_info",
        "description": "Return live schema for a config section (selection|simulation|emission|quality|constraints|events).",
        "input_schema": {
            "type": "object",
            "properties": {"section": {"type": "string"}},
            "required": ["section"],
        },
    },
    {
        "name": "list_difficulty_presets",
        "description": "List all named difficulty presets.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "apply_difficulty_preset",
        "description": "Apply a named difficulty preset to a scenario.",
        "input_schema": {
            "type": "object",
            "properties": {
                "scenario_id": {"type": "string"},
                "preset_name": {"type": "string"},
                "session_id": {"type": "string"},
            },
            "required": ["scenario_id", "preset_name"],
        },
    },
]


class ConfigAgent(BaseAgent):
    """Translates natural language into exact YAML patches."""

    def __init__(self, api_key: str | None = None, provider: str | None = None):
        super().__init__(api_key=api_key, provider=provider)

    def get_system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def get_tools(self) -> list[dict[str, Any]]:
        return _TOOLS

    def dispatch_tool(self, name: str, inputs: dict[str, Any], session_id: str) -> dict[str, Any]:
        import sog_tools as t

        inputs_with_session = {**inputs, "session_id": session_id}
        try:
            if name == "list_scenarios":
                return t.list_scenarios()
            if name == "read_scenario":
                return t.read_scenario(**inputs_with_session)
            if name == "update_scenario":
                return t.update_scenario(**inputs_with_session)
            if name == "validate_scenario":
                return t.validate_scenario(**inputs_with_session)
            if name == "create_scenario_from_template":
                return t.create_scenario_from_template(**inputs_with_session)
            if name == "get_schema_info":
                return t.get_schema_info(**inputs)
            if name == "list_difficulty_presets":
                return t.list_difficulty_presets()
            if name == "apply_difficulty_preset":
                return t.apply_difficulty_preset(**inputs_with_session)
            return {"error": f"Unknown tool: {name}"}
        except Exception as exc:
            return {"error": str(exc)}

    def run(
        self,
        user_input: str,
        session_id: str,
        context: dict[str, Any],
    ) -> AgentResponse:
        """Process a configuration request and return an AgentResponse."""
        messages: list[dict[str, Any]] = []

        ctx_parts = []
        if context.get("last_scenario_id"):
            ctx_parts.append(f"Last configured scenario: {context['last_scenario_id']}")
        if context.get("last_run_id"):
            ctx_parts.append(f"Last run: {context['last_run_id']}")
        if ctx_parts:
            messages.append(
                {
                    "role": "user",
                    "content": "[Session context]\n" + "\n".join(ctx_parts),
                }
            )
            messages.append(
                {
                    "role": "assistant",
                    "content": "Understood. Ready to configure.",
                }
            )

        messages.append({"role": "user", "content": user_input})

        try:
            text, data = self.run_tool_loop(messages, session_id)
            scenario_id = data.get("scenario_id") or context.get("last_scenario_id", "")
            return AgentResponse(
                success=True,
                message=text,
                data=data,
                session_updates={"last_scenario_id": scenario_id} if scenario_id else {},
            )
        except Exception as exc:
            return AgentResponse(
                success=False,
                message=f"Configuration failed: {exc}",
            )
