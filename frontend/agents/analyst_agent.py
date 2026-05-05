"""AnalystAgent — ER-focused quality interpretation and visualization.

Interprets run results and charts in terms of entity resolution difficulty.
Never dumps raw JSON. Always provides a difficulty rating and actionable insight.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .base import AgentResponse, BaseAgent

_SYSTEM_PROMPT = """\
You are AnalystAgent, the ER (Entity Resolution) quality analyst for SOG.

YOUR JOB: Interpret run results and explain what they mean for entity resolution testing.
Use the compact report format below. Never dump raw JSON. Always include a difficulty rating.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DIFFICULTY RUBRIC  (score out of 7)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
overlap_entity_pct:
  < 40%  → +3 (severe recall pressure)
  40-60% → +2
  60-75% → +1
  > 75%  → +0

name noise (phonetic + ocr + nickname combined A+B):
  > 10%  → +2  (name blocking will fail)
  5-10%  → +1
  < 5%   → +0

duplication (A or B > 10%):
  → +1  (precision challenge)

match_mode = many_to_many:
  → +1  (cardinality confusion)

Score → Rating:
  0-1: VERY EASY | 2-3: MEDIUM | 4-5: HARD | 6+: VERY HARD

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMPACT REPORT FORMAT  (use this every time)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Run: {run_id}
Scenario: {scenario_id}
Population: {N} people | Events: {N}
Dataset A: {N} rows | Dataset B: {N} rows
Overlap: {N}% | Match mode: {mode}
Dup rate — A: {N}% | B: {N}%
ER difficulty: {RATING}
Quality: {status}

Key:
- {one sentence: primary matching challenge}
- {one sentence: blocking strategy recommendation}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCKING STRATEGY RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
name noise > 8%       → Use fuzzy blocking (Soundex/Metaphone) + similarity thresholds
overlap < 50%         → Conservative blocking: prioritize precision over recall
duplication > 15%     → Deduplicate within each dataset first, then block across
many_to_many mode     → Ensure blocking keys handle cardinality (don't assume 1-to-1)
otherwise             → Standard blocking: LastName + DOB year usually sufficient

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHART INTERPRETATION HINTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
noise_radar:     A vs B asymmetry shows which dataset to weight lower in scoring
overlap_venn:    overlap < 50% means expect many non-matches; precision is critical
missing_matrix:  field missing > 20% in B → don't rely on it for blocking
scorecard:       three gauges (overlap, noise, duplication) show where difficulty comes from

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMPARE FORMAT  (when comparing two runs)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Use a markdown table with columns: Metric | Run A | Run B | Delta
Include: overlap%, match_mode, dup_A%, dup_B%, name_noise, difficulty_rating
End with one sentence: what changed and whether it got harder or easier.

SCENARIO TEMPLATE ANALYSIS
- If the user names a supported scenario id and asks to open, explain, inspect, or walk through it, treat that as scenario-template analysis, not run analysis.
- Do not assume a scenario id is a run id.
- For scenario-template analysis, explain topology, match mode, overlap target, duplication target, noise stance, primary event focus, and ER difficulty.
"""

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCENARIOS_DIR = PROJECT_ROOT / "phase2" / "scenarios"
_RUN_ID_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}_[A-Za-z0-9_]+_seed\d+\b")
_SCENARIO_HINTS = (
    "open",
    "explain",
    "inspect",
    "walk through",
    "walk me through",
    "tell me about",
    "what is",
    "focus on",
    "scenario",
    "template",
)

_TOOLS = [
    {
        "name": "list_scenarios",
        "description": "List available scenario templates.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "read_scenario",
        "description": "Read a scenario template or working copy.",
        "input_schema": {
            "type": "object",
            "properties": {"scenario_id": {"type": "string"}},
            "required": ["scenario_id"],
        },
    },
    {
        "name": "get_run_results",
        "description": "Get quality metrics and download paths for a completed run.",
        "input_schema": {
            "type": "object",
            "properties": {"run_id": {"type": "string"}},
            "required": ["run_id"],
        },
    },
    {
        "name": "summarize_run_for_er",
        "description": "Get ER-focused summary with difficulty score and rating.",
        "input_schema": {
            "type": "object",
            "properties": {"run_id": {"type": "string"}},
            "required": ["run_id"],
        },
    },
    {
        "name": "compare_runs",
        "description": "Diff two runs — quality metrics and scenario parameters.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id_a": {"type": "string"},
                "run_id_b": {"type": "string"},
            },
            "required": ["run_id_a", "run_id_b"],
        },
    },
    {
        "name": "list_recent_runs",
        "description": "List recent completed runs.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer"},
                "scenario_id_filter": {"type": "string"},
            },
        },
    },
    {
        "name": "generate_chart",
        "description": (
            "Generate a visualization chart. "
            "chart_type: noise_radar | overlap_venn | difficulty_scorecard | "
            "missing_matrix | age_distribution | event_type_bar. "
            "fmt: png (default) | html (interactive, recommended for noise_radar)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "chart_type": {"type": "string"},
                "fmt": {"type": "string"},
            },
            "required": ["run_id", "chart_type"],
        },
    },
    {
        "name": "generate_dashboard",
        "description": "Generate the standard 4-chart dashboard for a run.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "include_charts": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["run_id"],
        },
    },
]


class AnalystAgent(BaseAgent):
    """Interprets run quality in ER terms and generates charts."""

    def __init__(self, api_key: str | None = None, provider: str | None = None):
        super().__init__(api_key=api_key, provider=provider)

    def get_system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def get_tools(self) -> list[dict[str, Any]]:
        return _TOOLS

    def _known_runnable_scenario_ids(self) -> list[str]:
        scenario_ids: list[str] = []
        try:
            from sog_phase2.scenario_catalog import get_scenario_catalog_entries

            for entry in get_scenario_catalog_entries():
                if str(entry.get("status", "")).strip() != "supported":
                    continue
                scenario_id = str(entry.get("scenario_id", "")).strip()
                if scenario_id and (SCENARIOS_DIR / f"{scenario_id}.yaml").exists():
                    scenario_ids.append(scenario_id)
        except Exception:
            pass

        if not scenario_ids:
            for yaml_path in sorted(SCENARIOS_DIR.glob("*.yaml")):
                if yaml_path.stem.startswith("_") or yaml_path.name == "catalog.yaml":
                    continue
                scenario_ids.append(yaml_path.stem)

        return sorted(set(scenario_ids), key=len, reverse=True)

    def _infer_scenario_id(self, user_input: str) -> str:
        known_ids = self._known_runnable_scenario_ids()
        if not known_ids:
            return ""
        pattern = r"\b(" + "|".join(re.escape(item) for item in known_ids) + r")\b"
        match = re.search(pattern, user_input)
        return match.group(1) if match else ""

    def _looks_like_scenario_template_request(self, user_input: str) -> str:
        if _RUN_ID_PATTERN.search(user_input):
            return ""
        scenario_id = self._infer_scenario_id(user_input)
        if not scenario_id:
            return ""
        lowered = user_input.lower()
        if any(hint in lowered for hint in _SCENARIO_HINTS):
            return scenario_id
        return ""

    @staticmethod
    def _primary_events(parameters: dict[str, Any]) -> list[str]:
        event_map = {
            "move_rate_pct": "MOVE",
            "cohabit_rate_pct": "COHABIT",
            "birth_rate_pct": "BIRTH",
            "divorce_rate_pct": "DIVORCE",
            "split_rate_pct": "LEAVE_HOME",
        }
        events = []
        for key, event_name in event_map.items():
            try:
                if float(parameters.get(key, 0) or 0) > 0:
                    events.append(event_name)
            except Exception:
                continue
        return events

    @staticmethod
    def _sum_noise(noise: dict[str, Any] | None) -> float:
        if not isinstance(noise, dict):
            return 0.0
        total = 0.0
        for value in noise.values():
            try:
                total += float(value or 0)
            except Exception:
                continue
        return round(total, 2)

    @staticmethod
    def _sum_name_noise(noise: dict[str, Any] | None) -> float:
        if not isinstance(noise, dict):
            return 0.0
        total = 0.0
        for key in ("name_typo_pct", "phonetic_error_pct", "ocr_error_pct", "nickname_pct"):
            try:
                total += float(noise.get(key, 0) or 0)
            except Exception:
                continue
        return round(total, 2)

    def _explain_scenario_template(self, scenario_id: str, session_id: str) -> str:
        import sog_tools as t

        scenario = t.read_scenario(scenario_id, session_id=session_id)
        if scenario.get("error"):
            return f"Scenario template could not be opened: {scenario['error']}"

        parsed = scenario.get("parsed", {}) if isinstance(scenario.get("parsed", {}), dict) else {}
        parameters = parsed.get("parameters", {}) if isinstance(parsed.get("parameters", {}), dict) else {}
        emission = parsed.get("emission", {}) if isinstance(parsed.get("emission", {}), dict) else {}
        datasets = emission.get("datasets", []) if isinstance(emission.get("datasets", []), list) else []
        match_mode = str(emission.get("crossfile_match_mode", "one_to_one")).strip() or "one_to_one"
        overlap = emission.get("overlap_entity_pct")
        primary_events = self._primary_events(parameters)

        if datasets:
            dataset_count = len(datasets)
            if dataset_count == 1 or match_mode == "single_dataset":
                topology = "single-dataset dedup benchmark"
            elif dataset_count == 2:
                topology = "pairwise linkage benchmark"
            else:
                topology = f"{dataset_count}-dataset N-way linkage benchmark"
            duplication_summary = ", ".join(
                f"{str(item.get('dataset_id', '?')).strip()}: {float(item.get('duplication_pct', 0) or 0):.1f}%"
                for item in datasets
                if isinstance(item, dict)
            )
            noise_summary = ", ".join(
                (
                    f"{str(item.get('dataset_id', '?')).strip()} "
                    f"(name noise {self._sum_name_noise(item.get('noise', {})):.1f}%, "
                    f"total configured noise {self._sum_noise(item.get('noise', {})):.1f}%)"
                )
                for item in datasets
                if isinstance(item, dict)
            )
        else:
            topology = "single-dataset dedup benchmark" if match_mode == "single_dataset" else "pairwise linkage benchmark"
            duplication_summary = (
                f"A: {float(emission.get('duplication_in_A_pct', 0) or 0):.1f}%, "
                f"B: {float(emission.get('duplication_in_B_pct', 0) or 0):.1f}%"
            )
            noise_a = emission.get("noise", {}).get("A", {}) if isinstance(emission.get("noise", {}), dict) else {}
            noise_b = emission.get("noise", {}).get("B", {}) if isinstance(emission.get("noise", {}), dict) else {}
            noise_summary = (
                f"A (name noise {self._sum_name_noise(noise_a):.1f}%, total configured noise {self._sum_noise(noise_a):.1f}%), "
                f"B (name noise {self._sum_name_noise(noise_b):.1f}%, total configured noise {self._sum_noise(noise_b):.1f}%)"
            )

        if match_mode == "single_dataset":
            difficulty_read = "This is primarily a dedup benchmark, so the main pressure comes from within-file duplication and field corruption rather than cross-file overlap."
        elif match_mode == "many_to_many":
            difficulty_read = "This is a high-ambiguity benchmark because duplication exists on both sides and the crosswalk cannot be treated as one-to-one."
        elif match_mode in {"one_to_many", "many_to_one"}:
            difficulty_read = "This is an asymmetrical linkage benchmark, so one side carries more duplication pressure and the evaluator should expect cardinality confusion."
        elif overlap is not None and float(overlap or 0) >= 85:
            difficulty_read = "This is a relatively clean sanity benchmark because overlap is high and the linkage structure is close to one-to-one."
        else:
            difficulty_read = "This is a general linkage benchmark where difficulty comes from the balance of overlap, duplication, and dataset-specific noise."

        overlap_text = "not explicitly configured" if overlap is None else f"{float(overlap):.1f}%"
        events_text = ", ".join(primary_events) if primary_events else "no major truth event pressure"

        return (
            f"Scenario template: `{scenario_id}`.\n\n"
            f"- Topology: {topology}\n"
            f"- Match mode: `{match_mode}`\n"
            f"- Overlap target: {overlap_text}\n"
            f"- Duplication target: {duplication_summary}\n"
            f"- Noise stance: {noise_summary}\n"
            f"- Primary event focus: {events_text}\n\n"
            f"ER reading: {difficulty_read}"
        )

    def dispatch_tool(self, name: str, inputs: dict[str, Any], session_id: str) -> dict[str, Any]:
        import sog_tools as t
        try:
            if name == "list_scenarios":
                return t.list_scenarios()
            if name == "read_scenario":
                return t.read_scenario(**{**inputs, "session_id": session_id})
            if name == "get_run_results":
                return t.get_run_results(**inputs)
            if name == "summarize_run_for_er":
                return t.summarize_run_for_er(**inputs)
            if name == "compare_runs":
                return t.compare_runs(**inputs)
            if name == "list_recent_runs":
                return t.list_recent_runs(**inputs)
            if name == "generate_chart":
                return t.generate_chart(**inputs)
            if name == "generate_dashboard":
                return t.generate_dashboard(**inputs)
            return {"error": f"Unknown tool: {name}"}
        except Exception as exc:
            return {"error": str(exc)}

    def run(
        self,
        user_input: str,
        session_id: str,
        context: dict[str, Any],
    ) -> AgentResponse:
        """Analyze runs and generate charts."""
        scenario_id = self._looks_like_scenario_template_request(user_input)
        if scenario_id:
            return AgentResponse(
                success=True,
                message=self._explain_scenario_template(scenario_id, session_id),
                session_updates={"last_scenario_id": scenario_id},
            )

        messages: list[dict[str, Any]] = []

        # Provide context so agent can reference last run without user specifying run_id
        ctx_parts = []
        if context.get("last_run_id"):
            ctx_parts.append(f"Last run: {context['last_run_id']}")
        if context.get("last_scenario_id"):
            ctx_parts.append(f"Last scenario: {context['last_scenario_id']}")
        if ctx_parts:
            messages.append({
                "role": "user",
                "content": "[Session context]\n" + "\n".join(ctx_parts),
            })
            messages.append({
                "role": "assistant",
                "content": "Understood.",
            })

        messages.append({"role": "user", "content": user_input})

        try:
            text, data = self.run_tool_loop(messages, session_id)
            # Collect charts for Streamlit rendering
            charts = []
            if "charts" in data:
                charts = data["charts"]
            elif "chart_path" in data:
                charts = [{"chart_path": data["chart_path"], "insight": data.get("insight", "")}]

            updates: dict[str, Any] = {}
            if data.get("download_paths"):
                updates["last_download_paths"] = data["download_paths"]

            return AgentResponse(
                success=True,
                message=text,
                data=data,
                session_updates=updates,
                charts=charts,
            )
        except Exception as exc:
            return AgentResponse(
                success=False,
                message=f"Analysis failed: {exc}",
            )
