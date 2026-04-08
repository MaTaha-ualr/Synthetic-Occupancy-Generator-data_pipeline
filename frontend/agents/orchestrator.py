"""Orchestrator — Routes user messages to specialist agents.

Uses a lightweight LLM call (Haiku) to classify intent, then delegates
to ConfigAgent, RunAgent, AnalystAgent, or ExportAgent.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from .base import AgentResponse


# Intent classification prompt — kept short so Haiku is fast
_CLASSIFY_PROMPT = """\
Classify the user message into exactly ONE of these intents:
  configure  — configure, tune, adjust, create, modify, change, preset, noise, overlap, rates, difficulty
  run        — run, build, generate, execute, start, launch, go ahead, submit (when scenario already configured)
  analyze    — results, metrics, quality, how did it go, show me, report, difficulty rating, summary, charts, visualize
  export     — export, download, Splink, Zingg, zip, package, format for

Reply with ONLY the single word intent. No explanation.
"""

_WANTS_RUN_KEYWORDS = frozenset([
    "run", "build", "generate", "execute", "start", "launch",
    "go ahead", "go", "proceed", "submit", "do it",
])

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCENARIOS_DIR = PROJECT_ROOT / "phase2" / "scenarios"


class Orchestrator:
    """Routes messages to specialist agents. Agents are initialized lazily."""

    def __init__(self, api_key: str | None = None):
        self._api_key = api_key
        self._client = None
        self._config_agent = None
        self._run_agent = None
        self._analyst_agent = None
        self._export_agent = None

    def _get_client(self):
        if self._client is None:
            import anthropic
            key = self._api_key or os.environ.get("ANTHROPIC_API_KEY")
            if not key:
                raise ValueError("ANTHROPIC_API_KEY not set")
            self._client = anthropic.Anthropic(api_key=key)
        return self._client

    def _config(self):
        if self._config_agent is None:
            from .config_agent import ConfigAgent
            self._config_agent = ConfigAgent(api_key=self._api_key)
        return self._config_agent

    def _run(self):
        if self._run_agent is None:
            from .run_agent import RunAgent
            self._run_agent = RunAgent(api_key=self._api_key)
        return self._run_agent

    def _analyst(self):
        if self._analyst_agent is None:
            from .analyst_agent import AnalystAgent
            self._analyst_agent = AnalystAgent(api_key=self._api_key)
        return self._analyst_agent

    def _export(self):
        if self._export_agent is None:
            from .export_agent import ExportAgent
            self._export_agent = ExportAgent(api_key=self._api_key)
        return self._export_agent

    # ------------------------------------------------------------------
    # Intent classification
    # ------------------------------------------------------------------

    def classify_intent(self, user_input: str) -> str:
        """Use Haiku to classify intent into configure | run | analyze | export."""
        try:
            client = self._get_client()
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=10,
                system=_CLASSIFY_PROMPT,
                messages=[{"role": "user", "content": user_input[:500]}],
            )
            raw = ""
            for block in response.content:
                if hasattr(block, "text"):
                    raw += block.text
            intent = raw.strip().lower().split()[0] if raw.strip() else "configure"
            return intent if intent in ("configure", "run", "analyze", "export") else "configure"
        except Exception:
            # Fallback: keyword-based
            text = user_input.lower()
            if any(w in text for w in ["export", "splink", "zingg", "zip", "package"]):
                return "export"
            if any(w in text for w in ["result", "metric", "quality", "how did", "show me", "chart", "visual", "report", "difficulty"]):
                return "analyze"
            if any(w in text for w in _WANTS_RUN_KEYWORDS):
                return "run"
            return "configure"

    def _user_wants_run(self, user_input: str) -> bool:
        """Check if the user's message also requests execution."""
        text = user_input.lower()
        return any(kw in text for kw in _WANTS_RUN_KEYWORDS)

    def _known_runnable_scenario_ids(self) -> list[str]:
        scenario_ids: list[str] = []
        try:
            from sog_phase2.scenario_catalog import get_scenario_catalog_entries

            for entry in get_scenario_catalog_entries():
                if str(entry.get("status", "")).strip() != "supported":
                    continue
                scenario_id = str(entry.get("scenario_id", "")).strip()
                if not scenario_id:
                    continue
                if (SCENARIOS_DIR / f"{scenario_id}.yaml").exists():
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

    # ------------------------------------------------------------------
    # Main turn handler
    # ------------------------------------------------------------------

    def run_turn(
        self,
        user_input: str,
        session_id: str,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Process one user turn. Returns:
          {message, session_updates, pending_job_id?, charts?, download_paths?}
        """
        intent = self.classify_intent(user_input)

        # ── configure ────────────────────────────────────────────────
        if intent == "configure":
            config_result = self._config().run(user_input, session_id, context)
            merged_context = {**context, **config_result.session_updates}

            # If user also said run/build/go, chain to RunAgent
            if self._user_wants_run(user_input) and config_result.success:
                scenario_id = (
                    config_result.session_updates.get("last_scenario_id")
                    or merged_context.get("last_scenario_id", "")
                )
                if scenario_id:
                    run_result = self._run().run(scenario_id, session_id, merged_context)
                    combined_message = config_result.message
                    if run_result.message:
                        combined_message += "\n\n" + run_result.message
                    return {
                        "message": combined_message,
                        "session_updates": {**config_result.session_updates, **run_result.session_updates},
                        "pending_job_id": run_result.pending_job_id,
                    }

            return {
                "message": config_result.message,
                "session_updates": config_result.session_updates,
            }

        # ── run ──────────────────────────────────────────────────────
        if intent == "run":
            scenario_id = context.get("last_scenario_id", "")
            if not scenario_id:
                scenario_id = self._infer_scenario_id(user_input)
                if not scenario_id:
                    # Nothing configured yet — delegate to ConfigAgent
                    config_result = self._config().run(user_input, session_id, context)
                    scenario_id = config_result.session_updates.get("last_scenario_id", "")
                    if scenario_id and config_result.success:
                        run_result = self._run().run(
                            scenario_id, session_id,
                            {**context, **config_result.session_updates},
                        )
                        return {
                            "message": config_result.message + "\n\n" + run_result.message,
                            "session_updates": {**config_result.session_updates, **run_result.session_updates},
                            "pending_job_id": run_result.pending_job_id,
                        }
                    return {
                        "message": config_result.message,
                        "session_updates": config_result.session_updates,
                    }

            run_result = self._run().run(scenario_id, session_id, context)
            return {
                "message": run_result.message,
                "session_updates": run_result.session_updates,
                "pending_job_id": run_result.pending_job_id,
            }

        # ── analyze ──────────────────────────────────────────────────
        if intent == "analyze":
            analyst_result = self._analyst().run(user_input, session_id, context)
            out: dict[str, Any] = {
                "message": analyst_result.message,
                "session_updates": analyst_result.session_updates,
            }
            if analyst_result.charts:
                out["charts"] = analyst_result.charts
            if analyst_result.data.get("download_paths"):
                out["download_paths"] = analyst_result.data["download_paths"]
            return out

        # ── export ───────────────────────────────────────────────────
        if intent == "export":
            export_result = self._export().run(user_input, session_id, context)
            out = {
                "message": export_result.message,
                "session_updates": export_result.session_updates,
            }
            if export_result.data.get("zip_path"):
                out["export_zip"] = export_result.data["zip_path"]
            elif export_result.data.get("output_dir"):
                out["export_dir"] = export_result.data["output_dir"]
            return out

        # Fallback
        return {
            "message": "I'm not sure how to help with that. Try describing what you want to build or analyze.",
            "session_updates": {},
        }
