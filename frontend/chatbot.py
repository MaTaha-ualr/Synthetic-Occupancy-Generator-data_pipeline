"""SOG Assistant — Streamlit chatbot powered by Claude API.

Start with:
    cd SOG
    streamlit run frontend/chatbot.py

Requires ANTHROPIC_API_KEY environment variable.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

# Load .env from the project root (SOG/.env) if it exists
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass  # python-dotenv not installed — fall back to environment variable

import streamlit as st

# Make sure the project src is importable
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))
if str(PROJECT_ROOT / "frontend") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "frontend"))

from sog_tools import (  # noqa: E402
    get_run_results,
    list_scenarios,
    read_scenario,
    run_scenario,
    update_scenario,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="SOG Assistant",
    page_icon="🗂️",
    layout="centered",
)
st.title("SOG Dataset Generator")
st.caption("Describe what you want — the assistant will configure and run the pipeline for you.")

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

if "messages" not in st.session_state:
    st.session_state.messages = []
if "session_id" not in st.session_state:
    st.session_state.session_id = uuid.uuid4().hex[:8]
if "last_run_downloads" not in st.session_state:
    st.session_state.last_run_downloads = {}

SESSION_ID: str = st.session_state.session_id

# ---------------------------------------------------------------------------
# Tool schema
# ---------------------------------------------------------------------------

TOOLS_SCHEMA = [
    {
        "name": "list_scenarios",
        "description": (
            "List all available Phase-2 scenario templates. Call this first when the user "
            "asks what scenarios exist or wants to pick one."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "read_scenario",
        "description": (
            "Read the full configuration of a specific scenario template. Use this to show "
            "the user the current parameter values before suggesting changes."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "scenario_id": {
                    "type": "string",
                    "description": "The scenario template ID, e.g. 'single_movers'.",
                }
            },
            "required": ["scenario_id"],
        },
    },
    {
        "name": "update_scenario",
        "description": (
            "Apply parameter changes to a scenario using dot-notation paths. "
            "Examples: 'parameters.move_rate_pct', 'emission.noise.B.phonetic_error_pct'. "
            "Always validate before running. Show the user a summary of what changed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "scenario_id": {"type": "string"},
                "patches": {
                    "type": "object",
                    "description": (
                        "Flat dict of dot-path keys to new values. "
                        "E.g. {\"parameters.move_rate_pct\": 25.0, "
                        "\"emission.noise.B.phonetic_error_pct\": 3.0}"
                    ),
                },
            },
            "required": ["scenario_id", "patches"],
        },
    },
    {
        "name": "run_scenario",
        "description": (
            "Run the full Phase-2 pipeline for a scenario "
            "(selection → truth simulation → observed emission → quality → validation). "
            "This takes 10–60 seconds. Always confirm with the user before calling."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "scenario_id": {"type": "string"},
                "seed": {
                    "type": "integer",
                    "description": "Optional seed override. If omitted uses the scenario's seed.",
                },
            },
            "required": ["scenario_id"],
        },
    },
    {
        "name": "get_run_results",
        "description": (
            "Get quality metrics, event counts, and download links for a completed run. "
            "Call this after run_scenario returns a run_id."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {
                    "type": "string",
                    "description": "Run ID in format YYYY-MM-DD_<scenario_id>_seed<seed>.",
                }
            },
            "required": ["run_id"],
        },
    },
]

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are the SOG (Synthetic Population Generator) assistant. You help researchers generate synthetic person-resolution benchmark datasets for entity resolution (ER) testing.

## Scenario types
- **single_movers**: People who move addresses. Key param: `parameters.move_rate_pct` (default 12%).
- **couple_merge**: Couples moving in together. Key param: `parameters.cohabit_rate_pct` (default 4%).
- **family_birth**: Births within households. Key param: `parameters.birth_rate_pct` (default 3.2%).
- **divorce_custody**: Divorcing couples. Key params: `parameters.cohabit_rate_pct`, `parameters.divorce_rate_pct`.
- **roommates_split**: Roommate households splitting. Key params: `parameters.move_rate_pct`, `parameters.split_rate_pct`.

## Tunable parameters (dot-notation paths)

### Event rates (parameters section)
- `parameters.move_rate_pct` — % of population moving per year
- `parameters.cohabit_rate_pct` — % forming partnerships per year
- `parameters.birth_rate_pct` — % of fertile population giving birth per year
- `parameters.divorce_rate_pct` — % of couples divorcing per year
- `parameters.split_rate_pct` — % of shared households splitting per year

### Simulation timing
- `simulation.start_date` — ISO date (YYYY-MM-DD)
- `simulation.periods` — number of months to simulate (default 12)

### Dataset overlap and duplication (emission section)
- `emission.overlap_entity_pct` — % of entities in both DatasetA and DatasetB (0–100)
- `emission.appearance_A_pct` / `emission.appearance_B_pct` — coverage per dataset
- `emission.duplication_in_A_pct` / `emission.duplication_in_B_pct` — duplicate record rates
- `emission.crossfile_match_mode` — one_to_one | one_to_many | many_to_one | many_to_many

### Noise models (emission.noise.A and emission.noise.B)
Original noise types:
- `name_typo_pct` — random character substitution in names
- `dob_shift_pct` — shift DOB by ±3 days
- `ssn_mask_pct` — mask SSN to ***-**-XXXX
- `phone_mask_pct` — remove phone number
- `address_missing_pct` — clear address
- `middle_name_missing_pct` — remove middle name

Enhanced noise types (novel — higher values = harder ER problem):
- `phonetic_error_pct` — mutate name to phonetically similar form (Smith→Smyth). Great for testing phonetic-blocking algorithms.
- `ocr_error_pct` — OCR character confusion (O↔0, l↔1, rn↔m). Use for historical records.
- `date_swap_pct` — swap month and day in DOB (01/15 → 15/01 when valid).
- `zip_digit_error_pct` — substitute one digit in ZIP code. Useful for address-blocking tests.
- `nickname_pct` — replace formal first name with nickname (Robert→Bobby, Jennifer→Jen).
- `suffix_missing_pct` — remove Jr./Sr./III suffix.

Dataset A is typically cleaner; Dataset B is noisier. Set higher values in B.

## Your workflow
1. Ask which scenario type fits the user's use case (or call list_scenarios to show options)
2. Read the current scenario config with read_scenario
3. Suggest parameter changes based on what the user describes, then call update_scenario
4. **Always confirm with the user before calling run_scenario** (it takes 10–60 seconds)
5. After run_scenario returns a run_id, call get_run_results and show a clean summary

## Response style
- Users are researchers who understand ER benchmarking — be concise
- Show parameter tables in markdown format
- After a run completes: show event counts by type, DatasetA/B row counts, overlap entity count, quality status
- If a run fails with FileExistsError, explain the run already exists and suggest overwrite=True
- Noise guidance: higher phonetic + OCR = harder problem (resembles scanned/historical records); higher nickname_pct = harder name-based blocking"""

# ---------------------------------------------------------------------------
# Tool dispatcher
# ---------------------------------------------------------------------------

def _dispatch_tool(name: str, inputs: dict[str, Any]) -> dict[str, Any]:
    try:
        if name == "list_scenarios":
            return list_scenarios()
        elif name == "read_scenario":
            return read_scenario(session_id=SESSION_ID, **inputs)
        elif name == "update_scenario":
            return update_scenario(session_id=SESSION_ID, **inputs)
        elif name == "run_scenario":
            return run_scenario(session_id=SESSION_ID, **inputs)
        elif name == "get_run_results":
            return get_run_results(**inputs)
        else:
            return {"error": f"Unknown tool: {name}"}
    except Exception as exc:
        return {"error": str(exc)}


def _extract_text(response: Any) -> str:
    parts = []
    for block in response.content:
        if hasattr(block, "text"):
            parts.append(block.text)
    return "\n".join(parts).strip()


# ---------------------------------------------------------------------------
# Agentic Claude loop
# ---------------------------------------------------------------------------

def _run_claude_turn(messages: list[dict]) -> tuple[str, dict[str, str]]:
    """Run Claude with tool use until end_turn. Returns (text, download_paths)."""
    import anthropic

    client = anthropic.Anthropic()
    working = list(messages)
    downloads: dict[str, str] = {}

    while True:
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS_SCHEMA,
            messages=working,
        )

        if response.stop_reason == "end_turn":
            return _extract_text(response), downloads

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = _dispatch_tool(block.name, block.input)
                    # Capture download paths from get_run_results
                    if block.name == "get_run_results" and "download_paths" in result:
                        downloads = result["download_paths"]
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result),
                    })
            working.append({"role": "assistant", "content": response.content})
            working.append({"role": "user", "content": tool_results})
        else:
            # Unexpected stop reason — return whatever text is available
            return _extract_text(response), downloads


# ---------------------------------------------------------------------------
# Render chat history
# ---------------------------------------------------------------------------

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Render download buttons from last completed run
if st.session_state.last_run_downloads:
    st.divider()
    st.caption("Downloads from last run:")
    cols = st.columns(len(st.session_state.last_run_downloads))
    for col, (label, path_str) in zip(cols, st.session_state.last_run_downloads.items()):
        p = Path(path_str)
        if p.exists():
            with open(p, "rb") as f:
                col.download_button(
                    label=f"⬇ {label}",
                    data=f.read(),
                    file_name=p.name,
                    key=f"dl_{label}",
                )

# ---------------------------------------------------------------------------
# Chat input
# ---------------------------------------------------------------------------

if not os.environ.get("ANTHROPIC_API_KEY"):
    st.error(
        "ANTHROPIC_API_KEY environment variable is not set. "
        "Set it before running: `set ANTHROPIC_API_KEY=sk-ant-...` (Windows) "
        "or `export ANTHROPIC_API_KEY=sk-ant-...` (Unix)."
    )
    st.stop()

if prompt := st.chat_input("Describe what you want to generate…"):
    # Append and display user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Run Claude turn
    with st.chat_message("assistant"):
        with st.spinner("Working…"):
            try:
                text, downloads = _run_claude_turn(st.session_state.messages)
            except Exception as exc:
                text = f"Error communicating with Claude API: {exc}"
                downloads = {}
        st.markdown(text)

    # Update session state
    st.session_state.messages.append({"role": "assistant", "content": text})
    if downloads:
        st.session_state.last_run_downloads = downloads
        st.rerun()
