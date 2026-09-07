# SOG Frontend

This directory contains the local Streamlit frontend for scenario drafting, run execution, charting, and artifact export. It also includes an optional NVIDIA-backed, proposal-first natural-language scenario authoring flow.

## Supported Entry Points

- `chatbot_production.py`: supported Streamlit entrypoint
- `chatbot.py`: live application module executed by the production wrapper

Run it from the repository root:

```powershell
.\run_frontend.ps1
```

Or manually:

```powershell
python -u -m streamlit run frontend/chatbot_production.py --server.headless true
```

## Key Files

- `agents/`: natural-language orchestration helpers
- `scenario_authoring.py`: read-only NVIDIA proposal client, strict proposal contract, validation, and explicit YAML approval
- `visualizations/`: chart and theme helpers
- `sog_tools.py`: frontend-facing tool layer
- `session_manager.py`: session persistence and restore logic
- `pipeline_bridge.py`: process bridge into the pipeline scripts
- `DESIGN_SYSTEM.md`: frontend design notes

## Runtime State

The frontend writes transient files under:

- `phase2/.sog_jobs/`
- `phase2/.sog_sessions/`
- `phase2/.sog_charts/`
- `phase2/.sog_exports/`

Those folders are generated local state and are gitignored.

## Optional Model Providers

- Set `NVIDIA_API_KEY` (or enter it in the authoring panel) to turn plain-English requirements into a read-only scenario proposal. The default model is `nvidia/nemotron-3.5-lightning-30b-a3b`; `NVIDIA_SCENARIO_MODEL` and `NVIDIA_API_BASE_URL` are optional overrides.
- Set `ANTHROPIC_API_KEY` to use the existing conversational analysis, orchestration, and export agents.

The NVIDIA layer cannot write until the user explicitly approves a proposal that passed the existing SOG validators. The approved YAML remains the pipeline input and authoritative experiment specification.

## More Documentation

- `docs/FRONTEND_RUNBOOK.md`
- `docs/SCENARIO_AUTHORING.md`
- `frontend/DESIGN_SYSTEM.md`
