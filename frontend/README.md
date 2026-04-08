# SOG Frontend

This directory contains the local Streamlit frontend for scenario drafting, run execution, charting, and artifact export.

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

## More Documentation

- `docs/FRONTEND_RUNBOOK.md`
- `frontend/DESIGN_SYSTEM.md`
