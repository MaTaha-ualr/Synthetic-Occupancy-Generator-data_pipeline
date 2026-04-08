# SOG Frontend Runbook

## Purpose

Run the local Streamlit frontend for:

- scenario drafting and edits
- Phase-2 orchestration
- result summaries and chart rendering
- artifact download and export packaging

## Entry Point

Use `frontend/chatbot_production.py` as the supported Streamlit entrypoint.

That wrapper executes `frontend/chatbot.py` directly on each rerun so the live app logic stays in one place.

## Startup

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -u -m streamlit run frontend/chatbot_production.py --server.headless true
```

Or use the helper launcher:

```powershell
.\run_frontend.ps1
```

Then open `http://localhost:8501`.

## Runtime State

The frontend writes transient state under:

- `phase2/.sog_jobs/`
- `phase2/.sog_sessions/`
- `phase2/.sog_charts/`
- `phase2/.sog_exports/`

These folders are generated local state. They can be cleared before a fresh run or before publishing the repository.

## Troubleshooting

- Import or module errors: start the app from the repository root.
- The page loads but actions are disabled: set `ANTHROPIC_API_KEY` in the environment or enter it in the UI when prompted.
- A job appears stuck: inspect the newest file under `phase2/.sog_jobs/` and refresh the page.
- Charts or exports look stale: clear the generated `.sog_*` folders and rerun the scenario.
- No URL appears in the terminal: use `python -u` or `.\run_frontend.ps1` so startup logs flush immediately.
