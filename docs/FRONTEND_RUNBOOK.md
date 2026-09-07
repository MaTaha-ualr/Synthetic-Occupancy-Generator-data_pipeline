# SOG Frontend Runbook

## Purpose

Run the local Streamlit frontend for:

- proposal-first natural-language scenario drafting
- scenario review, validation, approval, and edits
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

## Optional Natural-Language Authoring

Set an NVIDIA developer key before startup, or enter it in the password field inside the authoring panel:

```powershell
$env:NVIDIA_API_KEY = "your NVIDIA developer API key"
```

The default authoring model is `nvidia/nemotron-3.5-lightning-30b-a3b`. Use `NVIDIA_SCENARIO_MODEL` to select another NVIDIA NIM model and `NVIDIA_API_BASE_URL` to use another compatible hosted or self-hosted NIM endpoint.

Creating a proposal does not write a file. Review the decision summary, proposed JSON, candidate YAML, and validation status, then use **Approve validated YAML** to authorize a session-scoped working YAML. The existing validators run before approval and again after serialization. **Run approved scenario** passes that file into the unchanged deterministic SOG pipeline.

See [Natural-Language Scenario Authoring](SCENARIO_AUTHORING.md) for the authority boundary and Python API.

## Runtime State

The frontend writes transient state under:

- `phase2/.sog_jobs/`
- `phase2/.sog_sessions/`
- `phase2/.sog_charts/`
- `phase2/.sog_exports/`

These folders are generated local state. They can be cleared before a fresh run or before publishing the repository.

## Troubleshooting

- Import or module errors: start the app from the repository root.
- A `DEFAULT_EXCLUDED_CONTENT_TYPES` Starlette import error means the environment has an old transitive dependency; rerun `pip install -r requirements.txt` inside the project virtual environment.
- Natural-language proposal creation says the NVIDIA key is missing: set `NVIDIA_API_KEY` or enter the key in the authoring panel.
- Conversational analysis or export is unavailable: set `ANTHROPIC_API_KEY` or open the clearly marked optional Claude connection expander. Anthropic is not required for scenario authoring or execution.
- A job appears stuck: inspect the newest file under `phase2/.sog_jobs/` and refresh the page.
- Charts or exports look stale: clear the generated `.sog_*` folders and rerun the scenario.
- No URL appears in the terminal: use `python -u` or `.\run_frontend.ps1` so startup logs flush immediately.
