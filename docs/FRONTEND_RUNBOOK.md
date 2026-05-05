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

## Model Providers

The assistant uses a quality-first model policy. If no provider is configured, it defaults to the hosted open-model path:

```powershell
SOG_LLM_PROVIDER=together
TOGETHER_API_KEY=...
SOG_LLM_MODEL=zai-org/GLM-5.1
```

Anthropic remains supported as the premium proprietary option:

```powershell
SOG_LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=...
SOG_LLM_MODEL=claude-opus-4-7
```

Every assistant role uses the same selected quality model. There is no fast/basic classifier or cheap routing model.

Allowed models:

```powershell
SOG_LLM_PROVIDER=together
SOG_LLM_MODEL=zai-org/GLM-5.1

SOG_LLM_PROVIDER=anthropic
SOG_LLM_MODEL=claude-opus-4-7
```

The app rejects lower-tier or route-optimized model overrides, including `SOG_LLM_FAST_MODEL`, `SOG_LLM_CLASSIFY_MODEL`, `SOG_LLM_SMART_MODEL`, `instant`, `fastest`, Haiku, and 8B classifier models.

Do not use local Hugging Face `transformers` serving for this app. The selected providers are hosted APIs, so no local model weights, local GPU, or local model cache are required.

## Guardrails

The assistant now adds deterministic guardrails around hosted model calls:

- blocks prompt-injection attempts that ask to bypass instructions or reveal hidden prompts, keys, tokens, or environment variables
- redacts recognizable secrets before prompts, tool results, and final responses are sent or shown
- allows only declared SOG tools and bounds tool-call argument size
- blocks model-selected output paths that resolve outside the repository workspace
- caps agent iterations so a tool loop cannot spin indefinitely

Optional limits:

```powershell
SOG_GUARDRAIL_MAX_USER_CHARS=4000
SOG_GUARDRAIL_MAX_OUTPUT_CHARS=6000
SOG_GUARDRAIL_MAX_TOOL_INPUT_CHARS=6000
SOG_AGENT_MAX_ITERATIONS=20
SOG_LLM_TIMEOUT_SECONDS=90
SOG_LLM_TEMPERATURE=0.2
```

## Runtime State

The frontend writes transient state under:

- `phase2/.sog_jobs/`
- `phase2/.sog_sessions/`
- `phase2/.sog_charts/`
- `phase2/.sog_exports/`

These folders are generated local state. They can be cleared before a fresh run or before publishing the repository.

## Troubleshooting

- Import or module errors: start the app from the repository root.
- The page loads but actions are disabled: set `TOGETHER_API_KEY` or `ANTHROPIC_API_KEY`, or enter the selected provider key in the UI.
- Hosted provider errors: confirm `SOG_LLM_PROVIDER` is `together` or `anthropic`, and that `SOG_LLM_MODEL` is the approved model for that provider.
- A job appears stuck: inspect the newest file under `phase2/.sog_jobs/` and refresh the page.
- Charts or exports look stale: clear the generated `.sog_*` folders and rerun the scenario.
- No URL appears in the terminal: use `python -u` or `.\run_frontend.ps1` so startup logs flush immediately.
