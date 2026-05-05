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

NVIDIA NIM remains supported as the quality hosted trial/free-endpoint option:

```powershell
SOG_LLM_PROVIDER=nvidia
NVIDIA_API_KEY=...
SOG_LLM_MODEL=moonshotai/kimi-k2.6
```

Every assistant role uses the same selected quality model. There is no fast/basic classifier or cheap routing model.

Use NVIDIA/Kimi when you want to test the hosted open-model path without local model downloads, local Hugging Face `transformers`, or a local GPU. The app calls NVIDIA's hosted NIM-compatible endpoint and uses streaming internally for Kimi. Keep `moonshot.txt` local only; copy the key into `.env` or paste it into the UI access gate, but do not commit the key file.

Allowed models:

```powershell
SOG_LLM_PROVIDER=together
SOG_LLM_MODEL=zai-org/GLM-5.1

SOG_LLM_PROVIDER=anthropic
SOG_LLM_MODEL=claude-opus-4-7

SOG_LLM_PROVIDER=nvidia
SOG_LLM_MODEL=moonshotai/kimi-k2.6
```

The app rejects lower-tier or route-optimized model overrides, including `SOG_LLM_FAST_MODEL`, `SOG_LLM_CLASSIFY_MODEL`, `SOG_LLM_SMART_MODEL`, `instant`, `fastest`, Haiku, and 8B classifier models.

Do not use local Hugging Face `transformers` serving for this app. The selected providers are hosted APIs, so no local model weights, local GPU, or local model cache are required.

Provider notes:

- Together `zai-org/GLM-5.1`: default hosted open-model path.
- NVIDIA `moonshotai/kimi-k2.6`: hosted Kimi path through NVIDIA NIM. It supports tool calling, but live responses can be slow; the provider default timeout is 180 seconds and temperature is 0.0.
- Anthropic `claude-opus-4-7`: premium proprietary path retained for quality comparison.
- NVIDIA Developer Program hosted endpoints are appropriate for development/testing. Confirm licensing and production terms before using NIM-hosted models in a deployed service.

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
# Provider defaults are used if unset; NVIDIA defaults to 180 seconds and temperature 0.0.
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

## Artifact Formats

Canonical Phase-2 run outputs stay in their original CSV/parquet plus metadata form under `phase2/runs/`. The frontend can re-encode tabular artifacts at download time without mutating the canonical run directory.

Supported download and bundle formats:

```powershell
csv
tsv
txt
xlsx
json
jsonl
parquet
```

The same conversion path is available from the CLI:

```powershell
python scripts/export_run.py --run-id <run_id> --format xlsx
python scripts/export_run.py --run-id <run_id> --format jsonl --artifact DatasetA.csv --output phase2/.sog_exports/
python scripts/export_run.py --run-id <run_id> --format tsv --bundle --output phase2/.sog_exports/<run_id>_tsv.zip
```

## Troubleshooting

- Import or module errors: start the app from the repository root.
- The page loads but actions are disabled: set `TOGETHER_API_KEY`, `NVIDIA_API_KEY`, or `ANTHROPIC_API_KEY`, or enter the selected provider key in the UI.
- Hosted provider errors: confirm `SOG_LLM_PROVIDER` is `together`, `nvidia`, or `anthropic`, and that `SOG_LLM_MODEL` is the approved model for that provider.
- NVIDIA/Kimi timeouts: keep `SOG_LLM_TIMEOUT_SECONDS` unset or set it to at least `180`; short 90-second limits can fail on live hosted Kimi responses.
- A job appears stuck: inspect the newest file under `phase2/.sog_jobs/` and refresh the page.
- Charts or exports look stale: clear the generated `.sog_*` folders and rerun the scenario.
- No URL appears in the terminal: use `python -u` or `.\run_frontend.ps1` so startup logs flush immediately.
