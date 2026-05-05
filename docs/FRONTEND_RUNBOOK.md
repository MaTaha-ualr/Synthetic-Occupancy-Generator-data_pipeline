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

The assistant defaults to Anthropic when no provider is configured:

```powershell
ANTHROPIC_API_KEY=...
```

For hosted open-model inference without a local GPU, set `SOG_LLM_PROVIDER` and the matching provider key in `.env` or in the shell before startup:

```powershell
# Fast hosted open models with OpenAI-compatible tool calling
SOG_LLM_PROVIDER=groq
GROQ_API_KEY=...

# Other supported hosted providers
# SOG_LLM_PROVIDER=together      # TOGETHER_API_KEY=...
# SOG_LLM_PROVIDER=fireworks     # FIREWORKS_API_KEY=...
# SOG_LLM_PROVIDER=huggingface   # HF_TOKEN=...
# SOG_LLM_PROVIDER=openrouter    # OPENROUTER_API_KEY=...
```

Recommended starting point when no GPU is available:

- `groq` for low-latency hosted open models. Defaults: `openai/gpt-oss-120b` for smart agent turns and `llama-3.1-8b-instant` for fast routing.
- `together` when you want a broader open-source model catalog. Defaults: `zai-org/GLM-5.1` for tool-heavy turns and `meta-llama/Llama-3.3-70B-Instruct-Turbo` for fast turns.
- `fireworks` when you need production-oriented open-model serving with tool calling and structured outputs.

Override models per role when needed:

```powershell
SOG_LLM_SMART_MODEL=openai/gpt-oss-120b
SOG_LLM_FAST_MODEL=llama-3.1-8b-instant
SOG_LLM_CLASSIFY_MODEL=llama-3.1-8b-instant
```

For a custom hosted OpenAI-compatible endpoint:

```powershell
SOG_LLM_PROVIDER=openai_compatible
SOG_OPENAI_COMPAT_BASE_URL=https://provider.example.com/v1
SOG_OPENAI_COMPAT_API_KEY=...
SOG_LLM_MODEL=provider/model-id
```

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
- The page loads but actions are disabled: set a provider key such as `ANTHROPIC_API_KEY` or `GROQ_API_KEY`, or enter it in the UI when prompted.
- Hosted provider errors: confirm `SOG_LLM_PROVIDER`, the provider API key, and any model override are valid for that provider.
- A job appears stuck: inspect the newest file under `phase2/.sog_jobs/` and refresh the page.
- Charts or exports look stale: clear the generated `.sog_*` folders and rerun the scenario.
- No URL appears in the terminal: use `python -u` or `.\run_frontend.ps1` so startup logs flush immediately.
