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
- `agents/llm_provider.py`: Anthropic and hosted OpenAI-compatible model adapters
- `agents/guardrails.py`: deterministic prompt, tool-call, and output guardrails
- `visualizations/`: chart and theme helpers
- `sog_tools.py`: frontend-facing tool layer
- `session_manager.py`: session persistence and restore logic
- `pipeline_bridge.py`: process bridge into the pipeline scripts
- `DESIGN_SYSTEM.md`: frontend design notes

## Hosted Model Backends

The assistant uses one quality model for every role. The hosted open-model default is Together AI:

```powershell
SOG_LLM_PROVIDER=together
TOGETHER_API_KEY=...
SOG_LLM_MODEL=zai-org/GLM-5.1
```

Anthropic remains supported as a premium provider:

```powershell
SOG_LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=...
SOG_LLM_MODEL=claude-opus-4-7
```

NVIDIA NIM remains supported for hosted Kimi testing without local model downloads:

```powershell
SOG_LLM_PROVIDER=nvidia
NVIDIA_API_KEY=...
SOG_LLM_MODEL=moonshotai/kimi-k2.6
```

Fast/basic role-specific overrides are intentionally disabled. See `docs/FRONTEND_RUNBOOK.md` for the full policy.

NVIDIA/Kimi is a hosted API path, not a local `transformers` path. It requires `NVIDIA_API_KEY`, uses `moonshotai/kimi-k2.6`, streams responses internally, and defaults to a longer 180-second timeout because live tool-calling responses can be slow. Keep local key snippets such as `moonshot.txt` out of git.

## Export Formats

The artifact shelf can download individual tabular run artifacts, or bundle the full run as a zip, in these formats:

```text
csv, tsv, txt, xlsx, json, jsonl, parquet
```

Conversions are additive. Canonical run outputs under `phase2/runs/` are not rewritten.

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
