# Natural-Language Scenario Authoring

SOG includes an optional proposal-first layer that translates plain-English entity-resolution benchmark requirements into a structured scenario proposal. The deterministic YAML pipeline remains unchanged.

## Authority Boundary

```text
User request
  -> NVIDIA scenario authoring agent (no tools)
  -> constrained JSON proposal (memory only)
  -> canonical template merge (memory only)
  -> existing SOG schema and semantic validators
  -> explicit user approval
  -> session-scoped validated YAML
  -> existing deterministic SOG pipeline
  -> run-local resolved scenario.yaml + truth + observations + benchmark artifacts
```

The validated YAML is the experiment specification. Model output is never accepted as truth or as a pipeline artifact. The model cannot call the generator, write files, edit canonical scenarios, or modify records, events, truth tables, or benchmark outputs.

## Provider and Model

The default is [`nvidia/nemotron-3.5-lightning-30b-a3b`](https://build.nvidia.com/nvidia/nemotron-3.5-lightning-30b-a3b/modelcard). It is a strong fit for this narrow authoring task because NVIDIA describes it as an instruction-following and agentic model with structured-output and tool-use training, with 30B total parameters and 3B active parameters.

The client uses NVIDIA NIM's OpenAI-compatible `POST /v1/chat/completions` endpoint and requests JSON output. It sends no `tools` field. See NVIDIA's [API reference](https://docs.nvidia.com/nim/large-language-models/latest/api-reference.html), [Nemotron 3.5 structured-output guidance](https://docs.nvidia.com/nim/large-language-models/2.0.10/get-started/advanced/get-started-nemotron-3.5-lightning.html), and [structured generation guide](https://docs.nvidia.com/nim/large-language-models/1.15.0/structured-generation.html).

Configure the provider at runtime:

```powershell
$env:NVIDIA_API_KEY = "your NVIDIA developer API key"

# Optional overrides
$env:NVIDIA_SCENARIO_MODEL = "nvidia/nemotron-3.5-lightning-30b-a3b"
$env:NVIDIA_API_BASE_URL = "https://integrate.api.nvidia.com/v1"
```

Do not commit a key. The frontend also accepts it through a password input for the current Streamlit session.

## Frontend Workflow

Start the supported frontend:

```powershell
.\run_frontend.ps1
```

Use the always-visible **Scenario Composer**, enter requirements, and select **Generate structured proposal**. Review the decision summary, structured JSON, candidate YAML, and validator status in the adjacent review surface. No file is written at this stage.

Select **Approve validated YAML** to create a session-scoped file:

```text
phase2/scenarios/_working_<session_id>_<scenario_id>.yaml
```

Only this approval action writes the YAML. **Run approved scenario** submits that exact file through the existing asynchronous pipeline entry point. The existing pipeline creates the final resolved `scenario.yaml` inside its run directory.

The NVIDIA authoring panel is independent of the existing Anthropic chat assistant. `ANTHROPIC_API_KEY` is needed only for the optional conversational analysis and export workspace, which stays collapsed when it is not connected.

## Proposal Contract and Guardrails

A proposal may select one canonical template and override only these scenario roots:

- `parameters`
- `simulation`
- `selection`
- `constraints`
- `emission`
- `quality`

The proposal contract rejects unknown fields, unsupported nested keys, unsafe dataset filenames, invalid scalar types, and direct overrides of Phase-1 inputs, truth, records, events, outputs, or their paths. Approval then:

1. verifies the canonical template checksum;
2. verifies the proposal checksum to detect changes after review;
3. rebuilds the candidate from the canonical template;
4. preserves the canonical `phase1` input block;
5. reruns the existing SOG schema and semantic validators;
6. serializes and revalidates a temporary YAML; and
7. atomically promotes it to the session-scoped working YAML.

Canonical scenario files are never overwritten by this workflow.

## Python API

The same boundary is available without Streamlit:

```python
from frontend.scenario_authoring import (
    NvidiaScenarioProposalClient,
    ScenarioAuthoringAgent,
)

agent = ScenarioAuthoringAgent(NvidiaScenarioProposalClient())
draft = agent.propose(
    "Create a clean 50,000-person two-source linkage benchmark with 80% overlap."
)

# Present draft["proposal"] and draft["candidate_yaml"] for human review.
approved = agent.approve(
    draft["proposal"],
    session_id="experiment_001",
    expected_proposal_id=draft["proposal"]["proposal_id"],
)
print(approved["yaml_path"])
```

Calling `propose()` performs no writes. Calling `approve()` is the explicit authorization boundary.

## Verification

The infrastructure tests cover read-only proposal generation, forbidden-field rejection, semantic-validation failures, proposal tamper detection, canonical-template preservation, atomic approval, NVIDIA request shape, missing credentials, and handoff of the approved YAML to the existing asynchronous runner:

```powershell
python -m pytest tests/test_scenario_authoring.py -q
```

No live provider call is required for the automated suite; the provider response is injected in tests.
