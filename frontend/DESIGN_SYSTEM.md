# SOG Benchmark Studio design system

This document describes the production Streamlit interface in `frontend/chatbot.py`.
The live `APP_CSS` tokens remain the implementation source of truth.

## Design direction

The studio uses a quiet application-shell aesthetic: a cool neutral canvas, a
dedicated navigation and system-status rail, white working surfaces, and cobalt
actions. The experiment canvas is visually primary; provider and session details
remain available without competing with the authoring task.

## Product principles

1. **Task first** — the Scenario Composer belongs in the first working viewport;
   introductory content must not delay the primary task.
2. **One dominant action** — each stage exposes one clear next action: generate,
   approve, or run.
3. **Progressive disclosure** — resolved targets appear first; raw JSON, YAML,
   provider settings, and optional analysis remain behind tabs or expanders.
4. **Readable before decorative** — normal text maintains AA contrast, short line
   lengths, visible labels, and comfortable spacing.
5. **Quiet density** — use compact status summaries and remove repeated marketing
   cards, descriptions, and controls that do not advance the workflow.

## Color system

### Neutral surfaces and text

| Token | Value | Use |
| --- | --- | --- |
| `--sog-paper` | `#f3eee7` | Main warm page background |
| `--sog-paper-strong` | `#fbf8f4` | Elevated light surfaces |
| `--sog-ink` | `#121922` | Primary text |
| `--sog-ink-soft` | `#55606c` | Secondary text |
| `--sog-ink-faint` | `#626d79` | Captions and placeholders; AA contrast on paper |
| `--sog-line` | `rgba(18, 25, 34, 0.10)` | Subtle borders |
| `--sog-line-strong` | `rgba(18, 25, 34, 0.16)` | Input and emphasis borders |

### Brand and actions

| Token | Value | Use |
| --- | --- | --- |
| `--sog-navy` | `#0f1724` | Hero and code surfaces |
| `--sog-navy-2` | `#162235` | Hero gradient midpoint |
| `--sog-navy-3` | `#223451` | Hero gradient endpoint |
| `--sog-blue` | `#2754ff` | Primary actions, links, and focus identity |
| Cobalt endpoint | `#4669ed` | Accessible primary-button gradient endpoint |
| `--sog-copper` | `#f1683f` | Restrained accent and progress emphasis |

Semantic states use dark readable text on lightly tinted surfaces:

- Success: `#17633e` text with a pale green surface.
- Warning or disconnected: `#8b3c24` text with a pale copper surface.
- Errors and information use Streamlit alerts restyled to match the card system.

White button text maintains at least WCAG AA contrast across the cobalt gradient.
Normal faint text also maintains AA contrast against the paper canvas.

## Typography

| Role | Family | Use |
| --- | --- | --- |
| Display and headings | Sora | Hero, section, and card titles |
| Interface body | Manrope | Controls, paragraphs, and supporting copy |
| Data and status | IBM Plex Mono | Seeds, labels, badges, paths, and technical values |

The Google Fonts import is progressive: browser fallbacks remain usable if the
remote font request is unavailable.

## Core components

### Application shell and status rail

- The left rail owns product identity, workflow orientation, session state,
  provider setup, and the authoritative-YAML boundary.
- The main page begins directly with the authoring goal and experiment canvas.
- On smaller screens, Streamlit collapses the supporting rail and the canvas
  becomes a single-column workflow.

### Scenario Composer

- A compact process line communicates proposal, validation, and deterministic generation.
- NVIDIA setup lives in the supporting rail instead of interrupting the experiment brief.
- The authority note stays visible beside session context rather than occupying the main canvas.
- The editor and review surface use a two-column desktop layout and Streamlit's responsive stacking.

### Resolved experiment targets

The decision summary begins with values derived from the candidate YAML through
the same emission parser used by the runtime. Population, exact record counts,
overlap, schedule, seed, and A/B identity-noise values appear before the model's
advisory summary, assumptions, and warnings.

### Controls and states

- Primary controls use the AA-safe cobalt gradient.
- Inputs use strong neutral borders and high-contrast placeholders.
- Buttons, inputs, text areas, and expanders have visible keyboard focus rings.
- Success, failure, empty, approval, and background-progress states remain inline.

## Spacing and shape

- Page width: `1240px` maximum, beside the application rail.
- Major surfaces: `12px` to `16px` radii.
- Controls: `9px` to `10px` radii.
- Cards use cool neutral borders and minimal shadows to keep the interface tool-like.
- The layout stacks at `960px`; target cards also collapse to one column.

## Accessibility checks

- Use semantic headings, sections, tables, buttons, labels, and alerts.
- Do not communicate validation state through color alone; always include text.
- Preserve a visible `:focus-visible` outline for keyboard navigation.
- Keep normal text at a contrast ratio of at least 4.5:1.
- Keep every API-key field masked and explain where the credential is used.

## Authoritative files

- `frontend/chatbot_production.py` — production Streamlit entry point.
- `frontend/chatbot.py` — live layout, interactions, and `APP_CSS` theme.
- `frontend/scenario_authoring.py` — proposal contract and resolved-target review data.
- `frontend/visualizations/theme.py` — chart-specific visual theme.
