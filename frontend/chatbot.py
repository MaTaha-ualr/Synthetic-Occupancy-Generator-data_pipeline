"""SOG Assistant - Streamlit frontend with a polished benchmark studio UI."""

from __future__ import annotations

import os
import sys
import uuid
from html import escape
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))
if str(PROJECT_ROOT / "frontend") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "frontend"))


st.set_page_config(
    page_title="SOG Benchmark Studio",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="collapsed",
)


APP_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Manrope:wght@400;500;600;700;800&family=Sora:wght@500;600;700;800&display=swap');

:root {
    --sog-paper: #f3eee7;
    --sog-paper-strong: #fbf8f4;
    --sog-ink: #121922;
    --sog-ink-soft: #55606c;
    --sog-ink-faint: #7a838d;
    --sog-navy: #0f1724;
    --sog-navy-2: #162235;
    --sog-navy-3: #223451;
    --sog-blue: #2754ff;
    --sog-copper: #f1683f;
    --sog-line: rgba(18, 25, 34, 0.10);
    --sog-line-strong: rgba(18, 25, 34, 0.16);
    --sog-white-line: rgba(255, 255, 255, 0.12);
    --sog-shadow-lg: 0 28px 80px rgba(18, 25, 34, 0.14);
    --sog-shadow-md: 0 16px 44px rgba(18, 25, 34, 0.09);
    --sog-shadow-sm: 0 8px 24px rgba(18, 25, 34, 0.06);
    --sog-radius-xl: 30px;
    --sog-radius-lg: 24px;
}

html, body, [class*="css"] {
    font-family: "Manrope", sans-serif;
    color: var(--sog-ink);
}

.stApp {
    background:
        radial-gradient(circle at 10% 12%, rgba(39, 84, 255, 0.08), transparent 24%),
        radial-gradient(circle at 84% 6%, rgba(241, 104, 63, 0.09), transparent 20%),
        linear-gradient(180deg, #f9f6f0 0%, var(--sog-paper) 54%, #ebe4dc 100%);
}

[data-testid="stAppViewContainer"] {
    background: transparent;
}

[data-testid="stAppViewContainer"]::before {
    content: "";
    position: fixed;
    inset: 0;
    pointer-events: none;
    background-image:
        linear-gradient(rgba(18, 25, 34, 0.028) 1px, transparent 1px),
        linear-gradient(90deg, rgba(18, 25, 34, 0.028) 1px, transparent 1px);
    background-size: 38px 38px;
    mask-image: linear-gradient(180deg, rgba(0,0,0,0.50), transparent 84%);
    opacity: 0.45;
}

header[data-testid="stHeader"] {
    background: transparent;
}

#MainMenu, footer {
    visibility: hidden;
}

.block-container {
    max-width: 1180px;
    padding-top: 1.1rem;
    padding-bottom: 4.25rem;
}

h1, h2, h3, .sog-display, .sog-section-title {
    font-family: "Sora", sans-serif;
}

.sog-eyebrow,
.sog-section-label,
.sog-stat-label,
.sog-console-label,
.sog-download-type,
.sog-status-pill {
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.18em;
}

.sog-hero {
    position: relative;
    overflow: hidden;
    margin-bottom: 1.2rem;
    border-radius: var(--sog-radius-xl);
    border: 1px solid var(--sog-white-line);
    background:
        radial-gradient(circle at 0% 0%, rgba(255, 255, 255, 0.10), transparent 30%),
        radial-gradient(circle at 100% 10%, rgba(241, 104, 63, 0.14), transparent 28%),
        radial-gradient(circle at 70% 100%, rgba(39, 84, 255, 0.22), transparent 34%),
        linear-gradient(135deg, var(--sog-navy) 0%, var(--sog-navy-2) 52%, var(--sog-navy-3) 100%);
    box-shadow: 0 36px 90px rgba(15, 23, 36, 0.26);
}

.sog-hero::before {
    content: "";
    position: absolute;
    inset: auto auto -18% -8%;
    width: 24rem;
    height: 24rem;
    border-radius: 999px;
    background: radial-gradient(circle, rgba(255, 255, 255, 0.10), transparent 66%);
    filter: blur(20px);
}

.sog-hero-grid {
    position: relative;
    z-index: 1;
    display: grid;
    grid-template-columns: minmax(0, 1.62fr) minmax(280px, 0.82fr);
    gap: 1.15rem;
    padding: 1.28rem;
}

.sog-hero-panel {
    padding: 0.35rem 0.4rem 0.2rem 0.1rem;
}

.sog-brand-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 1rem;
    padding-bottom: 1rem;
    margin-bottom: 1rem;
    border-bottom: 1px solid rgba(255, 255, 255, 0.10);
}

.sog-brand {
    display: flex;
    align-items: center;
    gap: 0.9rem;
}

.sog-brand-mark {
    width: 2.9rem;
    height: 2.9rem;
    border-radius: 16px;
    background: linear-gradient(135deg, #f7f4ef 0%, #dce4ff 100%);
    color: var(--sog-navy);
    display: flex;
    align-items: center;
    justify-content: center;
    font-family: "Sora", sans-serif;
    font-weight: 800;
    letter-spacing: -0.04em;
    box-shadow: inset 0 1px 1px rgba(255, 255, 255, 0.5), 0 12px 30px rgba(15, 23, 36, 0.25);
}

.sog-brand-meta {
    color: rgba(255, 255, 255, 0.74);
    font-size: 0.9rem;
}

.sog-brand-title {
    color: white;
    font-weight: 700;
    letter-spacing: -0.02em;
}

.sog-brand-side {
    color: rgba(255, 255, 255, 0.70);
    text-align: right;
}

.sog-brand-side strong {
    display: block;
    color: white;
    font-size: 0.94rem;
    font-weight: 700;
    letter-spacing: -0.02em;
}

.sog-display {
    max-width: 14ch;
    margin: 0;
    color: #f8f3ec;
    font-size: clamp(2.9rem, 6vw, 5.1rem);
    line-height: 1.01;
    letter-spacing: -0.055em;
    text-align: left;
    text-wrap: balance;
    text-shadow: 0 12px 34px rgba(5, 10, 20, 0.24);
    position: relative;
    z-index: 1;
}

.sog-display span {
    display: block;
    color: #f8f3ec;
    opacity: 1;
}

.sog-display-accent {
    color: #b8cbff;
}

.sog-hero .sog-eyebrow {
    max-width: 34rem;
    margin-bottom: 1rem;
    color: rgba(207, 218, 244, 0.70);
}

.sog-lead {
    max-width: 44rem;
    margin: 1.1rem 0 1.3rem;
    color: rgba(241, 236, 228, 0.84);
    font-size: 1.08rem;
    line-height: 1.62;
}

.sog-chip-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.7rem;
    margin-bottom: 1.15rem;
}

.sog-chip {
    padding: 0.55rem 0.85rem;
    border-radius: 999px;
    border: 1px solid rgba(255, 255, 255, 0.13);
    background: rgba(255, 255, 255, 0.07);
    color: rgba(255, 255, 255, 0.88);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.72rem;
    letter-spacing: 0.03em;
}

.sog-stat-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.8rem;
}

.sog-stat {
    border-radius: 18px;
    border: 1px solid rgba(255, 255, 255, 0.10);
    background: rgba(255, 255, 255, 0.08);
    padding: 0.95rem 1rem;
    backdrop-filter: blur(14px);
}

.sog-stat-label {
    color: rgba(255, 255, 255, 0.62);
}

.sog-stat-value {
    margin-top: 0.5rem;
    color: white;
    font-size: 1.04rem;
    font-weight: 700;
    line-height: 1.25;
}

.sog-stat-copy {
    margin-top: 0.35rem;
    color: rgba(255, 255, 255, 0.66);
    font-size: 0.92rem;
    line-height: 1.45;
}

.sog-console {
    border-radius: 26px;
    border: 1px solid rgba(255, 255, 255, 0.10);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.09), rgba(255, 255, 255, 0.04));
    padding: 1rem;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    min-height: 100%;
    backdrop-filter: blur(16px);
}

.sog-console-head {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
    margin-bottom: 1rem;
}

.sog-console-title {
    color: white;
    font-family: "Sora", sans-serif;
    font-size: 1.02rem;
    font-weight: 700;
    letter-spacing: -0.03em;
}

.sog-console-copy {
    margin-top: 0.35rem;
    color: rgba(255, 255, 255, 0.68);
    font-size: 0.92rem;
    line-height: 1.45;
}

.sog-status-pill {
    border-radius: 999px;
    padding: 0.45rem 0.72rem;
    background: rgba(255, 255, 255, 0.10);
    color: white;
    border: 1px solid rgba(255, 255, 255, 0.10);
}

.sog-console-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.65rem;
    margin-bottom: 0.9rem;
}

.sog-console-card {
    border-radius: 16px;
    padding: 0.85rem 0.9rem;
    background: rgba(7, 12, 20, 0.22);
    border: 1px solid rgba(255, 255, 255, 0.08);
}

.sog-console-label {
    color: rgba(255, 255, 255, 0.56);
    font-size: 0.67rem;
    letter-spacing: 0.12em;
}

.sog-console-value {
    margin-top: 0.4rem;
    color: white;
    font-size: 0.95rem;
    font-weight: 700;
    line-height: 1.35;
    word-break: break-word;
}

.sog-console-runlist {
    border-radius: 18px;
    background: rgba(7, 12, 20, 0.22);
    border: 1px solid rgba(255, 255, 255, 0.08);
    padding: 0.85rem 0.9rem;
}

.sog-console-run {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    padding: 0.5rem 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.08);
    color: rgba(255, 255, 255, 0.72);
    font-size: 0.88rem;
}

.sog-console-run:last-child {
    border-bottom: none;
}

.sog-console-run strong {
    color: white;
    font-weight: 700;
}

.sog-section-intro {
    margin-top: 1.15rem;
    margin-bottom: 0.85rem;
}

.sog-section-label {
    color: var(--sog-blue);
}

.sog-section-title {
    margin-top: 0.38rem;
    color: var(--sog-ink);
    font-size: 1.6rem;
    letter-spacing: -0.04em;
}

.sog-section-copy {
    margin-top: 0.35rem;
    color: var(--sog-ink-soft);
    font-size: 0.98rem;
    line-height: 1.55;
    max-width: 50rem;
}

.sog-quick-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.95rem;
    margin-bottom: 1.2rem;
}

.sog-quick-card,
.sog-empty-card,
.sog-access-card,
.sog-progress-card,
.sog-download-card {
    border-radius: var(--sog-radius-lg);
    border: 1px solid var(--sog-line);
    background: linear-gradient(180deg, rgba(255,255,255,0.78), rgba(255,255,255,0.62));
    box-shadow: var(--sog-shadow-sm);
    backdrop-filter: blur(12px);
}

.sog-quick-card {
    min-height: 188px;
    padding: 1rem 1.05rem 1.1rem;
}

.sog-quick-card .sog-eyebrow {
    color: var(--sog-blue);
}

.sog-quick-title {
    margin-top: 0.6rem;
    color: var(--sog-ink);
    font-family: "Sora", sans-serif;
    font-size: 1.05rem;
    font-weight: 700;
    letter-spacing: -0.03em;
}

.sog-quick-copy {
    margin-top: 0.45rem;
    color: var(--sog-ink-soft);
    font-size: 0.93rem;
    line-height: 1.5;
}

.sog-quick-example {
    margin-top: 0.85rem;
    padding-top: 0.85rem;
    border-top: 1px solid var(--sog-line);
    color: var(--sog-ink);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.73rem;
    line-height: 1.55;
}

.sog-empty-card,
.sog-access-card,
.sog-progress-card {
    padding: 1.15rem 1.2rem;
}

.sog-empty-title,
.sog-access-title {
    color: var(--sog-ink);
    font-family: "Sora", sans-serif;
    font-size: 1.05rem;
    font-weight: 700;
    letter-spacing: -0.03em;
}

.sog-empty-copy,
.sog-access-copy,
.sog-progress-copy {
    margin-top: 0.45rem;
    color: var(--sog-ink-soft);
    font-size: 0.95rem;
    line-height: 1.58;
}

.sog-progress-head {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    align-items: baseline;
}

.sog-progress-stage {
    color: var(--sog-ink);
    font-family: "Sora", sans-serif;
    font-size: 1.05rem;
    font-weight: 700;
    letter-spacing: -0.03em;
}

.sog-progress-pct {
    color: var(--sog-blue);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.82rem;
}

.sog-progress-bar {
    width: 100%;
    height: 10px;
    border-radius: 999px;
    background: rgba(18, 25, 34, 0.08);
    overflow: hidden;
    margin-top: 0.95rem;
}

.sog-progress-fill {
    height: 100%;
    background: linear-gradient(90deg, var(--sog-blue) 0%, var(--sog-copper) 100%);
    border-radius: 999px;
}

[data-testid="stChatMessage"] {
    border-radius: 24px;
    border: 1px solid var(--sog-line);
    background: linear-gradient(180deg, rgba(255,255,255,0.82), rgba(255,255,255,0.66));
    box-shadow: var(--sog-shadow-md);
    padding: 0.45rem 0.55rem;
    margin-bottom: 0.95rem;
    backdrop-filter: blur(12px);
}

[data-testid="stChatMessage"] p,
[data-testid="stChatMessage"] li,
[data-testid="stChatMessage"] span {
    color: var(--sog-ink);
    font-size: 1rem;
    line-height: 1.62;
}

[data-testid="stChatMessage"] a {
    color: var(--sog-blue);
}

[data-testid="stChatMessage"] code {
    font-family: "IBM Plex Mono", monospace;
    color: var(--sog-blue);
    background: rgba(39, 84, 255, 0.09);
    padding: 0.15rem 0.36rem;
    border-radius: 8px;
}

[data-testid="stChatMessage"] pre {
    background: var(--sog-navy);
    border-radius: 16px;
    border: 1px solid rgba(255,255,255,0.06);
}

[data-testid="stChatMessage"] pre code {
    color: #edf3ff;
    background: transparent;
}

[data-testid="stChatMessage"] table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 0.75rem;
    overflow: hidden;
    border-radius: 14px;
    background: rgba(255, 255, 255, 0.84);
}

[data-testid="stChatMessage"] th,
[data-testid="stChatMessage"] td {
    border: 1px solid rgba(18, 25, 34, 0.08);
    padding: 0.65rem 0.8rem;
    text-align: left;
    font-size: 0.92rem;
}

[data-testid="stChatMessage"] th {
    background: #eef3ff;
    color: #20314d;
    font-weight: 700;
}

div[data-testid="stChatInput"] {
    border-radius: 22px;
    border: 1px solid var(--sog-line-strong);
    background: rgba(255, 255, 255, 0.84);
    box-shadow: 0 18px 46px rgba(18, 25, 34, 0.10);
    backdrop-filter: blur(12px);
}

div[data-testid="stChatInput"] textarea,
div[data-testid="stTextInput"] input {
    color: var(--sog-ink) !important;
    font-family: "Manrope", sans-serif !important;
}

div[data-testid="stChatInput"] textarea::placeholder,
div[data-testid="stTextInput"] input::placeholder {
    color: var(--sog-ink-faint) !important;
}

.stButton > button,
.stDownloadButton > button,
div[data-testid="stFormSubmitButton"] button {
    width: 100%;
    border: none !important;
    border-radius: 14px !important;
    padding: 0.82rem 1rem !important;
    background: linear-gradient(135deg, var(--sog-blue) 0%, #5275ff 100%) !important;
    color: white !important;
    font-family: "Sora", sans-serif !important;
    font-size: 0.82rem !important;
    font-weight: 700 !important;
    letter-spacing: -0.01em !important;
    box-shadow: 0 14px 30px rgba(39, 84, 255, 0.18) !important;
    transition: transform 0.18s ease, box-shadow 0.18s ease, filter 0.18s ease !important;
}

.stButton > button:hover,
.stDownloadButton > button:hover,
div[data-testid="stFormSubmitButton"] button:hover {
    transform: translateY(-1px);
    box-shadow: 0 18px 34px rgba(39, 84, 255, 0.22) !important;
    filter: saturate(1.03);
}

[data-testid="stTextInput"] input {
    border-radius: 16px !important;
    border: 1px solid var(--sog-line-strong) !important;
    background: rgba(255, 255, 255, 0.78) !important;
}

[data-testid="stAlert"] {
    border-radius: 18px;
    border: 1px solid rgba(39, 84, 255, 0.14);
    background: rgba(39, 84, 255, 0.06);
}

[data-testid="stImage"] img,
iframe[title="st.iframe"] {
    border-radius: 24px;
    border: 1px solid var(--sog-line);
    background: rgba(255, 255, 255, 0.78);
    box-shadow: var(--sog-shadow-sm);
}

div[data-testid="stCaptionContainer"] p {
    color: var(--sog-ink-soft);
    font-size: 0.9rem;
}

div[data-testid="stDownloadButton"] {
    margin-top: 0.45rem;
}

.sog-download-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 0.9rem;
    margin-top: 0.4rem;
}

.sog-download-card {
    padding: 0.95rem;
}

.sog-download-type {
    color: var(--sog-blue);
    font-size: 0.68rem;
    letter-spacing: 0.14em;
}

.sog-download-name {
    margin-top: 0.5rem;
    color: var(--sog-ink);
    font-family: "Sora", sans-serif;
    font-size: 0.92rem;
    font-weight: 700;
    line-height: 1.4;
    word-break: break-word;
}

.sog-download-copy {
    margin-top: 0.32rem;
    color: var(--sog-ink-soft);
    font-size: 0.88rem;
    line-height: 1.45;
}

.sog-footer {
    margin-top: 2.6rem;
    padding-top: 1.4rem;
    border-top: 1px solid var(--sog-line);
    color: var(--sog-ink-faint);
    font-size: 0.86rem;
    line-height: 1.6;
}

.sog-footer strong {
    color: var(--sog-ink);
}

@media (max-width: 960px) {
    .sog-hero-grid,
    .sog-quick-grid,
    .sog-download-grid,
    .sog-stat-grid {
        grid-template-columns: 1fr;
    }

    .sog-brand-row,
    .sog-progress-head {
        flex-direction: column;
        align-items: flex-start;
    }

    .sog-brand-side {
        text-align: left;
    }

    .sog-display {
        max-width: none;
        font-size: clamp(2.4rem, 13vw, 4rem);
        text-wrap: pretty;
    }

    .block-container {
        padding-top: 0.8rem;
    }
}
</style>
"""


def _inject_theme() -> None:
    st.markdown(APP_CSS, unsafe_allow_html=True)


def _render_section_intro(kicker: str, title: str, copy: str) -> None:
    st.markdown(
        f"""
        <div class="sog-section-intro">
            <div class="sog-section-label">{escape(kicker)}</div>
            <div class="sog-section-title">{escape(title)}</div>
            <div class="sog-section-copy">{escape(copy)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_hero(context: dict[str, object], pending_job_id: str | None) -> None:
    last_run = str(context.get("last_run_id") or "No completed run")
    last_scenario = str(context.get("last_scenario_id") or "No active draft")
    run_history = list(context.get("run_history", []) or [])[:3]
    queue_state = "Pipeline active" if pending_job_id else "Ready"

    history_markup = ""
    for item in run_history:
        run_id = escape(str(item.get("run_id", "")))
        scenario_id = escape(str(item.get("scenario_id", "scenario")))
        history_markup += (
            f'<div class="sog-console-run">'
            f"<span>{scenario_id}</span>"
            f"<strong>{run_id}</strong>"
            f"</div>"
        )
    if not history_markup:
        history_markup = (
            '<div class="sog-console-run">'
            "<span>Session</span>"
            "<strong>No artifacts yet</strong>"
            "</div>"
        )

    st.markdown(
        f"""
        <section class="sog-hero">
            <div class="sog-hero-grid">
                <div class="sog-hero-panel">
                    <div class="sog-brand-row">
                        <div class="sog-brand">
                            <div class="sog-brand-mark">S</div>
                            <div class="sog-brand-meta">
                                <div class="sog-brand-title">Synthetic Occupancy Generator</div>
                                <div>Benchmark studio for entity resolution research</div>
                            </div>
                        </div>
                        <div class="sog-brand-side">
                            <strong>UALR / ERIQ</strong>
                            Taha Mohammed, SOG Lead
                        </div>
                    </div>
                    <div class="sog-eyebrow">Benchmark studio for scenario design, pipeline execution, and ER artifact review.</div>
                    <h1 class="sog-display">
                        <span>Build benchmark datasets</span>
                        <span class="sog-display-accent">with research-grade rigor.</span>
                    </h1>
                    <p class="sog-lead">
                        Draft a scenario, launch the pipeline, inspect ER difficulty, and export artifacts from one
                        interface that feels like a real product surface instead of a themed demo.
                    </p>
                    <div class="sog-chip-row">
                        <span class="sog-chip">scenario drafting</span>
                        <span class="sog-chip">async execution</span>
                        <span class="sog-chip">ER analytics</span>
                        <span class="sog-chip">tool-ready exports</span>
                    </div>
                    <div class="sog-stat-grid">
                        <div class="sog-stat">
                            <div class="sog-stat-label">Use it for</div>
                            <div class="sog-stat-value">Noisy linkage benchmarks with explicit overlap, duplication, and event dynamics.</div>
                            <div class="sog-stat-copy">Tune the hard parts directly instead of editing YAML by hand.</div>
                        </div>
                        <div class="sog-stat">
                            <div class="sog-stat-label">Workflow</div>
                            <div class="sog-stat-value">Describe intent once. Let the assistant configure, run, analyze, and package.</div>
                            <div class="sog-stat-copy">The frontend now reads like a benchmark studio rather than a generic chat shell.</div>
                        </div>
                        <div class="sog-stat">
                            <div class="sog-stat-label">Design stance</div>
                            <div class="sog-stat-value">Editorial, crisp, and operational.</div>
                            <div class="sog-stat-copy">Warm paper background, dark hero, sharp cobalt actions, and higher signal density.</div>
                        </div>
                    </div>
                </div>
                <aside class="sog-console">
                    <div>
                        <div class="sog-console-head">
                            <div>
                                <div class="sog-console-title">Session control</div>
                                <div class="sog-console-copy">Keep the current draft, last run, and recent artifacts visible at the top of the page.</div>
                            </div>
                            <div class="sog-status-pill">{escape(queue_state)}</div>
                        </div>
                        <div class="sog-console-grid">
                            <div class="sog-console-card">
                                <div class="sog-console-label">Active draft</div>
                                <div class="sog-console-value">{escape(last_scenario)}</div>
                            </div>
                            <div class="sog-console-card">
                                <div class="sog-console-label">Last run</div>
                                <div class="sog-console-value">{escape(last_run)}</div>
                            </div>
                            <div class="sog-console-card">
                                <div class="sog-console-label">Run history</div>
                                <div class="sog-console-value">{len(run_history)}</div>
                            </div>
                            <div class="sog-console-card">
                                <div class="sog-console-label">Session key</div>
                                <div class="sog-console-value">{escape(st.session_state.session_id)}</div>
                            </div>
                        </div>
                    </div>
                    <div class="sog-console-runlist">
                        <div class="sog-console-label">Recent artifacts</div>
                        {history_markup}
                    </div>
                </aside>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def _render_prompt_gallery() -> None:
    st.markdown(
        """
        <div class="sog-quick-grid">
            <div class="sog-quick-card">
                <div class="sog-eyebrow">Configure</div>
                <div class="sog-quick-title">Shape the benchmark directly</div>
                <div class="sog-quick-copy">
                    Ask for harder overlap, more duplication, a noisier Dataset B, or a specific scenario family.
                </div>
                <div class="sog-quick-example">"Build a harder couple_merge benchmark with 50% overlap and much noisier Dataset B."</div>
            </div>
            <div class="sog-quick-card">
                <div class="sog-eyebrow">Analyze</div>
                <div class="sog-quick-title">Read results in ER terms</div>
                <div class="sog-quick-copy">
                    Pull a concise difficulty summary and generate the dashboard when the run finishes.
                </div>
                <div class="sog-quick-example">"Summarize the last run and show the dashboard with overlap, noise, and missingness."</div>
            </div>
            <div class="sog-quick-card">
                <div class="sog-eyebrow">Export</div>
                <div class="sog-quick-title">Package for downstream tooling</div>
                <div class="sog-quick-copy">
                    Normalize outputs for Splink or Zingg, or package the run into a downloadable bundle.
                </div>
                <div class="sog-quick-example">"Export the latest run for Splink and package everything into a zip."</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_empty_conversation() -> None:
    st.markdown(
        """
        <div class="sog-empty-card">
            <div class="sog-empty-title">Conversation is empty</div>
            <div class="sog-empty-copy">
                Start with one direct request. The best prompts are operational: describe the scenario, the difficulty,
                or the artifact you want next.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_progress(stage: str, pct: int) -> None:
    pct = max(0, min(int(pct), 100))
    st.markdown(
        f"""
        <div class="sog-progress-card">
            <div class="sog-progress-head">
                <div class="sog-progress-stage">Pipeline in progress: {escape(stage)}</div>
                <div class="sog-progress-pct">{pct}% complete</div>
            </div>
            <div class="sog-progress-copy">
                The job is running in the background. This page will refresh automatically until the artifact is ready.
            </div>
            <div class="sog-progress-bar">
                <div class="sog-progress-fill" style="width: {pct}%;"></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _active_model_access() -> tuple[str, str]:
    from agents.llm_provider import normalize_provider, provider_api_key_env

    provider = normalize_provider()
    key_env = provider_api_key_env(provider)
    return provider, os.environ.get(key_env) or os.environ.get("SOG_LLM_API_KEY", "")


def _model_access_ready() -> bool:
    from agents.llm_provider import has_provider_credentials, normalize_provider

    try:
        return has_provider_credentials(normalize_provider())
    except Exception:
        return False


def _render_access_gate() -> tuple[str, str, str]:
    from agents.llm_provider import (
        available_provider_ids,
        normalize_provider,
        provider_api_key_placeholder,
        provider_label,
    )

    provider_ids = available_provider_ids()
    try:
        current_provider = normalize_provider()
    except Exception as exc:
        st.warning(str(exc))
        current_provider = "together"
    st.markdown(
        """
        <div class="sog-access-card">
            <div class="sog-access-title">Connect the model layer</div>
            <div class="sog-access-copy">
                Choose a quality-first hosted model provider for this local session. Models run remotely, so no local GPU or model download is required.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    provider = st.selectbox(
        "Model provider",
        provider_ids,
        index=provider_ids.index(current_provider) if current_provider in provider_ids else 0,
        format_func=provider_label,
        help="Together uses GLM-5.1, NVIDIA uses Kimi K2.6, and Anthropic uses Claude Opus 4.7. Fast/basic routes are disabled.",
    )
    key = st.text_input(
        f"{provider_label(provider)} API key",
        type="password",
        placeholder=provider_api_key_placeholder(provider),
        help="Stored only for this browser session.",
    )
    return provider, key, ""


def _render_footer() -> None:
    st.markdown(
        """
        <div class="sog-footer">
            <strong>SOG Benchmark Studio</strong><br>
            Synthetic Occupancy Generator for entity resolution benchmarking. Local session persistence, async runs,
            chart generation, and export packaging live in one Streamlit surface.
        </div>
        """,
        unsafe_allow_html=True,
    )


from session_manager import SessionData, load_session, save_session  # noqa: E402


def _resolve_session_id() -> str:
    sid = st.query_params.get("sid", "")
    if isinstance(sid, list):
        sid = sid[0] if sid else ""
    sid = str(sid).strip()
    if sid:
        return sid
    sid = uuid.uuid4().hex[:8]
    st.query_params["sid"] = sid
    return sid


if "session_id" not in st.session_state:
    st.session_state.session_id = _resolve_session_id()
else:
    query_sid = st.query_params.get("sid", "")
    if isinstance(query_sid, list):
        query_sid = query_sid[0] if query_sid else ""
    if str(query_sid).strip() != st.session_state.session_id:
        st.query_params["sid"] = st.session_state.session_id

SESSION_ID = st.session_state.session_id

if "session_loaded" not in st.session_state:
    saved = load_session(SESSION_ID)
    st.session_state.messages = saved.messages
    st.session_state.context = saved.context
    st.session_state.last_run_downloads = saved.last_run_downloads
    st.session_state.pending_job_id = saved.pending_job_id
    st.session_state.pending_charts = saved.pending_charts
    st.session_state.session_loaded = True
else:
    defaults = {
        "messages": [],
        "context": {
            "last_run_id": None,
            "last_scenario_id": None,
            "run_history": [],
        },
        "last_run_downloads": {},
        "pending_job_id": None,
        "pending_charts": [],
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default


def _persist() -> None:
    save_session(
        SessionData(
            session_id=SESSION_ID,
            context=st.session_state.context,
            messages=st.session_state.messages,
            last_run_downloads=st.session_state.last_run_downloads,
            pending_job_id=st.session_state.pending_job_id,
            pending_charts=st.session_state.pending_charts,
        )
    )


@st.cache_resource
def _get_orchestrator(provider: str, api_key: str):
    from agents.orchestrator import Orchestrator

    return Orchestrator(api_key=api_key, provider=provider)


def _poll_job() -> None:
    job_id = st.session_state.pending_job_id
    if not job_id:
        return

    from async_runner import poll_status

    status = poll_status(job_id)

    if status["status"] == "completed":
        run_id = status.get("run_id", "")
        if run_id:
            st.session_state.context["last_run_id"] = run_id
            history = st.session_state.context.get("run_history", [])
            history.insert(
                0,
                {
                    "run_id": run_id,
                    "scenario_id": status.get("scenario_id", ""),
                },
            )
            st.session_state.context["run_history"] = history[:20]

            try:
                provider, api_key = _active_model_access()
                orch = _get_orchestrator(provider, api_key)
                result = orch._analyst().run(
                    f"Summarize run {run_id}",
                    SESSION_ID,
                    st.session_state.context,
                )
                message = f"Run complete: **{run_id}**\n\n{result.message}"
                if result.charts:
                    st.session_state.pending_charts = result.charts
                if result.data.get("download_paths"):
                    st.session_state.last_run_downloads = result.data["download_paths"]
            except Exception as exc:
                message = f"Run complete: **{run_id}**\n\nCould not summarize: {exc}"
        else:
            message = "Run completed."

        st.session_state.messages.append({"role": "assistant", "content": message})
        st.session_state.pending_job_id = None
        _persist()
        st.rerun()

    if status["status"] == "failed":
        st.session_state.messages.append(
            {
                "role": "assistant",
                "content": f"Run failed: {status.get('error', 'unknown error')}",
            }
        )
        st.session_state.pending_job_id = None
        _persist()
        st.rerun()

    _render_section_intro(
        "Execution",
        "Background pipeline is running",
        "The interface will keep the current session warm while the pipeline finishes in the background.",
    )
    _render_progress(
        str(status.get("current_stage", "running")),
        int(status.get("progress_percent", 0)),
    )
    st.markdown('<meta http-equiv="refresh" content="3">', unsafe_allow_html=True)
    st.stop()


_inject_theme()
_render_hero(st.session_state.context, st.session_state.pending_job_id)
_render_prompt_gallery()
_poll_job()

_render_section_intro(
    "Conversation",
    "Direct the benchmark assistant",
    "Use natural language. Ask for configuration changes, run execution, result interpretation, or export packaging.",
)

if not st.session_state.messages:
    _render_empty_conversation()

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if st.session_state.pending_charts:
    _render_section_intro(
        "Analysis",
        "Latest charts",
        "Generated visual diagnostics for the most recent run in this session.",
    )
    for chart in st.session_state.pending_charts:
        path = chart.get("chart_path", "")
        if not path or not Path(path).exists():
            continue
        if Path(path).suffix.lower() == ".html":
            st.components.v1.html(
                Path(path).read_text(encoding="utf-8"),
                height=520,
                scrolling=True,
            )
        else:
            st.image(path)
        if chart.get("insight"):
            st.caption(chart["insight"])
        with open(path, "rb") as handle:
            st.download_button(
                f"Download {Path(path).name}",
                handle.read(),
                file_name=Path(path).name,
                key=f"chart_dl_{Path(path).stem}",
            )
    st.session_state.pending_charts = []
    _persist()

if st.session_state.last_run_downloads:
    from sog_phase2.format_export import (
        TABULAR_FORMATS,
        bundle_run_as_zip,
        convert_artifact_bytes,
        extension_for,
        is_tabular_artifact,
        mime_for,
    )

    _render_section_intro(
        "Artifacts",
        "Output shelf",
        "Download the latest benchmark artifacts directly from this session. "
        "Tabular files can be re-encoded into Excel, TSV/TXT, JSON, JSONL, or Parquet on the fly.",
    )
    columns = st.columns(len(st.session_state.last_run_downloads))
    for col, (label, path_str) in zip(columns, st.session_state.last_run_downloads.items()):
        path = Path(path_str)
        if not path.exists():
            continue
        with col:
            st.markdown(
                f"""
                <div class="sog-download-card">
                    <div class="sog-download-type">{escape(label)}</div>
                    <div class="sog-download-name">{escape(path.name)}</div>
                    <div class="sog-download-copy">Latest generated artifact for this session.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if is_tabular_artifact(path):
                fmt = st.selectbox(
                    "Format",
                    TABULAR_FORMATS,
                    index=TABULAR_FORMATS.index(path.suffix.lower().lstrip(".")) if path.suffix.lower().lstrip(".") in TABULAR_FORMATS else 0,
                    key=f"fmt_{label}",
                )
                try:
                    payload = convert_artifact_bytes(path, fmt)
                    download_name = f"{path.stem}{extension_for(fmt)}"
                    download_mime = mime_for(fmt)
                except Exception as exc:
                    st.error(f"Conversion failed: {exc}")
                    payload = path.read_bytes()
                    download_name = path.name
                    download_mime = "application/octet-stream"
                st.download_button(
                    f"Download {label}",
                    payload,
                    file_name=download_name,
                    mime=download_mime,
                    key=f"dl_{label}_{fmt}",
                )
            else:
                st.download_button(
                    f"Download {label}",
                    path.read_bytes(),
                    file_name=path.name,
                    key=f"dl_{label}",
                )

    run_dirs = {Path(p).parent for p in st.session_state.last_run_downloads.values() if Path(p).exists()}
    if len(run_dirs) == 1:
        run_dir = next(iter(run_dirs))
        st.markdown("---")
        bundle_col1, bundle_col2 = st.columns([1, 2])
        with bundle_col1:
            bundle_fmt = st.selectbox(
                "Bundle format",
                TABULAR_FORMATS,
                index=TABULAR_FORMATS.index("csv"),
                key="bundle_fmt",
                help="Re-encode every tabular file in the run, then zip the result with the meta files.",
            )
        with bundle_col2:
            try:
                bundle_bytes = bundle_run_as_zip(run_dir, bundle_fmt)
                st.download_button(
                    f"Download full run as {bundle_fmt} zip",
                    bundle_bytes,
                    file_name=f"{run_dir.name}_{bundle_fmt}.zip",
                    mime="application/zip",
                    key=f"bundle_dl_{bundle_fmt}",
                )
            except Exception as exc:
                st.error(f"Bundle failed: {exc}")

if not _model_access_ready():
    _render_section_intro(
        "Access",
        "Enable the assistant layer",
        "The frontend is ready. Add a hosted model API key once and continue working in the same browser session.",
    )
    provider, key, base_url = _render_access_gate()
    if key.strip():
        from agents.llm_provider import provider_api_key_env

        os.environ["SOG_LLM_PROVIDER"] = provider
        os.environ[provider_api_key_env(provider)] = key.strip()
        st.rerun()
    _render_footer()
    st.stop()

if prompt := st.chat_input("Describe the benchmark, result, or export you want next..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    _persist()

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Working..."):
            try:
                provider, api_key = _active_model_access()
                orch = _get_orchestrator(provider, api_key)
                result = orch.run_turn(prompt, SESSION_ID, st.session_state.context)

                text = result.get("message", "")
                if result.get("session_updates"):
                    st.session_state.context.update(result["session_updates"])
                if result.get("pending_job_id"):
                    st.session_state.pending_job_id = result["pending_job_id"]
                if result.get("charts"):
                    st.session_state.pending_charts = result["charts"]
                if result.get("download_paths"):
                    st.session_state.last_run_downloads = result["download_paths"]
                _persist()
            except Exception as exc:
                text = f"Error: {exc}"

        st.markdown(text)

    st.session_state.messages.append({"role": "assistant", "content": text})
    _persist()
    if (
        st.session_state.pending_job_id
        or st.session_state.last_run_downloads
        or st.session_state.pending_charts
    ):
        st.rerun()

_render_footer()
