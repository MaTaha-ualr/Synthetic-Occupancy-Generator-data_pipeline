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
if str(PROJECT_ROOT / "phase2" / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "phase2" / "src"))
if str(PROJECT_ROOT / "frontend") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "frontend"))


st.set_page_config(
    page_title="SOG Benchmark Studio",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="auto",
)


APP_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Manrope:wght@400;500;600;700;800&family=Sora:wght@500;600;700;800&display=swap');

:root {
    --sog-paper: #f3eee7;
    --sog-paper-strong: #fbf8f4;
    --sog-ink: #121922;
    --sog-ink-soft: #55606c;
    --sog-ink-faint: #626d79;
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

*, *::before, *::after {
    box-sizing: border-box;
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
    color: var(--sog-ink);
}

header[data-testid="stHeader"] [data-testid="stToolbar"],
header[data-testid="stHeader"] [data-testid="stToolbar"] button,
header[data-testid="stHeader"] [data-testid="stToolbar"] a {
    color: var(--sog-ink) !important;
}

header[data-testid="stHeader"] [data-testid="stAppDeployButton"] button,
header[data-testid="stHeader"] [data-testid="stToolbarActions"] button {
    margin: 0.3rem 0.35rem 0 0 !important;
    padding: 0.3rem 0.65rem !important;
    border: 1px solid rgba(18, 25, 34, 0.08) !important;
    border-radius: 10px !important;
    background-color: rgba(251, 248, 244, 0.92) !important;
    box-shadow: 0 6px 20px rgba(18, 25, 34, 0.08) !important;
    backdrop-filter: blur(12px);
}

[data-testid="stExpandSidebarButton"] {
    margin: 0.3rem 0 0 0.35rem !important;
    border: 1px solid rgba(18, 25, 34, 0.10) !important;
    border-radius: 10px !important;
    background: rgba(255, 255, 255, 0.94) !important;
    color: var(--sog-ink) !important;
    box-shadow: 0 6px 20px rgba(18, 25, 34, 0.08) !important;
}

[data-testid="stExpandSidebarButton"] [data-testid="stIconMaterial"] {
    color: var(--sog-ink) !important;
}

header[data-testid="stHeader"] [data-testid="stToolbar"] svg {
    color: var(--sog-ink) !important;
    fill: currentColor !important;
}

#MainMenu, footer {
    visibility: hidden;
}

.block-container {
    max-width: 1220px;
    padding-top: 0.8rem;
    padding-bottom: 3rem;
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
    margin-bottom: 1rem;
    border-radius: 26px;
    border: 1px solid var(--sog-white-line);
    background:
        radial-gradient(circle at 0% 0%, rgba(255, 255, 255, 0.10), transparent 30%),
        radial-gradient(circle at 100% 10%, rgba(241, 104, 63, 0.14), transparent 28%),
        radial-gradient(circle at 70% 100%, rgba(39, 84, 255, 0.22), transparent 34%),
        linear-gradient(135deg, var(--sog-navy) 0%, var(--sog-navy-2) 52%, var(--sog-navy-3) 100%);
    box-shadow: 0 24px 64px rgba(15, 23, 36, 0.20);
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
    grid-template-columns: minmax(0, 1.35fr) minmax(300px, 0.75fr);
    gap: 1rem;
    padding: 1.05rem 1.2rem 1.2rem;
    align-items: start;
}

.sog-hero-panel {
    padding: 0.25rem 0.35rem 0 0;
}

.sog-brand-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 1rem;
    padding: 0.9rem 1.2rem;
    margin: 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.10);
}

.sog-brand {
    display: flex;
    align-items: center;
    gap: 0.9rem;
}

.sog-brand-mark {
    width: 2.35rem;
    height: 2.35rem;
    border-radius: 13px;
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
    font-size: 0.82rem;
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
    font-size: 0.86rem;
    font-weight: 700;
    letter-spacing: -0.02em;
}

.sog-display {
    max-width: 17ch;
    margin: 0;
    padding: 0 !important;
    color: #f8f3ec;
    font-size: clamp(2.35rem, 4.4vw, 3.85rem);
    line-height: 1.04;
    letter-spacing: -0.05em;
    text-align: left;
    text-wrap: balance;
    text-shadow: 0 12px 34px rgba(5, 10, 20, 0.24);
    position: relative;
    z-index: 1;
}

.sog-display-line {
    display: block;
    color: #f8f3ec;
    opacity: 1;
}

.sog-display > span:not([data-testid="stHeaderActionElements"]) {
    display: block;
}

.sog-display [data-testid="stHeaderActionElements"] {
    display: none;
}

.sog-display-accent {
    color: #b8cbff;
}

.sog-hero .sog-eyebrow {
    max-width: 34rem;
    margin-bottom: 0.65rem;
    color: rgba(218, 226, 247, 0.78);
}

.sog-lead {
    max-width: 42rem;
    margin: 0.8rem 0 0.95rem;
    color: rgba(245, 241, 235, 0.88);
    font-size: 0.98rem;
    line-height: 1.55;
}

.sog-chip-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-bottom: 0;
}

.sog-chip {
    padding: 0.42rem 0.65rem;
    border-radius: 999px;
    border: 1px solid rgba(255, 255, 255, 0.13);
    background: rgba(255, 255, 255, 0.07);
    color: rgba(255, 255, 255, 0.88);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.68rem;
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
    border-radius: 20px;
    border: 1px solid rgba(255, 255, 255, 0.10);
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.09), rgba(255, 255, 255, 0.04));
    padding: 0.85rem;
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    min-height: 0;
    backdrop-filter: blur(16px);
}

.sog-console-head {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
    margin-bottom: 0.7rem;
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
    font-size: 0.82rem;
    line-height: 1.45;
}

.sog-status-pill {
    border-radius: 999px;
    padding: 0.38rem 0.58rem;
    background: rgba(255, 255, 255, 0.10);
    color: white;
    border: 1px solid rgba(255, 255, 255, 0.10);
}

.sog-console-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.5rem;
    margin-bottom: 0.55rem;
}

.sog-console-card {
    min-width: 0;
    border-radius: 13px;
    padding: 0.65rem 0.7rem;
    background: rgba(7, 12, 20, 0.22);
    border: 1px solid rgba(255, 255, 255, 0.08);
}

.sog-console-label {
    color: rgba(255, 255, 255, 0.56);
    font-size: 0.67rem;
    letter-spacing: 0.12em;
}

.sog-console-value {
    margin-top: 0.28rem;
    color: white;
    font-size: 0.85rem;
    font-weight: 700;
    line-height: 1.35;
    word-break: break-word;
}

.sog-console-runlist {
    border-radius: 13px;
    background: rgba(7, 12, 20, 0.22);
    border: 1px solid rgba(255, 255, 255, 0.08);
    padding: 0.62rem 0.7rem;
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

.sog-workbench-head {
    position: relative;
    overflow: hidden;
    margin: 0 0 0.75rem;
    padding: 1rem 1.1rem;
    border-radius: 22px;
    border: 1px solid rgba(255, 255, 255, 0.10);
    background:
        radial-gradient(circle at 94% 12%, rgba(241, 104, 63, 0.22), transparent 26%),
        radial-gradient(circle at 54% 120%, rgba(39, 84, 255, 0.32), transparent 42%),
        linear-gradient(135deg, #111a29 0%, #1b2a42 100%);
    box-shadow: 0 16px 42px rgba(15, 23, 36, 0.14);
}

.sog-workbench-kicker {
    color: #a9beff;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.17em;
}

.sog-workbench-title {
    margin-top: 0.3rem;
    color: white;
    font-family: "Sora", sans-serif;
    font-size: clamp(1.5rem, 2.5vw, 1.95rem);
    font-weight: 700;
    letter-spacing: -0.045em;
}

.sog-workbench-copy {
    max-width: 48rem;
    margin-top: 0.3rem;
    color: rgba(239, 243, 252, 0.82);
    font-size: 0.88rem;
    line-height: 1.5;
}

.sog-stage-rail {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.55rem;
    margin-top: 0.75rem;
}

.sog-stage {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    padding: 0.55rem 0.62rem;
    border-radius: 13px;
    border: 1px solid rgba(255, 255, 255, 0.09);
    background: rgba(6, 11, 19, 0.22);
}

.sog-stage-number {
    display: grid;
    place-items: center;
    flex: 0 0 1.55rem;
    height: 1.55rem;
    border-radius: 8px;
    background: rgba(169, 190, 255, 0.14);
    color: #cbd7ff;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.7rem;
}

.sog-stage strong {
    display: block;
    color: white;
    font-size: 0.78rem;
}

.sog-stage span {
    display: block;
    margin-top: 0.12rem;
    color: rgba(255, 255, 255, 0.56);
    font-size: 0.68rem;
}

.sog-provider-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.65rem;
    margin-bottom: 0.65rem;
}

.sog-provider-card {
    display: grid;
    grid-template-columns: auto minmax(0, 1fr) auto;
    gap: 0.65rem;
    align-items: center;
    padding: 0.68rem 0.75rem;
    border-radius: 16px;
    border: 1px solid var(--sog-line);
    background: rgba(255, 255, 255, 0.68);
    box-shadow: var(--sog-shadow-sm);
}

.sog-provider-card.is-primary {
    border-color: rgba(39, 84, 255, 0.24);
    background: linear-gradient(135deg, rgba(238, 243, 255, 0.94), rgba(255, 255, 255, 0.72));
}

.sog-provider-mark {
    display: grid;
    place-items: center;
    width: 2rem;
    height: 2rem;
    border-radius: 10px;
    color: white;
    background: linear-gradient(135deg, var(--sog-blue), #4669ed);
    font-family: "Sora", sans-serif;
    font-weight: 800;
}

.sog-provider-card:not(.is-primary) .sog-provider-mark {
    background: linear-gradient(135deg, #2e3949, #55657c);
}

.sog-provider-name {
    color: var(--sog-ink);
    font-family: "Sora", sans-serif;
    font-size: 0.92rem;
    font-weight: 700;
}

.sog-provider-role {
    margin-top: 0.22rem;
    color: var(--sog-ink-soft);
    font-size: 0.75rem;
    line-height: 1.35;
}

.sog-provider-status {
    white-space: nowrap;
    padding: 0.35rem 0.52rem;
    border-radius: 999px;
    color: #8b3c24;
    background: rgba(241, 104, 63, 0.11);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.62rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

.sog-provider-status.is-connected {
    color: #17633e;
    background: rgba(31, 157, 94, 0.11);
}

.sog-boundary-note {
    margin: 0 0 0.75rem;
    padding: 0.65rem 0.75rem;
    border-left: 3px solid var(--sog-blue);
    border-radius: 4px 16px 16px 4px;
    color: #31405a;
    background: rgba(39, 84, 255, 0.055);
    font-size: 0.8rem;
    line-height: 1.45;
}

.sog-step-kicker {
    color: var(--sog-blue);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.14em;
}

.sog-step-title {
    margin-top: 0.35rem;
    color: var(--sog-ink);
    font-family: "Sora", sans-serif;
    font-size: 1.06rem;
    font-weight: 700;
    letter-spacing: -0.03em;
}

.sog-step-copy {
    margin: 0.3rem 0 0.75rem;
    color: var(--sog-ink-soft);
    font-size: 0.82rem;
    line-height: 1.5;
}

.sog-preview-empty {
    min-height: 240px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    padding: 1.2rem;
    border-radius: 20px;
    border: 1px dashed rgba(39, 84, 255, 0.28);
    background:
        linear-gradient(135deg, rgba(238, 243, 255, 0.76), rgba(255, 255, 255, 0.48));
}

.sog-preview-glyph {
    display: grid;
    place-items: center;
    width: 2.7rem;
    height: 2.7rem;
    border-radius: 14px;
    background: var(--sog-navy);
    color: white;
    font-family: "IBM Plex Mono", monospace;
}

.sog-preview-title {
    margin-top: 0.85rem;
    color: var(--sog-ink);
    font-family: "Sora", sans-serif;
    font-size: 1.05rem;
    font-weight: 700;
}

.sog-preview-copy {
    max-width: 28rem;
    margin-top: 0.4rem;
    color: var(--sog-ink-soft);
    font-size: 0.88rem;
    line-height: 1.55;
}

.sog-authority-card {
    margin-top: 0.85rem;
    padding: 0.95rem 1rem;
    border-radius: 18px;
    border: 1px solid rgba(31, 157, 94, 0.18);
    background: rgba(31, 157, 94, 0.065);
}

.sog-authority-title {
    color: #165f3b;
    font-family: "Sora", sans-serif;
    font-size: 0.92rem;
    font-weight: 700;
}

.sog-authority-copy {
    margin-top: 0.35rem;
    color: #3c6551;
    font-size: 0.82rem;
    line-height: 1.5;
    word-break: break-word;
}

.sog-resolved-panel {
    margin: 0.2rem 0 1rem;
    padding: 0.95rem;
    border-radius: 18px;
    border: 1px solid rgba(39, 84, 255, 0.18);
    background: linear-gradient(135deg, rgba(238, 243, 255, 0.88), rgba(255, 255, 255, 0.72));
}

.sog-resolved-head {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
}

.sog-resolved-title {
    color: var(--sog-ink);
    font-family: "Sora", sans-serif;
    font-size: 0.96rem;
    font-weight: 700;
    letter-spacing: -0.025em;
}

.sog-resolved-source {
    color: var(--sog-blue);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.62rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

.sog-target-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.58rem;
}

.sog-target-card {
    min-width: 0;
    padding: 0.72rem 0.76rem;
    border-radius: 14px;
    border: 1px solid rgba(18, 25, 34, 0.08);
    background: rgba(255, 255, 255, 0.72);
}

.sog-target-label {
    color: var(--sog-ink-soft);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.61rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

.sog-target-value {
    margin-top: 0.28rem;
    color: var(--sog-ink);
    font-family: "Sora", sans-serif;
    font-size: 1rem;
    font-weight: 700;
    line-height: 1.25;
}

.sog-target-detail {
    margin-top: 0.18rem;
    color: var(--sog-ink-soft);
    font-size: 0.72rem;
    line-height: 1.35;
    overflow-wrap: anywhere;
}

.sog-noise-wrap {
    margin-top: 0.72rem;
    overflow-x: auto;
    border-radius: 13px;
    border: 1px solid rgba(18, 25, 34, 0.08);
    background: rgba(255, 255, 255, 0.72);
}

.sog-noise-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.72rem;
}

.sog-noise-table th,
.sog-noise-table td {
    padding: 0.48rem 0.58rem;
    border-bottom: 1px solid rgba(18, 25, 34, 0.07);
    text-align: right;
    white-space: nowrap;
}

.sog-noise-table th {
    color: var(--sog-ink-soft);
    background: rgba(39, 84, 255, 0.045);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.6rem;
    text-transform: uppercase;
    letter-spacing: 0.07em;
}

.sog-noise-table th:first-child,
.sog-noise-table td:first-child {
    text-align: left;
}

.sog-noise-table tr:last-child td {
    border-bottom: 0;
}

.sog-advisory-label {
    margin: 0.85rem 0 0.35rem;
    color: var(--sog-ink-soft);
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.62rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

.st-key-scenario_authoring_workspace {
    margin-bottom: 0.75rem;
}

.st-key-scenario_authoring_workspace > div {
    border-color: rgba(18, 25, 34, 0.11) !important;
    border-radius: 20px !important;
    background: rgba(255, 255, 255, 0.72) !important;
    box-shadow: var(--sog-shadow-sm);
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
div[data-testid="stTextInput"] input,
div[data-testid="stTextArea"] textarea {
    color: var(--sog-ink) !important;
    font-family: "Manrope", sans-serif !important;
}

div[data-testid="stChatInput"] textarea::placeholder,
div[data-testid="stTextInput"] input::placeholder,
div[data-testid="stTextArea"] textarea::placeholder {
    color: var(--sog-ink-faint) !important;
    opacity: 1 !important;
}

.stButton > button,
.stDownloadButton > button,
div[data-testid="stFormSubmitButton"] button {
    width: 100%;
    border: none !important;
    border-radius: 14px !important;
    padding: 0.82rem 1rem !important;
    background: linear-gradient(135deg, var(--sog-blue) 0%, #4669ed 100%) !important;
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

.stButton > button:focus-visible,
.stDownloadButton > button:focus-visible,
div[data-testid="stFormSubmitButton"] button:focus-visible,
[data-testid="stTextInput"] input:focus-visible,
[data-testid="stTextArea"] textarea:focus-visible,
[data-testid="stExpander"] summary:focus-visible {
    outline: 3px solid rgba(39, 84, 255, 0.42) !important;
    outline-offset: 2px !important;
}

[data-testid="stTextInput"] input,
[data-testid="stTextArea"] textarea,
[data-baseweb="input"] {
    border-radius: 16px !important;
    border: 1px solid var(--sog-line-strong) !important;
    background: rgba(255, 255, 255, 0.90) !important;
}

[data-testid="stTextInput"] label p,
[data-testid="stTextArea"] label p {
    color: var(--sog-ink) !important;
    font-weight: 650 !important;
}

[data-testid="stTextInput"] button,
[data-testid="stTextArea"] button {
    color: var(--sog-ink-soft) !important;
}

[data-testid="stExpander"] {
    border: 1px solid var(--sog-line) !important;
    border-radius: 16px !important;
    background: rgba(255, 255, 255, 0.56) !important;
}

[data-testid="stExpander"] summary p,
[data-testid="stExpander"] summary svg {
    color: var(--sog-ink) !important;
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

/* Application shell: the canvas is primary; navigation and system state live in the rail. */
.stApp {
    background: #f5f7fa;
}

[data-testid="stAppViewContainer"]::before {
    display: none;
}

.block-container {
    max-width: 1240px;
    padding-top: 2rem;
    padding-bottom: 3rem;
}

[data-testid="stSidebar"] {
    border-right: 1px solid #e1e6ee;
    background: #fbfcfe;
}

[data-testid="stSidebarContent"] {
    padding: 0.8rem 0.72rem 1.5rem;
}

[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
    color: var(--sog-ink-soft);
}

.sog-side-brand {
    display: flex;
    align-items: center;
    gap: 0.7rem;
    padding: 0.35rem 0.35rem 1.05rem;
    border-bottom: 1px solid #e7eaf0;
}

.sog-side-mark {
    display: grid;
    place-items: center;
    width: 2.25rem;
    height: 2.25rem;
    border-radius: 10px;
    background: #1a2740;
    color: white;
    font-family: "Sora", sans-serif;
    font-weight: 800;
}

.sog-side-brand-name {
    color: #172033;
    font-family: "Sora", sans-serif;
    font-size: 0.94rem;
    font-weight: 700;
    letter-spacing: -0.025em;
}

.sog-side-brand-copy {
    margin-top: 0.08rem;
    color: #737e8d;
    font-size: 0.72rem;
}

.sog-side-section-label {
    margin: 1.05rem 0.35rem 0.48rem;
    color: #8791a0;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.62rem;
    font-weight: 500;
    letter-spacing: 0.12em;
    text-transform: uppercase;
}

.sog-side-nav {
    display: grid;
    gap: 0.28rem;
}

.sog-side-nav-item {
    display: grid;
    grid-template-columns: 1.65rem minmax(0, 1fr);
    gap: 0.58rem;
    align-items: center;
    padding: 0.58rem 0.62rem;
    border-radius: 10px;
    color: #465267;
}

.sog-side-nav-item.is-active {
    background: #edf2ff;
    color: #284cb8;
}

.sog-side-nav-item > span {
    display: grid;
    place-items: center;
    height: 1.65rem;
    border: 1px solid #d8deea;
    border-radius: 8px;
    background: white;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.63rem;
}

.sog-side-nav-item strong,
.sog-side-nav-item small {
    display: block;
}

.sog-side-nav-item strong {
    color: inherit;
    font-size: 0.79rem;
    font-weight: 700;
}

.sog-side-nav-item small {
    margin-top: 0.08rem;
    color: #7d8795;
    font-size: 0.67rem;
}

.sog-side-session,
.sog-side-provider {
    border: 1px solid #e3e7ee;
    border-radius: 12px;
    background: white;
}

.sog-side-session {
    padding: 0.7rem;
}

.sog-side-session-head,
.sog-side-provider-head {
    display: flex;
    align-items: center;
    gap: 0.52rem;
}

.sog-side-session-head {
    justify-content: space-between;
    padding-bottom: 0.55rem;
    border-bottom: 1px solid #edf0f4;
}

.sog-side-session-head strong {
    color: #263247;
    font-size: 0.76rem;
}

.sog-side-session-head span {
    padding: 0.22rem 0.42rem;
    border-radius: 999px;
    background: #e9f7ef;
    color: #17633e;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.58rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
}

.sog-side-session dl {
    margin: 0;
}

.sog-side-session dl > div {
    display: grid;
    grid-template-columns: 4rem minmax(0, 1fr);
    gap: 0.5rem;
    padding: 0.48rem 0;
    border-bottom: 1px solid #f0f2f5;
}

.sog-side-session dt {
    color: #8a94a2;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.6rem;
    text-transform: uppercase;
}

.sog-side-session dd {
    min-width: 0;
    margin: 0;
    overflow-wrap: anywhere;
    color: #344055;
    font-size: 0.7rem;
    font-weight: 650;
    text-align: right;
}

.sog-side-runs {
    padding-top: 0.55rem;
}

.sog-side-runs-label {
    margin-bottom: 0.32rem;
    color: #8a94a2;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.58rem;
    text-transform: uppercase;
}

.sog-side-run {
    display: flex;
    justify-content: space-between;
    gap: 0.5rem;
    color: #667184;
    font-size: 0.67rem;
}

.sog-side-run strong {
    color: #344055;
}

.sog-side-empty {
    color: #8490a0;
    font-size: 0.68rem;
    line-height: 1.45;
}

.sog-side-authority {
    display: grid;
    grid-template-columns: 1.45rem minmax(0, 1fr);
    gap: 0.55rem;
    margin-top: 0.65rem;
    padding: 0.65rem;
    border-radius: 10px;
    background: #eef8f2;
    color: #426454;
    font-size: 0.68rem;
    line-height: 1.4;
}

.sog-side-authority > span {
    display: grid;
    place-items: center;
    height: 1.45rem;
    border-radius: 999px;
    background: #17633e;
    color: white;
    font-weight: 800;
}

.sog-side-authority strong {
    display: block;
    margin-bottom: 0.12rem;
    color: #204d36;
}

.sog-side-provider {
    padding: 0.65rem;
}

.sog-side-provider.is-secondary {
    margin-top: 0.65rem;
    background: #f8f9fb;
}

.sog-side-provider-head {
    display: grid;
    grid-template-columns: auto minmax(0, 1fr) auto;
}

.sog-side-provider-head strong,
.sog-side-provider-head small {
    display: block;
}

.sog-side-provider-head strong {
    color: #273246;
    font-size: 0.76rem;
}

.sog-side-provider-head small,
.sog-side-provider p {
    color: #778292;
    font-size: 0.65rem;
}

.sog-side-provider p {
    margin: 0.48rem 0 0;
    line-height: 1.45;
}

.sog-side-provider .sog-provider-mark {
    width: 1.7rem;
    height: 1.7rem;
    border-radius: 8px;
    font-size: 0.72rem;
}

.sog-side-provider.is-secondary .sog-provider-mark {
    background: #41516a;
}

.sog-side-provider .sog-provider-status {
    padding: 0.24rem 0.38rem;
    font-size: 0.53rem;
    font-style: normal;
}

[data-testid="stSidebar"] [data-testid="stTextInput"] {
    margin-top: 0.55rem;
}

[data-testid="stSidebar"] [data-testid="stTextInput"] label p {
    color: #596579 !important;
    font-size: 0.7rem !important;
}

[data-testid="stSidebar"] [data-testid="stTextInput"] input {
    min-height: 2.35rem;
    border-radius: 9px !important;
    font-size: 0.75rem;
}

[data-testid="stSidebar"] [data-testid="stExpander"] {
    border-radius: 10px !important;
    background: #f8f9fb !important;
}

[data-testid="stSidebar"] [data-testid="stExpander"] summary p {
    font-size: 0.7rem;
}

.sog-page-head {
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    gap: 2rem;
    margin: 0 0 1.15rem;
}

.sog-page-kicker {
    color: #3157c8;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.66rem;
    font-weight: 500;
    letter-spacing: 0.14em;
    text-transform: uppercase;
}

.sog-page-head h1 {
    margin: 0.35rem 0 0;
    padding: 0 !important;
    color: #172033;
    font-size: clamp(2rem, 3vw, 2.75rem);
    line-height: 1.08;
    letter-spacing: -0.045em;
}

.sog-page-head h1 [data-testid="stHeaderActionElements"] {
    display: none;
}

.sog-page-head p {
    max-width: 48rem;
    margin: 0.55rem 0 0;
    color: #667184;
    font-size: 0.92rem;
    line-height: 1.55;
}

.sog-page-state {
    display: inline-flex;
    align-items: center;
    flex: 0 0 auto;
    gap: 0.42rem;
    padding: 0.44rem 0.66rem;
    border: 1px solid #dce2eb;
    border-radius: 999px;
    background: white;
    color: #536074;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.62rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

.sog-page-state > span {
    width: 0.45rem;
    height: 0.45rem;
    border-radius: 999px;
    background: #2c9a5f;
    box-shadow: 0 0 0 3px rgba(44, 154, 95, 0.12);
}

.sog-page-divider {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin-bottom: 0.75rem;
    color: #7a8595;
    font-family: "IBM Plex Mono", monospace;
    font-size: 0.59rem;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}

.sog-page-divider i {
    width: 1.25rem;
    height: 1px;
    background: #cbd2dc;
}

.sog-canvas-label {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 1rem;
    margin-bottom: 0.45rem;
}

.sog-canvas-label span {
    color: #2d394d;
    font-size: 0.76rem;
    font-weight: 700;
}

.sog-canvas-label small {
    color: #7a8595;
    font-size: 0.68rem;
}

.st-key-scenario_authoring_workspace > div {
    border: 1px solid #e0e5ed !important;
    border-radius: 15px !important;
    background: white !important;
    box-shadow: 0 8px 28px rgba(31, 42, 61, 0.055);
}

.sog-step-kicker {
    color: #3157c8;
}

.sog-step-title {
    font-size: 1.12rem;
}

.sog-preview-empty {
    min-height: 290px;
    border-radius: 12px;
    border-color: #cfd8e8;
    background: #f8faff;
}

.stButton > button,
.stDownloadButton > button,
div[data-testid="stFormSubmitButton"] button {
    min-height: 2.6rem;
    border-radius: 10px !important;
    background: #3559d5 !important;
    box-shadow: none !important;
}

.stButton > button:hover,
.stDownloadButton > button:hover,
div[data-testid="stFormSubmitButton"] button:hover {
    background: #294bbb !important;
    box-shadow: 0 6px 18px rgba(53, 89, 213, 0.18) !important;
}

[class*="st-key-authoring_example_"] button,
.st-key-clear_scenario_proposal button {
    border: 1px solid #dce2ea !important;
    background: #f7f8fa !important;
    color: #435066 !important;
    box-shadow: none !important;
}

[class*="st-key-authoring_example_"] button:hover,
.st-key-clear_scenario_proposal button:hover {
    border-color: #b8c5df !important;
    background: #eef2fb !important;
    color: #294da9 !important;
}

[data-testid="stTextArea"] textarea,
[data-testid="stTextInput"] input {
    border-radius: 10px !important;
    border-color: #cfd6e1 !important;
    background: white !important;
}

@media (max-width: 960px) {
    .sog-hero-grid,
    .sog-quick-grid,
    .sog-provider-grid,
    .sog-download-grid,
    .sog-stat-grid,
    .sog-target-grid {
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
        font-size: clamp(2.3rem, 10vw, 3.4rem);
        text-wrap: pretty;
    }

    .sog-resolved-head {
        align-items: flex-start;
        flex-direction: column;
    }

    .sog-page-head {
        align-items: flex-start;
    }

    .block-container {
        padding-top: 0.8rem;
    }
}

@media (max-width: 600px) {
    .block-container {
        width: 100%;
        padding: 3.8rem 0.75rem 2.5rem !important;
        overflow-x: hidden;
    }

    .sog-hero,
    .sog-workbench-head,
    .st-key-scenario_authoring_workspace > div {
        max-width: 100%;
        border-radius: 18px !important;
    }

    .sog-brand-row {
        padding: 0.75rem 0.85rem;
    }

    .sog-brand-side {
        display: none;
    }

    .sog-hero-grid {
        gap: 0.75rem;
        padding: 0.8rem 0.85rem 0.9rem;
    }

    .sog-display {
        font-size: clamp(2.05rem, 10.5vw, 2.75rem);
    }

    .sog-lead {
        margin-bottom: 0;
        font-size: 0.92rem;
    }

    .sog-chip-row {
        display: none;
    }

    .sog-console,
    .sog-workbench-head {
        padding: 0.78rem;
    }

    .sog-stage-rail {
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.38rem;
    }

    .sog-stage {
        min-width: 0;
        align-items: flex-start;
        flex-direction: column;
        padding: 0.48rem;
    }

    .sog-stage strong {
        font-size: 0.7rem;
        line-height: 1.25;
    }

    .sog-stage span {
        display: none;
    }

    .sog-provider-card {
        grid-template-columns: auto minmax(0, 1fr);
        align-items: start;
    }

    .sog-provider-status {
        grid-column: 2;
        justify-self: start;
        white-space: normal;
    }

    .sog-boundary-note {
        overflow-wrap: anywhere;
    }

    .sog-page-head {
        align-items: flex-start;
        flex-direction: column;
        gap: 0.75rem;
        margin-bottom: 0.85rem;
    }

    .sog-page-head h1 {
        font-size: 2rem;
    }

    .sog-page-divider {
        flex-wrap: wrap;
        gap: 0.38rem;
    }

    .sog-page-divider i {
        width: 0.8rem;
    }

    .sog-canvas-label {
        align-items: flex-start;
        flex-direction: column;
        gap: 0.15rem;
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


def _render_app_shell(context: dict[str, object], pending_job_id: str | None) -> None:
    """Render compact navigation and keep the authoring canvas visually primary."""
    last_run = str(context.get("last_run_id") or "No completed run")
    last_scenario = str(context.get("last_scenario_id") or "No active draft")
    run_history = list(context.get("run_history", []) or [])[:3]
    queue_state = "Pipeline active" if pending_job_id else "Ready"

    history_markup = ""
    for item in run_history:
        run_id = escape(str(item.get("run_id", "")))
        scenario_id = escape(str(item.get("scenario_id", "scenario")))
        history_markup += (
            '<div class="sog-side-run">'
            f"<span>{scenario_id}</span><strong>{run_id}</strong>"
            "</div>"
        )
    if not history_markup:
        history_markup = (
            '<div class="sog-side-empty">'
            "Artifacts will appear here after the first completed run."
            "</div>"
        )

    with st.sidebar:
        st.markdown(
            f"""
            <div class="sog-side-brand">
                <div class="sog-side-mark">S</div>
                <div>
                    <div class="sog-side-brand-name">SOG Studio</div>
                    <div class="sog-side-brand-copy">Entity-resolution benchmarks</div>
                </div>
            </div>
            <div class="sog-side-section-label">Workflow</div>
            <nav class="sog-side-nav" aria-label="Scenario workflow">
                <div class="sog-side-nav-item is-active">
                    <span>01</span><div><strong>Describe</strong><small>Write the experiment brief</small></div>
                </div>
                <div class="sog-side-nav-item">
                    <span>02</span><div><strong>Review</strong><small>Inspect targets and YAML</small></div>
                </div>
                <div class="sog-side-nav-item">
                    <span>03</span><div><strong>Approve & run</strong><small>Execute validated YAML</small></div>
                </div>
            </nav>
            <div class="sog-side-section-label">Current session</div>
            <section class="sog-side-session">
                <div class="sog-side-session-head">
                    <strong>Status</strong><span>{escape(queue_state)}</span>
                </div>
                <dl>
                    <div><dt>Scenario</dt><dd>{escape(last_scenario)}</dd></div>
                    <div><dt>Last run</dt><dd>{escape(last_run)}</dd></div>
                    <div><dt>Session</dt><dd>{escape(st.session_state.session_id)}</dd></div>
                </dl>
                <div class="sog-side-runs">
                    <div class="sog-side-runs-label">Recent artifacts</div>
                    {history_markup}
                </div>
            </section>
            <div class="sog-side-authority">
                <span aria-hidden="true">&#10003;</span>
                <div><strong>Authoritative specification</strong>Only approved, validated YAML enters the deterministic pipeline.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(
        f"""
        <header class="sog-page-head">
            <div>
                <div class="sog-page-kicker">Scenario authoring</div>
                <h1>Build an ER benchmark</h1>
                <p>Describe the population, records, overlap, noise, and timing. Review the resolved configuration before anything runs.</p>
            </div>
            <div class="sog-page-state"><span></span>{escape(queue_state)}</div>
        </header>
        <div class="sog-page-divider">
            <span>Natural-language proposal</span><i></i>
            <span>Validated YAML</span><i></i>
            <span>Deterministic generation</span>
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


def _format_resolved_number(value: object) -> str:
    if isinstance(value, bool) or value is None:
        return "Not fixed"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float) and value.is_integer():
        return f"{int(value):,}"
    return escape(str(value))


def _format_resolved_pct(value: object) -> str:
    if isinstance(value, bool) or value is None:
        return "Not set"
    try:
        return f"{float(value):g}%"
    except (TypeError, ValueError):
        return escape(str(value))


def _scenario_authoring_module():
    """Load the current authoring API even during a Streamlit hot reload."""
    import importlib

    sog_tools_module = importlib.import_module("sog_tools")
    if getattr(sog_tools_module, "SOG_TOOLS_API_VERSION", 0) < 2:
        importlib.reload(sog_tools_module)
    authoring = importlib.import_module("scenario_authoring")
    if getattr(authoring, "SCENARIO_AUTHORING_API_VERSION", 0) < 3:
        authoring = importlib.reload(authoring)
    return authoring


def _render_resolved_candidate(candidate: dict[str, object]) -> None:
    """Show runtime-resolved facts separately from model-authored narrative."""
    # Streamlit reruns this script with runpy, while imported helper modules can
    # remain cached across a source update. Refresh only when the running module
    # predates the resolved-review API so a live development session can recover
    # without exposing a stale-module ImportError to the user.
    authoring = _scenario_authoring_module()
    review_noise_keys = authoring.REVIEW_NOISE_KEYS
    summarize_candidate = authoring.summarize_candidate

    resolved = summarize_candidate(candidate)
    if not resolved["runtime_emission_valid"]:
        st.error(
            "Runtime emission parsing failed: "
            + str(resolved.get("runtime_emission_error") or "unknown error")
        )
        return

    population_mode = str(resolved.get("population_mode") or "")
    population_value = resolved.get("population_value")
    if population_mode == "count":
        population_label = _format_resolved_number(population_value)
        population_detail = "distinct Phase-1 people selected"
    elif population_mode == "pct":
        population_label = _format_resolved_pct(population_value)
        population_detail = "of available Phase-1 people"
    else:
        population_label = "Not fixed"
        population_detail = "selection mode is not resolved"

    record_counts = resolved.get("record_counts", {})
    if not isinstance(record_counts, dict):
        record_counts = {}
    record_detail = " · ".join(
        f"{escape(str(dataset_id))} {_format_resolved_number(count)}"
        for dataset_id, count in record_counts.items()
    )
    total_records = resolved.get("total_records")
    if total_records is None:
        record_detail = "No exact target for every source"

    periods = _format_resolved_number(resolved.get("simulation_periods"))
    granularity = escape(str(resolved.get("simulation_granularity") or "periods"))
    start_date = escape(str(resolved.get("simulation_start") or "not set"))
    seed = _format_resolved_number(resolved.get("seed"))

    noise = resolved.get("noise", {})
    noise_markup = ""
    if isinstance(noise, dict) and noise:
        dataset_ids = list(noise)
        labels = {
            "nickname_pct": "Nickname",
            "name_typo_pct": "Name typo",
            "address_missing_pct": "Address missing",
            "dob_shift_pct": "DOB shift",
        }
        header = "".join(f"<th>{escape(str(item))}</th>" for item in dataset_ids)
        rows = ""
        for key in review_noise_keys:
            values = "".join(
                f"<td>{_format_resolved_pct(noise[item].get(key))}</td>"
                for item in dataset_ids
            )
            rows += f"<tr><td>{escape(labels.get(key, key))}</td>{values}</tr>"
        noise_markup = (
            '<div class="sog-noise-wrap">'
            '<table class="sog-noise-table" aria-label="Resolved identity noise by dataset">'
            f"<thead><tr><th>Identity noise</th>{header}</tr></thead>"
            f"<tbody>{rows}</tbody>"
            "</table></div>"
        )

    st.markdown(
        f"""
        <section class="sog-resolved-panel">
            <div class="sog-resolved-head">
                <div class="sog-resolved-title">Resolved experiment targets</div>
                <div class="sog-resolved-source">runtime parser / candidate YAML · seed {seed}</div>
            </div>
            <div class="sog-target-grid">
                <div class="sog-target-card">
                    <div class="sog-target-label">Population</div>
                    <div class="sog-target-value">{population_label}</div>
                    <div class="sog-target-detail">{population_detail}</div>
                </div>
                <div class="sog-target-card">
                    <div class="sog-target-label">Observed records</div>
                    <div class="sog-target-value">{_format_resolved_number(total_records)}</div>
                    <div class="sog-target-detail">{record_detail}</div>
                </div>
                <div class="sog-target-card">
                    <div class="sog-target-label">Entity overlap</div>
                    <div class="sog-target-value">{_format_resolved_pct(resolved.get('overlap_entity_pct'))}</div>
                    <div class="sog-target-detail">across {len(record_counts)} observed sources</div>
                </div>
                <div class="sog-target-card">
                    <div class="sog-target-label">Simulation</div>
                    <div class="sog-target-value">{periods} {granularity}</div>
                    <div class="sog-target-detail">starting {start_date}</div>
                </div>
            </div>
            {noise_markup}
        </section>
        <div class="sog-advisory-label">Model-provided rationale · advisory only</div>
        """,
        unsafe_allow_html=True,
    )


def _render_access_gate() -> str:
    st.markdown(
        """
        <div class="sog-access-card">
            <div class="sog-access-title">Optional: connect the Claude analyst</div>
            <div class="sog-access-copy">
                Anthropic is <strong>not required</strong> for NVIDIA scenario authoring, YAML validation,
                approval, pipeline execution, or artifact generation. Add a Claude key only if you want the
                separate conversational analyst and export assistant below.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    return st.text_input(
        "Anthropic API key",
        type="password",
        placeholder="sk-ant-...",
        key="anthropic_optional_api_key",
        help="Optional. Stored only in the current process and used by the Claude chat assistant.",
    )


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

if "scenario_authoring_bundle" not in st.session_state:
    st.session_state.scenario_authoring_bundle = None
if "approved_scenario_yaml" not in st.session_state:
    st.session_state.approved_scenario_yaml = None


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
def _get_orchestrator(api_key: str):
    from agents.orchestrator import Orchestrator

    return Orchestrator(api_key=api_key)


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

            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            try:
                if api_key:
                    orch = _get_orchestrator(api_key)
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
                else:
                    from sog_tools import get_run_results

                    run_result = get_run_results(run_id)
                    st.session_state.last_run_downloads = run_result.get("download_paths", {})
                    quality = run_result.get("quality_status", "unknown")
                    message = f"Run complete: **{run_id}**\n\nQuality status: **{quality}**."
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


def _render_scenario_authoring() -> None:
    """Render the proposal-first NVIDIA scenario authoring workbench."""
    authoring = _scenario_authoring_module()
    DEFAULT_NVIDIA_API_BASE_URL = authoring.DEFAULT_NVIDIA_API_BASE_URL
    DEFAULT_NVIDIA_MODEL = authoring.DEFAULT_NVIDIA_MODEL
    NvidiaScenarioProposalClient = authoring.NvidiaScenarioProposalClient
    ScenarioAuthoringAgent = authoring.ScenarioAuthoringAgent

    configured_key = os.environ.get("NVIDIA_API_KEY", "").strip()
    entered_key = str(st.session_state.get("nvidia_scenario_api_key", "")).strip()
    nvidia_ready = bool(configured_key or entered_key)
    anthropic_ready = bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
    nvidia_status = "Connected" if nvidia_ready else "Key required"
    anthropic_status = "Connected" if anthropic_ready else "Optional"
    nvidia_status_class = " is-connected" if nvidia_ready else ""
    anthropic_status_class = " is-connected" if anthropic_ready else ""

    with st.sidebar:
        st.markdown(
            f"""
            <div class="sog-side-section-label">Authoring provider</div>
            <div class="sog-side-provider">
                <div class="sog-side-provider-head">
                    <span class="sog-provider-mark">N</span>
                    <div><strong>NVIDIA NIM</strong><small>Structured proposal model</small></div>
                    <em class="sog-provider-status{nvidia_status_class}">{escape(nvidia_status)}</em>
                </div>
                <p>Used only to translate your brief into a constrained proposal.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        api_key = st.text_input(
            "NVIDIA API key",
            type="password",
            value="",
            key="nvidia_scenario_api_key",
            placeholder=(
                "nvapi-..."
                if not configured_key
                else "Using NVIDIA_API_KEY from the environment"
            ),
            help="Use the key from build.nvidia.com. It is sent only as the API authorization header.",
        )
        with st.expander("Provider settings", expanded=False):
            model = st.text_input(
                "Model",
                value=os.environ.get("NVIDIA_SCENARIO_MODEL", DEFAULT_NVIDIA_MODEL),
                key="nvidia_scenario_model",
                help="Nemotron 3.5 Lightning is the default structured-authoring model.",
            )
            base_url = st.text_input(
                "API base URL",
                value=os.environ.get(
                    "NVIDIA_API_BASE_URL", DEFAULT_NVIDIA_API_BASE_URL
                ),
                key="nvidia_scenario_base_url",
                help="Change this only for another compatible NIM endpoint.",
            )
        st.markdown(
            f"""
            <div class="sog-side-provider is-secondary">
                <div class="sog-side-provider-head">
                    <span class="sog-provider-mark">C</span>
                    <div><strong>Claude analyst</strong><small>Post-run analysis</small></div>
                    <em class="sog-provider-status{anthropic_status_class}">{escape(anthropic_status)}</em>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(
        """
        <div class="sog-canvas-label">
            <span>New scenario draft</span>
            <small>Nothing is written until validation and explicit approval succeed.</small>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.container(border=True, key="scenario_authoring_workspace"):
        editor_col, review_col = st.columns([1.02, 0.98], gap="large")

        with editor_col:
            st.markdown(
                """
                <div class="sog-step-kicker">Step 01 / experiment brief</div>
                <div class="sog-step-title">Describe the benchmark you need</div>
                <div class="sog-step-copy">
                    Specify people and record targets, sources, overlap, noise, dates, events, and seed.
                </div>
                """,
                unsafe_allow_html=True,
            )

            example_prompts = {
                "Clean linkage": (
                    "Create a clean two-source linkage benchmark using the reference baseline. "
                    "Use 85% entity overlap, one-to-one cardinality, 2% duplication in each source, "
                    "and only light name and address noise."
                ),
                "Identity drift": (
                    "Create a difficult identity-drift benchmark with nickname, phonetic, OCR, ZIP, "
                    "and date-swap errors. Keep household dynamics simple so the field corruption is isolated."
                ),
                "Single-file dedup": (
                    "Create one observed registry for within-file deduplication with 15% duplicated records, "
                    "moderate name noise, and full entity appearance."
                ),
            }
            example_columns = st.columns(3, gap="small")
            for column, (label, example) in zip(example_columns, example_prompts.items()):
                with column:
                    if st.button(label, key=f"authoring_example_{label}", use_container_width=True):
                        st.session_state.scenario_authoring_requirements = example
                        st.rerun()

            requirements = st.text_area(
                "Experiment requirements",
                key="scenario_authoring_requirements",
                height=190,
                placeholder=(
                    "Example: Build a clean two-source linkage benchmark with 80% overlap, "
                    "2% duplication in each source, moderate nickname noise, and a fixed seed."
                ),
                help="This text is sent to the configured NVIDIA endpoint only when you create a proposal.",
            )

            if st.button(
                "Generate structured proposal",
                type="primary",
                key="create_scenario_proposal",
                use_container_width=True,
            ):
                try:
                    client = NvidiaScenarioProposalClient(
                        api_key=api_key.strip() or configured_key,
                        model=model.strip() or DEFAULT_NVIDIA_MODEL,
                        base_url=base_url.strip() or DEFAULT_NVIDIA_API_BASE_URL,
                    )
                    with st.spinner("Mapping the brief to the live SOG scenario schema..."):
                        st.session_state.scenario_authoring_bundle = ScenarioAuthoringAgent(
                            client
                        ).propose(requirements)
                    st.session_state.approved_scenario_yaml = None
                except Exception as exc:
                    st.session_state.scenario_authoring_bundle = None
                    print(
                        f"[scenario-authoring] {type(exc).__name__}: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                    st.error(str(exc))

        with review_col:
            st.markdown(
                """
                <div class="sog-step-kicker">Step 02 / review surface</div>
                <div class="sog-step-title">Inspect before anything is written</div>
                <div class="sog-step-copy">
                    Confirm resolved targets first; open JSON or YAML only when you need the full detail.
                </div>
                """,
                unsafe_allow_html=True,
            )

            bundle = st.session_state.scenario_authoring_bundle
            if not bundle:
                st.markdown(
                    """
                    <div class="sog-preview-empty">
                        <div class="sog-preview-glyph">{ }</div>
                        <div class="sog-preview-title">Waiting for a proposal</div>
                        <div class="sog-preview-copy">
                            Choose an example or write a detailed experiment brief. The review surface will show
                            the template choice, proposed overrides, validator status, and candidate YAML here.
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                proposal = bundle["proposal"]
                validation = bundle["validation"]
                preflight = bundle.get("preflight") or authoring.validate_scenario_runtime_inputs(
                    bundle["candidate"]
                )
                ready_to_approve = bool(
                    validation.get("valid") and preflight.get("valid")
                )
                metric_columns = st.columns(3)
                metric_columns[0].metric("Template", proposal["template_id"])
                metric_columns[1].metric("Scenario", proposal["scenario_id"])
                metric_columns[2].metric(
                    "Validation", "Ready" if ready_to_approve else "Blocked"
                )

                overview_tab, json_tab, yaml_tab = st.tabs(
                    ["Decision summary", "Proposal JSON", "Candidate YAML"]
                )
                with overview_tab:
                    _render_resolved_candidate(bundle["candidate"])
                    st.markdown(proposal.get("summary") or "No summary supplied.")
                    assumptions = proposal.get("assumptions", [])
                    warnings = proposal.get("warnings", [])
                    if assumptions:
                        st.markdown("**Assumptions**")
                        st.markdown("\n".join(f"- {item}" for item in assumptions))
                    if warnings:
                        st.markdown("**Warnings**")
                        st.markdown("\n".join(f"- {item}" for item in warnings))
                    if not validation.get("valid"):
                        st.error(
                            "Approval blocked by scenario validation: "
                            + "; ".join(validation.get("errors", []))
                        )
                    elif not preflight.get("valid"):
                        st.error(
                            "Approval blocked by runtime-input preflight: "
                            + "; ".join(preflight.get("errors", []))
                        )
                    else:
                        st.success(
                            "Schema, semantic validation, and Phase-1 input preflight passed. "
                            "No file has been written."
                        )
                        resolved_data_path = preflight.get("resolved_paths", {}).get(
                            "data_path"
                        )
                        if resolved_data_path:
                            st.caption(f"Resolved Phase-1 CSV: {resolved_data_path}")
                with json_tab:
                    st.json(proposal)
                with yaml_tab:
                    st.code(bundle["candidate_yaml"], language="yaml")

                action_columns = st.columns([1, 1])
                with action_columns[0]:
                    if ready_to_approve and st.button(
                        "Approve validated YAML",
                        type="primary",
                        key=f"approve_{proposal['proposal_id']}",
                        use_container_width=True,
                    ):
                        try:
                            approved = ScenarioAuthoringAgent(client=None).approve(
                                proposal,
                                session_id=SESSION_ID,
                                expected_proposal_id=proposal["proposal_id"],
                            )
                            st.session_state.approved_scenario_yaml = approved
                            st.session_state.context["last_scenario_id"] = approved[
                                "scenario_id"
                            ]
                            _persist()
                            st.rerun()
                        except Exception as exc:
                            st.error(str(exc))
                with action_columns[1]:
                    if st.button(
                        "Clear proposal",
                        key="clear_scenario_proposal",
                        use_container_width=True,
                    ):
                        st.session_state.scenario_authoring_bundle = None
                        st.session_state.approved_scenario_yaml = None
                        st.rerun()

        approved = st.session_state.approved_scenario_yaml
        if approved:
            st.markdown(
                f"""
                <div class="sog-authority-card">
                    <div class="sog-authority-title">Validated YAML is now authoritative</div>
                    <div class="sog-authority-copy">
                        <strong>{escape(approved['scenario_id'])}</strong> was approved as a session-scoped YAML.
                        SHA-256: <code>{escape(approved['yaml_sha256'][:16])}…</code><br>
                        {escape(approved['yaml_path'])}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            run_col, note_col = st.columns([0.36, 0.64], vertical_alignment="center")
            with run_col:
                if st.button(
                    "Run approved scenario",
                    type="primary",
                    key=f"run_{approved['proposal_id']}",
                    use_container_width=True,
                ):
                    from sog_tools import submit_run_async

                    submitted = submit_run_async(
                        approved["scenario_id"],
                        session_id=SESSION_ID,
                        overwrite=False,
                    )
                    if submitted.get("error"):
                        st.error(submitted["error"])
                    else:
                        st.session_state.pending_job_id = submitted["job_id"]
                        _persist()
                        st.rerun()
            with note_col:
                st.caption(
                    "Execution uses the existing deterministic pipeline. The model cannot alter the YAML, "
                    "truth tables, observations, events, or benchmark artifacts after approval."
                )


_inject_theme()
_render_app_shell(st.session_state.context, st.session_state.pending_job_id)
_poll_job()
_render_scenario_authoring()

anthropic_enabled = bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
if anthropic_enabled and (
    st.session_state.messages or st.session_state.context.get("last_run_id")
):
    _render_section_intro(
        "Optional Claude workspace / connected",
        "Analyze and package completed runs",
        "Use the conversational analyst for result interpretation, visualization, and export packaging.",
    )

if not st.session_state.messages and anthropic_enabled:
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
    _render_section_intro(
        "Artifacts",
        "Output shelf",
        "Download the latest benchmark artifacts directly from this session.",
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
            with open(path, "rb") as handle:
                st.download_button(
                    f"Download {label}",
                    handle.read(),
                    file_name=path.name,
                    key=f"dl_{label}",
                )

if not anthropic_enabled:
    with st.sidebar.expander("Connect the optional Claude analyst", expanded=False):
        key = _render_access_gate()
        if st.button(
            "Connect Claude analyst",
            key="connect_optional_anthropic",
            use_container_width=True,
        ) and key.strip():
            os.environ["ANTHROPIC_API_KEY"] = key.strip()
            st.rerun()

prompt = None
if anthropic_enabled:
    prompt = st.chat_input(
        "Ask the Claude analyst about results, charts, or exports...",
        key="anthropic_chat_input",
    )

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    _persist()

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Working..."):
            try:
                api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                orch = _get_orchestrator(api_key)
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
