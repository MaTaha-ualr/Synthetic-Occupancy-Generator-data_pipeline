"""SOG Assistant — Streamlit chatbot with enhanced UI/UX and branding.

Redesigned with professional branding, smooth animations, and improved visual hierarchy.
Branding: Taha Mohammed © 2024 | UALR | ERIQ

Start with:
    cd SOG
    streamlit run frontend/chatbot_v2.py
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════════════
# Environment Setup
# ═══════════════════════════════════════════════════════════════════════════════

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

# ═══════════════════════════════════════════════════════════════════════════════
# Branding Constants
# ═══════════════════════════════════════════════════════════════════════════════

BRANDING = {
    "author": "Taha Mohammed",
    "year": "2024",
    "institution": "UALR",
    "lab": "ERIQ",
    "full_title": "Synthetic Occupancy Generator",
    "short_title": "SOG",
    "tagline": "Entity Resolution Benchmarking Platform",
}

# ═══════════════════════════════════════════════════════════════════════════════
# Enhanced CSS with Animations and Branding
# ═══════════════════════════════════════════════════════════════════════════════

APP_CSS = """
<style>
/* ═══════════════════════════════════════════════════════════════════════════ */
/* CSS VARIABLES — SOG Design System                                          */
/* ═══════════════════════════════════════════════════════════════════════════ */

:root {
    /* Primary Colors */
    --sog-bg-deep: #050a0f;
    --sog-bg-primary: #08131a;
    --sog-bg-secondary: #0b1822;
    --sog-bg-tertiary: #0f2130;
    --sog-bg-card: rgba(11, 30, 42, 0.85);
    
    /* Accent Colors */
    --sog-accent-cyan: #7df0d0;
    --sog-accent-cyan-dim: rgba(125, 240, 208, 0.15);
    --sog-accent-orange: #ff9b62;
    --sog-accent-orange-dim: rgba(255, 155, 98, 0.15);
    --sog-accent-blue: #74e5ff;
    --sog-accent-purple: #a78bfa;
    
    /* Text Colors */
    --sog-text-primary: #f5ebdb;
    --sog-text-secondary: #c9d1d9;
    --sog-text-muted: #8b949e;
    --sog-text-subtle: rgba(158, 182, 194, 0.7);
    
    /* Borders & Lines */
    --sog-border-subtle: rgba(125, 240, 208, 0.12);
    --sog-border-medium: rgba(125, 240, 208, 0.25);
    --sog-border-strong: rgba(125, 240, 208, 0.4);
    
    /* Spacing */
    --sog-space-xs: 0.5rem;
    --sog-space-sm: 0.75rem;
    --sog-space-md: 1rem;
    --sog-space-lg: 1.5rem;
    --sog-space-xl: 2rem;
    --sog-space-2xl: 3rem;
    
    /* Shadows */
    --sog-shadow-sm: 0 2px 8px rgba(0, 0, 0, 0.3);
    --sog-shadow-md: 0 8px 24px rgba(0, 0, 0, 0.4);
    --sog-shadow-lg: 0 16px 48px rgba(0, 0, 0, 0.5);
    --sog-shadow-glow: 0 0 40px rgba(125, 240, 208, 0.15);
    
    /* Typography */
    --font-display: "Fraunces", Georgia, serif;
    --font-body: "Inter", -apple-system, BlinkMacSystemFont, sans-serif;
    --font-mono: "JetBrains Mono", "Fira Code", monospace;
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* GLOBAL RESETS & BASE STYLES                                                */
/* ═══════════════════════════════════════════════════════════════════════════ */

@import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,400;9..144,500;9..144,600;9..144,700&family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

html, body {
    font-family: var(--font-body);
    background: var(--sog-bg-primary);
    color: var(--sog-text-primary);
}

.stApp {
    background: 
        /* Deep gradient base */
        radial-gradient(ellipse 80% 50% at 20% 40%, rgba(125, 240, 208, 0.03), transparent),
        radial-gradient(ellipse 60% 40% at 80% 60%, rgba(255, 155, 98, 0.02), transparent),
        radial-gradient(ellipse 50% 30% at 50% 100%, rgba(116, 229, 255, 0.02), transparent),
        linear-gradient(180deg, var(--sog-bg-deep) 0%, var(--sog-bg-primary) 50%, var(--sog-bg-secondary) 100%);
}

/* Grid Pattern Overlay */
[data-testid="stAppViewContainer"]::before {
    content: "";
    position: fixed;
    inset: 0;
    pointer-events: none;
    background-image: 
        linear-gradient(rgba(125, 240, 208, 0.015) 1px, transparent 1px),
        linear-gradient(90deg, rgba(125, 240, 208, 0.015) 1px, transparent 1px);
    background-size: 60px 60px;
    mask-image: linear-gradient(180deg, rgba(0,0,0,0.4) 0%, transparent 80%);
    animation: gridPulse 20s ease-in-out infinite;
}

@keyframes gridPulse {
    0%, 100% { opacity: 0.3; }
    50% { opacity: 0.6; }
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* HEADER / HERO SECTION                                                      */
/* ═══════════════════════════════════════════════════════════════════════════ */

.sog-header {
    position: relative;
    margin: -4rem -4rem 2rem -4rem;
    padding: 4rem 4rem 3rem;
    background: 
        linear-gradient(135deg, rgba(11, 30, 42, 0.95), rgba(8, 19, 26, 0.98)),
        radial-gradient(ellipse 600px 300px at 10% 20%, rgba(255, 155, 98, 0.08), transparent),
        radial-gradient(ellipse 400px 200px at 90% 10%, rgba(125, 240, 208, 0.06), transparent);
    border-bottom: 1px solid var(--sog-border-subtle);
    overflow: hidden;
}

.sog-header::before {
    content: "";
    position: absolute;
    top: -50%;
    left: -10%;
    width: 500px;
    height: 500px;
    background: radial-gradient(circle, rgba(125, 240, 208, 0.08), transparent 60%);
    filter: blur(60px);
    animation: floatGlow 15s ease-in-out infinite;
}

@keyframes floatGlow {
    0%, 100% { transform: translate(0, 0) scale(1); opacity: 0.5; }
    50% { transform: translate(30px, -20px) scale(1.1); opacity: 0.8; }
}

/* Branding Bar */
.sog-branding-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 2rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid var(--sog-border-subtle);
}

.sog-branding-left {
    display: flex;
    align-items: center;
    gap: 1rem;
}

.sog-logo {
    width: 48px;
    height: 48px;
    background: linear-gradient(135deg, var(--sog-accent-cyan), var(--sog-accent-blue));
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-family: var(--font-display);
    font-size: 1.5rem;
    font-weight: 700;
    color: var(--sog-bg-deep);
    box-shadow: 0 4px 16px rgba(125, 240, 208, 0.3);
}

.sog-brand-text {
    display: flex;
    flex-direction: column;
}

.sog-brand-title {
    font-family: var(--font-display);
    font-size: 1.25rem;
    font-weight: 600;
    color: var(--sog-text-primary);
    letter-spacing: -0.02em;
}

.sog-brand-subtitle {
    font-size: 0.75rem;
    color: var(--sog-text-muted);
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

/* Academic Branding — Subtle but Present */
.sog-academic-branding {
    display: flex;
    align-items: center;
    gap: 1.5rem;
    font-size: 0.7rem;
    color: var(--sog-text-subtle);
    text-transform: uppercase;
    letter-spacing: 0.15em;
}

.sog-academic-item {
    position: relative;
    padding: 0.25rem 0;
    opacity: 0.6;
    transition: opacity 0.3s ease;
}

.sog-academic-item:hover {
    opacity: 1;
}

.sog-academic-item::after {
    content: "";
    position: absolute;
    bottom: 0;
    left: 0;
    width: 0;
    height: 1px;
    background: var(--sog-accent-cyan);
    transition: width 0.3s ease;
}

.sog-academic-item:hover::after {
    width: 100%;
}

.sog-copyright {
    font-size: 0.65rem;
    color: var(--sog-text-muted);
    opacity: 0.4;
    margin-top: 0.25rem;
}

/* Hero Content */
.sog-hero-content {
    max-width: 700px;
    position: relative;
    z-index: 1;
}

.sog-kicker {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    font-family: var(--font-mono);
    font-size: 0.7rem;
    font-weight: 500;
    color: var(--sog-accent-cyan);
    text-transform: uppercase;
    letter-spacing: 0.15em;
    margin-bottom: 1rem;
}

.sog-kicker::before {
    content: "◆";
    font-size: 0.5rem;
    animation: pulse 2s ease-in-out infinite;
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}

.sog-title {
    font-family: var(--font-display);
    font-size: clamp(2.5rem, 6vw, 4rem);
    font-weight: 600;
    line-height: 1.1;
    color: var(--sog-text-primary);
    margin-bottom: 1rem;
    letter-spacing: -0.03em;
}

.sog-title-accent {
    background: linear-gradient(135deg, var(--sog-accent-cyan), var(--sog-accent-blue));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.sog-tagline {
    font-size: 1.125rem;
    color: var(--sog-text-secondary);
    line-height: 1.6;
    margin-bottom: 2rem;
    max-width: 600px;
}

/* Feature Pills */
.sog-features {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
    margin-bottom: 2rem;
}

.sog-pill {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    background: var(--sog-accent-cyan-dim);
    border: 1px solid var(--sog-border-medium);
    border-radius: 100px;
    font-family: var(--font-mono);
    font-size: 0.75rem;
    color: var(--sog-accent-cyan);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    transition: all 0.3s ease;
}

.sog-pill:hover {
    background: rgba(125, 240, 208, 0.2);
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(125, 240, 208, 0.15);
}

.sog-pill-icon {
    font-size: 0.875rem;
}

/* Stats Row */
.sog-stats-row {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 1rem;
    margin-top: 2rem;
}

.sog-stat-card {
    padding: 1.25rem;
    background: var(--sog-bg-card);
    border: 1px solid var(--sog-border-subtle);
    border-radius: 16px;
    transition: all 0.3s ease;
}

.sog-stat-card:hover {
    border-color: var(--sog-border-medium);
    transform: translateY(-2px);
    box-shadow: var(--sog-shadow-md);
}

.sog-stat-label {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    font-weight: 500;
    color: var(--sog-text-muted);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.5rem;
}

.sog-stat-value {
    font-family: var(--font-display);
    font-size: 1.125rem;
    font-weight: 600;
    color: var(--sog-text-primary);
}

.sog-stat-desc {
    font-size: 0.8rem;
    color: var(--sog-text-subtle);
    margin-top: 0.25rem;
    line-height: 1.4;
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* CHAT INTERFACE                                                              */
/* ═══════════════════════════════════════════════════════════════════════════ */

/* Chat Messages */
[data-testid="stChatMessage"] {
    background: var(--sog-bg-card) !important;
    border: 1px solid var(--sog-border-subtle) !important;
    border-radius: 20px !important;
    padding: 1.25rem !important;
    margin-bottom: 1rem !important;
    box-shadow: var(--sog-shadow-sm) !important;
    transition: all 0.3s ease !important;
    animation: messageSlide 0.4s ease-out;
}

@keyframes messageSlide {
    from {
        opacity: 0;
        transform: translateY(10px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

[data-testid="stChatMessage"]:hover {
    border-color: var(--sog-border-medium) !important;
    box-shadow: var(--sog-shadow-md) !important;
}

/* User Messages */
[data-testid="stChatMessage"][data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) {
    background: linear-gradient(135deg, rgba(125, 240, 208, 0.05), rgba(11, 30, 42, 0.8)) !important;
    border-left: 3px solid var(--sog-accent-cyan) !important;
}

/* Assistant Messages */
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) {
    background: linear-gradient(135deg, rgba(255, 155, 98, 0.03), rgba(11, 30, 42, 0.8)) !important;
    border-left: 3px solid var(--sog-accent-orange) !important;
}

/* Chat Input */
div[data-testid="stChatInput"] {
    background: var(--sog-bg-secondary) !important;
    border: 1px solid var(--sog-border-subtle) !important;
    border-radius: 24px !important;
    padding: 0.5rem !important;
    box-shadow: var(--sog-shadow-md) !important;
    transition: all 0.3s ease !important;
}

div[data-testid="stChatInput"]:focus-within {
    border-color: var(--sog-border-strong) !important;
    box-shadow: var(--sog-shadow-glow) !important;
}

div[data-testid="stChatInput"] textarea {
    color: var(--sog-text-primary) !important;
    font-family: var(--font-body) !important;
    font-size: 0.95rem !important;
}

div[data-testid="stChatInput"] textarea::placeholder {
    color: var(--sog-text-subtle) !important;
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* COMPONENTS & CARDS                                                          */
/* ═══════════════════════════════════════════════════════════════════════════ */

/* Section Labels */
.sog-section-label {
    font-family: var(--font-mono);
    font-size: 0.7rem;
    font-weight: 600;
    color: var(--sog-accent-orange);
    text-transform: uppercase;
    letter-spacing: 0.15em;
    margin: 2rem 0 1rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.sog-section-label::before {
    content: "";
    width: 24px;
    height: 1px;
    background: var(--sog-accent-orange);
}

/* Artifact Cabinet */
.sog-artifact-cabinet {
    background: var(--sog-bg-card);
    border: 1px solid var(--sog-border-subtle);
    border-radius: 16px;
    padding: 1.5rem;
    margin: 1.5rem 0;
}

.sog-artifact-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
}

.sog-artifact-item {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 1rem;
    background: rgba(255, 255, 255, 0.02);
    border: 1px solid var(--sog-border-subtle);
    border-radius: 12px;
    transition: all 0.3s ease;
}

.sog-artifact-item:hover {
    background: rgba(125, 240, 208, 0.05);
    border-color: var(--sog-border-medium);
    transform: translateY(-1px);
}

.sog-artifact-icon {
    width: 40px;
    height: 40px;
    background: var(--sog-accent-cyan-dim);
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 1.25rem;
}

.sog-artifact-info {
    flex: 1;
}

.sog-artifact-name {
    font-weight: 500;
    color: var(--sog-text-primary);
    font-size: 0.9rem;
}

.sog-artifact-meta {
    font-size: 0.75rem;
    color: var(--sog-text-muted);
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* BUTTONS & CONTROLS                                                          */
/* ═══════════════════════════════════════════════════════════════════════════ */

.stButton > button,
.stDownloadButton > button {
    background: linear-gradient(135deg, var(--sog-accent-cyan), var(--sog-accent-blue)) !important;
    color: var(--sog-bg-deep) !important;
    border: none !important;
    border-radius: 12px !important;
    padding: 0.75rem 1.5rem !important;
    font-family: var(--font-mono) !important;
    font-size: 0.8rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em !important;
    box-shadow: 0 4px 16px rgba(125, 240, 208, 0.25) !important;
    transition: all 0.3s ease !important;
}

.stButton > button:hover,
.stDownloadButton > button:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 8px 24px rgba(125, 240, 208, 0.35) !important;
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* STATUS & PROGRESS                                                           */
/* ═══════════════════════════════════════════════════════════════════════════ */

/* Progress Bar */
.sog-progress-container {
    background: var(--sog-bg-secondary);
    border: 1px solid var(--sog-border-subtle);
    border-radius: 12px;
    padding: 1.5rem;
    margin: 1rem 0;
}

.sog-progress-bar {
    height: 6px;
    background: var(--sog-bg-tertiary);
    border-radius: 3px;
    overflow: hidden;
    margin: 1rem 0;
}

.sog-progress-fill {
    height: 100%;
    background: linear-gradient(90deg, var(--sog-accent-cyan), var(--sog-accent-blue));
    border-radius: 3px;
    transition: width 0.5s ease;
    animation: shimmer 2s ease-in-out infinite;
}

@keyframes shimmer {
    0% { opacity: 1; }
    50% { opacity: 0.7; }
    100% { opacity: 1; }
}

.sog-progress-stage {
    font-family: var(--font-mono);
    font-size: 0.75rem;
    color: var(--sog-accent-cyan);
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* FOOTER                                                                      */
/* ═══════════════════════════════════════════════════════════════════════════ */

.sog-footer {
    margin: 4rem -4rem -4rem;
    padding: 2rem 4rem;
    background: var(--sog-bg-deep);
    border-top: 1px solid var(--sog-border-subtle);
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.75rem;
    color: var(--sog-text-muted);
}

.sog-footer-left {
    display: flex;
    align-items: center;
    gap: 1.5rem;
}

.sog-footer-brand {
    font-family: var(--font-mono);
    font-weight: 500;
}

.sog-footer-links {
    display: flex;
    gap: 1rem;
}

.sog-footer-link {
    color: var(--sog-text-subtle);
    text-decoration: none;
    transition: color 0.3s ease;
}

.sog-footer-link:hover {
    color: var(--sog-accent-cyan);
}

.sog-footer-copyright {
    font-size: 0.7rem;
    opacity: 0.5;
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* RESPONSIVE                                                                  */
/* ═══════════════════════════════════════════════════════════════════════════ */

@media (max-width: 768px) {
    .sog-header {
        margin: -2rem -2rem 1.5rem -2rem;
        padding: 2rem 2rem 2rem;
    }
    
    .sog-branding-bar {
        flex-direction: column;
        align-items: flex-start;
        gap: 1rem;
    }
    
    .sog-academic-branding {
        width: 100%;
        justify-content: flex-start;
    }
    
    .sog-title {
        font-size: 2rem;
    }
    
    .sog-stats-row {
        grid-template-columns: 1fr;
    }
    
    .sog-footer {
        flex-direction: column;
        gap: 1rem;
        text-align: center;
        margin: 2rem -2rem -2rem;
        padding: 1.5rem 2rem;
    }
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* UTILITY ANIMATIONS                                                          */
/* ═══════════════════════════════════════════════════════════════════════════ */

.fade-in {
    animation: fadeIn 0.5s ease-out;
}

@keyframes fadeIn {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
}

.glow-pulse {
    animation: glowPulse 3s ease-in-out infinite;
}

@keyframes glowPulse {
    0%, 100% { box-shadow: 0 0 20px rgba(125, 240, 208, 0.2); }
    50% { box-shadow: 0 0 40px rgba(125, 240, 208, 0.4); }
}
</style>
"""

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title=f"{BRANDING['short_title']} — {BRANDING['full_title']}",
    page_icon="🔷",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ═══════════════════════════════════════════════════════════════════════════════
# UI COMPONENTS
# ═══════════════════════════════════════════════════════════════════════════════

def _inject_theme() -> None:
    """Inject custom CSS theme."""
    st.markdown(APP_CSS, unsafe_allow_html=True)


def _render_header() -> None:
    """Render branded header with academic affiliations."""
    st.markdown(f"""
    <div class="sog-header">
        <div class="sog-branding-bar">
            <div class="sog-branding-left">
                <div class="sog-logo">{BRANDING['short_title']}</div>
                <div class="sog-brand-text">
                    <div class="sog-brand-title">{BRANDING['full_title']}</div>
                    <div class="sog-brand-subtitle">{BRANDING['tagline']}</div>
                </div>
            </div>
            <div class="sog-academic-branding">
                <span class="sog-academic-item">{BRANDING['institution']}</span>
                <span class="sog-academic-item">{BRANDING['lab']}</span>
                <div class="sog-copyright">© {BRANDING['year']} {BRANDING['author']}</div>
            </div>
        </div>
        
        <div class="sog-hero-content">
            <div class="sog-kicker">Entity Resolution Benchmarking</div>
            <h1 class="sog-title">
                Design a benchmark that<br>
                <span class="sog-title-accent">feels intentional.</span>
            </h1>
            <p class="sog-tagline">
                Describe your scenario, overlap, duplication, timing, and noise profile.
                The assistant translates your intent into synthetic data and runs the full
                pipeline — without dragging you through YAML configuration.
            </p>
            
            <div class="sog-features">
                <span class="sog-pill">
                    <span class="sog-pill-icon">🏠</span>
                    Truth Simulation
                </span>
                <span class="sog-pill">
                    <span class="sog-pill-icon">📊</span>
                    Observed Emission
                </span>
                <span class="sog-pill">
                    <span class="sog-pill-icon">📥</span>
                    Download-Ready
                </span>
            </div>
            
            <div class="sog-stats-row">
                <div class="sog-stat-card">
                    <div class="sog-stat-label">Phase 1 Baseline</div>
                    <div class="sog-stat-value">Canonical Population</div>
                    <div class="sog-stat-desc">One foundation, many scenarios</div>
                </div>
                <div class="sog-stat-card">
                    <div class="sog-stat-label">Life Events</div>
                    <div class="sog-stat-value">Moves, Merges, Births</div>
                    <div class="sog-stat-desc">Tune overlap & noise</div>
                </div>
                <div class="sog-stat-card">
                    <div class="sog-stat-label">Quick Prompt</div>
                    <div class="sog-stat-value">Natural Language</div>
                    <div class="sog-stat-desc">The assistant infers the rest</div>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_footer() -> None:
    """Render footer with branding."""
    st.markdown(f"""
    <div class="sog-footer">
        <div class="sog-footer-left">
            <span class="sog-footer-brand">{BRANDING['short_title']}</span>
            <div class="sog-footer-links">
                <a href="#" class="sog-footer-link">Documentation</a>
                <a href="#" class="sog-footer-link">GitHub</a>
                <a href="#" class="sog-footer-link">Report Issue</a>
            </div>
        </div>
        <div class="sog-footer-copyright">
            {BRANDING['full_title']} © {BRANDING['year']} {BRANDING['author']} · 
            {BRANDING['institution']} · {BRANDING['lab']}
        </div>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION MANAGEMENT (from session_manager.py)
# ═══════════════════════════════════════════════════════════════════════════════

from session_manager import SessionData, load_session, save_session  # noqa: E402


def _resolve_session_id() -> str:
    """Keep a stable session ID in the URL."""
    sid = st.query_params.get("sid", "")
    if isinstance(sid, list):
        sid = sid[0] if sid else ""
    sid = str(sid).strip()
    if sid:
        return sid
    sid = uuid.uuid4().hex[:8]
    st.query_params["sid"] = sid
    return sid


def _initialize_session() -> str:
    """Initialize or restore session state."""
    if "session_id" not in st.session_state:
        st.session_state.session_id = _resolve_session_id()
    else:
        query_sid = st.query_params.get("sid", "")
        if isinstance(query_sid, list):
            query_sid = query_sid[0] if query_sid else ""
        if str(query_sid).strip() != st.session_state.session_id:
            st.query_params["sid"] = st.session_state.session_id

    session_id = st.session_state.session_id

    if "session_loaded" not in st.session_state:
        _saved = load_session(session_id)
        st.session_state.messages = _saved.messages
        st.session_state.context = _saved.context
        st.session_state.last_run_downloads = _saved.last_run_downloads
        st.session_state.pending_job_id = _saved.pending_job_id
        st.session_state.pending_charts = _saved.pending_charts
        st.session_state.session_loaded = True
    else:
        if "messages" not in st.session_state:
            st.session_state.messages = []
        if "context" not in st.session_state:
            st.session_state.context = {"last_run_id": None, "last_scenario_id": None, "run_history": []}

    if "last_run_downloads" not in st.session_state:
        st.session_state.last_run_downloads = {}
    if "pending_job_id" not in st.session_state:
        st.session_state.pending_job_id = None
    if "pending_charts" not in st.session_state:
        st.session_state.pending_charts = []

    return session_id


def _persist_session(session_id: str) -> None:
    """Save current session state to disk."""
    save_session(SessionData(
        session_id=session_id,
        context=st.session_state.context,
        messages=st.session_state.messages,
        last_run_downloads=st.session_state.last_run_downloads,
        pending_job_id=st.session_state.pending_job_id,
        pending_charts=st.session_state.pending_charts,
    ))


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═══════════════════════════════════════════════════════════════════════════════

_inject_theme()
_render_header()

SESSION_ID = _initialize_session()

# ═══════════════════════════════════════════════════════════════════════════════
# JOB POLLING
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def _get_orchestrator(api_key: str):
    from agents.orchestrator import Orchestrator
    return Orchestrator(api_key=api_key)


def _poll_pending_job() -> None:
    """Check background job status."""
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
            history.insert(0, {"run_id": run_id, "scenario_id": status.get("scenario_id", "")})
            st.session_state.context["run_history"] = history[:20]

            try:
                api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                orch = _get_orchestrator(api_key)
                analyst_result = orch._analyst().run(
                    f"Summarize run {run_id}", SESSION_ID, st.session_state.context
                )
                completion_msg = f"Run complete: **{run_id}**\n\n{analyst_result.message}"
                if analyst_result.charts:
                    st.session_state.pending_charts = analyst_result.charts
            except Exception as exc:
                completion_msg = f"Run complete: **{run_id}**\n\nCould not auto-summarize: {exc}"
        else:
            completion_msg = "Run completed (no run_id returned)."

        st.session_state.messages.append({"role": "assistant", "content": completion_msg})
        st.session_state.pending_job_id = None
        _persist_session(SESSION_ID)
        st.rerun()

    elif status["status"] == "failed":
        error = status.get("error", "unknown error")
        st.session_state.messages.append({"role": "assistant", "content": f"Run failed: {error}"})
        st.session_state.pending_job_id = None
        _persist_session(SESSION_ID)
        st.rerun()

    else:
        stage = status.get("current_stage", "running")
        pct = status.get("progress_percent", 0)
        st.info(f"Pipeline running: **{stage}** ({pct}%)")
        st.markdown('<meta http-equiv="refresh" content="3">', unsafe_allow_html=True)
        st.stop()


_poll_pending_job()

# ═══════════════════════════════════════════════════════════════════════════════
# CHAT HISTORY
# ═══════════════════════════════════════════════════════════════════════════════

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Render pending charts
if st.session_state.pending_charts:
    for chart in st.session_state.pending_charts:
        chart_path = chart.get("chart_path", "")
        insight = chart.get("insight", "")
        if chart_path and Path(chart_path).exists():
            ext = Path(chart_path).suffix.lower()
            if ext == ".html":
                st.components.v1.html(Path(chart_path).read_text(encoding="utf-8"), height=500, scrolling=True)
            else:
                st.image(chart_path)
            if insight:
                st.caption(insight)
    st.session_state.pending_charts = []
    _persist_session(SESSION_ID)

# ═══════════════════════════════════════════════════════════════════════════════
# ARTIFACT CABINET
# ═══════════════════════════════════════════════════════════════════════════════

if st.session_state.last_run_downloads:
    st.markdown('<div class="sog-section-label">Artifact Cabinet</div>', unsafe_allow_html=True)
    cols = st.columns(len(st.session_state.last_run_downloads))
    for col, (label, path_str) in zip(cols, st.session_state.last_run_downloads.items()):
        download_path = Path(path_str)
        if not download_path.exists():
            continue
        with open(download_path, "rb") as handle:
            col.download_button(
                label=f"Download {label}",
                data=handle.read(),
                file_name=download_path.name,
                key=f"dl_{label}",
            )

# ═══════════════════════════════════════════════════════════════════════════════
# API KEY GATE
# ═══════════════════════════════════════════════════════════════════════════════

if not os.environ.get("ANTHROPIC_API_KEY"):
    st.markdown("""
    <div style="text-align: center; padding: 3rem 1rem;">
        <div style="font-size: 3rem; margin-bottom: 1rem;">🔐</div>
        <h3 style="color: var(--sog-text-primary); margin-bottom: 0.5rem;">API Key Required</h3>
        <p style="color: var(--sog-text-muted); max-width: 400px; margin: 0 auto 1.5rem;">
            Enter your Anthropic API key to start generating synthetic data.
            Your key is only stored in this browser session.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    key_input = st.text_input("Anthropic API key", type="password", placeholder="sk-ant-api03-...")
    if key_input.strip():
        os.environ["ANTHROPIC_API_KEY"] = key_input.strip()
        st.rerun()
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
# CHAT INPUT
# ═══════════════════════════════════════════════════════════════════════════════

if prompt := st.chat_input("Describe what you want to generate..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    _persist_session(SESSION_ID)
    
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Working..."):
            try:
                api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                orch = _get_orchestrator(api_key)
                result = orch.run_turn(
                    user_input=prompt,
                    session_id=SESSION_ID,
                    context=st.session_state.context,
                )
                text = result.get("message", "")
                
                if result.get("session_updates"):
                    st.session_state.context.update(result["session_updates"])
                if result.get("pending_job_id"):
                    st.session_state.pending_job_id = result["pending_job_id"]
                if result.get("charts"):
                    st.session_state.pending_charts = result["charts"]
                if result.get("download_paths"):
                    st.session_state.last_run_downloads = result["download_paths"]
                
                _persist_session(SESSION_ID)
                
            except Exception as exc:
                text = f"Error: {exc}"

        st.markdown(text)

    st.session_state.messages.append({"role": "assistant", "content": text})
    _persist_session(SESSION_ID)
    
    if st.session_state.last_run_downloads or st.session_state.pending_job_id:
        st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════════════

_render_footer()
