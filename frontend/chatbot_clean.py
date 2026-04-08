"""SOG Assistant - Clean, working version with beautiful UI."""

from __future__ import annotations

import os
import sys
import uuid
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

# Page config FIRST
st.set_page_config(
    page_title="SOG - Synthetic Occupancy Generator",
    page_icon="🔷",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# Simple, clean CSS that works
st.markdown("""
<style>
/* Base */
.stApp {
    background: linear-gradient(180deg, #0a1628 0%, #0d1f2d 50%, #08131a 100%);
}

/* Header */
.sog-header {
    background: linear-gradient(135deg, rgba(15, 40, 55, 0.95), rgba(8, 19, 26, 0.98));
    border: 1px solid rgba(125, 240, 208, 0.2);
    border-radius: 20px;
    padding: 2rem;
    margin-bottom: 2rem;
    box-shadow: 0 20px 60px rgba(0, 0, 0, 0.4);
}

.sog-title {
    font-size: 2.5rem;
    font-weight: 700;
    color: #f5ebdb;
    margin: 0 0 0.5rem 0;
    letter-spacing: -0.02em;
}

.sog-title span {
    color: #7df0d0;
}

.sog-subtitle {
    color: #9eb6c2;
    font-size: 1.1rem;
    margin: 0 0 1.5rem 0;
}

.sog-branding {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid rgba(125, 240, 208, 0.1);
}

.sog-logo-section {
    display: flex;
    align-items: center;
    gap: 1rem;
}

.sog-logo {
    width: 48px;
    height: 48px;
    background: linear-gradient(135deg, #7df0d0, #74e5ff);
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 1.5rem;
    font-weight: bold;
    color: #08131a;
}

.sog-academic {
    text-align: right;
    color: rgba(158, 182, 194, 0.6);
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

.sog-academic:hover {
    color: rgba(158, 182, 194, 0.9);
}

/* Features */
.sog-features {
    display: flex;
    gap: 0.75rem;
    margin-top: 1.5rem;
}

.sog-pill {
    background: rgba(125, 240, 208, 0.1);
    border: 1px solid rgba(125, 240, 208, 0.2);
    border-radius: 100px;
    padding: 0.5rem 1rem;
    color: #7df0d0;
    font-size: 0.85rem;
}

/* Chat messages */
[data-testid="stChatMessage"] {
    background: rgba(11, 30, 42, 0.8) !important;
    border: 1px solid rgba(125, 240, 208, 0.1) !important;
    border-radius: 16px !important;
    margin-bottom: 1rem !important;
    padding: 1rem !important;
}

/* Input */
[data-testid="stChatInput"] {
    background: rgba(11, 30, 42, 0.9) !important;
    border: 1px solid rgba(125, 240, 208, 0.2) !important;
    border-radius: 20px !important;
}

/* Footer */
.sog-footer {
    margin-top: 3rem;
    padding-top: 1.5rem;
    border-top: 1px solid rgba(125, 240, 208, 0.1);
    text-align: center;
    color: rgba(158, 182, 194, 0.5);
    font-size: 0.8rem;
}

.sog-footer a {
    color: rgba(158, 182, 194, 0.6);
    text-decoration: none;
    margin: 0 0.5rem;
}

.sog-footer a:hover {
    color: #7df0d0;
}

/* Progress */
.sog-progress {
    background: rgba(11, 30, 42, 0.8);
    border: 1px solid rgba(125, 240, 208, 0.2);
    border-radius: 12px;
    padding: 1rem;
    margin: 1rem 0;
}
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="sog-header">
    <div class="sog-branding">
        <div class="sog-logo-section">
            <div class="sog-logo">S</div>
            <div>
                <div style="font-size: 1.25rem; font-weight: 600; color: #f5ebdb;">SOG</div>
                <div style="font-size: 0.75rem; color: #9eb6c2;">Synthetic Occupancy Generator</div>
            </div>
        </div>
        <div class="sog-academic">
            UALR · ERIQ<br>
            <span style="font-size: 0.65rem; opacity: 0.7;">© 2024 Taha Mohammed</span>
        </div>
    </div>
    <h1 class="sog-title">Design a benchmark that<br><span>feels intentional.</span></h1>
    <p class="sog-subtitle">Describe your scenario. The assistant builds the synthetic data.</p>
    <div class="sog-features">
        <span class="sog-pill">🏠 Truth Simulation</span>
        <span class="sog-pill">📊 Observed Emission</span>
        <span class="sog-pill">📥 Download-Ready</span>
    </div>
</div>
""", unsafe_allow_html=True)

# Session setup
from session_manager import SessionData, load_session, save_session

def resolve_session_id():
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
    st.session_state.session_id = resolve_session_id()
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
    for key, default in [("messages", []), ("context", {"last_run_id": None, "last_scenario_id": None, "run_history": []}), 
                         ("last_run_downloads", {}), ("pending_job_id", None), ("pending_charts", [])]:
        if key not in st.session_state:
            st.session_state[key] = default

def persist():
    save_session(SessionData(
        session_id=SESSION_ID,
        context=st.session_state.context,
        messages=st.session_state.messages,
        last_run_downloads=st.session_state.last_run_downloads,
        pending_job_id=st.session_state.pending_job_id,
        pending_charts=st.session_state.pending_charts,
    ))

# Job polling
@st.cache_resource
def get_orchestrator(api_key):
    from agents.orchestrator import Orchestrator
    return Orchestrator(api_key=api_key)

def poll_job():
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
                orch = get_orchestrator(api_key)
                result = orch._analyst().run(f"Summarize run {run_id}", SESSION_ID, st.session_state.context)
                msg = f"Run complete: **{run_id}**\n\n{result.message}"
                if result.charts:
                    st.session_state.pending_charts = result.charts
            except Exception as e:
                msg = f"Run complete: **{run_id}**\n\nCould not summarize: {e}"
        else:
            msg = "Run completed."
        
        st.session_state.messages.append({"role": "assistant", "content": msg})
        st.session_state.pending_job_id = None
        persist()
        st.rerun()
    
    elif status["status"] == "failed":
        st.session_state.messages.append({"role": "assistant", "content": f"Run failed: {status.get('error', 'unknown error')}"})
        st.session_state.pending_job_id = None
        persist()
        st.rerun()
    
    else:
        st.markdown(f"""
        <div class="sog-progress">
            <strong>Pipeline running:</strong> {status.get('current_stage', 'running')} ({status.get('progress_percent', 0)}%)
        </div>
        """, unsafe_allow_html=True)
        st.markdown('<meta http-equiv="refresh" content="3">', unsafe_allow_html=True)
        st.stop()

poll_job()

# Chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Charts
if st.session_state.pending_charts:
    for chart in st.session_state.pending_charts:
        path = chart.get("chart_path", "")
        if path and Path(path).exists():
            if Path(path).suffix == ".html":
                st.components.v1.html(Path(path).read_text(encoding="utf-8"), height=500)
            else:
                st.image(path)
            if chart.get("insight"):
                st.caption(chart["insight"])
    st.session_state.pending_charts = []
    persist()

# Downloads
if st.session_state.last_run_downloads:
    st.markdown("**📦 Downloads**")
    cols = st.columns(len(st.session_state.last_run_downloads))
    for col, (label, path_str) in zip(cols, st.session_state.last_run_downloads.items()):
        p = Path(path_str)
        if p.exists():
            with open(p, "rb") as f:
                col.download_button(f"Download {label}", f.read(), p.name, key=f"dl_{label}")

# API Key
if not os.environ.get("ANTHROPIC_API_KEY"):
    st.info("🔐 Enter your Anthropic API key to continue")
    key = st.text_input("API Key", type="password", placeholder="sk-ant-api03-...")
    if key.strip():
        os.environ["ANTHROPIC_API_KEY"] = key.strip()
        st.rerun()
    st.stop()

# Chat input
if prompt := st.chat_input("Describe what you want to generate..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    persist()
    
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("Working..."):
            try:
                api_key = os.environ.get("ANTHROPIC_API_KEY", "")
                orch = get_orchestrator(api_key)
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
                persist()
            except Exception as e:
                text = f"Error: {e}"
        
        st.markdown(text)
    
    st.session_state.messages.append({"role": "assistant", "content": text})
    persist()
    if st.session_state.pending_job_id or st.session_state.last_run_downloads:
        st.rerun()

# Footer
st.markdown("""
<div class="sog-footer">
    <a href="#">Documentation</a> ·
    <a href="#">GitHub</a> ·
    <a href="#">Report Issue</a><br><br>
    Synthetic Occupancy Generator · UALR · ERIQ<br>
    © 2024 Taha Mohammed
</div>
""", unsafe_allow_html=True)
