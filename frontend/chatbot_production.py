"""Production entrypoint for the live SOG Streamlit frontend.

This file intentionally executes ``frontend/chatbot.py`` on every Streamlit
rerun so there is only one real application architecture to keep aligned with
the backend pipeline.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = PROJECT_ROOT / "frontend"
SRC_DIR = PROJECT_ROOT / "src"

for path in (FRONTEND_DIR, SRC_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


# Execute the live app script directly so Streamlit reruns do not get stuck on
# Python's module cache after actions like API-key entry.
runpy.run_path(str(FRONTEND_DIR / "chatbot.py"), run_name="__main__")
