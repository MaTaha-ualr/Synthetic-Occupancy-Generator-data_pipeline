from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = PROJECT_ROOT / "frontend" / "chatbot_production.py"


def test_chatbot_production_executes_live_chatbot_script_on_each_rerun() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)

    run_path_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "run_path"
    ]

    assert run_path_calls, "Expected chatbot_production.py to execute frontend/chatbot.py directly"
    assert any("chatbot.py" in ast.unparse(call) for call in run_path_calls)
