"""Static guard for the dashboard source (M8).

Streamlit "magic" displays the VALUE of any bare expression statement. A line like
    st.dataframe(df) if not df.empty else st.write("empty")
is such a statement: it printed the returned DeltaGenerator object and Streamlit's whole documentation into the page
(found by looking at a real screenshot). Use an if/else statement instead.
"""
import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit
APP = Path(__file__).resolve().parents[2] / "src" / "dashboard" / "app.py"


def bare_expression_problems(source: str) -> list[str]:
    problems = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Expr):
            value = node.value
            if isinstance(value, (ast.IfExp, ast.BoolOp, ast.Compare, ast.Name, ast.BinOp)):
                problems.append(f"line {node.lineno}: bare {type(value).__name__} statement would be displayed by Streamlit magic")
    return problems


def test_dashboard_has_no_bare_expression_statements():
    assert bare_expression_problems(APP.read_text(encoding="utf-8-sig")) == []


def test_the_guard_really_catches_the_original_mistake():
    bad = 'import streamlit as st\nst.dataframe(df) if not df.empty else st.write("empty")\n'
    assert len(bare_expression_problems(bad)) == 1
