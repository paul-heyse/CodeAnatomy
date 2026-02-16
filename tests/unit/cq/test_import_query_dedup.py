"""Regression tests for import entity query output stability."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor import ExecutePlanRequestV1, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_import_query_does_not_duplicate_from_import_multi(tmp_path: Path) -> None:
    """Test import query does not duplicate from import multi."""
    tc = Toolchain.detect()
    if not tc.has_sgpy:
        pytest.skip("ast-grep-py not available")

    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent("""\
            from typing import Any, Protocol
            """),
    )

    query = parse_query("entity=import name=typing")
    plan = compile_query(query)
    result = execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=query,
            root=str(repo),
            argv=("cq", "q", "entity=import"),
        ),
        tc=tc,
    )

    languages = result.summary.get("languages")
    assert isinstance(languages, dict)
    python_summary = languages.get("python")
    assert isinstance(python_summary, dict)
    assert python_summary.get("matches") == 1
    assert len(result.key_findings) == 1
    finding = result.key_findings[0]
    assert finding.message == "from_import: typing"
    assert finding.anchor is not None
    assert finding.anchor.file == "mod.py"
    assert finding.anchor.line == 1


def test_import_query_ignores_commas_in_inline_comments(tmp_path: Path) -> None:
    """Test import query ignores commas in inline comments."""
    tc = Toolchain.detect()
    if not tc.has_sgpy:
        pytest.skip("ast-grep-py not available")

    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent("""\
            from typing import Any  # comment, with comma
            """),
    )

    query = parse_query("entity=import name=Any")
    plan = compile_query(query)
    result = execute_plan(
        ExecutePlanRequestV1(
            plan=plan,
            query=query,
            root=str(repo),
            argv=("cq", "q", "entity=import"),
        ),
        tc=tc,
    )

    languages = result.summary.get("languages")
    assert isinstance(languages, dict)
    python_summary = languages.get("python")
    assert isinstance(python_summary, dict)
    assert python_summary.get("matches") == 1
    assert len(result.key_findings) == 1
    finding = result.key_findings[0]
    assert finding.message == "from_import: Any"
    assert finding.anchor is not None
    assert finding.anchor.file == "mod.py"
    assert finding.anchor.line == 1
