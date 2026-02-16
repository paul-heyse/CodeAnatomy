"""Integration test for cq calls macro."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls import cmd_calls


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_calls_integration_basic(tmp_path: Path) -> None:
    """Test calls integration basic."""
    tc = Toolchain.detect()
    if not tc.has_sgpy:
        pytest.skip("ast-grep-py not available")

    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent("""\
            def foo(x):
                return x

            def bar():
                return foo(1)
            """),
    )

    result = cmd_calls(tc, repo, ["cq", "calls", "foo"], "foo")
    assert result.summary["total_sites"] == 1
    assert result.summary["scan_method"] == "ast-grep"
