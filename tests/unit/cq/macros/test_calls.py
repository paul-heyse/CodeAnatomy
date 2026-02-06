"""Tests for cq calls macro."""

from __future__ import annotations

import textwrap
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls import _extract_context_snippet, _find_function_signature, cmd_calls


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_cmd_calls_uses_ast_grep_when_available(tmp_path: Path) -> None:
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

    assert result.summary["scan_method"] == "ast-grep"
    assert result.summary["total_sites"] == 1


def test_cmd_calls_finds_call_sites(tmp_path: Path) -> None:
    """Test that cmd_calls finds call sites and includes basic details."""
    tc = Toolchain.detect()
    if not tc.has_sgpy:
        pytest.skip("ast-grep-py not available")

    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent("""\
            def foo(a, *, b):
                return a + b

            def bar():
                return foo(1)
            """),
    )

    result = cmd_calls(tc, repo, ["cq", "calls", "foo"], "foo")
    assert result.summary["scan_method"] == "ast-grep"
    assert not result.evidence
    sites = [
        finding
        for section in result.sections
        for finding in section.findings
        if finding.category == "call"
    ]
    assert sites
    assert len(sites) == result.summary["total_sites"]
    details = cast("Mapping[str, object]", sites[0].details or {})
    # Basic call site fields should be present
    assert "context" in details
    assert "call_id" in details
    assert details.get("num_args") == 1


def test_find_function_signature(tmp_path: Path) -> None:
    """Test on-demand function signature lookup."""
    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent("""\
            def foo(x, y, z):
                return x + y + z
            """),
    )

    sig = _find_function_signature(repo, "foo")
    assert sig == "(x, y, z)"


def test_find_function_signature_qualified_name(tmp_path: Path) -> None:
    """Test signature lookup with qualified name."""
    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent("""\
            class MyClass:
                def method(self, a, b):
                    return a + b
            """),
    )

    sig = _find_function_signature(repo, "MyClass.method")
    assert sig == "(self, a, b)"


def test_find_function_signature_not_found(tmp_path: Path) -> None:
    """Test signature lookup when function not found."""
    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent("""\
            def other():
                pass
            """),
    )

    sig = _find_function_signature(repo, "nonexistent")
    assert sig == ""


def test_cmd_calls_signature_in_summary(tmp_path: Path) -> None:
    """Test that signature is populated in summary via on-demand lookup."""
    tc = Toolchain.detect()
    if not tc.has_sgpy:
        pytest.skip("ast-grep-py not available")

    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent("""\
            def foo(x, y):
                return x + y

            def bar():
                return foo(1, 2)
            """),
    )

    result = cmd_calls(tc, repo, ["cq", "calls", "foo"], "foo")
    assert result.summary["signature"] == "(x, y)"


def test_cmd_calls_sets_summary_query_and_mode(tmp_path: Path) -> None:
    """Calls macro should expose top-level summary query/mode metadata."""
    tc = Toolchain.detect()
    if not tc.has_sgpy:
        pytest.skip("ast-grep-py not available")

    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent("""\
            def foo():
                return 1

            def bar():
                return foo()
            """),
    )

    result = cmd_calls(tc, repo, ["cq", "calls", "foo"], "foo")
    assert result.summary["mode"] == "macro:calls"
    assert result.summary["query"] == "foo"


def test_extract_context_snippet_prioritizes_anchor_block() -> None:
    """Context snippet should include function top and matched anchor block."""
    source = textwrap.dedent(
        """\
        def outer():
            \"\"\"docstring should be omitted\"\"\"
            head = 1
            filler_a = 1
            filler_b = 2
            filler_c = 3
            filler_d = 4
            if condition:
                a = 1
                b = 2
                target_marker = b
            tail = head
            end = tail
        """
    ).splitlines()
    snippet = _extract_context_snippet(
        source,
        1,
        len(source),
        match_line=10,
        max_lines=6,
    )
    assert isinstance(snippet, str)
    assert "def outer():" in snippet
    assert "if condition:" in snippet
    assert "target_marker = b" in snippet
    assert "docstring should be omitted" not in snippet
