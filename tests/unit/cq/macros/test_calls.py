"""Tests for cq calls macro."""

from __future__ import annotations

import textwrap
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls import cmd_calls
from tools.cq.macros.calls.context_snippet import _extract_context_snippet
from tools.cq.macros.calls.insight import _find_function_signature
from tools.cq.macros.calls.neighborhood import (
    CallAnalysisSummary,
    CallsNeighborhoodRequest,
    _build_calls_neighborhood,
)
from tools.cq.macros.calls.semantic import _calls_payload_reason
from tools.cq.macros.contracts import CallsRequest
from tools.cq.neighborhood.contracts import TreeSitterNeighborhoodCollectResult

MAX_TARGET_CALLEE_FINDINGS = 10


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_cmd_calls_uses_ast_grep_when_available(tmp_path: Path) -> None:
    """Test cmd calls uses ast grep when available."""
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

    result = cmd_calls(
        CallsRequest(tc=tc, root=repo, argv=["cq", "calls", "foo"], function_name="foo")
    )

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

    result = cmd_calls(
        CallsRequest(tc=tc, root=repo, argv=["cq", "calls", "foo"], function_name="foo")
    )
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
    assert not sig


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

    result = cmd_calls(
        CallsRequest(tc=tc, root=repo, argv=["cq", "calls", "foo"], function_name="foo")
    )
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

    result = cmd_calls(
        CallsRequest(tc=tc, root=repo, argv=["cq", "calls", "foo"], function_name="foo")
    )
    assert result.summary["mode"] == "macro:calls"
    assert result.summary["query"] == "foo"


def test_cmd_calls_includes_target_callees_and_insight_counters(tmp_path: Path) -> None:
    """Calls output should include bounded target-callee preview and risk counters."""
    tc = Toolchain.detect()
    if not tc.has_sgpy:
        pytest.skip("ast-grep-py not available")

    repo = tmp_path / "repo"
    _write_file(
        repo / "mod.py",
        textwrap.dedent(
            """\
            def helper(x):
                return str(x)

            def foo(value, **kwargs):
                helper(value)
                return max(value, 1)

            def bar():
                return foo(1, debug=True)
            """
        ),
    )

    result = cmd_calls(
        CallsRequest(tc=tc, root=repo, argv=["cq", "calls", "foo"], function_name="foo")
    )
    target_callees = [section for section in result.sections if section.title == "Target Callees"]
    assert target_callees, "Target Callees section missing"
    assert len(target_callees[0].findings) <= MAX_TARGET_CALLEE_FINDINGS

    insight = cast("Mapping[str, object]", result.summary.get("front_door_insight", {}))
    assert insight
    risk = cast("Mapping[str, object]", insight.get("risk", {}))
    counters = cast("Mapping[str, object]", risk.get("counters", {}))
    files_with_calls = counters.get("files_with_calls", 0)
    forwarding_count = counters.get("forwarding_count", 0)
    assert isinstance(files_with_calls, int)
    assert isinstance(forwarding_count, int)
    assert files_with_calls >= 1
    assert forwarding_count >= 0


def test_cmd_calls_rust_target_metadata_and_callees(tmp_path: Path) -> None:
    """Rust calls flow should resolve target metadata and emit target-callee preview."""
    tc = Toolchain.detect()
    if not tc.has_sgpy:
        pytest.skip("ast-grep-py not available")

    repo = tmp_path / "repo"
    _write_file(
        repo / "src/lib.rs",
        textwrap.dedent(
            """\
            fn helper(value: usize) -> usize {
                value + 1
            }

            pub(crate) fn compile_target(input: usize) -> usize {
                helper(input)
            }

            pub fn driver() -> usize {
                compile_target(1)
            }
            """
        ),
    )

    result = cmd_calls(
        CallsRequest(
            tc=tc, root=repo, argv=["cq", "calls", "compile_target"], function_name="compile_target"
        )
    )
    target_file = result.summary.get("target_file")
    target_line = result.summary.get("target_line")
    assert isinstance(target_file, str)
    assert target_file.endswith(".rs")
    assert isinstance(target_line, int)
    target_callees = [section for section in result.sections if section.title == "Target Callees"]
    assert target_callees
    assert any("helper" in finding.message for finding in target_callees[0].findings)


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


def test_calls_payload_reason_normalizes_python_semantic_timeout() -> None:
    """Test calls payload reason normalizes python semantic timeout."""
    payload: dict[str, object] = {
        "coverage": {"status": "not_resolved", "reason": "timeout"},
    }
    assert _calls_payload_reason("python", payload) == "request_timeout"


def test_calls_payload_reason_uses_fallback_for_rust() -> None:
    """Test calls payload reason uses fallback for rust."""
    payload: dict[str, object] = {}
    assert (
        _calls_payload_reason("rust", payload, fallback_reason="request_failed") == "request_failed"
    )


def test_build_calls_neighborhood_uses_rust_language_for_rs_targets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Calls neighborhood collector should use rust parser for .rs targets."""
    captured_language: str | None = None

    def _fake_collect(request: object) -> TreeSitterNeighborhoodCollectResult:
        nonlocal captured_language
        captured_language = getattr(request, "language", None)
        return TreeSitterNeighborhoodCollectResult()

    monkeypatch.setattr(
        "tools.cq.neighborhood.collector.collect_tree_sitter_neighborhood",
        _fake_collect,
    )

    _build_calls_neighborhood(
        CallsNeighborhoodRequest(
            root=tmp_path,
            function_name="compile_target",
            target_location=("src/lib.rs", 6),
            target_callees=Counter(),
            analysis=CallAnalysisSummary(
                arg_shapes=Counter(),
                kwarg_usage=Counter(),
                forwarding_count=0,
                contexts=Counter(),
                hazard_counts=Counter(),
            ),
            score=None,
            target_language=None,
            preview_per_slice=3,
        )
    )
    assert captured_language == "rust"


def test_build_calls_neighborhood_defaults_to_python_for_py_targets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Calls neighborhood collector should default to python for .py targets."""
    captured_language: str | None = None

    def _fake_collect(request: object) -> TreeSitterNeighborhoodCollectResult:
        nonlocal captured_language
        captured_language = getattr(request, "language", None)
        return TreeSitterNeighborhoodCollectResult()

    monkeypatch.setattr(
        "tools.cq.neighborhood.collector.collect_tree_sitter_neighborhood",
        _fake_collect,
    )

    _build_calls_neighborhood(
        CallsNeighborhoodRequest(
            root=tmp_path,
            function_name="foo",
            target_location=("src/mod.py", 3),
            target_callees=Counter(),
            analysis=CallAnalysisSummary(
                arg_shapes=Counter(),
                kwarg_usage=Counter(),
                forwarding_count=0,
                contexts=Counter(),
                hazard_counts=Counter(),
            ),
            score=None,
            target_language=None,
            preview_per_slice=3,
        )
    )
    assert captured_language == "python"
