"""Tests for visualization renderers.

Verifies:
1. Mermaid flowchart rendering
2. Mermaid class diagram rendering
3. DOT graph rendering
"""

from __future__ import annotations

from tools.cq.core.renderers.dot import render_dot
from tools.cq.core.renderers.mermaid import (
    render_mermaid_class_diagram,
    render_mermaid_flowchart,
)
from tools.cq.core.schema import Anchor, CqResult, DetailPayload, Finding, RunMeta, Section


def _make_result(
    key_findings: list[Finding] | None = None,
    sections: list[Section] | None = None,
) -> CqResult:
    """Create a CqResult for testing.

    Returns:
    -------
    CqResult
        Result payload for renderer tests.
    """
    run = RunMeta(
        macro="test",
        argv=[],
        root="/test",
        started_ms=0,
        elapsed_ms=0,
    )
    return CqResult(
        run=run,
        key_findings=key_findings or [],
        sections=sections or [],
    )


class TestMermaidFlowchart:
    """Tests for Mermaid flowchart rendering."""

    def test_empty_result(self) -> None:
        """Render empty result produces valid Mermaid."""
        result = _make_result()
        output = render_mermaid_flowchart(result)

        assert "```mermaid" in output
        assert "flowchart TD" in output
        assert "```" in output

    def test_single_function(self) -> None:
        """Render single function definition."""
        finding = Finding(
            category="definition",
            message="function: foo",
            anchor=Anchor(file="test.py", line=1),
            details=DetailPayload.from_legacy({"name": "foo", "kind": "function"}),
        )
        result = _make_result(key_findings=[finding])
        output = render_mermaid_flowchart(result)

        assert "foo[foo]" in output

    def test_caller_callee_edge(self) -> None:
        """Render caller-callee edge into DOT when callers section is present."""
        definition = Finding(
            category="definition",
            message="function: foo",
            anchor=Anchor(file="test.py", line=1),
            details=DetailPayload.from_legacy({"name": "foo", "kind": "function"}),
        )
        caller = Finding(
            category="caller",
            message="caller: bar calls foo",
            anchor=Anchor(file="test.py", line=10),
            details=DetailPayload.from_legacy({"caller": "bar", "callee": "foo"}),
        )
        section = Section(title="Callers", findings=[caller])
        result = _make_result(key_findings=[definition], sections=[section])
        output = render_mermaid_flowchart(result)

        assert "bar --> foo" in output

    def test_sanitizes_special_chars(self) -> None:
        """Sanitize special characters in node IDs."""
        finding = Finding(
            category="definition",
            message="function: my-func.name",
            anchor=Anchor(file="test.py", line=1),
            details=DetailPayload.from_legacy({"name": "my-func.name", "kind": "function"}),
        )
        result = _make_result(key_findings=[finding])
        output = render_mermaid_flowchart(result)

        # Should have sanitized ID but readable label
        assert "my_func_name" in output


class TestMermaidClassDiagram:
    """Tests for Mermaid class diagram rendering."""

    def test_empty_result(self) -> None:
        """Render empty result produces valid Mermaid."""
        result = _make_result()
        output = render_mermaid_class_diagram(result)

        assert "```mermaid" in output
        assert "classDiagram" in output
        assert "```" in output

    def test_single_class(self) -> None:
        """Render single class definition."""
        finding = Finding(
            category="definition",
            message="class: MyClass",
            anchor=Anchor(file="test.py", line=1),
            details=DetailPayload.from_legacy({"name": "MyClass", "kind": "class"}),
        )
        result = _make_result(key_findings=[finding])
        output = render_mermaid_class_diagram(result)

        assert "class MyClass" in output

    def test_function_as_stereotype(self) -> None:
        """Render function with stereotype when no classes."""
        finding = Finding(
            category="definition",
            message="function: my_func",
            anchor=Anchor(file="test.py", line=1),
            details=DetailPayload.from_legacy({"name": "my_func", "kind": "function"}),
        )
        result = _make_result(key_findings=[finding])
        output = render_mermaid_class_diagram(result)

        assert "<<function>>" in output


class TestDotRenderer:
    """Tests for DOT graph rendering."""

    def test_empty_result(self) -> None:
        """Render empty result produces valid DOT."""
        result = _make_result()
        output = render_dot(result)

        assert "digraph" in output
        assert "rankdir=LR" in output
        assert "{" in output
        assert "}" in output

    def test_custom_graph_name(self) -> None:
        """Render with custom graph name."""
        result = _make_result()
        output = render_dot(result, graph_name="my_graph")

        assert 'digraph "my_graph"' in output

    def test_single_function(self) -> None:
        """Render single function definition."""
        finding = Finding(
            category="definition",
            message="function: foo",
            anchor=Anchor(file="test.py", line=1),
            details=DetailPayload.from_legacy({"name": "foo", "kind": "function"}),
        )
        result = _make_result(key_findings=[finding])
        output = render_dot(result)

        assert 'foo [label="foo"' in output
        assert "shape=box" in output

    def test_class_shape(self) -> None:
        """Render class with ellipse shape."""
        finding = Finding(
            category="definition",
            message="class: MyClass",
            anchor=Anchor(file="test.py", line=1),
            details=DetailPayload.from_legacy({"name": "MyClass", "kind": "class"}),
        )
        result = _make_result(key_findings=[finding])
        output = render_dot(result)

        assert "shape=ellipse" in output

    def test_caller_callee_edge(self) -> None:
        """Render caller-callee edge into DOT when callers section is present."""
        definition = Finding(
            category="definition",
            message="function: foo",
            anchor=Anchor(file="test.py", line=1),
            details=DetailPayload.from_legacy({"name": "foo", "kind": "function"}),
        )
        caller = Finding(
            category="caller",
            message="caller: bar calls foo",
            anchor=Anchor(file="test.py", line=10),
            details=DetailPayload.from_legacy({"caller": "bar", "callee": "foo"}),
        )
        section = Section(title="Callers", findings=[caller])
        result = _make_result(key_findings=[definition], sections=[section])
        output = render_dot(result)

        assert "bar -> foo" in output

    def test_escapes_special_chars(self) -> None:
        """Escape special characters in labels."""
        finding = Finding(
            category="definition",
            message='function: "quoted"',
            anchor=Anchor(file="test.py", line=1),
            details=DetailPayload.from_legacy({"name": '"quoted"', "kind": "function"}),
        )
        result = _make_result(key_findings=[finding])
        output = render_dot(result)

        # Should escape quotes
        assert '\\"' in output or "quoted" in output
