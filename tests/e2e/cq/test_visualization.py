"""E2E tests for visualization features.

Tests the Mermaid and DOT renderers for CqResult visualization.
"""

from __future__ import annotations

from tools.cq.core.renderers.dot import _escape_dot_string, _sanitize_dot_id, render_dot
from tools.cq.core.renderers.mermaid import (
    _sanitize_node_id,
    render_mermaid_class_diagram,
    render_mermaid_flowchart,
)
from tools.cq.core.schema import Anchor, CqResult, DetailPayload, Finding, RunMeta, Section


def _make_run_meta() -> RunMeta:
    """Create a minimal RunMeta for testing.

    Returns
    -------
    RunMeta
        Minimal run metadata for tests.
    """
    return RunMeta(
        macro="test",
        argv=[],
        root="/test",
        started_ms=0.0,
        elapsed_ms=0.0,
        toolchain={},
        schema_version="1.0",
    )


class TestMermaidFlowchart:
    """Tests for Mermaid flowchart rendering."""

    def test_render_empty_result(self) -> None:
        """Empty result produces minimal diagram."""
        result = CqResult(run=_make_run_meta(), key_findings=[], sections=[])
        output = render_mermaid_flowchart(result)
        assert "```mermaid" in output
        assert "flowchart TD" in output
        assert "```" in output

    def test_render_with_definitions(self) -> None:
        """Result with definitions includes nodes."""
        findings = [
            Finding(
                category="definition",
                message="Function foo",
                anchor=Anchor(file="test.py", line=1, col=0),
                details=DetailPayload.from_legacy({"name": "foo", "kind": "function"}),
            )
        ]
        result = CqResult(run=_make_run_meta(), key_findings=findings, sections=[])
        output = render_mermaid_flowchart(result)
        assert "foo" in output

    def test_render_with_callers(self) -> None:
        """Result with caller section includes edges."""
        callers = [
            Finding(
                category="caller",
                message="bar calls foo",
                anchor=Anchor(file="test.py", line=10, col=0),
                details=DetailPayload.from_legacy({"caller": "bar", "callee": "foo"}),
            )
        ]
        result = CqResult(
            run=_make_run_meta(),
            key_findings=[],
            sections=[Section(title="Callers", findings=callers)],
        )
        output = render_mermaid_flowchart(result)
        assert "-->" in output


class TestMermaidClassDiagram:
    """Tests for Mermaid class diagram rendering."""

    def test_render_empty_result(self) -> None:
        """Empty result produces minimal diagram."""
        result = CqResult(run=_make_run_meta(), key_findings=[], sections=[])
        output = render_mermaid_class_diagram(result)
        assert "```mermaid" in output
        assert "classDiagram" in output
        assert "```" in output

    def test_render_with_class(self) -> None:
        """Result with class definition includes class node."""
        findings = [
            Finding(
                category="definition",
                message="Class MyClass",
                anchor=Anchor(file="test.py", line=1, col=0),
                details=DetailPayload.from_legacy({"name": "MyClass", "kind": "class"}),
            )
        ]
        result = CqResult(run=_make_run_meta(), key_findings=findings, sections=[])
        output = render_mermaid_class_diagram(result)
        assert "MyClass" in output

    def test_render_function_as_class_node(self) -> None:
        """Functions without class context render as class nodes."""
        findings = [
            Finding(
                category="definition",
                message="Function standalone",
                anchor=Anchor(file="test.py", line=1, col=0),
                details=DetailPayload.from_legacy({"name": "standalone", "kind": "function"}),
            )
        ]
        result = CqResult(run=_make_run_meta(), key_findings=findings, sections=[])
        output = render_mermaid_class_diagram(result)
        assert "<<function>>" in output


class TestMermaidNodeSanitization:
    """Tests for Mermaid node ID sanitization."""

    def test_simple_name(self) -> None:
        """Simple names pass through."""
        assert _sanitize_node_id("foo") == "foo"

    def test_name_with_special_chars(self) -> None:
        """Special characters are replaced."""
        result = _sanitize_node_id("foo.bar")
        assert "." not in result
        assert "foo" in result

    def test_name_starting_with_number(self) -> None:
        """Names starting with numbers get prefix."""
        result = _sanitize_node_id("123abc")
        assert result[0].isalpha()

    def test_empty_name(self) -> None:
        """Empty name returns 'unknown'."""
        assert _sanitize_node_id("") == "unknown"


class TestDotRenderer:
    """Tests for DOT format rendering."""

    def test_render_empty_result(self) -> None:
        """Empty result produces valid DOT graph."""
        result = CqResult(run=_make_run_meta(), key_findings=[], sections=[])
        output = render_dot(result)
        assert "digraph" in output
        assert "rankdir=LR" in output

    def test_render_with_definitions(self) -> None:
        """Result with definitions includes nodes."""
        findings = [
            Finding(
                category="definition",
                message="Function foo",
                anchor=Anchor(file="test.py", line=1, col=0),
                details=DetailPayload.from_legacy({"name": "foo", "kind": "function"}),
            )
        ]
        result = CqResult(run=_make_run_meta(), key_findings=findings, sections=[])
        output = render_dot(result)
        assert "foo" in output
        assert "[label=" in output

    def test_render_with_callers(self) -> None:
        """Result with caller section includes edges."""
        callers = [
            Finding(
                category="caller",
                message="bar calls foo",
                anchor=Anchor(file="test.py", line=10, col=0),
                details=DetailPayload.from_legacy({"caller": "bar", "callee": "foo"}),
            )
        ]
        result = CqResult(
            run=_make_run_meta(),
            key_findings=[],
            sections=[Section(title="Callers", findings=callers)],
        )
        output = render_dot(result)
        assert "->" in output


class TestDotSanitization:
    """Tests for DOT string sanitization."""

    def test_sanitize_simple_id(self) -> None:
        """Simple IDs pass through."""
        assert _sanitize_dot_id("foo") == "foo"

    def test_sanitize_id_with_dots(self) -> None:
        """Dots are replaced in IDs."""
        result = _sanitize_dot_id("foo.bar")
        assert "." not in result

    def test_escape_quotes_in_labels(self) -> None:
        """Quotes are escaped in labels."""
        result = _escape_dot_string('foo "bar" baz')
        assert '\\"' in result

    def test_escape_backslashes(self) -> None:
        """Backslashes are escaped in labels."""
        result = _escape_dot_string("foo\\bar")
        assert "\\\\" in result

    def test_escape_newlines(self) -> None:
        """Newlines are escaped in labels."""
        result = _escape_dot_string("foo\nbar")
        assert "\\n" in result
