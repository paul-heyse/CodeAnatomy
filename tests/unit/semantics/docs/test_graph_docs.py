"""Tests for explain-plan documentation generation."""

from __future__ import annotations

import tempfile
from pathlib import Path

from semantics.docs import (
    export_graph_documentation,
    generate_markdown_docs,
    generate_mermaid_diagram,
)


def _sample_explain_payload() -> dict[str, object]:
    return {
        "semantic_model_hash": "model_hash",
        "semantic_ir_hash": "ir_hash",
        "views": [
            {
                "name": "view_a",
                "kind": "normalize",
                "inputs": ["input_x"],
                "outputs": ["view_a"],
            },
            {
                "name": "view_b",
                "kind": "relate",
                "inputs": ["view_a"],
                "outputs": ["view_b"],
            },
        ],
        "join_groups": [],
    }


def test_generate_mermaid_diagram_renders_edges() -> None:
    payload = _sample_explain_payload()
    diagram = generate_mermaid_diagram(payload)
    assert diagram.startswith("```mermaid")
    assert "flowchart TD" in diagram
    assert "view_a[view_a]" in diagram
    assert "view_b[view_b]" in diagram
    assert "-->" in diagram


def test_generate_markdown_docs_from_payload() -> None:
    payload = _sample_explain_payload()
    docs = generate_markdown_docs(explain_payload=payload)
    assert docs.startswith("# Semantic Explain Plan")
    assert "## Views" in docs
    assert "view_a" in docs
    assert "## Data Flow Diagram" in docs


def test_generate_markdown_docs_with_report_payload() -> None:
    payload = _sample_explain_payload()
    report_payload = {"markdown": "# Report Header"}
    docs = generate_markdown_docs(
        report_payload=report_payload,
        explain_payload=payload,
    )
    assert docs.startswith("# Report Header")
    assert "## Data Flow Diagram" in docs


def test_export_graph_documentation_writes_file() -> None:
    payload = _sample_explain_payload()
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "graph.md"
        content = export_graph_documentation(
            str(output_path),
            explain_payload=payload,
        )
        assert output_path.exists()
        assert output_path.read_text(encoding="utf-8") == content
