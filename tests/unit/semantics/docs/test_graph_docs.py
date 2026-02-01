"""Tests for graph documentation generation."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from semantics.catalog import SemanticCatalog
from semantics.docs import (
    export_graph_documentation,
    generate_markdown_docs,
    generate_mermaid_diagram,
)

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext


class MockBuilder:
    """Mock builder for testing catalog integration."""

    def __init__(self, name: str, tier: int, deps: tuple[str, ...]) -> None:
        self._name = name
        self._tier = tier
        self._deps = deps

    @property
    def name(self) -> str:
        """Return the builder name."""
        return self._name

    @property
    def evidence_tier(self) -> int:
        """Return the evidence tier."""
        return self._tier

    @property
    def upstream_deps(self) -> tuple[str, ...]:
        """Return upstream dependencies."""
        return self._deps

    def build(self, ctx: SessionContext) -> DataFrame:
        """Mock build hook for protocol compliance."""
        raise NotImplementedError


class TestGenerateMermaidDiagram:
    """Tests for generate_mermaid_diagram function."""

    def test_returns_string(self) -> None:
        """Verify diagram returns a string."""
        result = generate_mermaid_diagram()
        assert isinstance(result, str)

    def test_starts_with_mermaid_fence(self) -> None:
        """Verify diagram starts with mermaid code fence."""
        result = generate_mermaid_diagram()
        assert result.startswith("```mermaid")

    def test_ends_with_code_fence(self) -> None:
        """Verify diagram ends with closing code fence."""
        result = generate_mermaid_diagram()
        assert result.strip().endswith("```")

    def test_contains_flowchart_directive(self) -> None:
        """Verify diagram contains flowchart directive."""
        result = generate_mermaid_diagram()
        assert "flowchart TD" in result

    def test_contains_extraction_subgraph(self) -> None:
        """Verify diagram contains extraction subgraph."""
        result = generate_mermaid_diagram()
        assert "subgraph Extraction" in result

    def test_contains_normalization_subgraph(self) -> None:
        """Verify diagram contains normalization subgraph."""
        result = generate_mermaid_diagram()
        assert "subgraph Normalization" in result

    def test_contains_relationships_subgraph(self) -> None:
        """Verify diagram contains relationships subgraph."""
        result = generate_mermaid_diagram()
        assert "subgraph Relationships" in result

    def test_contains_outputs_subgraph(self) -> None:
        """Verify diagram contains outputs subgraph."""
        result = generate_mermaid_diagram()
        assert "subgraph Outputs" in result

    def test_contains_extraction_nodes(self) -> None:
        """Verify diagram contains extraction input nodes."""
        result = generate_mermaid_diagram()
        assert "cst_refs[cst_refs]" in result
        assert "cst_defs[cst_defs]" in result
        assert "scip_occurrences[scip_occurrences]" in result

    def test_contains_cpg_output_nodes(self) -> None:
        """Verify diagram contains CPG output nodes."""
        result = generate_mermaid_diagram()
        assert "cpg_nodes[cpg_nodes_v1]" in result
        assert "cpg_edges[cpg_edges_v1]" in result

    def test_contains_edge_definitions(self) -> None:
        """Verify diagram contains edge definitions."""
        result = generate_mermaid_diagram()
        assert "-->" in result
        assert "cst_refs --> cst_refs_norm" in result

    def test_with_catalog_includes_catalog_edges(self) -> None:
        """Verify diagram includes catalog dependency edges."""
        catalog = SemanticCatalog()
        catalog.register(MockBuilder("view_a", 1, ()))
        catalog.register(MockBuilder("view_b", 2, ("view_a",)))

        result = generate_mermaid_diagram(catalog)
        assert "%% Catalog Dependencies" in result
        assert "view_a --> view_b" in result

    def test_with_empty_catalog_no_catalog_section(self) -> None:
        """Verify empty catalog does not add catalog section."""
        catalog = SemanticCatalog()
        result = generate_mermaid_diagram(catalog)
        assert "%% Catalog Dependencies" not in result


class TestGenerateMarkdownDocs:
    """Tests for generate_markdown_docs function."""

    def test_returns_string(self) -> None:
        """Verify docs returns a string."""
        result = generate_markdown_docs()
        assert isinstance(result, str)

    def test_starts_with_title(self) -> None:
        """Verify docs starts with title."""
        result = generate_markdown_docs()
        assert result.startswith("# Semantic Pipeline Documentation")

    def test_contains_overview_section(self) -> None:
        """Verify docs contains overview section."""
        result = generate_markdown_docs()
        assert "## Overview" in result

    def test_contains_extraction_inputs_section(self) -> None:
        """Verify docs contains extraction inputs section."""
        result = generate_markdown_docs()
        assert "## Extraction Inputs" in result

    def test_contains_table_specs_section(self) -> None:
        """Verify docs contains table specifications section."""
        result = generate_markdown_docs()
        assert "## Table Specifications" in result

    def test_contains_relationship_specs_section(self) -> None:
        """Verify docs contains relationship specifications section."""
        result = generate_markdown_docs()
        assert "## Relationship Specifications" in result

    def test_contains_output_views_section(self) -> None:
        """Verify docs contains output views section."""
        result = generate_markdown_docs()
        assert "## Output Views" in result

    def test_contains_data_flow_diagram_section(self) -> None:
        """Verify docs contains data flow diagram section."""
        result = generate_markdown_docs()
        assert "## Data Flow Diagram" in result

    def test_contains_mermaid_diagram(self) -> None:
        """Verify docs contains embedded mermaid diagram."""
        result = generate_markdown_docs()
        assert "```mermaid" in result

    def test_with_catalog_includes_catalog_entries(self) -> None:
        """Verify docs includes catalog entries section when provided."""
        catalog = SemanticCatalog()
        catalog.register(MockBuilder("view_a", 1, ()))
        catalog.register(MockBuilder("view_b", 2, ("view_a",)))

        result = generate_markdown_docs(catalog)
        assert "## Catalog Entries" in result
        assert "| `view_a` | 1 | None |" in result
        assert "| `view_b` | 2 | `view_a` |" in result

    def test_with_empty_catalog_no_entries_section(self) -> None:
        """Verify empty catalog does not add entries section."""
        catalog = SemanticCatalog()
        result = generate_markdown_docs(catalog)
        assert "## Catalog Entries" not in result


class TestExportGraphDocumentation:
    """Tests for export_graph_documentation function."""

    def test_returns_string(self) -> None:
        """Verify export returns a string."""
        result = export_graph_documentation()
        assert isinstance(result, str)

    def test_returns_complete_docs(self) -> None:
        """Verify export returns complete documentation."""
        result = export_graph_documentation()
        assert "# Semantic Pipeline Documentation" in result
        assert "## Data Flow Diagram" in result
        assert "```mermaid" in result

    def test_writes_to_file_when_path_provided(self) -> None:
        """Verify export writes to file when path provided."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "docs.md"
            result = export_graph_documentation(str(output_path))

            assert output_path.exists()
            content = output_path.read_text(encoding="utf-8")
            assert content == result
            assert "# Semantic Pipeline Documentation" in content

    def test_does_not_write_when_no_path(self) -> None:
        """Verify export does not create files when no path provided."""
        result = export_graph_documentation()
        assert result is not None
        # No file should be created - just returns string

    def test_with_catalog_parameter(self) -> None:
        """Verify export accepts catalog parameter."""
        catalog = SemanticCatalog()
        catalog.register(MockBuilder("test_view", 1, ()))

        result = export_graph_documentation(catalog=catalog)
        assert "## Catalog Entries" in result
        assert "| `test_view` | 1 | None |" in result


class TestRelationshipSpecsInDocumentation:
    """Tests for relationship specs appearing in documentation."""

    def test_diagram_contains_relationship_names(self) -> None:
        """Verify diagram contains relationship spec names."""
        result = generate_mermaid_diagram()
        # Check for known relationship specs from spec_registry
        assert "rel_name_symbol" in result
        assert "rel_def_symbol" in result

    def test_docs_contains_relationship_details(self) -> None:
        """Verify docs contain relationship spec details."""
        result = generate_markdown_docs()
        assert "### rel_name_symbol_v1" in result
        assert "- **Left view**:" in result
        assert "- **Right view**:" in result
        assert "- **Join type**:" in result


class TestTableSpecsInDocumentation:
    """Tests for table specs appearing in documentation."""

    def test_docs_contains_table_spec_names(self) -> None:
        """Verify docs contain table spec names."""
        result = generate_markdown_docs()
        assert "### cst_defs" in result
        assert "### cst_imports" in result

    def test_docs_contains_table_spec_details(self) -> None:
        """Verify docs contain table spec details."""
        result = generate_markdown_docs()
        assert "- **Primary span**:" in result
        assert "- **Entity ID**:" in result
