"""Generate documentation from semantic catalog and relationship specs.

This module auto-generates Mermaid flowchart diagrams and Markdown
documentation from the registered semantic views and relationship
specifications. The documentation reflects the actual pipeline structure
defined in the spec registries.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from semantics.catalog import SemanticCatalog
    from semantics.quality import QualityRelationshipSpec
    from semantics.specs import RelationshipSpec, SemanticTableSpec


def _build_extraction_subgraph() -> list[str]:
    """Build Mermaid lines for extraction input subgraph.

    Returns
    -------
    list[str]
        Mermaid diagram lines for extraction subgraph.
    """
    from semantics.spec_registry import SEMANTIC_NORMALIZATION_SPECS

    sources = sorted({spec.source_table for spec in SEMANTIC_NORMALIZATION_SPECS})
    lines = ["    subgraph Extraction"]
    lines.extend([f"        {table}[{table}]" for table in sources])
    lines.append("        scip_occurrences[scip_occurrences]")
    lines.append("        file_line_index[file_line_index_v1]")
    lines.append("    end")
    return lines


def _build_normalization_subgraph() -> list[str]:
    """Build Mermaid lines for normalization subgraph.

    Returns
    -------
    list[str]
        Mermaid diagram lines for normalization subgraph.
    """
    from semantics.naming import canonical_output_name
    from semantics.spec_registry import SEMANTIC_NORMALIZATION_SPECS

    lines = ["    subgraph Normalization"]
    lines.extend(
        [
            f"        {spec.normalized_name}[{spec.output_name}]"
            for spec in SEMANTIC_NORMALIZATION_SPECS
        ]
    )
    scip_output = canonical_output_name("scip_occurrences_norm")
    lines.append(f"        scip_occurrences_norm[{scip_output}]")
    lines.append("    end")
    return lines


def _build_relationship_subgraph(
    specs: tuple[RelationshipSpec | QualityRelationshipSpec, ...],
) -> list[str]:
    """Build Mermaid lines for relationship subgraph.

    Returns
    -------
    list[str]
        Mermaid diagram lines for relationship subgraph.
    """
    lines = ["    subgraph Relationships"]
    for spec in specs:
        node_id = spec.name.replace("_v1", "").replace("-", "_")
        lines.append(f"        {node_id}[{spec.name}]")
    lines.append("    end")
    return lines


def _build_output_subgraph() -> list[str]:
    """Build Mermaid lines for output subgraph.

    Returns
    -------
    list[str]
        Mermaid diagram lines for output subgraph.
    """
    return [
        "    subgraph Outputs",
        "        cpg_nodes[cpg_nodes_v1]",
        "        cpg_edges[cpg_edges_v1]",
        "    end",
    ]


def _build_extraction_edges() -> list[str]:
    """Build Mermaid edges from extraction to normalization.

    Returns
    -------
    list[str]
        Mermaid diagram edge lines.
    """
    from semantics.spec_registry import SEMANTIC_NORMALIZATION_SPECS

    lines = ["", "    %% Extraction to Normalization"]
    lines.extend(
        [
            f"    {spec.source_table} --> {spec.normalized_name}"
            for spec in SEMANTIC_NORMALIZATION_SPECS
        ]
    )
    lines.append("    scip_occurrences --> scip_occurrences_norm")
    lines.append("    file_line_index --> scip_occurrences_norm")
    return lines


def _build_relationship_edges(
    specs: tuple[RelationshipSpec | QualityRelationshipSpec, ...],
) -> list[str]:
    """Build Mermaid edges from normalization to relationships.

    Returns
    -------
    list[str]
        Mermaid diagram edge lines.
    """
    lines = ["", "    %% Normalization to Relationships"]
    for spec in specs:
        node_id = spec.name.replace("_v1", "").replace("-", "_")
        left_base, right_base = _relationship_left_right(spec)
        left_base = left_base.replace("_v1", "").replace("-", "_")
        right_base = right_base.replace("_v1", "").replace("-", "_")
        lines.append(f"    {left_base} --> {node_id}")
        lines.append(f"    {right_base} --> {node_id}")
    return lines


def _build_output_edges(
    specs: tuple[RelationshipSpec | QualityRelationshipSpec, ...],
) -> list[str]:
    """Build Mermaid edges to final outputs.

    Returns
    -------
    list[str]
        Mermaid diagram edge lines.
    """
    from semantics.spec_registry import SEMANTIC_NORMALIZATION_SPECS

    lines = ["", "    %% To Final Outputs"]
    lines.extend(
        [
            f"    {spec.normalized_name} --> cpg_nodes"
            for spec in SEMANTIC_NORMALIZATION_SPECS
            if spec.include_in_cpg_nodes
        ]
    )
    for spec in specs:
        node_id = spec.name.replace("_v1", "").replace("-", "_")
        lines.append(f"    {node_id} --> cpg_edges")
    return lines


def _build_catalog_edges(catalog: SemanticCatalog) -> list[str]:
    """Build additional Mermaid edges from catalog dependencies.

    Returns
    -------
    list[str]
        Mermaid diagram edge lines from catalog dependency graph.
    """
    lines: list[str] = []
    if len(catalog) == 0:
        return lines

    lines.append("")
    lines.append("    %% Catalog Dependencies")
    dep_graph = catalog.dependency_graph()
    for name, deps in dep_graph.items():
        node_id = name.replace("_v1", "").replace("-", "_")
        for dep in deps:
            dep_id = dep.replace("_v1", "").replace("-", "_")
            edge = f"    {dep_id} --> {node_id}"
            if edge not in lines:
                lines.append(edge)
    return lines


def generate_mermaid_diagram(
    catalog: SemanticCatalog | None = None,
) -> str:
    """Generate Mermaid flowchart from registered views.

    Generate a Mermaid diagram showing the data flow through the semantic
    pipeline, including extraction inputs, normalization views, relationship
    joins, and final CPG outputs.

    Parameters
    ----------
    catalog
        Semantic catalog with registered views. If provided, additional
        dependency edges from the catalog are included.

    Returns
    -------
    str
        Mermaid diagram source code with markdown code fence.

    Example
    -------
    >>> diagram = generate_mermaid_diagram()
    >>> print(diagram[:50])
    ```mermaid
    flowchart TD
        subgraph Extraction
    """
    from semantics.spec_registry import RELATIONSHIP_SPECS

    lines = ["```mermaid", "flowchart TD"]

    # Build subgraphs
    lines.extend(_build_extraction_subgraph())
    lines.extend(_build_normalization_subgraph())
    lines.extend(_build_relationship_subgraph(RELATIONSHIP_SPECS))
    lines.extend(_build_output_subgraph())

    # Build edges
    lines.extend(_build_extraction_edges())
    lines.extend(_build_relationship_edges(RELATIONSHIP_SPECS))
    lines.extend(_build_output_edges(RELATIONSHIP_SPECS))

    # Add catalog-derived edges if available
    if catalog is not None:
        lines.extend(_build_catalog_edges(catalog))

    lines.append("```")

    return "\n".join(lines)


def _format_extraction_inputs() -> list[str]:
    """Format extraction inputs section.

    Returns
    -------
    list[str]
        Markdown lines for extraction inputs section.
    """
    from semantics.input_registry import SEMANTIC_INPUT_SPECS

    descriptions = {
        "cst_refs": "Name references extracted from CST",
        "cst_defs": "Definitions (functions, classes, etc.) from CST",
        "cst_imports": "Import statements from CST",
        "cst_callsites": "Function/method call sites from CST",
        "cst_call_args": "Call argument spans from CST",
        "cst_docstrings": "Docstring spans from CST",
        "cst_decorators": "Decorator spans from CST",
        "scip_occurrences": "Symbol occurrences from SCIP index",
        "file_line_index_v1": "Line-to-byte offset mapping for SCIP normalization",
    }

    lines = [
        "## Extraction Inputs",
        "",
        "The pipeline consumes these extraction tables:",
        "",
        "| Table | Description |",
        "|-------|-------------|",
    ]
    for spec in SEMANTIC_INPUT_SPECS:
        name = spec.extraction_source
        desc = descriptions.get(spec.canonical_name) or descriptions.get(name)
        if desc is None:
            desc = "Extraction input table"
        lines.append(f"| `{name}` | {desc} |")
    lines.append("")
    return lines


def _format_table_spec(name: str, spec: SemanticTableSpec) -> list[str]:
    """Format a single table specification.

    Returns
    -------
    list[str]
        Markdown lines for the table specification.
    """
    lines = [
        f"### {name}",
        "",
        f"- **Primary span**: `{spec.primary_span.start_col}` to `{spec.primary_span.end_col}`",
        f"- **Entity ID**: `{spec.entity_id.out_col}` (namespace: `{spec.entity_id.namespace}`)",
    ]
    if spec.foreign_keys:
        fk_names = ", ".join(f"`{fk.out_col}`" for fk in spec.foreign_keys)
        lines.append(f"- **Foreign keys**: {fk_names}")
    if spec.text_cols:
        text_names = ", ".join(f"`{col}`" for col in spec.text_cols)
        lines.append(f"- **Text columns**: {text_names}")
    lines.append("")
    return lines


def _format_table_specs(
    specs: dict[str, SemanticTableSpec],
) -> list[str]:
    """Format table specifications section.

    Returns
    -------
    list[str]
        Markdown lines for table specifications section.
    """
    description = (
        "Table specifications define how extraction tables are normalized with "
        "byte-span anchoring and stable ID generation."
    )
    lines = [
        "## Table Specifications",
        "",
        description,
        "",
    ]
    for name, spec in specs.items():
        lines.extend(_format_table_spec(name, spec))
    return lines


def _relationship_left_right(
    spec: RelationshipSpec | QualityRelationshipSpec,
) -> tuple[str, str]:
    """Return left/right view names for a relationship spec.

    Returns
    -------
    tuple[str, str]
        Left and right view names.
    """
    from semantics.quality import QualityRelationshipSpec

    if isinstance(spec, QualityRelationshipSpec):
        return spec.left_view, spec.right_view
    return spec.left_table, spec.right_table


def _format_relationship_spec(
    spec: RelationshipSpec | QualityRelationshipSpec,
) -> list[str]:
    """Format a single relationship specification.

    Returns
    -------
    list[str]
        Markdown lines for the relationship specification.
    """
    from semantics.quality import QualityRelationshipSpec

    left_view, right_view = _relationship_left_right(spec)
    lines = [
        f"### {spec.name}",
        "",
        f"- **Left view**: `{left_view}`",
        f"- **Right view**: `{right_view}`",
    ]
    if isinstance(spec, QualityRelationshipSpec):
        join_pairs = ", ".join(
            f"{left}={right}" for left, right in zip(spec.left_on, spec.right_on, strict=False)
        )
        lines.append(f"- **Join type**: `{spec.how}`")
        if join_pairs:
            lines.append(f"- **Join keys**: `{join_pairs}`")
        lines.append(f"- **Origin**: `{spec.origin}`")
        lines.append(f"- **Provider**: `{spec.provider}`")
        if spec.rule_name is not None:
            lines.append(f"- **Rule name**: `{spec.rule_name}`")
    else:
        lines.append(f"- **Join hint**: `{spec.join_hint}`")
        lines.append(f"- **Origin**: `{spec.origin}`")
        if spec.filter_sql:
            lines.append(f"- **Filter**: `{spec.filter_sql}`")
    lines.append("")
    return lines


def _format_relationship_specs(
    specs: tuple[RelationshipSpec | QualityRelationshipSpec, ...],
) -> list[str]:
    """Format relationship specifications section.

    Returns
    -------
    list[str]
        Markdown lines for relationship specifications section.
    """
    description = (
        "Relationship specifications define how normalized tables are joined to "
        "build CPG edges. New relationships can be added to the registry without "
        "modifying pipeline code."
    )
    lines = [
        "## Relationship Specifications",
        "",
        description,
        "",
    ]
    for spec in specs:
        lines.extend(_format_relationship_spec(spec))
    return lines


def _format_output_views() -> list[str]:
    """Format output views section.

    Returns
    -------
    list[str]
        Markdown lines for output views section.
    """
    from semantics.naming import SEMANTIC_OUTPUT_NAMES

    lines = [
        "## Output Views",
        "",
        "All semantic outputs use versioned canonical names:",
        "",
        "| Internal Name | Output Name |",
        "|---------------|-------------|",
    ]
    for internal, output in SEMANTIC_OUTPUT_NAMES.items():
        lines.append(f"| `{internal}` | `{output}` |")
    lines.extend(
        [
            "",
            "## Final CPG Outputs",
            "",
            "| Output | Description |",
            "|--------|-------------|",
            "| `semantic_nodes_union_v1` | Union of all normalized entity tables |",
            "| `semantic_edges_union_v1` | Union of all relationship views |",
            "| `cpg_nodes_v1` | Canonical CPG node output (stable IDs, spans) |",
            "| `cpg_edges_v1` | Canonical CPG edge output (stable IDs, spans) |",
            "",
        ]
    )
    return lines


def _format_catalog_entries(catalog: SemanticCatalog) -> list[str]:
    """Format catalog entries section.

    Returns
    -------
    list[str]
        Markdown lines for catalog entries section, or empty if no entries.
    """
    if len(catalog) == 0:
        return []

    lines = [
        "## Catalog Entries",
        "",
        "Views registered in the semantic catalog:",
        "",
        "| View | Evidence Tier | Upstream Dependencies |",
        "|------|---------------|----------------------|",
    ]
    for name in catalog.topological_order():
        entry = catalog.get(name)
        if entry is not None:
            deps = ", ".join(f"`{d}`" for d in entry.upstream_deps) or "None"
            lines.append(f"| `{name}` | {entry.evidence_tier} | {deps} |")
    lines.append("")
    return lines


def generate_markdown_docs(
    catalog: SemanticCatalog | None = None,
) -> str:
    """Generate Markdown documentation for semantic views.

    Produce comprehensive Markdown documentation that describes the semantic
    pipeline including table specifications, relationship specifications,
    and the data flow diagram.

    Parameters
    ----------
    catalog
        Semantic catalog with registered views. If None, catalog section
        is omitted from the output.

    Returns
    -------
    str
        Complete Markdown documentation.
    """
    from semantics.spec_registry import RELATIONSHIP_SPECS, SEMANTIC_TABLE_SPECS

    sections: list[str] = [
        "# Semantic Pipeline Documentation",
        "",
        "## Overview",
        "",
        "The semantic pipeline transforms extraction outputs into CPG nodes and edges.",
        "It uses a declarative specification-driven approach where relationships are",
        "defined in the spec registry and automatically compiled into DataFusion views.",
        "",
    ]

    sections.extend(_format_extraction_inputs())
    sections.extend(_format_table_specs(SEMANTIC_TABLE_SPECS))
    sections.extend(_format_relationship_specs(RELATIONSHIP_SPECS))
    sections.extend(_format_output_views())

    if catalog is not None:
        sections.extend(_format_catalog_entries(catalog))

    sections.extend(
        [
            "## Data Flow Diagram",
            "",
            generate_mermaid_diagram(catalog),
        ]
    )

    return "\n".join(sections)


def export_graph_documentation(
    output_path: str | None = None,
    *,
    catalog: SemanticCatalog | None = None,
) -> str:
    """Export full documentation to file or return as string.

    Generate comprehensive documentation for the semantic pipeline and
    optionally write it to a file.

    Parameters
    ----------
    output_path
        Path to write documentation. If None, returns string only.
    catalog
        Semantic catalog to document. If None, uses default catalog.

    Returns
    -------
    str
        Generated documentation content.

    Example
    -------
    >>> # Export to file
    >>> export_graph_documentation("docs/semantic_graph.md")
    '# Semantic Pipeline Documentation...'

    >>> # Get as string only
    >>> content = export_graph_documentation()
    """
    content = generate_markdown_docs(catalog)

    if output_path:
        Path(output_path).write_text(content, encoding="utf-8")

    return content


__all__ = [
    "export_graph_documentation",
    "generate_markdown_docs",
    "generate_mermaid_diagram",
]
