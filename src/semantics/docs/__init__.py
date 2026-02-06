"""Documentation generation for semantic pipeline.

This module provides auto-generation of Mermaid diagrams and Markdown
documentation from semantic explain-plan artifacts or the semantic IR.

Example:
-------
>>> from semantics.docs import export_graph_documentation
>>>
>>> # Export to file from IR
>>> export_graph_documentation("docs/semantic_graph.md")
>>>
>>> # Get as string
>>> md_content = export_graph_documentation()
"""

from __future__ import annotations

from semantics.docs.graph_docs import (
    export_graph_documentation,
    generate_markdown_docs,
    generate_mermaid_diagram,
)

__all__ = [
    "export_graph_documentation",
    "generate_markdown_docs",
    "generate_mermaid_diagram",
]
