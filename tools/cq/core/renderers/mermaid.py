"""Mermaid diagram renderers for cq results.

Renders CqResult to Mermaid flowchart and class diagram formats.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult, Finding


def render_mermaid_flowchart(result: CqResult) -> str:
    """Render CqResult as Mermaid flowchart.

    Visualizes call relationships between functions as a directed graph.

    Parameters
    ----------
    result
        CqResult to render

    Returns
    -------
    str
        Mermaid flowchart markdown.
    """
    lines: list[str] = ["```mermaid", "flowchart TD"]

    # Track nodes and edges
    nodes: set[str] = set()
    edges: list[tuple[str, str]] = []

    # Process key findings for definitions
    for finding in result.key_findings:
        if finding.category in ("definition", "function", "class"):
            name = finding.details.get("name", "unknown")
            node_id = _sanitize_node_id(name)
            nodes.add(node_id)
            lines.append(f"    {node_id}[{name}]")

    # Process caller sections
    for section in result.sections:
        if section.title.lower() == "callers":
            for finding in section.findings:
                caller = finding.details.get("caller", "")
                callee = finding.details.get("callee", "")
                if caller and callee:
                    caller_id = _sanitize_node_id(caller)
                    callee_id = _sanitize_node_id(callee)

                    # Add nodes if not present
                    if caller_id not in nodes:
                        nodes.add(caller_id)
                        lines.append(f"    {caller_id}[{caller}]")
                    if callee_id not in nodes:
                        nodes.add(callee_id)
                        lines.append(f"    {callee_id}[{callee}]")

                    edge = (caller_id, callee_id)
                    if edge not in edges:
                        edges.append(edge)
                        lines.append(f"    {caller_id} --> {callee_id}")

    lines.append("```")

    return "\n".join(lines)


def render_mermaid_class_diagram(result: CqResult) -> str:
    """Render CqResult as Mermaid class diagram.

    Visualizes class definitions and their relationships.

    Parameters
    ----------
    result
        CqResult to render

    Returns
    -------
    str
        Mermaid class diagram markdown.
    """
    lines: list[str] = ["```mermaid", "classDiagram"]

    # Track classes and relationships
    classes: dict[str, list[str]] = {}  # class_name -> methods

    # Process key findings for class/method definitions
    for finding in result.key_findings:
        kind = finding.details.get("kind", "")

        if finding.category == "definition":
            name = finding.details.get("name", "unknown")

            if kind in ("class", "class_bases", "class_typeparams"):
                if name not in classes:
                    classes[name] = []
            elif kind in ("function", "async_function", "method"):
                # Try to associate with a class based on file context
                # For now, add as standalone
                pass

    # Render classes
    for class_name, methods in classes.items():
        class_id = _sanitize_node_id(class_name)
        lines.append(f"    class {class_id} {{")
        for method in methods:
            lines.append(f"        +{method}()")
        lines.append("    }")

    # If no classes found, show functions as nodes
    if not classes:
        for finding in result.key_findings:
            if finding.category == "definition":
                name = finding.details.get("name", "unknown")
                kind = finding.details.get("kind", "")
                func_id = _sanitize_node_id(name)
                if kind in ("function", "async_function"):
                    lines.append(f"    class {func_id} {{")
                    lines.append(f"        <<function>>")
                    lines.append("    }")

    lines.append("```")

    return "\n".join(lines)


def _sanitize_node_id(name: str) -> str:
    """Sanitize a name for use as a Mermaid node ID.

    Parameters
    ----------
    name
        Original name

    Returns
    -------
    str
        Sanitized node ID safe for Mermaid.
    """
    # Replace special characters with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)

    # Ensure starts with letter
    if sanitized and not sanitized[0].isalpha():
        sanitized = "n_" + sanitized

    # Handle empty result
    if not sanitized:
        sanitized = "unknown"

    return sanitized
