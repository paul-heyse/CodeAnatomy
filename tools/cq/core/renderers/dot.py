"""Graphviz DOT renderer for cq results.

Renders CqResult to Graphviz DOT format for visualization.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult


def render_dot(result: CqResult, graph_name: str = "cq_result") -> str:
    """Render CqResult as Graphviz DOT format.

    Parameters
    ----------
    result
        CqResult to render
    graph_name
        Name for the DOT graph

    Returns
    -------
    str
        DOT format string.
    """
    lines: list[str] = [
        f'digraph "{_escape_dot_string(graph_name)}" {{',
        '    rankdir=LR;',
        '    node [shape=box, style=rounded];',
    ]

    # Track nodes and edges
    nodes: set[str] = set()
    edges: list[tuple[str, str, dict]] = []

    # Process key findings for definitions
    for finding in result.key_findings:
        if finding.category in ("definition", "function", "class", "pattern_match"):
            name = finding.details.get("name", "unknown")
            node_id = _sanitize_dot_id(name)
            kind = finding.details.get("kind", "")

            if node_id not in nodes:
                nodes.add(node_id)
                shape = _kind_to_shape(kind)
                label = _escape_dot_string(name)
                lines.append(f'    {node_id} [label="{label}", shape={shape}];')

    # Process caller sections
    for section in result.sections:
        if section.title.lower() == "callers":
            for finding in section.findings:
                caller = finding.details.get("caller", "")
                callee = finding.details.get("callee", "")
                if caller and callee:
                    caller_id = _sanitize_dot_id(caller)
                    callee_id = _sanitize_dot_id(callee)

                    # Add nodes if not present
                    if caller_id not in nodes:
                        nodes.add(caller_id)
                        label = _escape_dot_string(caller)
                        lines.append(f'    {caller_id} [label="{label}"];')
                    if callee_id not in nodes:
                        nodes.add(callee_id)
                        label = _escape_dot_string(callee)
                        lines.append(f'    {callee_id} [label="{label}"];')

                    edge = (caller_id, callee_id, {})
                    if (caller_id, callee_id, {}) not in edges:
                        edges.append(edge)
                        lines.append(f'    {caller_id} -> {callee_id};')

    lines.append("}")

    return "\n".join(lines)


def _sanitize_dot_id(name: str) -> str:
    """Sanitize a name for use as a DOT node ID.

    Parameters
    ----------
    name
        Original name

    Returns
    -------
    str
        Sanitized node ID safe for DOT.
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


def _escape_dot_string(s: str) -> str:
    """Escape a string for use in DOT labels.

    Parameters
    ----------
    s
        Original string

    Returns
    -------
    str
        Escaped string safe for DOT labels.
    """
    # Escape quotes and backslashes
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    # Escape newlines
    escaped = escaped.replace("\n", "\\n")
    return escaped


def _kind_to_shape(kind: str) -> str:
    """Map entity kind to DOT shape.

    Parameters
    ----------
    kind
        Entity kind (function, class, etc.)

    Returns
    -------
    str
        DOT shape name.
    """
    shape_map = {
        "function": "box",
        "async_function": "box",
        "function_typeparams": "box",
        "class": "ellipse",
        "class_bases": "ellipse",
        "class_typeparams": "ellipse",
        "import": "parallelogram",
        "from_import": "parallelogram",
        "module": "folder",
        "method": "component",
    }
    return shape_map.get(kind, "box")
