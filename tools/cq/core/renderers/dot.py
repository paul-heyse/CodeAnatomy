"""Graphviz DOT renderer for cq results.

Renders CqResult to Graphviz DOT format for visualization.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult


@dataclass
class DotRenderBuilder:
    """Builder for DOT output with deduped nodes/edges."""

    graph_name: str
    lines: list[str] = field(default_factory=list)
    nodes: set[str] = field(default_factory=set)
    edges: set[tuple[str, str]] = field(default_factory=set)

    def __post_init__(self) -> None:
        """Initialize the DOT header and graph defaults."""
        escaped_name = _escape_dot_string(self.graph_name)
        self.lines.extend(
            [
                f'digraph "{escaped_name}" {{',
                "    rankdir=LR;",
                "    node [shape=box, style=rounded];",
            ]
        )

    def add_node(self, node_id: str, *, label: str, shape: str | None = None) -> None:
        """Add a node definition when it does not already exist."""
        if node_id in self.nodes:
            return
        self.nodes.add(node_id)
        if shape:
            self.lines.append(f'    {node_id} [label="{label}", shape={shape}];')
        else:
            self.lines.append(f'    {node_id} [label="{label}"];')

    def add_edge(self, source: str, target: str) -> None:
        """Add a directed edge between two nodes."""
        edge = (source, target)
        if edge in self.edges:
            return
        self.edges.add(edge)
        self.lines.append(f"    {source} -> {target};")

    def render(self) -> str:
        """Render the accumulated DOT graph.

        Returns:
        -------
        str
            Complete DOT graph payload.
        """
        return "\n".join([*self.lines, "}"])


def render_dot(result: CqResult, graph_name: str = "cq_result") -> str:
    """Render CqResult as Graphviz DOT format.

    Parameters
    ----------
    result
        CqResult to render
    graph_name
        Name for the DOT graph

    Returns:
    -------
    str
        DOT format string.
    """
    builder = DotRenderBuilder(graph_name)

    # Process key findings for definitions
    for finding in result.key_findings:
        if finding.category in {"definition", "function", "class", "pattern_match"}:
            raw_name = finding.details.get("name", "unknown")
            name = str(raw_name) if raw_name is not None else "unknown"
            node_id = _sanitize_dot_id(name)
            raw_kind = finding.details.get("kind", "")
            kind = str(raw_kind) if raw_kind is not None else ""

            shape = _kind_to_shape(kind)
            label = _escape_dot_string(name)
            builder.add_node(node_id, label=label, shape=shape)

    # Process caller sections
    for section in result.sections:
        if section.title.lower() == "callers":
            for finding in section.findings:
                raw_caller = finding.details.get("caller", "")
                raw_callee = finding.details.get("callee", "")
                caller = str(raw_caller) if raw_caller else ""
                callee = str(raw_callee) if raw_callee else ""
                if caller and callee:
                    caller_id = _sanitize_dot_id(caller)
                    callee_id = _sanitize_dot_id(callee)

                    # Add nodes if not present
                    builder.add_node(caller_id, label=_escape_dot_string(caller))
                    builder.add_node(callee_id, label=_escape_dot_string(callee))
                    builder.add_edge(caller_id, callee_id)

    return builder.render()


def _sanitize_dot_id(name: str) -> str:
    """Sanitize a name for use as a DOT node ID.

    Parameters
    ----------
    name
        Original name

    Returns:
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

    Returns:
    -------
    str
        Escaped string safe for DOT labels.
    """
    # Escape quotes and backslashes
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return escaped.replace("\n", "\\n")


def _kind_to_shape(kind: str) -> str:
    """Map entity kind to DOT shape.

    Parameters
    ----------
    kind
        Entity kind (function, class, etc.)

    Returns:
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
