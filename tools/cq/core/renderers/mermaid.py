"""Mermaid diagram renderers for cq results.

Renders CqResult to Mermaid flowchart and class diagram formats.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult


@dataclass
class MermaidFlowBuilder:
    """Builder for Mermaid flowchart output."""

    lines: list[str] = field(default_factory=list)
    nodes: set[str] = field(default_factory=set)
    edges: set[tuple[str, str]] = field(default_factory=set)

    def __post_init__(self) -> None:
        """Initialize the Mermaid flowchart header."""
        self.lines.extend(["```mermaid", "flowchart TD"])

    def add_node(self, node_id: str, label: str) -> None:
        """Add a flowchart node when it is not already present."""
        if node_id in self.nodes:
            return
        self.nodes.add(node_id)
        self.lines.append(f"    {node_id}[{label}]")

    def add_edge(self, source: str, target: str) -> None:
        """Add a directed flowchart edge."""
        edge = (source, target)
        if edge in self.edges:
            return
        self.edges.add(edge)
        self.lines.append(f"    {source} --> {target}")

    def render(self) -> str:
        """Render the flowchart markdown.

        Returns
        -------
        str
            Mermaid flowchart markdown.
        """
        return "\n".join([*self.lines, "```"])


@dataclass
class MermaidClassBuilder:
    """Builder for Mermaid class diagram output."""

    classes: dict[str, list[str]] = field(default_factory=dict)
    functions: list[str] = field(default_factory=list)

    def add_class(self, name: str) -> None:
        """Register a class name for the diagram."""
        self.classes.setdefault(name, [])

    def add_function(self, name: str) -> None:
        """Register a top-level function name for the diagram."""
        self.functions.append(name)

    def render(self) -> str:
        """Render the class diagram markdown.

        Returns
        -------
        str
            Mermaid class diagram markdown.
        """
        lines: list[str] = ["```mermaid", "classDiagram"]

        if self.classes:
            for class_name, methods in self.classes.items():
                class_id = _sanitize_node_id(class_name)
                lines.append(f"    class {class_id} {{")
                lines.extend(f"        +{method}()" for method in methods)
                lines.append("    }")
        else:
            for name in self.functions:
                func_id = _sanitize_node_id(name)
                lines.append(f"    class {func_id} {{")
                lines.append("        <<function>>")
                lines.append("    }")

        lines.append("```")
        return "\n".join(lines)


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
    builder = MermaidFlowBuilder()

    # Process key findings for definitions
    for finding in result.key_findings:
        if finding.category in {"definition", "function", "class"}:
            name_value = finding.details.get("name", "unknown")
            name = str(name_value) if name_value is not None else "unknown"
            node_id = _sanitize_node_id(name)
            builder.add_node(node_id, name)

    # Process caller sections
    for section in result.sections:
        if section.title.lower() == "callers":
            for finding in section.findings:
                caller_value = finding.details.get("caller", "")
                callee_value = finding.details.get("callee", "")
                caller = str(caller_value) if caller_value is not None else ""
                callee = str(callee_value) if callee_value is not None else ""
                if caller and callee:
                    caller_id = _sanitize_node_id(caller)
                    callee_id = _sanitize_node_id(callee)

                    # Add nodes if not present
                    builder.add_node(caller_id, caller)
                    builder.add_node(callee_id, callee)
                    builder.add_edge(caller_id, callee_id)

    return builder.render()


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
    builder = MermaidClassBuilder()

    # Process key findings for class/method definitions
    for finding in result.key_findings:
        kind_value = finding.details.get("kind", "")
        kind = str(kind_value) if kind_value is not None else ""

        if finding.category == "definition":
            name_value = finding.details.get("name", "unknown")
            name = str(name_value) if name_value is not None else "unknown"

            if kind in {"class", "class_bases", "class_typeparams"}:
                builder.add_class(name)
            elif kind in {"function", "async_function", "method"}:
                # Try to associate with a class based on file context
                # For now, add as standalone
                builder.add_function(name)

    return builder.render()


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
