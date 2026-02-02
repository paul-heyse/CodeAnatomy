"""Generate documentation from semantic explain-plan artifacts.

This module renders Markdown and Mermaid diagrams using the
semantic_explain_plan_v1 and semantic_explain_plan_report_v1 artifacts
emitted during semantic compilation. When artifacts are not provided,
the helpers fall back to building an explain payload directly from the
semantic IR.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

ArtifactPayload = Mapping[str, object]
ArtifactsSnapshot = Mapping[str, Sequence[ArtifactPayload]]


def _latest_artifact(
    artifacts: ArtifactsSnapshot,
    name: str,
) -> ArtifactPayload | None:
    rows = artifacts.get(name)
    if not rows:
        return None
    return dict(rows[-1])


def _coerce_str_list(value: object) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [item for item in value if isinstance(item, str)]
    return []


def _node_id(name: str) -> str:
    sanitized = name.replace("-", "_").replace(".", "_").replace("/", "_").replace(":", "_")
    return sanitized


def _explain_payload_from_ir(outputs: Sequence[str] | None) -> dict[str, object]:
    from semantics.ir_pipeline import build_semantic_ir

    semantic_ir = build_semantic_ir(outputs=set(outputs) if outputs else None)
    return {
        "semantic_model_hash": semantic_ir.model_hash,
        "semantic_ir_hash": semantic_ir.ir_hash,
        "views": [
            {
                "name": view.name,
                "kind": view.kind,
                "inputs": view.inputs,
                "outputs": view.outputs,
            }
            for view in semantic_ir.views
        ],
        "join_groups": [
            {
                "name": group.name,
                "left_view": group.left_view,
                "right_view": group.right_view,
                "left_on": group.left_on,
                "right_on": group.right_on,
                "how": group.how,
                "relationship_names": group.relationship_names,
            }
            for group in semantic_ir.join_groups
        ],
    }


def _resolve_explain_payload(
    *,
    artifacts: ArtifactsSnapshot | None,
    explain_payload: ArtifactPayload | None,
    requested_outputs: Sequence[str] | None,
) -> dict[str, object] | None:
    if explain_payload is not None:
        return dict(explain_payload)
    if artifacts is not None:
        payload = _latest_artifact(artifacts, "semantic_explain_plan_v1")
        if payload is not None:
            return dict(payload)
    if requested_outputs is None and artifacts is None:
        return _explain_payload_from_ir(outputs=None)
    if requested_outputs is not None:
        return _explain_payload_from_ir(outputs=requested_outputs)
    return None


def _resolve_report_markdown(
    *,
    artifacts: ArtifactsSnapshot | None,
    report_payload: ArtifactPayload | None,
) -> str | None:
    if report_payload is not None:
        markdown = report_payload.get("markdown")
        if isinstance(markdown, str):
            return markdown
        return None
    if artifacts is None:
        return None
    payload = _latest_artifact(artifacts, "semantic_explain_plan_report_v1")
    if payload is None:
        return None
    markdown = payload.get("markdown")
    if isinstance(markdown, str):
        return markdown
    return None


def generate_mermaid_diagram(explain_payload: ArtifactPayload) -> str:
    """Generate a Mermaid flowchart diagram from explain-plan payload."""
    views = explain_payload.get("views")
    if not isinstance(views, Sequence):
        return "```mermaid\nflowchart TD\n```"

    view_rows: list[Mapping[str, object]] = [
        row for row in views if isinstance(row, Mapping) and "name" in row
    ]
    nodes: dict[str, str] = {}
    for row in view_rows:
        name = row.get("name")
        if isinstance(name, str):
            nodes[name] = _node_id(name)

    edge_lines: list[str] = []
    for row in view_rows:
        view_name = row.get("name")
        if not isinstance(view_name, str):
            continue
        inputs = _coerce_str_list(row.get("inputs"))
        for dep in inputs:
            nodes.setdefault(dep, _node_id(dep))
            edge_lines.append(f"    {nodes[dep]} --> {nodes[view_name]}")

    lines = ["```mermaid", "flowchart TD"]
    for name, node_id in nodes.items():
        lines.append(f"    {node_id}[{name}]")
    if edge_lines:
        lines.append("")
        lines.append("    %% Dependencies")
        lines.extend(edge_lines)
    lines.append("```")
    return "\n".join(lines)


def _format_explain_markdown(explain_payload: ArtifactPayload) -> str:
    model_hash = explain_payload.get("semantic_model_hash") or ""
    ir_hash = explain_payload.get("semantic_ir_hash") or ""
    views = explain_payload.get("views")
    join_groups = explain_payload.get("join_groups")

    view_rows: list[Mapping[str, object]] = [
        row for row in views if isinstance(row, Mapping)
    ] if isinstance(views, Sequence) else []
    group_rows: list[Mapping[str, object]] = [
        row for row in join_groups if isinstance(row, Mapping)
    ] if isinstance(join_groups, Sequence) else []

    lines: list[str] = [
        "# Semantic Explain Plan",
        "",
        f"- semantic_model_hash: {model_hash}",
        f"- semantic_ir_hash: {ir_hash}",
        f"- view_count: {len(view_rows)}",
        f"- join_group_count: {len(group_rows)}",
        "",
        "## Views",
        "| name | kind | inputs | outputs |",
        "| --- | --- | --- | --- |",
    ]
    for row in view_rows:
        name = row.get("name")
        kind = row.get("kind")
        inputs = ", ".join(_coerce_str_list(row.get("inputs")))
        outputs = ", ".join(_coerce_str_list(row.get("outputs")))
        lines.append(
            "| "
            + " | ".join(
                [
                    str(name or ""),
                    str(kind or ""),
                    inputs,
                    outputs,
                ]
            )
            + " |"
        )

    if group_rows:
        lines.extend(
            [
                "",
                "## Join Groups",
                "| name | left_view | right_view | how | relationships |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for row in group_rows:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("name") or ""),
                        str(row.get("left_view") or ""),
                        str(row.get("right_view") or ""),
                        str(row.get("how") or ""),
                        ", ".join(_coerce_str_list(row.get("relationship_names"))),
                    ]
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## Data Flow Diagram",
            "",
            generate_mermaid_diagram(explain_payload),
        ]
    )
    return "\n".join(lines)


def generate_markdown_docs(
    *,
    artifacts: ArtifactsSnapshot | None = None,
    explain_payload: ArtifactPayload | None = None,
    report_payload: ArtifactPayload | None = None,
    requested_outputs: Sequence[str] | None = None,
) -> str:
    """Generate Markdown documentation from explain-plan artifacts.

    Parameters
    ----------
    artifacts
        Diagnostics artifact snapshot keyed by artifact name.
    explain_payload
        Explicit semantic_explain_plan_v1 payload.
    report_payload
        Explicit semantic_explain_plan_report_v1 payload.
    requested_outputs
        Optional output subset to build an IR-based explain payload when
        artifacts are not provided.
    """
    report = _resolve_report_markdown(artifacts=artifacts, report_payload=report_payload)
    explain_payload_resolved = _resolve_explain_payload(
        artifacts=artifacts,
        explain_payload=explain_payload,
        requested_outputs=requested_outputs,
    )
    if report is not None:
        report_text = report.rstrip()
        if explain_payload_resolved is None:
            return report_text
        diagram = generate_mermaid_diagram(explain_payload_resolved)
        return "\n".join([report_text, "", "## Data Flow Diagram", "", diagram])
    if explain_payload_resolved is None:
        return "# Semantic Explain Plan\n\nNo explain-plan artifacts available."
    return _format_explain_markdown(explain_payload_resolved)


def export_graph_documentation(
    output_path: str | None = None,
    *,
    artifacts: ArtifactsSnapshot | None = None,
    explain_payload: ArtifactPayload | None = None,
    report_payload: ArtifactPayload | None = None,
    requested_outputs: Sequence[str] | None = None,
) -> str:
    """Export semantic graph docs from explain-plan artifacts.

    When no artifacts are provided, the documentation is generated directly
    from the semantic IR using the requested outputs (if supplied).
    """
    content = generate_markdown_docs(
        artifacts=artifacts,
        explain_payload=explain_payload,
        report_payload=report_payload,
        requested_outputs=requested_outputs,
    )
    if output_path:
        Path(output_path).write_text(content, encoding="utf-8")
    return content


__all__ = [
    "export_graph_documentation",
    "generate_markdown_docs",
    "generate_mermaid_diagram",
]
