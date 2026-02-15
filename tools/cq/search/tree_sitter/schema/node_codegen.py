"""Code generation helpers for tree-sitter node-type snapshot modules."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.tree_sitter.schema.node_schema import GrammarSchemaV1


def render_node_types_module(schema: GrammarSchemaV1) -> str:
    """Render Python module source for generated NODE_TYPES snapshots.

    Returns:
        str: Generated Python module source.
    """
    rows = [
        f'    ("{node.type}", {node.named}, {tuple(node.fields)!r}),'
        for node in sorted(schema.node_types, key=lambda row: row.type)
    ]
    body = "\n".join(rows)
    return (
        f'"""Generated {schema.language} node-type snapshot."""\n\n'
        "from __future__ import annotations\n\n"
        "NODE_TYPES: tuple[tuple[str, bool, tuple[str, ...]], ...] = (\n"
        f"{body}\n"
        ")\n\n"
        '__all__ = ["NODE_TYPES"]\n'
    )


def write_node_types_module(*, schema: GrammarSchemaV1, output_path: Path) -> None:
    """Write a generated node-types module to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_node_types_module(schema), encoding="utf-8")


__all__ = ["render_node_types_module", "write_node_types_module"]
