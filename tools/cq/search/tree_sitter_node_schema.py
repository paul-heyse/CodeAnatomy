"""Static node-type schema loading for tree-sitter query-pack validation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

from tools.cq.core.structs import CqStruct


class GrammarNodeTypeV1(CqStruct, frozen=True):
    """One node-type row from `node-types.json`."""

    type: str
    named: bool
    fields: tuple[str, ...] = ()


class GrammarSchemaV1(CqStruct, frozen=True):
    """Simplified grammar schema for lint-time checks."""

    language: str
    node_types: tuple[GrammarNodeTypeV1, ...] = ()


@dataclass(frozen=True, slots=True)
class GrammarSchemaIndex:
    """Runtime indexes for fast lint lookups."""

    schema: GrammarSchemaV1
    named_node_kinds: frozenset[str]
    all_node_kinds: frozenset[str]
    field_names: frozenset[str]


_LANGUAGE_DISTRIBUTION: dict[str, str] = {
    "python": "tree-sitter-python",
    "rust": "tree-sitter-rust",
}


def _distribution_node_types_path(distribution_name: str) -> Path | None:
    try:
        dist = distribution(distribution_name)
    except PackageNotFoundError:
        return None
    files = list(dist.files or ())
    if not files:
        return None
    preferred: Path | None = None
    fallback: Path | None = None
    for item in files:
        item_text = str(item)
        if not item_text.endswith("node-types.json"):
            continue
        candidate = Path(str(dist.locate_file(item)))
        if item_text.endswith("src/node-types.json"):
            preferred = candidate
            break
        fallback = candidate
    return preferred or fallback


def _decode_node_types(path: Path) -> list[dict[str, object]]:
    raw = path.read_text(encoding="utf-8")
    payload = json.loads(raw)
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _row_to_node_type(row: dict[str, object]) -> GrammarNodeTypeV1 | None:
    node_type = row.get("type")
    named = row.get("named")
    if not isinstance(node_type, str) or not isinstance(named, bool):
        return None
    fields_raw = row.get("fields")
    field_names: tuple[str, ...] = ()
    if isinstance(fields_raw, dict):
        field_names = tuple(sorted(key for key in fields_raw if isinstance(key, str)))
    return GrammarNodeTypeV1(type=node_type, named=named, fields=field_names)


def load_grammar_schema(language: str) -> GrammarSchemaV1 | None:
    """Load grammar schema for a language from installed distribution assets.

    Returns:
    -------
    GrammarSchemaV1 | None
        Simplified schema rows when distribution assets are available.
    """
    distribution_name = _LANGUAGE_DISTRIBUTION.get(language)
    if distribution_name is None:
        return None
    path = _distribution_node_types_path(distribution_name)
    if path is None or not path.exists():
        return None
    rows = _decode_node_types(path)
    node_types = tuple(
        node_type for row in rows if (node_type := _row_to_node_type(row)) is not None
    )
    return GrammarSchemaV1(language=language, node_types=node_types)


def build_schema_index(schema: GrammarSchemaV1) -> GrammarSchemaIndex:
    """Build runtime indexes for node/field lookup checks.

    Returns:
    -------
    GrammarSchemaIndex
        Named/all node-kind and field lookup tables.
    """
    named_node_kinds = frozenset(node.type for node in schema.node_types if node.named)
    all_node_kinds = frozenset(node.type for node in schema.node_types)
    field_names = frozenset(field for node in schema.node_types for field in node.fields)
    return GrammarSchemaIndex(
        schema=schema,
        named_node_kinds=named_node_kinds,
        all_node_kinds=all_node_kinds,
        field_names=field_names,
    )


__all__ = [
    "GrammarNodeTypeV1",
    "GrammarSchemaIndex",
    "GrammarSchemaV1",
    "build_schema_index",
    "load_grammar_schema",
]
