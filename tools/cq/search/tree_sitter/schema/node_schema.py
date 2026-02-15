"""Static node-type schema loading for tree-sitter query-pack validation."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any, cast

from tools.cq.core.structs import CqStruct

try:
    from tree_sitter import Language as _TreeSitterLanguage
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterLanguage = None

try:
    import tree_sitter_python as _tree_sitter_python
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_python = None

try:
    import tree_sitter_rust as _tree_sitter_rust
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_rust = None


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


_GENERATED_MODULES: dict[str, str] = {
    "python": "tools.cq.search.generated.python_node_types_v1",
    "rust": "tools.cq.search.generated.rust_node_types_v1",
}
_NODE_TYPE_ROW_LENGTH = 3
_RUNTIME_FIELD_REGISTRY_NODE = "__field_registry__"


def _load_generated_node_types(language: str) -> tuple[GrammarNodeTypeV1, ...]:
    module_name = _GENERATED_MODULES.get(language)
    if module_name is None:
        return ()
    try:
        module = import_module(module_name)
    except (ImportError, RuntimeError, TypeError, ValueError):
        return ()
    rows = getattr(module, "NODE_TYPES", ())
    if not isinstance(rows, tuple):
        return ()
    out: list[GrammarNodeTypeV1] = []
    for row in rows:
        if not isinstance(row, tuple) or len(row) != _NODE_TYPE_ROW_LENGTH:
            continue
        node_type, named, fields = row
        if not isinstance(node_type, str) or not isinstance(named, bool):
            continue
        if not isinstance(fields, tuple):
            continue
        normalized_fields = tuple(value for value in fields if isinstance(value, str))
        out.append(GrammarNodeTypeV1(type=node_type, named=named, fields=normalized_fields))
    return tuple(out)


def _runtime_language(language: str) -> object | None:
    if _TreeSitterLanguage is None:
        return None
    normalized = language.strip().lower()
    if normalized == "python" and _tree_sitter_python is not None:
        return _TreeSitterLanguage(_tree_sitter_python.language())
    if normalized == "rust" and _tree_sitter_rust is not None:
        return _TreeSitterLanguage(_tree_sitter_rust.language())
    return None


def _load_runtime_node_types(language: str) -> tuple[GrammarNodeTypeV1, ...]:
    runtime_language_obj = _runtime_language(language)
    if runtime_language_obj is None:
        return ()
    runtime_language = cast("Any", runtime_language_obj)
    node_kind_count = int(getattr(runtime_language, "node_kind_count", 0))
    field_count = int(getattr(runtime_language, "field_count", 0))
    if node_kind_count <= 0:
        return ()

    rows: list[GrammarNodeTypeV1] = []
    for kind_id in range(node_kind_count):
        node_kind = runtime_language.node_kind_for_id(kind_id)
        if not isinstance(node_kind, str) or not node_kind:
            continue
        rows.append(
            GrammarNodeTypeV1(
                type=node_kind,
                named=bool(runtime_language.node_kind_is_named(kind_id)),
                fields=(),
            )
        )

    field_names = tuple(
        field_name
        for field_id in range(field_count)
        if isinstance((field_name := runtime_language.field_name_for_id(field_id)), str)
        and field_name
    )
    if field_names:
        rows.append(
            GrammarNodeTypeV1(
                type=_RUNTIME_FIELD_REGISTRY_NODE,
                named=False,
                fields=field_names,
            )
        )
    return tuple(rows)


def load_grammar_schema(language: str) -> GrammarSchemaV1 | None:
    """Load grammar schema for a language from generated node-type modules.

    Returns:
    -------
    GrammarSchemaV1 | None
        Simplified schema rows when generated modules are available.
    """
    runtime = _load_runtime_node_types(language)
    if runtime:
        return GrammarSchemaV1(language=language, node_types=runtime)
    generated = _load_generated_node_types(language)
    if generated:
        return GrammarSchemaV1(language=language, node_types=generated)
    return None


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
