"""Static node-type schema loading, runtime id helpers, and code generation."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol, cast

from tools.cq.core.structs import CqStruct


class GrammarNodeTypeV1(CqStruct, frozen=True):
    """One node-type row from `node-types.json`."""

    type: str
    named: bool
    fields: tuple[str, ...] = ()
    is_visible: bool | None = None
    is_supertype: bool | None = None


class GrammarSchemaV1(CqStruct, frozen=True):
    """Simplified grammar schema for lint-time checks."""

    language: str
    node_types: tuple[GrammarNodeTypeV1, ...] = ()
    supertype_index: tuple[SupertypeIndexV1, ...] = ()
    grammar_name: str | None = None
    semantic_version: tuple[int, int, int] | None = None
    abi_version: int | None = None


class SupertypeIndexV1(CqStruct, frozen=True):
    """Supertype-to-subtype mapping derived from runtime language metadata."""

    supertype_id: int
    supertype: str
    subtype_ids: tuple[int, ...] = ()
    subtypes: tuple[str, ...] = ()


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
    from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language

    return load_tree_sitter_language(language)


def _extract_provenance(
    runtime_language_obj: object | None,
) -> tuple[str | None, tuple[int, int, int] | None, int | None]:
    from tools.cq.search.tree_sitter.core.language_registry import normalize_semantic_version

    if runtime_language_obj is None:
        return None, None, None
    grammar_name = getattr(runtime_language_obj, "name", None)
    semantic_version = normalize_semantic_version(
        getattr(runtime_language_obj, "semantic_version", None)
    )
    abi_version_raw = getattr(runtime_language_obj, "abi_version", None)
    abi_version = (
        int(abi_version_raw)
        if isinstance(abi_version_raw, int) and not isinstance(abi_version_raw, bool)
        else None
    )
    return (
        grammar_name if isinstance(grammar_name, str) and grammar_name else None,
        semantic_version,
        abi_version,
    )


def _safe_bool_call(obj: object, method: str, *args: object) -> bool | None:
    fn = getattr(obj, method, None)
    if not callable(fn):
        return None
    try:
        return bool(fn(*args))
    except (TypeError, RuntimeError, ValueError, AttributeError):
        return None


def _kind_name(language_obj: object, kind_id: int) -> str | None:
    node_kind_for_id = getattr(language_obj, "node_kind_for_id", None)
    if not callable(node_kind_for_id):
        return None
    try:
        value = node_kind_for_id(kind_id)
    except (TypeError, RuntimeError, ValueError, AttributeError):
        return None
    return value if isinstance(value, str) and value else None


def _load_supertype_ids(language_obj: object) -> tuple[int, ...]:
    supertypes_attr = getattr(language_obj, "supertypes", None)
    if not isinstance(supertypes_attr, tuple):
        return ()
    ids: list[int] = []
    for value in supertypes_attr:
        if isinstance(value, bool) or not isinstance(value, int):
            continue
        ids.append(int(value))
    return tuple(sorted(set(ids)))


def _load_subtype_ids(language_obj: object, supertype_id: int) -> tuple[int, ...]:
    subtypes_fn = getattr(language_obj, "subtypes", None)
    if not callable(subtypes_fn):
        return ()
    try:
        subtypes = subtypes_fn(supertype_id)
    except (TypeError, RuntimeError, ValueError, AttributeError):
        return ()
    if not isinstance(subtypes, tuple):
        return ()
    ids: list[int] = []
    for value in subtypes:
        if isinstance(value, bool) or not isinstance(value, int):
            continue
        ids.append(int(value))
    return tuple(sorted(set(ids)))


def build_supertype_index(language_obj: object) -> tuple[SupertypeIndexV1, ...]:
    """Build a supertype taxonomy index from runtime language metadata.

    Returns:
        tuple[SupertypeIndexV1, ...]: Function return value.
    """
    rows: list[SupertypeIndexV1] = []
    for supertype_id in _load_supertype_ids(language_obj):
        subtype_ids = _load_subtype_ids(language_obj, supertype_id)
        rows.append(
            SupertypeIndexV1(
                supertype_id=supertype_id,
                supertype=_kind_name(language_obj, supertype_id) or f"id:{supertype_id}",
                subtype_ids=subtype_ids,
                subtypes=tuple(
                    name
                    for subtype_id in subtype_ids
                    if (name := _kind_name(language_obj, subtype_id)) is not None
                ),
            )
        )
    return tuple(rows)


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
                is_visible=_safe_bool_call(runtime_language, "node_kind_is_visible", kind_id),
                is_supertype=_safe_bool_call(runtime_language, "node_kind_is_supertype", kind_id),
            )
        )

    field_names = tuple(
        field_name
        for field_id in range(1, field_count + 1)
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
        runtime_language_obj = _runtime_language(language)
        grammar_name, semantic_version, abi_version = _extract_provenance(runtime_language_obj)
        return GrammarSchemaV1(
            language=language,
            node_types=runtime,
            supertype_index=build_supertype_index(runtime_language_obj)
            if runtime_language_obj is not None
            else (),
            grammar_name=grammar_name,
            semantic_version=semantic_version,
            abi_version=abi_version,
        )
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


# -- Runtime Grammar-ID Helpers (absorbed from runtime.py) --------------------


class RuntimeLanguage(Protocol):
    """Subset of tree-sitter Language APIs used for runtime id lookups."""

    def id_for_node_kind(self, name: str, named: object) -> int | None:
        """Return node-kind id for a given name."""
        ...

    def field_id_for_name(self, name: str) -> int | None:
        """Return field id for a given field name."""
        ...


def build_runtime_ids(language: object) -> dict[str, int]:
    """Build runtime node-kind ids for frequently queried symbols.

    Returns:
        dict[str, int]: Function return value.
    """
    out: dict[str, int] = {}
    named = True
    runtime_language = cast("Any", language)
    for kind in ("identifier", "call", "function_definition"):
        try:
            out[kind] = int(runtime_language.id_for_node_kind(kind, named))
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
    return out


def build_runtime_field_ids(
    language: object,
    *,
    field_names: tuple[str, ...] | None = None,
) -> dict[str, int]:
    """Build runtime field ids for frequently queried field names.

    Returns:
        dict[str, int]: Function return value.
    """
    out: dict[str, int] = {}
    defaults = ("name", "body", "parameters")
    candidates: tuple[str, ...] = field_names if field_names is not None else defaults
    seen: set[str] = set()
    runtime_language = cast("Any", language)
    for field_name in defaults:
        if field_name in seen:
            continue
        seen.add(field_name)
        try:
            out[field_name] = int(runtime_language.field_id_for_name(field_name))
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
    for field_name in candidates:
        if field_name in seen:
            continue
        seen.add(field_name)
        try:
            out[field_name] = int(runtime_language.field_id_for_name(field_name))
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
    return out


# -- Code Generation (absorbed from node_codegen.py) --------------------------


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


__all__ = [
    "GrammarNodeTypeV1",
    "GrammarSchemaIndex",
    "GrammarSchemaV1",
    "RuntimeLanguage",
    "SupertypeIndexV1",
    "build_runtime_field_ids",
    "build_runtime_ids",
    "build_schema_index",
    "build_supertype_index",
    "load_grammar_schema",
    "render_node_types_module",
    "write_node_types_module",
]
