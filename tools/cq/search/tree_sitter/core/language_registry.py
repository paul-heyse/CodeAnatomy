"""Language introspection registry for tree-sitter schema metadata."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, cast

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.schema.node_schema import load_grammar_schema

try:
    import tree_sitter_python as _tree_sitter_python
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_python = None

try:
    import tree_sitter_rust as _tree_sitter_rust
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_rust = None

try:
    from tree_sitter import Language as _TreeSitterLanguage
    from tree_sitter import Parser as _TreeSitterParser
    from tree_sitter import QueryCursor as _TreeSitterQueryCursor
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterLanguage = None
    _TreeSitterParser = None
    _TreeSitterQueryCursor = None

try:
    import tree_sitter as _ts
except ImportError:  # pragma: no cover - optional dependency
    _ts = None


class TreeSitterLanguageRegistryV1(CqStruct, frozen=True):
    """Normalized grammar metadata for one language lane."""

    language: str
    node_kinds: tuple[str, ...] = ()
    named_node_kinds: tuple[str, ...] = ()
    field_names: tuple[str, ...] = ()
    supertypes: tuple[str, ...] = ()
    grammar_name: str | None = None
    semantic_version: tuple[int, int, int] | None = None
    abi_version: int | None = None


class TreeSitterRuntimeCapabilitiesV1(CqStruct, frozen=True):
    """Capability snapshot for runtime tree-sitter lane integration."""

    language: str
    has_cursor_copy: bool = False
    has_cursor_reset: bool = False
    has_cursor_reset_to: bool = False
    has_goto_first_child_for_byte: bool = False
    has_query_cursor_containing_byte_range: bool = False
    has_query_cursor_containing_point_range: bool = False


_SEMANTIC_VERSION_PARTS = 3


def normalize_semantic_version(value: object) -> tuple[int, int, int] | None:
    if not isinstance(value, tuple) or len(value) != _SEMANTIC_VERSION_PARTS:
        return None
    if not all(isinstance(part, int) and not isinstance(part, bool) for part in value):
        return None
    major, minor, patch = value
    return (int(major), int(minor), int(patch))


def _normalize_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return int(value)


def _extract_provenance(
    language_obj: object | None,
) -> tuple[str | None, tuple[int, int, int] | None, int | None]:
    if language_obj is None:
        return None, None, None
    grammar_name = getattr(language_obj, "name", None)
    semantic_version = getattr(language_obj, "semantic_version", None)
    abi_version = getattr(language_obj, "abi_version", None)
    return (
        grammar_name if isinstance(grammar_name, str) and grammar_name else None,
        normalize_semantic_version(semantic_version),
        _normalize_int(abi_version),
    )


def _assert_abi_compatible(language_obj: object, language_name: str) -> None:
    """Assert grammar ABI version is compatible with runtime bindings.

    Raises:
        RuntimeError: If grammar ABI is outside runtime-compatible ABI range.
    """
    if _ts is None:
        return
    ts_max = _normalize_int(getattr(_ts, "LANGUAGE_VERSION", None))
    ts_min = _normalize_int(getattr(_ts, "MIN_COMPATIBLE_LANGUAGE_VERSION", None))
    abi = _normalize_int(getattr(language_obj, "abi_version", None))
    if ts_max is None or ts_min is None or abi is None:
        return
    if ts_min <= abi <= ts_max:
        return
    msg = (
        f"tree-sitter ABI mismatch for {language_name}: "
        f"grammar abi_version={abi}, runtime accepts [{ts_min}, {ts_max}]"
    )
    raise RuntimeError(msg)


def _load_supertype_names(language_obj: object | None) -> tuple[str, ...]:
    if language_obj is None:
        return ()
    supertypes_value = getattr(language_obj, "supertypes", None)
    node_kind_for_id = getattr(language_obj, "node_kind_for_id", None)
    if not isinstance(supertypes_value, tuple) or not callable(node_kind_for_id):
        return ()
    names: list[str] = []
    for supertype_id in supertypes_value:
        if isinstance(supertype_id, bool) or not isinstance(supertype_id, int):
            continue
        try:
            name = node_kind_for_id(int(supertype_id))
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
        if isinstance(name, str) and name:
            names.append(name)
    return tuple(sorted(set(names)))


def _parser_for_language(language_obj: object) -> Any | None:
    if _TreeSitterParser is None:
        return None
    try:
        return _TreeSitterParser(cast("Any", language_obj))
    except TypeError:
        try:
            parser = _TreeSitterParser()
            parser.language = cast("Any", language_obj)
        except (RuntimeError, TypeError, ValueError, AttributeError):
            return None
        return parser
    except (RuntimeError, ValueError, AttributeError):
        return None


def _cursor_capabilities(language_obj: object) -> tuple[bool, bool, bool, bool]:
    parser = _parser_for_language(language_obj)
    if parser is None:
        return False, False, False, False
    try:
        tree = parser.parse(b"x = 1")
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return False, False, False, False
    root = getattr(tree, "root_node", None) if tree is not None else None
    if root is None:
        return False, False, False, False
    try:
        cursor = root.walk()
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return False, False, False, False
    return (
        callable(getattr(cursor, "copy", None)),
        callable(getattr(cursor, "reset", None)),
        callable(getattr(cursor, "reset_to", None)),
        callable(getattr(cursor, "goto_first_child_for_byte", None)),
    )


def _query_cursor_containing_capabilities() -> tuple[bool, bool]:
    if _TreeSitterQueryCursor is None:
        return False, False
    return (
        hasattr(_TreeSitterQueryCursor, "set_containing_byte_range"),
        hasattr(_TreeSitterQueryCursor, "set_containing_point_range"),
    )


@lru_cache(maxsize=8)
def load_language_registry(language: str) -> TreeSitterLanguageRegistryV1 | None:
    """Load normalized introspection metadata for a language.

    Returns:
        TreeSitterLanguageRegistryV1 | None: Registry row when schema is available.
    """
    normalized = language.strip().lower()
    schema = load_grammar_schema(normalized)
    if schema is None:
        return None
    language_obj = load_tree_sitter_language(normalized)
    grammar_name, semantic_version, abi_version = _extract_provenance(language_obj)
    node_kinds = tuple(sorted({row.type for row in schema.node_types}))
    named_node_kinds = tuple(sorted({row.type for row in schema.node_types if row.named}))
    field_names = tuple(sorted({field for row in schema.node_types for field in row.fields}))
    supertypes = (
        tuple(sorted({row.supertype for row in schema.supertype_index}))
        if schema.supertype_index
        else _load_supertype_names(language_obj)
    )
    return TreeSitterLanguageRegistryV1(
        language=normalized,
        node_kinds=node_kinds,
        named_node_kinds=named_node_kinds,
        field_names=field_names,
        supertypes=supertypes,
        grammar_name=grammar_name,
        semantic_version=semantic_version,
        abi_version=abi_version,
    )


@lru_cache(maxsize=8)
def load_tree_sitter_language(language: str) -> object | None:
    """Load runtime tree-sitter Language object for one lane.

    Returns:
        object | None: Runtime language object when available.
    """
    if _TreeSitterLanguage is None:
        return None
    normalized = language.strip().lower()
    if normalized == "python" and _tree_sitter_python is not None:
        language_obj = _TreeSitterLanguage(_tree_sitter_python.language())
        _assert_abi_compatible(language_obj, "python")
        return language_obj
    if normalized == "rust" and _tree_sitter_rust is not None:
        language_obj = _TreeSitterLanguage(_tree_sitter_rust.language())
        _assert_abi_compatible(language_obj, "rust")
        return language_obj
    return None


@lru_cache(maxsize=8)
def load_tree_sitter_capabilities(language: str) -> TreeSitterRuntimeCapabilitiesV1:
    """Load runtime capability flags for tree-sitter APIs used by CQ.

    Returns:
        TreeSitterRuntimeCapabilitiesV1: Function return value.
    """
    normalized = language.strip().lower()
    language_obj = load_tree_sitter_language(normalized)
    if language_obj is None:
        return TreeSitterRuntimeCapabilitiesV1(language=normalized)
    (
        has_cursor_copy,
        has_cursor_reset,
        has_cursor_reset_to,
        has_goto_first_child_for_byte,
    ) = _cursor_capabilities(language_obj)
    (
        has_query_cursor_containing_byte_range,
        has_query_cursor_containing_point_range,
    ) = _query_cursor_containing_capabilities()

    return TreeSitterRuntimeCapabilitiesV1(
        language=normalized,
        has_cursor_copy=has_cursor_copy,
        has_cursor_reset=has_cursor_reset,
        has_cursor_reset_to=has_cursor_reset_to,
        has_goto_first_child_for_byte=has_goto_first_child_for_byte,
        has_query_cursor_containing_byte_range=has_query_cursor_containing_byte_range,
        has_query_cursor_containing_point_range=has_query_cursor_containing_point_range,
    )


__all__ = [
    "TreeSitterLanguageRegistryV1",
    "TreeSitterRuntimeCapabilitiesV1",
    "load_language_registry",
    "load_tree_sitter_capabilities",
    "load_tree_sitter_language",
    "normalize_semantic_version",
]
