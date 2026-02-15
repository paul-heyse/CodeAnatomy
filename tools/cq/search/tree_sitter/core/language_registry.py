"""Language introspection registry for tree-sitter schema metadata."""

from __future__ import annotations

from functools import lru_cache

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
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterLanguage = None


class TreeSitterLanguageRegistryV1(CqStruct, frozen=True):
    """Normalized grammar metadata for one language lane."""

    language: str
    node_kinds: tuple[str, ...] = ()
    named_node_kinds: tuple[str, ...] = ()
    field_names: tuple[str, ...] = ()
    supertypes: tuple[str, ...] = ()


@lru_cache(maxsize=8)
def load_language_registry(language: str) -> TreeSitterLanguageRegistryV1 | None:
    """Load normalized introspection metadata for a language.

    Returns:
        TreeSitterLanguageRegistryV1 | None: Registry row when schema is available.
    """
    schema = load_grammar_schema(language)
    if schema is None:
        return None
    node_kinds = tuple(sorted({row.type for row in schema.node_types}))
    named_node_kinds = tuple(sorted({row.type for row in schema.node_types if row.named}))
    field_names = tuple(sorted({field for row in schema.node_types for field in row.fields}))
    return TreeSitterLanguageRegistryV1(
        language=language,
        node_kinds=node_kinds,
        named_node_kinds=named_node_kinds,
        field_names=field_names,
        supertypes=(),
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
        return _TreeSitterLanguage(_tree_sitter_python.language())
    if normalized == "rust" and _tree_sitter_rust is not None:
        return _TreeSitterLanguage(_tree_sitter_rust.language())
    return None


__all__ = ["TreeSitterLanguageRegistryV1", "load_language_registry", "load_tree_sitter_language"]
