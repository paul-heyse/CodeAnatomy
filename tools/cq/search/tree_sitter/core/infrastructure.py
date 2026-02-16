"""Language/runtime helpers for tree-sitter execution."""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any, cast

from tools.cq.search.tree_sitter.core.parallel import run_file_lanes_parallel
from tools.cq.search.tree_sitter.core.parser_controls import (
    ParserControlSettingsV1,
    apply_parser_controls,
    parser_controls_from_env,
)
from tools.cq.search.tree_sitter.core.streaming_parse import (
    build_stream_reader,
    parse_streaming_source,
)
from tools.cq.search.tree_sitter.schema.node_schema import (
    build_runtime_field_ids,
    build_runtime_ids,
    load_grammar_schema,
)

if TYPE_CHECKING:
    from tree_sitter import Language, Parser

try:
    from tree_sitter import Parser as _TreeSitterParser
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterParser = None


def load_language(language: str) -> Language:
    """Load one tree-sitter language.

    Raises:
        RuntimeError: If the requested language parser cannot be loaded.

    Returns:
        Language: Loaded tree-sitter language object.
    """
    from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language

    resolved = load_tree_sitter_language(language)
    if resolved is None:
        msg = f"tree-sitter language unavailable: {language}"
        raise RuntimeError(msg)
    return cast("Language", resolved)


@lru_cache(maxsize=4)
def _cached_field_ids(language: str) -> dict[str, int]:
    normalized = language.strip().lower()
    lang_obj = load_language(normalized)
    schema = load_grammar_schema(normalized)
    field_names = (
        tuple(sorted({field for row in schema.node_types for field in row.fields}))
        if schema is not None
        else ()
    )
    return build_runtime_field_ids(lang_obj, field_names=field_names)


@lru_cache(maxsize=4)
def _cached_node_ids(language: str) -> dict[str, int]:
    normalized = language.strip().lower()
    lang_obj = load_language(normalized)
    return build_runtime_ids(lang_obj)


def cached_field_ids(language: str) -> dict[str, int]:
    """Return cached field-id map for one language.

    Returns:
        dict[str, int]: Field-name to field-id mapping for the language.
    """
    return _cached_field_ids(language)


def cached_node_ids(language: str) -> dict[str, int]:
    """Return cached node-kind id map for one language.

    Returns:
        dict[str, int]: Node-kind to node-id mapping for the language.
    """
    return _cached_node_ids(language)


def child_by_field(
    node: Any,
    field_name: str,
    field_ids: dict[str, int],
) -> Any | None:
    """Resolve child by cached field id, falling back to name lookup.

    Returns:
        Any | None: Resolved child node for the requested field, when available.
    """
    field_id = field_ids.get(field_name)
    if field_id is not None:
        by_id = getattr(node, "child_by_field_id", None)
        if callable(by_id):
            try:
                child = by_id(field_id)
            except (TypeError, ValueError, RuntimeError, AttributeError):
                child = None
            if child is not None:
                return child
    by_name = getattr(node, "child_by_field_name", None)
    if callable(by_name):
        try:
            return by_name(field_name)
        except (TypeError, ValueError, RuntimeError, AttributeError):
            return None
    return None


def make_parser(language: str) -> Parser:
    """Construct one parser bound to one language lane.

    Raises:
        RuntimeError: If the tree-sitter parser binding is unavailable.

    Returns:
        Parser: Parser instance configured for the requested language.
    """
    if _TreeSitterParser is None:
        msg = "tree_sitter.Parser is unavailable"
        raise RuntimeError(msg)

    resolved = load_language(language)
    try:
        return _TreeSitterParser(resolved)
    except TypeError:
        parser = _TreeSitterParser()
        parser.language = resolved
        return parser


__all__ = [
    "ParserControlSettingsV1",
    "apply_parser_controls",
    "build_stream_reader",
    "cached_field_ids",
    "cached_node_ids",
    "child_by_field",
    "load_language",
    "make_parser",
    "parse_streaming_source",
    "parser_controls_from_env",
    "run_file_lanes_parallel",
]
