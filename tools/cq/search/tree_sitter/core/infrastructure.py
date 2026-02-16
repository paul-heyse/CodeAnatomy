"""Infrastructure utilities for tree-sitter execution.

This module consolidates parallel execution, streaming source parsing,
parser controls, and language runtime helpers for tree-sitter workloads.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import suppress
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.schema.node_schema import (
    build_runtime_field_ids,
    build_runtime_ids,
    load_grammar_schema,
)

if TYPE_CHECKING:
    from tree_sitter import Language, Parser, Tree

try:
    from tree_sitter import Parser as _TreeSitterParser
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterParser = None


# ── Parallel Execution ──


def run_file_lanes_parallel[T, R](
    jobs: Iterable[T],
    *,
    worker: Callable[[T], R],
    max_workers: int,
) -> list[R]:
    """Execute independent file jobs in parallel and return deterministic results.

    Returns:
    -------
    list[R]
        Results in the same order as input jobs.
    """
    job_list = list(jobs)
    if not job_list:
        return []
    if max_workers <= 1:
        return [worker(job) for job in job_list]

    indexed_results: dict[int, R] = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {executor.submit(worker, job): idx for idx, job in enumerate(job_list)}
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            indexed_results[idx] = future.result()
    return [indexed_results[idx] for idx in range(len(job_list))]


# ── Streaming Source Parsing ──


def build_stream_reader(
    source_bytes: bytes,
    *,
    chunk_size: int = 64 * 1024,
) -> Callable[[int, tuple[int, int]], bytes]:
    """Build a callback-based reader compatible with tree-sitter parser.parse.

    Returns:
    -------
    Callable[[int, tuple[int, int]], bytes]
        Reader callback for parser input.
    """
    safe_chunk_size = max(1, int(chunk_size))
    source_len = len(source_bytes)

    def _reader(byte_offset: int, _point: tuple[int, int]) -> bytes:
        if byte_offset < 0 or byte_offset >= source_len:
            return b""
        end = min(source_len, byte_offset + safe_chunk_size)
        return source_bytes[byte_offset:end]

    return _reader


def parse_streaming_source(
    parser: object,
    source_bytes: bytes,
    *,
    old_tree: Tree | None = None,
    chunk_size: int = 64 * 1024,
) -> Tree | None:
    """Parse via streaming callback with fail-open fallback to bytes parsing.

    Returns:
    -------
    Tree | None
        Parsed tree when parsing succeeds.
    """
    parser_any = cast("Any", parser)
    reader = build_stream_reader(source_bytes, chunk_size=chunk_size)
    try:
        return cast("Tree | None", parser_any.parse(reader, old_tree=old_tree, encoding="utf8"))
    except TypeError:
        pass
    try:
        return cast("Tree | None", parser_any.parse(reader, old_tree=old_tree))
    except TypeError:
        pass
    if old_tree is None:
        return cast("Tree | None", parser_any.parse(source_bytes))
    return cast("Tree | None", parser_any.parse(source_bytes, old_tree=old_tree))


# ── Parser Controls ──


class ParserControlSettingsV1(CqStruct, frozen=True):
    """Parser control-plane settings."""

    reset_before_parse: bool = False
    enable_logger: bool = False
    dot_graph_dir: str | None = None


def parser_controls_from_env() -> ParserControlSettingsV1:
    """Load parser control settings from environment flags.

    Returns:
    -------
    ParserControlSettingsV1
        Resolved parser control options.
    """
    reset_before_parse = os.getenv("CQ_TREE_SITTER_PARSER_RESET", "0") == "1"
    enable_logger = os.getenv("CQ_TREE_SITTER_PARSER_LOGGER", "0") == "1"
    dot_graph_dir = os.getenv("CQ_TREE_SITTER_DOT_GRAPH_DIR")
    if dot_graph_dir:
        try:
            Path(dot_graph_dir).mkdir(parents=True, exist_ok=True)
        except OSError:
            dot_graph_dir = None
    return ParserControlSettingsV1(
        reset_before_parse=reset_before_parse,
        enable_logger=enable_logger,
        dot_graph_dir=dot_graph_dir,
    )


def apply_parser_controls(parser: object, settings: ParserControlSettingsV1) -> None:
    """Apply parser controls in a fail-open manner."""
    parser_any = cast("Any", parser)
    if settings.reset_before_parse and hasattr(parser, "reset"):
        with suppress(RuntimeError, TypeError, ValueError, AttributeError):
            parser_any.reset()

    if settings.enable_logger and hasattr(parser, "logger"):
        with suppress(RuntimeError, TypeError, ValueError, AttributeError):
            # Use a cheap no-op logger to enable runtime tracing hooks without IO.
            parser_any.logger = lambda _msg: None

    if settings.dot_graph_dir and hasattr(parser, "print_dot_graphs"):
        with suppress(RuntimeError, TypeError, ValueError, AttributeError):
            parser_any.print_dot_graphs(str(Path(settings.dot_graph_dir) / "tree_sitter.dot"))


# ── Language Runtime Helpers ──


def load_language(language: str) -> Language:
    """Load one tree-sitter language.

    Args:
        language: Language identifier (for example, ``python`` or ``rust``).

    Returns:
        Language: Loaded tree-sitter language object.

    Raises:
        RuntimeError: If the requested language is unavailable.
    """
    from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language

    resolved = load_tree_sitter_language(language)
    if resolved is None:
        msg = f"tree-sitter language unavailable: {language}"
        raise RuntimeError(msg)
    return cast("Language", resolved)


@lru_cache(maxsize=4)
def _cached_field_ids(language: str) -> dict[str, int]:
    """Load and cache runtime field ids for one language.

    Returns:
        dict[str, int]: Function return value.
    """
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
    """Load and cache runtime node-kind ids for one language.

    Returns:
        dict[str, int]: Function return value.
    """
    normalized = language.strip().lower()
    lang_obj = load_language(normalized)
    return build_runtime_ids(lang_obj)


def cached_field_ids(language: str) -> dict[str, int]:
    """Return cached field-id map for one language."""
    return _cached_field_ids(language)


def cached_node_ids(language: str) -> dict[str, int]:
    """Return cached node-kind id map for one language."""
    return _cached_node_ids(language)


def child_by_field(
    node: Any,
    field_name: str,
    field_ids: dict[str, int],
) -> Any | None:
    """Resolve child by cached field id, falling back to name lookup.

    Returns:
        Any | None: Function return value.
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

    Args:
        language: Language identifier (for example, ``python`` or ``rust``).

    Returns:
        Parser: Parser configured for the requested language.

    Raises:
        RuntimeError: If ``tree_sitter.Parser`` bindings are unavailable.
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
