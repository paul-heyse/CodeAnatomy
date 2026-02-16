"""Consolidated shared helpers, contracts, runtime handles, requests, and timeout utilities."""

from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from hashlib import blake2b
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.core.structs import CqSettingsStruct, CqStruct
from tools.cq.core.typed_boundary import convert_lax
from tools.cq.query.language import QueryLanguage

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from ast_grep_py import SgNode
    from tree_sitter import Node

    from tools.cq.search.pipeline.classifier import QueryMode
    from tools.cq.search.pipeline.profiles import SearchLimits


# Shared core helpers
_RUNTIME_ONLY_ATTR_NAMES: frozenset[str] = frozenset(
    {
        "ast",
        "ast_tree",
        "entry",
        "tree",
        "sg_root",
        "node",
        "resolved_node",
        "parser",
        "wrapper",
        "session",
        "cache",
    }
)

_JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
_JSON_DECODER = msgspec.json.Decoder(type=dict[str, object])


def line_col_to_byte_offset(source_bytes: bytes, line: int, col: int) -> int | None:
    """Convert 1-indexed line and 0-indexed char column to byte offset."""
    if line < 1 or col < 0:
        return None
    lines = source_bytes.splitlines(keepends=True)
    if line > len(lines):
        return None
    prefix = b"".join(lines[: line - 1])
    line_bytes = lines[line - 1]
    line_text = line_bytes.decode("utf-8", errors="replace")
    char_col = min(col, len(line_text))
    byte_col = len(line_text[:char_col].encode("utf-8", errors="replace"))
    return len(prefix) + byte_col


def encode_mapping(payload: dict[str, object]) -> bytes:
    """Encode mapping payload with deterministic JSON ordering."""
    return _JSON_ENCODER.encode(payload)


def decode_mapping(payload: bytes) -> dict[str, object]:
    """Decode mapping payload using reusable typed decoder."""
    return _JSON_DECODER.decode(payload)


def source_hash(source_bytes: bytes) -> str:
    """Return stable 128-bit BLAKE2 hash of source bytes."""
    return blake2b(source_bytes, digest_size=16).hexdigest()


def truncate(text: str, max_len: int) -> str:
    """Return text truncated to max_len with ellipsis when needed."""
    if len(text) <= max_len:
        return text
    return text[: max(1, max_len - 3)] + "..."


def node_text(node: Node, source_bytes: bytes, *, strip: bool = True) -> str:
    """Extract UTF-8 text for a tree-sitter node range."""
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    text = source_bytes[start:end].decode("utf-8", errors="replace")
    return text.strip() if strip else text


def sg_node_text(node: SgNode | None) -> str | None:
    """Extract normalized text from an ast-grep node."""
    if node is None:
        return None
    text = node.text().strip()
    return text if text else None


def convert_from_attributes(obj: object, *, type_: object) -> object:
    """Convert runtime objects to target type using attribute access."""
    return convert_lax(obj, type_=type_, from_attributes=True)


class RuntimeBoundarySummary(CqStruct, frozen=True):
    """Lightweight typed summary for runtime-boundary checks."""

    has_runtime_only_keys: bool = False


def to_mapping_payload(value: object) -> dict[str, object]:
    """Convert contract value into a builtins mapping payload.

    Raises:
        TypeError: If the converted payload is not a mapping.
    """
    payload = contract_to_builtins(value)
    if isinstance(payload, dict):
        return payload
    msg = f"Expected mapping payload, got {type(payload).__name__}"
    raise TypeError(msg)


def has_runtime_only_keys(payload: Mapping[str, object]) -> bool:
    """Return whether payload appears to contain runtime-only keys."""
    return any(key in _RUNTIME_ONLY_ATTR_NAMES for key in payload)


def assert_no_runtime_only_keys(payload: Mapping[str, object]) -> None:
    """Raise when runtime-only keys leak into serializable payloads.

    Raises:
        TypeError: If runtime-only fields are present.
    """
    if not has_runtime_only_keys(payload):
        return
    leaked = sorted(key for key in payload if key in _RUNTIME_ONLY_ATTR_NAMES)
    msg = f"Runtime-only keys are not serializable: {', '.join(leaked)}"
    raise TypeError(msg)


# Shared serializable contracts
class PythonNodeEnrichmentSettingsV1(CqSettingsStruct, frozen=True):
    """Serializable settings for node-anchored Python enrichment."""

    source_bytes: bytes
    line: int
    col: int
    cache_key: str
    byte_start: int | None = None
    byte_end: int | None = None
    query_budget_ms: int | None = None


class PythonByteRangeEnrichmentSettingsV1(CqSettingsStruct, frozen=True):
    """Serializable settings for byte-range Python enrichment."""

    source_bytes: bytes
    byte_start: int
    byte_end: int
    cache_key: str
    resolved_line: int | None = None
    resolved_col: int | None = None
    query_budget_ms: int | None = None


# Runtime-only handle containers
@dataclass(frozen=True, slots=True)
class PythonNodeRuntimeV1:
    """Runtime-only handles for node-anchored Python enrichment."""

    sg_root: object
    node: object
    session: object | None = None


@dataclass(frozen=True, slots=True)
class PythonByteRangeRuntimeV1:
    """Runtime-only handles for byte-range Python enrichment."""

    sg_root: object
    resolved_node: object | None = None
    session: object | None = None


# Shared requests
class RgRunRequest(CqStruct, frozen=True):
    """Input contract for native ripgrep execution."""

    root: Path
    pattern: str
    mode: QueryMode
    lang_types: tuple[str, ...]
    limits: SearchLimits
    include_globs: list[str] = msgspec.field(default_factory=list)
    exclude_globs: list[str] = msgspec.field(default_factory=list)
    operation: str = "json"
    paths: tuple[str, ...] = (".",)
    extra_patterns: tuple[str, ...] = ()

    def to_settings(self) -> object:
        """Return serializable rg execution settings contract."""
        from tools.cq.search.rg.contracts import RgRunSettingsV1

        return RgRunSettingsV1(
            pattern=self.pattern,
            mode=self.mode.value,
            lang_types=self.lang_types,
            include_globs=tuple(self.include_globs),
            exclude_globs=tuple(self.exclude_globs),
            operation=self.operation,
            paths=self.paths,
            extra_patterns=self.extra_patterns,
        )


class CandidateCollectionRequest(CqStruct, frozen=True):
    """Input contract for raw candidate collection."""

    root: Path
    pattern: str
    mode: QueryMode
    limits: SearchLimits
    lang: QueryLanguage
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    pcre2_available: bool = False


class PythonNodeEnrichmentRequest(CqStruct, frozen=True):
    """Input contract for node-anchored Python enrichment."""

    sg_root: object
    node: object
    source_bytes: bytes
    line: int
    col: int
    cache_key: str
    byte_start: int | None = None
    byte_end: int | None = None
    query_budget_ms: int | None = None
    session: object | None = None

    def to_settings(self) -> PythonNodeEnrichmentSettingsV1:
        """Return serializable settings subset for transport/cache boundaries."""
        return PythonNodeEnrichmentSettingsV1(
            source_bytes=self.source_bytes,
            line=self.line,
            col=self.col,
            cache_key=self.cache_key,
            byte_start=self.byte_start,
            byte_end=self.byte_end,
            query_budget_ms=self.query_budget_ms,
        )

    def to_runtime(self) -> PythonNodeRuntimeV1:
        """Return runtime-only handle container."""
        return PythonNodeRuntimeV1(sg_root=self.sg_root, node=self.node, session=self.session)


class PythonByteRangeEnrichmentRequest(CqStruct, frozen=True):
    """Input contract for byte-range anchored Python enrichment."""

    sg_root: object
    source_bytes: bytes
    byte_start: int
    byte_end: int
    cache_key: str
    resolved_node: object | None = None
    resolved_line: int | None = None
    resolved_col: int | None = None
    query_budget_ms: int | None = None
    session: object | None = None

    def to_settings(self) -> PythonByteRangeEnrichmentSettingsV1:
        """Return serializable settings subset for transport/cache boundaries."""
        return PythonByteRangeEnrichmentSettingsV1(
            source_bytes=self.source_bytes,
            byte_start=self.byte_start,
            byte_end=self.byte_end,
            cache_key=self.cache_key,
            resolved_line=self.resolved_line,
            resolved_col=self.resolved_col,
            query_budget_ms=self.query_budget_ms,
        )

    def to_runtime(self) -> PythonByteRangeRuntimeV1:
        """Return runtime-only handle container."""
        return PythonByteRangeRuntimeV1(
            sg_root=self.sg_root,
            resolved_node=self.resolved_node,
            session=self.session,
        )


class RustEnrichmentRequest(CqStruct, frozen=True):
    """Input contract for byte-range anchored Rust enrichment."""

    source: str
    byte_start: int
    byte_end: int
    cache_key: str | None = None
    max_scope_depth: int = 24
    query_budget_ms: int | None = None


# Timeout wrappers


def search_sync_with_timeout[T](
    fn: Callable[..., T],
    timeout: float,
    *,
    args: tuple[object, ...] | None = None,
    kwargs: dict[str, object] | None = None,
) -> T:
    """Execute a synchronous function with a timeout.

    Raises:
        ValueError: If ``timeout`` is negative.
        TimeoutError: If execution exceeds the timeout.
    """
    if timeout < 0:
        msg = "Timeout must be positive"
        raise ValueError(msg)

    args = args or ()
    kwargs = kwargs or {}

    scheduler = get_worker_scheduler()
    future = scheduler.submit_io(fn, *args, **kwargs)
    try:
        return future.result(timeout=timeout if timeout > 0 else None)
    except FuturesTimeoutError as e:
        msg = f"Search operation timed out after {timeout:.1f} seconds"
        raise TimeoutError(msg) from e


async def search_async_with_timeout[T](coro: Awaitable[T], timeout_seconds: float) -> T:
    """Execute an awaitable with a timeout.

    Raises:
        ValueError: If ``timeout_seconds`` is negative.
        TimeoutError: If execution exceeds the timeout.
    """
    if timeout_seconds < 0:
        msg = "Timeout must be positive"
        raise ValueError(msg)

    try:
        return await asyncio.wait_for(
            coro,
            timeout=timeout_seconds if timeout_seconds > 0 else None,
        )
    except TimeoutError as e:
        msg = f"Async search operation timed out after {timeout_seconds:.1f} seconds"
        raise TimeoutError(msg) from e


__all__ = [
    "CandidateCollectionRequest",
    "PythonByteRangeEnrichmentRequest",
    "PythonByteRangeEnrichmentSettingsV1",
    "PythonByteRangeRuntimeV1",
    "PythonNodeEnrichmentRequest",
    "PythonNodeEnrichmentSettingsV1",
    "PythonNodeRuntimeV1",
    "RgRunRequest",
    "RuntimeBoundarySummary",
    "RustEnrichmentRequest",
    "assert_no_runtime_only_keys",
    "convert_from_attributes",
    "decode_mapping",
    "encode_mapping",
    "has_runtime_only_keys",
    "line_col_to_byte_offset",
    "node_text",
    "search_async_with_timeout",
    "search_sync_with_timeout",
    "sg_node_text",
    "source_hash",
    "to_mapping_payload",
    "truncate",
]
