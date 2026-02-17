"""Shared helper and boundary functions for search subsystem."""

from __future__ import annotations

from hashlib import blake2b
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.contracts import contract_to_builtins
from tools.cq.core.structs import CqStruct
from tools.cq.core.typed_boundary import convert_lax

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ast_grep_py import SgNode

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


class RuntimeBoundarySummary(CqStruct, frozen=True):
    """Lightweight typed summary for runtime-boundary checks."""

    has_runtime_only_keys: bool = False


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


def sg_node_text(node: SgNode | None) -> str | None:
    """Extract normalized text from an ast-grep node."""
    if node is None:
        return None
    text = node.text().strip()
    return text if text else None


def node_text(
    node: object | None,
    source_bytes: bytes,
    *,
    strip: bool = True,
    max_len: int | None = None,
) -> str:
    """Extract UTF-8 text from a node's byte span."""
    from tools.cq.search.tree_sitter.core.node_utils import node_text as _node_text

    return _node_text(node, source_bytes, strip=strip, max_len=max_len)


def convert_from_attributes(obj: object, *, type_: object) -> object:
    """Convert runtime objects to target type using attribute access."""
    return convert_lax(obj, type_=type_, from_attributes=True)


def to_mapping_payload(value: object) -> dict[str, object]:
    """Convert contract value into a builtins mapping payload."""
    payload = contract_to_builtins(value)
    if isinstance(payload, dict):
        return payload
    msg = f"Expected mapping payload, got {type(payload).__name__}"
    raise TypeError(msg)


def has_runtime_only_keys(payload: Mapping[str, object]) -> bool:
    """Return whether payload appears to contain runtime-only keys."""
    return any(key in _RUNTIME_ONLY_ATTR_NAMES for key in payload)


def assert_no_runtime_only_keys(payload: Mapping[str, object]) -> None:
    """Raise when runtime-only keys leak into serializable payloads."""
    if not has_runtime_only_keys(payload):
        return
    leaked = sorted(key for key in payload if key in _RUNTIME_ONLY_ATTR_NAMES)
    msg = f"Runtime-only keys are not serializable: {', '.join(leaked)}"
    raise TypeError(msg)


__all__ = [
    "RuntimeBoundarySummary",
    "assert_no_runtime_only_keys",
    "convert_from_attributes",
    "decode_mapping",
    "encode_mapping",
    "has_runtime_only_keys",
    "line_col_to_byte_offset",
    "node_text",
    "sg_node_text",
    "source_hash",
    "to_mapping_payload",
    "truncate",
]
