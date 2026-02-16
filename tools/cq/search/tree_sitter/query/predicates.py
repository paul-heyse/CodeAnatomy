"""Custom tree-sitter predicate callbacks for CQ query packs."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress

from tools.cq.core.structs import CqStruct

_CAPTURE_KIND_NAMES = frozenset({"capture", "named-capture"})
_LITERAL_KIND = "literal"
_TWO_ARGS = 2
_CUSTOM_PREDICATE_PATTERN = re.compile(r"#\s*cq-[A-Za-z0-9_-]+\?")


class QueryPredicateInputsV1(CqStruct, frozen=True):
    """Normalized callback argument record."""

    predicate_name: str
    args: tuple[tuple[str, str], ...]
    pattern_idx: int


def _to_text(value: object) -> str:
    return str(value)


def _normalize_args(args: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(args, (list, tuple)):
        return ()
    out: list[tuple[str, str]] = []
    for raw_arg in args:
        if (
            isinstance(raw_arg, tuple)
            and len(raw_arg) == _TWO_ARGS
            and isinstance(raw_arg[0], str)
            and isinstance(raw_arg[1], str)
        ):
            first, second = raw_arg
            if first in _CAPTURE_KIND_NAMES:
                out.append((first, second))
            elif second in _CAPTURE_KIND_NAMES:
                out.append((second, first))
            elif first == _LITERAL_KIND:
                out.append((_LITERAL_KIND, second))
            elif second == _LITERAL_KIND:
                out.append((_LITERAL_KIND, first))
            else:
                out.append((_LITERAL_KIND, first))
            continue
        if isinstance(raw_arg, str):
            out.append((_LITERAL_KIND, raw_arg))
            continue
        out.append((_LITERAL_KIND, _to_text(raw_arg)))
    return tuple(out)


def _has_valid_source_slice(*, start: object, end: object, source_bytes: bytes | None) -> bool:
    if source_bytes is None:
        return False
    if not isinstance(start, int) or not isinstance(end, int):
        return False
    if start < 0 or end < start:
        return False
    return end <= len(source_bytes)


def _extract_node_text(
    captures: Mapping[str, Sequence[object]],
    arg: tuple[str, str],
    source_bytes: bytes | None,
) -> str:
    kind, capture_name = arg
    if kind not in _CAPTURE_KIND_NAMES:
        return _to_text(capture_name)

    nodes = captures.get(capture_name)
    if not nodes:
        return ""
    node = nodes[0]
    start = getattr(node, "start_byte", None)
    end = getattr(node, "end_byte", None)
    text = ""
    if _has_valid_source_slice(start=start, end=end, source_bytes=source_bytes):
        if source_bytes is None or not isinstance(start, int) or not isinstance(end, int):
            msg = "Invalid source slice for capture node"
            raise ValueError(msg)
        text = source_bytes[start:end].decode("utf-8", errors="replace")
    else:
        raw_text = getattr(node, "text", None)
        if isinstance(raw_text, str):
            text = raw_text
        elif isinstance(raw_text, (bytes, bytearray, memoryview)):
            text = _decode_raw_text(raw_text)
        elif raw_text is not None:
            text = _to_text(raw_text)
    return text


def _decode_raw_text(raw_text: bytes | bytearray | memoryview) -> str:
    with suppress(UnicodeError, TypeError, AttributeError):
        return bytes(raw_text).decode("utf-8", errors="replace")
    return ""


def _arg_text(args: tuple[tuple[str, str], ...], index: int) -> str:
    if index < len(args):
        return args[index][1]
    return ""


def _regex_predicate(
    normalized: tuple[tuple[str, str], ...],
    captures: Mapping[str, Sequence[object]],
    source_bytes: bytes | None,
) -> bool:
    if len(normalized) != _TWO_ARGS:
        return False
    target = _extract_node_text(captures, normalized[0], source_bytes)
    pattern = _arg_text(normalized, 1)
    with suppress(re.error):
        return re.search(pattern, target) is not None
    return False


def _eq_predicate(
    normalized: tuple[tuple[str, str], ...],
    captures: Mapping[str, Sequence[object]],
    source_bytes: bytes | None,
) -> bool:
    if len(normalized) < _TWO_ARGS:
        return False
    lhs = _extract_node_text(captures, normalized[0], source_bytes)
    rhs = _extract_node_text(captures, normalized[1], source_bytes)
    return lhs == rhs


def _match_predicate(
    normalized: tuple[tuple[str, str], ...],
    captures: Mapping[str, Sequence[object]],
    source_bytes: bytes | None,
) -> bool:
    if len(normalized) < _TWO_ARGS:
        return False
    target = _extract_node_text(captures, normalized[0], source_bytes)
    pattern = _arg_text(normalized, 1)
    with suppress(re.error):
        return re.fullmatch(pattern, target) is not None
    return False


def _any_of_predicate(
    normalized: tuple[tuple[str, str], ...],
    captures: Mapping[str, Sequence[object]],
    source_bytes: bytes | None,
) -> bool:
    if len(normalized) < _TWO_ARGS:
        return False
    target = _extract_node_text(captures, normalized[0], source_bytes)
    candidates = [_arg_text(normalized, index) for index in range(1, len(normalized))]
    return target in candidates


_PREDICATE_HANDLERS: dict[
    str,
    Callable[[tuple[tuple[str, str], ...], Mapping[str, Sequence[object]], bytes | None], bool],
] = {
    "cq-regex?": _regex_predicate,
    "regex?": _regex_predicate,
    "cq-eq?": _eq_predicate,
    "eq?": _eq_predicate,
    "cq-match?": _match_predicate,
    "match?": _match_predicate,
    "cq-any-of?": _any_of_predicate,
    "any-of?": _any_of_predicate,
}


def make_query_predicate(
    *,
    source_bytes: bytes | None = None,
) -> Callable[[str, object, int, Mapping[str, Sequence[object]]], bool]:
    """Build a predicate callback suitable for ``QueryCursor`` invocations.

    Returns:
        Callable[[str, object, int, Mapping[str, Sequence[object]]], bool]:
            Predicate callback used by tree-sitter runtime.
    """

    def _predicate(
        predicate_name: str,
        args: object,
        pattern_idx: int,
        captures: Mapping[str, Sequence[object]],
    ) -> bool:
        normalized = _normalize_args(args)
        _ = QueryPredicateInputsV1(
            predicate_name=predicate_name,
            args=normalized,
            pattern_idx=pattern_idx,
        )
        handler = _PREDICATE_HANDLERS.get(predicate_name)
        if handler is None:
            return True
        return handler(normalized, captures, source_bytes)

    return _predicate


def has_custom_predicates(query_source: str) -> bool:
    """Return whether a query source uses CQ custom predicate clauses."""
    return _CUSTOM_PREDICATE_PATTERN.search(query_source) is not None


__all__ = ["QueryPredicateInputsV1", "has_custom_predicates", "make_query_predicate"]
