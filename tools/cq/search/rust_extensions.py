"""Rust-analyzer deep extension plane for CQ enrichment.

This module provides typed structures and stub functions for rust-analyzer
specific extensions (macro expansion, runnables). All functions are capability-gated
and fail-open.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from tools.cq.core.structs import CqStruct

_FAIL_OPEN_EXCEPTIONS = (OSError, RuntimeError, TimeoutError, ValueError, TypeError)


class RustMacroExpansionV1(CqStruct, frozen=True):
    """Result of rust-analyzer/expandMacro request.

    Parameters
    ----------
    name
        Macro name.
    expansion
        Expanded source text.
    expansion_byte_len
        Byte length of the expanded text.
    """

    name: str
    expansion: str
    expansion_byte_len: int = 0


class RustRunnableV1(CqStruct, frozen=True):
    """Normalized runnable from rust-analyzer/runnables request.

    Parameters
    ----------
    label
        Runnable label (e.g., "test my_test").
    kind
        Runnable kind: "cargo", "test", or "bench".

    Args:
        Cargo arguments for the runnable.
    location_uri
        File URI where runnable is defined.
    location_line
        Line number where runnable is defined (0-indexed).
    """

    label: str
    kind: str
    args: tuple[str, ...] = ()
    location_uri: str | None = None
    location_line: int = 0


class _LspRequestFn(Protocol):
    def __call__(
        self,
        method: str,
        params: Mapping[str, object],
        *,
        timeout_seconds: float | None = None,
    ) -> object: ...


def expand_macro(
    session: object,
    uri: str,
    line: int,
    col: int,
) -> RustMacroExpansionV1 | None:
    """Expand macro at position. Fail-open. Rust-analyzer specific.

    Parameters
    ----------
    session
        Rust LSP session (_RustLspSession).
    uri
        Document URI.
    line
        Line position (0-indexed).
    col
        Column position.

    Returns:
    -------
    RustMacroExpansionV1 | None
        Expanded macro, or None if unavailable or not a macro position.
    """
    request = _request_fn(session)
    if request is None:
        return None

    params = {
        "textDocument": {"uri": uri},
        "position": {"line": max(0, line), "character": max(0, col)},
    }

    response = None
    for method in ("rust-analyzer/expandMacro", "experimental/expandMacro"):
        try:
            response = request(method, params)
            break
        except _FAIL_OPEN_EXCEPTIONS:
            continue

    if not isinstance(response, dict):
        return None
    name = response.get("name")
    expansion = response.get("expansion")
    if not isinstance(expansion, str):
        return None
    macro_name = name if isinstance(name, str) and name else "<macro>"
    expansion_bytes = len(expansion.encode("utf-8", errors="replace"))
    return RustMacroExpansionV1(
        name=macro_name, expansion=expansion, expansion_byte_len=expansion_bytes
    )


def get_runnables(
    session: object,
    uri: str,
) -> tuple[RustRunnableV1, ...]:
    """Get runnables for a file. Fail-open. Rust-analyzer specific.

    Parameters
    ----------
    session
        Rust LSP session (_RustLspSession).
    uri
        Document URI.

    Returns:
    -------
    tuple[RustRunnableV1, ...]
        Runnables for the file, or empty tuple if unavailable.
    """
    request = _request_fn(session)
    if request is None:
        return ()

    params = {"textDocument": {"uri": uri}}
    response = None
    for method in ("experimental/runnables", "rust-analyzer/runnables"):
        try:
            response = request(method, params)
            break
        except _FAIL_OPEN_EXCEPTIONS:
            continue

    if not isinstance(response, list):
        return ()

    runnables: list[RustRunnableV1] = []
    for item in response:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_runnable(item)
        if normalized is not None:
            runnables.append(normalized)
    return tuple(runnables)


def _request_fn(session: object) -> _LspRequestFn | None:
    request = getattr(session, "_send_request", None)
    if callable(request):
        return request
    return None


def _normalize_runnable(item: dict[str, object]) -> RustRunnableV1 | None:
    label = item.get("label")
    kind = item.get("kind")
    if not isinstance(label, str) or not isinstance(kind, str):
        return None

    args: tuple[str, ...] = ()
    cargo_args = item.get("args")
    if isinstance(cargo_args, dict):
        cargo = cargo_args.get("cargoArgs")
        if isinstance(cargo, list):
            args = tuple(arg for arg in cargo if isinstance(arg, str))

    location_uri = None
    location_line = 0
    location = item.get("location")
    if isinstance(location, dict):
        target = location.get("target")
        if isinstance(target, dict):
            uri = target.get("uri")
            if isinstance(uri, str):
                location_uri = uri
            range_data = target.get("range")
            if isinstance(range_data, dict):
                start = range_data.get("start")
                if isinstance(start, dict):
                    line_value = start.get("line")
                    if isinstance(line_value, int):
                        location_line = line_value

    return RustRunnableV1(
        label=label,
        kind=kind,
        args=args,
        location_uri=location_uri,
        location_line=location_line,
    )


__all__ = [
    "RustMacroExpansionV1",
    "RustRunnableV1",
    "expand_macro",
    "get_runnables",
]
