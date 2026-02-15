"""Optional rust-analyzer macro expansion bridge."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

from tools.cq.search.rust.macro_expansion_contracts import (
    RustMacroExpansionRequestV1,
    RustMacroExpansionResultV1,
)


def _to_document_uri(file_path: str) -> str:
    path = Path(file_path)
    if path.is_absolute():
        return f"file://{quote(path.as_posix())}"
    return f"file://{quote(path.resolve().as_posix())}"


def expand_macro(
    client: object, request: RustMacroExpansionRequestV1
) -> RustMacroExpansionResultV1:
    """Request macro expansion from rust-analyzer when client supports ``request``."""
    request_fn = getattr(client, "request", None)
    if not callable(request_fn):
        return RustMacroExpansionResultV1(macro_call_id=request.macro_call_id)

    payload = {
        "textDocument": {"uri": _to_document_uri(request.file_path)},
        "position": {
            "line": max(0, int(request.line)),
            "character": max(0, int(request.col)),
        },
    }
    try:
        response = request_fn("rust-analyzer/expandMacro", payload)
    except (RuntimeError, TypeError, ValueError, AttributeError, OSError):
        return RustMacroExpansionResultV1(macro_call_id=request.macro_call_id)

    if isinstance(response, dict) and isinstance(response.get("result"), dict):
        result = response.get("result")
    elif isinstance(response, dict):
        result = response
    else:
        result = None

    if not isinstance(result, dict):
        return RustMacroExpansionResultV1(macro_call_id=request.macro_call_id)

    name = result.get("name")
    expansion = result.get("expansion")
    return RustMacroExpansionResultV1(
        macro_call_id=request.macro_call_id,
        name=name if isinstance(name, str) else None,
        expansion=expansion if isinstance(expansion, str) else None,
        applied=True,
    )


def expand_macros(
    *,
    client: object,
    requests: tuple[RustMacroExpansionRequestV1, ...],
) -> tuple[RustMacroExpansionResultV1, ...]:
    """Expand a batch of macro requests with fail-open behavior."""
    return tuple(expand_macro(client, request) for request in requests)


__all__ = ["expand_macro", "expand_macros"]
