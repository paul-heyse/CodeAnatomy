"""Bounded advanced LSP planes for artifact-first CQ diagnostics."""

from __future__ import annotations

import msgspec

from tools.cq.search.diagnostics_pull import (
    pull_text_document_diagnostics,
    pull_workspace_diagnostics,
)
from tools.cq.search.lsp.request_queue import run_lsp_requests
from tools.cq.search.rust_extensions import expand_macro, get_runnables
from tools.cq.search.semantic_overlays import fetch_inlay_hints_range, fetch_semantic_tokens_range

_DEFAULT_TOKEN_WINDOW = 8
_MAX_PREVIEW = 8
_DOC_DIAGNOSTICS_INDEX = 2
_WORKSPACE_DIAGNOSTICS_INDEX = 3


def collect_advanced_lsp_planes(
    *,
    session: object,
    language: str,
    uri: str,
    line: int,
    col: int,
) -> dict[str, object]:
    """Collect bounded advanced-plane payload from an active LSP session.

    Returns:
    -------
    dict[str, object]
        Compact advanced-plane payload suitable for artifact offload.
    """
    start_line = max(0, int(line) - _DEFAULT_TOKEN_WINDOW)
    end_line = max(start_line, int(line) + _DEFAULT_TOKEN_WINDOW)
    callables = [
        lambda: fetch_semantic_tokens_range(session, uri, start_line, end_line),
        lambda: fetch_inlay_hints_range(session, uri, start_line, end_line),
        lambda: pull_text_document_diagnostics(session, uri),
        lambda: pull_workspace_diagnostics(session),
    ]
    results, timed_out = run_lsp_requests(callables, timeout_seconds=1.0)
    semantic_tokens = results[0] if len(results) > 0 and isinstance(results[0], tuple) else None
    inlay_hints = results[1] if len(results) > 1 and isinstance(results[1], tuple) else None
    doc_diagnostics = (
        results[_DOC_DIAGNOSTICS_INDEX]
        if len(results) > _DOC_DIAGNOSTICS_INDEX
        and isinstance(results[_DOC_DIAGNOSTICS_INDEX], tuple)
        else None
    )
    workspace_diagnostics = (
        results[_WORKSPACE_DIAGNOSTICS_INDEX]
        if len(results) > _WORKSPACE_DIAGNOSTICS_INDEX
        and isinstance(results[_WORKSPACE_DIAGNOSTICS_INDEX], tuple)
        else None
    )

    payload: dict[str, object] = {
        "semantic_tokens_count": len(semantic_tokens or ()),
        "semantic_tokens_preview": [
            msgspec.to_builtins(item) for item in (semantic_tokens or ())[:_MAX_PREVIEW]
        ],
        "inlay_hints_count": len(inlay_hints or ()),
        "inlay_hints_preview": [
            msgspec.to_builtins(item) for item in (inlay_hints or ())[:_MAX_PREVIEW]
        ],
        "document_diagnostics_count": len(doc_diagnostics or ()),
        "document_diagnostics_preview": list((doc_diagnostics or ())[:_MAX_PREVIEW]),
        "workspace_diagnostics_count": len(workspace_diagnostics or ()),
        "workspace_diagnostics_preview": list((workspace_diagnostics or ())[:_MAX_PREVIEW]),
        "plane_timeouts": timed_out,
    }

    if language == "rust":
        rust_callables = [
            lambda: expand_macro(session, uri, max(0, line), max(0, col)),
            lambda: get_runnables(session, uri),
        ]
        rust_results, rust_timed_out = run_lsp_requests(rust_callables, timeout_seconds=1.0)
        macro = rust_results[0] if len(rust_results) > 0 else None
        runnables: tuple[object, ...] = (
            rust_results[1] if len(rust_results) > 1 and isinstance(rust_results[1], tuple) else ()
        )
        payload["macro_expansion_available"] = macro is not None
        payload["macro_expansion_preview"] = (
            msgspec.to_builtins(macro) if macro is not None else None
        )
        payload["runnables_count"] = len(runnables)
        payload["runnables_preview"] = [
            msgspec.to_builtins(item) for item in runnables[:_MAX_PREVIEW]
        ]
        payload["rust_plane_timeouts"] = rust_timed_out
    return payload


__all__ = ["collect_advanced_lsp_planes"]
