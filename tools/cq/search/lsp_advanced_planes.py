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
_SEMANTIC_TOKENS_KEY = "semantic_tokens"
_INLAY_HINTS_KEY = "inlay_hints"
_DOC_DIAGNOSTICS_KEY = "document_diagnostics"
_WORKSPACE_DIAGNOSTICS_KEY = "workspace_diagnostics"
_RUST_MACRO_KEY = "macro_expansion"
_RUST_RUNNABLES_KEY = "runnables"


def _coerce_result_tuple(
    value: object,
    *,
    default: tuple[object, ...] = (),
) -> tuple[object, ...]:
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        return tuple(value)
    return default


def collect_advanced_lsp_planes(
    *,
    session: object,
    language: str,
    uri: str,
    line: int,
    col: int,
    include_rust_extras: bool = True,
) -> dict[str, object]:
    """Collect bounded advanced-plane payload from an active LSP session.

    Returns:
    -------
    dict[str, object]
        Compact advanced-plane payload suitable for artifact offload.
    """
    start_line = max(0, int(line) - _DEFAULT_TOKEN_WINDOW)
    end_line = max(start_line, int(line) + _DEFAULT_TOKEN_WINDOW)
    callables = {
        _SEMANTIC_TOKENS_KEY: lambda: fetch_semantic_tokens_range(
            session, uri, start_line, end_line
        ),
        _INLAY_HINTS_KEY: lambda: fetch_inlay_hints_range(session, uri, start_line, end_line),
        _DOC_DIAGNOSTICS_KEY: lambda: pull_text_document_diagnostics(session, uri),
        _WORKSPACE_DIAGNOSTICS_KEY: lambda: pull_workspace_diagnostics(session),
    }
    batch = run_lsp_requests(callables, timeout_seconds=1.0)
    semantic_tokens = _coerce_result_tuple(batch.results.get(_SEMANTIC_TOKENS_KEY))
    inlay_hints = _coerce_result_tuple(batch.results.get(_INLAY_HINTS_KEY))
    doc_diagnostics = _coerce_result_tuple(batch.results.get(_DOC_DIAGNOSTICS_KEY))
    workspace_diagnostics = _coerce_result_tuple(batch.results.get(_WORKSPACE_DIAGNOSTICS_KEY))

    payload: dict[str, object] = {
        "semantic_tokens_count": len(semantic_tokens),
        "semantic_tokens_preview": [
            msgspec.to_builtins(item) for item in semantic_tokens[:_MAX_PREVIEW]
        ],
        "inlay_hints_count": len(inlay_hints),
        "inlay_hints_preview": [msgspec.to_builtins(item) for item in inlay_hints[:_MAX_PREVIEW]],
        "document_diagnostics_count": len(doc_diagnostics),
        "document_diagnostics_preview": list(doc_diagnostics[:_MAX_PREVIEW]),
        "workspace_diagnostics_count": len(workspace_diagnostics),
        "workspace_diagnostics_preview": list(workspace_diagnostics[:_MAX_PREVIEW]),
        "plane_timeouts": len(batch.timed_out),
        "plane_timed_out_methods": list(batch.timed_out),
    }

    if language == "rust" and include_rust_extras:
        rust_callables = {
            _RUST_MACRO_KEY: lambda: expand_macro(session, uri, max(0, line), max(0, col)),
            _RUST_RUNNABLES_KEY: lambda: get_runnables(session, uri),
        }
        rust_batch = run_lsp_requests(rust_callables, timeout_seconds=1.0)
        macro = rust_batch.results.get(_RUST_MACRO_KEY)
        runnables = _coerce_result_tuple(rust_batch.results.get(_RUST_RUNNABLES_KEY))
        payload["macro_expansion_available"] = macro is not None
        payload["macro_expansion_preview"] = (
            msgspec.to_builtins(macro) if macro is not None else None
        )
        payload["runnables_count"] = len(runnables)
        payload["runnables_preview"] = [
            msgspec.to_builtins(item) for item in runnables[:_MAX_PREVIEW]
        ]
        payload["rust_plane_timeouts"] = len(rust_batch.timed_out)
        payload["rust_plane_timed_out_methods"] = list(rust_batch.timed_out)
    elif language == "rust":
        payload["macro_expansion_available"] = False
        payload["macro_expansion_preview"] = None
        payload["runnables_count"] = 0
        payload["runnables_preview"] = list[object]()
        payload["rust_plane_timeouts"] = 0
        payload["rust_plane_timed_out_methods"] = list[str]()
        payload["rust_extras_skipped_reason"] = "workspace_not_quiescent"
    return payload


__all__ = ["collect_advanced_lsp_planes"]
