"""Bounded advanced LSP planes for artifact-first CQ diagnostics."""

from __future__ import annotations

import msgspec

from tools.cq.search.diagnostics_pull import (
    pull_text_document_diagnostics,
    pull_workspace_diagnostics,
)
from tools.cq.search.rust_extensions import expand_macro, get_runnables
from tools.cq.search.semantic_overlays import fetch_inlay_hints_range, fetch_semantic_tokens_range

_DEFAULT_TOKEN_WINDOW = 8
_MAX_PREVIEW = 8


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
    semantic_tokens = fetch_semantic_tokens_range(session, uri, start_line, end_line)
    inlay_hints = fetch_inlay_hints_range(session, uri, start_line, end_line)
    doc_diagnostics = pull_text_document_diagnostics(session, uri)
    workspace_diagnostics = pull_workspace_diagnostics(session)

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
    }

    if language == "rust":
        macro = expand_macro(session, uri, max(0, line), max(0, col))
        runnables = get_runnables(session, uri)
        payload["macro_expansion_available"] = macro is not None
        payload["macro_expansion_preview"] = (
            msgspec.to_builtins(macro) if macro is not None else None
        )
        payload["runnables_count"] = len(runnables)
        payload["runnables_preview"] = [
            msgspec.to_builtins(item) for item in runnables[:_MAX_PREVIEW]
        ]
    return payload


__all__ = ["collect_advanced_lsp_planes"]
