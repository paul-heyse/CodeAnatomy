"""Capability gate functions for Pyrefly LSP enrichment.

This module provides capability checking for optional Pyrefly enrichment surfaces.
Gates return DegradeEventV1 when capabilities are unavailable, enabling graceful
degradation in neighborhood assembly.
"""

from __future__ import annotations

from tools.cq.core.snb_schema import DegradeEventV1


def gate_document_symbols(
    server_caps: dict[str, object],
) -> DegradeEventV1 | None:
    """Check if documentSymbol capability is available.

    Parameters
    ----------
    server_caps
        Server capabilities dict from LSP initialization response.

    Returns:
    -------
    DegradeEventV1 | None
        None if capability is available (proceed), DegradeEventV1 if unavailable.
    """
    if not server_caps.get("documentSymbolProvider"):
        return DegradeEventV1(
            stage="lsp.pyrefly",
            severity="info",
            category="unavailable",
            message="documentSymbolProvider not negotiated",
        )
    return None


def gate_workspace_symbols(
    server_caps: dict[str, object],
) -> DegradeEventV1 | None:
    """Check if workspace/symbol capability is available.

    Parameters
    ----------
    server_caps
        Server capabilities dict from LSP initialization response.

    Returns:
    -------
    DegradeEventV1 | None
        None if capability is available (proceed), DegradeEventV1 if unavailable.
    """
    if not server_caps.get("workspaceSymbolProvider"):
        return DegradeEventV1(
            stage="lsp.pyrefly",
            severity="info",
            category="unavailable",
            message="workspaceSymbolProvider not negotiated",
        )
    return None


def gate_semantic_tokens(
    server_caps: dict[str, object],
) -> DegradeEventV1 | None:
    """Check if semantic tokens capability is available.

    Parameters
    ----------
    server_caps
        Server capabilities dict from LSP initialization response.

    Returns:
    -------
    DegradeEventV1 | None
        None if capability is available (proceed), DegradeEventV1 if unavailable.
    """
    semantic_tokens_caps = server_caps.get("semanticTokensProvider")
    if not semantic_tokens_caps:
        return DegradeEventV1(
            stage="lsp.pyrefly",
            severity="info",
            category="unavailable",
            message="semanticTokensProvider not negotiated",
        )

    if not isinstance(semantic_tokens_caps, dict):
        return DegradeEventV1(
            stage="lsp.pyrefly",
            severity="info",
            category="unavailable",
            message="semanticTokensProvider present but not a capability dict",
        )

    if not semantic_tokens_caps.get("full"):
        return DegradeEventV1(
            stage="lsp.pyrefly",
            severity="info",
            category="unavailable",
            message="semanticTokens/full not supported by server",
        )

    return None


__all__ = [
    "gate_document_symbols",
    "gate_semantic_tokens",
    "gate_workspace_symbols",
]
