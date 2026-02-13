"""Shared LSP capability helpers for CQ enrichment planes."""

from __future__ import annotations

from collections.abc import Mapping

_METHOD_TO_CAPABILITY: dict[str, str] = {
    "textDocument/inlayHint": "inlayHintProvider",
    "textDocument/diagnostic": "diagnosticProvider",
    "workspace/diagnostic": "workspaceDiagnosticProvider",
    "textDocument/codeAction": "codeActionProvider",
    "textDocument/definition": "definitionProvider",
    "textDocument/typeDefinition": "typeDefinitionProvider",
    "textDocument/implementation": "implementationProvider",
    "textDocument/references": "referencesProvider",
    "textDocument/documentSymbol": "documentSymbolProvider",
    "textDocument/hover": "hoverProvider",
    "textDocument/prepareCallHierarchy": "callHierarchyProvider",
    "typeHierarchy/supertypes": "typeHierarchyProvider",
    "workspace/symbol": "workspaceSymbolProvider",
}


def _supports_semantic_tokens(capabilities: Mapping[str, object], method: str) -> bool:
    tokens = capabilities.get("semanticTokensProvider")
    if isinstance(tokens, Mapping):
        if method == "textDocument/semanticTokens/full":
            return bool(tokens.get("full")) or "full" in tokens
        return bool(tokens.get("range")) or "range" in tokens
    return bool(tokens)


def _supports_workspace_diagnostic(
    capabilities: Mapping[str, object], capability_field: str
) -> bool:
    if bool(capabilities.get(capability_field)):
        return True
    diagnostic_provider = capabilities.get("diagnosticProvider")
    if isinstance(diagnostic_provider, Mapping):
        workspace = diagnostic_provider.get("workspaceDiagnostics")
        return bool(workspace)
    return False


def _supports_text_document_diagnostic(
    capabilities: Mapping[str, object],
    capability_field: str,
) -> bool:
    provider = capabilities.get(capability_field)
    return bool(provider) or isinstance(provider, Mapping)


def supports_method(capabilities: Mapping[str, object], method: str) -> bool:
    """Return whether server capabilities indicate method support."""
    if method in {"textDocument/semanticTokens/range", "textDocument/semanticTokens/full"}:
        return _supports_semantic_tokens(capabilities, method)

    capability_field = _METHOD_TO_CAPABILITY.get(method)
    if capability_field is None:
        return False

    if method == "workspace/diagnostic":
        return _supports_workspace_diagnostic(capabilities, capability_field)

    if method == "textDocument/diagnostic":
        return _supports_text_document_diagnostic(capabilities, capability_field)

    return bool(capabilities.get(capability_field))


def coerce_capabilities(payload: object) -> dict[str, object]:
    """Normalize capability payload to dictionary form.

    Returns:
        Dictionary-form capability payload.
    """
    if isinstance(payload, Mapping):
        return {str(key): value for key, value in payload.items()}
    return {}


__all__ = ["coerce_capabilities", "supports_method"]
