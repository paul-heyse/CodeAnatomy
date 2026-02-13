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


def supports_method(capabilities: Mapping[str, object], method: str) -> bool:
    """Return whether server capabilities indicate method support."""
    if method == "textDocument/semanticTokens/range":
        tokens = capabilities.get("semanticTokensProvider")
        return isinstance(tokens, Mapping)
    capability_field = _METHOD_TO_CAPABILITY.get(method)
    if capability_field is None:
        return False
    if method == "workspace/diagnostic" and not bool(capabilities.get(capability_field)):
        diagnostic_provider = capabilities.get("diagnosticProvider")
        if isinstance(diagnostic_provider, Mapping):
            workspace = diagnostic_provider.get("workspaceDiagnostics")
            return bool(workspace)
        return False
    if method == "textDocument/diagnostic":
        provider = capabilities.get(capability_field)
        return bool(provider) or isinstance(provider, Mapping)
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
