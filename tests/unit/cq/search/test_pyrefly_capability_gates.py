"""Tests for Pyrefly capability gate functions."""

from __future__ import annotations

from tools.cq.core.snb_schema import DegradeEventV1
from tools.cq.search.pyrefly_capability_gates import (
    gate_document_symbols,
    gate_semantic_tokens,
    gate_workspace_symbols,
)

_EMPTY_TOKEN_TYPES: list[str] = []
_EMPTY_TOKEN_MODIFIERS: list[str] = []
_EMPTY_LEGEND: dict[str, list[str]] = {
    "tokenTypes": _EMPTY_TOKEN_TYPES,
    "tokenModifiers": _EMPTY_TOKEN_MODIFIERS,
}
_EMPTY_DICT: dict[str, object] = {}


class TestDocumentSymbolsGate:
    """Test documentSymbol capability gating."""

    def test_capability_present_returns_none(self) -> None:
        """When documentSymbolProvider is present, gate returns None (proceed)."""
        server_caps = {"documentSymbolProvider": True}
        result = gate_document_symbols(server_caps)
        assert result is None

    def test_capability_missing_returns_degrade_event(self) -> None:
        """When documentSymbolProvider is missing, gate returns DegradeEventV1."""
        server_caps: dict[str, object] = {}
        result = gate_document_symbols(server_caps)
        assert isinstance(result, DegradeEventV1)
        assert result.stage == "lsp.pyrefly"
        assert result.severity == "info"
        assert result.category == "unavailable"
        assert "documentSymbolProvider" in result.message

    def test_capability_false_returns_degrade_event(self) -> None:
        """When documentSymbolProvider is explicitly False, gate returns DegradeEventV1."""
        server_caps = {"documentSymbolProvider": False}
        result = gate_document_symbols(server_caps)
        assert isinstance(result, DegradeEventV1)
        assert result.category == "unavailable"

    def test_capability_present_as_dict_returns_none(self) -> None:
        """When documentSymbolProvider is a dict (capabilities object), gate returns None."""
        server_caps = {"documentSymbolProvider": {"hierarchicalSupport": True}}
        result = gate_document_symbols(server_caps)
        assert result is None


class TestWorkspaceSymbolsGate:
    """Test workspace/symbol capability gating."""

    def test_capability_present_returns_none(self) -> None:
        """When workspaceSymbolProvider is present, gate returns None (proceed)."""
        server_caps = {"workspaceSymbolProvider": True}
        result = gate_workspace_symbols(server_caps)
        assert result is None

    def test_capability_missing_returns_degrade_event(self) -> None:
        """When workspaceSymbolProvider is missing, gate returns DegradeEventV1."""
        server_caps: dict[str, object] = {}
        result = gate_workspace_symbols(server_caps)
        assert isinstance(result, DegradeEventV1)
        assert result.stage == "lsp.pyrefly"
        assert result.severity == "info"
        assert result.category == "unavailable"
        assert "workspaceSymbolProvider" in result.message

    def test_capability_false_returns_degrade_event(self) -> None:
        """When workspaceSymbolProvider is explicitly False, gate returns DegradeEventV1."""
        server_caps = {"workspaceSymbolProvider": False}
        result = gate_workspace_symbols(server_caps)
        assert isinstance(result, DegradeEventV1)
        assert result.category == "unavailable"


class TestSemanticTokensGate:
    """Test semantic tokens capability gating."""

    def test_full_capability_present_returns_none(self) -> None:
        """When semanticTokensProvider with full=True, gate returns None (proceed)."""
        server_caps = {
            "semanticTokensProvider": {
                "full": True,
                "legend": {
                    "tokenTypes": ["function", "variable"],
                    "tokenModifiers": ["declaration"],
                },
            }
        }
        result = gate_semantic_tokens(server_caps)
        assert result is None

    def test_full_capability_as_dict_returns_none(self) -> None:
        """When semanticTokensProvider.full is a dict (delta support), gate returns None."""
        server_caps = {
            "semanticTokensProvider": {
                "full": {"delta": True},
                "legend": dict(_EMPTY_LEGEND),
            }
        }
        result = gate_semantic_tokens(server_caps)
        assert result is None

    def test_capability_missing_returns_degrade_event(self) -> None:
        """When semanticTokensProvider is missing, gate returns DegradeEventV1."""
        server_caps: dict[str, object] = {}
        result = gate_semantic_tokens(server_caps)
        assert isinstance(result, DegradeEventV1)
        assert result.stage == "lsp.pyrefly"
        assert result.severity == "info"
        assert result.category == "unavailable"
        assert "semanticTokensProvider" in result.message

    def test_capability_not_dict_returns_degrade_event(self) -> None:
        """When semanticTokensProvider is not a dict, gate returns DegradeEventV1."""
        server_caps = {"semanticTokensProvider": True}
        result = gate_semantic_tokens(server_caps)
        assert isinstance(result, DegradeEventV1)
        assert result.category == "unavailable"
        assert "not a capability dict" in result.message

    def test_full_missing_returns_degrade_event(self) -> None:
        """When semanticTokensProvider.full is missing, gate returns DegradeEventV1."""
        server_caps = {"semanticTokensProvider": {"legend": dict(_EMPTY_LEGEND)}}
        result = gate_semantic_tokens(server_caps)
        assert isinstance(result, DegradeEventV1)
        assert result.category == "unavailable"
        assert "semanticTokens/full not supported" in result.message

    def test_full_false_returns_degrade_event(self) -> None:
        """When semanticTokensProvider.full is False, gate returns DegradeEventV1."""
        server_caps = {
            "semanticTokensProvider": {
                "full": False,
                "legend": dict(_EMPTY_LEGEND),
            }
        }
        result = gate_semantic_tokens(server_caps)
        assert isinstance(result, DegradeEventV1)
        assert result.category == "unavailable"


class TestCapabilityVariations:
    """Test various capability configuration patterns."""

    def test_all_capabilities_present(self) -> None:
        """When all capabilities are present, all gates return None."""
        server_caps = {
            "documentSymbolProvider": True,
            "workspaceSymbolProvider": True,
            "semanticTokensProvider": {
                "full": True,
                "legend": dict(_EMPTY_LEGEND),
            },
        }
        assert gate_document_symbols(server_caps) is None
        assert gate_workspace_symbols(server_caps) is None
        assert gate_semantic_tokens(server_caps) is None

    def test_no_capabilities_present(self) -> None:
        """When no capabilities are present, all gates return DegradeEventV1."""
        server_caps: dict[str, object] = {}
        assert isinstance(gate_document_symbols(server_caps), DegradeEventV1)
        assert isinstance(gate_workspace_symbols(server_caps), DegradeEventV1)
        assert isinstance(gate_semantic_tokens(server_caps), DegradeEventV1)

    def test_partial_capabilities(self) -> None:
        """When some capabilities are present, gates return appropriate results."""
        server_caps = {
            "documentSymbolProvider": True,
            "semanticTokensProvider": {"full": True, "legend": dict(_EMPTY_DICT)},
        }
        assert gate_document_symbols(server_caps) is None
        assert isinstance(gate_workspace_symbols(server_caps), DegradeEventV1)
        assert gate_semantic_tokens(server_caps) is None
