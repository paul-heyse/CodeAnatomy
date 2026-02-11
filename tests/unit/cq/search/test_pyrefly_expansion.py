# ruff: noqa: ARG001
"""Tests for Pyrefly expansion surfaces."""

from __future__ import annotations

from tools.cq.core.snb_schema import DegradeEventV1
from tools.cq.search.pyrefly_expansion import (
    fetch_document_symbols,
    fetch_semantic_tokens,
    fetch_workspace_symbols,
)


def test_fetch_document_symbols_returns_normalized_rows() -> None:
    caps: dict[str, object] = {"documentSymbolProvider": True}

    def request(method: str, params: object) -> object:
        assert method == "textDocument/documentSymbol"
        assert isinstance(params, dict)
        return [
            {
                "name": "foo",
                "kind": 12,
                "range": {
                    "start": {"line": 1, "character": 2},
                    "end": {"line": 3, "character": 0},
                },
            }
        ]

    rows = fetch_document_symbols(caps, request, "file:///test.py")
    assert not isinstance(rows, DegradeEventV1)
    assert len(rows) == 1
    assert rows[0].name == "foo"
    assert rows[0].range_start_line == 1


def test_fetch_workspace_symbols_uses_resolve_when_missing_range() -> None:
    calls: list[str] = []
    caps: dict[str, object] = {"workspaceSymbolProvider": {"resolveProvider": True}}

    def request(method: str, params: object) -> object:
        calls.append(method)
        if method == "workspace/symbol":
            return [{"name": "foo", "kind": 12}]
        if method == "workspaceSymbol/resolve":
            return {
                "name": "foo",
                "kind": 12,
                "location": {
                    "uri": "file:///tmp/test.py",
                    "range": {"start": {"line": 10, "character": 1}},
                },
            }
        raise AssertionError(method)

    rows = fetch_workspace_symbols(caps, request, query="foo")
    assert not isinstance(rows, DegradeEventV1)
    assert len(rows) == 1
    assert rows[0].file == "/tmp/test.py"
    assert "workspaceSymbol/resolve" in calls


def test_fetch_semantic_tokens_decodes_with_legend() -> None:
    caps: dict[str, object] = {
        "semanticTokensProvider": {
            "full": True,
            "legend": {
                "tokenTypes": ["function"],
                "tokenModifiers": ["declaration"],
            },
        }
    }

    def request(method: str, params: object) -> object:
        assert method == "textDocument/semanticTokens/full"
        assert isinstance(params, dict)
        return {"data": [0, 1, 2, 0, 1]}

    rows = fetch_semantic_tokens(caps, request, "file:///test.py")
    assert not isinstance(rows, DegradeEventV1)
    assert len(rows) == 1
    assert rows[0].token_type == "function"
    assert rows[0].token_modifiers == ("declaration",)
