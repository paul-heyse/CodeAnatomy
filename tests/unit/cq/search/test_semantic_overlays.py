# ruff: noqa: ANN001, EM102, TRY003
"""Unit tests for semantic overlay plane helpers."""

from __future__ import annotations

from types import SimpleNamespace

import msgspec
from tools.cq.search.semantic_overlays import (
    InlayHintV1,
    SemanticTokenBundleV1,
    SemanticTokenSpanV1,
    fetch_inlay_hints_range,
    fetch_semantic_tokens_range,
)


def _session_with_caps(
    *,
    request,
    semantic_tokens_provider,
    inlay_hint_provider,
) -> object:
    return SimpleNamespace(
        _send_request=request,
        _session_env=SimpleNamespace(
            position_encoding="utf-16",
            capabilities=SimpleNamespace(
                server_caps=SimpleNamespace(
                    semantic_tokens_provider_raw=semantic_tokens_provider,
                    semantic_tokens_provider=bool(semantic_tokens_provider),
                    inlay_hint_provider=inlay_hint_provider,
                )
            ),
        ),
    )


def test_semantic_token_span_roundtrip() -> None:
    token = SemanticTokenSpanV1(
        line=10,
        start_char=5,
        length=8,
        token_type="function",
        modifiers=("declaration", "definition"),
    )
    encoded = msgspec.json.encode(token)
    decoded = msgspec.json.decode(encoded, type=SemanticTokenSpanV1)
    assert decoded == token


def test_semantic_token_bundle_defaults() -> None:
    bundle = SemanticTokenBundleV1()
    assert bundle.position_encoding == "utf-16"
    assert bundle.legend_token_types == ()
    assert bundle.legend_token_modifiers == ()
    assert bundle.tokens == ()


def test_inlay_hint_roundtrip() -> None:
    hint = InlayHintV1(
        line=5,
        character=10,
        label=": String",
        kind="type",
        padding_left=True,
    )
    encoded = msgspec.json.encode(hint)
    decoded = msgspec.json.decode(encoded, type=InlayHintV1)
    assert decoded == hint


def test_fetch_semantic_tokens_range_returns_none_without_request_fn() -> None:
    session = object()
    assert fetch_semantic_tokens_range(session, "file:///test.py", 0, 10) is None


def test_fetch_semantic_tokens_range_returns_none_when_provider_missing() -> None:
    def request(_method: str, _params: object) -> object:
        return {"data": [0, 1, 3, 0, 1]}

    session = _session_with_caps(
        request=request,
        semantic_tokens_provider=False,
        inlay_hint_provider=False,
    )
    assert fetch_semantic_tokens_range(session, "file:///test.py", 0, 10) is None


def test_fetch_semantic_tokens_range_decodes_tokens() -> None:
    def request(method: str, _params: object) -> object:
        assert method == "textDocument/semanticTokens/full"
        return {"data": [0, 1, 3, 0, 1]}  # one token at line 0 col 1

    session = _session_with_caps(
        request=request,
        semantic_tokens_provider={
            "full": True,
            "legend": {
                "tokenTypes": ["function"],
                "tokenModifiers": ["declaration"],
            },
        },
        inlay_hint_provider=False,
    )
    tokens = fetch_semantic_tokens_range(session, "file:///test.py", 0, 10)
    assert tokens is not None
    assert len(tokens) == 1
    assert tokens[0].token_type == "function"
    assert tokens[0].modifiers == ("declaration",)


def test_fetch_semantic_tokens_range_handles_malformed_data_fail_open() -> None:
    def request(_method: str, _params: object) -> object:
        return {"data": [0, 1, "bad", 0, 1]}

    session = _session_with_caps(
        request=request,
        semantic_tokens_provider={"full": True, "legend": {}},
        inlay_hint_provider=False,
    )
    assert fetch_semantic_tokens_range(session, "file:///test.py", 0, 10) is None


def test_fetch_inlay_hints_range_returns_none_without_provider() -> None:
    def request(_method: str, _params: object) -> object:
        return [{"position": {"line": 0, "character": 0}, "label": ": ignored"}]

    session = _session_with_caps(
        request=request,
        semantic_tokens_provider=False,
        inlay_hint_provider=False,
    )
    assert fetch_inlay_hints_range(session, "file:///test.py", 0, 5) is None


def test_fetch_inlay_hints_range_resolve_path() -> None:
    def request(method: str, params: object) -> object:
        if method == "textDocument/inlayHint":
            return [
                {
                    "position": {"line": 1, "character": 3},
                    "label": [{"value": ": i32"}],
                    "data": {"id": 1},
                }
            ]
        if method == "inlayHint/resolve":
            assert isinstance(params, dict)
            return {
                "position": {"line": 1, "character": 3},
                "label": ": i32",
                "paddingLeft": True,
                "paddingRight": False,
            }
        raise AssertionError(f"Unexpected method: {method}")

    session = _session_with_caps(
        request=request,
        semantic_tokens_provider=False,
        inlay_hint_provider={"resolveProvider": True},
    )
    hints = fetch_inlay_hints_range(session, "file:///test.py", 0, 20)
    assert hints is not None
    assert len(hints) == 1
    assert hints[0].label == ": i32"
    assert hints[0].padding_left is True


def test_fetch_inlay_hints_range_handles_non_list_fail_open() -> None:
    def request(_method: str, _params: object) -> object:
        return {"not": "a list"}

    session = _session_with_caps(
        request=request,
        semantic_tokens_provider=False,
        inlay_hint_provider=True,
    )
    assert fetch_inlay_hints_range(session, "file:///test.py", 0, 20) is None


def test_fetch_semantic_tokens_uses_capabilities_snapshot_when_present() -> None:
    class _SnapshotSession:
        def capabilities_snapshot(self) -> dict[str, object]:
            return {
                "semanticTokensProvider": {
                    "full": True,
                    "legend": {"tokenTypes": ["function"], "tokenModifiers": ["declaration"]},
                }
            }

        def _send_request(self, method: str, _params: object) -> object:
            assert method == "textDocument/semanticTokens/full"
            return {"data": [0, 0, 3, 0, 1]}

    tokens = fetch_semantic_tokens_range(_SnapshotSession(), "file:///test.py", 0, 10)
    assert tokens is not None
    assert len(tokens) == 1
