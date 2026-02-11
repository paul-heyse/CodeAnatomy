# ruff: noqa: C901,PLR0911,BLE001,ANN202
"""Semantic overlay plane for CQ enrichment (tokens, hints).

This module provides typed structures and stub functions for semantic token
and inlay hint enrichment. All functions are capability-gated and fail-open.
"""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class SemanticTokenSpanV1(CqStruct, frozen=True):
    """Normalized semantic token with resolved type/modifier names.

    Parameters
    ----------
    line
        Token line (0-indexed).
    start_char
        Token start column.
    length
        Token length in characters.
    token_type
        Resolved token type string from legend (e.g., "function", "variable").
    modifiers
        Resolved token modifier strings from legend (e.g., "declaration", "readonly").
    """

    line: int
    start_char: int
    length: int
    token_type: str
    modifiers: tuple[str, ...] = ()


class SemanticTokenBundleV1(CqStruct, frozen=True):
    """Atomic semantic token bundle with legend and encoding metadata.

    Must always store negotiated position encoding, server legend (token types
    and modifiers arrays), raw data stream (or delta edits), and decoded rows
    with resolved names.

    References:
    - rust_lsp.md: semantic tokens require legend + encoding to decode
    - pyrefly_lsp_data.md: same requirement

    Parameters
    ----------
    position_encoding
        Negotiated position encoding (utf-8, utf-16, utf-32).
    legend_token_types
        Token type legend from server capabilities.
    legend_token_modifiers
        Token modifier legend from server capabilities.
    result_id
        Cache result ID from server (for incremental updates).
    previous_result_id
        Previous cache result ID (for delta requests).
    tokens
        Decoded semantic tokens with resolved names.
    raw_data
        Original integer array for recomputation and caching.
    """

    position_encoding: str = "utf-16"
    legend_token_types: tuple[str, ...] = ()
    legend_token_modifiers: tuple[str, ...] = ()
    result_id: str | None = None
    previous_result_id: str | None = None
    tokens: tuple[SemanticTokenSpanV1, ...] = ()
    raw_data: tuple[int, ...] | None = None


class InlayHintV1(CqStruct, frozen=True):
    """Normalized inlay hint from LSP server.

    Parameters
    ----------
    line
        Hint line (0-indexed).
    character
        Hint character position.
    label
        Hint label text.
    kind
        Hint kind: "type", "parameter", or None.
    padding_left
        Whether to add padding before the hint.
    padding_right
        Whether to add padding after the hint.
    """

    line: int
    character: int
    label: str
    kind: str | None = None
    padding_left: bool = False
    padding_right: bool = False


def fetch_semantic_tokens_range(
    session: object,
    uri: str,
    start_line: int,
    end_line: int,
) -> tuple[SemanticTokenSpanV1, ...] | None:
    """Fetch semantic tokens for a range. Fail-open. Capability-gated.

    Parameters
    ----------
    session
        LSP session (_PyreflyLspSession or _RustLspSession).
    uri
        Document URI.
    start_line
        Range start line (0-indexed).
    end_line
        Range end line (0-indexed).

    Returns:
    -------
    tuple[SemanticTokenSpanV1, ...] | None
        Semantic tokens for the range, or None if unavailable.
    """
    request = _request_fn(session)
    if request is None:
        return None

    caps = _server_caps(session)
    semantic_caps = caps.get("semanticTokensProvider")
    if not semantic_caps:
        return None

    params_range = {
        "textDocument": {"uri": uri},
        "range": {
            "start": {"line": max(0, start_line), "character": 0},
            "end": {"line": max(start_line, end_line), "character": 0},
        },
    }
    params_full = {
        "textDocument": {"uri": uri},
    }

    try:
        if isinstance(semantic_caps, dict) and semantic_caps.get("range"):
            result = request("textDocument/semanticTokens/range", params_range)
        else:
            result = request("textDocument/semanticTokens/full", params_full)
    except Exception:
        return None

    if not isinstance(result, dict):
        return None
    raw_data = result.get("data")
    if not isinstance(raw_data, (list, tuple)) or len(raw_data) % 5 != 0:
        return None
    if not all(isinstance(item, int) for item in raw_data):
        return None

    legend = semantic_caps.get("legend") if isinstance(semantic_caps, dict) else None
    token_types: list[str] = []
    token_modifiers: list[str] = []
    if isinstance(legend, dict):
        raw_types = legend.get("tokenTypes")
        raw_modifiers = legend.get("tokenModifiers")
        if isinstance(raw_types, (list, tuple)):
            token_types = [item for item in raw_types if isinstance(item, str)]
        if isinstance(raw_modifiers, (list, tuple)):
            token_modifiers = [item for item in raw_modifiers if isinstance(item, str)]

    decoded = _decode_semantic_tokens(raw_data, token_types, token_modifiers)
    return tuple(
        token for token in decoded if max(0, start_line) <= token.line <= max(start_line, end_line)
    )


def fetch_inlay_hints_range(
    session: object,
    uri: str,
    start_line: int,
    end_line: int,
) -> tuple[InlayHintV1, ...] | None:
    """Fetch inlay hints for a range. Fail-open. Capability-gated.

    Parameters
    ----------
    session
        LSP session (_PyreflyLspSession or _RustLspSession).
    uri
        Document URI.
    start_line
        Range start line (0-indexed).
    end_line
        Range end line (0-indexed).

    Returns:
    -------
    tuple[InlayHintV1, ...] | None
        Inlay hints for the range, or None if unavailable.
    """
    request = _request_fn(session)
    if request is None:
        return None

    caps = _server_caps(session)
    inlay_caps = caps.get("inlayHintProvider")
    if not inlay_caps:
        return None

    params = {
        "textDocument": {"uri": uri},
        "range": {
            "start": {"line": max(0, start_line), "character": 0},
            "end": {"line": max(start_line, end_line), "character": 0},
        },
    }
    try:
        response = request("textDocument/inlayHint", params)
    except Exception:
        return None

    if not isinstance(response, (list, tuple)):
        return None

    supports_resolve = isinstance(inlay_caps, dict) and bool(inlay_caps.get("resolveProvider"))
    hints: list[InlayHintV1] = []
    for item in response:
        if not isinstance(item, dict):
            continue
        mapping = item
        if supports_resolve and mapping.get("data") is not None:
            try:
                resolved = request("inlayHint/resolve", mapping)
                if isinstance(resolved, dict):
                    mapping = resolved
            except Exception:
                pass
        normalized = _normalize_inlay_hint(mapping)
        if normalized is not None:
            hints.append(normalized)
    return tuple(hints)


def _request_fn(session: object):
    request = getattr(session, "_send_request", None)
    if callable(request):
        return request
    return None


def _server_caps(session: object) -> dict[str, object]:
    env = getattr(session, "_session_env", None)
    if env is None:
        return {}
    caps = getattr(env, "capabilities", None)
    if caps is None:
        return {}
    server_caps = getattr(caps, "server_caps", None)
    if server_caps is None:
        return {}
    semantic_raw = getattr(server_caps, "semantic_tokens_provider_raw", None)
    inlay_provider = getattr(server_caps, "inlay_hint_provider", False)
    return {
        "semanticTokensProvider": semantic_raw
        if semantic_raw is not None
        else getattr(server_caps, "semantic_tokens_provider", False),
        "inlayHintProvider": inlay_provider,
        "positionEncoding": getattr(env, "position_encoding", "utf-16"),
    }


def _decode_semantic_tokens(
    data: list[int] | tuple[int, ...],
    token_types: list[str],
    token_modifiers: list[str],
) -> tuple[SemanticTokenSpanV1, ...]:
    tokens: list[SemanticTokenSpanV1] = []
    current_line = 0
    current_col = 0
    for i in range(0, len(data), 5):
        delta_line = data[i]
        delta_col = data[i + 1]
        length = data[i + 2]
        token_type_idx = data[i + 3]
        token_modifiers_bits = data[i + 4]

        if delta_line != 0:
            current_line += delta_line
            current_col = delta_col
        else:
            current_col += delta_col

        token_type = token_types[token_type_idx] if 0 <= token_type_idx < len(token_types) else ""
        modifiers = [
            token_modifiers[idx]
            for idx in range(32)
            if token_modifiers_bits & (1 << idx) and idx < len(token_modifiers)
        ]
        tokens.append(
            SemanticTokenSpanV1(
                line=current_line,
                start_char=current_col,
                length=length,
                token_type=token_type,
                modifiers=tuple(modifiers),
            )
        )
    return tuple(tokens)


def _normalize_inlay_hint(value: dict[str, object]) -> InlayHintV1 | None:
    position = value.get("position")
    if not isinstance(position, dict):
        return None
    line = position.get("line")
    character = position.get("character")
    if not isinstance(line, int) or not isinstance(character, int):
        return None

    label_value = value.get("label")
    label = _render_inlay_label(label_value)
    if not label:
        return None

    kind = value.get("kind")
    kind_value = str(kind) if isinstance(kind, (int, str)) else None

    return InlayHintV1(
        line=line,
        character=character,
        label=label,
        kind=kind_value,
        padding_left=bool(value.get("paddingLeft")),
        padding_right=bool(value.get("paddingRight")),
    )


def _render_inlay_label(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("value")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts)
    if isinstance(value, dict):
        text = value.get("value")
        if isinstance(text, str):
            return text
    return ""


__all__ = [
    "InlayHintV1",
    "SemanticTokenBundleV1",
    "SemanticTokenSpanV1",
    "fetch_inlay_hints_range",
    "fetch_semantic_tokens_range",
]
