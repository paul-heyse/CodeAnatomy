"""Semantic overlay plane for CQ enrichment (tokens, hints)."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import cast

from tools.cq.core.structs import CqStruct
from tools.cq.search.lsp.capabilities import coerce_capabilities, supports_method

_FAIL_OPEN_EXCEPTIONS = (OSError, RuntimeError, TimeoutError, ValueError, TypeError)


class SemanticTokenSpanV1(CqStruct, frozen=True):
    """Normalized semantic token with resolved type/modifier names."""

    line: int
    start_char: int
    length: int
    token_type: str
    modifiers: tuple[str, ...] = ()


class SemanticTokenBundleV1(CqStruct, frozen=True):
    """Atomic semantic token bundle with legend and encoding metadata."""

    position_encoding: str = "utf-16"
    legend_token_types: tuple[str, ...] = ()
    legend_token_modifiers: tuple[str, ...] = ()
    result_id: str | None = None
    previous_result_id: str | None = None
    tokens: tuple[SemanticTokenSpanV1, ...] = ()
    raw_data: tuple[int, ...] | None = None


class InlayHintV1(CqStruct, frozen=True):
    """Normalized inlay hint from LSP server."""

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
    """Fetch semantic tokens for a range with fail-open capability gates.

    Returns:
        Normalized semantic tokens for the requested range, or `None`.
    """
    request = _request_fn(session)
    if request is None:
        return None

    caps = _server_caps(session)
    if not supports_method(caps, "textDocument/semanticTokens/range") and not bool(
        caps.get("semanticTokensProvider")
    ):
        return None

    token_caps = caps.get("semanticTokensProvider")
    use_range = isinstance(token_caps, Mapping) and bool(token_caps.get("range"))
    method, params = _semantic_request(
        uri, start_line=start_line, end_line=end_line, use_range=use_range
    )

    try:
        result = request(method, _request_params(params))
    except _FAIL_OPEN_EXCEPTIONS:
        return None

    raw_data = _semantic_raw_data(result)
    if raw_data is None:
        return None
    token_types, token_modifiers = _semantic_legend(token_caps)
    decoded = _decode_semantic_tokens(raw_data, token_types, token_modifiers)
    low = max(0, start_line)
    high = max(start_line, end_line)
    return tuple(token for token in decoded if low <= token.line <= high)


def fetch_inlay_hints_range(
    session: object,
    uri: str,
    start_line: int,
    end_line: int,
) -> tuple[InlayHintV1, ...] | None:
    """Fetch inlay hints for a range with fail-open capability gates.

    Returns:
        Normalized inlay hints for the requested range, or `None`.
    """
    request = _request_fn(session)
    if request is None:
        return None

    caps = _server_caps(session)
    if not supports_method(caps, "textDocument/inlayHint"):
        return None

    params = {
        "textDocument": {"uri": uri},
        "range": {
            "start": {"line": max(0, start_line), "character": 0},
            "end": {"line": max(start_line, end_line), "character": 0},
        },
    }
    try:
        response = request("textDocument/inlayHint", _request_params(params))
    except _FAIL_OPEN_EXCEPTIONS:
        return None

    if not isinstance(response, (list, tuple)):
        return None

    inlay_caps = caps.get("inlayHintProvider")
    supports_resolve = isinstance(inlay_caps, Mapping) and bool(inlay_caps.get("resolveProvider"))
    hints: list[InlayHintV1] = []
    for item in response:
        normalized = _normalize_maybe_resolved_hint(
            request,
            item,
            supports_resolve=supports_resolve,
        )
        if normalized is not None:
            hints.append(normalized)
    return tuple(hints)


def _semantic_request(
    uri: str,
    *,
    start_line: int,
    end_line: int,
    use_range: bool,
) -> tuple[str, dict[str, object]]:
    if use_range:
        return (
            "textDocument/semanticTokens/range",
            {
                "textDocument": {"uri": uri},
                "range": {
                    "start": {"line": max(0, start_line), "character": 0},
                    "end": {"line": max(start_line, end_line), "character": 0},
                },
            },
        )
    return "textDocument/semanticTokens/full", {"textDocument": {"uri": uri}}


def _semantic_raw_data(result: object) -> list[int] | tuple[int, ...] | None:
    if not isinstance(result, Mapping):
        return None
    raw_data = result.get("data")
    if not isinstance(raw_data, (list, tuple)) or len(raw_data) % 5 != 0:
        return None
    if not all(isinstance(item, int) for item in raw_data):
        return None
    return cast("list[int] | tuple[int, ...]", raw_data)


def _semantic_legend(token_caps: object) -> tuple[list[str], list[str]]:
    if not isinstance(token_caps, Mapping):
        return [], []
    legend = token_caps.get("legend")
    if not isinstance(legend, Mapping):
        return [], []

    raw_types = legend.get("tokenTypes")
    raw_modifiers = legend.get("tokenModifiers")
    token_types = (
        [item for item in raw_types if isinstance(item, str)]
        if isinstance(raw_types, Sequence)
        else []
    )
    token_modifiers = (
        [item for item in raw_modifiers if isinstance(item, str)]
        if isinstance(raw_modifiers, Sequence)
        else []
    )
    return token_types, token_modifiers


def _normalize_maybe_resolved_hint(
    request: Callable[[str, dict[str, object]], object],
    item: object,
    *,
    supports_resolve: bool,
) -> InlayHintV1 | None:
    if not isinstance(item, Mapping):
        return None
    mapping: Mapping[str, object] = item
    if supports_resolve and mapping.get("data") is not None:
        try:
            resolved = request("inlayHint/resolve", dict(mapping))
        except _FAIL_OPEN_EXCEPTIONS:
            resolved = None
        if isinstance(resolved, Mapping):
            mapping = resolved
    return _normalize_inlay_hint(dict(mapping))


def _request_params(payload: Mapping[str, object]) -> dict[str, object]:
    return dict(payload)


def _request_fn(session: object) -> Callable[[str, dict[str, object]], object] | None:
    request = getattr(session, "_send_request", None)
    if callable(request):
        return cast("Callable[[str, dict[str, object]], object]", request)
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
    return coerce_capabilities(
        {
            "semanticTokensProvider": semantic_raw
            if semantic_raw is not None
            else getattr(server_caps, "semantic_tokens_provider", False),
            "inlayHintProvider": getattr(server_caps, "inlay_hint_provider", False),
            "positionEncoding": getattr(env, "position_encoding", "utf-16"),
        }
    )


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
