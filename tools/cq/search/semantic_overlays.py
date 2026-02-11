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


__all__ = [
    "InlayHintV1",
    "SemanticTokenBundleV1",
    "SemanticTokenSpanV1",
    "fetch_inlay_hints_range",
    "fetch_semantic_tokens_range",
]
