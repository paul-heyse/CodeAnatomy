# ruff: noqa: C901,BLE001
"""Extended Pyrefly enrichment surfaces with capability gating.

This module provides optional LSP enrichment surfaces that are capability-gated.
Each surface checks server capabilities before making requests and returns typed
results or degrade events when capabilities are unavailable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

from tools.cq.core.snb_schema import DegradeEventV1
from tools.cq.core.structs import CqStruct
from tools.cq.search.pyrefly_capability_gates import (
    gate_document_symbols,
    gate_semantic_tokens,
    gate_workspace_symbols,
)


class PyreflyDocumentSymbol(CqStruct, frozen=True):
    """Document symbol descriptor from textDocument/documentSymbol.

    Parameters
    ----------
    name
        Symbol name.
    kind
        Symbol kind (e.g., "Function", "Class", "Variable").
    range_start_line
        Start line of the symbol's range (0-indexed).
    range_start_col
        Start column of the symbol's range.
    range_end_line
        End line of the symbol's range (0-indexed).
    range_end_col
        End column of the symbol's range.
    children
        Nested child symbols.
    """

    name: str
    kind: str = ""
    range_start_line: int = 0
    range_start_col: int = 0
    range_end_line: int = 0
    range_end_col: int = 0
    children: tuple[PyreflyDocumentSymbol, ...] = ()


class PyreflyWorkspaceSymbol(CqStruct, frozen=True):
    """Workspace symbol descriptor from workspace/symbol.

    Parameters
    ----------
    name
        Symbol name.
    kind
        Symbol kind (e.g., "Function", "Class", "Variable").
    container_name
        Optional container name (e.g., parent class).
    uri
        File URI where symbol is defined.
    file
        File path (derived from URI).
    line
        Line number (1-indexed).
    col
        Column number (0-indexed).
    """

    name: str
    kind: str = ""
    container_name: str | None = None
    uri: str | None = None
    file: str | None = None
    line: int = 0
    col: int = 0


class PyreflySemanticToken(CqStruct, frozen=True):
    """Semantic token descriptor from textDocument/semanticTokens/full.

    Parameters
    ----------
    line
        Token line (0-indexed).
    start_col
        Token start column.
    length
        Token length in characters.
    token_type
        Token type string (e.g., "function", "variable", "class").
    token_modifiers
        Token modifier strings (e.g., "declaration", "definition", "readonly").
    """

    line: int
    start_col: int
    length: int
    token_type: str = ""
    token_modifiers: tuple[str, ...] = ()


class PyreflyExpansionPayload(CqStruct, frozen=True):
    """Extended enrichment payload with capability-gated surfaces.

    Parameters
    ----------
    document_symbols
        Document symbol hierarchy (if available).
    workspace_symbols
        Workspace symbol matches (if available).
    semantic_tokens
        Semantic tokens (if available and requested).
    degrade_events
        Degradation events for unavailable surfaces.
    """

    document_symbols: tuple[PyreflyDocumentSymbol, ...] = ()
    workspace_symbols: tuple[PyreflyWorkspaceSymbol, ...] = ()
    semantic_tokens: tuple[PyreflySemanticToken, ...] = ()
    degrade_events: tuple[DegradeEventV1, ...] = ()


def fetch_document_symbols(
    server_caps: dict[str, object],
    lsp_request_fn: object,
    uri: str,
) -> tuple[PyreflyDocumentSymbol, ...] | DegradeEventV1:
    """Fetch document symbols with capability gating.

    Parameters
    ----------
    server_caps
        Server capabilities from LSP initialization.
    lsp_request_fn
        LSP request function (session._request).
    uri
        Document URI.

    Returns:
    -------
    tuple[PyreflyDocumentSymbol, ...] | DegradeEventV1
        Typed document symbols or degrade event if capability unavailable.
    """
    gate_result = gate_document_symbols(server_caps)
    if gate_result is not None:
        return gate_result

    try:
        result = _invoke_lsp_request(
            lsp_request_fn,
            "textDocument/documentSymbol",
            {"textDocument": {"uri": uri}},
        )
    except Exception as exc:
        return DegradeEventV1(
            stage="lsp.pyrefly.expansion",
            severity="warning",
            category="request_failed",
            message=f"textDocument/documentSymbol failed: {type(exc).__name__}",
        )

    if not isinstance(result, Sequence):
        return ()

    symbols: list[PyreflyDocumentSymbol] = []
    for item in result:
        if not isinstance(item, Mapping):
            continue
        normalized = _normalize_document_symbol(cast("Mapping[str, object]", item))
        if normalized is not None:
            symbols.append(normalized)
    return tuple(symbols)


def fetch_workspace_symbols(
    server_caps: dict[str, object],
    lsp_request_fn: object,
    query: str = "",
) -> tuple[PyreflyWorkspaceSymbol, ...] | DegradeEventV1:
    """Fetch workspace symbols with capability gating.

    Parameters
    ----------
    server_caps
        Server capabilities from LSP initialization.
    lsp_request_fn
        LSP request function (session._request).
    query
        Symbol search query (empty for all symbols).

    Returns:
    -------
    tuple[PyreflyWorkspaceSymbol, ...] | DegradeEventV1
        Typed workspace symbols or degrade event if capability unavailable.
    """
    gate_result = gate_workspace_symbols(server_caps)
    if gate_result is not None:
        return gate_result

    try:
        result = _invoke_lsp_request(
            lsp_request_fn,
            "workspace/symbol",
            {"query": query},
        )
    except Exception as exc:
        return DegradeEventV1(
            stage="lsp.pyrefly.expansion",
            severity="warning",
            category="request_failed",
            message=f"workspace/symbol failed: {type(exc).__name__}",
        )

    if not isinstance(result, Sequence):
        return ()

    provider = server_caps.get("workspaceSymbolProvider")
    supports_resolve = isinstance(provider, Mapping) and bool(provider.get("resolveProvider"))

    symbols: list[PyreflyWorkspaceSymbol] = []
    for item in result:
        if not isinstance(item, Mapping):
            continue
        item_mapping = cast("Mapping[str, object]", item)
        normalized = _normalize_workspace_symbol(item_mapping)
        if normalized is None:
            continue

        if supports_resolve and (normalized.uri is None or normalized.line <= 0):
            try:
                resolved_item = _invoke_lsp_request(
                    lsp_request_fn,
                    "workspaceSymbol/resolve",
                    dict(item_mapping),
                )
                if isinstance(resolved_item, Mapping):
                    resolved_normalized = _normalize_workspace_symbol(
                        cast("Mapping[str, object]", resolved_item)
                    )
                    if resolved_normalized is not None:
                        normalized = resolved_normalized
            except Exception:
                # Keep unresolved symbol row as-is for fail-open behavior.
                pass

        symbols.append(normalized)
    return tuple(symbols)


def fetch_semantic_tokens(
    server_caps: dict[str, object],
    lsp_request_fn: object,
    uri: str,
) -> tuple[PyreflySemanticToken, ...] | DegradeEventV1:
    """Fetch semantic tokens with capability gating.

    Parameters
    ----------
    server_caps
        Server capabilities from LSP initialization.
    lsp_request_fn
        LSP request function (session._request).
    uri
        Document URI.

    Returns:
    -------
    tuple[PyreflySemanticToken, ...] | DegradeEventV1
        Typed semantic tokens or degrade event if capability unavailable.
    """
    gate_result = gate_semantic_tokens(server_caps)
    if gate_result is not None:
        return gate_result

    try:
        result = _invoke_lsp_request(
            lsp_request_fn,
            "textDocument/semanticTokens/full",
            {"textDocument": {"uri": uri}},
        )
    except Exception as exc:
        return DegradeEventV1(
            stage="lsp.pyrefly.expansion",
            severity="warning",
            category="request_failed",
            message=f"textDocument/semanticTokens/full failed: {type(exc).__name__}",
        )

    if not isinstance(result, Mapping):
        return ()
    token_data = result.get("data")
    if not isinstance(token_data, Sequence):
        return ()
    int_data = [item for item in token_data if isinstance(item, int)]
    if len(int_data) != len(token_data):
        return ()

    semantic_tokens_caps = server_caps.get("semanticTokensProvider")
    token_types: Sequence[str] = ()
    token_modifiers: Sequence[str] = ()
    if isinstance(semantic_tokens_caps, Mapping):
        legend = semantic_tokens_caps.get("legend")
        if isinstance(legend, Mapping):
            types_value = legend.get("tokenTypes")
            modifiers_value = legend.get("tokenModifiers")
            if isinstance(types_value, Sequence):
                token_types = [item for item in types_value if isinstance(item, str)]
            if isinstance(modifiers_value, Sequence):
                token_modifiers = [item for item in modifiers_value if isinstance(item, str)]

    return _normalize_semantic_tokens(int_data, token_types, token_modifiers)


def _invoke_lsp_request(
    lsp_request_fn: object,
    method: str,
    params: dict[str, object],
) -> object:
    if callable(lsp_request_fn):
        return lsp_request_fn(method, params)
    msg = "LSP request function is not callable"
    raise TypeError(msg)


def _normalize_document_symbol(
    symbol: Mapping[str, object],
) -> PyreflyDocumentSymbol | None:
    """Normalize a raw document symbol response to typed structure.

    Parameters
    ----------
    symbol
        Raw document symbol dict from LSP response.

    Returns:
    -------
    PyreflyDocumentSymbol | None
        Normalized symbol or None if invalid.
    """
    name = symbol.get("name")
    if not isinstance(name, str) or not name.strip():
        return None

    kind_value = symbol.get("kind")
    kind = str(kind_value) if kind_value is not None else ""

    range_data = symbol.get("range")
    if not isinstance(range_data, Mapping):
        return PyreflyDocumentSymbol(name=name, kind=kind)

    start = range_data.get("start")
    end = range_data.get("end")

    range_start_line = 0
    range_start_col = 0
    range_end_line = 0
    range_end_col = 0

    if isinstance(start, Mapping):
        line_val = start.get("line")
        col_val = start.get("character")
        range_start_line = int(line_val) if isinstance(line_val, int) else 0
        range_start_col = int(col_val) if isinstance(col_val, int) else 0

    if isinstance(end, Mapping):
        line_val = end.get("line")
        col_val = end.get("character")
        range_end_line = int(line_val) if isinstance(line_val, int) else 0
        range_end_col = int(col_val) if isinstance(col_val, int) else 0

    children: list[PyreflyDocumentSymbol] = []
    children_data = symbol.get("children")
    if isinstance(children_data, Sequence):
        for child in children_data:
            if isinstance(child, Mapping):
                normalized = _normalize_document_symbol(child)
                if normalized is not None:
                    children.append(normalized)

    return PyreflyDocumentSymbol(
        name=name,
        kind=kind,
        range_start_line=range_start_line,
        range_start_col=range_start_col,
        range_end_line=range_end_line,
        range_end_col=range_end_col,
        children=tuple(children),
    )


def _normalize_workspace_symbol(
    symbol: Mapping[str, object],
) -> PyreflyWorkspaceSymbol | None:
    """Normalize a raw workspace symbol response to typed structure.

    Parameters
    ----------
    symbol
        Raw workspace symbol dict from LSP response.

    Returns:
    -------
    PyreflyWorkspaceSymbol | None
        Normalized symbol or None if invalid.
    """
    name = symbol.get("name")
    if not isinstance(name, str) or not name.strip():
        return None

    kind_value = symbol.get("kind")
    kind = str(kind_value) if kind_value is not None else ""

    container_name_val = symbol.get("containerName")
    container_name = (
        str(container_name_val).strip() or None if isinstance(container_name_val, str) else None
    )

    location = symbol.get("location")
    uri: str | None = None
    file: str | None = None
    line = 0
    col = 0

    if isinstance(location, Mapping):
        uri_val = location.get("uri")
        if isinstance(uri_val, str):
            uri = uri_val
            file = uri_val.removeprefix("file://") if uri_val.startswith("file://") else uri_val

        range_data = location.get("range")
        if isinstance(range_data, Mapping):
            start = range_data.get("start")
            if isinstance(start, Mapping):
                line_val = start.get("line")
                col_val = start.get("character")
                line = int(line_val) + 1 if isinstance(line_val, int) else 0
                col = int(col_val) if isinstance(col_val, int) else 0

    return PyreflyWorkspaceSymbol(
        name=name,
        kind=kind,
        container_name=container_name,
        uri=uri,
        file=file,
        line=line,
        col=col,
    )


def _normalize_semantic_tokens(
    data: Sequence[int],
    token_types: Sequence[str],
    token_modifiers: Sequence[str],
) -> tuple[PyreflySemanticToken, ...]:
    """Normalize semantic tokens data to typed structure.

    Parameters
    ----------
    data
        Token data array (delta-encoded: line, startChar, length, tokenType, tokenModifiers).
    token_types
        Token type legend from server capabilities.
    token_modifiers
        Token modifier legend from server capabilities.

    Returns:
    -------
    tuple[PyreflySemanticToken, ...]
        Normalized semantic tokens.
    """
    tokens: list[PyreflySemanticToken] = []
    current_line = 0
    current_col = 0

    if not isinstance(data, Sequence) or len(data) % 5 != 0:
        return ()

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
            token_modifiers[bit_idx]
            for bit_idx in range(32)
            if token_modifiers_bits & (1 << bit_idx) and bit_idx < len(token_modifiers)
        ]

        tokens.append(
            PyreflySemanticToken(
                line=current_line,
                start_col=current_col,
                length=length,
                token_type=token_type,
                token_modifiers=tuple(modifiers),
            )
        )

    return tuple(tokens)


__all__ = [
    "PyreflyDocumentSymbol",
    "PyreflyExpansionPayload",
    "PyreflySemanticToken",
    "PyreflyWorkspaceSymbol",
    "fetch_document_symbols",
    "fetch_semantic_tokens",
    "fetch_workspace_symbols",
]
