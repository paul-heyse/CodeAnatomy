"""Extended Pyrefly enrichment surfaces with capability gating.

This module provides optional LSP enrichment surfaces that are capability-gated.
Each surface checks server capabilities before making requests and returns typed
results or degrade events when capabilities are unavailable.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

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
    lsp_request_fn: object,  # noqa: ARG001 - Reserved for future LSP request integration
    uri: str,  # noqa: ARG001 - Reserved for future LSP request integration
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

    return ()


def fetch_workspace_symbols(
    server_caps: dict[str, object],
    lsp_request_fn: object,  # noqa: ARG001 - Reserved for future LSP request integration
    query: str = "",  # noqa: ARG001 - Reserved for future LSP request integration
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

    return ()


def fetch_semantic_tokens(
    server_caps: dict[str, object],
    lsp_request_fn: object,  # noqa: ARG001 - Reserved for future LSP request integration
    uri: str,  # noqa: ARG001 - Reserved for future LSP request integration
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

    return ()


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
