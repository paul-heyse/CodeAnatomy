"""Typed Rust LSP enrichment contracts for CQ search pipelines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Literal, cast

import msgspec

from tools.cq.core.serialization import to_builtins
from tools.cq.core.structs import CqStruct


class LspServerCapabilitySnapshotV1(CqStruct, frozen=True):
    """Negotiated server capability projection.

    Stores capabilities using PROVIDER fields (definitionProvider,
    referencesProvider, etc.) for correct capability checking.
    Retains rich provider payloads for advanced-plane precision gates.
    """

    definition_provider: bool = False
    type_definition_provider: bool = False
    implementation_provider: bool = False
    references_provider: bool = False
    document_symbol_provider: bool = False
    call_hierarchy_provider: bool = False
    type_hierarchy_provider: bool = False
    hover_provider: bool = False
    workspace_symbol_provider: bool = False
    rename_provider: bool = False
    code_action_provider: bool = False
    semantic_tokens_provider: bool = False
    inlay_hint_provider: bool = False
    diagnostic_provider: bool = False
    workspace_diagnostic_provider: bool = False
    semantic_tokens_provider_raw: dict[str, object] | None = None
    code_action_provider_raw: object | None = None
    workspace_symbol_provider_raw: object | None = None
    diagnostic_provider_raw: object | None = None
    workspace_diagnostic_provider_raw: object | None = None
    position_encoding: str = "utf-16"


class LspClientPublishDiagnosticsCapsV1(CqStruct, frozen=True):
    """Client diagnostics capability detail from initialize request."""

    enabled: bool = False
    related_information: bool = False
    version_support: bool = False
    code_description_support: bool = False
    data_support: bool = False


class LspClientCapabilitySnapshotV1(CqStruct, frozen=True):
    """Client-advertised capability projection used for bridge gating."""

    publish_diagnostics: LspClientPublishDiagnosticsCapsV1 = msgspec.field(
        default_factory=LspClientPublishDiagnosticsCapsV1
    )
    inlay_hint_refresh_support: bool = False
    semantic_tokens_refresh_support: bool = False
    code_lens_refresh_support: bool = False
    diagnostics_refresh_support: bool = False
    position_encodings: tuple[str, ...] = ()


class LspExperimentalCapabilitySnapshotV1(CqStruct, frozen=True):
    """Experimental capability snapshot (client/server negotiated)."""

    server_status_notification: bool = False


class LspCapabilitySnapshotV1(CqStruct, frozen=True):
    """Typed split snapshot for capability gating decisions."""

    server_caps: LspServerCapabilitySnapshotV1 = msgspec.field(
        default_factory=LspServerCapabilitySnapshotV1
    )
    client_caps: LspClientCapabilitySnapshotV1 = msgspec.field(
        default_factory=LspClientCapabilitySnapshotV1
    )
    experimental_caps: LspExperimentalCapabilitySnapshotV1 = msgspec.field(
        default_factory=LspExperimentalCapabilitySnapshotV1
    )


class LspSessionEnvV1(CqStruct, frozen=True):
    """Shared LSP session/environment envelope for quality gating.

    Captures negotiated capabilities, server health, and configuration
    to enable reproducible enrichment and capability-based slice planning.
    """

    server_name: str | None = None
    server_version: str | None = None
    position_encoding: str = "utf-16"  # utf-8, utf-16, utf-32
    capabilities: LspCapabilitySnapshotV1 = msgspec.field(default_factory=LspCapabilitySnapshotV1)
    workspace_health: Literal["ok", "warning", "error", "unknown"] = "unknown"
    quiescent: bool = False
    config_fingerprint: str | None = None  # Hash of effective workspace/configuration responses
    refresh_events: tuple[str, ...] = ()  # e.g. ("workspace/didChangeConfiguration",)


class RustDiagnosticV1(CqStruct, frozen=True):
    """Normalized LSP diagnostic from publishDiagnostics notification.

    Bridges LSP diagnostics to CQ enrichment pipeline with full
    span normalization and code-action passthrough.
    """

    uri: str
    range_start_line: int = 0
    range_start_col: int = 0
    range_end_line: int = 0
    range_end_col: int = 0
    severity: int = 0  # 1=Error, 2=Warning, 3=Info, 4=Hint
    code: str | None = None
    source: str | None = None
    message: str = ""
    related_info: tuple[dict[str, object], ...] = ()
    data: dict[str, object] | None = None  # Passthrough for code-action bridging


class RustLspTarget(CqStruct, frozen=True):
    """Normalized LSP location target (definition, reference, etc.)."""

    uri: str
    range_start_line: int = 0
    range_start_col: int = 0
    range_end_line: int = 0
    range_end_col: int = 0
    container_name: str | None = None


class RustCallLink(CqStruct, frozen=True):
    """Call hierarchy link (caller or callee)."""

    name: str
    kind: int = 0  # SymbolKind enum
    uri: str = ""
    range_start_line: int = 0
    range_start_col: int = 0
    from_ranges: tuple[tuple[int, int, int, int], ...] = ()  # Call sites


class RustTypeLink(CqStruct, frozen=True):
    """Type hierarchy link (supertype or subtype)."""

    name: str
    kind: int = 0  # SymbolKind enum
    uri: str = ""
    range_start_line: int = 0
    range_start_col: int = 0


class RustDocSymbol(CqStruct, frozen=True):
    """Document symbol from textDocument/documentSymbol."""

    name: str
    kind: int = 0  # SymbolKind enum
    range_start_line: int = 0
    range_start_col: int = 0
    range_end_line: int = 0
    range_end_col: int = 0
    children: tuple[RustDocSymbol, ...] = ()


class RustSymbolGrounding(CqStruct, frozen=True):
    """Symbol grounding bundle (definitions, references, etc.)."""

    definitions: tuple[RustLspTarget, ...] = ()
    type_definitions: tuple[RustLspTarget, ...] = ()
    implementations: tuple[RustLspTarget, ...] = ()
    references: tuple[RustLspTarget, ...] = ()


class RustCallGraph(CqStruct, frozen=True):
    """Call graph bundle (incoming callers + outgoing callees)."""

    incoming_callers: tuple[RustCallLink, ...] = ()
    outgoing_callees: tuple[RustCallLink, ...] = ()


class RustTypeHierarchy(CqStruct, frozen=True):
    """Type hierarchy bundle (supertypes + subtypes)."""

    supertypes: tuple[RustTypeLink, ...] = ()
    subtypes: tuple[RustTypeLink, ...] = ()


class RustLspEnrichmentPayload(CqStruct, frozen=True):
    """Normalized Rust LSP enrichment payload — mirrors PyreflyEnrichmentPayload.

    Single unified payload for all Rust LSP enrichment surfaces.
    """

    session_env: LspSessionEnvV1 = msgspec.field(default_factory=LspSessionEnvV1)
    symbol_grounding: RustSymbolGrounding = msgspec.field(default_factory=RustSymbolGrounding)
    call_graph: RustCallGraph = msgspec.field(default_factory=RustCallGraph)
    type_hierarchy: RustTypeHierarchy = msgspec.field(default_factory=RustTypeHierarchy)
    document_symbols: tuple[RustDocSymbol, ...] = ()
    diagnostics: tuple[RustDiagnosticV1, ...] = ()
    hover_text: str | None = None
    advanced_planes: dict[str, object] = msgspec.field(default_factory=dict)


def _as_str(value: object) -> str | None:
    """Extract string from value or return None.

    Returns:
    -------
    str | None
        Stripped string if value is a non-empty string, otherwise None.
    """
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _as_int(value: object, default: int = 0) -> int:
    """Extract int from value or return default.

    Returns:
    -------
    int
        Integer value if valid int, otherwise default.
    """
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    return default


def _as_mapping(value: object) -> Mapping[str, object] | None:
    """Cast value to mapping or return None.

    Returns:
    -------
    Mapping[str, object] | None
        Mapping if value is a mapping, otherwise None.
    """
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    return None


def _as_sequence(value: object) -> Sequence[object] | None:
    """Cast value to sequence or return None.

    Returns:
    -------
    Sequence[object] | None
        Sequence if value is a sequence (excluding strings), otherwise None.
    """
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return cast("Sequence[object]", value)
    return None


def _coerce_session_env(data: object) -> LspSessionEnvV1:
    """Normalize session environment data to LspSessionEnvV1.

    Returns:
    -------
    LspSessionEnvV1
        Normalized session environment.
    """
    if not isinstance(data, Mapping):
        return LspSessionEnvV1()

    caps_data = _as_mapping(data.get("capabilities"))
    capabilities = LspCapabilitySnapshotV1()
    if caps_data:
        server_caps_data = _as_mapping(caps_data.get("server_caps"))
        client_caps_data = _as_mapping(caps_data.get("client_caps"))
        experimental_caps_data = _as_mapping(caps_data.get("experimental_caps"))

        capabilities = LspCapabilitySnapshotV1(
            server_caps=(
                msgspec.convert(dict(server_caps_data), LspServerCapabilitySnapshotV1)
                if server_caps_data
                else LspServerCapabilitySnapshotV1()
            ),
            client_caps=(
                msgspec.convert(dict(client_caps_data), LspClientCapabilitySnapshotV1)
                if client_caps_data
                else LspClientCapabilitySnapshotV1()
            ),
            experimental_caps=(
                msgspec.convert(dict(experimental_caps_data), LspExperimentalCapabilitySnapshotV1)
                if experimental_caps_data
                else LspExperimentalCapabilitySnapshotV1()
            ),
        )

    health_raw = data.get("workspace_health")
    health: Literal["ok", "warning", "error", "unknown"] = "unknown"
    valid_healths = {"ok", "warning", "error", "unknown"}
    if health_raw in valid_healths:
        health = cast('Literal["ok", "warning", "error", "unknown"]', health_raw)

    refresh_events_raw = _as_sequence(data.get("refresh_events"))
    refresh_events: tuple[str, ...] = ()
    if refresh_events_raw:
        refresh_events = tuple(s for item in refresh_events_raw if isinstance((s := item), str))

    return LspSessionEnvV1(
        server_name=_as_str(data.get("server_name")),
        server_version=_as_str(data.get("server_version")),
        position_encoding=_as_str(data.get("position_encoding")) or "utf-16",
        capabilities=capabilities,
        workspace_health=health,
        quiescent=bool(data.get("quiescent", False)),
        config_fingerprint=_as_str(data.get("config_fingerprint")),
        refresh_events=refresh_events,
    )


def _coerce_lsp_target(data: Mapping[str, object]) -> RustLspTarget | None:
    """Normalize LSP target mapping to RustLspTarget.

    Returns:
    -------
    RustLspTarget | None
        Normalized target or None if invalid.
    """
    uri = _as_str(data.get("uri"))
    if not uri:
        return None

    return RustLspTarget(
        uri=uri,
        range_start_line=_as_int(data.get("range_start_line")),
        range_start_col=_as_int(data.get("range_start_col")),
        range_end_line=_as_int(data.get("range_end_line")),
        range_end_col=_as_int(data.get("range_end_col")),
        container_name=_as_str(data.get("container_name")),
    )


def _coerce_symbol_grounding(data: object) -> RustSymbolGrounding:
    """Normalize symbol grounding data to RustSymbolGrounding.

    Returns:
    -------
    RustSymbolGrounding
        Normalized symbol grounding.
    """
    if not isinstance(data, Mapping):
        return RustSymbolGrounding()
    mapping_data: Mapping[str, object] = data

    def _coerce_targets(key: str) -> tuple[RustLspTarget, ...]:
        raw = _as_sequence(mapping_data.get(key))
        if not raw:
            return ()
        targets = []
        for item in raw:
            mapping = _as_mapping(item)
            if mapping:
                target = _coerce_lsp_target(mapping)
                if target:
                    targets.append(target)
        return tuple(targets)

    return RustSymbolGrounding(
        definitions=_coerce_targets("definitions"),
        type_definitions=_coerce_targets("type_definitions"),
        implementations=_coerce_targets("implementations"),
        references=_coerce_targets("references"),
    )


def _coerce_call_link(data: Mapping[str, object]) -> RustCallLink | None:
    """Normalize call link mapping to RustCallLink.

    Returns:
    -------
    RustCallLink | None
        Normalized call link or None if invalid.
    """
    name = _as_str(data.get("name"))
    if not name:
        return None

    from_ranges_arity = 4
    from_ranges_raw = _as_sequence(data.get("from_ranges"))
    from_ranges: tuple[tuple[int, int, int, int], ...] = ()
    if from_ranges_raw:
        from_ranges = tuple(
            (
                _as_int(item[0]),
                _as_int(item[1]),
                _as_int(item[2]),
                _as_int(item[3]),
            )
            for item in from_ranges_raw
            if isinstance(item, (list, tuple)) and len(item) == from_ranges_arity
        )

    return RustCallLink(
        name=name,
        kind=_as_int(data.get("kind")),
        uri=_as_str(data.get("uri")) or "",
        range_start_line=_as_int(data.get("range_start_line")),
        range_start_col=_as_int(data.get("range_start_col")),
        from_ranges=from_ranges,
    )


def _coerce_call_graph(data: object) -> RustCallGraph:
    """Normalize call graph data to RustCallGraph.

    Returns:
    -------
    RustCallGraph
        Normalized call graph.
    """
    if not isinstance(data, Mapping):
        return RustCallGraph()
    mapping_data: Mapping[str, object] = data

    def _coerce_links(key: str) -> tuple[RustCallLink, ...]:
        raw = _as_sequence(mapping_data.get(key))
        if not raw:
            return ()
        links = []
        for item in raw:
            mapping = _as_mapping(item)
            if mapping:
                link = _coerce_call_link(mapping)
                if link:
                    links.append(link)
        return tuple(links)

    return RustCallGraph(
        incoming_callers=_coerce_links("incoming_callers"),
        outgoing_callees=_coerce_links("outgoing_callees"),
    )


def _coerce_type_link(data: Mapping[str, object]) -> RustTypeLink | None:
    """Normalize type link mapping to RustTypeLink.

    Returns:
    -------
    RustTypeLink | None
        Normalized type link or None if invalid.
    """
    name = _as_str(data.get("name"))
    if not name:
        return None

    return RustTypeLink(
        name=name,
        kind=_as_int(data.get("kind")),
        uri=_as_str(data.get("uri")) or "",
        range_start_line=_as_int(data.get("range_start_line")),
        range_start_col=_as_int(data.get("range_start_col")),
    )


def _coerce_type_hierarchy(data: object) -> RustTypeHierarchy:
    """Normalize type hierarchy data to RustTypeHierarchy.

    Returns:
    -------
    RustTypeHierarchy
        Normalized type hierarchy.
    """
    if not isinstance(data, Mapping):
        return RustTypeHierarchy()
    mapping_data: Mapping[str, object] = data

    def _coerce_links(key: str) -> tuple[RustTypeLink, ...]:
        raw = _as_sequence(mapping_data.get(key))
        if not raw:
            return ()
        links = []
        for item in raw:
            mapping = _as_mapping(item)
            if mapping:
                link = _coerce_type_link(mapping)
                if link:
                    links.append(link)
        return tuple(links)

    return RustTypeHierarchy(
        supertypes=_coerce_links("supertypes"),
        subtypes=_coerce_links("subtypes"),
    )


def _coerce_doc_symbol(data: Mapping[str, object]) -> RustDocSymbol | None:
    """Normalize document symbol mapping to RustDocSymbol.

    Returns:
    -------
    RustDocSymbol | None
        Normalized document symbol or None if invalid.
    """
    name = _as_str(data.get("name"))
    if not name:
        return None

    children_raw = _as_sequence(data.get("children"))
    children: tuple[RustDocSymbol, ...] = ()
    if children_raw:
        children_list = []
        for item in children_raw:
            mapping = _as_mapping(item)
            if mapping:
                child = _coerce_doc_symbol(mapping)
                if child:
                    children_list.append(child)
        children = tuple(children_list)

    return RustDocSymbol(
        name=name,
        kind=_as_int(data.get("kind")),
        range_start_line=_as_int(data.get("range_start_line")),
        range_start_col=_as_int(data.get("range_start_col")),
        range_end_line=_as_int(data.get("range_end_line")),
        range_end_col=_as_int(data.get("range_end_col")),
        children=children,
    )


def _coerce_document_symbols(data: object) -> tuple[RustDocSymbol, ...]:
    """Normalize document symbols list.

    Returns:
    -------
    tuple[RustDocSymbol, ...]
        Tuple of normalized document symbols.
    """
    if not isinstance(data, (list, tuple)):
        return ()
    symbols = []
    for item in data:
        mapping = _as_mapping(item)
        if mapping:
            symbol = _coerce_doc_symbol(mapping)
            if symbol:
                symbols.append(symbol)
    return tuple(symbols)


def _coerce_diagnostic(data: Mapping[str, object]) -> RustDiagnosticV1 | None:
    """Normalize diagnostic mapping to RustDiagnosticV1.

    Returns:
    -------
    RustDiagnosticV1 | None
        Normalized diagnostic or None if invalid.
    """
    uri = _as_str(data.get("uri"))
    if not uri:
        return None

    related_info_raw = _as_sequence(data.get("related_info"))
    related_info: tuple[dict[str, object], ...] = ()
    if related_info_raw:
        info_list = []
        for item in related_info_raw:
            mapping = _as_mapping(item)
            if mapping:
                info_list.append(dict(mapping))
        related_info = tuple(info_list)

    data_raw = _as_mapping(data.get("data"))
    data_dict = dict(data_raw) if data_raw else None

    return RustDiagnosticV1(
        uri=uri,
        range_start_line=_as_int(data.get("range_start_line")),
        range_start_col=_as_int(data.get("range_start_col")),
        range_end_line=_as_int(data.get("range_end_line")),
        range_end_col=_as_int(data.get("range_end_col")),
        severity=_as_int(data.get("severity")),
        code=_as_str(data.get("code")),
        source=_as_str(data.get("source")),
        message=_as_str(data.get("message")) or "",
        related_info=related_info,
        data=data_dict,
    )


def _coerce_diagnostics(data: object) -> tuple[RustDiagnosticV1, ...]:
    """Normalize diagnostics list.

    Returns:
    -------
    tuple[RustDiagnosticV1, ...]
        Tuple of normalized diagnostics.
    """
    if not isinstance(data, (list, tuple)):
        return ()
    diagnostics = []
    for item in data:
        mapping = _as_mapping(item)
        if mapping:
            diagnostic = _coerce_diagnostic(mapping)
            if diagnostic:
                diagnostics.append(diagnostic)
    return tuple(diagnostics)


def coerce_rust_lsp_payload(
    payload: Mapping[str, object] | None,
) -> RustLspEnrichmentPayload:
    """Normalize loose Rust LSP payload to typed contracts.

    Mirrors coerce_pyrefly_payload() pattern exactly.
    Guarantees type-safe RustLspEnrichmentPayload even with partial data.

    Parameters
    ----------
    payload
        Raw payload mapping from LSP enrichment session.

    Returns:
    -------
    RustLspEnrichmentPayload
        Typed enrichment payload with defaults for missing sections.
    """
    if payload is None:
        return RustLspEnrichmentPayload()

    # Extract and normalize each section
    session_env = _coerce_session_env(payload.get("session_env"))
    symbol_grounding = _coerce_symbol_grounding(payload.get("symbol_grounding"))
    call_graph = _coerce_call_graph(payload.get("call_graph"))
    type_hierarchy = _coerce_type_hierarchy(payload.get("type_hierarchy"))
    document_symbols = _coerce_document_symbols(payload.get("document_symbols"))
    diagnostics = _coerce_diagnostics(payload.get("diagnostics"))
    hover_text = payload.get("hover_text")
    advanced_planes = payload.get("advanced_planes")

    return RustLspEnrichmentPayload(
        session_env=session_env,
        symbol_grounding=symbol_grounding,
        call_graph=call_graph,
        type_hierarchy=type_hierarchy,
        document_symbols=document_symbols,
        diagnostics=diagnostics,
        hover_text=hover_text if isinstance(hover_text, str) else None,
        advanced_planes=dict(advanced_planes) if isinstance(advanced_planes, Mapping) else {},
    )


def rust_lsp_payload_to_dict(
    payload: RustLspEnrichmentPayload,
) -> dict[str, object]:
    """Convert typed payload back to dict for enrichment pipeline.

    Matches pyrefly_payload_to_dict() pattern.

    Parameters
    ----------
    payload
        Typed Rust LSP enrichment payload.

    Returns:
    -------
    dict[str, object]
        Builtin dict suitable for CQ result details.
    """
    return cast("dict[str, object]", to_builtins(payload))


__all__ = [
    "LspCapabilitySnapshotV1",
    "LspClientCapabilitySnapshotV1",
    "LspClientPublishDiagnosticsCapsV1",
    "LspExperimentalCapabilitySnapshotV1",
    "LspServerCapabilitySnapshotV1",
    "LspSessionEnvV1",
    "RustCallGraph",
    "RustCallLink",
    "RustDiagnosticV1",
    "RustDocSymbol",
    "RustLspEnrichmentPayload",
    "RustLspTarget",
    "RustSymbolGrounding",
    "RustTypeHierarchy",
    "RustTypeLink",
    "coerce_rust_lsp_payload",
    "rust_lsp_payload_to_dict",
]
