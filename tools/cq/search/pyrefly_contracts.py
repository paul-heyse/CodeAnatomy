"""Typed Pyrefly enrichment contracts for CQ search pipelines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

import msgspec

from tools.cq.core.serialization import to_builtins
from tools.cq.core.structs import CqStruct

LspSeverity = str
_SIGNATURE_LABEL_SPAN_ARITY = 2


def _as_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    return default


def _as_str(value: object) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _as_mapping(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    return None


def _as_sequence(value: object) -> Sequence[object] | None:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return cast("Sequence[object]", value)
    return None


class PyreflyTarget(CqStruct, frozen=True):
    """Location target row emitted by grounding endpoints."""

    kind: str = ""
    uri: str | None = None
    file: str | None = None
    line: int = 0
    col: int = 0
    end_line: int = 0
    end_col: int = 0

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> PyreflyTarget:
        """Build a target row from a loosely typed mapping.

        Returns:
            A normalized target row.
        """
        return cls(
            kind=str(payload.get("kind", "")),
            uri=_as_str(payload.get("uri")),
            file=_as_str(payload.get("file")),
            line=_as_int(payload.get("line")),
            col=_as_int(payload.get("col")),
            end_line=_as_int(payload.get("end_line")),
            end_col=_as_int(payload.get("end_col")),
        )


class PyreflySymbolGrounding(CqStruct, frozen=True):
    """Grounding targets for one anchor."""

    definition_targets: list[PyreflyTarget] = msgspec.field(default_factory=list)
    declaration_targets: list[PyreflyTarget] = msgspec.field(default_factory=list)
    type_definition_targets: list[PyreflyTarget] = msgspec.field(default_factory=list)
    implementation_targets: list[PyreflyTarget] = msgspec.field(default_factory=list)


class PyreflySignatureParameter(CqStruct, frozen=True):
    """Signature parameter descriptor."""

    label: str = ""
    label_span_in_signature: list[int] | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> PyreflySignatureParameter | None:
        """Build a signature parameter row when the label field is present.

        Returns:
            A normalized parameter row, or `None` when the source row is invalid.
        """
        label = _as_str(payload.get("label"))
        if label is None:
            return None
        span: list[int] | None = None
        raw_span = _as_sequence(payload.get("label_span_in_signature"))
        if raw_span and len(raw_span) == _SIGNATURE_LABEL_SPAN_ARITY:
            span = [_as_int(raw_span[0], -1), _as_int(raw_span[1], -1)]
            if span[0] < 0 or span[1] < 0:
                span = None
        return cls(label=label, label_span_in_signature=span)


class PyreflyCallGraphEdge(CqStruct, frozen=True):
    """Incoming/outgoing call graph row."""

    symbol: str = ""
    file: str | None = None
    line: int = 0
    col: int = 0

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> PyreflyCallGraphEdge | None:
        """Build a call graph edge row when the symbol field is present.

        Returns:
            A normalized call graph edge, or `None` when the source row is invalid.
        """
        symbol = _as_str(payload.get("symbol"))
        if symbol is None:
            return None
        return cls(
            symbol=symbol,
            file=_as_str(payload.get("file")),
            line=_as_int(payload.get("line")),
            col=_as_int(payload.get("col")),
        )


class PyreflyReferenceLocation(CqStruct, frozen=True):
    """Reference row for same-scope context."""

    file: str | None = None
    line: int = 0
    col: int = 0

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> PyreflyReferenceLocation:
        """Build a reference location from a loosely typed mapping.

        Returns:
            A normalized reference location row.
        """
        return cls(
            file=_as_str(payload.get("file")),
            line=_as_int(payload.get("line")),
            col=_as_int(payload.get("col")),
        )


class PyreflyTypeContract(CqStruct, frozen=True):
    """Resolved type/call signature contract."""

    resolved_type: str | None = None
    callable_signature: str | None = None
    parameters: list[PyreflySignatureParameter] = msgspec.field(default_factory=list)
    active_signature_index: int = 0
    active_parameter_index: int = 0
    return_type: str | None = None
    generic_params: list[str] = msgspec.field(default_factory=list)
    is_async: bool = False
    is_generator: bool = False
    hover_summary: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> PyreflyTypeContract:
        """Build a typed type-contract payload from a loosely typed mapping.

        Returns:
            A normalized type contract payload.
        """
        if payload is None:
            return cls()
        params: list[PyreflySignatureParameter] = []
        raw_params = _as_sequence(payload.get("parameters"))
        if raw_params:
            for item in raw_params:
                row = PyreflySignatureParameter.from_mapping(_as_mapping(item) or {})
                if row is not None:
                    params.append(row)
        generic_params: list[str] = []
        raw_generic = _as_sequence(payload.get("generic_params"))
        if raw_generic:
            generic_params = [text for item in raw_generic if (text := _as_str(item)) is not None]
        return cls(
            resolved_type=_as_str(payload.get("resolved_type")),
            callable_signature=_as_str(payload.get("callable_signature")),
            parameters=params,
            active_signature_index=_as_int(payload.get("active_signature_index")),
            active_parameter_index=_as_int(payload.get("active_parameter_index")),
            return_type=_as_str(payload.get("return_type")),
            generic_params=generic_params,
            is_async=bool(payload.get("is_async", False)),
            is_generator=bool(payload.get("is_generator", False)),
            hover_summary=_as_str(payload.get("hover_summary")),
        )


class PyreflyCallGraph(CqStruct, frozen=True):
    """Incoming and outgoing call slices."""

    incoming_callers: list[PyreflyCallGraphEdge] = msgspec.field(default_factory=list)
    outgoing_callees: list[PyreflyCallGraphEdge] = msgspec.field(default_factory=list)
    incoming_total: int = 0
    outgoing_total: int = 0

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> PyreflyCallGraph:
        """Build a typed call-graph payload from a loosely typed mapping.

        Returns:
            A normalized call graph payload.
        """
        if payload is None:
            return cls()
        payload_map: Mapping[str, object] = payload

        def _edges(key: str) -> list[PyreflyCallGraphEdge]:
            rows: list[PyreflyCallGraphEdge] = []
            raw_rows = _as_sequence(payload_map.get(key))
            if raw_rows is None:
                return rows
            for item in raw_rows:
                edge = PyreflyCallGraphEdge.from_mapping(_as_mapping(item) or {})
                if edge is not None:
                    rows.append(edge)
            return rows

        incoming = _edges("incoming_callers")
        outgoing = _edges("outgoing_callees")
        incoming_total = _as_int(payload_map.get("incoming_total"), len(incoming))
        outgoing_total = _as_int(payload_map.get("outgoing_total"), len(outgoing))
        return cls(
            incoming_callers=incoming,
            outgoing_callees=outgoing,
            incoming_total=incoming_total,
            outgoing_total=outgoing_total,
        )


class PyreflyClassMethodContext(CqStruct, frozen=True):
    """Class and override context around the anchor."""

    enclosing_class: str | None = None
    base_classes: list[str] = msgspec.field(default_factory=list)
    overridden_methods: list[str] = msgspec.field(default_factory=list)
    overriding_methods: list[str] = msgspec.field(default_factory=list)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> PyreflyClassMethodContext:
        """Build class/method context fields from a loosely typed mapping.

        Returns:
            A normalized class/method context payload.
        """
        if payload is None:
            return cls()
        payload_map: Mapping[str, object] = payload

        def _string_list(key: str) -> list[str]:
            values = _as_sequence(payload_map.get(key))
            if values is None:
                return []
            return [text for item in values if (text := _as_str(item)) is not None]

        return cls(
            enclosing_class=_as_str(payload_map.get("enclosing_class")),
            base_classes=_string_list("base_classes"),
            overridden_methods=_string_list("overridden_methods"),
            overriding_methods=_string_list("overriding_methods"),
        )


class PyreflyLocalScopeContext(CqStruct, frozen=True):
    """Local scope neighbors and references for one anchor."""

    same_scope_symbols: list[object] = msgspec.field(default_factory=list)
    nearest_assignments: list[object] = msgspec.field(default_factory=list)
    narrowing_hints: list[object] = msgspec.field(default_factory=list)
    reference_locations: list[PyreflyReferenceLocation] = msgspec.field(default_factory=list)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> PyreflyLocalScopeContext:
        """Build same-scope context fields from a loosely typed mapping.

        Returns:
            A normalized local-scope context payload.
        """
        if payload is None:
            return cls()
        payload_map: Mapping[str, object] = payload

        def _objects(key: str) -> list[object]:
            values = _as_sequence(payload_map.get(key))
            return list(values) if values is not None else []

        refs: list[PyreflyReferenceLocation] = []
        raw_refs = _as_sequence(payload_map.get("reference_locations"))
        if raw_refs:
            for item in raw_refs:
                mapping = _as_mapping(item)
                if mapping is None:
                    continue
                refs.append(PyreflyReferenceLocation.from_mapping(mapping))

        return cls(
            same_scope_symbols=_objects("same_scope_symbols"),
            nearest_assignments=_objects("nearest_assignments"),
            narrowing_hints=_objects("narrowing_hints"),
            reference_locations=refs,
        )


class PyreflyImportAliasResolution(CqStruct, frozen=True):
    """Import path and alias chain."""

    resolved_path: str | None = None
    alias_chain: list[str] = msgspec.field(default_factory=list)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> PyreflyImportAliasResolution:
        """Build import alias-resolution fields from a loosely typed mapping.

        Returns:
            A normalized import alias-resolution payload.
        """
        if payload is None:
            return cls()
        alias_chain: list[str] = []
        raw_alias_chain = _as_sequence(payload.get("alias_chain"))
        if raw_alias_chain:
            alias_chain = [text for item in raw_alias_chain if (text := _as_str(item)) is not None]
        return cls(
            resolved_path=_as_str(payload.get("resolved_path")),
            alias_chain=alias_chain,
        )


class PyreflyAnchorDiagnostic(CqStruct, frozen=True):
    """Anchor-intersecting diagnostic entry."""

    severity: LspSeverity = "info"
    code: str = ""
    message: str = ""
    line: int = 0
    col: int = 0

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> PyreflyAnchorDiagnostic | None:
        """Build an anchor diagnostic row when the message field is present.

        Returns:
            A normalized diagnostic row, or `None` when the source row is invalid.
        """
        if payload is None:
            return None
        message = _as_str(payload.get("message"))
        if message is None:
            return None
        severity = _as_str(payload.get("severity")) or "info"
        return cls(
            severity=severity,
            code=_as_str(payload.get("code")) or "",
            message=message,
            line=_as_int(payload.get("line")),
            col=_as_int(payload.get("col")),
        )


class PyreflyCoverage(CqStruct, frozen=True):
    """Coverage status for pyrefly enrichment fields."""

    status: str = "not_resolved"
    reason: str | None = None
    position_encoding: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> PyreflyCoverage:
        """Build coverage metadata from a loosely typed mapping.

        Returns:
            A normalized coverage payload.
        """
        if payload is None:
            return cls()
        return cls(
            status=_as_str(payload.get("status")) or "not_resolved",
            reason=_as_str(payload.get("reason")),
            position_encoding=_as_str(payload.get("position_encoding")),
        )


class PyreflyEnrichmentPayload(CqStruct, frozen=True):
    """Complete Pyrefly payload for one anchor."""

    symbol_grounding: PyreflySymbolGrounding = msgspec.field(default_factory=PyreflySymbolGrounding)
    type_contract: PyreflyTypeContract = msgspec.field(default_factory=PyreflyTypeContract)
    call_graph: PyreflyCallGraph = msgspec.field(default_factory=PyreflyCallGraph)
    class_method_context: PyreflyClassMethodContext = msgspec.field(
        default_factory=PyreflyClassMethodContext
    )
    local_scope_context: PyreflyLocalScopeContext = msgspec.field(
        default_factory=PyreflyLocalScopeContext
    )
    import_alias_resolution: PyreflyImportAliasResolution = msgspec.field(
        default_factory=PyreflyImportAliasResolution
    )
    anchor_diagnostics: list[PyreflyAnchorDiagnostic] = msgspec.field(default_factory=list)
    coverage: PyreflyCoverage = msgspec.field(default_factory=PyreflyCoverage)
    advanced_planes: dict[str, object] = msgspec.field(default_factory=dict)


def _coerce_targets(payload: Mapping[str, object] | None, key: str) -> list[PyreflyTarget]:
    if payload is None:
        return []
    raw_rows = _as_sequence(payload.get(key))
    if raw_rows is None:
        return []
    rows: list[PyreflyTarget] = []
    for item in raw_rows:
        mapping = _as_mapping(item)
        if mapping is None:
            continue
        rows.append(PyreflyTarget.from_mapping(mapping))
    return rows


def coerce_pyrefly_payload(
    payload: Mapping[str, object] | None,
) -> PyreflyEnrichmentPayload:
    """Normalize a loose Pyrefly payload to typed contracts.

    Returns:
        A fully typed payload with defaults for missing sections.
    """
    if payload is None:
        return PyreflyEnrichmentPayload()
    symbol_grounding = _as_mapping(payload.get("symbol_grounding"))
    diagnostics: list[PyreflyAnchorDiagnostic] = []
    raw_diagnostics = _as_sequence(payload.get("anchor_diagnostics"))
    if raw_diagnostics:
        for item in raw_diagnostics:
            row = PyreflyAnchorDiagnostic.from_mapping(_as_mapping(item))
            if row is not None:
                diagnostics.append(row)
    return PyreflyEnrichmentPayload(
        symbol_grounding=PyreflySymbolGrounding(
            definition_targets=_coerce_targets(symbol_grounding, "definition_targets"),
            declaration_targets=_coerce_targets(symbol_grounding, "declaration_targets"),
            type_definition_targets=_coerce_targets(symbol_grounding, "type_definition_targets"),
            implementation_targets=_coerce_targets(symbol_grounding, "implementation_targets"),
        ),
        type_contract=PyreflyTypeContract.from_mapping(_as_mapping(payload.get("type_contract"))),
        call_graph=PyreflyCallGraph.from_mapping(_as_mapping(payload.get("call_graph"))),
        class_method_context=PyreflyClassMethodContext.from_mapping(
            _as_mapping(payload.get("class_method_context"))
        ),
        local_scope_context=PyreflyLocalScopeContext.from_mapping(
            _as_mapping(payload.get("local_scope_context"))
        ),
        import_alias_resolution=PyreflyImportAliasResolution.from_mapping(
            _as_mapping(payload.get("import_alias_resolution"))
        ),
        anchor_diagnostics=diagnostics,
        coverage=PyreflyCoverage.from_mapping(_as_mapping(payload.get("coverage"))),
        advanced_planes=(
            dict(planes)
            if isinstance((planes := _as_mapping(payload.get("advanced_planes"))), Mapping)
            else {}
        ),
    )


def pyrefly_payload_to_dict(payload: PyreflyEnrichmentPayload) -> dict[str, object]:
    """Serialize typed payload to builtins mapping for result details.

    Returns:
        A builtin `dict[str, object]` suitable for CQ result details.
    """
    return cast("dict[str, object]", to_builtins(payload))


__all__ = [
    "PyreflyAnchorDiagnostic",
    "PyreflyCallGraph",
    "PyreflyCallGraphEdge",
    "PyreflyClassMethodContext",
    "PyreflyCoverage",
    "PyreflyEnrichmentPayload",
    "PyreflyImportAliasResolution",
    "PyreflyLocalScopeContext",
    "PyreflyReferenceLocation",
    "PyreflySignatureParameter",
    "PyreflySymbolGrounding",
    "PyreflyTarget",
    "PyreflyTypeContract",
    "coerce_pyrefly_payload",
    "pyrefly_payload_to_dict",
]
