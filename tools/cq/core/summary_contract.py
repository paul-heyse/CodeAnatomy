"""Typed summary contracts for CQ results."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Literal, cast

import msgspec

type SummaryVariantName = Literal["search", "calls", "impact", "run", "neighborhood"]


class SemanticTelemetryV1(msgspec.Struct, frozen=True, omit_defaults=True):
    """Canonical semantic telemetry counters."""

    attempted: int = 0
    applied: int = 0
    failed: int = 0
    skipped: int = 0
    timed_out: int = 0


class SummaryEnvelopeV1(msgspec.Struct, omit_defaults=True):
    """Mode-tagged summary envelope for :class:`tools.cq.core.schema.CqResult`."""

    schema_version: int = 1
    summary_variant: SummaryVariantName = "search"
    matches: int = 0
    files_scanned: int = 0
    total_matches: int = 0
    matched_files: int = 0
    scanned_files: int = 0
    scanned_files_is_estimate: bool | None = None
    returned_matches: int = 0
    timed_out: bool = False
    truncated: bool = False
    caps_hit: str | None = None
    query_text: str | None = None
    query: str | None = None
    lang: str | None = None
    mode: str | None = None
    lang_scope: str | None = None
    language: str | None = None
    file_filters: list[str] = msgspec.field(default_factory=list)
    language_order: list[str] = msgspec.field(default_factory=list)
    languages: dict[str, object] = msgspec.field(default_factory=dict)
    steps: list[str] = msgspec.field(default_factory=list)
    step_summaries: dict[str, dict[str, object]] = msgspec.field(default_factory=dict)
    macro_summaries: dict[str, dict[str, object]] = msgspec.field(default_factory=dict)
    python_semantic_telemetry: SemanticTelemetryV1 | None = None
    rust_semantic_telemetry: SemanticTelemetryV1 | None = None
    python_semantic_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    python_semantic_overview: dict[str, object] = msgspec.field(default_factory=dict)
    semantic_planes: dict[str, object] = msgspec.field(default_factory=dict)
    language_capabilities: dict[str, object] = msgspec.field(default_factory=dict)
    cross_language_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    cache_backend: dict[str, object] = msgspec.field(default_factory=dict)
    front_door_insight: dict[str, object] | None = None
    target: str | None = None
    target_file: str | None = None
    target_line: int | None = None
    target_name: str | None = None
    skipped: str | None = None
    error: str | None = None

    @classmethod
    def field_names(cls) -> frozenset[str]:
        """Return the canonical set of summary field names."""
        return frozenset(cls.__struct_fields__)

    def __getitem__(self, key: str) -> object:
        """Return the summary value for *key*.

        Raises:
            KeyError: If *key* is not part of the summary schema.
        """
        if key not in self.__struct_fields__:
            msg = f"Unknown summary key: {key}"
            raise KeyError(msg)
        return getattr(self, key)

    def get(self, key: str, default: object | None = None) -> object | None:
        """Return *key* when present and non-``None``, else *default*."""
        if key not in self.__struct_fields__:
            return default
        value = getattr(self, key)
        return default if value is None else value

    def __contains__(self, key: object) -> bool:
        """Return whether *key* exists in summary fields."""
        return isinstance(key, str) and key in self.__struct_fields__

    def keys(self) -> tuple[str, ...]:
        """Return summary field names."""
        return tuple(self.__struct_fields__)

    def values(self) -> tuple[object, ...]:
        """Return summary field values."""
        return tuple(getattr(self, key) for key in self.__struct_fields__)

    def items(self) -> tuple[tuple[str, object], ...]:
        """Return summary field ``(name, value)`` pairs."""
        return tuple((key, getattr(self, key)) for key in self.__struct_fields__)

    def __iter__(self) -> Iterator[str]:  # type: ignore[override]
        """Iterate summary field names.

        Returns:
            Iterator[str]: Iterator over summary field names.
        """
        return iter(self.__struct_fields__)

    def __len__(self) -> int:
        """Return the number of summary fields."""
        return len(self.__struct_fields__)

    def to_dict(self) -> dict[str, Any]:
        """Convert to deterministic builtins mapping.

        Returns:
            dict[str, Any]: Deterministic mapping representation.
        """
        payload = msgspec.to_builtins(self, str_keys=True, order="deterministic")
        payload["summary_variant"] = self.summary_variant
        return payload


class SearchSummaryV1(SummaryEnvelopeV1):
    """Search/query-focused summary payload."""

    summary_variant: SummaryVariantName = "search"
    pattern: str | None = None
    pattern_context: str | None = None
    pattern_selector: str | None = None
    mode_requested: str | None = None
    mode_effective: str | None = None
    mode_chain: list[str] = msgspec.field(default_factory=list)
    file_globs: list[str] = msgspec.field(default_factory=list)
    include: list[str] = msgspec.field(default_factory=list)
    exclude: list[str] = msgspec.field(default_factory=list)
    context_lines: dict[str, int] = msgspec.field(default_factory=dict)
    fallback_applied: bool | None = None
    limit: int | None = None
    plan: dict[str, object] = msgspec.field(default_factory=dict)
    cache_maintenance: dict[str, object] = msgspec.field(default_factory=dict)
    enrichment_telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    query_pack_lint: dict[str, object] = msgspec.field(default_factory=dict)
    search_stage_timings_ms: dict[str, float] = msgspec.field(default_factory=dict)
    tree_sitter_neighborhood: dict[str, object] = msgspec.field(default_factory=dict)
    dropped_by_scope: object | None = None
    case_sensitive: bool | None = None
    with_neighborhood: bool | None = None
    scan_method: str | None = None
    rg_pcre2_available: bool | None = None
    rg_pcre2_version: str | None = None
    rg_stats: dict[str, object] = msgspec.field(default_factory=dict)
    entity_kind: str | None = None
    is_pattern_query: bool | None = None
    total_defs: int = 0
    total_calls: int = 0
    total_imports: int = 0
    resolved_objects: int = 0
    resolved_occurrences: int = 0
    module_filter: str | None = None
    total_files: int = 0
    internal_dependencies: int = 0
    external_dependencies: int = 0
    cycles_found: int = 0
    status: str | None = None
    symbol: str | None = None
    in_dir: str | None = None
    files_analyzed: int = 0
    scope_file_count: int = 0
    scope_filter_applied: bool | None = None
    total_effects: int = 0
    ambient_reads: int = 0
    global_writes: int = 0
    top_level_calls: int = 0
    unique_globals: int = 0
    unique_attrs: int = 0
    bare_excepts: int = 0
    total_raises: int = 0
    total_catches: int = 0
    unique_exception_types: int = 0
    code_objects: int = 0
    reraises: int = 0
    bundle: dict[str, object] | None = None
    scopes_with_captures: int = 0


class CallsSummaryV1(SummaryEnvelopeV1):
    """Calls-focused summary payload."""

    summary_variant: SummaryVariantName = "calls"
    function: str | None = None
    signature: str | None = None
    total_sites: int = 0
    files_with_calls: int = 0
    total_py_files: int = 0
    candidate_files: int = 0
    rg_candidates: int = 0
    fallback_count: int = 0
    call_records: int = 0
    scan_method: str | None = None


class ImpactSummaryV1(SummaryEnvelopeV1):
    """Impact/sig-impact summary payload."""

    summary_variant: SummaryVariantName = "impact"
    function: str | None = None
    parameter: str | None = None
    new_signature: str | None = None
    symbol: str | None = None
    status: str | None = None
    taint_sites: int = 0
    max_depth: int = 0
    functions_analyzed: int = 0
    callers_found: int = 0
    would_break: int = 0
    ambiguous: int = 0
    ok: int = 0
    call_sites: int = 0


class RunSummaryV1(SummaryEnvelopeV1):
    """Run/chain summary payload."""

    summary_variant: SummaryVariantName = "run"
    plan_version: int | None = None
    plan: dict[str, object] = msgspec.field(default_factory=dict)
    cache_maintenance: dict[str, object] = msgspec.field(default_factory=dict)


class NeighborhoodSummaryV1(SummaryEnvelopeV1):
    """Neighborhood summary payload."""

    summary_variant: SummaryVariantName = "neighborhood"
    target_resolution_kind: str | None = None
    top_k: int | None = None
    enable_semantic_enrichment: bool | None = None
    bundle_id: str | None = None
    semantic_health: object | None = None
    semantic_quiescent: object | None = None
    semantic_position_encoding: object | None = None
    total_slices: int = 0
    total_diagnostics: int = 0
    total_nodes: int = 0
    total_edges: int = 0
    bundle: dict[str, object] | None = None


type SummaryV1 = (
    SearchSummaryV1 | CallsSummaryV1 | ImpactSummaryV1 | RunSummaryV1 | NeighborhoodSummaryV1
)


_SUMMARY_VARIANT_BY_NAME: dict[SummaryVariantName, type[SummaryEnvelopeV1]] = {
    "search": SearchSummaryV1,
    "calls": CallsSummaryV1,
    "impact": ImpactSummaryV1,
    "run": RunSummaryV1,
    "neighborhood": NeighborhoodSummaryV1,
}

_MODE_TO_VARIANT_NAME: dict[str, SummaryVariantName] = {
    "search": "search",
    "identifier": "search",
    "regex": "search",
    "literal": "search",
    "entity": "search",
    "pattern": "search",
    "bytecode": "search",
    "imports": "search",
    "exceptions": "search",
    "side-effects": "search",
    "calls": "calls",
    "macro:calls": "calls",
    "impact": "impact",
    "sig-impact": "impact",
    "macro:impact": "impact",
    "macro:sig-impact": "impact",
    "run": "run",
    "chain": "run",
    "neighborhood": "neighborhood",
}

_MACRO_TO_VARIANT_NAME: dict[str, SummaryVariantName] = {
    "search": "search",
    "q": "search",
    "bytecode": "search",
    "imports": "search",
    "exceptions": "search",
    "side-effects": "search",
    "calls": "calls",
    "impact": "impact",
    "sig-impact": "impact",
    "run": "run",
    "chain": "run",
    "neighborhood": "neighborhood",
}


def build_semantic_telemetry(
    *,
    attempted: int,
    applied: int,
    failed: int,
    skipped: int = 0,
    timed_out: int = 0,
) -> SemanticTelemetryV1:
    """Build normalized semantic telemetry payload.

    Returns:
        SemanticTelemetryV1: Normalized telemetry payload.
    """
    return SemanticTelemetryV1(
        attempted=max(0, attempted),
        applied=max(0, applied),
        failed=max(0, failed, 0, attempted - applied),
        skipped=max(0, skipped),
        timed_out=max(0, timed_out),
    )


def coerce_semantic_telemetry(value: object) -> SemanticTelemetryV1 | None:
    """Normalize semantic telemetry payloads into ``SemanticTelemetryV1``.

    Returns:
        SemanticTelemetryV1 | None: Normalized telemetry, when coercion succeeds.
    """
    if isinstance(value, SemanticTelemetryV1):
        return value
    if isinstance(value, Mapping):
        telemetry_keys = {"attempted", "applied", "failed", "skipped", "timed_out"}
        if all(isinstance(key, str) and key in telemetry_keys for key in value):
            attempted_raw = value.get("attempted")
            applied_raw = value.get("applied")
            failed_raw = value.get("failed")
            skipped_raw = value.get("skipped")
            timed_out_raw = value.get("timed_out")
            return build_semantic_telemetry(
                attempted=attempted_raw if isinstance(attempted_raw, int) else 0,
                applied=applied_raw if isinstance(applied_raw, int) else 0,
                failed=failed_raw if isinstance(failed_raw, int) else 0,
                skipped=skipped_raw if isinstance(skipped_raw, int) else 0,
                timed_out=timed_out_raw if isinstance(timed_out_raw, int) else 0,
            )
    return None


def summary_class_for_variant(name: SummaryVariantName) -> type[SummaryEnvelopeV1]:
    """Resolve the concrete summary struct for a variant name.

    Returns:
        type[SummaryEnvelopeV1]: Concrete summary type.
    """
    return _SUMMARY_VARIANT_BY_NAME[name]


def resolve_summary_variant_name(
    *,
    mode: object | None = None,
    macro: str | None = None,
    explicit: object | None = None,
) -> SummaryVariantName:
    """Resolve summary variant from explicit tag, mode, and macro metadata.

    Returns:
        SummaryVariantName: Canonical summary variant name.
    """
    if isinstance(explicit, str):
        normalized = explicit.strip().lower()
        if normalized in _SUMMARY_VARIANT_BY_NAME:
            return cast("SummaryVariantName", normalized)

    if isinstance(mode, str):
        normalized = mode.strip().lower()
        variant = _MODE_TO_VARIANT_NAME.get(normalized)
        if variant is not None:
            return variant

    if isinstance(macro, str):
        normalized_macro = macro.strip().lower()
        return _MACRO_TO_VARIANT_NAME.get(normalized_macro, "search")

    return "search"


def summary_for_variant(name: SummaryVariantName) -> SummaryV1:
    """Instantiate a summary envelope for a variant name.

    Returns:
        SummaryV1: New summary instance for the variant.
    """
    return cast("SummaryV1", summary_class_for_variant(name)())


def summary_for_mode(mode: str | None, *, macro: str | None = None) -> SummaryV1:
    """Instantiate summary envelope inferred from summary mode and macro.

    Returns:
        SummaryEnvelopeV1: New summary instance.
    """
    variant = resolve_summary_variant_name(mode=mode, macro=macro)
    return summary_for_variant(variant)


def summary_for_macro(macro: str | None) -> SummaryV1:
    """Instantiate summary envelope inferred from macro name.

    Returns:
        SummaryEnvelopeV1: New summary instance.
    """
    variant = resolve_summary_variant_name(macro=macro)
    return summary_for_variant(variant)


def summary_from_mapping(
    value: SummaryEnvelopeV1 | Mapping[str, object] | None,
) -> SummaryV1:
    """Build ``SummaryEnvelopeV1`` from mapping-like payloads.

    Returns:
        SummaryEnvelopeV1: Normalized summary instance.
    """
    if isinstance(
        value,
        SearchSummaryV1 | CallsSummaryV1 | ImpactSummaryV1 | RunSummaryV1 | NeighborhoodSummaryV1,
    ):
        return value
    if isinstance(value, SummaryEnvelopeV1):
        return summary_from_mapping(value.to_dict())
    if not isinstance(value, Mapping):
        return summary_for_variant("search")

    variant = resolve_summary_variant_name(
        mode=value.get("mode"),
        explicit=value.get("summary_variant"),
    )
    summary = summary_for_variant(variant)
    apply_summary_mapping(summary, value)
    summary.summary_variant = variant
    summary.python_semantic_telemetry = coerce_semantic_telemetry(summary.python_semantic_telemetry)
    summary.rust_semantic_telemetry = coerce_semantic_telemetry(summary.rust_semantic_telemetry)
    return summary


def as_search_summary(summary: SummaryV1) -> SearchSummaryV1:
    """Return ``SearchSummaryV1`` or raise when variant mismatches.

    Raises:
        TypeError: If ``summary`` is not the ``search`` variant.
    """
    if isinstance(summary, SearchSummaryV1):
        return summary
    msg = f"Expected search summary variant, got {type(summary).__name__}"
    raise TypeError(msg)


def as_calls_summary(summary: SummaryV1) -> CallsSummaryV1:
    """Return ``CallsSummaryV1`` or raise when variant mismatches.

    Raises:
        TypeError: If ``summary`` is not the ``calls`` variant.
    """
    if isinstance(summary, CallsSummaryV1):
        return summary
    msg = f"Expected calls summary variant, got {type(summary).__name__}"
    raise TypeError(msg)


def as_impact_summary(summary: SummaryV1) -> ImpactSummaryV1:
    """Return ``ImpactSummaryV1`` or raise when variant mismatches.

    Raises:
        TypeError: If ``summary`` is not the ``impact`` variant.
    """
    if isinstance(summary, ImpactSummaryV1):
        return summary
    msg = f"Expected impact summary variant, got {type(summary).__name__}"
    raise TypeError(msg)


def as_run_summary(summary: SummaryV1) -> RunSummaryV1:
    """Return ``RunSummaryV1`` or raise when variant mismatches.

    Raises:
        TypeError: If ``summary`` is not the ``run`` variant.
    """
    if isinstance(summary, RunSummaryV1):
        return summary
    msg = f"Expected run summary variant, got {type(summary).__name__}"
    raise TypeError(msg)


def as_neighborhood_summary(summary: SummaryV1) -> NeighborhoodSummaryV1:
    """Return ``NeighborhoodSummaryV1`` or raise when variant mismatches.

    Raises:
        TypeError: If ``summary`` is not the ``neighborhood`` variant.
    """
    if isinstance(summary, NeighborhoodSummaryV1):
        return summary
    msg = f"Expected neighborhood summary variant, got {type(summary).__name__}"
    raise TypeError(msg)


def apply_summary_mapping(
    summary: SummaryEnvelopeV1,
    mapping: Mapping[str, object] | Iterable[tuple[str, object]],
) -> None:
    """Apply a mapping payload onto a typed ``SummaryEnvelopeV1`` instance.

    Raises:
        TypeError: If any update key is not a string.
        KeyError: If an update key is not part of the schema.
    """
    items = mapping.items() if isinstance(mapping, Mapping) else mapping
    for key_obj, value in items:
        if not isinstance(key_obj, str):
            msg = "Summary update keys must be strings."
            raise TypeError(msg)
        if key_obj not in summary.__struct_fields__:
            msg = f"Unknown summary key: {key_obj}"
            raise KeyError(msg)
        setattr(summary, key_obj, value)


def extract_match_count(result: object | None) -> int:
    """Extract canonical match count from a result-like object.

    Returns:
        int: Match count resolved from summary/result fallbacks.
    """
    if result is None:
        return 0
    summary = getattr(result, "summary", None)
    if summary is None:
        return 0
    total = getattr(summary, "total_matches", None)
    if isinstance(total, int):
        return total
    matches = getattr(summary, "matches", None)
    if isinstance(matches, int):
        return matches
    key_findings = getattr(result, "key_findings", ())
    return len(key_findings) if hasattr(key_findings, "__len__") else 0


__all__ = [
    "CallsSummaryV1",
    "ImpactSummaryV1",
    "NeighborhoodSummaryV1",
    "RunSummaryV1",
    "SearchSummaryV1",
    "SemanticTelemetryV1",
    "SummaryEnvelopeV1",
    "SummaryV1",
    "SummaryVariantName",
    "apply_summary_mapping",
    "as_calls_summary",
    "as_impact_summary",
    "as_neighborhood_summary",
    "as_run_summary",
    "as_search_summary",
    "build_semantic_telemetry",
    "coerce_semantic_telemetry",
    "extract_match_count",
    "resolve_summary_variant_name",
    "summary_class_for_variant",
    "summary_for_macro",
    "summary_for_mode",
    "summary_for_variant",
    "summary_from_mapping",
]
