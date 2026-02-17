"""Typed summary contracts for CQ results."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any

import msgspec


class SemanticTelemetryV1(msgspec.Struct, frozen=True, omit_defaults=True):
    """Canonical semantic telemetry counters."""

    attempted: int = 0
    applied: int = 0
    failed: int = 0
    skipped: int = 0
    timed_out: int = 0


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
    -------
    SemanticTelemetryV1
        Normalized telemetry counters with non-negative values.
    """
    return SemanticTelemetryV1(
        attempted=max(0, attempted),
        applied=max(0, applied),
        failed=max(0, failed, 0, attempted - applied),
        skipped=max(0, skipped),
        timed_out=max(0, timed_out),
    )


class CqSummary(msgspec.Struct, omit_defaults=True):
    """Typed summary payload for :class:`tools.cq.core.schema.CqResult`."""

    schema_version: int = 1
    matches: int = 0
    files_scanned: int = 0
    total_matches: int = 0
    matched_files: int = 0
    scanned_files: int = 0
    query_text: str | None = None
    query: str | None = None
    pattern: str | None = None
    pattern_context: str | None = None
    pattern_selector: str | None = None
    lang: str | None = None
    mode: str | None = None
    mode_requested: str | None = None
    mode_effective: str | None = None
    mode_chain: list[str] = msgspec.field(default_factory=list)
    lang_scope: str | None = None
    language: str | None = None
    plan_version: int | None = None
    file_filters: list[str] = msgspec.field(default_factory=list)
    file_globs: list[str] = msgspec.field(default_factory=list)
    include: list[str] = msgspec.field(default_factory=list)
    exclude: list[str] = msgspec.field(default_factory=list)
    context_lines: dict[str, int] = msgspec.field(default_factory=dict)
    fallback_applied: bool | None = None
    limit: int | None = None
    steps: list[str] = msgspec.field(default_factory=list)
    language_order: list[str] = msgspec.field(default_factory=list)
    languages: dict[str, object] = msgspec.field(default_factory=dict)
    step_summaries: dict[str, dict[str, object]] = msgspec.field(default_factory=dict)
    macro_summaries: dict[str, dict[str, object]] = msgspec.field(default_factory=dict)
    plan: dict[str, object] = msgspec.field(default_factory=dict)
    python_semantic_telemetry: SemanticTelemetryV1 | None = None
    rust_semantic_telemetry: SemanticTelemetryV1 | None = None
    python_semantic_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    python_semantic_overview: dict[str, object] = msgspec.field(default_factory=dict)
    semantic_planes: dict[str, object] = msgspec.field(default_factory=dict)
    language_capabilities: dict[str, object] = msgspec.field(default_factory=dict)
    cross_language_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    enrichment_telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    query_pack_lint: dict[str, object] = msgspec.field(default_factory=dict)
    search_stage_timings_ms: dict[str, float] = msgspec.field(default_factory=dict)
    tree_sitter_neighborhood: dict[str, object] = msgspec.field(default_factory=dict)
    cache_backend: dict[str, object] = msgspec.field(default_factory=dict)
    cache_maintenance: dict[str, object] = msgspec.field(default_factory=dict)
    front_door_insight: dict[str, object] | None = None
    dropped_by_scope: object | None = None
    scanned_files_is_estimate: bool | None = None
    returned_matches: int = 0
    timed_out: bool = False
    truncated: bool = False
    caps_hit: str | None = None
    case_sensitive: bool | None = None
    with_neighborhood: bool | None = None
    scan_method: str | None = None
    rg_pcre2_available: bool | None = None
    rg_pcre2_version: str | None = None
    rg_stats: dict[str, object] = msgspec.field(default_factory=dict)
    entity_kind: str | None = None
    is_pattern_query: bool | None = None
    would_break: int = 0
    ambiguous: int = 0
    ok: int = 0
    total_defs: int = 0
    total_calls: int = 0
    total_imports: int = 0
    total_slices: int = 0
    total_diagnostics: int = 0
    total_nodes: int = 0
    total_edges: int = 0
    resolved_objects: int = 0
    resolved_occurrences: int = 0
    target_resolution_kind: str | None = None
    target: str | None = None
    target_file: str | None = None
    target_line: int | None = None
    target_name: str | None = None
    top_k: int | None = None
    enable_semantic_enrichment: bool | None = None
    bundle_id: str | None = None
    semantic_health: object | None = None
    semantic_quiescent: object | None = None
    semantic_position_encoding: object | None = None
    module_filter: str | None = None
    cycles_found: int = 0
    function: str | None = None
    signature: str | None = None
    parameter: str | None = None
    new_signature: str | None = None
    status: str | None = None
    symbol: str | None = None
    in_dir: str | None = None
    total_sites: int = 0
    files_with_calls: int = 0
    total_py_files: int = 0
    candidate_files: int = 0
    rg_candidates: int = 0
    fallback_count: int = 0
    call_records: int = 0
    callers_found: int = 0
    functions_analyzed: int = 0
    files_analyzed: int = 0
    scope_file_count: int = 0
    scope_filter_applied: bool | None = None
    taint_sites: int = 0
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
    max_depth: int = 0
    call_sites: int = 0
    reraises: int = 0
    bundle: dict[str, object] | None = None
    skipped: str | None = None
    error: str | None = None

    @classmethod
    def field_names(cls) -> frozenset[str]:
        """Return the canonical set of summary field names."""
        return frozenset(cls.__struct_fields__)

    def _set_known_field(self, key: str, value: object) -> None:
        if key not in self.__struct_fields__:
            msg = f"Unknown summary key: {key}"
            raise KeyError(msg)
        setattr(self, key, value)

    def __getitem__(self, key: str) -> object:
        """Return the summary value for *key*.

        Raises:
            KeyError: If *key* is not a known summary field.
        """
        if key not in self.__struct_fields__:
            msg = f"Unknown summary key: {key}"
            raise KeyError(msg)
        return getattr(self, key)

    def __setitem__(self, key: str, value: object) -> None:
        """Set the summary value for *key*."""
        self._set_known_field(key, value)

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
        -------
        Iterator[str]
            Iterator over canonical summary field names.
        """
        return iter(self.__struct_fields__)

    def __len__(self) -> int:
        """Return the number of summary fields."""
        return len(self.__struct_fields__)

    def update(self, mapping: Mapping[str, object] | Iterable[tuple[str, object]]) -> None:
        """Update summary fields from mapping/iterable input.

        Raises:
            TypeError: If iterable entries are not ``(str, value)`` pairs.
        """
        if isinstance(mapping, Mapping):
            for key_obj, value in mapping.items():
                if not isinstance(key_obj, str):
                    msg = "Summary update keys must be strings."
                    raise TypeError(msg)
                self._set_known_field(key_obj, value)
            return
        for key, value in mapping:
            if not isinstance(key, str):
                msg = "Summary update keys must be strings."
                raise TypeError(msg)
            self._set_known_field(key, value)

    def to_dict(self) -> dict[str, Any]:
        """Convert to deterministic builtins mapping.

        Returns:
        -------
        dict[str, Any]
            Deterministic mapping representation of the summary.
        """
        return msgspec.to_builtins(self, str_keys=True, order="deterministic")


def coerce_semantic_telemetry(value: object) -> SemanticTelemetryV1 | None:
    """Normalize semantic telemetry payloads into ``SemanticTelemetryV1``.

    Returns:
    -------
    SemanticTelemetryV1 | None
        Parsed telemetry payload when valid; otherwise ``None``.
    """
    if isinstance(value, SemanticTelemetryV1):
        return value
    if isinstance(value, Mapping):
        attempted = value.get("attempted")
        applied = value.get("applied")
        failed = value.get("failed")
        skipped = value.get("skipped")
        timed_out = value.get("timed_out")
        if isinstance(attempted, int) and isinstance(applied, int) and isinstance(failed, int):
            return build_semantic_telemetry(
                attempted=attempted,
                applied=applied,
                failed=failed,
                skipped=skipped if isinstance(skipped, int) else 0,
                timed_out=timed_out if isinstance(timed_out, int) else 0,
            )
    return None


def summary_from_mapping(value: CqSummary | Mapping[str, object] | None) -> CqSummary:
    """Build ``CqSummary`` from mapping-like payloads.

    Returns:
    -------
    CqSummary
        Normalized summary object.
    """
    if isinstance(value, CqSummary):
        return value
    summary = CqSummary()
    if not isinstance(value, Mapping):
        return summary
    summary.update(value)
    summary.python_semantic_telemetry = coerce_semantic_telemetry(summary.python_semantic_telemetry)
    summary.rust_semantic_telemetry = coerce_semantic_telemetry(summary.rust_semantic_telemetry)
    return summary


def extract_match_count(result: object | None) -> int:
    """Extract canonical match count from a result-like object.

    Returns:
    -------
    int
        Match count resolved as ``total_matches`` -> ``matches`` -> key findings length.
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
    "CqSummary",
    "SemanticTelemetryV1",
    "build_semantic_telemetry",
    "coerce_semantic_telemetry",
    "extract_match_count",
    "summary_from_mapping",
]
