"""Shared enrichment helpers for payload normalization and merge policy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

import msgspec

from tools.cq.search._shared.helpers import encode_mapping
from tools.cq.search.enrichment.contracts import (
    EnrichmentMeta,
    EnrichmentStatus,
)
from tools.cq.search.enrichment.incremental_facts import (
    IncrementalAnchorFacts,
    IncrementalFacts,
)
from tools.cq.search.enrichment.python_facts import (
    PythonBehaviorFacts,
    PythonCallFacts,
    PythonClassShapeFacts,
    PythonEnrichmentFacts,
    PythonImportFacts,
    PythonLocalsFacts,
    PythonParseQualityFacts,
    PythonResolutionFacts,
    PythonSignatureFacts,
    PythonStructureFacts,
)
from tools.cq.search.enrichment.rust_facts import (
    RustDefinitionFacts,
    RustEnrichmentFacts,
    RustMacroExpansionFacts,
    RustStructureFacts,
)

_DEFAULT_METADATA_KEYS: frozenset[str] = frozenset(
    {"enrichment_status", "enrichment_sources", "degrade_reason", "language"}
)


def payload_size_hint(payload: Mapping[str, object]) -> int:
    """Estimate payload size using msgspec JSON encoding.

    Returns:
        Estimated serialized payload size in bytes.
    """
    return len(encode_mapping(dict(payload)))


def has_value(value: object) -> bool:
    """Return whether a payload value is considered non-empty.

    Returns:
        `True` when value is non-empty for merge purposes.
    """
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def string_or_none(value: object) -> str | None:
    """Return stripped string when value is a non-empty string."""
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return None


def coerce_enrichment_status(value: object) -> EnrichmentStatus:
    """Normalize enrichment status to the allowed literal domain.

    Returns:
        One of ``"applied"``, ``"degraded"``, or ``"skipped"``.
    """
    if isinstance(value, str) and value in {"applied", "degraded", "skipped"}:
        return cast("EnrichmentStatus", value)
    return "applied"


def accumulate_runtime_flags(
    *,
    lang_bucket: dict[str, object],
    runtime_payload: object,
) -> None:
    """Accumulate query-runtime flags into one language telemetry bucket."""
    runtime_bucket = lang_bucket.get("query_runtime")
    if not isinstance(runtime_payload, dict) or not isinstance(runtime_bucket, dict):
        return
    if bool(runtime_payload.get("did_exceed_match_limit")):
        runtime_bucket["did_exceed_match_limit"] = (
            int(runtime_bucket.get("did_exceed_match_limit", 0)) + 1
        )
    if bool(runtime_payload.get("cancelled")):
        runtime_bucket["cancelled"] = int(runtime_bucket.get("cancelled", 0)) + 1


def build_tree_sitter_diagnostic_rows(
    rows: object,
    *,
    max_rows: int = 8,
) -> list[dict[str, object]]:
    """Build normalized tree-sitter diagnostic rows.

    Returns:
        list[dict[str, object]]: Normalized diagnostic rows.
    """
    if not isinstance(rows, list):
        return []
    return [
        {
            "kind": string_or_none(item.get("kind")) or "tree_sitter",
            "message": string_or_none(item.get("message")) or "tree-sitter diagnostic",
            "line": item.get("start_line"),
            "col": item.get("start_col"),
        }
        for item in rows[:max_rows]
        if isinstance(item, Mapping)
    ]


def _mapping_or_none(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    return {key: item for key, item in value.items() if isinstance(key, str)}


def _convert_or_none[StructT](value: object, type_: type[StructT]) -> StructT | None:
    mapped = _mapping_or_none(value)
    if not mapped:
        return None
    try:
        return msgspec.convert(mapped, type=type_, strict=False)
    except (msgspec.ValidationError, TypeError, ValueError, RuntimeError):
        return None


_PYTHON_SECTION_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("resolution", ("resolution",)),
    ("behavior", ("behavior",)),
    ("structure", ("structure", "structural")),
    ("signature", ("signature",)),
    ("call", ("call",)),
    ("import_", ("import_", "import")),
    ("class_shape", ("class_shape",)),
    ("locals", ("locals",)),
    ("parse_quality", ("parse_quality",)),
)
_PYTHON_FLAT_SECTION_FIELDS: tuple[tuple[str, frozenset[str]], ...] = (
    ("resolution", frozenset(PythonResolutionFacts.__struct_fields__)),
    ("behavior", frozenset(PythonBehaviorFacts.__struct_fields__)),
    ("structure", frozenset(PythonStructureFacts.__struct_fields__)),
    ("signature", frozenset(PythonSignatureFacts.__struct_fields__)),
    ("call", frozenset(PythonCallFacts.__struct_fields__)),
    ("import_", frozenset(PythonImportFacts.__struct_fields__)),
    ("class_shape", frozenset(PythonClassShapeFacts.__struct_fields__)),
    ("locals", frozenset(PythonLocalsFacts.__struct_fields__)),
    ("parse_quality", frozenset(PythonParseQualityFacts.__struct_fields__)),
)


def _named_python_sections(raw: Mapping[str, object]) -> dict[str, dict[str, object]]:
    sections: dict[str, dict[str, object]] = {}
    for section_name, aliases in _PYTHON_SECTION_ALIASES:
        for alias in aliases:
            mapped = _mapping_or_none(raw.get(alias))
            if mapped:
                sections[section_name] = mapped
                break
    return sections


def _section_for_flat_python_field(key: str) -> str | None:
    for section_name, fields in _PYTHON_FLAT_SECTION_FIELDS:
        if key in fields:
            return section_name
    return None


def _flat_python_sections(raw: Mapping[str, object]) -> dict[str, dict[str, object]]:
    sections: dict[str, dict[str, object]] = {
        section_name: {} for section_name, _ in _PYTHON_FLAT_SECTION_FIELDS
    }
    for key, value in raw.items():
        section_name = _section_for_flat_python_field(key)
        if section_name is not None:
            sections[section_name][key] = value
    return {key: value for key, value in sections.items() if value}


def _python_fact_sections(raw: Mapping[str, object]) -> dict[str, dict[str, object]]:
    sections = _named_python_sections(raw)
    if sections:
        return sections
    return _flat_python_sections(raw)


def parse_python_enrichment(raw: Mapping[str, object] | None) -> PythonEnrichmentFacts | None:
    """Parse Python enrichment payload into typed facts.

    Returns:
        PythonEnrichmentFacts | None: Parsed Python enrichment facts when valid.
    """
    if raw is None:
        return None
    sections = _python_fact_sections(raw)
    if not sections:
        return None
    return PythonEnrichmentFacts(
        resolution=_convert_or_none(sections.get("resolution"), PythonResolutionFacts),
        behavior=_convert_or_none(sections.get("behavior"), PythonBehaviorFacts),
        structure=_convert_or_none(sections.get("structure"), PythonStructureFacts),
        signature=_convert_or_none(sections.get("signature"), PythonSignatureFacts),
        call=_convert_or_none(sections.get("call"), PythonCallFacts),
        import_=_convert_or_none(sections.get("import_"), PythonImportFacts),
        class_shape=_convert_or_none(sections.get("class_shape"), PythonClassShapeFacts),
        locals=_convert_or_none(sections.get("locals"), PythonLocalsFacts),
        parse_quality=_convert_or_none(sections.get("parse_quality"), PythonParseQualityFacts),
    )


def parse_rust_enrichment(raw: Mapping[str, object] | None) -> RustEnrichmentFacts | None:
    """Parse Rust enrichment payload into typed facts.

    Returns:
        RustEnrichmentFacts | None: Parsed Rust enrichment facts when valid.
    """
    if raw is None:
        return None
    structure = _mapping_or_none(raw.get("structure")) or {
        key: value for key, value in raw.items() if key in RustStructureFacts.__struct_fields__
    }
    definition = _mapping_or_none(raw.get("definition")) or {
        key: value for key, value in raw.items() if key in RustDefinitionFacts.__struct_fields__
    }
    macro_expansion = _mapping_or_none(raw.get("macro_expansion"))
    macro_expansion_results = raw.get("macro_expansion_results")
    if not macro_expansion and isinstance(macro_expansion_results, list):
        macro_expansion = {
            "macro_expansion_results": [
                item for item in macro_expansion_results if isinstance(item, Mapping)
            ]
        }
    if not structure and not definition and not macro_expansion:
        return None
    return RustEnrichmentFacts(
        structure=_convert_or_none(structure, RustStructureFacts),
        definition=_convert_or_none(definition, RustDefinitionFacts),
        macro_expansion=_convert_or_none(macro_expansion, RustMacroExpansionFacts),
    )


def parse_incremental_enrichment(raw: Mapping[str, object] | None) -> IncrementalFacts | None:
    """Parse incremental enrichment payload into typed facts.

    Returns:
        Incremental facts payload when parseable; otherwise `None`.
    """
    if raw is None:
        return None
    anchor = _convert_or_none(raw.get("anchor"), IncrementalAnchorFacts)
    details_raw = raw.get("details")
    details = (
        [
            {key: value for key, value in item.items() if isinstance(key, str)}
            for item in details_raw
            if isinstance(item, Mapping)
        ]
        if isinstance(details_raw, list)
        else []
    )
    facts = IncrementalFacts(
        anchor=anchor,
        semantic=_mapping_or_none(raw.get("semantic")) or {},
        sym=_mapping_or_none(raw.get("sym")) or {},
        dis=_mapping_or_none(raw.get("dis")) or {},
        inspect=_mapping_or_none(raw.get("inspect")) or {},
        compound=_mapping_or_none(raw.get("compound")) or {},
        details=details,
        stage_status=_mapping_or_none(raw.get("stage_status")) or {},
        stage_errors=_mapping_or_none(raw.get("stage_errors")) or {},
        timings_ms=_mapping_or_none(raw.get("timings_ms")) or {},
        session_errors=_mapping_or_none(raw.get("session_errors")) or {},
    )
    if not _has_incremental_fact_content(facts):
        return None
    return facts


def _has_incremental_fact_content(facts: IncrementalFacts) -> bool:
    if facts.anchor is not None:
        return True
    for value in (
        facts.semantic,
        facts.sym,
        facts.dis,
        facts.inspect,
        facts.compound,
        facts.details,
        facts.stage_status,
        facts.stage_errors,
        facts.timings_ms,
        facts.session_errors,
    ):
        if value:
            return True
    return False


def append_source(payload: dict[str, object], source: str) -> None:
    """Append an enrichment source name if it is not already present."""
    raw = payload.get("enrichment_sources")
    sources: list[str] = (
        [item for item in raw if isinstance(item, str)] if isinstance(raw, list) else []
    )
    if source not in sources:
        sources.append(source)
    payload["enrichment_sources"] = sources


def set_degraded(payload: dict[str, object], reason: str) -> None:
    """Mark payload as degraded while preserving earlier degradation reason."""
    payload["enrichment_status"] = "degraded"
    existing = payload.get("degrade_reason")
    if isinstance(existing, str) and existing:
        payload["degrade_reason"] = f"{existing}; {reason}"
    else:
        payload["degrade_reason"] = reason


def merge_gap_fill_payload(
    primary: Mapping[str, object],
    secondary: Mapping[str, object],
    *,
    metadata_keys: Sequence[str] | None = None,
) -> dict[str, object]:
    """Merge secondary payload values only when primary is missing/empty.

    Returns:
        Merged payload preserving existing populated primary values.
    """
    merged = dict(primary)
    protected = set(metadata_keys) if metadata_keys is not None else set(_DEFAULT_METADATA_KEYS)
    for key, value in secondary.items():
        if key in protected:
            continue
        if key not in merged or not has_value(merged.get(key)):
            merged[key] = value
    return merged


def check_payload_budget(
    payload: Mapping[str, object],
    *,
    max_payload_bytes: int,
) -> tuple[bool, int]:
    """Check whether payload fits size budget without mutating it.

    Returns:
        ``(fits_budget, size_hint)``.
    """
    size = payload_size_hint(payload)
    return size <= max_payload_bytes, size


def trim_payload_to_budget(
    payload: Mapping[str, object],
    *,
    max_payload_bytes: int,
    drop_order: Sequence[str],
) -> tuple[dict[str, object], list[str], int]:
    """Return trimmed payload copy that fits budget when possible.

    Returns:
        ``(trimmed_payload, dropped_keys, size_hint)``.
    """
    trimmed = dict(payload)
    fits_budget, size = check_payload_budget(trimmed, max_payload_bytes=max_payload_bytes)
    if fits_budget:
        return trimmed, [], size

    dropped: list[str] = []
    for key in drop_order:
        if key not in trimmed:
            continue
        trimmed.pop(key, None)
        dropped.append(key)
        fits_budget, size = check_payload_budget(trimmed, max_payload_bytes=max_payload_bytes)
        if fits_budget:
            break
    return trimmed, dropped, size


def _meta_from_flat(payload: Mapping[str, object], *, language: str) -> EnrichmentMeta:
    status = coerce_enrichment_status(payload.get("enrichment_status"))
    sources_raw = payload.get("enrichment_sources")
    sources = (
        [item for item in sources_raw if isinstance(item, str)]
        if isinstance(sources_raw, list)
        else []
    )
    degrade_reason = payload.get("degrade_reason")
    payload_size = payload.get("payload_size_hint")
    dropped = payload.get("dropped_fields")
    truncated = payload.get("truncated_fields")
    return EnrichmentMeta(
        language=language,
        enrichment_status=status,
        enrichment_sources=sources,
        degrade_reason=degrade_reason if isinstance(degrade_reason, str) else None,
        payload_size_hint=payload_size if isinstance(payload_size, int) else None,
        dropped_fields=(list(dropped) if isinstance(dropped, list) else None),
        truncated_fields=(list(truncated) if isinstance(truncated, list) else None),
    )


_PY_RESOLUTION_KEYS: frozenset[str] = frozenset(
    (
        *PythonResolutionFacts.__struct_fields__,
        *PythonCallFacts.__struct_fields__,
        *PythonImportFacts.__struct_fields__,
    )
)
_PY_BEHAVIOR_KEYS: frozenset[str] = frozenset(PythonBehaviorFacts.__struct_fields__)
_PY_STRUCTURAL_KEYS: frozenset[str] = frozenset(
    (
        *PythonStructureFacts.__struct_fields__,
        *PythonSignatureFacts.__struct_fields__,
        *PythonClassShapeFacts.__struct_fields__,
    )
)
_PY_FLAT_EXCLUDED_KEYS: frozenset[str] = frozenset(
    {
        "language",
        "enrichment_status",
        "enrichment_sources",
        "degrade_reason",
        "payload_size_hint",
        "dropped_fields",
        "truncated_fields",
        "stage_status",
        "stage_timings_ms",
    }
)


def _to_python_wrapper_data(payload: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in payload.items() if key not in _PY_FLAT_EXCLUDED_KEYS}


def _derive_behavior_flags(behavior: dict[str, object], structural: dict[str, object]) -> None:
    if "awaits" in behavior and "has_await" not in behavior:
        behavior["has_await"] = bool(behavior["awaits"])
    if "yields" in behavior and "has_yield" not in behavior:
        behavior["has_yield"] = bool(behavior["yields"])
    if "raises_exception" in behavior and "has_raise" not in behavior:
        behavior["has_raise"] = bool(behavior["raises_exception"])

    ctx = structural.get("structural_context")
    if not isinstance(ctx, str):
        return
    behavior.setdefault("in_try", ctx == "try_block")
    behavior.setdefault("in_except", ctx == "except_handler")
    behavior.setdefault("in_with", ctx == "with_block")
    behavior.setdefault("in_loop", ctx in {"for_loop", "while_loop", "comprehension"})


def _build_python_meta(
    *,
    payload: dict[str, object],
    meta: EnrichmentMeta,
) -> dict[str, object]:
    out: dict[str, object] = {
        "language": meta.language,
        "enrichment_status": meta.enrichment_status,
        "enrichment_sources": meta.enrichment_sources,
    }
    stage_status = payload.get("stage_status")
    if isinstance(stage_status, dict):
        out["stage_status"] = stage_status
    stage_timings = payload.get("stage_timings_ms")
    if isinstance(stage_timings, dict):
        out["stage_timings_ms"] = stage_timings
    if meta.degrade_reason:
        out["degrade_reason"] = meta.degrade_reason
    if meta.payload_size_hint is not None:
        out["payload_size_hint"] = meta.payload_size_hint
    if meta.dropped_fields:
        out["dropped_fields"] = meta.dropped_fields
    if meta.truncated_fields:
        out["truncated_fields"] = meta.truncated_fields
    return out


def normalize_python_payload(payload: dict[str, object] | None) -> dict[str, object] | None:
    """Normalize Python enrichment payload via typed wrapper contract.

    Returns:
        Structured Python payload envelope, or `None` when input is `None`.
    """
    if payload is None:
        return None
    meta_contract = _meta_from_flat(payload, language="python")
    wrapper_data = _to_python_wrapper_data(payload)
    resolution: dict[str, object] = {}
    behavior: dict[str, object] = {}
    structural: dict[str, object] = {}
    parse_quality: dict[str, object] = {}
    agreement: dict[str, object] = {"status": "partial", "conflicts": []}
    for key, value in wrapper_data.items():
        if key in _PY_RESOLUTION_KEYS:
            resolution[key] = value
            continue
        if key in _PY_BEHAVIOR_KEYS:
            behavior[key] = value
            continue
        if key in _PY_STRUCTURAL_KEYS:
            structural[key] = value
            continue
        if key == "parse_quality" and isinstance(value, dict):
            parse_quality = dict(value)
            continue
        if key == "agreement" and isinstance(value, dict):
            agreement = dict(value)
            continue
        structural[key] = value
    _derive_behavior_flags(behavior, structural)
    meta = _build_python_meta(payload=payload, meta=meta_contract)

    return {
        "meta": meta,
        "resolution": resolution,
        "behavior": behavior,
        "structural": structural,
        "parse_quality": parse_quality,
        "agreement": agreement,
    }


def normalize_rust_payload(payload: dict[str, object] | None) -> dict[str, object] | None:
    """Normalize Rust enrichment payload via typed wrapper contract.

    Returns:
        Structured Rust payload envelope, or `None` when input is `None`.
    """
    if payload is None:
        return None
    meta = _meta_from_flat(payload, language="rust")
    out = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "language",
            "enrichment_status",
            "enrichment_sources",
            "degrade_reason",
            "payload_size_hint",
            "dropped_fields",
            "truncated_fields",
        }
    }
    out["language"] = meta.language
    out["enrichment_status"] = meta.enrichment_status
    out["enrichment_sources"] = meta.enrichment_sources
    if meta.degrade_reason:
        out["degrade_reason"] = meta.degrade_reason
    if meta.payload_size_hint is not None:
        out["payload_size_hint"] = meta.payload_size_hint
    if meta.dropped_fields:
        out["dropped_fields"] = meta.dropped_fields
    if meta.truncated_fields:
        out["truncated_fields"] = meta.truncated_fields
    return out


__all__ = [
    "append_source",
    "check_payload_budget",
    "coerce_enrichment_status",
    "has_value",
    "merge_gap_fill_payload",
    "normalize_python_payload",
    "normalize_rust_payload",
    "parse_incremental_enrichment",
    "parse_python_enrichment",
    "parse_rust_enrichment",
    "payload_size_hint",
    "set_degraded",
    "string_or_none",
    "trim_payload_to_budget",
]
