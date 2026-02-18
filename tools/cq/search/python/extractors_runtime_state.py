"""Typed stage-state ownership for Python enrichment runtime."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import msgspec

from tools.cq.search.enrichment.core import has_value as _has_enrichment_value
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
from tools.cq.search.python.extractors_agreement import (
    build_agreement_section as _build_agreement_section_shared,
)
from tools.cq.search.python.extractors_fact_merge import (
    merge_python_enrichment_stage_facts as _merge_python_enrichment_stage_facts_shared,
)
from tools.cq.search.python.extractors_fact_normalize import (
    flatten_python_enrichment_facts as _flatten_python_enrichment_facts_shared,
)
from tools.cq.search.python.extractors_runtime_astgrep import EnrichmentContext, append_source

FULL_AGREEMENT_SOURCE_COUNT = 3


@dataclass(slots=True)
class PythonAgreementStage:
    """Typed subset used for cross-source agreement comparison."""

    resolution: PythonResolutionFacts | None = None
    structure: PythonStructureFacts | None = None
    call: PythonCallFacts | None = None

    def as_fields(self) -> dict[str, object]:
        payload: dict[str, object] = {}
        for section in (self.resolution, self.structure, self.call):
            if section is None:
                continue
            section_payload = msgspec.to_builtins(section, str_keys=True)
            if isinstance(section_payload, dict):
                payload.update(section_payload)
        return payload


@dataclass(slots=True)
class PythonEnrichmentState:
    """Per-request mutable state for staged enrichment execution."""

    metadata: dict[str, object]
    context: EnrichmentContext = field(default_factory=EnrichmentContext)
    facts: PythonEnrichmentFacts = field(default_factory=PythonEnrichmentFacts)
    stage_status: dict[str, str] = field(default_factory=dict)
    stage_timings_ms: dict[str, float] = field(default_factory=dict)
    degrade_reasons: list[str] = field(default_factory=list)
    ast_stage: PythonAgreementStage = field(default_factory=PythonAgreementStage)
    python_resolution_stage: PythonAgreementStage = field(default_factory=PythonAgreementStage)
    tree_sitter_stage: PythonAgreementStage = field(default_factory=PythonAgreementStage)


@dataclass(slots=True)
class PythonStageFactPatch:
    """Typed update patch for one enrichment stage."""

    facts: PythonEnrichmentFacts = field(default_factory=PythonEnrichmentFacts)
    metadata: dict[str, object] = field(default_factory=dict)


_PY_RESOLUTION_FIELDS: frozenset[str] = frozenset(PythonResolutionFacts.__struct_fields__)
_PY_BEHAVIOR_FIELDS: frozenset[str] = frozenset(PythonBehaviorFacts.__struct_fields__)
_PY_STRUCTURE_FIELDS: frozenset[str] = frozenset(PythonStructureFacts.__struct_fields__)
_PY_SIGNATURE_FIELDS: frozenset[str] = frozenset(PythonSignatureFacts.__struct_fields__)
_PY_CALL_FIELDS: frozenset[str] = frozenset(PythonCallFacts.__struct_fields__)
_PY_IMPORT_FIELDS: frozenset[str] = frozenset(PythonImportFacts.__struct_fields__)
_PY_CLASS_SHAPE_FIELDS: frozenset[str] = frozenset(PythonClassShapeFacts.__struct_fields__)
_PY_METADATA_FIELDS: frozenset[str] = frozenset(
    {
        "language",
        "enrichment_status",
        "enrichment_sources",
        "degrade_reason",
        "tree_sitter_query_telemetry",
        "cst_query_hits",
        "cst_diagnostics",
        "imports",
        "resolution",
    }
)


def _fact_bucket_name_for_field(key: str) -> str | None:
    fact_buckets: tuple[tuple[str, frozenset[str]], ...] = (
        ("resolution", _PY_RESOLUTION_FIELDS),
        ("behavior", _PY_BEHAVIOR_FIELDS),
        ("structure", _PY_STRUCTURE_FIELDS),
        ("signature", _PY_SIGNATURE_FIELDS),
        ("call", _PY_CALL_FIELDS),
        ("import", _PY_IMPORT_FIELDS),
        ("class_shape", _PY_CLASS_SHAPE_FIELDS),
    )
    for bucket_name, field_names in fact_buckets:
        if key in field_names:
            return bucket_name
    return None


def _assign_fact_field(
    key: str,
    value: object,
    *,
    buckets: dict[str, dict[str, object]],
) -> None:
    if not _has_enrichment_value(value):
        return
    bucket_name = _fact_bucket_name_for_field(key)
    if bucket_name is not None:
        buckets[bucket_name][key] = value


def _merge_stage_enrichment_sources(metadata: dict[str, object], value: object) -> None:
    if not isinstance(value, list):
        return
    for source_name in value:
        if isinstance(source_name, str):
            append_source(metadata, source_name)


def _merge_stage_degrade_reason(metadata: dict[str, object], value: object) -> None:
    if not isinstance(value, str) or not value:
        return
    existing = metadata.get("degrade_reason")
    if isinstance(existing, str) and existing:
        metadata["degrade_reason"] = f"{existing}; {value}"
    else:
        metadata["degrade_reason"] = value


def _merge_stage_metadata_field(
    *,
    metadata: dict[str, object],
    key: str,
    value: object,
) -> None:
    if key == "enrichment_sources":
        _merge_stage_enrichment_sources(metadata, value)
        return
    if key == "degrade_reason":
        _merge_stage_degrade_reason(metadata, value)
        return
    metadata[key] = value


def build_stage_fact_patch(fields: Mapping[str, object]) -> PythonStageFactPatch:
    """Build typed stage-fact patch from a raw field mapping."""

    def _convert_section[StructT](
        section: dict[str, object],
        type_: type[StructT],
    ) -> StructT | None:
        if not section:
            return None
        try:
            return msgspec.convert(section, type=type_, strict=False)
        except (msgspec.ValidationError, TypeError, ValueError):
            return None

    def _merge_mapping_fields(target: dict[str, object], payload: object) -> None:
        if not isinstance(payload, Mapping):
            return
        target.update({key: value for key, value in payload.items() if isinstance(key, str)})

    resolution: dict[str, object] = {}
    behavior: dict[str, object] = {}
    structure: dict[str, object] = {}
    signature: dict[str, object] = {}
    call: dict[str, object] = {}
    import_: dict[str, object] = {}
    class_shape: dict[str, object] = {}
    locals_dict: dict[str, object] = {}
    parse_quality: dict[str, object] = {}
    metadata: dict[str, object] = {}
    fact_buckets = {
        "resolution": resolution,
        "behavior": behavior,
        "structure": structure,
        "signature": signature,
        "call": call,
        "import": import_,
        "class_shape": class_shape,
    }

    _merge_mapping_fields(resolution, fields.get("resolution"))
    _merge_mapping_fields(locals_dict, fields.get("locals"))
    _merge_mapping_fields(parse_quality, fields.get("parse_quality"))
    imports_payload = fields.get("imports")
    if isinstance(imports_payload, Mapping):
        modules = imports_payload.get("modules")
        aliases = imports_payload.get("aliases")
        if isinstance(modules, list) and modules:
            import_["import_module"] = next(
                (item for item in modules if isinstance(item, str)),
                import_.get("import_module"),
            )
        if isinstance(aliases, list):
            import_["import_names"] = [item for item in aliases if isinstance(item, str)]

    for key, value in fields.items():
        if key in _PY_METADATA_FIELDS:
            _merge_stage_metadata_field(metadata=metadata, key=key, value=value)
        _assign_fact_field(key, value, buckets=fact_buckets)

    for key in ("qualified_name_candidates", "binding_candidates", "import_alias_chain"):
        raw = resolution.get(key)
        if not isinstance(raw, Sequence):
            continue
        rows: list[dict[str, object]] = []
        for item in raw:
            if isinstance(item, Mapping):
                rows.append({k: v for k, v in item.items() if isinstance(k, str)})
            elif isinstance(item, str) and item:
                rows.append({"name": item})
        resolution[key] = rows

    if "error_nodes" in parse_quality and "error_count" not in parse_quality:
        nodes = parse_quality.get("error_nodes")
        parse_quality["error_count"] = len(nodes) if isinstance(nodes, list) else 0

    return PythonStageFactPatch(
        facts=PythonEnrichmentFacts(
            resolution=_convert_section(resolution, PythonResolutionFacts),
            behavior=_convert_section(behavior, PythonBehaviorFacts),
            structure=_convert_section(structure, PythonStructureFacts),
            signature=_convert_section(signature, PythonSignatureFacts),
            call=_convert_section(call, PythonCallFacts),
            import_=_convert_section(import_, PythonImportFacts),
            class_shape=_convert_section(class_shape, PythonClassShapeFacts),
            locals=_convert_section(locals_dict, PythonLocalsFacts),
            parse_quality=_convert_section(parse_quality, PythonParseQualityFacts),
        ),
        metadata=metadata,
    )


def merge_python_enrichment_stage_facts(
    current: PythonEnrichmentFacts,
    patch: PythonStageFactPatch,
) -> PythonEnrichmentFacts:
    """Merge stage patch into current typed enrichment facts."""
    return _merge_python_enrichment_stage_facts_shared(
        current,
        patch.facts,
        has_value=_has_enrichment_value,
    )


def flatten_python_enrichment_facts(facts: PythonEnrichmentFacts) -> dict[str, object]:
    """Flatten typed enrichment facts into output payload fields."""
    return _flatten_python_enrichment_facts_shared(facts)


def build_stage_facts_from_enrichment(facts: PythonEnrichmentFacts) -> PythonAgreementStage:
    """Project agreement-relevant sections from typed enrichment facts."""
    return PythonAgreementStage(
        resolution=facts.resolution,
        structure=facts.structure,
        call=facts.call,
    )


def build_stage_facts(fields: Mapping[str, object]) -> PythonAgreementStage:
    """Build agreement-stage projection from raw stage fields."""
    patch = build_stage_fact_patch(fields)
    return build_stage_facts_from_enrichment(patch.facts)


def build_agreement_section(
    *,
    ast_stage: PythonAgreementStage,
    python_resolution_stage: PythonAgreementStage,
    tree_sitter_stage: PythonAgreementStage,
) -> dict[str, object]:
    """Build deterministic cross-source agreement payload."""
    return _build_agreement_section_shared(
        ast_fields=ast_stage.as_fields(),
        python_resolution_fields=python_resolution_stage.as_fields(),
        tree_sitter_fields=tree_sitter_stage.as_fields(),
        full_agreement_source_count=FULL_AGREEMENT_SOURCE_COUNT,
    )


def ingest_stage_fact_patch(
    state: PythonEnrichmentState,
    patch: PythonStageFactPatch,
    *,
    source: str | None = None,
) -> None:
    """Merge one stage patch into enrichment state."""
    if patch.facts == PythonEnrichmentFacts() and not patch.metadata and source is None:
        return
    state.facts = merge_python_enrichment_stage_facts(state.facts, patch)
    for key, value in patch.metadata.items():
        _merge_stage_metadata_field(metadata=state.metadata, key=key, value=value)
    if source is not None:
        append_source(state.metadata, source)


__all__ = [
    "PythonAgreementStage",
    "PythonEnrichmentState",
    "PythonStageFactPatch",
    "build_agreement_section",
    "build_stage_fact_patch",
    "build_stage_facts",
    "build_stage_facts_from_enrichment",
    "flatten_python_enrichment_facts",
    "ingest_stage_fact_patch",
]
