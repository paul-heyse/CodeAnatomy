"""Typed fact-merge helpers for Python enrichment stages."""

from __future__ import annotations

from collections.abc import Callable

import msgspec

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


def merge_fact_section[StructT](
    current: StructT | None,
    incoming: StructT | None,
    *,
    type_: type[StructT],
    has_value: Callable[[object], bool],
) -> StructT | None:
    """Merge one typed fact section by preserving only meaningful incoming values.

    Returns:
        Merged typed section payload when conversion succeeds.
    """

    def _as_mapping(section: object | None) -> dict[str, object]:
        if section is None:
            return {}
        raw = msgspec.to_builtins(section, str_keys=True)
        return raw if isinstance(raw, dict) else {}

    if incoming is None:
        return current
    if current is None:
        return incoming
    merged = _as_mapping(current)
    for key, value in _as_mapping(incoming).items():
        if has_value(value):
            merged[key] = value
    try:
        return msgspec.convert(merged, type=type_, strict=False)
    except (msgspec.ValidationError, TypeError, ValueError):
        return current


def merge_python_enrichment_stage_facts(
    current: PythonEnrichmentFacts,
    incoming: PythonEnrichmentFacts,
    *,
    has_value: Callable[[object], bool],
) -> PythonEnrichmentFacts:
    """Merge staged typed facts into the current Python-enrichment fact contract.

    Returns:
        Python enrichment facts with incoming non-empty fields merged.
    """
    return msgspec.structs.replace(
        current,
        resolution=merge_fact_section(
            current.resolution,
            incoming.resolution,
            type_=PythonResolutionFacts,
            has_value=has_value,
        ),
        behavior=merge_fact_section(
            current.behavior,
            incoming.behavior,
            type_=PythonBehaviorFacts,
            has_value=has_value,
        ),
        structure=merge_fact_section(
            current.structure,
            incoming.structure,
            type_=PythonStructureFacts,
            has_value=has_value,
        ),
        signature=merge_fact_section(
            current.signature,
            incoming.signature,
            type_=PythonSignatureFacts,
            has_value=has_value,
        ),
        call=merge_fact_section(
            current.call,
            incoming.call,
            type_=PythonCallFacts,
            has_value=has_value,
        ),
        import_=merge_fact_section(
            current.import_,
            incoming.import_,
            type_=PythonImportFacts,
            has_value=has_value,
        ),
        class_shape=merge_fact_section(
            current.class_shape,
            incoming.class_shape,
            type_=PythonClassShapeFacts,
            has_value=has_value,
        ),
        locals=merge_fact_section(
            current.locals,
            incoming.locals,
            type_=PythonLocalsFacts,
            has_value=has_value,
        ),
        parse_quality=merge_fact_section(
            current.parse_quality,
            incoming.parse_quality,
            type_=PythonParseQualityFacts,
            has_value=has_value,
        ),
    )


__all__ = ["merge_fact_section", "merge_python_enrichment_stage_facts"]
