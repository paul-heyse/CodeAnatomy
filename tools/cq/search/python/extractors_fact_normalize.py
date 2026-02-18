"""Normalization helpers for typed Python enrichment facts."""

from __future__ import annotations

import msgspec

from tools.cq.search.enrichment.python_facts import PythonEnrichmentFacts


def flatten_python_enrichment_facts(facts: PythonEnrichmentFacts) -> dict[str, object]:
    """Flatten typed fact sections into deterministic builtins payload fields.

    Returns:
        Deterministic builtins payload flattened across fact sections.
    """
    payload: dict[str, object] = {}
    for section in (
        facts.resolution,
        facts.behavior,
        facts.structure,
        facts.signature,
        facts.call,
        facts.import_,
        facts.class_shape,
    ):
        if section is None:
            continue
        section_map = msgspec.to_builtins(section, str_keys=True)
        if isinstance(section_map, dict):
            payload.update(section_map)

    if facts.locals is not None:
        locals_payload = msgspec.to_builtins(facts.locals, str_keys=True)
        if isinstance(locals_payload, dict):
            payload["locals"] = locals_payload
    if facts.parse_quality is not None:
        parse_payload = msgspec.to_builtins(facts.parse_quality, str_keys=True)
        if isinstance(parse_payload, dict):
            payload["parse_quality"] = parse_payload
    return payload


__all__ = ["flatten_python_enrichment_facts"]
