"""Template defaults for normalize dataset metadata."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from arrowdsl.core.context import DeterminismTier, OrderingLevel


@dataclass(frozen=True)
class RegistryTemplate:
    """Template defaults for normalize datasets."""

    stage: str
    ordering_level: OrderingLevel = OrderingLevel.IMPLICIT
    metadata_extra: Mapping[bytes, bytes] | None = None
    determinism_tier: DeterminismTier | None = None


_TEMPLATES: dict[str, RegistryTemplate] = {
    "normalize": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
    ),
    "normalize_cst": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra={
            b"confidence_policy": b"cst",
            b"evidence_family": b"cst",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"3",
        },
    ),
    "normalize_scip": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra={
            b"confidence_policy": b"scip",
            b"evidence_family": b"scip",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"1",
        },
    ),
    "normalize_bytecode": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra={
            b"confidence_policy": b"bytecode",
            b"evidence_family": b"bytecode",
            b"coordinate_system": b"offsets",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"5",
        },
    ),
    "normalize_diagnostics": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra={
            b"confidence_policy": b"diagnostic",
            b"evidence_family": b"diagnostic",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"2",
        },
    ),
    "normalize_span": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra={
            b"confidence_policy": b"span",
            b"evidence_family": b"span",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"4",
        },
    ),
    "normalize_type": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra={
            b"confidence_policy": b"type",
            b"evidence_family": b"type",
            b"coordinate_system": b"none",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"2",
        },
    ),
    "normalize_evidence": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra={
            b"confidence_policy": b"evidence",
            b"evidence_family": b"normalize_evidence",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"0",
        },
    ),
}


def template(name: str) -> RegistryTemplate:
    """Return a registry template by name.

    Returns
    -------
    RegistryTemplate
        Template defaults.
    """
    return _TEMPLATES[name]


def template_names() -> tuple[str, ...]:
    """Return the template registry keys.

    Returns
    -------
    tuple[str, ...]
        Template names.
    """
    return tuple(_TEMPLATES.keys())


__all__ = ["RegistryTemplate", "template", "template_names"]
