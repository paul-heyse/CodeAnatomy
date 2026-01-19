"""Template defaults for normalize dataset metadata."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from arrowdsl.core.context import OrderingLevel
from arrowdsl.core.determinism import DeterminismTier
from normalize.evidence_specs import EVIDENCE_OUTPUT_LITERALS_META, EVIDENCE_OUTPUT_MAP_META
from registry_common.metadata import (
    EvidenceMetadataSpec,
    evidence_metadata,
    metadata_map_bytes,
    metadata_scalar_map_bytes,
)


@dataclass(frozen=True)
class RegistryTemplate:
    """Template defaults for normalize datasets."""

    stage: str
    ordering_level: OrderingLevel = OrderingLevel.IMPLICIT
    metadata_extra: Mapping[bytes, bytes] | None = None
    determinism_tier: DeterminismTier | None = None


_SPAN_COORD_POLICY = b"line_base=0;col_unit=utf32;end_exclusive=true"


_TEMPLATES: dict[str, RegistryTemplate] = {
    "normalize": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
    ),
    "normalize_cst": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra=evidence_metadata(
            spec=EvidenceMetadataSpec(
                evidence_family="cst",
                coordinate_system="bytes",
                ambiguity_policy="preserve",
                superior_rank=3,
                span_coord_policy=_SPAN_COORD_POLICY,
            ),
            extra={
                b"confidence_policy": b"cst",
                EVIDENCE_OUTPUT_MAP_META: metadata_map_bytes({"role": "expr_role"}),
                EVIDENCE_OUTPUT_LITERALS_META: metadata_scalar_map_bytes(
                    {"source": "cst_type_exprs"}
                ),
            },
        ),
    ),
    "normalize_scip": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra=evidence_metadata(
            spec=EvidenceMetadataSpec(
                evidence_family="scip",
                coordinate_system="bytes",
                ambiguity_policy="preserve",
                superior_rank=1,
                span_coord_policy=_SPAN_COORD_POLICY,
            ),
            extra={b"confidence_policy": b"scip"},
        ),
    ),
    "normalize_bytecode": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra=evidence_metadata(
            spec=EvidenceMetadataSpec(
                evidence_family="bytecode",
                coordinate_system="offsets",
                ambiguity_policy="preserve",
                superior_rank=5,
            ),
            extra={b"confidence_policy": b"bytecode"},
        ),
    ),
    "normalize_diagnostics": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra=evidence_metadata(
            spec=EvidenceMetadataSpec(
                evidence_family="diagnostic",
                coordinate_system="bytes",
                ambiguity_policy="preserve",
                superior_rank=2,
                span_coord_policy=_SPAN_COORD_POLICY,
            ),
            extra={
                b"confidence_policy": b"diagnostic",
                EVIDENCE_OUTPUT_MAP_META: metadata_map_bytes(
                    {"role": "severity", "source": "diag_source"}
                ),
            },
        ),
    ),
    "normalize_span": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra=evidence_metadata(
            spec=EvidenceMetadataSpec(
                evidence_family="span",
                coordinate_system="bytes",
                ambiguity_policy="preserve",
                superior_rank=4,
                span_coord_policy=_SPAN_COORD_POLICY,
            ),
            extra={
                b"confidence_policy": b"span",
                EVIDENCE_OUTPUT_MAP_META: metadata_map_bytes({"role": "reason"}),
                EVIDENCE_OUTPUT_LITERALS_META: metadata_scalar_map_bytes({"source": "span_errors"}),
            },
        ),
    ),
    "normalize_type": RegistryTemplate(
        stage="normalize",
        ordering_level=OrderingLevel.EXPLICIT,
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra=evidence_metadata(
            spec=EvidenceMetadataSpec(
                evidence_family="type",
                coordinate_system="none",
                ambiguity_policy="preserve",
                superior_rank=2,
            ),
            extra={
                b"confidence_policy": b"type",
                EVIDENCE_OUTPUT_MAP_META: metadata_map_bytes({"role": "type_form"}),
                EVIDENCE_OUTPUT_LITERALS_META: metadata_scalar_map_bytes({"source": "type_nodes"}),
            },
        ),
    ),
    "normalize_evidence": RegistryTemplate(
        stage="normalize",
        determinism_tier=DeterminismTier.BEST_EFFORT,
        metadata_extra=evidence_metadata(
            spec=EvidenceMetadataSpec(
                evidence_family="normalize_evidence",
                coordinate_system="bytes",
                ambiguity_policy="preserve",
                superior_rank=0,
                span_coord_policy=_SPAN_COORD_POLICY,
            ),
            extra={b"confidence_policy": b"evidence"},
        ),
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
