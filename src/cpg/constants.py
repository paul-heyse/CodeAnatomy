"""Shared CPG constants and lightweight helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace

from arrowdsl.core.interop import TableLike
from arrowdsl.core.metrics import (
    QUALITY_SCHEMA,
    QualityPlanSpec,
    concat_quality_tables,
    empty_quality_table,
    quality_from_ids,
)
from arrowdsl.core.scan_telemetry import ScanTelemetry
from arrowdsl.finalize.finalize import FinalizeResult
from cpg.scip_roles import SCIP_ROLE_FORWARD_DEFINITION, SCIP_ROLE_GENERATED, SCIP_ROLE_TEST
from ibis_engine.hashing import HashExprSpec
from ibis_engine.hashing import hash_expr_spec_factory as hash_spec_factory


@dataclass(frozen=True)
class CpgBuildArtifacts:
    """Finalized output plus quality artifacts."""

    finalize: FinalizeResult
    quality: TableLike
    pipeline_breakers: tuple[str, ...] = ()
    relspec_scan_telemetry: Mapping[str, ScanTelemetry] = field(default_factory=dict)
    extra_outputs: Mapping[str, TableLike] = field(default_factory=dict)


EDGE_ID_BASE = hash_spec_factory(
    prefix="edge",
    cols=("src", "dst"),
)
EDGE_ID_SPAN = hash_spec_factory(
    prefix="edge",
    cols=("src", "dst", "path", "bstart", "bend"),
)


ROLE_FLAG_SPECS: tuple[tuple[str, int, str], ...] = (
    ("generated", SCIP_ROLE_GENERATED, "scip_role_generated"),
    ("test", SCIP_ROLE_TEST, "scip_role_test"),
    ("forward_definition", SCIP_ROLE_FORWARD_DEFINITION, "scip_role_forward_definition"),
)


def edge_hash_specs(edge_kind: str) -> tuple[HashExprSpec, HashExprSpec]:
    """Return base + span hash specs with the edge kind literal applied.

    Returns
    -------
    tuple[HashExprSpec, HashExprSpec]
        Base and span hash specifications for edge IDs.
    """
    extra = (edge_kind,) if edge_kind else ()
    return (
        replace(EDGE_ID_BASE, extra_literals=EDGE_ID_BASE.extra_literals + extra),
        replace(EDGE_ID_SPAN, extra_literals=EDGE_ID_SPAN.extra_literals + extra),
    )


__all__ = [
    "EDGE_ID_BASE",
    "EDGE_ID_SPAN",
    "QUALITY_SCHEMA",
    "ROLE_FLAG_SPECS",
    "CpgBuildArtifacts",
    "QualityPlanSpec",
    "concat_quality_tables",
    "edge_hash_specs",
    "empty_quality_table",
    "quality_from_ids",
]
