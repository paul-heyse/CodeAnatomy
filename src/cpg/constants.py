"""Shared CPG constants and lightweight helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from arrowdsl.compute.ids import hash_spec_factory
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, TableLike, pc
from arrowdsl.finalize.finalize import FinalizeResult
from arrowdsl.plan.metrics import (
    QUALITY_SCHEMA,
    QualityPlanSpec,
    concat_quality_tables,
    empty_quality_table,
    quality_from_ids,
    quality_plan_from_ids,
)
from arrowdsl.plan.query import ScanTelemetry
from cpg.kinds_ultimate import (
    SCIP_ROLE_FORWARD_DEFINITION,
    SCIP_ROLE_GENERATED,
    SCIP_ROLE_TEST,
)

if TYPE_CHECKING:
    from arrowdsl.core.ids import HashSpec

type ValuesLike = ArrayLike | ChunkedArrayLike


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


def edge_hash_specs(edge_kind: str) -> tuple[HashSpec, HashSpec]:
    """Return base + span hash specs with the edge kind literal applied.

    Returns
    -------
    tuple[HashSpec, HashSpec]
        Base and span hash specifications for edge IDs.
    """
    extra = (edge_kind,) if edge_kind else ()
    return (
        replace(EDGE_ID_BASE, extra_literals=EDGE_ID_BASE.extra_literals + extra),
        replace(EDGE_ID_SPAN, extra_literals=EDGE_ID_SPAN.extra_literals + extra),
    )


def fill_nulls(values: ValuesLike, *, default: object) -> ValuesLike:
    """Fill null values with a default.

    Returns
    -------
    ValuesLike
        Values with nulls replaced by the default.
    """
    if values.null_count == 0:
        return values
    return pc.fill_null(values, fill_value=default)


def fill_nulls_float(values: ValuesLike, *, default: float) -> ValuesLike:
    """Fill null float values with a default.

    Returns
    -------
    ValuesLike
        Values with nulls replaced by the default.
    """
    return fill_nulls(values, default=default)


def fill_nulls_string(values: ValuesLike, *, default: str) -> ValuesLike:
    """Fill null string values with a default.

    Returns
    -------
    ValuesLike
        Values with nulls replaced by the default.
    """
    return fill_nulls(values, default=default)


__all__ = [
    "EDGE_ID_BASE",
    "EDGE_ID_SPAN",
    "QUALITY_SCHEMA",
    "ROLE_FLAG_SPECS",
    "CpgBuildArtifacts",
    "QualityPlanSpec",
    "ValuesLike",
    "concat_quality_tables",
    "edge_hash_specs",
    "empty_quality_table",
    "fill_nulls",
    "fill_nulls_float",
    "fill_nulls_string",
    "quality_from_ids",
    "quality_plan_from_ids",
]
