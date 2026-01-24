"""Shared CPG constants and lightweight helpers."""

from __future__ import annotations

from cpg.scip_roles import SCIP_ROLE_FORWARD_DEFINITION, SCIP_ROLE_GENERATED, SCIP_ROLE_TEST
from obs.metrics import (
    QUALITY_SCHEMA,
    QualityPlanSpec,
    concat_quality_tables,
    empty_quality_table,
    quality_from_ids,
)

ROLE_FLAG_SPECS: tuple[tuple[str, int, str], ...] = (
    ("generated", SCIP_ROLE_GENERATED, "scip_role_generated"),
    ("test", SCIP_ROLE_TEST, "scip_role_test"),
    ("forward_definition", SCIP_ROLE_FORWARD_DEFINITION, "scip_role_forward_definition"),
)


__all__ = [
    "QUALITY_SCHEMA",
    "ROLE_FLAG_SPECS",
    "QualityPlanSpec",
    "concat_quality_tables",
    "empty_quality_table",
    "quality_from_ids",
]
