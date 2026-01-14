"""Ibis bridge wrappers for normalize plan builders."""

from __future__ import annotations

from collections.abc import Mapping

from normalize.ibis_plan_builders import (
    IbisPlanCatalog,
    IbisPlanDeriver,
    plan_builders_ibis,
    resolve_plan_builder_ibis,
)

PLAN_BUILDERS_IBIS: Mapping[str, IbisPlanDeriver] = plan_builders_ibis()

__all__ = [
    "PLAN_BUILDERS_IBIS",
    "IbisPlanCatalog",
    "IbisPlanDeriver",
    "plan_builders_ibis",
    "resolve_plan_builder_ibis",
]
