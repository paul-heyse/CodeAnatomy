"""Plan lane coverage audit helpers."""

from __future__ import annotations

from typing import TypedDict

from extract.registry_extractors import extractor_specs
from normalize.ibis_bridge import PLAN_BUILDERS_IBIS
from normalize.plan_builders import PLAN_BUILDERS


class NormalizeCoverage(TypedDict):
    """Coverage details for normalize plan builders."""

    plan_builders: list[str]
    ibis_builders: list[str]
    missing_ibis: list[str]
    extra_ibis: list[str]


class ExtractTemplateCoverage(TypedDict):
    """Coverage details for extract templates."""

    template: str
    supports_plan: bool
    ibis_capable: bool


class ExtractCoverage(TypedDict):
    """Coverage details for extraction templates."""

    templates: list[ExtractTemplateCoverage]


class PlanCoverageReport(TypedDict):
    """Report structure for plan coverage audits."""

    normalize: NormalizeCoverage
    extract: ExtractCoverage


def audit_plan_coverage() -> PlanCoverageReport:
    """Return a structured audit of ArrowDSL vs Ibis plan coverage.

    Returns
    -------
    PlanCoverageReport
        Coverage report with missing plan builders and extractor metadata.
    """
    normalize_plan = set(PLAN_BUILDERS)
    normalize_ibis = set(PLAN_BUILDERS_IBIS)
    normalize_missing_ibis = sorted(normalize_plan - normalize_ibis)
    normalize_extra_ibis = sorted(normalize_ibis - normalize_plan)
    extract_templates: list[ExtractTemplateCoverage] = [
        {
            "template": spec.name,
            "supports_plan": spec.supports_plan,
            "ibis_capable": spec.supports_ibis,
        }
        for spec in extractor_specs()
    ]
    return {
        "normalize": {
            "plan_builders": sorted(normalize_plan),
            "ibis_builders": sorted(normalize_ibis),
            "missing_ibis": normalize_missing_ibis,
            "extra_ibis": normalize_extra_ibis,
        },
        "extract": {"templates": extract_templates},
    }


__all__ = ["audit_plan_coverage"]
