"""Extract adapter for centralized rule definitions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cache
from typing import TYPE_CHECKING

from extract.registry_definitions import extract_rule_definitions
from extract.registry_template_specs import DATASET_TEMPLATE_SPECS
from extract.registry_templates import config as extractor_config
from extract.registry_templates import expand_dataset_templates
from relspec.rules.definitions import RuleDefinition
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.registry import RuleAdapter
from relspec.rules.templates import RuleTemplateSpec

if TYPE_CHECKING:
    from extract.registry_template_specs import DatasetTemplateSpec
    from relspec.rules.definitions import RuleDomain


class ExtractRuleAdapter(RuleAdapter):
    """Adapter that exposes extract dataset specs as rule definitions."""

    domain: RuleDomain = "extract"

    @staticmethod
    def rule_definitions() -> Sequence[RuleDefinition]:
        """Return extract rule definitions for the central registry.

        Returns
        -------
        Sequence[RuleDefinition]
            Extract rule definitions.
        """
        return extract_rule_definitions()

    @staticmethod
    def templates() -> Sequence[RuleTemplateSpec]:
        """Return extract dataset templates for expansion.

        Returns
        -------
        Sequence[RuleTemplateSpec]
            Centralized template specs.
        """
        specs, _ = _template_catalog()
        return specs

    @staticmethod
    def template_diagnostics() -> Sequence[RuleDiagnostic]:
        """Return template diagnostics for extract templates.

        Returns
        -------
        Sequence[RuleDiagnostic]
            Template diagnostics for extract templates.
        """
        _, diagnostics = _template_catalog()
        return diagnostics


@cache
def _template_catalog() -> tuple[tuple[RuleTemplateSpec, ...], tuple[RuleDiagnostic, ...]]:
    """Build the cached catalog of extract templates and diagnostics.

    Returns
    -------
    tuple[tuple[RuleTemplateSpec, ...], tuple[RuleDiagnostic, ...]]
        Template specs and diagnostics for the extract adapter.
    """
    specs: list[RuleTemplateSpec] = []
    diagnostics: list[RuleDiagnostic] = []
    for spec in DATASET_TEMPLATE_SPECS:
        outputs, output_diagnostics = _outputs_for_spec(spec)
        diagnostics.extend(output_diagnostics)
        feature_flags, feature_diagnostics = _feature_flags_for_spec(spec)
        diagnostics.extend(feature_diagnostics)
        specs.append(
            RuleTemplateSpec(
                name=spec.name,
                domain="extract",
                template=spec.template,
                outputs=outputs,
                feature_flags=feature_flags,
                metadata=_template_metadata(spec),
            )
        )
    return tuple(specs), tuple(diagnostics)


def _outputs_for_spec(
    spec: DatasetTemplateSpec,
) -> tuple[tuple[str, ...], tuple[RuleDiagnostic, ...]]:
    """Expand a dataset template spec and capture diagnostics.

    Parameters
    ----------
    spec
        Dataset template specification.

    Returns
    -------
    tuple[tuple[str, ...], tuple[RuleDiagnostic, ...]]
        Output names and any diagnostics from expansion.
    """
    try:
        records = expand_dataset_templates((spec,))
    except (KeyError, TypeError, ValueError) as exc:
        return (
            (),
            (
                RuleDiagnostic(
                    domain="extract",
                    template=spec.template,
                    rule_name=None,
                    severity="error",
                    message=f"Template expansion failed for {spec.name!r}: {exc}",
                    metadata={"template": spec.template},
                ),
            ),
        )
    outputs = tuple(str(record.get("name")) for record in records if record.get("name"))
    return outputs, ()


def _feature_flags_for_spec(
    spec: DatasetTemplateSpec,
) -> tuple[tuple[str, ...], tuple[RuleDiagnostic, ...]]:
    """Resolve feature flags for a dataset template spec.

    Parameters
    ----------
    spec
        Dataset template specification.

    Returns
    -------
    tuple[tuple[str, ...], tuple[RuleDiagnostic, ...]]
        Feature flags and any diagnostics from config lookup.
    """
    try:
        flags = extractor_config(spec.template).feature_flags
    except (KeyError, TypeError, ValueError) as exc:
        return (
            (),
            (
                RuleDiagnostic(
                    domain="extract",
                    template=spec.template,
                    rule_name=None,
                    severity="error",
                    message=f"Extractor config missing for {spec.template!r}: {exc}",
                    metadata={"template": spec.template},
                ),
            ),
        )
    return tuple(flags), ()


def _template_metadata(spec: DatasetTemplateSpec) -> Mapping[str, str]:
    """Build metadata for an extract dataset template.

    Parameters
    ----------
    spec
        Dataset template specification.

    Returns
    -------
    Mapping[str, str]
        Metadata mapping for the template.
    """
    metadata: dict[str, str] = {"spec_name": spec.name}
    for key, value in spec.params.items():
        metadata[f"param.{key}"] = str(value)
    return metadata


__all__ = ["ExtractRuleAdapter"]
