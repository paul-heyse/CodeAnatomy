"""Bundle registration for extract registry rules and templates."""

from __future__ import annotations

from collections.abc import Mapping
from functools import cache

from datafusion_engine.extract_rules import extract_rule_definitions as _extract_rule_definitions
from datafusion_engine.extract_templates import config as extractor_config
from datafusion_engine.extract_templates import expand_dataset_templates
from relspec.extract.registry_template_specs import DatasetTemplateSpec, dataset_template_specs
from relspec.rules.decorators import rule_bundle
from relspec.rules.definitions import RuleDefinition
from relspec.rules.diagnostics import RuleDiagnostic
from relspec.rules.templates import RuleTemplateSpec


def extract_template_specs() -> tuple[RuleTemplateSpec, ...]:
    """Return extract template specs for registry use.

    Returns
    -------
    tuple[RuleTemplateSpec, ...]
        Template specs derived from extract dataset templates.
    """
    specs, _ = _template_catalog()
    return specs


def extract_template_diagnostics() -> tuple[RuleDiagnostic, ...]:
    """Return template diagnostics for extract registry use.

    Returns
    -------
    tuple[RuleDiagnostic, ...]
        Template diagnostics emitted for extract templates.
    """
    _, diagnostics = _template_catalog()
    return diagnostics


@rule_bundle(
    name="relspec.extract",
    domain="extract",
    templates=extract_template_specs,
    diagnostics=extract_template_diagnostics,
)
@cache
def extract_rule_definitions() -> tuple[RuleDefinition, ...]:
    """Return extract rule definitions for registry use.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Extract rule definitions derived from dataset specs.
    """
    return _extract_rule_definitions()


@cache
def _template_catalog() -> tuple[tuple[RuleTemplateSpec, ...], tuple[RuleDiagnostic, ...]]:
    """Build the cached catalog of extract templates and diagnostics.

    Returns
    -------
    tuple[tuple[RuleTemplateSpec, ...], tuple[RuleDiagnostic, ...]]
        Template specs and diagnostics for the extract bundle.
    """
    specs: list[RuleTemplateSpec] = []
    diagnostics: list[RuleDiagnostic] = []
    for spec in dataset_template_specs():
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


__all__ = [
    "extract_rule_definitions",
    "extract_template_diagnostics",
    "extract_template_specs",
]
