"""Template-driven normalize rule-family specs."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache

from relspec.errors import RelspecValidationError
from relspec.normalize.rule_specs import NormalizeRuleFamilySpec


@dataclass(frozen=True)
class RuleTemplateSpec:
    """Template spec for generating normalize rule families.

    Note: The ``inputs`` field has been removed. Dependencies are now inferred
    from Ibis/DataFusion expression analysis rather than declared manually.
    """

    name: str
    factory: str
    params: Mapping[str, object] = field(default_factory=dict)


TemplateFactory = Callable[[RuleTemplateSpec], tuple[NormalizeRuleFamilySpec, ...]]


def _param_str(spec: RuleTemplateSpec, key: str) -> str | None:
    value = spec.params.get(key)
    if value is None:
        return None
    return str(value)


def _base_overrides(spec: RuleTemplateSpec) -> dict[str, str | None]:
    return {
        "output": _param_str(spec, "output"),
        "confidence_policy": _param_str(spec, "confidence_policy"),
        "ambiguity_policy": _param_str(spec, "ambiguity_policy"),
        "option_flag": _param_str(spec, "option_flag"),
        "execution_mode": _param_str(spec, "execution_mode"),
    }


def _single_family_template(spec: RuleTemplateSpec) -> tuple[NormalizeRuleFamilySpec, ...]:
    family_factory = _param_str(spec, "rule_factory")
    if not family_factory:
        msg = f"RuleTemplateSpec {spec.name!r} missing rule_factory param."
        raise RelspecValidationError(msg)
    overrides = _base_overrides(spec)
    return (
        NormalizeRuleFamilySpec(
            name=spec.name,
            factory=family_factory,
            output=overrides["output"],
            confidence_policy=overrides["confidence_policy"],
            ambiguity_policy=overrides["ambiguity_policy"],
            option_flag=overrides["option_flag"],
            execution_mode=overrides["execution_mode"],
        ),
    )


def _bytecode_template(spec: RuleTemplateSpec) -> tuple[NormalizeRuleFamilySpec, ...]:
    # Note: cfg_inputs and dfg_inputs params are no longer used.
    # Dependencies are now inferred from expression analysis.
    overrides = _base_overrides(spec)
    return (
        NormalizeRuleFamilySpec(
            name=f"{spec.name}_cfg",
            factory="bytecode_cfg",
            output=overrides["output"],
            confidence_policy=overrides["confidence_policy"],
            ambiguity_policy=overrides["ambiguity_policy"],
            option_flag=overrides["option_flag"],
            execution_mode=overrides["execution_mode"],
        ),
        NormalizeRuleFamilySpec(
            name=f"{spec.name}_dfg",
            factory="bytecode_dfg",
            output=overrides["output"],
            confidence_policy=overrides["confidence_policy"],
            ambiguity_policy=overrides["ambiguity_policy"],
            option_flag=overrides["option_flag"],
            execution_mode=overrides["execution_mode"],
        ),
    )


TEMPLATE_REGISTRY: dict[str, TemplateFactory] = {
    "single_family": _single_family_template,
    "bytecode": _bytecode_template,
}


def expand_rule_templates(
    specs: Sequence[RuleTemplateSpec],
) -> tuple[NormalizeRuleFamilySpec, ...]:
    """Expand template specs into normalize rule-family specs.

    Returns
    -------
    tuple[NormalizeRuleFamilySpec, ...]
        Expanded rule-family specs.

    Raises
    ------
    KeyError
        Raised when a template factory is unknown.
    """
    expanded: list[NormalizeRuleFamilySpec] = []
    for spec in specs:
        factory = TEMPLATE_REGISTRY.get(spec.factory)
        if factory is None:
            msg = f"Unknown normalize rule template: {spec.factory!r}."
            raise KeyError(msg)
        expanded.extend(factory(spec))
    return tuple(expanded)


@cache
def rule_template_specs() -> tuple[RuleTemplateSpec, ...]:
    """Return normalize rule template specs.

    Note: Dependencies are now inferred from expression analysis
    rather than declared via inputs parameter.

    Returns
    -------
    tuple[RuleTemplateSpec, ...]
        Template specs for normalize rule families.
    """
    return (
        RuleTemplateSpec(
            name="types",
            factory="single_family",
            params={"rule_factory": "types"},
        ),
        RuleTemplateSpec(
            name="bytecode",
            factory="bytecode",
            params={},
        ),
        RuleTemplateSpec(
            name="diagnostics",
            factory="single_family",
            params={"rule_factory": "diagnostics"},
        ),
        RuleTemplateSpec(
            name="span_errors",
            factory="single_family",
            params={"rule_factory": "span_errors"},
        ),
    )


__all__ = [
    "RuleTemplateSpec",
    "expand_rule_templates",
    "rule_template_specs",
]
