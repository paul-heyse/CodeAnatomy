"""Factory helpers for normalize rule definition specs."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from relspec.normalize.rule_specs import NormalizeRuleFamilySpec
from relspec.rules.definitions import (
    EvidenceOutput,
    EvidenceSpec,
    ExecutionMode,
    NormalizePayload,
    PolicyOverrides,
    RuleDefinition,
)

if TYPE_CHECKING:
    from relspec.rules.definitions import RuleDomain


@dataclass(frozen=True)
class RuleConfig:
    """Configuration overrides for normalize rule factories."""

    evidence_sources: Sequence[str] = ()
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None
    evidence_output: EvidenceOutput | None = None


@dataclass(frozen=True)
class RuleDefinitionSpec:
    """Structural inputs for a normalize rule definition spec."""

    name: str
    output: str
    inputs: Sequence[str]
    plan_builder: str | None
    execution_mode: ExecutionMode = "auto"


RuleFamilyFactory = Callable[[NormalizeRuleFamilySpec], tuple[RuleDefinition, ...]]


def build_rule_definitions_from_specs(
    specs: Sequence[NormalizeRuleFamilySpec],
) -> tuple[RuleDefinition, ...]:
    """Build normalize rules from rule-family specs.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Normalize rule definitions derived from the specs.

    Raises
    ------
    KeyError
        Raised when a rule family spec references an unknown factory.
    """
    rules: list[RuleDefinition] = []
    for spec in specs:
        factory = RULE_FAMILY_FACTORIES.get(spec.factory)
        if factory is None:
            msg = f"Unknown normalize rule factory: {spec.factory!r}."
            raise KeyError(msg)
        rules.extend(factory(spec))
    return tuple(rules)


def _rule_config(
    spec: NormalizeRuleFamilySpec,
    *,
    evidence_sources: Sequence[str] = (),
    confidence_policy: str | None = None,
    ambiguity_policy: str | None = None,
    evidence_output: EvidenceOutput | None = None,
) -> RuleConfig:
    return RuleConfig(
        evidence_sources=tuple(evidence_sources),
        confidence_policy=confidence_policy or spec.confidence_policy,
        ambiguity_policy=ambiguity_policy or spec.ambiguity_policy,
        evidence_output=evidence_output,
    )


def _execution_mode_from_spec(spec: NormalizeRuleFamilySpec) -> ExecutionMode:
    if spec.execution_mode is None:
        return "auto"
    mode = spec.execution_mode
    if mode == "auto":
        return "auto"
    if mode == "plan":
        return "plan"
    if mode == "table":
        return "table"
    msg = f"Invalid normalize execution mode: {mode!r}."
    raise ValueError(msg)


def build_type_rules(
    spec: NormalizeRuleFamilySpec,
) -> tuple[RuleDefinition, ...]:
    """Return normalize rules for type expressions and nodes.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Type-related normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _definition(
            RuleDefinitionSpec(
                name="type_exprs_norm",
                output="type_exprs_norm_v1",
                inputs=("cst_type_exprs",),
                plan_builder="type_exprs",
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=("cst_type_exprs",),
            ),
        ),
        _definition(
            RuleDefinitionSpec(
                name="type_nodes",
                output="type_nodes_v1",
                inputs=("type_exprs_norm_v1", "scip_symbol_information"),
                plan_builder="type_nodes",
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=(),
            ),
        ),
    )


def build_bytecode_cfg_rules(
    spec: NormalizeRuleFamilySpec,
) -> tuple[RuleDefinition, ...]:
    """Return normalize rules for bytecode CFG outputs.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Bytecode normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _definition(
            RuleDefinitionSpec(
                name="cfg_blocks_norm",
                output="py_bc_blocks_norm_v1",
                inputs=("py_bc_blocks", "py_bc_code_units"),
                plan_builder="cfg_blocks",
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=("py_bc_blocks",),
            ),
        ),
        _definition(
            RuleDefinitionSpec(
                name="cfg_edges_norm",
                output="py_bc_cfg_edges_norm_v1",
                inputs=("py_bc_cfg_edges", "py_bc_code_units"),
                plan_builder="cfg_edges",
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=("py_bc_cfg_edges",),
            ),
        ),
    )


def build_bytecode_dfg_rules(
    spec: NormalizeRuleFamilySpec,
) -> tuple[RuleDefinition, ...]:
    """Return normalize rules for bytecode DFG outputs.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Bytecode DFG normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _definition(
            RuleDefinitionSpec(
                name="def_use_events",
                output="py_bc_def_use_events_v1",
                inputs=("py_bc_instructions",),
                plan_builder="def_use_events",
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=("py_bc_instructions",),
            ),
        ),
        _definition(
            RuleDefinitionSpec(
                name="reaching_defs",
                output="py_bc_reaches_v1",
                inputs=("py_bc_def_use_events_v1",),
                plan_builder="reaching_defs",
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=(),
            ),
        ),
    )


def build_diagnostics_rules(
    spec: NormalizeRuleFamilySpec,
) -> tuple[RuleDefinition, ...]:
    """Return normalize rules for diagnostics outputs.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Diagnostics normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _definition(
            RuleDefinitionSpec(
                name="diagnostics_norm",
                output="diagnostics_norm_v1",
                inputs=(
                    "cst_parse_errors",
                    "file_line_index",
                    "symtable_scopes",
                    "ts_errors",
                    "ts_missing",
                    "py_bc_code_units",
                    "scip_diagnostics",
                    "scip_documents",
                ),
                plan_builder="diagnostics",
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=(),
            ),
        ),
    )


def build_span_error_rules(
    spec: NormalizeRuleFamilySpec,
) -> tuple[RuleDefinition, ...]:
    """Return normalize rules for span error outputs.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Span error normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _definition(
            RuleDefinitionSpec(
                name="span_errors",
                output="span_errors_v1",
                inputs=(),
                plan_builder="span_errors",
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=(),
            ),
        ),
    )


RULE_FAMILY_FACTORIES: dict[str, RuleFamilyFactory] = {
    "types": build_type_rules,
    "bytecode_cfg": build_bytecode_cfg_rules,
    "bytecode_dfg": build_bytecode_dfg_rules,
    "diagnostics": build_diagnostics_rules,
    "span_errors": build_span_error_rules,
}


def _definition(
    spec: RuleDefinitionSpec,
    *,
    config: RuleConfig | None = None,
) -> RuleDefinition:
    config = config or RuleConfig()
    evidence = _evidence_spec(config.evidence_sources)
    return RuleDefinition(
        name=spec.name,
        domain=_normalize_domain(),
        kind="normalize",
        output=spec.output,
        inputs=tuple(spec.inputs),
        payload=NormalizePayload(plan_builder=spec.plan_builder, query=None),
        evidence=evidence,
        evidence_output=config.evidence_output,
        execution_mode=spec.execution_mode,
        policy_overrides=PolicyOverrides(
            confidence_policy=config.confidence_policy,
            ambiguity_policy=config.ambiguity_policy,
        ),
    )


def _evidence_spec(sources: Sequence[str]) -> EvidenceSpec | None:
    if not sources:
        return None
    return EvidenceSpec(sources=tuple(sources))


def _normalize_domain() -> RuleDomain:
    return "normalize"


__all__ = [
    "RULE_FAMILY_FACTORIES",
    "RuleFamilyFactory",
    "build_bytecode_cfg_rules",
    "build_bytecode_dfg_rules",
    "build_diagnostics_rules",
    "build_rule_definitions_from_specs",
    "build_span_error_rules",
    "build_type_rules",
]
