"""Factory helpers for normalize rule definitions."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import cast

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.plan.catalog import PlanCatalog, PlanDeriver
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import run_plan
from arrowdsl.plan.scan_io import PlanSource, plan_from_source
from normalize.bytecode_cfg_plans import cfg_blocks_plan, cfg_edges_plan
from normalize.bytecode_dfg_plans import def_use_events_plan, reaching_defs_plan
from normalize.diagnostics_plans import DiagnosticsSources, diagnostics_plan
from normalize.policy_registry import resolve_ambiguity_policy, resolve_confidence_policy
from normalize.rule_model import (
    AmbiguityPolicy,
    ConfidencePolicy,
    EvidenceOutput,
    EvidenceSpec,
    ExecutionMode,
    NormalizeRule,
)
from normalize.rule_registry_specs import NormalizeRuleFamilySpec
from normalize.text_index import RepoTextIndex
from normalize.types_plans import type_exprs_plan, type_nodes_plan


@dataclass(frozen=True)
class RuleConfig:
    """Configuration overrides for normalize rule factories."""

    evidence_sources: Sequence[str] = ()
    confidence_policy: ConfidencePolicy | None = None
    ambiguity_policy: AmbiguityPolicy | None = None
    evidence_output: EvidenceOutput | None = None


@dataclass(frozen=True)
class RuleSpec:
    """Structural inputs for a normalize rule definition."""

    name: str
    output: str
    inputs: Sequence[str]
    derive: PlanDeriver | None
    execution_mode: ExecutionMode = "auto"


RuleFamilyFactory = Callable[[NormalizeRuleFamilySpec], tuple[NormalizeRule, ...]]


def build_rules_from_specs(
    specs: Sequence[NormalizeRuleFamilySpec],
) -> tuple[NormalizeRule, ...]:
    """Build normalize rules from rule-family specs.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Normalize rules derived from the specs.

    Raises
    ------
    KeyError
        Raised when a rule family spec references an unknown factory.
    """
    rules: list[NormalizeRule] = []
    for spec in specs:
        factory = RULE_FAMILY_FACTORIES.get(spec.factory)
        if factory is None:
            msg = f"Unknown normalize rule factory: {spec.factory!r}."
            raise KeyError(msg)
        rules.extend(factory(spec))
    return tuple(rules)


def _confidence_policy_from_spec(
    spec: NormalizeRuleFamilySpec,
) -> ConfidencePolicy | None:
    if spec.confidence_policy is None:
        return None
    return resolve_confidence_policy(spec.confidence_policy)


def _ambiguity_policy_from_spec(
    spec: NormalizeRuleFamilySpec,
) -> AmbiguityPolicy | None:
    if spec.ambiguity_policy is None:
        return None
    return resolve_ambiguity_policy(spec.ambiguity_policy)


def _rule_config(
    spec: NormalizeRuleFamilySpec,
    *,
    evidence_sources: Sequence[str] = (),
    confidence_policy: ConfidencePolicy | None = None,
    ambiguity_policy: AmbiguityPolicy | None = None,
    evidence_output: EvidenceOutput | None = None,
) -> RuleConfig:
    resolved_confidence = (
        confidence_policy if confidence_policy is not None else _confidence_policy_from_spec(spec)
    )
    resolved_ambiguity = (
        ambiguity_policy if ambiguity_policy is not None else _ambiguity_policy_from_spec(spec)
    )
    return RuleConfig(
        evidence_sources=tuple(evidence_sources),
        confidence_policy=resolved_confidence,
        ambiguity_policy=resolved_ambiguity,
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
) -> tuple[NormalizeRule, ...]:
    """Return normalize rules for type expressions and nodes.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Type-related normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _rule(
            RuleSpec(
                name="type_exprs_norm",
                output="type_exprs_norm_v1",
                inputs=("cst_type_exprs",),
                derive=_derive_type_exprs,
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=("cst_type_exprs",),
                evidence_output=EvidenceOutput(
                    column_map={"role": "expr_role"},
                    literals={"source": "cst_type_exprs"},
                ),
            ),
        ),
        _rule(
            RuleSpec(
                name="type_nodes",
                output="type_nodes_v1",
                inputs=("type_exprs_norm_v1", "scip_symbol_information"),
                derive=_derive_type_nodes,
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=(),
                evidence_output=EvidenceOutput(
                    column_map={"role": "type_form"},
                    literals={"source": "type_nodes"},
                ),
            ),
        ),
    )


def build_bytecode_cfg_rules(
    spec: NormalizeRuleFamilySpec,
) -> tuple[NormalizeRule, ...]:
    """Return normalize rules for bytecode CFG outputs.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Bytecode normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _rule(
            RuleSpec(
                name="cfg_blocks_norm",
                output="py_bc_blocks_norm_v1",
                inputs=("py_bc_blocks", "py_bc_code_units"),
                derive=_derive_cfg_blocks,
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=("py_bc_blocks",),
                evidence_output=EvidenceOutput(
                    column_map={
                        "bstart": "start_offset",
                        "bend": "end_offset",
                        "role": "kind",
                    },
                    literals={"source": "py_bc_blocks"},
                ),
            ),
        ),
        _rule(
            RuleSpec(
                name="cfg_edges_norm",
                output="py_bc_cfg_edges_norm_v1",
                inputs=("py_bc_cfg_edges", "py_bc_code_units"),
                derive=_derive_cfg_edges,
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=("py_bc_cfg_edges",),
                evidence_output=EvidenceOutput(
                    column_map={"role": "kind"},
                    literals={"source": "py_bc_cfg_edges"},
                ),
            ),
        ),
    )


def build_bytecode_dfg_rules(
    spec: NormalizeRuleFamilySpec,
) -> tuple[NormalizeRule, ...]:
    """Return normalize rules for bytecode DFG outputs.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Bytecode DFG normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _rule(
            RuleSpec(
                name="def_use_events",
                output="py_bc_def_use_events_v1",
                inputs=("py_bc_instructions",),
                derive=_derive_def_use_events,
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=("py_bc_instructions",),
                evidence_output=EvidenceOutput(
                    column_map={
                        "bstart": "offset",
                        "bend": "offset",
                        "role": "kind",
                    },
                    literals={"source": "py_bc_instructions"},
                ),
            ),
        ),
        _rule(
            RuleSpec(
                name="reaching_defs",
                output="py_bc_reaches_v1",
                inputs=("py_bc_def_use_events_v1",),
                derive=_derive_reaches,
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=(),
                evidence_output=EvidenceOutput(
                    literals={"source": "py_bc_reaches"},
                ),
            ),
        ),
    )


def build_diagnostics_rules(
    spec: NormalizeRuleFamilySpec,
) -> tuple[NormalizeRule, ...]:
    """Return normalize rules for diagnostics outputs.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Diagnostics normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _rule(
            RuleSpec(
                name="diagnostics_norm",
                output="diagnostics_norm_v1",
                inputs=(
                    "cst_parse_errors",
                    "ts_errors",
                    "ts_missing",
                    "scip_diagnostics",
                    "scip_documents",
                ),
                derive=_derive_diagnostics,
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=(),
                evidence_output=EvidenceOutput(
                    column_map={"role": "severity", "source": "diag_source"},
                ),
            ),
        ),
    )


def build_span_error_rules(
    spec: NormalizeRuleFamilySpec,
) -> tuple[NormalizeRule, ...]:
    """Return normalize rules for span error outputs.

    Returns
    -------
    tuple[NormalizeRule, ...]
        Span error normalize rule definitions.
    """
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _rule(
            RuleSpec(
                name="span_errors",
                output="span_errors_v1",
                inputs=(),
                derive=_derive_span_errors,
                execution_mode=execution_mode,
            ),
            config=_rule_config(
                spec,
                evidence_sources=(),
                evidence_output=EvidenceOutput(
                    column_map={"role": "reason"},
                    literals={"source": "span_errors"},
                ),
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


def _rule(spec: RuleSpec, *, config: RuleConfig | None = None) -> NormalizeRule:
    config = config or RuleConfig()
    evidence = _evidence_spec(config.evidence_sources)
    return NormalizeRule(
        name=spec.name,
        output=spec.output,
        inputs=tuple(spec.inputs),
        derive=spec.derive,
        evidence=evidence,
        confidence_policy=config.confidence_policy,
        ambiguity_policy=config.ambiguity_policy,
        evidence_output=config.evidence_output,
        execution_mode=spec.execution_mode,
    )


def _evidence_spec(sources: Sequence[str]) -> EvidenceSpec | None:
    if not sources:
        return None
    return EvidenceSpec(sources=tuple(sources))


def _derive_type_exprs(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    source = catalog.tables.get("cst_type_exprs")
    if source is None:
        return None
    return type_exprs_plan(source, ctx=ctx)


def _derive_type_nodes(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    type_exprs_norm = catalog.tables.get("type_exprs_norm_v1")
    if type_exprs_norm is None:
        return None
    scip_info = catalog.tables.get("scip_symbol_information")
    return type_nodes_plan(type_exprs_norm, scip_info, ctx=ctx)


def _derive_cfg_blocks(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    blocks = catalog.tables.get("py_bc_blocks")
    code_units = catalog.tables.get("py_bc_code_units")
    if blocks is None or code_units is None:
        return None
    return cfg_blocks_plan(blocks, code_units, ctx=ctx)


def _derive_cfg_edges(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    edges = catalog.tables.get("py_bc_cfg_edges")
    code_units = catalog.tables.get("py_bc_code_units")
    if edges is None or code_units is None:
        return None
    return cfg_edges_plan(code_units, edges, ctx=ctx)


def _derive_def_use_events(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    instructions = catalog.tables.get("py_bc_instructions")
    if instructions is None:
        return None
    return def_use_events_plan(instructions, ctx=ctx)


def _derive_reaches(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    def_use = catalog.tables.get("py_bc_def_use_events_v1")
    if def_use is None:
        return None
    return reaching_defs_plan(def_use, ctx=ctx)


def _derive_diagnostics(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    repo_text_index = getattr(catalog, "repo_text_index", None)
    if not isinstance(repo_text_index, RepoTextIndex):
        return None
    sources = DiagnosticsSources(
        cst_parse_errors=_table_from_source(catalog.tables.get("cst_parse_errors"), ctx=ctx),
        ts_errors=_table_from_source(catalog.tables.get("ts_errors"), ctx=ctx),
        ts_missing=_table_from_source(catalog.tables.get("ts_missing"), ctx=ctx),
        scip_diagnostics=_table_from_source(catalog.tables.get("scip_diagnostics"), ctx=ctx),
        scip_documents=_table_from_source(catalog.tables.get("scip_documents"), ctx=ctx),
    )
    return diagnostics_plan(repo_text_index, sources=sources, ctx=ctx)


def _derive_span_errors(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    _ = ctx
    source = catalog.tables.get("span_errors_v1")
    if source is None:
        return None
    return plan_from_source(source, ctx=ctx, label="span_errors")


def _table_from_source(source: PlanSource | None, *, ctx: ExecutionContext) -> TableLike | None:
    if source is None:
        return None
    if isinstance(source, TableLike):
        return source
    if isinstance(source, Plan):
        result = run_plan(
            source,
            ctx=ctx,
            prefer_reader=False,
            attach_ordering_metadata=True,
        )
        return cast("TableLike", result.value)
    plan = plan_from_source(source, ctx=ctx, label="normalize_rule_source")
    result = run_plan(
        plan,
        ctx=ctx,
        prefer_reader=False,
        attach_ordering_metadata=True,
    )
    return cast("TableLike", result.value)


__all__ = [
    "RULE_FAMILY_FACTORIES",
    "RuleFamilyFactory",
    "build_bytecode_cfg_rules",
    "build_bytecode_dfg_rules",
    "build_diagnostics_rules",
    "build_rules_from_specs",
    "build_span_error_rules",
    "build_type_rules",
]
