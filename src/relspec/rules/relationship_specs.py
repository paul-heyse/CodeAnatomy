"""Relationship rule definitions for relspec compilation."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache

from relspec.model import HashJoinConfig, IntervalAlignConfig, RuleKind
from relspec.rules.decorators import rule_bundle
from relspec.rules.definitions import EvidenceSpec, RelationshipPayload, RuleDefinition


@rule_bundle(name="relspec.relationships", domain="cpg")
@cache
def relationship_rule_definitions() -> tuple[RuleDefinition, ...]:
    """Return relationship rule definitions for relspec compilation.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Relationship rule definitions.
    """
    return (
        _interval_align_rule(
            _IntervalAlignRuleSpec(
                name="cst_refs__to__scip_occurrences",
                output_dataset="rel_name_symbol",
                contract_name="rel_name_symbol_v1",
                inputs=("cst_refs", "scip_occurrences"),
                left_start_col="bstart",
                left_end_col="bend",
                select_left=("ref_id", "path", "bstart", "bend"),
            )
        ),
        _interval_align_rule(
            _IntervalAlignRuleSpec(
                name="cst_imports__to__scip_occurrences",
                output_dataset="rel_import_symbol",
                contract_name="rel_import_symbol_v1",
                inputs=("cst_imports", "scip_occurrences"),
                left_start_col="bstart",
                left_end_col="bend",
                select_left=("import_alias_id", "path", "bstart", "bend"),
            )
        ),
        _interval_align_rule(
            _IntervalAlignRuleSpec(
                name="cst_defs__name_to__scip_occurrences",
                output_dataset="rel_def_symbol",
                contract_name="rel_def_symbol_v1",
                inputs=("cst_defs", "scip_occurrences"),
                left_start_col="bstart",
                left_end_col="bend",
                select_left=("def_id", "path", "bstart", "bend"),
            )
        ),
        _interval_align_rule(
            _IntervalAlignRuleSpec(
                name="cst_callsites__callee_to__scip_occurrences",
                output_dataset="rel_callsite_symbol",
                contract_name="rel_callsite_symbol_v1",
                inputs=("cst_callsites", "scip_occurrences"),
                left_start_col="callee_bstart",
                left_end_col="callee_bend",
                select_left=("call_id", "path", "call_bstart", "call_bend"),
            )
        ),
        _hash_join_rule(
            name="callsite_qname_candidates__join__dim_qualified_names",
            output_dataset="rel_callsite_qname",
            contract_name="rel_callsite_qname_v1",
            inputs=("callsite_qname_candidates", "dim_qualified_names"),
        ),
    )


@dataclass(frozen=True)
class _IntervalAlignRuleSpec:
    """Specification for interval-align relationship rules."""

    name: str
    output_dataset: str
    contract_name: str
    inputs: tuple[str, str]
    left_start_col: str
    left_end_col: str
    select_left: tuple[str, ...]


def _interval_align_rule(spec: _IntervalAlignRuleSpec) -> RuleDefinition:
    """Build an interval-align rule definition from a spec.

    Parameters
    ----------
    spec
        Interval-align rule specification.

    Returns
    -------
    RuleDefinition
        Rule definition for the interval-align rule.
    """
    payload = RelationshipPayload(
        output_dataset=spec.output_dataset,
        contract_name=spec.contract_name,
        interval_align=IntervalAlignConfig(
            mode="CONTAINED_BEST",
            how="inner",
            left_path_col="path",
            left_start_col=spec.left_start_col,
            left_end_col=spec.left_end_col,
            right_path_col="path",
            right_start_col="bstart",
            right_end_col="bend",
            select_left=spec.select_left,
            select_right=("symbol", "symbol_roles"),
            emit_match_meta=False,
        ),
    )
    return RuleDefinition(
        name=spec.name,
        domain="cpg",
        kind=RuleKind.INTERVAL_ALIGN.value,
        inputs=spec.inputs,
        output=spec.output_dataset,
        priority=0,
        evidence=EvidenceSpec(sources=spec.inputs),
        payload=payload,
    )


def _hash_join_rule(
    *,
    name: str,
    output_dataset: str,
    contract_name: str,
    inputs: tuple[str, str],
) -> RuleDefinition:
    """Build a hash-join rule definition.

    Parameters
    ----------
    name
        Rule name.
    output_dataset
        Output dataset name.
    contract_name
        Contract name for the output dataset.
    inputs
        Input dataset names.

    Returns
    -------
    RuleDefinition
        Rule definition for the hash-join rule.
    """
    payload = RelationshipPayload(
        output_dataset=output_dataset,
        contract_name=contract_name,
        hash_join=HashJoinConfig(
            join_type="inner",
            left_keys=("qname",),
            right_keys=("qname",),
            left_output=("call_id", "path", "call_bstart", "call_bend", "qname_source"),
            right_output=("qname_id",),
        ),
    )
    return RuleDefinition(
        name=name,
        domain="cpg",
        kind=RuleKind.HASH_JOIN.value,
        inputs=inputs,
        output=output_dataset,
        priority=10,
        evidence=EvidenceSpec(sources=inputs),
        payload=payload,
    )


__all__ = ["relationship_rule_definitions"]
