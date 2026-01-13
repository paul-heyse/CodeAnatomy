"""Default rule-family specs for normalize rule generation."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache


@dataclass(frozen=True)
class NormalizeRuleFamilySpec:
    """Specification for a normalize rule family."""

    name: str
    factory: str
    inputs: tuple[str, ...] = ()
    output: str | None = None
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None
    option_flag: str | None = None
    execution_mode: str | None = None


RULE_FAMILY_SPECS: tuple[NormalizeRuleFamilySpec, ...] = (
    NormalizeRuleFamilySpec(
        name="types",
        factory="types",
        inputs=("cst_type_exprs", "scip_symbol_information"),
    ),
    NormalizeRuleFamilySpec(
        name="bytecode_cfg",
        factory="bytecode_cfg",
        inputs=("py_bc_blocks", "py_bc_code_units", "py_bc_cfg_edges"),
    ),
    NormalizeRuleFamilySpec(
        name="bytecode_dfg",
        factory="bytecode_dfg",
        inputs=("py_bc_instructions",),
    ),
    NormalizeRuleFamilySpec(
        name="diagnostics",
        factory="diagnostics",
        inputs=(
            "cst_parse_errors",
            "ts_errors",
            "ts_missing",
            "scip_diagnostics",
            "scip_documents",
        ),
    ),
    NormalizeRuleFamilySpec(
        name="span_errors",
        factory="span_errors",
        inputs=("span_errors_v1",),
    ),
)


@cache
def rule_family_specs() -> tuple[NormalizeRuleFamilySpec, ...]:
    """Return default rule-family specs.

    Returns
    -------
    tuple[NormalizeRuleFamilySpec, ...]
        Rule family specification entries.
    """
    return RULE_FAMILY_SPECS


__all__ = ["NormalizeRuleFamilySpec", "rule_family_specs"]
