"""Normalize op specs derived from centralized rule definitions."""

from __future__ import annotations

from dataclasses import dataclass

from normalize.registry_runtime import dataset_alias
from relspec.rules.definitions import RuleDefinition


@dataclass(frozen=True)
class NormalizeOpSpec:
    """Specification for normalize op dependency expansion.

    Note: The ``inputs`` field is deprecated for manual specification.
    Dependencies are now inferred from Ibis/DataFusion expression analysis.
    The field is retained for backward compatibility but defaults to empty.
    """

    name: str
    outputs: tuple[str, ...]
    inputs: tuple[str, ...] = ()  # Deprecated: now inferred from expression analysis


def _rule_op_specs(definitions: tuple[RuleDefinition, ...]) -> tuple[NormalizeOpSpec, ...]:
    """Build NormalizeOpSpec instances from rule definitions.

    Note: The inputs are set to empty since dependencies are now inferred
    from expression analysis rather than declared in rule definitions.

    Returns
    -------
    tuple[NormalizeOpSpec, ...]
        Normalize op specs derived from rule definitions.
    """
    specs: list[NormalizeOpSpec] = []
    for rule in definitions:
        output = dataset_alias(rule.output)
        # Dependencies are now inferred from expression analysis
        specs.append(NormalizeOpSpec(name=output, outputs=(output,)))
    return tuple(specs)


# Extra op specs for normalize operations not derived from rule definitions.
# Note: inputs are no longer manually specified; dependencies are inferred
# from expression analysis at runtime.
_EXTRA_OP_SPECS: tuple[NormalizeOpSpec, ...] = (
    NormalizeOpSpec(
        name="cst_imports_norm",
        outputs=("cst_imports_norm", "cst_imports"),
    ),
    NormalizeOpSpec(
        name="cst_defs_norm",
        outputs=("cst_defs_norm", "cst_defs"),
    ),
    NormalizeOpSpec(
        name="scip_occurrences_norm",
        outputs=("scip_occurrences_norm", "scip_occurrences", "scip_span_errors"),
    ),
    NormalizeOpSpec(
        name="dim_qualified_names",
        outputs=("dim_qualified_names",),
    ),
    NormalizeOpSpec(
        name="callsite_qname_candidates",
        outputs=("callsite_qname_candidates",),
    ),
    NormalizeOpSpec(
        name="ast_nodes_norm",
        outputs=("ast_nodes_norm",),
    ),
    NormalizeOpSpec(
        name="py_bc_instructions_norm",
        outputs=("py_bc_instructions_norm",),
    ),
)


def normalize_op_specs(
    definitions: tuple[RuleDefinition, ...],
) -> tuple[NormalizeOpSpec, ...]:
    """Return normalize op specs derived from rule definitions.

    Returns
    -------
    tuple[NormalizeOpSpec, ...]
        Normalize op specs in deterministic order.
    """
    specs = {spec.name: spec for spec in _rule_op_specs(definitions)}
    for spec in _EXTRA_OP_SPECS:
        specs.setdefault(spec.name, spec)
    return tuple(specs[name] for name in sorted(specs))


__all__ = ["NormalizeOpSpec", "normalize_op_specs"]
