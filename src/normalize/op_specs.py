"""Normalize op specs derived from centralized rule definitions."""

from __future__ import annotations

from dataclasses import dataclass

from normalize.registry_specs import dataset_alias, dataset_names
from relspec.rules.definitions import RuleDefinition


@dataclass(frozen=True)
class NormalizeOpSpec:
    """Specification for normalize op dependency expansion."""

    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


_NORMALIZE_DATASETS: frozenset[str] = frozenset(dataset_names())


def _maybe_alias(name: str) -> str:
    if name in _NORMALIZE_DATASETS:
        return dataset_alias(name)
    return name


def _rule_op_specs(definitions: tuple[RuleDefinition, ...]) -> tuple[NormalizeOpSpec, ...]:
    specs: list[NormalizeOpSpec] = []
    for rule in definitions:
        output = dataset_alias(rule.output)
        inputs = tuple(_maybe_alias(name) for name in rule.inputs)
        specs.append(NormalizeOpSpec(name=output, inputs=inputs, outputs=(output,)))
    return tuple(specs)


_EXTRA_OP_SPECS: tuple[NormalizeOpSpec, ...] = (
    NormalizeOpSpec(
        name="cst_imports_norm",
        inputs=("cst_imports",),
        outputs=("cst_imports_norm", "cst_imports"),
    ),
    NormalizeOpSpec(
        name="cst_defs_norm",
        inputs=("cst_defs",),
        outputs=("cst_defs_norm", "cst_defs"),
    ),
    NormalizeOpSpec(
        name="scip_occurrences_norm",
        inputs=("scip_documents", "scip_occurrences", "file_line_index"),
        outputs=("scip_occurrences_norm", "scip_occurrences", "scip_span_errors"),
    ),
    NormalizeOpSpec(
        name="dim_qualified_names",
        inputs=("cst_callsites", "cst_defs"),
        outputs=("dim_qualified_names",),
    ),
    NormalizeOpSpec(
        name="callsite_qname_candidates",
        inputs=("cst_callsites",),
        outputs=("callsite_qname_candidates",),
    ),
    NormalizeOpSpec(
        name="ast_nodes_norm",
        inputs=("ast_nodes", "file_line_index"),
        outputs=("ast_nodes_norm",),
    ),
    NormalizeOpSpec(
        name="py_bc_instructions_norm",
        inputs=("py_bc_instructions", "file_line_index"),
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
