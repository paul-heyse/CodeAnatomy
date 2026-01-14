"""Registry of normalization operations used by relationship evidence."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache

from normalize.registry_specs import dataset_alias, dataset_names
from relspec.adapters import NormalizeRuleAdapter
from relspec.rules.definitions import RuleDefinition
from relspec.rules.registry import RuleRegistry


@dataclass(frozen=True)
class NormalizeOp:
    """Named normalization operator mapping inputs to outputs."""

    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


_NORMALIZE_DATASETS: frozenset[str] = frozenset(dataset_names())
_ORDERED_OP_NAMES: tuple[str, ...] = (
    "cst_imports_norm",
    "cst_defs_norm",
    "scip_occurrences_norm",
    "diagnostics_norm",
    "type_exprs_norm",
    "types_norm",
    "dim_qualified_names",
    "callsite_qname_candidates",
    "ast_nodes_norm",
    "py_bc_instructions_norm",
    "py_bc_blocks_norm",
    "py_bc_cfg_edges_norm",
    "py_bc_def_use_events",
    "py_bc_reaching_defs",
)


def _maybe_alias(name: str) -> str:
    if name in _NORMALIZE_DATASETS:
        return dataset_alias(name)
    return name


def _normalize_rule_ops() -> tuple[NormalizeOp, ...]:
    ops: list[NormalizeOp] = []
    for rule in _normalize_rule_definitions():
        output = dataset_alias(rule.output)
        inputs = tuple(_maybe_alias(name) for name in rule.inputs)
        ops.append(NormalizeOp(name=output, inputs=inputs, outputs=(output,)))
    return tuple(ops)


@cache
def _normalize_rule_definitions() -> tuple[RuleDefinition, ...]:
    registry = RuleRegistry(adapters=(NormalizeRuleAdapter(),))
    return registry.rules_for_domain("normalize")


_EXTRA_OPS: tuple[NormalizeOp, ...] = (
    NormalizeOp(
        name="cst_imports_norm",
        inputs=("cst_imports",),
        outputs=("cst_imports_norm", "cst_imports"),
    ),
    NormalizeOp(
        name="cst_defs_norm",
        inputs=("cst_defs",),
        outputs=("cst_defs_norm", "cst_defs"),
    ),
    NormalizeOp(
        name="scip_occurrences_norm",
        inputs=("scip_documents", "scip_occurrences", "repo_text_index"),
        outputs=("scip_occurrences_norm", "scip_occurrences", "scip_span_errors"),
    ),
    NormalizeOp(
        name="dim_qualified_names",
        inputs=("cst_callsites", "cst_defs"),
        outputs=("dim_qualified_names",),
    ),
    NormalizeOp(
        name="callsite_qname_candidates",
        inputs=("cst_callsites",),
        outputs=("callsite_qname_candidates",),
    ),
    NormalizeOp(
        name="ast_nodes_norm",
        inputs=("ast_nodes", "repo_text_index"),
        outputs=("ast_nodes_norm",),
    ),
    NormalizeOp(
        name="py_bc_instructions_norm",
        inputs=("py_bc_instructions", "repo_text_index"),
        outputs=("py_bc_instructions_norm",),
    ),
)

_RULE_OPS_BY_NAME: dict[str, NormalizeOp] = {op.name: op for op in _normalize_rule_ops()}
_EXTRA_OPS_BY_NAME: dict[str, NormalizeOp] = {op.name: op for op in _EXTRA_OPS}

_NORMALIZE_OPS: tuple[NormalizeOp, ...] = tuple(
    op
    for name in _ORDERED_OP_NAMES
    for op in (_RULE_OPS_BY_NAME.get(name) or _EXTRA_OPS_BY_NAME.get(name),)
    if op is not None
) + tuple(op for name, op in _RULE_OPS_BY_NAME.items() if name not in _ORDERED_OP_NAMES)

_OPS_BY_NAME: dict[str, NormalizeOp] = {op.name: op for op in _NORMALIZE_OPS}
_OPS_BY_OUTPUT: dict[str, tuple[NormalizeOp, ...]] = {}
for _op in _NORMALIZE_OPS:
    for _output in _op.outputs:
        _OPS_BY_OUTPUT.setdefault(_output, ())
        _OPS_BY_OUTPUT[_output] = (*_OPS_BY_OUTPUT[_output], _op)


def normalize_op(name: str) -> NormalizeOp:
    """Return a normalization operator by name.

    Returns
    -------
    NormalizeOp
        Registered normalization operator.
    """
    return _OPS_BY_NAME[name]


def normalize_ops() -> tuple[NormalizeOp, ...]:
    """Return all registered normalization operators.

    Returns
    -------
    tuple[NormalizeOp, ...]
        Normalization operators in registry order.
    """
    return _NORMALIZE_OPS


def normalize_ops_for_output(name: str) -> tuple[NormalizeOp, ...]:
    """Return normalization operators that produce a given output.

    Returns
    -------
    tuple[NormalizeOp, ...]
        Operators producing the output name.
    """
    return _OPS_BY_OUTPUT.get(name, ())


__all__ = ["NormalizeOp", "normalize_op", "normalize_ops", "normalize_ops_for_output"]
