"""Registry of normalization operations used by relationship evidence."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NormalizeOp:
    """Named normalization operator mapping inputs to outputs."""

    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


_NORMALIZE_OPS: tuple[NormalizeOp, ...] = (
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
        name="diagnostics_norm",
        inputs=(
            "cst_parse_errors",
            "ts_errors",
            "ts_missing",
            "scip_diagnostics",
            "scip_documents",
            "repo_text_index",
        ),
        outputs=("diagnostics_norm",),
    ),
    NormalizeOp(
        name="type_exprs_norm",
        inputs=("cst_type_exprs",),
        outputs=("type_exprs_norm",),
    ),
    NormalizeOp(
        name="types_norm",
        inputs=("type_exprs_norm", "scip_symbol_information"),
        outputs=("types_norm",),
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
    NormalizeOp(
        name="py_bc_blocks_norm",
        inputs=("py_bc_blocks", "py_bc_code_units"),
        outputs=("py_bc_blocks_norm",),
    ),
    NormalizeOp(
        name="py_bc_cfg_edges_norm",
        inputs=("py_bc_cfg_edges", "py_bc_code_units"),
        outputs=("py_bc_cfg_edges_norm",),
    ),
    NormalizeOp(
        name="py_bc_def_use_events",
        inputs=("py_bc_instructions",),
        outputs=("py_bc_def_use_events",),
    ),
    NormalizeOp(
        name="py_bc_reaching_defs",
        inputs=("py_bc_def_use_events",),
        outputs=("py_bc_reaching_defs",),
    ),
)

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
