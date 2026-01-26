"""Dependency inference from SQLGlot expression analysis."""

from __future__ import annotations

import contextlib
import importlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from sqlglot_tools.lineage import referenced_tables, referenced_udf_calls
from sqlglot_tools.optimizer import (
    ast_policy_fingerprint,
    canonical_ast_fingerprint,
    resolve_sqlglot_policy,
    sqlglot_policy_snapshot_for,
)

if TYPE_CHECKING:
    from datafusion_engine.schema_contracts import SchemaContract
    from datafusion_engine.view_graph_registry import ViewNode
    from schema_spec.system import DatasetSpec
    from sqlglot_tools.compat import Expression


@dataclass(frozen=True)
class InferredDeps:
    """Dependencies inferred from Ibis/SQLGlot expression analysis.

    Captures table and column-level dependencies by analyzing the actual
    query plan rather than relying on declared inputs.

    Attributes
    ----------
    task_name : str
        Name of the task these dependencies apply to.
    output : str
        Output dataset name produced by the task.
    inputs : tuple[str, ...]
        Table names inferred from expression analysis.
    required_columns : Mapping[str, tuple[str, ...]]
        Per-table columns required by the expression.
    required_types : Mapping[str, tuple[tuple[str, str], ...]]
        Per-table required column/type pairs.
    required_metadata : Mapping[str, tuple[tuple[bytes, bytes], ...]]
        Per-table required metadata entries.
    plan_fingerprint : str
        Stable hash for caching and comparison.
    required_udfs : tuple[str, ...]
        Required UDF names inferred from the expression.
    """

    task_name: str
    output: str
    inputs: tuple[str, ...]
    required_columns: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    required_types: Mapping[str, tuple[tuple[str, str], ...]] = field(default_factory=dict)
    required_metadata: Mapping[str, tuple[tuple[bytes, bytes], ...]] = field(default_factory=dict)
    plan_fingerprint: str = ""
    required_udfs: tuple[str, ...] = ()


@dataclass(frozen=True)
class InferredDepsInputs:
    """Inputs needed to infer dependencies from SQLGlot."""

    expr: object
    task_name: str
    output: str
    dialect: str = "datafusion"
    snapshot: Mapping[str, object] | None = None
    required_udfs: Sequence[str] | None = None


def infer_deps_from_sqlglot_expr(
    inputs: InferredDepsInputs,
) -> InferredDeps:
    """Infer dependencies from a raw SQLGlot expression.

    Lower-level variant that works directly with SQLGlot expressions
    when an Ibis plan is not available.

    Parameters
    ----------
    inputs : InferredDepsInputs
        Dependency inference inputs including AST and task metadata.

    Returns
    -------
    InferredDeps
        Inferred dependencies.

    Raises
    ------
    TypeError
        Raised when expr is not a SQLGlot Expression.
    """
    from sqlglot_tools.compat import Expression

    expr = inputs.expr
    if not isinstance(expr, Expression):
        msg = f"Expected SQLGlot Expression, got {type(expr).__name__}"
        raise TypeError(msg)
    _ = inputs.dialect

    # Extract table references
    tables = referenced_tables(expr)
    columns_by_table = _required_columns_from_sqlglot(expr)

    required_types = _required_types_from_registry(columns_by_table)
    required_metadata = _required_metadata_for_tables(columns_by_table)
    resolved_udfs: tuple[str, ...] = ()
    if inputs.required_udfs is not None:
        resolved_udfs = tuple(inputs.required_udfs)
    elif inputs.snapshot is not None:
        resolved_udfs = _required_udfs_from_ast(expr, snapshot=inputs.snapshot)
    else:
        resolved_udfs = tuple(sorted(referenced_udf_calls(expr)))
    if inputs.snapshot is not None and resolved_udfs:
        from datafusion_engine.udf_runtime import validate_required_udfs

        validate_required_udfs(inputs.snapshot, required=resolved_udfs)

    # Compute plan fingerprint
    policy = resolve_sqlglot_policy(name="datafusion_compile")
    policy_hash = sqlglot_policy_snapshot_for(policy).policy_hash
    fingerprint = ast_policy_fingerprint(
        ast_fingerprint=canonical_ast_fingerprint(expr),
        policy_hash=policy_hash,
    )

    return InferredDeps(
        task_name=inputs.task_name,
        output=inputs.output,
        inputs=tables,
        required_columns=columns_by_table,
        required_types=required_types,
        required_metadata=required_metadata,
        plan_fingerprint=fingerprint,
        required_udfs=resolved_udfs,
    )


def infer_deps_from_view_nodes(
    nodes: Sequence[ViewNode],
    *,
    snapshot: Mapping[str, object] | None = None,
) -> tuple[InferredDeps, ...]:
    """Infer dependencies for view nodes using their SQLGlot ASTs.

    Parameters
    ----------
    nodes : Sequence[ViewNode]
        View nodes with SQLGlot ASTs attached.
    snapshot : Mapping[str, object] | None
        Optional Rust UDF snapshot used for required UDF validation.

    Returns
    -------
    tuple[InferredDeps, ...]
        Inferred dependency records for each view node.

    Raises
    ------
    ValueError
        Raised when a view node lacks a SQLGlot AST.
    """
    inferred: list[InferredDeps] = []
    for node in nodes:
        if node.sqlglot_ast is None:
            msg = f"View node {node.name!r} is missing a SQLGlot AST."
            raise ValueError(msg)
        inferred.append(
            infer_deps_from_sqlglot_expr(
                InferredDepsInputs(
                    expr=node.sqlglot_ast,
                    task_name=node.name,
                    output=node.name,
                    snapshot=snapshot,
                    required_udfs=tuple(node.required_udfs) if node.required_udfs else None,
                )
            )
        )
    return tuple(inferred)


def _required_udfs_from_ast(
    expr: Expression,
    *,
    snapshot: Mapping[str, object],
) -> tuple[str, ...]:
    from datafusion_engine.udf_runtime import udf_names_from_snapshot

    udf_calls = referenced_udf_calls(expr)
    if not udf_calls:
        return ()
    snapshot_names = udf_names_from_snapshot(snapshot)
    lookup = {name.lower(): name for name in snapshot_names}
    required = {
        lookup[name.lower()]
        for name in udf_calls
        if isinstance(name, str) and name.lower() in lookup
    }
    return tuple(sorted(required))


def _required_metadata_for_tables(
    columns_by_table: Mapping[str, tuple[str, ...]],
) -> dict[str, tuple[tuple[bytes, bytes], ...]]:
    required: dict[str, tuple[tuple[bytes, bytes], ...]] = {}
    for table_name in columns_by_table:
        spec = _dataset_spec_for_table(table_name)
        if spec is None:
            continue
        metadata = spec.schema().metadata
        if metadata:
            required[table_name] = tuple(sorted(metadata.items(), key=lambda item: item[0]))
    return required


def _required_columns_from_sqlglot(
    expr: Expression,
) -> dict[str, tuple[str, ...]]:
    from sqlglot_tools.compat import exp

    required: dict[str, set[str]] = {}
    for column in expr.find_all(exp.Column):
        table = column.table
        if not table:
            continue
        required.setdefault(table, set()).add(column.name)
    return {table: tuple(sorted(cols)) for table, cols in required.items()}


def _required_types_from_registry(
    columns_by_table: Mapping[str, tuple[str, ...]],
) -> dict[str, tuple[tuple[str, str], ...]]:
    required: dict[str, tuple[tuple[str, str], ...]] = {}
    for table_name, columns in columns_by_table.items():
        contract = _schema_contract_for_table(table_name)
        if contract is None:
            continue
        pairs = _types_from_contract(contract, columns)
        if pairs:
            required[table_name] = pairs
    return required


def _optional_module_attr(module: str, attr: str) -> object | None:
    try:
        loaded = importlib.import_module(module)
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    return getattr(loaded, attr, None)


def _extract_dataset_spec(name: str) -> DatasetSpec | None:
    extract_dataset_spec = _optional_module_attr(
        "datafusion_engine.extract_registry",
        "dataset_spec",
    )
    if not callable(extract_dataset_spec):
        return None
    extract_dataset_spec = cast("Callable[[str], DatasetSpec]", extract_dataset_spec)
    try:
        return extract_dataset_spec(name)
    except KeyError:
        return None


def _normalize_dataset_spec(name: str) -> DatasetSpec | None:
    normalize_dataset_spec = _optional_module_attr(
        "normalize.registry_runtime",
        "dataset_spec",
    )
    if not callable(normalize_dataset_spec):
        return None
    normalize_dataset_spec = cast("Callable[..., DatasetSpec]", normalize_dataset_spec)
    try:
        return normalize_dataset_spec(name, ctx=None)
    except KeyError:
        return None


def _incremental_dataset_spec(name: str) -> DatasetSpec | None:
    incremental_dataset_spec = _optional_module_attr(
        "incremental.registry_specs",
        "dataset_spec",
    )
    if not callable(incremental_dataset_spec):
        return None
    incremental_dataset_spec = cast("Callable[[str], DatasetSpec]", incremental_dataset_spec)
    try:
        return incremental_dataset_spec(name)
    except KeyError:
        return None


def _relationship_dataset_spec(name: str) -> DatasetSpec | None:
    relationship_dataset_specs = _optional_module_attr(
        "schema_spec.relationship_specs",
        "relationship_dataset_specs",
    )
    if not callable(relationship_dataset_specs):
        return None
    relationship_dataset_specs = cast(
        "Callable[[], Sequence[DatasetSpec]]",
        relationship_dataset_specs,
    )
    with contextlib.suppress(RuntimeError, TypeError, ValueError):
        for spec in relationship_dataset_specs():
            if spec.name == name:
                return spec
    return None


def _dataset_spec_for_table(name: str) -> DatasetSpec | None:
    resolvers = (
        _extract_dataset_spec,
        _normalize_dataset_spec,
        _incremental_dataset_spec,
        _relationship_dataset_spec,
    )
    for resolver in resolvers:
        spec = resolver(name)
        if spec is not None:
            return spec
    return None


def _schema_contract_for_table(name: str) -> SchemaContract | None:
    spec = _dataset_spec_for_table(name)
    if spec is None:
        return None
    try:
        from datafusion_engine.schema_contracts import schema_contract_from_dataset_spec
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    return schema_contract_from_dataset_spec(name=spec.name, spec=spec)


def _types_from_contract(
    contract: SchemaContract,
    columns: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    if not contract.enforce_columns:
        return ()
    by_name = {col.name: col for col in contract.columns}
    pairs: list[tuple[str, str]] = []
    for name in columns:
        column = by_name.get(name)
        if column is not None:
            pairs.append((name, str(column.arrow_type)))
    return tuple(pairs)


__all__ = [
    "InferredDeps",
    "infer_deps_from_sqlglot_expr",
]
