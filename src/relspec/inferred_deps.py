"""Dependency inference from SQLGlot expression analysis."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlglot_tools.lineage import referenced_tables
from sqlglot_tools.optimizer import plan_fingerprint

if TYPE_CHECKING:
    from datafusion_engine.schema_contracts import SchemaContract
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
    """

    task_name: str
    output: str
    inputs: tuple[str, ...]
    required_columns: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    required_types: Mapping[str, tuple[tuple[str, str], ...]] = field(default_factory=dict)
    required_metadata: Mapping[str, tuple[tuple[bytes, bytes], ...]] = field(default_factory=dict)
    plan_fingerprint: str = ""


def infer_deps_from_sqlglot_expr(
    expr: object,
    *,
    task_name: str,
    output: str,
    dialect: str = "datafusion",
) -> InferredDeps:
    """Infer dependencies from a raw SQLGlot expression.

    Lower-level variant that works directly with SQLGlot expressions
    when an Ibis plan is not available.

    Parameters
    ----------
    expr : Expression
        SQLGlot expression to analyze.
    task_name : str
        Name of the task being analyzed.
    output : str
        Output dataset name.
    dialect : str
        SQL dialect for fingerprinting.

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

    if not isinstance(expr, Expression):
        msg = f"Expected SQLGlot Expression, got {type(expr).__name__}"
        raise TypeError(msg)

    # Extract table references
    tables = referenced_tables(expr)
    columns_by_table = _required_columns_from_sqlglot(expr)

    required_types = _required_types_from_registry(columns_by_table)
    required_metadata = _required_metadata_for_tables(columns_by_table)

    # Compute plan fingerprint
    fingerprint = plan_fingerprint(expr, dialect=dialect)

    return InferredDeps(
        task_name=task_name,
        output=output,
        inputs=tables,
        required_columns=columns_by_table,
        required_types=required_types,
        required_metadata=required_metadata,
        plan_fingerprint=fingerprint,
    )


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


def _dataset_spec_for_table(name: str) -> DatasetSpec | None:
    try:
        from normalize.registry_runtime import dataset_spec as normalize_dataset_spec
    except (ImportError, RuntimeError, TypeError, ValueError):
        normalize_dataset_spec = None
    if normalize_dataset_spec is not None:
        try:
            return normalize_dataset_spec(name)
        except KeyError:
            pass
    try:
        from relspec.contracts import (
            REL_CALLSITE_QNAME_NAME,
            REL_CALLSITE_SYMBOL_NAME,
            REL_DEF_SYMBOL_NAME,
            REL_IMPORT_SYMBOL_NAME,
            REL_NAME_SYMBOL_NAME,
            RELATION_OUTPUT_NAME,
            rel_callsite_qname_spec,
            rel_callsite_symbol_spec,
            rel_def_symbol_spec,
            rel_import_symbol_spec,
            rel_name_symbol_spec,
            relation_output_spec,
        )
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    relspec_specs: dict[str, Callable[[], DatasetSpec]] = {
        REL_NAME_SYMBOL_NAME: rel_name_symbol_spec,
        REL_IMPORT_SYMBOL_NAME: rel_import_symbol_spec,
        REL_DEF_SYMBOL_NAME: rel_def_symbol_spec,
        REL_CALLSITE_SYMBOL_NAME: rel_callsite_symbol_spec,
        REL_CALLSITE_QNAME_NAME: rel_callsite_qname_spec,
        RELATION_OUTPUT_NAME: relation_output_spec,
    }
    spec_factory = relspec_specs.get(name)
    if spec_factory is None:
        try:
            from cpg import schemas as cpg_schemas
        except (ImportError, RuntimeError, TypeError, ValueError):
            return None
        cpg_specs: dict[str, DatasetSpec] = {
            "cpg_nodes_v1": cpg_schemas.CPG_NODES_SPEC,
            "cpg_edges_v1": cpg_schemas.CPG_EDGES_SPEC,
            "cpg_props_v1": cpg_schemas.CPG_PROPS_SPEC,
            "cpg_props_json_v1": cpg_schemas.CPG_PROPS_JSON_SPEC,
            "cpg_props_by_file_id_v1": cpg_schemas.CPG_PROPS_BY_FILE_ID_SPEC,
            "cpg_props_global_v1": cpg_schemas.CPG_PROPS_GLOBAL_SPEC,
        }
        return cpg_specs.get(name)
    return spec_factory()


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
