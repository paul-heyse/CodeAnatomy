"""QuerySpec-style compilation into Ibis expressions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import cast

import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import BooleanValue, Table, Value

from arrowdsl.core.interop import SchemaLike
from ibis_engine.expr_compiler import expr_ir_to_ibis
from ibis_engine.macros import IbisMacroSpec, apply_macros
from ibis_engine.param_tables import ParamTablePolicy, ParamTableRegistry, ParamTableSpec
from ibis_engine.params_bridge import list_param_join
from sqlglot_tools.expr_spec import ExprIR, SqlExprSpec

FILE_ID_PARAM_THRESHOLD = 500


@dataclass(frozen=True)
class FileIdParamMacro:
    """Macro that filters rows using a parameter table join."""

    param_table: Table
    file_id_column: str
    param_key_column: str

    def __call__(self, table: Table) -> Table:
        """Apply a semi-join against the parameter table.

        Returns
        -------
        ibis.expr.types.Table
            Filtered table containing rows that match the parameter table.
        """
        return list_param_join(
            table,
            param_table=self.param_table,
            left_col=self.file_id_column,
            right_col=self.param_key_column,
        )


@dataclass(frozen=True)
class IbisProjectionSpec:
    """Projection spec for Ibis query compilation."""

    base: tuple[str, ...]
    derived: Mapping[str, SqlExprSpec] = field(default_factory=dict)


@dataclass(frozen=True)
class IbisQuerySpec:
    """Declarative query spec for Ibis execution."""

    projection: IbisProjectionSpec
    predicate: SqlExprSpec | None = None
    pushdown_predicate: SqlExprSpec | None = None
    macros: tuple[IbisMacroSpec, ...] = ()

    @staticmethod
    def simple(*cols: str) -> IbisQuerySpec:
        """Return a simple query spec from column names.

        Returns
        -------
        IbisQuerySpec
            Query spec with base columns only.
        """
        return IbisQuerySpec(projection=IbisProjectionSpec(base=tuple(cols)))


def query_for_schema(schema: SchemaLike) -> IbisQuerySpec:
    """Return an IbisQuerySpec projecting the schema columns.

    Returns
    -------
    IbisQuerySpec
        Query spec with base columns set to the schema names.
    """
    return IbisQuerySpec(projection=IbisProjectionSpec(base=tuple(schema.names)))


@dataclass(frozen=True)
class FileIdQueryOptions:
    """Options for file-id filtered query compilation."""

    file_id_column: str = "file_id"
    param_table_name: str | None = None
    param_key_column: str | None = None
    param_table_threshold: int = FILE_ID_PARAM_THRESHOLD
    param_table_prefix: str = "p_"


def dataset_query_for_file_ids(
    file_ids: Sequence[str],
    *,
    schema: SchemaLike | None = None,
    columns: Sequence[str] | None = None,
    options: FileIdQueryOptions | None = None,
    backend: BaseBackend | None = None,
) -> IbisQuerySpec:
    """Return an IbisQuerySpec filtering to the provided file ids.

    Parameters
    ----------
    file_ids:
        File ids to include.
    schema:
        Optional schema used to build the projection.
    columns:
        Optional explicit projection columns.
    options:
        Optional file-id query options including param-table tuning.
    backend:
        Backend used to register parameter tables when needed.

    Returns
    -------
    IbisQuerySpec
        Query spec with file_id predicates for plan and pushdown lanes.

    Raises
    ------
    ValueError
        Raised when neither columns nor schema are provided.
    """
    resolved_options = options or FileIdQueryOptions()
    if columns is None:
        if schema is None:
            msg = "dataset_query_for_file_ids requires columns or schema."
            raise ValueError(msg)
        columns = list(schema.names)
    if not file_ids:
        predicate = _false_predicate()
        return IbisQuerySpec(
            projection=IbisProjectionSpec(base=tuple(columns)),
            predicate=predicate,
            pushdown_predicate=predicate,
        )
    if len(file_ids) >= resolved_options.param_table_threshold:
        if backend is None:
            msg = "dataset_query_for_file_ids requires a backend for param table registration."
            raise ValueError(msg)
        key_column = resolved_options.param_key_column or resolved_options.file_id_column
        logical_name = resolved_options.param_table_name or "file_id_params"
        schema = pa.schema([pa.field(key_column, pa.string())])
        registry = ParamTableRegistry(
            specs={
                logical_name: ParamTableSpec(
                    logical_name=logical_name,
                    key_col=key_column,
                    schema=schema,
                    distinct=True,
                )
            },
            policy=ParamTablePolicy(prefix=resolved_options.param_table_prefix),
        )
        artifact = registry.register_values(logical_name, list(file_ids))
        param_table = registry.ibis_tables(backend)[artifact.logical_name]
        return IbisQuerySpec(
            projection=IbisProjectionSpec(base=tuple(columns)),
            macros=(
                FileIdParamMacro(
                    param_table=param_table,
                    file_id_column=resolved_options.file_id_column,
                    param_key_column=key_column,
                ),
            ),
        )
    predicate = _in_set_expr(resolved_options.file_id_column, tuple(file_ids))
    return IbisQuerySpec(
        projection=IbisProjectionSpec(base=tuple(columns)),
        predicate=predicate,
        pushdown_predicate=predicate,
    )


def apply_query_spec(
    table: Table,
    *,
    spec: IbisQuerySpec,
) -> Table:
    """Apply a query spec to an Ibis table.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table with filters and projections applied.
    """
    if spec.macros:
        table = apply_macros(table, macros=spec.macros)
    table = _apply_derived(table, spec.projection.derived)
    cols = _projection_columns(table, spec.projection.base, spec.projection.derived)
    if cols:
        table = table.select(cols)
    if spec.pushdown_predicate is not None:
        table = _apply_predicate(table, spec.pushdown_predicate)
    if spec.predicate is not None:
        table = _apply_predicate(table, spec.predicate)
    return table


def apply_projection(
    table: Table,
    *,
    base: Sequence[str],
    derived: Mapping[str, SqlExprSpec] | None = None,
) -> Table:
    """Apply a projection with optional derived columns.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table with projection applied.
    """
    spec = IbisQuerySpec(
        projection=IbisProjectionSpec(base=tuple(base), derived=derived or {}),
    )
    return apply_query_spec(table, spec=spec)


def _apply_derived(
    table: Table,
    derived: Mapping[str, SqlExprSpec],
) -> Table:
    if not derived:
        return table
    mutations = {
        name: expr_ir_to_ibis(_require_expr_ir(expr), table) for name, expr in derived.items()
    }
    return table.mutate(**mutations)


def _projection_columns(
    table: Table,
    base: Sequence[str],
    derived: Mapping[str, SqlExprSpec],
) -> list[Value]:
    cols: list[Value] = []
    seen: set[str] = set()
    for name in base:
        if name in table.columns and name not in seen:
            cols.append(table[name])
            seen.add(name)
    for name in derived:
        if name in table.columns and name not in seen:
            cols.append(table[name])
            seen.add(name)
    return cols


def _apply_predicate(
    table: Table,
    predicate: SqlExprSpec,
) -> Table:
    compiled = expr_ir_to_ibis(_require_expr_ir(predicate), table)
    return table.filter(cast("BooleanValue", compiled))


def _false_predicate() -> SqlExprSpec:
    return SqlExprSpec(expr_ir=ExprIR(op="literal", value=False))


def _in_set_expr(name: str, values: Sequence[str]) -> SqlExprSpec:
    if not values:
        return _false_predicate()
    args = (
        ExprIR(op="field", name=name),
        *(ExprIR(op="literal", value=value) for value in values),
    )
    return SqlExprSpec(expr_ir=ExprIR(op="call", name="in_set", args=args))


def _require_expr_ir(spec: SqlExprSpec) -> ExprIR:
    expr_ir = spec.expr_ir
    if expr_ir is None:
        msg = "SqlExprSpec missing expr_ir; SQL execution is not supported."
        raise ValueError(msg)
    return expr_ir
