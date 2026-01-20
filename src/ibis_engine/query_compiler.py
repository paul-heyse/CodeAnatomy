"""QuerySpec-style compilation into Ibis expressions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import cast

from ibis.expr.types import BooleanValue, Table, Value

from arrowdsl.core.expr_types import ScalarValue
from arrowdsl.core.interop import SchemaLike
from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.expr_compiler import (
    ExprIRLike,
    IbisExprRegistry,
    default_expr_registry,
    expr_ir_to_ibis,
)
from ibis_engine.macros import IbisMacroSpec, apply_macros

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
        return table.semi_join(
            self.param_table,
            (table[self.file_id_column] == self.param_table[self.param_key_column],),
        )


@dataclass(frozen=True)
class IbisProjectionSpec:
    """Projection spec for Ibis query compilation."""

    base: tuple[str, ...]
    derived: Mapping[str, ExprIRLike] = field(default_factory=dict)


@dataclass(frozen=True)
class IbisQuerySpec:
    """Declarative query spec for Ibis execution."""

    projection: IbisProjectionSpec
    predicate: ExprIRLike | None = None
    pushdown_predicate: ExprIRLike | None = None
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
    param_table: Table | None = None
    param_key_column: str | None = None
    param_table_threshold: int = FILE_ID_PARAM_THRESHOLD


def dataset_query_for_file_ids(
    file_ids: Sequence[str],
    *,
    schema: SchemaLike | None = None,
    columns: Sequence[str] | None = None,
    options: FileIdQueryOptions | None = None,
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
        predicate = _literal_expr(value=False)
        return IbisQuerySpec(
            projection=IbisProjectionSpec(base=tuple(columns)),
            predicate=predicate,
            pushdown_predicate=predicate,
        )
    if (
        resolved_options.param_table is not None
        and len(file_ids) >= resolved_options.param_table_threshold
    ):
        key_column = resolved_options.param_key_column or resolved_options.file_id_column
        return IbisQuerySpec(
            projection=IbisProjectionSpec(base=tuple(columns)),
            macros=(
                FileIdParamMacro(
                    param_table=resolved_options.param_table,
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
    registry: IbisExprRegistry | None = None,
) -> Table:
    """Apply a query spec to an Ibis table.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table with filters and projections applied.
    """
    registry = registry or default_expr_registry()
    if spec.macros:
        table = apply_macros(table, macros=spec.macros)
    table = _apply_derived(table, spec.projection.derived, registry=registry)
    cols = _projection_columns(table, spec.projection.base, spec.projection.derived)
    if cols:
        table = table.select(cols)
    if spec.pushdown_predicate is not None:
        predicate = expr_ir_to_ibis(spec.pushdown_predicate, table, registry=registry)
        table = table.filter(cast("BooleanValue", predicate))
    if spec.predicate is not None:
        predicate = expr_ir_to_ibis(spec.predicate, table, registry=registry)
        table = table.filter(cast("BooleanValue", predicate))
    return table


def apply_projection(
    table: Table,
    *,
    base: Sequence[str],
    derived: Mapping[str, ExprIRLike] | None = None,
    registry: IbisExprRegistry | None = None,
) -> Table:
    """Apply a projection with optional derived columns.

    Returns
    -------
    ibis.expr.types.Table
        Ibis table with projection applied.
    """
    derived = derived or {}
    registry = registry or default_expr_registry()
    table = _apply_derived(table, derived, registry=registry)
    cols = _projection_columns(table, base, derived)
    if not cols:
        return table
    return table.select(cols)


def _apply_derived(
    table: Table,
    derived: Mapping[str, ExprIRLike],
    *,
    registry: IbisExprRegistry,
) -> Table:
    out = table
    for name, expr in derived.items():
        out = out.mutate(**{name: expr_ir_to_ibis(expr, out, registry=registry)})
    return out


def _projection_columns(
    table: Table,
    base: Sequence[str],
    derived: Mapping[str, ExprIRLike],
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


def _field_expr(name: str) -> ExprIR:
    return ExprIR(op="field", name=name)


def _literal_expr(value: ScalarValue) -> ExprIR:
    return ExprIR(op="literal", value=value)


def _call_expr(name: str, *args: ExprIR) -> ExprIR:
    return ExprIR(op="call", name=name, args=tuple(args))


def _or_exprs(exprs: Sequence[ExprIR]) -> ExprIR:
    if not exprs:
        return _literal_expr(value=False)
    out = exprs[0]
    for expr in exprs[1:]:
        out = _call_expr("bit_wise_or", out, expr)
    return out


def _in_set_expr(name: str, values: Sequence[str]) -> ExprIR:
    exprs = [_call_expr("equal", _field_expr(name), _literal_expr(value)) for value in values]
    return _or_exprs(exprs)
