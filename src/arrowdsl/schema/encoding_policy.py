"""Encoding policy helpers for ArrowDSL schemas."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.types as patypes
from datafusion import SessionContext, SQLOptions

from arrowdsl.core.interop import (
    DataTypeLike,
    RecordBatchReaderLike,
    TableLike,
    coerce_table_like,
)
from datafusion_engine.introspection import invalidate_introspection_cache
from sqlglot_tools.compat import Expression, exp

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

DEFAULT_DICTIONARY_INDEX_TYPE = pa.int32()


@dataclass(frozen=True)
class EncodingSpec:
    """Column-level dictionary encoding specification."""

    column: str
    index_type: DataTypeLike | None = None
    ordered: bool | None = None


@dataclass(frozen=True)
class EncodingPolicy:
    """Dictionary encoding policy for schema alignment."""

    dictionary_cols: frozenset[str] = field(default_factory=frozenset)
    specs: tuple[EncodingSpec, ...] = ()
    dictionary_index_type: DataTypeLike = DEFAULT_DICTIONARY_INDEX_TYPE
    dictionary_ordered: bool = False
    dictionary_index_types: dict[str, DataTypeLike] = field(default_factory=dict)
    dictionary_ordered_flags: dict[str, bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize derived encoding fields from specs."""
        if not self.specs:
            return
        if not self.dictionary_cols:
            cols = frozenset(spec.column for spec in self.specs)
            object.__setattr__(self, "dictionary_cols", cols)
        if not self.dictionary_index_types:
            index_types = {
                spec.column: spec.index_type for spec in self.specs if spec.index_type is not None
            }
            object.__setattr__(self, "dictionary_index_types", index_types)
        if not self.dictionary_ordered_flags:
            ordered_flags = {
                spec.column: spec.ordered for spec in self.specs if spec.ordered is not None
            }
            object.__setattr__(self, "dictionary_ordered_flags", ordered_flags)

    def apply(self, table: TableLike) -> TableLike:
        """Apply dictionary encoding policy to a table.

        Returns
        -------
        TableLike
            Table with dictionary encoding applied.
        """
        return apply_encoding(table, policy=self)


def apply_encoding(table: TableLike, *, policy: EncodingPolicy) -> TableLike:
    """Apply dictionary encoding to requested columns.

    Returns
    -------
    TableLike
        Table with dictionary-encoded columns applied.
    """
    if not policy.dictionary_cols:
        return table
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    df_ctx = _datafusion_context()
    resolved = _ensure_table(table)
    table_name = f"_encoding_{uuid.uuid4().hex}"
    adapter = DataFusionIOAdapter(ctx=df_ctx, profile=None)
    adapter.register_record_batches(table_name, [resolved.to_batches()])
    try:
        expr = _encoding_select_expr(
            schema=resolved.schema,
            policy=policy,
            ctx=df_ctx,
            table_name=table_name,
        )
        return _expr_table(df_ctx, expr)
    finally:
        deregister = getattr(df_ctx, "deregister_table", None)
        if callable(deregister):
            deregister(table_name)
            invalidate_introspection_cache(df_ctx)


def _encoding_select_expr(
    *,
    schema: pa.Schema,
    policy: EncodingPolicy,
    ctx: SessionContext,
    table_name: str,
) -> Expression:
    selections: list[Expression] = []
    for schema_field in schema:
        name = schema_field.name
        identifier = exp.column(name)
        if name not in policy.dictionary_cols:
            selections.append(identifier)
            continue
        if patypes.is_dictionary(schema_field.type):
            selections.append(identifier)
            continue
        index_type = policy.dictionary_index_types.get(name, policy.dictionary_index_type)
        ordered = policy.dictionary_ordered_flags.get(name, policy.dictionary_ordered)
        dict_type = _dictionary_type_name(
            ctx,
            index_type,
            schema_field.type,
            ordered=ordered,
        )
        selections.append(
            exp.func("arrow_cast", identifier, exp.Literal.string(dict_type)).as_(name)
        )
    return exp.select(*selections).from_(table_name)


def _datafusion_context() -> SessionContext:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    profile = DataFusionRuntimeProfile()
    return profile.session_context()


def _sql_options_for_profile(profile: DataFusionRuntimeProfile | None) -> SQLOptions:
    from datafusion_engine.sql_options import sql_options_for_profile

    return sql_options_for_profile(profile)


def _expr_table(ctx: SessionContext, expr: Expression) -> pa.Table:
    from datafusion_engine.compile_options import DataFusionCompileOptions, DataFusionSqlPolicy
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=None)
    plan = facade.compile(
        expr,
        options=DataFusionCompileOptions(
            sql_options=_sql_options_for_profile(None),
            sql_policy=DataFusionSqlPolicy(),
        ),
    )
    result = facade.execute(plan)
    if result.dataframe is None:
        msg = "Encoding policy execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return result.dataframe.to_arrow_table()


def _ensure_table(value: TableLike) -> pa.Table:
    resolved = coerce_table_like(value)
    if isinstance(resolved, RecordBatchReaderLike):
        return pa.Table.from_batches(list(resolved))
    if isinstance(resolved, pa.Table):
        return resolved
    return pa.table(resolved.to_pydict())


def _arrow_type_name(ctx: SessionContext, dtype: pa.DataType) -> str:
    temp_name = f"_dtype_{uuid.uuid4().hex}"
    table = pa.table({"value": pa.array([None], type=dtype)})
    from datafusion_engine.io_adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_record_batches(temp_name, [list(table.to_batches())])
    try:
        expr = (
            exp.select(exp.func("arrow_typeof", exp.column("value")).as_("dtype"))
            .from_(temp_name)
            .limit(1)
        )
        result_table = _expr_table(ctx, expr)
        value = result_table["dtype"][0].as_py()
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            deregister(temp_name)
    if not isinstance(value, str):
        msg = "Failed to resolve DataFusion type name."
        raise TypeError(msg)
    return value


def _dictionary_type_name(
    ctx: SessionContext,
    index_type: pa.DataType,
    value_type: pa.DataType,
    *,
    ordered: bool,
) -> str:
    index_name = _arrow_type_name(ctx, index_type)
    value_name = _arrow_type_name(ctx, value_type)
    _ = ordered
    return f"Dictionary({index_name}, {value_name})"


__all__ = [
    "DEFAULT_DICTIONARY_INDEX_TYPE",
    "EncodingPolicy",
    "EncodingSpec",
    "apply_encoding",
]
