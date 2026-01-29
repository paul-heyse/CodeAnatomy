"""Encoding helpers for DataFusion-backed table normalization."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.types as patypes
from datafusion import SessionContext, col, lit
from datafusion import functions as f

from datafusion_engine.arrow_interop import (
    DataTypeLike,
    RecordBatchReaderLike,
    TableLike,
    coerce_table_like,
)
from datafusion_engine.arrow_schema.chunking import ChunkPolicy
from datafusion_engine.arrow_schema.encoding import EncodingPolicy
from datafusion_engine.introspection import invalidate_introspection_cache

if TYPE_CHECKING:
    from datafusion.expr import Expr

DEFAULT_DICTIONARY_INDEX_TYPE = pa.int32()


@dataclass(frozen=True)
class NormalizePolicy:
    """Encoding policy plus chunk normalization."""

    encoding: EncodingPolicy
    chunk: ChunkPolicy = field(default_factory=ChunkPolicy)

    def apply(self, table: TableLike) -> TableLike:
        """Apply encoding and chunk normalization to a table.

        Returns
        -------
        TableLike
            Normalized table.
        """
        encoded = apply_encoding(table, policy=self.encoding)
        return self.chunk.apply(encoded)


def apply_encoding(table: TableLike, *, policy: EncodingPolicy) -> TableLike:
    """Apply dictionary encoding to requested columns.

    Returns
    -------
    TableLike
        Table with dictionary-encoded columns applied.
    """
    if not policy.dictionary_cols:
        return table
    from datafusion_engine.ingest import datafusion_from_arrow

    df_ctx = _datafusion_context()
    resolved = _ensure_table(table)
    table_name = f"_encoding_{uuid.uuid4().hex}"
    df = datafusion_from_arrow(df_ctx, name=table_name, value=resolved)
    try:
        selections = _encoding_select_expr(
            schema=resolved.schema,
            policy=policy,
            ctx=df_ctx,
        )
        return df.select(*selections).to_arrow_table()
    finally:
        deregister = getattr(df_ctx, "deregister_table", None)
        if callable(deregister):
            deregister(table_name)
            invalidate_introspection_cache(df_ctx)


def encode_table(table: TableLike, *, columns: Sequence[str]) -> TableLike:
    """Dictionary-encode specified columns on a table.

    Returns
    -------
    TableLike
        Table with encoded columns.
    """
    if not columns:
        return table
    policy = EncodingPolicy(dictionary_cols=frozenset(columns))
    return apply_encoding(table, policy=policy)


def _encoding_select_expr(
    *,
    schema: pa.Schema,
    policy: EncodingPolicy,
    ctx: SessionContext,
) -> list[Expr]:
    selections: list[Expr] = []
    for schema_field in schema:
        name = schema_field.name
        if name not in policy.dictionary_cols:
            selections.append(col(name))
            continue
        if patypes.is_dictionary(schema_field.type):
            selections.append(col(name))
            continue
        index_type = policy.dictionary_index_types.get(name, policy.dictionary_index_type)
        if index_type is None:
            index_type = DEFAULT_DICTIONARY_INDEX_TYPE
        ordered = policy.dictionary_ordered_flags.get(name, policy.dictionary_ordered)
        dict_type = _dictionary_type_name(
            ctx,
            index_type,
            schema_field.type,
            ordered=ordered,
        )
        selections.append(f.arrow_cast(col(name), lit(dict_type)).alias(name))
    return selections


def _datafusion_context() -> SessionContext:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

    profile = DataFusionRuntimeProfile()
    return profile.session_runtime().ctx


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
    from datafusion_engine.ingest import datafusion_from_arrow

    df = datafusion_from_arrow(ctx, name=temp_name, value=table)
    try:
        df = df.select(f.arrow_typeof(col("value")).alias("dtype")).limit(1)
        value = df.to_arrow_table()["dtype"][0].as_py()
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
    index_type: DataTypeLike,
    value_type: DataTypeLike,
    *,
    ordered: bool,
) -> str:
    index_name = _arrow_type_name(ctx, _ensure_arrow_dtype(index_type))
    value_name = _arrow_type_name(ctx, _ensure_arrow_dtype(value_type))
    _ = ordered
    return f"Dictionary({index_name}, {value_name})"


def _ensure_arrow_dtype(dtype: DataTypeLike) -> pa.DataType:
    if isinstance(dtype, pa.DataType):
        return dtype
    msg = f"Expected pyarrow.DataType, got {type(dtype)!r}."
    raise TypeError(msg)


__all__ = [
    "DEFAULT_DICTIONARY_INDEX_TYPE",
    "NormalizePolicy",
    "apply_encoding",
    "encode_table",
]
