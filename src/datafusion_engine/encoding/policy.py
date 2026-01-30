"""Encoding helpers for DataFusion-backed table normalization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.types as patypes
from datafusion import SessionContext, col, lit
from datafusion import functions as f

from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.arrow.chunking import ChunkPolicy
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import (
    DataTypeLike,
    TableLike,
)
from datafusion_engine.session.helpers import temp_table
from utils.validation import ensure_table

if TYPE_CHECKING:
    from datafusion.expr import Expr

DEFAULT_DICTIONARY_INDEX_TYPE = pa.int32()


@dataclass(frozen=True)
class NormalizePolicy(FingerprintableConfig):
    """Encoding policy plus chunk normalization."""

    encoding: EncodingPolicy
    chunk: ChunkPolicy = field(default_factory=ChunkPolicy)

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for normalization policy.

        Returns
        -------
        Mapping[str, object]
            Payload describing normalization policy settings.
        """
        return {
            "encoding": self.encoding.fingerprint(),
            "chunk": self.chunk.fingerprint(),
        }

    def fingerprint(self) -> str:
        """Return fingerprint for normalization policy.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())

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
    df_ctx = _datafusion_context()
    resolved = ensure_table(table, label="table")
    with temp_table(df_ctx, resolved, prefix="_encoding_") as table_name:
        df = df_ctx.table(table_name)
        selections = _encoding_select_expr(
            schema=resolved.schema,
            policy=policy,
            ctx=df_ctx,
        )
        return df.select(*selections).to_arrow_table()


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
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

    profile = DataFusionRuntimeProfile()
    return profile.session_runtime().ctx


def _arrow_type_name(ctx: SessionContext, dtype: pa.DataType) -> str:
    table = pa.table({"value": pa.array([None], type=dtype)})
    with temp_table(ctx, table, prefix="_dtype_") as temp_name:
        df = ctx.table(temp_name).select(f.arrow_typeof(col("value")).alias("dtype")).limit(1)
        value = df.to_arrow_table()["dtype"][0].as_py()
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
