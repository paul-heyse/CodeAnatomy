"""Encoding helpers for DataFusion-backed table normalization."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.types as patypes

from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.arrow.chunking import ChunkPolicy
from datafusion_engine.arrow.coercion import ensure_arrow_table
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.arrow.types import DEFAULT_DICTIONARY_INDEX_TYPE
from datafusion_engine.schema.type_resolution import ensure_arrow_dtype

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NormalizePolicy(FingerprintableConfig):
    """Encoding policy plus chunk normalization."""

    encoding: EncodingPolicy
    chunk: ChunkPolicy = field(default_factory=ChunkPolicy)

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for normalization policy.

        Returns:
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

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())

    def apply(self, table: TableLike) -> TableLike:
        """Apply encoding and chunk normalization to a table.

        Returns:
        -------
        TableLike
            Normalized table.
        """
        encoded = apply_encoding(table, policy=self.encoding)
        return self.chunk.apply(encoded)


def apply_encoding(table: TableLike, *, policy: EncodingPolicy) -> TableLike:
    """Apply dictionary encoding to requested columns.

    Returns:
    -------
    TableLike
        Table with dictionary-encoded columns applied.
    """
    if not policy.dictionary_cols:
        logger.debug("Encoding policy contains no dictionary columns; returning input table")
        return table
    resolved = ensure_arrow_table(table, label="table")
    logger.debug(
        "Applying dictionary encoding to %s columns for table with %s rows",
        len(policy.dictionary_cols),
        resolved.num_rows,
    )
    encoded_columns: list[pa.ChunkedArray] = []
    encoded_fields: list[pa.Field] = []
    for schema_field, column in zip(resolved.schema, resolved.columns, strict=True):
        if schema_field.name not in policy.dictionary_cols or patypes.is_dictionary(
            schema_field.type
        ):
            encoded_columns.append(column)
            encoded_fields.append(schema_field)
            continue
        index_type = policy.dictionary_index_types.get(
            schema_field.name,
            policy.dictionary_index_type,
        )
        if index_type is None:
            index_type = DEFAULT_DICTIONARY_INDEX_TYPE
        ordered = policy.dictionary_ordered_flags.get(
            schema_field.name,
            policy.dictionary_ordered,
        )
        dict_type = pa.dictionary(
            ensure_arrow_dtype(index_type),
            ensure_arrow_dtype(schema_field.type),
            ordered=ordered,
        )
        encoded_column = column.dictionary_encode().cast(dict_type)
        encoded_columns.append(encoded_column)
        encoded_fields.append(
            pa.field(
                schema_field.name,
                encoded_column.type,
                nullable=schema_field.nullable,
                metadata=schema_field.metadata,
            ),
        )
    encoded_schema = pa.schema(encoded_fields, metadata=resolved.schema.metadata)
    return pa.Table.from_arrays(encoded_columns, schema=encoded_schema)


def encode_table(table: TableLike, *, columns: Sequence[str]) -> TableLike:
    """Dictionary-encode specified columns on a table.

    Returns:
    -------
    TableLike
        Table with encoded columns.
    """
    if not columns:
        return table
    policy = EncodingPolicy(dictionary_cols=frozenset(columns))
    return apply_encoding(table, policy=policy)


__all__ = [
    "DEFAULT_DICTIONARY_INDEX_TYPE",
    "NormalizePolicy",
    "apply_encoding",
    "encode_table",
]
