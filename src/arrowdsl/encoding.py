"""Dictionary encoding helpers for categorical columns."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow.types as patypes

from arrowdsl.column_ops import set_or_append_column
from arrowdsl.compute import pc
from arrowdsl.kernels import ChunkPolicy
from arrowdsl.pyarrow_protocols import TableLike


@dataclass(frozen=True)
class EncodingSpec:
    """Specification for dictionary encoding a column."""

    column: str


@dataclass(frozen=True)
class EncodingPolicy:
    """Encoding policy bundling dictionary encodes and chunk normalization."""

    specs: tuple[EncodingSpec, ...] = ()
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)

    def apply(self, table: TableLike) -> TableLike:
        """Apply encoding specs and chunk policy.

        Returns
        -------
        TableLike
            Table with encoding and chunk normalization applied.
        """
        out = encode_columns(table, specs=self.specs)
        return self.chunk_policy.apply(out)


def encode_dictionary(table: TableLike, spec: EncodingSpec) -> TableLike:
    """Dictionary-encode a column.

    Parameters
    ----------
    table:
        Input table.
    spec:
        Encoding specification.

    Returns
    -------
    TableLike
        Updated table with dictionary-encoded column.
    """
    if spec.column not in table.column_names:
        return table
    column = table[spec.column]
    if patypes.is_dictionary(column.type):
        return table
    encoded = pc.dictionary_encode(column)
    return set_or_append_column(table, spec.column, encoded)


def encode_columns(table: TableLike, specs: tuple[EncodingSpec, ...]) -> TableLike:
    """Apply dictionary encoding for multiple columns.

    Parameters
    ----------
    table:
        Input table.
    specs:
        Encoding specs to apply.

    Returns
    -------
    TableLike
        Updated table.
    """
    out = table
    for spec in specs:
        out = encode_dictionary(out, spec)
    return out
