"""Encoding policy helpers for ArrowDSL schemas."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.core.interop import DataTypeLike, TableLike, pc

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
    out = table
    for name in policy.dictionary_cols:
        if name not in out.column_names:
            continue
        arr = out[name]
        if patypes.is_dictionary(arr.type):
            continue
        index_type = policy.dictionary_index_types.get(name, policy.dictionary_index_type)
        ordered = policy.dictionary_ordered_flags.get(name, policy.dictionary_ordered)
        encoded = pc.dictionary_encode(arr)
        target_type = pa.dictionary(
            index_type,
            arr.type,
            ordered=ordered,
        )
        encoded = pc.cast(encoded, target_type)
        idx = out.schema.get_field_index(name)
        out = out.set_column(idx, name, encoded)
    return out


__all__ = [
    "DEFAULT_DICTIONARY_INDEX_TYPE",
    "EncodingPolicy",
    "EncodingSpec",
    "apply_encoding",
]
