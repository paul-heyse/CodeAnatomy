"""Normalization helpers for schema encoding and chunk layout."""

from __future__ import annotations

from dataclasses import dataclass, field

from arrowdsl.compute.kernels import ChunkPolicy
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.encoding_policy import EncodingPolicy, apply_encoding


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


__all__ = ["NormalizePolicy"]
