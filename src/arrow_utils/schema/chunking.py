"""Chunk normalization helpers for Arrow tables."""

from __future__ import annotations

from dataclasses import dataclass

from arrow_utils.core.interop import TableLike
from arrow_utils.schema.dictionary import normalize_dictionaries


@dataclass(frozen=True)
class ChunkPolicy:
    """Normalization policy for dictionary encoding and chunking."""

    unify_dictionaries: bool = True
    combine_chunks: bool = True

    def apply(self, table: TableLike) -> TableLike:
        """Apply dictionary unification and chunk combination.

        Returns
        -------
        TableLike
            Normalized table with unified dictionaries and combined chunks.
        """
        out = table
        if self.unify_dictionaries:
            out = normalize_dictionaries(out, combine_chunks=False)
        if self.combine_chunks:
            out = out.combine_chunks()
        return out


__all__ = ["ChunkPolicy"]
