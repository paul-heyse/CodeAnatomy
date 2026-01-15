"""Chunk normalization helpers for Arrow tables."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.core.interop import TableLike


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
            out = out.unify_dictionaries()
        if self.combine_chunks:
            out = out.combine_chunks()
        return out


__all__ = ["ChunkPolicy"]
