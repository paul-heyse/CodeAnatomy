"""Chunk normalization helpers for Arrow tables."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from core.config_base import FingerprintableConfig, config_fingerprint
from datafusion_engine.arrow.dictionary import normalize_dictionaries
from datafusion_engine.arrow.interop import TableLike


@dataclass(frozen=True)
class ChunkPolicy(FingerprintableConfig):
    """Normalization policy for dictionary encoding and chunking."""

    unify_dictionaries: bool = True
    combine_chunks: bool = True

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the chunk policy.

        Returns
        -------
        Mapping[str, object]
            Payload describing chunk policy settings.
        """
        return {
            "unify_dictionaries": self.unify_dictionaries,
            "combine_chunks": self.combine_chunks,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the chunk policy.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())

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
