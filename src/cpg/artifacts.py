"""Build artifacts for CPG table construction."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.core.interop import TableLike
from arrowdsl.finalize.finalize import FinalizeResult


@dataclass(frozen=True)
class CpgBuildArtifacts:
    """Finalized output plus quality artifacts."""

    finalize: FinalizeResult
    quality: TableLike


__all__ = ["CpgBuildArtifacts"]
