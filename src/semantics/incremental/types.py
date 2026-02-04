"""Incremental pipeline configuration types."""

from __future__ import annotations

from dataclasses import dataclass

from semantics.incremental.config import SemanticIncrementalConfig


@dataclass(frozen=True)
class IncrementalImpact:
    """Impact scope derived from incremental diffs and closures."""

    changed_file_ids: tuple[str, ...] = ()
    deleted_file_ids: tuple[str, ...] = ()
    impacted_file_ids: tuple[str, ...] = ()
    full_refresh: bool = False


@dataclass(frozen=True)
class IncrementalFileChanges:
    """File change sets derived from Delta CDF updates."""

    changed_file_ids: tuple[str, ...] = ()
    deleted_file_ids: tuple[str, ...] = ()
    full_refresh: bool = False


__all__ = [
    "IncrementalFileChanges",
    "IncrementalImpact",
    "SemanticIncrementalConfig",
]
