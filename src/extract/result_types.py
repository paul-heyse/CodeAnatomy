"""Generic result containers for extractors."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractResult[T]:
    """Generic extract result container."""

    table: T
    extractor_name: str


__all__ = ["ExtractResult"]
