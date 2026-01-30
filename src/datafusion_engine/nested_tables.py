"""Utilities for working with nested DataFusion tables."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SimpleViewRef:
    """Simple reference to a DataFusion-registered view by name."""

    name: str


__all__ = ["SimpleViewRef"]
