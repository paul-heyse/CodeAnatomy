"""Configuration contracts for Delta control-plane operations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DeltaControlConfig:
    """High-level controls for Delta control-plane behavior."""

    require_native_extension: bool = True
    emit_observability: bool = True


__all__ = ["DeltaControlConfig"]
