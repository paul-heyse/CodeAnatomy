"""Ibis backend configuration models."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class IbisBackendConfig:
    """Configuration for building an Ibis backend session."""

    database: str = ":memory:"
    read_only: bool = False
    extensions: tuple[str, ...] = ()
    config: dict[str, object] = field(default_factory=dict)
    filesystem: object | None = None
