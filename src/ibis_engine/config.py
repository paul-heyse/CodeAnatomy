"""Ibis backend configuration models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class IbisBackendConfig:
    """Configuration for building an Ibis backend session."""

    engine: Literal["duckdb", "datafusion"] = "duckdb"
    database: str = ":memory:"
    read_only: bool = False
    extensions: tuple[str, ...] = ()
    config: dict[str, object] = field(default_factory=dict)
    filesystem: object | None = None
    datafusion_profile: DataFusionRuntimeProfile | None = None
