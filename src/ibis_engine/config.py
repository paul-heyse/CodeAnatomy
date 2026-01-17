"""Ibis backend configuration models."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class IbisBackendConfig:
    """Configuration for building an Ibis backend session."""

    datafusion_profile: DataFusionRuntimeProfile | None = None
    fuse_selects: bool | None = None
