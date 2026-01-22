"""Ibis backend configuration models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class IbisBackendConfig:
    """Configuration for building an Ibis backend session."""

    datafusion_profile: DataFusionRuntimeProfile | None = None
    fuse_selects: bool | None = None
    default_limit: int | None = None
    default_dialect: str | None = None
    interactive: bool | None = None
    object_stores: tuple[ObjectStoreConfig, ...] = ()


@dataclass(frozen=True)
class ObjectStoreConfig:
    """DataFusion object store registration entry."""

    scheme: str
    store: object
    host: str | None = None
