"""Cache policy contracts and defaults for CQ runtime caching."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import msgspec

from tools.cq.core.runtime.execution_policy import default_runtime_execution_policy
from tools.cq.core.structs import CqSettingsStruct

_DEFAULT_DIR = ".cq_cache"

PositiveInt = Annotated[int, msgspec.Meta(ge=1)]
PositiveFloat = Annotated[float, msgspec.Meta(gt=0.0)]


class CqCachePolicyV1(CqSettingsStruct, frozen=True):
    """Policy controlling disk-backed CQ cache behavior."""

    enabled: bool = True
    directory: str = _DEFAULT_DIR
    shards: PositiveInt = 8
    timeout_seconds: PositiveFloat = 0.05
    ttl_seconds: PositiveInt = 900


def default_cache_policy(*, root: Path) -> CqCachePolicyV1:
    """Build cache policy from runtime defaults and optional env overrides.

    Returns:
        Cache policy resolved from runtime defaults and environment.
    """
    runtime = default_runtime_execution_policy().cache
    raw_enabled = os.getenv("CQ_CACHE_ENABLED")
    enabled = runtime.enabled
    if raw_enabled is not None:
        enabled = raw_enabled.strip().lower() not in {"0", "false", "no", "off"}

    raw_dir = os.getenv("CQ_CACHE_DIR")
    directory = raw_dir.strip() if raw_dir else str(root / _DEFAULT_DIR)

    raw_ttl = os.getenv("CQ_CACHE_TTL_SECONDS")
    ttl_seconds = runtime.ttl_seconds
    if raw_ttl:
        try:
            ttl_seconds = max(1, int(raw_ttl))
        except ValueError:
            ttl_seconds = runtime.ttl_seconds

    return CqCachePolicyV1(
        enabled=enabled,
        directory=directory,
        shards=runtime.shards,
        timeout_seconds=runtime.timeout_seconds,
        ttl_seconds=ttl_seconds,
    )


__all__ = ["CqCachePolicyV1", "default_cache_policy"]
