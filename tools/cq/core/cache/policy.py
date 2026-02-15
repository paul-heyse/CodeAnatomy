"""Cache policy contracts and defaults for CQ runtime caching."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import msgspec

from tools.cq.core.runtime.env_namespace import (
    NamespacePatternV1,
    parse_namespace_bool_overrides,
    parse_namespace_int_overrides,
)
from tools.cq.core.runtime.execution_policy import default_runtime_execution_policy
from tools.cq.core.structs import CqSettingsStruct

_DEFAULT_DIR = ".cq_cache"

PositiveInt = Annotated[int, msgspec.Meta(ge=1)]
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]
PositiveFloat = Annotated[float, msgspec.Meta(gt=0.0)]

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


class CqCachePolicyV1(CqSettingsStruct, frozen=True):
    """Policy controlling disk-backed CQ cache behavior."""

    enabled: bool = True
    directory: str = _DEFAULT_DIR
    shards: PositiveInt = 8
    timeout_seconds: PositiveFloat = 0.05
    ttl_seconds: PositiveInt = 900
    evict_run_tag_on_exit: bool = False
    namespace_ttl_seconds: dict[str, int] = msgspec.field(default_factory=dict)
    namespace_enabled: dict[str, bool] = msgspec.field(default_factory=dict)
    namespace_ephemeral: dict[str, bool] = msgspec.field(default_factory=dict)
    size_limit_bytes: PositiveInt = 2_147_483_648
    cull_limit: NonNegativeInt = 16
    eviction_policy: str = "least-recently-stored"
    statistics_enabled: bool = False
    max_tree_sitter_lanes: PositiveInt = 4
    lane_lock_ttl_seconds: PositiveInt = 15
    sqlite_mmap_size: NonNegativeInt = 0
    sqlite_cache_size: NonNegativeInt = 0
    transaction_batch_size: PositiveInt = 128


class _ResolvedCacheScalarSettings(CqSettingsStruct, frozen=True):
    directory: str
    ttl_seconds: int
    shards: int
    timeout_seconds: float
    size_limit_bytes: int
    cull_limit: int
    eviction_policy: str
    statistics_enabled: bool
    max_tree_sitter_lanes: int
    lane_lock_ttl_seconds: int
    sqlite_mmap_size: int
    sqlite_cache_size: int
    transaction_batch_size: int


def _env_bool(raw: str | None, *, default: bool) -> bool:
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return default


def _env_int(raw: str | None, *, default: int, minimum: int = 1) -> int:
    if raw is None:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return max(minimum, parsed)


def _resolve_namespace_ttl_from_env(
    *,
    defaults: dict[str, int],
) -> dict[str, int]:
    resolved = dict(defaults)
    parsed = parse_namespace_int_overrides(
        env=os.environ,
        patterns=(
            NamespacePatternV1(prefix="CQ_CACHE_NAMESPACE_", suffix="_TTL_SECONDS"),
            NamespacePatternV1(prefix="CQ_CACHE_TTL_", suffix="_SECONDS"),
        ),
        minimum=1,
    )
    for namespace, value in parsed.values.items():
        if isinstance(value, int):
            resolved[str(namespace)] = _env_int(
                str(value),
                default=resolved.get(str(namespace), 900),
                minimum=1,
            )
    return resolved


def _resolve_namespace_enabled_from_env(
    *,
    defaults: dict[str, bool],
) -> dict[str, bool]:
    resolved = dict(defaults)
    parsed = parse_namespace_bool_overrides(
        env=os.environ,
        patterns=(
            NamespacePatternV1(prefix="CQ_CACHE_NAMESPACE_", suffix="_ENABLED"),
            NamespacePatternV1(prefix="CQ_CACHE_ENABLE_"),
        ),
    )
    for namespace, value in parsed.values.items():
        if isinstance(value, bool):
            resolved[str(namespace)] = _env_bool(
                "1" if value else "0",
                default=resolved.get(str(namespace), True),
            )
    return resolved


def _resolve_namespace_ephemeral_from_env(
    *,
    defaults: dict[str, bool],
) -> dict[str, bool]:
    resolved = dict(defaults)
    parsed = parse_namespace_bool_overrides(
        env=os.environ,
        patterns=(NamespacePatternV1(prefix="CQ_CACHE_EPHEMERAL_"),),
    )
    for namespace, value in parsed.values.items():
        if isinstance(value, bool):
            resolved[str(namespace)] = _env_bool(
                "1" if value else "0",
                default=resolved.get(str(namespace), False),
            )
    return resolved


def _resolve_cache_scalar_settings(
    *,
    root: Path,
    runtime: object,
) -> _ResolvedCacheScalarSettings:
    directory_value = os.getenv("CQ_CACHE_DIR")
    directory = directory_value.strip() if directory_value else str(root / _DEFAULT_DIR)
    statistics_enabled = _env_bool(
        os.getenv("CQ_CACHE_STATISTICS_ENABLED"),
        default=bool(getattr(runtime, "statistics_enabled", False)),
    )
    if not statistics_enabled:
        statistics_enabled = _env_bool(
            os.getenv("CQ_CACHE_STATS_ENABLED"),
            default=bool(getattr(runtime, "statistics_enabled", False)),
        )
    return _ResolvedCacheScalarSettings(
        directory=directory,
        ttl_seconds=_env_int(
            os.getenv("CQ_CACHE_TTL_SECONDS"),
            default=int(getattr(runtime, "ttl_seconds", 900)),
            minimum=1,
        ),
        shards=_env_int(
            os.getenv("CQ_CACHE_SHARDS"),
            default=int(getattr(runtime, "shards", 8)),
            minimum=1,
        ),
        timeout_seconds=max(
            0.001,
            float(
                os.getenv("CQ_CACHE_TIMEOUT_SECONDS") or getattr(runtime, "timeout_seconds", 0.05)
            ),
        ),
        size_limit_bytes=_env_int(
            os.getenv("CQ_CACHE_SIZE_LIMIT_BYTES"),
            default=int(getattr(runtime, "size_limit_bytes", 2_147_483_648)),
            minimum=1,
        ),
        cull_limit=_env_int(
            os.getenv("CQ_CACHE_CULL_LIMIT"),
            default=int(getattr(runtime, "cull_limit", 16)),
            minimum=0,
        ),
        eviction_policy=(
            os.getenv("CQ_CACHE_EVICTION_POLICY")
            or getattr(runtime, "eviction_policy", "least-recently-stored")
        ).strip()
        or str(getattr(runtime, "eviction_policy", "least-recently-stored")),
        statistics_enabled=statistics_enabled,
        max_tree_sitter_lanes=_env_int(
            os.getenv("CQ_CACHE_MAX_TREE_SITTER_LANES"),
            default=int(getattr(runtime, "max_tree_sitter_lanes", 4)),
            minimum=1,
        ),
        lane_lock_ttl_seconds=_env_int(
            os.getenv("CQ_CACHE_LANE_LOCK_TTL_SECONDS"),
            default=int(getattr(runtime, "lane_lock_ttl_seconds", 15)),
            minimum=1,
        ),
        sqlite_mmap_size=_env_int(
            os.getenv("CQ_CACHE_SQLITE_MMAP_SIZE"),
            default=int(getattr(runtime, "sqlite_mmap_size", 0)),
            minimum=0,
        ),
        sqlite_cache_size=_env_int(
            os.getenv("CQ_CACHE_SQLITE_CACHE_SIZE"),
            default=int(getattr(runtime, "sqlite_cache_size", 0)),
            minimum=0,
        ),
        transaction_batch_size=_env_int(
            os.getenv("CQ_CACHE_TRANSACTION_BATCH_SIZE"),
            default=int(getattr(runtime, "transaction_batch_size", 128)),
            minimum=1,
        ),
    )


def default_cache_policy(*, root: Path) -> CqCachePolicyV1:
    """Build cache policy from runtime defaults and optional env overrides.

    Returns:
        Cache policy resolved from runtime defaults and environment.
    """
    runtime = default_runtime_execution_policy().cache
    enabled = _env_bool(os.getenv("CQ_CACHE_ENABLED"), default=runtime.enabled)
    settings = _resolve_cache_scalar_settings(root=root, runtime=runtime)

    return CqCachePolicyV1(
        enabled=enabled,
        directory=settings.directory,
        shards=settings.shards,
        timeout_seconds=settings.timeout_seconds,
        ttl_seconds=settings.ttl_seconds,
        evict_run_tag_on_exit=_env_bool(
            os.getenv("CQ_CACHE_EVICT_RUN_TAG_ON_EXIT"),
            default=runtime.evict_run_tag_on_exit,
        ),
        namespace_ttl_seconds=_resolve_namespace_ttl_from_env(
            defaults=runtime.namespace_ttl_seconds
        ),
        namespace_enabled=_resolve_namespace_enabled_from_env(defaults=runtime.namespace_enabled),
        namespace_ephemeral=_resolve_namespace_ephemeral_from_env(
            defaults=runtime.namespace_ephemeral,
        ),
        size_limit_bytes=settings.size_limit_bytes,
        cull_limit=settings.cull_limit,
        eviction_policy=settings.eviction_policy,
        statistics_enabled=settings.statistics_enabled,
        max_tree_sitter_lanes=settings.max_tree_sitter_lanes,
        lane_lock_ttl_seconds=settings.lane_lock_ttl_seconds,
        sqlite_mmap_size=settings.sqlite_mmap_size,
        sqlite_cache_size=settings.sqlite_cache_size,
        transaction_batch_size=settings.transaction_batch_size,
    )


__all__ = ["CqCachePolicyV1", "default_cache_policy"]
