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


def default_cache_policy(*, root: Path) -> CqCachePolicyV1:
    """Build cache policy from runtime defaults and optional env overrides.

    Returns:
        Cache policy resolved from runtime defaults and environment.
    """
    runtime = default_runtime_execution_policy().cache
    enabled = _env_bool(os.getenv("CQ_CACHE_ENABLED"), default=runtime.enabled)

    raw_dir = os.getenv("CQ_CACHE_DIR")
    directory = raw_dir.strip() if raw_dir else str(root / _DEFAULT_DIR)

    ttl_seconds = _env_int(
        os.getenv("CQ_CACHE_TTL_SECONDS"),
        default=runtime.ttl_seconds,
        minimum=1,
    )
    shards = _env_int(os.getenv("CQ_CACHE_SHARDS"), default=runtime.shards, minimum=1)
    timeout_seconds = max(
        0.001,
        float(os.getenv("CQ_CACHE_TIMEOUT_SECONDS") or runtime.timeout_seconds),
    )

    size_limit_bytes = _env_int(
        os.getenv("CQ_CACHE_SIZE_LIMIT_BYTES"),
        default=runtime.size_limit_bytes,
        minimum=1,
    )
    cull_limit = _env_int(
        os.getenv("CQ_CACHE_CULL_LIMIT"),
        default=runtime.cull_limit,
        minimum=0,
    )
    eviction_policy = (
        os.getenv("CQ_CACHE_EVICTION_POLICY") or runtime.eviction_policy
    ).strip() or runtime.eviction_policy
    statistics_enabled = _env_bool(
        os.getenv("CQ_CACHE_STATISTICS_ENABLED"),
        default=runtime.statistics_enabled,
    )
    if not statistics_enabled:
        statistics_enabled = _env_bool(
            os.getenv("CQ_CACHE_STATS_ENABLED"),
            default=runtime.statistics_enabled,
        )

    evict_run_tag_on_exit = _env_bool(
        os.getenv("CQ_CACHE_EVICT_RUN_TAG_ON_EXIT"),
        default=runtime.evict_run_tag_on_exit,
    )

    namespace_ttl_seconds = _resolve_namespace_ttl_from_env(defaults=runtime.namespace_ttl_seconds)
    namespace_enabled = _resolve_namespace_enabled_from_env(defaults=runtime.namespace_enabled)
    namespace_ephemeral = _resolve_namespace_ephemeral_from_env(
        defaults=runtime.namespace_ephemeral,
    )

    return CqCachePolicyV1(
        enabled=enabled,
        directory=directory,
        shards=shards,
        timeout_seconds=timeout_seconds,
        ttl_seconds=ttl_seconds,
        evict_run_tag_on_exit=evict_run_tag_on_exit,
        namespace_ttl_seconds=namespace_ttl_seconds,
        namespace_enabled=namespace_enabled,
        namespace_ephemeral=namespace_ephemeral,
        size_limit_bytes=size_limit_bytes,
        cull_limit=cull_limit,
        eviction_policy=eviction_policy,
        statistics_enabled=statistics_enabled,
    )


__all__ = ["CqCachePolicyV1", "default_cache_policy"]
