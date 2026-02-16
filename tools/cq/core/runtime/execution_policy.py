"""Runtime execution policy contracts for CQ hot paths."""

from __future__ import annotations

import os

import msgspec

from tools.cq.core.cache.defaults import (
    DEFAULT_CACHE_CULL_LIMIT,
    DEFAULT_CACHE_EVICTION_POLICY,
    DEFAULT_CACHE_SHARDS,
    DEFAULT_CACHE_SIZE_LIMIT_BYTES,
    DEFAULT_CACHE_TIMEOUT_SECONDS,
    DEFAULT_CACHE_TTL_SECONDS,
)
from tools.cq.core.contracts_constraints import NonNegativeInt, PositiveFloat, PositiveInt
from tools.cq.core.runtime.env_namespace import (
    NamespacePatternV1,
    env_bool,
    env_int,
    parse_namespace_bool_overrides,
    parse_namespace_int_overrides,
)
from tools.cq.core.structs import CqSettingsStruct

_DEFAULT_SEMANTIC_TIMEOUT_MS = 2_000
_DEFAULT_IO_WORKERS = 8
_DEFAULT_SEMANTIC_WORKERS = 4
_DEFAULT_QUERY_PARTITION_WORKERS = 2
_DEFAULT_CALLS_FILE_WORKERS = 4
_DEFAULT_RUN_STEP_WORKERS = 4
_ENV_PREFIX = "CQ_RUNTIME_"


class ParallelismPolicy(CqSettingsStruct, frozen=True):
    """Worker policy for CPU and I/O execution."""

    cpu_workers: PositiveInt
    io_workers: PositiveInt
    semantic_request_workers: PositiveInt
    query_partition_workers: PositiveInt = _DEFAULT_QUERY_PARTITION_WORKERS
    calls_file_workers: PositiveInt = _DEFAULT_CALLS_FILE_WORKERS
    run_step_workers: PositiveInt = _DEFAULT_RUN_STEP_WORKERS
    enable_process_pool: bool = True


class SemanticRuntimePolicy(CqSettingsStruct, frozen=True):
    """Static semantic timing and budgeting policy."""

    timeout_ms: PositiveInt = _DEFAULT_SEMANTIC_TIMEOUT_MS
    startup_timeout_ms: PositiveInt = _DEFAULT_SEMANTIC_TIMEOUT_MS
    max_targets_search: PositiveInt = 1
    max_targets_calls: PositiveInt = 1
    max_targets_entity: PositiveInt = 3


class CacheRuntimePolicy(CqSettingsStruct, frozen=True):
    """Cache policy for CQ runtime adapters."""

    enabled: bool = True
    ttl_seconds: PositiveInt = DEFAULT_CACHE_TTL_SECONDS
    shards: PositiveInt = DEFAULT_CACHE_SHARDS
    timeout_seconds: PositiveFloat = DEFAULT_CACHE_TIMEOUT_SECONDS
    evict_run_tag_on_exit: bool = False
    namespace_ttl_seconds: dict[str, int] = msgspec.field(default_factory=dict)
    namespace_enabled: dict[str, bool] = msgspec.field(default_factory=dict)
    namespace_ephemeral: dict[str, bool] = msgspec.field(default_factory=dict)
    size_limit_bytes: PositiveInt = DEFAULT_CACHE_SIZE_LIMIT_BYTES
    cull_limit: NonNegativeInt = DEFAULT_CACHE_CULL_LIMIT
    eviction_policy: str = DEFAULT_CACHE_EVICTION_POLICY
    statistics_enabled: bool = False


class RuntimeExecutionPolicy(CqSettingsStruct, frozen=True):
    """Top-level runtime policy envelope."""

    parallelism: ParallelismPolicy
    semantic: SemanticRuntimePolicy = SemanticRuntimePolicy()
    cache: CacheRuntimePolicy = CacheRuntimePolicy()


def _runtime_env(name: str) -> str | None:
    return os.getenv(f"{_ENV_PREFIX}{name}")


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = _runtime_env(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _env_namespace_ttls() -> dict[str, int]:
    parsed = parse_namespace_int_overrides(
        env=os.environ,
        patterns=(
            NamespacePatternV1(
                prefix=f"{_ENV_PREFIX}CACHE_NAMESPACE_",
                suffix="_TTL_SECONDS",
            ),
            NamespacePatternV1(
                prefix=f"{_ENV_PREFIX}CACHE_TTL_",
                suffix="_SECONDS",
            ),
        ),
        minimum=1,
    )
    return {str(key): int(value) for key, value in parsed.values.items() if isinstance(value, int)}


def _env_namespace_enabled() -> dict[str, bool]:
    parsed = parse_namespace_bool_overrides(
        env=os.environ,
        patterns=(
            NamespacePatternV1(
                prefix=f"{_ENV_PREFIX}CACHE_NAMESPACE_",
                suffix="_ENABLED",
            ),
            NamespacePatternV1(prefix=f"{_ENV_PREFIX}CACHE_ENABLE_"),
        ),
    )
    return {
        str(key): bool(value) for key, value in parsed.values.items() if isinstance(value, bool)
    }


def _env_namespace_ephemeral() -> dict[str, bool]:
    parsed = parse_namespace_bool_overrides(
        env=os.environ,
        patterns=(NamespacePatternV1(prefix=f"{_ENV_PREFIX}CACHE_EPHEMERAL_"),),
    )
    return {
        str(key): bool(value) for key, value in parsed.values.items() if isinstance(value, bool)
    }


def default_runtime_execution_policy() -> RuntimeExecutionPolicy:
    """Build runtime policy from host defaults and optional env overrides.

    Returns:
        Runtime policy populated from defaults and environment values.
    """
    cpu_count = max(1, os.cpu_count() or 1)
    cpu_workers = env_int(_runtime_env("CPU_WORKERS"), default=max(1, cpu_count - 1), minimum=1)
    io_workers = env_int(
        _runtime_env("IO_WORKERS"),
        default=max(_DEFAULT_IO_WORKERS, cpu_count),
        minimum=1,
    )
    semantic_workers = env_int(
        _runtime_env("SEMANTIC_REQUEST_WORKERS"),
        default=_DEFAULT_SEMANTIC_WORKERS,
        minimum=1,
    )
    enable_process_pool = env_bool(_runtime_env("ENABLE_PROCESS_POOL"), default=True)
    semantic_timeout_ms = env_int(
        _runtime_env("SEMANTIC_TIMEOUT_MS"),
        default=_DEFAULT_SEMANTIC_TIMEOUT_MS,
        minimum=100,
    )
    semantic_startup_ms = env_int(
        _runtime_env("SEMANTIC_STARTUP_TIMEOUT_MS"),
        default=_DEFAULT_SEMANTIC_TIMEOUT_MS,
        minimum=100,
    )

    return RuntimeExecutionPolicy(
        parallelism=ParallelismPolicy(
            cpu_workers=cpu_workers,
            io_workers=io_workers,
            semantic_request_workers=semantic_workers,
            query_partition_workers=env_int(
                _runtime_env("QUERY_PARTITION_WORKERS"),
                default=_DEFAULT_QUERY_PARTITION_WORKERS,
                minimum=1,
            ),
            calls_file_workers=env_int(
                _runtime_env("CALLS_FILE_WORKERS"),
                default=_DEFAULT_CALLS_FILE_WORKERS,
                minimum=1,
            ),
            run_step_workers=env_int(
                _runtime_env("RUN_STEP_WORKERS"),
                default=_DEFAULT_RUN_STEP_WORKERS,
                minimum=1,
            ),
            enable_process_pool=enable_process_pool,
        ),
        semantic=SemanticRuntimePolicy(
            timeout_ms=semantic_timeout_ms,
            startup_timeout_ms=semantic_startup_ms,
            max_targets_search=env_int(
                _runtime_env("SEMANTIC_TARGETS_SEARCH"), default=1, minimum=1
            ),
            max_targets_calls=env_int(_runtime_env("SEMANTIC_TARGETS_CALLS"), default=1, minimum=1),
            max_targets_entity=env_int(
                _runtime_env("SEMANTIC_TARGETS_ENTITY"), default=3, minimum=1
            ),
        ),
        cache=CacheRuntimePolicy(
            enabled=env_bool(_runtime_env("CACHE_ENABLED"), default=True),
            ttl_seconds=env_int(
                _runtime_env("CACHE_TTL_SECONDS"),
                default=DEFAULT_CACHE_TTL_SECONDS,
                minimum=1,
            ),
            shards=env_int(_runtime_env("CACHE_SHARDS"), default=8, minimum=1),
            timeout_seconds=_env_float("CACHE_TIMEOUT_SECONDS", 0.05),
            evict_run_tag_on_exit=env_bool(
                _runtime_env("CACHE_EVICT_RUN_TAG_ON_EXIT"),
                default=False,
            ),
            namespace_ttl_seconds=_env_namespace_ttls(),
            namespace_enabled=_env_namespace_enabled(),
            namespace_ephemeral=_env_namespace_ephemeral(),
            size_limit_bytes=env_int(
                _runtime_env("CACHE_SIZE_LIMIT_BYTES"),
                default=DEFAULT_CACHE_SIZE_LIMIT_BYTES,
                minimum=1,
            ),
            cull_limit=env_int(
                _runtime_env("CACHE_CULL_LIMIT"),
                default=DEFAULT_CACHE_CULL_LIMIT,
                minimum=0,
            ),
            eviction_policy=(
                _runtime_env("CACHE_EVICTION_POLICY") or DEFAULT_CACHE_EVICTION_POLICY
            ),
            statistics_enabled=(
                env_bool(_runtime_env("CACHE_STATISTICS_ENABLED"), default=False)
                or env_bool(_runtime_env("CACHE_STATS_ENABLED"), default=False)
            ),
        ),
    )


__all__ = [
    "CacheRuntimePolicy",
    "ParallelismPolicy",
    "RuntimeExecutionPolicy",
    "SemanticRuntimePolicy",
    "default_runtime_execution_policy",
]
