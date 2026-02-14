"""Runtime execution policy contracts for CQ hot paths."""

from __future__ import annotations

import os
from typing import Annotated

import msgspec

from tools.cq.core.structs import CqSettingsStruct

_DEFAULT_LSP_TIMEOUT_MS = 2_000
_DEFAULT_CACHE_TTL_SECONDS = 900
_DEFAULT_IO_WORKERS = 8
_DEFAULT_LSP_WORKERS = 4
_DEFAULT_QUERY_PARTITION_WORKERS = 2
_DEFAULT_CALLS_FILE_WORKERS = 4
_DEFAULT_RUN_STEP_WORKERS = 4
_DEFAULT_CACHE_SIZE_LIMIT_BYTES = 2_147_483_648
_DEFAULT_CACHE_CULL_LIMIT = 16
_DEFAULT_CACHE_EVICTION_POLICY = "least-recently-stored"
_ENV_PREFIX = "CQ_RUNTIME_"


PositiveInt = Annotated[int, msgspec.Meta(ge=1)]
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]
PositiveFloat = Annotated[float, msgspec.Meta(gt=0.0)]


class ParallelismPolicy(CqSettingsStruct, frozen=True):
    """Worker policy for CPU and I/O execution."""

    cpu_workers: PositiveInt
    io_workers: PositiveInt
    lsp_request_workers: PositiveInt
    query_partition_workers: PositiveInt = _DEFAULT_QUERY_PARTITION_WORKERS
    calls_file_workers: PositiveInt = _DEFAULT_CALLS_FILE_WORKERS
    run_step_workers: PositiveInt = _DEFAULT_RUN_STEP_WORKERS
    enable_process_pool: bool = True


class LspRuntimePolicy(CqSettingsStruct, frozen=True):
    """LSP timing and budgeting policy."""

    timeout_ms: PositiveInt = _DEFAULT_LSP_TIMEOUT_MS
    startup_timeout_ms: PositiveInt = _DEFAULT_LSP_TIMEOUT_MS
    max_targets_search: PositiveInt = 1
    max_targets_calls: PositiveInt = 1
    max_targets_entity: PositiveInt = 3


class CacheRuntimePolicy(CqSettingsStruct, frozen=True):
    """Cache policy for CQ runtime adapters."""

    enabled: bool = True
    ttl_seconds: PositiveInt = _DEFAULT_CACHE_TTL_SECONDS
    shards: PositiveInt = 8
    timeout_seconds: PositiveFloat = 0.05
    evict_run_tag_on_exit: bool = False
    namespace_ttl_seconds: dict[str, int] = msgspec.field(default_factory=dict)
    namespace_enabled: dict[str, bool] = msgspec.field(default_factory=dict)
    namespace_ephemeral: dict[str, bool] = msgspec.field(default_factory=dict)
    size_limit_bytes: PositiveInt = _DEFAULT_CACHE_SIZE_LIMIT_BYTES
    cull_limit: NonNegativeInt = _DEFAULT_CACHE_CULL_LIMIT
    eviction_policy: str = _DEFAULT_CACHE_EVICTION_POLICY
    statistics_enabled: bool = False


class RuntimeExecutionPolicy(CqSettingsStruct, frozen=True):
    """Top-level runtime policy envelope."""

    parallelism: ParallelismPolicy
    lsp: LspRuntimePolicy = LspRuntimePolicy()
    cache: CacheRuntimePolicy = CacheRuntimePolicy()


_DEF_BOOL_TRUE = {"1", "true", "yes", "on"}
_DEF_BOOL_FALSE = {"0", "false", "no", "off"}


def _parse_bool(raw: str | None) -> bool | None:
    if raw is None:
        return None
    value = raw.strip().lower()
    if value in _DEF_BOOL_TRUE:
        return True
    if value in _DEF_BOOL_FALSE:
        return False
    return None


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(f"{_ENV_PREFIX}{name}")
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.getenv(f"{_ENV_PREFIX}{name}")
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _env_bool(name: str, *, default: bool) -> bool:
    parsed = _parse_bool(os.getenv(f"{_ENV_PREFIX}{name}"))
    if parsed is None:
        return default
    return parsed


def _namespace_from_env_suffix(suffix: str) -> str:
    return suffix.strip("_").lower()


def _env_namespace_ttls() -> dict[str, int]:
    overrides: dict[str, int] = {}
    prefix = f"{_ENV_PREFIX}CACHE_NAMESPACE_"
    suffix = "_TTL_SECONDS"
    for key, value in os.environ.items():
        if not key.startswith(prefix) or not key.endswith(suffix):
            continue
        namespace = _namespace_from_env_suffix(key[len(prefix) : -len(suffix)])
        if not namespace:
            continue
        try:
            ttl = int(value)
        except ValueError:
            continue
        if ttl > 0:
            overrides[namespace] = ttl
    legacy_prefix = f"{_ENV_PREFIX}CACHE_TTL_"
    legacy_suffix = "_SECONDS"
    for key, value in os.environ.items():
        if not key.startswith(legacy_prefix) or not key.endswith(legacy_suffix):
            continue
        namespace = _namespace_from_env_suffix(key[len(legacy_prefix) : -len(legacy_suffix)])
        if not namespace:
            continue
        try:
            ttl = int(value)
        except ValueError:
            continue
        if ttl > 0:
            overrides[namespace] = ttl
    return overrides


def _env_namespace_enabled() -> dict[str, bool]:
    overrides: dict[str, bool] = {}
    prefix = f"{_ENV_PREFIX}CACHE_NAMESPACE_"
    suffix = "_ENABLED"
    for key, value in os.environ.items():
        if not key.startswith(prefix) or not key.endswith(suffix):
            continue
        namespace = _namespace_from_env_suffix(key[len(prefix) : -len(suffix)])
        if not namespace:
            continue
        parsed = _parse_bool(value)
        if parsed is not None:
            overrides[namespace] = parsed
    legacy_prefix = f"{_ENV_PREFIX}CACHE_ENABLE_"
    for key, value in os.environ.items():
        if not key.startswith(legacy_prefix):
            continue
        namespace = _namespace_from_env_suffix(key[len(legacy_prefix) :])
        if not namespace:
            continue
        parsed = _parse_bool(value)
        if parsed is not None:
            overrides[namespace] = parsed
    return overrides


def _env_namespace_ephemeral() -> dict[str, bool]:
    overrides: dict[str, bool] = {}
    prefix = f"{_ENV_PREFIX}CACHE_EPHEMERAL_"
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        namespace = _namespace_from_env_suffix(key[len(prefix) :])
        if not namespace:
            continue
        parsed = _parse_bool(value)
        if parsed is not None:
            overrides[namespace] = parsed
    return overrides


def default_runtime_execution_policy() -> RuntimeExecutionPolicy:
    """Build runtime policy from host defaults and optional env overrides.

    Returns:
        Runtime policy populated from defaults and environment values.
    """
    cpu_count = max(1, os.cpu_count() or 1)
    cpu_workers = _env_int("CPU_WORKERS", max(1, cpu_count - 1))
    io_workers = _env_int("IO_WORKERS", max(_DEFAULT_IO_WORKERS, cpu_count))
    lsp_workers = _env_int("LSP_REQUEST_WORKERS", _DEFAULT_LSP_WORKERS)
    enable_process_pool = _env_bool("ENABLE_PROCESS_POOL", default=True)
    lsp_timeout_ms = _env_int("LSP_TIMEOUT_MS", _DEFAULT_LSP_TIMEOUT_MS, minimum=100)
    lsp_startup_ms = _env_int("LSP_STARTUP_TIMEOUT_MS", _DEFAULT_LSP_TIMEOUT_MS, minimum=100)

    return RuntimeExecutionPolicy(
        parallelism=ParallelismPolicy(
            cpu_workers=cpu_workers,
            io_workers=io_workers,
            lsp_request_workers=lsp_workers,
            query_partition_workers=_env_int(
                "QUERY_PARTITION_WORKERS",
                _DEFAULT_QUERY_PARTITION_WORKERS,
            ),
            calls_file_workers=_env_int(
                "CALLS_FILE_WORKERS",
                _DEFAULT_CALLS_FILE_WORKERS,
            ),
            run_step_workers=_env_int(
                "RUN_STEP_WORKERS",
                _DEFAULT_RUN_STEP_WORKERS,
            ),
            enable_process_pool=enable_process_pool,
        ),
        lsp=LspRuntimePolicy(
            timeout_ms=lsp_timeout_ms,
            startup_timeout_ms=lsp_startup_ms,
            max_targets_search=_env_int("LSP_TARGETS_SEARCH", 1),
            max_targets_calls=_env_int("LSP_TARGETS_CALLS", 1),
            max_targets_entity=_env_int("LSP_TARGETS_ENTITY", 3),
        ),
        cache=CacheRuntimePolicy(
            enabled=_env_bool("CACHE_ENABLED", default=True),
            ttl_seconds=_env_int("CACHE_TTL_SECONDS", _DEFAULT_CACHE_TTL_SECONDS),
            shards=_env_int("CACHE_SHARDS", 8),
            timeout_seconds=_env_float("CACHE_TIMEOUT_SECONDS", 0.05),
            evict_run_tag_on_exit=_env_bool("CACHE_EVICT_RUN_TAG_ON_EXIT", default=False),
            namespace_ttl_seconds=_env_namespace_ttls(),
            namespace_enabled=_env_namespace_enabled(),
            namespace_ephemeral=_env_namespace_ephemeral(),
            size_limit_bytes=_env_int(
                "CACHE_SIZE_LIMIT_BYTES",
                _DEFAULT_CACHE_SIZE_LIMIT_BYTES,
            ),
            cull_limit=_env_int("CACHE_CULL_LIMIT", _DEFAULT_CACHE_CULL_LIMIT, minimum=0),
            eviction_policy=(
                os.getenv(f"{_ENV_PREFIX}CACHE_EVICTION_POLICY") or _DEFAULT_CACHE_EVICTION_POLICY
            ),
            statistics_enabled=(
                _env_bool("CACHE_STATISTICS_ENABLED", default=False)
                or _env_bool("CACHE_STATS_ENABLED", default=False)
            ),
        ),
    )


__all__ = [
    "CacheRuntimePolicy",
    "LspRuntimePolicy",
    "ParallelismPolicy",
    "RuntimeExecutionPolicy",
    "default_runtime_execution_policy",
]
