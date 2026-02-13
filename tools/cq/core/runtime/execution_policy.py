"""Runtime execution policy contracts for CQ hot paths."""

from __future__ import annotations

import os

from tools.cq.core.structs import CqStruct

_DEFAULT_LSP_TIMEOUT_MS = 2_000
_DEFAULT_CACHE_TTL_SECONDS = 900
_DEFAULT_IO_WORKERS = 8
_DEFAULT_LSP_WORKERS = 4
_DEFAULT_QUERY_PARTITION_WORKERS = 2
_DEFAULT_CALLS_FILE_WORKERS = 4
_DEFAULT_RUN_STEP_WORKERS = 4
_ENV_PREFIX = "CQ_RUNTIME_"


class ParallelismPolicy(CqStruct, frozen=True):
    """Worker policy for CPU and I/O execution."""

    cpu_workers: int
    io_workers: int
    lsp_request_workers: int
    query_partition_workers: int = _DEFAULT_QUERY_PARTITION_WORKERS
    calls_file_workers: int = _DEFAULT_CALLS_FILE_WORKERS
    run_step_workers: int = _DEFAULT_RUN_STEP_WORKERS
    enable_process_pool: bool = True


class LspRuntimePolicy(CqStruct, frozen=True):
    """LSP timing and budgeting policy."""

    timeout_ms: int = _DEFAULT_LSP_TIMEOUT_MS
    startup_timeout_ms: int = _DEFAULT_LSP_TIMEOUT_MS
    max_targets_search: int = 1
    max_targets_calls: int = 1
    max_targets_entity: int = 3


class CacheRuntimePolicy(CqStruct, frozen=True):
    """Cache policy for CQ runtime adapters."""

    enabled: bool = True
    ttl_seconds: int = _DEFAULT_CACHE_TTL_SECONDS
    shards: int = 8
    timeout_seconds: float = 0.05


class RuntimeExecutionPolicy(CqStruct, frozen=True):
    """Top-level runtime policy envelope."""

    parallelism: ParallelismPolicy
    lsp: LspRuntimePolicy = LspRuntimePolicy()
    cache: CacheRuntimePolicy = CacheRuntimePolicy()


_DEF_BOOL_TRUE = {"1", "true", "yes", "on"}
_DEF_BOOL_FALSE = {"0", "false", "no", "off"}


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
    raw = os.getenv(f"{_ENV_PREFIX}{name}")
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _DEF_BOOL_TRUE:
        return True
    if value in _DEF_BOOL_FALSE:
        return False
    return default


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
        ),
    )


__all__ = [
    "CacheRuntimePolicy",
    "LspRuntimePolicy",
    "ParallelismPolicy",
    "RuntimeExecutionPolicy",
    "default_runtime_execution_policy",
]
