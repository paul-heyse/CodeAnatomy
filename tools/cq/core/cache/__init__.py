"""CQ runtime cache interfaces and disk-backed backend."""

from tools.cq.core.cache.content_hash import (
    FileContentHashV1,
    file_content_hash,
    reset_file_content_hash_cache,
)
from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.cache.diskcache_backend import (
    DiskcacheBackend,
    close_cq_cache_backend,
    get_cq_cache_backend,
)
from tools.cq.core.cache.interface import CqCacheBackend, NoopCacheBackend
from tools.cq.core.cache.key_builder import (
    build_cache_key,
    build_cache_tag,
    build_namespace_cache_tag,
    build_run_cache_tag,
    build_scope_hash,
    canonicalize_cache_payload,
)
from tools.cq.core.cache.namespaces import (
    cache_namespace_env_suffix,
    is_namespace_cache_enabled,
    is_namespace_ephemeral,
    namespace_defaults,
    resolve_namespace_ttl_seconds,
)
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.cache.run_lifecycle import (
    maybe_evict_run_cache_tag,
    resolve_write_cache_tag,
)
from tools.cq.core.cache.snapshot_fingerprint import (
    ScopeFileStatV1,
    ScopeSnapshotFingerprintV1,
    build_scope_snapshot_fingerprint,
)
from tools.cq.core.cache.telemetry import (
    CacheNamespaceTelemetry,
    record_cache_abort,
    record_cache_cull,
    record_cache_decode_failure,
    record_cache_delete,
    record_cache_evict,
    record_cache_get,
    record_cache_key,
    record_cache_set,
    record_cache_timeout,
    record_cache_volume,
    reset_cache_telemetry,
    snapshot_cache_telemetry,
)

__all__ = [
    "CacheNamespaceTelemetry",
    "CqCacheBackend",
    "CqCachePolicyV1",
    "DiskcacheBackend",
    "FileContentHashV1",
    "NoopCacheBackend",
    "ScopeFileStatV1",
    "ScopeSnapshotFingerprintV1",
    "build_cache_key",
    "build_cache_tag",
    "build_namespace_cache_tag",
    "build_run_cache_tag",
    "build_scope_hash",
    "build_scope_snapshot_fingerprint",
    "cache_namespace_env_suffix",
    "canonicalize_cache_payload",
    "close_cq_cache_backend",
    "default_cache_policy",
    "file_content_hash",
    "get_cq_cache_backend",
    "is_namespace_cache_enabled",
    "is_namespace_ephemeral",
    "maybe_evict_run_cache_tag",
    "namespace_defaults",
    "record_cache_abort",
    "record_cache_cull",
    "record_cache_decode_failure",
    "record_cache_delete",
    "record_cache_evict",
    "record_cache_get",
    "record_cache_key",
    "record_cache_set",
    "record_cache_timeout",
    "record_cache_volume",
    "reset_cache_telemetry",
    "reset_file_content_hash_cache",
    "resolve_namespace_ttl_seconds",
    "resolve_write_cache_tag",
    "snapshot_backend_metrics",
    "snapshot_cache_telemetry",
]
