"""CQ runtime cache interfaces and disk-backed backend."""

from tools.cq.core.cache.base_contracts import (
    CacheMaintenanceSnapshotV1,
    LaneCoordinationPolicyV1,
    TreeSitterCacheEnvelopeV1,
)
from tools.cq.core.cache.content_hash import (
    FileContentHashV1,
    file_content_hash,
    reset_file_content_hash_cache,
)
from tools.cq.core.cache.coordination import publish_once_per_barrier, tree_sitter_lane_guard
from tools.cq.core.cache.diagnostics import snapshot_backend_metrics
from tools.cq.core.cache.diskcache_backend import (
    DiskcacheBackend,
    close_cq_cache_backend,
    get_cq_cache_backend,
)
from tools.cq.core.cache.fragment_codecs import (
    decode_fragment_payload,
    encode_fragment_payload,
)
from tools.cq.core.cache.fragment_contracts import (
    FragmentEntryV1,
    FragmentHitV1,
    FragmentMissV1,
    FragmentPartitionV1,
    FragmentRequestV1,
    FragmentWriteV1,
)
from tools.cq.core.cache.fragment_engine import (
    FragmentPersistRuntimeV1,
    FragmentProbeRuntimeV1,
    partition_fragment_entries,
    persist_fragment_writes,
)
from tools.cq.core.cache.interface import CqCacheBackend, NoopCacheBackend
from tools.cq.core.cache.key_builder import (
    build_cache_key,
    build_cache_tag,
    build_namespace_cache_tag,
    build_run_cache_tag,
    build_scope_hash,
    build_search_artifact_cache_key,
    build_search_artifact_index_key,
    canonicalize_cache_payload,
)
from tools.cq.core.cache.maintenance import maintenance_tick
from tools.cq.core.cache.namespaces import (
    cache_namespace_env_suffix,
    is_namespace_cache_enabled,
    is_namespace_ephemeral,
    namespace_defaults,
    resolve_namespace_ttl_seconds,
)
from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.cache.run_lifecycle import (
    CacheWriteTagRequestV1,
    maybe_evict_run_cache_tag,
    resolve_write_cache_tag,
)
from tools.cq.core.cache.scope_services import ScopePlanV1, ScopeResolutionV1, resolve_scope
from tools.cq.core.cache.search_artifact_store import (
    list_search_artifact_entries,
    load_search_artifact_bundle,
    persist_search_artifact_bundle,
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
from tools.cq.core.cache.tree_sitter_cache_store import (
    build_tree_sitter_cache_key,
    load_tree_sitter_payload,
    persist_tree_sitter_payload,
)

__all__ = [
    "CacheMaintenanceSnapshotV1",
    "CacheNamespaceTelemetry",
    "CacheWriteTagRequestV1",
    "CqCacheBackend",
    "CqCachePolicyV1",
    "DiskcacheBackend",
    "FileContentHashV1",
    "FragmentEntryV1",
    "FragmentHitV1",
    "FragmentMissV1",
    "FragmentPartitionV1",
    "FragmentPersistRuntimeV1",
    "FragmentProbeRuntimeV1",
    "FragmentRequestV1",
    "FragmentWriteV1",
    "LaneCoordinationPolicyV1",
    "NoopCacheBackend",
    "ScopeFileStatV1",
    "ScopePlanV1",
    "ScopeResolutionV1",
    "ScopeSnapshotFingerprintV1",
    "TreeSitterCacheEnvelopeV1",
    "build_cache_key",
    "build_cache_tag",
    "build_namespace_cache_tag",
    "build_run_cache_tag",
    "build_scope_hash",
    "build_scope_snapshot_fingerprint",
    "build_search_artifact_cache_key",
    "build_search_artifact_index_key",
    "build_tree_sitter_cache_key",
    "cache_namespace_env_suffix",
    "canonicalize_cache_payload",
    "close_cq_cache_backend",
    "decode_fragment_payload",
    "default_cache_policy",
    "encode_fragment_payload",
    "file_content_hash",
    "get_cq_cache_backend",
    "is_namespace_cache_enabled",
    "is_namespace_ephemeral",
    "list_search_artifact_entries",
    "load_search_artifact_bundle",
    "load_tree_sitter_payload",
    "maintenance_tick",
    "maybe_evict_run_cache_tag",
    "namespace_defaults",
    "partition_fragment_entries",
    "persist_fragment_writes",
    "persist_search_artifact_bundle",
    "persist_tree_sitter_payload",
    "publish_once_per_barrier",
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
    "resolve_scope",
    "resolve_write_cache_tag",
    "snapshot_backend_metrics",
    "snapshot_cache_telemetry",
    "tree_sitter_lane_guard",
]
