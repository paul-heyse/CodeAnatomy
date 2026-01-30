"""Infrastructure utilities for extraction pipelines.

This subpackage provides:
- DiskCache integration (cache_utils)
- Parallel execution helpers (parallel)
- Schema fingerprinting (schema_cache)
- Worklist management (worklists)
- Shared option mixins (options)
- Result containers (result_types)
- String normalization (string_utils)
"""

from __future__ import annotations

from extract.infrastructure.cache_utils import (
    CACHE_VERSION,
    LOCK_EXPIRE_SECONDS,
    CacheSetOptions,
    cache_for_extract,
    cache_for_kind_optional,
    cache_get,
    cache_lock,
    cache_set,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    evict_extract_cache,
    evict_repo_scan_cache,
    stable_cache_key,
    stable_cache_label,
)
from extract.infrastructure.options import (
    BatchOptions,
    ParallelOptions,
    RepoOptions,
    WorkerOptions,
    WorklistQueueOptions,
)
from extract.infrastructure.parallel import (
    gil_disabled,
    parallel_map,
    resolve_max_workers,
    supports_fork,
)
from extract.infrastructure.result_types import ExtractResult
from extract.infrastructure.schema_cache import (
    ast_files_fingerprint,
    bytecode_files_fingerprint,
    cached_schema_identity_hash,
    libcst_files_fingerprint,
    repo_file_blobs_fingerprint,
    repo_files_fingerprint,
    symtable_files_fingerprint,
    tree_sitter_files_fingerprint,
)
from extract.infrastructure.string_utils import normalize_string_items
from extract.infrastructure.worklists import (
    WorklistRequest,
    iter_worklist_contexts,
    worklist_builder,
    worklist_queue_name,
)

__all__ = [
    # cache_utils
    "CACHE_VERSION",
    "LOCK_EXPIRE_SECONDS",
    # options
    "BatchOptions",
    "CacheSetOptions",
    # result_types
    "ExtractResult",
    "ParallelOptions",
    "RepoOptions",
    "WorkerOptions",
    "WorklistQueueOptions",
    # worklists
    "WorklistRequest",
    # schema_cache
    "ast_files_fingerprint",
    "bytecode_files_fingerprint",
    "cache_for_extract",
    "cache_for_kind_optional",
    "cache_get",
    "cache_lock",
    "cache_set",
    "cache_ttl_seconds",
    "cached_schema_identity_hash",
    "diskcache_profile_from_ctx",
    "evict_extract_cache",
    "evict_repo_scan_cache",
    # parallel
    "gil_disabled",
    "iter_worklist_contexts",
    "libcst_files_fingerprint",
    # string_utils
    "normalize_string_items",
    "parallel_map",
    "repo_file_blobs_fingerprint",
    "repo_files_fingerprint",
    "resolve_max_workers",
    "stable_cache_key",
    "stable_cache_label",
    "supports_fork",
    "symtable_files_fingerprint",
    "tree_sitter_files_fingerprint",
    "worklist_builder",
    "worklist_queue_name",
]
