# DiskCache Integration & Cache Streamlining Plan

## Goal
Adopt DiskCache as the shared cache substrate for cross-run, cross-process reuse and persistence. Replace high-impact in-memory caches with DiskCache equivalents, add missing cache layers where repeated work is observed, and centralize cache configuration/diagnostics so behavior is consistent across extract, engine, and runtime layers.

## Guiding Principles
- **Durable by default:** Prefer disk-backed caches for expensive, deterministic work that benefits from reuse across runs.
- **Single cache surface:** Centralize DiskCache configuration and lifecycle (directory, TTL, size, policy).
- **Multi-process safe:** Use `FanoutCache` for concurrent writers.
- **Explicit invalidation:** Tag keys and evict by tag on schema/profile/policy changes.
- **Local-disk only:** Cache directories are per-host and must remain off NFS/shared filesystems.

## Status Summary
- Last updated: 2026-01-24
- Completed: Scopes 1, 2, 3, 5, 6
- Partially complete: Scope 4 (manual invalidation hook), Scope 8 (engine/materialize diagnostics)
- Deferred/optional: Scope 7 (queue/index integration), optional stampede guard for extract caches

---

## Scope 1 — Add a shared DiskCache factory and config surface
**Objective:** Provide a single module that builds caches (Cache/FanoutCache) with unified settings, directory layout, and tags.

**Status:** Completed

**Representative pattern**
```python
# cache/diskcache_factory.py
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from diskcache import Cache, FanoutCache

type DiskCacheKind = Literal["plan", "extract", "schema", "repo_scan", "runtime", "queue", "index"]

@dataclass(frozen=True)
class DiskCacheSettings:
    size_limit_bytes: int
    cull_limit: int = 10
    eviction_policy: str = "least-recently-used"
    statistics: bool = False
    tag_index: bool = True
    shards: int | None = None
    timeout_seconds: float = 60.0


@dataclass(frozen=True)
class DiskCacheProfile:
    root: Path = field(default_factory=_default_cache_root)
    base_settings: DiskCacheSettings = field(...)
    overrides: Mapping[DiskCacheKind, DiskCacheSettings] = field(default_factory=dict)
    ttl_seconds: Mapping[DiskCacheKind, float | None] = field(default_factory=dict)


def cache_for_kind(profile: DiskCacheProfile, kind: DiskCacheKind) -> Cache | FanoutCache:
    settings = profile.settings_for(kind)
    if settings.shards and settings.shards > 1:
        return FanoutCache(str(profile.root / kind), shards=settings.shards, ...)
    return Cache(str(profile.root / kind), ...)
```

**Target files**
- `src/cache/diskcache_factory.py`
- `src/cache/__init__.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- [x] Add a central DiskCache settings dataclass (directory, size, TTL defaults, eviction policy, shard count).
- [x] Add a factory that returns `Cache` or `FanoutCache` based on usage pattern.
- [x] Add cache directory layout conventions (per-kind subdirectories).
- [x] Wire settings into runtime profile (`DataFusionRuntimeProfile.diskcache_profile`).
- [x] Add a diagnostics helper to record cache stats/volume on demand.

**Decommission candidates**
- Ad-hoc cache construction (prevented by central factory).

---

## Scope 2 — Replace Substrait plan cache with DiskCache
**Objective:** Replace the in-memory `PlanCache` with a persistent DiskCache-backed implementation to avoid recompute across runs and processes.

**Status:** Completed

**Representative pattern**
```python
# engine/plan_cache.py
@dataclass
class PlanCache:
    cache_profile: DiskCacheProfile | None = field(default_factory=default_diskcache_profile)
    _cache: Cache | FanoutCache | None = field(default=None, init=False)

    def _ensure_cache(self) -> Cache | FanoutCache | None:
        if self._cache is None and self.cache_profile is not None:
            self._cache = cache_for_kind(self.cache_profile, "plan")
        return self._cache

    def put(self, entry: PlanCacheEntry) -> None:
        cache = self._ensure_cache()
        if cache is None:
            return
        cache.set(entry.key().as_key(), entry, tag=entry.profile_hash, retry=True)
```

**Target files**
- `src/engine/plan_cache.py`
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/compile_options.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- [x] Replace dict-backed `PlanCache` with DiskCache-backed version.
- [x] Use stable string keys (avoid tuple serialization pitfalls).
- [x] Tag plan entries by `profile_hash` (profile hash includes SQLGlot policy fingerprint).
- [x] Add size limits and eviction policy suitable for plan bytes (profile overrides).
- [x] Ensure runtime profile provides the cache instance.

**Decommission candidates**
- `PlanCache.entries` dict storage (removed).

---

## Scope 3 — DiskCache for AST / bytecode / symtable extraction
**Objective:** Replace per-process dict caches with DiskCache so concurrent workers and repeated runs can reuse extraction results.

**Status:** Completed (FanoutCache-enabled via profile shards; stampede guard optional)

**Representative pattern**
```python
# extract/ast_extract.py (illustrative)
from extract.cache_utils import cache_for_extract, cache_ttl_seconds, stable_cache_key

cache = cache_for_extract(profile)
cache_key = stable_cache_key(
    "extract:ast",
    {"file_sha256": file_ctx.file_sha256, "options": to_builtins(options)},
)
cached = cache.get(cache_key, default=None, retry=True)
if cached is not None:
    return cached

result = compute_ast(...)
cache.set(cache_key, result, tag=options.repo_id, expire=cache_ttl, retry=True)
```

**Target files**
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/cache_utils.py`
- `src/extract/session.py`

**Implementation checklist**
- [x] Add a shared cache key builder for extract outputs.
- [x] Replace `_AST_WORKER_CACHE`, `_BYTECODE_CACHE`, `_SYMTABLE_CACHE` with DiskCache.
- [x] Use FanoutCache for concurrent extract workloads (profile shards).
- [x] Tag entries by repo id and extractor options for invalidation.
- [x] Add TTL controls aligned with runtime profile settings.
- [ ] Optional: add stampede protection helpers for heavy extract workloads.

**Decommission candidates**
- `_AST_WORKER_CACHE` in `src/extract/ast_extract.py` (removed).
- `_BYTECODE_CACHE` in `src/extract/bytecode_extract.py` (removed).
- `_SYMTABLE_CACHE` in `src/extract/symtable_extract.py` (removed).

---

## Scope 4 — Cache schema introspection and contract metadata
**Objective:** Persist expensive schema introspection queries (information_schema lookups, constraint queries) across runs.

**Status:** Partially complete (cache + TTL + tags done; manual invalidation hook outstanding)

**Representative pattern**
```python
# datafusion_engine/schema_introspection.py
def _cached_rows(self, prefix: str, payload: Mapping[str, object], fetch: Callable[[], list[dict]]) -> list[dict]:
    key = stable_cache_key(prefix, payload)
    cached = self.cache.get(key, default=None, retry=True)
    if cached is None:
        cached = fetch()
        self.cache.set(key, cached, expire=self.cache_ttl, tag=self.cache_prefix, retry=True)
    return cached
```

**Target files**
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`

**Implementation checklist**
- [x] Add a schema-introspection cache with TTL.
- [x] Cache `table_names_snapshot`, `columns_snapshot`, `table_constraints`, `routine_parameters`, etc.
- [x] Tag entries by profile/schema-map hash prefix.
- [ ] Add an explicit invalidation hook when schema changes are detected.

**Decommission candidates**
- None (behavioral replacement only).

---

## Scope 5 — Cache repo scan results and worklist inputs
**Objective:** Avoid repeated repo scanning on stable inputs by caching scan outputs keyed by repo root and options.

**Status:** Completed

**Representative pattern**
```python
# extract/repo_scan.py
cache_key = stable_cache_key("repo_scan", {"repo_root": str(repo_root), "options": to_builtins(options)})
cached = cache.get(cache_key, default=None, retry=True)
if isinstance(cached, list):
    return extract_plan_from_rows("repo_files_v1", cached, ...)

rows = list(iter_rows())
cache.set(cache_key, rows, tag=options.repo_id, expire=cache_ttl, retry=True)
```

**Target files**
- `src/extract/repo_scan.py`
- `src/extract/repo_scan_fs.py`
- `src/extract/repo_scan_git.py`

**Implementation checklist**
- [x] Introduce a repo-scan cache keyed by repo root + include/exclude options.
- [x] Add TTL and tag eviction by repo id.
- [x] Store results as serializable rows.

**Decommission candidates**
- None (enhancement path).

---

## Scope 6 — Persist runtime artifact schema metadata
**Objective:** Persist schema fingerprints and materialized metadata across runs without serializing non-picklable runtime handles.

**Status:** Completed

**Representative pattern**
```python
# relspec/runtime_artifacts.py
cache_key = f"runtime_schema:{dataset_name}:{schema_fingerprint}"
diskcache.set(cache_key, schema, tag=plan_fingerprint, retry=True)
```

**Target files**
- `src/relspec/runtime_artifacts.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- [x] Persist `schema_cache` entries (SchemaLike only) into DiskCache.
- [x] Persist `MaterializedTable` metadata (not TableLike itself).
- [x] Provide a load path that reads from DiskCache when a run starts (`get_schema`).

**Decommission candidates**
- None (augmenting existing containers).

---

## Scope 7 — Optional persistent queues and indexes
**Objective:** Use DiskCache `Deque` and `Index` for durable work queues or ordered metadata if needed.

**Status:** Deferred (helpers added; no call sites yet)

**Representative pattern**
```python
from cache.diskcache_factory import build_deque

work_queue = build_deque(profile, name="extract_work_queue", maxlen=None)
work_queue.append(file_id)
```

**Target files**
- `src/cache/diskcache_factory.py`
- `src/cache/__init__.py`
- `src/extract/worklists.py` (if converting to a persistent queue)
- `src/incremental/*` (if durable job queues are desired)

**Implementation checklist**
- [x] Provide helper builders for DiskCache `Deque` and `Index`.
- [ ] Identify any queue-like patterns that would benefit from a persistent Deque.
- [ ] Implement durable queues with at-least-once semantics where justified.

**Decommission candidates**
- None (new capability).

---

## Scope 8 — Diagnostics and cache health reporting
**Objective:** Emit cache stats (hits, misses, size, volume) into existing diagnostics sinks for observability.

**Status:** Partially complete (DataFusion runtime emits stats; engine-level sinks pending)

**Representative pattern**
```python
from cache.diskcache_factory import diskcache_stats_snapshot

payload = diskcache_stats_snapshot(cache)
reporter.record("diskcache_stats_v1", payload)
```

**Target files**
- `src/cache/diskcache_factory.py`
- `src/datafusion_engine/runtime.py`
- `src/engine/materialize_pipeline.py`

**Implementation checklist**
- [x] Add a helper to snapshot cache stats and volume.
- [x] Plumb into DataFusion diagnostics sink with a stable event schema.
- [ ] Extend engine/materialization diagnostics to emit cache stats.
- [ ] Keep maintenance ops (`cache.check()`) opt-in only.

**Decommission candidates**
- None (observability enhancement).

---

## Decommission & Removal Summary
**Functions / objects removed**
- `_AST_WORKER_CACHE` in `src/extract/ast_extract.py`.
- `_BYTECODE_CACHE` in `src/extract/bytecode_extract.py`.
- `_SYMTABLE_CACHE` in `src/extract/symtable_extract.py`.
- `PlanCache.entries` dict storage in `src/engine/plan_cache.py`.

**Files to delete**
- None required for this migration (changes were in-place replacements).

---

## Execution Order (implemented)
1. Scope 1 (DiskCache factory + config surface)
2. Scope 2 (Plan cache persistence)
3. Scope 3 (Extract caches)
4. Scope 4 (Schema introspection caching) — partial (invalidation hook pending)
5. Scope 5 (Repo scan caching)
6. Scope 6 (Runtime artifact schema persistence)
7. Scope 8 (Diagnostics) — partial (engine-level sinks pending)
8. Scope 7 (Persistent queues/indexes) — deferred/optional
