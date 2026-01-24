# DiskCache Integration & Cache Streamlining Plan

## Goal
Adopt DiskCache as the shared cache substrate for cross-run, cross-process reuse and persistence. Replace high-impact in-memory caches with DiskCache equivalents, add missing cache layers where repeated work is observed, and centralize cache configuration/diagnostics so behavior is consistent across extract, engine, and runtime layers.

## Guiding Principles
- **Durable by default:** Prefer disk-backed caches for expensive, deterministic work that benefits from reuse across runs.
- **Single cache surface:** Centralize DiskCache configuration and lifecycle (directory, TTL, size, policy).
- **Multi-process safe:** Use `FanoutCache` for concurrent writers and stampede protection for heavy tasks.
- **Explicit invalidation:** Tag keys and evict by tag on schema/profile/policy changes.
- **Local-disk only:** Cache directories are per-host and must remain off NFS/shared filesystems.

---

## Scope 1 — Add a shared DiskCache factory and config surface
**Objective:** Provide a single module that builds caches (Cache/FanoutCache) with unified settings, directory layout, and tags.

**Status:** Planned

**Representative pattern**
```python
# cache/diskcache_factory.py
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from diskcache import Cache, FanoutCache

CacheKind = Literal["plan", "extract", "schema", "repo_scan", "runtime"]

@dataclass(frozen=True)
class DiskCacheSettings:
    directory: Path
    size_limit_bytes: int
    cull_limit: int
    eviction_policy: str
    statistics: bool
    tag_index: bool
    shards: int | None = None


def build_cache(kind: CacheKind, settings: DiskCacheSettings) -> Cache | FanoutCache:
    base_dir = settings.directory / kind
    if settings.shards and settings.shards > 1:
        return FanoutCache(
            str(base_dir),
            shards=settings.shards,
            size_limit=settings.size_limit_bytes,
            cull_limit=settings.cull_limit,
            eviction_policy=settings.eviction_policy,
            statistics=settings.statistics,
            tag_index=settings.tag_index,
        )
    return Cache(
        str(base_dir),
        size_limit=settings.size_limit_bytes,
        cull_limit=settings.cull_limit,
        eviction_policy=settings.eviction_policy,
        statistics=settings.statistics,
        tag_index=settings.tag_index,
    )
```

**Target files**
- `src/cache/diskcache_factory.py` (new)
- `src/engine/runtime_profile.py` (settings surface)
- `src/datafusion_engine/runtime.py` (plumb cache settings into profile/runtime)

**Implementation checklist**
- [ ] Add a central DiskCache settings dataclass (directory, size, TTL defaults, eviction policy, shard count).
- [ ] Add a factory that returns `Cache` or `FanoutCache` based on usage pattern.
- [ ] Add cache directory layout conventions (per-kind subdirectories).
- [ ] Wire settings into runtime profile so caches are configurable from one place.
- [ ] Add a small diagnostics helper to record cache stats/volume on demand.

**Decommission candidates**
- Ad-hoc cache construction (none today; this prevents future drift).

---

## Scope 2 — Replace Substrait plan cache with DiskCache
**Objective:** Replace the in-memory `PlanCache` with a persistent DiskCache-backed implementation to avoid recompute across runs and processes.

**Status:** Planned

**Representative pattern**
```python
# engine/plan_cache.py
from dataclasses import dataclass
from diskcache import Cache

@dataclass(frozen=True)
class PlanCacheKey:
    plan_hash: str
    profile_hash: str
    substrait_hash: str

    def as_key(self) -> str:
        return f"plan:{self.plan_hash}:{self.profile_hash}:{self.substrait_hash}"


@dataclass
class PlanCache:
    cache: Cache

    def get(self, key: PlanCacheKey) -> bytes | None:
        return self.cache.get(key.as_key())

    def put(self, entry: PlanCacheEntry) -> None:
        self.cache.set(entry.key().as_key(), entry.plan_bytes, tag=entry.profile_hash)
```

**Target files**
- `src/engine/plan_cache.py`
- `src/datafusion_engine/compile_options.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Replace dict-backed `PlanCache` with DiskCache-backed version.
- [ ] Use stable string keys (avoid tuple serialization pitfalls).
- [ ] Tag plan entries by `profile_hash` and `sqlglot_policy_hash` for bulk eviction.
- [ ] Add size limits and eviction policy suitable for plan bytes.
- [ ] Ensure runtime profile provides the cache instance.

**Decommission candidates**
- `PlanCache.entries` dict storage.

---

## Scope 3 — DiskCache for AST / bytecode / symtable extraction
**Objective:** Replace per-process dict caches with DiskCache so concurrent workers and repeated runs can reuse extraction results.

**Status:** Planned

**Representative pattern**
```python
# extract/cache_keys.py
from dataclasses import dataclass

@dataclass(frozen=True)
class ExtractCacheKey:
    kind: str
    file_sha256: str
    options_fingerprint: str

    def as_key(self) -> str:
        return f"extract:{self.kind}:{self.file_sha256}:{self.options_fingerprint}"
```

```python
# extract/ast_extract.py (illustrative)
cache_key = ExtractCacheKey(
    kind="ast",
    file_sha256=file_ctx.file_sha256,
    options_fingerprint=options_fingerprint,
).as_key()

cached = cache.get(cache_key)
if cached is not None:
    return cached

result = compute_ast(...)
cache.set(cache_key, result, tag=options.repo_id, expire=ttl_seconds)
```

**Target files**
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/cache_keys.py` (new)
- `src/extract/session.py` (plumb cache access)

**Implementation checklist**
- [ ] Add a shared cache key builder for extract outputs.
- [ ] Replace `_AST_WORKER_CACHE`, `_BYTECODE_CACHE`, `_SYMTABLE_CACHE` with DiskCache.
- [ ] Use `FanoutCache` + `memoize_stampede` to avoid duplicate work across processes.
- [ ] Tag entries by repo id and extractor options for invalidation.
- [ ] Add TTL controls aligned with runtime profile settings.

**Decommission candidates**
- `_AST_WORKER_CACHE` in `src/extract/ast_extract.py`.
- `_BYTECODE_CACHE` in `src/extract/bytecode_extract.py`.
- `_SYMTABLE_CACHE` in `src/extract/symtable_extract.py`.

---

## Scope 4 — Cache schema introspection and contract metadata
**Objective:** Persist expensive schema introspection queries (information_schema lookups, constraint queries) across runs.

**Status:** Planned

**Representative pattern**
```python
# datafusion_engine/schema_introspection.py
cache_key = f"schema:introspect:{profile_hash}:{table_name}"
rows = cache.get(cache_key)
if rows is None:
    rows = _rows_for_query(ctx, query)
    cache.set(cache_key, rows, tag=profile_hash, expire=ttl_seconds)
return rows
```

**Target files**
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/runtime.py` (cache access)
- `src/engine/runtime_profile.py` (TTL/limits)

**Implementation checklist**
- [ ] Add a schema-introspection cache (Cache or FanoutCache) with TTL.
- [ ] Cache `table_names_snapshot`, `table_constraint_rows`, `constraint_rows` results.
- [ ] Tag entries by `profile_hash` and `schema_map_hash`.
- [ ] Provide a manual invalidation hook when schema changes are detected.

**Decommission candidates**
- None (behavioral replacement only).

---

## Scope 5 — Cache repo scan results and worklist inputs
**Objective:** Avoid repeated repo scanning on stable inputs by caching scan outputs keyed by repo root and options.

**Status:** Planned

**Representative pattern**
```python
# extract/repo_scan.py
scan_key = f"repo_scan:{repo_root}:{options_fingerprint}"
cached = cache.get(scan_key)
if cached is not None:
    return cached

result = scan_repo_fs_or_git(...)
cache.set(scan_key, result, tag=options.repo_id, expire=ttl_seconds)
```

**Target files**
- `src/extract/repo_scan.py`
- `src/extract/repo_scan_fs.py`
- `src/extract/repo_scan_git.py`

**Implementation checklist**
- [ ] Introduce a repo-scan cache keyed by repo root + include/exclude options.
- [ ] Add TTL and tag eviction by repo id.
- [ ] Ensure cache results are stored as serializable rows or Arrow batches.

**Decommission candidates**
- None (enhancement path).

---

## Scope 6 — Persist runtime artifact schema metadata
**Objective:** Persist schema fingerprints and materialized metadata across runs without serializing non-picklable runtime handles.

**Status:** Planned

**Representative pattern**
```python
# relspec/runtime_artifacts.py
cache_key = f"runtime_schema:{dataset_name}:{schema_fingerprint}"
cache.set(cache_key, schema, tag=plan_fingerprint)
```

**Target files**
- `src/relspec/runtime_artifacts.py`
- `src/engine/runtime_profile.py` (cache settings)

**Implementation checklist**
- [ ] Persist `schema_cache` entries (SchemaLike only) into DiskCache.
- [ ] Persist `MaterializedTable` metadata (not TableLike itself).
- [ ] Provide a load path to seed caches when a run starts.

**Decommission candidates**
- None (augmenting existing containers).

---

## Scope 7 — Optional persistent queues and indexes
**Objective:** Use DiskCache `Deque` and `Index` for durable work queues or ordered metadata if needed.

**Status:** Planned

**Representative pattern**
```python
from diskcache import Deque

work_queue = Deque(str(cache_dir / "extract_work_queue"))
work_queue.append(file_id)
next_item = work_queue.popleft()
```

**Target files**
- `src/extract/worklists.py` (if converting to a persistent queue)
- `src/incremental/*` (if durable job queues are desired)

**Implementation checklist**
- [ ] Identify any queue-like patterns that would benefit from a persistent Deque.
- [ ] Implement durable queue with at-least-once semantics if needed.
- [ ] Ensure eviction policy is "none" for Deque/Index use cases.

**Decommission candidates**
- None (new capability).

---

## Scope 8 — Diagnostics and cache health reporting
**Objective:** Emit cache stats (hits, misses, size, volume) into existing diagnostics sinks for observability.

**Status:** Planned

**Representative pattern**
```python
stats = cache.stats()
volume = cache.volume()
reporter.record("diskcache_stats_v1", {"stats": stats, "volume": volume})
```

**Target files**
- `src/engine/runtime_profile.py`
- `src/datafusion_engine/runtime.py`
- `src/engine/materialize_pipeline.py`

**Implementation checklist**
- [ ] Add a helper to snapshot cache stats and volume.
- [ ] Plumb into diagnostics sink with a stable event schema.
- [ ] Avoid heavy `cache.check()` on hot paths; use opt-in maintenance.

**Decommission candidates**
- None (observability enhancement).

---

## Decommission & Removal Summary
**Functions / objects to remove after migration**
- `_AST_WORKER_CACHE` in `src/extract/ast_extract.py`.
- `_BYTECODE_CACHE` in `src/extract/bytecode_extract.py`.
- `_SYMTABLE_CACHE` in `src/extract/symtable_extract.py`.
- `PlanCache.entries` dict storage in `src/engine/plan_cache.py`.

**Files to delete**
- None required for this migration (all changes are in-place replacements).

---

## Execution Order (recommended)
1. Scope 1 (DiskCache factory + config surface)
2. Scope 2 (Plan cache persistence)
3. Scope 3 (Extract caches)
4. Scope 4 (Schema introspection caching)
5. Scope 5 (Repo scan caching)
6. Scope 6 (Runtime artifact schema persistence)
7. Scope 8 (Diagnostics)
8. Scope 7 (Persistent queues/indexes, optional)
