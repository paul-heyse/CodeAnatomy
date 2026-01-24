# DiskCache Feature Expansion & Hardening Plan

## Goal
Leverage the full DiskCache feature set to improve cache observability, durability, eviction control, performance, and safety across extract, DataFusion, and runtime subsystems.

## Guiding Principles
- **Centralized policy**: expose all DiskCache knobs through one profile/settings surface.
- **Observability first**: cache stats should be actionable in diagnostics.
- **Explicit invalidation**: build evict-by-tag APIs for every cache kind.
- **Safe concurrency**: guard heavy workloads against stampede and timeout pitfalls.
- **Predictable persistence**: avoid stale caches via versioned keys and explicit maintenance.

---

## Status Summary
- Last updated: 2026-01-24
- Completed: Scopes 1–8
- Remaining: None

---

## Scope 1 — Enable stats/tuning profiles per cache kind
**Objective:** allow high‑value stats and DB tuning to be controlled per kind (plan/extract/schema/repo_scan/runtime/queue/index) without ad‑hoc overrides.

**Representative pattern**
```python
# cache/diskcache_factory.py
@dataclass(frozen=True)
class DiskCacheSettings:
    size_limit_bytes: int
    cull_limit: int = 10
    eviction_policy: str = "least-recently-used"
    statistics: bool = False
    tag_index: bool = True
    shards: int | None = None
    timeout_seconds: float = 60.0
    disk_min_file_size: int | None = None
    # new
    sqlite_journal_mode: str | None = "wal"
    sqlite_mmap_size: int | None = None
    sqlite_synchronous: str | None = None


def cache_for_kind(profile: DiskCacheProfile, kind: DiskCacheKind) -> Cache | FanoutCache:
    settings = profile.settings_for(kind)
    cache = Cache(
        str(profile.root / kind),
        size_limit=settings.size_limit_bytes,
        cull_limit=settings.cull_limit,
        eviction_policy=settings.eviction_policy,
        statistics=settings.statistics,
        tag_index=settings.tag_index,
        timeout=int(settings.timeout_seconds),
        disk_min_file_size=settings.disk_min_file_size,
        sqlite_journal_mode=settings.sqlite_journal_mode,
        sqlite_mmap_size=settings.sqlite_mmap_size,
        sqlite_synchronous=settings.sqlite_synchronous,
    )
    return cache
```

**Target files**
- `src/cache/diskcache_factory.py`
- `src/datafusion_engine/runtime.py`
- `src/engine/materialize_pipeline.py`

**Implementation checklist**
- [x] Extend `DiskCacheSettings` with SQLite/Disk knobs (journal/mmap/synchronous).
- [x] Support per‑kind overrides for stats enablement and tuning.
- [x] Update diagnostics to show settings payloads when stats are enabled.

**Decommission candidates**
- None.

---

## Scope 2 — Add explicit maintenance and eviction APIs
**Objective:** provide cache maintenance controls (expire/cull/check) and evict‑by‑tag surfaces for each cache kind.

**Representative pattern**
```python
# cache/diskcache_factory.py
@dataclass(frozen=True)
class DiskCacheMaintenance:
    kind: DiskCacheKind
    expired: int
    culled: int
    checked: int


def run_cache_maintenance(profile: DiskCacheProfile, *, kind: DiskCacheKind) -> DiskCacheMaintenance:
    cache = cache_for_kind(profile, kind)
    expired = cache.expire(retry=True)
    culled = cache.cull(retry=True)
    checked = cache.check()  # optional, off hot paths
    return DiskCacheMaintenance(kind=kind, expired=expired, culled=culled, checked=checked)
```

**Target files**
- `src/cache/diskcache_factory.py`
- `src/datafusion_engine/runtime.py`
- `src/engine/plan_cache.py`
- `src/extract/cache_utils.py`

**Implementation checklist**
- [x] Add cache maintenance helper and expose in runtime diagnostics tooling.
- [x] Add `evict_*` helpers for plan/extract/schema/repo_scan/runtime caches (tag-based).
- [x] Ensure maintenance is opt‑in and never on hot paths.

**Decommission candidates**
- None.

---

## Scope 3 — Standardize retry/timeout and lock semantics
**Objective:** ensure write operations use consistent retry behavior, and locks never block indefinitely.

**Representative pattern**
```python
# extract/cache_utils.py
LOCK_EXPIRE_SECONDS: float = 60.0

@contextmanager
def cache_lock(cache: Cache | FanoutCache | None, *, key: str) -> Iterator[None]:
    if cache is None:
        yield None
        return
    with Lock(cache, f"lock:{key}", expire=LOCK_EXPIRE_SECONDS):
        yield None
```

**Target files**
- `src/extract/cache_utils.py`
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/symtable_extract.py`

**Implementation checklist**
- [x] Use a short lock TTL independent of cache TTL.
- [x] Wrap DiskCache operations in helpers to enforce `retry=True` where needed.
- [x] Audit cache writes for FanoutCache timeout semantics; enforce retries uniformly.

**Decommission candidates**
- None.

---

## Scope 4 — Versioned keys and schema fingerprints
**Objective:** prevent stale caches after schema or algorithm changes by versioning cache keys.

**Representative pattern**
```python
# extract/cache_utils.py
CACHE_VERSION: int = 1

def stable_cache_key(prefix: str, payload: Mapping[str, object]) -> str:
    payload = {"version": CACHE_VERSION, **payload}
    digest = _hash_payload(payload)
    return f"{prefix}:{digest}"
```

**Target files**
- `src/extract/cache_utils.py`
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/repo_scan.py`

**Implementation checklist**
- [x] Add cache version in shared key builder.
- [x] Include schema/algorithm fingerprints in extract cache payloads.
- [x] Add version bump notes to docs when cache schema changes.

**Decommission candidates**
- None.

---

## Scope 5 — File‑backed values for large payloads
**Objective:** reduce SQLite bloat for large cached objects via file‑backed storage.

**Representative pattern**
```python
# extract/repo_blobs.py
cache.set(cache_key, blob_bytes, read=True, expire=ttl, tag=repo_id, retry=True)
```

**Target files**
- `src/extract/repo_blobs.py`
- `src/extract/repo_blobs_git.py`
- `src/cache/diskcache_factory.py`

**Implementation checklist**
- [x] Add diskcache usage to repo blob scanning with `read=True` for large blobs.
- [x] Tune `disk_min_file_size` per kind to keep large payloads off SQLite.

**Decommission candidates**
- None.

---

## Scope 6 — Memoize / throttle / barrier for expensive pure functions
**Objective:** use DiskCache decorators where they simplify pure, deterministic workloads.

**Representative pattern**
```python
# datafusion_engine/schema_introspection.py
from diskcache import memoize_stampede

@memoize_stampede(cache, expire=ttl_seconds, tag=profile_key)
def schema_map_snapshot(...):
    ...
```

**Target files**
- `src/datafusion_engine/schema_introspection.py`
- `src/extract/repo_scan.py`
- `src/extract/repo_blobs.py`

**Implementation checklist**
- [x] Identify pure functions with stable inputs (schema map, repo scan, blob reads).
- [x] Apply `memoize_stampede` where call frequency is high and contention occurs.
- [x] Use `throttle`/`barrier` in any concurrent scanning hotspots where coordination helps.

**Decommission candidates**
- None.

---

## Scope 7 — Transactional batching for bulk cache writes
**Objective:** speed up heavy cache writes and ensure atomic write batches.

**Representative pattern**
```python
# plan_cache.py
with cache.transact():
    for entry in entries:
        cache.set(entry.key().as_key(), entry, tag=entry.profile_hash, retry=True)
```

**Target files**
- `src/engine/plan_cache.py`
- `src/relspec/runtime_artifacts.py`

**Implementation checklist**
- [x] Add transactional bulk‑write helpers for Cache (non‑FanoutCache).
- [x] Use shard‑scoped cache for transactions when FanoutCache is configured.

**Decommission candidates**
- None.

---

## Scope 8 — Queue/index configuration hardening
**Objective:** ensure persistent `Deque`/`Index` usage respects DiskCache semantics and never evicts.

**Representative pattern**
```python
# cache/diskcache_factory.py
settings = profile.settings_for("index")
Index(
    str(cache_dir),
    size_limit=settings.size_limit_bytes,
    eviction_policy="none",
    ...,
)
```

**Target files**
- `src/cache/diskcache_factory.py`
- `src/extract/worklists.py`

**Implementation checklist**
- [x] Ensure queue/index settings are always `eviction_policy="none"`.
- [x] Add safeguards to prevent accidental eviction or TTL on queue/index data.

**Decommission candidates**
- None.

---

## Decommission & Removal Summary
**Functions / objects to remove after migration**
- None (changes are additive and hardening only).

**Files to delete**
- None.

---

## Execution Order (recommended)
1. Scope 1 — settings/tuning surface
2. Scope 2 — maintenance + eviction APIs
3. Scope 3 — lock and retry semantics
4. Scope 4 — versioned keys
5. Scope 5 — file‑backed payloads
6. Scope 6 — memoize/throttle/barrier
7. Scope 7 — transactional batching
8. Scope 8 — queue/index hardening
