# CQ DiskCache + UUID6 Implementation Plan

Date: 2026-02-02
Owner: Codex (draft for review)
Status: In-Progress (most scope implemented; remaining items listed per scope)

Status Update (2026-02-02)
This plan has been reviewed against the current codebase. Each scope item below now includes:
- Status: Completed / Partial
- Completed work (what is already in code)
- Remaining work (gaps vs plan)

Goal
Upgrade CQ's caching, run identity, and artifact management by leveraging DiskCache (diskcache) and time-ordered UUIDs (uuid6/uuid7), delivering stronger correctness, better throughput, and operationally safer invalidation/maintenance.


Scope Item 1 — Run IDs + Artifact Naming (uuid6/uuid7)

Intent
- Introduce stable, time-ordered `run_id` for every CQ invocation.
- Use `run_id` in artifacts and result metadata to enable correlation and deterministic sorting.

Representative code snippets
```python
# tools/cq/core/schema.py
@dataclass
class RunMeta:
    macro: str
    argv: list[str]
    root: str
    started_ms: float
    elapsed_ms: float
    toolchain: dict[str, str | None] = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION
    run_id: str | None = None  # NEW
```

```python
# tools/cq/core/schema.py (mk_runmeta)
from tools.cq.utils.uuid_factory import uuid7_str

def mk_runmeta(...):
    ...
    return RunMeta(
        macro=macro,
        argv=argv,
        root=root,
        started_ms=started_ms,
        elapsed_ms=elapsed,
        toolchain=toolchain,
        run_id=uuid7_str(),
    )
```

```python
# tools/cq/core/artifacts.py
def save_artifact_json(...):
    ...
    if filename is None:
        ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        run_id = result.run.run_id or "no_run_id"
        filename = f"{result.run.macro}_{ts}_{run_id}.json"
```

Target files
- `tools/cq/core/schema.py`
- `tools/cq/core/artifacts.py`
- `tools/cq/cli_app/result.py` (ensure run_id is included in saved artifacts)
- `tools/cq/cli_app/context.py` (if run_id needs to be passed through)
- `tools/cq/utils/uuid_factory.py` (new CQ-local uuid helpers, no src/ dependency)

Deprecate/delete after completion
- None (augmentation only).

Implementation checklist
- Add `run_id` to `RunMeta` (serialize/deserialize).
- Update `mk_runmeta()` to populate `run_id` via `uuid7_str()`.
- Update artifact filename generation to include `run_id`.
- Add/adjust tests for RunMeta JSON serialization and artifact filenames.
- Create CQ-local `uuid_factory` (uuid6/uuid7 fallback, thread-safe) under `tools/cq/utils/`.

Status: Partial (implementation complete; tests not yet added)
Completed:
- `RunMeta.run_id` added and serialized/deserialized.
- `mk_runmeta()` now generates a UUIDv7 via `tools/cq/utils/uuid_factory.py`.
- Artifact filenames include `run_id`.
- CQ-local UUID utilities added under `tools/cq/utils/`.
Remaining:
- Add/adjust tests for RunMeta JSON serialization and artifact filenames (plan requirement not yet implemented).


Scope Item 2 — DiskCache-backed Query Cache (replace SQLite QueryCache)

Intent
- Replace `tools/cq/index/query_cache.py` with DiskCache-backed implementation.
- Enable TTL, size limits, and tagged invalidation instead of LIKE-based deletes.

Representative code snippets
```python
# tools/cq/index/diskcache_query_cache.py (new)
from diskcache import Cache
from tools.cq.cache.diskcache_profile import DiskCacheProfile, cache_for_kind

class DiskQueryCache:
    def __init__(self, profile: DiskCacheProfile, *, kind: str = "cq_query") -> None:
        self._cache = cache_for_kind(profile, kind)  # Cache or FanoutCache

    def get(self, key: str) -> object | None:
        return self._cache.get(key, default=None)

    def set(self, key: str, value: object, *, tags: list[str], expire: float | None) -> None:
        self._cache.set(key, value, tag=tags, expire=expire, retry=True)

    def evict_tag(self, tag: str) -> int:
        return int(self._cache.evict(tag, retry=True))
```

```python
# tools/cq/query/executor.py (cache tags)
tags = [
    f"query:{cache_key}",
    f"scope:{plan.scope.scope_key()}",
    f"rule:{rule_version}",
]
for file in cache_files:
    tags.append(f"file:{file.relative_to(root)}")
```

Target files
- `tools/cq/index/query_cache.py` (deprecated)
- `tools/cq/index/diskcache_query_cache.py` (new)
- `tools/cq/query/executor.py`
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/core/bundles.py`
- `tools/cq/cache/diskcache_profile.py` (new CQ-local DiskCache utilities)

Deprecate/delete after completion
- `tools/cq/index/query_cache.py` (replace references, then delete)

Implementation checklist
- Create DiskCache-backed QueryCache with `get`, `set`, `evict_tag`, `stats`.
- Build a tag scheme (`file:`, `query:`, `rule:`, `schema:`).
- Update CQ execution to use DiskCache cache object.
- Add stats surfaced in `cq cache --stats`.
- Add migration/compat: fallback to SQLite cache if diskcache unavailable.

Status: Partial (aggressive migration done; fallback and tag breadth remain)
Completed:
- New DiskCache-backed cache at `tools/cq/index/diskcache_query_cache.py`.
- All call sites migrated; legacy `tools/cq/index/query_cache.py` deleted.
- Admin cache stats now show size + hits/misses.
- Query cache TTL wired via `DiskCacheProfile` (env override supported).
Remaining:
- Tag scheme breadth: current tags include `query:` and `file:` only; plan called out `rule:`/`schema:` tags.
- Optional fallback path for missing diskcache is not implemented (aggressive migration removes SQLite).


Scope Item 3 — DiskCache-backed Index Cache (replace SQLite IndexCache)

Intent
- Replace `tools/cq/index/sqlite_cache.py` with DiskCache for scan results.
- Use tag-based invalidation and DiskCache transactions for multi-record updates.

Representative code snippets
```python
# tools/cq/index/diskcache_index_cache.py (new)
class DiskIndexCache:
    def __init__(...):
        self._cache = cache_for_kind(profile, "cq_index")

    def store(self, file: Path, records: list[dict[str, object]], record_types: set[str]) -> None:
        key = f"scan:{file}"
        tags = [f"file:{file}", f"record_types:{','.join(sorted(record_types))}"]
        self._cache.set(key, records, tag=tags, retry=True)

    def retrieve(...):
        ...
```

```python
# tools/cq/query/sg_parser.py (switch from IndexCache to DiskIndexCache)
records = sg_scan(..., index_cache=index_cache)
```

Target files
- `tools/cq/index/sqlite_cache.py` (deprecated)
- `tools/cq/index/diskcache_index_cache.py` (new)
- `tools/cq/query/sg_parser.py`
- `tools/cq/cli_app/commands/admin.py` (index cache management)
- `tools/cq/cache/diskcache_profile.py` (new CQ-local DiskCache utilities)

Deprecate/delete after completion
- `tools/cq/index/sqlite_cache.py` (after migration)

Implementation checklist
- Implement DiskCache index cache with `needs_rescan`, `store`, `retrieve`, `stats`.
- Use `cache.transact()` for storing multiple files.
- Include rule/schema version in keys or tags to ensure invalidation.
- Update admin commands for index cache stats/clear.

Status: Partial (core migration done; transactions/batching remain)
Completed:
- New DiskCache-backed cache at `tools/cq/index/diskcache_index_cache.py`.
- All call sites migrated; legacy `tools/cq/index/sqlite_cache.py` deleted.
- Admin index stats/clear wired.
Remaining:
- Transactional/batched write path is not implemented; current store writes per file.


Scope Item 4 — Cache Profiles, Maintenance, and CLI Controls

Intent
- Add CQ-specific diskcache profile (settings + TTL) to tune size limits, eviction, tag index.
- Provide CLI controls for cache stats, evictions, and maintenance.

Representative code snippets
```python
# tools/cq/cache/diskcache_profile.py (new)
from tools.cq.cache.diskcache_profile import DiskCacheProfile, DiskCacheSettings

def cq_diskcache_profile() -> DiskCacheProfile:
    base = DiskCacheSettings(size_limit_bytes=512 * 1024 * 1024, tag_index=True)
    overrides = {
        "cq_query": DiskCacheSettings(size_limit_bytes=512 * 1024 * 1024, shards=4),
        "cq_index": DiskCacheSettings(size_limit_bytes=256 * 1024 * 1024),
    }
    return DiskCacheProfile(root=Path.home() / ".cache" / "codeanatomy" / "cq", base_settings=base, overrides=overrides)
```

```python
# tools/cq/cli_app/commands/admin.py
if stats:
    stats = cache.stats()
    # surface hits/misses/count/size/volume
```

Target files
- `tools/cq/cache/diskcache_profile.py` (new, replaces cache_profile stub)
- `tools/cq/cli_app/commands/admin.py`
- `tools/cq/cli_app/params.py` (new CQ cache options: ttl, size limit overrides)

Deprecate/delete after completion
- None (new control plane)

Implementation checklist
- Define CQ diskcache profile + environment overrides (CQ-local utilities).
- Implement CQ-local `DiskCacheSettings`, `DiskCacheProfile`, and `cache_for_kind` in `tools/cq/cache/diskcache_profile.py`.
- Add minimal maintenance helpers in the same module (`diskcache_stats_snapshot`, `run_cache_maintenance`, `evict_cache_tag`) as needed by CQ CLI.
- Wire profile into CQ cache construction.
- Extend `cq cache` and `cq index` commands for stats/clear/evict-tag.
- Add maintenance hooks (expire/cull/check).

Status: Completed
Completed:
- CQ-local DiskCache profile + settings in `tools/cq/cache/diskcache_profile.py`.
- Maintenance helpers (`run_cache_maintenance`, `evict_cache_tag`, stats snapshot).
- CLI admin commands support stats/clear/evict-tag/maintenance.
- Global CLI flags added for DiskCache overrides; README updated.


Scope Item 5 — Stampede Protection + Locking

Intent
- Avoid repeated heavy scans and cache rebuilds by using DiskCache `Lock`, `barrier`, and `memoize_stampede`.

Representative code snippets
```python
# tools/cq/index/def_index.py (guard build with Lock)
from diskcache import Lock

with Lock(cache, f"def_index:{root}"):
    index = DefIndex.build(root, ...)
```

```python
# tools/cq/query/executor.py (memoize_stampede for hazard rule builds)
from diskcache import memoize_stampede

@memoize_stampede(cache, expire=300)
def _hazard_rules_yaml() -> str:
    return detector.build_inline_rules_yaml()
```

Target files
- `tools/cq/index/def_index.py`
- `tools/cq/query/executor.py`
- `tools/cq/query/hazards.py` (if rule construction lives here)

Deprecate/delete after completion
- None

Implementation checklist
- Add lock around heavy, rebuildable operations.
- Use `memoize_stampede` for hazard rules YAML construction.
- Ensure lock scopes are short and cache kinds are isolated.

Status: Completed
Completed:
- `DefIndex.build` guarded by DiskCache `Lock` using `cq_coordination` cache.
- Hazard rules YAML is memoized with `memoize_stampede` and hashed via `hazard_rules_hash`.


Scope Item 6 — Observability & Cache Stats Emission

Intent
- Surface DiskCache hit/miss statistics in `cq cache --stats` and (optionally) in CQ results summary.

Representative code snippets
```python
# tools/cq/core/report.py (append cache stats)
if result.summary.get("cache"):
    lines.append(f"- Cache: {result.summary['cache']}")
```

```python
# tools/cq/index/diskcache_query_cache.py
def stats(self) -> dict[str, object]:
    stats = self._cache.stats()
    return {"hits": stats.hits, "misses": stats.misses, "size": stats.size}
```

Target files
- `tools/cq/cli_app/commands/admin.py`
- `tools/cq/core/report.py`
- `tools/cq/query/executor.py`

Deprecate/delete after completion
- None

Implementation checklist
- Add cache stats collection.
- Add cache stats to summary output and admin commands.
- Document the new stats format in `tools/cq/README.md`.

Status: Partial
Completed:
- Admin cache stats expose hits/misses/size; README updated with cache CLI flags.
Remaining:
- Add cache stats to CQ result summary/report output (plan suggested `tools/cq/core/report.py` change).


Global Migration/Compatibility Notes

- Provide a fallback path when diskcache is unavailable (optional dependency).
- All CQ DiskCache utilities live under `tools/cq/cache/` (do not import from `src/`).
- All CQ UUID utilities live under `tools/cq/utils/` (do not import from `src/`).
- Migrate cache directories to a new CQ-specific root (avoid clobbering existing SQLite caches).
- Keep old cache files intact until the new caches are stable; offer a `cq cache --migrate` if needed.


Test Plan (post-implementation)
- Unit tests for new cache key/tag logic and run_id serialization.
- Integration test: `cq q "entity=function name=build_graph_product"` with cache hit path.
- Load test (optional): repeated runs to verify `memoize_stampede` behavior.
