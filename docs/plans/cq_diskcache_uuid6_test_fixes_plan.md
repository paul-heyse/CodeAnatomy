# CQ DiskCache + UUID6 Follow‑up Plan (CQ Test Fixes + Cache Architecture)

## Goal
Resolve CQ test failures from the new diskcache/uuid6 migration while improving cache correctness, invalidation, and performance. This plan focuses on:
- Correct multi‑tag behavior without violating diskcache’s single‑tag column.
- Fast, deterministic cache invalidation and warm‑query performance.
- Stable golden snapshots after run metadata changes.
- Safe default caching in non‑CLI usage.

---

## Scope 1 — Tag Index + Single‑Tag DiskCache Writes

**Why**
Diskcache’s `Cache.set(tag=...)` only accepts a single string tag. Passing lists causes `sqlite3.ProgrammingError`. We still want multi‑tag semantics (file, rule, schema, etc.), so we’ll implement a tag index that maps tags → keys and store the tag list in the entry payload.

**Representative snippets**
```python
# tools/cq/cache/tag_index.py
@dataclass
class TagIndex:
    cache: Cache

    def add(self, key: str, tags: Sequence[str]) -> None:
        for tag in tags:
            index = self._index(tag)
            index.add(key)

    def keys_for(self, tag: str) -> set[str]:
        return set(self._index(tag))

    def remove(self, key: str, tags: Sequence[str]) -> None:
        for tag in tags:
            index = self._index(tag)
            index.discard(key)

    def _index(self, tag: str) -> diskcache.Index:
        return diskcache.Index.fromcache(self.cache, tag)
```

```python
# tools/cq/index/diskcache_query_cache.py
entry = {"value": value, "file_paths": file_path_strs, "tags": tags, ...}
self._cache.set(key, entry, tag="cq_query", expire=expire, retry=True)
self._tag_index.add(key, tags)
```

**Target files**
- `tools/cq/cache/tag_index.py` (new)
- `tools/cq/cache/__init__.py`
- `tools/cq/index/diskcache_query_cache.py`
- `tools/cq/index/diskcache_index_cache.py`
- `tools/cq/cache/diskcache_profile.py` (evict to use TagIndex)

**Deprecate/remove after completion**
- No file deletions required.
- Remove any direct multi‑tag `tag=[...]` usage in cache code.

**Implementation checklist**
- Add TagIndex helper and export it.
- Wire TagIndex into QueryCache and IndexCache.
- Persist tags in entry payload.
- Update `evict_cache_tag()` to use TagIndex and delete all matching keys.
- Ensure diskcache tag is always a single string.

---

## Scope 2 — File Fingerprint Cache for Fast Warm Queries

**Why**
Warm queries currently rehash all files in `_compute_paths_hash`, which defeats cache speed. We need a stable file fingerprint cache that avoids rehashing unchanged files.

**Representative snippets**
```python
# tools/cq/cache/file_fingerprint.py
@dataclass
class FileFingerprint:
    mtime: float
    size: int
    hash: str

class FileFingerprintCache:
    def __init__(self, cache: Cache):
        self._cache = cache

    def get_or_compute(self, path: Path) -> str:
        stat = path.stat()
        key = path.resolve().as_posix()
        entry = self._cache.get(key)
        if isinstance(entry, dict):
            if entry.get("mtime") == stat.st_mtime and entry.get("size") == stat.st_size:
                return str(entry.get("hash"))
        digest = _hash_file(path)
        self._cache.set(key, {"mtime": stat.st_mtime, "size": stat.st_size, "hash": digest}, tag="cq_fingerprint", retry=True)
        return digest
```

```python
# tools/cq/index/diskcache_query_cache.py
file_hash = fp_cache.get_or_compute(path)
```

**Target files**
- `tools/cq/cache/file_fingerprint.py` (new)
- `tools/cq/cache/__init__.py`
- `tools/cq/index/diskcache_query_cache.py`
- `tools/cq/cache/diskcache_profile.py` (new cache kind `cq_fingerprint`)

**Deprecate/remove after completion**
- `_compute_file_hash()` direct rehashing for query cache (keep for index cache).

**Implementation checklist**
- Add fingerprint cache and export it.
- Add new cache kind + settings in diskcache profile.
- Switch query cache hash computation to fingerprint cache.
- Keep logic for missing files (“missing” hash) consistent.

---

## Scope 3 — Default Cache Wiring for Non‑CLI Execution

**Why**
`execute_plan()` uses caches only if callers pass them. Tests and library callers bypass caches, harming warm performance.

**Representative snippets**
```python
# tools/cq/query/executor.py
if use_cache and query_cache is None:
    profile = default_cq_diskcache_profile()
    index_cache = index_cache or IndexCache(root, rule_version, profile=profile)
    query_cache = QueryCache(root, profile=profile)
    auto_close = True
```

**Target files**
- `tools/cq/query/executor.py`
- `tools/cq/cache/__init__.py` (optional cache manager export)

**Deprecate/remove after completion**
- None.

**Implementation checklist**
- Add optional auto‑construction and auto‑closing of caches in `execute_plan`.
- Ensure cache lifecycle is safe for callers (use `ExitStack`).
- Preserve current CLI behavior.

---

## Scope 4 — Golden Snapshot Stability

**Why**
Golden fixtures now fail because `run_id` is nondeterministic, toolchain keys changed, and query results include additional record kinds or new file contents.

**Representative snippets**
```python
# tests/e2e/cq/_support/goldens.py
if "run" in result_dict:
    result_dict["run"].pop("run_id", None)
    result_dict["run"].pop("started_ms", None)
    result_dict["run"].pop("elapsed_ms", None)
```

```python
# tests/e2e/cq/test_query_golden.py
query_text = "entity=import name=Path in=tools/cq/index/files.py"
```

**Target files**
- `tests/e2e/cq/_support/goldens.py`
- `tests/e2e/cq/test_query_golden.py`
- `tests/e2e/cq/fixtures/*.json` (update via update‑golden flow)

**Deprecate/remove after completion**
- None.

**Implementation checklist**
- Normalize `run_id` out of goldens.
- Update queries so snapshots match stable imports.
- Refresh fixtures after behavior changes (manual update‑golden run by user).

---

## Scope 5 — Cache Invalidation with Tag Index

**Why**
We need efficient eviction by tag without diskcache’s single tag limitation, and query invalidation should not require full scans.

**Representative snippets**
```python
# tools/cq/cache/diskcache_profile.py
cache_obj = cache_for_kind(profile, kind)
index = TagIndex(cache_obj)
for key in index.keys_for(tag):
    cache_obj.delete(key, retry=True)
```

```python
# tools/cq/index/diskcache_query_cache.py
# invalidate_file uses tag index to resolve keys
keys = self._tag_index.keys_for(f"file:{rel_path}")
for key in keys:
    self._cache.delete(key, retry=True)
```

**Target files**
- `tools/cq/cache/diskcache_profile.py`
- `tools/cq/index/diskcache_query_cache.py`
- `tools/cq/index/diskcache_index_cache.py`

**Deprecate/remove after completion**
- `QueryCache.invalidate_by_hash()` full scans can be kept but will be less critical.

**Implementation checklist**
- Use TagIndex in eviction paths.
- Ensure index is updated on add/remove for all cached entries.
- Keep fallback scanning path for safety when TagIndex is missing.

---

## Validation Notes
- Do not run `pytest` (per request). We will update tests and fixtures, but user will run the final test suite.
- Ruff/pyrefly/pyright should run only after all scopes are implemented (user requested separate sequencing earlier).

---

## Summary of Expected Outcomes
- All CQ failures caused by diskcache tag binding are eliminated.
- Warm‑query benchmark should return to <2s on typical hardware.
- Golden snapshot tests become deterministic again.
- Cache invalidation is robust, fast, and explicit.

