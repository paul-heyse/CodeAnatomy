# CQ Calls: Best-in-Class Architecture Plan (v1)

> **Status:** Implemented; cache scope removed per request
> **Author:** Codex
> **Created:** 2026-02-03
> **Context:** Modernize `cq calls` and align CQ macro architecture with ast-grep-py, rpygrep, diskcache, msgspec, and uuid7 best practices

---

## Executive Summary

This plan upgrades `cq calls` to a **structural, cached, and confidence-scored** callsite analyzer, and aligns CQ macros with the same **ast-grep-py + diskcache** substrate used by `cq q`. The changes emphasize **correctness, speed, and predictability**:

- Structural callsite detection (ast-grep-py) instead of regex-only candidates
- Incremental scanning with IndexCache
- Persisted DefIndex with cache invalidation
- Bounded rpygrep fallback with timeouts
- Call resolution + signature binding + hazard-aware confidence scoring
- Consistent msgspec models and uuid7 identifiers

---

## Goals

- Make `cq calls` accurate and fast on large repos.
- Eliminate redundant full-repo parsing in hot paths.
- Standardize cache behavior across macros and `q`.
- Produce stable, debuggable outputs with confidence metadata.

## Non-Goals

- Full rewrite of the CQ query engine (`cq q`).
- Replacing ast-grep rule definitions beyond what `calls` needs.
- Modifying unrelated macros unless explicitly called out in a scope item.

---

## Scope Item 1: ast-grep-py Callsite Scan + IndexCache

**Objective:** Replace regex-based call candidates with structural callsite records from ast-grep-py, and enable IndexCache for incremental scans.

**Representative snippet:**

```python
from tools.cq.query.sg_parser import sg_scan
from tools.cq.index.diskcache_index_cache import IndexCache

records = sg_scan(
    paths=[root],
    record_types={"call"},
    root=root,
    index_cache=index_cache,
)

call_records = [r for r in records if r.record == "call"]
# Filter by callee name, then enrich with arg previews + context.
```

**Target files:**
- `tools/cq/macros/calls.py`
- `tools/cq/query/sg_parser.py`
- `tools/cq/astgrep/rules_py.py` (if a refined call rule is needed)

**Deprecate/delete after completion:**
- `_rg_find_candidates()` in `tools/cq/macros/calls.py` (retained as fallback; deprecation pending)
- Direct reliance on `find_call_candidates()` in `cq calls` (retained as fallback; deprecation pending)

**Implementation checklist:**
- [x] Add `sg_scan(..., record_types={"call"})` path in `cmd_calls`.
- [x] Build a callsite filter that matches `call_name` and `call_attr` records.
- [x] Use `IndexCache` for incremental scan support.
- [x] Preserve existing output sections (shapes, kwargs, contexts), but derived from ast-grep records.
- [x] Add unit coverage for call records scan path.

---

## Scope Item 2: Persisted DefIndex (diskcache-backed)

**Objective:** Avoid full repo re-parsing just to show a signature preview; persist `DefIndex` by repo root and invalidate via file hashes.

**Representative snippet:**

```python
from tools.cq.index.def_index import DefIndex
from tools.cq.index.diskcache_index_cache import IndexCache

index = DefIndex.load_or_build(
    root=root,
    index_cache=index_cache,
    max_files=2000,
)
```

**Target files:**
- `tools/cq/index/def_index.py`
- `tools/cq/index/diskcache_index_cache.py`
- `tools/cq/macros/calls.py`
- `tools/cq/cache/diskcache_profile.py` (adds `cq_def_index` cache kind)

**Deprecate/delete after completion:**
- Repeated `DefIndex.build(...)` calls in `cmd_calls`.

**Implementation checklist:**
- [x] Add `DefIndex.load_or_build(...)` with diskcache backing (implementation retained, not used by `calls`).
- [x] Serialize `DefIndex` via msgspec msgpack (fast, stable).
- [x] Cache key includes repo root + file hash fingerprint.
- [ ] Wire `cmd_calls` signature preview to cached index (intentionally reverted; caching removed from `calls`).
- [ ] Add tests for cache hit/miss and invalidation (intentionally removed from scope).

---

## Scope Item 3: Call Resolution + Signature Binding + Confidence

**Objective:** Add call resolution and argument binding to classify callsites (ok/ambiguous/would_break) and score confidence.

**Representative snippet:**

```python
from tools.cq.index.call_resolver import resolve_calls
from inspect import signature

resolved = resolve_calls(index, call_sites)
for call in resolved:
    if call.targets:
        sig = signature(call.targets[0].to_callable_stub())
        sig.bind_partial(*call.args, **call.kwargs)  # raises if incompatible
```

**Target files:**
- `tools/cq/index/call_resolver.py`
- `tools/cq/macros/calls.py`
- `tools/cq/core/scoring.py`

**Deprecate/delete after completion:**
- Flat “found/not found” callsite reporting without confidence metadata.

**Implementation checklist:**
- [x] Extend callsite model to include resolution targets and confidence.
- [x] Add binding classification: ok/ambiguous/would_break.
- [x] Annotate hazards (dynamic getattr, eval/exec) with lower confidence.
- [x] Surface confidence in summary and key findings.
- [x] Add unit tests for resolution + binding.

---

## Scope Item 4: Bounded rpygrep Fallback + Timeouts

**Objective:** Keep rpygrep as a fallback prefilter but enforce safety limits (max depth, file size, match count, timeouts).

**Representative snippet:**

```python
from tools.cq.search.timeout import search_sync_with_timeout

searcher = (
    RipGrepSearch()
    .set_working_directory(root)
    .add_pattern(pattern)
    .include_type("py")
    .max_count(limits.max_matches_per_file)
    .max_file_size(2 * 1024 * 1024)
    .max_depth(20)
    .add_safe_defaults()
)

results = search_sync_with_timeout(searcher.run, limits.timeout_seconds)
```

**Target files:**
- `tools/cq/search/adapter.py`
- `tools/cq/search/profiles.py`
- `tools/cq/search/timeout.py`

**Deprecate/delete after completion:**
- Unbounded `find_call_candidates` execution (no timeouts).

**Implementation checklist:**
- [x] Apply `max_count`, `max_file_size`, `max_depth`, and `add_safe_defaults`.
- [x] Wrap sync calls with `search_sync_with_timeout`.
- [x] Respect `SearchLimits.max_files` and `max_matches_per_file`.
- [x] Add tests covering timeout + limits.

---

## Scope Item 5: Macro Result Cache + Telemetry Envelope

**Objective:** Add a macro-level cache for `calls` results and emit cache/scan telemetry for observability.

**Representative snippet:**

```python
cache_key = make_cache_key("calls", function_name, {"root": str(root)})
result = query_cache.get(cache_key, cache_files)
if result is None:
    result = compute_calls(...)
    query_cache.set(cache_key, result, cache_files)
result.summary["cache"] = {"status": "hit" if hit else "miss"}
```

**Target files:**
- `tools/cq/macros/calls.py`
- `tools/cq/index/diskcache_query_cache.py`
- `tools/cq/core/schema.py`

**Deprecate/delete after completion:**
- Ad-hoc caching logic in `calls` (if any future local caching appears).

**Implementation checklist:**
- [ ] Define `cache_files` from scanned call record files (not applicable; cache removed).
- [ ] Add cache hit/miss metadata to summary (not applicable; cache removed).
- [x] Emit scan time, files scanned, and records reused (scan stats emitted; reuse not applicable without cache).
- [ ] Add `--no-cache` support for `calls` if needed (parity with `q`) (removed with cache).

---

## Scope Item 6: Stable Identifiers (uuid7) + msgspec Consistency

**Objective:** Ensure all `calls` outputs are traceable and schema-consistent across runs.

**Representative snippet:**

```python
from tools.cq.utils.uuid_factory import uuid7_str

result.run.run_id = result.run.run_id or uuid7_str()
callsite_id = uuid7_str()
```

**Target files:**
- `tools/cq/core/schema.py`
- `tools/cq/macros/calls.py`
- `tools/cq/core/artifacts.py`

**Deprecate/delete after completion:**
- Ad-hoc identifiers that are not time-ordered or stable.

**Implementation checklist:**
- [x] Ensure `run_id` is always set for `calls` results.
- [x] Add callsite-level IDs where useful (optional, gated by size).
- [x] Confirm msgspec serialization compatibility for new fields (validated via msgspec structs).

---

## Cross-Cutting Testing Plan

- Unit tests for:
  - `calls` using ast-grep record path.
  - DefIndex cache hit/miss and invalidation.
  - rpygrep limits/timeouts.
  - call resolution + binding classification.
- Integration test for `cq calls <fn>` using cached index and record reuse.

**Status update:**
- `calls` ast-grep path: **implemented + unit test added**
- DefIndex cache: **implementation retained, not used in `calls`**
- rpygrep timeout/limits tests: **implemented**
- resolution/binding tests: **implemented**
- integration test: **implemented**

---

## Rollout Plan

1. Implement Scope Items 1 and 2 (core performance + correctness).
2. Add Scope Item 3 (resolution + confidence). Run perf checks.
3. Add Scope Item 4 (fallback hardening).
4. Add Scope Items 5 and 6 (cache + telemetry + identifiers).
5. Run full CQ quality gate.

---

## Risks and Mitigations

- **ast-grep rule mismatch**: Add explicit tests for call_name/call_attr patterns.
- **Cache invalidation bugs**: Use file hash + repo root fingerprint in keys.
- **Signature binding errors**: Catch `TypeError` and classify as ambiguous, not fatal.
- **Output size growth**: Gate per-callsite IDs behind a limit or config.

---

## Expected Outcomes

- `cq calls` is accurate, fast, and stable on large repositories.
- CQ macros share the same incremental scanning and caching substrate.
- Outputs are structured, confidence-scored, and easy to debug.
