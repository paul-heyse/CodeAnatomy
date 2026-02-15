# CQ Search Performance Optimization Plan v1 (2026-02-14)

## Scope
This document captures measured bottlenecks and design recommendations to reduce `cq search` latency from ~90-110s (cold path) to a few seconds for common identifier queries.

## Baseline Measurements (local)

### Query used
- `./cq search stable_id`

### Timings
- Warm-ish path (cache enabled): `~10-15s`
- Cold path (cache disabled): `~107-109s`
- Cold path with semantic enrichment disabled (`CQ_ENABLE_SEMANTIC_ENRICHMENT=0`): `~94s`
- Raw ripgrep candidate scan equivalent: `~0.04s`
- Candidate file list (`rg -l ...`): `~0.02s` for `24` files

### What this proves
- `ripgrep` is not the bottleneck.
- Semantic front-door contributes, but is not the dominant cost.
- Dominant latency is in post-candidate pipeline work.

## Profiling Findings

A `cProfile` run of cold search shows dominant cumulative cost in structural neighborhood assembly and file tabulation/path normalization:

- `tools/cq/search/smart_search.py:_build_structural_neighborhood_preview`
- `tools/cq/neighborhood/scan_snapshot.py:ScanSnapshot.build_from_repo`
- `tools/cq/neighborhood/cache_pipeline.py:build_neighborhood_records`
- `tools/cq/query/sg_parser.py:_tabulate_scan_files`
- `tools/cq/index/files.py:tabulate_files`
- `tools/cq/index/files.py:_filter_tracked_to_scope`
- `tools/cq/index/files.py:_path_is_under`

Secondary cost exists in Python native resolution:
- `tools/cq/search/python_native_resolution.py:enrich_python_resolution_by_byte_range`

ast-grep structural review confirms repeated AST walks and per-anchor full-node scans in native resolution and enrichment code paths.

## Root Causes

1. Search always builds a full structural neighborhood snapshot from repo scope.
- This is effectively a second large scan during `search` result assembly.

2. Scope/file tabulation is path-operation heavy.
- High-volume `Path.resolve()`, `Path.relative_to()`, `Path(...)` conversions and nested tracked-path checks dominate CPU.

3. Python native resolution is anchor-centric with repeated AST traversal.
- Per-match `_find_ast_anchor` loops through all AST nodes and recomputes node byte spans.

4. Deep enrichment is applied too broadly.
- Enrichment is still per-occurrence heavy before object-level consolidation decisions are fully exploited.

5. Cache encoding/decoding remains conversion-heavy in hot loops.
- Repeated `contract_to_builtins`/`msgspec.convert` around many small cache operations adds overhead.

## Optimization Recommendations

## P0: Remove Highest-Latency Work from Default Search Path

### 1) Make structural neighborhood lazy/opt-in for `search`
- Do not run `ScanSnapshot.build_from_repo(...)` in default `cq search` execution.
- Compute neighborhood only when explicitly requested (`--with-neighborhood`) or deferred into artifacts.

Representative change pattern:
```python
if request.with_neighborhood:
    neighborhood = build_structural_neighborhood(...)
else:
    neighborhood = None
```

Expected impact:
- Largest single reduction; removes full-repo secondary scan from default path.

### 2) Two-phase boundary narrowing: `ripgrep -> ast-grep -> deep enrichment`
- Phase A: `rg -l` to get candidate files fast.
- Phase B: run ast-grep rules only on candidate files to collect structural anchors.
- Phase C: run deep Python/native/tree-sitter enrichment only for selected anchors (object representatives first).

Representative pipeline:
```python
candidate_files = rg_files(pattern)
anchors = ast_grep_collect(candidate_files, query)
selected = prioritize_object_representatives(anchors)
enriched = enrich_selected(selected)
```

Expected impact:
- Prevents deep analysis on low-value occurrences.

### 3) Stop full-repo file tabulation for search-local operations
- Reuse candidate file set whenever feasible instead of re-tabulating full language scope.
- For neighborhood-on-demand, bound scan roots to target file subtree or explicit scope.

## P0: Optimize Path/Scope Hot Loops

### 4) Replace Path-heavy containment checks with normalized string-prefix checks
- Precompute normalized repo root once.
- Convert tracked paths once to normalized POSIX strings.
- Replace repeated `Path(...).relative_to(...)` in `_path_is_under`/`_filter_tracked_to_scope` with string prefix checks where safe.

Representative pattern:
```python
def is_under(rel_path: str, scope_prefix: str) -> bool:
    return rel_path == scope_prefix or rel_path.startswith(scope_prefix + "/")
```

Expected impact:
- Major CPU reduction in `tabulate_files` and scope filtering.

### 5) Eliminate repeated `resolve()` in tight loops
- Cache `resolved_root` and resolved scope roots once.
- Avoid calling `root.resolve()` inside helper methods per file.

## P1: Improve Deep Enrichment Cost Model

### 6) Build per-file AST byte-span index once; query anchors by interval
Current issue:
- `_find_ast_anchor` performs full AST scan per anchor.

Recommendation:
- On session initialization, build a per-file sorted interval index of AST node byte spans.
- Resolve anchor node via interval lookup (`O(log n)`/small local scan) instead of full walk.

Representative session extension:
```python
class PythonAnalysisSession(...):
    ast_span_index: AstSpanIndex | None = None

    def ensure_ast_span_index(self) -> AstSpanIndex:
        if self.ast_span_index is None:
            self.ast_span_index = build_ast_span_index(self.ensure_ast(), self.source_bytes)
        return self.ast_span_index
```

Expected impact:
- Reduces native-resolution cost on multi-match files.

### 7) Representative-first enrichment budget
- Resolve object groups first from cheap signals.
- Run expensive enrichment on representative anchor per object.
- Backfill additional occurrences only when required by output mode.

## P1: Parallelization Improvements

### 8) Parallelize cache miss enrichment by file partition (not only classification)
- Keep per-file session affinity in worker process.
- Execute miss enrichment in parallel batches with bounded worker count.

### 9) Concurrency-safe semantic prefetch budgeting
- Keep semantic enrichment optional and capped.
- Run semantic prefetch in parallel with deterministic timeout budget per query.

## P2: msgspec-First Data Path Hardening

### 10) Use msgspec structs end-to-end in hot paths
- Avoid dict round-trips for internal payload transport.
- Replace repeated `contract_to_builtins` in hot loops with typed structs until final boundary.

### 11) Persist cache payloads as msgpack bytes for high-frequency entries
- Use `msgspec.msgpack.Encoder/Decoder` instances reused at module scope.
- Reduce Python object churn for per-anchor cache get/set.

Representative pattern:
```python
ENC = msgspec.msgpack.Encoder()
DEC = msgspec.msgpack.Decoder(type=SearchEnrichmentAnchorCacheV1)
blob = ENC.encode(payload)
obj = DEC.decode(blob)
```

### 12) Consider `array_like=True` for dense cache structs where schema stability is controlled
- Apply only to internal cache contracts with strict append-only evolution discipline.

## Implementation Sequence

1. Gate structural neighborhood out of default `search` path.
2. Rework search assembly to consume candidate-file scoped inputs (no extra full scan).
3. Optimize `tools/cq/index/files.py` path operations (`_filter_tracked_to_scope`, `_path_is_under`, resolve usage).
4. Add per-file AST span index in `tools/cq/search/python_analysis_session.py` and switch native resolution lookup to indexed path.
5. Introduce representative-first enrichment budget in `tools/cq/search/smart_search.py` and object resolver integration.
6. Parallelize enrichment misses by file partitions in `tools/cq/search/partition_pipeline.py`.
7. Apply msgspec msgpack codecs for hot cache namespaces (`search_enrichment`, `neighborhood_fragment`, `semantic_front_door`).
8. Add performance telemetry contracts and command-level stage timing summary for each major phase.

## Target Files (primary)
- `tools/cq/search/smart_search.py`
- `tools/cq/neighborhood/scan_snapshot.py`
- `tools/cq/neighborhood/cache_pipeline.py`
- `tools/cq/query/sg_parser.py`
- `tools/cq/index/files.py`
- `tools/cq/search/python_analysis_session.py`
- `tools/cq/search/python_native_resolution.py`
- `tools/cq/search/partition_pipeline.py`
- `tools/cq/core/cache/diskcache_backend.py`
- `tools/cq/core/cache/fragment_codecs.py`
- `tools/cq/core/cache/contracts.py`

## Success Criteria
- `./cq search stable_id` cold path: <= 8s target, <= 12s max.
- Warm path: <= 3s target.
- No regression in result correctness for object grouping, occurrences, and artifacts.
- Performance telemetry emitted for:
  - candidate collection
  - structural prefilter
  - enrichment
  - neighborhood (if enabled)
  - render/artifact persistence

## Notes
- This plan intentionally prioritizes performance-critical architectural shifts over compatibility preservation in line with current design-phase direction.
