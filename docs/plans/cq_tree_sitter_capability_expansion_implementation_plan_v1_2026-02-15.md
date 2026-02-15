# CQ Tree-sitter Capability Expansion Implementation Plan v1 (2026-02-15)

## Scope
Implement a design-phase hard cutover that upgrades `tools/cq` from partial tree-sitter usage to a tree-sitter-primary structural intelligence pipeline focused on:
- faster targeted analysis,
- stronger correctness under syntax edge cases,
- richer object-level facts and grounding,
- better artifact quality for programming LLM workflows.

This plan is intentionally aggressive: no compatibility shims and no legacy dual-path behavior retained after migration.

## Design Principles
1. Query-targeted only: no whole-repo scan/caching as a default search behavior.
2. Request-scoped caching only: reuse work only inside one request unless validity is fingerprint-proven.
3. Tree-sitter is the primary structural plane; Python native AST/symtable remains semantic augmentation.
4. Object-resolved output is canonical: one object summary, many grounded occurrences.
5. Every occurrence must include precise location and containing block boundaries.
6. Use diskcache native coordination/data structures (`Lock`, `RLock`, `BoundedSemaphore`, `Deque`, `Index`, `memoize_stampede`, `transact`, `touch`) before bespoke implementations.
7. Cache identity is deterministic and fingerprint-based (`file_hash`, grammar/query-pack digests, scope digest); UUIDs are never part of cache keys.
8. UUIDv7 is for sortable run/artifact correlation only; cache correctness never depends on UUID ordering.

## Current Baseline (Observed)
- CQ uses `QueryCursor` bounds (`match_limit`, `set_max_start_depth`) and byte windows.
- CQ does not yet use incremental parse (`Tree.edit` + `old_tree` + `changed_ranges`) in `tools/cq` lanes.
- Query metadata (`#set!` -> `pattern_settings`) is largely unused.
- Neighborhood call graph edges still include heuristic text matching.
- Injection output is planning-only; embedded language parse pass is not executed.
- Single-flight locking in semantic enrichment still relies on `cache.add(lock_key, ...)` + polling rather than `diskcache.Lock`.
- Tree-sitter lane caches are still process-local dictionaries and counters instead of diskcache-native coordinated stores.
- Search artifact index persistence currently stores builtins payloads under single keys rather than `msgspec` bytes plus diskcache persistent `Deque`/`Index`.
- UUID usage in CQ is currently v7-only helper calls; `uuid6` advanced capabilities are not yet formalized for correlation normalization.

---

## S1. Request-Scoped Incremental Parse Sessions
### Goal
Replace full reparse-per-call behavior with request-scoped incremental sessions and changed-range invalidation.

### Representative Code Snippet
```python
# tools/cq/search/tree_sitter_parse_session.py
from tree_sitter import Parser, Point, Tree

class ParseSession:
    def __init__(self, parser: Parser) -> None:
        self._parser = parser
        self._trees: dict[str, tuple[Tree, bytes]] = {}

    def parse(self, file_key: str, source_bytes: bytes) -> tuple[Tree | None, tuple[object, ...], bool]:
        cached = self._trees.get(file_key)
        if cached is None:
            tree = self._parser.parse(source_bytes)
            if tree is None:
                return None, (), False
            self._trees[file_key] = (tree, source_bytes)
            return tree, (), False

        old_tree, old_bytes = cached
        edit = compute_input_edit(old_bytes, source_bytes)  # bytes + Point deltas
        if edit is not None:
            old_tree.edit(
                edit.start_byte,
                edit.old_end_byte,
                edit.new_end_byte,
                Point(*edit.start_point),
                Point(*edit.old_end_point),
                Point(*edit.new_end_point),
            )
        new_tree = self._parser.parse(source_bytes, old_tree=old_tree)
        if new_tree is None:
            return None, (), True
        changed = tuple(old_tree.changed_ranges(new_tree))
        self._trees[file_key] = (new_tree, source_bytes)
        return new_tree, changed, True
```

### Files to Edit
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/search/smart_search.py`

### New Files to Create
- `tools/cq/search/tree_sitter_parse_session.py`
- `tools/cq/search/tree_sitter_parse_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_parse_session.py`

### Legacy Decommission/Delete Scope
- Delete module-level parse caches duplicated per lane:
- `tools/cq/search/tree_sitter_python.py` `_TREE_CACHE`, `_get_tree`
- `tools/cq/search/tree_sitter_rust.py` `_TREE_CACHE`, `_get_tree`

---

## S2. Cooperative Cancellation and Runtime Budgets
### Goal
Wire `progress_callback` and explicit query/parse budgets through CQ runtime so heavy queries fail fast and deterministically.

### Representative Code Snippet
```python
# tools/cq/search/tree_sitter_runtime.py
import time


def _deadline_progress(deadline_s: float):
    def _cb(_: object) -> bool:
        return time.monotonic() < deadline_s
    return _cb


def run_matches_with_budget(query, root, window, *, match_limit: int, budget_ms: int):
    cursor = QueryCursor(query, match_limit=match_limit)
    cursor.set_byte_range(window.start_byte, window.end_byte)
    deadline = time.monotonic() + (budget_ms / 1000.0)
    matches = cursor.matches(root, progress_callback=_deadline_progress(deadline))
    return matches, bool(cursor.did_exceed_match_limit)
```

### Files to Edit
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/tree_sitter_runtime_contracts.py`
- `tools/cq/search/smart_search.py`

### New Files to Create
- `tests/unit/cq/search/test_tree_sitter_runtime_budgets.py`

### Legacy Decommission/Delete Scope
- Delete no-op callback paths:
- `tools/cq/search/tree_sitter_runtime.py` `_ = progress_callback` code paths

---

## S3. Point-Range Execution for Line/Column Anchors
### Goal
Add first-class point-range windowing (`set_point_range`) for line/column anchored operations and neighborhood slicing.

### Representative Code Snippet
```python
# tools/cq/search/tree_sitter_runtime_contracts.py
class QueryPointWindowV1(CqStruct, frozen=True):
    start_row: int
    start_col: int
    end_row: int
    end_col: int


# tools/cq/search/tree_sitter_runtime.py
if point_window is not None:
    cursor.set_point_range(
        (point_window.start_row, point_window.start_col),
        (point_window.end_row, point_window.end_col),
    )
else:
    cursor.set_byte_range(window.start_byte, window.end_byte)
```

### Files to Edit
- `tools/cq/search/tree_sitter_runtime_contracts.py`
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`

### New Files to Create
- `tests/unit/cq/search/test_tree_sitter_point_windowing.py`

### Legacy Decommission/Delete Scope
- Delete byte-only assumptions for all line/column anchored neighborhood operations.

---

## S4. Containment-Correct Window Semantics
### Goal
Guarantee incremental correctness by enforcing containment semantics for match acceptance.

### Representative Code Snippet
```python
# py-tree-sitter 0.25.2 does not expose set_containing_byte_range;
# emulate containment with post-filtering for now.
def _is_fully_contained(nodes: list[Node], start_byte: int, end_byte: int) -> bool:
    return all(start_byte <= n.start_byte and n.end_byte <= end_byte for n in nodes)


for pattern_idx, capture_map in cursor.matches(root):
    captured_nodes = [n for values in capture_map.values() for n in values]
    if not _is_fully_contained(captured_nodes, window.start_byte, window.end_byte):
        continue
    out.append((pattern_idx, capture_map))
```

### Files to Edit
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/tree_sitter_runtime_contracts.py`
- `tools/cq/search/tree_sitter_query_contracts.py`

### New Files to Create
- `tests/unit/cq/search/test_tree_sitter_containment_window.py`

### Legacy Decommission/Delete Scope
- Delete any remaining logic that treats intersecting matches as valid for changed-range reindex.

---

## S5. Query Metadata Contracts via `#set!` and `pattern_settings`
### Goal
Move capture interpretation from hard-coded Python/Rust logic into query-pack metadata contracts.

### Representative Code Snippet
```python
# query pack example
# (call_expression function: (_) @call.target)
# (#set! cq.emit "calls")
# (#set! cq.kind "call")
# (#set! cq.anchor "call.target")
# (#set! cq.slot.callee "call.target")

for pattern_idx, capture_map in matches:
    settings = query.pattern_settings(pattern_idx)
    emit = settings.get("cq.emit")
    kind = settings.get("cq.kind")
    anchor_name = settings.get("cq.anchor")
    slot_callee = settings.get("cq.slot.callee")
    if emit == "calls" and slot_callee is not None:
        node = first_capture(capture_map, slot_callee)
        rows.append({"kind": kind, "callee": node_text(node) if node else None})
```

### Files to Edit
- `tools/cq/search/tree_sitter_python_facts.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_query_contracts.py`
- `tools/cq/search/query_pack_lint.py`
- `tools/cq/search/queries/python/*.scm`
- `tools/cq/search/queries/rust/*.scm`

### New Files to Create
- `tools/cq/search/tree_sitter_pack_metadata.py`
- `tests/unit/cq/search/test_tree_sitter_pattern_settings.py`

### Legacy Decommission/Delete Scope
- Delete hard-coded capture-name mapping tables in:
- `tools/cq/search/tree_sitter_python_facts.py` (`_extract_fact_lists` capture groups)
- `tools/cq/search/tree_sitter_rust.py` (`_capture_texts_from_captures` name bundles)

---

## S6. Activate `contracts.yaml` Pack Governance
### Goal
Make `contracts.yaml` enforceable at lint time and runtime load time (rooted/non-local/windowable/schema requirements).

### Representative Code Snippet
```python
class QueryPackRulesV1(msgspec.Struct, frozen=True):
    require_rooted: bool = True
    forbid_non_local: bool = True


rules = load_pack_rules(language="python")
for i in range(query.pattern_count):
    if rules.require_rooted and not query.is_pattern_rooted(i):
        fail(f"{pack_name}: pattern {i} not rooted")
    if rules.forbid_non_local and query.is_pattern_non_local(i):
        fail(f"{pack_name}: pattern {i} non-local")
```

### Files to Edit
- `tools/cq/search/query_pack_lint.py`
- `tools/cq/search/tree_sitter_query_registry.py`
- `tools/cq/search/tree_sitter_query_contracts.py`

### New Files to Create
- `tools/cq/search/tree_sitter_pack_contracts.py`
- `tests/unit/cq/search/test_query_pack_contracts.py`

### Legacy Decommission/Delete Scope
- Delete duplicated Python-only lint entrypoint:
- `tools/cq/search/tree_sitter_python.py` `lint_python_query_packs`

---

## S7. `matches()`-First Object Evidence Rows
### Goal
Use `QueryCursor.matches()` as the canonical extraction primitive so multi-capture relationships are preserved and object resolution quality improves.

### Representative Code Snippet
```python
for pattern_idx, capture_map in run_bounded_query_matches(...):
    settings = query.pattern_settings(pattern_idx)
    anchor_capture = settings.get("cq.anchor")
    anchor = first_capture(capture_map, anchor_capture)
    if anchor is None:
        continue
    evidence_rows.append(
        ObjectEvidenceRowV1(
            emit=str(settings.get("cq.emit", "unknown")),
            kind=str(settings.get("cq.kind", "unknown")),
            anchor_start_byte=int(anchor.start_byte),
            anchor_end_byte=int(anchor.end_byte),
            captures=compact_capture_payload(capture_map),
        )
    )
```

### Files to Edit
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/tree_sitter_python_facts.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/object_resolver.py`
- `tools/cq/search/object_fact_attachment.py`

### New Files to Create
- `tools/cq/search/tree_sitter_match_rows.py`
- `tools/cq/search/tree_sitter_match_row_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_match_rows.py`

### Legacy Decommission/Delete Scope
- Delete capture-only row construction utilities that lose pattern-level relationships.

---

## S8. Query-Driven Neighborhood Assembly (Remove Heuristic Call Matching)
### Goal
Replace substring-based caller/callee heuristics with query-pack derived neighborhood edges.

### Representative Code Snippet
```python
# tools/cq/neighborhood/queries/python/calls.scm
# (call function: (identifier) @call.callee) @call.site

for pattern_idx, capture_map in neighborhood_matches:
    site = first_capture(capture_map, "call.site")
    callee = first_capture(capture_map, "call.callee")
    if site is None or callee is None:
        continue
    edges.append(build_call_edge(site=site, callee=callee, source_bytes=source_bytes))
```

### Files to Edit
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/neighborhood/tree_sitter_contracts.py`
- `tools/cq/search/smart_search.py`

### New Files to Create
- `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py`
- `tools/cq/neighborhood/queries/python/00_anchor.scm`
- `tools/cq/neighborhood/queries/python/10_calls.scm`
- `tools/cq/neighborhood/queries/python/20_structure.scm`
- `tools/cq/neighborhood/queries/rust/00_anchor.scm`
- `tools/cq/neighborhood/queries/rust/10_calls.scm`
- `tools/cq/neighborhood/queries/rust/20_structure.scm`
- `tests/unit/cq/neighborhood/test_tree_sitter_neighborhood_queries.py`

### Legacy Decommission/Delete Scope
- Delete heuristic caller/callee path in `tools/cq/neighborhood/tree_sitter_collector.py`:
- `_collect_callers_callees` string inclusion (`anchor_name in target_text`)

---

## S9. TreeCursor Structural Export Plane
### Goal
Add a deterministic structural export (nodes/edges/field labels) for high-fidelity grounding and debug artifacts.

### Representative Code Snippet
```python
cursor = root.walk()
child_indices: list[int] = []
while True:
    node = cursor.node
    if node is None:
        break
    rows.nodes.append(node_row(node=node, field_name=cursor.field_name, child_index=child_indices[-1] if child_indices else None))
    if cursor.goto_first_child():
        child_indices.append(0)
        continue
    while True:
        if cursor.goto_next_sibling():
            if child_indices:
                child_indices[-1] += 1
            break
        if not cursor.goto_parent():
            return rows
        if child_indices:
            child_indices.pop()
```

### Files to Edit
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/search/tree_sitter_artifact_contracts.py`
- `tools/cq/core/artifacts.py`

### New Files to Create
- `tools/cq/search/tree_sitter_structural_export.py`
- `tools/cq/search/tree_sitter_structural_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_structural_export.py`

### Legacy Decommission/Delete Scope
- Delete ad-hoc structural relationship assembly that does not preserve field labels.

---

## S10. Diagnostics Plane: `ERROR` + `MISSING` + Degrade Events
### Goal
Emit first-class diagnostics rows (with spans and context) instead of generic warnings and opaque degrade reasons.

### Representative Code Snippet
```python
diag_query = Query(language, "(ERROR) @diag.error (MISSING) @diag.missing")
for _, capture_map in run_bounded_query_matches(diag_query, root, windows=windows, settings=settings)[0]:
    for cap_name, nodes in capture_map.items():
        for node in nodes:
            diagnostics.append(
                TreeSitterDiagnosticV1(
                    kind="ERROR" if cap_name == "diag.error" else "MISSING",
                    start_byte=int(node.start_byte),
                    end_byte=int(node.end_byte),
                    start_line=int(node.start_point.row) + 1,
                    start_col=int(node.start_point.column),
                    message=f"tree-sitter {cap_name}",
                )
            )
```

### Files to Edit
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/semantic_planes_static.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`

### New Files to Create
- `tools/cq/search/tree_sitter_diagnostics.py`
- `tools/cq/search/tree_sitter_diagnostics_contracts.py`
- `tools/cq/search/queries/python/95_diagnostics.scm`
- `tools/cq/search/queries/rust/95_diagnostics.scm`
- `tests/unit/cq/search/test_tree_sitter_diagnostics.py`

### Legacy Decommission/Delete Scope
- Delete generic parse warning-only pathway:
- `tools/cq/neighborhood/tree_sitter_collector.py` `_tree_parse_diagnostics`

---

## S11. Execute Injection Plans with `included_ranges`
### Goal
Promote injections from metadata output to executable embedded-language extraction with host-byte coordinate preservation.

### Representative Code Snippet
```python
from tree_sitter import Parser, Range, Point


def parse_injected(host_bytes: bytes, lang, plan_rows: list[InjectionPlanV1]):
    parser = Parser(lang)
    ranges = [
        Range(
            Point(row.start_row, row.start_col),
            Point(row.end_row, row.end_col),
            row.start_byte,
            row.end_byte,
        )
        for row in plan_rows
    ]
    parser.included_ranges = ranges
    return parser.parse(host_bytes)
```

### Files to Edit
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_injections.py`
- `tools/cq/search/tree_sitter_rust_bundle.py`
- `tools/cq/search/semantic_planes_static.py`

### New Files to Create
- `tools/cq/search/tree_sitter_injection_runtime.py`
- `tools/cq/search/tree_sitter_injection_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_injection_runtime.py`

### Legacy Decommission/Delete Scope
- Delete plan-only behavior that never executes embedded parsing.
- Delete positional language/content pairing assumptions based on capture index.

---

## S12. Parse-State Recovery Hints (`next_state`, `lookahead_iterator`)
### Goal
Provide syntax recovery and completion hints near parse failures for better diagnostics and LLM guidance.

### Representative Code Snippet
```python
# choose node near failure
state_after = language.next_state(node.parse_state, node.grammar_id)
it = language.lookahead_iterator(state_after)
expected: list[str] = []
for sym_id in it:
    sym_name = language.node_kind_for_id(sym_id)
    if sym_name is not None:
        expected.append(sym_name)
        if len(expected) >= 12:
            break
```

### Files to Edit
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_diagnostics.py`
- `tools/cq/search/semantic_planes_static.py`

### New Files to Create
- `tools/cq/search/tree_sitter_recovery_hints.py`
- `tests/unit/cq/search/test_tree_sitter_recovery_hints.py`

### Legacy Decommission/Delete Scope
- Delete degrade paths that emit only opaque `degrade_reason` without syntactic expectation hints.

---

## S13. Coordinate-Rich Occurrence and Block Grounding
### Goal
Standardize occurrence output to include precise line location plus containing code-block start/end lines; move full snippet content to artifacts.

### Representative Code Snippet
```python
class OccurrenceV1(msgspec.Struct, frozen=True):
    file_path: str
    line: int
    col: int
    block_start_line: int
    block_end_line: int
    byte_start: int
    byte_end: int


def to_occurrence(node: Node, block: Node, file_path: str) -> OccurrenceV1:
    return OccurrenceV1(
        file_path=file_path,
        line=int(node.start_point.row) + 1,
        col=int(node.start_point.column),
        block_start_line=int(block.start_point.row) + 1,
        block_end_line=int(block.end_point.row) + 1,
        byte_start=int(node.start_byte),
        byte_end=int(node.end_byte),
    )
```

### Files to Edit
- `tools/cq/search/smart_search.py`
- `tools/cq/search/object_sections.py`
- `tools/cq/search/object_resolution_contracts.py`
- `tools/cq/search/tree_sitter_artifact_contracts.py`
- `tools/cq/core/artifacts.py`

### New Files to Create
- `tools/cq/search/object_occurrence_contracts.py`
- `tests/unit/cq/search/test_object_occurrence_grounding.py`
- `tests/e2e/cq/test_search_occurrence_grounding.py`

### Legacy Decommission/Delete Scope
- Delete terminal full-snippet rendering in search result listings.
- Delete terminal rendering of repeated `N/A - not applicable` fields.

---

## S14. DiskCache-Native Coordination for Tree-Sitter Lanes
### Goal
Replace bespoke lock/poll logic with diskcache-native concurrency primitives so tree-sitter parse/query work is bounded, single-flight protected, and cross-process safe.

### Representative Code Snippet
```python
# tools/cq/core/cache/coordination.py
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from diskcache import BoundedSemaphore, Lock, RLock, barrier
from diskcache import FanoutCache


@contextmanager
def tree_sitter_lane_guard(
    cache: FanoutCache,
    *,
    lock_key: str,
    semaphore_key: str,
    lane_limit: int,
    ttl_seconds: int,
) -> Iterator[None]:
    # Built-in diskcache primitives, no bespoke spin/sleep loop.
    with BoundedSemaphore(cache, semaphore_key, value=lane_limit, expire=ttl_seconds):
        with Lock(cache, lock_key, expire=ttl_seconds):
            # Allow safe nested calls within one process/request path.
            with RLock(cache, f"{lock_key}:reentrant", expire=ttl_seconds):
                yield


def publish_once(cache: FanoutCache, publish_fn: Callable[[], None]) -> None:
    # Serialize one publish phase per barrier key.
    gated_publish = barrier(cache, "cq:tree_sitter:publish")(publish_fn)
    gated_publish()
```

```python
# tools/cq/search/language_front_door_pipeline.py
with tree_sitter_lane_guard(
    fanout_cache,
    lock_key=context.lock_key,
    semaphore_key="cq:tree_sitter:lanes",
    lane_limit=policy.max_tree_sitter_lanes,
    ttl_seconds=lock_ttl_seconds,
):
    cached = fanout_cache.get(context.cache_key, default=None, retry=True)
    if cached is not None:
        return decode_cached(cached)
    payload = compute_tree_sitter_payload(...)
    fanout_cache.set(context.cache_key, payload, tag=write_tag, expire=ttl_seconds, retry=True)
```

### Files to Edit
- `tools/cq/core/cache/interface.py`
- `tools/cq/core/cache/diskcache_backend.py`
- `tools/cq/search/language_front_door_pipeline.py`
- `tools/cq/search/tree_sitter_parallel.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`

### New Files to Create
- `tools/cq/core/cache/coordination.py`
- `tools/cq/core/cache/coordination_contracts.py`
- `tests/unit/cq/core/test_cache_coordination.py`
- `tests/unit/cq/search/test_tree_sitter_single_flight_diskcache.py`

### Legacy Decommission/Delete Scope
- Delete `cache.add(lock_key, ...)` + retry sleep loops in semantic enrichment paths.
- Delete ad-hoc in-module lock state used to emulate cross-process single-flight.
- Delete unbounded tree-sitter lane execution paths that bypass coordination semaphores.

---

## S15. Deterministic DiskCache Stores and Artifact Retrieval Plane
### Goal
Standardize tree-sitter and search artifact persistence around deterministic keys/tags, msgspec binary payloads, atomic publish, and diskcache persistent structures (`Deque`, `Index`).

### Representative Code Snippet
```python
# tools/cq/core/artifacts.py
from pathlib import Path

import msgspec
from diskcache import Deque, FanoutCache, Index

encoder = msgspec.msgpack.Encoder()
decoder = msgspec.msgpack.Decoder(type=SearchArtifactBundleV2)


def persist_bundle(
    cache: FanoutCache,
    *,
    cache_dir: Path,
    run_id: str,
    bundle: SearchArtifactBundleV2,
    file_hash: str,
    grammar_hash: str,
    query_pack_hash: str,
    scope_hash: str,
    ttl_seconds: int,
) -> None:
    cache_key = build_cache_key(
        "search_artifacts",
        version="v2",
        workspace=bundle.workspace,
        language=bundle.language,
        target=bundle.target_file,
        extras={
            "run_id": run_id,
            "file_hash": file_hash,
            "grammar_hash": grammar_hash,
            "query_pack_hash": query_pack_hash,
            "scope_hash": scope_hash,
        },
    )
    tag = (
        f"ns:search_artifacts|lang:{bundle.language}|run:{run_id}"
        f"|grammar:{grammar_hash}|pack:{query_pack_hash}|scope:{scope_hash}"
    )

    # Atomic multi-key publish.
    with cache.transact(retry=True):
        cache.set(cache_key, encoder.encode(bundle), expire=ttl_seconds, tag=tag, retry=True)

    # Built-in persistent structures for retrieval index.
    run_order = Deque(str(cache_dir / "search_artifact_runs"), maxlen=1000)
    run_index = Index(str(cache_dir / "search_artifact_index"))
    run_order.appendleft(run_id)
    run_index[run_id] = {"cache_key": cache_key, "created_ms": bundle.created_ms}


def load_bundle(cache: FanoutCache, *, cache_key: str, ttl_seconds: int) -> SearchArtifactBundleV2 | None:
    value, expire_time, stored_tag = cache.get(
        cache_key,
        default=(None, None, None),
        retry=True,
        expire_time=True,
        tag=True,
    )
    _ = (expire_time, stored_tag)
    if not isinstance(value, (bytes, bytearray, memoryview)):
        return None
    cache.touch(cache_key, expire=ttl_seconds, retry=True)
    return decoder.decode(value)
```

```python
# tools/cq/core/artifacts.py
# Large snippet storage without bespoke file-copy logic.
with snippet_path.open("rb") as handle:
    cache.set(snippet_cache_key, handle, read=True, expire=ttl_seconds, tag=tag, retry=True)
```

### Files to Edit
- `tools/cq/core/cache/key_builder.py`
- `tools/cq/core/cache/contracts.py`
- `tools/cq/core/cache/fragment_codecs.py`
- `tools/cq/core/artifacts.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_python_facts.py`
- `tools/cq/search/smart_search.py`

### New Files to Create
- `tools/cq/core/cache/tree_sitter_cache_store.py`
- `tools/cq/core/cache/tree_sitter_cache_store_contracts.py`
- `tools/cq/core/cache/search_artifact_store.py`
- `tests/unit/cq/core/test_tree_sitter_cache_store.py`
- `tests/unit/cq/core/test_search_artifact_store.py`
- `tests/e2e/cq/test_search_artifact_cache_interface.py`

### Legacy Decommission/Delete Scope
- Delete builtins/dict-based search artifact bundle persistence in cache paths.
- Delete single-key list index mutation paths for artifact history.
- Delete bespoke "recent artifacts" bookkeeping once `Deque`/`Index` store is authoritative.

---

## S16. Stampede Control and Cache Maintenance Control Plane
### Goal
Use diskcache built-ins for deterministic memoization and operational maintenance instead of bespoke cache wrappers and manual maintenance logic.

### Representative Code Snippet
```python
# tools/cq/search/tree_sitter_query_registry.py
from diskcache import FanoutCache, memoize_stampede


def register_cached_loaders(cache: FanoutCache):
    @memoize_stampede(cache, expire=300, tag="ns:tree_sitter|kind:query_pack_load")
    def load_query_pack(language: str, query_pack_hash: str) -> tuple[QueryPackSourceV1, ...]:
        # Deterministic function: safe to stampede-protect.
        return _load_query_pack_sources(language=language, query_pack_hash=query_pack_hash)

    return load_query_pack
```

```python
# tools/cq/core/cache/maintenance.py
def maintenance_tick(cache: FanoutCache) -> dict[str, int]:
    hits, misses = cache.stats(enable=True, reset=False)
    expired_removed = int(cache.expire(retry=True))
    culled_removed = int(cache.cull(retry=True))
    integrity_errors = int(cache.check(fix=False, retry=True))
    if integrity_errors > 0:
        cache.check(fix=True, retry=True)
    return {
        "hits": int(hits),
        "misses": int(misses),
        "expired_removed": expired_removed,
        "culled_removed": culled_removed,
        "integrity_errors": integrity_errors,
    }
```

### Files to Edit
- `tools/cq/search/tree_sitter_query_registry.py`
- `tools/cq/search/query_pack_lint.py`
- `tools/cq/search/tree_sitter_rust_bundle.py`
- `tools/cq/core/cache/diskcache_backend.py`
- `tools/cq/core/cache/policy.py`
- `tools/cq/search/smart_search.py`

### New Files to Create
- `tools/cq/core/cache/maintenance.py`
- `tools/cq/core/cache/maintenance_contracts.py`
- `tests/unit/cq/core/test_cache_maintenance.py`
- `tests/unit/cq/search/test_tree_sitter_query_pack_memoize.py`

### Legacy Decommission/Delete Scope
- Delete ad-hoc tree-sitter query-pack memoization that does not use `memoize_stampede`.
- Delete bespoke cache maintenance loops where diskcache `expire`, `cull`, and `check` become authoritative.
- Delete tree-sitter cache telemetry counters that duplicate diskcache stats in runtime reporting.

---

## S17. UUIDv7/uuid6 Correlation Identity Contract
### Goal
Formalize UUID usage so tree-sitter/search pipelines use sortable, library-standard identifiers for run/artifact correlation while keeping cache keys deterministic and fingerprint-based.

### Representative Code Snippet
```python
# tools/cq/utils/uuid_factory.py
import uuid

import uuid6


def new_uuid7() -> uuid.UUID:
    uuid7_fn = getattr(uuid, "uuid7", None)
    if callable(uuid7_fn):
        return uuid7_fn()
    return uuid6.uuid7()


def run_id() -> str:
    return str(new_uuid7())


def artifact_id_hex() -> str:
    return new_uuid7().hex


def normalize_legacy_identity(value: uuid.UUID) -> uuid.UUID:
    # Built-in uuid6 conversion path for legacy v1 ingestion.
    if value.version == 1:
        return uuid6.uuid1_to_uuid6(value)
    return value


def legacy_compatible_event_id(*, node: int | None = None, clock_seq: int | None = None) -> uuid.UUID:
    # Explicit v6 generation only for legacy v1-compatible interoperability paths.
    return uuid6.uuid6(node=node, clock_seq=clock_seq)
```

```python
# tools/cq/core/cache/key_builder.py
# UUID is intentionally excluded from cache identity.
cache_key = build_cache_key(
    namespace="tree_sitter",
    version="v3",
    workspace=workspace,
    language=language,
    target=file_path,
    extras={
        "file_hash": file_hash,
        "grammar_hash": grammar_hash,
        "query_pack_hash": query_pack_hash,
        "scope_hash": scope_hash,
    },
)
```

### Files to Edit
- `tools/cq/utils/uuid_factory.py`
- `src/utils/uuid_factory.py`
- `tools/cq/core/run_context.py`
- `tools/cq/core/artifacts.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/core/cache/key_builder.py`

### New Files to Create
- `tools/cq/core/uuid_contracts.py`
- `tests/unit/cq/utils/test_uuid_factory.py`
- `tests/unit/cq/core/test_uuid_contracts.py`
- `tests/e2e/cq/test_search_run_id_ordering.py`

### Legacy Decommission/Delete Scope
- Delete direct UUID generation outside CQ UUID factory helpers for search/neighborhood/artifact IDs.
- Delete any cache-key extras derived from random or wall-clock UUID material.
- Delete ad-hoc legacy UUID normalization logic once `uuid1_to_uuid6` contract path is centralized.
- Keep `uuid6.uuid8()` out of CQ runtime identity paths unless a dedicated cross-language contract is added.

---

## Cross-Scope Legacy Decommission and Deletion Plan
These deletions rely on multiple scope items landing and should be executed only when the listed dependency sets are complete.

### Batch D1 (after S1, S2, S3, S4)
- Remove lane-specific parser cache code from:
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- Remove byte-only window contracts that cannot express point or containment semantics.

### Batch D2 (after S5, S6, S7)
- Remove hard-coded capture-to-field mappers from:
- `tools/cq/search/tree_sitter_python_facts.py`
- `tools/cq/search/tree_sitter_rust.py`
- Remove duplicated lint surfaces and enforce `contracts.yaml` as canonical pack rules source.

### Batch D3 (after S8, S9, S10)
- Remove heuristic neighborhood call matching from:
- `tools/cq/neighborhood/tree_sitter_collector.py`
- Remove generic parse-warning-only diagnostics paths in neighborhood/search rendering.

### Batch D4 (after S11, S12, S13)
- Remove injection planning code that does not execute embedded parsing.
- Remove output pathways that cannot supply occurrence line + block boundaries.
- Remove any terminal rendering paths that print non-actionable `N/A - not applicable` placeholders.

### Batch D5 (after S14, S15, S16, S17)
- Remove lock/poll single-flight loops and replace with diskcache coordination primitives.
- Remove builtins/dict artifact cache payload pathways and single-key list index mutation logic.
- Remove ad-hoc memoization/maintenance logic where `memoize_stampede`, `expire`, `cull`, and `check` are authoritative.
- Remove direct UUID generation paths outside centralized UUID factories.

## Implementation Sequence
1. S1 parse sessions and changed-range wiring.
2. S2 runtime budgets/cancellation.
3. S3 point-range support.
4. S4 containment-correct window filtering.
5. S5 metadata-driven extraction via `#set!` + `pattern_settings`.
6. S6 contract activation for pack governance.
7. S7 matches-first object evidence rows.
8. S8 neighborhood query-engine cutover.
9. S9 structural export plane.
10. S10 diagnostics plane.
11. S11 injection runtime execution.
12. S12 parse-state recovery hints.
13. S13 coordinate-rich output and artifact grounding.
14. S14 diskcache-native coordination primitives cutover.
15. S15 deterministic diskcache stores and artifact retrieval plane.
16. S16 stampede control and maintenance control plane.
17. S17 UUIDv7/uuid6 correlation identity contract.
18. Execute deletion batches D1-D5 in order.

## Implementation Checklist
- [x] S1: Introduce request-scoped incremental parse session and remove per-lane caches.
- [x] S2: Wire `progress_callback` and query budgets through runtime and telemetry.
- [x] S3: Add `set_point_range` support and contracts for point windows.
- [x] S4: Enforce containment-correct window semantics for incremental re-analysis.
- [x] S5: Add `#set!` metadata to packs and read with `pattern_settings`.
- [x] S6: Load and enforce `contracts.yaml` at lint/runtime pack load.
- [x] S7: Convert extraction to `matches()`-first evidence row model.
- [x] S8: Replace heuristic neighborhood edges with query-driven edges.
- [x] S9: Add TreeCursor structural export nodes/edges with field labels.
- [x] S10: Emit first-class ERROR/MISSING diagnostics with spans and context.
- [x] S11: Execute injection plans using `included_ranges` parser passes.
- [x] S12: Add parse-state recovery hints from `next_state`/`lookahead_iterator`.
- [x] S13: Add occurrence + block grounding contracts and terminal/artifact split.
- [x] S14: Replace lock/poll and unbounded lanes with `Lock`/`RLock`/`BoundedSemaphore`/`barrier` coordination.
- [x] S15: Move tree-sitter/artifact stores to deterministic keys, msgspec bytes, and `Deque`/`Index` retrieval indices.
- [x] S16: Add `memoize_stampede` loaders plus diskcache-native maintenance (`stats`, `expire`, `cull`, `check`).
- [x] S17: Centralize UUID generation/normalization (`uuid7`, `uuid1_to_uuid6`) and keep UUIDs out of cache-key identity.
- [x] D1: Delete legacy parse/cache/window code after S1-S4 pass.
- [x] D2: Delete hard-coded capture mappers and duplicate lint paths after S5-S7 pass.
- [x] D3: Delete heuristic neighborhood diagnostics/matching after S8-S10 pass.
- [x] D4: Delete non-executable injection and weak grounding output paths after S11-S13 pass.
- [x] D5: Delete bespoke coordination/cache-index/memoization/UUID paths after S14-S17 pass.
- [x] Add/refresh CQ-focused tests only under `tests/unit/cq` and `tests/e2e/cq` for each scope item.
- [x] Run formatting, linting, type checks, and CQ-focused pytest only after full scope implementation.
