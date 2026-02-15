# CQ Tree-sitter Unutilized Capabilities Best-in-Class Implementation Plan v1 (2026-02-15)

## Scope Summary
This plan executes a design-phase hard cutover to fully leverage advanced tree-sitter features that are currently underused in `tools/cq`, with explicit goals of faster query-time execution, higher grounding accuracy, stronger robustness under syntax/macro edge cases, and broader high-value artifacts for programming LLM workflows. The implementation stance is aggressive: no compatibility shims, no dual-path legacy behavior, and explicit deletion of superseded logic once each dependent scope lands.

Included scope covers all identified opportunities from the prior review:
- query IR introspection planner,
- query specialization toggles,
- custom predicates,
- predicate pushdown in query packs,
- changed-range selective recomputation,
- end-to-end cancellation/budget propagation,
- parser operational controls,
- streaming parse callbacks,
- language introspection registry,
- node-types code generation,
- CST token output plane,
- richer Rust injection handling,
- Rust macro expansion evidence plane,
- Rust module/import graph resolution,
- grammar drift compatibility governance,
- diskcache-native memoization/throttling/averaging primitives for tree-sitter lanes,
- diskcache named substores and queue primitives for changed-range execution,
- file-backed payload storage and tag-index governance for large tree-sitter artifacts,
- uuid6/uuid7 temporal contracts for stable run/event ordering and legacy UUID normalization.

No scope items are excluded.

## Design Principles
1. Hard cutover only: when a new tree-sitter plane is authoritative, legacy heuristic code is deleted.
2. Query-targeted execution only: no whole-repo parse/index; all work is request-bounded.
3. Predicate pushdown first: filter at query time before Python-side post-processing.
4. Incremental-first semantics: changed-range windows drive selective recomputation.
5. Cancellation is end-to-end: front-door budgets propagate into every query/parse lane.
6. Contracts are msgspec-first for runtime artifacts and cache persistence boundaries.
7. Grammar/version drift is enforced, not advisory.
8. Rust macro/module resolution is evidence-backed, not string-heuristic.
9. Output planes are explicit and typed (events, structural graph, CST tokens, module graph).
10. Every new module has unit coverage; cross-language behavior changes get CQ e2e tests.
11. Prefer built-in `diskcache` primitives (`memoize`, `memoize_stampede`, `throttle`, `Averager`, `push/pull/peek`, named `cache/deque/index`) over bespoke coordination logic.
12. UUID semantics are explicit and versioned: UUIDv7 is default, `.time` is canonical for ordering metadata, and UUIDv1 is normalized through `uuid1_to_uuid6` where legacy identifiers are ingested.

## Current Baseline (Observed)
- Bounded query runtime exists and already supports `QueryCursor` bounds, `set_point_range`, `set_max_start_depth`, match limits, and optional progress callbacks in `tools/cq/search/tree_sitter_runtime.py`.
- Query pack metadata is currently limited to `pattern_settings` and first-capture extraction in `tools/cq/search/tree_sitter_pack_metadata.py` and `tools/cq/search/tree_sitter_match_rows.py`.
- Incremental parse sessions already emit `changed_ranges` via `Tree.edit(...)` and `old_tree.changed_ranges(new_tree)` in `tools/cq/search/tree_sitter_parse_session.py`, but Python/Rust enrichment loops do not use changed-ranges to scope pack execution (`tools/cq/search/tree_sitter_python.py`, `tools/cq/search/tree_sitter_rust.py`).
- Query pack linting currently enforces rooted/non-local checks and schema node/field existence (`tools/cq/search/tree_sitter_query_contracts.py`, `tools/cq/search/query_pack_lint.py`), but does not use advanced query IR planning/specialization.
- Query packs in `tools/cq/search/queries/python/*.scm` and `tools/cq/search/queries/rust/*.scm` currently have no `#eq?`, `#match?`, or `#any-of?` predicates.
- Structural export exists as nodes/edges only in `tools/cq/search/tree_sitter_structural_export.py` and `tools/cq/search/tree_sitter_structural_contracts.py`; there is no dedicated CST token plane.
- Rust injection support exists (`tools/cq/search/queries/rust/60_injections.scm`, `tools/cq/search/tree_sitter_injections.py`, `tools/cq/search/tree_sitter_injection_runtime.py`) but currently uses broad macro token-tree captures and a default language bias.
- Neighborhood caller/callee assembly still includes heuristic fallback traversal when query output is sparse (`tools/cq/neighborhood/tree_sitter_collector.py`).
- Node-types loading exists in `tools/cq/search/tree_sitter_node_schema.py`, but no generated typed schema modules are currently consumed by runtime lanes.
- Existing runtime grep shows no current use of `Query.disable_pattern`, `Query.disable_capture`, `QueryPredicate`, `Parser.reset`, `Parser.print_dot_graphs`, or `injection.combined` within `tools/cq` execution paths.
- `diskcache` usage is strong in core backend and lane coordination (`tools/cq/core/cache/diskcache_backend.py`, `tools/cq/core/cache/coordination.py`), but CQ does not yet use `Cache.memoize`, `throttle`, `Averager`, `Cache.push/pull/peek`, or `FanoutCache.cache/deque/index` for tree-sitter runtime planning and work queues.
- Query-pack loading uses `memoize_stampede` in `tools/cq/search/tree_sitter_query_registry.py`, but planner/introspection outputs and lane-cost adaptation are not memoized with cache-native decorators.
- Artifact storage already uses standalone `Deque`/`Index` in `tools/cq/core/cache/search_artifact_store.py`, but tree-sitter-specific named substores and queue-backed changed-range orchestration are not wired.
- UUID generation currently defaults to UUIDv7 in `tools/cq/utils/uuid_factory.py`, but UUID `.time` is not yet used as canonical event ordering metadata and UUIDv1-to-v6 normalization paths are not integrated into enrichment/runtime contracts.

---

## S1. Query IR Planner and Costed Pack Scheduling
### Goal
Introduce a query IR planner that inspects compiled `Query` objects and produces a cost/selectivity plan used to schedule pack execution order per request and per window.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_query_planner.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class QueryPatternPlanV1(CqStruct, frozen=True):
    pattern_idx: int
    rooted: bool
    non_local: bool
    guaranteed_step0: bool
    start_byte_in_query: int
    end_byte_in_query: int
    assertions: dict[str, tuple[str, bool]]
    capture_quantifiers: tuple[str, ...]
    score: float


def build_pattern_plan(query) -> tuple[QueryPatternPlanV1, ...]:
    plans: list[QueryPatternPlanV1] = []
    for pattern_idx in range(query.pattern_count):
        rooted = bool(query.is_pattern_rooted(pattern_idx))
        non_local = bool(query.is_pattern_non_local(pattern_idx))
        guaranteed = bool(query.is_pattern_guaranteed_at_step(pattern_idx))
        assertions = {
            str(key): (str(value), bool(is_pos))
            for key, (value, is_pos) in query.pattern_assertions(pattern_idx).items()
        }
        quantifiers = tuple(
            str(query.capture_quantifier(pattern_idx, capture_idx))
            for capture_idx in range(query.capture_count)
        )
        score = (2.0 if rooted else -1.0) + (1.0 if guaranteed else 0.0) - (2.5 if non_local else 0.0)
        plans.append(
            QueryPatternPlanV1(
                pattern_idx=pattern_idx,
                rooted=rooted,
                non_local=non_local,
                guaranteed_step0=guaranteed,
                start_byte_in_query=int(query.start_byte_for_pattern(pattern_idx)),
                end_byte_in_query=int(query.end_byte_for_pattern(pattern_idx)),
                assertions=assertions,
                capture_quantifiers=quantifiers,
                score=score,
            )
        )
    return tuple(sorted(plans, key=lambda row: row.score, reverse=True))
```

### Files to Edit
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_query_registry.py`
- `tools/cq/search/tree_sitter_runtime_contracts.py`

### New Files to Create
- `tools/cq/search/tree_sitter_query_planner.py`
- `tools/cq/search/tree_sitter_query_planner_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_query_planner.py`

### Legacy Decommission/Delete Scope
- Delete fixed source-order execution behavior in `tools/cq/search/tree_sitter_python.py` (`_query_sources` consumption in `_capture_gap_fill_fields`).
- Delete fixed source-order execution behavior in `tools/cq/search/tree_sitter_rust.py` (the `_pack_sources()` loop currently used for pack execution).

---

## S2. Query Specialization Profiles via `disable_pattern` / `disable_capture`
### Goal
Add mode-specific query specialization (terminal vs artifact vs diagnostics) by disabling irrelevant patterns/captures at compile-time per request surface.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_query_specialization.py
from __future__ import annotations


def specialize_query(query, *, surface: str):
    # Disable high-cost artifact-only patterns for terminal summary surfaces.
    for pattern_idx in range(query.pattern_count):
        settings = query.pattern_settings(pattern_idx)
        if surface == "terminal" and settings.get("cq.surface") == "artifact_only":
            query.disable_pattern(pattern_idx)

    # Disable verbose payload captures for terminal view.
    for capture_idx in range(query.capture_count):
        capture_name = query.capture_name(capture_idx)
        if surface == "terminal" and capture_name.startswith("payload."):
            query.disable_capture(capture_name)

    return query
```

### Files to Edit
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_match_rows.py`
- `tools/cq/search/smart_search.py`

### New Files to Create
- `tools/cq/search/tree_sitter_query_specialization.py`
- `tools/cq/search/tree_sitter_query_specialization_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_query_specialization.py`

### Legacy Decommission/Delete Scope
- Delete broad unfiltered capture serialization in `tools/cq/search/tree_sitter_match_rows.py::_capture_payload` and replace with specialization-aware capture selection.
- Delete any terminal-surface post-hoc field pruning logic added outside query runtime once specialization is authoritative.

---

## S3. Custom Predicate Runtime (`QueryPredicate`) for Semantic Filtering
### Goal
Support custom tree-sitter predicates in CQ query execution so high-value semantic guards run inside query matching instead of Python post-filter passes.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_custom_predicates.py
from __future__ import annotations

import re


def cq_predicate(name: str, args: list[tuple[str, str]], _pattern_idx: int, captures: dict) -> bool:
    # Callback signature is provided by py-tree-sitter QueryCursor.
    if name == "cq-regex?":
        cap_name, cap_kind = args[0]
        regex_text, regex_kind = args[1]
        if cap_kind != "capture" or regex_kind != "string":
            return False
        nodes = captures.get(cap_name, [])
        if not nodes:
            return False
        text = nodes[0].text.decode("utf-8", errors="replace")
        return re.search(regex_text, text) is not None
    return True

# tools/cq/search/tree_sitter_runtime.py (integration)
matches = cursor.matches(
    root,
    predicate=cq_predicate,
    progress_callback=progress,
)
```

### Files to Edit
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/tree_sitter_runtime_contracts.py`
- `tools/cq/search/tree_sitter_query_contracts.py`
- `tools/cq/search/query_pack_lint.py`

### New Files to Create
- `tools/cq/search/tree_sitter_custom_predicates.py`
- `tools/cq/search/tree_sitter_custom_predicate_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_custom_predicates.py`

### Legacy Decommission/Delete Scope
- Delete string/regex post-filters that duplicate predicate intent in Python after query match materialization.
- Delete ad-hoc per-pack predicate emulation blocks once predicate callback execution is available in runtime lanes.

---

## S4. Predicate-Pushdown Query Pack Upgrade (`#eq?`, `#match?`, `#any-of?`)
### Goal
Refactor Python and Rust `.scm` packs to encode selectivity/precision directly in query predicates, minimizing false positives and downstream filtering cost.

### Representative Code Snippets
```scm
; tools/cq/search/queries/python/20_calls.scm
((call function: (identifier) @call.target.identifier) @call
 (#match? @call.target.identifier "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "call")
 (#set! cq.anchor "call.target.identifier"))

; tools/cq/search/queries/rust/50_modules_imports.scm
((use_declaration argument: (scoped_identifier) @import.path) @import.declaration
 (#match? @import.path "^[A-Za-z_][A-Za-z0-9_:]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration")
 (#set! cq.anchor "import.path"))

((macro_invocation macro: (identifier) @macro.name) @macro.call
 (#any-of? @macro.name "sql" "query" "regex")
 (#set! cq.emit "injections")
 (#set! cq.kind "macro_injection_candidate")
 (#set! cq.anchor "macro.name"))
```

### Files to Edit
- `tools/cq/search/queries/python/10_refs.scm`
- `tools/cq/search/queries/python/20_calls.scm`
- `tools/cq/search/queries/python/30_imports.scm`
- `tools/cq/search/queries/rust/10_refs.scm`
- `tools/cq/search/queries/rust/20_calls.scm`
- `tools/cq/search/queries/rust/50_modules_imports.scm`
- `tools/cq/neighborhood/queries/python/10_calls.scm`
- `tools/cq/neighborhood/queries/rust/10_calls.scm`
- `tools/cq/search/query_pack_lint.py`

### New Files to Create
- `tools/cq/search/queries/python/70_predicate_filters.scm`
- `tools/cq/search/queries/rust/70_predicate_filters.scm`
- `tests/unit/cq/search/test_tree_sitter_query_predicate_packs.py`

### Legacy Decommission/Delete Scope
- Delete broad catch-all identifier pattern blocks in `tools/cq/search/queries/python/10_refs.scm` that are superseded by filtered predicate packs.
- Delete broad catch-all identifier pattern blocks in `tools/cq/search/queries/rust/10_refs.scm` that are superseded by filtered predicate packs.
- Delete `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py::_matches_name` and rely on predicate-driven call matching.

---

## S5. Changed-Range Selective Query and Enrichment Recompute
### Goal
Make changed-range windows the default execution scope for incremental parse updates, so only affected spans are re-queried and re-enriched.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_change_windows.py
from __future__ import annotations

from tools.cq.search.tree_sitter_runtime_contracts import QueryWindowV1


def windows_from_changed_ranges(changed_ranges: tuple[object, ...], *, pad_bytes: int = 64):
    windows: list[QueryWindowV1] = []
    for rng in changed_ranges:
        start = max(0, int(getattr(rng, "start_byte", 0)) - pad_bytes)
        end = int(getattr(rng, "end_byte", start)) + pad_bytes
        if end > start:
            windows.append(QueryWindowV1(start_byte=start, end_byte=end))
    return tuple(windows)

# tools/cq/search/tree_sitter_python.py (integration)
tree, changed_ranges = parse_python_tree_with_ranges(source, cache_key=cache_key)
windows = windows_from_changed_ranges(changed_ranges)
if not windows:
    windows = (QueryWindowV1(start_byte=0, end_byte=len(source_bytes)),)
matches, telemetry = run_bounded_query_matches(query, tree.root_node, windows=windows, settings=settings)
```

### Files to Edit
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_parse_session.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py`

### New Files to Create
- `tools/cq/search/tree_sitter_change_windows.py`
- `tools/cq/search/tree_sitter_change_windows_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_change_windows.py`

### Legacy Decommission/Delete Scope
- Delete unconditional full-span pack iteration in `tools/cq/search/tree_sitter_python.py::_capture_gap_fill_fields`.
- Delete unconditional full-span pack iteration in `tools/cq/search/tree_sitter_rust.py` pack execution loops.
- Delete global-plus-subtree double-scan fallback in `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py` once change-window scoped passes are authoritative.

---

## S6. End-to-End Cancellation and Budget Propagation
### Goal
Thread cancellation and time budgets from Smart Search request context into every tree-sitter parse/query path to bound worst-case latency.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_runtime_contracts.py
class QueryExecutionSettingsV2(CqStruct, frozen=True):
    match_limit: int = 4096
    max_start_depth: int | None = None
    budget_ms: int | None = None
    require_containment: bool = False
    timeout_micros: int | None = None

# tools/cq/search/smart_search.py (integration)
def _deadline_progress(deadline_s: float):
    from time import monotonic
    return lambda _state: monotonic() < deadline_s

deadline = monotonic() + 0.250
query_settings = QueryExecutionSettingsV2(match_limit=2048, budget_ms=250)
matches, telemetry = run_bounded_query_matches(
    query,
    root,
    windows=windows,
    settings=query_settings,
    progress_callback=_deadline_progress(deadline),
)
```

### Files to Edit
- `tools/cq/search/smart_search.py`
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/tree_sitter_runtime_contracts.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`

### New Files to Create
- `tools/cq/search/tree_sitter_budgeting.py`
- `tests/unit/cq/search/test_tree_sitter_budgeting.py`

### Legacy Decommission/Delete Scope
- Delete static `"cancelled": False` runtime payload values in `tools/cq/search/tree_sitter_python.py` query telemetry assembly.
- Delete hard-coded match-limit constants that bypass caller budgets when request-level settings are available.

---

## S7. Parser Operational Hardening (`reset`, `logger`, `print_dot_graphs`)
### Goal
Adopt parser-level operational controls for deterministic recovery and deep parse diagnostics when requested.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_parser_controls.py
from __future__ import annotations

import os


def configure_parser(parser, *, timeout_micros: int | None, dot_fd: int | None):
    if timeout_micros is not None:
        parser.timeout_micros = timeout_micros

    if os.getenv("CQ_TREE_SITTER_TRACE", "0") == "1":
        parser.logger = lambda level, message: print(f"[ts:{level}] {message}")

    if dot_fd is not None:
        parser.print_dot_graphs(dot_fd)


def parse_with_recovery(parser, source, *, old_tree=None):
    try:
        return parser.parse(source, old_tree=old_tree)
    except TimeoutError:
        parser.reset()
        return parser.parse(source)
    finally:
        parser.print_dot_graphs(None)
```

### Files to Edit
- `tools/cq/search/tree_sitter_parse_session.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`

### New Files to Create
- `tools/cq/search/tree_sitter_parser_controls.py`
- `tests/unit/cq/search/test_tree_sitter_parser_controls.py`

### Legacy Decommission/Delete Scope
- Delete direct parser usage in `tools/cq/neighborhood/tree_sitter_collector.py::_parse_tree_for_request` that bypasses shared parser control hooks.
- Delete duplicate ad-hoc parse error wrappers once parser controls provide uniform recovery/error metadata.

---

## S8. Streaming Parse Source Callbacks for Large Inputs
### Goal
Use callback-based parser input for large files to reduce memory churn and support chunked parsing in request-scoped operations.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_stream_source.py
from __future__ import annotations


def make_source_callback(source_bytes: bytes, *, chunk_size: int = 16384):
    size = len(source_bytes)

    def _source(byte_offset: int, _point: object):
        if byte_offset >= size:
            return None
        return source_bytes[byte_offset : min(size, byte_offset + chunk_size)]

    return _source

# tools/cq/search/tree_sitter_parse_session.py (integration)
source_cb = make_source_callback(source_bytes)
new_tree = parser.parse(source_cb, old_tree=old_tree, encoding="utf8")
```

### Files to Edit
- `tools/cq/search/tree_sitter_parse_session.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`

### New Files to Create
- `tools/cq/search/tree_sitter_stream_source.py`
- `tests/unit/cq/search/test_tree_sitter_stream_source.py`

### Legacy Decommission/Delete Scope
- Delete byte-copy-heavy full-buffer parse callsites that repeatedly allocate encoded source for unchanged windows.
- Delete coarse source-size bailouts where streaming parse can safely handle large inputs under budgets.

---

## S9. Language Introspection Registry (Kinds, Fields, Supertypes)
### Goal
Build a runtime language registry from `Language` introspection APIs to provide version-aware node/field/category metadata shared by linting, planning, and output contracts.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_language_registry.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class LanguageIntrospectionV1(CqStruct, frozen=True):
    language: str
    abi_version: int
    semantic_version: tuple[int, int, int]
    node_kind_count: int
    field_count: int
    named_kinds: tuple[str, ...]
    supertypes: dict[str, tuple[str, ...]]


def build_language_introspection(language_name: str, language_obj) -> LanguageIntrospectionV1:
    named_kinds = tuple(
        language_obj.node_kind_for_id(i)
        for i in range(language_obj.node_kind_count)
        if language_obj.node_kind_is_named(i)
    )
    supertypes: dict[str, tuple[str, ...]] = {}
    for supertype_id in language_obj.supertypes:
        super_name = language_obj.node_kind_for_id(supertype_id)
        sub_names = tuple(language_obj.node_kind_for_id(sub_id) for sub_id in language_obj.subtypes(supertype_id))
        supertypes[super_name] = sub_names
    return LanguageIntrospectionV1(
        language=language_name,
        abi_version=int(language_obj.abi_version),
        semantic_version=tuple(language_obj.semantic_version),
        node_kind_count=int(language_obj.node_kind_count),
        field_count=int(language_obj.field_count),
        named_kinds=tuple(sorted(named_kinds)),
        supertypes=supertypes,
    )
```

### Files to Edit
- `tools/cq/search/query_pack_lint.py`
- `tools/cq/search/tree_sitter_query_contracts.py`
- `tools/cq/search/tree_sitter_node_schema.py`

### New Files to Create
- `tools/cq/search/tree_sitter_language_registry.py`
- `tests/unit/cq/search/test_tree_sitter_language_registry.py`

### Legacy Decommission/Delete Scope
- Delete duplicated language-loader caches in `tools/cq/search/query_pack_lint.py` (`_python_language`, `_rust_language`) in favor of registry-backed resolution.
- Delete scattered direct language metadata reads once centralized registry accessors are in place.

---

## S10. `node-types.json` Code Generation and Typed Schema Modules
### Goal
Generate checked Python modules from grammar `node-types.json` so query/lint/runtime consumers rely on typed constants instead of repeated runtime JSON decoding.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_node_codegen.py
from __future__ import annotations

from pathlib import Path

from tools.cq.search.tree_sitter_node_schema import load_grammar_schema


def generate_node_module(language: str, output_path: Path) -> None:
    schema = load_grammar_schema(language)
    if schema is None:
        raise RuntimeError(f"missing node schema for {language}")

    all_kinds = sorted({row.type for row in schema.node_types})
    named_kinds = sorted({row.type for row in schema.node_types if row.named})
    field_names = sorted({field for row in schema.node_types for field in row.fields})

    output_path.write_text(
        "\n".join(
            [
                f"LANGUAGE = {language!r}",
                f"ALL_NODE_KINDS = frozenset({all_kinds!r})",
                f"NAMED_NODE_KINDS = frozenset({named_kinds!r})",
                f"FIELD_NAMES = frozenset({field_names!r})",
                "",
            ]
        ),
        encoding="utf-8",
    )
```

### Files to Edit
- `tools/cq/search/query_pack_lint.py`
- `tools/cq/search/tree_sitter_query_contracts.py`
- `tools/cq/search/tree_sitter_node_schema.py`
- `pyproject.toml`

### New Files to Create
- `tools/cq/search/tree_sitter_node_codegen.py`
- `tools/cq/search/generated/python_node_types_v1.py`
- `tools/cq/search/generated/rust_node_types_v1.py`
- `tests/unit/cq/search/test_tree_sitter_node_codegen.py`
- `tests/unit/cq/search/test_tree_sitter_generated_node_types.py`

### Legacy Decommission/Delete Scope
- Delete runtime distribution traversal and decode hot paths in `tools/cq/search/tree_sitter_node_schema.py` (`_distribution_node_types_path`, `_decode_node_types`, `_row_to_node_type`) after generated modules are adopted.
- Delete fallback schema loading branches in lint runtime that silently skip enforcement when generated schema modules are available.

---

## S11. CST Token Plane and Expanded Structural ABI
### Goal
Add a first-class CST token output plane and richer structural edge metadata so downstream CQ artifacts support precise block/token grounding and structural reconstruction.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_structural_contracts.py
class TreeSitterTokenV1(CqStruct, frozen=True):
    token_id: str
    node_id: str
    kind: str
    text: str
    start_byte: int
    end_byte: int
    start_line: int
    start_col: int
    end_line: int
    end_col: int

class TreeSitterStructuralEdgeV2(CqStruct, frozen=True):
    edge_id: str
    source_node_id: str
    target_node_id: str
    kind: str = "contains"
    field_name: str | None = None
    field_id: int | None = None

# tools/cq/search/tree_sitter_structural_export.py (token extraction)
if node.child_count == 0:
    token_text = source_bytes[node.start_byte : node.end_byte].decode("utf-8", errors="replace")
    tokens.append(
        TreeSitterTokenV1(
            token_id=f"{file_path}:{node.start_byte}:{node.end_byte}",
            node_id=node_id,
            kind=node.type,
            text=token_text,
            start_byte=node.start_byte,
            end_byte=node.end_byte,
            start_line=node.start_point[0] + 1,
            start_col=node.start_point[1],
            end_line=node.end_point[0] + 1,
            end_col=node.end_point[1],
        )
    )
```

### Files to Edit
- `tools/cq/search/tree_sitter_structural_contracts.py`
- `tools/cq/search/tree_sitter_structural_export.py`
- `tools/cq/neighborhood/tree_sitter_contracts.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/search/tree_sitter_artifact_contracts.py`

### New Files to Create
- `tools/cq/search/tree_sitter_token_export.py`
- `tests/unit/cq/search/test_tree_sitter_token_export.py`
- `tests/unit/cq/neighborhood/test_tree_sitter_structural_tokens.py`

### Legacy Decommission/Delete Scope
- Delete standalone tags-as-structure fallback in `tools/cq/search/tree_sitter_tags.py` once CST tokens and structural edges are the canonical structural artifact plane.
- Delete ad-hoc token reconstruction from raw snippets in neighborhood rendering where token plane provides authoritative values.

---

## S12. Rust Injection Coverage Expansion (`injection.combined`, Profiled Macros)
### Goal
Upgrade Rust injection extraction to use macro-profiled language mapping and combined injections, then parse each injected language lane with bounded included-ranges.

### Representative Code Snippets
```scm
; tools/cq/search/queries/rust/60_injections.scm
((macro_invocation
  macro: (identifier) @inj.macro
  (token_tree) @injection.content)
 (#any-of? @inj.macro "sql" "query" "regex")
 (#set! cq.emit "injections")
 (#set! cq.kind "macro_injection")
 (#set! cq.anchor "injection.content")
 (#set! injection.language "sql")
 (#set! injection.combined))
```

```python
# tools/cq/search/tree_sitter_injection_runtime.py
for language_name, plans_for_lang in group_plans_by_language(plans).items():
    parser = parser_for_language(language_name)
    parser.included_ranges = to_ranges(plans_for_lang)
    injected_tree = parser.parse(source_bytes)
    # emit per-language injection diagnostics/telemetry rows
```

### Files to Edit
- `tools/cq/search/queries/rust/60_injections.scm`
- `tools/cq/search/tree_sitter_injections.py`
- `tools/cq/search/tree_sitter_injection_runtime.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_injection_contracts.py`

### New Files to Create
- `tools/cq/search/tree_sitter_rust_injection_profiles.py`
- `tests/unit/cq/search/test_tree_sitter_rust_injection_profiles.py`
- `tests/unit/cq/search/test_tree_sitter_injection_runtime_multilang.py`
- `tests/e2e/cq/test_rust_injection_enrichment_e2e.py`

### Legacy Decommission/Delete Scope
- Delete default-language-only behavior in `tools/cq/search/tree_sitter_injections.py::build_injection_plan` (`default_language="rust"` fallback semantics).
- Delete catch-all token-tree-only injection patterns in `tools/cq/search/queries/rust/60_injections.scm` once profile-specific patterns are in place.

---

## S13. Rust Macro Expansion Evidence Bridge
### Goal
Add an optional macro expansion evidence plane (request-scoped, fail-open) and feed expanded source through tree-sitter lanes for improved call/type/reference grounding.

### Representative Code Snippets
```python
# tools/cq/search/rust_macro_expansion_bridge.py
from __future__ import annotations

import subprocess
from pathlib import Path


def expand_with_cargo(crate_root: Path, *, timeout_s: float = 5.0) -> str | None:
    # Best-effort: if cargo-expand is unavailable, lane degrades without failing base search.
    proc = subprocess.run(
        ["cargo", "expand", "--quiet", "--color=never"],
        cwd=crate_root,
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    return proc.stdout

# tools/cq/search/rust_enrichment.py (integration)
expanded = expand_with_cargo(crate_root)
if expanded is not None:
    expanded_payload = tree_sitter_enrich(expanded, cache_key=f"{cache_key}:expanded")
    merge_macro_expansion_payload(base_payload, expanded_payload)
```

### Files to Edit
- `tools/cq/search/rust_enrichment.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/language_front_door_pipeline.py`
- `tools/cq/search/language_front_door_contracts.py`

### New Files to Create
- `tools/cq/search/rust_macro_expansion_bridge.py`
- `tools/cq/search/rust_macro_expansion_contracts.py`
- `tests/unit/cq/search/test_rust_macro_expansion_bridge.py`
- `tests/e2e/cq/test_rust_macro_expansion_enrichment_e2e.py`

### Legacy Decommission/Delete Scope
- Delete macro-only heuristic classification branches in `tools/cq/search/rust_enrichment.py` once expansion-derived evidence becomes authoritative for macro call grounding.
- Delete string-only macro target fallback fields in `tools/cq/search/tree_sitter_rust.py` when expansion evidence provides resolved targets.

---

## S14. Rust Module and Import Graph Resolution from Tree-sitter Evidence
### Goal
Build a robust Rust module/import graph resolver (`mod`, `use`, `extern crate`, `#[path]`) with deterministic file resolution and graph contracts for object-level grounding.

### Representative Code Snippets
```python
# tools/cq/search/rust_module_graph.py
from __future__ import annotations

from pathlib import Path


def resolve_mod_file(module_name: str, *, parent_file: Path, attr_path: str | None) -> Path | None:
    if attr_path:
        candidate = (parent_file.parent / attr_path).resolve()
        return candidate if candidate.exists() else None
    a = parent_file.parent / f"{module_name}.rs"
    b = parent_file.parent / module_name / "mod.rs"
    if a.exists():
        return a.resolve()
    if b.exists():
        return b.resolve()
    return None


def build_module_graph(rows) -> dict[str, list[str]]:
    graph: dict[str, list[str]] = {}
    for row in rows:
        if row.kind == "module_item":
            graph.setdefault(row.file, []).append(row.captures.get("module.name", ""))
    return graph
```

### Files to Edit
- `tools/cq/search/queries/rust/50_modules_imports.scm`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/rust_enrichment.py`
- `tools/cq/search/object_resolver.py`
- `tools/cq/search/object_resolution_contracts.py`

### New Files to Create
- `tools/cq/search/rust_module_graph.py`
- `tools/cq/search/rust_module_graph_contracts.py`
- `tests/unit/cq/search/test_rust_module_graph.py`
- `tests/e2e/cq/test_rust_module_graph_resolution_e2e.py`

### Legacy Decommission/Delete Scope
- Delete module/import string-list heuristics in `tools/cq/search/tree_sitter_rust.py` that are not path-resolved.
- Delete module/import fallback logic in `tools/cq/search/object_resolver.py` that infers module relationships without module graph evidence.

---

## S15. Grammar Drift and Query-Pack Compatibility Gates
### Goal
Add explicit grammar/query compatibility snapshots and fail-fast governance so tree-sitter upgrades cannot silently degrade CQ output quality.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_grammar_drift.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class GrammarSnapshotV1(CqStruct, frozen=True):
    language: str
    abi_version: int
    semantic_version: tuple[int, int, int]
    node_kind_count: int
    field_count: int
    query_pack_hash: str


def grammar_snapshot(language: str, language_obj, query_pack_hash: str) -> GrammarSnapshotV1:
    return GrammarSnapshotV1(
        language=language,
        abi_version=int(language_obj.abi_version),
        semantic_version=tuple(language_obj.semantic_version),
        node_kind_count=int(language_obj.node_kind_count),
        field_count=int(language_obj.field_count),
        query_pack_hash=query_pack_hash,
    )


def assert_compatible(previous: GrammarSnapshotV1, current: GrammarSnapshotV1) -> None:
    if previous.abi_version != current.abi_version:
        raise ValueError("tree-sitter ABI drift detected")
    if previous.query_pack_hash != current.query_pack_hash:
        raise ValueError("query-pack drift detected")
```

### Files to Edit
- `tools/cq/search/query_pack_lint.py`
- `tools/cq/search/tree_sitter_query_registry.py`
- `tools/cq/search/tree_sitter_query_contracts.py`
- `tools/cq/search/tree_sitter_rust_bundle.py`

### New Files to Create
- `tools/cq/search/tree_sitter_grammar_drift.py`
- `tools/cq/search/tree_sitter_grammar_drift_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_grammar_drift.py`

### Legacy Decommission/Delete Scope
- Delete silent fail-open branches in query-pack lint/runtime loading that currently skip enforcement when grammar/schema state is missing.
- Delete mtime/size-only query pack hashing assumptions in `tools/cq/search/tree_sitter_query_registry.py` once grammar-aware compatibility snapshots are canonical.

---

## S16. DiskCache-Native Memoization, Throttling, and Adaptive Lane Control
### Goal
Use built-in `diskcache` decorators and primitives to reduce repeated tree-sitter planner/runtime work and apply adaptive lane throttling without bespoke coordination code.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_adaptive_runtime.py
from __future__ import annotations

from time import monotonic

from diskcache import Averager, memoize_stampede, throttle


def bind_tree_sitter_adaptive_runtime(cache):
    @cache.memoize(
        name="tree_sitter:query_ir_plan",
        expire=300,
        tag="ns:tree_sitter|kind:query_ir_plan",
    )
    def query_ir_plan(language: str, grammar_hash: str, query_pack_hash: str):
        return build_planned_pack_rows(
            language=language,
            grammar_hash=grammar_hash,
            query_pack_hash=query_pack_hash,
        )

    @memoize_stampede(
        cache,
        expire=300,
        name="tree_sitter:pack_sources",
        tag="ns:tree_sitter|kind:query_pack_load",
    )
    def load_pack_sources(language: str, local_hash: str):
        _ = local_hash
        return load_query_pack_sources(language, include_distribution=True)

    lane_ms = Averager(cache, "tree_sitter:lane_ms:macro_expand", expire=300)
    lane_throttle = throttle(
        cache,
        count=8,
        seconds=1,
        name="tree_sitter:macro_expand",
        expire=30,
        tag="ns:tree_sitter|kind:lane_throttle",
    )

    @lane_throttle
    def run_macro_expand_lane(*, request):
        started = monotonic()
        result = execute_macro_expand_lane(request=request)
        lane_ms.add((monotonic() - started) * 1000.0)
        return result

    return query_ir_plan, load_pack_sources, run_macro_expand_lane, lane_ms
```

### Files to Edit
- `tools/cq/search/tree_sitter_query_registry.py`
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/language_front_door_pipeline.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/smart_search.py`

### New Files to Create
- `tools/cq/search/tree_sitter_adaptive_runtime.py`
- `tools/cq/search/tree_sitter_adaptive_runtime_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_adaptive_runtime.py`

### Legacy Decommission/Delete Scope
- Delete duplicated process-local query-pack loader cache maps in `tools/cq/search/tree_sitter_query_registry.py` (`_STAMPED_LOADERS`) once cache-native memoized factories are canonical.
- Delete fixed lane-limit constants in tree-sitter-heavy execution paths where `Averager`-driven adaptive throttling is authoritative.

---

## S17. DiskCache Named Substores and Queue-Driven Changed-Range Worklists
### Goal
Replace one-shot in-memory changed-range loops with named `FanoutCache` substores and queue primitives so request-scoped tree-sitter work can be ordered, resumed, and bounded deterministically.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter_work_queue.py
from __future__ import annotations

from diskcache import FanoutCache


def enqueue_changed_ranges(*, fanout: FanoutCache, run_id: str, rows: list[dict[str, object]], ttl: int, tag: str):
    work_cache = fanout.cache("tree_sitter_work", tag_index=True)
    run_order = fanout.deque(f"tree_sitter_work_order:{run_id}", maxlen=10_000)
    run_index = fanout.index(f"tree_sitter_work_index:{run_id}")
    prefix = f"{run_id}:changed"
    for row in rows:
        key = work_cache.push(
            row,
            prefix=prefix,
            side="back",
            expire=ttl,
            tag=tag,
            retry=True,
        )
        run_order.appendleft(key)
        run_index[key] = {"status": "queued"}


def dequeue_next_changed_range(*, fanout: FanoutCache, run_id: str):
    work_cache = fanout.cache("tree_sitter_work", tag_index=True)
    prefix = f"{run_id}:changed"
    _key, payload = work_cache.pull(
        prefix=prefix,
        side="front",
        default=(None, None),
        retry=True,
    )
    return payload


def peek_pending_changed_range(*, fanout: FanoutCache, run_id: str):
    work_cache = fanout.cache("tree_sitter_work", tag_index=True)
    prefix = f"{run_id}:changed"
    _key, payload = work_cache.peek(
        prefix=prefix,
        side="front",
        default=(None, None),
        retry=True,
    )
    return payload
```

### Files to Edit
- `tools/cq/search/tree_sitter_change_windows.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/tree_sitter_runtime.py`
- `tools/cq/search/smart_search.py`

### New Files to Create
- `tools/cq/search/tree_sitter_work_queue.py`
- `tools/cq/search/tree_sitter_work_queue_contracts.py`
- `tests/unit/cq/search/test_tree_sitter_work_queue.py`

### Legacy Decommission/Delete Scope
- Delete in-memory changed-range accumulator loops used as the only work orchestration path in tree-sitter lanes once queue-backed orchestration is active.
- Delete bespoke pending-work dedupe maps in query execution paths where `push/pull/peek` ordering is canonical.

---

## S18. File-Backed Tree-sitter Payload Storage and Tag-Index Governance
### Goal
Use built-in disk-backed value IO (`read=True` / `cache.read`) and explicit tag-index lifecycle controls for large tree-sitter artifacts to reduce SQLite payload pressure and speed artifact retrieval.

### Representative Code Snippets
```python
# tools/cq/core/cache/tree_sitter_blob_store.py
from __future__ import annotations

from pathlib import Path

from diskcache import FanoutCache, JSONDisk


def open_tree_sitter_blob_cache(directory: Path) -> FanoutCache:
    return FanoutCache(
        directory=str(directory),
        disk=JSONDisk,
        tag_index=True,
        statistics=True,
    )


def persist_large_tree_sitter_blob(*, cache, cache_key: str, blob_path: Path, ttl: int, tag: str) -> bool:
    cache.create_tag_index()
    with blob_path.open("rb") as fh:
        return bool(
            cache.set(
                cache_key,
                fh,
                read=True,
                expire=ttl,
                tag=tag,
                retry=True,
            )
        )


def load_large_tree_sitter_blob(*, cache, cache_key: str) -> bytes | None:
    try:
        with cache.read(cache_key, retry=True) as reader:
            return reader.read()
    except (RuntimeError, TypeError, ValueError, OSError):
        return None


def drop_tree_sitter_tag_index(*, cache) -> None:
    cache.drop_tag_index()
```

### Files to Edit
- `tools/cq/core/cache/tree_sitter_cache_store.py`
- `tools/cq/core/cache/search_artifact_store.py`
- `tools/cq/core/cache/diskcache_backend.py`
- `tools/cq/search/tree_sitter_artifact_contracts.py`
- `tools/cq/search/tree_sitter_structural_export.py`

### New Files to Create
- `tools/cq/core/cache/tree_sitter_blob_store.py`
- `tools/cq/core/cache/tree_sitter_blob_store_contracts.py`
- `tests/unit/cq/core/test_tree_sitter_blob_store.py`

### Legacy Decommission/Delete Scope
- Delete direct large-bytes inline writes in `tools/cq/core/cache/tree_sitter_cache_store.py` for oversized payload categories once file-backed blob paths are canonical.
- Delete ad-hoc tag-index assumptions that rely on process startup defaults rather than explicit `create_tag_index` lifecycle control.

---

## S19. DiskCache Control-Plane Tuning and Transactional Multi-Write Hardening
### Goal
Expose cache runtime tuning through built-in `Cache.reset` and ensure multi-write index/update operations use `transact()` consistently for correctness under concurrency.

### Representative Code Snippets
```python
# tools/cq/core/cache/cache_runtime_tuning.py
from __future__ import annotations


def configure_tree_sitter_cache_runtime(cache) -> None:
    cache.reset("cull_limit", 0)
    cache.reset("eviction_policy", "least-recently-used")
    cache.reset("statistics", 1)
    cache.create_tag_index()


def persist_index_rows_atomically(*, cache, rows: list[tuple[str, object]], ttl: int, tag: str) -> None:
    with cache.transact(retry=True):
        for key, value in rows:
            cache.set(key, value, expire=ttl, tag=tag, retry=True)

    hits, misses = cache.stats(enable=True, reset=False)
    _ = (hits, misses)
```

### Files to Edit
- `tools/cq/core/cache/policy.py`
- `tools/cq/core/cache/diskcache_backend.py`
- `tools/cq/core/cache/maintenance.py`
- `tools/cq/core/cache/search_artifact_store.py`
- `tools/cq/core/cache/fragment_engine.py`

### New Files to Create
- `tools/cq/core/cache/cache_runtime_tuning.py`
- `tools/cq/core/cache/cache_runtime_tuning_contracts.py`
- `tests/unit/cq/core/test_cache_runtime_tuning.py`

### Legacy Decommission/Delete Scope
- Delete non-transactional multi-write paths in `tools/cq/core/cache/search_artifact_store.py` where global/run index + order updates can diverge under failure.
- Delete static cache tuning assumptions that cannot be reconfigured at runtime once reset-driven tuning hooks are landed.

---

## S20. UUID Temporal Contracts, Legacy Normalization, and Conformance Harness
### Goal
Elevate UUID semantics to first-class runtime contracts: UUIDv7 `.time` is canonical for run/event ordering, UUIDv1 identities normalize through `uuid1_to_uuid6`, UUIDv8 usage is contract-gated, and conformance tests enforce version/variant/ordering invariants.

### Representative Code Snippets
```python
# tools/cq/utils/uuid_temporal_contracts.py
from __future__ import annotations

import threading
import uuid

from tools.cq.utils.uuid_factory import UUID6_MODULE, normalize_legacy_identity, uuid7

_UUID_ALT_LOCK = threading.Lock()


def run_uuid_row() -> dict[str, object]:
    run_uuid = uuid7()
    return {
        "run_id": str(run_uuid),
        "run_uuid_version": int(run_uuid.version or 0),
        "run_created_ms": int(run_uuid.time),  # UUIDv7 epoch milliseconds
    }


def normalize_external_identity(raw_value: str) -> uuid.UUID:
    incoming = uuid.UUID(raw_value)
    if incoming.version == 1 and UUID6_MODULE is not None:
        convert = getattr(UUID6_MODULE, "uuid1_to_uuid6", None)
        if callable(convert):
            return convert(incoming)
    return normalize_legacy_identity(incoming)


def gated_uuid8(*, enable_v8: bool) -> uuid.UUID:
    if not enable_v8 or UUID6_MODULE is None:
        return uuid7()
    uuid8_fn = getattr(UUID6_MODULE, "uuid8", None)
    if not callable(uuid8_fn):
        return uuid7()
    with _UUID_ALT_LOCK:
        return uuid8_fn()


def legacy_event_uuid(*, node: int | None = None, clock_seq: int | None = None) -> uuid.UUID:
    if UUID6_MODULE is None:
        return uuid7()
    uuid6_fn = getattr(UUID6_MODULE, "uuid6", None)
    if not callable(uuid6_fn):
        return uuid7()
    with _UUID_ALT_LOCK:
        return uuid6_fn(node=node, clock_seq=clock_seq)
```

```python
# tests/unit/cq/core/test_uuid_temporal_contracts.py
from tools.cq.utils.uuid_factory import uuid7


def test_uuid7_ordering_and_time_contract() -> None:
    rows = [uuid7() for _ in range(32)]
    assert all(row.version == 7 for row in rows)
    assert rows == sorted(rows, key=lambda item: item.int)
    assert all(int(rows[idx].time) <= int(rows[idx + 1].time) for idx in range(len(rows) - 1))
```

### Files to Edit
- `tools/cq/utils/uuid_factory.py`
- `tools/cq/core/schema.py`
- `tools/cq/core/run_context.py`
- `tools/cq/search/tree_sitter_event_contracts.py`
- `tools/cq/search/tree_sitter_artifact_contracts.py`
- `tools/cq/cli_app/telemetry.py`

### New Files to Create
- `tools/cq/utils/uuid_temporal_contracts.py`
- `tools/cq/utils/uuid_temporal_contracts_models.py`
- `tests/unit/cq/core/test_uuid_temporal_contracts.py`
- `tests/unit/cq/core/test_uuid_temporal_contracts_models.py`
- `tests/unit/cq/core/test_tree_sitter_cache_key_temporal_ordering.py`

### Legacy Decommission/Delete Scope
- Delete ad-hoc wall-clock-only created-time fields where run/event ordering should derive from UUIDv7 `.time` contract fields.
- Delete permissive string-only identity handling in tree-sitter artifact/event surfaces where normalized UUID contracts are required.
- Delete uncontracted UUIDv8 fallback usage paths; UUIDv8 generation must be explicitly gated by runtime policy and tested conformance.

---

## Cross-Scope Legacy Decommission and Deletion Plan
### Batch D1 (after S1, S2, S3, S4)
- Delete non-planned pack execution paths in `tools/cq/search/tree_sitter_python.py` and `tools/cq/search/tree_sitter_rust.py` (source-order execution loops).
- Delete heuristic name-matching helper `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py::_matches_name`.
- Delete broad catch-all identifier query patterns in `tools/cq/search/queries/python/10_refs.scm` and `tools/cq/search/queries/rust/10_refs.scm` that are superseded by predicate-pushdown packs.

### Batch D2 (after S5, S6, S7, S8)
- Delete unconditional full-window query passes in enrichment lanes where changed-range windows are available.
- Delete parser callsites that bypass shared parser controls and budget-aware parse wrappers (`tools/cq/neighborhood/tree_sitter_collector.py` direct parse path).
- Delete static cancelled/timeout placeholders in runtime telemetry payloads that are superseded by real cancellation propagation.

### Batch D3 (after S11, S12, S13, S14)
- Delete `tools/cq/search/tree_sitter_tags.py` as a primary structure plane once CST tokens + structural edges + module graph + injection/macro planes are canonical.
- Delete macro/module fallback heuristics in `tools/cq/search/rust_enrichment.py` and `tools/cq/search/object_resolver.py` that are superseded by expansion/module-graph evidence.
- Delete legacy catch-all injection patterns in `tools/cq/search/queries/rust/60_injections.scm`.

### Batch D4 (after S9, S10, S15)
- Delete runtime JSON-schema loading in `tools/cq/search/tree_sitter_node_schema.py` once generated node-type modules are canonical.
- Delete duplicated language bootstrap code paths in `tools/cq/search/query_pack_lint.py` in favor of the introspection registry.
- Delete lint/runtime branches that silently continue on grammar drift and enforce fail-fast compatibility gates.

### Batch D5 (after S16, S17, S18, S19, S20)
- Delete bespoke tree-sitter lane throttling and moving-average plumbing once `diskcache.throttle` and `diskcache.Averager` are canonical runtime primitives.
- Delete one-shot in-memory changed-range worklist orchestration once queue-backed `push/pull/peek` flow is authoritative.
- Delete non-transactional multi-index artifact persistence paths after transaction-hardened cache writes are landed.
- Delete non-contractual run/event timestamp derivation paths and rely on UUIDv7 `.time` contract fields.

## Implementation Sequence
1. S1: establish IR planner first so all later pack/runtime work has a stable planning substrate.
2. S2: add specialization profiles before predicate expansion to avoid regressions in terminal/artifact surfaces.
3. S3: land custom predicate runtime so new predicate-heavy packs have execution support.
4. S4: upgrade query packs to predicate-pushdown once runtime support exists.
5. S5: wire changed-range selective execution after planner/predicate upgrades stabilize match semantics.
6. S6: propagate cancellation/budgets end-to-end to cap latency before additional planes are added.
7. S16: introduce diskcache-native memoization/throttling/averaging before adding more expensive tree-sitter lanes.
8. S17: move changed-range orchestration to queue-backed named substores to stabilize request-scoped work scheduling.
9. S18: land file-backed payload storage for large structural/macro artifacts to reduce cache IO pressure.
10. S19: apply cache tuning + transaction hardening once new stores/queues are in place.
11. S7: standardize parser operational controls to improve recoverability and instrumentation.
12. S8: add streaming parse input after parser controls to reduce memory pressure without destabilizing parser behavior.
13. S9: introduce language introspection registry as the shared metadata foundation for governance.
14. S10: generate typed node schema modules and switch lint/runtime consumers.
15. S11: add CST token plane and structural ABI expansion.
16. S12: expand Rust injection capabilities on top of new structural/runtime foundations.
17. S13: add macro expansion evidence bridge for macro-heavy Rust projects.
18. S14: implement Rust module/import graph for cross-file grounding completeness.
19. S20: enforce UUID temporal contracts and conformance harness for run/event/artifact identity semantics.
20. S15: finalize grammar drift fail-fast governance and compatibility enforcement.
21. D1 batch deletions.
22. D2 batch deletions.
23. D3 batch deletions.
24. D4 batch deletions.
25. D5 batch deletions.

## Implementation Checklist
- [ ] S1. Query IR Planner and Costed Pack Scheduling
- [ ] S2. Query Specialization Profiles via `disable_pattern` / `disable_capture`
- [ ] S3. Custom Predicate Runtime (`QueryPredicate`) for Semantic Filtering
- [ ] S4. Predicate-Pushdown Query Pack Upgrade (`#eq?`, `#match?`, `#any-of?`)
- [ ] S5. Changed-Range Selective Query and Enrichment Recompute
- [ ] S6. End-to-End Cancellation and Budget Propagation
- [ ] S7. Parser Operational Hardening (`reset`, `logger`, `print_dot_graphs`)
- [ ] S8. Streaming Parse Source Callbacks for Large Inputs
- [ ] S9. Language Introspection Registry (Kinds, Fields, Supertypes)
- [ ] S10. `node-types.json` Code Generation and Typed Schema Modules
- [ ] S11. CST Token Plane and Expanded Structural ABI
- [ ] S12. Rust Injection Coverage Expansion (`injection.combined`, Profiled Macros)
- [ ] S13. Rust Macro Expansion Evidence Bridge
- [ ] S14. Rust Module and Import Graph Resolution from Tree-sitter Evidence
- [ ] S15. Grammar Drift and Query-Pack Compatibility Gates
- [ ] S16. DiskCache-Native Memoization, Throttling, and Adaptive Lane Control
- [ ] S17. DiskCache Named Substores and Queue-Driven Changed-Range Worklists
- [ ] S18. File-Backed Tree-sitter Payload Storage and Tag-Index Governance
- [ ] S19. DiskCache Control-Plane Tuning and Transactional Multi-Write Hardening
- [ ] S20. UUID Temporal Contracts, Legacy Normalization, and Conformance Harness
- [ ] D1. Cross-scope decommission batch
- [ ] D2. Cross-scope decommission batch
- [ ] D3. Cross-scope decommission batch
- [ ] D4. Cross-scope decommission batch
- [ ] D5. Cross-scope decommission batch
