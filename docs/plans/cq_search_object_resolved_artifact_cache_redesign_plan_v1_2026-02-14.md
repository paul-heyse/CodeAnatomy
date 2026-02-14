# CQ Search Object-Resolved Output + Cache-Backed Artifact Redesign Plan (v1, 2026-02-14)

## Scope
Redesign `./cq search` output and artifact handling to:
1. resolve matches by object and emit one Code Facts summary per object,
2. list occurrence locations without inline snippets in terminal,
3. remove terminal summary/diagnostic materialization,
4. persist artifacts in cache with msgspec-native contracts and retrieval interface,
5. improve semantic/call-graph attachment by resolving each match to closest-fit object.

This plan is based on:
- current implementation in `tools/cq/search/smart_search.py`, `tools/cq/core/report.py`, `tools/cq/cli_app/result.py`, `tools/cq/core/artifacts.py`, cache modules under `tools/cq/core/cache`.
- observed behavior from `./cq search stable_id --in tools/cq`.
- reference capability review of:
  - `docs/python_library_reference/python_ast_libraries_and_cpg_construction.md`
  - `docs/python_library_reference/tree-sitter_advanced.md`
  - `docs/python_library_reference/tree-sitter_outputs_overview.md`

## Reference-Driven Design Principles (Added 2026-02-14)
1. Object resolution must be multi-evidence and deterministic: AST/symtable binding evidence first, Tree-sitter structural evidence second, dis/inspect overlays third, lexical fallback last.
2. Search execution must be incremental and range-scoped where possible: changed ranges, byte-windowed query execution, bounded query cursors, and explicit partial-result telemetry.
3. Query packs and enrichment contracts must be schema-validated: rooted/local query patterns, capture cardinality checks, and grammar drift checks against `node-types.json`.
4. Enrichment must be retained with explicit quality labels rather than dropped on strict gates; result quality is communicated as `full_signal | partial_signal | structural_only`.
5. Terminal and artifact concerns remain hard-separated: terminal shows concise object/occurrence navigation; full snippets, diagnostics, telemetry, and raw payloads live in cache artifacts.

## Findings

### 1. Search output is match-centric, not object-centric
- Sections are built from `EnrichedMatch` instances and rendered as per-match findings (`tools/cq/search/smart_search.py:1486`, `tools/cq/search/smart_search.py:1566`, `tools/cq/search/smart_search.py:1593`).
- `build_finding` emits one finding per match with local enrichment (`tools/cq/search/smart_search.py:1129`).
- Result: duplicated Code Facts blocks for repeated references to the same symbol/object.

### 2. Terminal currently renders inline code snippets
- `context_snippet` is attached into finding details (`tools/cq/search/smart_search.py:1191`).
- Renderer materializes snippet blocks in terminal (`tools/cq/core/report.py:203`, `tools/cq/core/report.py:216`).
- Result: noisy terminal output and repeated code blocks.

### 3. Terminal currently renders a summary payload block
- Markdown render includes `Summary` section (`tools/cq/core/report.py:1596`).
- `_render_summary` outputs the compact JSON line (`tools/cq/core/report.py:1092`).
- Result: diagnostics/telemetry/status payloads still appear in terminal.

### 4. Artifacts are file-backed JSON, not cache-backed contracts
- `handle_result` writes JSON artifact files (`tools/cq/cli_app/result.py:189`).
- Artifact functions write JSON to `.cq/artifacts` (`tools/cq/core/artifacts.py:32`, `tools/cq/core/artifacts.py:51`).
- Result: filesystem roundtrip and JSON-only retrieval path.

### 5. Python semantic enrichment signal is frequently dropped for identifier searches
- Semantic enrichment is budget-limited to first N anchors (`MAX_PYTHON_SEMANTIC_ENRICH_FINDINGS=8`) (`tools/cq/search/smart_search.py:174`, `tools/cq/search/smart_search.py:2302`, `tools/cq/search/smart_search.py:2520`).
- Payloads without deemed “actionable signal” are treated as failures and not attached (`tools/cq/search/smart_search.py:2336`, `tools/cq/search/smart_search.py:2451`).
- `stable_id` sample: `python_semantic_telemetry.attempted=6`, `applied=0`, diagnostics reason `no_signal` for all anchors.
- Front-door insight semantic degradation may remain failed with `request_failed` for target semantic augmentation.

### 6. Call graph N/A is expected for many non-callable anchors under current model
- Code Facts call graph fields are sourced from `python_semantic.call_graph` (`tools/cq/core/enrichment_facts.py:226`).
- For field/variable references (e.g., `Finding.stable_id`), current per-anchor payload often has resolution but no semantic call graph object-level mapping.
- Result: frequent `N/A — not resolved` in Call Graph cluster even when symbol resolution exists.

### 7. Current data model lacks explicit object identity grouping
- `Finding` has `stable_id` for finding identity, not resolved object identity (`tools/cq/core/schema.py:412`).
- No struct exists for object summary + occurrences grouping in search output.

## Conclusions
1. The current match-centric rendering is the root cause of repeated Code Facts and snippet noise.
2. Terminal rendering and artifact payload concerns are mixed; they need hard separation.
3. The current semantic gating is too strict for identifier/reference-heavy queries and discards usable partial payloads.
4. A durable object-resolution layer is required before section building and rendering.
5. Cache backend already exists and is sufficient to host artifact bundles; we need typed artifact contracts and retrieval commands.

## Proposed Design Changes

## A. Introduce object-resolved search aggregation (hard cutover)
Add a new aggregation stage between enrichment and section building:

1. Resolve each `EnrichedMatch` to `ResolvedObjectRef`.
2. Group matches by object key.
3. Build one `ObjectSummary` per group.
4. Emit occurrence entries separately.

Representative contract sketch:

```python
# tools/cq/search/object_resolution_contracts.py
class ResolvedObjectRef(CqOutputStruct, frozen=True):
    object_id: str                 # deterministic object identity digest
    language: QueryLanguage
    symbol: str
    qualified_name: str | None = None
    kind: str | None = None        # function/class/field/module/unknown
    canonical_file: str | None = None
    canonical_line: int | None = None

class SearchOccurrenceV1(CqOutputStruct, frozen=True):
    object_id: str
    file: str
    line: int
    col: int | None = None
    context_start_line: int | None = None
    context_end_line: int | None = None
    category: str

class SearchObjectSummaryV1(CqOutputStruct, frozen=True):
    object_ref: ResolvedObjectRef
    code_facts: dict[str, object]
    occurrence_count: int
    files: list[str]
```

Resolution precedence for `object_id`:
1. definition candidate (`name`, `kind`, anchor),
2. `python_semantic.symbol_grounding.definition_targets`,
3. python native binding evidence (`symtable`-backed binding candidates, scope graph resolution),
4. Tree-sitter locals/imports/callsite evidence (`@local.definition`, `@local.reference`, import edges),
5. `resolution.qualified_name_candidates` + `binding_candidates`,
6. import resolution path,
7. lexical fallback (`query + normalized match_text + containing_scope`).

## B. Replace per-match Code Facts with per-object summaries
- Remove Code Facts rendering from every occurrence row.
- Add a new section: `Resolved Objects` with one summary row per object.
- Keep `Target Candidates` / `Neighborhood Preview` when available, but derived from object summaries.

Representative section assembly sketch:

```python
# tools/cq/search/smart_search.py
object_view = build_object_resolved_view(enriched_matches, query=ctx.query)
sections = [
    build_resolved_objects_section(object_view.summaries),
    build_occurrences_section(object_view.occurrences),
    build_uses_by_kind_section(object_view.occurrences),
    build_hot_files_section(object_view.occurrences),
]
```

## C. Occurrence rows: location + enclosing block range only
For each occurrence in terminal output, show:
- `file:line[:col]`,
- `block_start_line-block_end_line` (context window boundaries),
- category/kind.

Do not show inline source code snippet in terminal.

Representative renderer change:

```python
# tools/cq/core/report.py
# old: emits fenced context snippet
# new:
lines.append(f"  Location: `{file}:{line}`")
lines.append(f"  Block: `{file}:{start_line}-{end_line}`")
# no code fence materialization
```

## D. Terminal summary removal for search
For `search` macro markdown rendering:
- remove `## Summary` block from terminal,
- keep high-signal headers (`Insight Card`, object summaries, occurrences),
- diagnostics and telemetry remain artifact-only.

Representative guard:

```python
if result.run.macro != "search":
    lines.extend(_render_summary(compact_summary))
```

## E. Cache-backed artifact store with msgspec-native retrieval
Replace file JSON artifacts for search with cache namespace storage.

### E1. New cache contracts

```python
# tools/cq/core/cache/contracts.py
class SearchArtifactBundleV1(CqCacheStruct, frozen=True):
    run_id: str
    query: str
    object_summaries: list[SearchObjectSummaryV1]
    occurrences: list[SearchOccurrenceV1]
    diagnostics: dict[str, object] = msgspec.field(default_factory=dict)
    snippets: dict[str, str] = msgspec.field(default_factory=dict)  # keyed by occurrence id

class SearchArtifactIndexEntryV1(CqCacheStruct, frozen=True):
    run_id: str
    cache_key: str
    created_ms: float
```

### E2. Storage model
- Namespace: `search_artifacts`.
- Store bundle as msgspec builtins (or msgpack bytes) in cache backend.
- Maintain run-scoped index key to list/retrieve artifact bundles.

### E3. Retrieval interface
Add a new command group (not deprecated `admin cache`):
- `cq artifact list [--run-id ...]`
- `cq artifact get --run-id ... [--kind search_bundle|diagnostics|snippets]`

All retrieval flows decode typed contracts via `msgspec.convert`/msgpack decoder, avoiding JSON roundtrip.

## F. Improve semantic attachment and “closest fit” behavior

### F1. Relax “no signal” drop policy for search object resolution
Current policy discards payload when no call graph/local context/grounding threshold is unmet.
Change policy:
- retain payload if it has any of: resolution candidates, type contract, parse quality, source attribution, class context.
- mark coverage state explicitly (`partial_signal`) instead of dropping.

### F2. Resolve semantic target from object group, not only top definition match
Current insight semantic path requires both `primary_target_finding` and `top_definition_match` (`tools/cq/search/smart_search.py:2941`).
Change to object-group target selection:
- pick best object summary by confidence + occurrence rank,
- use canonical object anchor for semantic provider requests,
- fallback to nearest reference anchor if no definition anchor exists.

### F3. Call graph attachment policy
- For callable/type objects: attach object-level call graph once in object summary.
- For non-callable objects: mark `not_applicable_non_callable` (not generic `not_resolved`).

This removes misleading `N/A` for fundamentally non-callable identifiers.

## G. Multi-Evidence Object Resolver (Reference-Driven Augmentation)
Implement a weighted object resolver that fuses evidence planes into one canonical object assignment per match.

### G1. Evidence planes
- `AST + candidate_normalizer`: local definition/reference candidates and containing scope.
- `symtable/native`: scope type, symbol flags (`local/global/nonlocal/free/imported/annotated`), namespace edges.
- `Tree-sitter`: CST anchors, locals captures, import captures, callsite shape, enclosing block ranges.
- `python_semantic`: grounding, type contract, call graph fragments, class context.
- `dis`/bytecode (optional per object kind): code-unit callsite cues and control/dataflow anchors.
- lexical fallback: normalized identifier + scope signature hash.

### G2. Deterministic scoring and tie-break
- Score each candidate object with additive weights by evidence quality.
- Favor candidates with:
  - definition anchor over reference-only anchor,
  - exact scope/binding agreement over lexical match,
  - cross-plane agreement over single-plane confidence.
- Tie-break by stable ordering:
  - canonical file path,
  - canonical line,
  - object key digest.

### G3. Required output quality fields
- `resolution_quality: "strong" | "medium" | "weak"`
- `evidence_planes: list[str]`
- `agreement: "full" | "partial" | "conflict"`
- `fallback_used: bool`

## H. Tree-sitter Execution and Query-Pack Quality Gates
Use advanced Tree-sitter capabilities as first-class runtime constraints for scale and determinism.

### H1. Incremental and bounded query execution
- Parse with reusable trees where available and scope extraction to `changed_ranges` windows.
- Execute queries using byte range windows (`set_byte_range`) and bounded cursor settings:
  - `match_limit`
  - `set_max_start_depth`
  - progress callback cancellation for runaway matches.
- Surface `did_exceed_match_limit` and cancellation as explicit telemetry in search diagnostics.

### H2. Query-pack static quality gates
- Enforce rooted/local patterns for search/enrichment query packs.
- Validate field and node usage against grammar schema (`node-types.json`) to prevent silent drift.
- Capture quantifier/cardinality metadata for stable result shaping and memory-safe execution.

### H3. Occurrence/block derivation
- Derive occurrence location and enclosing block bounds from CST anchors (`start_point/end_point`) rather than rendered snippets.
- Emit block range metadata in terminal and full snippet payloads in artifacts only.

## I. Python Native Enrichment Contracts (symtable/dis/inspect)
Add explicit native enrichment contracts to reduce `N/A` and improve closest-fit attachment quality.

### I1. Symtable-backed lexical resolution
- Materialize (or consume existing) scope/symbol facts into resolver-friendly rows:
  - scope kind, parent scope, namespace edges,
  - symbol flags and binding class.
- Use this to map references to stable binding slots before fallback heuristics.

### I2. dis-backed callable/callsite enrichment
- For callable-aligned objects, attach bytecode-informed callsite/cfg facts where available.
- Keep this overlay object-level and optional; do not block base search output on dis availability.

### I3. inspect overlay policy
- Keep inspect optional and isolated (safe/static mode first).
- Use inspect only to enrich callable signatures/wrapper metadata when import-safe and budget-allowed.
- Record overlay provenance in object summaries; no hard dependency for base resolution.

## J. Coverage Semantics and N/A Elimination Policy
Replace opaque `N/A`/drop behavior with explicit applicability and coverage categories.

### J1. Coverage levels
- `full_signal`: high-confidence grounding + applicable enrichment fields present.
- `partial_signal`: at least one meaningful enrichment plane attached.
- `structural_only`: only structural/scoped evidence available (still valid result).
- `not_applicable_non_callable`: call graph/type-call planes intentionally not applicable.

### J2. Required summary fields
- `coverage_level`
- `applicability` map by enrichment dimension (`call_graph`, `type_contract`, `locals`, `imports`)
- `coverage_reasons` (normalized reason codes)

## K. Validation and Drift Gates (Plan-Level Acceptance Expansion)
Add acceptance gates derived from the reference docs to prevent silent regressions.

### K1. Resolver correctness gates
- Same query term across repeated references produces stable object IDs for identical objects.
- Cross-file/object grouping remains deterministic across runs with unchanged inputs.

### K2. Query/runtime safety gates
- Match-limit and cancellation telemetry must be preserved in artifact diagnostics.
- Query-pack lint checks must fail CI for non-rooted/non-local patterns in CQ search enrichment packs.

### K3. Native enrichment gates
- Symtable-backed binding evidence should materially reduce `no_signal` diagnostics for identifier-heavy queries.
- Non-callable queries must report applicability as `not_applicable_non_callable` rather than unresolved call graph failures.

## Implementation Sequence

1. Add object-resolution contracts, evidence-plane enums, and resolver scoring/tie-break logic.
2. Integrate symtable/native evidence into object resolution candidate construction.
3. Integrate Tree-sitter occurrence/block-range extraction as canonical occurrence source.
4. Rewire search section assembly to object summaries + occurrences.
5. Update markdown/ldmd renderer to suppress snippets and summary for search terminal view.
6. Add cache artifact contracts + persistence service + retrieval commands.
7. Update semantic coverage policy, applicability fields, and closest-fit target selection from object groups.
8. Add query-pack lint/runtime gates (rooted/local, bounded cursor telemetry).
9. Refresh tests/fixtures with coverage-level and applicability assertions.

## Target Files To Modify

### Search pipeline
- `tools/cq/search/smart_search.py`
- `tools/cq/search/section_builder.py`
- `tools/cq/search/candidate_normalizer.py`
- `tools/cq/search/python_native_resolution.py`
- `tools/cq/search/python_semantic_signal.py`
- `tools/cq/search/tree_sitter_python.py`
- `tools/cq/search/language_front_door_pipeline.py` (if coverage status contract changes)
- `tools/cq/search/language_front_door_contracts.py`

### New search modules
- `tools/cq/search/object_resolution_contracts.py`
- `tools/cq/search/object_resolver.py`
- `tools/cq/search/object_sections.py`
- `tools/cq/search/object_resolution_scoring.py`
- `tools/cq/search/query_pack_lint.py`

### Rendering/output
- `tools/cq/core/report.py`
- `tools/cq/ldmd/writer.py`
- `tools/cq/cli_app/result.py`

### Artifact/cache layer
- `tools/cq/core/artifacts.py` (replace/branch search artifact path)
- `tools/cq/core/cache/contracts.py`
- `tools/cq/core/cache/namespaces.py`
- `tools/cq/core/cache/key_builder.py` (artifact key helpers)
- `tools/cq/core/cache/fragment_codecs.py` or new cache codec helper for typed artifact bundles

### CLI surface
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/commands/` (new `artifact.py`)
- `tools/cq/cli_app/types.py` (if new format/token support is needed)

### Tests/fixtures (CQ-only)
- `tests/unit/cq/search/test_smart_search.py`
- `tests/unit/cq/search/test_object_resolver.py`
- `tests/unit/cq/search/test_query_pack_lint.py`
- `tests/unit/cq/test_report.py`
- `tests/unit/cq/core/test_compact_rendering.py`
- `tests/unit/cq/test_cli_result_handling.py`
- `tests/unit/cq/core/test_diagnostics_contracts.py`
- `tests/unit/cq/test_runmeta_artifacts.py`
- `tests/e2e/cq/test_search_command_e2e.py`
- `tests/e2e/cq/test_ldmd_command_e2e.py`
- `tests/e2e/cq/fixtures/search_*.json`

## Migration Policy
Design-phase hard cutover for search output path:
- no compatibility renderer for old per-match Code Facts terminal layout,
- no file-first JSON search artifacts,
- no dual-write path for search artifacts (cache-only for search bundles).

Non-search macros can remain unchanged in phase 1, with optional later unification.

## Validation Criteria
1. `./cq search stable_id` terminal output shows:
   - object summary section (deduplicated),
   - occurrence listings with file/line + block range,
   - no inline snippet blocks,
   - no terminal summary JSON block.
2. Search artifacts are retrievable from cache via new artifact commands.
3. Object summaries include best-effort enrichment for all grouped objects via closest-fit resolution.
4. Python semantic telemetry reflects retained partial payloads rather than blanket `no_signal` drops.
5. Non-callable objects report applicability as `not_applicable_non_callable` for call-graph dimensions.
6. Query execution diagnostics include match-limit/cancellation telemetry when bounded execution degrades.
7. Resolver output includes evidence-plane and agreement metadata for each object summary.
