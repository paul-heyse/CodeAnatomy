# CQ Python Enrichment Proposal Review and Optimization Plan (2026-02-06)

## Reviewed Inputs

1. `docs/plans/cq_python_enrichment_best_in_class_recommendations.md`
2. `docs/python_library_reference/ast-grep-py.md`
3. `docs/python_library_reference/tree-sitter_outputs_overview.md`
4. `docs/python_library_reference/tree-sitter.md`
5. `docs/python_library_reference/tree-sitter_advanced.md`
6. `docs/python_library_reference/python_ast_libraries_and_cpg_construction.md`
7. Current CQ implementation:
   - `tools/cq/search/smart_search.py`
   - `tools/cq/search/classifier.py`
   - `tools/cq/astgrep/rules_py.py`
   - `tools/cq/search/tree_sitter_rust.py` (as architecture reference)

## Executive Summary

The Python enrichment proposal is high value and directionally correct. The strongest parts are:

1. Prioritizing ast-grep-first extraction from already parsed trees.
2. Adding signature/decorator/class/call/import context to remove follow-up queries.
3. Keeping enrichment additive and fail-open.

The main issues to correct are:

1. A few recommendations conflict with current CQ contracts and scoring internals.
2. The proposed module split and payload shape diverge from the now-mature Rust enrichment pattern.
3. Several extraction details need tighter semantics to avoid false positives.
4. A major coordinate correctness gap (ripgrep byte offsets vs char columns) is not addressed in the proposal and should be treated as blocking.

## Current Implementation Baseline (Python Path)

From `tools/cq/search/smart_search.py` and `tools/cq/search/classifier.py`:

1. Python already has:
   - category classification and confidence (`heuristic`, ast-grep node classification, record classification),
   - `containing_scope`,
   - `binding_flags` from `symtable`,
   - `context_snippet`,
   - raw `node_kind` and `line_text`.
2. There is no Python-specific enrichment module analogous to Rust.
3. Node resolution is currently by line/column path (`classify_from_node(...)`), not by reusable byte-range anchor.
4. Rust enrichment architecture (`tools/cq/search/tree_sitter_rust.py`) is now significantly advanced and should be reused as the design template for Python:
   - bounded caches with staleness checks,
   - bounded payload extraction,
   - `enrichment_status`,
   - fail-open degradation semantics,
   - grouped extraction helpers.

## Corrections to the Proposal

## C1. Keep additive contract strict and do not change ranking semantics

The proposal is mostly aligned, but two points need correction:

1. Do not replace confidence internals with bucket-only semantics.
2. Do not let enrichment fields influence relevance sorting.

Reason:
1. Current ranking relies on numeric `confidence` (`compute_relevance_score`).
2. Rust enrichment contract explicitly forbids ranking/classification side effects and should remain consistent across languages.

Correction:
1. Keep numeric confidence in score details.
2. Add optional display buckets without removing numeric signal.

## C2. Do not remove `evidence_kind` from core score payload in v1 of this effort

The proposal states `evidence_kind` is low-value and should be removed. That is too aggressive.

Reason:
1. `evidence_kind` currently drives score bucketing and test expectations.
2. It is useful for diagnosing classifier regressions.

Correction:
1. Keep `evidence_kind` in `ScoreDetails`.
2. If token optimization is required, drop it from rendered prose first, not from structured payload.

## C3. `context_window` removal should be staged, not immediate

The proposal recommends removing `context_window` entirely.

Reason:
1. It is low-value for human output but can still support deterministic snippet reconstruction and test diagnostics.
2. Immediate removal creates avoidable churn in existing tests and consumers.

Correction:
1. Mark `context_window` deprecated for external display.
2. Keep internal contract until a schema cutover (for example, `cq.v2` payload change set).

## C4. Module naming should reflect primary backend

The proposal suggests `tools/cq/search/tree_sitter_python.py` while also stating ast-grep is primary.

Correction:
1. Use `tools/cq/search/python_enrichment.py` as the primary module.
2. Add `tools/cq/search/tree_sitter_python.py` only as optional gap-fill backend.
3. Keep one orchestrating entrypoint (`enrich_python_context(...)`) that merges sources by precedence.

## C5. Coordinate correctness is a blocking gap and must be explicit

Current search ingestion uses ripgrep submatch byte offsets; classification and enrichment paths currently use line/column coordinates.

Risk:
1. Unicode lines can desynchronize byte offsets and character columns.

Correction:
1. Add Python enrichment byte-range entrypoint from day one:
   - `enrich_python_context_by_byte_range(...)`.
2. Convert ripgrep byte offsets to tree lookup by byte range first.
3. Use line/column only as fallback.

## C6. `is_generator` logic should be semantics-safe

The proposal suggests bounded-depth body walk (`depth=3`) for `yield`.

Risk:
1. This can miss legitimate yields or misclassify nested scopes.

Correction:
1. Use Python `ast` for generator determination at function scope boundary:
   - walk function body,
   - ignore nested function/class/lambda scopes,
   - detect `Yield`/`YieldFrom`.
2. Keep ast-grep as first pass for speed, then confirm via `ast` when ambiguous.

## C7. `item_role` should be additive to `node_kind`, not replacement

The proposal recommends replacing raw `node_kind`.

Correction:
1. Keep `node_kind` for debugging and rule diagnostics.
2. Add `item_role` as the agent-facing abstraction.

## C8. Import detail extraction requires exact grammar handling

The proposal’s import extraction is valid in intent but underspecified for:

1. multi-import statements,
2. alias combinations,
3. relative imports,
4. `if TYPE_CHECKING` detection.

Correction:
1. Use a two-pass extraction:
   - ast-grep structural capture for statement kind,
   - Python `ast` normalization for canonical fields (`module`, `names`, `asname`, `level`, `type_checking_guarded`).

## Optimized Architecture (Python)

## A1. Mirror the Rust enrichment architecture pattern

Adopt Rust module design principles in Python enrichment:

1. bounded, hash-aware cache,
2. per-extractor bounds and truncation rules,
3. grouped extraction helpers,
4. explicit enrichment runtime metadata:
   - `enrichment_status`,
   - `enrichment_sources`,
   - `degrade_reason`.

## A2. Two-tier backend with deterministic precedence

Recommended extraction order:

1. ast-grep/SgNode (primary),
2. Python `ast` (semantic normalization and scope-safe facts),
3. tree-sitter-python QueryCursor (only for gaps where both above are weak).

Precedence:

1. keep first non-empty value from primary source,
2. fill missing fields from lower tiers,
3. never overwrite non-empty values unless explicitly marked canonical override.

## A3. Keep Python and Rust payload contracts parallel

Define shared field groups across languages:

1. `identity`,
2. `signature`,
3. `modifiers`,
4. `scope`,
5. `call`,
6. `import`,
7. `binding`,
8. `runtime`.

Language-specific fields remain allowed, but group names and runtime semantics should match.

## A4. Integrate with existing classifier node lookup without redundant traversal

Current classifier already resolves a node candidate (`classify_from_node` path). Optimize by:

1. returning resolved node metadata to enrichment step (or storing a per-match node locator),
2. avoiding second full-tree lookup for the same match.

## Additional Value-Add Scope Expansions

## O1. Add function behavior summary fields (high LLM value)

Using Python `ast` + existing symtable context, add:

1. `returns_value` (has non-None return),
2. `raises_exception` (contains raise at function scope),
3. `yields` (generator behavior),
4. `awaits` (async body contains await),
5. `has_context_manager` (contains with/async with).

This directly answers common follow-up questions in planning loops.

## O2. Expand binding signal richness from symtable

Current output surfaces a subset of symtable flags. Expand to include:

1. `is_referenced`,
2. `is_local`,
3. `is_nonlocal`.

These are already available from `symtable` and improve refactor safety reasoning.

## O3. Add class API shape summary

For class definitions, add bounded summaries:

1. method count,
2. property names,
3. abstract member count,
4. dataclass/frozen/slots markers.

This gives fast architectural context without file reads.

## O4. Add query-pack quality gates for any tree-sitter Python layer

If tree-sitter queries are introduced:

1. lint for rooted/local patterns,
2. enforce `match_limit`,
3. use range-scoped execution (`set_byte_range`),
4. wire progress callback cancellation,
5. persist pattern metadata (`pattern_settings`, assertions) for diagnostics.

This follows best practices in `tree-sitter_advanced.md`.

## O5. Introduce schema-lint tests from grammar node-types

For both ast-grep and tree-sitter enrichment code:

1. validate referenced node kinds and field names against grammar schema,
2. fail CI when grammar upgrades invalidate assumptions.

This prevents silent extractor drift.

## O6. Add per-match payload budget metadata

Expose truncation metadata:

1. `truncated_fields`,
2. `payload_size_hint`,
3. optional `dropped_fields`.

This enables bounded outputs and explicit tradeoffs.

## O7. Add incremental parse/query scaffolding for repeated searches

Leverage tree-sitter incremental model for interactive repeated queries:

1. cache per-file tree,
2. apply edits and compute changed ranges,
3. run range-scoped enrichment queries only on changed regions.

Even if initially optional, this enables future high-scale performance improvements.

## Suggested Revised Execution Order

1. Correctness and contracts first:
   - C1, C5, C7.
2. Primary Python enrichment module and ast-grep extraction:
   - C4, A1, A2.
3. Semantic normalization tier:
   - C6, C8, O1, O2.
4. Payload shaping and backward-compat staging:
   - C2, C3, O6.
5. Optional tree-sitter augmentation and quality gates:
   - O4, O5, O7.

## Test Additions Recommended

1. Unicode coordinate tests for byte-range anchor correctness.
2. Snapshot parity tests ensuring enrichment does not alter ranking order.
3. Degradation tests with per-extractor failures (status stays fail-open).
4. Function behavior summary tests (`yield`, `raise`, `await`) with nested scopes.
5. Import normalization tests covering aliases, relative imports, and `TYPE_CHECKING`.
6. Payload bound/truncation tests.
7. Grammar schema-lint tests for node kinds/fields used by extractors.

## Acceptance Criteria

1. Enrichment remains additive and fail-open.
2. No changes to relevance ranking or match counts from enrichment fields.
3. Python enrichment returns deterministic, bounded payloads.
4. Byte/column correctness is guaranteed for non-ASCII source.
5. Follow-up query rate for common Python review tasks is materially reduced.
6. Python and Rust enrichment runtime contracts are structurally aligned.
