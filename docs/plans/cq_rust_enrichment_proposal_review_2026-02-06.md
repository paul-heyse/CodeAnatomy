# CQ Rust Enrichment Proposal Review and Optimization Plan (2026-02-06)

## Reviewed Inputs

1. `docs/plans/cq_rust_enrichment_best_in_class_recommendations_v1.md`
2. `docs/python_library_reference/tree-sitter-rust.md`
3. `docs/python_library_reference/ast-grep-py_rust_deepdive.md`
4. Current implementation in:
   - `tools/cq/search/tree_sitter_rust.py`
   - `tools/cq/search/smart_search.py`
   - `tools/cq/search/classifier.py`
   - `tools/cq/astgrep/rules_rust.py`

## Executive Summary

The v1 recommendations are directionally strong and align with the current CQ architecture goal: richer Rust context from existing parse work. The largest gaps are:

1. Some extraction details in v1 rely on grammar assumptions that are not always valid (`visibility_modifier` as a field, signature slicing by bytes then string index).
2. The v1 proposal recommends ranking changes from enrichment signals, which conflicts with CQ's fail-open additive enrichment contract.
3. The current design can be materially improved by making ast-grep-py the primary semantic extraction path and using tree-sitter-rust as targeted context augmentation where ast-grep is weak (macro token trees, query-pack metadata, fine-grained scoped captures).
4. The current enrichment runtime has correctness and stability risks (cache staleness, byte-vs-column coordinates, unbounded cache growth) that should be fixed before feature expansion.

## Corrections to v1 Proposal

## C1. Preserve additive enrichment contract strictly

The v1 Phase 4 suggestion to use visibility in grouping/ranking should be removed.

Reason:
1. Current module contract in `tools/cq/search/tree_sitter_rust.py` states enrichment must not affect confidence, counts, classification, or ranking.
2. Ranking side effects create hard-to-debug regressions in search stability and golden output.

Required adjustment:
1. Keep enrichment fields in `details.data["rust_tree_sitter"]` only.
2. Only allow display-level fallback behavior already in place (for example, `containing_scope` fill-in), and keep that behavior deterministic and optional.

## C2. Fix signature extraction logic

The sample implementation slices decoded strings with byte offsets.

Reason:
1. Byte offsets and Python string indices are not equivalent for non-ASCII source.
2. Proposed expression `source_bytes[:sig_end].decode(...)[fn_node.start_byte:sig_end]` is incorrect and can corrupt output.

Required adjustment:
1. Slice bytes first, then decode:
   - `sig_bytes = source_bytes[fn_node.start_byte:body.start_byte]`
   - `signature = sig_bytes.decode("utf-8", errors="replace").strip()`

## C3. Correct visibility extraction assumptions

The v1 sample assumes `visibility_modifier` is always a field.

Reason:
1. Rust grammar usage in the deep dive indicates visibility is often represented as child node shape rather than guaranteed field in every item kind.
2. Field-only lookup can silently miss valid visibility data.

Required adjustment:
1. Use field lookup first, then child-kind fallback for visibility.
2. Normalize to canonical values: `pub`, `pub(crate)`, `pub(super)`, `private`.

## C4. Attribute extraction needs robust sibling/trivia handling

The v1 sibling-walk approach is useful but incomplete.

Reason:
1. Real code may include comments/doc comments between attributes and items.
2. Inner attributes and doc-comment forms need explicit handling rules.

Required adjustment:
1. Define exact attach policy:
   - include contiguous leading outer attributes for the target item,
   - exclude detached comments unless explicitly promoted to tags,
   - include doc comments as separate normalized tags if enabled.

## C5. Coordinate correctness must be byte-safe

Current path feeds ripgrep `submatches.start/end` into span cols and then uses line/col for tree-sitter point lookups.

Reason:
1. Ripgrep submatch positions are byte offsets.
2. Tree-sitter point columns are character-based positions.
3. Unicode source can desynchronize offsets.

Required adjustment:
1. Derive enrichment anchor from byte offsets where available.
2. Maintain a per-line byte-to-column mapping helper.
3. Prefer `named_descendant_for_byte_range` for exact byte-anchored lookup where possible.

## C6. Cache strategy needs staleness and memory controls

Current `_TREE_CACHE` is keyed only by `cache_key` and unbounded.

Reason:
1. Same `cache_key` can serve stale trees after file changes.
2. Unbounded cache can grow indefinitely on large repos.

Required adjustment:
1. Key entries by `(cache_key, source_hash)` or verify source hash before reuse.
2. Add bounded LRU behavior for parsed trees.
3. Expose counters for cache hits, misses, evictions in debug diagnostics.

## Optimized Architecture (Ast-grep first, tree-sitter targeted)

## A1. Two-tier extraction pipeline

1. Tier 1 (default): ast-grep-py extraction from existing parsed `SgRoot`.
2. Tier 2 (optional): tree-sitter-rust augmentation only when Tier 1 cannot provide requested fields.

Why this is optimal:
1. CQ already parses Rust through ast-grep in search/classifier and rule scanning.
2. Avoids redundant parsing and keeps primary logic aligned across Python and Rust surfaces.
3. Uses tree-sitter where it is uniquely strong: query packs, injection metadata, token-tree scoped analysis.

## A2. Unified enrichment contract model

Define explicit field groups:

1. `identity`:
   - `node_kind`
   - `item_role`
   - `language`
2. `scope`:
   - `scope_chain`
   - `scope_kind`
   - `scope_name`
   - `impl_context` (`impl_type`, `impl_trait`, `impl_kind`)
3. `signature`:
   - `signature`
   - `params`
   - `return_type`
   - `generics`
   - `is_async`
   - `is_unsafe`
4. `modifiers`:
   - `visibility`
   - `attributes`
5. `call`:
   - `call_target`
   - `call_receiver`
   - `call_method`
   - `macro_name`
6. `shape`:
   - `struct_field_count`
   - `struct_fields`
   - `enum_variant_count`
   - `enum_variants`
7. `runtime`:
   - `enrichment_status` (`applied|skipped|degraded`)
   - `enrichment_sources` (`["ast_grep"]`, `["ast_grep","tree_sitter"]`)
   - `degrade_reason` (optional)

## A3. Deterministic precedence rules

1. Use ast-grep value when present and non-empty.
2. Fill gaps from tree-sitter.
3. Never override an existing non-empty value unless explicitly configured as canonical override.

This avoids conflicting output across identical matches.

## Additional Improvement Opportunities from Reference Capabilities

## O1. Leverage node-types as a linted schema for extraction code

From `tree-sitter-rust.md`: `node-types.json` is the canonical node and field schema.

Opportunity:
1. Add a CQ test utility that validates each referenced node kind and field name in enrichment code.
2. Fail CI on unknown kinds/fields after grammar upgrades.

Impact:
1. Prevents silent breakage when tree-sitter-rust updates.

## O2. Query-pack driven enrichment micro-rules

From `tree-sitter-rust.md`: use `.scm` packs plus `Query.pattern_settings`, `pattern_assertions`, capture metadata.

Opportunity:
1. Introduce minimal CQ-owned enrichment packs under `tools/cq/search/queries/rust/`:
   - `enrichment_calls.scm`
   - `enrichment_attrs.scm`
   - `enrichment_impl.scm`
2. Use `#set!` metadata for stable capture semantics and routing.
3. Enforce rooted/local pattern constraints for range-bounded execution.

Impact:
1. Better maintainability than long imperative node-walk code for complex capture logic.

## O3. Injection-aware macro token-tree analysis

From `tree-sitter-rust.md`: Rust injections include token-tree parsing intent.

Opportunity:
1. Add opt-in extraction for macro token-tree context:
   - detect injected Rust fragments in macro bodies,
   - expose concise tags like `macro_body_has_match_expression`.
2. Keep this bounded and disabled by default for heavy paths.

Impact:
1. Substantial context gain for macro-heavy Rust code with controlled cost.

## O4. Ast-grep context/selector templates for Rust fragment parity

From `ast-grep-py_rust_deepdive.md`: Rust fragment matching should prefer `context + selector`.

Opportunity:
1. Extend `tools/cq/astgrep/rules_rust.py` with high-value selector-based rules for:
   - methods inside impl,
   - function signatures without bodies (`function_signature_item`),
   - attribute-bearing items,
   - scoped calls and turbofish forms.
2. Reuse these rules for both classification and enrichment bootstrap.

Impact:
1. Higher parity with Python query surfaces using one structural mechanism.

## O5. Cross-check mode for extraction confidence

Opportunity:
1. Add optional debug mode that compares ast-grep and tree-sitter extracted fields for the same match.
2. Emit mismatch diagnostics in test/dev mode only.

Impact:
1. Faster hardening and safer evolution of enrichment rules.

## O6. Bounded payload policy and truncation metadata

Opportunity:
1. Add explicit caps and truncation flags:
   - max attributes,
   - max params,
   - max shape members,
   - max per-field string length.
2. Include per-field truncation booleans in runtime metadata.

Impact:
1. Prevents output bloat and keeps evidence payload predictable.

## O7. Telemetry for practical quality and cost control

Opportunity:
1. Record counters in summary/artifacts:
   - enrichment applied/degraded/skipped counts,
   - cache hit/miss stats,
   - average enrichment latency per match.

Impact:
1. Enables data-driven tuning for quality vs latency.

## Recommended Delivery Sequence

1. Fix correctness/stability first:
   - C2, C5, C6.
2. Add contract and precedence rules:
   - A2, A3, O6.
3. Expand ast-grep-first extraction:
   - O4 and selected v1 recommendations.
4. Add tree-sitter augmentation where ast-grep is weak:
   - O2, O3.
5. Add observability and hardening:
   - O1, O5, O7.

## Test Additions Recommended

1. Unicode coordinate tests:
   - verify byte offsets map correctly to tree-sitter target nodes.
2. Cache staleness tests:
   - same `cache_key`, changed source must produce updated payload.
3. Deterministic precedence tests:
   - ast-grep vs tree-sitter conflicting values resolve consistently.
4. Payload cap tests:
   - truncation flags set when limits are reached.
5. Schema-lint tests:
   - referenced node kinds/fields must exist in pinned grammar schema.
6. Macro token-tree optional tests:
   - enabled path enriches; disabled path is unchanged.

## Suggested Acceptance Criteria

1. No change to search ranking, confidence, or match counts from enrichment.
2. Rust enrichment remains fail-open under parser/query failures.
3. Enrichment payload stays bounded and deterministic across repeated runs.
4. Ast-grep provides primary semantic fields for shared Rust query semantics.
5. Tree-sitter augmentation adds measurable context without correctness regressions.
