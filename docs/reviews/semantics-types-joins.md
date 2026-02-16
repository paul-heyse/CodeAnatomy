# Design Review: Schema, Type System & Join Inference

**Date:** 2026-02-16
**Scope:** `src/semantics/schema.py`, `src/semantics/types/`, `src/semantics/joins/`, `src/semantics/join_helpers.py`, `src/semantics/column_types.py`
**Focus:** All principles (1-24), with special attention to Open/Closed in the type system, DRY between schema operations, information hiding of join internals, dependency direction, and strategy dispatch correctness
**Depth:** deep
**Files reviewed:** 10

## Executive Summary

This subsystem is well-architected with clear separation between schema discovery (`SemanticSchema`), semantic typing (`SemanticType`/`CompatibilityGroup`), and join strategy inference. The layered design -- column types at the bottom, annotated schemas in the middle, join inference at the top -- follows correct dependency direction. However, the subsystem suffers from a significant DRY violation: two parallel type classification systems (`ColumnType` and `SemanticType`) encode overlapping knowledge with divergent pattern-matching logic. Additionally, a duplicate `TableType`-to-entity mapping in `catalog/tags.py`, dead code in `SemanticSchema`, and a subtle correctness issue in `require_join_strategy` (double inference) warrant attention. The join strategy dispatch is robust and well-tested.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `SemanticSchema.column_types` dict is public; `_`-prefixed predicates expose internals inconsistently |
| 2 | Separation of concerns | 3 | - | - | Schema discovery, typing, and join logic are cleanly separated |
| 3 | SRP | 2 | medium | medium | `SemanticSchema` mixes discovery, validation, and DataFusion expression generation |
| 4 | High cohesion, low coupling | 2 | medium | low | Two parallel type systems (`ColumnType` vs `SemanticType`) with overlapping semantics |
| 5 | Dependency direction | 3 | - | - | Types do not depend on joins; joins depend on types; correct layering |
| 6 | Ports & Adapters | 3 | - | - | N/A for this subsystem; DataFusion coupling is appropriately deferred via lazy imports |
| 7 | DRY | 1 | medium | medium | Two column-type classification systems; duplicate `TableType`-to-entity mapping |
| 8 | Design by contract | 2 | small | low | `require_*` methods provide good contracts; but `JoinStrategy.confidence` has no enforced range |
| 9 | Parse, don't validate | 2 | small | low | `SemanticSchema.from_df` parses once; but compiler re-scans `column_types` dict for span candidates |
| 10 | Make illegal states unrepresentable | 2 | small | low | `JoinStrategy` allows empty `left_keys`/`right_keys` which are invalid at runtime |
| 11 | CQS | 3 | - | - | Clean separation; all inference functions are pure queries |
| 12 | DI + explicit composition | 3 | - | - | `SemanticConfig` injected; `AnnotatedSchema` constructed from arrow schemas |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition via dataclasses |
| 14 | Law of Demeter | 2 | small | low | Compiler reaches into `info.sem.column_types.items()` bypassing schema methods |
| 15 | Tell, don't ask | 2 | small | low | Compiler asks schema for raw data then reimplements ambiguity check |
| 16 | Functional core, imperative shell | 3 | - | - | All inference is pure; IO at edges only |
| 17 | Idempotency | 3 | - | - | All operations are stateless; same inputs produce same outputs |
| 18 | Determinism | 3 | - | - | Strategy priority order is explicit and fixed |
| 19 | KISS | 2 | small | low | `_infer_with_hint` repeats patterns that mirror `_infer_default` |
| 20 | YAGNI | 2 | small | low | Dead code: `_has_ambiguous_span`, `_span_start_candidates`, `_span_end_candidates` unused outside `schema.py` |
| 21 | Least astonishment | 2 | small | medium | `require_join_strategy` called after `infer_join_strategy_with_confidence` returns None -- but it will also return None, then raise |
| 22 | Public contracts | 2 | small | low | `__all__` declarations are thorough; but `column_types` dict on `SemanticSchema` lacks access control |
| 23 | Design for testability | 3 | - | - | Comprehensive test suite; pure functions; no IO dependencies |
| 24 | Observability | 2 | small | low | Join confidence logged in compiler; but no structured logging in inference module itself |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`SemanticSchema` exposes its internal `column_types: dict[str, ColumnType]` as a public attribute on a frozen dataclass. The compiler directly iterates this dict at `src/semantics/compiler.py:724-733` to re-derive span candidates, bypassing the schema's own methods.

**Findings:**
- `src/semantics/schema.py:59` -- `column_types` is a public field on a frozen dataclass. While frozen prevents mutation, consumers can (and do) iterate the raw dict.
- `src/semantics/compiler.py:724-733` -- The compiler re-scans `sem.column_types.items()` filtering by `ColumnType.SPAN_START` and `ColumnType.SPAN_END`, duplicating the logic in `SemanticSchema._span_start_candidates()` and `_span_end_candidates()`.
- `src/semantics/schema.py:183-231` -- Predicate methods are `_`-prefixed (private) but `require_evidence`, `require_entity`, `require_symbol_source` are public. The naming convention is inconsistent.

**Suggested improvement:**
Make `column_types` a private field (`_column_types`) and expose only typed accessor methods. The compiler at line 724 should call `sem._span_start_candidates()` or, better, a public `span_candidates()` method, rather than reimplementing the scan.

**Effort:** small
**Risk if unaddressed:** low

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
`SemanticSchema` has three distinct responsibilities: (1) column discovery from DataFrame schemas, (2) schema validation/requirement checking, and (3) DataFusion expression generation (span_expr, entity_id_expr, etc.).

**Findings:**
- `src/semantics/schema.py:69-177` -- Discovery logic in `from_df`.
- `src/semantics/schema.py:233-311` -- Validation via `require_*` methods.
- `src/semantics/schema.py:470-567` -- Expression generation (depends on DataFusion and UDF runtime).

**Suggested improvement:**
Extract expression generation into a separate `SemanticExprBuilder` that takes a `SemanticSchema` as input. This separates the DataFrame/UDF concern from the pure schema concern. The schema class would change only for column-discovery reasons; the expression builder would change only for DataFusion API reasons.

**Effort:** medium
**Risk if unaddressed:** medium -- DataFusion UDF API changes currently force changes to the schema discovery class.

---

### Category: Knowledge

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The subsystem contains two parallel column classification systems that encode the same domain knowledge with different representations and subtly different logic.

**Findings:**

1. **Parallel type enums:**
   - `src/semantics/column_types.py:19-31` -- `ColumnType` enum (PATH, FILE_ID, SPAN_START, SPAN_END, ENTITY_ID, SYMBOL, TEXT, EVIDENCE, NESTED, OTHER).
   - `src/semantics/types/core.py:21-51` -- `SemanticType` enum (FILE_ID, PATH, ENTITY_ID, SPAN_START, SPAN_END, LINE_NO, COL_NO, SYMBOL, QNAME, ORIGIN, CONFIDENCE, EVIDENCE_TIER, UNKNOWN).
   - Both classify columns semantically but with divergent category boundaries. `ColumnType.EVIDENCE` maps to three distinct `SemanticType` values (ORIGIN, CONFIDENCE, EVIDENCE_TIER). `SemanticType` has LINE_NO and COL_NO that `ColumnType` lacks.

2. **Parallel pattern-matching logic:**
   - `src/semantics/column_types.py:36-45` -- `TYPE_PATTERNS` uses compiled regexes.
   - `src/semantics/types/core.py:199-211` -- `_INFERENCE_PATTERNS` uses a custom string-based pattern DSL (`exact:`, `suffix:`, `contains:`).
   - Both match column names to semantic types, but use completely different matching mechanisms.

3. **Duplicate `TableType`-to-entity-grain mapping:**
   - `src/semantics/catalog/tags.py:171-177` -- `_infer_entity_from_schema()` defines `mapping = {TableType.RELATION: ("edge", "per_edge"), ...}`.
   - `src/semantics/catalog/tags.py:183-189` -- `_infer_entity_grain_from_table_type()` defines the identical mapping. These two functions contain byte-for-byte identical mapping dictionaries.

**Suggested improvement:**
(a) Define a single canonical column classification that maps to the richer `SemanticType` system, with a computed `ColumnType`-equivalent grouping derivable from `SemanticType`. This eliminates the need for two pattern-matching systems.
(b) Extract the `TableType`-to-entity-grain mapping in `catalog/tags.py` into a single module-level constant and have both functions reference it.

**Effort:** medium (for (a)), small (for (b))
**Risk if unaddressed:** medium -- Drift between the two classification systems could cause join inference to disagree with schema discovery, producing incorrect joins silently.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The `require_*` methods on `SemanticSchema` provide good design-by-contract patterns. However, `JoinStrategy.confidence` lacks invariant enforcement.

**Findings:**
- `src/semantics/joins/strategies.py:64` -- `confidence: float = 1.0` has no validation that it is in [0.0, 1.0]. The docstring claims the range but nothing enforces it.
- `src/semantics/joins/inference.py:597` -- `build_join_inference_confidence` uses `strategy.confidence >= _HIGH_CONFIDENCE_THRESHOLD` to route to `high_confidence` vs `low_confidence`, but `high_confidence` clamps to `[0.8, 1.0]` and `low_confidence` clamps to `[0.0, 0.49]`. Values in [0.5, 0.8) -- like `SYMBOL_CONFIDENCE = 0.75` -- get clamped to 0.49 via `low_confidence`, silently losing precision.

**Suggested improvement:**
Add a `__post_init__` to `JoinStrategy` that validates `0.0 <= confidence <= 1.0`. Consider whether the gap between 0.5 and 0.8 in `InferenceConfidence` is intentional or if a `medium_confidence` constructor is needed.

**Effort:** small
**Risk if unaddressed:** low -- The clamping behavior is currently consistent because all concrete confidence values are either above 0.8 or exactly 0.6.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`SemanticSchema.from_df` performs a thorough parse-once at the boundary. However, the compiler re-validates and re-derives information from the already-parsed schema.

**Findings:**
- `src/semantics/compiler.py:724-733` -- The compiler re-scans `sem.column_types.items()` for span candidates instead of using the already-computed `_span_start` and `_span_end` fields, which were resolved during `from_df` parsing.
- `src/semantics/schema.py:397-431` -- `_span_start_candidates()`, `_span_end_candidates()`, and `_has_ambiguous_span()` exist on the class and perform the same scan, but the compiler does not use them.

**Suggested improvement:**
Have the compiler call `sem._has_ambiguous_span()` (or a public equivalent) instead of reimplementing the span candidate scan. Better yet, make ambiguity detection part of the parsing phase in `from_df` so it is computed once and stored.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`JoinStrategy` allows construction with empty `left_keys` and `right_keys`, which is invalid for all strategy types at runtime.

**Findings:**
- `src/semantics/joins/strategies.py:60-63` -- `left_keys: tuple[str, ...]` and `right_keys: tuple[str, ...]` can be empty tuples.
- `src/semantics/compiler.py:487-489` -- The compiler checks `if not left_keys or not right_keys` and raises, meaning this is a runtime validation rather than a construction-time invariant.

**Suggested improvement:**
Add a `__post_init__` validation on `JoinStrategy` that verifies `left_keys` and `right_keys` are non-empty. This shifts the error from runtime dispatch to construction time.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
The compiler reaches through multiple layers to access schema internals.

**Findings:**
- `src/semantics/compiler.py:724-727` -- `sem.column_types.items()` is a two-level reach-through: compiler -> SemanticSchema -> internal dict -> items iteration.
- `src/semantics/compiler.py:340` -- `left_info.annotated.infer_join_keys(right_info.annotated)` -- acceptable, but `left_info.annotated.get(left_col_name)` at line 351 reaches through `TableInfo` -> `AnnotatedSchema` -> column lookup.

**Suggested improvement:**
Add a `has_ambiguous_spans()` public method to `SemanticSchema` so the compiler does not need to iterate `column_types` directly. The `_resolve_join_keys` method could accept the two `AnnotatedSchema` objects directly rather than reaching through `TableInfo`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The compiler asks `SemanticSchema` for raw data and then reimplements logic that the schema already knows.

**Findings:**
- `src/semantics/compiler.py:724-740` -- The compiler scans `column_types` for ambiguous spans, constructs a diagnostic message, and raises. This logic duplicates what `_has_ambiguous_span()` already computes. The schema should be told "reject ambiguous spans" rather than being asked for raw data.

**Suggested improvement:**
Add a `require_unambiguous_spans(table: str)` method to `SemanticSchema` that raises `SemanticSchemaError` with the diagnostic. The compiler would call `sem.require_unambiguous_spans(table=table_name)` as a single statement.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 3/3

**Current state:**
All inference functions (`infer_join_strategy`, `infer_column_type`, `infer_semantic_type`, `infer_table_type`) are pure. IO and DataFusion interactions are confined to the compiler's imperative shell. This is well-executed.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All operations are stateless and idempotent. `SemanticSchema.from_df` produces the same result for the same DataFrame. Join inference is purely functional.

---

#### P18. Determinism -- Alignment: 3/3

**Current state:**
Strategy priority order is explicit and fixed in `_infer_default` (`src/semantics/joins/inference.py:363-415`). Pattern matching uses ordered tuples, ensuring first-match-wins determinism.

---

### Category: Simplicity

#### P19. KISS -- Alignment: 2/3

**Current state:**
The inference module has some structural repetition between hint-guided and default inference paths.

**Findings:**
- `src/semantics/joins/inference.py:303-360` -- `_infer_with_hint` reimplements each strategy check with if/elif chains.
- `src/semantics/joins/inference.py:363-415` -- `_infer_default` has a similar cascade.
- The confidence assignment pattern `replace(strategy, confidence=...)` is repeated 7 times across both functions.

**Suggested improvement:**
Define a strategy resolver table: `_STRATEGY_RESOLVERS: tuple[tuple[JoinStrategyType, Callable, float], ...]` mapping each strategy type to its capability check, resolver function, and confidence score. Both `_infer_with_hint` and `_infer_default` would iterate this table, simplifying the code.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Several methods on `SemanticSchema` are defined but never called outside the class.

**Findings:**
- `src/semantics/schema.py:423-431` -- `_has_ambiguous_span()` is defined but never called anywhere in the codebase (the compiler reimplements the logic instead).
- `src/semantics/schema.py:397-421` -- `_span_start_candidates()` and `_span_end_candidates()` are only called by `_has_ambiguous_span()`, which is itself unused.

**Suggested improvement:**
Either (a) make the compiler use `_has_ambiguous_span()` (preferred, see P15 suggestion), or (b) remove the dead methods if they truly serve no purpose. Option (a) is better because it satisfies P15 simultaneously.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
The compiler's fallback pattern for join strategy inference is surprising.

**Findings:**
- `src/semantics/compiler.py:898-910` -- The compiler calls `infer_join_strategy_with_confidence(...)`. If that returns `None`, it calls `require_join_strategy(...)`. But `require_join_strategy` internally calls `infer_join_strategy` again (same function, same inputs). Since `infer_join_strategy_with_confidence` delegates to `infer_join_strategy`, the second call is guaranteed to return `None` again, meaning `require_join_strategy` will always raise `JoinInferenceError`. This double-inference is wasteful and confusing -- a reader would expect the fallback to try something different.

**Suggested improvement:**
Replace the pattern with direct exception raising when `infer_join_strategy_with_confidence` returns `None`. Build the diagnostic message inline using `JoinCapabilities.from_schema` and `_format_capabilities`, avoiding the redundant second inference pass.

**Effort:** small
**Risk if unaddressed:** medium -- The double-inference wastes CPU on schema analysis and confuses maintainers about whether the fallback path adds value.

---

### Category: Quality

#### P23. Design for testability -- Alignment: 3/3

**Current state:**
Excellent test coverage. `tests/unit/semantics/test_joins.py` (465 lines) covers all strategy types, capabilities extraction, confidence building, immutability, and error paths. `tests/unit/semantics/test_join_inference.py` (457 lines) tests compiler-level join key resolution with parity tests between compiler and IR pipeline. All inference functions are pure and testable without mocks.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The compiler logs join confidence via `logger.debug` at `src/semantics/compiler.py:916-924`. However, the inference module itself has no structured logging.

**Findings:**
- `src/semantics/joins/inference.py` -- No logging at all. When join inference fails silently (returns `None`), there is no trace of what was attempted.
- `src/semantics/compiler.py:916-924` -- Confidence is logged only when inference succeeds, not when it fails.

**Suggested improvement:**
Add structured debug logging to `_infer_default` and `_infer_with_hint` for each strategy check, recording which strategies were considered and rejected. This enables diagnosis of unexpected join strategy selections without debugger attachment.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Parallel Type Systems Create Drift Risk

The existence of both `ColumnType` (in `column_types.py`) and `SemanticType` (in `types/core.py`) is the most significant structural issue. Both serve the same fundamental purpose -- classifying columns by semantic role -- but they use different enum values, different pattern-matching mechanisms, and different granularity levels. Today they are loosely aligned, but they encode the same business truth in two places. When a new column naming convention is introduced, both systems must be updated independently, and a missed update could cause join inference to disagree with schema discovery.

**Affected principles:** P7 (DRY), P4 (cohesion), P21 (least astonishment)

**Suggested approach:** Designate `SemanticType` as the canonical classification and derive `ColumnType` categories as computed groupings. Replace `infer_column_type` with a thin wrapper that maps `SemanticType` to the coarser `ColumnType` groups. This preserves backward compatibility while eliminating the second pattern-matching system.

### Theme 2: Schema Knows Too Much

`SemanticSchema` combines schema discovery, validation, and DataFusion expression generation. This creates a "God class" tendency where the schema object must change for three different reasons. The expression generation methods (`span_expr`, `entity_id_expr`, `path_col`, etc.) depend on DataFusion and UDF runtime, pulling framework concerns into what should be a pure data structure.

**Affected principles:** P3 (SRP), P5 (dependency direction), P1 (information hiding)

**Suggested approach:** Extract expression generation into a separate `SemanticExprBuilder` that accepts a `SemanticSchema` and produces DataFusion expressions. This is a non-breaking change since `SemanticSchema` can retain the methods as delegating wrappers during migration.

### Theme 3: Compiler Bypasses Schema Encapsulation

Multiple places in the compiler reach into `SemanticSchema` internals (iterating `column_types`, re-deriving span candidates) rather than using the schema's own methods. This creates coupling between the compiler and the schema's internal representation.

**Affected principles:** P14 (Law of Demeter), P15 (Tell, don't ask), P9 (Parse, don't validate)

**Suggested approach:** Add public methods to `SemanticSchema` for the operations the compiler needs (e.g., `require_unambiguous_spans()`, `has_ambiguous_spans()`) and update the compiler to use them. This is a small refactor with outsized encapsulation benefits.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract duplicate `TableType`-to-entity mapping in `catalog/tags.py:171-189` into a single constant | small | Eliminates exact duplication of 4-entry mapping |
| 2 | P15 (Tell, don't ask) | Add `require_unambiguous_spans(table)` to `SemanticSchema`; compiler calls it instead of reimplementing span candidate scan at `compiler.py:724-740` | small | Fixes P14, P15, P20 simultaneously (makes dead methods live) |
| 3 | P21 (Least astonishment) | Remove double-inference in `compiler.py:898-910`; raise `JoinInferenceError` directly when `infer_join_strategy_with_confidence` returns None | small | Eliminates confusing redundant computation |
| 4 | P10 (Illegal states) | Add `__post_init__` validation to `JoinStrategy` for non-empty keys and confidence range | small | Shifts runtime errors to construction time |
| 5 | P19 (KISS) | Extract strategy-resolver table in `inference.py` to eliminate duplicated hint/default cascade | small | Reduces ~120 lines of if/elif to a data-driven dispatch |

## Recommended Action Sequence

1. **Extract duplicate `TableType` mapping** (P7, quick win #1). In `src/semantics/catalog/tags.py`, define a single `_TABLE_TYPE_TO_ENTITY_GRAIN` constant at module level and have both `_infer_entity_from_schema` and `_infer_entity_grain_from_table_type` reference it. No API changes.

2. **Add `require_unambiguous_spans` to `SemanticSchema`** (P15, P14, P20, quick win #2). This makes the existing dead methods (`_span_start_candidates`, `_span_end_candidates`, `_has_ambiguous_span`) serve a purpose and simplifies `compiler.py:724-740` to a single method call.

3. **Fix double-inference in compiler** (P21, quick win #3). Replace the `infer_join_strategy_with_confidence` + `require_join_strategy` pattern with a single inference call followed by direct error construction.

4. **Add `__post_init__` validations to `JoinStrategy`** (P10, P8, quick win #4). Enforce non-empty keys and confidence range `[0.0, 1.0]`.

5. **Simplify inference dispatch** (P19, quick win #5). Extract a strategy-resolver table in `inference.py` to unify `_infer_with_hint` and `_infer_default`.

6. **Unify type classification** (P7, Theme 1). This is the highest-impact but highest-effort change. Designate `SemanticType` as canonical, derive `ColumnType` groupings, and retire the parallel pattern-matching in `column_types.py`. This should be done after items 1-5 to avoid conflating structural refactors with API changes.

7. **Extract `SemanticExprBuilder`** (P3, Theme 2). Separate DataFusion expression generation from `SemanticSchema`. This is a medium-effort change that improves SRP and dependency direction, but is lower urgency since the current design works correctly.
