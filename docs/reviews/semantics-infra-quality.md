# Design Review: src/semantics/ -- Quality, Normalization & Infrastructure

**Date:** 2026-02-16
**Scope:** `src/semantics/` infrastructure subsystem (~6,000 LOC across 28 files)
**Focus:** All principles (1-24), with special attention to DRY, SRP, information hiding, dependency direction, and stats/metrics/signals cohesion
**Depth:** deep (all files in scope)
**Files reviewed:** 28

## Executive Summary

The infrastructure subsystem demonstrates strong architectural foundations -- the quality spec model (`quality.py`, `quality_specs.py`, `quality_templates.py`) is well-structured with immutable frozen dataclasses and a clean three-tier signal model. The normalization modules (`span_normalize.py`, `scip_normalize.py`, `bytecode_line_table.py`) correctly enforce the byte-span canonical invariant. However, three systemic issues reduce overall alignment: (1) duplicated knowledge across `_table_exists`, `_byte_offset`, and line-index join patterns in three separate modules; (2) the diagnostics module (773 LOC) mixes multiple distinct responsibilities -- metrics aggregation, coverage reporting, issue batching, and schema anomaly detection; and (3) the `metrics.py` and `stats/collector.py` modules appear to be dead or near-dead code with no external callers.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `naming.py` uses `getattr` on `object` to avoid circular imports, leaking structural knowledge |
| 2 | Separation of concerns | 1 | medium | medium | `diagnostics.py` mixes 5 distinct concerns in one 773-LOC file |
| 3 | SRP | 1 | medium | medium | `diagnostics.py` changes for quality metrics, coverage, issue batching, schema anomalies, and view builders |
| 4 | High cohesion, low coupling | 2 | medium | medium | stats/metrics/signals triad has overlapping purposes; `_table_exists` duplicated |
| 5 | Dependency direction | 2 | small | low | Core normalization modules correctly depend inward; minor inward leak from `diagnostics.py` importing `registry` |
| 6 | Ports & Adapters | 2 | small | low | DataFusion interaction is well-contained; no port abstraction for table-existence checking |
| 7 | DRY | 1 | medium | medium | `_table_exists` duplicated 3x; `_byte_offset` duplicated 2x; line-index join pattern duplicated 2x |
| 8 | Design by contract | 2 | small | low | Good contract types (`SpanNormalizeConfig`, `PlanFingerprint`, validation result); some implicit contracts in naming |
| 9 | Parse, don't validate | 2 | small | low | `config.py` properly parses spec to runtime config; `naming.py` identity pass-through reduces value |
| 10 | Make illegal states unrepresentable | 2 | small | low | Frozen dataclasses and Literal types; `ViewKind` StrEnum consolidates kind values |
| 11 | CQS | 2 | small | low | Most functions are pure queries; `PipelineMetrics.add_operation` is a clean command |
| 12 | DI + explicit composition | 2 | small | low | `build_semantic_execution_context` takes deps as params; deferred imports for composition |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition-based |
| 14 | Law of Demeter | 2 | small | low | `_join_if_present` in signals.py reaches deeply into DataFrame schema internals |
| 15 | Tell, don't ask | 2 | small | low | Good in quality model; diagnostics asks about schema names extensively |
| 16 | Functional core, imperative shell | 2 | small | low | Normalization is pure transform; diagnostics mixes IO (ctx.table) with logic |
| 17 | Idempotency | 3 | - | - | All builders produce same output for same inputs; compile tracker is correctly scoped |
| 18 | Determinism | 3 | - | - | Fingerprints ensure deterministic plan hashing; sorted output in payloads |
| 19 | KISS | 2 | small | low | `fingerprints.py` has excessive defensive getattr chains; `_join_if_present` over-engineered alias logic |
| 20 | YAGNI | 1 | small | low | `metrics.py` and `stats/collector.py` appear unused; `ViewKindParams` is speculative |
| 21 | Least astonishment | 2 | small | low | `naming.py` identity mapping is surprising given its name implies transformation |
| 22 | Public contracts | 2 | small | low | Good `__all__` exports; `ViewKindStr` Literal kept in sync with `ViewKind` enum |
| 23 | Testability | 2 | small | low | Pure dataclasses are testable; DataFusion-coupled builders require session setup |
| 24 | Observability | 2 | small | low | Consistent `stage_span` usage in normalization; diagnostics lacks structured logging |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The normalization modules hide their internal span-resolution strategies behind clean interfaces (`normalize_byte_span_df`, `scip_to_byte_offsets`). The quality spec model hides scoring internals behind the `QualityRelationshipSpec` dataclass.

**Findings:**
- `src/semantics/naming.py:42` uses `getattr(manifest, "output_name_map", None)` with `manifest` typed as `object` to avoid circular imports. This hides the actual type but also prevents type checking from catching misuse.
- `src/semantics/naming.py:85` similarly uses `getattr(view, "name", "")` with `views` typed as `tuple[object, ...]`. The actual `SemanticIRView` type is hidden entirely from callers.
- `src/semantics/diagnostics.py:109-132` exposes `_RELATIONSHIP_DIAG_SCHEMA` as a module-level tuple computed at import time by reaching into `RELATIONSHIP_SPECS`. This is a stable internal, but computed eagerly from the registry.

**Suggested improvement:**
In `naming.py`, introduce a minimal Protocol (e.g., `HasOutputNameMap`) that captures just the `output_name_map` attribute, allowing type-safe access without circular imports. This replaces `object` with a narrow structural type.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`diagnostics.py` at 773 LOC is the most significant SoC violation in scope. It contains five distinct responsibilities:

1. Quality metrics aggregation (`build_relationship_quality_metrics`, lines 153-267)
2. File coverage reporting (`build_file_coverage_report`, lines 270-371)
3. Ambiguity analysis (`build_ambiguity_analysis`, lines 374-483)
4. Issue batching (`semantic_quality_issue_batches`, `_issue_batches`, lines 618-717)
5. Schema anomaly detection (`build_schema_anomalies_view`, lines 528-566)

These are unified only by "diagnostics" as a theme, but change for very different reasons.

**Findings:**
- `src/semantics/diagnostics.py:486-514` (`build_relationship_candidates_view`) imports `RELATIONSHIP_SPECS` from registry -- a dependency that only the candidates/decisions builders need, not the quality metrics or coverage functions.
- `src/semantics/diagnostics.py:536-566` (`build_schema_anomalies_view`) imports from `datafusion_engine.schema.catalog_contracts`, `datafusion_engine.schema.contracts`, `datafusion_engine.views.bundle_extraction`, and `semantics.catalog.dataset_specs` -- four imports unique to this one function.
- `src/semantics/diagnostics.py:618-683` (`semantic_quality_issue_batches`) is a dispatcher that uses string matching on `view_name` to route to different issue-extraction logic. Each branch has distinct filtering logic.

**Suggested improvement:**
Split `diagnostics.py` into focused modules:
- `diagnostics/quality_metrics.py` -- relationship quality metrics and summary
- `diagnostics/coverage.py` -- file coverage report
- `diagnostics/ambiguity.py` -- ambiguity analysis
- `diagnostics/issue_batching.py` -- issue batch extraction for obs emission
- `diagnostics/schema_anomalies.py` -- schema contract violation detection
- `diagnostics/__init__.py` -- re-exports for backward compatibility

**Effort:** medium
**Risk if unaddressed:** medium -- the file will continue to accumulate unrelated responsibilities

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
The SRP concern is concentrated in `diagnostics.py` (same as P2) but also appears in `signals.py`.

**Findings:**
- `src/semantics/diagnostics.py` changes when: (a) quality metric formulas change, (b) coverage sources change, (c) ambiguity grouping logic changes, (d) issue emission format changes, (e) schema contracts change. Five reasons to change in one file.
- `src/semantics/signals.py:193-229` (`_join_if_present`) is a generic DataFrame join utility embedded within a file-quality signal builder. It contains 37 lines of defensive join logic including alias collision avoidance that has nothing to do with quality signals.

**Suggested improvement:**
Same split as P2. Additionally, extract `_join_if_present` from `signals.py` into a shared DataFrame utility, or simplify it by relying on DataFusion's join key handling rather than manual alias management.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The stats/metrics/signals triad represents three modules with overlapping purposes:
- `metrics.py` -- pipeline operation timing and row counts
- `stats/collector.py` -- view-level row counts and column statistics
- `signals.py` -- file-level quality signals from extraction sources

**Findings:**
- `src/semantics/metrics.py` (296 LOC) defines `SemanticOperationMetrics` and `PipelineMetrics` but has zero external importers (only its own docstring example). It appears to be dead code.
- `src/semantics/stats/collector.py` (459 LOC) defines `ViewStats`, `ViewStatsCache`, `collect_view_stats`, `estimate_row_count` but also has zero external importers outside its own package (only `stats/__init__.py` re-exports it).
- `src/semantics/signals.py` (385 LOC) is well-used (imported by `pipeline.py` and `catalog/analysis_builders.py`), building a concrete file-quality view. Its `_table_exists` helper at line 53 duplicates `diagnostics.py:36`.

**Suggested improvement:**
Consolidate or remove `metrics.py` and `stats/collector.py` if they have no production callers. If they are aspirational, mark them with a clear `# TODO: Wire into pipeline` comment and reduce them to minimal stubs. The `_table_exists` helper should be extracted to a shared utility (see DRY, P7).

**Effort:** medium
**Risk if unaddressed:** medium -- dead code accumulates and misleads contributors

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Core normalization modules (`span_normalize.py`, `scip_normalize.py`, `bytecode_line_table.py`) correctly depend only on DataFusion engine utilities and observability. The quality spec model (`quality.py`) depends only on `exprs.py` types.

**Findings:**
- `src/semantics/diagnostics.py:495-496` imports `RELATIONSHIP_SPECS` from `semantics.registry` at function scope. This creates a dependency from a cross-cutting diagnostic module back into the core registry, pulling in all relationship spec definitions.
- `src/semantics/diagnostics.py:536-539` imports from four `datafusion_engine` submodules and `semantics.catalog.dataset_specs` inside `build_schema_anomalies_view`. This function depends on a wide cross-section of the system.
- `src/semantics/compile_context.py:9-14` correctly centralizes heavy dependencies (naming, program_manifest) and uses TYPE_CHECKING guards for the rest.

**Suggested improvement:**
The `build_relationship_candidates_view` and `build_schema_anomalies_view` functions should receive their spec/catalog dependencies as parameters rather than importing them internally. This inverts the dependency and makes the diagnostic builders injectable.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
DataFusion interaction is well-contained within view builders. The `SessionContext` is passed as a parameter throughout, which is a form of port.

**Findings:**
- The `_table_exists` function (duplicated in `diagnostics.py:36` and `signals.py:53`) catches five exception types (`KeyError, OSError, RuntimeError, TypeError, ValueError`) to handle DataFusion's varied failure modes. This broad catch masks the absence of a proper "table registry" port.
- `src/semantics/scip_normalize.py:13` and `src/semantics/bytecode_line_table.py:16` use `table_names_snapshot(ctx)` from `datafusion_engine.schema.introspection` -- a cleaner approach that could replace `_table_exists` everywhere.

**Suggested improvement:**
Standardize on `table_names_snapshot(ctx)` as the canonical table-existence check and remove the ad-hoc `_table_exists` pattern. This concentrates the DataFusion adaptation in one place.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Three distinct knowledge-duplication patterns are present in the scope.

**Findings:**

1. **`_table_exists` duplicated 3x:**
   - `src/semantics/diagnostics.py:36-48` -- catches 5 exception types
   - `src/semantics/signals.py:53-65` -- identical implementation
   - `src/semantics/incremental/cdf_reader.py:364` -- variant (out of scope but same pattern)
   - Meanwhile, `scip_normalize.py:71` and `bytecode_line_table.py:94` use the superior `table_names_snapshot` approach.

2. **`_byte_offset` inner function duplicated 2x:**
   - `src/semantics/span_normalize.py:215-222` -- uses configurable `unit_expr` and conditional guard
   - `src/semantics/scip_normalize.py:151-161` -- hardcodes `col("col_unit")` and always includes the null guard
   - Both compute `base + offset` with `udf_expr("col_to_byte", ...)` and the same null-guard pattern. The only difference is how the unit expression is sourced.

3. **Line-index join boilerplate duplicated 2x:**
   - `src/semantics/span_normalize.py:186-211` -- aliases start/end index columns, joins twice
   - `src/semantics/scip_normalize.py:113-145` -- nearly identical alias/join pattern
   - Both produce `start_file_id`, `start_path`, `start_line_no`, `start_line_start_byte`, `start_line_text` (and corresponding `end_*` columns) then join on path+line_no.

**Suggested improvement:**
Extract a shared `line_index_byte_offset_join(df, line_index, *, start_line_col, end_line_col, start_col_col, end_col_col, unit_expr)` function into a new `src/semantics/normalization_helpers.py` module. This function would encapsulate: (a) the start/end alias projection, (b) the two left joins, (c) the `_byte_offset` expression builder, and (d) the cleanup drop. Both `span_normalize.py` and `scip_normalize.py` would call it.

**Effort:** medium
**Risk if unaddressed:** medium -- drift between the two implementations could produce subtle byte-offset discrepancies

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Strong contract types exist: `SpanNormalizeConfig` (frozen dataclass with documented defaults), `PlanFingerprint` (with `matches()` predicate), `SemanticInputValidationResult` (frozen result type), `QualityRelationshipSpec` (frozen with explicit signal tiers).

**Findings:**
- `src/semantics/span_normalize.py:60-117` (`normalize_byte_span_df`) has implicit preconditions: the DataFrame must have certain column combinations, but these are discovered at runtime through the cascading `_resolve_direct_byte_spans` / `_resolve_byte_len_spans` / `_normalize_via_line_index` chain. Only the last fallback raises `ValueError` with a clear message.
- `src/semantics/compile_invariants.py:14-66` (`CompileTracker`) is an exemplary contract enforcement tool -- postcondition checking via `assert_compile_count`.
- `src/semantics/validation/catalog_validation.py:18-33` provides clean result types but `SEMANTIC_INPUT_COLUMN_SPECS` (line 36) is an empty tuple, deferring all validation to the dynamic `_semantic_input_column_specs()` function.

**Suggested improvement:**
Document the column-presence preconditions of `normalize_byte_span_df` more explicitly, perhaps as a docstring table showing which column sets trigger which resolution paths. This is documentation, not code change.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`config.py` demonstrates good parse-don't-validate with `semantic_config_from_spec` converting `SemanticConfigSpec` (serializable) into `SemanticConfig` (runtime, with compiled patterns).

**Findings:**
- `src/semantics/config.py:85-112` (`semantic_config_from_spec`) properly compiles regex patterns from string specs into `re.Pattern` objects at parse time.
- `src/semantics/naming.py:15-45` (`canonical_output_name`) accepts `manifest` as `object | None` and probes it with `getattr`. This bypasses the parse-don't-validate principle -- the function should receive a typed mapping or None, not an opaque object.

**Suggested improvement:**
Change `canonical_output_name` signature to accept `output_name_map: Mapping[str, str] | None = None` directly instead of `manifest: object | None`. Callers already have access to the manifest and can pass `manifest.output_name_map`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Good use of frozen dataclasses, `Literal` types for constrained values (`JoinHow`, `RankKeep`, `FeatureKind`, `SpanUnit`), and `ViewKind` as a `StrEnum`.

**Findings:**
- `src/semantics/view_kinds.py:19-34` (`ViewKindStr`) Literal type is kept manually in sync with `ViewKind` StrEnum (lines 42-63). This dual definition could drift, though the StrEnum provides runtime enforcement.
- `src/semantics/quality.py:191-232` (`RankSpec`) allows `ambiguity_group_id_expr` to be `None` independently of `ambiguity_key_expr`. It is unclear whether a `None` group ID with a non-None key is a valid combination or a bug waiting to happen.

**Suggested improvement:**
Consider auto-generating `ViewKindStr` from `ViewKind` members (e.g., `get_args()` at module level or a test that validates sync). For `RankSpec`, document whether `ambiguity_group_id_expr=None` is intentional or should default to `ambiguity_key_expr`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions are pure queries returning DataFrames or data structures. `PipelineMetrics.add_operation` is a clean command.

**Findings:**
- `src/semantics/metrics.py:162-171` (`PipelineMetrics.add_operation`) mutates `operation_metrics` list and increments `views_created`. These two mutations happen atomically, which is fine, but the coupling of "append" and "increment counter" means the counter could drift if someone appends directly to `operation_metrics`.
- `src/semantics/docs/graph_docs.py:277-303` (`export_graph_documentation`) both generates content (query) and writes to disk (command). The return value is the content, but the side effect is file writing.

**Suggested improvement:**
In `graph_docs.py`, the `export_graph_documentation` function already separates the concerns well (it calls `generate_markdown_docs` then optionally writes). The current design is acceptable -- the file-writing is explicitly controlled by the `output_path` parameter.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`build_semantic_execution_context` in `compile_context.py` is a good composition root that takes `runtime_profile`, `policy`, and optional `facade` as injectable dependencies.

**Findings:**
- `src/semantics/compile_context.py:49-103` receives all major dependencies as parameters. Deferred imports (lines 63-68) are used appropriately for lazy loading.
- `src/semantics/diagnostics.py:495-496` and `diagnostics.py:536-543` use function-level deferred imports to pull in heavyweight dependencies, which is composition by convention rather than explicit injection. The diagnostic builders could receive their spec registries as parameters.

**Suggested improvement:**
Parametrize `build_relationship_candidates_view` and `build_schema_anomalies_view` to accept their spec/catalog dependencies as arguments, improving testability and making the composition explicit.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies in scope. All behavior is composed through frozen dataclasses, function composition, and configuration objects.

**Findings:**
- `quality.py` uses composition of `HardPredicate`, `Feature`, `SignalsSpec`, `RankSpec`, and `SelectExpr` into `QualityRelationshipSpec`.
- `quality_templates.py` uses factory functions (`entity_symbol_relationship`) to compose specs from `EntitySymbolConfig`, avoiding template inheritance.
- `ManifestDatasetResolver` in `program_manifest.py` is a Protocol, not an ABC.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most modules interact with DataFusion via direct `ctx.table(name)` and DataFrame method chains, which is standard for a query-builder API.

**Findings:**
- `src/semantics/signals.py:193-229` (`_join_if_present`) reaches into `signals.schema().names` and `base.schema().names` with fallback for missing `names` attribute (`except AttributeError: signal_columns = []`). This defensive coding pattern suggests uncertainty about the DataFrame API surface.
- `src/semantics/plans/fingerprints.py:143-153` (`_extract_field_type`) chains through `getattr(schema, "field", None)` then `field_method(idx)` then `getattr(field, "type", None)` then conditionally calls or stringifies `type_attr`. This is four levels of Demeter violation driven by schema object polymorphism.

**Suggested improvement:**
In `fingerprints.py`, replace the `_extract_field_type` / `_schema_from_names` / `_schema_to_string` chain with a single function that accepts a `pa.Schema` (PyArrow schema) and uses its well-defined API. The DataFusion `df.schema()` already returns an Arrow-compatible schema, so the polymorphism handling is unnecessary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The quality spec model follows tell-don't-ask well -- callers pass specs to compilation, they don't interrogate raw data.

**Findings:**
- `src/semantics/diagnostics.py:153-267` (`build_relationship_quality_metrics`) extensively asks about schema (`has_confidence`, `has_score`, `has_src`, `has_dst`) and builds different aggregation expressions based on the answers. This is a "ask, then decide" pattern that could be simplified.
- `src/semantics/signals.py:272-378` (`build_file_quality_view`) similarly asks about table existence for each signal source, then branches on the result.

**Suggested improvement:**
These are DataFusion view builders operating on schema metadata, which inherently requires interrogation. The pattern is acceptable here since the "asking" is about data shape, not business logic. No action needed beyond the SRP split.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Quality spec definitions (`quality.py`, `quality_specs.py`, `quality_templates.py`) are purely declarative. Normalization functions are pure transforms given a DataFrame.

**Findings:**
- `src/semantics/diagnostics.py:536-566` (`build_schema_anomalies_view`) mixes IO (table access, schema introspection) with business logic (violation detection, row construction). The core of the function could be a pure function taking specs and schemas as input.
- `src/semantics/compile_invariants.py:68-123` uses module-level mutable state (`_tracker_state`) with thread-safety via `_tracker_lock`. This is a necessary imperative shell construct, correctly isolated.

**Suggested improvement:**
Extract the violation-detection logic from `build_schema_anomalies_view` into a pure function that takes `(specs, schemas) -> list[dict]`, leaving the DataFrame wrapping in the builder.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All view builders produce deterministic outputs for the same session state. `CompileTracker` correctly enforces single-invocation invariants.

**Findings:**
- `src/semantics/compile_invariants.py:74-102` (`compile_tracking`) is a context manager that correctly restores previous tracker state on exit, making it idempotent across nested invocations.
- `src/semantics/program_manifest.py:222-235` (`with_fingerprint`) produces a new manifest with a deterministic fingerprint, not modifying the original.

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Fingerprint computation uses SHA256. Payload serialization sorts keys. ViewKind ordering is deterministic.

**Findings:**
- `src/semantics/plans/fingerprints.py:426-464` (`compute_plan_fingerprint`) uses optimized logical plan text and schema representation for deterministic hashing.
- `src/semantics/program_manifest.py:173-200` (`payload()`) sorts mapping items (`dict(sorted(self.input_mapping.items()))`) for reproducible serialization.
- `src/semantics/view_kinds.py:66-81` (`VIEW_KIND_ORDER`) provides deterministic execution ordering.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are appropriately simple. Two notable exceptions exist.

**Findings:**
- `src/semantics/plans/fingerprints.py:128-208` contains 80 lines of defensive `getattr`/`callable`/`try-except` chains (`_extract_field_type`, `_schema_from_names`, `_schema_to_string`, `_plan_to_string`) to handle schema/plan object polymorphism. Given that DataFusion schemas are always PyArrow schemas, much of this defensive code is unnecessary.
- `src/semantics/signals.py:193-229` (`_join_if_present`) has 37 lines of alias collision avoidance logic (incrementing suffix counters) for a join that should work with DataFusion's built-in key handling.

**Suggested improvement:**
Simplify `fingerprints.py` by assuming PyArrow schema (which DataFusion returns) and removing the polymorphism handling. Simplify `_join_if_present` by using `join_keys` parameter instead of manual aliasing.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 1/3

**Current state:**
Two complete modules appear to be unused aspirational code.

**Findings:**
- `src/semantics/metrics.py` (296 LOC): `SemanticOperationMetrics`, `PipelineMetrics`, and `collect_dataframe_metrics` have no external importers. The `collect_dataframe_metrics` function (line 242-289) receives a `df` parameter it immediately discards (`_ = df  # Acknowledge parameter for future use`).
- `src/semantics/stats/collector.py` (459 LOC): `ViewStats`, `ViewStatsCache`, `collect_view_stats`, `estimate_row_count` have no importers outside `stats/__init__.py`. The `ViewStatsCache` class (178-266) implements a complete cache with invalidation, containment checks, and length -- all unused.
- `src/semantics/view_kinds.py:127-152` (`ViewKindParams`) is annotated with "for future consolidated kinds" and has no external importers.
- `src/semantics/view_kinds.py:93-118` (`CONSOLIDATED_KIND` mapping) is a forward-looking 14->6 kind consolidation that is currently unused.

**Suggested improvement:**
Remove or clearly mark `metrics.py` and `stats/collector.py` as experimental/unused. Remove the unused `df` parameter from `collect_dataframe_metrics` or delete the function. Consider whether `ViewKindParams` and `CONSOLIDATED_KIND` should remain as aspirational code or be added when actually needed.

**Effort:** small
**Risk if unaddressed:** low -- dead code creates confusion but no runtime risk

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. Two naming choices are mildly surprising.

**Findings:**
- `src/semantics/naming.py:15-45` (`canonical_output_name`) and `naming.py:48-62` (`internal_name`) are both identity functions. A module named "naming" that provides identity-mapped naming functions is surprising -- the name implies transformation, but the code does nothing. This is a vestige of when names had `_v1` suffixes.
- `src/semantics/diagnostics.py:517-525` (`build_relationship_decisions_view`) simply delegates to `build_relationship_candidates_view`. A "decisions" view that is identical to "candidates" is surprising unless it is a placeholder.

**Suggested improvement:**
Add a docstring to `naming.py` module explaining that canonical naming has been simplified to identity mapping and the functions exist for API stability. Add a `# NOTE: Currently identical to candidates; reserved for future decision-layer filtering` comment to `build_relationship_decisions_view`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
All modules declare `__all__` exports. The `__init__.py` files provide clean re-export surfaces.

**Findings:**
- `src/semantics/__init__.py:38-57` exports a curated `__all__` list that does not include any of the infrastructure modules (diagnostics, signals, metrics, stats, etc.), which is correct -- those are internal details.
- `src/semantics/spec_registry.py:22-31` re-exports 11 names from `semantics.registry`, creating a facade. This is a valid pattern but adds indirection.
- `src/semantics/validation/__init__.py:1-27` cleanly re-exports from both `catalog_validation` and `policy` submodules.

**Suggested improvement:**
No immediate action needed. The `__all__` declarations are consistent and well-maintained.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure dataclass modules (`quality.py`, `specs.py`, `view_kinds.py`) are highly testable. DataFusion-dependent builders require session setup.

**Findings:**
- `src/semantics/compile_invariants.py:14-66` (`CompileTracker`) is exceptionally testable -- a simple dataclass with clear state transitions and assertions, no external dependencies.
- `src/semantics/diagnostics.py:486-514` (`build_relationship_candidates_view`) imports `RELATIONSHIP_SPECS` internally, making it hard to test with a custom spec set. Parameterizing the specs would enable isolated testing.
- `src/semantics/metrics.py:242-289` (`collect_dataframe_metrics`) accepts a `df` it does not use. This is untestable in a meaningful sense -- you cannot verify the function does anything with its primary input.

**Suggested improvement:**
Make `build_relationship_candidates_view` accept specs as a parameter (defaulting to `RELATIONSHIP_SPECS` for backward compatibility). Remove or fix `collect_dataframe_metrics`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The normalization modules use `stage_span` from OpenTelemetry consistently for tracing. The diagnostics module emits quality issue batches via `obs.metrics.quality_issue_rows`.

**Findings:**
- `src/semantics/span_normalize.py:77-85` uses `stage_span` with meaningful attributes (`codeanatomy.span_col`, `codeanatomy.line_index_table`).
- `src/semantics/scip_normalize.py:62-70` uses `stage_span` with consistent attribute naming.
- `src/semantics/bytecode_line_table.py:85-93` uses `stage_span` with the same pattern.
- `src/semantics/diagnostics.py` does not use any structured logging or tracing. A 773-LOC module producing diagnostic views has no observability of its own operations.
- `src/semantics/signals.py` similarly lacks `stage_span` instrumentation for the quality-view build.

**Suggested improvement:**
Add `stage_span` instrumentation to `build_file_quality_view` in `signals.py` and to the major diagnostic builders in `diagnostics.py`, consistent with the pattern already established in the normalization modules.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Duplicated DataFusion Utility Patterns

**Root cause:** Three normalization-adjacent modules (`diagnostics.py`, `signals.py`, `span_normalize.py`, `scip_normalize.py`, `bytecode_line_table.py`) independently evolved solutions for the same problems: checking table existence, computing byte offsets from line indexes, and joining line-index data.

**Affected principles:** P7 (DRY), P4 (cohesion), P6 (Ports & Adapters)

**Suggested approach:** Create `src/semantics/normalization_helpers.py` containing:
1. A `table_exists_in(ctx, name)` function (or standardize on `table_names_snapshot`)
2. A `line_index_byte_offset_join(df, line_index, ...)` function encapsulating the aliased-join + byte-offset pattern
3. A `byte_offset_expr(line_start, line_text, col_name, unit_expr)` function

### Theme 2: The Diagnostics Monolith

**Root cause:** `diagnostics.py` was likely the initial landing zone for all "quality and diagnostics" concerns and grew organically. No natural fission point was identified early.

**Affected principles:** P2 (SoC), P3 (SRP), P5 (dependency direction), P23 (testability)

**Suggested approach:** Split into a `diagnostics/` package with 4-5 focused modules, each with a single builder function and its helpers. The current `__all__` export can be preserved via `diagnostics/__init__.py` re-exports.

### Theme 3: Unused Aspirational Infrastructure

**Root cause:** `metrics.py`, `stats/collector.py`, `ViewKindParams`, and `CONSOLIDATED_KIND` were built ahead of need. They represent planned features that were never wired in.

**Affected principles:** P20 (YAGNI), P4 (cohesion -- confusing for new contributors)

**Suggested approach:** Audit each for current usage. If none, either remove or add explicit `@deprecated` annotations and `# FUTURE:` comments explaining when they would be activated.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract shared `_table_exists` to use `table_names_snapshot` | small | Eliminates 3 duplicate implementations and 5-exception-type catch pattern |
| 2 | P20 (YAGNI) | Mark or remove unused `metrics.py` and `stats/collector.py` | small | Reduces ~755 LOC of dead code confusion |
| 3 | P7 (DRY) | Extract shared `_byte_offset` + line-index join pattern | medium | Single source of truth for byte-span normalization |
| 4 | P24 (Observability) | Add `stage_span` to `signals.py:build_file_quality_view` and diagnostics builders | small | Consistent observability across all semantic builders |
| 5 | P19 (KISS) | Simplify `fingerprints.py` defensive getattr chains to assume PyArrow schema | small | ~80 LOC reduction, clearer intent |

## Recommended Action Sequence

1. **Standardize table-existence checking** (P7, P6): Replace all `_table_exists` implementations with `table_names_snapshot(ctx)` usage. This is a safe, mechanical change.

2. **Extract normalization helpers** (P7): Create `src/semantics/normalization_helpers.py` with the shared `_byte_offset` and line-index join pattern. Update `span_normalize.py` and `scip_normalize.py` to use it.

3. **Audit and address dead code** (P20): Determine if `metrics.py` and `stats/collector.py` are needed. If not, remove them. If planned for future use, add explicit markers.

4. **Add observability to signals and diagnostics** (P24): Add `stage_span` instrumentation to `build_file_quality_view` and the major diagnostic builders, following the established pattern in normalization modules.

5. **Split diagnostics.py** (P2, P3): This is the highest-impact structural change. Create a `diagnostics/` package with focused modules. Preserve the public API via re-exports. This can be done incrementally -- move one builder at a time.

6. **Simplify fingerprints.py** (P19, P14): Remove unnecessary polymorphism handling. Assume PyArrow schema from DataFusion.

7. **Improve naming.py type safety** (P1, P9): Replace `object` parameter types with narrow Protocols or direct `Mapping[str, str] | None` parameters.
