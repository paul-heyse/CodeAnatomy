# Design Review: DataFusion Plan + Lineage + Views

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/plan`, `src/datafusion_engine/lineage`, `src/datafusion_engine/views`, `src/datafusion_engine/compile`, `src/datafusion_engine/sql`, `src/datafusion_engine/symtable`
**Focus:** All principles (1-24), with special emphasis on Boundaries (1-6) and Correctness (16-18)
**Depth:** moderate
**Files reviewed:** 36 (13,514 LOC)

## Executive Summary

The reviewed modules form the planning, lineage, view registration, and SQL execution core of the DataFusion engine layer. The architecture demonstrates strong correctness foundations -- determinism validation, plan fingerprinting, and immutable frozen dataclasses are used consistently. Dependency direction is well-managed with explicit module docstrings documenting dependency hierarchies. The primary structural concerns are: (1) `bundle_artifact.py` at 2561 lines is a single-responsibility violation under active decomposition, (2) duplicated knowledge across `normalization.py` and `walk.py` for safe plan input extraction, (3) `graph.py` contains ~370 lines of near-identical cache registration logic for two cache tiers, and (4) `contracts.py` uses bare `Any` types undermining type safety at a boundary contract. Quick wins include extracting the duplicated `_safe_plan_inputs` function and introducing a structured return type for `_delta_scan_candidates`.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `artifact_store_core.py` exports private-prefixed names in `__all__` |
| 2 | Separation of concerns | 1 | large | medium | `bundle_artifact.py` mixes plan building, Delta pinning, UDF snapshotting, Substrait, and fingerprinting |
| 3 | SRP | 1 | large | medium | `bundle_artifact.py` (2561 LOC) changes for 5+ distinct reasons |
| 4 | High cohesion, low coupling | 2 | medium | medium | `artifact_store_core.py` uses bottom-of-file imports to re-export from siblings |
| 5 | Dependency direction | 2 | small | low | Correct overall; `result_types.py` documents dependency hierarchy explicitly |
| 6 | Ports & Adapters | 2 | medium | low | `DiagnosticsSink` protocol is a good port; `contracts.py` uses `Any` instead of typed ports |
| 7 | DRY | 1 | small | medium | `_safe_plan_inputs` duplicated between `normalization.py:87` and `walk.py:24` |
| 8 | Design by contract | 2 | small | low | `DeterminismValidationResult` and `PlanBundleComparisonPolicy` encode contracts well |
| 9 | Parse, don't validate | 2 | small | low | `normalize_substrait_plan` converts at boundary; `contracts.py` defers with `Any` |
| 10 | Make illegal states unrepresentable | 1 | medium | medium | `_delta_scan_candidates` returns 7-element tuple; `ExecutionResult` allows impossible field combos |
| 11 | CQS | 2 | small | low | `_ViewGraphRegistrationContext` mutates state via `install_udf_platform` and returns None |
| 12 | DI + explicit composition | 2 | small | low | `ensure_view_graph` uses constructor injection; some deferred imports for DI |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance hierarchies; composition via frozen dataclasses throughout |
| 14 | Law of Demeter | 2 | medium | low | `profile.delta_ops.delta_service().table_schema(...)` chains in `artifact_store_tables.py:98` |
| 15 | Tell, don't ask | 2 | small | low | `_register_view_with_cache` dispatches on `node.cache_policy` string instead of polymorphism |
| 16 | Functional core, imperative shell | 2 | medium | low | `signals.py` is pure functional; `artifact_store_core.py` mixes transforms with IO |
| 17 | Idempotency | 2 | small | low | Plan artifact persistence uses append mode with determinism checks |
| 18 | Determinism / reproducibility | 3 | - | - | `DeterminismValidationResult`, plan fingerprinting, identity hashing are exemplary |
| 19 | KISS | 2 | small | low | `artifact_store_core.py` module-level import pattern adds complexity |
| 20 | YAGNI | 3 | - | - | No speculative generality observed |
| 21 | Least astonishment | 1 | small | medium | `LineageReport` has duplicate field `referenced_udfs` at lines 84 and 91 |
| 22 | Declare and version public contracts | 2 | small | low | Empty `__init__.py` `__all__` lists; versioned table names (`_v10`, `_v2`) are good |
| 23 | Design for testability | 2 | medium | low | `DiagnosticsSink` protocol enables test doubles; heavy `SessionContext` deps elsewhere |
| 24 | Observability | 3 | - | - | OTel spans, artifact recording, and structured diagnostics are thorough |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Module internals are generally well-hidden behind `__all__` declarations. However, `artifact_store_core.py` re-exports private-prefixed names from sibling modules.

**Findings:**
- `src/datafusion_engine/plan/artifact_store_core.py:773-778`: The `__all__` list includes `_bootstrap_pipeline_events_table`, `_bootstrap_plan_artifacts_table`, `_delta_schema_available`, `_pipeline_events_location`, `_plan_artifacts_location`, and `_reset_artifacts_table_path` -- all private-prefixed names. Exporting underscore-prefixed symbols in `__all__` signals an internal API that is treated as public, violating Python convention.
- `src/datafusion_engine/plan/artifact_store_tables.py:360-378`: The entire `__all__` list consists exclusively of private-prefixed names. This module exists to hold extracted helpers from `artifact_store_core.py`, but the naming convention contradicts the export intent.
- `src/datafusion_engine/plan/contracts.py:13-14`: `view_nodes: tuple[Any, ...]` and `runtime_profile: Any | None` expose no type information at a contract boundary, defeating information hiding by making the caller guess what types are expected.

**Suggested improvement:**
Rename the extracted helpers in `artifact_store_tables.py` to drop the leading underscore since they are genuinely part of the subpackage's internal API surface. Alternatively, stop exporting them from `artifact_store_core.py` and have consumers import directly from `artifact_store_tables.py`. For `contracts.py`, replace `Any` with forward references or protocol types.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`bundle_artifact.py` is the most significant SoC violation. It handles plan building, Delta input pinning, UDF snapshot capture, Substrait serialization, execution plan compilation, explain capture, plan fingerprinting, and plan detail assembly. The module docstring acknowledges it as the "single canonical plan artifact" but this conflates multiple distinct concerns.

**Findings:**
- `src/datafusion_engine/plan/bundle_artifact.py` (2561 LOC): This single module handles at least five distinct concerns:
  1. Plan compilation (`build_plan_artifact`, `_compile_dataframe_plan`, `_compile_sql_plan`)
  2. Delta input pinning (`_pin_delta_inputs`, `_build_delta_input_pin`)
  3. UDF snapshot capture (`_udf_snapshot_artifacts`)
  4. Substrait serialization (`_produce_substrait`, `_substrait_validation`)
  5. Plan identity hashing (`_plan_fingerprint`, `_plan_identity_hash`)
- `src/datafusion_engine/views/graph.py` (1408 LOC): Mixes view node management, topological registration, and two distinct cache registration strategies (`_register_delta_staging_cache` at line 663 and `_register_delta_output_cache` at line 850).

**Suggested improvement:**
Continue the decomposition that has already been started (as noted in the size-exception comment). Extract Delta input pinning into `plan/delta_inputs.py`, UDF snapshot capture into `plan/udf_snapshot.py`, and Substrait serialization into `plan/substrait.py`. For `graph.py`, extract cache registration into a dedicated `views/cache_registration.py` module.

**Effort:** large
**Risk if unaddressed:** medium -- changes to any of the five concerns require understanding the full 2561-line module

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Several modules have multiple reasons to change. The most acute case is `bundle_artifact.py` which changes whenever plan compilation, Delta pinning, UDF handling, Substrait serialization, or fingerprinting logic changes.

**Findings:**
- `src/datafusion_engine/plan/bundle_artifact.py`: Changes for plan compilation logic, Delta protocol updates, UDF snapshot format changes, Substrait library updates, and fingerprint algorithm changes. This is acknowledged with a `# NOTE(size-exception)` comment in `artifact_store_core.py:2` referencing ongoing decomposition work.
- `src/datafusion_engine/lineage/diagnostics.py` (908 LOC): Contains the `DiagnosticsSink` protocol, three record types (`CompilationRecord`, `ExecutionRecord`, `WriteRecord`), `InMemoryDiagnosticsSink`, `DiagnosticsContext`, `DiagnosticsRecorder`, `DiagnosticsRecorderAdapter`, and multiple payload builder functions. This changes for protocol changes, record format changes, payload builder changes, and recorder behavior changes.
- `src/datafusion_engine/lineage/scheduling.py` (822 LOC): Handles both scan planning logic and Delta-specific file pruning/candidate selection.

**Suggested improvement:**
For `diagnostics.py`: extract `DiagnosticsSink` protocol and record types into `lineage/diagnostics_protocol.py`, payload builders into `lineage/diagnostics_payloads.py`, and keep the recorder implementation in `lineage/diagnostics.py`. For `scheduling.py`: extract Delta candidate selection into `lineage/delta_candidates.py`.

**Effort:** large
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Most modules have reasonable cohesion. The coupling concern is in `artifact_store_core.py` which uses bottom-of-file module-level imports from sibling modules to maintain backward compatibility during decomposition.

**Findings:**
- `src/datafusion_engine/plan/artifact_store_core.py:294-297`: Module-level import from `artifact_store_query` placed mid-file, after function definitions. This is a coupling mechanism used during decomposition to allow callers to import from the original module.
- `src/datafusion_engine/plan/artifact_store_core.py:745-761`: Module-level import from `artifact_store_tables` placed at the bottom of the file, re-exporting 16 private-prefixed names. This creates a tight coupling loop where `artifact_store_tables.py` imports from `artifact_store_core.py` (line 19-28) and `artifact_store_core.py` re-imports from `artifact_store_tables.py` (line 745-761).
- `src/datafusion_engine/plan/artifact_store_core.py:1-62`: The import section has 14 aliased imports from `artifact_serialization.py`, each renaming a public function to a private name (e.g., `delta_inputs_payload as _delta_inputs_payload`). This coupling pattern obscures the actual dependency.

**Suggested improvement:**
Complete the decomposition by updating all callers to import directly from the extracted modules (`artifact_store_tables`, `artifact_store_query`, `artifact_store_cache`) instead of through `artifact_store_core.py`. Remove the bottom-of-file re-exports once callers are migrated.

**Effort:** medium
**Risk if unaddressed:** medium -- the circular re-import pattern makes it hard to reason about module initialization order

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Dependency direction is mostly correct. `result_types.py:8-12` explicitly documents its dependency hierarchy to break circular dependencies. Core types flow inward correctly. The one concern is `contracts.py` using `Any` to avoid declaring dependencies.

**Findings:**
- `src/datafusion_engine/plan/result_types.py:8-12`: Documents `result_types.py (types + execution helpers, minimal deps) <- facade.py (high-level orchestration)`. This is good practice.
- `src/datafusion_engine/plan/contracts.py:13-14`: Uses `Any` for `view_nodes` and `runtime_profile` to avoid importing concrete types from higher-level modules. While this avoids a circular dependency, it weakens the type contract.
- `src/datafusion_engine/views/graph.py:17-18`: Imports `DataFusionIOAdapter` directly (a concrete class) rather than depending on a protocol, creating coupling to the IO layer.

**Suggested improvement:**
For `contracts.py`, use `TYPE_CHECKING` forward references to the actual types rather than `Any`. For `graph.py`, consider whether `DataFusionIOAdapter` could be replaced with a protocol for view registration operations.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`DiagnosticsSink` (at `diagnostics.py:48`) is an exemplary port -- a `@runtime_checkable` Protocol that defines the diagnostics recording contract. `LineageRecorder` and `LineageQuery` protocols in `lineage/protocols.py` are similarly well-defined ports. The gap is in `contracts.py` where the planning request envelope uses `Any` instead of typed ports.

**Findings:**
- `src/datafusion_engine/lineage/diagnostics.py:48-97`: `DiagnosticsSink` Protocol with 5 well-defined methods. Good port design.
- `src/datafusion_engine/lineage/protocols.py:1-87`: `LineageScan`, `LineageJoin`, `LineageExpr`, `LineageRecorder`, `LineageQuery` -- all `@runtime_checkable` protocols. Excellent port design for lineage extraction.
- `src/datafusion_engine/plan/contracts.py:10-17`: `PlanWithDeltaPinsRequestV1` uses `Any` for its core fields, which defeats the purpose of a typed contract at a module boundary.

**Suggested improvement:**
Replace `Any` fields in `PlanWithDeltaPinsRequestV1` with either forward references (via `TYPE_CHECKING`) or protocol types that express the minimum required interface.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
There are two significant knowledge duplications: the safe plan input extraction logic and the cache registration logic.

**Findings:**
- `src/datafusion_engine/plan/normalization.py:87-97` duplicates `src/datafusion_engine/plan/walk.py:24-46`: Both implement `_safe_plan_inputs` / `_safe_inputs` with identical logic -- get inputs method, check callable, try/except, filter None values. The only difference is the parameter type annotation (`DataFusionLogicalPlan` vs `object`) and `Sequence` vs `Iterable` check.
- `src/datafusion_engine/views/graph.py:663-847` and `src/datafusion_engine/views/graph.py:850-1050`: `_register_delta_staging_cache` and `_register_delta_output_cache` share ~90% identical structure -- both perform cache hit resolution, schema evolution enforcement, write pipeline execution, cached table registration, inventory recording, and artifact recording. The only differences are the cache path source and specific span names.
- `src/datafusion_engine/plan/artifact_store_tables.py:328-340` duplicates `src/datafusion_engine/plan/artifact_serialization.py:40-58`: Both implement `commit_metadata_for_rows` with the same logic for building commit metadata from plan artifact rows, differing only in parameter style (rows vs decomposed fields).

**Suggested improvement:**
(1) Consolidate `_safe_plan_inputs` into `walk.py` and have `normalization.py` import and use it. (2) Extract the shared cache registration logic into a parameterized helper function in `views/cache_registration.py` that accepts cache-tier-specific configuration. (3) Remove the duplicate `_commit_metadata_for_rows` from `artifact_store_tables.py` and use the version from `artifact_serialization.py`.

**Effort:** small (for item 1), medium (for items 2 and 3)
**Risk if unaddressed:** medium -- bugs fixed in one copy may not be propagated to the other

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Contracts are generally well-defined through frozen dataclasses and msgspec structs. `DeterminismValidationResult` and `PlanBundleComparisonPolicy` encode clear invariants. Preconditions are checked in `ensure_view_graph` (line 207-209).

**Findings:**
- `src/datafusion_engine/plan/artifact_store_core.py:99-126`: `DeterminismValidationResult` is a well-designed contract with clear fields for status, fingerprint, count, and error state.
- `src/datafusion_engine/views/registration.py:207-209`: `ensure_view_graph` validates preconditions (`runtime_profile is None` check) before proceeding.
- `src/datafusion_engine/plan/artifact_validation.py` (22 lines): Minimal validation module. Could be expanded to validate plan identity payload structure.

**Suggested improvement:**
Add postcondition assertions to `build_plan_artifact` in `bundle_artifact.py` to verify that the returned `DataFusionPlanArtifact` has a non-empty `plan_fingerprint` and consistent `plan_identity_hash`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`normalize_substrait_plan` in `normalization.py` is a good example of parse-don't-validate -- it converts plans at the boundary into a Substrait-compatible representation. The `_DeterminismRow` in `artifact_store_query.py:127` uses `msgspec.convert` for strict parsing. The gap is `contracts.py` which defers parsing by using `Any`.

**Findings:**
- `src/datafusion_engine/plan/normalization.py:35-49`: `normalize_substrait_plan` strips unsupported variants and falls back to a probe table plan, converting at the boundary.
- `src/datafusion_engine/plan/artifact_store_query.py:126-131`: `_determinism_sets` uses `msgspec.convert(row, type=_DeterminismRow, strict=True)` to parse rows into a typed representation, raising `ValueError` on validation failure.
- `src/datafusion_engine/plan/contracts.py:13`: `view_nodes: tuple[Any, ...]` accepts any tuple without parsing into a typed representation.

**Suggested improvement:**
Define a `ViewNodeLike` protocol with the minimum required attributes (`name`, `plan_bundle`, `cache_policy`) and use it instead of `Any` in `PlanWithDeltaPinsRequestV1`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
Several data structures allow impossible state combinations.

**Findings:**
- `src/datafusion_engine/lineage/scheduling.py:399-407`: `_delta_scan_candidates` returns `tuple[tuple[Path, ...], int | None, int | None, DeltaProtocolSnapshot | None, int, int, int]` -- a 7-element positional tuple where the meaning of each element is opaque and relationships between elements (e.g., candidate count must equal length of paths tuple) cannot be enforced.
- `src/datafusion_engine/plan/result_types.py:54-63`: `ExecutionResult` uses a tagged union pattern with a `kind` discriminator and optional fields (`dataframe`, `table`, `reader`, `write_result`). While static factory methods enforce correct construction, nothing prevents `ExecutionResult(kind=ExecutionResultKind.DATAFRAME, dataframe=None)` -- the kind says DataFrame but the field is None.
- `src/datafusion_engine/lineage/reporting.py:84,91`: `LineageReport` has `referenced_udfs` declared twice -- at line 84 as `required_udfs: tuple[str, ...]` and at line 91 as `referenced_udfs: tuple[str, ...]`. The field at line 91 shadows any value set at line 84's position, creating confusion about which `referenced_udfs` is authoritative.

**Suggested improvement:**
(1) Replace the 7-tuple return of `_delta_scan_candidates` with a frozen dataclass `DeltaScanCandidateResult` that names each field and can enforce invariants (e.g., `len(candidate_files) == candidate_file_count`). (2) For `ExecutionResult`, consider using separate result classes per kind or `__post_init__` validation to reject mismatched kind/field combinations. (3) Resolve the duplicate `referenced_udfs` field in `LineageReport`.

**Effort:** medium
**Risk if unaddressed:** medium -- the 7-tuple is a maintenance hazard; the duplicate field is a correctness risk

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. The notable exception is `_ViewGraphRegistrationContext` whose methods mutate instance state while being used in a validation/registration pipeline.

**Findings:**
- `src/datafusion_engine/views/registration.py:61-74`: `install_udf_platform()` mutates `self.snapshot` as a side effect. It is called via the `RegistrationPhaseOrchestrator` which expects a callable returning None.
- `src/datafusion_engine/views/registration.py:88-99`: `build_view_nodes()` mutates `self.nodes` as a side effect. This is a command masquerading as a named validation step.
- `src/datafusion_engine/plan/artifact_store_tables.py:181-192`: `_ensure_substrait_probe_table` both creates a table (command) and returns its name (query), violating CQS.

**Suggested improvement:**
The `_ViewGraphRegistrationContext` pattern is acceptable for orchestration contexts where mutation order is controlled by the phase orchestrator. For `_ensure_substrait_probe_table` in `normalization.py`, split into `_register_substrait_probe_table` (command) and a separate name constant.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
DI is used in several places. `ensure_view_graph` takes its dependencies as parameters. The `DiagnosticsSink` protocol enables DI for diagnostics. Some modules use deferred imports as a form of DI to break circular dependencies.

**Findings:**
- `src/datafusion_engine/views/registration.py:183-221`: `ensure_view_graph` takes all dependencies as explicit parameters (`ctx`, `runtime_profile`, `scan_units`, `semantic_manifest`, `dataset_resolver`).
- `src/datafusion_engine/views/graph.py:663-691`: `_register_delta_staging_cache` uses 8 deferred imports inside the function body. While this breaks circular dependencies, it creates hidden dependencies that are not visible from the function signature.

**Suggested improvement:**
Consider collecting the deferred imports for cache registration into a `CacheRegistrationDependencies` protocol or dataclass that is passed in, making the dependencies explicit rather than hidden behind deferred imports.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The codebase consistently uses composition. Frozen dataclasses compose smaller types (`PerformancePolicy` composes `CachePolicyTier`, `StatisticsPolicy`, `PlanBundleComparisonPolicy`). No inheritance hierarchies are used in the reviewed modules.

**Findings:**
- No inheritance hierarchies found in any of the 36 reviewed files. All behavior is assembled through composition of frozen dataclasses, protocols, and function composition.

**Suggested improvement:** None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code follows LoD. A few chain violations exist in the artifact store and scheduling modules.

**Findings:**
- `src/datafusion_engine/plan/artifact_store_tables.py:98-99`: `profile.delta_ops.delta_service().table_schema(...)` chains through three levels of indirection.
- `src/datafusion_engine/plan/artifact_store_cache.py:40`: `profile.delta_ops.delta_service().table_version(path=...)` same chain pattern.
- `src/datafusion_engine/plan/artifact_store_tables.py:136-137`: `profile.policies.plan_artifacts_root` and `profile.policies.local_filesystem_root` -- two-level chains that are acceptable for config access.

**Suggested improvement:**
Add a convenience method on the profile or a helper function that wraps `profile.delta_ops.delta_service()` to reduce the chain depth. For example, `profile.delta_service()` or a standalone `delta_service_for_profile(profile)`.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Cache registration dispatches on string values rather than using polymorphism.

**Findings:**
- `src/datafusion_engine/views/graph.py:651-660`: `_register_view_with_cache` checks `node.cache_policy` string values (`"delta_staging"`, `"delta_output"`, `"none"`) to dispatch to different registration functions. This "ask" pattern means the view graph module must know about all cache policy types.
- `src/datafusion_engine/plan/artifact_store_core.py:276-290`: `_apply_plan_artifact_retention` inspects `comparison_policy.retain_p0_artifacts` and `comparison_policy.retain_p1_artifacts` and conditionally nullifies fields. The comparison policy could instead own the retention logic.

**Suggested improvement:**
For cache registration, consider a `CachePolicyHandler` protocol or registry that maps cache policy strings to handler functions, allowing the dispatch to be externalized. For retention, add a `retain(row)` method to `PlanBundleComparisonPolicy`.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
`signals.py` is an exemplary functional core -- all functions are pure transforms from plan bundles to signal dataclasses with no IO. `artifact_serialization.py` is similarly pure. The imperative shell is in `artifact_store_core.py` and `graph.py` which mix IO (Delta writes, table registration) with transformation logic.

**Findings:**
- `src/datafusion_engine/plan/signals.py` (450 LOC): Pure functional module. `extract_plan_signals` takes a plan bundle and returns a `PlanSignals` frozen dataclass. No IO, no side effects.
- `src/datafusion_engine/plan/artifact_serialization.py` (434 LOC): Pure serialization transforms. All functions take data in, return data out.
- `src/datafusion_engine/plan/artifact_store_core.py:315-370`: `persist_execution_artifact` mixes artifact row construction (pure) with persistence (IO) in the same function.
- `src/datafusion_engine/views/graph.py:663-847`: `_register_delta_staging_cache` interleaves pure logic (cache hit resolution, schema comparison) with IO (Delta writes, table registration, artifact recording).

**Suggested improvement:**
In `artifact_store_core.py`, separate `build_plan_artifact_row` (pure, already exists) from the persistence step. In `graph.py`, extract the pure cache decision logic (should we hit, should we write, should we evolve schema) into a pure function that returns a decision, then have the imperative shell execute the decision.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Plan artifact persistence uses Delta append mode with determinism validation, which is idempotent in the sense that re-running with the same inputs produces the same plan fingerprint and identity hash. However, appending duplicate rows to the artifact store is not prevented.

**Findings:**
- `src/datafusion_engine/plan/artifact_store_query.py:184-243`: `validate_plan_determinism` queries the artifact store to check whether previous executions with the same fingerprint produced the same identity hash. This detects non-determinism but does not prevent duplicate writes.
- `src/datafusion_engine/plan/artifact_store_core.py:82-83`: Table names include version suffixes (`_v10`, `_v2`) which ensures schema evolution does not corrupt existing data.
- `src/datafusion_engine/plan/artifact_store_tables.py:111-132`: `_reset_artifacts_table_path` deletes and recreates the table path, which is idempotent but destructive.

**Suggested improvement:**
Consider adding a deduplication check before appending plan artifact rows, using the `plan_identity_hash` as a natural key to skip already-persisted artifacts.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class architectural concern. The system has comprehensive infrastructure for plan fingerprinting, identity hashing, and determinism validation.

**Findings:**
- `src/datafusion_engine/plan/artifact_serialization.py:276-309`: `plan_identity_payload` constructs a canonical payload including version, fingerprint, UDF snapshot hash, function registry hash, domain planner names, DF settings, planning env hash, rulepack hash, information schema hash, Delta inputs, scan units, scan keys, profile settings hash, and profile context key. This is thorough determinism tracking.
- `src/datafusion_engine/plan/artifact_store_query.py:184-243`: `validate_plan_determinism` performs cross-execution determinism validation by querying historical artifacts.
- `src/datafusion_engine/plan/artifact_serialization.py:332-362`: `udf_compatibility` checks runtime UDF snapshot consistency against planned UDF snapshots.
- `src/datafusion_engine/plan/signals.py:16-18`: `_SELECTIVITY_DECAY_PER_PREDICATE = 0.5` is a deterministic constant, not a runtime parameter.

**Suggested improvement:** None needed. Determinism infrastructure is exemplary.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. The complexity concern is the `artifact_store_core.py` module-level import pattern which creates a confusing re-export structure.

**Findings:**
- `src/datafusion_engine/plan/artifact_store_core.py:294-297,745-761`: Module-level imports placed mid-file and at end-of-file are unusual Python patterns that will surprise readers. This pattern exists to maintain backward compatibility during decomposition.
- `src/datafusion_engine/plan/artifact_store_core.py:21-62`: 14 aliased imports from `artifact_serialization.py`, each renaming a public function to a private name. This indirection adds cognitive load without clear benefit.
- `src/datafusion_engine/plan/normalization.py:145-164`: `_fallback_substrait_plan` has a multi-step fallback chain (available tables -> probe table -> original plan) that could be simplified with a clearer strategy pattern.

**Suggested improvement:**
Once decomposition is complete, remove the re-export indirection and update callers to import directly from the target modules. Remove the aliased imports that rename public functions to private names.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 3/3

**Current state:**
No speculative generality observed. All modules serve active use cases. The `PlanBundleComparisonPolicy` provides configuration seams without unused extension points.

**Findings:**
- No speculative abstractions, unused interfaces, or over-engineered extension points found.

**Suggested improvement:** None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
The duplicate `referenced_udfs` field in `LineageReport` is a significant surprise for consumers.

**Findings:**
- `src/datafusion_engine/lineage/reporting.py:84,91`: `LineageReport` declares `required_udfs: tuple[str, ...]` at line 84 and `referenced_udfs: tuple[str, ...]` at line 91. But `referenced_udfs` also appears conceptually at line 84 via the field name `required_udfs`. Both fields represent UDF references but with different names. Additionally, the `ExprInfo` struct at line 74 has its own `referenced_udfs` field. This creates confusion about which field is authoritative for "which UDFs does this plan reference."
- `src/datafusion_engine/plan/artifact_store_core.py:294-297`: A module-level import statement appearing after function definitions (line 294) violates the Python convention of placing all imports at the top of the file. Readers scanning the import block will miss these dependencies.
- `src/datafusion_engine/plan/contracts.py:10-17`: A contract named `PlanWithDeltaPinsRequestV1` with `Any` typed fields is astonishing -- callers expect a versioned contract to be precisely typed.

**Suggested improvement:**
(1) Clarify the relationship between `required_udfs` and `referenced_udfs` in `LineageReport` -- if they represent different concepts, document the distinction clearly; if they are duplicates, remove one. (2) Add a comment block before the mid-file imports explaining the decomposition pattern. (3) Type the `contracts.py` fields.

**Effort:** small
**Risk if unaddressed:** medium -- the duplicate field creates risk of using the wrong UDF list for dependency resolution

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
Table names are versioned (`_v10`, `_v2`). Contract types use `V1` suffixes where appropriate. However, the package `__init__.py` files export empty `__all__` lists, making it unclear what the public API surface is.

**Findings:**
- `src/datafusion_engine/plan/__init__.py:5`: `__all__: list[str] = []` -- empty public API despite the package containing many public types.
- `src/datafusion_engine/lineage/__init__.py:5`: Same pattern.
- `src/datafusion_engine/views/__init__.py:7`: Same pattern, plus a deprecation handler for `VIEW_SELECT_REGISTRY`.
- `src/datafusion_engine/plan/contracts.py:10`: `PlanWithDeltaPinsRequestV1` is properly versioned.
- `src/datafusion_engine/plan/artifact_store_core.py:82-84`: Table names `PLAN_ARTIFACTS_TABLE_NAME = "datafusion_plan_artifacts_v10"` are versioned.

**Suggested improvement:**
Populate the `__init__.py` `__all__` lists with the intended public API surface for each subpackage. This makes it explicit which types are stable and which are internal.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
`DiagnosticsSink` protocol and `InMemoryDiagnosticsSink` enable excellent test doubles for diagnostics. `signals.py` and `artifact_serialization.py` are pure functions that are trivially testable. The challenge is in modules that depend on `SessionContext` -- a heavyweight DataFusion object that is expensive to create in tests.

**Findings:**
- `src/datafusion_engine/lineage/diagnostics.py:146-155`: `InMemoryDiagnosticsSink` is a clean test double for `DiagnosticsSink`.
- `src/datafusion_engine/plan/signals.py`: All functions are pure transforms. `extract_plan_signals(bundle, ...)` takes immutable inputs and returns a frozen dataclass. Fully testable without mocks.
- `src/datafusion_engine/plan/artifact_store_query.py:184-243`: `validate_plan_determinism` requires a live `SessionContext` for SQL execution. Testing this function requires a full DataFusion session with registered Delta tables.
- `src/datafusion_engine/views/graph.py:663-847`: Cache registration functions require `SessionContext`, `DataFusionIOAdapter`, and Delta tables, making unit testing expensive.

**Suggested improvement:**
For `validate_plan_determinism`, extract the query building (`_determinism_validation_query`) and result interpretation (`_determinism_sets`, `_determinism_outcome`) into pure functions (already partially done) and keep the SQL execution in a thin shell. For cache registration, parameterize the write pipeline creation to allow test doubles.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Observability is comprehensive and well-structured. OTel spans, artifact recording via `record_artifact`, structured diagnostics payloads, and plan phase diagnostics are used consistently.

**Findings:**
- `src/datafusion_engine/plan/diagnostics.py`: `PlanPhaseDiagnostics` and `record_plan_phase_diagnostics` provide structured OTel span instrumentation for plan compilation phases.
- `src/datafusion_engine/lineage/diagnostics.py:48-97`: `DiagnosticsSink` protocol provides a clean observability port.
- `src/datafusion_engine/views/registration.py:117-133`: Schema contract violations are recorded as artifacts before re-raising.
- `src/datafusion_engine/plan/artifact_store_tables.py:122-132`: `_reset_artifacts_table_path` records a structured artifact with event time, table name, path, and reason.
- `src/datafusion_engine/plan/perf_policy.py:102-127`: `performance_policy_artifact_payload` produces structured artifact payloads for performance policy tracing.

**Suggested improvement:** None needed. Observability is thorough and consistent.

**Effort:** -
**Risk if unaddressed:** -

---

## Cross-Cutting Themes

### Theme 1: Active Decomposition in Progress

The `artifact_store_core.py` module is in the middle of a decomposition effort (acknowledged by the `NOTE(size-exception)` comment). The module has been partially split into `artifact_store_tables.py`, `artifact_store_query.py`, `artifact_store_cache.py`, and `artifact_serialization.py`, but the original module still re-exports symbols from the extracted modules via bottom-of-file imports. This mid-decomposition state affects principles P1 (private name exports), P4 (coupling via re-exports), P19 (unusual import placement), and P21 (surprise for readers). The root cause is maintaining backward compatibility while migrating callers incrementally. Completing the migration and removing re-exports would resolve four principle gaps simultaneously.

### Theme 2: The `bundle_artifact.py` Monolith

At 2561 lines, `bundle_artifact.py` is the largest file in scope and concentrates five distinct concerns. This affects P2 (separation of concerns), P3 (SRP), and P16 (functional core / imperative shell). The module mixes pure plan construction logic with IO-coupled operations. Decomposition has been started for the artifact store but not yet for the bundle artifact itself. This is the single highest-impact structural improvement opportunity.

### Theme 3: Unstructured Multi-Value Returns

Several functions return tuples with positional semantics instead of named structures. `_delta_scan_candidates` returns a 7-tuple, `_plan_identifiers` returns a 2-tuple, and `_determinism_outcome` returns a 3-tuple. This affects P10 (make illegal states unrepresentable) and P21 (least astonishment). Converting these to frozen dataclasses would improve readability and enable invariant enforcement.

### Theme 4: Strong Determinism Architecture

The determinism infrastructure (plan fingerprinting, identity hashing, determinism validation, UDF compatibility checking) is the strongest aspect of the reviewed code. It affects P8 (design by contract), P17 (idempotency), and P18 (determinism) positively. The plan identity payload at `artifact_serialization.py:276-309` is comprehensive and well-versioned.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate `_safe_plan_inputs` from `normalization.py:87` into `walk.py:24` | small | Eliminates knowledge duplication; prevents divergent bug fixes |
| 2 | P10 | Replace 7-tuple return of `_delta_scan_candidates` with a frozen dataclass | small | Named fields, enforceable invariants, self-documenting code |
| 3 | P21 | Resolve duplicate `referenced_udfs` field in `LineageReport` at `reporting.py:84,91` | small | Eliminates field shadowing confusion |
| 4 | P1 | Drop underscore prefix from exported names in `artifact_store_tables.py` | small | Aligns naming convention with export intent |
| 5 | P9/P6 | Replace `Any` fields in `contracts.py:13-14` with typed protocols or forward refs | small | Restores type safety at contract boundary |

## Recommended Action Sequence

1. **Resolve `LineageReport` duplicate field** (P21). Fix the `referenced_udfs` field duplication at `reporting.py:84,91`. This is a correctness fix with no dependencies on other changes.

2. **Consolidate `_safe_plan_inputs`** (P7). Move the canonical implementation to `walk.py` and update `normalization.py:87` to import from `walk.py`. This is a mechanical change with no design decisions.

3. **Introduce `DeltaScanCandidateResult` dataclass** (P10). Replace the 7-tuple return of `_delta_scan_candidates` in `scheduling.py:399-407` with a named frozen dataclass. Update all callers.

4. **Type `PlanWithDeltaPinsRequestV1` fields** (P6/P9). Replace `Any` with forward references or protocols in `contracts.py:13-14`.

5. **Rename exported symbols in `artifact_store_tables.py`** (P1). Drop leading underscores from the 17 exported names. Update imports in `artifact_store_core.py` and `artifact_store_cache.py`.

6. **Complete `artifact_store_core.py` decomposition** (P4/P19). Migrate remaining callers to import directly from extracted modules. Remove bottom-of-file re-exports at lines 294-297 and 745-761.

7. **Extract cache registration logic from `graph.py`** (P7/P2). Create `views/cache_registration.py` with a parameterized helper that handles both `delta_staging` and `delta_output` tiers.

8. **Begin `bundle_artifact.py` decomposition** (P2/P3). Extract Delta input pinning, UDF snapshot capture, and Substrait serialization into dedicated modules within `plan/`. This is the largest effort but addresses the most principles.

9. **Extract diagnostics payload builders** (P3). Move the ~15 payload builder functions from `diagnostics.py` into `lineage/diagnostics_payloads.py`, keeping the `DiagnosticsSink` protocol and recorder in the original module.
