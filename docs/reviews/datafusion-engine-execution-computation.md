# Design Review: src/datafusion_engine/ -- Execution & Computation Layer

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/{plan,lineage,udf,expr,extensions,cache,compile,sql,encoding,symtable,pruning,extract}/`
**Focus:** All principles (1-24), with emphasis on SRP, CQS, and dependency direction
**Depth:** Deep (exhaustive file-by-file analysis)
**Files reviewed:** 68

## Executive Summary

The Execution & Computation layer is a well-decomposed ~19K LOC subsystem that handles plan compilation, lineage extraction, UDF management, expression building, caching, and extraction metadata. It demonstrates strong architectural separation between protocol definitions, pure computation, and IO operations. The primary weaknesses are (1) two oversized files (`artifact_store.py` at ~1655 lines and `extension_runtime.py` at ~1661 lines) that bundle multiple responsibilities, (2) duplicated type-coercion helpers across at least four modules, (3) some CQS violations in functions that both mutate state and return results, and (4) heavy use of lazy imports creating implicit coupling. The extract/ submodule is notably clean with its template-driven metadata expansion pattern. The expr/spec.py module is a well-designed expression IR with proper tagged union types.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `datafusion_ext.py` `__getattr__` exposes entire `_internal` module surface |
| 2 | Separation of concerns | 2 | medium | medium | `artifact_store.py` mixes persistence, SQL construction, schema bootstrap, and diff gates |
| 3 | SRP | 1 | large | high | `artifact_store.py` (1655 LOC) and `extension_runtime.py` (1661 LOC) each have 4+ reasons to change |
| 4 | High cohesion, low coupling | 2 | medium | medium | cache/ submodule has high internal coupling between inventory, ledger, registry |
| 5 | Dependency direction | 2 | medium | medium | `lineage/diagnostics.py` contains view-specific helpers that couple lineage to view concepts |
| 6 | Ports & Adapters | 2 | small | low | Good Protocol use (DiagnosticsSink, LineageQuery) but some adapters mix concerns |
| 7 | DRY (knowledge) | 1 | medium | medium | `_coerce_int`/`_coerce_opt_int`/`_coerce_optional_int` duplicated across 4+ modules |
| 8 | Design by contract | 2 | small | low | ExprIR validates call arity; most Protocols lack postcondition documentation |
| 9 | Parse, don't validate | 2 | small | low | `expr/spec.py` ScalarLiteralCodec is exemplary; some boundary parsing is scattered |
| 10 | Make illegal states unrepresentable | 2 | medium | low | ExprIR `op` is a string, not an enum; ExprSpec allows both `sql` and `expr_ir` to be None before `__post_init__` |
| 11 | CQS | 2 | medium | medium | `record_cache_inventory_entry` and `record_cache_run_summary` both mutate and return version |
| 12 | Dependency inversion | 2 | small | low | Good Protocol-based DI in lineage/; some concrete imports in cache/ |
| 13 | Composition over inheritance | 3 | -- | -- | No inheritance hierarchies; composition is used throughout |
| 14 | Law of Demeter | 2 | small | low | `profile.io_ops.cache_root()` and `profile.policies.sql_policy` chains |
| 15 | Tell, don't ask | 2 | small | low | `DataFusionPlanArtifact` exposes many attributes for external logic |
| 16 | Functional core, imperative shell | 2 | medium | low | expr/, lineage/reporting.py are pure; cache/ and artifact_store.py mix IO deeply |
| 17 | Idempotency | 2 | small | low | Delta append operations are inherently non-idempotent; no dedup guard |
| 18 | Determinism | 3 | -- | -- | Fingerprinting is thorough; plan identity hashes ensure reproducibility |
| 19 | KISS | 2 | small | low | `context_adaptation.py` has unused complexity (`select_context_candidate` always returns one candidate) |
| 20 | YAGNI | 2 | small | low | Some thin wrappers (`extension_loader.py`, `extension_lifecycle.py`) add indirection without clear benefit |
| 21 | Least astonishment | 2 | small | low | `safe_sql_options_for_profile` ignores its argument (always returns read-only) |
| 22 | Public contracts | 2 | small | low | `__all__` consistently declared; versioned table names; but ExprIR ops are undocumented strings |
| 23 | Testability | 2 | medium | medium | cache/ and artifact_store.py require full DataFusion + Delta stack to test |
| 24 | Observability | 2 | small | low | Diagnostics sinks are well-designed; some functions lack structured error payloads |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most modules correctly hide internals behind `__all__` declarations and use private prefixes. However, `datafusion_ext.py` uses `__getattr__` (line 131) to proxy the entire `datafusion._internal` module surface, which means callers can access any attribute of the native extension, bypassing any abstraction boundary.

**Findings:**
- `src/datafusion_engine/extensions/datafusion_ext.py:131` -- `__getattr__` delegates to `_wrapped_attr`, exposing the full `datafusion._internal` surface without filtering. Any internal API of the Rust extension becomes implicitly part of this module's contract.
- `src/datafusion_engine/plan/bundle_artifact.py:64` -- `DataFrameBuilder` type alias is public but the actual `DataFusionPlanArtifact` class exposes 20+ mutable fields as its public interface, making it hard to evolve.
- `src/datafusion_engine/udf/metadata.py` -- `UdfCatalog` exposes raw `_entries` dict via methods rather than hiding the storage implementation behind a narrow interface.

**Suggested improvement:**
Add an explicit allowlist to `datafusion_ext.py.__getattr__` that enumerates the exact functions and attributes from `_internal` that are part of the public surface. This prevents accidental coupling to Rust internals.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The decomposition into plan/, lineage/, udf/, expr/, etc. is well-conceived and most modules separate policy from plumbing. The main exceptions are `artifact_store.py` which mixes persistence logic with SQL construction, schema bootstrapping, diff gating, and diagnostics recording, and `lineage/diagnostics.py` which contains view-specific payload builders alongside the generic diagnostics infrastructure.

**Findings:**
- `src/datafusion_engine/plan/artifact_store.py` -- This single file handles: (1) Delta table bootstrapping, (2) plan artifact SQL queries, (3) write artifact recording, (4) pipeline event recording, (5) determinism validation queries, (6) diff-gate logic, (7) plan comparison. These are at least four distinct concerns.
- `src/datafusion_engine/lineage/diagnostics.py:780+` -- `view_udf_parity_payload` and `view_fingerprint_payload` are view-layer helpers embedded in the lineage diagnostics module.
- `src/datafusion_engine/udf/extension_runtime.py` -- Handles UDF registration, snapshot validation, DDL generation, capabilities probing, and WeakKeyDictionary-based session state management in one file.

**Suggested improvement:**
Split `artifact_store.py` into: (1) `artifact_store_bootstrap.py` (table creation/schema), (2) `artifact_store_queries.py` (SQL query construction), (3) `artifact_store_recording.py` (write/append operations), (4) `artifact_store_validation.py` (determinism checks). Extract view-specific helpers from `lineage/diagnostics.py` into a dedicated `plan/view_diagnostics.py` or into the views/ module where they belong.

**Effort:** medium
**Risk if unaddressed:** medium -- the large mixed-concern files accumulate more responsibilities over time.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
This is the weakest principle in scope. Two files exceed 1600 lines each and have four or more reasons to change.

**Findings:**
- `src/datafusion_engine/plan/artifact_store.py` (~1655 lines) changes for: Delta schema changes, SQL dialect updates, plan comparison logic, diff-gate policy, diagnostics format, and table naming. This violates SRP significantly.
- `src/datafusion_engine/udf/extension_runtime.py` (~1661 lines) changes for: Rust UDF registration ABI, snapshot validation rules, DDL SQL generation, WeakKeyDictionary lifecycle, capabilities probing, and error handling. At least 5 distinct reasons to change.
- `src/datafusion_engine/udf/metadata.py` (~1229 lines) changes for: UDF catalog refresh logic, function catalog introspection, UDF spec resolution, and rewrite tag indexing.
- `src/datafusion_engine/plan/bundle_artifact.py` (estimated ~2000+ lines based on 100KB size) -- the canonical plan artifact likely bundles computation, serialization, and fingerprinting concerns.
- `src/datafusion_engine/extensions/runtime_capabilities.py` (~357 lines) handles Delta compatibility probing, plugin manifest collection, plan capabilities detection, and execution metrics collection -- four distinct probe concerns.

**Suggested improvement:**
Prioritize splitting `artifact_store.py` and `extension_runtime.py`. Each should be decomposed into 3-4 focused modules of 300-500 lines, each with a single reason to change. For `extension_runtime.py`, extract: `udf_registration.py` (register/install), `udf_snapshot.py` (snapshot/validate), `udf_ddl.py` (DDL generation), and keep `extension_runtime.py` as a thin facade.

**Effort:** large
**Risk if unaddressed:** high -- these files are central to the system and will continue accumulating responsibilities.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The submodule boundaries generally produce cohesive groupings. The cache/ submodule, however, has high internal coupling -- `registry.py` imports from `inventory.py`, `ledger.py` imports from `commit_metadata.py`, `metadata_snapshots.py` imports from both `ledger.py` and `commit_metadata.py`, and the `__init__.py` re-exports from two modules. This forms a tightly coupled cluster rather than a set of independent components.

**Findings:**
- `src/datafusion_engine/cache/registry.py:12-17` -- imports `CacheInventoryEntry`, `ensure_cache_inventory_table`, `record_cache_inventory_entry` from `inventory.py`, creating circular awareness.
- `src/datafusion_engine/cache/metadata_snapshots.py:85-93` -- imports from both `commit_metadata.py` and `ledger.py` inside a loop body (lazy imports in hot path).
- `src/datafusion_engine/plan/udf_analysis.py` -- imports lazily from both `lineage.reporting` and `udf.extension_runtime`, coupling plan analysis to two distant submodules.
- `src/datafusion_engine/expr/domain_planner.py:8` -- imports `rewrite_tag_index` from `udf.metadata`, coupling expression planning to UDF metadata.

**Suggested improvement:**
Introduce a shared `cache/types.py` with pure data types (CacheInventoryEntry, CacheCommitMetadataRequest) and have both `inventory.py` and `registry.py` import from it. This breaks the bidirectional import path. For `domain_planner.py`, accept `rewrite_tag_index` as a parameter or define a Protocol for the tag index provider.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is generally correct -- expr/ and lineage/protocols.py are pure core modules that don't depend on infrastructure. However, some modules in the "core computation" layer have outward dependencies on infrastructure.

**Findings:**
- `src/datafusion_engine/lineage/diagnostics.py` -- Contains `view_udf_parity_payload` and `view_fingerprint_payload` which depend on view-layer concepts (view names, view fingerprints), violating the inward dependency rule. The lineage module should not know about views.
- `src/datafusion_engine/encoding/policy.py:141-145` -- `_datafusion_context()` creates a full `DataFusionRuntimeProfile` just to get a SessionContext for encoding operations. This is an unnecessarily heavy dependency for a utility function.
- `src/datafusion_engine/cache/inventory.py:21-22` -- Imports `DataFusionExecutionFacade` and `WritePipeline` directly, making the inventory module dependent on the full execution stack.
- `src/datafusion_engine/plan/result_types.py:~line 400+` -- Lazy import of `DataFusionExecutionFacade` inside `execute_plan_artifact()` to break circular dependency. This works but hides the coupling.

**Suggested improvement:**
Extract `view_udf_parity_payload` and `view_fingerprint_payload` from `lineage/diagnostics.py` into a views-layer module. For `encoding/policy.py`, accept a `SessionContext` parameter instead of creating one internally. For cache/ modules, define a `WriteSink` Protocol that the cache modules depend on, with the concrete `WritePipeline` implementation injected at composition time.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The lineage/ submodule demonstrates excellent Ports & Adapters design with `DiagnosticsSink` Protocol (`lineage/diagnostics.py:10`), `LineageQuery` and `LineageRecorder` Protocols (`lineage/protocols.py`), and an `InMemoryDiagnosticsSink` for testing. The extensions/ submodule uses `context_adaptation.py` to normalize native extension invocations. However, the cache/ and artifact_store code directly couples to Delta Lake and DataFusion without port abstractions.

**Findings:**
- `src/datafusion_engine/lineage/protocols.py` -- Clean set of runtime-checkable Protocols (`LineageScan`, `LineageJoin`, `LineageExpr`, `LineageQuery`) that define the lineage port boundary.
- `src/datafusion_engine/lineage/diagnostics.py:10` -- `DiagnosticsSink` Protocol with `InMemoryDiagnosticsSink` for testing is a textbook port/adapter.
- `src/datafusion_engine/plan/artifact_persistence.py:10` -- `_PlanPersistenceStore` Protocol is good but only used in one place and the Protocol is private.
- `src/datafusion_engine/cache/ledger.py` -- Directly imports `write_deltalake` from `deltalake.writer` at line 238, tightly coupling the ledger to DeltaLake.
- `src/datafusion_engine/cache/inventory.py` -- Directly calls `DataFusionExecutionFacade` and `WritePipeline`, bypassing any port abstraction.

**Suggested improvement:**
Make `_PlanPersistenceStore` public and use it consistently for all artifact store operations. For cache/, define a `CacheStore` Protocol that abstracts the Delta write operations, enabling unit testing without the Delta stack.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The most visible DRY violation is the duplication of type-coercion helpers across multiple modules. The same `_coerce_int` / `_coerce_opt_int` / `_coerce_optional_int` pattern appears in at least four files, each implementing the same business knowledge (how to safely convert a heterogeneous value to an integer).

**Findings:**
- `src/datafusion_engine/cache/inventory.py:309-321` -- `_coerce_int(value)` converts value to int.
- `src/datafusion_engine/cache/registry.py:296-308` -- `_coerce_opt_int(value)` -- identical logic, different name.
- `src/datafusion_engine/cache/metadata_snapshots.py:202-212` -- `_coerce_optional_int(value)` -- same logic, third name variant.
- `src/datafusion_engine/plan/diagnostics.py` -- Contains `_coerce_int` and `_coerce_float` helpers (noted as unused in prior analysis).
- `src/datafusion_engine/cache/metadata_snapshots.py:215-218` -- `_coerce_optional_str(value)` duplicates the pattern for strings.
- `src/datafusion_engine/plan/normalization.py` vs `src/datafusion_engine/plan/walk.py` -- Both implement `_safe_plan_inputs` / `_safe_inputs` for safely extracting plan child nodes.

**Suggested improvement:**
Create `src/datafusion_engine/coercion.py` (or use `src/utils/value_coercion.py` which already exists) as the single authoritative place for type-coercion helpers. All four modules should import from this shared location. Similarly, consolidate `_safe_plan_inputs` into `plan/walk.py` as the single source.

**Effort:** medium
**Risk if unaddressed:** medium -- the coercion logic will continue to drift between copies.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The ExprIR module demonstrates good contract enforcement with `_validate_call_name` checking exact and minimum argument counts. Protocol definitions in `lineage/protocols.py` establish clear interface contracts. However, many Protocols lack postcondition documentation, and some functions accept `object` types where narrower types would enforce contracts at the type level.

**Findings:**
- `src/datafusion_engine/expr/spec.py:277-285` -- `_validate_call_name` enforces arity contracts for expression calls. Exemplary.
- `src/datafusion_engine/plan/contracts.py:13-18` -- `PlanWithDeltaPinsRequestV1` uses `Any` for `view_nodes` and `runtime_profile`, weakening the contract boundary.
- `src/datafusion_engine/plan/artifact_persistence.py:11` -- `persist_plan` returns `object` in the Protocol, then uses `getattr` to extract `plan_identity_hash`. A stronger contract would declare the return type.
- `src/datafusion_engine/extensions/context_adaptation.py:47-53` -- `ExtensionEntrypointInvocation` uses `object` for `ctx`, `internal_ctx`, and `args`, providing no type-level contract.

**Suggested improvement:**
Replace `object` types in `PlanWithDeltaPinsRequestV1` with proper type annotations or Protocol references. Define a `PersistResult` Protocol or dataclass for `_PlanPersistenceStore.persist_plan` to avoid the `getattr` pattern.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The `expr/spec.py` module is the exemplar here: `ScalarLiteralCodec.from_input` parses heterogeneous scalar values into a tagged union (`ScalarLiteralSpec`) at the boundary, and all downstream code operates on the structured type. The extract metadata system (`metadata.py`) similarly parses untyped record mappings into frozen `ExtractMetadata` dataclasses once.

**Findings:**
- `src/datafusion_engine/expr/spec.py:119-134` -- `ScalarLiteralCodec.from_input` is textbook parse-don't-validate. Converts `ScalarLiteralInput` to `ScalarLiteralSpec` once at the boundary.
- `src/datafusion_engine/extract/metadata.py:160-183` -- `_metadata_from_record` parses untyped `Mapping[str, object]` into a fully typed `ExtractMetadata` dataclass.
- `src/datafusion_engine/cache/registry.py:262-278` -- `_record_from_row` parses a raw row mapping into a typed `CacheInventoryRecord`, but uses per-field coercion helpers rather than a single structured parse step.
- `src/datafusion_engine/extensions/runtime_capabilities.py:329-343` -- `_invoke_extension_with_context` tries multiple context candidates, validating at runtime. This is validation-style rather than parse-style.

**Suggested improvement:**
Consolidate the cache row parsing in `registry.py` to use a single `msgspec.convert` or `msgspec.decode` call for more structured parsing. This is a minor improvement since the current approach is functional.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The tagged union types in `expr/spec.py` (`ScalarLiteralSpec` using `tag=True`) are excellent examples of making illegal states unrepresentable. However, the `ExprIR.op` field is an unrestricted `str` when it should be an enum, and `ExprSpec` allows both `sql` and `expr_ir` to be `None` (only caught in `__post_init__`).

**Findings:**
- `src/datafusion_engine/expr/spec.py:38-83` -- `ScalarLiteralSpec` tagged union with `ScalarNullLiteral`, `ScalarBoolLiteral`, etc. makes impossible literal types unrepresentable.
- `src/datafusion_engine/expr/spec.py:194-201` -- `ExprIR.op` is `str` but only three values are valid: `"field"`, `"literal"`, `"call"`. An enum or `Literal["field", "literal", "call"]` type would prevent invalid ops at construction.
- `src/datafusion_engine/expr/spec.py:726-742` -- `ExprSpec` allows both `sql=None` and `expr_ir=None`, which is invalid state. The `__post_init__` raises at runtime, but the type system allows construction.
- `src/datafusion_engine/plan/result_types.py` -- `ExecutionResult` uses a `kind` string discriminator ("plan", "stream", etc.) instead of a tagged union.
- `src/datafusion_engine/compile/options.py:32-37` -- `DataFusionSqlPolicy` uses three independent bools that allow 8 combinations, but some combinations may be nonsensical (e.g., `allow_dml=True` with `allow_statements=False`).

**Suggested improvement:**
Change `ExprIR.op` to a `Literal["field", "literal", "call"]` type annotation. For `ExprSpec`, consider a tagged union with `SqlExprSpec` and `IrExprSpec` variants. For `ExecutionResult`, use msgspec tagged unions.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most query functions are pure (e.g., `extract_lineage`, `extract_plan_signals`, all expr/ functions). However, several cache/ledger functions violate CQS by both recording data (command) and returning the Delta version (query).

**Findings:**
- `src/datafusion_engine/cache/inventory.py:164-225` -- `record_cache_inventory_entry` writes a Delta row (command) and returns `int | None` (version, a query). Callers need to know if the write succeeded, but this mixes concerns.
- `src/datafusion_engine/cache/ledger.py:136-195` -- `record_cache_run_summary` writes, re-registers the table, and returns the version number. Three effects plus a return value.
- `src/datafusion_engine/cache/ledger.py:230-290` -- `record_cache_snapshot_registry` same pattern -- write + re-register + return version.
- `src/datafusion_engine/cache/registry.py:131-174` -- `resolve_cache_hit` is a query that internally calls `ensure_cache_inventory_table` (a command) and `enforce_schema_evolution` (a command).
- `src/datafusion_engine/cache/metadata_snapshots.py:67-186` -- `snapshot_datafusion_caches` writes snapshots (command), registers tables (command), records to ledger (command), and returns diagnostics payloads (query). This is the most CQS-violating function in scope.

**Suggested improvement:**
Separate recording from version reporting. `record_cache_inventory_entry` should return `None` (pure command), with a separate `latest_cache_inventory_version(view_name)` query. Alternatively, define a `RecordResult` dataclass that makes the command/query boundary explicit: `RecordResult(recorded=True, version=3)`.

**Effort:** medium
**Risk if unaddressed:** medium -- mixing commands and queries makes reasoning about side effects harder during refactoring.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
The lineage/ submodule demonstrates strong dependency inversion with its Protocol-based design. The extensions/ submodule uses `resolve_extension_module` and `invoke_entrypoint_with_adapted_context` to abstract native extension access. However, the cache/ submodule creates `DataFusionExecutionFacade` and `WritePipeline` directly, tightly coupling to concrete implementations.

**Findings:**
- `src/datafusion_engine/lineage/protocols.py` -- Five runtime-checkable Protocols defining the lineage abstraction boundary. Exemplary DI.
- `src/datafusion_engine/cache/inventory.py:142-148` -- Creates `DataFusionExecutionFacade` inline rather than accepting it as a parameter.
- `src/datafusion_engine/cache/ledger.py:125-132` -- Same pattern: `DataFusionExecutionFacade` created inline in `ensure_cache_run_summary_table`.
- `src/datafusion_engine/plan/result_types.py:~400` -- Lazy import of `DataFusionExecutionFacade` to avoid circular import, but this hides an explicit dependency.
- `src/datafusion_engine/encoding/policy.py:141-145` -- `_datafusion_context()` creates `DataFusionRuntimeProfile()` internally, hiding a significant dependency.

**Suggested improvement:**
Accept `DataFusionExecutionFacade` as a parameter in cache/ledger functions instead of creating it internally. For encoding, accept `SessionContext` as a parameter.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
The codebase consistently uses composition. `PerformancePolicy` composes `CachePolicyTier`, `StatisticsPolicy`, and `PlanBundleComparisonPolicy`. `NormalizePolicy` composes `EncodingPolicy` and `ChunkPolicy`. `CacheInventoryRegistry` composes `MutableRegistry`. No deep inheritance hierarchies exist in the reviewed scope.

**Findings:**
- `src/datafusion_engine/plan/perf_policy.py` -- `PerformancePolicy` composes three sub-policies. Clean composition.
- `src/datafusion_engine/encoding/policy.py:33-38` -- `NormalizePolicy` composes `EncodingPolicy` and `ChunkPolicy`.
- `src/datafusion_engine/cache/registry.py:63-67` -- `CacheInventoryRegistry` delegates to `MutableRegistry`.

No action needed.

**Effort:** --
**Risk if unaddressed:** --

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code follows LoD, but some chain through runtime profile accessors.

**Findings:**
- `src/datafusion_engine/cache/ledger.py:325-326` -- `profile.io_ops.cache_root()` traverses two levels into the profile object.
- `src/datafusion_engine/cache/inventory.py:249` -- `profile.policies.plan_artifacts_root` traverses into nested policy objects.
- `src/datafusion_engine/sql/guard.py:237-239` -- `runtime_profile.features.enable_expr_planners` and `runtime_profile.policies.expr_planner_hook` traverse two levels.
- `src/datafusion_engine/cache/registry.py:156` -- `profile.delta_ops.delta_service().table_version(...)` traverses three levels.

**Suggested improvement:**
Consider adding convenience methods to `DataFusionRuntimeProfile` like `cache_root()`, `plan_artifacts_root()`, `delta_table_version(path, ...)` that encapsulate the traversal. This keeps the profile as the direct collaborator.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most data types properly encapsulate their logic. `ExprSpec.to_expr()` and `ExprIR.to_sql()` are good examples of telling objects to produce their output rather than asking for raw data. However, `DataFusionPlanArtifact` appears to be largely an anemic data holder that exposes many attributes for external logic to operate on.

**Findings:**
- `src/datafusion_engine/expr/spec.py:206-225` -- `ExprIR.to_sql()` encapsulates SQL generation logic within the spec object. Good tell-don't-ask.
- `src/datafusion_engine/expr/spec.py:227-251` -- `ExprIR.to_expr()` similarly encapsulates expression construction.
- `src/datafusion_engine/plan/artifact_serialization.py:10-17` -- `bundle_payload(bundle)` asks `bundle` for 4 attributes and constructs a payload externally. This should be a method on `DataFusionPlanArtifact`.
- `src/datafusion_engine/plan/artifact_persistence.py:27` -- `getattr(row, "plan_identity_hash", None)` uses runtime attribute access rather than a typed interface.
- `src/datafusion_engine/plan/signals.py` -- `extract_plan_signals` receives a plan artifact and queries many of its attributes to build `PlanSignals`. Some of this logic could be methods on the artifact itself.

**Suggested improvement:**
Move `bundle_payload` logic into a `to_payload()` method on `DataFusionPlanArtifact`. Add `to_signals()` or `extract_signals()` as a method on the artifact class.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The expr/ submodule is almost entirely functional -- pure expression building, SQL generation, and type conversions with no IO. The lineage/reporting.py module is a pure functional extraction of lineage from plan objects. The lineage/protocols.py definitions are pure. The cache/ and artifact_store code, by contrast, deeply interleave IO (Delta writes, table registration) with computation (schema construction, fingerprinting).

**Findings:**
- `src/datafusion_engine/expr/` -- Entire submodule is functional core. `spec.py`, `query_spec.py`, `cast.py`, `span.py` are all pure transformations.
- `src/datafusion_engine/lineage/reporting.py` -- `extract_lineage` is a pure function that walks a plan tree and returns frozen structs.
- `src/datafusion_engine/plan/signals.py` -- `extract_plan_signals` is pure computation.
- `src/datafusion_engine/cache/metadata_snapshots.py:67-186` -- `snapshot_datafusion_caches` deeply interleaves SQL execution, Delta writes, table registration, diagnostics recording, and error handling in a single function.
- `src/datafusion_engine/cache/inventory.py:84-161` -- `ensure_cache_inventory_table` mixes filesystem checks, Delta bootstrap writes, table registration, and diagnostics recording.

**Suggested improvement:**
Extract the pure schema and payload construction in cache/ modules into separate pure functions. The IO operations (write, register, record) should be in a thin imperative shell that calls the pure functions for data preparation.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
The Delta append operations in cache/ are inherently non-idempotent -- re-running with the same inputs creates duplicate rows. The `ensure_cache_inventory_table` function is designed to be safe to call multiple times (it checks for existing `_delta_log`), which is good. Plan fingerprinting supports idempotency at the plan level.

**Findings:**
- `src/datafusion_engine/cache/inventory.py:84-97` -- `ensure_cache_inventory_table` checks for existing Delta log before bootstrapping. Idempotent by design.
- `src/datafusion_engine/cache/inventory.py:164-225` -- `record_cache_inventory_entry` performs an append with no dedup check. Re-running creates duplicate inventory entries.
- `src/datafusion_engine/cache/ledger.py:136-195` -- `record_cache_run_summary` same pattern -- no dedup guard on repeated calls with the same `run_id`.

**Suggested improvement:**
For `record_cache_inventory_entry`, consider adding an upsert mode or a dedup-on-read strategy that uses `run_id` + `view_name` as a natural key. Alternatively, document the design decision that dedup is handled at the read layer.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is a first-class concern throughout. Plan fingerprinting uses canonical hashing (`hash_json_default`, `hash_msgpack_canonical`, `hash_sha256_hex`). Schema identity hashing, plan identity hashing, and policy fingerprints are all computed deterministically. The `ExprIR` spec produces deterministic SQL output. The `artifact_store.py` includes determinism validation against historical artifacts.

**Findings:**
- `src/datafusion_engine/plan/bundle_artifact.py:42-46` -- Uses `hash_json_default`, `hash_msgpack_canonical`, `hash_sha256_hex` from `utils/hashing.py` for deterministic fingerprinting.
- `src/datafusion_engine/plan/artifact_store.py:67-77` -- `DeterminismValidationResult` dataclass for validating plan determinism against the artifact store.
- `src/datafusion_engine/compile/options.py:39-44` -- `DataFusionSqlPolicy.fingerprint_payload()` and `fingerprint()` produce deterministic hashes.
- `src/datafusion_engine/expr/planner.py:24-34` -- `ExprPlannerPolicy` has deterministic `fingerprint()`.

No action needed.

**Effort:** --
**Risk if unaddressed:** --

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are appropriately simple. A few show unnecessary complexity.

**Findings:**
- `src/datafusion_engine/extensions/context_adaptation.py:82-97` -- `select_context_candidate` accepts four parameters but always returns a single `("outer", ctx)` pair regardless of inputs. The `internal_ctx`, `allow_fallback`, and `fallback_ctx` parameters are unused (assigned to `_`). This is dead complexity.
- `src/datafusion_engine/expr/spec.py:18-35` -- 17 named constants (`_EXACT_ONE = 1`, `_EXACT_TWO = 2`, etc.) add indirection without meaningful semantic value. Using raw literals `1`, `2`, `3` in arity checks would be equally clear and simpler.
- `src/datafusion_engine/expr/planner.py:70-92` -- `_load_extension()` iterates over a tuple with exactly one element `("datafusion_engine.extensions.datafusion_ext",)`, adding loop complexity for a single-item collection.

**Suggested improvement:**
Simplify `select_context_candidate` to remove unused parameters. Remove the `_EXACT_ONE`..`_EXACT_FOUR` constants and use integer literals directly (the `_MIN_*` and `_MAX_*` constants with meaningful names can stay). Simplify `_load_extension` to directly import the single known module.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
A few thin wrapper modules and unused flexibility add small amounts of speculative generality.

**Findings:**
- `src/datafusion_engine/udf/extension_loader.py` -- Contains a single `load_extension_module` function that wraps `importlib.import_module`. The thin wrapper adds indirection without clear extensibility benefit.
- `src/datafusion_engine/udf/extension_lifecycle.py` -- Contains two thin wrappers (`install_udfs`, `install_udf_ddl`) that could be inlined into callers.
- `src/datafusion_engine/udf/extension_validation.py` -- Contains two thin wrappers (`validate_runtime_capabilities`, `capability_report`) that delegate directly to `extension_runtime`.
- `src/datafusion_engine/sql/options.py:69-82` -- `statement_sql_options_for_profile` and `planning_sql_options` are trivial wrappers around `sql_options_for_profile` and `safe_sql_options_for_profile` respectively.

**Suggested improvement:**
Consider inlining the three thin UDF wrapper modules (`extension_loader.py`, `extension_lifecycle.py`, `extension_validation.py`) into `extension_runtime.py` or the caller sites. However, if these are seams designed for future extension, document the intent.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. Two naming/behavior surprises stand out.

**Findings:**
- `src/datafusion_engine/sql/options.py:57-66` -- `safe_sql_options_for_profile(profile)` accepts a `profile` parameter but ignores it entirely (`_ = profile`), always returning the default read-only options. A caller providing a profile might expect it to influence the result. The function name suggests profile-dependent behavior.
- `src/datafusion_engine/plan/profiler.py` -- `capture_explain` catches `BaseException` (not `Exception`) and checks the class name as a string (`"PanicException"`), which is an unusual and potentially surprising error handling pattern.
- `src/datafusion_engine/extract/registry.py:87-103` -- `adapter_executor_key(adapter_name)` is a function that always returns its argument unchanged (`return adapter_name`). This adds a layer of indirection that may surprise readers.

**Suggested improvement:**
Either rename `safe_sql_options_for_profile` to `default_safe_sql_options()` (removing the unused parameter) or implement profile-dependent behavior. For `adapter_executor_key`, add a docstring explaining this is a seam for future mapping changes, or inline it.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
All modules have explicit `__all__` declarations, which is excellent. Table names are versioned (`datafusion_plan_artifacts_v10`, `datafusion_write_artifacts_v2`). However, the ExprIR operation names (`"field"`, `"literal"`, `"call"`) are undocumented magic strings, and the `_EXPR_CALLS` / `_SQL_CALLS` dispatch tables define the supported function name contract implicitly through dictionary keys rather than through explicit documentation or type.

**Findings:**
- All 68 files have `__all__` declarations. Consistent and disciplined.
- `src/datafusion_engine/plan/artifact_store.py:50-53` -- Table names are versioned: `PLAN_ARTIFACTS_TABLE_NAME = "datafusion_plan_artifacts_v10"`.
- `src/datafusion_engine/expr/spec.py:616-660` -- `_EXACT_CALL_COUNTS` and `_MIN_CALL_COUNTS` define the supported function arity contract, but only as dict keys. No public documentation of supported function names.
- `src/datafusion_engine/expr/spec.py:194-201` -- `ExprIR.op` is an unconstrained string. The valid values are only documented by the `if/elif` chains in `to_sql()` and `to_expr()`.

**Suggested improvement:**
Define a `Literal["field", "literal", "call"]` type for `ExprIR.op`. Consider a public constant tuple `SUPPORTED_EXPR_FUNCTIONS: tuple[str, ...]` that documents the supported function names for the `_EXPR_CALLS` dispatch table.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The expr/ and lineage/ submodules are highly testable -- pure functions with no IO dependencies. The cache/ submodule is the least testable, requiring a full DataFusion SessionContext and Delta Lake filesystem to exercise. The `InMemoryDiagnosticsSink` in `lineage/diagnostics.py` is an excellent testing seam.

**Findings:**
- `src/datafusion_engine/expr/spec.py` -- Pure functions, testable without any mocking.
- `src/datafusion_engine/lineage/reporting.py` -- `extract_lineage` is pure, testable with synthetic plan objects.
- `src/datafusion_engine/lineage/diagnostics.py` -- `InMemoryDiagnosticsSink` enables testing diagnostics recording without real sinks.
- `src/datafusion_engine/cache/inventory.py:84-161` -- `ensure_cache_inventory_table` requires a real SessionContext and filesystem, making unit testing expensive.
- `src/datafusion_engine/cache/ledger.py:293-322` -- `_bootstrap_cache_ledger_table` directly calls `write_deltalake` and `WritePipeline`, requiring integration-level setup.
- `src/datafusion_engine/encoding/policy.py:141-145` -- `_datafusion_context()` creates a full runtime profile internally, making it impossible to inject a test context.

**Suggested improvement:**
Extract pure computation (schema construction, payload building, SQL generation) from cache/ functions into separate testable functions. Accept `SessionContext` and write-infrastructure as parameters rather than creating them internally. For encoding, accept `SessionContext` as a parameter.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The diagnostics infrastructure is well-designed. `DiagnosticsSink` Protocol with `InMemoryDiagnosticsSink` provides structured diagnostics. `PlanPhaseDiagnostics`, `PlanBundleDiagnostics`, and `PlanSignals` provide structured plan telemetry. `RuntimeCapabilitiesSnapshot` captures a comprehensive capability probe. However, some functions catch exceptions broadly without structured error reporting.

**Findings:**
- `src/datafusion_engine/lineage/diagnostics.py` -- Full diagnostics infrastructure with `DiagnosticsSink` Protocol, `DiagnosticsRecorder`, and structured payload types. Excellent.
- `src/datafusion_engine/plan/diagnostics.py` -- `PlanPhaseDiagnostics` and `PlanBundleDiagnostics` provide structured plan telemetry.
- `src/datafusion_engine/extensions/runtime_capabilities.py:113` -- Catches `(RuntimeError, TypeError, ValueError, AttributeError)` with only `_LOGGER.debug`. Production probe failures may be silently swallowed.
- `src/datafusion_engine/cache/metadata_snapshots.py:107-108` -- `with suppress(Exception): source = ctx.sql(sql)` swallows all exceptions silently, including programming errors. Should at minimum log.
- `src/datafusion_engine/udf/extension_runtime.py` -- Large file with error handling that sometimes catches `BaseException` or broad exception sets.

**Suggested improvement:**
Replace `suppress(Exception)` in `metadata_snapshots.py:107` with explicit exception types and logging. Add structured error payloads to capability probe failures in `runtime_capabilities.py` instead of swallowing silently at debug level.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Oversized multi-responsibility files

`artifact_store.py` (1655 LOC) and `extension_runtime.py` (1661 LOC) are the two largest files in the reviewed scope and both have 4+ reasons to change. They share a pattern: they started as single-concern modules but accumulated related functionality over time because it was convenient. Both need decomposition to maintain long-term velocity.

**Root cause:** Missing internal module boundaries within the plan/ and udf/ submodules.
**Affected principles:** P2 (Separation of concerns), P3 (SRP), P23 (Testability).
**Suggested approach:** Break each into 3-4 focused modules with clear responsibilities. For `artifact_store.py`: bootstrap, queries, recording, validation. For `extension_runtime.py`: registration, snapshot, DDL, capabilities.

### Theme 2: Duplicated type-coercion knowledge

At least four copies of integer coercion helpers exist across cache/inventory.py, cache/registry.py, cache/metadata_snapshots.py, and plan/diagnostics.py. Each has a slightly different name and slightly different edge-case handling, which means they will drift.

**Root cause:** No shared utility for safe type coercion in the DataFusion engine layer. `utils/value_coercion.py` exists at the project level but is not consistently used.
**Affected principles:** P7 (DRY).
**Suggested approach:** Consolidate all coercion helpers into `utils/value_coercion.py` or create `src/datafusion_engine/coercion.py` and import from it everywhere.

### Theme 3: Cache submodule couples computation with IO

The cache/ submodule deeply interleaves pure computation (schema construction, payload building) with IO operations (Delta writes, table registration, filesystem checks). This makes unit testing expensive and violates functional core / imperative shell.

**Root cause:** Each cache module was written to perform end-to-end operations rather than separating data preparation from persistence.
**Affected principles:** P11 (CQS), P16 (Functional core), P23 (Testability).
**Suggested approach:** Extract pure data preparation functions from each cache module. Create a thin `CacheWriter` that accepts prepared data and handles the IO. Test the preparation independently.

### Theme 4: Lazy imports as coupling management

Throughout the scope, lazy imports (imports inside function bodies) are used to break circular dependencies: `plan/udf_analysis.py` lazily imports from `lineage/reporting` and `udf/extension_runtime`; `plan/result_types.py` lazily imports `DataFusionExecutionFacade`; `cache/metadata_snapshots.py` lazily imports from `commit_metadata` and `ledger` inside a loop body.

**Root cause:** Some cross-submodule dependencies create circular import chains that would fail at module load time.
**Affected principles:** P4 (Low coupling), P5 (Dependency direction).
**Suggested approach:** Analyze the import cycles and resolve by extracting shared types into leaf modules, or by introducing Protocol-based dependency inversion at the boundaries.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate 4+ duplicate `_coerce_int` / `_coerce_opt_int` helpers into shared module | small | Eliminates knowledge drift across cache/ modules |
| 2 | P21 (Least astonishment) | Rename or fix `safe_sql_options_for_profile` that ignores its argument | small | Prevents caller confusion |
| 3 | P19 (KISS) | Remove unused parameters from `select_context_candidate` | small | Reduces dead complexity |
| 4 | P22 (Public contracts) | Type `ExprIR.op` as `Literal["field", "literal", "call"]` | small | Makes valid operations explicit at type level |
| 5 | P24 (Observability) | Replace `suppress(Exception)` with explicit exception types + logging in `metadata_snapshots.py` | small | Prevents silent swallowing of programming errors |

## Recommended Action Sequence

1. **Consolidate coercion helpers (P7).** Create `src/datafusion_engine/coercion.py` or extend `utils/value_coercion.py` with `coerce_int`, `coerce_opt_int`, `coerce_opt_str`, `coerce_str_tuple`. Update all four cache/ modules to import from this shared location. Also consolidate `_safe_plan_inputs` from `plan/normalization.py` into `plan/walk.py`.

2. **Fix naming and unused parameters (P19, P21).** Rename `safe_sql_options_for_profile` or implement profile-dependent behavior. Remove unused parameters from `select_context_candidate`. Type `ExprIR.op` as `Literal`.

3. **Replace broad exception suppression (P24).** Replace `suppress(Exception)` in `metadata_snapshots.py` with explicit exception types and structured logging.

4. **Decompose `artifact_store.py` (P3, P2).** Split the 1655-line file into bootstrap, queries, recording, and validation modules. This is the most impactful SRP improvement.

5. **Decompose `extension_runtime.py` (P3, P2).** Split the 1661-line file into registration, snapshot, DDL, and capabilities modules.

6. **Extract pure computation from cache/ (P16, P23, P11).** Separate data preparation functions from IO operations in `cache/inventory.py`, `cache/ledger.py`, and `cache/metadata_snapshots.py`. This enables unit testing and clarifies CQS boundaries.

7. **Resolve lazy import cycles (P4, P5).** Analyze and resolve circular imports by extracting shared types into leaf modules or introducing Protocols at submodule boundaries. Start with the `plan/ <-> lineage/` and `plan/ <-> udf/` cycles.

8. **Move view-specific helpers from lineage/diagnostics.py (P5).** Extract `view_udf_parity_payload` and `view_fingerprint_payload` to the views layer where they conceptually belong.
