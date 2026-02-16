# Design Review: src/semantics/incremental/

**Date:** 2026-02-16
**Scope:** `src/semantics/incremental/` (18 files, ~3,900 LOC)
**Focus:** All principles (1-24), with special attention to information hiding, SRP in runtime/reader split, dependency direction, ports & adapters for Delta Lake, and state management correctness
**Depth:** deep (all files examined)
**Files reviewed:** 18

## Executive Summary

The incremental/CDF subsystem demonstrates strong foundational design in its type system (`cdf_types.py`, `cdf_cursors.py`, `config.py`) and merge strategy framework (`cdf_joins.py`), with well-defined value types, frozen dataclasses, and good test coverage for the lower layers. The primary structural concerns are: (1) a **duplicate `CdfReadResult` class and `read_cdf_changes` function** across `cdf_reader.py` and `cdf_runtime.py` with divergent signatures, violating DRY and creating confusion about which is canonical; (2) the **Delta Lake storage layer is not abstracted behind a port**, causing deep coupling between domain logic and `deltalake`/DataFusion internals; (3) an **inverted dependency** where `datafusion_engine/session/runtime.py` imports `CdfCursorStore` from `semantics.incremental`, violating the dependency direction principle; and (4) **several modules (metadata.py, cdf_runtime.py, delta_context.py) mix orchestration, IO, and domain logic**, compressing concerns that would benefit from separation.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `IncrementalRuntime` exposes `profile` publicly; internal `_CdfReadState` well hidden |
| 2 | Separation of concerns | 1 | medium | medium | `cdf_runtime.py` and `metadata.py` mix IO, orchestration, and domain logic |
| 3 | SRP | 1 | medium | medium | `cdf_runtime.py` handles read, registration, telemetry, and fallback in one module |
| 4 | High cohesion, low coupling | 2 | small | low | Types module well cohesive; coupling to DataFusion profile internals is broad |
| 5 | Dependency direction | 1 | medium | high | `datafusion_engine` imports `CdfCursorStore`; core depends on detail |
| 6 | Ports & Adapters | 1 | large | medium | No storage port; `deltalake` API embedded in domain modules |
| 7 | DRY | 0 | medium | high | Duplicate `CdfReadResult` and `read_cdf_changes` in two files |
| 8 | Design by contract | 2 | small | low | `CdfCursor` validates via `NonNegInt`; `CdfReadOptions` validation is thin |
| 9 | Parse, don't validate | 2 | small | low | `CdfCursor` uses msgspec struct parsing; `CdfFilterPolicy` constructed via factories |
| 10 | Illegal states | 2 | small | medium | `SemanticIncrementalConfig` allows `enabled=True` with `state_dir=None` |
| 11 | CQS | 2 | small | low | `update_version` returns and persists (minor CQS blend); most functions clean |
| 12 | DI + explicit composition | 1 | medium | medium | `IncrementalRuntime` internally constructs `RegistryFacade`; `cdf_reader.py` uses callable injection |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition-based |
| 14 | Law of Demeter | 1 | medium | medium | Deep chain access `runtime.profile.policies.scan_policy`, `runtime.profile.delta_ops.delta_service()` |
| 15 | Tell, don't ask | 2 | small | low | `CdfFilterPolicy` encapsulates predicate generation well; some raw data access in metadata |
| 16 | Functional core, imperative shell | 1 | large | medium | No clear separation; IO interspersed through domain transformations |
| 17 | Idempotency | 2 | small | low | Cursor writes are overwrite-safe; `ensure_dirs` idempotent; re-read is safe |
| 18 | Determinism | 2 | small | low | Plan fingerprints support reproducibility; `uuid7_hex` introduces controlled non-determinism |
| 19 | KISS | 2 | small | low | Most modules are straightforward; `cdf_runtime.py` has unnecessary complexity in fallback chains |
| 20 | YAGNI | 2 | small | low | `export_builders.py` is complex but in use; `CdfReadOptions` has 4 testing overrides |
| 21 | Least astonishment | 1 | medium | medium | Two `CdfReadResult` classes with different fields; two `read_cdf_changes` with different signatures |
| 22 | Public contracts | 2 | small | low | `__all__` consistently declared; `__init__.py` curates public surface |
| 23 | Testability | 2 | small | low | `cdf_reader.py` well-testable via callable injection; `cdf_runtime.py` requires full runtime |
| 24 | Observability | 2 | small | low | `record_artifact` used for CDF reads and maintenance; no logging or tracing spans |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The subsystem generally hides internal state well. Private dataclasses like `_CdfReadState` and `_CdfReadRecord` in `cdf_runtime.py` are properly prefixed and not exported. The `CdfCursorStore` hides its filesystem layout behind `_cursor_file()`.

**Findings:**
- `IncrementalRuntime` at `src/semantics/incremental/runtime.py:42` exposes `profile: DataFusionRuntimeProfile` as a public field, which is then accessed deeply by 8 different call sites across the subsystem (e.g., `runtime.profile.policies.scan_policy` at `cdf_runtime.py:108`, `runtime.profile.delta_ops.delta_service()` at `cdf_runtime.py:111`). This leaks the entire profile structure as a public dependency surface.
- `StateStore` at `src/semantics/incremental/state_store.py:10-197` exposes its full filesystem layout through 14 public path methods, making the directory structure a public contract rather than an internal detail.

**Suggested improvement:**
Add domain-specific accessor methods to `IncrementalRuntime` that encapsulate the common profile access patterns (e.g., `delta_service()`, `scan_policy()`, `settings_hash()`), reducing direct `profile.xxx.yyy` chains in consuming modules.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
Several modules conflate orchestration, IO, observability, and domain logic.

**Findings:**
- `cdf_runtime.py` (500 LOC) at `src/semantics/incremental/cdf_runtime.py` handles: (a) Delta CDF input resolution (lines 84-125), (b) DataFusion table registration (lines 262-292), (c) CDF read with fallback orchestration (lines 128-198, 299-326), (d) telemetry recording (lines 447-497), and (e) cursor state management (lines 201-229, 428-430) -- all in one module.
- `metadata.py` at `src/semantics/incremental/metadata.py` mixes three distinct concerns: runtime metadata persistence (lines 42-82), cursor snapshot persistence (lines 85-135), and diagnostics artifact persistence (lines 172-341). Each has distinct change reasons.
- `delta_context.py` at `src/semantics/incremental/delta_context.py:189-257` embeds Delta maintenance decision logic, artifact recording, and plan execution in `run_delta_maintenance_if_configured()`, which conflates policy resolution with IO execution.

**Suggested improvement:**
Extract `cdf_runtime.py` into at least three focused modules: (1) CDF input resolution (pure data transforms), (2) CDF dataset registration (DataFusion side effects), and (3) CDF read orchestration (coordinator). Split `metadata.py` into per-concern persistence modules.

**Effort:** medium
**Risk if unaddressed:** medium -- increasing difficulty adding new persistence targets or changing telemetry patterns.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
The module-level separation mixes "what" (CDF domain logic) with "how" (Delta Lake specifics) and "when" (orchestration).

**Findings:**
- `cdf_runtime.py` changes for at least four reasons: (1) CDF read logic changes, (2) DataFusion registration API changes, (3) telemetry format changes, (4) fallback strategy changes. The function `read_cdf_changes()` at `cdf_runtime.py:329-444` is 115 lines orchestrating all four concerns.
- `plan_fingerprints.py` at `src/semantics/incremental/plan_fingerprints.py` mixes fingerprint data model concerns (lines 42-58) with Delta read/write IO (lines 72-192) and storage option resolution (lines 166-178). The storage option dict-comprehension at lines 167-178 is particularly noisy plumbing that obscures the domain intent.
- `metadata.py` functions `_write_artifact_table` (lines 314-341), `_write_artifact_rows` (lines 260-284), and `_write_view_artifact_rows` (lines 287-311) are near-identical except for the table construction step, indicating a template-method opportunity.

**Suggested improvement:**
For `metadata.py`, extract a shared `_write_delta_artifact(name, path, table, context)` helper and have each specific writer supply only the table construction logic. For `plan_fingerprints.py`, separate the snapshot data model from the Delta persistence mechanism.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Type definition modules (`cdf_types.py`, `cdf_cursors.py`, `types.py`, `config.py`) are well-cohesive. However, most operational modules have broad coupling to `datafusion_engine` internals.

**Findings:**
- `cdf_runtime.py` imports from 10 distinct `datafusion_engine` submodules at lines 13-19, creating a wide coupling surface.
- `delta_context.py` at lines 12-28 imports from 8 distinct modules spanning `datafusion_engine.dataset`, `datafusion_engine.delta`, `datafusion_engine.session`, and `storage.deltalake`, coupling itself to the full Delta operational stack.
- Conversely, `cdf_types.py` and `cdf_joins.py` are well-bounded: `cdf_types.py` only imports `core.config_base` and `datafusion.expr` (the latter only under `TYPE_CHECKING`).

**Suggested improvement:**
Consider a facade or adapter that encapsulates the common `datafusion_engine` operations needed by the incremental subsystem (register dataset, execute DF, resolve storage), reducing the import surface for operational modules.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
There is a clear dependency inversion violation where the infrastructure layer depends on the semantic domain layer.

**Findings:**
- `src/datafusion_engine/session/runtime.py:172` imports `CdfCursorStore` from `semantics.incremental.cdf_cursors`. This means the DataFusion engine infrastructure layer depends on the semantic domain layer, which inverts the intended dependency direction (core/infrastructure should not import domain-specific types).
- The `IncrementalRuntime` at `src/semantics/incremental/runtime.py:39-148` depends on `DataFusionRuntimeProfile`, `SessionRuntime`, `PolicyBundleConfig`, `DataFusionIOAdapter`, `ProviderRegistry`, `DatasetCatalog`, `RegistryFacade`, and `UdfCatalogAdapter` -- all from `datafusion_engine`. While this direction (domain depending on infrastructure) is acceptable for adapters, the runtime class is not positioned as an adapter; it is the core domain runtime.

**Suggested improvement:**
Move `CdfCursorStore` (or at minimum, its protocol/interface) to a shared types location (e.g., `core_types` or `storage.deltalake`) so that `datafusion_engine` can depend on the abstraction without importing from `semantics`. Alternatively, have `datafusion_engine` declare a protocol that `CdfCursorStore` satisfies.

**Effort:** medium
**Risk if unaddressed:** high -- this circular-ish dependency makes it harder to test and evolve either subsystem independently.

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
The Delta Lake storage layer has no port abstraction. Domain logic directly calls `deltalake.DeltaTable` and DataFusion-specific APIs.

**Findings:**
- `cdf_runtime.py:152-198` (`_read_cdf_table_fallback`) directly imports and instantiates `deltalake.DeltaTable`, calls `table.load_cdf()`, and manually normalizes arro3 types. This is raw vendor integration embedded in the domain module.
- `cdf_reader.py:221-237` (`_fallback_load_cdf`) contains a near-identical direct `deltalake.DeltaTable` instantiation and `load_cdf()` call -- duplicating the vendor coupling.
- `write_helpers.py:45-108` (`write_delta_table_via_pipeline`) tightly couples to `WritePipeline`, `WriteRequest`, `WriteFormat`, and `WriteMode` from `datafusion_engine.io.write`, with no abstraction boundary.
- In contrast, `cdf_reader.py` demonstrates partial port thinking: `CdfReadOptions` at lines 68-117 accepts injectable `delta_table_version_fn`, `delta_cdf_enabled_fn`, `read_delta_cdf_fn`, and `arrow_table_to_dataframe_fn` callables, enabling testability. This pattern is not applied to `cdf_runtime.py`.

**Suggested improvement:**
Define a `DeltaCdfPort` protocol with methods like `table_version()`, `cdf_enabled()`, `read_cdf()`, and `write_table()`. Have `cdf_reader.py`'s callable injection formalized into this protocol, and have `cdf_runtime.py` depend on the same abstraction rather than direct `deltalake` calls.

**Effort:** large
**Risk if unaddressed:** medium -- vendor API changes (e.g., arro3 normalization at `cdf_runtime.py:165-167`) propagate into domain logic; testing `cdf_runtime.py` requires full Delta infrastructure.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 0/3

**Current state:**
The most critical DRY violation in the subsystem is the duplication of the `CdfReadResult` type and `read_cdf_changes` function.

**Findings:**
- **Duplicate `CdfReadResult` class:** Defined at `cdf_reader.py:40` (fields: `df`, `start_version`, `end_version`, `has_changes`) AND at `cdf_runtime.py:36` (fields: `table`, `updated_version`). These are **different types with the same name** representing the same concept. The `__init__.py` at line 30 re-exports from `cdf_reader.py`, but `cdf_runtime.py`'s version is also exported at line 500.
- **Duplicate `read_cdf_changes` function:** Defined at `cdf_reader.py:253` (takes `SessionContext`, `table_path`, `CdfReadOptions`) AND at `cdf_runtime.py:329` (takes `DeltaAccessContext`, keyword-only params, `CdfCursorStore`). Both implement CDF read orchestration with different approaches to the same problem.
- **Duplicate fallback logic:** `cdf_reader.py:221-237` (`_fallback_load_cdf`) and `cdf_runtime.py:147-198` (`_read_cdf_table_fallback`) both instantiate `deltalake.DeltaTable`, call `load_cdf()` with the same options pattern, and coerce results. The `cdf_runtime.py` version additionally handles filter policy application.
- **Repeated storage option resolution:** The pattern `resolved_storage = resolved_store.storage_options or {}` / `resolved_log_storage = resolved_store.log_storage_options or {}` appears in `cdf_runtime.py:96-97`, `delta_context.py:143-144`, and `plan_fingerprints.py:167-172`.
- **Repeated write boilerplate:** `metadata.py` contains three near-identical functions (`_write_artifact_table` lines 314-341, `_write_artifact_rows` lines 260-284, `_write_view_artifact_rows` lines 287-311) that differ only in how they construct the `pa.Table`.

**Suggested improvement:**
Consolidate to a single canonical `CdfReadResult` type. If the two read pathways serve different callers (session-context-based vs. runtime-based), keep one orchestrator and have the other delegate. Extract a shared `_fallback_read_cdf()` helper. Extract a generic `_write_named_artifact(name, path, table, context)` helper for `metadata.py`.

**Effort:** medium
**Risk if unaddressed:** high -- Two types with the same name but different fields will cause import confusion, silent bugs when the wrong one is used, and maintenance drift.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Some boundary validation exists but there are gaps in enforcing preconditions.

**Findings:**
- `CdfCursor` at `cdf_cursors.py:49` uses `NonNegInt = Annotated[int, msgspec.Meta(ge=0)]` to enforce that `last_version` is non-negative -- good contract enforcement at the type level.
- `_validate_cdf_options` at `cdf_reader.py:120-123` validates that `dataset_name` is required when `cursor_store` is provided, but there is no validation that `start_version` is non-negative.
- `SemanticIncrementalConfig` has no validation that `state_dir` is provided when `enabled=True`. The `with_cdf_enabled` factory ensures this, but direct construction at `config.py:92-102` allows `enabled=True, state_dir=None`.

**Suggested improvement:**
Add a `__post_init__` to `SemanticIncrementalConfig` that raises `ValueError` when `enabled=True` and `state_dir is None`. Add a non-negative check on `start_version` in `_validate_cdf_options`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The subsystem uses msgspec structs and frozen dataclasses to parse configuration into well-typed values at boundaries.

**Findings:**
- `CdfCursor` at `cdf_cursors.py:23-93` uses msgspec `Struct` with `frozen=False` (but manually implements `__setattr__` to enforce immutability). The `loads_json` call at line 174 performs parse-on-read.
- `CdfChangeType.from_cdf_column` at `cdf_types.py:33-53` correctly parses string values into typed enum variants at the boundary.
- `plan_fingerprints.py:96-109` (`read_plan_snapshots`) does row-by-row dict access with `row.get("task_name")` and `row.get("plan_fingerprint")` after `to_pylist()`, which is validate-then-use rather than parse-into-struct. This could use a structured deserialization step.

**Suggested improvement:**
For `read_plan_snapshots`, parse rows into `PlanFingerprintSnapshot` instances using a structured deserialization path (e.g., msgspec decode of the row dict) rather than manual field extraction.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
The type system prevents some illegal states but leaves others representable.

**Findings:**
- `SemanticIncrementalConfig` at `config.py:36-102` allows `enabled=True` with `state_dir=None`, which is an illegal configuration that would fail at runtime when `cursor_store_path` returns `None`.
- `CdfReadOptions` at `cdf_reader.py:68-117` allows `cursor_store` to be set without `dataset_name` (caught only at runtime by `_validate_cdf_options`). These could be a single optional `CursorTracking(store, name)` type that prevents the partial combination.
- `_CdfReadRecord` at `cdf_runtime.py:66-74` has `inputs`, `state`, `table`, and `error` all as `| None`, but certain status values require specific non-None combinations (e.g., `status="read"` requires non-None `table`). This is a classic "stringly typed state machine" pattern.

**Suggested improvement:**
Replace `_CdfReadRecord` with a discriminated union (e.g., `CdfReadUnavailable | CdfReadNoChanges | CdfReadSuccess | CdfReadError`), each carrying only its relevant fields. For `CdfReadOptions`, consider a `CursorConfig(store, name)` combined type.

**Effort:** small (for CursorConfig), medium (for discriminated CdfReadRecord)
**Risk if unaddressed:** medium -- invalid states lead to subtle bugs and defensive null-checking code.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. A few blend query and command.

**Findings:**
- `CdfCursorStore.update_version()` at `cdf_cursors.py:234-267` both saves the cursor (command) AND returns the cursor (query). This is a common convenience pattern but technically violates CQS.
- `_prepare_cdf_read_state()` at `cdf_runtime.py:201-229` performs a side-effect (saves a new cursor at line 212 via `cursor_store.save_cursor(...)`) while also being structured as a query that returns `_CdfReadState | None`. The cursor save happens on the "first time" path, which is surprising for a function named `_prepare_*`.

**Suggested improvement:**
For `_prepare_cdf_read_state`, extract the cursor initialization side-effect into the caller (`read_cdf_changes`), keeping `_prepare_cdf_read_state` as a pure query that only computes state without saving.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 1/3

**Current state:**
Dependency injection is partial and inconsistent across the subsystem.

**Findings:**
- `cdf_reader.py:68-117` (`CdfReadOptions`) demonstrates good DI via callable injection with `delta_table_version_fn`, `delta_cdf_enabled_fn`, `read_delta_cdf_fn`, and `arrow_table_to_dataframe_fn`. Tests use these seams effectively.
- `cdf_runtime.py` has **no injection seams**. It directly imports and calls `DeltaTable`, `CdfCursorStore`, `DeltaAccessContext`, and `execute_df_to_table`. Testing `read_cdf_changes` at `cdf_runtime.py:329` requires a live `DeltaAccessContext` with a real runtime.
- `IncrementalRuntime.registry_facade` at `runtime.py:82-118` internally constructs `DatasetCatalog`, `ProviderRegistry`, and `RegistryFacade` with lazy caching. This hidden construction prevents injection and makes the runtime difficult to test in isolation.
- `schemas.py:26` (`incremental_dataset_specs`) creates a `DataFusionRuntimeProfile()` internally to get a session context, which is a hidden global construction.

**Suggested improvement:**
Accept an optional `RegistryFacade` in `IncrementalRuntime.build()` or `IncrementalRuntimeBuildRequest`. For `cdf_runtime.py`, adopt the same injectable-callable pattern used in `cdf_reader.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- testability and composability are limited for the runtime pathway.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
The subsystem uses no inheritance hierarchies. All behavior is composed through dataclass fields, function parameters, and module-level helpers.

**Findings:**
- No classes inherit from others (except base `msgspec.Struct` and `StructBaseStrict` for serialization).
- Behavior is assembled by passing `IncrementalRuntime`, `DeltaAccessContext`, and `CdfCursorStore` as parameters.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Deep chain access to the `DataFusionRuntimeProfile` internals is pervasive.

**Findings:**
- `cdf_runtime.py:108`: `runtime.profile.policies.scan_policy` (3 levels deep)
- `cdf_runtime.py:111`: `runtime.profile.delta_ops.delta_service().table_version(...)` (4 levels deep)
- `delta_context.py:81`: `self.runtime.profile.policies.delta_store_policy` (4 levels deep)
- `metadata.py:59`: `runtime.profile.policies.config_policy_name` (3 levels deep)
- `metadata.py:63`: `runtime.profile.settings_hash()` (2 levels, acceptable)
- `metadata.py:320`: `context.runtime.profile.diagnostics.diagnostics_sink` (4 levels deep)
- `plan_fingerprints.py:88`: `context.runtime.profile.delta_ops.delta_service().table_version(...)` (5 levels deep)

**Suggested improvement:**
Add facade methods on `IncrementalRuntime` that encapsulate the most-used chains. For example: `runtime.delta_service()`, `runtime.scan_policy()`, `runtime.delta_store_policy()`, `runtime.diagnostics_sink()`. This reduces the blast radius when `DataFusionRuntimeProfile` internal structure changes.

**Effort:** medium
**Risk if unaddressed:** medium -- any restructuring of `DataFusionRuntimeProfile` cascades into 8+ call sites across the subsystem.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Domain objects generally encapsulate behavior. Some raw-data patterns exist.

**Findings:**
- `CdfFilterPolicy` at `cdf_types.py:67-211` is a good example of "tell": callers say `policy.to_datafusion_predicate()` or `policy.matches(change_type)` rather than inspecting the three boolean fields directly.
- `CDFJoinSpec.effective_filter_policy()` at `cdf_joins.py:123-137` encapsulates the default-policy logic rather than exposing the nullable field.
- Counter-example: `metadata.py:320-325` asks `context.runtime.profile.diagnostics.diagnostics_sink` then checks `if sink is None`, then asks `sink.artifacts_snapshot().get(name)`. This is ask-then-ask-then-act.

**Suggested improvement:**
Introduce a method on the runtime or diagnostics object like `runtime.artifacts_for(name)` that returns the table or `None`, encapsulating the null-sink and missing-artifact checks.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 1/3

**Current state:**
There is no clear separation between pure domain transforms and IO/side-effect-producing operations.

**Findings:**
- `cdf_runtime.py:329-444` (`read_cdf_changes`) interleaves: profile access, input resolution (which checks `path.exists()` at line 91), cursor load/save, dataset registration in DataFusion, plan execution, telemetry recording, and error handling -- all in one function with no pure core.
- `delta_context.py:125-186` (`register_delta_df`) mixes storage resolution (pure-ish), DataFusion registration (IO), and error handling.
- In contrast, `cdf_types.py` is entirely pure (filter policy construction, predicate generation, fingerprinting). `cdf_joins.py` is mostly pure (join building, merge strategy application).

**Suggested improvement:**
Extract the pure decision-making parts of `read_cdf_changes` (input resolution, cursor state computation, CDF options construction) into testable pure functions. Keep the IO (registration, execution, telemetry) in a thin orchestrator.

**Effort:** large
**Risk if unaddressed:** medium -- difficult to unit test the decision logic without setting up full IO infrastructure.

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Most operations are idempotent or designed for safe re-execution.

**Findings:**
- `CdfCursorStore.save_cursor()` at `cdf_cursors.py:139-152` overwrites the cursor file, making repeated saves idempotent.
- `StateStore.ensure_dirs()` at `state_store.py:15-19` uses `mkdir(parents=True, exist_ok=True)` -- idempotent.
- `write_delta_table_via_pipeline` at `write_helpers.py:45-108` supports `WriteMode.OVERWRITE`, making repeated writes idempotent.
- Potential issue: `_prepare_cdf_read_state` at `cdf_runtime.py:211-212` saves a brand-new cursor on first encounter. If the subsequent CDF read fails after this save, re-running would skip the version range because the cursor was already advanced. However, the cursor is also saved at `cdf_runtime.py:428-430` after successful read, which would overwrite, so the risk is bounded.

**Suggested improvement:**
Move the initial cursor save in `_prepare_cdf_read_state` (line 212) to after the successful read, alongside the existing save at line 428. This ensures cursors are only advanced on success.

**Effort:** small
**Risk if unaddressed:** low (bounded by the overwrite-on-success pattern)

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Plan fingerprints and runtime snapshots support reproducibility tracking.

**Findings:**
- `PlanFingerprintSnapshot` at `plan_fingerprints.py:42-58` captures both `plan_fingerprint` (SHA256 of plan) and `plan_task_signature` (runtime-aware) for change detection.
- `metadata.py:62-65` records `datafusion_settings_hash` and `runtime_profile_hash` for reproducibility.
- `uuid7_hex()` at `runtime.py:168` and `write_helpers.py:70` introduces controlled non-determinism (monotonic UUIDs for temp table names), which is acceptable for transient resources.
- `CdfCursorStore.update_version()` at `cdf_cursors.py:260` generates a timestamp via `datetime.now(tz=UTC).isoformat()`, introducing wall-clock non-determinism. This is acceptable for audit metadata.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. Some unnecessary complexity exists in the fallback chains.

**Findings:**
- `cdf_runtime.py:147-198` (`_read_cdf_table_fallback`) has a complex type normalization path: cast `_change_type` to string, catch `ArrowInvalid`/`ArrowTypeError`/`NotImplementedError`, fall back to Python list comprehension, then build a boolean mask. This 50-line function handles three layers of type coercion defensively.
- `cdf_runtime.py:299-326` (`_load_cdf_table`) has a three-layer try/fallback: try DataFusion read, check for `_change_type` column, fallback to direct `load_cdf()`, catch `RuntimeError|ValueError`, check for specific error message, fallback again.
- The `_raise_non_cdf_start_error` function at `cdf_runtime.py:295-296` is a single-line function `raise exc` that exists solely to satisfy a typing/coverage pattern. This adds indirection without value.

**Suggested improvement:**
Consolidate the fallback logic in `_load_cdf_table` to use a single try/except that captures any provider error and falls back to `_read_cdf_table_fallback`. Remove `_raise_non_cdf_start_error` and inline the raise.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The subsystem is focused on its current use cases. Mild over-design in testing hooks.

**Findings:**
- `CdfReadOptions` at `cdf_reader.py:68-117` has four injectable function fields (`delta_table_version_fn`, `delta_cdf_enabled_fn`, `read_delta_cdf_fn`, `arrow_table_to_dataframe_fn`) explicitly labeled "for testing." While this enables good tests, the pattern could be simplified to a single `DeltaCdfAdapter` or resolved via DI rather than 4 optional callables on a data class.
- `CDFMergeStrategy.DELETE_INSERT` at `cdf_joins.py:364-367` has identical implementation to `UPSERT` (anti-join + union). The comment says "explicit delete-then-insert semantics" but the code is literally the same. If the semantics are truly the same, this is speculative.

**Suggested improvement:**
No immediate action required. If `DELETE_INSERT` never acquires distinct behavior, consider removing it in a future cleanup pass. The testing callables are valuable despite mild over-engineering.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
The duplicate types and functions create significant confusion.

**Findings:**
- **Two `CdfReadResult` classes** with different fields: `cdf_reader.py:40` has `(df, start_version, end_version, has_changes)` while `cdf_runtime.py:36` has `(table, updated_version)`. A reader encountering `CdfReadResult` cannot know which one they have without checking the import path.
- **Two `read_cdf_changes` functions** with different signatures: `cdf_reader.py:253` takes `(ctx, table_path, options)` while `cdf_runtime.py:329` takes `(context, *, dataset_path, dataset_name, cursor_store, filter_policy)`. The `__init__.py` re-exports from `cdf_reader.py`, but `cdf_runtime.py` is used by the pipeline.
- `_prepare_cdf_read_state` at `cdf_runtime.py:201-229` silently saves a cursor on first encounter (line 212) while returning `None` to indicate "no changes." The caller sees "no changes" but a state mutation occurred. This is surprising.

**Suggested improvement:**
Designate one `CdfReadResult` and one `read_cdf_changes` as canonical. If both pathways are needed, rename one clearly (e.g., `RuntimeCdfReadResult` or `read_cdf_changes_via_runtime`) to distinguish them. Fix the `__init__.py` to re-export only the canonical version.

**Effort:** medium
**Risk if unaddressed:** medium -- developers importing from the package-level `__init__.py` get one type while the pipeline uses another.

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
`__all__` is consistently declared in all modules. The `__init__.py` curates a public surface.

**Findings:**
- Every module declares `__all__` at the bottom, which is good.
- `__init__.py` at lines 33-48 declares a clear public API surface.
- However, the public surface exports `CdfReadResult` and `read_cdf_changes` from `cdf_reader.py` while the pipeline actually uses the versions from `cdf_runtime.py`, creating an inconsistency between the declared public API and actual usage.
- `PLAN_FINGERPRINTS_VERSION = 5` at `plan_fingerprints.py:30` is a module-level constant that serves as a version marker but is not exported or documented as a contract version.

**Suggested improvement:**
Align the `__init__.py` exports with actual usage. Document `PLAN_FINGERPRINTS_VERSION` as part of the plan fingerprint contract.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The lower layers (cursors, types, config, joins) are well-testable. The upper layers (runtime, cdf_runtime, metadata) are difficult to test in isolation.

**Findings:**
- `test_cdf_cursors.py` (297 lines, 19 tests) comprehensively tests `CdfCursor` and `CdfCursorStore` using only `tmp_path` -- excellent unit testability.
- `test_cdf_reader.py` (145 lines) tests `read_cdf_changes` via the injectable callables in `CdfReadOptions` -- good dependency injection for testing.
- `test_merge_strategies.py` (393 lines) tests all merge strategies using only `SessionContext` and in-memory data -- good.
- `test_config.py` (219 lines) thoroughly tests configuration with no external dependencies.
- **Missing test coverage:** No tests exist for `cdf_runtime.py`, `metadata.py`, `delta_context.py`, `plan_fingerprints.py`, `plan_bundle_exec.py`, `write_helpers.py`, `export_builders.py`, or `schemas.py`. These modules require `IncrementalRuntime`, `DeltaAccessContext`, or live Delta tables, making them difficult to unit test.
- `schemas.py:26` (`incremental_dataset_specs`) constructs a `DataFusionRuntimeProfile()` internally, making it impossible to test without the full DataFusion stack.

**Suggested improvement:**
Apply the same injectable-callback or protocol pattern from `cdf_reader.py` to `cdf_runtime.py` and `metadata.py`. This would enable unit testing of the CDF read orchestration and metadata persistence logic without requiring live Delta infrastructure.

**Effort:** small (for adding injection seams), medium (for writing the tests)
**Risk if unaddressed:** low (the untested modules are integration-level code, but the lack of unit tests increases regression risk)

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Telemetry is recorded via `record_artifact` for CDF reads and maintenance decisions. No structured logging or OpenTelemetry spans exist.

**Findings:**
- `cdf_runtime.py:447-479` (`_record_cdf_read`) records a structured artifact payload including event time, dataset, status, version range, row counts, change counts, filter policy, and errors. This is good structured telemetry.
- `delta_context.py:239-249` records maintenance decision artifacts.
- **No logging:** None of the 18 modules use Python `logging`. Failures in `CdfCursorStore.load_cursor()` at `cdf_cursors.py:173-176` silently return `None` on `DecodeError` or `OSError` -- a corrupted cursor file would be invisible.
- **No tracing spans:** The incremental pipeline has no OpenTelemetry spans, unlike the main semantic pipeline which uses `SCOPE_SEMANTICS` spans. Long-running CDF reads and Delta writes lack timing telemetry.

**Suggested improvement:**
Add `logger.warning()` calls for cursor decode failures and CDF fallback paths. Consider adding OpenTelemetry spans to `read_cdf_changes` and `write_delta_table_via_pipeline` for operational visibility.

**Effort:** small
**Risk if unaddressed:** low (artifact recording provides some visibility, but silent failures in cursors are a debugging hazard)

---

## Cross-Cutting Themes

### Theme 1: Dual-Pathway CDF Read Architecture

**Root cause:** Two independently developed CDF read implementations exist -- `cdf_reader.py` (session-context-oriented, with DI) and `cdf_runtime.py` (runtime-oriented, with no DI). They duplicate types, logic, and naming, creating confusion about which is canonical.

**Affected principles:** P7 (DRY), P21 (Least astonishment), P22 (Public contracts), P23 (Testability)

**Suggested approach:** Choose one as canonical. If both pathways serve genuinely different callers, give them distinct names and have one delegate to the other for shared logic (version resolution, CDF read, cursor management).

### Theme 2: Deep Profile Chain Access

**Root cause:** `IncrementalRuntime` exposes `profile: DataFusionRuntimeProfile` as a public field, and all operational modules reach deeply into its nested structure for policies, services, diagnostics, and settings.

**Affected principles:** P1 (Information hiding), P4 (Coupling), P14 (Law of Demeter)

**Suggested approach:** Add focused accessor methods to `IncrementalRuntime` (e.g., `delta_service()`, `scan_policy()`, `diagnostics_sink()`) that encapsulate the most-used chains. This creates a stable facade that absorbs future profile restructuring.

### Theme 3: Missing Storage Port Abstraction

**Root cause:** Delta Lake operations (table version checks, CDF reads, table writes, maintenance) are called directly from domain modules without an intermediate abstraction.

**Affected principles:** P5 (Dependency direction), P6 (Ports & Adapters), P16 (Functional core), P23 (Testability)

**Suggested approach:** Define a `DeltaStoragePort` protocol or abstract base with the operations the subsystem needs. The current `DeltaService` from `datafusion_engine` partially serves this role, but the direct `deltalake.DeltaTable` calls bypass it. Consolidate all Delta access through the service.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate duplicate `CdfReadResult` classes into one canonical type | small | High -- eliminates the most confusing name collision |
| 2 | P21 (Least astonishment) | Rename or remove the non-canonical `read_cdf_changes` / align `__init__.py` exports | small | High -- clears import confusion |
| 3 | P14 (Law of Demeter) | Add `delta_service()`, `scan_policy()`, `diagnostics_sink()` accessors on `IncrementalRuntime` | small | Medium -- reduces 8+ deep chain accesses |
| 4 | P10 (Illegal states) | Add `__post_init__` to `SemanticIncrementalConfig` rejecting `enabled=True, state_dir=None` | small | Medium -- prevents invalid configuration |
| 5 | P24 (Observability) | Add `logger.warning()` for cursor decode failures and CDF fallback paths | small | Medium -- makes silent failures visible |

## Recommended Action Sequence

1. **Consolidate duplicate types (P7, P21).** Designate `cdf_runtime.py`'s `CdfReadResult` as canonical (it is the one used by the pipeline). Remove or rename `cdf_reader.py`'s version. Update `__init__.py` to export from the correct source. This unblocks all further work by establishing a single source of truth.

2. **Add `IncrementalRuntime` facade methods (P14, P1).** Add `delta_service()`, `scan_policy()`, `delta_store_policy()`, `diagnostics_sink()`, and `settings_hash()` to `IncrementalRuntime`. Update all call sites to use the facade instead of deep chains. This reduces coupling and sets up a stable internal API.

3. **Enforce `SemanticIncrementalConfig` invariant (P10, P8).** Add a `__post_init__` check that rejects `enabled=True` with `state_dir=None`. This prevents a class of runtime errors at construction time.

4. **Add observability to silent failure paths (P24).** Add structured logging for cursor decode errors (`cdf_cursors.py:173-176`), CDF read fallback transitions (`cdf_runtime.py:314-325`), and dataset registration failures.

5. **Extract shared write helper for metadata.py (P3, P7).** Refactor the three near-identical `_write_*` functions in `metadata.py` into a single parameterized helper, reducing 80+ lines of duplicated Delta write orchestration.

6. **Move CdfCursorStore type to a shared location (P5).** Relocate the type (or define a protocol) so that `datafusion_engine/session/runtime.py` no longer imports from `semantics.incremental`, fixing the dependency direction violation.

7. **Separate concerns in cdf_runtime.py (P2, P3, P16).** Extract input resolution, dataset registration, and telemetry recording into focused submodules. Keep `read_cdf_changes` as a thin orchestrator that composes these. This is the largest change and benefits from items 1-4 being done first.

8. **Introduce DeltaCdfPort protocol (P6, P12).** Define a port with `table_version()`, `cdf_enabled()`, `read_cdf()` methods. Adapt both `cdf_reader.py` (already partially there) and `cdf_runtime.py` to depend on this protocol. This is a longer-term improvement that enables full unit testability of the CDF read pathway.

<parameter name="file_path">/Users/paulheyse/CodeAnatomy/docs/reviews/semantics-incremental.md