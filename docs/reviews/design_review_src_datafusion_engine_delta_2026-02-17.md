# Design Review: DataFusion Engine Delta + IO + Cache + Pruning

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/delta/`, `src/datafusion_engine/io/`, `src/datafusion_engine/cache/`, `src/datafusion_engine/pruning/`
**Focus:** All principles (1-24), with emphasis on Correctness (16-18) and Boundaries (1-6)
**Depth:** deep
**Files reviewed:** 41 (25 delta, 10 io, 4 cache, 2 pruning)

## Executive Summary

The Delta + IO subsystem demonstrates strong architectural intent -- particularly in its Ports & Adapters boundary via `DeltaServicePort`/`DeltaFeatureOpsPort` protocols and the clean separation between `storage.deltalake` (technology adapter) and `datafusion_engine.delta` (engine-level orchestration). However, significant boundary violations undermine this intent: private functions are exported as public API across module boundaries, a God class (`WritePipeline` at ~1456 LOC) concentrates too many responsibilities, and several DRY violations duplicate knowledge across the write path. The correctness profile is generally solid -- idempotent writes are supported via `CommitProperties`, retry logic is well-structured with configurable policies, and graceful degradation with fallback paths is consistently implemented. The highest-priority improvements are: (1) eliminate cross-module private function exports in `delta_runtime_ops.py`, (2) decompose `WritePipeline` into focused collaborators, and (3) consolidate duplicated helper functions across the write modules.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | `delta_runtime_ops.py` exports 26 private functions in `__all__`; `service_protocol.py:250` exposes private method in Protocol |
| 2 | Separation of concerns | 1 | large | medium | `WritePipeline` (~1456 LOC) mixes write orchestration, Delta bootstrap, maintenance, diagnostics |
| 3 | SRP | 1 | large | medium | `observability.py` (~1122 LOC) handles schema management, tracing init, table bootstrap, row appending, drift detection |
| 4 | High cohesion, low coupling | 2 | medium | medium | `write_core.py` imports private functions from `write_delta.py`; `service.py` couples directly to `storage.deltalake` |
| 5 | Dependency direction | 2 | medium | medium | `storage.deltalake.delta_read` imports from `datafusion_engine.delta.control_plane_core` (inner ring depends on engine ring) |
| 6 | Ports & Adapters | 2 | small | low | Protocol surface exists but `DeltaServicePort` includes private method `_provider_artifact_payload` |
| 7 | DRY | 1 | small | medium | 4 duplicated functions across write modules: `sql_string_literal`, `_delta_schema_policy_override`, `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS`, `_is_delta_observability_operation` |
| 8 | Design by contract | 2 | small | low | Request structs enforce preconditions; `_validate_update_constraints` is a no-op stub |
| 9 | Parse, don't validate | 2 | small | low | `coerce_delta_input()` normalizes at boundary; some raw dict access in report parsing |
| 10 | Make illegal states unrepresentable | 2 | small | low | `DeltaReadRequest` allows both `version` and `timestamp` (runtime check, not type-level) |
| 11 | CQS | 1 | medium | low | `run_delta_maintenance` both executes operations AND records observability; `_write_delta` method does write + feature enable + constraint + mutation recording |
| 12 | DI + explicit composition | 2 | medium | low | WritePipeline takes `ctx` and `runtime_profile` as constructor args; some hidden internal creation |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition via frozen dataclasses |
| 14 | Law of Demeter | 1 | medium | medium | `provider_artifacts.py` reaches deep: `profile.delta_ops.delta_service().table_version(...)` |
| 15 | Tell, don't ask | 2 | small | low | Feature mutation functions ask for `delta_table_version()` before acting; `delta_report_file_count` inspects 7 key variants |
| 16 | Functional core, imperative shell | 2 | medium | low | Pure functions exist (`compute_adaptive_file_size`, `delta_protocol_compatibility`); WritePipeline mixes both |
| 17 | Idempotency | 2 | small | medium | `IdempotentWriteOptions` + `CommitProperties` enable idempotent writes; not enforced by default |
| 18 | Determinism / reproducibility | 2 | small | low | Policy fingerprinting via `config_fingerprint()`; `scan_identity_hash` provides deterministic scan IDs |
| 19 | KISS | 2 | small | low | Lazy import facades add indirection but serve real startup-time goals |
| 20 | YAGNI | 2 | small | low | `_validate_update_constraints` is a no-op stub; `transactions.py` is pure pass-through |
| 21 | Least astonishment | 2 | small | low | `delta_commit_metadata()` always returns `None` (line 449); `DeltaWriteResult.version` can be `None` |
| 22 | Declare/version public contracts | 2 | small | low | `__all__` consistently declared; version suffixes on table names |
| 23 | Design for testability | 2 | small | low | Frozen dataclasses, protocol types, configurable policies; WritePipeline is hard to test in isolation |
| 24 | Observability | 2 | small | low | Consistent `stage_span` instrumentation; structured attributes; artifact recording |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information Hiding -- Alignment: 1/3

**Current state:**
The scope exhibits a significant information-hiding breach where internal implementation functions are exported as public API across module boundaries. `storage/deltalake/delta_runtime_ops.py` contains 26 private-named functions (prefixed with `_`) in its `__all__` list (lines 866-898), and these private functions are imported and used by sibling modules (`delta_write.py`, `delta_feature_mutations.py`, `delta_maintenance.py`) as if they were public API. The `service_protocol.py` Protocol contract includes `_provider_artifact_payload` (line 250), a private method name in a public protocol interface.

**Findings:**
- `src/storage/deltalake/delta_runtime_ops.py:866-898` -- Exports 26 private functions in `__all__` (e.g., `_enforce_locking_provider`, `_delta_retry_classification`, `_execute_delta_merge`, `_record_mutation_artifact`). These are imported by `delta_write.py`, `delta_feature_mutations.py`, and `delta_maintenance.py` as cross-module dependencies.
- `src/datafusion_engine/delta/service_protocol.py:250` -- `DeltaServicePort` protocol includes `_provider_artifact_payload`, a private method name in a public interface contract. Callers implementing this protocol must define a private-looking method.
- `src/datafusion_engine/io/write_core.py:574-583` -- Imports private functions from `write_delta.py` at module level: `_delta_write_policy_from_spec`, `_delta_schema_policy_override`, etc.
- `src/datafusion_engine/delta/control_plane_core.py:674-722` -- ~50 lines of module-level re-exports aliasing internal functions for external consumption.

**Suggested improvement:**
Promote the 26 private functions in `delta_runtime_ops.py` to public names (dropping the `_` prefix) since they are genuinely part of the module's cross-boundary contract. Alternatively, restructure so each sibling module (`delta_write.py`, `delta_feature_mutations.py`) contains its own implementation logic rather than importing shared helpers from a single "runtime_ops" module. For `service_protocol.py`, rename `_provider_artifact_payload` to `provider_artifact_payload` in the protocol definition.

**Effort:** medium
**Risk if unaddressed:** high -- Private function exports create a fragile cross-module contract where renaming any `_`-prefixed function breaks downstream modules silently.

---

#### P2. Separation of Concerns -- Alignment: 1/3

**Current state:**
The `WritePipeline` class in `write_pipeline.py` (~1456 LOC) is the most significant separation-of-concerns violation. It handles: (a) write orchestration (COPY, INSERT, streaming paths), (b) Delta-specific bootstrap and table creation, (c) maintenance scheduling and execution, (d) diagnostics collection and artifact recording, (e) format validation, (f) partition and file-size policy application. Similarly, `observability.py` (~1122 LOC) mixes schema management for 5+ observability tables, Delta tracing initialization, table bootstrap logic, row-level append operations, schema drift detection, and corrupt log recovery.

**Findings:**
- `src/datafusion_engine/io/write_pipeline.py` -- `WritePipeline` class handles write routing, Delta bootstrap, maintenance triggers, diagnostics emission, and format-specific logic in a single class. The `_write_delta` method alone orchestrates: write execution, feature enablement, constraint application, mutation recording, and maintenance scheduling.
- `src/datafusion_engine/delta/observability.py` -- ~1122 LOC file mixing: observability table schema definitions, tracing initialization, bootstrap/ensure logic, row append operations, schema drift detection, and corrupt log recovery. Each of these is a distinct concern.
- `src/datafusion_engine/delta/control_plane_maintenance.py` -- ~690 LOC with 30+ enable/disable feature functions. While each function is simple, the module conflates feature-toggle operations with maintenance operations.

**Suggested improvement:**
Decompose `WritePipeline` into: (1) `WriteRouter` -- routes write requests to format-specific handlers, (2) `DeltaWriteHandler` -- Delta-specific write orchestration, (3) `WriteMaintenanceScheduler` -- post-write maintenance decisions, (4) `WriteDiagnosticsCollector` -- artifact recording. For `observability.py`, extract schema definitions into a `delta_observability_schema.py` module and bootstrap/append logic into a `delta_observability_writer.py` module.

**Effort:** large
**Risk if unaddressed:** medium -- The God class pattern makes it difficult to test individual write-path behaviors in isolation and increases the blast radius of any change.

---

#### P3. SRP (One Reason to Change) -- Alignment: 1/3

**Current state:**
Several modules have multiple distinct reasons to change. `observability.py` changes when: observability table schemas evolve, tracing initialization requirements change, bootstrap logic changes, or schema drift detection logic changes. `write_pipeline.py` changes when: write orchestration logic changes, Delta bootstrap logic changes, maintenance scheduling changes, or diagnostics requirements change.

**Findings:**
- `src/datafusion_engine/delta/observability.py` -- 6+ distinct responsibilities: (1) snapshot schema, (2) mutation schema, (3) scan plan schema, (4) maintenance schema, (5) bootstrap/ensure table logic, (6) row append operations. Each is a separate reason to change.
- `src/datafusion_engine/io/write_pipeline.py` -- 4+ distinct responsibilities: (1) write routing, (2) Delta-specific write handling, (3) maintenance scheduling, (4) diagnostics/artifact recording.
- `src/datafusion_engine/delta/control_plane_core.py` -- ~789 LOC mixing request struct definitions (15+ structs) with helper functions and module-level re-exports. The structs change for schema reasons; the helpers change for runtime reasons.

**Suggested improvement:**
Extract request structs from `control_plane_core.py` into a dedicated `control_plane_types.py` module. Extract observability schemas into a `delta_observability_schema.py` module. These are low-risk extractions that reduce change coupling.

**Effort:** large
**Risk if unaddressed:** medium

---

#### P4. High Cohesion, Low Coupling -- Alignment: 2/3

**Current state:**
Most modules demonstrate good internal cohesion -- `protocol.py`, `specs.py`, `store_policy.py`, `schema_guard.py` each focus on a single concept. However, coupling between the IO write modules is higher than necessary: `write_core.py` imports private functions from `write_delta.py`, creating a hidden bidirectional dependency between the "core" and "delta-specific" layers.

**Findings:**
- `src/datafusion_engine/io/write_core.py:574-583` -- Imports `_delta_write_policy_from_spec`, `_delta_schema_policy_override`, `_replace_where_predicate`, `_apply_delta_features_from_spec` from `write_delta.py` at module level. This couples "core" write types to Delta-specific implementation details.
- `src/datafusion_engine/delta/service.py` -- Imports directly from `storage.deltalake.delta_read` and `storage.deltalake.delta_write`, coupling the engine-ring `DeltaService` to concrete storage-ring implementations rather than abstractions.
- `src/datafusion_engine/cache/metadata_snapshots.py:108` -- `suppress(Exception)` silently swallows SQL errors, coupling cache snapshot logic to undocumented DataFusion failure modes.

**Suggested improvement:**
Move Delta-specific write helpers currently imported by `write_core.py` into `write_core.py` itself (they are part of its effective interface) or create an explicit `write_delta_policy.py` module that both can import. For `service.py`, introduce a narrow protocol for the storage operations it actually needs.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency Direction -- Alignment: 2/3

**Current state:**
The dependency direction generally flows correctly: `storage.deltalake` (outer/adapter ring) depends on `datafusion_engine.delta` (engine ring) for control-plane operations. However, there are reverse dependencies: `storage.deltalake.delta_read.py` imports from `datafusion_engine.delta.control_plane_core` (lines 35-39, 142-145) and `datafusion_engine.delta.protocol` (line 16). The `storage.deltalake.delta_runtime_ops.py` imports extensively from `datafusion_engine.delta` modules. This creates a bidirectional dependency between what should be distinct layers.

**Findings:**
- `src/storage/deltalake/delta_read.py:35-39` -- TYPE_CHECKING imports from `datafusion_engine.delta.control_plane_core` (DeltaMergeRequest) and `datafusion_engine.delta.protocol` (DeltaFeatureGate). These are compile-time only, mitigating the runtime risk.
- `src/storage/deltalake/delta_runtime_ops.py:16` -- Runtime import of `DeltaFeatureGate` and `DeltaProtocolSnapshot` from `datafusion_engine.delta.protocol`. This is a genuine reverse dependency: the storage adapter depends on engine-ring types.
- `src/storage/deltalake/delta_read.py:677-686` -- Runtime imports from `storage.deltalake.delta_runtime_ops` (which itself imports from `datafusion_engine.delta`), creating a transitive reverse dependency chain.

**Suggested improvement:**
Move shared types (`DeltaFeatureGate`, `DeltaProtocolSnapshot`, `StorageOptions`) into a shared types module (e.g., `src/storage/deltalake/types.py`) that both rings can depend on without creating circular dependencies. Alternatively, define these as protocols in the storage layer and have the engine layer implement them.

**Effort:** medium
**Risk if unaddressed:** medium -- Bidirectional dependencies between layers make it harder to reason about change impact and can lead to import cycles as the codebase grows.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `DeltaServicePort` and `DeltaFeatureOpsPort` protocols in `service_protocol.py` establish a clean port boundary for Delta operations. The `ExternalIndexProvider` protocol in `storage/external_index/provider.py` is an exemplary port definition. However, the port contracts are undermined by including a private method name (`_provider_artifact_payload`) and by `DeltaService` directly coupling to concrete storage implementations.

**Findings:**
- `src/datafusion_engine/delta/service_protocol.py:250` -- `DeltaServicePort` protocol includes `_provider_artifact_payload` method. Protocol methods should be public by convention.
- `src/storage/external_index/provider.py:46-62` -- Clean `ExternalIndexProvider` protocol with `supports()` and `select_candidates()` methods. Good port design.
- `src/datafusion_engine/io/adapter.py` -- `DataFusionIOAdapter` provides a clean adapter for object store, table, view, and dataset registration. Well-designed.
- `src/datafusion_engine/delta/object_store.py` -- Clean adapter for S3/GCS/Azure/HTTP object store registration.

**Suggested improvement:**
Rename `_provider_artifact_payload` to `provider_artifact_payload` in the `DeltaServicePort` protocol to follow the convention that protocol methods are public API.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (Knowledge, Not Lines) -- Alignment: 1/3

**Current state:**
Four distinct knowledge duplications exist across the IO write modules, each representing the same business rule or invariant defined in multiple places.

**Findings:**
- `src/datafusion_engine/io/write_pipeline.py` and `src/datafusion_engine/io/write_execution.py` -- Both define `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` and `is_retryable_delta_stream_error` function. This is the same retry classification knowledge duplicated across two modules.
- `src/datafusion_engine/io/write_pipeline.py` and `src/datafusion_engine/io/write_formats.py` -- Both define `sql_string_literal()` helper for SQL string escaping. Same knowledge in two places.
- `src/datafusion_engine/io/write_core.py` and `src/datafusion_engine/io/write_delta.py` -- Both define `_delta_schema_policy_override()`. Same policy resolution logic duplicated.
- `src/datafusion_engine/delta/observability.py` -- Schema definitions as functions (`_delta_snapshot_schema()`, `_delta_mutation_schema()`, etc.) rather than module-level constants, meaning they are reconstructed on every call even though schemas are immutable.

**Suggested improvement:**
Consolidate `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` and `is_retryable_delta_stream_error` into a single location (e.g., `write_execution.py`) and import from there. Move `sql_string_literal` to `write_formats.py` as the single authority. Consolidate `_delta_schema_policy_override` into `write_delta.py` as the single authority. Convert observability schema functions to module-level constants since `pa.Schema` objects are immutable.

**Effort:** small
**Risk if unaddressed:** medium -- Duplicated knowledge means changes to retry markers, SQL escaping, or schema policy must be applied in multiple places, creating drift risk.

---

#### P8. Design by Contract -- Alignment: 2/3

**Current state:**
Request dataclasses effectively serve as precondition contracts -- `DeltaWriteRequest`, `DeltaDeleteRequest`, etc. encode required inputs as typed fields. Runtime validation exists for critical invariants (e.g., `delta_update` checks `request.updates` is non-empty at `control_plane_mutation.py:77`). However, some stubs remain.

**Findings:**
- `src/datafusion_engine/delta/control_plane_mutation.py:59-62` -- `_validate_update_constraints` is a no-op: `if not request.extra_constraints: return` followed by `_ = ctx`. The function signature promises constraint validation but delivers nothing.
- `src/datafusion_engine/delta/control_plane_mutation.py:77-79` -- Good: `delta_update` validates `request.updates` is non-empty before proceeding.
- `src/storage/deltalake/delta_read.py:357-359` -- Good: `_open_delta_table` validates mutual exclusivity of `version` and `timestamp` parameters.

**Suggested improvement:**
Either implement `_validate_update_constraints` or remove it and document why constraint validation is deferred to the Rust control plane.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, Don't Validate -- Alignment: 2/3

**Current state:**
`coerce_delta_input()` in `delta_read.py` is a good "parse, don't validate" implementation -- it converts raw `TableLike | RecordBatchReaderLike` inputs into a structured `DeltaInput` with schema and optional row count at the boundary. Similarly, `_record_from_row()` in `cache/registry.py` parses raw dict rows into structured `CacheInventoryRecord` objects. However, report payloads from Delta operations are passed around as `Mapping[str, object]` and parsed repeatedly with `.get()` calls.

**Findings:**
- `src/storage/deltalake/delta_read.py:604-649` -- `coerce_delta_input()` normalizes inputs at the boundary. Good parse-once pattern.
- `src/storage/deltalake/delta_runtime_ops.py:168-186` -- `_merge_rows_affected()` probes 6 different key names in the metrics dict. This repetitive probing indicates the report payload should be parsed into a structured type once.
- `src/datafusion_engine/cache/inventory.py:277-307` -- `delta_report_file_count()` probes 7 different key names in the report payload. Same smell.

**Suggested improvement:**
Define a `DeltaMutationReport` structured type that parses the raw `Mapping[str, object]` report from the control plane into typed fields once, then pass this structured type to downstream consumers. This eliminates repeated `.get()` probing across multiple functions.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make Illegal States Unrepresentable -- Alignment: 2/3

**Current state:**
`DeltaReadRequest` allows both `version` and `timestamp` to be set simultaneously, which is invalid. The invariant is checked at runtime (`delta_read.py:461-463`) rather than being prevented by the type system. This is a pragmatic choice given Python's limited sum-type support, but it means the invalid state exists in the type space.

**Findings:**
- `src/storage/deltalake/delta_read.py:262-274` -- `DeltaReadRequest` has both `version: int | None` and `timestamp: str | None`. Setting both is invalid but representable.
- `src/storage/deltalake/delta_read.py:194-204` -- `DeltaCdfOptions` has `starting_version`, `ending_version`, `starting_timestamp`, `ending_timestamp`. The valid combinations are not enforced by the type.
- `src/datafusion_engine/io/write_core.py` -- `WriteMode` enum cleanly enumerates `APPEND`, `OVERWRITE`, `ERROR_IF_EXISTS`, making invalid modes unrepresentable. Good.

**Suggested improvement:**
Consider a `DeltaTimeTravelSpec` union type: `VersionSpec(version: int) | TimestampSpec(timestamp: str) | LatestSpec()` that makes it impossible to specify both version and timestamp. This is a low-priority improvement given the runtime guard.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS (Command/Query Separation) -- Alignment: 1/3

**Current state:**
Several functions in the scope both modify state and return information, violating CQS. The most significant are the maintenance and write functions that execute operations AND record observability artifacts AND return report payloads.

**Findings:**
- `src/datafusion_engine/delta/maintenance.py` -- `run_delta_maintenance()` executes optimize/vacuum/checkpoint operations, records observability artifacts via `record_delta_mutation`, and returns a `DeltaMaintenancePlan` with execution results. This is a command (state mutation) that returns a query result.
- `src/storage/deltalake/delta_write.py:108-220` -- `delta_delete_where()` deletes rows (command), records a mutation artifact (side effect), AND returns a report payload (query). Three concerns in one function.
- `src/datafusion_engine/cache/ledger.py:136-195` -- `record_cache_run_summary()` writes a cache entry (command), registers a dataset (side effect), AND returns a version number (query).
- `src/datafusion_engine/cache/inventory.py:165-226` -- `record_cache_inventory_entry()` writes an inventory row AND returns the Delta version.

**Suggested improvement:**
For the maintenance and write functions, consider separating the operation execution from artifact recording. The execution function returns a result; a separate function records the artifact. This is lower priority because the current pattern is pragmatic for ensuring artifacts are always recorded alongside operations.

**Effort:** medium
**Risk if unaddressed:** low -- CQS violations here are pragmatic; the recording is tightly coupled to the operation for reliability reasons.

---

### Category: Composition (12-15)

#### P12. Dependency Inversion + Explicit Composition -- Alignment: 2/3

**Current state:**
`WritePipeline` receives `ctx` and `runtime_profile` via constructor injection, which is good. `DeltaService` is a frozen dataclass wrapping `DataFusionRuntimeProfile`, also using DI. However, some internal creation is hidden -- `WritePipeline` is created inline in cache modules (`inventory.py:200`, `ledger.py:170`, `ledger.py:265`, `metadata_snapshots.py:82`) rather than being injected.

**Findings:**
- `src/datafusion_engine/cache/inventory.py:200` -- `WritePipeline(ctx=active_ctx, runtime_profile=profile)` created inline. Same pattern at `ledger.py:170`, `ledger.py:265`, `ledger.py:313`, `metadata_snapshots.py:82`.
- `src/datafusion_engine/io/write_core.py:86-104` -- Complex lazy proxy metaclass `_WritePipelineLazyProxy` adds indirection for deferred import resolution.

**Suggested improvement:**
Consider injecting a `WritePipeline` (or a `WriterPort` protocol) into the cache modules rather than creating it inline. This would make the dependency explicit and improve testability of cache modules.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P13. Prefer Composition over Inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the scope. All types use frozen dataclasses and composition. `CacheInventoryRegistry` composes `MutableRegistry` rather than inheriting behavior. `DeltaService` wraps `DataFusionRuntimeProfile` via composition.

**Findings:**
No violations found. This principle is well-satisfied.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Several deep call chains reach through multiple levels of object nesting, violating the Law of Demeter.

**Findings:**
- `src/datafusion_engine/cache/registry.py:157` -- `profile.delta_ops.delta_service().table_version(...)` chains through 3 levels of object nesting. The cache registry reaches into the runtime profile's delta ops, gets the delta service, then calls a method.
- `src/datafusion_engine/cache/inventory.py:250` -- `profile.policies.plan_artifacts_root` accesses a nested policy attribute.
- `src/storage/deltalake/delta_maintenance.py:148` -- `runtime_profile.delta_ops.delta_runtime_ctx()` reaches through two levels.
- `src/storage/deltalake/delta_feature_mutations.py:52-53` -- `profile.delta_ops.delta_runtime_ctx()` repeated pattern.
- `src/datafusion_engine/cache/ledger.py:326` -- `profile.io_ops.cache_root()` is borderline (2 levels) but consistent throughout cache modules.

**Suggested improvement:**
Introduce convenience methods on `DataFusionRuntimeProfile` that wrap common deep access patterns. For example, `profile.delta_runtime_ctx()` instead of `profile.delta_ops.delta_runtime_ctx()`, and `profile.table_version(path, ...)` instead of `profile.delta_ops.delta_service().table_version(...)`.

**Effort:** medium
**Risk if unaddressed:** medium -- Deep chains couple callers to the internal structure of `DataFusionRuntimeProfile`, meaning any restructuring of the profile's internal organization breaks all callers.

---

#### P15. Tell, Don't Ask -- Alignment: 2/3

**Current state:**
Most code follows tell-don't-ask reasonably well -- request objects encapsulate inputs and functions operate on them. However, some functions probe raw data for information rather than having it provided in a structured form.

**Findings:**
- `src/datafusion_engine/cache/inventory.py:277-307` -- `delta_report_file_count()` inspects 7 different key variants (`numFiles`, `num_files`, `files`, `added_files`, `files_added`, `numAddFiles`, `numAddedFiles`) to extract a file count. The report should provide this in a canonical form.
- `src/storage/deltalake/delta_runtime_ops.py:168-186` -- `_merge_rows_affected()` probes 6 key variants to extract affected row counts.
- `src/storage/deltalake/delta_feature_mutations.py:38-46` -- `enable_delta_features()` checks `delta_table_version()` before acting, asking the table for its state rather than being told.

**Suggested improvement:**
Define a canonical `DeltaOperationReport` type that the control plane populates with standardized field names, eliminating the need for multi-key probing across consumers.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional Core, Imperative Shell -- Alignment: 2/3

**Current state:**
Pure functions exist at the core: `compute_adaptive_file_size()` in `write_planning.py`, `delta_protocol_compatibility()` in `protocol.py`, `canonical_table_uri()` in `delta_metadata.py`, `resolve_stats_columns()` in `config.py`. These are deterministic transforms with no IO. However, `WritePipeline` is a significant imperative blob that mixes pure decision-making with IO operations (file writes, Delta commits, table registration).

**Findings:**
- `src/datafusion_engine/io/write_planning.py` -- `compute_adaptive_file_size()` and `AdaptiveFileSizeDecision` are clean pure functions. Good functional core.
- `src/datafusion_engine/delta/protocol.py` -- `delta_protocol_compatibility()` is a pure function computing protocol compatibility. Good.
- `src/storage/deltalake/config.py:319-344` -- `resolve_stats_columns()` is a pure function resolving stats columns from policy. Good.
- `src/datafusion_engine/io/write_pipeline.py` -- `WritePipeline._write_delta` mixes pure decision-making (should we run maintenance? what mode?) with IO (Delta writes, artifact recording). The decisions could be extracted as pure functions.

**Suggested improvement:**
Extract write-mode decision logic from `WritePipeline._write_delta` into pure functions: `should_run_maintenance(metrics, thresholds) -> bool`, `resolve_write_mode(spec, table_exists) -> WriteMode`, etc. Keep the IO orchestration in the imperative shell.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
The system provides opt-in idempotency via `IdempotentWriteOptions` and `CommitProperties` with app transactions. When used, duplicate commits are safely ignored. Cache inventory writes use `WritePipeline` with `WriteMode.APPEND`, which is idempotent at the Delta level (append-only commits). However, idempotency is not enforced by default -- callers must explicitly opt in.

**Findings:**
- `src/storage/deltalake/delta_read.py:176-192` -- `IdempotentWriteOptions` with `app_id` and `version` fields enables safe retries.
- `src/storage/deltalake/delta_write.py:72-105` -- `idempotent_commit_properties()` builds `CommitProperties` with `Transaction` entries for commit deduplication.
- `src/storage/deltalake/delta_runtime_ops.py:189-213` -- `_execute_delta_merge()` retry loop re-attempts on retryable errors with exponential backoff. However, the retry does not use idempotent commit properties, so retried commits could create duplicate data if the original commit succeeded but the response was lost.
- `src/datafusion_engine/cache/inventory.py:165-226` -- `record_cache_inventory_entry()` uses `WriteMode.APPEND` without idempotency tokens. If called twice with the same entry, it will create duplicate rows.

**Suggested improvement:**
Consider adding `IdempotentWriteOptions` to the retry path in `_execute_delta_merge()` so that retried commits use app transactions for deduplication. For cache inventory writes, consider using a composite key (view_name + plan_identity_hash + event_time) as an app transaction ID to prevent duplicate entries.

**Effort:** small
**Risk if unaddressed:** medium -- Without idempotent retries, a successful-but-unacknowledged commit followed by a retry could create duplicate data.

---

#### P18. Determinism / Reproducibility -- Alignment: 2/3

**Current state:**
Policy fingerprinting is well-implemented: `DeltaWritePolicy.fingerprint()`, `DeltaSchemaPolicy.fingerprint()`, `FilePruningPolicy.fingerprint()` all use `config_fingerprint()` for deterministic hashes. `DeltaScanConfigIdentity` provides deterministic scan identification. However, `time.time()` calls in cache modules introduce non-determinism in event timestamps.

**Findings:**
- `src/storage/deltalake/config.py` -- All policy classes implement `fingerprint_payload()` and `fingerprint()` for deterministic identity. Good.
- `src/datafusion_engine/delta/scan_config.py` -- `delta_scan_identity_hash()` provides deterministic scan IDs. Good.
- `src/datafusion_engine/cache/inventory.py:68` -- `int(time.time() * 1000)` used as fallback for `event_time_unix_ms`. Non-deterministic.
- `src/datafusion_engine/cache/ledger.py:61,94` -- Same `time.time()` pattern for event timestamps.
- `src/datafusion_engine/cache/metadata_snapshots.py:59` -- Same pattern.

**Suggested improvement:**
Inject a clock function via the runtime profile or as a parameter to cache entry constructors, allowing tests to provide deterministic timestamps. The current `time.time()` calls are pragmatic for production but complicate reproducibility testing.

**Effort:** small
**Risk if unaddressed:** low -- The non-determinism is in metadata timestamps, not in computation results.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The lazy import facades (`delta/__init__.py`, `storage/deltalake/__init__.py`) add indirection but serve a real purpose: reducing import-time overhead for a module with 25+ submodules. The `_WritePipelineLazyProxy` metaclass in `write_core.py` is more complex than necessary.

**Findings:**
- `src/datafusion_engine/io/write_core.py:86-104` -- `_WritePipelineLazyProxy` metaclass with `__instancecheck__` and `__subclasscheck__` overrides. This is a complex pattern for deferred import resolution that could be simplified.
- `src/datafusion_engine/delta/__init__.py` -- Clean lazy import pattern using `__getattr__` + `_EXPORT_MAP`. Appropriate complexity.
- `src/storage/deltalake/__init__.py` -- Same pattern, but with 154 lines of export mappings. The size is appropriate given the breadth of the module.

**Suggested improvement:**
Replace `_WritePipelineLazyProxy` with a simpler lazy import pattern (e.g., a module-level function `get_write_pipeline_class()` or deferred import at call site).

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Two pieces of code exist without current utility: `_validate_update_constraints` is a no-op stub, and `transactions.py` provides pure pass-through wrappers that add no value over calling `control_plane_core` functions directly.

**Findings:**
- `src/datafusion_engine/delta/control_plane_mutation.py:59-62` -- `_validate_update_constraints` is a no-op stub. Speculative placeholder for future constraint validation.
- `src/datafusion_engine/delta/transactions.py` -- Pure pass-through wrappers around `control_plane_core` functions. No added value, no additional validation, no additional logic.
- `src/storage/deltalake/delta_read.py:558-567` -- `read_delta_table_eager()` is a one-liner calling `read_delta_table().read_all()`. This is a convenience function, not speculative generality, so it is acceptable.

**Suggested improvement:**
Remove `_validate_update_constraints` or document why it exists as a placeholder. Consider whether `transactions.py` adds architectural value as an abstraction layer or should be removed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least Astonishment -- Alignment: 2/3

**Current state:**
One function consistently returns `None` regardless of input, which is surprising: `delta_commit_metadata()` in `delta_runtime_ops.py:414-449` always returns `None` at line 449, even after successfully retrieving a snapshot. A reader would expect it to extract and return commit metadata from the snapshot.

**Findings:**
- `src/storage/deltalake/delta_runtime_ops.py:448-449` -- `delta_commit_metadata()` retrieves a snapshot successfully at line 439 but then returns `None` on line 449 without extracting any metadata. The function name and docstring promise "custom commit metadata for the latest Delta table version" but always returns None.
- `src/datafusion_engine/io/write_core.py` -- `DeltaWriteResult.version` can be `None` even after a successful write (see `_write_delta_bootstrap` which creates results with `version=None`). This is mildly surprising but documented.

**Suggested improvement:**
Either implement `delta_commit_metadata()` to actually extract commit metadata from the snapshot, or remove it and document why commit metadata retrieval is not yet supported.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and Version Public Contracts -- Alignment: 2/3

**Current state:**
`__all__` is consistently declared in every module, which is good. Table names use `_v1` suffixes for versioning (e.g., `CACHE_INVENTORY_TABLE_NAME = "datafusion_view_cache_inventory_v1"`). However, `delta_runtime_ops.py` exports private-named functions in `__all__`, blurring the public/private boundary.

**Findings:**
- All modules define `__all__` -- consistent and good.
- `src/datafusion_engine/cache/inventory.py:31` -- `CACHE_INVENTORY_TABLE_NAME = "datafusion_view_cache_inventory_v1"` includes version suffix. Good.
- `src/storage/deltalake/delta_runtime_ops.py:865-898` -- `__all__` includes 26 private-named functions. The `__all__` declaration contradicts the private naming convention.

**Suggested improvement:**
Either rename the 26 private functions to public names or remove them from `__all__` and have consumers import them explicitly (acknowledging they are internal).

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for Testability -- Alignment: 2/3

**Current state:**
Frozen dataclasses with explicit constructor parameters make most types easily testable. Protocol types (`DeltaServicePort`, `ExternalIndexProvider`) enable mock implementations. However, `WritePipeline` is difficult to test in isolation because it directly constructs DataFusion sessions and Delta tables internally.

**Findings:**
- `src/datafusion_engine/delta/service.py` -- `DeltaService` is a frozen dataclass, easily constructible in tests.
- `src/storage/deltalake/config.py` -- All policy classes are frozen msgspec structs with defaults, trivially constructible.
- `src/datafusion_engine/io/write_pipeline.py` -- `WritePipeline` requires a real `SessionContext` and `DataFusionRuntimeProfile` to construct, making unit testing require heavyweight setup.
- `src/datafusion_engine/cache/metadata_snapshots.py:108` -- `suppress(Exception)` makes failure paths invisible in tests.

**Suggested improvement:**
Extract the pure decision-making logic from `WritePipeline` into testable pure functions. The remaining IO orchestration can be tested via integration tests with real sessions.

**Effort:** small
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Observability instrumentation is comprehensive and consistent. All storage operations use `stage_span()` with structured attributes following a `codeanatomy.*` naming convention. Artifact recording via `record_delta_mutation()`, `record_delta_maintenance()` etc. provides audit trails. However, `observability.py` itself at ~1122 LOC is over-complex.

**Findings:**
- `src/storage/deltalake/delta_runtime_ops.py:280-300` -- `_storage_span_attributes()` provides a consistent attribute builder for all storage spans. Good.
- `src/storage/deltalake/file_pruning.py:308-335` -- `evaluate_and_select_files()` instruments pruning with span attributes for total files, selected files, pruned files. Good.
- `src/datafusion_engine/delta/observability.py` -- ~1122 LOC for observability infrastructure. The file itself is a maintainability concern despite serving a critical observability function.
- `src/datafusion_engine/cache/metadata_snapshots.py:108` -- `suppress(Exception)` silently swallows errors without any logging or tracing, creating an observability gap.

**Suggested improvement:**
Replace `suppress(Exception)` at `metadata_snapshots.py:108` with explicit error handling that at minimum logs the exception. Consider decomposing `observability.py` into schema and writer modules as noted under P3.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Private Functions as Public Cross-Module API

The most pervasive pattern across the scope is the use of `_`-prefixed functions as cross-module API. `delta_runtime_ops.py` exports 26 private functions that are consumed by `delta_write.py`, `delta_feature_mutations.py`, and `delta_maintenance.py`. Similarly, `write_core.py` imports private functions from `write_delta.py`. This pattern undermines information hiding (P1), violates public contract conventions (P22), and creates fragile coupling (P4).

**Root cause:** The decomposition of large files into smaller modules was done by extracting functions without promoting them to public API. The `_` prefix was preserved from their original context within a single module.

**Affected principles:** P1 (Information Hiding), P4 (Coupling), P22 (Public Contracts).

**Suggested approach:** Audit all `_`-prefixed exports and either: (a) promote to public names if they are genuinely part of the module's contract, or (b) restructure so each consumer module owns its implementation logic.

### Theme 2: Report Payload Fragility

Delta operation reports flow as `Mapping[str, object]` through multiple consumers, each probing for different key variants (e.g., `numFiles` vs `num_files` vs `files`). This creates a fragile contract between the Rust control plane and Python consumers.

**Root cause:** The Rust control plane returns duck-typed payloads without a formal schema. Python consumers must defensively probe for multiple key names across different delta-rs versions.

**Affected principles:** P9 (Parse, Don't Validate), P15 (Tell, Don't Ask), P7 (DRY).

**Suggested approach:** Define a `DeltaOperationReport` structured type that parses the raw report once at the Python-Rust boundary, mapping variant key names to canonical fields.

### Theme 3: God Objects in Write and Observability Paths

`WritePipeline` (~1456 LOC) and `observability.py` (~1122 LOC) concentrate too many responsibilities. Both files are noted in comments as temporarily oversized during decomposition, but the current state makes testing, reasoning about changes, and maintaining these modules difficult.

**Root cause:** Incremental growth and the challenge of decomposing stateful orchestration logic into smaller units without introducing excessive coordination overhead.

**Affected principles:** P2 (Separation of Concerns), P3 (SRP), P16 (Functional Core), P23 (Testability).

**Suggested approach:** Extract pure decision functions first (easiest wins), then extract format-specific handlers, then extract diagnostics recording.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate 4 duplicated functions across write modules (`sql_string_literal`, `_delta_schema_policy_override`, `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS`, `_is_delta_observability_operation`) | small | Eliminates drift risk in retry/escape/policy logic |
| 2 | P6 (Ports & Adapters) | Rename `_provider_artifact_payload` to `provider_artifact_payload` in `DeltaServicePort` protocol | small | Fixes protocol convention violation |
| 3 | P21 (Least Astonishment) | Fix or remove `delta_commit_metadata()` which always returns `None` | small | Eliminates dead/misleading code |
| 4 | P20 (YAGNI) | Remove no-op `_validate_update_constraints` stub and pure pass-through `transactions.py` | small | Reduces code surface without functional impact |
| 5 | P1 (Information Hiding) | Promote 26 private functions in `delta_runtime_ops.py __all__` to public names | small | Aligns naming with actual usage as cross-module API |

## Recommended Action Sequence

1. **Consolidate duplicated helpers (P7):** Move `sql_string_literal` to `write_formats.py`, `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` to `write_execution.py`, `_delta_schema_policy_override` to `write_delta.py`. Update all importers. This is zero-risk and eliminates drift.

2. **Fix protocol naming (P6):** Rename `_provider_artifact_payload` to `provider_artifact_payload` in `service_protocol.py` and all implementations. Small blast radius.

3. **Clean up dead code (P20, P21):** Remove `_validate_update_constraints` no-op, evaluate removing `transactions.py`, fix or remove `delta_commit_metadata()`.

4. **Promote private exports (P1, P22):** Rename the 26 private functions in `delta_runtime_ops.py` to public names. Update all importers across `delta_write.py`, `delta_feature_mutations.py`, `delta_maintenance.py`.

5. **Extract pure functions from WritePipeline (P2, P16, P23):** Identify pure decision functions (maintenance thresholds, write mode resolution, adaptive file sizing decisions) and extract them as standalone functions. This reduces WritePipeline size and improves testability without changing the orchestration architecture.

6. **Parse Delta reports at boundary (P9, P15):** Define `DeltaOperationReport` structured type. Parse raw reports at the Python-Rust boundary. Eliminate multi-key probing across consumers.

7. **Decompose observability.py (P3):** Extract schema definitions into `delta_observability_schema.py`. Extract writer logic into `delta_observability_writer.py`. Reduce `observability.py` to orchestration/dispatch.

8. **Add Demeter convenience methods (P14):** Add `profile.delta_runtime_ctx()` and similar convenience methods to `DataFusionRuntimeProfile` to reduce chain depth across the codebase.
