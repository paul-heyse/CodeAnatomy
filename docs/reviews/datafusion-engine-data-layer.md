# Design Review: datafusion_engine Data Layer

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/delta/`, `src/datafusion_engine/arrow/`, `src/datafusion_engine/dataset/`, `src/datafusion_engine/io/`
**Focus:** All principles (1-24), with special attention to Ports & Adapters at IO boundaries, information hiding of Arrow/Delta internals, and coupling between delta and arrow layers
**Depth:** Deep (exhaustive, all files)
**Files reviewed:** 55 (21,684 LOC)

## Executive Summary

The Data Layer demonstrates strong Protocol-based port definitions for Arrow types (`arrow/interop.py`) and Delta Rust FFI boundaries (`delta/protocols.py`), and the `DataFusionIOAdapter` provides a well-factored single entry point for all registration operations. However, the layer suffers from three systemic issues: (1) the `delta/control_plane.py` mega-module (2,070 LOC, 19 request types, 12+ operation functions) concentrates too many responsibilities and exposes brittle positional argument passing to Rust FFI; (2) the `.resolved` property on `DatasetLocation` creates pervasive Law of Demeter violations across 25+ call sites that chain through intermediate resolved objects; and (3) `dataset/registration.py` (3,368 LOC) has accumulated extraction-specific scan defaults, DDL generation, cache management, and provider resolution into a single file that changes for many independent reasons.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | `commit_payload_values[0..5]` exposes Rust FFI positional layout in 12 call sites |
| 2 | Separation of concerns | 1 | large | high | `registration.py` mixes DDL generation, scan defaults, cache policy, provider resolution |
| 3 | SRP (one reason to change) | 1 | large | high | `control_plane.py` changes for request types, Rust FFI, AND feature toggles |
| 4 | High cohesion, low coupling | 2 | medium | medium | `delta/` is well-bounded from `arrow/`; but `registration.py` pulls from 25+ imports |
| 5 | Dependency direction | 2 | medium | medium | Core types defined properly; some reverse deps via `delta_service_for_profile(None)` |
| 6 | Ports & Adapters | 2 | medium | medium | Arrow protocols excellent; Delta FFI ports good; `DataFusionIOAdapter` well-designed |
| 7 | DRY (knowledge, not lines) | 1 | medium | medium | `commit_payload_values` pattern repeated 12 times; `table_ref` boilerplate in 19 classes |
| 8 | Design by contract | 2 | small | low | Request types are well-typed; some loose `object` return types in resolution |
| 9 | Parse, don't validate | 2 | small | low | Good boundary parsing via `DatasetLocation` and `DeltaProviderContract` |
| 10 | Make illegal states unrepresentable | 2 | medium | medium | `DatasetLocation` allows contradictory states (delta_cdf + non-delta format) |
| 11 | CQS | 2 | small | low | `DataFusionIOAdapter` methods mix registration + artifact recording (minor) |
| 12 | Dependency inversion + explicit composition | 1 | medium | high | `delta_service_for_profile(None)` creates hidden dependencies in 3 call sites |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance hierarchies; composition throughout |
| 14 | Law of Demeter | 1 | medium | high | 25+ sites access `location.resolved.delta_*` chaining through intermediate |
| 15 | Tell, don't ask | 2 | medium | medium | `registry_snapshot()` pulls 15+ individual fields from resolved location |
| 16 | Functional core, imperative shell | 2 | medium | medium | Arrow builders are pure; Delta operations mix IO with transform logic |
| 17 | Idempotency | 2 | small | low | Write operations support idempotent commits; object store re-registration guarded |
| 18 | Determinism / reproducibility | 2 | small | low | Fingerprinting present; sorted ordering in payloads |
| 19 | KISS | 1 | large | medium | `registration.py` at 3,368 LOC with hardcoded scan defaults per extraction type |
| 20 | YAGNI | 2 | small | low | Minimal speculative abstractions; thin facades exist but are lightweight |
| 21 | Least astonishment | 2 | small | low | `DatasetLocation.resolved` property triggers lazy computation (minor surprise) |
| 22 | Declare and version public contracts | 2 | small | low | `__all__` consistently declared; msgspec structs have clear boundaries |
| 23 | Design for testability | 1 | medium | high | `delta_service_for_profile(None)` creates real Delta services in unit paths |
| 24 | Observability | 2 | small | low | Good artifact recording; structured diagnostics throughout `DataFusionIOAdapter` |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
The `delta/control_plane.py` module exposes the internal layout of Rust FFI positional arguments through `commit_payload_values[0]` through `commit_payload_values[5]`, repeated across 12 distinct operation functions. Any change to the Rust entrypoint argument ordering silently breaks all 12 call sites.

**Findings:**
- `delta/control_plane.py:972-977` (and 11 more sites through line 1484): Positional indexing `commit_payload_values[0..5]` exposes the internal tuple layout of `commit_payload()` return value. This is the most critical information-hiding violation in the data layer.
- `delta/service.py:11-65`: Imports 20+ symbols from `storage.deltalake.delta`, exposing the internal decomposition of the storage layer to the service facade.
- `delta/observability.py:1-1121`: Exposes internal schema definitions (4 table schemas) alongside recording functions, mixing schema knowledge with recording behavior.

**Suggested improvement:**
Extract a `_invoke_rust_entrypoint(entrypoint, *, commit: CommitPayload, table_ref: DeltaTableRef, ...)` helper function in `control_plane.py` that encapsulates the positional argument unpacking in one place. The `commit_payload()` function should return a named struct (e.g., `CommitPayloadBundle`) with named fields instead of relying on tuple positional access.

**Effort:** medium
**Risk if unaddressed:** high -- Any Rust FFI argument reordering silently breaks 12 functions.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`dataset/registration.py` (3,368 LOC) mixes at least five distinct concerns: DDL generation and SQL string building, extraction-specific scan default configurations, DataFusion cache management, provider resolution and registration orchestration, and artifact/diagnostics recording.

**Findings:**
- `dataset/registration.py:141-208`: Hardcoded extraction-type scan defaults (`_CST_EXTERNAL_TABLE_NAME`, `_AST_EXTERNAL_TABLE_NAME`, etc.) are domain-specific extraction knowledge embedded in a generic registration module.
- `dataset/registration.py:694-739`: DDL string building functions (`_ddl_identifier`, `_ddl_string_literal`, `_ddl_order_clause`, `_ddl_options_clause`) are SQL generation concerns mixed with registration.
- `delta/observability.py:1-1121`: Mixes artifact type definitions, Arrow schema definitions for 4 observability tables, recording functions, schema drift detection, and table bootstrap/repair logic in a single file.
- `io/write.py:1-2689`: Combines write format enums, Delta policy resolution, write execution, maintenance triggering, and diagnostics recording.

**Suggested improvement:**
Split `dataset/registration.py` into: (1) `dataset/ddl.py` for SQL/DDL generation helpers, (2) `dataset/scan_defaults.py` for extraction-type-specific scan configurations, (3) `dataset/cache.py` for `DatasetCaches` and cache management, and keep `registration.py` focused on the registration orchestration. Similarly, split `delta/observability.py` into `observability_schemas.py` (table definitions) and `observability_recording.py` (recording functions).

**Effort:** large
**Risk if unaddressed:** high -- The 3,368-LOC registration module is a maintenance bottleneck; changes to DDL syntax, scan defaults, or cache policy all modify the same file.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`delta/control_plane.py` changes for three independent reasons: (1) adding/modifying Delta request types (19 classes), (2) changing Rust FFI calling conventions, and (3) modifying Delta feature toggle operations.

**Findings:**
- `delta/control_plane.py:63-456`: 19 request type class definitions that change when the Delta API contract evolves.
- `delta/control_plane.py:457-1500`: 12+ operation functions (`delta_provider_from_session`, `delta_write_ipc`, `delta_delete`, etc.) that change when Rust FFI conventions change.
- `delta/control_plane.py:1500-2070`: Feature enable/disable functions (`delta_enable_features`, `delta_add_constraints`, etc.) that change when feature management evolves.
- `dataset/registration.py`: Changes when scan defaults change, DDL syntax changes, cache policy changes, provider resolution changes, or artifact recording changes -- at least 5 independent reasons to change.

**Suggested improvement:**
Split `delta/control_plane.py` into: (1) `delta/request_types.py` for all 19 request dataclass definitions, (2) `delta/control_plane.py` for the core Rust FFI adapter operations, and (3) `delta/feature_management.py` for feature toggle operations.

**Effort:** large
**Risk if unaddressed:** high -- The 2,070-LOC file makes it difficult to assess blast radius of changes and increases merge conflict probability.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The `delta/` and `arrow/` directories are well-decoupled from each other: only 4 import paths cross from `delta/` into `arrow/` (for `schema_to_dict`, `RecordBatchReaderLike`, and `field_builders`), and zero imports go from `arrow/` into `delta/`. This is a good boundary. However, `dataset/registration.py` has 25+ import lines pulling from across the entire `datafusion_engine` package.

**Findings:**
- `delta/` to `arrow/` coupling: Only 4 imports (`delta/scan_config.py:9`, `delta/service.py:11`, `delta/observability.py:19`, `delta/control_plane.py:21`). This is appropriately narrow.
- `arrow/` to `delta/` coupling: Zero imports. Arrow is fully independent of Delta. Excellent.
- `dataset/registration.py:55-134`: 25+ import lines spanning `arrow`, `catalog`, `dataset`, `delta`, `errors`, `identity`, `io`, `lineage`, `plan`, `schema`, `session`, `sql`, `tables`, plus external packages. This file is a coupling hotspot.
- `io/write.py:44-104`: Similarly heavy imports (20+ lines) spanning multiple subpackages.

**Suggested improvement:**
For `dataset/registration.py`, introduce a `RegistrationContext` protocol or facade that encapsulates the minimum needed from each subpackage, reducing direct imports. For `io/write.py`, extract the Delta-specific write path into `io/write_delta.py` to isolate Delta coupling from CSV/JSON/Arrow writes.

**Effort:** medium
**Risk if unaddressed:** medium -- High fan-in files resist refactoring and create cascading test breakage.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Core types (`DatasetLocation`, Arrow protocols, Delta request types) are defined in leaf modules and depended upon by higher-level orchestration. This is correct. However, `delta_service_for_profile(None)` creates a reverse dependency where pure contract/schema modules reach into the service layer.

**Findings:**
- `delta/contracts.py:261`: `delta_service_for_profile(None).table_schema(request)` -- a contract-building function reaches into the service layer to resolve a schema, inverting the expected dependency direction.
- `delta/schema_guard.py:103`: Same pattern -- `delta_service_for_profile(None).table_schema(request)` in a guard/validation function.
- `dataset/registry.py:621`: Same pattern -- schema resolution calls into the Delta service layer from a registry module.
- `arrow/` depends on `pyarrow` and `serde_msgspec` only -- clean dependency direction with no reverse deps.

**Suggested improvement:**
Inject a `SchemaResolver` protocol (a callable or protocol with a `table_schema()` method) into `build_delta_provider_contract()`, `enforce_schema_policy()`, and `_resolve_dataset_schema_internal()` instead of having them create `DeltaService` instances via `delta_service_for_profile(None)`.

**Effort:** medium
**Risk if unaddressed:** medium -- Hidden service creation makes these functions non-deterministic and harder to test.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The Arrow port layer (`arrow/interop.py`) provides 15+ `runtime_checkable` Protocol types (`DataTypeLike`, `FieldLike`, `TableLike`, `SchemaLike`, etc.) that successfully abstract over PyArrow concrete types. The Delta FFI port layer (`delta/protocols.py`) defines typed Protocols for Rust entry points. `DataFusionIOAdapter` (`io/adapter.py`) consolidates all IO operations behind a clean adapter surface. These are strong patterns.

**Findings:**
- `arrow/interop.py:1-905`: Excellent port definitions with 15+ protocols covering the full Arrow type hierarchy. Callers throughout the codebase use `SchemaLike`, `ArrayLike`, etc. instead of concrete `pa.Schema`, `pa.Array`.
- `delta/protocols.py:1-140`: Typed Protocol surface for Rust Delta FFI handles (`InternalSessionContext`, `RustDeltaEntrypoint`, `DeltaTableHandle`, etc.). Good isolation of the Rust boundary.
- `io/adapter.py:114-537`: `DataFusionIOAdapter` is a well-designed adapter consolidating object store, table, listing table, catalog, view, and dataset registration with consistent diagnostics.
- Gap: The Rust FFI boundary in `delta/control_plane.py` does not use the protocols defined in `delta/protocols.py` consistently -- many functions accept `SessionContext` directly and call `getattr(ctx, ...)` patterns rather than going through typed protocol methods.

**Suggested improvement:**
Ensure all Rust FFI invocations in `delta/control_plane.py` go through the Protocol types defined in `delta/protocols.py`. Replace `getattr(ctx, "delta_entrypoint", None)` patterns with typed protocol method calls.

**Effort:** medium
**Risk if unaddressed:** medium -- Without consistent protocol usage, the port abstraction exists but is bypassed in the critical FFI path.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The `commit_payload_values[0..5]` positional unpacking pattern is repeated 12 times in `delta/control_plane.py`, encoding the same knowledge about Rust FFI argument ordering in 12 separate locations. The `table_ref` property boilerplate is repeated across 19 request classes.

**Findings:**
- `delta/control_plane.py:972-1484`: The `commit_payload_values[0..5]` pattern appears 12 times (one per Rust-backed Delta operation). Each encodes identical knowledge about how commit payloads map to Rust entrypoint arguments.
- `delta/control_plane.py:63-456`: 19 request classes each define a `table_ref` property with nearly identical implementation (construct `DeltaTableRef` from `path` and `storage_options` fields).
- `delta/service.py` method bodies: `_resolve_store_options()` pattern repeated across `provider()`, `table_version()`, `table_schema()`, `read_table()`, etc.
- `dataset/registration.py:141-193`: Extraction-specific scan defaults repeat the pattern of partition_fields + file_sort_order + cache_ttl for 5 different extraction types.

**Suggested improvement:**
Extract a `_invoke_with_commit(entrypoint, table_ref, commit_payload, ...)` helper to eliminate the 12 repetitions. For request classes, use a mixin or `__init_subclass__` to generate the `table_ref` property from `path` and `storage_options` fields automatically.

**Effort:** medium
**Risk if unaddressed:** medium -- When the Rust FFI argument layout changes, all 12 sites must be updated in lockstep.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Request types are well-typed with `frozen=True` msgspec Structs that enforce field types at construction time. `DataFusionIOAdapter` validates preconditions (table existence before overwrite). Explicit error types (`DataFusionEngineError` with `ErrorKind`) provide classified failures.

**Findings:**
- `delta/control_plane.py:63-456`: All 19 request types are `frozen=True` Structs with typed fields. Good construction-time validation.
- `io/adapter.py:218`: `register_arrow_table` checks `ctx.table_exist(name)` before overwrite. Good precondition checking.
- `dataset/resolution.py:64`: `DatasetResolution.provider` is typed as `object` instead of a more specific type or protocol. This weakens the postcondition contract.
- `io/write_validation.py:6-14`: `validate_destination()` provides minimal validation (non-empty check only). Write operations could benefit from stronger path validation contracts.

**Suggested improvement:**
Type `DatasetResolution.provider` as `TableProviderCapsule | object` or a dedicated `DeltaProvider` protocol to strengthen the postcondition. Add path format validation to `validate_destination()`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`DatasetLocation` and `ResolvedDatasetLocation` demonstrate good parse-don't-validate patterns: raw location data is parsed once into a structured `DatasetLocation`, then resolved once into `ResolvedDatasetLocation` with all derived fields computed. After resolution, callers work with well-typed fields.

**Findings:**
- `dataset/registry.py:75-101`: `DatasetLocation` normalizes path in `__post_init__` and provides a `.resolved` property that computes the fully resolved view once.
- `delta/contracts.py:70-180`: `build_delta_provider_contract()` and `build_delta_cdf_contract()` parse raw location data into structured contracts once.
- `delta/payload.py:1-100`: `schema_ipc_payload()`, `commit_payload()`, `cdf_options_payload()` convert raw inputs to structured payloads at the boundary.
- Minor gap: `io/ingest.py:148-165` uses runtime type checking (`_is_pydict_input`, `_is_row_mapping_sequence`) rather than converting to a typed union at the boundary.

**Suggested improvement:**
In `io/ingest.py`, convert the input to a discriminated union type (`ArrowTableInput | PyDictInput | RowSequenceInput`) at the top of `datafusion_from_arrow()` instead of using predicate functions in the body.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most request types use frozen Structs that prevent mutation after construction. However, `DatasetLocation` allows contradictory field combinations that should be prevented by construction.

**Findings:**
- `dataset/registry.py:75-91`: `DatasetLocation` allows `format="parquet"` + `delta_cdf_options=DeltaCdfOptions(...)`, which is contradictory. Similarly, `delta_version` is meaningless for non-delta formats.
- `delta/control_plane.py:154-183`: `DeltaWriteRequest` has `commit_options` and `app_transaction` that are both optional but mutually interact -- their valid combinations are not enforced by the type.
- `arrow/encoding.py:1-36`: `EncodingSpec` and `EncodingPolicy` are well-constructed with valid-only combinations.

**Suggested improvement:**
Consider adding a `__post_init__` validation to `DatasetLocation` that raises `ValueError` when `delta_cdf_options` is set for non-delta formats, or when `delta_version`/`delta_timestamp` are set for non-delta formats.

**Effort:** medium
**Risk if unaddressed:** medium -- Contradictory locations can pass through construction and cause confusing downstream errors.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Arrow builder functions (`build.py`, `nested.py`) are pure queries. Delta recording functions are pure commands. Minor violations exist in `DataFusionIOAdapter` where registration methods both mutate state and return diagnostics artifacts.

**Findings:**
- `arrow/build.py:1-843`: All builder functions are pure -- they return new arrays/tables without side effects. Good CQS alignment.
- `io/adapter.py:143-190`: `register_object_store()` mutates the session context AND records diagnostics artifacts AND updates the internal registry set. Three side effects in one call.
- `delta/service.py:250-280`: `mutate()` method both performs the mutation AND returns the result mapping. This is inherent to the operation semantics and acceptable.

**Suggested improvement:**
Consider separating the diagnostics recording in `DataFusionIOAdapter` methods into a post-registration hook rather than embedding it in each registration method.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
The most significant DI violation is `delta_service_for_profile(None)` which creates real `DeltaService` instances with default profiles in three locations that should receive their dependencies explicitly.

**Findings:**
- `delta/contracts.py:261`: `schema = delta_service_for_profile(None).table_schema(request)` -- hidden service creation inside a contract builder.
- `delta/schema_guard.py:103`: `existing_schema = delta_service_for_profile(None).table_schema(request)` -- hidden service creation inside a schema guard.
- `dataset/registry.py:619-630`: `return delta_service_for_profile(None).table_schema(...)` -- hidden service creation during schema resolution.
- `dataset/semantic_catalog.py:27-35` and `dataset/semantic_catalog.py:158-166`: Deferred imports of `schema_spec.contracts` inside function bodies. While these avoid circular imports, they hide dependencies from the function signature.

**Suggested improvement:**
Add an optional `schema_resolver: Callable[[DeltaSchemaRequest], SchemaLike | None] | None = None` parameter to `build_delta_provider_contract()`, `enforce_schema_policy()`, and `_resolve_dataset_schema_internal()`. Default to `delta_service_for_profile(None).table_schema` when None is provided, but allow injection for testing and decoupled composition.

**Effort:** medium
**Risk if unaddressed:** high -- These hidden dependencies prevent isolated unit testing and create implicit IO in functions that appear to be pure contract builders.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The data layer uses composition consistently. No inheritance hierarchies exist. All request types are standalone frozen Structs. `DataFusionIOAdapter` composes a `SessionContext` and `DataFusionRuntimeProfile` rather than inheriting from either. `DeltaService` wraps a `DataFusionRuntimeProfile` via composition.

**Findings:**
- No class hierarchies found across all 55 files.
- `delta/service.py:100-120`: `DeltaService` is a frozen dataclass wrapping `DataFusionRuntimeProfile` via composition.
- `io/adapter.py:114-140`: `DataFusionIOAdapter` composes `SessionContext` + `DataFusionRuntimeProfile`.
- `arrow/semantic.py:97-103`: `_SemanticExtensionType` extends `pa.ExtensionType`, which is the correct use of inheritance for PyArrow's extension type protocol.

**Suggested improvement:**
None needed. Composition is used appropriately throughout.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
The `.resolved` property on `DatasetLocation` creates a systemic Law of Demeter violation. 25+ call sites across the codebase access `location.resolved.delta_*` fields, chaining through the intermediate `ResolvedDatasetLocation` object. This means changes to the `ResolvedDatasetLocation` structure ripple to every consumer.

**Findings:**
- `io/write.py:380`: `dataset_location.resolved.delta_write_policy` -- 2-level chain
- `io/write.py:383`: `dataset_location.resolved.delta_schema_policy` -- 2-level chain
- `io/write.py:1481`: `inputs.dataset_location.resolved.delta_maintenance_policy` -- 3-level chain
- `io/write.py:1497`: `inputs.dataset_location.resolved.delta_feature_gate` -- 3-level chain
- `delta/contracts.py:142`: `location.resolved.delta_log_storage_options` -- 2-level chain
- `delta/contracts.py:174`: `location.resolved.delta_feature_gate` -- 2-level chain
- `dataset/resolution.py:110`: `request.location.resolved.delta_feature_gate` -- 3-level chain
- `dataset/registration.py:570,1204,1261,2460,3230,3247`: 6 additional `.resolved.` chains
- Total: 25+ call sites violating LoD across `io/`, `delta/`, `dataset/`, `plan/`, `lineage/`, `views/`

**Suggested improvement:**
Add convenience methods or properties directly on `DatasetLocation` that delegate to the resolved view: e.g., `location.delta_write_policy` -> `self.resolved.delta_write_policy`. Alternatively, refactor callers to accept the resolved view directly rather than reaching through the location.

**Effort:** medium
**Risk if unaddressed:** high -- Any structural change to `ResolvedDatasetLocation` forces updates across 25+ call sites in 6+ modules.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Some modules ask `DatasetLocation` for raw data and then implement logic externally, rather than telling the location to perform the operation.

**Findings:**
- `dataset/registry.py:183-308`: `registry_snapshot()` pulls 15+ individual fields from `resolved` and manually constructs a dict for each location. The location object should know how to serialize itself.
- `dataset/registration.py:588-631`: `_resolve_registry_options_for_location()` extracts `resolved.datafusion_scan`, `resolved.schema`, `resolved.datafusion_provider`, `resolved.delta_cdf_policy` individually and reassembles them. The location should provide a `to_registry_options()` method.
- Arrow builder functions (`build.py`, `nested.py`) appropriately use Tell-Don't-Ask -- callers pass data in, builders produce results.

**Suggested improvement:**
Add a `to_snapshot_dict()` method on `DatasetLocation` or `ResolvedDatasetLocation` to encapsulate the snapshot serialization logic currently scattered in `registry_snapshot()`.

**Effort:** medium
**Risk if unaddressed:** medium -- Serialization logic for the same data shape is likely to drift if implemented in multiple places.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The Arrow sub-package is almost entirely a functional core -- `build.py`, `nested.py`, `coercion.py`, `metadata_codec.py` are pure transformation functions with no side effects. The Delta sub-package mixes IO operations with data transformation in several modules.

**Findings:**
- `arrow/build.py:1-843`: Pure array/table builders with no IO. Excellent functional core.
- `arrow/nested.py:1-820`: Pure nested array builders. Excellent functional core.
- `arrow/metadata_codec.py:1-282`: Pure encode/decode functions. Excellent functional core.
- `delta/control_plane.py`: Mixes Rust FFI calls (imperative) with request type definitions (functional). The request types are pure but live alongside IO-performing functions.
- `delta/observability.py:750-1121`: Mixes schema definition (functional) with table bootstrap/repair/append operations (imperative IO).
- `io/adapter.py:114-537`: Appropriately imperative -- this is the shell that performs all IO.

**Suggested improvement:**
Move request type definitions out of `delta/control_plane.py` into a separate `delta/request_types.py` module to cleanly separate the functional core (types) from the imperative shell (FFI operations).

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Write operations support idempotent commits via `idempotent_commit_properties()`. Object store registration in `DataFusionIOAdapter` is guarded against duplicate registration. Table overwrite semantics are handled explicitly.

**Findings:**
- `io/adapter.py:169-175`: Object store registration tracks registered `(scheme, host)` pairs and skips duplicates. Good idempotency guard.
- `io/adapter.py:218`: Table registration checks `table_exist()` before overwrite. Explicit idempotency.
- `io/write.py:77-78`: Uses `idempotent_commit_properties` from `storage.deltalake`.
- `delta/observability.py:750+`: Table bootstrap with `_ensure_observability_tables()` checks for existence before creating.

**Suggested improvement:**
None critical. The idempotency patterns are adequate.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Fingerprinting is present throughout the data layer. `EncodingPolicy.fingerprint()`, `_DeltaPolicyContext.fingerprint()`, `DataFusionCachePolicy.fingerprint()` all produce deterministic hashes. Metadata encoding uses sorted key ordering.

**Findings:**
- `arrow/metadata_codec.py:64-74`: `encode_metadata_map()` sorts entries by key before encoding. Deterministic.
- `arrow/encoding.py:1-36`: `EncodingPolicy` includes `fingerprint()` for reproducible policy identity.
- `io/write.py:199-248`: `_DeltaPolicyContext.fingerprint_payload()` produces sorted, deterministic policy snapshots.
- `dataset/resolution.py:237-254`: `scan_units_hash()` sorts units before hashing. Deterministic.

**Suggested improvement:**
None critical. Determinism patterns are well-established.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
The `dataset/registration.py` file at 3,368 LOC is the most significant complexity concern in the data layer. It contains hardcoded per-extraction-type scan defaults, DDL generation, cache management, provider resolution, and registration orchestration. The `delta/control_plane.py` at 2,070 LOC is similarly oversized.

**Findings:**
- `dataset/registration.py:141-193`: Hardcoded scan defaults for 5 extraction types (`_CST_EXTERNAL_TABLE_NAME`, `_AST_EXTERNAL_TABLE_NAME`, `_BYTECODE_EXTERNAL_TABLE_NAME`, `_TREE_SITTER_EXTERNAL_TABLE_NAME`, `_SYMTABLE_EXTERNAL_TABLE_NAME`) with column names, sort orders, TTLs, and parquet options. This is extraction-domain knowledge in a generic registration module.
- `delta/control_plane.py:1-2070`: 19 request classes + 12 operation functions + feature toggles in one file.
- `io/write.py:1-2689`: WritePipeline, WriteRequest, WriteFormat, Delta policy resolution, maintenance triggering, diagnostics -- all in one module.
- `dataset/registry.py:183-308`: `registry_snapshot()` is a 126-line function building a manually-constructed dict for each registry entry. Could be simplified with a serialization method on the location type.

**Suggested improvement:**
As priority, split `dataset/registration.py` into focused modules (DDL, scan defaults, cache, orchestration). The extraction-specific scan defaults should live in the extraction package or a dedicated scan policy module, not in the generic registration code.

**Effort:** large
**Risk if unaddressed:** medium -- High LOC files resist comprehension and increase onboarding cost.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The data layer is generally lean. Thin facades (`delta/transactions.py`, `delta/table_management.py`, `arrow/metadata_read.py`, `arrow/metadata_write.py`) are lightweight delegation layers that add minimal overhead. A few exist primarily for import convenience.

**Findings:**
- `delta/transactions.py:1-87`: 4 functions that are 1-line delegations to `control_plane`. Borderline unnecessary but very lightweight.
- `delta/table_management.py`: Similarly thin facade.
- `arrow/metadata_read.py:1-16`: Single function delegating to `metadata.metadata_payload`. Very thin.
- `arrow/metadata_write.py:1-19`: Single function delegating to `metadata.merge_metadata_specs`. Very thin.
- `io/write_execution.py:1-19`: `execute_write()` is a 1-line function calling `callable_write(request)`. This appears to be YAGNI but may serve as a future extension point.
- `io/write_validation.py:1-17`: `validate_destination()` checks only non-empty. Minimal but could grow.

**Suggested improvement:**
Consider consolidating the thinnest facades (`metadata_read.py`, `metadata_write.py`, `write_execution.py`) back into their parent modules if they have no additional consumers beyond the original import path. However, this is low priority -- the overhead is minimal.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs generally follow expected conventions. Named parameters, keyword-only arguments, and consistent `__all__` exports make the surface predictable. One notable surprise: `DatasetLocation.resolved` is a property that triggers potentially expensive lazy computation.

**Findings:**
- `dataset/registry.py:97-100`: `DatasetLocation.resolved` property calls `_resolve_cached_location(self)` which performs full policy resolution, Delta schema lookups, and feature gate validation on every access. No caching is apparent.
- `arrow/interop.py:1-905`: Protocol types are named consistently with `*Like` suffix. Predictable and conventional.
- `io/adapter.py:114-537`: `DataFusionIOAdapter` methods use keyword-only parameters consistently. Good.
- `delta/payload.py:1-100`: Helper functions have clear, predictable names (`schema_ipc_payload`, `commit_payload`, `settings_bool`).

**Suggested improvement:**
Cache the `resolved` property result on `DatasetLocation` (via `functools.cached_property` or an internal `_resolved` slot) to prevent repeated expensive resolution. Document that `.resolved` may perform IO on first access.

**Effort:** small
**Risk if unaddressed:** low -- But repeated `.resolved` calls in hot paths could have performance impact.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules declare `__all__` exports consistently. The `delta/__init__.py` uses a lazy import pattern with `_EXPORT_MAP`. Msgspec structs with `frozen=True` provide stable serialization contracts. The `METADATA_PAYLOAD_VERSION` constant in `metadata_codec.py` is a good versioning example.

**Findings:**
- `arrow/metadata_codec.py:12`: `METADATA_PAYLOAD_VERSION: int = 1` -- explicit versioning for metadata payloads. Good.
- All 55 files declare `__all__`. Consistent.
- `dataset/registry.py:712-726`: `__getattr__` provides lazy re-exports with deprecation-style forwarding for removed symbols. Good evolution strategy.
- `delta/__init__.py`: Lazy import via `_EXPORT_MAP` and `__getattr__`. Clean public surface.

**Suggested improvement:**
None critical. Contract versioning and `__all__` declarations are consistently applied.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The `delta_service_for_profile(None)` pattern creates real `DeltaService` instances in contract builders and schema guards, making isolated unit testing impossible without monkeypatching or actually having a Delta table on disk.

**Findings:**
- `delta/contracts.py:261`: `delta_service_for_profile(None).table_schema(request)` -- unit testing `build_delta_provider_contract()` requires a real Delta table or monkeypatching.
- `delta/schema_guard.py:103`: Same problem in `enforce_schema_policy()`.
- `dataset/registry.py:619-630`: Same problem in `_resolve_dataset_schema_internal()`.
- `io/adapter.py:114-537`: `DataFusionIOAdapter` accepts `SessionContext` and `DataFusionRuntimeProfile` as constructor arguments -- good DI that enables test doubles.
- `arrow/build.py`, `arrow/nested.py`: Pure functions with no dependencies -- trivially testable. Excellent.

**Suggested improvement:**
Add an optional `schema_resolver` parameter to the functions that call `delta_service_for_profile(None)`, defaulting to the real implementation. Tests can inject a stub that returns a fixed schema.

**Effort:** medium
**Risk if unaddressed:** high -- Without injectable dependencies, these functions require integration-level test infrastructure for what should be unit-level behavior.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Observability is well-structured through `DataFusionIOAdapter._record_artifact()` and the `record_artifact()` function from `lineage.diagnostics`. The `delta/observability.py` module provides 5 artifact types with structured schemas. However, the observability module itself has grown to mix schema definition with IO operations.

**Findings:**
- `io/adapter.py:499-535`: Consistent `_record_registration()` and `_record_artifact()` methods on `DataFusionIOAdapter`. All registration types produce structured diagnostics.
- `delta/observability.py:194-450`: `record_delta_snapshot()`, `record_delta_mutation()`, `record_delta_feature_state()`, `record_delta_scan_plan()`, `record_delta_maintenance()` -- well-named recording functions with structured payloads.
- `delta/observability.py:460-1121`: Schema definitions for 4 observability tables with bootstrap/repair logic. This is appropriately comprehensive but mixes concerns.
- `io/ingest.py:168-183`: `_emit_arrow_ingest()` hook provides structured ingest diagnostics via callback.

**Suggested improvement:**
Split `delta/observability.py` into `delta/observability_schemas.py` (table definitions + schema constants) and `delta/observability_recording.py` (recording functions + bootstrap logic) to separate the schema knowledge from the IO operations.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: The `.resolved` Property Chain Anti-Pattern

The `DatasetLocation.resolved` property creates a pervasive LoD violation (P14) that affects P1 (information hiding), P4 (coupling), and P23 (testability). 25+ call sites across 6+ modules chain through `location.resolved.delta_*` fields. This pattern was likely introduced for convenience but has become a structural coupling hotspot.

**Root cause:** `DatasetLocation` is both a data carrier (path, format, options) and a resolution gateway (via `.resolved`). Callers use `.resolved` as a shortcut to avoid passing resolved views through function signatures.

**Affected principles:** P1, P4, P14, P15, P23

**Suggested approach:** Either (a) add delegation properties on `DatasetLocation` itself, or (b) require callers to resolve once and pass `ResolvedDatasetLocation` through their call chains explicitly.

### Theme 2: The 2,000+ LOC Module Pattern

Three modules exceed 2,000 LOC: `dataset/registration.py` (3,368), `io/write.py` (2,689), and `delta/control_plane.py` (2,070). These files violate P2 (separation of concerns), P3 (SRP), P4 (coupling), and P19 (KISS) simultaneously.

**Root cause:** Organic growth without periodic decomposition. Each module started focused but accumulated adjacent responsibilities over time.

**Affected principles:** P2, P3, P4, P19

**Suggested approach:** Decompose along responsibility seams: request types vs. operations, DDL generation vs. registration orchestration, Delta-specific writes vs. format-agnostic writes.

### Theme 3: Hidden Service Creation

The `delta_service_for_profile(None)` pattern in 3 locations creates real service instances with default profiles, hiding IO dependencies and preventing isolated unit testing. This violates P5 (dependency direction), P12 (DI), and P23 (testability).

**Root cause:** Schema resolution for Delta tables requires an actual Delta service, and the convenience of `delta_service_for_profile(None)` avoided threading a dependency through the call chain.

**Affected principles:** P5, P12, P23

**Suggested approach:** Introduce a `SchemaResolver` protocol and inject it into the 3 affected functions. Default to the real implementation at the composition root.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract `_invoke_with_commit()` helper to eliminate 12 repeated `commit_payload_values[0..5]` sites in `control_plane.py` | small | Eliminates highest-frequency semantic duplication and reduces FFI breakage risk |
| 2 | P12/P23 (DI/Testability) | Add optional `schema_resolver` parameter to `build_delta_provider_contract()`, `enforce_schema_policy()`, `_resolve_dataset_schema_internal()` | small | Unlocks unit testing for 3 currently integration-only functions |
| 3 | P14 (LoD) | Add delegation properties on `DatasetLocation` for commonly-accessed resolved fields | small | Reduces structural coupling in 25+ call sites without changing behavior |
| 4 | P21 (Least astonishment) | Cache `DatasetLocation.resolved` with `functools.cached_property` | small | Prevents repeated expensive resolution in hot paths |
| 5 | P24 (Observability) | Split `delta/observability.py` into schema definitions and recording functions | small | Separates schema knowledge from IO; easier to test and modify independently |

## Recommended Action Sequence

1. **Extract `_invoke_with_commit()` helper in `delta/control_plane.py`** (P1, P7) -- Encapsulate the `commit_payload_values[0..5]` positional unpacking in a single function. This is the highest-risk duplication in the layer and the simplest to fix. Single file change.

2. **Inject `schema_resolver` into 3 hidden-service sites** (P5, P12, P23) -- Add optional resolver parameter to `build_delta_provider_contract()`, `enforce_schema_policy()`, and `_resolve_dataset_schema_internal()`. Enables unit testing without Delta tables.

3. **Add delegation properties on `DatasetLocation`** (P14) -- Reduce the 25+ `.resolved.delta_*` chains to `location.delta_write_policy` etc. Non-breaking change; callers can migrate incrementally.

4. **Cache `DatasetLocation.resolved`** (P21) -- Use `functools.cached_property` or internal slot to avoid repeated resolution. Small change with potential performance benefit.

5. **Split `delta/control_plane.py` into request types + operations + feature management** (P2, P3) -- Move 19 request classes to `delta/request_types.py`, feature toggles to `delta/feature_management.py`. Requires import updates across consumers.

6. **Split `delta/observability.py` into schemas + recording** (P2, P24) -- Separate the 4 table schema definitions from the recording/bootstrap logic.

7. **Extract extraction-specific scan defaults from `dataset/registration.py`** (P2, P19) -- Move `_DEFAULT_SCAN_CONFIGS` and related constants to `dataset/scan_defaults.py` or the extraction package.

8. **Split `dataset/registration.py` DDL generation** (P2, P19) -- Extract `_ddl_*` functions to `dataset/ddl.py`.

9. **Extract Delta-specific write path from `io/write.py`** (P2, P4) -- Move Delta policy resolution, writer properties, and maintenance into `io/write_delta.py`.

10. **Enforce Protocol usage in `delta/control_plane.py` FFI calls** (P6) -- Replace `getattr(ctx, ...)` patterns with typed Protocol method invocations through `delta/protocols.py`.
