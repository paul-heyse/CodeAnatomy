# Design Review: src/datafusion_engine/delta/

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/delta/`
**Focus:** All principles (1-24)
**Depth:** deep (all files)
**Files reviewed:** 16

## Executive Summary

The `delta/` subpackage provides a well-structured layered adapter between the CodeAnatomy core pipeline and Delta Lake storage. The overall architecture demonstrates strong separation between protocol abstractions, storage policies, and service boundaries. However, the `control_plane.py` module (2,297 lines) exhibits significant structural issues: pervasive knowledge duplication across 17 request structs that share identical `table_ref` fields, heavy positional-argument coupling to the Rust FFI boundary, and a flat file that conflates CRUD operations, feature toggles, and metadata queries. The observability module is well-intentioned but tightly coupled to concrete IO infrastructure and contains a bare `except Exception`. Store policy encapsulation and the contracts module demonstrate good principle alignment overall.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | medium | medium | control_plane exposes Rust FFI positional calling convention; gate_payload tuple indexing leaks |
| 2 | Separation of concerns | 1 | large | high | control_plane.py mixes CRUD, feature toggles, metadata queries, and Rust FFI marshalling in one file |
| 3 | SRP | 1 | large | high | control_plane.py changes for 5+ different reasons; observability.py changes for schema, write pipeline, and telemetry |
| 4 | High cohesion, low coupling | 2 | medium | medium | Good intra-module cohesion for protocol/store_policy/specs; control_plane has high fan-out |
| 5 | Dependency direction | 2 | small | low | Core types in protocol.py/specs.py have minimal deps; service.py correctly wraps storage.deltalake |
| 6 | Ports & Adapters | 2 | medium | medium | store_policy and object_store act as adapters; missing explicit port abstraction for the Rust control plane |
| 7 | DRY (knowledge) | 1 | medium | high | 17 request structs repeat identical 4-field table_ref pattern; gate_payload destructuring repeated 15+ times |
| 8 | Design by contract | 2 | small | low | Request structs enforce shape; DeltaMutationRequest.validate() is explicit; some weak `object` typing |
| 9 | Parse, don't validate | 2 | small | low | Request structs parse at boundary; ensure_mapping used consistently at Rust FFI responses |
| 10 | Make illegal states unrepresentable | 2 | small | medium | DeltaMutationRequest allows both merge+delete to be set (caught at runtime); feature enable/disable via separate functions is clean |
| 11 | CQS | 2 | small | low | Most functions are either queries or commands; observability record_* functions are command-only |
| 12 | DI + explicit composition | 2 | small | low | DeltaService takes profile via constructor; delta_service_for_profile is explicit factory |
| 13 | Composition over inheritance | 3 | n/a | n/a | No inheritance hierarchies; DeltaFeatureOps composes via reference to DeltaService |
| 14 | Law of Demeter | 1 | medium | medium | provider_artifacts.py chains getattr 3+ levels deep; control_plane._compatibility_message uses getattr chains |
| 15 | Tell, don't ask | 2 | small | low | DeltaService encapsulates store resolution; some raw data exposure in provider_artifacts |
| 16 | Functional core, imperative shell | 2 | small | low | Protocol compatibility is pure; payload normalization is pure; IO concentrated in service/observability |
| 17 | Idempotency | 2 | small | low | Snapshot reads are idempotent; write operations use commit versioning; observability append is fail-open |
| 18 | Determinism | 3 | n/a | n/a | Sorted payloads, canonical URIs, fingerprint hashing all enforce reproducibility |
| 19 | KISS | 1 | medium | medium | control_plane feature enable/disable has 12 near-identical functions; could be data-driven |
| 20 | YAGNI | 2 | small | low | DeltaProviderBuildResult has 31 optional fields; some may be speculative but most are used |
| 21 | Least astonishment | 2 | small | low | Naming is consistent; delta_enable_*/delta_disable_* pairs are predictable; some disable functions raise unconditionally |
| 22 | Public contracts | 2 | small | low | __all__ exports are comprehensive; _v2 table names signal versioning; DeltaTableRef lacks formal versioning |
| 23 | Design for testability | 1 | medium | high | control_plane functions cannot be tested without Rust extension; observability requires full write pipeline |
| 24 | Observability | 2 | small | low | Comprehensive artifact recording; structured schemas; OTel tracing integration; bare except Exception at line 790 |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The `protocol.py`, `specs.py`, and `store_policy.py` modules effectively hide their internal decisions behind clean surfaces. However, `control_plane.py` leaks the Rust FFI calling convention into every public function.

**Findings:**
- `control_plane.py:716-720` -- The `gate_payload` tuple is destructured by positional index (`gate_payload[0]`, `gate_payload[1]`, `gate_payload[2]`, `gate_payload[3]`) in every Rust entrypoint call. This pattern repeats at lines 716-719, 796-799, 849-852, 893-895, 961-975, 1019-1028, 1119-1138, 1173-1203, 1229-1250, 1278-1299, 1326-1345, 1377-1394, 1425-1443, 1470-1487, 1511-1529, 1549-1559, 1579-1590. The positional tuple indexing exposes the Rust function signature layout to every caller.
- `control_plane.py:976-982` -- The `commit_payload_values` tuple is similarly destructured by positional index (`commit_payload_values[0]` through `[5]`), leaking the commit options wire format across all mutation functions.
- `control_plane.py:561-580` -- `_compatibility_message` accesses compatibility object internals via repeated `getattr` calls rather than requiring a typed interface.

**Suggested improvement:**
Extract a `_invoke_rust_entrypoint` helper that accepts the entrypoint name, a `DeltaTableRef`, optional gate, and optional commit options, and handles the positional argument marshalling internally. This confines the FFI calling convention to a single location.

**Effort:** medium
**Risk if unaddressed:** medium -- Every new Rust entrypoint or gate field change requires updating 15+ call sites.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`control_plane.py` conflates at least five distinct responsibilities in a single 2,297-line file: (1) request/response data structures, (2) Rust FFI marshalling, (3) CRUD operations (write/delete/update/merge), (4) metadata queries (snapshot/add_actions), (5) feature enable/disable management.

**Findings:**
- `control_plane.py:62-463` -- 17 request struct definitions (data modeling concern).
- `control_plane.py:466-658` -- Internal helpers for Rust extension resolution, context adaptation, schema IPC, and scan-config normalization (FFI marshalling concern).
- `control_plane.py:661-814` -- Provider construction functions (scan planning concern).
- `control_plane.py:866-1205` -- DML operations: snapshot, add_actions, write, delete, update, merge (CRUD concern).
- `control_plane.py:1207-2203` -- Optimize, vacuum, restore, properties, features, constraints, checkpoints, plus 12 feature enable/disable wrappers (maintenance + feature management concern).

**Suggested improvement:**
Split `control_plane.py` into at least three modules: (1) `control_plane_types.py` for request/response structs and `DeltaTableRef`, (2) `control_plane_ffi.py` for the Rust extension marshalling layer (`_invoke_rust_entrypoint`, `_internal_ctx`, `_require_internal_entrypoint`), and (3) keep `control_plane.py` as the public function surface that delegates to the FFI layer. Alternatively, group by operation category: `control_plane_scan.py`, `control_plane_dml.py`, `control_plane_features.py`.

**Effort:** large
**Risk if unaddressed:** high -- The file is already the 2nd largest in the module and conflating concerns makes it the first place every Delta-related change touches.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Multiple files change for multiple distinct reasons.

**Findings:**
- `control_plane.py` changes when: (a) Rust FFI signature changes, (b) new Delta operations are added, (c) request struct shapes evolve, (d) feature toggle management changes, (e) scan config normalization evolves. This is at least 5 distinct change vectors in a single file.
- `observability.py` changes when: (a) schema definitions for observability tables evolve (lines 954-1052), (b) the write pipeline API changes (lines 827-881), (c) telemetry integration changes (lines 184-192), (d) bootstrap/recovery logic changes (lines 761-803).
- `service.py` is better scoped but still mixes read operations, mutation operations, feature operations, and provider resolution in a single `DeltaService` class (lines 334-886).

**Suggested improvement:**
For `control_plane.py`, see P2 suggestion. For `service.py`, the existing `DeltaFeatureOps` extraction (line 132) demonstrates the right pattern -- continue this by extracting `DeltaReadOps` and `DeltaMutationOps` as focused collaborators. For `observability.py`, extract the four schema definitions into a `observability_schemas.py` module to decouple schema evolution from recording logic.

**Effort:** large
**Risk if unaddressed:** high -- The control plane is a bottleneck for all Delta-related changes, creating merge conflicts and cognitive load.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Several modules are well-cohesive: `protocol.py` handles only protocol compatibility, `store_policy.py` handles only storage option resolution, `specs.py` contains only serializable option types. The coupling between modules is reasonable via import dependencies.

**Findings:**
- `control_plane.py` has high fan-out, importing from 10+ modules including `capabilities`, `object_store`, `payload`, `protocol`, `specs`, `store_policy`, `errors`, `extensions.context_adaptation`, `generated.delta_types`, `schema_spec.contracts`, `serde_msgspec`, and `utils`. This high fan-out reflects the mixed concerns identified under P2/P3.
- `maintenance.py:13-29` imports from `control_plane`, `observability`, and `service` -- three different layers within the same package -- creating a multi-layer dependency within a sibling relationship.
- `contracts.py` has a circular-ish deferred import to `service` at line 259 (`from datafusion_engine.delta.service import delta_service_for_profile`).

**Suggested improvement:**
Reducing the scope of `control_plane.py` (per P2) would naturally reduce its fan-out. The deferred import in `contracts.py:259` could be eliminated by accepting a schema-resolution callable as a parameter to `delta_schema_identity_hash` instead of reaching into the service layer.

**Effort:** medium
**Risk if unaddressed:** medium -- High fan-out increases fragility to upstream changes.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency flow generally follows the correct direction: core types (`protocol.py`, `specs.py`) have minimal dependencies, while `service.py` and `maintenance.py` depend on concrete implementations. The `control_plane.py` acts as the adapter layer between Python and Rust.

**Findings:**
- `protocol.py` imports only from `datafusion_engine.errors`, `extensions.context_adaptation`, `generated.delta_types`, and `serde_msgspec` -- appropriately lean for a core types module.
- `specs.py` depends only on `serde_msgspec` -- excellent.
- `service.py:31-63` imports from `storage.deltalake.delta`, which is the concrete Delta implementation. This is correct for a service boundary but means the service layer is coupled to the specific storage backend.
- `schema_spec/contracts.py:37` imports `DeltaFeatureGate` from `datafusion_engine.delta.protocol`, creating a dependency from a core schema module to the delta engine -- a mild inward dependency violation.

**Suggested improvement:**
The `DeltaFeatureGate` type lives in `datafusion_engine.generated.delta_types`, which `schema_spec.contracts` already depends on transitively. The import at `schema_spec/contracts.py:37` could reference the generated types directly rather than going through `delta.protocol`, but this is a minor concern given the TYPE_CHECKING guard.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The module exhibits good adapter-like patterns: `store_policy.py` adapts storage configuration, `object_store.py` adapts cloud object stores to DataFusion registration, and `capabilities.py` probes and adapts the Rust extension module. However, there is no explicit port (Protocol/ABC) defining what the control plane expects from its Rust backend.

**Findings:**
- `control_plane.py:54-59` -- `InternalSessionContext` and `_DeltaCdfExtension` are defined as Protocol types, showing intent to abstract the Rust boundary. But `InternalSessionContext` has an empty body and `_DeltaCdfExtension` only covers CDF. The 15+ other entrypoints are resolved dynamically via `_require_internal_entrypoint`.
- `object_store.py:57-131` -- The cloud store builders (`_build_s3_store`, `_build_gcs_store`, `_build_azure_store`) are clean adapters with a registry-like dispatch in `_resolve_builder`.
- No explicit port exists for the observability recording target -- `observability.py` directly instantiates `WritePipeline` and `DataFusionIOAdapter`.

**Suggested improvement:**
Define a `DeltaControlPlanePort` protocol with typed methods for each operation category (scan, dml, maintenance, features). This would make the Rust FFI boundary explicit and enable test doubles without mocking module internals. For observability, accept a `RecordingPort` or similar to decouple from the write pipeline.

**Effort:** medium
**Risk if unaddressed:** medium -- Testing and alternative backend support remain difficult without explicit ports.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The most significant DRY violation in this scope is the repeated `table_ref` pattern across 17 request structs and the repeated gate/commit payload destructuring.

**Findings:**
- `control_plane.py:62-463` -- All 17 request structs (`DeltaSnapshotRequest`, `DeltaProviderRequest`, `DeltaCdfRequest`, `DeltaWriteRequest`, `DeltaDeleteRequest`, `DeltaUpdateRequest`, `DeltaMergeRequest`, `DeltaOptimizeRequest`, `DeltaVacuumRequest`, `DeltaRestoreRequest`, `DeltaSetPropertiesRequest`, `DeltaAddFeaturesRequest`, `DeltaFeatureEnableRequest`, `DeltaAddConstraintsRequest`, `DeltaDropConstraintsRequest`, `DeltaCheckpointRequest`) duplicate the same four fields (`table_uri`, `storage_options`, `version`, `timestamp`) and the same `table_ref` property implementation. This is semantic duplication -- they all encode the same "table identity" knowledge.
- `control_plane.py` -- The `gate_payload[0..3]` and `commit_payload_values[0..5]` destructuring pattern is repeated in every Rust entrypoint call, approximately 15 times each. This is knowledge about the Rust FFI wire format encoded in 15+ locations.
- `control_plane.py:1606-2201` -- The 12 `delta_enable_*` and `delta_disable_*` functions follow an identical pattern: call `delta_set_properties` with a property dict and/or `delta_add_features` with a feature list. This is a data-driven pattern encoded as 12 separate functions.
- `observability.py:954-1052` -- Four schema-building functions share a common prefix pattern (event_time, run_id, dataset_name, table_uri, trace_id, span_id) that is repeated in each schema definition.

**Suggested improvement:**
(1) Extract a `DeltaTableIdentity` base struct with the four common fields and the `table_ref` property; have request structs compose it via a `table` field or inherit from it. (2) Create a `_call_rust_entrypoint(name, table_ref, gate, commit_options, *extra_args)` helper. (3) Replace the 12 feature enable/disable functions with a data-driven `delta_toggle_feature(ctx, request, feature_name, properties, features)` function backed by a feature registry dict. (4) Extract a `_common_observability_prefix()` helper for the shared schema fields.

**Effort:** medium
**Risk if unaddressed:** high -- Each new Delta operation or feature toggle requires copying the same boilerplate into yet another function, and the Rust FFI coupling is multiplied across every call site.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Request structs provide structural contracts via frozen dataclasses and msgspec types. Some explicit precondition validation exists (e.g., `DeltaMutationRequest.validate()`, `delta_set_properties` checking for empty properties).

**Findings:**
- `service.py:81-92` -- `DeltaMutationRequest.validate()` explicitly checks mutual exclusivity, though this is a runtime check rather than a structural guarantee (see P10).
- `control_plane.py:1109-1111` -- `delta_update` validates non-empty updates before proceeding.
- `control_plane.py:1368-1370` -- `delta_set_properties` validates non-empty properties.
- `provider_artifacts.py:73-106` -- `DeltaProviderBuildRequest` has 31 optional fields with no structural enforcement of which combinations are valid. The `compatibility: object` field at line 88 is untyped.
- `protocol.py:195-239` -- `validate_delta_gate` has explicit preconditions on snapshot presence and extension availability.

**Suggested improvement:**
Type the `compatibility` field in `DeltaProviderBuildRequest` as `DeltaExtensionCompatibility | None` rather than `object`. Consider adding `__post_init__` validation to `DeltaWriteRequest` beyond just normalizing `data_ipc`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The module generally follows parse-don't-validate at boundaries: request structs parse inputs into frozen typed representations, `ensure_mapping` converts Rust responses into validated mappings, and `_protocol_snapshot` at `protocol.py:263-286` performs a clean parse-or-None conversion.

**Findings:**
- `protocol.py:263-286` -- `_protocol_snapshot` cleanly parses `Mapping | DeltaProtocolSnapshot | None` into a validated `DeltaProtocolSnapshot | None`, returning None for invalid inputs. This is a textbook parse-don't-validate implementation.
- `control_plane.py:721-723` -- Rust response payloads are immediately parsed via `ensure_mapping` at every call site. Consistent pattern.
- `observability.py:805-824` -- `_bootstrap_observability_row` generates default values based on schema type inspection rather than validating inputs.

**Suggested improvement:**
No major changes needed. The existing patterns are sound.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most types use frozen structs effectively. The feature enable/disable functions prevent impossible operations at the API level (e.g., `delta_disable_column_mapping` always raises).

**Findings:**
- `service.py:74-92` -- `DeltaMutationRequest` allows both `merge` and `delete` to be set simultaneously, requiring a runtime `validate()` call. A tagged union (e.g., separate types or a discriminated field) would prevent this by construction.
- `control_plane.py:2204-2231` -- `delta_disable_column_mapping`, `delta_disable_generated_columns`, `delta_disable_invariants`, `delta_disable_check_constraints`, `delta_disable_v2_checkpoints` accept `*_args` and `**_kwargs` and always raise. The existence of these functions as valid callables that always error is a mild representability issue -- callers can call them and only discover the failure at runtime.
- `store_policy.py:17-24` -- `DeltaStorePolicy` correctly constrains `require_local_paths: bool` to prevent ambiguity.
- `protocol.py:16` -- `NonNegInt = Annotated[int, msgspec.Meta(ge=0)]` constrains protocol versions to non-negative at the type level -- excellent.

**Suggested improvement:**
Replace `DeltaMutationRequest` with a union type: `DeltaMutation = DeltaMergeArrowRequest | DeltaDeleteWhereRequest`. For the always-raising disable functions, consider removing them from `__all__` and raising `NotImplementedError` with a clear message, or modeling them as absent from a feature toggle registry.

**Effort:** small
**Risk if unaddressed:** medium -- The merge+delete simultaneous-set case is a latent bug waiting for someone to skip calling `validate()`.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS well. Query functions (`delta_snapshot_info`, `delta_add_actions`, `delta_protocol_compatibility`) return information without side effects. Command functions (`delta_write_ipc`, `delta_delete`) mutate state and return reports.

**Findings:**
- `observability.py:194-250` -- `record_delta_snapshot` returns `int | None` (the Delta version) while also performing a side effect (writing to the observability table). This is a mild CQS violation, though the return value serves as a confirmation signal.
- `control_plane.py:661-735` -- `delta_provider_from_session` both registers an object store (side effect) and returns a provider bundle (query). This is inherent to the operation but worth noting.

**Suggested improvement:**
The CQS violations here are pragmatic -- the return values are confirmation tokens for the side effects they perform. No action needed unless the codebase moves toward stricter CQS enforcement.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`DeltaService` is composed via constructor injection of `DataFusionRuntimeProfile`. The `delta_service_for_profile` factory provides explicit construction.

**Findings:**
- `service.py:333-337` -- `DeltaService` takes `profile` as a constructor argument, enabling testing with alternative profiles.
- `service.py:888-900` -- `delta_service_for_profile` is an explicit factory with a sensible default (creating a new `DataFusionRuntimeProfile` when None is passed).
- `observability.py:829` -- `WritePipeline` is created inline inside `_append_observability_row` rather than injected. This makes the observability recording hard to test without a live write pipeline.
- `maintenance.py:427` -- `delta_service_for_profile(runtime_profile)` is called inside `run_delta_maintenance` rather than receiving a service instance. This creates a hidden dependency.

**Suggested improvement:**
Accept a `DeltaService` or `WritePipeline` as a parameter in `run_delta_maintenance` and `_append_observability_row` rather than constructing them internally. This enables testing without live infrastructure.

**Effort:** small
**Risk if unaddressed:** low -- Currently acceptable for production code but limits testability.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The module uses no class inheritance hierarchies. All behavior is built through composition: `DeltaFeatureOps` holds a reference to `DeltaService`; `DeltaService` holds a `DataFusionRuntimeProfile`; request structs are flat data carriers.

**Findings:**
- No inheritance relationships found in the module.
- `service.py:132-135` -- `DeltaFeatureOps` composes via `service: DeltaService` field.

**Suggested improvement:**
None needed. This principle is well-satisfied.

**Effort:** n/a
**Risk if unaddressed:** n/a

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Several modules reach deep into collaborator structures via `getattr` chains and attribute traversal.

**Findings:**
- `provider_artifacts.py:143-176` -- `provider_build_request_from_registration_context` chains `_read_attr` (which is `getattr`) calls to traverse `context.add_actions`, `context.delta_scan`, `context.registration_path`, etc., and then further into nested attributes. Lines 143-176 show 15+ attribute accesses on an untyped `context: object`.
- `provider_artifacts.py:189-238` -- `provider_build_request_from_service_context` similarly chains `_read_attr` through `request -> location -> path`, `request -> resolution -> delta_snapshot -> ffi_table_provider`, reaching 3-4 levels deep.
- `control_plane.py:561-580` -- `_compatibility_message` accesses `compatibility.probe_result`, `compatibility.module`, `compatibility.ctx_kind`, `compatibility.error` via `getattr` on an untyped `object`.
- `service.py:448-449` -- `_provider_artifact_payload` traverses `request.resolution.delta_snapshot` via `_read_attr` chains.
- `maintenance.py:547-555` -- `_has_maintenance` accesses `policy.optimize_file_threshold`, `policy.total_file_threshold`, etc. via `getattr` because the `DeltaMaintenancePolicy` type apparently lacks guaranteed attributes.

**Suggested improvement:**
(1) Type the `context` parameter in `provider_build_request_from_registration_context` as a Protocol with the expected attributes, eliminating the need for `getattr`. (2) Type `compatibility` in `_compatibility_message` as `DeltaExtensionCompatibility` instead of `object`. (3) For `_has_maintenance`, if `DeltaMaintenancePolicy` is a known type, use direct attribute access; if it varies, define a protocol.

**Effort:** medium
**Risk if unaddressed:** medium -- Untyped attribute chains are fragile and silently return None on typos or structural changes.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`DeltaService` properly encapsulates store policy resolution behind method calls rather than exposing raw policy data. `DeltaProviderContract.to_request()` is a good "tell" pattern.

**Findings:**
- `contracts.py:41-69` -- `DeltaProviderContract.to_request()` encapsulates the conversion logic rather than exposing internal fields for external assembly.
- `service.py:384-421` -- `DeltaService.provider()` encapsulates the full provider resolution and artifact recording workflow.
- `provider_artifacts.py:62-70` -- `DeltaProviderBuildResult.as_payload()` encapsulates serialization logic.
- `maintenance.py:384-410` -- `maintenance_decision_artifact_payload` asks `decision.plan.policy` for individual flags rather than telling the plan to produce its own payload. This is a mild "ask" pattern.

**Suggested improvement:**
Add a `to_artifact_payload()` method to `DeltaMaintenancePlan` or `DeltaMaintenanceDecision` instead of the external `maintenance_decision_artifact_payload` function.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The module exhibits reasonable separation: pure functions for protocol compatibility evaluation, payload normalization, and fingerprint hashing exist alongside imperative functions for IO (writing, registration, Rust calls).

**Findings:**
- `protocol.py:69-133` -- `delta_protocol_compatibility` is a pure function: takes inputs, returns a structured result with no side effects. Excellent functional core.
- `payload.py` -- All functions are pure transformations (normalization, encoding). Clean functional core.
- `store_policy.py:117-169` -- `resolve_storage_profile` is mostly pure (URI normalization, option merging) but validates constraints and raises.
- `observability.py:827-881` -- `_append_observability_row` is deeply imperative: creates tables, writes data, handles retries, records failures. This is appropriate for a shell function but it is large and non-decomposed.

**Suggested improvement:**
The current separation is adequate. The observability `_append_observability_row` could be decomposed into a pure "prepare payload" step and an imperative "write and handle errors" step, but the current structure is not problematic.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Read operations are naturally idempotent. Write operations leverage Delta's versioned commits for idempotency (same version + same data = same outcome). The observability append is fail-open.

**Findings:**
- `observability.py:172-181` -- `_AppendObservabilityRequest` captures all inputs needed for a repeatable append.
- `control_plane.py:167-168` -- `DeltaCommitOptions` supports `app_transaction` with `app_id` and `version` for idempotent commit tracking.
- `observability.py:835` -- Append names include `time.time_ns()` for uniqueness, which breaks strict idempotency but prevents name collisions.

**Suggested improvement:**
No major changes needed. The time-based naming is a pragmatic choice for observability append operations where strict idempotency is less critical.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
The module demonstrates strong determinism patterns throughout.

**Findings:**
- `control_plane.py:1118` -- Update payloads are sorted: `sorted((str(key), str(value)) for key, value in request.updates.items())`.
- `control_plane.py:1375` -- Property payloads are sorted similarly.
- `store_policy.py:34-39` -- `fingerprint_payload` sorts options for deterministic hashing.
- `provider_artifacts.py:379-384` -- `storage_profile_fingerprint` sorts options before hashing.
- `protocol.py:104-105` -- Missing features are sorted for deterministic output.
- `scan_config.py:119` -- `hash_msgpack_canonical` used for scan identity hashing.

**Suggested improvement:**
None needed. Determinism is well-maintained throughout.

**Effort:** n/a
**Risk if unaddressed:** n/a

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
The 12 near-identical `delta_enable_*`/`delta_disable_*` functions in `control_plane.py` are the primary KISS violation. Each follows the same pattern but is manually written as a separate function.

**Findings:**
- `control_plane.py:1606-2231` -- 12 functions (`delta_enable_column_mapping`, `delta_enable_deletion_vectors`, `delta_enable_row_tracking`, `delta_enable_change_data_feed`, `delta_enable_generated_columns`, `delta_enable_invariants`, `delta_enable_check_constraints`, `delta_enable_in_commit_timestamps`, `delta_enable_v2_checkpoints`, `delta_enable_vacuum_protocol_check`, `delta_enable_checkpoint_protection`, plus their disable counterparts) total approximately 600 lines of nearly identical code. Each enable function calls `delta_set_properties` and/or `delta_add_features` with specific property/feature names.
- `observability.py:474-580` -- `_ensure_observability_table` is 106 lines with 3 levels of error handling, bootstrap logic, schema drift detection, registration with fallback, and artifact recording. The cyclomatic complexity is high.

**Suggested improvement:**
Replace the 12 feature enable/disable functions with a data-driven approach:
```python
_FEATURE_REGISTRY = {
    "column_mapping": FeatureSpec(properties={"delta.columnMapping.mode": "name", ...}, features=["columnMapping"]),
    "deletion_vectors": FeatureSpec(properties={"delta.enableDeletionVectors": "true"}, features=["deletionVectors"]),
    ...
}

def delta_enable_feature(ctx, *, request, feature_name, allow_protocol_versions_increase=True):
    spec = _FEATURE_REGISTRY[feature_name]
    ...
```
Keep the individual function names as thin wrappers for backward compatibility if needed, but the core logic becomes a single generic function. This eliminates approximately 500 lines.

**Effort:** medium
**Risk if unaddressed:** medium -- The boilerplate increases linearly with each new Delta feature.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions serve clear near-term purposes. The `DeltaProviderBuildResult` with 31 optional fields could be considered borderline speculative, but many fields are used by downstream diagnostics.

**Findings:**
- `provider_artifacts.py:20-61` -- `DeltaProviderBuildResult` has 31 fields, all optional. While comprehensive, every field appears to be populated by at least one of the two `provider_build_request_from_*` functions.
- `control_plane.py:380-398` -- `DeltaFeatureEnableRequest` is a lean request type used by all feature toggle functions. Appropriately scoped.
- `scan_config.py:18-24` -- `DeltaScanConfigIdentity` wraps two fields. Simple and justified by the hashing use case.

**Suggested improvement:**
No major changes. The 31-field result type could be split into sub-payloads (scan payload, pruning payload, compatibility payload) for better composability, but this is a low-priority improvement.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
API naming is consistent and predictable. The `delta_enable_*`/`delta_disable_*` pairs follow a clear convention. However, some disable functions always raise, which could surprise callers.

**Findings:**
- `control_plane.py:2204-2231` -- `delta_disable_column_mapping`, `delta_disable_generated_columns`, `delta_disable_invariants`, `delta_disable_check_constraints`, `delta_disable_v2_checkpoints` all accept `*_args, **_kwargs` and always raise. A caller discovering these functions in `__all__` or autocomplete would reasonably expect them to work. The always-raise behavior is documented in the docstring but surprising at the API surface level.
- `service.py:807-885` -- `DeltaService.mutate()` delegates to either `merge_arrow` or `delete_where` based on which field is set, which is a natural and unsurprising dispatch pattern.
- `observability.py:55-58` -- Table names use `_v2` suffixes for versioning, which is consistent with the project convention.

**Suggested improvement:**
For the always-raising disable functions, consider either (a) removing them from `__all__` and making them truly private, or (b) using a `delta_feature_disableable(feature_name) -> bool` query to let callers check before calling. The current approach of exposing functions that always fail is mildly surprising.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules have comprehensive `__all__` exports. Table names include `_v2` version suffixes. The `__init__.py` uses lazy loading with an explicit `_EXPORT_MAP` for the public surface.

**Findings:**
- `__init__.py:8-18` -- The lazy-loading `_EXPORT_MAP` with `__getattr__` explicitly declares what the public surface is. Clean pattern.
- `observability.py:55-58` -- `_v2` suffixed table names signal versioned contracts.
- `control_plane.py:2234-2297` -- Comprehensive `__all__` with 63 exports. The large export list itself is a signal that the module may be doing too much.
- No formal schema versioning mechanism exists for the request/response structs. Adding a new field to `DeltaProviderBundle` would be a silent contract change.

**Suggested improvement:**
Consider adding a `_VERSION` or version suffix to the key data contract structs (`DeltaProviderBundle`, `DeltaSnapshotRequest`, etc.) to signal when their shape changes. The `_v2` pattern used for observability table names could be extended.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The control plane functions are essentially untestable in isolation because they all require a live Rust extension module (`datafusion_ext`). The observability recording requires a live Delta write pipeline.

**Findings:**
- `control_plane.py:466-488` -- `_resolve_extension_module` dynamically loads a Rust extension. Every public function in `control_plane.py` calls this, meaning none can be tested without the Rust extension installed.
- `control_plane.py:502-545` -- `_internal_ctx` performs live compatibility probing against a DataFusion session. No seam for testing.
- `observability.py:827-881` -- `_append_observability_row` creates a `WritePipeline` and `datafusion_from_arrow` inline, requiring a live DataFusion session and filesystem access.
- `service.py:384-421` -- `DeltaService.provider()` calls `resolve_dataset_provider`, which chains through the full provider resolution stack. The `profile` injection helps, but the resolution stack is deeply coupled.
- `protocol.py:69-133` -- `delta_protocol_compatibility` is fully testable as a pure function. Good model for the rest of the module.
- `payload.py` -- All functions are pure and easily testable. Good model.

**Suggested improvement:**
(1) Extract the Rust FFI interaction into a `DeltaControlPlanePort` protocol with an implementation class that wraps the actual Rust calls and a `FakeDeltaControlPlane` for tests. (2) Accept a `WritePipeline` factory or recording sink as a parameter to the observability functions. (3) The pure functions in `payload.py` and `protocol.py` demonstrate the right pattern -- maximize pure logic, minimize IO coupling.

**Effort:** medium
**Risk if unaddressed:** high -- The control plane and observability modules are difficult to test in CI environments without Rust extensions and filesystem access.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The observability infrastructure is comprehensive and well-structured. Artifact schemas are explicitly defined with Arrow types. OTel trace/span integration is present. Multiple artifact types (snapshot, mutation, scan plan, maintenance, feature state) provide operational visibility.

**Findings:**
- `observability.py:184-192` -- `_trace_span_ids` integrates with OTel tracing for correlation.
- `observability.py:55-58` -- Four versioned observability tables with explicit schemas.
- `observability.py:790` -- `except Exception as exc:` is a bare broad exception catch in `_bootstrap_observability_table`. While this is a fail-open path for observability bootstrap (checking for corrupt delta logs), it catches everything including `KeyboardInterrupt`-derived exceptions.
- `observability.py:68-77` -- `_OBSERVABILITY_APPEND_ERRORS` defines an explicit tuple of catchable errors, which is good practice. But the list includes `KeyError`, `ImportError`, and `AttributeError`, which could mask programming errors.
- `observability.py:901-911` -- `_observability_commit_metadata` sanitizes metadata keys, filtering out `operation` to prevent conflicts. This is a good defensive pattern.

**Suggested improvement:**
Replace `except Exception` at line 790 with a more specific tuple of expected exceptions (e.g., `RuntimeError, ValueError, OSError, ImportError`) to avoid masking unexpected errors. Consider whether `AttributeError` and `KeyError` in `_OBSERVABILITY_APPEND_ERRORS` should really be caught -- these often indicate programming bugs rather than operational failures.

**Effort:** small
**Risk if unaddressed:** low -- The broad catch is in a fail-open path, but could mask real bugs during development.

---

## Cross-Cutting Themes

### Theme 1: control_plane.py is a monolith with compounding duplication

**Description:** The 2,297-line control_plane.py file is the root cause of violations in P2 (separation of concerns), P3 (SRP), P7 (DRY), P19 (KISS), and P23 (testability). Its 17 request structs share identical 4-field patterns, its 15+ Rust entrypoint calls repeat the same positional argument marshalling, and its 12 feature toggle functions are near-identical. Every new Delta operation or feature amplifies these issues linearly.

**Root cause:** The file grew organically as a single adapter between Python and Rust, accumulating all Delta-related responsibilities without internal decomposition.

**Affected principles:** P2, P3, P7, P14, P19, P23

**Suggested approach:** Decompose into (1) `control_plane_types.py` for request/response structs with a shared `DeltaTableIdentity`, (2) `control_plane_ffi.py` for Rust marshalling with a generic `_invoke_rust_entrypoint`, (3) `control_plane_features.py` for data-driven feature toggles. This addresses P2/P3/P7/P19 simultaneously and creates testable seams for P23.

### Theme 2: Untyped object boundaries reduce safety across the FFI layer

**Description:** Multiple functions accept `object` types and use `getattr` chains to extract fields. This pattern appears in `_compatibility_message`, `provider_build_request_from_registration_context`, `provider_build_request_from_service_context`, and `_delta_gate_values`. The lack of typing at these boundaries undermines P8 (design by contract), P14 (Law of Demeter), and increases the risk of silent failures.

**Root cause:** The Rust FFI returns dynamically-typed objects, and the adaptation layer propagated this dynamic typing upward rather than parsing into typed structures at the boundary.

**Affected principles:** P8, P9, P14

**Suggested approach:** Define Protocol types for the Rust return payloads and convert at the FFI boundary. The existing `ensure_mapping` pattern does this for dicts; extend it to structured types for compatibility objects and context results.

### Theme 3: Observability is comprehensive but tightly coupled

**Description:** The observability module provides excellent coverage with four versioned tables, artifact recording, OTel integration, and schema drift detection. However, it is tightly coupled to concrete IO infrastructure (`WritePipeline`, `DataFusionIOAdapter`, `deltalake.writer`), making it hard to test and creating a large blast radius for IO changes.

**Root cause:** Observability was implemented as a concrete module rather than a port-and-adapter pair. Recording directly writes to Delta tables rather than going through an injectable recording interface.

**Affected principles:** P6, P12, P23

**Suggested approach:** Extract a `ObservabilityRecorder` protocol that accepts structured artifact payloads, with the current Delta-based implementation as one adapter. This enables test fakes and alternative recording backends.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract `_invoke_rust_entrypoint` helper to eliminate 15+ repeated gate/commit payload destructurings in control_plane.py | small | Eliminates ~300 lines of positional-index coupling |
| 2 | P19 (KISS) | Replace 12 feature enable/disable functions with data-driven `_FEATURE_REGISTRY` + generic function | medium | Eliminates ~500 lines; future features are config, not code |
| 3 | P14 (LoD) | Type `compatibility: object` as `DeltaExtensionCompatibility | None` in `_compatibility_message` and `DeltaProviderBuildRequest` | small | Removes getattr chains; enables IDE support |
| 4 | P24 (Observability) | Replace `except Exception` at observability.py:790 with specific exception tuple | small | Prevents masking programming errors in fail-open path |
| 5 | P7 (DRY) | Extract `DeltaTableIdentity` base with the 4 common fields shared by 17 request structs | medium | Reduces ~200 lines of structural duplication |

## Recommended Action Sequence

1. **Extract `_invoke_rust_entrypoint` helper in control_plane.py** (P1, P7) -- Confine the Rust FFI positional calling convention to a single function. This is a prerequisite for further decomposition and immediately reduces the maintenance burden.

2. **Replace feature toggle functions with data-driven registry** (P7, P19) -- Convert the 12 `delta_enable_*`/`delta_disable_*` functions into a `_FEATURE_REGISTRY` dict with a generic `delta_toggle_feature` function. Keep the named functions as thin wrappers for backward compatibility. This removes ~500 lines.

3. **Type the untyped `object` boundaries** (P8, P14) -- Replace `compatibility: object` with `DeltaExtensionCompatibility | None` in `_compatibility_message`, `DeltaProviderBuildRequest`, and `_provider_artifact_payload`. Type the `context` parameter in provider_artifacts functions with a Protocol.

4. **Extract `DeltaTableIdentity` base type** (P7) -- Create a shared struct with `table_uri`, `storage_options`, `version`, `timestamp`, and `table_ref` property. Compose it into the 17 request structs.

5. **Decompose control_plane.py** (P2, P3) -- After steps 1-4 reduce the internal complexity, split into `control_plane_types.py`, `control_plane_ffi.py`, and optionally `control_plane_features.py`. This requires the prior steps to be clean.

6. **Fix broad exception catch in observability** (P24) -- Replace `except Exception` at `observability.py:790` with the specific tuple `(RuntimeError, TypeError, ValueError, OSError, ImportError)`.

7. **Extract observability recording port** (P6, P23) -- Define a `RecordingPort` protocol and inject it into the observability functions, enabling test doubles for the write pipeline.
