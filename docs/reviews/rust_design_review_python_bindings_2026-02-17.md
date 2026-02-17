# Design Review: Python FFI Bindings & Plugin Architecture

**Date:** 2026-02-17
**Scope:** `rust/codeanatomy_engine_py/src/`, `rust/datafusion_python/src/codeanatomy_ext/`, `rust/datafusion_python/src/{context,dataframe,functions,lib,errors,utils,store,config,catalog}.rs`, `rust/datafusion_python/src/common/`, `rust/datafusion_python/tests/`, `rust/df_plugin_codeanatomy/`, `rust/df_plugin_host/`, `rust/df_plugin_api/`, `rust/df_plugin_common/`, `rust/datafusion_ext_py/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 44 source files + 9 test files (~11.3K LOC production, ~271 LOC tests)

## Executive Summary

The prior review round (2026-02-16) identified `codeanatomy_ext.rs` (then 3,582 LOC, 80+ `#[pyfunction]`, 7+ change reasons) as the single most severe architectural concern, scoring 0/3 on P2, P3, and P19. The implementation plan (S1-S14, D1-D5) was marked fully Complete. This follow-up review finds that the decomposition **landed structurally** but **stopped at the facade layer**: `legacy.rs` remains a 2,368 LOC monolith with 39 `#[pyfunction]` definitions and 40 `extract_session_ctx()` calls. The "decomposed" submodules (`plugin_bridge.rs`, `udf_registration.rs`, `cache_tables.rs`, `session_utils.rs`) are 9-38 line thin wrappers that re-export functions from `legacy.rs` via `super::legacy::*`. Within the genuinely decomposed modules (`delta_mutations.rs`, `delta_maintenance.rs`), three helper functions (`runtime()`, `table_version_from_options()`, `parse_msgpack_payload()`) are triplicated. The `codeanatomy_engine_py` crate is well-structured with clean SRP separation. The plugin crates (`df_plugin_api`, `df_plugin_common`, `df_plugin_host`, `df_plugin_codeanatomy`) demonstrate strong boundary discipline. Overall: the engine crate and plugin architecture score well (avg ~2.5/3), but the `codeanatomy_ext` facade represents a significant incomplete decomposition that pulls down boundary, knowledge, and simplicity scores.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `PySessionContext.ctx` is `pub(crate)` with accessor -- good. `RegistrySnapshot` fields remain `pub` in `legacy.rs:989-1063` |
| 2 | Separation of concerns | 1 | large | high | `legacy.rs` mixes 7+ concerns: UDF registration, plugin loading, delta ops, session utils, schema evolution, cache tables, extraction session |
| 3 | SRP (one reason to change) | 1 | large | high | `legacy.rs` has 7+ change reasons despite S8 marking decomposition as Complete |
| 4 | High cohesion, low coupling | 2 | medium | medium | Decomposed delta submodules are cohesive; facade modules couple to `legacy.rs` |
| 5 | Dependency direction | 2 | small | low | Core engine crate has no PyO3 deps; `codeanatomy_engine_py` wraps cleanly |
| 6 | Ports & Adapters | 2 | medium | low | Plugin ABI defines clear port; PyO3 layer is adapter; `legacy.rs` blurs adapter boundaries |
| 7 | DRY (knowledge) | 1 | medium | medium | `runtime()` triplicated, `table_version_from_options()` triplicated, `parse_msgpack_payload()` duplicated |
| 8 | Design by contract | 2 | small | low | `TableVersion::from_options` enforces mutual exclusion; `parse_and_validate_spec` has good contract checks |
| 9 | Parse, don't validate | 2 | small | low | msgpack payloads parsed once at boundary; `parse_and_validate_spec` is exemplary |
| 10 | Make illegal states unrepresentable | 2 | small | low | `TableVersion` enum replaces `(Option<i64>, Option<String>)` -- landed well |
| 11 | CQS | 2 | small | low | `register_dataset_provider` both registers and returns payload (minor CQS concern) |
| 12 | DI + explicit composition | 2 | small | low | `SessionFactory` injected into materializer cleanly; `mod.rs` wires submodules explicitly |
| 13 | Composition over inheritance | 3 | -- | -- | No inheritance hierarchies in scope; composition throughout |
| 14 | Law of Demeter | 2 | small | low | `extract_session_ctx` eliminates prior `session_context_contract(ctx)?.ctx` chain; minor `.state().config_options()` chains remain |
| 15 | Tell, don't ask | 2 | small | low | `RegistrySnapshot` exposes raw fields for Python dict construction; could encapsulate |
| 16 | Functional core, imperative shell | 2 | small | low | `compiler.rs` delegates pure logic to `spec_helpers`; materializer is orchestration shell |
| 17 | Idempotency | 3 | -- | -- | All operations are either read-only or Delta ACID-committed; re-registration is guarded |
| 18 | Determinism / reproducibility | 2 | small | low | `now_unix_ms()` uses system time with fallback; `spec_hash` is deterministic |
| 19 | KISS | 1 | large | high | `legacy.rs` at 2,368 LOC with 39 `#[pyfunction]` is far from simple; facade submodules add indirection without reducing complexity |
| 20 | YAGNI | 2 | small | low | No speculative abstractions detected; `DeltaCdfOptions` pyclass is purpose-specific |
| 21 | Least astonishment | 2 | small | low | `install_codeanatomy_runtime` returns a payload (surprising for an install function) |
| 22 | Declare and version contracts | 2 | small | low | `SPEC_SCHEMA_VERSION`, `DELTA_SCAN_CONFIG_VERSION`, `contract_version: 3` are explicit; plugin ABI versioned |
| 23 | Design for testability | 2 | medium | medium | Plugin crate has good in-process tests; `legacy.rs` functions require Python GIL -- untestable in pure Rust |
| 24 | Observability | 2 | small | low | Decomposed delta modules have `#[instrument]`; `legacy.rs` functions lack tracing spans |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`PySessionContext.ctx` field is `pub(crate)` with a `ctx()` accessor at `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/context.rs:299,1111`. This is a significant improvement from the prior review. The `RegistrySnapshot` struct has its fields accessed directly in `legacy.rs:989-1063` for Python dict construction.

**Findings:**
- PRIOR FIX CONFIRMED: `session_context_contract(ctx)?.ctx` pattern (56 occurrences) is fully eliminated. `extract_session_ctx()` at `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/helpers.rs:9` provides the clean accessor.
- MINOR: `RegistrySnapshot` fields (`scalar`, `aggregate`, `window`, `table`, `aliases`, etc.) are accessed directly at `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:989-1063` in `registry_snapshot_py()`. The S12 plan called for encapsulated accessors.
- `CompiledPlan` in `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/compiler.rs:412-415` correctly hides `spec_json` and `spec_hash` behind `#[pymethods]` accessors.

**Suggested improvement:**
Use the `RegistrySnapshot` accessor methods from `registry/snapshot_types.rs` (which S12 created) rather than direct field access in `registry_snapshot_py()`. This is a small follow-through from S12 completion.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`legacy.rs` at 2,368 LOC is the renamed remnant of the prior 3,582 LOC `codeanatomy_ext.rs`. While 1,214 LOC was genuinely extracted into `delta_mutations.rs` (250 LOC), `delta_maintenance.rs` (409 LOC), and `delta_provider.rs` (351 LOC), the remaining 2,368 LOC still mixes at least 7 distinct concerns:

**Findings:**
- NEW: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs` contains:
  1. UDF registration and config (`register_codeanatomy_udfs` line 1073, `install_codeanatomy_runtime` line 1123, `install_codeanatomy_udf_config` line 514)
  2. Plugin loading and validation (`load_df_plugin` line 1215, `extract_plugin_handle` line 434, `ensure_plugin_manifest_compat` line 455)
  3. Registry snapshot assembly (`registry_snapshot_py` line 986, `capabilities_snapshot` line 641, `registry_snapshot_hash` line 343)
  4. Session utilities (`build_extraction_session` line 762, `session_context_contract_probe` line 1092)
  5. Schema evolution (`SchemaEvolutionAdapterFactory` line 1438, `CpgTableProvider` line 1452)
  6. Cache table registration (via re-export from `register_cache_tables`)
  7. Delta scan config and observability helpers (`scan_config_payload_from_ctx` line 178, `inject_delta_scan_defaults` line 199)
  8. Function factory derivation (`derive_function_factory_policy` line 572)
  9. Arrow stream conversion (`arrow_stream_to_batches` line 864)
  10. Substrait replay (`replay_substrait_plan` line 718)

- INCOMPLETE DECOMPOSITION: The S8 plan specified 7 focused submodules. Only 3 (delta_mutations, delta_maintenance, delta_provider) contain real logic. The other 4 (`plugin_bridge.rs`, `udf_registration.rs`, `cache_tables.rs`, `session_utils.rs`) are thin wrappers that simply re-export `super::legacy::*` functions:
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs:6-12` -- 8 `super::legacy::*` re-exports
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs:5-19` -- 13 `super::legacy::*` re-exports
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/cache_tables.rs:5-8` -- 2 `super::legacy::*` re-exports
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/session_utils.rs:5-23` -- 15 `super::legacy::*` re-exports

**Suggested improvement:**
Complete the S8 decomposition by moving the actual `#[pyfunction]` implementations out of `legacy.rs` into their respective submodules. The facade re-export pattern provides the module structure but no actual separation. Target: `legacy.rs` should contain zero `#[pyfunction]` definitions, serving only as a compatibility shim if needed, or be deleted entirely.

**Effort:** large (estimated 4-6 hours to move ~39 functions into 7 submodules with imports)
**Risk if unaddressed:** high (any change to the delta bridge, UDF registration, plugin loading, or session utilities requires understanding 2,368 LOC)

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Same root cause as P2. `legacy.rs` changes for at least 7 distinct reasons.

**Findings:**
- REGRESSION: The prior review scored this 0/3 for `codeanatomy_ext.rs` at 3,582 LOC. The current state scores 1/3 because 1,214 LOC of delta logic genuinely moved out, but `legacy.rs` at 2,368 LOC still has 7+ change reasons.
- CONTRAST: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/compiler.rs` (462 LOC, single concern: spec compilation) and `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/materializer.rs` (578 LOC, single concern: execution orchestration) demonstrate good SRP.
- `/Users/paulheyse/CodeAnatomy/rust/df_plugin_codeanatomy/src/lib.rs` (694 LOC) has 2 change reasons (UDF bundle construction + table provider construction) but these are tightly coupled via the plugin ABI contract, making this acceptable.

**Suggested improvement:**
Same action as P2: complete the physical decomposition of `legacy.rs`.

**Effort:** large
**Risk if unaddressed:** high

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The genuinely decomposed modules demonstrate good cohesion. `delta_mutations.rs` handles only write/delete/update/merge. `delta_maintenance.rs` handles only vacuum/optimize/restore/checkpoint. `delta_provider.rs` handles only provider construction and scanning.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/` crate is highly cohesive with clear module boundaries: `compiler.rs` (compile), `materializer.rs` (execute), `session.rs` (factory), `result.rs` (output), `errors.rs` (error types), `schema.rs` (schema runtime).
- COUPLING CONCERN: The facade modules (`plugin_bridge.rs`, `udf_registration.rs`, etc.) are tightly coupled to `legacy.rs` internals via `pub(crate)` visibility. If `legacy.rs` is eventually decomposed, all these facades break.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/df_plugin_common/src/lib.rs` (23 LOC) is minimal and focused -- exactly the shared knowledge needed by multiple crates.

**Suggested improvement:**
When completing the decomposition, ensure each module owns its `#[pyfunction]` definitions and `register_functions()` directly, eliminating the coupling to `legacy.rs`.

**Effort:** medium (included in the P2 decomposition effort)
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency graph is well-structured. `codeanatomy_engine` (pure Rust core) has no PyO3 dependencies. `codeanatomy_engine_py` depends on `codeanatomy_engine` and adds PyO3 wrappers. `datafusion_ext` provides DataFusion extensions without PyO3. `datafusion_python` adds the Python bindings.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/compiler.rs:8-17` imports from `codeanatomy_engine::compiler::*` and `codeanatomy_engine::spec::*` -- correct direction.
- GOOD: Plugin crate dependency chain: `df_plugin_api` (no deps) -> `df_plugin_common` (arrow only) -> `df_plugin_host` (+ `df_plugin_api`) -> `df_plugin_codeanatomy` (+ `datafusion_ext`). No circular deps.
- MINOR: `legacy.rs:101-104` imports from `codeanatomy_engine::compiler::lineage` and `codeanatomy_engine::session::extraction` -- these are thin adapter calls, appropriate for the binding layer.

**Suggested improvement:**
No structural changes needed. The dependency direction is sound.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The plugin architecture (`df_plugin_api`) defines a clear port via `DfPluginMod` with `extern "C"` functions. The host (`df_plugin_host`) loads and validates. The plugin (`df_plugin_codeanatomy`) implements. This is textbook Ports & Adapters.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/df_plugin_api/src/lib.rs:47-57` defines the `DfPluginMod` ABI contract with `manifest`, `exports`, `udf_bundle_with_options`, and `create_table_provider` as the port surface.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/df_plugin_host/src/registry_bridge.rs:39-68` implements the adapter that converts FFI types back to DataFusion native types.
- GAP: `legacy.rs` contains both adapter logic (PyO3 conversion) and business logic (delta scan config injection, registry snapshot hashing). These should be separated.

**Suggested improvement:**
During the P2/P3 decomposition, ensure business logic (like `inject_delta_scan_defaults`, `registry_snapshot_hash`, `derive_function_factory_policy`) lives in domain-appropriate modules, not in the PyO3 adapter layer.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The prior review identified DRY as the systemic weakest principle (avg 0.8/3). Cross-crate duplication was addressed by `df_plugin_common`. However, within the decomposed `codeanatomy_ext/` module, new duplication was introduced.

**Findings:**
- NEW: `runtime()` (Tokio runtime construction) is defined 3 times:
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:223`
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:92`
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs:132`

- NEW: `table_version_from_options()` (TableVersion parsing with PyResult error mapping) is defined 3 times:
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:149`
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:97`
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs:137`

- NEW: `parse_msgpack_payload()` (msgpack deserialization with PyResult error mapping) is defined 2 times:
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:84`
  - `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs:124`

- PRIOR FIX CONFIRMED: `parse_major()` is now single-authority at `/Users/paulheyse/CodeAnatomy/rust/df_plugin_common/src/lib.rs:8`. `schema_from_ipc()` at line 17. `DELTA_SCAN_CONFIG_VERSION` at line 22. These S5 fixes are solid.

- PRIOR FIX CONFIRMED: `session_context_contract(ctx)?.ctx` chain (56 occurrences) is replaced by `extract_session_ctx()` at `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/helpers.rs:9`.

**Suggested improvement:**
Move `runtime()`, `table_version_from_options()`, and `parse_msgpack_payload()` to `helpers.rs` (which already exists and is imported by all submodules). This is a 15-minute mechanical refactor.

**Effort:** small
**Risk if unaddressed:** medium (if error message formatting diverges, debugging becomes confusing)

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Contract enforcement is generally good, especially in the engine crate.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/compiler.rs:326-405` -- `parse_and_validate_spec()` validates version, view definitions, output targets with field-path diagnostics via `serde_path_to_error`.
- GOOD: `TableVersion::from_options()` enforces mutual exclusion at the type level, referenced at `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:149-156`.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/df_plugin_host/src/loader.rs:27-76` -- `validate_manifest()` checks ABI major, ABI minor, struct_size, FFI major, DataFusion major, Arrow major.
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:210-217` validates merge request preconditions (at least one matched/not-matched clause). Good.

**Suggested improvement:**
No major gaps. The contract enforcement is appropriate for the binding layer.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Boundary parsing is consistent. msgpack payloads are deserialized once into typed structs at the entry point.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs:30-122` defines 8 typed payload structs that parse once from msgpack.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/compiler.rs:350-371` uses `serde_path_to_error::deserialize` for rich parse diagnostics.
- MINOR GAP: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:228-240` `parse_python_payload()` goes through Python `json.dumps` -> Rust `serde_json::from_str`. This double-serialization is an impedance mismatch, not a validation issue, but it means the "parse boundary" involves two languages.

**Suggested improvement:**
The `parse_python_payload` approach (Python dict -> JSON string -> Rust struct) is pragmatic for the PyO3 boundary. No change needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Good progress from the prior round.

**Findings:**
- PRIOR FIX CONFIRMED: `TableVersion` enum at `delta_protocol.rs` replaces `(Option<i64>, Option<String>)` pairs. Used consistently in `delta_mutations.rs`, `delta_maintenance.rs`, `delta_provider.rs`, and `legacy.rs`.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/df_plugin_api/src/manifest.rs:7-29` -- `DfPluginManifestV1` uses concrete types (no optionals for required fields).
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:271-296` -- `DatasetProviderRequestPayload` has `version: Option<i64>, timestamp: Option<String>` which could use `TableVersion` directly if msgpack format allowed it.

**Suggested improvement:**
Consider using `#[serde(flatten)]` with `TableVersion` in request payloads to eliminate the `version/timestamp` option pair at the serde level. Low priority since `table_version_from_options()` catches it at runtime.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions are cleanly query or command.

**Findings:**
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:799-861` -- `register_dataset_provider()` both mutates state (registers a table provider) AND returns a payload dict with snapshot/scan_config/add_actions. This is a CQS violation -- the registration is a command and the snapshot return is a query.
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:1123-1172` -- `install_codeanatomy_runtime()` both installs UDFs/factory/planners AND returns a snapshot payload.

**Suggested improvement:**
Split into separate `register_*()` and `snapshot_*()` functions on the Python side. The Rust functions can remain combined if the Python API layer separates them. Low priority since the Python callers typically need both.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
Good composition patterns throughout.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/materializer.rs:113-117` -- `execute()` takes injected `session_factory` and `compiled_plan`, no hidden construction.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/mod.rs:16-26` -- `init_module()` explicitly wires all submodules.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/lib.rs:86-90` -- `run_build()` explicitly constructs `SessionFactory`, `SemanticPlanCompiler`, `CompiledPlan`, and `CpgMaterializer` in sequence.

**Suggested improvement:**
No changes needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies in the scope. All behavior is composed via trait implementations, function delegation, and module composition.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:1479` -- `CpgTableProvider` implements `TableProvider` via delegation to `self.inner` (composition, not inheritance).
- Plugin architecture uses composition: `PluginHandle` wraps a `DfPluginMod_Ref` module reference.

**Effort:** --
**Risk if unaddressed:** --

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
The prior `session_context_contract(ctx)?.ctx` chain is eliminated. Some minor chains remain.

**Findings:**
- PRIOR FIX CONFIRMED: `session_context_contract(ctx)?.ctx` (56 occurrences) is gone. `extract_session_ctx()` provides direct access.
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:514-521` -- `install_codeanatomy_udf_config()` chains `extract_session_ctx(ctx)?.state_ref().write().config_mut()`. This is 4 levels deep.
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:937-940` -- `install_codeanatomy_policy_config()` has the same `state_ref().write().config_mut()` chain.
- These chains are DataFusion API patterns and unavoidable without wrapping the entire config surface.

**Suggested improvement:**
Consider extracting a `with_session_config()` helper that takes `ctx` + a closure, reducing the repeated chain. Low priority since this is DataFusion's standard API pattern.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Registry snapshot construction asks for raw data and assembles it externally.

**Findings:**
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:986-1063` -- `registry_snapshot_py()` extracts 15+ fields from `RegistrySnapshot` and manually builds a PyDict. This "ask for data, assemble externally" pattern could be replaced by `RegistrySnapshot::to_py_dict()` or similar.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/result.rs:117-121` -- `PyRunResult::from_run_result()` encapsulates the conversion.

**Suggested improvement:**
Add a `to_json()` or `to_pydict()` method on `RegistrySnapshot` that owns the serialization logic. This would remove ~80 LOC from `legacy.rs` and keep the snapshot contract self-contained.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The engine crate separates pure logic from IO well.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/compiler.rs:11-14` imports pure functions from `codeanatomy_engine::compiler::spec_helpers` (build_join_edges, build_transform, canonical_rulepack_profile). This was extracted per S10.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/materializer.rs:441-458` delegates core pipeline execution to `pipeline::execute_pipeline()`, keeping the PyO3 layer as orchestration shell.
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:343-362` -- `registry_snapshot_hash()` is a pure function that could live in the `registry_snapshot` module rather than the PyO3 layer.

**Suggested improvement:**
Move `registry_snapshot_hash()` to the `datafusion_ext::registry_snapshot` module where the `RegistrySnapshot` type lives.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All operations are naturally idempotent or protected by Delta's ACID semantics.

**Findings:**
- Registration operations check for existing state (e.g., `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:837-838` -- `deregister_table` before re-registration when `overwrite=true`).
- Delta operations are commit-based and inherently idempotent at the table version level.
- Plugin loading through `PyCapsule` is stateless from the Rust side.

**Effort:** --
**Risk if unaddressed:** --

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Good determinism infrastructure.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/compiler.rs:385` -- `spec_hash` computed via `hash_spec()` which is deterministic.
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:416-425` -- `now_unix_ms_or()` and `now_unix_ms()` are used for cache table event times. These introduce nondeterminism. The fallback value is `-1` which is appropriate for error cases.
- PRIOR FIX CONFIRMED: S3 fixed schema hashing divergence. S13 added golden test contract. S14 removed async UDF global state.

**Suggested improvement:**
The remaining `now_unix_ms()` usage is for cache table timestamps, which are inherently time-dependent. No action needed.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
The `codeanatomy_ext` facade pattern adds complexity without delivering simplicity.

**Findings:**
- NEW: The current architecture has a 3-layer indirection for 4 of 7 submodules:
  1. Python calls `delta_optimize_compact_request_payload` (registered by `delta_maintenance.rs`)
  2. `delta_maintenance.rs` owns the implementation directly -- GOOD

  But for plugin/UDF/cache/session functions:
  1. Python calls `install_function_factory` (registered by `udf_registration.rs`)
  2. `udf_registration.rs:6` re-exports `super::legacy::install_function_factory`
  3. `legacy.rs:525` contains the actual implementation

  This 3-layer indirection provides no value. It makes code harder to find and navigate.

- `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs` at 2,368 LOC exceeds the plan's own 2,000 LOC decomposition threshold (from the design principles: "files exceeding 2,000 LOC require decomposition").

- CONTRAST: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/lib.rs` at 202 LOC is a clean, readable entry point that any developer can understand in minutes.

**Suggested improvement:**
Either complete the decomposition (move implementations from `legacy.rs` to submodules) or collapse the facades back into `legacy.rs` and rename it to `mod.rs` to eliminate the indirection. The former is strongly preferred.

**Effort:** large
**Risk if unaddressed:** high (new contributors waste time navigating the facade indirection; change impact analysis requires reading 2,368 LOC)

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
No speculative abstractions detected.

**Findings:**
- GOOD: No unused generic parameters, no trait hierarchies without implementations, no abstract factory patterns.
- `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:1438-1449` -- `SchemaEvolutionAdapterFactory` delegates directly to `DefaultPhysicalExprAdapterFactory`. This is a seam for future schema evolution customization. Currently it is a pass-through, which is borderline YAGNI, but the adapter pattern is needed for the factory registration API.

**Suggested improvement:**
No changes needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
API naming is mostly clear.

**Findings:**
- MINOR: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:1123` -- `install_codeanatomy_runtime()` returns a payload dict. "install" conventionally implies void/command semantics; returning a payload is surprising. S2 addressed similar naming in the engine crate with `ensure_source_registered`.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/errors.rs:9-13` -- `engine_execution_error()` takes `stage`, `code`, `message`, `details` -- clear and self-documenting.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/session.rs:59-61` -- `SessionFactory::from_class()` is idiomatic.

**Suggested improvement:**
Rename `install_codeanatomy_runtime` to `initialize_codeanatomy_runtime` or split into `install_codeanatomy_runtime()` (void) + `codeanatomy_runtime_snapshot()` (returns payload).

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
Key contracts are versioned.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/lib.rs:170` -- `contract_version: 3` in `run_build` response.
- GOOD: `SPEC_SCHEMA_VERSION` at `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/compiler.rs:17`.
- GOOD: `DELTA_SCAN_CONFIG_VERSION` at `/Users/paulheyse/CodeAnatomy/rust/df_plugin_common/src/lib.rs:22`.
- GOOD: Plugin ABI versioning at `/Users/paulheyse/CodeAnatomy/rust/df_plugin_api/src/manifest.rs:4-5` -- `DF_PLUGIN_ABI_MAJOR: 1, DF_PLUGIN_ABI_MINOR: 1`.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:656-658` -- `runtime_install_contract.version: 3`.

**Suggested improvement:**
No changes needed. Versioning is explicit and consistent.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Test coverage is present but shallow for the facade layer.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/df_plugin_codeanatomy/src/lib.rs:510-694` -- 184 LOC of tests including `plugin_delta_scan_config_matches_control_plane` and `plugin_snapshot_matches_native`. These are meaningful parity tests.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/tests/codeanatomy_ext_decomposition_tests.rs` -- verifies module registration and hard-cutover of positional entrypoints.
- GAP: The 271 LOC across 9 test files in `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/tests/` are primarily smoke tests (function exists on module, GIL doesn't crash). No behavioral tests of the actual delta bridge functions.
- GAP: `legacy.rs` functions requiring `Python<'_>` context are inherently untestable in pure Rust. The S8 plan intended the decomposed modules to contain testable Rust logic, but since the logic remains in `legacy.rs`, this benefit was not realized.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/compiler.rs:326-405` -- `parse_and_validate_spec()` is a pure Rust function that could be unit-tested without PyO3. It takes `&str` and returns `PyResult<SemanticExecutionSpec>` which only needs Python for error construction.

**Suggested improvement:**
When completing the decomposition, extract pure Rust logic (e.g., `registry_snapshot_hash`, `inject_delta_scan_defaults`, `scan_config_payload_from_ctx`) into functions that take Rust types and return Rust types. PyO3 conversion should be a thin last-mile adapter. This enables comprehensive Rust-native unit testing.

**Effort:** medium
**Risk if unaddressed:** medium (regressions in the bridge layer are caught only by Python-side integration tests)

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Tracing was added per S11, but coverage is uneven.

**Findings:**
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_provider.rs:24,69,135,200,228,256,264,303` -- All 8 `#[pyfunction]` entries have `#[instrument]` with appropriate `skip` clauses.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:113,144,170,202` -- All 4 functions instrumented.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_maintenance.rs:143,175,203,231,261,291,322,353,376` -- All 9 functions instrumented.
- GAP: `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs` -- None of the 39 `#[pyfunction]` entries have `#[instrument]`. The S11 plan specified tracing for "key PyO3 entry points" but this was only applied to the genuinely decomposed modules.
- GOOD: `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/materializer.rs:112` -- `execute()` has `#[instrument]` (conditional on `tracing` feature).

**Suggested improvement:**
Add `#[instrument(skip(py, ctx, ...))]` to the high-traffic functions in `legacy.rs`: `install_codeanatomy_runtime`, `register_dataset_provider`, `load_df_plugin`, `register_df_plugin`. This can be done incrementally.

**Effort:** small
**Risk if unaddressed:** low (observability gap is in the adapter layer, not the core)

---

## Cross-Cutting Themes

### Theme 1: Incomplete Legacy Decomposition

**Root cause:** The S8 decomposition created the module structure (`codeanatomy_ext/mod.rs` + 9 submodules) but only moved delta operation logic into the submodules. The remaining ~39 functions stayed in `legacy.rs` with thin re-export facades.

**Affected principles:** P2 (0/3 -> 1/3), P3 (0/3 -> 1/3), P7 (new duplication), P19 (added indirection), P23 (testability not improved), P24 (tracing not applied).

**Suggested approach:** Complete the physical decomposition in a single focused effort. The module structure and registration wiring already exist; the work is mechanical code movement. Target: `legacy.rs` should be deleted or contain only shared utility functions (< 200 LOC).

### Theme 2: Helper Triplication in Decomposed Modules

**Root cause:** When `delta_mutations.rs` and `delta_maintenance.rs` were extracted from `legacy.rs`, they copied helper functions (`runtime()`, `table_version_from_options()`, `parse_msgpack_payload()`) rather than sharing them via `helpers.rs`.

**Affected principles:** P7 (DRY score 1/3).

**Suggested approach:** Move all three helpers to `helpers.rs` and import from there. `helpers.rs` already exists with `extract_session_ctx()` and is imported by all submodules. 15-minute mechanical change.

### Theme 3: Strong Plugin Architecture

The plugin crate architecture (`df_plugin_api`, `df_plugin_common`, `df_plugin_host`, `df_plugin_codeanatomy`) is exemplary. Clear ABI contracts, versioned manifests, capability-gated registration, shared knowledge in `df_plugin_common`, comprehensive parity tests in `df_plugin_codeanatomy`. This represents the target architecture quality for the rest of the scope.

### Theme 4: Clean Engine Crate Separation

`codeanatomy_engine_py` demonstrates exactly the right PyO3 binding pattern: thin wrappers (`compiler.rs`, `materializer.rs`, `session.rs`), each with a single concern, delegating to the core engine crate. Pure logic extracted to `spec_helpers`. JSON-in/JSON-out boundary at `lib.rs::run_build`. This crate is the model for how `codeanatomy_ext` should be restructured.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 | Move `runtime()`, `table_version_from_options()`, `parse_msgpack_payload()` to `helpers.rs` | small (15 min) | Eliminates 3x triplication |
| 2 | P24 | Add `#[instrument]` to high-traffic `legacy.rs` functions | small (30 min) | Closes observability gap |
| 3 | P15 | Add `to_json()` method on `RegistrySnapshot` | small (1 hr) | Removes ~80 LOC from `legacy.rs` |
| 4 | P16 | Move `registry_snapshot_hash()` to `datafusion_ext::registry_snapshot` | small (30 min) | Correct module ownership |
| 5 | P21 | Rename `install_codeanatomy_runtime` -> `initialize_codeanatomy_runtime` | small (30 min) | Clearer semantics |

## Recommended Action Sequence

1. **Consolidate helpers** (P7, small): Move `runtime()`, `table_version_from_options()`, `parse_msgpack_payload()` to `helpers.rs`. All 3 decomposed delta modules import from there.

2. **Add tracing to legacy.rs** (P24, small): Add `#[instrument]` to the ~10 highest-traffic functions in `legacy.rs`. This improves observability before the decomposition.

3. **Complete legacy.rs decomposition** (P2, P3, P19, large): Move the remaining ~39 `#[pyfunction]` implementations from `legacy.rs` into their respective submodules:
   - UDF registration functions -> `udf_registration.rs`
   - Plugin functions -> `plugin_bridge.rs`
   - Cache table functions -> `cache_tables.rs`
   - Session/extraction functions -> `session_utils.rs`
   - Schema evolution adapter -> new `schema_evolution.rs`
   - Delta scan config helpers -> `delta_provider.rs` or new `delta_scan_config.rs`
   - Function factory derivation -> `udf_registration.rs`

   Delete `legacy.rs` when complete. Each submodule should own its `register_functions()` directly.

4. **Extract pure Rust logic from PyO3 layer** (P23, medium): Move `registry_snapshot_hash()`, `inject_delta_scan_defaults()`, `derive_function_factory_policy()` pure logic to domain-appropriate Rust modules. Add Rust-native unit tests.

5. **Encapsulate RegistrySnapshot** (P1, P15, small): Use accessor methods from S12's `snapshot_types.rs` and/or add a `to_json()`/`to_pydict()` method.

## Python Integration Boundary Assessment

### Error Type Translation
- `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/errors.rs:1-41` -- `EngineExecutionError` is a custom Python exception with `stage`, `code`, `message`, `details` attributes. Well-structured.
- `legacy.rs` uses `PyRuntimeError` and `PyValueError` for bridge errors. No custom exception types for the delta bridge -- this is appropriate since these are adapter-layer errors.

### JSON/msgpack Contract Versioning
- `contract_version: 3` in `run_build` response.
- `SPEC_SCHEMA_VERSION` enforced in `parse_and_validate_spec`.
- `DELTA_SCAN_CONFIG_VERSION` enforced in plugin scan config deserialization.
- msgpack payloads for delta mutations/maintenance have no explicit version field -- version is implicit in the struct shape. This is acceptable for internal Rust-Python communication within the same build.

### GIL Deadlock Prevention in Async Paths
- `/Users/paulheyse/CodeAnatomy/rust/codeanatomy_engine_py/src/materializer.rs:120` -- `runtime.block_on(async { ... })` is called without `py.allow_threads()`. This is safe because:
  1. The materializer creates its own Tokio runtime (not shared with Python's event loop)
  2. No Python callbacks occur within the async block
  3. The GIL is held throughout (needed for PyResult construction)
- `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/delta_provider.rs:102-116` -- Similarly uses `runtime.block_on()` without `py.allow_threads()`. Same safety analysis applies.
- Risk: if any future async path calls back into Python, GIL deadlock would occur. The current architecture prevents this by keeping all async work in pure Rust.

### Arrow IPC Memory Safety
- `/Users/paulheyse/CodeAnatomy/rust/df_plugin_common/src/lib.rs:17-20` -- `schema_from_ipc()` uses Arrow's `StreamReader` which validates IPC format boundaries. Safe.
- `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:864-889` -- `arrow_stream_to_batches()` uses `ArrowArrayStreamReader::from_pyarrow_bound()` which handles C Data Interface FFI. Memory ownership follows Arrow C Data Interface protocol (producer owns until consumer releases).
- `/Users/paulheyse/CodeAnatomy/rust/datafusion_python/src/codeanatomy_ext/legacy.rs:450` -- `unsafe { capsule.reference() }` for plugin handle extraction. This is the standard PyCapsule FFI pattern. Safety depends on the capsule containing the correct type, enforced by name check at line 445.
