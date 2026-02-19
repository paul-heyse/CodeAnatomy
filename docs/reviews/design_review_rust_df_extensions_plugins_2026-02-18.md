# Design Review: Rust DataFusion Extension Crates

**Date:** 2026-02-18
**Scope:** `rust/datafusion_python/` AND `rust/datafusion_ext/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 189

---

## Executive Summary

The two crates form a well-structured PyO3 binding and DataFusion extension layer. The architecture is principled: `datafusion_ext` owns pure Rust domain logic (UDFs, planner rules, Delta operations, registry snapshots) while `datafusion_python` translates that logic into Python-callable surfaces using PyO3. The crate boundary is clean and the dependency direction is correct. The strongest areas are information hiding (PyCapsule FFI at every provider boundary), parse-don't-validate (msgpack/JSON deserialized once at the PyO3 entry point), and the UDF registry design (spec-driven, declarative, single-source). The most significant gaps are a recurring Tokio runtime construction anti-pattern (each blocking call creates a new `Runtime` rather than reusing a shared one), mild CQS violations in a handful of bridge functions that both register side-effects and return payloads, and the absence of structured observability in the `codeanatomy_ext` bridge surface.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | — | low | PyCapsule wrapping consistently conceals internal types |
| 2 | Separation of concerns | 2 | small | low | `session_utils.rs` mixes session creation, plan capture, Delta codec, and tracing setup |
| 3 | SRP | 2 | medium | medium | `udf_registration.rs` spans UDF install, policy config, physical config, docs, snapshot, and factory derivation |
| 4 | High cohesion, low coupling | 2 | medium | low | `codeanatomy_ext/helpers.rs` is a wide-surface grab-bag; duplicate `extract_session_ctx` in `session_utils.rs` |
| 5 | Dependency direction | 3 | — | low | `datafusion_ext` has no PyO3 dependency; correct inward direction |
| 6 | Ports & Adapters | 3 | — | low | PyO3 functions are explicit adapters; FFI capsules are the port |
| 7 | DRY | 2 | small | medium | `extract_session_ctx` duplicated between `helpers.rs` and `session_utils.rs`; `decode_schema_ipc` duplicated across two files |
| 8 | Design by contract | 2 | small | medium | Mutation bridge silently drops `extra_constraints` on delete/update; `capabilities_snapshot` builds a fresh session unconditionally |
| 9 | Parse, don't validate | 3 | — | low | msgpack deserialization at boundary into typed request structs; JSON through `parse_python_payload` |
| 10 | Illegal states | 2 | small | low | `DeltaWriteRequestPayload` has `version` AND `timestamp` with runtime conflict; `init_module` / `init_internal_module` overlap creates registration confusion |
| 11 | CQS | 2 | medium | medium | `install_codeanatomy_runtime` installs UDFs, factory, and planners AND returns a snapshot; `register_dataset_provider` registers a table AND returns metadata |
| 12 | Dependency inversion | 3 | — | low | `datafusion_ext` depends on DataFusion traits; concrete session wired only at PyO3 boundary |
| 13 | Composition over inheritance | 3 | — | low | No deep inheritance; `CpgTableProvider` wraps `dyn TableProvider` via composition |
| 14 | Law of Demeter | 2 | small | low | `session_utils.rs:706-728` reaches through `runtime_env.cache_manager.get_file_metadata_cache().list_entries()` three levels deep |
| 15 | Tell, don't ask | 2 | small | low | `registry_snapshot_py` manually iterates all fields of `RegistrySnapshot` instead of asking the snapshot to serialize itself |
| 16 | Functional core / imperative shell | 2 | medium | medium | `snapshot.rs` calls `std::panic::catch_unwind` inside a data-collection loop; per-call `Runtime::new()` in `helpers.rs:runtime()` |
| 17 | Idempotency | 2 | small | medium | `install_codeanatomy_udf_config` is idempotent (checks before insert); `install_policy_rules` and `install_physical_rules` are NOT — calling them twice registers duplicate analyzer/optimizer rules |
| 18 | Determinism | 3 | — | low | `RegistrySnapshot` uses `BTreeMap`/`BTreeSet` throughout; sorted before serialization |
| 19 | KISS | 2 | medium | medium | Per-call `Runtime::new()` in six separate bridge functions adds latency and ignores the shared runtime already available |
| 20 | YAGNI | 3 | — | low | No speculative abstraction layers; `TracingMarkerRule` is a deliberate identity-rule marker |
| 21 | Least astonishment | 2 | small | medium | `register_dataset_provider` registers a table as a side effect of what the name implies is read-only; `session_context_contract_probe` is a pure read but named with "probe" |
| 22 | Public contracts | 2 | small | medium | `RegistrySnapshot::CURRENT_VERSION = 1` exists but `capabilities_snapshot` embeds `runtime_install_contract.version: 3` as a magic literal; no changelog or deprecation surface |
| 23 | Testability | 2 | medium | medium | Bridge functions create their own `Runtime` objects making async behaviour hard to test; `global_task_ctx_provider` uses a process-wide OnceLock that cannot be reset between tests |
| 24 | Observability | 1 | medium | high | `codeanatomy_ext` bridge functions have no structured tracing beyond the `#[instrument]` on the four delta mutation entry points; no span coverage for UDF registration, schema evolution, plugin loading |

---

## Detailed Findings

### Category: Boundaries (P1–P6)

#### P1. Information Hiding — Alignment: 3/3

The crate boundary is well-guarded. Internal DataFusion types (`SessionContext`, `SessionState`, `LogicalPlan`) never cross the Python boundary as raw structs. Instead:

- `PySessionContext.ctx` field is `pub(crate)` (`context.rs:298`).
- Table providers are wrapped in `FFI_TableProvider` and passed as `PyCapsule` (`schema_evolution.rs:367-370`, `helpers.rs:141-149`).
- Plugin handles cross as typed capsules with name validation (`plugin_bridge.rs:24-93`).

Callers cannot accidentally depend on internal layout. The only minor exposure is `PySessionContext.config` in `context.rs:79` being `pub` (not `pub(crate)`), which is intentional for Python subclassing but makes the field available to Rust callers as well.

---

#### P2. Separation of Concerns — Alignment: 2/3

**Current state:**
`codeanatomy_ext/session_utils.rs` (794 lines) conflates six unrelated responsibilities:
1. Substrait replay (`replay_substrait_plan`, `lineage_from_substrait`, `extract_lineage_json`)
2. Extraction session building (`build_extraction_session`)
3. Dataset/Delta provider registration (`register_dataset_provider`)
4. Plan bundle capture (`capture_plan_bundle_runtime`, `build_plan_bundle_artifact_with_warnings`)
5. Session introspection/diagnostics (`session_context_contract_probe`)
6. Delta configuration installation (`install_delta_table_factory`, `install_delta_plan_codecs`, `delta_session_context`)
7. Arrow I/O utilities (`arrow_stream_to_batches`)
8. Tracing installation (`install_tracing`)

**Findings:**
- `session_utils.rs:127-177`: Substrait replay and lineage extraction share a file with Delta codec installation (`session_utils.rs:675-682`) — completely unrelated change drivers.
- `session_utils.rs:482-512`: `TracingMarkerRule` — a physical optimizer rule struct — is defined inside the session utilities module rather than in `planner_rules.rs` or `physical_rules.rs`.

**Suggested improvement:**
Split `session_utils.rs` into at least three focused files: `substrait_bridge.rs` (Substrait/lineage functions), `delta_session_bridge.rs` (Delta session construction, codec installation, Delta factory), and `plan_bundle_bridge.rs` (plan bundle capture and artifact building). `TracingMarkerRule` belongs in `physical_rules.rs`. `arrow_stream_to_batches` belongs in a utility shim.

**Effort:** small
**Risk if unaddressed:** low (purely navigational cost now; increases as file grows)

---

#### P3. SRP — Alignment: 2/3

**Current state:**
`codeanatomy_ext/udf_registration.rs` (572 lines) has six distinct reasons to change:
1. UDF installation policy (async/sync toggle)
2. Planner rule installation (policy, physical)
3. ExprPlanner installation
4. Registry snapshot serialization (msgpack and Python dict)
5. Function factory policy derivation and installation
6. UDF documentation snapshot

**Findings:**
- `udf_registration.rs:52-61`: `install_codeanatomy_udf_config` (config installation).
- `udf_registration.rs:113-180`: `derive_function_factory_policy` (JSON computation over a snapshot, purely data-transformation).
- `udf_registration.rs:447-485`: `udf_docs_snapshot` (documentation extraction).
- `udf_registration.rs:531-541`: `install_planner_rules` / `install_physical_rules`.

These are seven conceptually independent operations in one file.

**Suggested improvement:**
Extract `install_planner_rules`, `install_physical_rules`, `install_expr_planners`, `install_codeanatomy_policy_config`, and `install_codeanatomy_physical_config` into a `rules_bridge.rs`. Extract `udf_docs_snapshot` into a `docs_bridge.rs`. Extract `derive_function_factory_policy` into a `factory_policy.rs`. Leave `udf_registration.rs` for just UDF installation and snapshot serialization.

**Effort:** medium
**Risk if unaddressed:** medium (any of the six change drivers touching this file risks adjacent bugs)

---

#### P4. High Cohesion, Low Coupling — Alignment: 2/3

**Current state:**

**Finding 1 — Duplicate session extraction logic:**
`extract_session_ctx` is defined in `codeanatomy_ext/helpers.rs:35-42` and virtually the same logic (`session_context_contract`) is re-implemented in `codeanatomy_ext/session_utils.rs:64-80`. Both attempt `ctx.extract::<Bound<'_, PySessionContext>>()` first, then fall back to `ctx.getattr("ctx")`. The only difference is the error message.

**Finding 2 — Duplicate IPC decode:**
`decode_schema_ipc` exists in both `codeanatomy_ext/helpers.rs:104-107` and `codeanatomy_ext/schema_evolution.rs:38-41`. Both call `schema_from_ipc` and both format the same error message.

**Finding 3 — `helpers.rs` grab-bag width:**
`helpers.rs` exposes 12 distinct helper functions spanning msgpack parsing, JSON conversion, DeltaFeatureGate construction, TableVersion parsing, schema IPC decoding, Tokio runtime construction, task context provider, FFI capsule wrapping, and three report-to-dict adapters. These would be more cohesive split into a serialization helper, a Delta option helper, and a report formatter.

**Suggested improvement:**
Delete `session_context_contract` from `session_utils.rs` and use `extract_session_ctx` from `helpers.rs` directly. Delete the local `decode_schema_ipc` in `schema_evolution.rs` and call the one in `helpers.rs`. Consider splitting `helpers.rs` into `parse_helpers.rs` and `delta_payload_helpers.rs`.

**Effort:** medium
**Risk if unaddressed:** low (drift between duplicates is the main risk)

---

#### P5. Dependency Direction — Alignment: 3/3

The dependency graph is correct:
- `datafusion_ext` imports only DataFusion, Arrow, DeltaLake, and standard library. No `pyo3` dependency.
- `datafusion_python` depends on `datafusion_ext` for domain logic and adds `pyo3` at its own layer.
- `codeanatomy_engine` is consumed by `datafusion_python` as a computation dependency, keeping it free of binding concerns.

`rust/Cargo.toml` confirms `pyo3 = "0.26"` is a workspace dependency not referenced by `datafusion_ext/Cargo.toml`.

---

#### P6. Ports & Adapters — Alignment: 3/3

The PyO3 boundary acts as a disciplined adapter layer. The port concept is explicit:
- `#[pyfunction]` wrappers are adapter functions; they call into `datafusion_ext` or `codeanatomy_engine` native functions.
- PyCapsule names serve as typed port contracts (`"datafusion_table_provider"`, `"datafusion_ext.DfPluginHandle"`, `"datafusion_catalog_provider"`).
- `plugin_bridge.rs:31-73`: `ensure_plugin_manifest_compat` validates the ABI contract before allowing a plugin through the port — an excellent explicit guard.

---

### Category: Knowledge (P7–P11)

#### P7. DRY — Alignment: 2/3

**Current state:**

**Finding 1 — `extract_session_ctx` vs. `session_context_contract`:**
The session-extraction pattern is written twice:
- `helpers.rs:35-42` — `extract_session_ctx`
- `session_utils.rs:64-80` — `session_context_contract`
Both try `ctx.extract::<Bound<'_, PySessionContext>>()` then fall back to `ctx.getattr("ctx")`. They diverge only in error messaging.

**Finding 2 — `decode_schema_ipc`:**
- `helpers.rs:104-107`
- `schema_evolution.rs:38-41`
Identical bodies: call `schema_from_ipc`, map error to `PyValueError`.

**Finding 3 — Tokio runtime construction:**
`Runtime::new()` is called independently in `helpers.rs:130`, `session_utils.rs:141`, `session_utils.rs:162`, `session_utils.rs:237`, `session_utils.rs:304`, `session_utils.rs:381`, `session_utils.rs:516-518`, `session_utils.rs:527-529`. The `helpers::runtime()` function centralizes the call but is not always used (direct `Runtime::new()` appears in `table_logical_plan` and `table_dfschema_tree`).

**Suggested improvement:**
Canonicalize `extract_session_ctx` from `helpers.rs` as the single implementation; remove `session_context_contract`. Canonicalize `decode_schema_ipc` to `helpers.rs`. Replace all `Runtime::new()` call sites with `helpers::runtime()` or, better, the shared runtime from `datafusion_ext::async_runtime::shared_runtime` to eliminate per-call runtime construction entirely.

**Effort:** small
**Risk if unaddressed:** medium (the duplicates diverge under independent maintenance)

---

#### P8. Design by Contract — Alignment: 2/3

**Current state:**

**Finding 1 — Silent `extra_constraints` drop on delete/update:**
In `codeanatomy_ext/delta_mutations.rs`:
- `delta_delete_request` (line 183): `let _ = request.extra_constraints;` — the field is deserialized but silently discarded.
- `delta_update_request` (line 214): Same pattern.

The `DeltaDeleteRequestPayload` and `DeltaUpdateRequestPayload` structs advertise `extra_constraints: Option<Vec<String>>` as a contract field, but callers who pass constraints will receive no error and no enforcement. This is a silent contract violation.

**Finding 2 — `capabilities_snapshot` creates a side-effect-free session unconditionally:**
`udf_registration.rs:300-302` builds a new `SessionContext` and calls `register_all` every time `capabilities_snapshot` is called. No precondition prevents a caller from invoking this redundantly; the contract that this call is cheap is undocumented.

**Finding 3 — `rulepack_fingerprint` placeholder:**
`session_utils.rs:316, 391` passes `rulepack_fingerprint: [0_u8; 32]` — a zeroed array clearly intended to be filled — with no assertion or contract that callers will supply a real value. The same applies to `provider_identities: Vec::new()` and `planning_surface_hash: [0_u8; 32]`.

**Suggested improvement:**
For the `extra_constraints` drop: either remove the field from the payload structs for delete/update or implement the constraint check before calling the native function. Return an error if non-empty constraints are supplied but not supported. For the fingerprint fields: either accept them as parameters or document them as intentionally zeroed in a comment.

**Effort:** small
**Risk if unaddressed:** medium (callers may believe constraints are enforced on delete/update)

---

#### P9. Parse, Don't Validate — Alignment: 3/3

The boundary deserialization pattern is consistently applied. Every PyO3 entry point that accepts structured data deserializes into a typed Rust struct at the first line of the function:

- `delta_mutations.rs:109`: `parse_msgpack_payload::<DeltaWriteRequestPayload>(&request_msgpack, "delta_write_ipc_request")?`
- `session_utils.rs:184`: `parse_python_payload::<ExtractionSessionPayload>(py, config_payload, "extraction session config")?`
- `rust_pivot.rs:76-79`: `parse_python_payload::<CachePolicyPayload>(py, payload, "cache policy request")?`

After deserialization, all subsequent code operates on typed values — no stringly-typed field access downstream. The `save_mode_from_str` function in `delta_mutations.rs:92-100` is a good example of a small parse step that converts a raw string into a domain `SaveMode` enum before any further processing.

---

#### P10. Make Illegal States Unrepresentable — Alignment: 2/3

**Current state:**

**Finding 1 — Mutually exclusive version/timestamp in payload structs:**
`DeltaWriteRequestPayload` (and its siblings `DeltaDeleteRequestPayload`, `DeltaUpdateRequestPayload`, `DeltaMergeRequestPayload`) carry both `version: Option<i64>` and `timestamp: Option<String>`. The `table_version_from_options` function enforces that at most one is set at runtime (`helpers.rs:96-102`). This is a validation-after-the-fact pattern; an enum `TableVersion { Latest, AtVersion(i64), AtTimestamp(String) }` at the payload level would make the mutual exclusion structurally enforced. The `delta_protocol.rs::TableVersion` enum already exists for this purpose but is not used in the payload structs.

**Finding 2 — `init_module` / `init_internal_module` overlap:**
`codeanatomy_ext/mod.rs:23-51` defines two registration paths (`init_module` for the standalone `codeanatomy_ext` module and `init_internal_module` for the `_internal` PyO3 module). Both register nearly identical sets of functions, but `init_module` additionally calls `session_utils::register_functions` while `init_internal_module` calls `session_utils::register_internal_functions`. The difference between the two sets is not represented in a type; it is encoded by the existence of two parallel lists. Adding a new bridge function requires updating both lists, and it is easy to put a function in the wrong one.

**Suggested improvement:**
Replace `Option<i64>` + `Option<String>` with a `#[serde(flatten)]` enum in the payload types, using the existing `TableVersion` variant names as the discriminant. For `init_module` vs `init_internal_module`: encode the function set difference in a `RegistrationProfile` enum or a boolean flag on each function's metadata, so a single list drives both paths.

**Effort:** small
**Risk if unaddressed:** low (existing runtime check catches the version/timestamp conflict; the module registration risk is an omission bug)

---

#### P11. CQS — Alignment: 2/3

**Current state:**

**Finding 1 — `install_codeanatomy_runtime` violates CQS:**
`udf_registration.rs:386-444`: This function installs UDFs (command), installs the FunctionFactory (command), installs ExprPlanners (command), AND returns a rich snapshot dict (query). A caller cannot observe the runtime state without also mutating it.

**Finding 2 — `register_dataset_provider` violates CQS:**
`session_utils.rs:217-279`: Registers a table provider in the session (command) AND returns a metadata dictionary with snapshot, scan config, and add actions (query).

**Finding 3 — `capture_plan_bundle_runtime` violates CQS:**
`session_utils.rs:282-355`: Reads the DataFrame's physical plan (query) AND uses the session context for runtime capture (potentially mutating optimizer state). The result is a JSON payload that is partly observational and partly a side-effect report.

**Suggested improvement:**
The CQS violations here are pragmatic — crossing the PyO3 boundary is expensive, and bundling install + snapshot avoids a second call. The improvement is to name the functions to signal the combined nature (`install_codeanatomy_runtime_and_snapshot` or structured as `install_...` that returns a minimal acknowledgment, with a separate `runtime_snapshot` query). Alternatively, accept the violation and document it explicitly as a "convenience combined operation."

**Effort:** medium
**Risk if unaddressed:** medium (testing is harder; retry logic that "just reads" triggers installs)

---

### Category: Composition (P12–P15)

#### P12. Dependency Inversion + Explicit Composition — Alignment: 3/3

`datafusion_ext` depends exclusively on DataFusion traits (`FunctionRegistry`, `AnalyzerRule`, `PhysicalOptimizerRule`, `ExprPlanner`, `FunctionRewrite`) rather than on concrete implementations. The composition of rules into session state is done by explicit installer functions (`install_policy_rules`, `install_physical_rules`) which are called from the PyO3 boundary — not auto-wired.

The `ScalarUdfSpec` / `TableUdfSpec` / `AggregateUdfSpec` pattern in `macros.rs` is a particularly clean example: each UDF is a pure constructor function; registration is separate.

---

#### P13. Composition Over Inheritance — Alignment: 3/3

No deep class hierarchies exist. `CpgTableProvider` in `schema_evolution.rs:44-138` composes a `dyn TableProvider` and delegates all operations to it, overriding only the fields it enriches (`ddl`, `logical_plan`, `column_defaults`, `constraints`). This is textbook decorator/wrapper composition.

`CodeAnatomyPolicyRule` and `CodeAnatomyPhysicalRule` implement DataFusion traits directly rather than inheriting from intermediate base types.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**

**Finding — `session_context_contract_probe` chains:**
`session_utils.rs:706-728` reaches across three object layers:
```
runtime_env.cache_manager.get_file_metadata_cache().list_entries()
runtime_env.cache_manager.get_list_files_cache().map(|cache| cache.len())
runtime_env.cache_manager.get_file_statistic_cache().map(|cache| cache.len())
```
The function has intimate knowledge of `RuntimeEnv`'s internal cache manager structure. If `RuntimeEnv` restructures its cache fields, this function breaks.

**Finding — `record_scalar_udfs` in `registry/snapshot.rs:104-132`:**
Accesses `udf.inner().with_updated_config(config_options)` and then `udf_ref.inner().as_ref()` — two-level unwrapping through the UDF type hierarchy.

**Suggested improvement:**
For `session_context_contract_probe`: extract the cache stat collection into a helper that accepts `&RuntimeEnv` and returns a typed `CacheStats` struct, isolating the traversal. For the snapshot builder: `datafusion_ext` is deliberately low-level here; accept it as inherent to the DataFusion API surface.

**Effort:** small
**Risk if unaddressed:** low (breakage is local to the probe function; tests would catch it)

---

#### P15. Tell, Don't Ask — Alignment: 2/3

**Current state:**

**Finding — `registry_snapshot_py` manually walks `RegistrySnapshot` fields:**
`udf_registration.rs:183-261` extracts every field of `RegistrySnapshot` by calling its accessor methods one by one and builds a Python dict. `RegistrySnapshot` already has `#[derive(Serialize)]`; a cleaner design would be to have `RegistrySnapshot` produce a Python-compatible representation via `serde_json::to_value` and then call `json_to_py`, rather than the caller knowing every field by name.

**Finding — `snapshot_hook_capabilities` in `registry/snapshot.rs` asks about individual flags:**
`snapshot.rs:26-77` iterates the function lists and then queries `snapshot.simplify.get(name)`, `snapshot.coerce_types.get(name)`, etc. per entry. The `RegistrySnapshot` could expose a `capabilities_for(name: &str) -> FunctionHookCapabilities` method, telling the snapshot what to return rather than the caller asking for individual maps.

**Suggested improvement:**
Add `RegistrySnapshot::to_python_dict(py: Python<'_>) -> PyResult<Py<PyAny>>` that uses `serde_json` serialization and `json_to_py`. Add `RegistrySnapshot::capabilities_for(name: &str) -> Option<FunctionHookCapabilities>`. These small additions eliminate the large manual extraction in `registry_snapshot_py` and the repeated `.get(name)` calls.

**Effort:** small
**Risk if unaddressed:** low (fragility if fields are added to `RegistrySnapshot` without updating the manual extraction)

---

### Category: Correctness (P16–P18)

#### P16. Functional Core, Imperative Shell — Alignment: 2/3

**Current state:**

**Finding 1 — `catch_unwind` in `record_signature_details`:**
`registry/snapshot.rs:767`:
```rust
std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| return_type(&arg_types)))
    .ok()
    .and_then(|value| value)
    .unwrap_or(DataType::Null)
```
`catch_unwind` is used to silently swallow panics from DataFusion's `return_type` computation. This is imperative/error-suppression logic embedded in a data-collection function. It indicates the function's contract is fragile (DataFusion UDFs should not panic on valid args) and hides errors that could indicate bugs.

**Finding 2 — Per-call `Runtime::new()` creates short-lived imperative shells:**
`helpers.rs:129-132`:
```rust
pub(crate) fn runtime() -> PyResult<Runtime> {
    Runtime::new()
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}")))
}
```
This is called from `delta_mutations.rs:113`, `session_utils.rs:141`, `session_utils.rs:237`, and five other sites. Each call creates and destroys a new multi-threaded runtime with its own thread pool. The shared runtime in `datafusion_ext::async_runtime` exists precisely for this; using it would make the boundary between async computation and the synchronous PyO3 shell explicit and efficient.

**Suggested improvement:**
Replace `helpers::runtime()` with `datafusion_ext::async_runtime::shared_runtime()` across all call sites. For `catch_unwind`: remove it and let `return_type` errors propagate as `DataType::Null` explicitly via `return_type(&arg_types).ok().unwrap_or(DataType::Null)` — which already achieves the same effect without the panic-catching overhead.

**Effort:** medium
**Risk if unaddressed:** medium (per-call runtime creates ~40ms latency on each delta write/delete/merge call from Python; `catch_unwind` suppresses bugs in UDF return-type implementations)

---

#### P17. Idempotency — Alignment: 2/3

**Current state:**

**Finding 1 — `install_policy_rules` and `install_physical_rules` are not idempotent:**
`planner_rules.rs:100-105` adds `CodeAnatomyPolicyRule` to the session's analyzer rules without checking whether it is already present. `physical_rules.rs:106-115` similarly adds `CodeAnatomyPhysicalRule`. Called twice on the same session, these functions register duplicate rules that run twice per query, silently producing redundant validation passes.

In contrast, `install_codeanatomy_udf_config` (`udf_registration.rs:53-61`) explicitly checks `if config.get_extension::<CodeAnatomyUdfConfig>().is_none()` before inserting — a correct idempotency guard.

**Finding 2 — `global_task_ctx_provider` is idempotent but has a static session:**
`helpers.rs:134-138`:
```rust
static TASK_CTX_PROVIDER: OnceLock<Arc<SessionContext>> = OnceLock::new();
let provider = TASK_CTX_PROVIDER.get_or_init(|| Arc::new(get_global_ctx().clone()));
```
The static ensures the provider is initialized once, but the underlying session is the global context that may not be configured with the same UDFs as the per-call session. This is not an idempotency violation but is a correctness risk (FFI providers created with `global_task_ctx_provider` use a session that may lack custom UDFs).

**Suggested improvement:**
Add a guard to `install_policy_rules` analogous to the existing `ensure_policy_config` call: if `CodeAnatomyPolicyRule` is already in the analyzer rules, return early. The same for `install_physical_rules`. DataFusion's `SessionState` does not expose a stable `has_analyzer_rule` API; the guard would need to be an extension flag similar to the existing `CodeAnatomyPolicyConfig`.

**Effort:** small
**Risk if unaddressed:** medium (double-rule registration wastes query planning cycles and can produce confusing double-error messages)

---

#### P18. Determinism / Reproducibility — Alignment: 3/3

The registry snapshot design is explicitly deterministic:
- `RegistrySnapshot` uses `BTreeMap` and `BTreeSet` for all collections (`snapshot_types.rs:11-28`).
- `registry_snapshot` in `snapshot.rs:91-97` sorts all `Vec` fields after population.
- `registry_snapshot_hash` in `udf_registration.rs:25-44` iterates over sorted lists before hashing.
- The workspace-level version pins (`Cargo.toml:23-60`) lock DataFusion to `52.1.0` and Arrow to `57.1.0`, preventing non-determinism from upstream changes.

N/A for UDF computation determinism (DataFusion's responsibility).

---

### Category: Simplicity (P19–P22)

#### P19. KISS — Alignment: 2/3

**Current state:**

**Finding — Per-call Tokio runtime construction:**
The per-call `Runtime::new()` in `helpers.rs:129-132` and the two isolated calls in `session_utils.rs:516` and `session_utils.rs:527` are the largest unnecessary complexity. Creating a new multi-threaded runtime for each blocking call (six call sites in the delta bridge alone) initialises a thread pool, sets up task queues, and tears it all down after `.block_on()` completes. The `datafusion_ext::async_runtime::shared_runtime()` function already solves this at the crate level.

**Finding — `plugin_bridge.rs:144-156`: `udf_config_payload_from_ctx`:**
Manually constructs a `serde_json::json!` object with 8 fields by extracting each from `CodeAnatomyUdfConfig` individually. Since `CodeAnatomyUdfConfig` is `#[derive(Serialize)]`, a simpler `serde_json::to_value(&config)` would produce the same result with less code and fewer opportunities for field-omission bugs.

**Suggested improvement:**
Replace all `helpers::runtime()` and inline `Runtime::new()` call sites with `datafusion_ext::async_runtime::shared_runtime()?.block_on(...)`. Replace the manual JSON construction in `udf_config_payload_from_ctx` with `serde_json::to_value(&config)`.

**Effort:** medium
**Risk if unaddressed:** medium (latency on every mutation call; correctness risk if a UDF config field is added without updating the manual JSON construction)

---

#### P20. YAGNI — Alignment: 3/3

The codebase is lean. The `TracingMarkerRule` identity optimizer rule (`session_utils.rs:483-501`) looks speculative but is a deliberate marker for tracing instrumentation — it signals to tooling that tracing is active without adding behavior. No abstraction layers exist without a current use case. `AggregateUdfSpec` and `WindowUdfSpec` in `macros.rs:16-27` define spec types that are not yet used by any registered UDAFs/UDWFs (which register via direct `builtin_udafs()`/`builtin_udwfs()` functions), but this is a minor structural inconsistency rather than speculative generality.

---

#### P21. Least Astonishment — Alignment: 2/3

**Current state:**

**Finding 1 — `register_dataset_provider` registers as a side effect:**
`session_utils.rs:217`: The function name suggests it is about registration. But callers who only want to introspect a Delta table's metadata cannot call this without also mutating the session's table registry. The surprise is that reading a Delta table snapshot requires accepting an irreversible table registration.

**Finding 2 — `register_df_plugin` in `plugin_bridge.rs:292-334`:**
This function registers UDFs, registers table functions, installs Delta plan codecs, and registers table providers — four distinct operations with no way to control which subset is performed. Compare with `register_df_plugin_udfs`, `register_df_plugin_table_functions`, and `register_df_plugin_table_providers` which are individual. The omnibus `register_df_plugin` function astonishes by doing more than its name suggests.

**Finding 3 — `delta_delete_request` ignores `extra_constraints`:**
As noted under P8: `delta_mutations.rs:183`: `let _ = request.extra_constraints;`. A caller who passes `extra_constraints: ["col IS NOT NULL"]` to a delete operation receives no error and no enforcement. This astonishes anyone who reasonably expected constraint checking.

**Suggested improvement:**
For `register_dataset_provider`: split into `inspect_delta_provider` (returns metadata without registering) and `register_delta_provider` (registration-only, no return value) to separate the concerns. For `register_df_plugin`: document that it is an omnibus convenience and consider logging a warning listing what it installed. For `extra_constraints`: remove the field or implement it.

**Effort:** small
**Risk if unaddressed:** medium (incorrect behavior expectations around constraint enforcement; test suites may test for enforcement that never happens)

---

#### P22. Declare and Version Public Contracts — Alignment: 2/3

**Current state:**

**Finding 1 — Multiple version constants with unclear relationships:**
- `RegistrySnapshot::CURRENT_VERSION = 1` (`snapshot_types.rs:31`)
- `capabilities_snapshot` returns `runtime_install_contract.version: 3` (`udf_registration.rs:314`) as a literal embedded in JSON — not referenced from any named constant.
- `DELTA_SCAN_CONFIG_VERSION` from `df_plugin_common` is imported and embedded in scan config payloads.

The snapshot contract version (1) and the runtime install contract version (3) are independent but both unnamed in the call site. A reader cannot determine from the code which version number governs which contract.

**Finding 2 — No deprecation surface:**
`register_table_provider` in `context.rs:631-639` is marked deprecated by a comment ("Deprecated: use `register_table` instead") but carries no `#[deprecated]` attribute that would produce compiler warnings for callers.

**Finding 3 — `init_module` vs `init_internal_module` bifurcation is undocumented:**
The existence of two module init paths (`mod.rs:23-51`) is not documented. External callers of the Python extension cannot determine which registration profile they are on.

**Suggested improvement:**
Introduce `const RUNTIME_INSTALL_CONTRACT_VERSION: u32 = 3;` alongside `RegistrySnapshot::CURRENT_VERSION`. Add `#[deprecated(since = "...", note = "use register_table instead")]` to `register_table_provider`. Add a module-level doc comment to `mod.rs` explaining the two registration profiles and when each is used.

**Effort:** small
**Risk if unaddressed:** medium (version drift between producers and consumers of the snapshot protocol is hard to detect)

---

### Category: Quality (P23–P24)

#### P23. Design for Testability — Alignment: 2/3

**Current state:**

**Finding 1 — `global_task_ctx_provider` uses a process-global singleton:**
`helpers.rs:134-138`: The `OnceLock<Arc<SessionContext>>` cannot be reset between tests. Any test that relies on `FFI_TableProvider` objects created by `provider_capsule` or `schema_evolution.rs:365-370` will share the same empty global context across all test runs in the same process.

**Finding 2 — Per-call runtime construction prevents mock async injection:**
The `helpers::runtime()` function creates a fresh runtime on each call. Test code that wants to use a custom executor or inject a controlled async environment cannot do so — there is no way to substitute the runtime.

**Finding 3 — `record_signature_details` uses `catch_unwind`:**
`registry/snapshot.rs:767`: `std::panic::catch_unwind` in a data collection function makes the function's behavior invisible to test assertions. A UDF with a buggy `return_type` implementation will silently produce `DataType::Null` in the snapshot, making the snapshot test pass while masking the underlying bug.

**Finding 4 — `capabilities_snapshot` creates a fresh session for every call:**
`udf_registration.rs:300-302`: This fresh-session approach means capability tests run through the full UDF registration path. While good for integration coverage, it makes it impossible to inject a pre-populated session or a partial registry for unit testing individual capabilities.

**Suggested improvement:**
Extract `global_task_ctx_provider` into an injectable dependency (`TaskContextProvider` parameter) on the functions that use it, rather than reading from a static. Extract the async blocking entry point from bridge functions into a thin shim over a `block_on`-accepting function, so unit tests can pass a controlled executor. Remove `catch_unwind` from `record_signature_details` and let return-type errors produce `DataType::Null` via `.ok()` directly.

**Effort:** medium
**Risk if unaddressed:** medium (test isolation suffers; bugs in UDF return-type implementations are silently masked)

---

#### P24. Observability — Alignment: 1/3

**Current state:**

The `#[instrument]` attribute appears on exactly four functions — the four delta mutation bridge functions (`delta_mutations.rs:103`, `176`, `203`, `234`) — and on the two rule-application functions in `planner_rules.rs:70` and `physical_rules.rs:80`. All other bridge functions in `codeanatomy_ext/` have no tracing spans.

**Findings:**
- `udf_registration.rs:280-296`: `register_codeanatomy_udfs` — registers 30+ UDFs with no span; when this fails, there is no structured trace showing which UDF caused the error.
- `plugin_bridge.rs:159-165`: `load_df_plugin` — loads a native shared library with no span; library load failures produce only a `PyRuntimeError` with no correlation to other traces.
- `schema_evolution.rs:250-371`: `parquet_listing_table_provider` — a multi-step function (parse schema, build listing options, construct provider, wrap in CpgTableProvider) with no span boundaries.
- `session_utils.rs:180-213`: `build_extraction_session` — no span.
- `session_utils.rs:282-355`: `capture_plan_bundle_runtime` — a multi-step async plan capture with no structured trace.
- `registry/snapshot.rs:79-97`: `registry_snapshot` — called on every `install_codeanatomy_runtime` invocation with no span; performance is invisible.

All of the above functions only produce output when they fail (via `PyRuntimeError`), making performance and partial-failure diagnosis impossible from telemetry alone.

**Suggested improvement:**
Add `#[instrument(level = "info", skip_all, fields(...))]` to at minimum:
- `udf_registration.rs`: `register_codeanatomy_udfs` (field: `enable_async`), `install_codeanatomy_runtime` (fields: `enable_async_udfs`), `capabilities_snapshot`.
- `plugin_bridge.rs`: `load_df_plugin` (field: `path`), `register_df_plugin` (field: `plugin` path), `register_df_plugin_udfs`.
- `session_utils.rs`: `build_extraction_session`, `capture_plan_bundle_runtime`, `register_dataset_provider` (field: `table_name`, `table_uri`).
- `schema_evolution.rs`: `parquet_listing_table_provider` (fields: `table_name`, `path`).

These nine additions would give end-to-end traceability for the main setup and query paths.

**Effort:** medium
**Risk if unaddressed:** high (production issues in UDF registration, plugin loading, and plan capture are currently diagnosed exclusively by exception messages with no correlation data)

---

## Cross-Cutting Themes

### Theme 1: The Per-Call Tokio Runtime Anti-Pattern

`helpers::runtime()` creates a new multi-threaded Tokio runtime (`Runtime::new()`) for every blocking call from Python. This function is invoked from at least eight call sites across `delta_mutations.rs` and `session_utils.rs`. The shared runtime in `datafusion_ext::async_runtime::shared_runtime()` already exists and is used by `utils.rs:get_tokio_runtime()` for `wait_for_future` calls in `context.rs` and `dataframe.rs`. The two code paths use incompatible runtime strategies: PyO3 `#[pymethods]` on `PySessionContext` use the shared runtime; `codeanatomy_ext` bridge functions create throwaway runtimes.

**Root cause:** `codeanatomy_ext` bridge functions were written separately from the `PySessionContext` methods and adopted `Runtime::new()` as a quick pattern without inheriting the shared runtime infrastructure.

**Affected principles:** P16 (functional core/imperative shell), P17 (idempotency), P19 (KISS), P23 (testability).

**Suggested approach:** Replace `helpers::runtime()` with `datafusion_ext::async_runtime::shared_runtime()` and call `.block_on()` on it. Remove `helpers::runtime()` entirely. Update the two isolated `Runtime::new()` calls in `session_utils.rs:516` and `session_utils.rs:527`.

---

### Theme 2: Mixed-Responsibility Bridge Files

Both `session_utils.rs` (794 lines, 8 concerns) and `udf_registration.rs` (572 lines, 6 concerns) violate SRP by accumulating every bridge function that doesn't fit elsewhere. The `codeanatomy_ext` sub-module was grown incrementally, with each new feature added to the most convenient existing file.

**Root cause:** Absence of file-level responsibility constraints when adding new bridge functions.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P4 (cohesion).

**Suggested approach:** Establish a file-per-concern convention for new bridge files: `substrait_bridge.rs`, `plan_bundle_bridge.rs`, `delta_session_bridge.rs`, `rules_bridge.rs`, `docs_bridge.rs`. Gradually migrate existing functions.

---

### Theme 3: Observability Gap at the Bridge Layer

The `codeanatomy_ext` bridge is the primary operational entry point: it is where UDFs are installed, plugins loaded, Delta operations executed, and schemas evolved. These are the operations that fail in production. Yet only the four delta mutation functions carry tracing instrumentation. The nine remaining high-value entry points produce no structured observability data.

**Root cause:** Instrumentation was added to the delta mutations (which have the most obvious production failure modes) but not extended to the setup and query paths.

**Affected principles:** P24 (observability).

**Suggested approach:** Add `#[instrument]` to the nine entry points listed under P24. Add span events (`tracing::info!`) at key decision points inside `register_codeanatomy_udfs` and `install_codeanatomy_runtime` to record per-UDF registration outcomes.

---

### Theme 4: Silent `extra_constraints` Drop on Mutation Bridge

`delta_delete_request` and `delta_update_request` accept and silently discard `extra_constraints`. The payload structs advertise the field; callers cannot distinguish "constraints accepted" from "constraints ignored." This is the kind of contract ambiguity that causes data integrity bugs — a caller adding a NOT NULL guard to a delete operation will believe it is enforced.

**Root cause:** The `DeltaDeleteRequest` and `DeltaUpdateRequest` types in `datafusion_ext/delta_mutations.rs` do not include an `extra_constraints` field, so the Python-layer struct accepts the field but has nowhere to pass it.

**Affected principles:** P8 (design by contract), P21 (least astonishment).

**Suggested approach:** Either (a) add `extra_constraints` to the native `DeltaDeleteRequest` / `DeltaUpdateRequest` structs and implement enforcement, or (b) return a validation error when non-empty `extra_constraints` are passed to delete/update until enforcement is implemented.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P24 Observability | Add `#[instrument]` to 9 bridge entry points (UDF registration, plugin load, extraction session, plan bundle, schema evolution) | small | Immediate production debuggability |
| 2 | P8 Design by contract | Error on non-empty `extra_constraints` in `delta_delete_request` / `delta_update_request` | small | Prevents silent data integrity gap |
| 3 | P7 DRY / P4 Cohesion | Remove `session_context_contract` from `session_utils.rs`; use `extract_session_ctx` from `helpers.rs` throughout; delete duplicate `decode_schema_ipc` in `schema_evolution.rs` | small | Eliminates drift risk between duplicates |
| 4 | P22 Contracts | Add `const RUNTIME_INSTALL_CONTRACT_VERSION: u32 = 3;`; add `#[deprecated]` to `register_table_provider` | small | Explicit version tracking; compiler-enforced deprecation |
| 5 | P19 KISS / P16 Functional core | Replace `helpers::runtime()` and all inline `Runtime::new()` call sites with `datafusion_ext::async_runtime::shared_runtime()?.block_on(...)` | medium | Eliminates per-call thread-pool construction; ~40ms latency saved per delta mutation |

---

## Recommended Action Sequence

1. **(P8 / P21) Fix `extra_constraints` contract** — Add a validation error in `codeanatomy_ext/delta_mutations.rs:183` and `delta_mutations.rs:214` when `extra_constraints` is non-empty and the operation is delete or update. This prevents data integrity misuse without a large refactor.

2. **(P7 / P4) Eliminate duplicate helpers** — Consolidate `session_context_contract` into `extract_session_ctx` and `decode_schema_ipc` into the `helpers.rs` copy. This reduces the surface for drift and is a two-file edit.

3. **(P24) Add bridge tracing** — Add `#[instrument]` to `register_codeanatomy_udfs`, `install_codeanatomy_runtime`, `capabilities_snapshot`, `load_df_plugin`, `register_df_plugin`, `register_df_plugin_udfs`, `build_extraction_session`, `register_dataset_provider`, `capture_plan_bundle_runtime`, and `parquet_listing_table_provider`. This is additive and requires no interface changes.

4. **(P19 / P16) Unify runtime strategy** — Replace all `helpers::runtime()` and inline `Runtime::new()` call sites with `datafusion_ext::async_runtime::shared_runtime()?.block_on(...)`. Remove `helpers::runtime()`. This unifies the two async execution strategies used in the codebase and eliminates per-call thread-pool overhead.

5. **(P22) Add version constants** — Define `const RUNTIME_INSTALL_CONTRACT_VERSION: u32 = 3;` in `udf_registration.rs`. Add `#[deprecated]` to `register_table_provider`. Add a doc comment to `codeanatomy_ext/mod.rs` explaining the two registration profiles.

6. **(P17) Make `install_policy_rules` and `install_physical_rules` idempotent** — Add a config extension flag (`codeanatomy_policy.rules_installed: bool`) that is set to `true` on first install and checked before adding the analyzer rule, mirroring the existing `ensure_policy_config` pattern. Same for the physical rule.

7. **(P3 / P2) Split `session_utils.rs` and `udf_registration.rs`** — Move Substrait/lineage functions to `substrait_bridge.rs`, plan bundle functions to `plan_bundle_bridge.rs`, Delta session construction to `delta_session_bridge.rs`. Move planner/physical rule installers and policy config to `rules_bridge.rs`. Move `udf_docs_snapshot` to `docs_bridge.rs`. This is a structural improvement best done in a dedicated refactor sprint.

8. **(P11 / P21) Clarify `register_dataset_provider` semantics** — Either rename to `register_and_inspect_dataset_provider` or split into two functions. Document that the registration is always performed when calling this function.
