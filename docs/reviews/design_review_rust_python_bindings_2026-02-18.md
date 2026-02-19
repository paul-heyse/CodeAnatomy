# Design Review: Rust Plugin Crates and PyO3 Binding Crates

**Date:** 2026-02-18
**Scope:** `rust/df_plugin_api/`, `rust/df_plugin_host/`, `rust/df_plugin_common/`, `rust/df_plugin_codeanatomy/`, `rust/codeanatomy_engine_py/`, `rust/datafusion_ext_py/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 22 Rust source files, 6 Cargo.toml files

---

## Executive Summary

The plugin and binding layer is architecturally strong where it matters most. The `df_plugin_api` crate defines a correct, `abi_stable`-backed C ABI boundary, the host validates six dimensions of the manifest before trusting any plugin, and the PyO3 binding layer (`codeanatomy_engine_py`) keeps Python-specific concerns cleanly separated from the Rust engine. The most significant findings are: (1) a global `OnceLock<SessionContext>` in `task_context.rs` leaks application-level state into a dynamically-loaded shared library — a serious correctness and testability issue; (2) `registry_bridge.rs` mixes Delta codec installation with UDF/table-provider registration in a single method, violating SRP; (3) the `run_build` convenience function in `lib.rs` contains substantial orchestration policy (120+ lines) that should be in `CpgMaterializer`; and (4) no stable public contract document exists for either the plugin ABI or the PyO3 binding surface. These are the highest-priority improvements; everything else is at alignment score 2 or 3.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Plugin internals (`task_context`, `options`) are `pub(crate)` correctly; manifest fields fully public with no accessor layer |
| 2 | Separation of concerns | 2 | small | medium | `registry_bridge.rs` mixes Delta codec install with UDF registration |
| 3 | SRP (one reason to change) | 2 | medium | medium | `run_build` in `lib.rs` owns orchestration + JSON shaping + artifact path logic |
| 4 | High cohesion, low coupling | 2 | small | low | `df_plugin_host` depends on `deltalake` in host — technology leaks into orchestration |
| 5 | Dependency direction | 3 | — | — | Core plugin API depends only on `abi_stable` + `datafusion-ffi`; details depend on API |
| 6 | Ports & Adapters | 3 | — | — | Plugin boundary is explicit C ABI port; host adapts FFI types to native DataFusion types |
| 7 | DRY | 2 | small | low | `build_id` field populated with `CARGO_PKG_VERSION` (same as `plugin_version`); semantic duplication |
| 8 | Design by contract | 2 | small | medium | `global_task_ctx_provider` returns opaque Arc with no preconditions; missing NULL-option contract on options JSON |
| 9 | Parse, don't validate | 3 | — | — | Options parsed once at the boundary (`parse_options`/`parse_udf_options`); IPC schema parsed once via `schema_from_ipc` |
| 10 | Make illegal states unrepresentable | 2 | small | low | `capabilities: u64` bitmask allows declaring `RELATION_PLANNER` without any corresponding export slot in `DfPluginExportsV1` |
| 11 | CQS | 2 | small | low | `global_task_ctx_provider` reads global state and constructs it on first call — mixed query/init |
| 12 | Dependency inversion + explicit composition | 1 | medium | high | `global_task_ctx_provider` instantiates `SessionContext` as a hidden global, bypassing all DI |
| 13 | Composition over inheritance | 3 | — | — | No inheritance; all extension via composition and trait objects |
| 14 | Law of Demeter | 2 | small | low | `registry_bridge.rs` calls `state_ref().write()` then `config_mut()` — two levels of indirection |
| 15 | Tell, don't ask | 3 | — | — | `PluginHandle` encapsulates all registration decisions; callers never inspect raw FFI types |
| 16 | Functional core, imperative shell | 2 | medium | medium | `run_build` in `lib.rs` mixes JSON shaping logic with IO orchestration in one 180-line function |
| 17 | Idempotency | 2 | small | low | `global_task_ctx_provider` is idempotent (OnceLock); re-running `load_plugin` on same path would reload |
| 18 | Determinism / reproducibility | 3 | — | — | Spec hash is computed before execution; `contract_version: 3` guards response shape |
| 19 | KISS | 2 | medium | low | `datafusion_ext_py` mirrors all of `datafusion._internal` dynamically at import time |
| 20 | YAGNI | 2 | small | low | `build_id` field exists in manifest but is never checked by the host validator |
| 21 | Least astonishment | 2 | small | low | `SessionFactory::from_class` and `SessionFactory::from_class_name` both public with identical semantics |
| 22 | Declare and version contracts | 1 | medium | high | No changelog, no stability annotation, no SEMVER policy for plugin ABI or PyO3 surface |
| 23 | Design for testability | 1 | medium | high | `global_task_ctx_provider` makes unit testing impossible without process isolation; no unit tests for PyO3 binding crates |
| 24 | Observability | 2 | small | low | `tracing::error!` used in `udf_bundle.rs` but silent failure path in `build_udf_bundle` swallows errors without structured fields |

---

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information Hiding — Alignment: 2/3

**Current state:**
The plugin API crate exposes all manifest fields as `pub` with no accessor layer (`rust/df_plugin_api/src/manifest.rs:18-30`). Callers can read and write fields directly; `plugin_abi_major`, `df_ffi_major`, etc. are raw public fields on a `#[repr(C)]` struct. This is appropriate for an ABI struct — mutability is intentional — but there is no documentation signalling which fields are read by the host versus informational only (e.g., `build_id` and `features` are never checked).

Within `df_plugin_codeanatomy`, all internal types are correctly `pub(crate)` (`options.rs`, `task_context.rs`, `providers.rs`, `udf_bundle.rs`), and the internal module structure is private to the cdylib. The one gap is that `DfPluginManifestV1` fields have no visibility distinction between "checked by host" and "informational."

**Findings:**
- `rust/df_plugin_api/src/manifest.rs:27-29`: `build_id`, `capabilities`, and `features` fields are public but `build_id` is never read by `validate_manifest` in `rust/df_plugin_host/src/loader.rs:27-76`. No annotation distinguishes checked from informational fields.
- `rust/codeanatomy_engine_py/src/lib.rs:10-15`: All internal modules are `pub mod`, making the Rust-level internals of the binding crate accessible from outside when built as `rlib`. Since this is consumed only as a `cdylib` + `rlib`, this is low-risk but worth tightening.

**Suggested improvement:**
Add doc comments to `DfPluginManifestV1` fields marking each as `/// Checked by host` or `/// Informational only`. Consider an `impl DfPluginManifestV1` that provides named accessor methods for the checked fields, so test code and the host use the same field-access path.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of Concerns — Alignment: 2/3

**Current state:**
`rust/df_plugin_host/src/registry_bridge.rs` bundles Delta-specific codec installation inside `register_table_providers`. The `install_delta_plan_codecs` function at lines 19-25 mutates the session's config to inject `DeltaLogicalCodec` and `DeltaPhysicalCodec`. This means that the host's generic "register providers from plugin" operation carries a hardcoded Delta dependency. If a future plugin provides non-Delta table providers, `install_delta_plan_codecs` is still invoked.

**Findings:**
- `rust/df_plugin_host/src/registry_bridge.rs:19-25`: `install_delta_plan_codecs` is a Delta-specific side effect embedded inside a generic provider registration method.
- `rust/df_plugin_host/src/registry_bridge.rs:88`: `Self::install_delta_plan_codecs(ctx)` called unconditionally at start of `register_table_providers`, before capability check.
- `rust/df_plugin_host/Cargo.toml`: `deltalake` is a direct dependency of `df_plugin_host`, tying the generic host to one specific storage technology.

**Suggested improvement:**
Extract `install_delta_plan_codecs` out of `registry_bridge.rs` and into the calling site (`df_plugin_codeanatomy` or a caller-provided callback). `register_table_providers` should accept an optional `SessionExtensionInstaller` callback, or the codec install should be the responsibility of the plugin itself (since only `df_plugin_codeanatomy` currently needs it). This would allow the host to remain technology-agnostic.

**Effort:** medium
**Risk if unaddressed:** medium — grows as more plugins are added

---

#### P3. SRP (One Reason to Change) — Alignment: 2/3

**Current state:**
`rust/codeanatomy_engine_py/src/lib.rs` contains `run_build`, a 150-line function that handles: (1) JSON deserialization of the request, (2) construction of `SessionFactory`, `SemanticPlanCompiler`, and `CpgMaterializer`, (3) execution, (4) result parsing, (5) output root inference via `infer_output_root`, and (6) construction of the response JSON shape with `artifacts`, `diagnostics`, `auxiliary_outputs`, `manifest_path`, and `run_bundle_dir`. Reasons to change this function include: changes to request schema, changes to artifact path conventions, changes to orchestration flags, and changes to the response contract shape.

**Findings:**
- `rust/codeanatomy_engine_py/src/lib.rs:32-179`: `run_build` performs at least five distinct responsibilities.
- `rust/codeanatomy_engine_py/src/lib.rs:17-29`: `infer_output_root` is a private helper that belongs logically with output/artifact shaping, not the binding module root.
- `rust/codeanatomy_engine_py/src/lib.rs:171`: Hard-coded `contract_version: 3` is embedded in orchestration code rather than a named constant.

**Suggested improvement:**
Extract output shaping (the `artifacts`, `manifest_path`, `auxiliary_outputs` construction) into a dedicated `response_builder` function or a `RunBuildResponse` struct with a builder. Move `infer_output_root` there. The `run_build` function should only: deserialize, dispatch to `CpgMaterializer::execute`, then delegate response building. Define `const CONTRACT_VERSION: u32 = 3` at module level.

**Effort:** medium
**Risk if unaddressed:** medium — this function will accumulate orchestration flags over time

---

#### P4. High Cohesion, Low Coupling — Alignment: 2/3

**Current state:**
`df_plugin_host` has a direct dependency on `deltalake`, which is a storage-specific technology. The host is designed to be a generic plugin loader and registration bridge, but its Cargo.toml requires `deltalake = { version = "0.31.0", features = ["datafusion"] }`. This couples the generic host to one storage backend.

`df_plugin_common` is well-scoped: two utility functions (`parse_major`, `schema_from_ipc`) and one constant (`DELTA_SCAN_CONFIG_VERSION`). The presence of `DELTA_SCAN_CONFIG_VERSION` in a crate named "common" is a cohesion gap — it is Delta-specific knowledge in a general utilities crate.

**Findings:**
- `rust/df_plugin_host/Cargo.toml`: `deltalake` dependency in a generic host crate.
- `rust/df_plugin_common/src/lib.rs:22`: `DELTA_SCAN_CONFIG_VERSION: u32 = 1` is Delta-specific but lives in the generic common crate.

**Suggested improvement:**
Move `DELTA_SCAN_CONFIG_VERSION` to `df_plugin_codeanatomy` (the only consumer that uses it meaningfully). Remove the `deltalake` dependency from `df_plugin_host` by making codec installation a caller responsibility (see P2 suggestion). `df_plugin_common` should contain only truly generic cross-crate utilities.

**Effort:** small
**Risk if unaddressed:** low — currently only one plugin, but grows as the ecosystem does

---

#### P5. Dependency Direction — Alignment: 3/3

The dependency hierarchy is correct and clean:
- `df_plugin_api` depends only on `abi_stable` and `datafusion-ffi` (pure interface contracts, no implementations).
- `df_plugin_common` depends only on `arrow` for IPC utilities.
- `df_plugin_host` depends on `df_plugin_api` and `df_plugin_common` (plus `datafusion` for type bridging).
- `df_plugin_codeanatomy` depends on `df_plugin_api`, `df_plugin_common`, and `datafusion_ext` (the implementation layer).
- `codeanatomy_engine_py` depends on `codeanatomy-engine` (core) and PyO3 (boundary).
- `datafusion_ext_py` depends on `datafusion-python` (boundary adapter).

No direction violations observed. Score 3.

---

#### P6. Ports & Adapters — Alignment: 3/3

The plugin boundary is a textbook Ports & Adapters implementation. `df_plugin_api::DfPluginMod` is the port: a `#[repr(C)]` vtable with `extern "C"` function pointers, protected by `abi_stable::StableAbi`. Plugins implement the port; the host uses it without knowing the implementation. `registry_bridge.rs` is the adapter: it converts FFI types (`FFI_ScalarUDF`, `FFI_TableProvider`, etc.) to native DataFusion types.

The PyO3 binding is also clean: `codeanatomy_engine_py` wraps the Rust engine behind a JSON-over-Python boundary, never exposing Rust-native types to Python. Score 3.

---

### Category: Knowledge (7-11)

#### P7. DRY (Knowledge, Not Lines) — Alignment: 2/3

**Current state:**
`DfPluginManifestV1` has both `plugin_version: RString` and `build_id: RString`. In `df_plugin_codeanatomy/src/lib.rs:31-32`, both are set to `env!("CARGO_PKG_VERSION")`. This means the same knowledge (the plugin's release version) is encoded twice. Neither field is validated differently by the host.

The `resolve_udf_policy` function in `options.rs` is the single authoritative source for async UDF validation rules — this is good. The `parse_and_validate_spec` function in `compiler.rs` is the single authoritative source for spec validation — also good.

**Findings:**
- `rust/df_plugin_codeanatomy/src/lib.rs:31-32`: `plugin_version` and `build_id` assigned the same value.
- `rust/df_plugin_api/src/manifest.rs:26-27`: Two fields with overlapping semantics and no documented distinction in purpose.

**Suggested improvement:**
Document the intended distinction between `plugin_version` (SemVer of the plugin's own versioned release) and `build_id` (a build-system artifact ID such as a git SHA or CI build number). If they are genuinely different concepts, the codeanatomy plugin should populate `build_id` from a build-time env var (e.g., `CARGO_PKG_BUILD_METADATA` or a custom `BUILD_ID` env injected by CI). If they are redundant, remove `build_id` from the manifest (this is an ABI-breaking change in a new `DfPluginManifestV2`).

**Effort:** small
**Risk if unaddressed:** low

---

#### P8. Design by Contract — Alignment: 2/3

**Current state:**
The host's `validate_manifest` function (`rust/df_plugin_host/src/loader.rs:27-76`) enforces six explicit contracts: ABI major match, ABI minor compatibility, struct-size floor, FFI major match, DataFusion major match, and Arrow major match. This is thorough and explicit. The precondition that `manifest.struct_size >= sizeof(DfPluginManifestV1)` ensures forward compatibility.

However, `global_task_ctx_provider` (`rust/df_plugin_codeanatomy/src/task_context.rs:6-10`) has no contract at all. It silently creates a `SessionContext` with no configuration, no timeout, and no documented behavior for callers. The function signature `-> Arc<dyn TaskContextProvider>` does not communicate whether the returned context is fresh or shared, or what configuration it carries.

The `options_json` parameter in `create_table_provider` is `ROption<RString>`, meaning `None` is a valid input that causes `parse_options` to return `Err("Missing options JSON")` — this is a precondition violation (calling without required options) that is detected only at runtime.

**Findings:**
- `rust/df_plugin_codeanatomy/src/task_context.rs:6-10`: No documentation of returned context's configuration, sharing semantics, or thread-safety guarantees.
- `rust/df_plugin_codeanatomy/src/providers.rs:155-166`: `create_table_provider` dispatches to `parse_options` which fails with "Missing options JSON" for both `delta` and `delta_cdf` — a hard precondition that is not declared in the function signature or the plugin manifest.

**Suggested improvement:**
Add a doc comment to `global_task_ctx_provider` documenting: (a) that it returns a process-global shared context, (b) that it is initialized with default `SessionContext::new()` configuration, and (c) its thread-safety guarantee. For `create_table_provider`, document in the plugin manifest's `features: RVec<RString>` or a new `required_options` protocol that both `delta` and `delta_cdf` require options JSON.

**Effort:** small
**Risk if unaddressed:** medium — callers cannot reason about session configuration from the API alone

---

#### P9. Parse, Don't Validate — Alignment: 3/3

This principle is well-executed throughout. `parse_options::<T>` and `parse_udf_options` in `options.rs` parse JSON into strongly-typed structs at the boundary. `schema_from_ipc` parses IPC bytes into `SchemaRef`. `serde_path_to_error::deserialize` in `compiler.rs:351` provides path-locating parse errors. The `deserialize_schema_ipc` custom deserializer in `options.rs:89-123` handles multiple input representations (base64 string, byte array) in a single parse step. Score 3.

---

#### P10. Make Illegal States Unrepresentable — Alignment: 2/3

**Current state:**
The `capabilities: u64` bitmask in `DfPluginManifestV1` (`rust/df_plugin_api/src/manifest.rs:28`) allows a plugin to declare `caps::RELATION_PLANNER` (bit 5) without any corresponding slot in `DfPluginExportsV1` (`rust/df_plugin_api/src/lib.rs:36-40`). `DfPluginExportsV1` has `table_provider_names`, `udf_bundle`, and `table_functions`, but no `relation_planner` field. A plugin can declare itself as having relation planning capability while the host has no mechanism to retrieve it. The host never validates `RELATION_PLANNER` capability in `registry_bridge.rs`.

**Findings:**
- `rust/df_plugin_api/src/manifest.rs:13`: `caps::RELATION_PLANNER: u64 = 1 << 5` is declared.
- `rust/df_plugin_api/src/lib.rs:36-40`: `DfPluginExportsV1` has no `relation_planner` field.
- `rust/df_plugin_codeanatomy/src/lib.rs:33-38`: The codeanatomy plugin declares `RELATION_PLANNER` in capabilities but exports no corresponding artifact.
- `rust/df_plugin_host/src/registry_bridge.rs`: No call to `require_capability(caps::RELATION_PLANNER, ...)` anywhere.

**Suggested improvement:**
Either: (a) add a `relation_planner: ROption<FfiRelationPlanner>` field to `DfPluginExportsV1` so that declaring the capability creates an actual representational slot, or (b) remove `RELATION_PLANNER` from `caps` until the corresponding exports field exists. The current state allows plugins to advertise a capability the host cannot consume.

**Effort:** small
**Risk if unaddressed:** low currently, medium when host tries to act on `RELATION_PLANNER`

---

#### P11. Command-Query Separation — Alignment: 2/3

**Current state:**
`global_task_ctx_provider` (`rust/df_plugin_codeanatomy/src/task_context.rs:6-10`) is named as a query ("get the provider") but its first invocation creates and stores a `SessionContext` as a side effect. This is a command-query violation: the function changes global state (initializing `TASK_CTX_PROVIDER`) while returning a value.

`build_udf_bundle_from_specs` in `udf_bundle.rs:68-96` is a pure transformation — it takes specs and config, returns a bundle. Clean. `config_options_from_udf_options` similarly is a pure transform. These are well-separated.

**Findings:**
- `rust/df_plugin_codeanatomy/src/task_context.rs:7-8`: `TASK_CTX_PROVIDER.get_or_init(...)` is a mutation-on-first-read pattern embedded in a query function.

**Suggested improvement:**
Rename to `get_or_init_global_task_ctx_provider` to signal the initialization side effect, or — better — eliminate the global and pass the provider explicitly into the functions that need it (see P12 discussion). The renaming is small; the structural fix is medium.

**Effort:** small (rename), medium (structural)
**Risk if unaddressed:** low — but misleading naming increases onboarding friction

---

### Category: Composition (12-15)

#### P12. Dependency Inversion + Explicit Composition — Alignment: 1/3

**Current state:**
`global_task_ctx_provider` in `task_context.rs` is a globally constructed `SessionContext` wrapped in a `OnceLock`. It is called from two sites:
- `rust/df_plugin_codeanatomy/src/providers.rs:103` and `141` inside `build_delta_provider` and `build_delta_cdf_provider`
- `rust/df_plugin_codeanatomy/src/udf_bundle.rs:142` inside `build_table_functions`

This means the `SessionContext` used for task context in every `FFI_TableProvider` and `FFI_TableFunction` is a single process-global instance created with `SessionContext::new()` — no configuration, no session-level settings, no observability hooks. Callers cannot inject a different context. There is no way to test `build_table_functions` with a controlled `SessionContext` without process isolation.

The existence of this global is architecturally explained by the FFI boundary: `FFI_TableProvider::new` and `FFI_TableFunction::new` both accept a `&dyn TaskContextProvider` reference, and across the C ABI, the provider must outlive all use. A static is a valid solution but the choice to use a zero-configuration `SessionContext::new()` as the provider is hidden from callers.

**Findings:**
- `rust/df_plugin_codeanatomy/src/task_context.rs:7-8`: `OnceLock<Arc<SessionContext>>` is a hidden global dependency.
- `rust/df_plugin_codeanatomy/src/providers.rs:103,141`: `global_task_ctx_provider()` called inside pure data-fetching functions.
- `rust/df_plugin_codeanatomy/src/udf_bundle.rs:142`: Same call inside `build_table_functions`.
- No path for a test to supply a different `TaskContextProvider`.

**Suggested improvement:**
The most practical improvement within the FFI constraint is to expose a `plugin_configure_task_context(config_json: ROption<RString>) -> DfResult<()>` function on the plugin vtable (`DfPluginMod`). The host would call this once immediately after loading, supplying configuration. The plugin would initialize `TASK_CTX_PROVIDER` from that configuration rather than using `SessionContext::new()` defaults. This keeps the global but makes its initialization explicit and configurable.

For testability, add a `cfg(test)` replacement: a function `#[cfg(test)] fn set_task_ctx_provider_for_test(provider: Arc<SessionContext>)` that forces the OnceLock via `OnceLock::set`.

**Effort:** medium
**Risk if unaddressed:** high — the hidden global makes integration and unit testing of the plugin boundary impossible without real session infrastructure

---

#### P13. Composition Over Inheritance — Alignment: 3/3

No inheritance is used. All extension is via trait objects (`Arc<dyn ScalarUDFImpl>`, `Arc<dyn TableProvider>`, `Arc<dyn TaskContextProvider>`). Behavior is built by combining components. Score 3.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
`install_delta_plan_codecs` in `registry_bridge.rs:19-25` chains: `ctx.state_ref().write()` then `.config_mut()` then `.set_extension(...)`. This is three levels of navigation into the DataFusion session internals. Any restructuring of `SessionContext` → `SessionState` → `SessionConfig` would ripple through this code. However, this pattern is idiomatic in DataFusion and unavoidable given the API surface.

Within the plugin crates' own code, the Demeter principle is respected: no deep chaining into plugin-owned types.

**Findings:**
- `rust/df_plugin_host/src/registry_bridge.rs:20-24`: `ctx.state_ref().write().config_mut().set_extension(...)` — three levels of navigation into DataFusion internals.

**Suggested improvement:**
Extract a `fn install_plan_codecs(ctx: &SessionContext)` helper in a separate module to isolate the navigation, so that changes to the DataFusion session model require updating only one place. (This is already partially done — `install_delta_plan_codecs` is private — but the navigation chain should be documented as a known coupling point.)

**Effort:** small
**Risk if unaddressed:** low — the coupling is to an external library's stable internal API

---

#### P15. Tell, Don't Ask — Alignment: 3/3

`PluginHandle` fully encapsulates the plugin registration decisions. Callers tell `handle.register_udfs(ctx, options)` and the handle decides how to iterate and register. Callers never inspect `FFI_ScalarUDF` structures directly. The `require_capability` helper at `registry_bridge.rs:27-36` enforces capability gates internally. The PyO3 `CpgMaterializer.execute` encapsulates the entire execution lifecycle. Score 3.

---

### Category: Correctness (16-18)

#### P16. Functional Core, Imperative Shell — Alignment: 2/3

**Current state:**
The `run_build` function in `lib.rs:32-179` violates the functional core / imperative shell separation. It contains pure JSON-shaping logic (constructing `artifacts`, `auxiliary_outputs`, `manifest_path` dictionaries) interleaved with IO orchestration (calling `compiler.compile_internal`, `materializer.execute_internal`). The pure logic cannot be tested without invoking the IO.

By contrast, `compiler.rs` and `session.rs` in `codeanatomy_engine_py` are well-separated: `parse_and_validate_spec` is a pure function, `SemanticPlanCompiler::compile` computes a hash and returns a value without IO. The `CpgMaterializer::execute` is correctly imperative.

**Findings:**
- `rust/codeanatomy_engine_py/src/lib.rs:62-178`: Mixed: JSON parsing, string mutations (`spec_obj.insert`), IO dispatch (`compiler.compile_internal`, `materializer.execute_internal`), and pure JSON assembly of the response all in one function body.
- `rust/codeanatomy_engine_py/src/lib.rs:139-169`: The `auxiliary_outputs` and `artifacts` construction is pure JSON logic that could be tested independently.

**Suggested improvement:**
Extract the response-shaping logic into a `fn build_run_build_response(outputs: &Value, warnings: &Value, orchestration: &Value, run_result: &Value) -> Value` pure function. This function has no IO and can be unit-tested with synthetic inputs. `run_build` becomes: deserialize → execute → build response → convert to Python.

**Effort:** medium
**Risk if unaddressed:** medium — the mixed function will grow as orchestration flags accumulate

---

#### P17. Idempotency — Alignment: 2/3

**Current state:**
`load_plugin` (`loader.rs:78-88`) is not idempotent: calling it twice on the same path loads the shared library twice. `abi_stable` may handle deduplication at the OS level, but there is no crate-level guard preventing double loading of the same plugin. This is a caller responsibility gap.

`global_task_ctx_provider` is idempotent via `OnceLock` — the initialization only runs once.

**Findings:**
- `rust/df_plugin_host/src/loader.rs:78-88`: `load_plugin` has no guard against double loading; the returned `PluginHandle` is a new instance each call.
- No `PluginRegistry` or deduplication layer in `df_plugin_host`.

**Suggested improvement:**
Add a `PluginRegistry` (a `HashMap<PathBuf, Arc<PluginHandle>>`) in `df_plugin_host` so that `load_plugin` is effectively idempotent: if a plugin at the same path is already loaded, return the existing handle. Alternatively, document clearly in `load_plugin`'s doc comment that it is the caller's responsibility to avoid double loading.

**Effort:** small (documentation), medium (registry)
**Risk if unaddressed:** low — currently only one plugin is expected

---

#### P18. Determinism / Reproducibility — Alignment: 3/3

Spec hashing (`codeanatomy_engine::spec::hashing::hash_spec`) is computed before execution and stored in `CompiledPlan`. The `contract_version: 3` field on `run_build` responses guards the response shape. The `PluginManifestV1` captures all relevant version dimensions (ABI, DataFusion, Arrow, FFI) to ensure reproducibility guarantees at the plugin boundary. Score 3.

---

### Category: Simplicity (19-22)

#### P19. KISS — Alignment: 2/3

**Current state:**
`datafusion_ext_py/src/lib.rs:7-23` implements the module initializer by dynamically mirroring the entire public surface of `datafusion._internal` at Python import time. It iterates `datafusion._internal`'s `__dict__`, filters out private names, and re-registers every symbol. This approach is clever but fragile: it depends on `datafusion._internal` being importable at Rust module init time, and any private symbol that begins with `_` is excluded but no other filtering is applied. The full public surface of `datafusion._internal` — including any symbols added in future datafusion-python versions — is implicitly re-exported.

**Findings:**
- `rust/datafusion_ext_py/src/lib.rs:11-19`: Dynamic mirroring of an external module's entire public surface with no allowlist.
- `rust/datafusion_ext_py/src/lib.rs:21`: `collect_public_names` constructs `__all__` from the same dynamic iteration.
- No test validates that the mirrored surface matches expectations.

**Suggested improvement:**
Define an explicit allowlist of symbols to re-export from `datafusion._internal`. This makes the contract explicit and prevents unintentional exposure of new upstream symbols. Alternatively, if the intent is to pass through the entire DataFusion Python surface, add a golden test that snapshots the mirrored names and alerts on changes.

**Effort:** small
**Risk if unaddressed:** low — but the implicit contract with `datafusion._internal` is a latent surprise

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**
`caps::RELATION_PLANNER` is defined in `manifest.rs:13` and declared in `df_plugin_codeanatomy`'s manifest (`lib.rs:38`), but there is no corresponding export slot in `DfPluginExportsV1`, no host-side registration logic for it, and no test for it. It is speculative capability for a feature that does not yet exist.

`build_id` in `DfPluginManifestV1` (see P7 finding) is similarly unused by the host validator — potentially speculative for future CI integration.

**Findings:**
- `rust/df_plugin_api/src/manifest.rs:13`: `RELATION_PLANNER` capability bit declared.
- `rust/df_plugin_codeanatomy/src/lib.rs:38`: `caps::RELATION_PLANNER` included in the plugin's declared capabilities.
- No corresponding export slot or host-side handler.

**Suggested improvement:**
Remove `RELATION_PLANNER` from both the `caps` module and the codeanatomy plugin's capability flags until the feature is implemented. Reserve the bit value to prevent future reuse conflicts by adding a comment: `// Reserved: 1 << 5 (RELATION_PLANNER) — not yet implemented`.

**Effort:** small
**Risk if unaddressed:** low — mainly causes confusion when reading the manifest

---

#### P21. Principle of Least Astonishment — Alignment: 2/3

**Current state:**
`SessionFactory::from_class` (`session.rs:59-61`) is a `#[staticmethod]` that calls `Self::from_class_name`. `SessionFactory::from_class_name` (`session.rs:117-132`) is a separate `pub(crate)` method. Both do the same thing. The public Python API exposes `from_class`; the internal Rust API uses `from_class_name`. A reader of `session.rs` encounters two nearly identical entry points.

Additionally, `CpgMaterializer` and `SemanticPlanCompiler` both expose `new()` (Python-callable) and `new_internal()` (Rust-callable) methods. The `_internal` suffix pattern is somewhat surprising — `new()` creates a Tokio runtime inside `#[pymethods]`, but `new_internal()` merely delegates to `new()`. The distinction adds indirection without clarity.

**Findings:**
- `rust/codeanatomy_engine_py/src/session.rs:59-61,117-132`: `from_class` and `from_class_name` are functionally identical.
- `rust/codeanatomy_engine_py/src/compiler.rs:455-462`: `new_internal()` calls `Self::new()` with no additional behavior.
- `rust/codeanatomy_engine_py/src/materializer.rs:559-576`: `new_internal()` and `execute_internal()` are thin pass-throughs.

**Suggested improvement:**
Collapse `from_class` to directly contain the match logic (removing `from_class_name`), or rename `from_class_name` to a clearly internal name like `_from_class_str`. Remove `new_internal` / `execute_internal` pass-throughs — the `run_build` function can call the `#[pymethods]` methods directly since it is in the same crate, or the internal methods can be documented as "bypass-for-Rust-callers."

**Effort:** small
**Risk if unaddressed:** low — minor friction for contributors to the binding crate

---

#### P22. Declare and Version Public Contracts — Alignment: 1/3

**Current state:**
This is the most significant structural gap in the scope. The plugin ABI has two versioning constants (`DF_PLUGIN_ABI_MAJOR = 1`, `DF_PLUGIN_ABI_MINOR = 1`) but no changelog, no written compatibility policy, and no documented definition of "what constitutes a breaking change." Neither the `df_plugin_api` nor the `codeanatomy_engine_py` crates have a `CHANGELOG.md`, a `COMPATIBILITY.md`, or doc-level stability attributes.

The PyO3 binding surface (`SessionFactory`, `SemanticPlanCompiler`, `CompiledPlan`, `CpgMaterializer`, `PyRunResult`, `SchemaRuntime`, `run_build`) is consumed by Python code but is not listed anywhere as a stable surface. Python callers have no way to know which methods are stable versus subject to change.

The `run_build` response shape has `contract_version: 3` hard-coded (`lib.rs:171`) but no documented definition of what versions 1, 2, and 3 represent, no migration guide, and no test that validates the shape is preserved across refactors.

**Findings:**
- `rust/df_plugin_api/src/manifest.rs:4-5`: Version constants defined but no policy document.
- `rust/codeanatomy_engine_py/src/lib.rs:171`: `contract_version: 3` with no changelog.
- No `CHANGELOG.md`, no `#[deprecated]` annotations, no stability attributes on any public item in any of the six crates.
- `rust/datafusion_ext_py/src/lib.rs:20`: `IS_STUB = false` is a convention that implies a stub/real distinction, but no documentation explains what "stub" means or when it applies.

**Suggested improvement:**
(1) Create `rust/df_plugin_api/CHANGELOG.md` documenting what changed between ABI versions and the backward-compatibility rule (minor-version additive only, major-version breaking). (2) Add a `COMPATIBILITY.md` or `doc` comment block in `codeanatomy_engine_py/src/lib.rs` listing the stable PyO3 surface and the contract that `run_build` response `contract_version` obeys. (3) Define `const RUN_BUILD_CONTRACT_VERSION: u32 = 3` and document it. (4) Document `IS_STUB` in `datafusion_ext_py`.

**Effort:** medium
**Risk if unaddressed:** high — plugin ABI is the most sensitive cross-process contract; undocumented breaks cause production runtime failures

---

### Category: Quality (23-24)

#### P23. Design for Testability — Alignment: 1/3

**Current state:**
`codeanatomy_engine_py` has no Rust-level unit tests (no `#[cfg(test)]` blocks in any of its six source files). `datafusion_ext_py` has no tests at all. The binding layer's behavior is tested only at the Python integration level (`tests/integration/test_rust_engine_e2e.py`), which requires a full engine build.

`df_plugin_codeanatomy` has integration tests in `lib.rs:70-252` that test the full plugin stack against a real `SessionContext` and an in-memory `DeltaTable`. These are good but heavyweight. The `global_task_ctx_provider` pattern (P12) makes it impossible to inject a test `SessionContext` into `build_delta_provider` or `build_table_functions` without process-level isolation.

`df_plugin_host/src/loader.rs` has a test module that covers `validate_manifest` in isolation — this is well-done. `df_plugin_common` tests `parse_major` and `schema_from_ipc` as pure utilities.

**Findings:**
- `rust/codeanatomy_engine_py/src/`: Zero `#[cfg(test)]` blocks across all six files.
- `rust/datafusion_ext_py/src/lib.rs`: No tests.
- `rust/df_plugin_codeanatomy/src/task_context.rs:7`: `OnceLock` prevents injection of a test context.
- `rust/codeanatomy_engine_py/src/compiler.rs:326-405`: `parse_and_validate_spec` is a pure function that would be easy to unit-test but is not tested at the Rust level.
- `rust/codeanatomy_engine_py/src/session.rs:117-132`: `from_class_name`'s match logic is not unit-tested at the Rust level.

**Suggested improvement:**
(1) Add `#[cfg(test)]` blocks in `codeanatomy_engine_py/src/compiler.rs` covering `parse_and_validate_spec` with: a valid spec, a legacy `parameter_templates` spec, a spec below `SPEC_SCHEMA_VERSION`, and an empty view definitions spec. These are pure-function tests with no external dependencies. (2) Add a test in `codeanatomy_engine_py/src/session.rs` covering all three `from_class_name` arms and the error case. (3) Add a test in `datafusion_ext_py/src/lib.rs` verifying `collect_public_names` returns sorted, deduplicated names. (4) For `global_task_ctx_provider`, expose `#[cfg(test)] fn reset_task_ctx_provider_for_test()` to allow state reset between tests.

**Effort:** medium
**Risk if unaddressed:** high — regression risk in the Python binding surface is invisible without Rust-level tests

---

#### P24. Observability — Alignment: 2/3

**Current state:**
`df_plugin_codeanatomy/src/udf_bundle.rs:116-126` contains a fallback path: if `build_udf_bundle_with_options` fails, `build_udf_bundle` logs `tracing::error!(error = %err, "Failed to build UDF bundle")` and returns an empty bundle. This is correct structured tracing. However the error path swallows the failure and returns an empty bundle — the caller (`exports()` at `lib.rs:162`) has no way to know the bundle is empty due to an error versus genuinely empty. The empty-bundle-on-error behavior is silent from the host's perspective.

`codeanatomy_engine_py/src/materializer.rs` uses full OpenTelemetry tracing behind `#[cfg(feature = "tracing")]` — this is well-structured. The engine-level pipeline has good observability.

The plugin loading path (`df_plugin_host/src/loader.rs`) has no tracing instrumentation at all — a successful plugin load, a manifest validation failure, and a file-not-found error all produce no structured events beyond the returned `Err`.

**Findings:**
- `rust/df_plugin_codeanatomy/src/udf_bundle.rs:114-126`: `build_udf_bundle` silently returns an empty bundle on error; the error signal is only a `tracing::error!` log.
- `rust/df_plugin_host/src/loader.rs:78-88`: `load_plugin` produces no structured tracing on success or failure.
- `rust/df_plugin_host/src/registry_bridge.rs:38-67`: `register_udfs` registers N UDFs with no per-UDF logging or count instrumentation.

**Suggested improvement:**
(1) Change `build_udf_bundle` to propagate the error (return `Result<DfUdfBundleV1, String>`) so that `exports()` can decide whether to panic, return an empty exports struct, or propagate. Silent empty-on-error is surprising from a host that expects a populated bundle. (2) Add `tracing::instrument` or manual span to `load_plugin` so that plugin load events appear in traces with `plugin_name`, `plugin_version`, and `path`. (3) Add a `tracing::info!` at successful UDF registration with a count.

**Effort:** small
**Risk if unaddressed:** low (logging) to medium (silent empty bundle on error)

---

## Cross-Cutting Themes

### Theme 1: The Global Session Context Knot

The `global_task_ctx_provider` OnceLock at `rust/df_plugin_codeanatomy/src/task_context.rs:7-8` is the root cause of three separate principle violations: P8 (no contract for what configuration the context carries), P11 (mutation-on-read CQS violation), P12 (hidden dependency bypassing DI), and P23 (untestable in isolation). The core reason it exists is valid — the C ABI requires a `'static` lifetime for the `TaskContextProvider` reference — but the *configuration* of that context is unnecessarily hardcoded. The suggested fix (a `plugin_configure_task_context` vtable entry) would resolve all four violations at once.

### Theme 2: Undocumented Public Contracts

The plugin ABI (P22) and PyO3 surface (P22, P23) are the two most operationally sensitive boundaries in the entire codebase — they cross process boundaries and language boundaries respectively. Yet neither has a stability declaration, changelog, or compatibility policy. This is the most impactful gap because breakage is silent (a plugin ABI mismatch causes a `DataFusionError::Plan` at runtime, not a compile error) and hard to debug.

### Theme 3: `RELATION_PLANNER` Phantom Capability

`caps::RELATION_PLANNER` is declared, exported in the manifest, but has no corresponding export slot, no host handler, and is never validated. This is a consistency gap between the manifest contract and the exports contract (P10, P20). It will confuse future implementers who see the capability declared but find no way to actually use it.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P23 | Add Rust unit tests for `parse_and_validate_spec` and `from_class_name` in `codeanatomy_engine_py` — pure functions, zero external deps | small | high |
| 2 | P22 | Write `rust/df_plugin_api/CHANGELOG.md` and add a doc block in `lib.rs` defining the ABI compatibility policy | small | high |
| 3 | P20 | Remove `caps::RELATION_PLANNER` from `df_plugin_codeanatomy` capabilities declaration until implemented | small | medium |
| 4 | P7 | Document or unify `plugin_version` vs `build_id` in `DfPluginManifestV1` | small | medium |
| 5 | P24 | Change `build_udf_bundle` to propagate errors rather than silently returning an empty bundle | small | medium |

---

## Recommended Action Sequence

1. **Write the ABI compatibility policy document** (P22, small): Before any other changes, record the current contract so that all subsequent changes are judged against it. This gates everything else.

2. **Add Rust unit tests for pure binding-layer functions** (P23, medium): Target `parse_and_validate_spec`, `from_class_name`, `resolve_udf_policy`, `infer_output_root`, and `collect_public_names`. These tests have no external dependencies and can be added file by file.

3. **Fix `build_udf_bundle` to propagate errors** (P24, small): Change the return type of `build_udf_bundle` from `DfUdfBundleV1` to `Result<DfUdfBundleV1, String>` and update `exports()` to propagate or panic explicitly. This removes the silent-empty-bundle hazard.

4. **Remove speculative `RELATION_PLANNER` capability** (P10, P20, small): Remove from `caps` (add reserved comment) and from `df_plugin_codeanatomy`'s capability flags until the export slot exists.

5. **Extract response-shaping logic from `run_build`** (P3, P16, medium): Move the `artifacts` / `auxiliary_outputs` / `manifest_path` JSON construction into a pure `build_run_build_response` function. This enables unit testing of the response shape independently of execution.

6. **Address the global task context** (P12, P8, P23, medium): Add a `plugin_configure_task_context` vtable entry (or an explicit initialization function) so the process-global `SessionContext` is configured externally rather than with `SessionContext::new()` defaults. Add a `cfg(test)` reset hook.

7. **Decouple Delta from the generic host** (P2, P4, medium): Move `install_delta_plan_codecs` out of `registry_bridge.rs` and move `DELTA_SCAN_CONFIG_VERSION` out of `df_plugin_common`. The generic host should have no `deltalake` dependency.

8. **Add plugin load tracing** (P24, small): Instrument `load_plugin` with a tracing span so plugin load events appear in the operational trace.
