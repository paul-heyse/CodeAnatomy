# Design Review: Python-Rust Cross-Language Boundary

**Date:** 2026-02-18
**Scope:** Cross-cutting — Python-Rust bridge files on both sides
**Focus:** Principles P1-15 (Boundaries, Knowledge, Composition) with special attention to P1, P5, P7, P9, P12-13
**Depth:** deep
**Files reviewed:** 17

## Files Analyzed

### Python side
- `src/extraction/rust_session_bridge.py`
- `src/datafusion_engine/extensions/datafusion_ext.py`
- `src/datafusion_ext.pyi` (stub file, 118 function signatures)
- `src/datafusion_engine/plan/substrait_artifacts.py`
- `src/datafusion_engine/udf/extension_runtime.py` (1,220 lines)
- `src/datafusion_engine/obs/metrics_bridge.py` (re-named in scope: `src/datafusion_engine/delta/control_plane_provider.py`)
- `src/obs/engine_metrics_bridge.py`
- `src/datafusion_engine/delta/control_plane_provider.py`
- `src/datafusion_engine/extensions/required_entrypoints.py`
- `src/datafusion_engine/errors.py`

### Rust side
- `rust/codeanatomy_engine_py/src/lib.rs`
- `rust/datafusion_ext_py/src/lib.rs`
- `rust/datafusion_python/src/context.rs` (partial)
- `rust/datafusion_python/src/codeanatomy_ext/mod.rs`
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs`
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs`
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs`
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs`
- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs`
- `rust/datafusion_ext_py/src/lib.rs`
- `rust/df_plugin_api/src/manifest.rs`

---

## Executive Summary

The Python-Rust boundary is architecturally sound in its large-scale structure: Rust owns computation, Python owns orchestration, and a well-defined module facade (`datafusion_ext.py`) provides the single crossing point for most calls. However, five recurring problems erode the boundary quality. First, `SessionContext` unwrapping is duplicated across eleven Python call sites and two Rust extraction patterns, with no shared adapter owning this secret. Second, the snapshot key schema (14 canonical keys including `scalar`, `aggregate`, `volatility`, `signature_inputs`) is separately enumerated in Python validation code, the Rust serializer, and the Python stub type — a tri-location schema violation. Third, the protocol constants `contract_version=3` and `runtime_install_mode="unified"` are independently hardcoded in both languages. Fourth, the `.pyi` stub file is materially incomplete: at least seven functions exposed by the Rust extension (`install_codeanatomy_policy_config`, `install_codeanatomy_physical_config`, `install_expr_planners`, `parquet_listing_table_provider`, `schema_evolution_adapter_factory`, `registry_catalog_provider_factory`, `install_codeanatomy_udf_config`) are absent from the stub, undermining its value as a type contract. Fifth, `control_plane_provider.py` contains literal duplicate docstrings for three functions, suggesting a copy-paste merge accident that was never resolved.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | SessionContext unwrapping secret lives in 11+ call sites across Python and 2 in Rust |
| 2 | Separation of concerns | 2 | small | low | `extension_runtime.py` mixes snapshot validation, ABI checking, alias resolution, and audit logic |
| 3 | SRP (one reason to change) | 1 | medium | medium | `extension_runtime.py` (1,220 lines) has at least five distinct responsibilities |
| 4 | High cohesion, low coupling | 2 | small | low | Extension facade is narrow; snapshot normalization scattered across two Python modules |
| 5 | Dependency direction | 2 | small | low | Python correctly depends on Rust via stable entrypoints; minor inversion in stub |
| 6 | Ports & Adapters | 2 | small | low | `datafusion_ext.py` is a clean port; fallback stub mechanism is consistent |
| 7 | DRY (knowledge, not lines) | 0 | medium | high | Snapshot key schema, contract version, ABI constants, and session unwrapping each live in multiple places |
| 8 | Design by contract | 2 | small | low | Boundary validation is thorough; validate-then-parse pattern exists but is inconsistent |
| 9 | Parse, don't validate | 1 | medium | medium | Python side validates Rust responses repeatedly rather than parsing into typed structs once |
| 10 | Make illegal states unrepresentable | 2 | small | low | `AsyncUdfPolicy` is well-modeled; snapshot shape is not |
| 11 | CQS | 2 | small | low | `register_dataset_provider` registers AND returns; intent is clear but worth noting |
| 12 | Dependency inversion + explicit composition | 2 | small | low | Extension loading is injected; module fallback logic is explicit |
| 13 | Prefer composition over inheritance | 3 | — | — | No inheritance at the boundary; composition used throughout |
| 14 | Law of Demeter | 1 | medium | medium | `getattr(ctx, "ctx", None)` chains traversal across 11 Python sites and 2 Rust sites |
| 15 | Tell, don't ask | 1 | medium | medium | Python repeatedly interrogates raw mapping payloads rather than calling typed methods on boundary objects |

---

## Detailed Findings

### Category: Boundaries (P1–P6)

#### P1. Information hiding — Alignment: 1/3

**Current state:**
The `SessionContext` unwrapping logic — the decision that a context may arrive as either a native `SessionContext` or as a wrapper exposing `.ctx` — is duplicated at eleven call sites in Python and two Rust sites. No single module owns this secret.

**Findings:**
- Python unwrapping scattered across: `src/datafusion_engine/extensions/datafusion_ext.py:85-86` (`_normalize_ctx`), `src/extraction/rust_session_bridge.py:45`, `src/datafusion_engine/extensions/runtime_capabilities.py:333`, `src/datafusion_engine/extensions/schema_evolution.py:75`, `src/datafusion_engine/udf/extension_runtime.py:106`, `src/datafusion_engine/udf/factory.py:267`, `src/datafusion_engine/delta/capabilities.py:99,203,231`, `src/datafusion_engine/expr/planner.py:125`, `src/datafusion_engine/session/runtime_extensions.py:97`.
- Rust unwrapping: `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:35-42` (`extract_session_ctx`) and `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs:64-80` (`session_context_contract`) each independently implement the same "try direct extract, fall back to `.ctx` attribute" pattern.
- `src/datafusion_engine/extensions/datafusion_ext.py` defines `_normalize_ctx` but also separately normalizes `df` in `_normalize_args:107-110`, encoding two parallel "wrapper-object" secrets in different functions.

**Suggested improvement:**
Extract a single `BoundaryContext` adapter class (Python) with a `from_any(ctx)` classmethod that encapsulates the unwrapping rule, raising a typed `BoundaryContextError` on failure. All call sites become `BoundaryContext.from_any(ctx).native`. On the Rust side, `extract_session_ctx` in `helpers.rs` is already the canonical extractor; `session_context_contract` in `session_utils.rs:64-80` should be eliminated and all local callers redirected to `extract_session_ctx`.

**Effort:** medium
**Risk if unaddressed:** medium — When DataFusion introduces a new wrapper type (DF 53+), all eleven Python sites and both Rust patterns must be updated consistently.

---

#### P2. Separation of concerns — Alignment: 2/3

**Current state:**
The extension facade in `datafusion_ext.py` is well-separated: it handles module loading, context normalization, and attribute dispatch without leaking domain logic. The `datafusion_engine/obs/metrics_bridge.py` module is notably clean, focusing purely on translating Rust `RunResult` payloads to OpenTelemetry instruments.

**Findings:**
- `src/datafusion_engine/udf/extension_runtime.py` mixes: ABI version checking (lines 82-84, 131-149), snapshot schema validation (lines 854-972), alias resolution (lines 724-741), audit reporting (lines 549-580), hash/serialization (lines 1161-1182), and session-cache management (lines 1065-1083). These are distinct concerns that happen to share the same module due to incremental growth.
- `src/obs/engine_metrics_bridge.py` is correctly scoped and separated — it delegates to `summarize_runtime_execution_metrics` for computation and handles only the OTel emission. This is a positive example of the principle working.

**Suggested improvement:**
Extract ABI checking into `datafusion_engine/udf/extension_abi.py`, snapshot schema validation and coercion into `datafusion_engine/udf/snapshot_validation.py`, and hash/serialization into `datafusion_engine/udf/snapshot_serialization.py`. `extension_runtime.py` would then become a thin orchestrator that calls these modules.

**Effort:** medium
**Risk if unaddressed:** low — This is a maintainability issue rather than a correctness risk.

---

#### P3. SRP (one reason to change) — Alignment: 1/3

**Current state:**
`src/datafusion_engine/udf/extension_runtime.py` at 1,220 lines is the boundary's God module. It changes when: the ABI version changes, the snapshot schema gains a new key, alias resolution rules change, async policy validation changes, hash algorithm changes, or session-cache eviction logic changes. Each is a distinct reason.

**Findings:**
- ABI version constants `_EXPECTED_PLUGIN_ABI_MAJOR = 1` and `_EXPECTED_PLUGIN_ABI_MINOR = 1` at lines 82-83 belong to an ABI compatibility subsystem, not a snapshot management module.
- `_empty_registry_snapshot()` at line 617, `_mutable_mapping()` at line 638, `_require_sequence()` at line 645, `_require_mapping()` at line 653, and `_require_bool_mapping()` at line 661 are schema utility helpers that would logically live in a dedicated schema module.
- `rust_udf_snapshot_bytes()` at line 1161 and `rust_udf_snapshot_hash()` at line 1173 are serialization/hashing concerns separate from the validation concerns immediately above them.
- `validate_runtime_capabilities()` at line 448 and `capability_report()` at line 457 are compatibility aliases that add confusion about which function is canonical.

**Suggested improvement:**
Decompose into: `extension_abi.py` (ABI version checking, `_EXPECTED_PLUGIN_ABI_*` constants), `snapshot_schema.py` (schema keys, `_empty_registry_snapshot`, `_require_*` helpers), `snapshot_validation.py` (`validate_rust_udf_snapshot`, `validate_required_udfs`), `snapshot_serialization.py` (`rust_udf_snapshot_bytes`, `rust_udf_snapshot_hash`). `extension_runtime.py` retains only session registration, orchestration, and public API delegation.

**Effort:** large
**Risk if unaddressed:** medium — Growing this file further will make targeted changes increasingly risky.

---

#### P4. High cohesion, low coupling — Alignment: 2/3

**Current state:**
The module structure is reasonably coherent. The `datafusion_ext.py` wrapper has a clear purpose. The Rust `codeanatomy_ext/` submodules are well-separated by feature domain (`delta_mutations`, `schema_evolution`, `udf_registration`, `session_utils`, `registry_bridge`).

**Findings:**
- `src/datafusion_engine/obs/metrics_bridge.py` (labeled in the scope as a metrics bridge) is actually the dataset statistics and Parquet telemetry module — its name implies a metrics bridge but its content is largely PyArrow dataset utilities. This naming mismatch reduces cohesion legibility.
- The snapshot normalization responsibilities are split between `extension_runtime.py` and `src/datafusion_engine/udf/runtime_snapshot_types.py`, coupling two files to the same evolving schema.

**Suggested improvement:**
Rename `src/datafusion_engine/obs/metrics_bridge.py` to `src/datafusion_engine/obs/dataset_statistics.py` to match its content. Consolidate snapshot normalization responsibilities in a single module.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction — Alignment: 2/3

**Current state:**
The dependency direction is generally correct: Python orchestration code depends on Rust via stable named entrypoints, and the Rust extension does not import Python modules. The `datafusion_ext.py` wrapper maintains the expected direction.

**Findings:**
- `rust/datafusion_ext_py/src/lib.rs:11` imports `datafusion._internal` at runtime using PyO3's `py.import()`, which means the Rust crate depends on the Python `datafusion` wheel at runtime. This is the correct direction for a PyO3 binding, but it means the Rust code's correctness is governed by the Python `datafusion` package's internal module structure — an upstream dependency on a private surface (`._internal`).
- `src/datafusion_engine/extensions/datafusion_ext.py:35-38` falls back to `test_support.datafusion_ext_stub` when `datafusion._internal` is missing. Production code depending on a test support module (even conditionally) inverts the test-to-production direction.

**Suggested improvement:**
Promote the stub to a proper `datafusion_engine.extensions.fallback_stub` module under `src/datafusion_engine/extensions/` with explicit versioning, so the fallback dependency runs in the correct direction (production code → production stub) without importing from `test_support`.

**Effort:** small
**Risk if unaddressed:** low — The fallback path is only exercised during testing, but the module import direction is structurally incorrect.

---

#### P6. Ports & Adapters — Alignment: 2/3

**Current state:**
`src/datafusion_engine/extensions/datafusion_ext.py` is effectively a port adapter: it defines the surface Python callers use, hides module resolution, context normalization, and fallback, and provides named methods like `install_codeanatomy_runtime()` and `build_extraction_session()`. The design is consistent with this pattern.

**Findings:**
- The required entrypoint list in `src/datafusion_engine/extensions/required_entrypoints.py` (21 entrypoints) is the port contract expressed as a Python tuple of strings. This is adequate but fragile: it is not type-checked against the Rust implementation. A typo in either the tuple or the Rust `register_functions` call is only caught at load time.
- `src/datafusion_ext.pyi` (118 lines) provides a type stub for the adapter surface, but it is incomplete — see P7 findings. An incomplete stub degrades the port contract.

**Suggested improvement:**
The port contract (`REQUIRED_RUNTIME_ENTRYPOINTS`) should be cross-validated against the Rust module's `__all__` at import time rather than relying only on the load-time check. A test that imports the extension and asserts all `REQUIRED_RUNTIME_ENTRYPOINTS` are callable provides the missing test-time contract enforcement.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (P7–P11)

#### P7. DRY (knowledge, not lines) — Alignment: 0/3

**Current state:**
This is the most severely violated principle at the boundary. The same semantic invariants are encoded independently in multiple places across both languages. Four distinct duplication sites were confirmed:

**Findings:**

**Site 1 — Snapshot key schema (highest severity):**
The 14-key snapshot schema (`scalar`, `aggregate`, `window`, `table`, `aliases`, `parameter_names`, `volatility`, `rewrite_tags`, `signature_inputs`, `return_types`, `simplify`, `coerce_types`, `short_circuits`, `config_defaults`) is separately enumerated in:
- `src/datafusion_engine/udf/extension_runtime.py:617-635` (`_empty_registry_snapshot`)
- `src/datafusion_engine/udf/extension_runtime.py:854-868` (`_validate_required_snapshot_keys`)
- `src/datafusion_engine/udf/extension_runtime.py:231-248` (the `.setdefault` block in `_normalize_registry_snapshot`)
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs:186-260` (the PyDict serializer in `registry_snapshot_py`)

If a new key is added to the Rust registry snapshot, it must be manually added in four locations. The Python `.pyi` stub's `Mapping[str, object]` return type for `registry_snapshot` provides zero cross-language schema enforcement.

**Site 2 — Protocol constants:**
`contract_version = 3` appears at:
- `src/datafusion_engine/udf/extension_runtime.py:169` (`payload_mapping.setdefault("contract_version", 3)`)
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs:424` (`payload.set_item("contract_version", 3)`)
- `rust/codeanatomy_engine_py/src/lib.rs:171` (in the `run_build` response JSON)

`runtime_install_mode = "unified"` appears at:
- `src/datafusion_engine/udf/extension_runtime.py:170`
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs:425`
- `src/test_support/datafusion_ext_stub.py:676`

**Site 3 — ABI version constants:**
`_EXPECTED_PLUGIN_ABI_MAJOR = 1` and `_EXPECTED_PLUGIN_ABI_MINOR = 1` at `src/datafusion_engine/udf/extension_runtime.py:82-83` mirror `DF_PLUGIN_ABI_MAJOR: u16 = 1` and `DF_PLUGIN_ABI_MINOR: u16 = 1` in `rust/df_plugin_api/src/manifest.rs:4-5`. The Rust side is authoritative (it defines the ABI); the Python side hardcodes the expected value independently.

**Site 4 — SessionContext unwrapping rule:**
The rule "if value has a `.ctx` attribute, unwrap it" is independently encoded in Python at 11 call sites and in Rust at 2 locations (see P1 findings).

**Site 5 — UDF category iteration:**
The pattern of iterating `("scalar", "aggregate", "window", "table")` appears in:
- `src/datafusion_engine/udf/extension_runtime.py:715-722` (`_snapshot_names`)
- `src/datafusion_engine/udf/extension_runtime.py:854-867` (`_validate_required_snapshot_keys` — required list)
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs:29-34` (registry hash computation)

**Suggested improvement:**
1. Define the snapshot key schema as a shared Rust `const` in `datafusion_ext/src/snapshot_schema.rs` and re-export it. On the Python side, generate or derive the key list from the Rust msgpack deserialization schema (`runtime_snapshot_types.py`) rather than listing keys independently.
2. Move `contract_version` and `runtime_install_mode` to a shared Rust constant (e.g., `datafusion_ext::protocol::CONTRACT_VERSION: u32 = 3`) and expose them to Python via a dedicated `protocol_constants()` entrypoint.
3. Remove `_EXPECTED_PLUGIN_ABI_MAJOR/MINOR` from Python and instead read the authoritative value from `capabilities_snapshot()["plugin_abi"]` at startup. The Python side should _compare_ against what Rust reports, not independently define what it expects.

**Effort:** medium
**Risk if unaddressed:** high — Schema drift is silent: a new snapshot key on the Rust side will silently pass through Python validation as an unknown field while the Python normalization block will zero-initialize it with the wrong default.

---

#### P8. Design by contract — Alignment: 2/3

**Current state:**
Boundary validation is generally thorough. Rust pyfunction bodies validate inputs at entry using `parse_python_payload` (which uses `serde::Deserialize`), and Python wrappers validate the response type before returning. The `validate_rust_udf_snapshot` function provides an explicit postcondition check.

**Findings:**
- `src/datafusion_engine/udf/extension_runtime.py:160-162`: `except Exception as exc` at line 160 is too broad — it catches all exceptions during msgpack deserialization and converts them to `TypeError`. This destroys the exception chain's specificity. The comment `# pragma: no cover - defensive decode wrapper` indicates it is not tested, meaning the contract is implicit.
- `src/datafusion_engine/delta/control_plane_provider.py:104-115` and `168-183` and `215-228` each have literal duplicate docstrings — the same docstring text appears twice consecutively for `delta_provider_with_files`, `delta_cdf_provider`, and `delta_snapshot_info`. This is a copy-paste artifact that should be corrected.
- The `delta_snapshot_info` function at line 230-241 catches `(RuntimeError, TypeError, ValueError)` generically, which is appropriate at the boundary but loses error kind information.

**Suggested improvement:**
Replace the `except Exception` at `extension_runtime.py:160` with `except (ValueError, TypeError, struct.error) as exc` or the msgpack-specific exception type. Remove the duplicate docstrings in `control_plane_provider.py`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate — Alignment: 1/3

**Current state:**
Rust boundary functions correctly parse incoming Python payloads into typed Rust structs via `parse_python_payload` (JSON round-trip through `serde::Deserialize`). However, on the Python side, Rust responses are returned as `Mapping[str, object]` and are validated repeatedly at each consumption site rather than being parsed into typed Python objects once at the boundary.

**Findings:**
- `src/datafusion_engine/udf/extension_runtime.py:208-270` (`_normalize_registry_snapshot`) performs extensive `isinstance` checks, `.get()` calls with defaults, and `.setdefault` injections on a raw mapping. This is a parse-at-boundary opportunity: a `msgspec.Struct` for the registry snapshot would replace all 62 lines of normalization with a single `msgspec.json.decode(...)` call using `coerce=True`.
- `src/obs/engine_metrics_bridge.py:40-146` extracts operator metrics, trace summaries, and runtime capabilities from a raw `Mapping[str, object]` using nested `.get()` chains. Each extraction is a re-validation of the shape already known to Rust. A typed `RunResultEnvelope` struct would parse once and provide typed access.
- `src/datafusion_engine/plan/substrait_artifacts.py:53-68` manually checks `isinstance(raw, (bytes, bytearray))` and `isinstance(raw, list)` for the `substrait_bytes` field. Rust could instead guarantee the bytes type and eliminate the branch.
- `src/extraction/rust_session_bridge.py:43-48` (`build_extraction_session`) checks if the returned object is a `SessionContext` or has a `.ctx` attribute — a shape validation that happens every call because the return type is `Any`. If Rust guaranteed `PySessionContext` as the return type, this check disappears.

**Suggested improvement:**
Define `msgspec.Struct` types for the three primary Rust response envelopes: `RegistrySnapshotPayload`, `RuntimeInstallPayload`, and `RunResultEnvelope`. Parse these once in the wrapper functions in `datafusion_ext.py` before returning to callers. The 14-key snapshot schema then becomes a single authoritative type definition.

**Effort:** medium
**Risk if unaddressed:** medium — Scattered validation accumulates technical debt and creates inconsistency: one caller may check a field another caller silently ignores.

---

#### P10. Make illegal states unrepresentable — Alignment: 2/3

**Current state:**
`AsyncUdfPolicy` at `src/datafusion_engine/udf/extension_runtime.py:73-79` is a correctly modeled frozen dataclass that makes the three-field async policy explicit. The `_async_udf_policy()` constructor at line 1185 validates invariants before constructing it.

**Findings:**
- The registry snapshot's 14 keys are not enforced by a type; they are a `Mapping[str, object]` that callers must check manually. The type alias `RustUdfSnapshot = Mapping[str, object]` at line 70 provides naming but no structural guarantee.
- `src/datafusion_engine/extensions/datafusion_ext.py:18-24`: `_DF52_PROVIDER_METHODS` as a `frozenset[str]` of method names is a valid use of sets for membership testing, but the `_normalize_args` function that consumes it at line 89-121 has complex conditional logic that would be cleaner as a typed dispatch table.

**Suggested improvement:**
Replace `RustUdfSnapshot = Mapping[str, object]` with a `msgspec.Struct` (or at minimum a `TypedDict`) that names and types each of the 14 fields. This makes the illegal state (missing `volatility` key) a parse error rather than a runtime check.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P11. CQS — Alignment: 2/3

**Current state:**
Most boundary functions are either queries (return data) or commands (side-effecting registration). The distinction is generally respected.

**Findings:**
- `src/extraction/rust_session_bridge.py:52-68` (`register_dataset_provider`) registers a provider with the session (command) and also returns a `Mapping[str, object]` payload (query). This is appropriate at the bridge layer where the caller needs confirmation, but the function name implies pure command.
- `src/extraction/rust_session_bridge.py:71-80` (`register_dataset_provider_payload`) is a trivially thin alias for `register_dataset_provider` — it adds no value and creates API ambiguity about which function to call. One caller in `src/datafusion_engine/dataset/resolution.py:171` uses the alias exclusively.

**Suggested improvement:**
Remove `register_dataset_provider_payload` and have `resolution.py` call `register_dataset_provider` directly. If the CQS split is desired in future, separate into `register_dataset_provider()` (command, no return) and `dataset_provider_payload()` (query, returns mapping).

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (P12–P15)

#### P12. Dependency inversion + explicit composition — Alignment: 2/3

**Current state:**
Extension loading in `datafusion_ext.py` is explicit: `_load_internal_module()` resolves the concrete module at startup, and the result is injected as module-level state. The `ExtensionRegistries` dataclass in `extension_runtime.py:46-63` is a clean injectable container for Rust UDF session state.

**Findings:**
- `src/datafusion_engine/udf/extension_runtime.py:293-308` (`_datafusion_internal()`) and `src/datafusion_engine/udf/extension_runtime.py:311-322` (`_extension_module_with_capabilities()`) both perform `importlib.import_module(EXTENSION_MODULE_PATH)` and check for the presence of specific attributes. This is effectively the same module-loading logic duplicated. Both functions are called independently, meaning the import resolution is not shared.
- `src/datafusion_engine/udf/extension_runtime.py:477` has a module-level import at the bottom of the file: `from datafusion_engine.udf.extension_ddl import _register_udf_aliases, _register_udf_specs`. This late import at module load time circumvents the normal top-of-file import order, hiding a circular dependency risk.

**Suggested improvement:**
Consolidate `_datafusion_internal()` and `_extension_module_with_capabilities()` into a single `_resolve_extension_module(required_attr: str)` function that is called lazily per use case. Move the late import of `extension_ddl` to the top of the file or to the function body that uses it.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance — Alignment: 3/3

**Current state:**
No inheritance hierarchies exist at the Python-Rust boundary. Rust uses trait implementations (`TableProvider`, `PhysicalOptimizerRule`) which are Rust's composition mechanism. Python uses dataclasses and protocol types. The `CpgTableProvider` wrapper in `schema_evolution.rs:44-138` composes over an inner `Arc<dyn TableProvider>` cleanly.

No action needed.

---

#### P14. Law of Demeter — Alignment: 1/3

**Current state:**
The repeated `getattr(ctx, "ctx", None)` pattern across 11 Python call sites and 2 Rust functions is a textbook Demeter violation: Python code accesses the `.ctx` attribute of a context wrapper to get the real context, navigating one level into the collaborator's internals.

**Findings:**
- `src/datafusion_engine/delta/capabilities.py:99`: `internal_ctx = getattr(ctx, "ctx", None)` — this module should not know that `SessionContext` wrappers expose `.ctx`.
- `src/datafusion_engine/extensions/datafusion_ext.py:85-86`: `_normalize_ctx` is the only place that acknowledges this is a "secret" by wrapping it in a function. All other 10 call sites access `.ctx` directly.
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs:70`: `if let Ok(inner) = ctx.getattr("ctx")` — the Rust code knows about Python's wrapper object structure.
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:39`: `let inner = ctx.getattr("ctx")?` — a second independent Rust implementation of the same unwrapping rule.

**Suggested improvement:**
Create a single `adapt_session_context(ctx: Any) -> SessionContext` function in `src/datafusion_engine/session/adapter.py` (Python) and eliminate all direct `.ctx` accesses outside this function. On the Rust side, `extract_session_ctx` in `helpers.rs` is already the canonical extractor; `session_context_contract` in `session_utils.rs:64-80` should delegate to `extract_session_ctx` rather than reimplementing the logic.

**Effort:** medium
**Risk if unaddressed:** medium — The DataFusion Python wrapper hierarchy has already changed between DF 50 and DF 52; each change requires auditing all 13 `.ctx` access sites.

---

#### P15. Tell, don't ask — Alignment: 1/3

**Current state:**
Python code extensively interrogates raw `Mapping[str, object]` payloads returned from Rust instead of asking the boundary object to perform operations. The boundary returns raw data and the caller reassembles meaning.

**Findings:**
- `src/obs/engine_metrics_bridge.py:40-100`: The `_record_trace_summary_metrics` function accesses `run_result.get("trace_metrics_summary")`, then `trace_summary.get("elapsed_compute_nanos")`, then `trace_summary.get("warning_count_total")` — three levels of "ask" before taking any action. A typed `TraceMetricsSummary` struct would encapsulate these fields and allow `emit_trace_metrics(summary)` directly.
- `src/datafusion_engine/udf/extension_runtime.py:380-408` (`extension_capabilities_report`): navigates `snapshot.get("plugin_abi")`, then `plugin_abi.get("major")`, then `plugin_abi.get("minor")` to assemble compatibility information. The ABI object should tell the caller whether it is compatible rather than exposing raw fields.
- `src/datafusion_engine/plan/substrait_artifacts.py:53-68`: extracts `artifact.get("substrait_bytes")` and then branches on its type. The artifact object should expose a `substrait_bytes() -> bytes` method that handles the type coercion internally.

**Suggested improvement:**
Define `msgspec.Struct` response types with methods or properties that encapsulate the common access patterns. For example, `CapabilitiesSnapshot.is_compatible() -> bool` instead of the 8-line compatibility check in `extension_capabilities_report`. `ArtifactPayload.substrait_bytes_required() -> bytes` instead of the multi-branch check in `substrait_artifacts.py:59-68`.

**Effort:** medium
**Risk if unaddressed:** medium — Callers accumulate defensive "ask" logic that diverges over time as the payload schema evolves.

---

## Cross-Cutting Themes

### Theme 1: The Untyped Mapping Anti-Pattern

All five principle gaps in the Knowledge and Composition categories share a single root cause: Rust returns `PyDict` (a `Mapping[str, object]` in Python) and Python then re-validates, re-parses, and re-queries the same structure at multiple consumption sites. This is the boundary's dominant structural weakness. The fix is a shared set of `msgspec.Struct` types for the three primary response envelopes (registry snapshot, runtime install payload, and run result), parsed once at the crossing point. This resolves P7 (schema duplication), P9 (parse-don't-validate), P14 (Demeter), and P15 (tell-don't-ask) simultaneously.

**Affected principles:** P7, P9, P14, P15
**Root cause:** PyO3 returns `PyAny`/`PyDict` by necessity; Python side chose not to define typed response objects.

### Theme 2: The `.ctx` Unwrapping Secret

The knowledge that a session context may arrive as a wrapper exposing `.ctx` is encoded independently 13 times across both languages. This violates P1, P7, and P14 simultaneously. One canonical `adapt_session_context` function in Python and delegation to `helpers::extract_session_ctx` in Rust would reduce the 13 sites to 2 authoritative implementations.

**Affected principles:** P1, P7, P14
**Root cause:** DF 52 wrapper objects were introduced without refactoring existing call sites to a single adapter.

### Theme 3: Stub Incompleteness and Snapshot Schema Drift

`src/datafusion_ext.pyi` is the declared public contract for the extension surface, but it omits at least seven callable functions that the Rust module registers (`install_codeanatomy_policy_config`, `install_codeanatomy_physical_config`, `install_expr_planners`, `parquet_listing_table_provider`, `schema_evolution_adapter_factory`, `registry_catalog_provider_factory`, `install_codeanatomy_udf_config`). Callers of these functions are silently typed as `Any -> Any` rather than using the actual signatures. Additionally, all functions returning registry snapshots have `Mapping[str, object]` return types in the stub, providing no schema documentation for the 14-key envelope.

**Affected principles:** P6, P7, P22 (not in scope but worth noting)
**Root cause:** The stub was not regenerated after new Rust functions were added.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P3/P7 (SRP/DRY) | Remove duplicate docstrings in `control_plane_provider.py:104-115`, `168-183`, `215-228` | small | Correctness signal — reveals the copy-paste nature of these functions |
| 2 | P11 (CQS) | Remove `register_dataset_provider_payload` alias (`rust_session_bridge.py:71-80`); update `resolution.py:171` | small | Removes API ambiguity with zero behavioral change |
| 3 | P5 (Dep direction) | Move test stub fallback from `test_support.datafusion_ext_stub` to `datafusion_engine.extensions.fallback_stub` | small | Corrects production-to-test-support dependency inversion |
| 4 | P8 (Contract) | Replace `except Exception` at `extension_runtime.py:160` with `except (ValueError, TypeError)` | small | Preserves exception chain specificity at the msgpack boundary |
| 5 | P7 (DRY) | Remove `_EXPECTED_PLUGIN_ABI_MAJOR/MINOR` from Python; read authoritative values from `capabilities_snapshot()["plugin_abi"]` instead | small | Eliminates one of the four confirmed DRY violations |

---

## Recommended Action Sequence

The findings cluster into three tiers:

**Tier 1 — Immediate corrections (no design change required):**
1. Remove duplicate docstrings from `src/datafusion_engine/delta/control_plane_provider.py` (three functions affected). (P8)
2. Remove `register_dataset_provider_payload` from `src/extraction/rust_session_bridge.py`; update `src/datafusion_engine/dataset/resolution.py:171` to call `register_dataset_provider` directly. (P11)
3. Replace `except Exception` at `extension_runtime.py:160` with a specific exception type. (P8)
4. Eliminate `_EXPECTED_PLUGIN_ABI_MAJOR/MINOR` constants from Python; derive from `capabilities_snapshot()["plugin_abi"]`. (P7)
5. Regenerate or extend `src/datafusion_ext.pyi` to include the seven missing Rust functions. (P6)

**Tier 2 — Structural improvements (contain the Demeter/unwrapping violation):**
6. Create `src/datafusion_engine/session/adapter.py` with `adapt_session_context(ctx: Any) -> SessionContext`; redirect all 11 Python `.ctx` access sites to this function. (P1, P7, P14)
7. Eliminate `session_context_contract` in `session_utils.rs:64-80` and redirect its two callers to `extract_session_ctx`. (P1, P14)
8. Move the test-stub fallback out of `test_support` into `datafusion_engine.extensions.fallback_stub`. (P5)

**Tier 3 — Parse-at-boundary refactor (highest leverage, requires typing infrastructure):**
9. Define `msgspec.Struct` for `RegistrySnapshotPayload` (14 fields) in `src/datafusion_engine/udf/snapshot_schema.py`. Migrate `_normalize_registry_snapshot`, `_empty_registry_snapshot`, and `_validate_required_snapshot_keys` to use it. (P7, P9, P10, P15)
10. Define `msgspec.Struct` for `RuntimeInstallPayload` and parse `install_codeanatomy_runtime`'s return value in `datafusion_ext.py` before returning to callers. (P9, P15)
11. Define `msgspec.Struct` for `RunResultEnvelope` in `src/obs/` and parse Rust `RunResult` responses once in `engine_metrics_bridge.py`. (P9, P15)
12. Decompose `extension_runtime.py` into `extension_abi.py`, `snapshot_schema.py`, `snapshot_validation.py`, and `snapshot_serialization.py` after the typing infrastructure is in place. (P2, P3)

Steps 9-12 depend on each other in order but are independent of steps 1-8. Steps 1-5 can be executed in any order.
