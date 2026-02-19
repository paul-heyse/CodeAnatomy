# Design Review Synthesis: Cross-Codebase Analysis

**Date:** 2026-02-18
**Scope:** Full codebase (all 11 component reviews)
**Reviews Synthesized:** 11 component design reviews, 2026-02-18
**Total Files Reviewed:** ~1,059 files across Python and Rust
**Principles Evaluated:** All 24 (P1-P24)

---

## Executive Summary

The CodeAnatomy codebase demonstrates strong architectural intent: the four-stage pipeline, inference-driven scheduling, byte-span canonicalization, and graceful degradation are well-conceived and broadly implemented. The core pipeline stages (Extraction, Semantic, Task/Plan, CPG) are individually coherent, and several subsystems — most notably the orchestration spine (`relspec/`) and the Python infrastructure utilities — score at or near ceiling on the design principles scale.

However, three systemic failures cut across nearly every layer: **DRY violations** (P7 scored below 3 in 10 of 11 reviews, with one score of 0), **Command-Query Separation failures** (P11 violations confirmed in five reviews), and **cross-language contract drift** (the Python-Rust boundary has the single worst per-principle score in the entire deployment — P7=0 — with the same schema repeated across four files in two languages). The DataFusion UDF/views/catalog layer (reviewed in Agent 6) is the highest-risk subsystem: nine of the twenty-four principles scored 1/3, including a silent duplicate function definition that causes one registration call to be permanently skipped. Two correctness defects require immediate attention regardless of refactoring priority: the `DeltaDeleteRequest.predicate` empty-string bypass and the `extra_constraints` silent drop at the Rust mutation layer.

The recommended sequence is: (1) patch the two correctness defects, (2) eliminate the duplicate `register_rust_udfs` and consolidate the CQS violations in the UDF layer, (3) resolve the cross-language contract duplication to a single authoritative source, (4) unify the per-call `Runtime::new()` anti-pattern across Rust FFI sites, and (5) systematically address the DRY violations in the extraction builders and the OTel config proliferation. The five large-file God-module violations should be addressed as a final structural phase once the contract cleanup is stable.

---

## Cross-Cutting Themes

### Theme 1: DRY Violations as Systemic Failure (P7 — 10/11 Reviews)

Knowledge duplication is the most pervasive design failure in the codebase, appearing in every layer from Rust constants to Python schema definitions to OTel configuration.

**Evidence across reviews:**

- **Agent 11 (cross-language boundary), P7=0/3:** Snapshot key schema (14 keys) enumerated in 4 locations: `src/datafusion_engine/datafusion_extension/extension_runtime.py:617-635`, `extension_runtime.py:854-868`, `extension_runtime.py:231-248`, and `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs:186-260`. Version constants `contract_version=3` hardcoded in `extension_runtime.py:169`, `udf_registration.rs:424`, and `rust/df_plugin_codeanatomy/src/lib.rs:171`. ABI version constants (`_EXPECTED_PLUGIN_ABI_MAJOR/MINOR` in Python, `DF_PLUGIN_ABI_MAJOR/MINOR` in Rust) maintained independently with no cross-language enforcement.

- **Agent 6 (UDF/views/catalog), P7=1/3:** `create_strict_catalog` and `create_default_catalog` at `src/datafusion_engine/catalog/metadata.py:868-902` are byte-for-byte identical. Duplicate `register_rust_udfs` definition at `src/datafusion_engine/datafusion_extension/extension_runtime.py:480-509` silently shadows first definition, causing the first registration path to be permanently skipped.

- **Agent 7 (delta/plan/lineage/io), P7=1/3:** `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` duplicated between `src/datafusion_engine/delta/write_pipeline.py:56-64` and `src/datafusion_engine/delta/write_execution.py:7-25`. Sixteen request types repeat `table_ref` property verbatim across `src/datafusion_engine/delta/control_plane_types.py:59-426`.

- **Agent 4 (Rust core engine), P7=1/3:** Topological sort duplicated between `rust/codeanatomy_engine/src/compiler/scheduling.rs:225-271` and `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:171-248` with divergent tie-breaking behavior. `enforce_pushdown_contracts` duplicated verbatim at `rust/codeanatomy_engine/src/compiler/compile_phases.rs:131` and `rust/codeanatomy_engine/src/executor/pipeline.rs:389`.

- **Agent 5 (DF session/runtime), P7=1/3:** Three independent bool-coercion implementations in `src/extract/scip/config.py`, `src/datafusion_engine/delta/file_pruning.py`, and `src/extract/options.py`, all diverging from canonical `src/utils/value_coercion.coerce_bool`.

- **Agent 9 (Python infra), P5=1/3:** OTel configuration duplicated across `OtelConfig`, `OtelConfigSpec`, `OtelConfigOverrides`, `OtelBootstrapOptions`, and `ObservabilityOptions` — five separate types representing the same concern.

- **Agent 2 (extraction pipeline), P7=1/3:** ~1,500 LOC structural duplication across builders files (`src/extract/ast/builders.py:926-964`, `src/extract/bytecode/builders.py`, `src/extract/cst/builders.py`, `src/extract/symtable_extract.py`, `src/extract/tree_sitter/builders.py`). `SpanSpec` at `src/extract/coordination/context.py:174` duplicates `SpanTemplateSpec` at `src/extract/row_builder.py:21`.

- **Agent 1 (semantics), P7=1/3:** Confidence constants duplicated between `src/semantics/joins/inference.py:49-55` and `src/semantics/ir_pipeline.py:1088-1093`.

- **Agent 8 (Rust DF extensions), P7=2/3:** `extract_session_ctx` duplicated between `rust/datafusion_python/src/utils.rs:35-42` and `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs:64-80`.

**Root cause:** No single authoritative location discipline for constants, schemas, or utility functions. Both Python and Rust layers evolve independently without a cross-language schema source-of-truth.

---

### Theme 2: Command-Query Separation Failures (P11 — 5/11 Reviews)

Functions that both return information and mutate state create subtle ordering dependencies and untestable behavior.

**Evidence across reviews:**

- **Agent 6 (UDF/views/catalog):** `IntrospectionCache.snapshot` property at `src/datafusion_engine/udf/introspection.py:346-363` clears `_invalidated` on read. `ParamTableRegistry.datafusion_tables` at `src/datafusion_engine/tables/param.py:195-208` registers tables as a side effect of a getter. `RegistrySchemaProvider.table` at `src/datafusion_engine/catalog/registry_schema.py` triggers installation on access.

- **Agent 5 (DF session/runtime):** `session_context()` at `src/datafusion_engine/session/runtime.py:439-461` mutates an internal cache while returning the context value.

- **Agent 1 (semantics):** `get_or_register` at `src/semantics/compiler.py:615-631` conditionally registers while returning the registered value.

- **Agent 7 (delta/plan/lineage):** Several delta request handlers mutate pipeline state during result construction.

- **Agent 8 (Rust DF extensions):** `install_policy_rules` and `install_physical_rules` in `rust/datafusion_ext/src/planner_rules.rs:100-105` and `rust/datafusion_ext/src/physical_rules.rs:106-115` have no idempotency guard, creating registration mutation on repeated reads.

**Root cause:** The catalog and registry layer was built around "lazy initialization" patterns that blend queries and mutations. The pattern propagated from the core catalog into UDF, session, and extension layers.

---

### Theme 3: Observability Gaps at Rust Boundaries (P24 — 4/11 Reviews)

Structured observability is strong in the Python layer but nearly absent in Rust bridge and extension code.

**Evidence across reviews:**

- **Agent 4 (Rust core engine), P24=1/3:** All OTel instrumentation gated behind `#[cfg(feature = "tracing")]` at `rust/codeanatomy_engine/src/executor/tracing/mod.rs:18-99`. No log fallback when feature is disabled. Zero `#[instrument]` annotations on most executor functions.

- **Agent 8 (Rust DF extensions), P24=1/3:** Only 4 of the many bridge functions in `rust/datafusion_python/src/` have `#[instrument]`. No structured error context on the most common failure paths (`helpers.rs`, `delta_mutations.rs`).

- **Agent 10 (Rust plugins/bindings), P23=1/3:** Zero `#[cfg(test)]` blocks in `rust/codeanatomy_engine_py/` binding crate. No integration test for the plugin registration path.

- **Agent 3 (orchestration spine), P24=2/3:** No OTel spans in `src/relspec/policy_compiler.py:102-167` or `src/relspec/inferred_deps.py:165-234`, which are the most critical scheduling functions in the codebase.

**Root cause:** Rust extension crates were built with a "wire it up first" approach that deferred instrumentation. The optional `tracing` feature gate in the core engine creates a binary choice between instrumented and dark execution.

---

### Theme 4: Global Mutable State and Module-Level Singletons (P16 — 4/11 Reviews)

State captured at import time or stored in global/module-level variables breaks determinism, makes tests unreliable, and prevents concurrent execution.

**Evidence across reviews:**

- **Agent 5 (DF session/runtime):** `_DDL_CATALOG_WARNING_STATE: dict[str, bool] = {"emitted": False}` at `src/datafusion_engine/session/runtime_extensions.py:69` cannot be reset between test runs.

- **Agent 7 (delta/plan/lineage):** `Path.cwd()` evaluated at module load in `src/datafusion_engine/delta/artifact_store_constants.py:18`, `src/datafusion_engine/delta/cache/inventory.py:42`, and `src/datafusion_engine/delta/obs_table_manager.py:38`. `_OBS_SESSION_ID` UUID captured at import time in `src/obs/diagnostics_bridge.py:37`.

- **Agent 2 (extraction pipeline):** `@cache` on `_delta_write_ctx` at `src/extract/orchestrator.py:193-198` — a `functools.cache` on a function returning a stateful Delta Lake context means the first caller's config permanently owns the context for the process.

- **Agent 10 (Rust plugins):** `global_task_ctx_provider` at `rust/df_plugin_codeanatomy/src/task_context.rs:6-10` uses `OnceLock<Arc<SessionContext>>` — a hidden global with no configuration injection point, bypassing the DI system entirely.

**Root cause:** Convenience-driven initialization patterns (module-level constants, `@cache` decorators on stateful factories) that are appropriate for pure values but not for stateful resources with lifecycle requirements.

---

### Theme 5: God-File Concentration (P3/SRP — 5/11 Reviews)

Several files have grown to 800-1,200+ LOC and handle multiple distinct concerns, increasing coupling and reducing testability.

**Evidence across reviews:**

- **Agent 6 (UDF/views/catalog):** `src/datafusion_engine/datafusion_extension/extension_runtime.py` at 1,221 lines covers UDF registration, session bootstrap, introspection, schema validation, and runtime extension in a single file.

- **Agent 7 (delta/plan/lineage):** `src/datafusion_engine/delta/control_plane_types.py` at 1,214 lines contains 16 request types with repeated `table_ref` boilerplate.

- **Agent 5 (DF session/runtime):** `src/datafusion_engine/session/runtime.py` handles session creation, caching, schema negotiation, and config resolution in one module.

- **Agent 8 (Rust DF extensions):** `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` at 794 lines mixes session manipulation, context creation, and bridge utilities.

- **Agent 4 (Rust core engine):** `rust/codeanatomy_engine/src/compiler/plan_compiler.rs` contains both plan compilation logic and an independent topological sort that conflicts with `scheduling.rs`.

**Root cause:** Organic growth without extraction discipline. Each addition to these files was locally reasonable; no trigger caused decomposition to be considered until the files became unwieldy.

---

### Theme 6: Cross-Language Contract Drift (P7/P22 — Python-Rust Boundary)

The Python-Rust FFI boundary has no single authoritative schema definition. Constants, struct layouts, and protocol versions are duplicated across files in both languages with no mechanical enforcement of consistency.

**Evidence across reviews:**

- **Agent 11 (cross-language boundary):** 14-key snapshot schema in 4 locations; `contract_version=3` in 3 locations; `runtime_install_mode="unified"` in 3 locations; `.pyi` stub missing 7 functions present in the Rust `#[pyfunction]` implementations.

- **Agent 8 (Rust DF extensions):** `extra_constraints` silently dropped at `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:183` (`let _ = request.extra_constraints`) — a cross-language API field ignored on the Rust side with no Python-side indication.

- **Agent 10 (Rust plugins):** ABI version declared separately in Python (`_EXPECTED_PLUGIN_ABI_MAJOR/MINOR`) and Rust (`DF_PLUGIN_ABI_MAJOR/MINOR`) with no build-time enforcement of equivalence.

**Root cause:** PyO3 binding generation does not enforce type-level contracts across the boundary. The `.pyi` stubs are written manually and have drifted. No codegen or validation step enforces schema parity.

---

## Ranked Findings

Rankings use **Risk × (1/Effort)** heuristic. Critical = immediate correctness risk; High = near-term stability risk; Medium = design debt; Low = maintenance burden.

### Critical — Correctness Defects

| ID | Finding | File | Risk | Effort | Principle |
|----|---------|------|------|--------|-----------|
| C1 | `DeltaDeleteRequest.predicate: str` accepts empty string silently, producing unconstrained deletes | `src/datafusion_engine/delta/control_plane_types.py:146-166` | High | Small | P8, P10 |
| C2 | `extra_constraints` silently dropped with `let _ = request.extra_constraints` | `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:183` | High | Small | P8, P21 |
| C3 | Duplicate `register_rust_udfs` — second definition silently shadows first, first registration path permanently skipped | `src/datafusion_engine/datafusion_extension/extension_runtime.py:480-509` | High | Small | P7, P3 |
| C4 | Two topological sorts with divergent tie-breaking — scheduling non-determinism under tie conditions | `rust/codeanatomy_engine/src/compiler/scheduling.rs:225-271` vs `plan_compiler.rs:171-248` | High | Medium | P7, P18 |

### High — Stability and Design Failures

| ID | Finding | File | Risk | Effort | Principle |
|----|---------|------|------|--------|-----------|
| H1 | `IntrospectionCache.snapshot` mutates `_invalidated` on read (CQS violation) | `src/datafusion_engine/udf/introspection.py:346-363` | High | Small | P11 |
| H2 | `ParamTableRegistry.datafusion_tables` registers as side-effect of getter (CQS violation) | `src/datafusion_engine/tables/param.py:195-208` | High | Small | P11 |
| H3 | `@cache` on stateful `_delta_write_ctx` — first caller's config permanently owns context | `src/extract/orchestrator.py:193-198` | High | Small | P16, P17 |
| H4 | `global_task_ctx_provider` OnceLock bypasses DI entirely at Rust plugin boundary | `rust/df_plugin_codeanatomy/src/task_context.rs:6-10` | High | Medium | P12, P16 |
| H5 | `ExecutionPackageArtifact.created_at_unix_ms` included in composite fingerprint — breaks determinism | `src/relspec/execution_package.py:217` | Medium | Small | P18 |
| H6 | Per-call `Runtime::new()` at 8+ FFI bridge sites — each call creates a new Tokio runtime (~40ms) | `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:129-132` (and 7+ others) | High | Medium | P19, P16 |
| H7 | `Path.cwd()` at module load — breaks test isolation, fails on directory change | `src/datafusion_engine/delta/artifact_store_constants.py:18`, `cache/inventory.py:42`, `delta/obs_table_manager.py:38` | Medium | Small | P16, P17 |
| H8 | P7=0 — snapshot key schema (14 keys) across 4 locations in 2 languages | `extension_runtime.py:617-635,854-868,231-248`; `udf_registration.rs:186-260` | High | Medium | P7, P22 |
| H9 | `create_strict_catalog` and `create_default_catalog` byte-for-byte identical | `src/datafusion_engine/catalog/metadata.py:868-902` | Medium | Small | P7, P3 |
| H10 | Mutable caches inside `frozen=True` struct — frozen guarantee violated | `src/datafusion_engine/session/runtime.py:196-204` | Medium | Small | P10, P16 |

### Medium — Design Debt

| ID | Finding | File | Risk | Effort | Principle |
|----|---------|------|------|--------|-----------|
| M1 | ~1,500 LOC structural duplication across extraction builders | `src/extract/ast/builders.py`, `bytecode/builders.py`, `cst/builders.py`, `symtable_extract.py`, `tree_sitter/builders.py` | Medium | Large | P7, P3 |
| M2 | OTel config duplicated across 5 Python types | `OtelConfig`, `OtelConfigSpec`, `OtelConfigOverrides`, `OtelBootstrapOptions`, `ObservabilityOptions` | Medium | Medium | P7, P5 |
| M3 | `contract_version=3` hardcoded in 3 cross-language locations | `extension_runtime.py:169`, `udf_registration.rs:424`, `lib.rs:171` | Medium | Small | P7, P22 |
| M4 | `.pyi` stub missing 7 functions present in Rust `#[pyfunction]` impls | `rust/datafusion_python/` binding stubs | Medium | Small | P22, P21 |
| M5 | `install_policy_rules`/`install_physical_rules` not idempotent — double registration possible | `rust/datafusion_ext/src/planner_rules.rs:100-105`, `physical_rules.rs:106-115` | Medium | Small | P17 |
| M6 | `_DDL_CATALOG_WARNING_STATE` unresetable global — test isolation broken | `src/datafusion_engine/session/runtime_extensions.py:69` | Medium | Small | P16, P17 |
| M7 | All OTel in Rust core engine behind `#[cfg(feature = "tracing")]` with no log fallback | `rust/codeanatomy_engine/src/executor/tracing/mod.rs:18-99` | Medium | Medium | P24 |
| M8 | `enforce_pushdown_contracts` duplicated verbatim in two Rust files | `rust/codeanatomy_engine/src/compiler/compile_phases.rs:131`, `executor/pipeline.rs:389` | Medium | Small | P7 |
| M9 | `IntervalAlignProvider::build_dataframe` creates bare `SessionContext::new()` — bypasses SessionFactory | `rust/codeanatomy_engine/src/providers/interval_align_provider.rs:108-116` | Medium | Medium | P6 |
| M10 | `session_context()` mutates cache while returning value (CQS violation) | `src/datafusion_engine/session/runtime.py:439-461` | Medium | Small | P11 |
| M11 | Six-mixin inheritance on `DataFusionRuntimeProfile` | `src/datafusion_engine/session/runtime.py:158-166` | Medium | Large | P13 |
| M12 | `RegistrationPhase(validate=...)` where `validate` callback performs installation — naming inversion | `src/datafusion_engine/udf/` | Medium | Small | P21 |
| M13 | `udf_parity_report` stub always returns empty — silently broken contract | `src/datafusion_engine/udf/parity.py:109-127` | Medium | Small | P8 |
| M14 | `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` duplicated between two delta write files | `src/datafusion_engine/delta/write_pipeline.py:56-64`, `write_execution.py:7-25` | Low | Small | P7 |
| M15 | `get_or_register` in semantics compiler mixes command and query | `src/semantics/compiler.py:615-631` | Medium | Small | P11 |
| M16 | No OTel spans in `policy_compiler.py` or `inferred_deps.py` | `src/relspec/policy_compiler.py:102-167`, `relspec/inferred_deps.py:165-234` | Medium | Small | P24 |
| M17 | `_OBS_SESSION_ID` UUID fixed at import time — breaks multi-run observability | `src/obs/diagnostics_bridge.py:37` | Low | Small | P17 |

### Low — Maintenance Burden

| ID | Finding | File | Risk | Effort | Principle |
|----|---------|------|------|--------|-----------|
| L1 | Three independent bool-coercion implementations diverge from canonical `coerce_bool` | `src/extract/scip/config.py`, `datafusion_engine/delta/file_pruning.py`, `extract/options.py` | Low | Small | P7 |
| L2 | `CalibrationMode` string literals leak into return type signature | `src/relspec/policy_calibrator.py:162-177` | Low | Small | P1, P22 |
| L3 | `SpanSpec` at context.py duplicates `SpanTemplateSpec` at row_builder.py | `src/extract/coordination/context.py:174`, `src/extract/row_builder.py:21` | Low | Small | P7 |
| L4 | `delta_write.py` imports 3 private symbols from `delta_read.py` | `src/datafusion_engine/delta/delta_write.py:29` | Low | Small | P1 |
| L5 | Confidence threshold constants duplicated between inference and ir_pipeline | `src/semantics/joins/inference.py:49-55`, `src/semantics/ir_pipeline.py:1088-1093` | Low | Small | P7 |
| L6 | `caps::RELATION_PLANNER` declared in plugin API with no export slot in `DfPluginExportsV1` | `rust/df_plugin_api/` | Low | Small | P20 |
| L7 | No ABI changelog or stability policy for plugin boundary | `rust/df_plugin_api/`, `rust/df_plugin_host/` | Low | Medium | P22 |
| L8 | `inspect.signature` duck-typing at orchestrator for runtime compatibility detection | `src/extract/orchestrator.py:229-240,342,378` | Low | Medium | P9 |
| L9 | Pydantic `_RuntimeProfileEnvPatchRuntime` in msgspec-canonical extraction module | `src/extract/runtime_profile.py:95-114` | Low | Small | P4 |
| L10 | `obs/diagnostics.py` imports Rust extension as side-effect at module load | `src/obs/diagnostics.py` | Low | Small | P5 |

---

## Dependency Chain Map

Some fixes enable or simplify other fixes. The chains below are ordered: completing an upstream fix makes the downstream fix cheaper or possible.

```
Chain A: Cross-Language Contract Consolidation
  H8 (snapshot key schema → single authoritative source)
    enables M3 (contract_version constant → derive from schema version)
    enables M4 (.pyi stub completeness → generate from authoritative schema)
    enables C2 (extra_constraints drop → detectable once schema is typed end-to-end)

Chain B: Rust Runtime Singleton
  H4 (global_task_ctx_provider → inject SessionFactory via DI)
    enables H6 (per-call Runtime::new() → shared runtime from injected factory)
    enables M9 (IntervalAlignProvider bare SessionContext → use injected factory)
    enables M7 (OTel behind feature flag → always present once runtime is injectable)

Chain C: UDF Layer CQS Cleanup
  C3 (duplicate register_rust_udfs → remove shadowing definition)
    enables H1 (IntrospectionCache.snapshot → split into query + command)
    enables H2 (ParamTableRegistry.datafusion_tables → split into query + command)
    enables M10 (session_context() → split cache write from value return)

Chain D: Delta Write Determinism
  H7 (Path.cwd() at module load → inject working directory)
    enables H5 (ExecutionPackageArtifact.created_at_unix_ms → remove from fingerprint)
    enables M6 (_DDL_CATALOG_WARNING_STATE → thread-local or injected)
    enables M17 (_OBS_SESSION_ID → inject at session creation, not import)

Chain E: Extraction Builder DRY
  M1 (builders duplication → shared base pattern or template)
    enables L3 (SpanSpec/SpanTemplateSpec → consolidate using shared base)
    enables L8 (inspect.signature duck-typing → typed protocol, detectable from shared base)

Chain F: OTel Unification
  M2 (OTel config 5 types → single OtelConfig + extension points)
    enables M16 (missing spans in relspec → trivial to add once config is unified)
    enables M7 (OTel feature-gated in Rust → align to unified Python OTel config)
```

---

## Python-Rust Alignment Gaps

This section consolidates all findings specific to the cross-language FFI boundary.

### Gap 1: Snapshot Key Schema in Four Locations (P7=0)

The 14-key snapshot key schema is the authoritative cross-language contract for UDF registration metadata. It currently exists as independent Python dicts in three locations and a Rust struct in one:

- `src/datafusion_engine/datafusion_extension/extension_runtime.py:617-635` — build snapshot
- `src/datafusion_engine/datafusion_extension/extension_runtime.py:854-868` — validate snapshot
- `src/datafusion_engine/datafusion_extension/extension_runtime.py:231-248` — deserialize snapshot
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs:186-260` — construct snapshot

**Recommended fix:** Define the canonical schema as a `msgspec.Struct` on the Python side and a `#[pyclass]` on the Rust side (or a plain dict with a `SNAPSHOT_KEYS: frozenset[str]` sentinel). Generate or derive the Rust side from the Python source. A build-time test asserting key parity would catch drift.

### Gap 2: Silent `extra_constraints` Drop (P8, P21)

`rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:183` contains:
```rust
let _ = request.extra_constraints;
```

This silently discards a field that callers on the Python side believe is honored. There is no error, no log line, and no Python-side indication that the constraints were ignored. Any Python caller relying on `extra_constraints` for correctness gets silent data corruption.

**Recommended fix:** Either implement `extra_constraints` processing, or return an explicit Python-side error (`PyValueError`) when `extra_constraints` is non-empty until implementation is complete. Remove `let _ =` which signals intentional discard.

### Gap 3: ABI Version Mirroring (P22)

`_EXPECTED_PLUGIN_ABI_MAJOR = 1` and `_EXPECTED_PLUGIN_ABI_MINOR = 0` are declared in Python source. `DF_PLUGIN_ABI_MAJOR: u32 = 1` and `DF_PLUGIN_ABI_MINOR: u32 = 0` are declared independently in Rust. No CI check enforces parity.

**Recommended fix:** Expose the Rust constants via `#[pyfunction]` or `pyo3_build_config` and have the Python layer import them directly. A single `test_abi_version_parity.py` that asserts `_EXPECTED_PLUGIN_ABI_MAJOR == rust_module.ABI_MAJOR` would catch future drift at test time.

### Gap 4: Missing .pyi Stubs (P22, P21)

Seven functions present in the Rust `#[pyfunction]` implementations have no corresponding entry in the `.pyi` stub files. This means Python callers get no type checking or IDE completion for these entry points.

**Recommended fix:** Add a CI check (`python -m mypy --ignore-missing-imports` on the stubs, or `stubtest`) that enforces stub completeness against the compiled extension. Add the seven missing stubs immediately.

### Gap 5: Per-Call `Runtime::new()` (P19, P16)

Eight or more bridge functions in `rust/datafusion_python/src/codeanatomy_ext/` call `Runtime::new()` directly inside the function body. Each call creates a new Tokio runtime (estimated 20-40ms per call). A shared runtime already exists in `datafusion_ext::async_runtime` but is not used consistently.

**Evidence locations:** `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:129-132` plus seven or more additional sites in `delta_mutations.rs`, `schema_evolution.rs`, `rust_pivot.rs`, `session_utils.rs`, `udf_registration.rs`.

**Recommended fix:** Create a module-level `static SHARED_RUNTIME: OnceLock<Runtime>` in `datafusion_ext::async_runtime` (or reuse the existing one) and route all bridge functions through it. The `global_task_ctx_provider` (Gap H4) should be refactored simultaneously to use the same shared runtime.

---

## Comparison with 2026-02-17 Reviews

The 2026-02-17 reviews (`docs/reviews/design_review_*_2026-02-17.md`) are no longer present on disk (deleted in working tree per git status). This comparison is based on the file names visible in git status and the structural patterns implied by the review scope alignment.

**Coverage expansion:** The 2026-02-18 deployment reviewed 11 component scopes vs. an implied similar set in 2026-02-17. The 2026-02-18 deployment added the cross-language boundary as an explicit 11th scope (`design_review_df_schema_arrow_encoding`), which produced the only P7=0 score in the entire deployment. This suggests the cross-language gap was either not surfaced or not treated as a distinct concern in the prior cycle.

**Rust scope coverage:** The 2026-02-17 deployment included `design_review_rust_core_engine`, `design_review_rust_df_extensions_plugins`, and `design_review_rust_python_bindings` as separate scopes. The 2026-02-18 deployment maintains this separation, adding the cross-language boundary as a distinct analytical unit. This is the correct direction: the FFI boundary deserves dedicated review coverage.

**Modified files (git status):** The Rust files listed as modified in git status (`rust/codeanatomy_engine/src/compiler/plan_compiler.rs`, `scheduling.rs`, `executor/delta_writer.rs`, `providers/interval_align_provider.rs`, `rust/datafusion_ext/src/planner_rules.rs`, `rust/datafusion_ext/src/udf_registry.rs`, `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs`, etc.) represent the DataFusion 52 transition work. Several findings in the 2026-02-18 review cycle (topological sort duplication, per-call Runtime, planner rule non-idempotency) are likely newly introduced or exacerbated by this transition and should be treated as regression candidates.

**Trend assessment:** Without the 2026-02-17 scores available for direct numerical comparison, the 2026-02-18 results establish a baseline. The lowest-scoring review (Agent 6, UDF/views/catalog: nine principles at 1/3) and the P7=0 cross-language boundary finding are the most important benchmarks to track in the next review cycle. If the DataFusion 52 transition stabilizes and the correctness defects are patched, the next cycle should see improvement in P7, P11, and P17 scores.

---

## Prioritized Action Plan

Actions ordered by (1) correctness risk, (2) fix-enables-fixes dependency chains, (3) effort efficiency. Each action references the chain it belongs to and the principles it addresses.

### Phase 1: Correctness Patches (all Small effort, complete within 1 sprint)

**Action 1 — Reject empty predicate in DeltaDeleteRequest** (C1)
- File: `src/datafusion_engine/delta/control_plane_types.py:146-166`
- Change: Add `__post_init__` or `msgspec.Struct` validator rejecting `predicate == ""`; raise `ValueError` with message identifying the field
- Principles: P8, P10
- Unblocks: nothing directly, but eliminates highest-risk correctness gap

**Action 2 — Remove `extra_constraints` silent drop in Rust** (C2)
- File: `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:183`
- Change: Replace `let _ = request.extra_constraints;` with either (a) actual implementation or (b) `if !request.extra_constraints.is_empty() { return Err(PyValueError::new_err("extra_constraints not yet supported")) }`
- Principles: P8, P21
- Unblocks: Chain A (Gap 1 schema consolidation)

**Action 3 — Remove duplicate `register_rust_udfs`** (C3)
- File: `src/datafusion_engine/datafusion_extension/extension_runtime.py:480-509`
- Change: Identify which definition is authoritative, delete the shadowed one, add a comment or test asserting there is exactly one registration path
- Principles: P7, P3
- Unblocks: Chain C (CQS cleanup in UDF layer)

**Action 4 — Unify topological sort in Rust compiler** (C4)
- Files: `rust/codeanatomy_engine/src/compiler/scheduling.rs:225-271`, `compiler/plan_compiler.rs:171-248`
- Change: Extract a single `topological_sort_deterministic(graph: &PyDiGraph) -> Vec<NodeIndex>` function in `scheduling.rs`; have `plan_compiler.rs` call it; ensure tie-breaking is stable (e.g., by node ID)
- Principles: P7, P18
- Unblocks: Chain B (runtime factory injection)

### Phase 2: High-Impact CQS and State Fixes (mostly Small effort, complete within 1-2 sprints)

**Action 5 — Split `IntrospectionCache.snapshot` into query + command** (H1)
- File: `src/datafusion_engine/udf/introspection.py:346-363`
- Change: Rename property to `current_snapshot` (pure read); add `clear_invalidated()` command method; update all callers
- Principles: P11, P21
- Depends on: Action 3 (UDF layer stabilized)

**Action 6 — Split `ParamTableRegistry.datafusion_tables`** (H2)
- File: `src/datafusion_engine/tables/param.py:195-208`
- Change: Separate the registration side-effect into an explicit `register_tables(session)` command; make the property return already-registered tables only
- Principles: P11

**Action 7 — Replace `@cache` on `_delta_write_ctx`** (H3)
- File: `src/extract/orchestrator.py:193-198`
- Change: Convert to an explicit instance attribute initialized in `__init__`; inject the Delta context configuration; add a `reset_delta_context()` method for test cleanup
- Principles: P16, P17
- Unblocks: Chain D (working-directory injection cascade)

**Action 8 — Inject working directory, eliminate `Path.cwd()` at module load** (H7)
- Files: `src/datafusion_engine/delta/artifact_store_constants.py:18`, `cache/inventory.py:42`, `delta/obs_table_manager.py:38`
- Change: Convert module-level `Path.cwd()` constants to functions that accept an optional `root: Path | None = None` parameter; inject from caller
- Principles: P16, P17
- Depends on: Chain D (Action 7 creates the injection point)

**Action 9 — Fix mutable caches inside `frozen=True` struct** (H10)
- File: `src/datafusion_engine/session/runtime.py:196-204`
- Change: Extract cache fields to a separate `DataFusionSessionCache` non-frozen `msgspec.Struct`; compose into `DataFusionRuntimeProfile`; document the mutability contract
- Principles: P10, P16

**Action 10 — Split `session_context()` CQS violation** (M10)
- File: `src/datafusion_engine/session/runtime.py:439-461`
- Change: Separate into `get_session_context() -> SessionContext | None` (pure) and `ensure_session_context() -> SessionContext` (creates + caches if absent)
- Principles: P11
- Depends on: Action 9 (cache struct extracted)

**Action 11 — Remove unresetable `_DDL_CATALOG_WARNING_STATE` global** (M6)
- File: `src/datafusion_engine/session/runtime_extensions.py:69`
- Change: Replace module-level dict with a parameter on the function that uses it (or a `warnings.warn` with a filter); add a test that confirms warning fires on each fresh call
- Principles: P16, P17

### Phase 3: Cross-Language Contract Consolidation (Medium effort, 2-3 sprints)

**Action 12 — Single authoritative snapshot key schema** (H8)
- Files: `extension_runtime.py:617-635,854-868,231-248`; `udf_registration.rs:186-260`
- Change: Define `SNAPSHOT_SCHEMA: Final[frozenset[str]]` in a dedicated `src/datafusion_engine/datafusion_extension/snapshot_schema.py`; derive all three Python usages from it; expose schema keys to Rust via a Python-callable or shared constant file; add a parity test
- Principles: P7, P22
- Depends on: Action 3 (UDF layer stable)
- Unblocks: Actions 13, 14, 15

**Action 13 — Derive `contract_version` from schema** (M3)
- Files: `extension_runtime.py:169`, `udf_registration.rs:424`, `lib.rs:171`
- Change: Derive version from the authoritative snapshot schema (e.g., hash of key set or explicit `SCHEMA_VERSION` constant adjacent to `SNAPSHOT_SCHEMA`); import in all three locations
- Principles: P7, P22
- Depends on: Action 12

**Action 14 — Complete .pyi stubs** (M4)
- Files: Rust `datafusion_python` binding stubs
- Change: Add 7 missing function stubs; add `stubtest` or equivalent to CI; gate PR merge on stub completeness
- Principles: P22, P21

**Action 15 — Enforce ABI version parity** (L7 / Gap 3)
- Files: Python ABI constants, Rust ABI constants
- Change: Expose Rust constants via `#[pyfunction]`; Python constants become assertions against Rust values; add `test_abi_version_parity.py`
- Principles: P22, P7

### Phase 4: Rust Runtime and DI Fixes (Medium effort, 1-2 sprints)

**Action 16 — Shared Tokio runtime for all FFI bridge functions** (H6)
- Files: `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:129-132` (and 7+ others)
- Change: Route all `Runtime::new()` calls through `datafusion_ext::async_runtime::shared_runtime()` or a `OnceLock<Runtime>` module-level singleton; add `async_runtime` as a required Cargo dependency where missing
- Principles: P19, P16
- Depends on: Action 4 (runtime ownership clarified)

**Action 17 — Inject `SessionFactory` into `global_task_ctx_provider`** (H4)
- File: `rust/df_plugin_codeanatomy/src/task_context.rs:6-10`
- Change: Replace `OnceLock<Arc<SessionContext>>` with an injectable `SessionFactory` trait; pass factory at plugin initialization from the host; remove the global
- Principles: P12, P16
- Depends on: Action 16 (shared runtime available)

**Action 18 — Make rule installation idempotent** (M5)
- Files: `rust/datafusion_ext/src/planner_rules.rs:100-105`, `physical_rules.rs:106-115`
- Change: Add a `HashSet<TypeId>` guard in the session state; check before registering each rule; log a warning if already registered rather than silently re-registering
- Principles: P17
- Depends on: Action 16 (runtime stable)

**Action 19 — Inject `SessionFactory` into `IntervalAlignProvider`** (M9)
- File: `rust/codeanatomy_engine/src/providers/interval_align_provider.rs:108-116`
- Change: Accept `Arc<dyn SessionFactory>` in `IntervalAlignProvider::new()`; replace `SessionContext::new()` with factory call
- Principles: P6, P12
- Depends on: Action 17 (factory injection pattern established)

### Phase 5: DRY and OTel Unification (mix of Small and Large efforts)

**Action 20 — Unify OTel config to single type** (M2)
- Files: All five OTel config types across `obs/`, `datafusion_engine/`, etc.
- Change: Define canonical `OtelConfig` in `src/obs/config.py`; derive all variant types from it via field subsets or extension; update all construction sites
- Principles: P7, P5
- Effort: Medium

**Action 21 — Add OTel spans to `policy_compiler` and `inferred_deps`** (M16)
- Files: `src/relspec/policy_compiler.py:102-167`, `src/relspec/inferred_deps.py:165-234`
- Change: Add `with tracer.start_as_current_span("relspec.policy_compiler.compile")` wrappers; add span attributes for input counts and output node counts
- Principles: P24
- Depends on: Action 20 (OTel config unified)
- Effort: Small

**Action 22 — Remove OTel feature flag in Rust core engine** (M7)
- File: `rust/codeanatomy_engine/src/executor/tracing/mod.rs:18-99`
- Change: Make `tracing` a non-optional dependency; replace `#[cfg(feature = "tracing")]` guards with unconditional `#[instrument]` annotations; add `log` fallback for environments without OTel subscriber
- Principles: P24
- Depends on: Action 20 (OTel config unified, provides guidance)
- Effort: Medium

**Action 23 — Extract shared builder abstraction for extraction builders** (M1)
- Files: `src/extract/ast/builders.py`, `bytecode/builders.py`, `cst/builders.py`, `symtable_extract.py`, `tree_sitter/builders.py`
- Change: Identify the ~1,500 LOC structural duplicate; extract to `src/extract/shared/base_builder.py`; have each concrete builder inherit or compose from the base; update tests
- Principles: P7, P3
- Effort: Large

**Action 24 — Consolidate bool-coercion to canonical `coerce_bool`** (L1)
- Files: `src/extract/scip/config.py`, `src/datafusion_engine/delta/file_pruning.py`, `src/extract/options.py`
- Change: Replace all three local implementations with imports of `src/utils/value_coercion.coerce_bool`; add a test asserting no other bool-coercion implementations exist (pattern test)
- Principles: P7
- Effort: Small

**Action 25 — Consolidate delta retry markers** (M14)
- Files: `src/datafusion_engine/delta/write_pipeline.py:56-64`, `write_execution.py:7-25`
- Change: Move `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` to `src/datafusion_engine/delta/retry_policy.py`; import in both locations
- Principles: P7
- Effort: Small

---

## Scorecard Summary (24 Principles × 11 Reviews)

Scores: 0=Active violation, 1=Significant gaps, 2=Mostly aligned, 3=Well aligned, N/A=Not applicable

| # | Principle | Agent 1 Semantics | Agent 2 Extraction | Agent 3 Relspec | Agent 4 Rust Core | Agent 5 DF Session | Agent 6 UDF/Views | Agent 7 Delta/IO | Agent 8 Rust DF | Agent 9 Infra | Agent 10 Plugins | Agent 11 XLang | Min | Avg |
|---|-----------|:-----------------:|:-----------------:|:---------------:|:-----------------:|:------------------:|:-----------------:|:----------------:|:---------------:|:-------------:|:----------------:|:--------------:|:---:|:---:|
| 1 | Information hiding | 3 | 2 | 3 | 2 | 2 | 1 | 2 | 2 | 1 | 2 | 2 | 1 | 2.1 |
| 2 | Separation of concerns | 3 | 2 | 3 | 2 | 2 | 1 | 2 | 2 | 2 | 2 | 2 | 1 | 2.2 |
| 3 | SRP | 3 | 1 | 3 | 2 | 2 | 1 | 2 | 2 | 2 | 2 | 1 | 1 | 2.1 |
| 4 | High cohesion / low coupling | 2 | 2 | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.1 |
| 5 | Dependency direction | 3 | 2 | 3 | 2 | 3 | 2 | 2 | 2 | 1 | 2 | 2 | 1 | 2.2 |
| 6 | Ports & Adapters | 3 | 2 | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.2 |
| 7 | DRY | 1 | 1 | 3 | 1 | 1 | 1 | 1 | 2 | 2 | 2 | 0 | 0 | 1.4 |
| 8 | Design by contract | 2 | 2 | 2 | 2 | 2 | 1 | 2 | 2 | 2 | 2 | 2 | 1 | 2.0 |
| 9 | Parse, don't validate | 3 | 2 | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.2 |
| 10 | Make illegal states unrepresentable | 3 | 2 | 3 | 2 | 1 | 1 | 1 | 2 | 2 | 2 | 2 | 1 | 2.1 |
| 11 | CQS | 1 | 2 | 3 | 2 | 1 | 1 | 2 | 2 | 2 | 2 | 2 | 1 | 1.8 |
| 12 | Dependency inversion | 3 | 2 | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 2 | 1 | 2.1 |
| 13 | Composition over inheritance | 3 | 3 | 3 | 2 | 1 | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 2.2 |
| 14 | Law of Demeter | 2 | 2 | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 1 | 2.0 |
| 15 | Tell, don't ask | 2 | 2 | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 1 | 2.0 |
| 16 | Functional core / imperative shell | 3 | 2 | 3 | 2 | 1 | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 2.1 |
| 17 | Idempotency | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.1 |
| 18 | Determinism / reproducibility | 2 | 2 | 2 | 1 | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 2.0 |
| 19 | KISS | 3 | 2 | 3 | 2 | 2 | 1 | 2 | 2 | 2 | 2 | 2 | 1 | 2.1 |
| 20 | YAGNI | 3 | 2 | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.2 |
| 21 | Least astonishment | 3 | 2 | 3 | 2 | 2 | 1 | 2 | 2 | 2 | 2 | 2 | 1 | 2.1 |
| 22 | Declare and version public contracts | 3 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 1 | 1 | 1.9 |
| 23 | Design for testability | 3 | 1 | 3 | 2 | 2 | 2 | 1 | 2 | 3 | 1 | 2 | 1 | 2.0 |
| 24 | Observability | 3 | 2 | 2 | 1 | 3 | 2 | 2 | 1 | 3 | 2 | 2 | 1 | 2.0 |
| | **Overall Avg** | **2.7** | **1.9** | **2.8** | **1.9** | **1.9** | **1.6** | **1.9** | **2.0** | **2.0** | **1.9** | **1.8** | | **2.0** |

**Column key:**
- Agent 1: `src/semantics/` (103 files)
- Agent 2: `src/extract/` + `src/extraction/` (81 files)
- Agent 3: `src/relspec/` + `src/obs/` + `src/cli/` + `src/utils/` (24 files reviewed)
- Agent 4: `rust/codeanatomy_engine/` (130 files)
- Agent 5: `src/datafusion_engine/session/` + related (82 files)
- Agent 6: `src/datafusion_engine/udf/` + `views/` + `catalog/` + `compile/` + `expr/` + `sql/` + `tables/` (43 files)
- Agent 7: `src/datafusion_engine/delta/` + `plan/` + `lineage/` + `io/` + `arrow/` (132 files)
- Agent 8: `rust/datafusion_python/` + `rust/datafusion_ext/` (189 files)
- Agent 9: `src/obs/` + `src/utils/` + `src/cli/` + `src/storage/` + `src/schema_spec/` + related (136 files)
- Agent 10: `rust/df_plugin_api/` + `rust/df_plugin_host/` + `rust/df_plugin_common/` + `rust/df_plugin_codeanatomy/` + binding crates (22 files)
- Agent 11: Cross-language boundary files (17 files)

**Principle summary by average:**
- Highest average: P9 Parse/don't validate (2.2), P6 Ports & Adapters (2.2), P13 Composition (2.2), P20 YAGNI (2.2)
- Lowest average: P7 DRY (1.4), P11 CQS (1.8), P22 Versioned contracts (1.9)
- Only P7=0 score: Agent 11 (cross-language boundary)
- Most principles at 1: Agent 6 (nine principles scored 1/3)
- Strongest review: Agent 1 (semantics, avg 2.7) and Agent 3 (relspec, avg 2.8)
- Weakest review: Agent 6 (UDF/views/catalog, avg 1.6)

---

*Synthesis prepared from 11 component design reviews conducted 2026-02-18 across approximately 1,059 Python and Rust source files.*
