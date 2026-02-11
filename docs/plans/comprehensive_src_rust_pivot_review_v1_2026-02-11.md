# Comprehensive src/ Dead Code & Rust Pivot Review (v1)

**Date:** 2026-02-11
**Scope:** All 20 `src/` modules + 7 top-level `src/*.py` files + 8 Rust crates
**Method:** CQ-driven import census, structural analysis, Rust cross-reference
**Prior work incorporated:** 5 existing plan documents (relspec, engine, cpg, schema_spec, cli, datafusion_engine)

---

## 1) Executive Summary

The CodeAnatomy codebase is mid-migration from Python-owned planning/orchestration/execution to Rust-owned authority. This review systematically assessed all 20 `src/` modules using import census data, structural analysis, and Rust capability cross-referencing.

**Key findings:**
- **3 modules are DEAD or near-dead** in production: `test_support` (0 prod importers), `validation` (5 importers, inlinable)
- **4 modules have existing decommission plans** with concrete delete scope: `relspec`, `planning_engine`, `cpg`, `schema_spec`
- **5 modules are RETAIN** (core, active, no Rust conflict): `extract`, `utils`, `semantics`, `obs`, `storage`
- **3 modules are FACADE/THIN** (orchestration wrappers): `extraction`, `graph`, `runtime_models`
- **All 8 Rust crates are at Complete or Mostly Complete maturity** — Rust authority is production-ready
- **Decommissioned modules** (`engine`, `hamilton_pipeline`, `normalize`, `incremental`) no longer exist on disk

**Consolidated dead code:** 7 immediate-delete files in `relspec`, 6 immediate-delete files in `cpg`/`schema_spec`, plus `test_support` is test-only (0 prod imports).

---

## 2) Methodology

### Evidence Layers

| Layer | Tool | Signal |
|-------|------|--------|
| Import census | `grep` for `from X import` / `import X` across `src/` and `tests/` | Production vs test-only usage |
| Module structure | `glob` + file reads | File counts, LOC, submodule breakdown |
| Rust capability mapping | Crate `src/` inspection, `#[pyclass]`/`#[pyfunction]` grep | PyO3 API surface, Python counterpart overlap |
| Prior plan documents | 5 existing assessments under `docs/plans/` | Pre-validated classifications for 6 modules |

### Classification Taxonomy

| Classification | Definition | Action |
|---------------|-----------|--------|
| **DEAD** | Zero production importers (src:0, tests-only or none) | Immediate delete |
| **MINIMAL** | 1-5 production importers, all callsites movable | Delete after small callsite move |
| **FACADE** | Re-exports only or thin orchestration, no unique logic | Delete after re-pointing imports |
| **RUST-READY** | Active but Rust equivalent exists and is complete | Pivot to Rust, delete Python |
| **RUST-PARTIAL** | Active, Rust has some equivalent, gap exists | Extend Rust, then delete Python |
| **RETAIN** | Active, Python-authoritative, no Rust equivalent feasible | Keep in Python |
| **RETAIN-THIN** | Keep as thin Python adapter over Rust authority | Reduce to adapter layer |

---

## 3) Per-Module Assessments

### 3.1 Previously Assessed Modules (from existing plans)

#### `src/relspec/` — RUST-READY

**Source:** `relspec_decommission_baseline_2026-02-10.md`
**Files:** 14 .py files, 2,632 LOC
**Status:** Active decommission in progress

| File | src imports | tests imports | Classification |
|------|-----------|--------------|---------------|
| `incremental.py` | 0 | 0 | DEAD — immediate delete |
| `counterfactual_replay.py` | 0 | 1 | DEAD — immediate delete |
| `decision_recorder.py` | 0 | 1 | DEAD — immediate delete |
| `fallback_quarantine.py` | 0 | 1 | DEAD — immediate delete |
| `policy_validation.py` | 0 | 1 | DEAD — immediate delete |
| `runtime_artifacts.py` | 0 | 1 | DEAD — immediate delete |
| `execution_authority.py` | 0 | 1 | DEAD — immediate delete |
| `rustworkx_graph.py` | 6 | 14 | Delete after callsite move |
| `rustworkx_schedule.py` | 3 | 4 | Delete after callsite move |
| `inferred_deps.py` | 6 | 8 | Delete after callsite move |
| `evidence.py` | 4 | 6 | Delete after callsite move |
| `inference_confidence.py` | 7 | 6 | Retain until Rust owns confidence |
| `compiled_policy.py` | 5 | 4 | Retain until Rust owns policy |
| `view_defs.py` | 8 | 0 | Retain until views consolidated |

**Recommendation:** Execute 7 immediate deletes. Move remaining callsites to Rust engine.

---

#### `src/planning_engine/` — RETAIN-THIN (residual)

**Source:** `engine_rust_planning_execution_decommission_implementation_plan_v1_2026-02-11.md`
**Files:** 4 remaining .py files (was 20+; bulk decommissioned)
**Production importers:** 13 (across 7 files)

| File | Purpose | Status |
|------|---------|--------|
| `config.py` | Engine config types | Retain or fold into CLI config |
| `output_contracts.py` | CPG output contract aliases | Retain until Rust contract stabilizes |
| `spec_contracts.py` | SemanticExecutionSpec, RuntimeConfig | Retain as Rust boundary types |
| `__init__.py` | Package facade | Keep minimal |

**Recommendation:** Retain as thin contract layer. Evaluate folding `config.py` into `cli/config_models.py`.

---

#### `src/cpg/` — RUST-READY (partial decommission)

**Source:** `cpg_schema_spec_rust_datafusion_pivot_assessment_v1_2026-02-11.md`

**Immediate deletes (Wave A):**
- `relationship_contracts.py` (src:0, tests:1) + test
- `constants.py` (src:1, inlinable) — inline into `spec_registry.py`
- `schemas.py` (src:1) — inline `SCHEMA_VERSION` into `product_build.py`
- `scip_roles.py` (src:1) — inline into `spec_registry.py`
- `contract_map.py` (src:1) — consumed only by `spec_registry`

**Retain until Rust CPG cutover (Wave C):**
- `view_builders_df.py` (src:2) — runtime-critical DataFrame builder
- `spec_registry.py` (src:2+) — view/spec index
- `prop_catalog.py`, `node_families.py`, `kind_catalog.py`, `emit_specs.py`, `specs.py`

**Recommendation:** Execute Wave A deletes. Wave C requires Rust compiler to produce CPG outputs.

---

#### `src/schema_spec/` — RUST-PARTIAL

**Source:** `cpg_schema_spec_rust_datafusion_pivot_assessment_v1_2026-02-11.md`

**Immediate deletes:**
- `dataset_handle.py` (effectively dead, self-referential only)
- `nested_types.py` (src:1, tests:1, no production path)
- `registration.py` (src:2, single meaningful consumer — movable)

**Core retain (high import density):**
- `system.py` (src:39, tests:15) — core authority
- `dataset_spec_ops.py` (src:19, tests:5) — broadly used
- `policies.py` (src:2, tests:1) — used by system

**Prune exports (dead functions within live files):**
- `view_specs.py`: `ViewSpecInputs`, `ViewSpecSqlInputs`, `view_spec_from_builder`, `view_spec_from_sql`
- `file_identity.py`: `file_identity_fields`, `file_identity_fields_for_nesting`, `file_identity_struct`
- `dataset_spec_ops.py`: `dataset_spec_handle`, `dataset_spec_finalize_context`, `dataset_spec_unify_tables`

**Recommendation:** Execute immediate deletes. Prune dead exports. Retain `system.py` and `dataset_spec_ops.py` until Rust exposes typed spec/policy APIs.

---

#### `src/cli/` — RETAIN

**Source:** `cli_datafusion_transition_plan_v1_2026-02-09.md`
**Status:** CLI scaffold is execution-agnostic. Build/plan commands transitioning to Rust engine.

**Retain as-is:** `app.py`, `context.py`, `result.py`, `groups.py`, `validators.py`, `converters.py`, `kv_parser.py`, `path_utils.py`
**Needs rework:** `commands/build.py` (remove Hamilton-specific options), `commands/plan.py` (use Rust compiler)
**Config update:** Remove `[hamilton]` from config template, remove rustworkx from version payload

**Recommendation:** Retain entire module. CLI is the user-facing surface — stays Python.

---

#### `src/datafusion_engine/` — RETAIN-THIN

**Source:** `datafusion_planning_best_in_class_architecture_plan_v1_2026-02-09.md`
**Status:** All 13 v1 scope items implemented in Rust. Python module is adapter layer over Rust DataFusion.

**Recommendation:** Retain as Python adapter. Rust `datafusion_ext` + `datafusion_python` are the authority. Python `datafusion_engine/` configures and delegates.

---

### 3.2 Newly Assessed Modules (CQ-driven)

#### `src/core/` — RETAIN-THIN

**Files:** 3 (`__init__.py`, `config_base.py`, `fingerprinting.py`)
**Production importers:** 29 | **Test importers:** 3

Thin fingerprinting layer providing `FingerprintableConfig` protocol, `config_fingerprint()`, and `CompositeFingerprint`. Heavily used by semantics, relspec, and datafusion_engine for config hashing.

**Dead code:** None
**Rust potential:** None (domain-specific Python semantics)
**Recommendation:** Keep as-is. Small, well-focused, actively imported.

---

#### `src/utils/` — RETAIN

**Files:** 10 (`hashing.py`, `uuid_factory.py`, `env_utils.py`, `registry_protocol.py`, `validation.py`, `value_coercion.py`, `file_io.py`, `storage_options.py`, `schema_from_struct.py`, `__init__.py`)
**Production importers:** 92 | **Test importers:** 15

Highest-usage module in the codebase. Every utility file has production importers. Top importers: `hashing` (36), `uuid_factory` (21), `env_utils` (16).

**Dead code:** None
**Rust potential:** None (high-level config/validation/hashing semantics)
**Recommendation:** Keep all. No dead code, no Rust candidates.

---

#### `src/arrow_utils/` — RUST-PARTIAL

**Files:** 7 (in `core/`: `ordering.py`, `array_iter.py`, `schema_constants.py`, `expr_types.py`, `streaming.py`)
**Production importers:** 23 | **Test importers:** 4

Metadata and iteration helpers for Arrow operations. `ordering.py` (8 importers) and `schema_constants.py` (8 importers) are most used.

**Dead code:** `expr_types.py` has only 1 importer — candidate for inlining into `datafusion_engine/kernels.py`
**Rust potential:** Low ROI. Ordering metadata could migrate if sort operations move to Rust. Array iteration is thin PyArrow wrapper (no speedup from Rust).
**Recommendation:** Keep. Inline `ExplodeSpec` from `expr_types.py` if desired.

---

#### `src/runtime_models/` — FACADE

**Files:** 9 (`base.py`, `root.py`, `engine.py`, `compile.py`, `otel.py`, `semantic.py`, `adapters.py`, `types.py`, `__init__.py`)
**Production importers:** 9 | **Test importers:** 0

Config validation layer using Pydantic TypeAdapters. Tight binding to CLI config loading. No dedicated tests.

**Dead code:** None, but very low external usage
**Rust potential:** None (configuration boundary)
**Recommendation:** Consider consolidating into `src/cli/config_models.py` during CLI refactoring.

---

#### `src/extract/` — RETAIN

**Files:** 52 across 8 subdirectories (extractors, coordination, scanning, git, python, infrastructure)
**Production importers:** 43 | **Test importers:** 25
**LOC:** ~20,000

Core evidence extraction layer. All extractors are pure Python functions producing PyArrow tables from LibCST, AST, symtable, bytecode, SCIP, and tree-sitter.

**Dead code:** None
**Rust potential:** Minimal. Extraction delegates to Python stdlib/libraries. SCIP and tree-sitter are already hybrid.
**Recommendation:** Keep as-is. Python-authoritative extraction layer.

---

#### `src/extraction/` — FACADE

**Files:** 13 (`orchestrator.py`, `engine_runtime.py`, `engine_session.py`, `engine_session_factory.py`, `plan_product.py`, `contracts.py`, `options.py`, `materialize_pipeline.py`, `runtime_profile.py`, `delta_tools.py`, `semantic_boundary.py`, `diagnostics.py`, `__init__.py`)
**Production importers:** 12 | **Test importers:** 10
**LOC:** ~3,400

Thin orchestration layer between `extract/` and Rust `planning_engine`. Routes extraction → Delta → Rust.

**Dead code:** `plan_product.py` appears test-referenced only (no production callers)
**Rust potential:** Session/runtime adapters could fold into PyO3 session APIs over time (per engine plan Scope C)
**Recommendation:** Retain as facade. Mark `plan_product.py` for audit.

---

#### `src/graph/` — RUST-READY

**Files:** 3 (`__init__.py`, `product_build.py`, `build_pipeline.py`)
**Production importers:** External (CLI entrypoint) | **Test importers:** 3
**LOC:** ~650

Minimal public API facade. `build_graph_product()` orchestrates extraction → semantic IR → Rust `run_build()`. Fully delegated to Rust for planning/execution.

**Dead code:** None
**Rust potential:** Complete — Rust engine already owns pipeline. Python is thin adapter.
**Recommendation:** Keep as Python entry point. Can become pure Rust CLI wrapper if desired.

---

#### `src/semantics/` — RETAIN

**Files:** 60+ across top-level + `catalog/`, `incremental/`, `types/`, `joins/`, `plans/`, `validation/`, `docs/`, `stats/`
**Production importers:** 50+ | **Test importers:** 49
**LOC:** ~23,000+ (largest module)

Core semantic IR compilation layer. Contains the `SemanticCompiler`, `ir_pipeline`, view/spec registries, entity model, naming conventions, type system, and join inference.

**Submodule breakdown:**

| Submodule | LOC | Status | Notes |
|-----------|-----|--------|-------|
| `pipeline.py` | 2,100 | CORE | Main `build_cpg()` entry |
| `compiler.py` | 900 | CORE | SemanticCompiler (10 semantic rules) |
| `ir_pipeline.py` | 2,400 | CORE | compile→infer→optimize→emit |
| `ir.py` | 900 | CORE | SemanticIR dataclasses |
| `spec_registry.py` | 1,200 | CORE | SemanticSpecIndex |
| `registry.py` | 2,400 | CORE | SEMANTIC_MODEL index (largest) |
| `catalog/` | 3,046 | CORE | View registry + builders (10 files) |
| `incremental/` | 5,800 | FEATURE-GATED | CDF processing (17 files, Phase B pending) |
| `types/` | 600 | CORE | Semantic type system (Phase I ~95%) |
| `joins/` | 500 | CORE | Join strategy inference |
| `validation/` | 300 | LOW-USE | Could inline (3 importers) |

**Dead code:** `semantics/validation/` is low-use (consider inlining). `incremental/` is feature-gated behind `_default_semantic_cache_policy()` at `pipeline.py:262`.
**Rust potential:** Python owns semantic rule definitions and IR compilation. Rust consumes compiled specs. Clean boundary.
**Recommendation:** Retain. Feature-gate `incremental/` clearly. Audit `validation/` for inlining.

---

#### `src/storage/` — RETAIN

**Files:** 10 across `deltalake/` and `external_index/` subdirs
**Production importers:** 48 | **Test importers:** 32
**LOC:** 5,212

Core Delta Lake abstraction layer. Sole interface for all Delta read/write/schema operations.

**Dead code:** None
**Rust potential:** File pruning could eventually migrate to Rust via deltalake-rs v1.4+ expansion. Current Delta ops use `deltalake` Python package (wrapping deltalake-rs).
**Recommendation:** Keep as-is. Monitor deltalake-rs roadmap.

---

#### `src/validation/` — MINIMAL

**Files:** 2 (`__init__.py`, `violations.py`)
**Production importers:** 5 | **Test importers:** 1
**LOC:** 163

Single-file module providing `ViolationType` enum (10 types) and `ValidationViolation` dataclass. Pure data carrier.

**Dead code:** None, but very thin
**Rust potential:** None
**Recommendation:** Candidate for consolidation into `datafusion_engine.schema.contracts`. Low priority.

---

#### `src/cache/` — RETAIN-THIN

**Files:** 2 (`__init__.py`, `diskcache_factory.py`)
**Production importers:** 22 | **Test importers:** 5
**LOC:** 517

Extraction/planning disk cache (distinct from removed CQ cache). 8 cache kinds for plan, extract, schema, repo_scan, runtime, queue, index, coordination.

**Dead code:** None
**Rust potential:** None (language-specific I/O)
**Recommendation:** Keep operational. Clarify in docs this is NOT the removed CQ cache.

---

#### `src/obs/` — RETAIN (RUST-PARTIAL for long-term)

**Files:** 18 across top-level + `otel/` subdir
**Production importers:** 91 (highest of all modules) | **Test importers:** 27
**LOC:** 7,772

Ubiquitous observability layer. Every subsystem instruments telemetry. OTel infrastructure (bootstrap, tracing, metrics, logging) is Python-authoritative.

**Dead code:** None
**Rust potential:** Rust crates already have `opentelemetry` dependencies. Python and Rust emit to same OTLP collectors. No migration needed.
**Recommendation:** Keep all. OTel infrastructure is language-specific.

---

#### `src/test_support/` — DEAD (production)

**Files:** 2 (`__init__.py`, `datafusion_ext_stub.py`)
**Production importers:** 0 | **Test importers:** 3
**LOC:** 1,134

Test-only fallback stub for `datafusion_ext` when Rust extension is not compiled. 100+ stub functions returning placeholders.

**Dead code:** Zero production usage (by design)
**Recommendation:** Keep as test utility. Consider moving to `tests/support/` during restructuring.

---

### 3.3 Top-Level `src/*.py` Files

| File | Production importers | Classification |
|------|---------------------|---------------|
| `core_types.py` | 51 | **RETAIN** — Core type definitions used everywhere |
| `serde_msgspec.py` | 90 | **RETAIN** — Foundational serialization |
| `serde_msgspec_ext.py` | (via serde_msgspec) | **RETAIN** — Extension types |
| `serde_msgspec_inspect.py` | (via serde_msgspec) | **RETAIN** — Introspection |
| `serde_artifacts.py` | 14 | **RETAIN** — Artifact format definitions |
| `serde_artifact_specs.py` | 79 | **RETAIN** — Artifact spec registry |
| `serde_schema_registry.py` | 8 | **RETAIN** — Schema evolution |

All top-level files are heavily used. No dead code.

---

## 4) Rust Capability Cross-Reference

### Crate Maturity Summary

| Rust Crate | Purpose | Maturity | Python Overlap |
|------------|---------|----------|----------------|
| `codeanatomy_engine` | Core execution (compile, execute, materialize) | Mostly Complete | `relspec`, `engine` (deleted), `semantics` (IR consumer) |
| `codeanatomy_engine_py` | PyO3 wrapper (SessionFactory, Compiler, Materializer, run_build) | Complete | `planning_engine` |
| `datafusion_ext` | Domain UDFs (23+), Delta control plane, optimizer rules | Complete | `semantics` UDF calls, `extract` hashing |
| `datafusion_ext_py` | Thin PyO3 bridge | Complete | `datafusion_engine` session |
| `datafusion_python` | DataFusion Python bindings + CodeAnatomy extensions (100+ PyO3 classes) | Complete | `datafusion_engine`, `storage` |
| `df_plugin_api` | Stable ABI for plugins | Complete | None (internal) |
| `df_plugin_host` | Plugin loading/registration | Complete | `datafusion_engine/extensions` |
| `df_plugin_codeanatomy` | Concrete plugin (UDFs + Delta providers) | Complete | Transparent to Python |

### Python→Rust Authority Map

| Concern | Python Authority | Rust Authority | Boundary |
|---------|-----------------|----------------|----------|
| Evidence extraction | `src/extract/` (52 files) | None | PyArrow tables → Delta |
| Semantic IR definition | `src/semantics/` (60+ files) | None | JSON-serialized SemanticExecutionSpec |
| Plan compilation | — | `codeanatomy_engine::compiler` | SemanticExecutionSpec JSON |
| Task scheduling | — | `codeanatomy_engine::compiler` | Internal to Rust |
| CPG execution | — | `codeanatomy_engine::executor` | Internal to Rust |
| Delta materialization | — | `codeanatomy_engine::executor` + `datafusion_ext` | Delta tables |
| UDF evaluation | — | `datafusion_ext::udf` (23+ UDFs) | DataFusion plans |
| Session management | `src/datafusion_engine/session/` (adapter) | `codeanatomy_engine::session` | SessionFactory PyO3 |
| CLI interface | `src/cli/` | None | User-facing Python |
| Observability | `src/obs/` (91 importers) | `opentelemetry` crate | OTLP protocol |

---

## 5) Consolidated Dead/Minimal-Use Code Inventory

### Immediate Delete (DEAD, no dependencies)

| File | Module | src imports | tests imports | Reason |
|------|--------|-----------|--------------|--------|
| `relspec/incremental.py` | relspec | 0 | 0 | Zero importers |
| `relspec/counterfactual_replay.py` | relspec | 0 | 1 | Test-only |
| `relspec/decision_recorder.py` | relspec | 0 | 1 | Test-only |
| `relspec/fallback_quarantine.py` | relspec | 0 | 1 | Test-only |
| `relspec/policy_validation.py` | relspec | 0 | 1 | Test-only |
| `relspec/runtime_artifacts.py` | relspec | 0 | 1 | Test-only |
| `relspec/execution_authority.py` | relspec | 0 | 1 | Test-only |
| `cpg/relationship_contracts.py` | cpg | 0 | 1 | Test-only |
| `schema_spec/dataset_handle.py` | schema_spec | 0 (self-ref) | 0 | Dead |
| `schema_spec/nested_types.py` | schema_spec | 1 (pkg export) | 1 | Near-dead |

### Delete After Small Callsite Move (MINIMAL)

| File | Module | Callsite to move | Target |
|------|--------|-----------------|--------|
| `cpg/schemas.py` | cpg | `SCHEMA_VERSION` in `product_build.py` | Inline constant |
| `cpg/constants.py` | cpg | `ROLE_FLAG_SPECS` in `spec_registry.py` | Inline |
| `cpg/scip_roles.py` | cpg | Role flags in `constants.py` | Inline into spec_registry |
| `schema_spec/registration.py` | schema_spec | `DatasetRegistration` in `semantics/catalog/spec_builder.py` | Use `make_dataset_spec()` directly |

### Feature-Gated (not dead, but dormant)

| Module | LOC | Gate | Phase |
|--------|-----|------|-------|
| `semantics/incremental/` (17 files) | 5,800 | `_default_semantic_cache_policy()` at `pipeline.py:262` | Phase B |

### Consolidation Candidates (low-use, inlinable)

| Module | LOC | Importers | Target |
|--------|-----|-----------|--------|
| `validation/` | 163 | 5 | `datafusion_engine.schema.contracts` |
| `runtime_models/` | ~500 | 9 | `cli/config_models.py` |
| `arrow_utils/core/expr_types.py` | ~50 | 1 | Inline into `datafusion_engine/kernels.py` |

---

## 6) Pivot Recommendations (Prioritized Waves)

### Wave 0: Immediate Deletes (no dependencies, safe now)

**Scope:** 10 files, ~1,500 LOC removed

1. Delete 7 `relspec/` dead files + their test-only tests
2. Delete `cpg/relationship_contracts.py` + `tests/unit/cpg/test_relationship_contracts.py`
3. Delete `schema_spec/dataset_handle.py` + remove `dataset_spec_handle()` from `dataset_spec_ops.py`
4. Delete `schema_spec/nested_types.py` + `tests/unit/test_nested_types.py`

### Wave 1: Small Callsite Moves Then Delete (~500 LOC)

1. Inline `SCHEMA_VERSION` from `cpg/schemas.py` → `graph/product_build.py`; delete `schemas.py`
2. Inline `ROLE_FLAG_SPECS` from `cpg/constants.py` + `scip_roles.py` → `cpg/spec_registry.py`; delete both
3. Replace `DatasetRegistration` usage in `semantics/catalog/spec_builder.py`; delete `schema_spec/registration.py`
4. Prune dead exports from `schema_spec/view_specs.py`, `file_identity.py`, `dataset_spec_ops.py`

### Wave 2: Relspec Callsite Migration (~2,600 LOC)

1. Move `rustworkx_graph.py`, `rustworkx_schedule.py`, `schedule_events.py` callsites to Rust engine
2. Move `graph_edge_validation.py`, `evidence.py`, `extract_plan.py` callsites
3. Move `execution_planning_runtime.py` callsites
4. Delete all 6 modules after callsite migration

### Wave 3: Rust CPG Cutover (requires Rust compiler changes)

1. Implement CPG output transforms in `rust/codeanatomy_engine/src/compiler/`
2. Delete `cpg/view_builders_df.py` and dependent builder modules
3. Delete remaining `cpg/` runtime authority files

### Wave 4: Rust Schema/Policy Cutover (requires new PyO3 APIs)

1. Add Rust/PyO3 dataset policy and schema-introspection contracts
2. Remove Python runtime authority from `schema_spec/system.py` and `dataset_spec_ops.py`
3. Reduce `datafusion_engine/` to thin adapter layer

### Wave 5: Consolidation (cleanup)

1. Fold `validation/violations.py` into `datafusion_engine/schema/contracts`
2. Consider folding `runtime_models/` into `cli/config_models.py`
3. Move `test_support/` to `tests/support/`
4. Feature-gate `semantics/incremental/` with explicit env var

---

## 7) Module Classification Summary

| # | Module | Files | LOC | Prod Importers | Classification | Wave |
|---|--------|-------|-----|---------------|---------------|------|
| 1 | `relspec/` | 14 | 2,632 | 7-55 (varies) | RUST-READY | 0+2 |
| 2 | `planning_engine/` | 4 | ~400 | 13 | RETAIN-THIN | — |
| 3 | `cpg/` | 12 | ~2,000 | 1-2 each | RUST-READY | 0+1+3 |
| 4 | `schema_spec/` | 12 | ~3,000 | 1-39 (varies) | RUST-PARTIAL | 0+1+4 |
| 5 | `cli/` | 15+ | ~3,000 | N/A (entry) | RETAIN | — |
| 6 | `datafusion_engine/` | 60+ | ~15,000 | N/A (core) | RETAIN-THIN | — |
| 7 | `core/` | 3 | ~300 | 29 | RETAIN-THIN | — |
| 8 | `utils/` | 10 | ~2,000 | 92 | RETAIN | — |
| 9 | `arrow_utils/` | 7 | ~800 | 23 | RUST-PARTIAL | — |
| 10 | `runtime_models/` | 9 | ~500 | 9 | FACADE | 5 |
| 11 | `extract/` | 52 | ~20,000 | 43 | RETAIN | — |
| 12 | `extraction/` | 13 | ~3,400 | 12 | FACADE | — |
| 13 | `graph/` | 3 | ~650 | ext | RUST-READY | — |
| 14 | `semantics/` | 60+ | ~23,000 | 50+ | RETAIN | — |
| 15 | `storage/` | 10 | 5,212 | 48 | RETAIN | — |
| 16 | `validation/` | 2 | 163 | 5 | MINIMAL | 5 |
| 17 | `cache/` | 2 | 517 | 22 | RETAIN-THIN | — |
| 18 | `obs/` | 18 | 7,772 | 91 | RETAIN | — |
| 19 | `test_support/` | 2 | 1,134 | 0 | DEAD (prod) | 5 |
| 20 | Top-level `*.py` | 7 | ~3,000 | 8-90 | RETAIN | — |

**Totals:** ~90,000 LOC across 20 modules + 7 top-level files

---

## 8) Consistency Check Against Prior Plans

| Prior Document | This Review's Assessment | Consistent? |
|---------------|------------------------|-------------|
| relspec decommission baseline | 7 immediate-delete + 6 after-move confirmed | Yes |
| engine decommission plan | `src/engine/` deleted; `planning_engine/` reduced to 4 files | Yes |
| cpg/schema_spec assessment | Wave A/B/C/D classifications confirmed | Yes |
| CLI transition plan | CLI RETAIN, build/plan commands need rework | Yes |
| DataFusion planning plan | All 13 v1 scope items complete in Rust | Yes |

No contradictions found between this review and prior plan documents.

---

## Appendix: Decommissioned Modules (already deleted)

These directories no longer exist on disk:
- `src/engine/` — Fully decommissioned to Rust
- `src/hamilton_pipeline/` — Fully decommissioned (Rust engine replaces Hamilton)
- `src/normalize/` — Deprecated facade (was re-export of `semantics.catalog`)
- `src/incremental/` — Deprecated facade (was re-export of `semantics.incremental`)
