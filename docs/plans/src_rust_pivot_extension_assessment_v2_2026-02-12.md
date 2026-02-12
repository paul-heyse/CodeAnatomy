# src/ Rust Pivot Extension Assessment (v2)

**Date:** 2026-02-12  
**Authoring mode:** Codex-assisted CQ + dfdl_ref review  
**Scope:** Re-assess `docs/plans/comprehensive_src_rust_pivot_review_v1_2026-02-11.md`, validate current state, and identify additional `src/` decommission candidates in favor of `rust/`.

---

## 1) Setup and Inputs

### Environment setup executed
- `scripts/bootstrap_codex.sh`
- `uv sync`

Result: environment validated with Python `3.13.12`, extension artifact present (`rust/datafusion_ext_py/plugin/libdf_plugin_codeanatomy.dylib`).

### Inputs reviewed
- `docs/plans/comprehensive_src_rust_pivot_review_v1_2026-02-11.md`
- `docs/plans/cpg_schema_spec_rust_datafusion_pivot_assessment_v1_2026-02-11.md`
- CQ scans over `src/` and `rust/` for importer density + callsite locality
- `dfdl_ref` references (planning, schema, UDF contracts, Delta integration, deployment gaps)

---

## 2) Reassessment of v1 Findings

The v1 assessment remains directionally correct:
- Decommission priorities in `relspec`, `cpg`, and `schema_spec` are still valid.
- Python remains authoritative for extraction and semantic IR generation today.
- Rust already owns planning/execution/materialization primitives and exposes substantial PyO3/API surface.

No contradictions were found with the major v1 conclusions.

---

## 3) Additional Decommission Opportunities (Beyond v1)

## A. Remove Python fallback UDF path after hard Rust-extension gate

### New finding
Python fallback UDF machinery appears isolated to fallback branches and not core production orchestration:
- `src/datafusion_engine/udf/fallback.py`
- Fallback entrypoint used from `src/datafusion_engine/udf/runtime.py` (`_fallback_registry_snapshot`)

At the same time, Rust already exposes:
- `register_codeanatomy_udfs`
- `registry_snapshot`
- `capabilities_snapshot`
- `install_function_factory`
- `install_expr_planners`
(via `rust/datafusion_python/src/codeanatomy_ext.rs`)

### Decommission target
- Delete `src/datafusion_engine/udf/fallback.py`
- Remove fallback branches in `src/datafusion_engine/udf/runtime.py`
- Remove fallback compatibility logic in `src/datafusion_engine/udf/factory.py` where it exists for type mismatch fallback

### Rust requirements
- Make extension capability/ABI validation a strict startup invariant (no soft runtime fallback path)
- Ensure required hooks (`register_codeanatomy_udfs`, function factory, expr planners) are always present and version-compatible

---

## B. Collapse Python FunctionFactory policy synthesis into Rust

### New finding
Python still computes and translates FunctionFactory policy payloads (`src/datafusion_engine/udf/factory.py`), even though Rust owns both registry semantics and installation hooks.

### Decommission target
- Reduce `src/datafusion_engine/udf/factory.py` to a thin adapter or remove it after Rust policy APIs are complete
- Keep `src/datafusion_engine/udf/platform.py` only as minimal installer facade (or fold it into runtime bootstrap)

### Rust requirements
- Expose a single policy-derivation API from Rust snapshot metadata (named args, volatility, signatures, domain planner hooks)
- Expose policy serialization contract directly from Rust (avoid Python-side schema duplication)

---

## C. Move Python build orchestration auxiliaries into `run_build` response contract

### New finding
`src/graph/build_pipeline.py` still performs significant orchestration and auxiliary artifact writing after Rust execution, including normalize outputs, manifest writing, and run bundle shaping.

Evidence also shows localized coupling:
- `run_extraction` import occurs from `src/graph/build_pipeline.py` only.

Rust side already has substantial pipeline primitives (`executor::pipeline`, `executor::runner`, `executor::result`) and currently returns artifact flags/placeholders from `codeanatomy_engine_py::run_build`.

### Decommission target
- Remove Python-side auxiliary-output writers in `src/graph/build_pipeline.py`
- Shrink or delete `src/graph/build_pipeline.py` after Rust emits complete artifact payloads

### Rust requirements
- Extend `run_build` to emit canonical artifact paths + metadata (manifest path, run bundle dir, auxiliary output locations)
- Include optional auxiliary materialization behavior behind request flags already present in orchestration payload

---

## D. Migrate runtime profile shaping from Python to Rust profile APIs

### New finding
`src/extraction/runtime_profile.py` remains a large Python profile-shaping layer; however it already imports `codeanatomy_engine.SessionFactory` and hydrates values from Rust profile JSON.

Importer locality is narrow:
- `resolve_runtime_profile` is imported in `src/extraction/orchestrator.py` and `src/graph/build_pipeline.py`

### Decommission target
- Reduce `src/extraction/runtime_profile.py` to compatibility shim, then deprecate
- Reduce `src/extraction/engine_session_factory.py` when Rust profile/session payload APIs cover diagnostics + policy overlays

### Rust requirements
- Add first-class profile overlay API (env patch + policy patch + diagnostics sink contract)
- Return profile hash/settings hash/telemetry payload in a single structured response

---

## E. Expand `SchemaRuntime` to absorb Python schema/policy authority + Pandera bridge

### New finding
`schema_spec` already calls Rust `SchemaRuntime`, but only for limited helpers (`dataset_*` JSON extract + policy merge). Python remains authoritative for dataframe validation and policy application.

`pandera_bridge` is referenced by:
- `src/semantics/pipeline.py`
- `src/datafusion_engine/io/write.py`
- `src/extract/coordination/schema_ops.py`
- `src/obs/diagnostics.py`
- `src/schema_spec/field_spec.py`

### Decommission target
- `src/schema_spec/pandera_bridge.py` (after Rust validation parity)
- large sections of `src/schema_spec/system.py` policy-merge code
- eventual reduction of `src/schema_spec/dataset_spec_ops.py`

### Rust requirements
- Add schema validation contract APIs to `SchemaRuntime` (constraint checks, type compatibility, violation payloads)
- Emit structured validation diagnostics compatible with existing `ValidationViolation` consumption
- Support DataFusion/Delta-native schema enforcement hooks (not Pandera-only path)

---

## 4) dfdl_ref-Guided Rust Capability Alignment

The following dfdl_ref-aligned capabilities are relevant to deleting additional `src/` modules:

1. `FunctionFactory` + named args + custom expression planning should be treated as Rust-owned defaults (remove Python policy synthesis duplication).
2. Delta provider and mutation surfaces already exist in Rust (`delta_*` APIs via `datafusion_python` bridge), enabling Python-side control-plane reduction.
3. Planning artifacts should be returned as Rust pipeline outputs (logical/physical provenance and run artifact metadata), not reassembled in Python.
4. Schema/runtime contracts should move to Rust via `SchemaRuntime` expansion (schema hash/diff/cast alignment + policy merge + validation).
5. Runtime cache/statistics knobs should be profile-driven in Rust SessionFactory, with Python consuming a typed summary only.

---

## 5) Updated Wave Plan (Incremental)

## Wave 0 (existing v1 fast deletes)
- Keep v1 immediate deletes (relspec/cpg/schema_spec dead files).

## Wave 1 (new low-risk Rust-first cutovers)
- Hard-gate extension capability and remove Python fallback UDF paths.
- Move FunctionFactory policy derivation/serialization into Rust; reduce Python wrappers.

## Wave 2 (orchestration consolidation)
- Extend `run_build` artifact contract and remove Python auxiliary output writers in `graph/build_pipeline.py`.

## Wave 3 (profile/session consolidation)
- Move profile patching and settings derivation into Rust SessionFactory APIs; reduce `extraction/runtime_profile.py` and `extraction/engine_session_factory.py`.

## Wave 4 (schema/validation authority shift)
- Expand `SchemaRuntime` to own policy+validation; decommission `pandera_bridge.py` and reduce `schema_spec/system.py` + `dataset_spec_ops.py`.

---

## 6) Net-New Conclusions

1. v1 did not fully account for the decommission potential in `src/datafusion_engine/udf/*` fallback/policy layers; these are now prime near-term delete targets once strict extension gating is enforced.
2. `graph/build_pipeline.py` still contains non-trivial Python orchestration that can move into existing Rust executor primitives with modest API expansion.
3. `extraction/runtime_profile.py` and `extraction/engine_session_factory.py` are strong reduction candidates because they already depend on Rust profile/session data and mainly reshape it.
4. `schema_spec` migration should prioritize replacing `pandera_bridge` with Rust/DataFusion-native validation contracts; this unlocks deletion of a large Python validation surface not explicitly prioritized in v1.
