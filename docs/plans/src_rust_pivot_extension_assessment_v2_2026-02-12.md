# src/ Rust Pivot Extension Assessment (v2, refreshed)

**Date:** 2026-02-12 (refresh pass)  
**Authoring mode:** Codex-assisted CQ + dfdl_ref review  
**Scope:** Refresh and update `docs/plans/src_rust_pivot_extension_assessment_v2_2026-02-12.md` against current code, then add additional near-term decommission scope with emphasis on `src/datafusion_engine`.

---

## 1) Environment + Review Inputs

### Environment setup executed
- `scripts/bootstrap_codex.sh`
- `uv sync`

Result: environment validated with Python `3.13.12`; extension artifact present (`rust/datafusion_ext_py/plugin/libdf_plugin_codeanatomy.dylib`).

### Inputs reviewed in this refresh
- `docs/plans/src_rust_pivot_extension_assessment_v2_2026-02-12.md` (current document, refreshed)
- `docs/plans/comprehensive_src_rust_pivot_review_v1_2026-02-11.md`
- CQ scans across `src/` and `rust/` for callsite locality + duplication
- `dfdl_ref` references (UDF/FunctionFactory, planning artifacts, schema/evolution adapters, Delta/DataFusion integration)

---

## 2) Scope Changes Since Prior v2 Draft

## Completed/advanced since prior draft
- `pandera` migration appears already actioned:
  - `src/schema_spec/pandera_bridge.py` is absent.
  - CQ search for `pandera` in `src/` now returns no matches.

## Still pending (core v2 items remain active)
- Python fallback UDF path is still present:
  - `src/datafusion_engine/udf/fallback.py`
  - fallback path in `src/datafusion_engine/udf/runtime.py` (`_fallback_registry_snapshot`)
  - fallback path in `src/datafusion_engine/udf/factory.py` (`_fallback_install_function_factory`)
- Python FunctionFactory policy + installation wrappers remain active:
  - `src/datafusion_engine/udf/factory.py`
  - `src/datafusion_engine/udf/platform.py`
- Python build orchestration still owns auxiliary output writing:
  - `src/graph/build_pipeline.py` writes normalize/error/manifest/run-bundle outputs.
  - Rust `run_build` still returns artifact placeholders (`manifest_path`/`run_bundle_dir` are `null`) in `rust/codeanatomy_engine_py/src/lib.rs`.
- Runtime profile shaping still Python-heavy and broader than previously scoped:
  - `resolve_runtime_profile` is used across `src/extraction/orchestrator.py`, `src/graph/build_pipeline.py`, `src/extract/coordination/context.py`, `src/extract/extractors/scip/extract.py`, and `src/extraction/runtime_profile.py`.

---

## 3) Refreshed Status of Existing v2 Workstreams

| Workstream (from v2) | Current status | Scope update |
|---|---|---|
| A. Remove Python fallback UDF path | **Not started** | Keep as high-priority delete wave. Fallback branches are still active in `udf/runtime.py` + `udf/factory.py`, backed by `udf/fallback.py`. |
| B. Collapse Python FunctionFactory policy synthesis | **Not started** | Keep; `udf/factory.py` still handles policy payload shaping, extension selection, and compatibility retry paths. |
| C. Move auxiliary orchestration into Rust `run_build` contract | **Not started** | Keep; `build_pipeline.py` still owns multiple artifact writers while Rust returns artifact flags/placeholders only. |
| D. Migrate runtime profile shaping to Rust | **In progress conceptually, not reduced in code** | Expand scope: profile resolver dependency fan-out is wider than prior v2 notes, so migration should start with Rust-emitted typed profile payloads + compatibility shim. |
| E. Replace Pandera bridge via Rust/DataFusion-native schema path | **Partially completed** | `pandera` appears removed from `src/`; remaining schema/runtime reduction should now focus on `SchemaRuntime` authority and schema-evolution adapter installation paths. |

---

## 4) Additional `src/datafusion_engine` Decommission Scope (New)

These are additional low-friction migration targets not explicitly captured in prior v2 scope.

## A. Collapse Python Delta gate shim into Rust-owned contract

### Current state
- `src/datafusion_engine/delta/protocol.py::validate_delta_gate` constructs msgpack payloads and delegates to `datafusion_ext.validate_protocol_gate`.
- Primary in-tree use is localized in `src/datafusion_engine/dataset/resolution.py` (`_resolve_delta_table`).

### Migration opportunity
- Replace Python payload assembly + extension call with a Rust-returned compatibility verdict in Delta provider/control-plane responses.
- Shrink `delta/protocol.py` to data contracts only (or delete validation function entirely).

### Rust alignment
- `rust/datafusion_python/src/codeanatomy_ext.rs::validate_protocol_gate` already exists.
- dfdl_ref Delta integration guidance supports keeping protocol checks in Rust control-plane seams.

## B. Remove Python cache-table registration bridge/mismatch loop

### Current state
- `src/datafusion_engine/catalog/introspection.py::register_cache_introspection_functions` performs Python-side registrar resolution and ABI mismatch retries.
- Runtime path is a single hop through `src/datafusion_engine/session/runtime.py::_register_cache_introspection_functions`.

### Migration opportunity
- Replace Python `_resolve_cache_table_registrar` and candidate-retry logic with a direct Rust call boundary from runtime bootstrap.

### Rust alignment
- `rust/datafusion_python/src/codeanatomy_ext.rs::register_cache_tables` already provides a direct registration API.
- dfdl_ref notes favor Rust-owned metadata/cache plumbing for observability and planning control surfaces.

## C. Consolidate schema-evolution adapter install/load paths (and remove duplicates)

### Current state
- Duplicate Python installer logic exists in:
  - `src/datafusion_engine/dataset/registration.py::_install_schema_evolution_adapter_factory`
  - `src/datafusion_engine/session/runtime.py::_install_schema_evolution_adapter_factory`
- Additional loader path in `src/datafusion_engine/session/runtime.py::_load_schema_evolution_adapter_factory`.
- Rust entrypoint currently exists but is a no-op:
  - `rust/datafusion_python/src/codeanatomy_ext.rs::install_schema_evolution_adapter_factory` returns `Ok(())`.

### Migration opportunity
- Implement the schema-evolution adapter installation semantics in Rust, then delete duplicate Python installation paths.
- Keep a single thin Python adapter if required for compatibility.

### Rust alignment
- dfdl_ref schema/evolution guidance points to scan-boundary adapter ownership in Rust/engine surfaces.

## D. Replace dynamic UDF platform probing with Rust capabilities contract

### Current state
- `src/datafusion_engine/udf/platform.py::native_udf_platform_available` dynamically imports `datafusion._internal`/`datafusion_ext` and introspects attributes.
- This influences runtime strictness in `src/datafusion_engine/session/facade.py` and named-args support checks in `src/datafusion_engine/session/runtime.py`.

### Migration opportunity
- Replace import+attribute probing with a Rust capability snapshot handshake at startup.
- Remove repeated module probing and make planner/UDF availability deterministic.

### Rust alignment
- `rust/datafusion_python/src/codeanatomy_ext.rs::capabilities_snapshot` already exposes feature/ABI metadata and UDF registry summary.
- dfdl_ref FunctionFactory/UDF guidance favors strongly typed, Rust-owned function platform contracts.

## E. De-duplicate extension module resolution surfaces across DataFusion engine

### Current state
- Independent resolver implementations and call paths exist in:
  - `src/datafusion_engine/session/runtime.py::_resolve_extension_module`
  - `src/datafusion_engine/delta/control_plane.py::_resolve_extension_module`
- CQ shows broad dependency fan-out across runtime + delta control-plane helpers.

### Migration opportunity
- Introduce a single Rust-exported extension descriptor/capability boundary and delete Python resolver duplication.
- Keep Python focused on orchestration, not module-selection policy.

### Rust alignment
- dfdl_ref planning/control-plane patterns favor centralized engine feature surfaces rather than per-module Python probing.

---

## 5) dfdl_ref-Aligned Capability Priorities (Updated)

1. Keep `FunctionFactory`/named-arg/ExprPlanner policy synthesis in Rust; Python should consume snapshots/policies, not derive them.
2. Treat Delta provider + protocol + scan-config mechanics as Rust control-plane ownership; Python wrappers should become thin request DTO layers.
3. Move schema-evolution adapter installation/selection to Rust scan/runtime boundaries, then remove duplicate Python install paths.
4. Return orchestration artifacts from Rust `run_build` directly (manifest, run bundle, auxiliary paths) to delete Python artifact writers.
5. Use a single capabilities handshake (`capabilities_snapshot`-style) instead of repeated Python dynamic import probing.

---

## 6) Revised Incremental Wave Plan

## Wave 0 (already advanced)
- Keep completed schema-side cleanup acknowledged (`pandera` removal from `src/`).

## Wave 1 (highest-value low-risk deletes in `src/datafusion_engine`)
- Remove fallback UDF branches (`udf/fallback.py`, runtime/factory fallback paths) after strict ABI/capability gate.
- Replace Python cache registration bridge with Rust direct call.
- Replace Python UDF availability probing with Rust capability snapshot.

## Wave 2 (control-plane simplification)
- Collapse Python Delta gate validation shim into Rust-owned response/validation contract.
- Consolidate extension module resolver duplication (`session/runtime.py` and `delta/control_plane.py`).

## Wave 3 (schema evolution + runtime profile)
- Implement real Rust schema-evolution adapter installer and remove duplicated Python installer/loaders.
- Move runtime profile shaping to Rust payload APIs and reduce `extraction/runtime_profile.py`/`engine_session_factory.py`.

## Wave 4 (orchestration consolidation)
- Extend Rust `run_build` artifact contract to emit manifest/bundle/auxiliary output paths.
- Delete Python auxiliary-output writers from `src/graph/build_pipeline.py`.

---

## 7) Net Conclusions (Refresh)

1. Prior v2 direction is still correct, but one major scope item (`pandera` removal) has already progressed and should no longer drive near-term effort.
2. The largest remaining easy wins are now concentrated in `src/datafusion_engine` bridge code: fallback UDF paths, cache registration wrappers, extension probing/resolution duplication, and thin Delta gate shims.
3. Schema-evolution adapter migration should be re-sequenced: implement the Rust installer behavior first (currently stubbed), then delete duplicated Python paths.
4. Python orchestration decommission (`build_pipeline.py`) is still blocked by Rust `run_build` artifact contract completeness; this remains a high-value but dependency-gated step.
