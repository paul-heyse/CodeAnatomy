# DataFusion Engine Decomposition Plan Review (v2, Design-Phase Hard Cutover)

**Date:** 2026-02-13  
**Plan Reviewed:** `docs/plans/datafusion_engine_decomposition_implementation_plan_v1_2026-02-12.md`  
**Reviewer:** Codex (with `dfdl_ref` reference set + codebase validation)

## Design-Phase Posture (Updated)

This review is updated to match a design-phase stance with **aggressive migration to the go-forward architecture**:

1. No shims.
2. No compatibility bridges.
3. No dual-path runtime behavior.
4. Break old internal import paths in the same wave they are replaced.

If a wave lands, legacy path(s) are removed immediately in that wave.

## Environment Setup Status

Completed successfully:
- `bash scripts/bootstrap_codex.sh`
- `uv sync`

Observed toolchain issue after sync:
- `./cq` currently fails with `AttributeError: module 'msgspec' has no attribute 'Enum'` (`tools/cq/search/lsp/status.py:10`).
- This affects CQ-driven preflight checks and should be fixed early, but does not change the architecture recommendations below.

## Executive Findings (Still Applicable)

## 1) Critical Plan Corrections

1. Wave 3 protocol move-to-Rust is already complete.
- `src/datafusion_engine/delta/protocol.py:194`
- `src/datafusion_engine/delta/protocol.py:212`
- `src/datafusion_engine/delta/protocol.py:217`
- `rust/datafusion_python/src/codeanatomy_ext.rs:2401`
- Action: remove Wave 3 protocol migration tasks and replace with conformance/perf validation only.

2. Wave 1 dead-export set contains live internals.
- `src/datafusion_engine/dataset/registration.py:230`
- `src/datafusion_engine/dataset/registration.py:571`
- `src/datafusion_engine/dataset/registration.py:1114`
- `src/datafusion_engine/dataset/registration.py:1122`
- `src/datafusion_engine/dataset/registration.py:1222`
- `src/datafusion_engine/extract/templates.py:251`
- `src/datafusion_engine/extract/templates.py:461`
- `src/datafusion_engine/extract/templates.py:1413`
- `src/datafusion_engine/extract/templates.py:1424`
- Action: correct these candidate lists before executing deletion waves.

3. Part of schema dead-set is incorrect.
- `src/datafusion_engine/schema/registry.py:1721`
- `src/datafusion_engine/schema/registry.py:1739`
- `src/datafusion_engine/schema/registry.py:2136`
- `src/datafusion_engine/schema/registry.py:2170`
- `src/datafusion_engine/schema/registry.py:4017`
- `src/datafusion_engine/session/runtime.py:112`
- `src/datafusion_engine/session/runtime.py:6350`
- Action: do not treat these as removable dead exports.

## 2) Hard-Cutover Architecture Decisions

These replace prior shim/compat recommendations:

1. Standardize to one extension surface: `datafusion_ext`.
- Remove `datafusion._internal` fallback code paths.
- Remove runtime probing that keeps both module contracts alive.

2. No compatibility re-export modules for monolith splits.
- Bulk-update imports repo-wide in the same PR/wave.
- Delete legacy modules once replacements compile and tests pass.

3. No deprecation holding period for internal architecture modules.
- This is a design-phase re-architecture; apply direct cutover.

4. Prefer Rust ownership for execution-critical paths now.
- Python remains orchestration/control plane only where necessary.

## 3) Additional Decommission Candidates (Aggressive)

1. `src/datafusion_engine/delta/plugin_options.py`
- Current references are lazy export + self-reference only.
- Action: delete with immediate API surface contraction notes.

2. `src/datafusion_engine/plan/pipeline_runtime.py`
- Still a valid delete target with associated tests in same wave.

3. `src/datafusion_engine/delta/scan_policy_inference.py`
- Re-evaluate immediately after `pipeline_runtime.py` deletion.
- If no direct runtime owner remains, delete it and its dedicated tests.

4. `src/datafusion_engine/workload/*`
- Not currently used in `src/` runtime call paths but test-backed.
- Action: make explicit binary decision in Wave 0:
  - Keep and wire into go-forward runtime now, or
  - Delete all workload modules + tests now.

## 4) Rust Transition Prioritization (Aggressive)

Prioritize by runtime criticality and DataFusion-native leverage:

1. `src/datafusion_engine/plan/execution_runtime.py`
- Direct cutover to Rust-backed execution path; remove Python legacy internals.

2. `src/datafusion_engine/lineage/datafusion.py` and `src/datafusion_engine/lineage/scan.py`
- Move lineage scan/planning internals to Rust and remove Python heavy-path logic.

3. `src/datafusion_engine/dataset/registration.py`
- High complexity, but high payoff.
- Split orchestration (Python) vs provider/catalog mechanics (Rust), then remove old Python mechanics.

Do not allocate migration effort to protocol validation; it is already Rust-backed.

## Best-in-Class DataFusion/Delta Guidance (Go-Forward)

## A) Contract and API Shape

1. Single runtime extension contract.
- Keep only `datafusion_ext` entrypoints in the final design.
- Remove fallback resolution and mixed-module probing.

2. Keep API usage aligned to DataFusion Python 51.
- Use `SessionConfig.set(...)` for configuration keys such as:
  - `datafusion.execution.collect_statistics`
  - `datafusion.execution.parquet.pushdown_filters`
  - `datafusion.execution.parquet.enable_page_index`

## B) Built-In Engine Capabilities to Lean On

1. Preserve existing high-value config defaults already present in runtime policy.
- `datafusion.execution.collect_statistics`
- `datafusion.execution.parquet.pushdown_filters`
- `datafusion.execution.parquet.enable_page_index`
- `datafusion.runtime.list_files_cache_limit`
- `datafusion.runtime.metadata_cache_limit`

2. Use plan/config reproducibility as a first-class artifact.
- Persist `SHOW ALL` / `information_schema.df_settings` snapshots alongside benchmark plans for each wave.

3. Keep Delta scan policy centered on `DeltaScanConfig`-equivalent controls.
- `file_column_name`
- `enable_parquet_pushdown`
- `schema_force_view_types`
- `wrap_partition_values`

## C) UDF and Function Strategy

1. Move toward DataFusion-native function registration strategy where possible (`FunctionFactory`/`CREATE FUNCTION` direction), while keeping custom Rust UDF kernels for performance-critical paths.

2. Enforce fail-fast behavior for missing Rust UDF entrypoints.
- No Python soft-fail retries.
- No compatibility fallback modules.

## 5) Revised Wave Plan (No Shims, No Compat)

1. Wave 0: Hard delete dead paths.
- Delete `plan/pipeline_runtime.py` + dedicated tests.
- Delete `delta/plugin_options.py`.
- Decide and execute keep/delete for `workload/*` in the same wave.

2. Wave 1: Correct dead-set and prune aggressively.
- Remove only truly dead symbols after corrected dependency checks.
- Do not preserve legacy exports for compatibility.

3. Wave 2: UDF runtime hard cutover.
- Standardize on `datafusion_ext` contract.
- Remove `datafusion._internal` path and associated probes/branches.

4. Wave 3: Delta control plane consolidation.
- Remove completed protocol-migration tasks.
- Simplify Python wrappers to minimal orchestration only.

5. Wave 4: Monolith decomposition with direct import rewrite.
- Split/replace `session/runtime.py` and `schema/registry.py`.
- Bulk-update imports in same wave.
- Delete legacy modules immediately after replacement.

6. Wave 5: Rust-first completion.
- Move execution runtime and lineage heavy path to Rust ownership.
- Leave Python only for boundary orchestration and request shaping.

## 6) Hard-Cutover Acceptance Gates

Use explicit execution gates per wave:

1. Zero remaining imports of deleted module paths.
2. Zero references to removed symbols.
3. Zero runtime branches for compatibility/fallback module resolution.
4. Benchmarks not worse than baseline on target workloads.
5. Full repo quality gate passes:
- `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`

## Final Conclusion

With the design-phase constraint applied, the recommended path is a **direct architecture cutover**, not a conservative migration.

The plan should be updated to:
1. remove already-completed protocol migration work,  
2. correct false dead-code assumptions,  
3. drop shim/compatibility sequencing,  
4. enforce single extension contract and fail-fast semantics, and  
5. delete legacy modules in-wave once replacements are live.

This is the fastest route to a clean, go-forward DataFusion/Delta architecture with clear ownership boundaries and minimal long-term maintenance drag.

## Primary References

- DataFusion Python `SessionConfig` API: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html#datafusion.context.SessionConfig
- DataFusion configuration reference: https://datafusion.apache.org/user-guide/configs.html
- DataFusion Python `DataFrame` API: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html
- DataFusion 40 release (`FunctionFactory` / `CREATE FUNCTION`): https://datafusion.apache.org/blog/2024/07/24/datafusion-40.0.0/
- Delta Lake + DataFusion `DeltaScanConfig`: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html
- Local `dfdl_ref` references used:
  - `.claude/skills/dfdl_ref/reference/datafusion.md`
  - `.claude/skills/dfdl_ref/reference/datafusion_addendum.md`
  - `.claude/skills/dfdl_ref/reference/datafusion_planning.md`
  - `.claude/skills/dfdl_ref/reference/deltalake_datafusion_integration.md`
