# Engine Rust Planning/Execution Pivot: Detailed Decommission Implementation Plan

Date: 2026-02-11
Related analysis: `src/engine` decommission classification and Rust fold-in recommendations

---

## 0) Summary

This plan migrates planning/execution authority from Python `src/engine` modules into Rust (`rust/codeanatomy_engine`, `rust/codeanatomy_engine_py`) and then deletes Python modules in controlled waves.

Each scope item includes:
1. Architectural implementation target
2. Representative code snippets
3. Exact file edit/create targets
4. Modules/functions deletable after scope completion
5. Implementation checklist

---

## 1) Scope Dependency Map

1. Scope A: Boundary Hardening (`facade` + run-result contracts)
2. Scope B: Spec Compilation Authority Move (`spec_builder` -> Rust)
3. Scope C: Runtime Profile + Session Authority Move
4. Scope D: Materialization + Delta Maintenance Authority Move
5. Scope E: Top-Level Build Orchestration Move
6. Scope F: Python Surface Cleanup + Export Removal
7. Scope G: Test/Golden/Docs Realignment + Final Sweep

Critical dependency order:
1. A -> B -> C -> D -> E -> F -> G
2. C and D can partially overlap after A/B land.

---

## 1A) Implementation Status Checkpoint (2026-02-11)

This checkpoint reflects the repository state after the current in-flight decommission branch updates.

### Scope status matrix

| Scope | Current status | Evidence in repo | Remaining work |
|---|---|---|---|
| A: Rust/Python boundary hardening | Partially complete | `src/engine/facade.py` and `src/engine/rust_planning_contract.py` are deleted; `rust/codeanatomy_engine_py/src/result.rs` exposes `task_schedule()`; `src/planning_engine/build_orchestrator.py` now invokes Rust compiler/materializer directly. | Replace Python-side heuristic error classification in `src/planning_engine/build_orchestrator.py` with Rust-originated typed stage/code errors; remove Python `result.to_json()` decoding at callsites by exposing a typed dict payload helper from Rust bindings. |
| B: Spec compilation authority to Rust | Partially complete | `src/cli/commands/plan.py` now consumes `compile_metadata_json(...)`; Rust binding exists in `rust/codeanatomy_engine_py/src/compiler.rs`. | Python `src/planning_engine/spec_builder.py` is still authoritative and widely imported; transform/spec derivation is not yet Rust-owned; CLI/build/graph still depend on Python spec construction. |
| C: Runtime profile + session authority move | Partially complete | Legacy `src/engine/runtime*.py` and session modules are deleted from old package namespace; runtime/session consumers were moved to `planning_engine.*` imports. | Runtime/session authority is still Python in `src/planning_engine/runtime_profile.py`, `src/planning_engine/runtime.py`, `src/planning_engine/session.py`, and `src/planning_engine/session_factory.py`; extraction callsites still construct sessions through these Python wrappers. |
| D: Materialization + Delta maintenance authority move | Partially complete | Old `src/engine/materialize_pipeline.py`, `src/engine/delta_tools.py`, `src/engine/semantic_boundary.py`, and `src/engine/plan_product.py` are deleted from the old package namespace. | Their functionality remains Python-owned in `src/planning_engine/materialize_pipeline.py`, `src/planning_engine/delta_tools.py`, `src/planning_engine/semantic_boundary.py`, and `src/planning_engine/plan_product.py`; extraction/materialization callsites still use these modules. |
| E: Top-level build orchestration move | Partially complete | `src/cli/commands/build.py` and `src/graph/product_build.py` moved off `engine.*` imports and now call `planning_engine.build_orchestrator.orchestrate_build(...)`. | No Rust `run_build` entrypoint exists yet in `rust/codeanatomy_engine_py/src/lib.rs`; orchestration remains Python-owned in `src/planning_engine/build_orchestrator.py`, including auxiliary output writing. |
| F: Python surface cleanup + export removal | Largely complete (hard-cut variant) | Entire tracked `src/engine` Python surface was deleted; migration guard updated in `scripts/check_no_legacy_planning_imports.sh`; CI already invokes guard (`.github/workflows/rust_quality.yml`). | If compatibility stubs are required, reintroduce a minimal compatibility policy explicitly; otherwise complete documentation updates and ensure no lingering references in docs/goldens. |
| G: Tests/goldens/docs realignment | Partially complete | `tests/unit/engine/test_facade.py` and `tests/unit/test_engine_error_taxonomy.py` deleted; msgspec goldens updated to `planning_engine.config.EngineConfigSpec`; Rust boundary tests for `compile_metadata_json` and `RunResult.task_schedule()` exist in `tests/integration/test_rust_engine_e2e.py`; plan-command test doubles were updated for `compile_metadata_json`. | Broad test realignment is not complete yet; Python-internal `planning_engine` tests remain; full `pytest -q` is not yet passing on this branch and still requires failure-cluster remediation and golden alignment. |

### Completed scope elements (branch-level)

1. Hard decommission of tracked `src/engine` modules.
2. Replacement namespace `src/planning_engine` created and wired into `src/` + `tests/`.
3. Legacy import guard tightened to include direct `engine` import patterns.
4. Plan command migrated to Rust compile metadata contract (`compile_metadata_json`).
5. Contract goldens updated to new module path for engine config contract class.

### Open decommission-critical work

1. Rust-first replacement of Python `spec_builder` authority (`src/planning_engine/spec_builder.py`).
2. Rust-first replacement of Python runtime/session wrappers (`src/planning_engine/runtime_profile.py`, `src/planning_engine/session_factory.py`).
3. Rust-first replacement of Python materialization and Delta helpers (`src/planning_engine/materialize_pipeline.py`, `src/planning_engine/delta_tools.py`).
4. Rust `run_build` orchestration boundary plus CLI/graph cutover to that API.
5. Test/golden/doc closure to green full quality gate.

---

## Scope A: Rust/Python Boundary Hardening

### Architectural goal

Move typed result decoding and error-stage mapping into Rust bindings so Python no longer owns runtime contract normalization.

### Representative code snippets

```rust
// rust/codeanatomy_engine_py/src/result.rs
#[pyclass]
pub struct RunResult {
    // existing fields
}

#[pymethods]
impl RunResult {
    pub fn task_schedule(&self) -> PyResult<PyObject> {
        // Return structured schedule payload directly from Rust contract.
        // Python should not re-interpret run-result JSON.
    }
}
```

```python
# src/engine/facade.py (target steady-state)
compiled = compiler.compile(spec_json)
result = materializer.execute(factory, compiled)
payload = result.to_json_dict()  # Rust-owned shape and error taxonomy
```

```rust
// rust/codeanatomy_engine_py/src/lib.rs
#[pyfunction]
fn execute_cpg_build_py(/* ... */) -> PyResult<PyObject> {
    // Convert Rust typed errors to Python exceptions with explicit stage/code.
}
```

### Target files to edit/create

1. Edit `rust/codeanatomy_engine_py/src/result.rs`
2. Edit `rust/codeanatomy_engine_py/src/lib.rs`
3. Edit `rust/codeanatomy_engine_py/src/compiler.rs`
4. Edit `src/engine/facade.py`
5. Delete `src/engine/rust_planning_contract.py`
6. Edit tests:
1. `tests/unit/engine/test_facade.py`
2. `tests/unit/test_engine_error_taxonomy.py`
3. `tests/contracts/test_output_contracts.py` (contract assertions from Rust payloads)

### Decommission/delete after Scope A

1. Module `src/engine/rust_planning_contract.py`
2. Functions in `src/engine/facade.py`:
1. `decode_run_result`
2. `plan_bundles_from_run_result`
3. `_raise_engine_error` (if Rust exposes explicit typed exception mapping)

### Implementation checklist

1. Add Rust-side typed run-result accessors for schedule/plan bundles.
2. Add Rust-side stable error stage/code envelope exposed to Python.
3. Remove Python payload re-decoding path.
4. Remove Python run-result contract dataclasses.
5. Update unit tests to assert Rust-owned contract shape.
6. Verify no imports of `engine.rust_planning_contract` remain.

---

## Scope B: Spec Compilation Authority Move to Rust

### Architectural goal

Eliminate Python-owned spec graph/transform derivation (`spec_builder`) and make Rust compiler the single source of truth for transform/build semantics.

### Representative code snippets

```rust
// rust/codeanatomy_engine/src/compiler/plan_compiler.rs
pub fn compile_metadata_json(&self, semantic_input_json: &str) -> Result<String> {
    // Parse input IR contract
    // Build ViewDefinition/JoinGraph/RuleIntents in Rust
    // Compile + return metadata JSON
}
```

```rust
// Uses DataFusion LogicalPlanBuilder for deterministic structural plan build
use datafusion::logical_expr::LogicalPlanBuilder;

let plan = LogicalPlanBuilder::from(source_plan)
    .filter(predicate_expr)?
    .project(project_exprs)?
    .build()?;
```

```python
# src/cli/commands/plan.py (target)
metadata_json = compiler.compile_metadata_json(semantic_input_json)
metadata = msgspec.json.decode(metadata_json, type=dict[str, object])
```

### Target files to edit/create

1. Edit `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`
2. Edit `rust/codeanatomy_engine/src/compiler/view_builder.rs`
3. Edit `rust/codeanatomy_engine/src/compiler/join_builder.rs`
4. Edit `rust/codeanatomy_engine/src/spec/relations.rs`
5. Edit `rust/codeanatomy_engine_py/src/compiler.rs`
6. Edit Python callsites:
1. `src/cli/commands/plan.py`
2. `src/cli/commands/build.py`
3. `src/graph/product_build.py`
4. `src/engine/build_orchestrator.py`
7. Edit or remove `src/engine/spec_builder.py`
8. Edit `src/engine/config.py` (remove `RulepackProfile` / `TracingPreset` dependency on `spec_builder`)

### Decommission/delete after Scope B

1. Module `src/engine/spec_builder.py` (full delete if all callsites are moved)
2. Functions:
1. `build_execution_spec`
2. `build_spec_from_ir`
3. `_build_transform`
4. `_build_relate_transform`
5. `_default_rule_intents`

### Implementation checklist

1. Define Rust input contract for semantic IR and output targets.
2. Implement Rust-side transform derivation and validation.
3. Expose Rust compile metadata API through PyO3.
4. Replace all Python callsites that build spec objects directly.
5. Remove Python `spec_builder` imports from CLI/graph/engine.
6. Add integration tests for deterministic `compile_metadata_json`.

---

## Scope C: Runtime Profile + Session Authority Move

### Architectural goal

Move runtime profile resolution, environment overrides, and session construction from Python (`runtime_profile`, `runtime`, `session`, `session_factory`) into Rust session factory APIs.

### Representative code snippets

```rust
// rust/codeanatomy_engine/src/session/runtime_profiles.rs
pub fn resolve_profile(
    profile_name: &str,
    env: &HashMap<String, String>,
) -> RuntimeProfile {
    // apply named profile defaults + env overrides
}
```

```rust
// rust/codeanatomy_engine/src/session/factory.rs
let session = SessionFactory::from_class("medium")
    .with_profile(profile)
    .build()?;
```

```python
# src/extraction/orchestrator.py (target)
factory = codeanatomy_engine.SessionFactory.from_class(profile_name)
session = factory.build_with_overrides(runtime_overrides)
```

### Target files to edit/create

1. Edit `rust/codeanatomy_engine/src/session/runtime_profiles.rs`
2. Edit `rust/codeanatomy_engine/src/session/factory.rs`
3. Edit `rust/codeanatomy_engine_py/src/session.rs`
4. Edit callsites:
1. `src/extraction/orchestrator.py`
2. `src/extract/session.py`
3. `src/extract/coordination/context.py`
4. `src/extract/extractors/scip/extract.py`
5. `src/semantics/incremental/metadata.py` (profile snapshot source)
5. Delete or shrink:
1. `src/engine/runtime_profile.py`
2. `src/engine/runtime.py`
3. `src/engine/session.py`
4. `src/engine/session_factory.py`

### Decommission/delete after Scope C

1. Module `src/engine/runtime.py`
2. Module `src/engine/session.py`
3. Module `src/engine/session_factory.py`
4. Module `src/engine/runtime_profile.py` (only when all runtime-profile callsites are Rust-bound)

### Implementation checklist

1. Add Rust runtime profile presets and env patch parser parity.
2. Add Rust runtime profile snapshot/hash emission API.
3. Expose Rust `SessionFactory` and runtime-profile snapshot in PyO3.
4. Move extraction/orchestration callsites to Rust factory.
5. Remove Python runtime/session wrappers and update tests.
6. Validate deterministic profile-hash behavior in integration tests.

---

## Scope D: Materialization + Delta Maintenance Authority Move

### Architectural goal

Move Python DataFusion materialization and Delta maintenance helpers into Rust executor/maintenance modules.

### Representative code snippets

```rust
// rust/codeanatomy_engine/src/executor/pipeline.rs
pub fn execute_materialization(
    ctx: &SessionContext,
    plan: &CompiledPlan,
    options: &ExecutionOptions,
) -> Result<RunResult> {
    // compile -> execute -> write outputs -> emit diagnostics
}
```

```rust
// rust/codeanatomy_engine/src/executor/maintenance.rs
pub fn vacuum_delta(path: &str, retention_hours: Option<u64>, dry_run: bool) -> Result<Vec<String>> {
    // Delta maintenance operation delegated to Rust maintenance layer
}
```

```python
# Removed target pattern from Python
# from engine.materialize_pipeline import build_view_product, write_extract_outputs
# from engine.delta_tools import delta_history, delta_vacuum, delta_query
```

### Target files to edit/create

1. Edit `rust/codeanatomy_engine/src/executor/pipeline.rs`
2. Edit `rust/codeanatomy_engine/src/executor/maintenance.rs`
3. Edit `rust/codeanatomy_engine_py/src/materializer.rs`
4. Edit `rust/codeanatomy_engine_py/src/lib.rs` (maintenance APIs)
5. Move callsites from Python modules:
1. `src/extract/coordination/materialization.py`
2. `src/extract/extractors/scip/extract.py`
3. `src/extraction/orchestrator.py`
6. Delete:
1. `src/engine/materialize_pipeline.py`
2. `src/engine/delta_tools.py`
3. `src/engine/semantic_boundary.py`
4. `src/engine/plan_product.py`

### Decommission/delete after Scope D

1. Module `src/engine/materialize_pipeline.py`
2. Module `src/engine/delta_tools.py`
3. Module `src/engine/semantic_boundary.py`
4. Module `src/engine/plan_product.py`
5. Functions:
1. `build_view_product`
2. `write_extract_outputs`
3. `delta_history`
4. `delta_vacuum`
5. `delta_query`

### Implementation checklist

1. Implement Rust materialization API parity for current Python execution paths.
2. Implement Rust maintenance API parity for history/vacuum/query pathways.
3. Ensure Delta registration uses DataFusion provider paths (`register_table`) for pushdown.
4. Move extraction/materialization callsites to Rust APIs.
5. Remove Python materialization/maintenance helpers.
6. Add integration tests for schedule + write + maintenance flows.

---

## Scope E: Top-Level Build Orchestration Move

### Architectural goal

Replace Python orchestrator (`engine.build_orchestrator`) with a Rust-owned orchestration boundary and keep Python CLI/graph entrypoints as thin adapters.

### Representative code snippets

```rust
// rust/codeanatomy_engine_py/src/lib.rs
#[pyfunction]
fn run_build(request_json: &str) -> PyResult<PyObject> {
    // extraction inputs -> compile -> execute -> output summary envelope
}
```

```python
# src/cli/commands/build.py (target)
request = {"repo_root": str(repo_root), "work_dir": str(work_dir), "output_dir": str(output_dir)}
result = codeanatomy_engine.run_build(msgspec.json.encode(request).decode())
```

```python
# src/graph/product_build.py (target)
run_result = codeanatomy_engine.run_build(request_json)
```

### Target files to edit/create

1. Edit `rust/codeanatomy_engine/src/executor/pipeline.rs` (or add orchestrator module)
2. Edit `rust/codeanatomy_engine_py/src/lib.rs`
3. Edit `src/cli/commands/build.py`
4. Edit `src/graph/product_build.py`
5. Delete `src/engine/build_orchestrator.py`
6. Potentially delete `src/engine/facade.py` if `execute_cpg_build` becomes unnecessary
7. Edit `src/engine/output_contracts.py` consumers to use Rust result schema directly

### Decommission/delete after Scope E

1. Module `src/engine/build_orchestrator.py`
2. Module `src/engine/facade.py` (if no remaining external consumers)
3. Module `src/engine/output_contracts.py` (if aliases are no longer required)
4. Functions:
1. `orchestrate_build`
2. `execute_cpg_build` (if replaced by one Rust build API)

### Implementation checklist

1. Define Rust build-request/build-result contract.
2. Expose one Python-callable build entrypoint from Rust.
3. Move CLI and graph product callsites to Rust entrypoint.
4. Remove orchestrator helper functions and auxiliary output writers from Python.
5. Preserve observability payload fields required by existing dashboards/tests.
6. Validate end-to-end build parity on representative repos.

---

## Scope F: Python Surface Cleanup + Export Removal

### Architectural goal

Remove legacy `engine` package re-export surface and delete modules that are now unused after the migration scopes.

### Representative code snippets

```python
# src/engine/__init__.py (target steady-state)
"""Legacy package retained for compatibility."""

__all__: list[str] = []
```

```bash
# repo guard to block reintroduction
rg -n "from engine\\.(spec_builder|runtime_profile|materialize_pipeline|delta_tools) import" src tests && exit 1
```

### Target files to edit/create

1. Edit `src/engine/__init__.py`
2. Delete immediate-low-risk files:
1. `src/engine/profile.py`
2. `src/engine/telemetry/__init__.py`
3. Delete `src/engine/config.py` or move needed types into `src/cli/config_models.py`
4. Add migration guard script:
1. `scripts/ci/forbid_legacy_engine_imports.sh`
5. Wire guard into CI entrypoint used by repo

### Decommission/delete after Scope F

1. Module `src/engine/profile.py`
2. Module `src/engine/telemetry/__init__.py`
3. Module `src/engine/config.py` (once CLI owns required config types)
4. Large portions of `src/engine/__init__.py` lazy export map

### Implementation checklist

1. Remove exports for all deleted modules from `engine.__init__`.
2. Move any remaining config-only types to CLI-owned modules.
3. Add CI guard for forbidden legacy engine imports.
4. Confirm no production callsites use `from engine import ...` legacy exports.
5. Update docs to point to Rust boundary APIs.

---

## Scope G: Test, Golden, and Docs Realignment

### Architectural goal

Replace Python-module-focused tests with behavior/contract tests at Rust boundary and remove stale test references to deleted modules.

### Representative code snippets

```python
def test_compile_metadata_json_contract_is_stable() -> None:
    compiler = codeanatomy_engine.SemanticPlanCompiler()
    metadata = msgspec.json.decode(compiler.compile_metadata_json(spec_json), type=dict[str, object])
    assert "task_schedule" in metadata
    assert "dependency_map" in metadata
```

```python
def test_run_result_task_schedule_roundtrip() -> None:
    result = run_fixture_build()
    schedule = result.task_schedule()
    assert isinstance(schedule, dict)
    assert "tasks" in schedule
```

### Target files to edit/create

1. Edit integration tests:
1. `tests/integration/relspec/test_compile_execution_plan.py` (replace with Rust boundary contract tests)
2. `tests/integration/scheduling/test_schedule_generation.py`
3. `tests/integration/relspec/test_schedule_edge_validation.py`
2. Edit/remove unit tests tied only to deleted engine modules.
3. Update contract goldens if engine schema references are removed.
4. Update docs:
1. `docs/plans` artifacts describing migration completion and deleted surfaces.

### Decommission/delete after Scope G

1. Any remaining Python tests importing deleted engine modules.
2. Compatibility-only assertions against old `engine.output_contracts` alias names.

### Implementation checklist

1. Replace module-level tests with Rust contract behavior tests.
2. Remove stale imports of deleted modules in tests.
3. Regenerate and commit any changed contract goldens.
4. Run full repo quality gate:
1. `uv run ruff format`
2. `uv run ruff check --fix`
3. `uv run pyrefly check`
4. `uv run pyright`
5. `uv run pytest -q`

---

## 2) Deletions That Require Multiple Scope Items

These files/functions cannot be safely deleted until multiple scopes are completed.

| Module/function | Required scopes | Why |
|---|---|---|
| `src/engine/facade.py` | A + E | Scope A removes decode/error shims; Scope E removes remaining execute entrypoint usage. |
| `src/engine/output_contracts.py` | A + E + G | Contract source moves to Rust in A/E; tests/docs must be realigned in G before final delete. |
| `src/engine/spec_builder.py` | B + E | Rust must own spec derivation (B), and all top-level build paths must stop importing it (E). |
| `src/engine/runtime_profile.py` | C + D | Runtime/session move in C; extraction/materialization users must be moved in D. |
| `src/engine/session_factory.py` | C + D | Session ownership moves in C; remaining extract/materialize callsites moved in D. |
| `src/engine/build_orchestrator.py` | D + E | Depends on Rust materialization parity (D) and Rust top-level build API (E). |
| `src/engine/__init__.py` (full deletion) | F + G | Requires all module deletions complete and tests/docs no longer depending on package exports. |
| `src/engine/config.py` | B + F | Remove `spec_builder` type dependencies (B), then migrate config types to CLI (F). |

---

## 3) Final Execution Checklist (Repo-Level)

1. Land changes in bisect-safe PR sequence:
1. PR1: Scope A + start of Scope B
2. PR2: finish Scope B + Scope C
3. PR3: Scope D + Scope E
4. PR4: Scope F + Scope G
2. After each PR, enforce no reintroduction of deleted imports.
3. Run full quality gate at each PR boundary.
4. Publish breaking-change notes for removed Python `engine` internals.
