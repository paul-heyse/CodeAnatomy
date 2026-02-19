# Rust Crates Cleanup Implementation Plan v1 (2026-02-18)

**Topic:** rust-crates-cleanup
**Version:** v1
**Date:** 2026-02-18
**Author:** Agent (Claude Sonnet 4.6)
**Source Reviews:**
- `docs/reviews/design_review_rust_core_engine_2026-02-18.md` (Agent 4 — codeanatomy_engine)
- `docs/reviews/design_review_rust_df_extensions_plugins_2026-02-18.md` (Agent 8/10 — datafusion_ext, datafusion_python, plugin crates)
- `docs/reviews/design_review_rust_python_bindings_2026-02-18.md` (Agent 11 — codeanatomy_engine_py, datafusion_ext_py, plugin crates)

---

## Scope Summary

Twenty-nine targeted improvements across `rust/` crates and DataFusion planning/runtime surfaces under `src/datafusion_engine/`, drawn from three design reviews plus the approved alignment review in `docs/plans/rust_crates_cleanup_plan_review_and_datafusion_alignment_v1_2026-02-19.md`. Items are grouped for safe execution: correctness and contract fixes first, then deduplication and runtime hygiene, then provider/planner architecture upgrades, then cross-layer convergence, and finally decommission/test hardening.

Design stance: incremental migration with explicit hard cutover of phantom capabilities and global fallback context patterns once the replacement surfaces are validated.

Crates covered:

| Crate | Path |
|---|---|
| Core engine | `rust/codeanatomy_engine/` |
| DataFusion extensions | `rust/datafusion_ext/` |
| Python DataFusion bindings | `rust/datafusion_python/` |
| Plugin API | `rust/df_plugin_api/` |
| Plugin host | `rust/df_plugin_host/` |
| Plugin common | `rust/df_plugin_common/` |
| Plugin implementation | `rust/df_plugin_codeanatomy/` |
| Engine PyO3 bindings | `rust/codeanatomy_engine_py/` |
| Extensions PyO3 bindings | `rust/datafusion_ext_py/` |
| Python DataFusion runtime/planning layer | `src/datafusion_engine/` |

**Out of scope:** Unrelated Python modules outside `src/datafusion_engine/` (for example CLI, extraction, semantics) except where needed for interface compatibility wiring.

---

## Design Principles

1. **Behavior changes are explicit and test-backed.** Deduplication and rename items must preserve semantics; behavior changes must be called out in the item goal and guarded by regression tests.
2. **Cross-layer determinism contract.** Planning-affecting behavior must be represented by explicit, versioned contracts across both `rust/` and `src/datafusion_engine/` surfaces (planners, factories, options, and capability reporting).
3. **Truthful capability surfaces.** No capability bit, API field, or runtime status may be advertised without executable implementation and tests.
4. **DataFusion-native first.** Prefer DataFusion builder/planner/provider/runtime primitives over bespoke wrappers when equivalent native surfaces exist.
5. **Smallest safe diff.** Move and centralize code rather than rewrite where possible, except when replacing unsafe patterns (nested bare SessionContext creation, global fallback context providers).
6. **Quality gate before merge.** Rust-scope changes pass `cargo build --workspace` and `cargo test --workspace`; Python integration changes run targeted `uv run` tests for modified `src/datafusion_engine` contracts.

---

## Current Baseline

### Key files verified to exist

```
rust/codeanatomy_engine/src/compiler/compile_phases.rs
rust/codeanatomy_engine/src/compiler/plan_compiler.rs
rust/codeanatomy_engine/src/compiler/scheduling.rs
rust/codeanatomy_engine/src/executor/delta_writer.rs
rust/codeanatomy_engine/src/executor/pipeline.rs
rust/codeanatomy_engine/src/executor/result.rs
rust/codeanatomy_engine/src/providers/interval_align_provider.rs
rust/codeanatomy_engine/src/providers/mod.rs
rust/codeanatomy_engine/src/providers/pushdown_contract.rs
rust/codeanatomy_engine/src/spec/hashing.rs
rust/datafusion_ext/src/async_runtime.rs
rust/datafusion_ext/src/planner_rules.rs
rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs
rust/datafusion_python/src/codeanatomy_ext/helpers.rs
rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs
rust/datafusion_python/src/codeanatomy_ext/session_utils.rs
rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs
rust/df_plugin_api/src/lib.rs
rust/df_plugin_api/src/manifest.rs
rust/df_plugin_codeanatomy/src/lib.rs
rust/df_plugin_codeanatomy/src/udf_bundle.rs
rust/df_plugin_codeanatomy/src/task_context.rs
rust/codeanatomy_engine_py/src/compiler.rs
rust/codeanatomy_engine_py/src/session.rs
rust/datafusion_ext_py/src/lib.rs
src/datafusion_engine/session/runtime_extensions.py
src/datafusion_engine/session/protocols.py
src/datafusion_engine/expr/relation_planner.py
src/datafusion_engine/expr/planner.py
src/datafusion_engine/extensions/context_adaptation.py
src/datafusion_engine/extensions/datafusion_ext.py
src/datafusion_engine/session/runtime_profile_config.py
src/datafusion_engine/session/runtime_config_policies.py
src/datafusion_engine/session/delta_session_builder.py
src/datafusion_engine/session/context_pool.py
```

### Critical baseline facts

- `TaskGraph::topological_sort` (`scheduling.rs:225`) uses `VecDeque` with FIFO insertion, **no lexicographic tie-breaking**.
- `SemanticPlanCompiler::topological_sort` (`plan_compiler.rs:171`) re-sorts the queue at every insertion for deterministic output.
- `enforce_pushdown_contracts` private function exists verbatim in both `compile_phases.rs:131` and `pipeline.rs` (not shown in excerpt but confirmed by reviews).
- `helpers::runtime()` at `helpers.rs:129` calls `Runtime::new()` per invocation; `datafusion_ext::async_runtime::shared_runtime()` already provides a process-level shared runtime.
- `delta_mutations.rs:183` and `:214` each have `let _ = request.extra_constraints;` — confirmed in source.
- `decode_schema_ipc` is defined identically at `helpers.rs:104` and `schema_evolution.rs:38`.
- `session_context_contract` at `session_utils.rs:64` duplicates `extract_session_ctx` at `helpers.rs:35`.
- `spec/hashing.rs::DeterminismContract` (two fields) is a strict subset of `executor/result.rs::DeterminismContract` (three fields).
- `caps::RELATION_PLANNER = 1 << 5` is declared in `manifest.rs:13` and asserted in `df_plugin_codeanatomy/src/lib.rs:38` but has no corresponding field in `DfPluginExportsV1`.
- `build_udf_bundle` in `udf_bundle.rs:114` swallows errors and returns an empty bundle — confirmed in source.
- `install_policy_rules` at `planner_rules.rs:100` calls `state.add_analyzer_rule(...)` without checking for duplicates.
- `IntervalAlignProvider::build_dataframe` at `interval_align_provider.rs:102` creates `SessionContext::new()` inline.
- Relation-planner integration in `src/datafusion_engine` is protocol-level only (`session/protocols.py`, `expr/relation_planner.py`) and currently defaults to no-op behavior.
- Provider capsule construction in `datafusion_python` still relies on a global task-context provider (`helpers.rs::global_task_ctx_provider`), while Python runtime relies on context-adaptation fallback probes.
- `PlanningSurfaceSpec` in `rust/codeanatomy_engine/src/session/planning_surface.rs` includes expr planners but no explicit type/relation planner slots.
- Runtime policy translation in `src/datafusion_engine` remains heavily string-key based (`runtime_config_policies.py`, `delta_session_builder.py`, `context_pool.py`) and is not yet unified with the Rust typed planning surface.

---

## Per-Scope-Item Sections

---

### S1. Fix silent extra_constraints drop on delete/update

**Priority:** CRITICAL (P8 / P21)
**Crate:** `rust/datafusion_python/`
**Effort:** small

#### Goal

`delta_delete_request` and `delta_update_request` both deserialize `extra_constraints` from the payload but discard it with `let _ = request.extra_constraints;`. Callers who supply non-empty constraints receive no error and no enforcement, creating a silent data-integrity contract violation.

Add a validation error when non-empty `extra_constraints` are passed to delete or update operations.

#### Representative Code Snippets

**Current (broken contract):**

```rust
// rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:183
let request: DeltaDeleteRequestPayload =
    parse_msgpack_payload(&request_msgpack, "delta_delete_request")?;
let _ = request.extra_constraints;   // <-- silently discards constraint data
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs:214
let request: DeltaUpdateRequestPayload =
    parse_msgpack_payload(&request_msgpack, "delta_update_request")?;
if request.updates.is_empty() { ... }
let _ = request.extra_constraints;   // <-- same issue
```

**After (explicit validation error):**

```rust
// delta_delete_request (line ~183)
let request: DeltaDeleteRequestPayload =
    parse_msgpack_payload(&request_msgpack, "delta_delete_request")?;
if let Some(constraints) = &request.extra_constraints {
    if !constraints.is_empty() {
        return Err(PyValueError::new_err(
            "extra_constraints are not supported on delete operations. \
             Pass an empty list or omit the field.",
        ));
    }
}
```

```rust
// delta_update_request (line ~214)
if let Some(constraints) = &request.extra_constraints {
    if !constraints.is_empty() {
        return Err(PyValueError::new_err(
            "extra_constraints are not supported on update operations. \
             Pass an empty list or omit the field.",
        ));
    }
}
```

#### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs` — replace two `let _ = request.extra_constraints;` lines with validation guards.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

None. The field remains on the struct to preserve deserialization compatibility. The change only converts a silent no-op into an explicit error.

#### Tests

Add `#[cfg(test)]` cases (or integration tests in `tests/provider_registration.rs`) verifying:

- `delta_delete_request` with non-empty `extra_constraints` returns `PyValueError`.
- `delta_update_request` with non-empty `extra_constraints` returns `PyValueError`.
- Both functions accept `extra_constraints: None` and `extra_constraints: Some(vec![])` without error.

---

### S2. Deduplicate enforce_pushdown_contracts

**Priority:** HIGH (P7)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** small

#### Goal

The private function `enforce_pushdown_contracts` is byte-for-byte identical in `compiler/compile_phases.rs:131` and `executor/pipeline.rs`. Move the canonical copy to `providers/pushdown_contract.rs` as `pub(crate)` and delete both originals.

#### Representative Code Snippets

**Current (duplicated):**

```rust
// rust/codeanatomy_engine/src/compiler/compile_phases.rs:131
fn enforce_pushdown_contracts(
    table_name: &str,
    mode: PushdownEnforcementMode,
    report: Option<&PushdownContractReport>,
    warnings: &mut Vec<RunWarning>,
) -> Result<()> {
    let Some(report) = report else { return Ok(()); };
    for violation in &report.violations {
        match mode {
            PushdownEnforcementMode::Strict => {
                return Err(DataFusionError::Plan(format!(
                    "Pushdown contract violation on table '{}': {}",
                    violation.table_name, violation.predicate_text
                )))
            }
            PushdownEnforcementMode::Warn => warnings.push( /* ... */ ),
            PushdownEnforcementMode::Disabled => {}
        }
    }
    Ok(())
}
```

**After (single canonical copy in pushdown_contract.rs):**

```rust
// rust/codeanatomy_engine/src/providers/pushdown_contract.rs
pub(crate) fn enforce_pushdown_contracts(
    table_name: &str,
    mode: PushdownEnforcementMode,
    report: Option<&PushdownContractReport>,
    warnings: &mut Vec<RunWarning>,
) -> datafusion_common::Result<()> {
    // ... identical body ...
}
```

```rust
// compile_phases.rs — import instead of define
use crate::providers::pushdown_contract::enforce_pushdown_contracts;
```

```rust
// pipeline.rs — same import
use crate::providers::pushdown_contract::enforce_pushdown_contracts;
```

#### Files to Edit

- `rust/codeanatomy_engine/src/providers/pushdown_contract.rs` — add `pub(crate) fn enforce_pushdown_contracts(...)`.
- `rust/codeanatomy_engine/src/compiler/compile_phases.rs` — delete private function, add import.
- `rust/codeanatomy_engine/src/executor/pipeline.rs` — delete private function, add import.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

**D1.** Delete `fn enforce_pushdown_contracts` from `compile_phases.rs`.
**D2.** Delete `fn enforce_pushdown_contracts` from `executor/pipeline.rs`.

---

### S3. Deduplicate task graph construction

**Priority:** HIGH (P7)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** small

#### Goal

The three-step extraction of `view_deps`, `scan_deps`, `output_deps` and the subsequent `TaskGraph::from_inferred_deps` call are duplicated between `compiler/compile_phases.rs:38-54` (inside `build_task_schedule_phase`) and `executor/pipeline.rs` (inside `build_task_graph_and_costs`). Create a single free function `build_task_graph_from_spec` in `compiler/scheduling.rs` and have both call sites delegate to it.

#### Representative Code Snippets

**Current (duplicated pattern in compile_phases.rs):**

```rust
let view_deps: Vec<(String, Vec<String>)> = spec
    .view_definitions
    .iter()
    .map(|view| (view.name.clone(), view.view_dependencies.clone()))
    .collect();
let scan_deps: Vec<(String, Vec<String>)> = spec
    .input_relations
    .iter()
    .map(|input| (input.logical_name.clone(), Vec::new()))
    .collect();
let output_deps: Vec<(String, Vec<String>)> = spec
    .output_targets
    .iter()
    .map(|target| (target.table_name.clone(), vec![target.source_view.clone()]))
    .collect();
let task_graph = TaskGraph::from_inferred_deps(&view_deps, &scan_deps, &output_deps)?;
```

**After (new public function in scheduling.rs):**

```rust
// rust/codeanatomy_engine/src/compiler/scheduling.rs
use crate::spec::execution_spec::SemanticExecutionSpec;

/// Build a `TaskGraph` from a semantic execution spec.
///
/// Extracts view, scan, and output dependency slices from the spec and
/// delegates to `TaskGraph::from_inferred_deps`. This is the single
/// authoritative task-graph construction path.
pub fn build_task_graph_from_spec(spec: &SemanticExecutionSpec) -> Result<TaskGraph> {
    let view_deps: Vec<(String, Vec<String>)> = spec
        .view_definitions
        .iter()
        .map(|v| (v.name.clone(), v.view_dependencies.clone()))
        .collect();
    let scan_deps: Vec<(String, Vec<String>)> = spec
        .input_relations
        .iter()
        .map(|i| (i.logical_name.clone(), Vec::new()))
        .collect();
    let output_deps: Vec<(String, Vec<String>)> = spec
        .output_targets
        .iter()
        .map(|t| (t.table_name.clone(), vec![t.source_view.clone()]))
        .collect();
    TaskGraph::from_inferred_deps(&view_deps, &scan_deps, &output_deps)
}
```

```rust
// compile_phases.rs — replacement
let task_graph = build_task_graph_from_spec(spec)?;
```

#### Files to Edit

- `rust/codeanatomy_engine/src/compiler/scheduling.rs` — add `pub fn build_task_graph_from_spec(spec: &SemanticExecutionSpec) -> Result<TaskGraph>`.
- `rust/codeanatomy_engine/src/compiler/compile_phases.rs` — replace inline extraction with `build_task_graph_from_spec(spec)`.
- `rust/codeanatomy_engine/src/executor/pipeline.rs` — replace inline extraction with `build_task_graph_from_spec(spec)`.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

**D3.** Delete the three-step extraction blocks (view_deps / scan_deps / output_deps + `from_inferred_deps` call) from both `compile_phases.rs` and `pipeline.rs`.

---

### S4. Unify topological sort implementations

**Priority:** HIGH (P7 / P16 / P18)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** small (determinism fix) + small (delegation wiring)

#### Goal

`TaskGraph::topological_sort` (FIFO queue, no lexicographic tie-breaking) and `SemanticPlanCompiler::topological_sort` (re-sorted queue, deterministic) produce different orderings on identical graphs. This divergence means the `TaskSchedule.execution_order` and the actual view compilation order may differ — a correctness issue for determinism auditing (P18).

**Step 1:** Add lexicographic tie-breaking to `TaskGraph::topological_sort` so it matches the deterministic behavior of the compiler version.

**Step 2:** Remove `SemanticPlanCompiler::topological_sort` and have `compile_with_warnings` construct order via `TaskGraph`.

#### Representative Code Snippets

**Current `TaskGraph::topological_sort` (no tie-breaking):**

```rust
// rust/codeanatomy_engine/src/compiler/scheduling.rs:225
pub fn topological_sort(&mut self) -> Result<()> {
    // ...
    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter_map(|(name, degree)| {
            if *degree == 0 { Some(name.clone()) } else { None }
        })
        .collect();
    // ...
    while let Some(node) = queue.pop_front() {
        order.push(node.clone());
        if let Some(children) = outgoing.get(&node) {
            for child in children {
                if let Some(entry) = in_degree.get_mut(child) {
                    *entry = entry.saturating_sub(1);
                    if *entry == 0 {
                        queue.push_back(child.clone());  // FIFO, no sorting
                    }
                }
            }
        }
    }
}
```

**After (with lexicographic tie-breaking matching plan_compiler.rs behavior):**

```rust
pub fn topological_sort(&mut self) -> Result<()> {
    // ... (in_degree and outgoing construction unchanged) ...
    let mut init: Vec<String> = in_degree
        .iter()
        .filter_map(|(name, &deg)| if deg == 0 { Some(name.clone()) } else { None })
        .collect();
    init.sort();   // <-- deterministic initial queue
    let mut queue: VecDeque<String> = init.into_iter().collect();
    let mut order = Vec::with_capacity(self.nodes.len());

    while let Some(node) = queue.pop_front() {
        order.push(node.clone());
        if let Some(children) = outgoing.get(&node) {
            for child in children {
                if let Some(entry) = in_degree.get_mut(child) {
                    *entry = entry.saturating_sub(1);
                    if *entry == 0 {
                        queue.push_back(child.clone());
                        // Re-sort for deterministic tie-breaking
                        let mut v: Vec<String> = queue.into_iter().collect();
                        v.sort();
                        queue = v.into_iter().collect();
                    }
                }
            }
        }
    }
    // ... cycle detection unchanged ...
}
```

**Remove `SemanticPlanCompiler::topological_sort` and wire to TaskGraph:**

`SemanticPlanCompiler::compile_with_warnings` calls `self.topological_sort()` to get `Vec<ViewDefinition>`. Replace with:

```rust
// In compile_with_warnings, step 2:
let task_graph = build_task_graph_from_spec(self.spec)?;  // from S3
let name_to_view: HashMap<&str, &ViewDefinition> = self.spec
    .view_definitions
    .iter()
    .map(|v| (v.name.as_str(), v))
    .collect();
let ordered: Vec<ViewDefinition> = task_graph
    .topological_order
    .iter()
    .filter_map(|name| name_to_view.get(name.as_str()).copied().cloned())
    .collect();
```

This is a pure delegation: `TaskGraph::topological_sort` now produces the same lexicographic-deterministic ordering as the old `SemanticPlanCompiler::topological_sort`, so the behavioral contract is preserved.

#### Files to Edit

- `rust/codeanatomy_engine/src/compiler/scheduling.rs` — add lexicographic sort to initial queue construction and to post-decrement insertion in `topological_sort`.
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs` — remove `fn topological_sort`, update `compile_with_warnings` step 2 to use `build_task_graph_from_spec` + `topological_order`.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

**D4.** Delete `SemanticPlanCompiler::topological_sort` (lines 171–248 of `plan_compiler.rs`).

#### Tests

Extend `scheduling.rs` test module with a diamond-graph case that asserts lexicographic ordering of the tie-broken nodes (e.g., `["base", "left", "right", "merged"]` not `["base", "right", "left", "merged"]`).

---

### S5. Replace per-call Runtime::new() with shared runtime

**Priority:** HIGH (P16 / P19)
**Crate:** `rust/datafusion_python/`
**Effort:** medium

#### Goal

`helpers::runtime()` constructs a new multi-threaded Tokio runtime on every call. This is invoked from at least 8 call sites in `delta_mutations.rs` and `session_utils.rs`. Two isolated `Runtime::new()` calls also exist directly in `session_utils.rs:516` and `:527`. Replace all of these with `datafusion_ext::async_runtime::shared_runtime()?.block_on(...)`.

The shared runtime (`datafusion_ext::async_runtime::SHARED_RUNTIME`) is already the strategy used by the `PySessionContext` pymethods via `utils::get_tokio_runtime()`. This change unifies both code paths.

#### Representative Code Snippets

**Current:**

```rust
// rust/datafusion_python/src/codeanatomy_ext/helpers.rs:129
pub(crate) fn runtime() -> PyResult<Runtime> {
    Runtime::new()
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}")))
}

// Call site in delta_mutations.rs:113
let runtime = runtime()?;
let report = runtime.block_on(execute_delta_write_native(...)).map_err(...)?;
```

**After (at each call site):**

```rust
use datafusion_ext::async_runtime;

let report = async_runtime::shared_runtime()
    .map_err(|err| PyRuntimeError::new_err(format!("Shared runtime unavailable: {err}")))?
    .block_on(execute_delta_write_native(...))
    .map_err(|err| PyRuntimeError::new_err(format!("Delta write failed: {err}")))?;
```

**Isolated call sites in session_utils.rs (before deletion):**

```rust
// session_utils.rs:516 (approximate)
let runtime = Runtime::new()
    .map_err(|err| PyRuntimeError::new_err(...))?;
let result = runtime.block_on(some_async_fn(...))?;

// After:
let result = async_runtime::shared_runtime()
    .map_err(...)?
    .block_on(some_async_fn(...))?;
```

#### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs` — delete `pub(crate) fn runtime()` and its `use tokio::runtime::Runtime` import.
- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs` — replace all `runtime()` calls with inline `async_runtime::shared_runtime()?`.
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — replace all `build_runtime()` (aliased from `helpers::runtime`) calls and both isolated `Runtime::new()` calls with `async_runtime::shared_runtime()?`.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

**D5.** Delete `pub(crate) fn runtime()` from `helpers.rs`.
**D6.** Delete `use tokio::runtime::Runtime;` from `helpers.rs` (if no longer needed after removal).
**D7.** Remove `runtime as build_runtime` re-export alias from `session_utils.rs` import list (line ~55: `runtime as build_runtime`).

---

### S6. Consolidate duplicate session extraction

**Priority:** HIGH (P7 / P4)
**Crate:** `rust/datafusion_python/`
**Effort:** small

#### Goal

Two nearly identical functions extract a `SessionContext` from a Python object:

- `extract_session_ctx` in `helpers.rs:35-42`
- `session_context_contract` in `session_utils.rs:64-80` (returns a wrapper `SessionContextContract`)

Both attempt `ctx.extract::<Bound<'_, PySessionContext>>()` then fall back to `ctx.getattr("ctx")`. The only difference is that `session_context_contract` wraps the result in a private `SessionContextContract` struct and has a slightly different error message.

Delete `session_context_contract`, replace all call sites with `extract_session_ctx`, and remove the `SessionContextContract` wrapper struct if it is used only to call `.ctx`.

#### Representative Code Snippets

**Current duplicate in session_utils.rs:**

```rust
// rust/datafusion_python/src/codeanatomy_ext/session_utils.rs:59-80
#[derive(Clone)]
struct SessionContextContract {
    ctx: SessionContext,
}

fn session_context_contract(ctx: &Bound<'_, PyAny>) -> PyResult<SessionContextContract> {
    if let Ok(session) = ctx.extract::<Bound<'_, PySessionContext>>() {
        return Ok(SessionContextContract { ctx: session.borrow().ctx().clone() });
    }
    if let Ok(inner) = ctx.getattr("ctx") {
        if let Ok(session) = inner.extract::<Bound<'_, PySessionContext>>() {
            return Ok(SessionContextContract { ctx: session.borrow().ctx().clone() });
        }
    }
    Err(PyValueError::new_err(
        "ctx must be datafusion.SessionContext or expose a compatible .ctx attribute",
    ))
}
```

**After:** delete `SessionContextContract` and `session_context_contract`. Callers that used `session_context_contract(ctx)?.ctx` become `extract_session_ctx(ctx)?`.

#### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — delete `SessionContextContract` struct, `session_context_contract` function, and `optional_session_context_contract` wrapper. Update all call sites (search for `session_context_contract(`) to call `extract_session_ctx` from `helpers.rs`.
- Add `extract_session_ctx` to the existing `use super::helpers::{ ... }` import block in `session_utils.rs`.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

**D8.** Delete `struct SessionContextContract`.
**D9.** Delete `fn session_context_contract`.
**D10.** Delete `fn optional_session_context_contract` (or re-implement trivially as `ctx.map(extract_session_ctx).transpose()`).

---

### S7. Consolidate duplicate decode_schema_ipc

**Priority:** MEDIUM (P7)
**Crate:** `rust/datafusion_python/`
**Effort:** small (2-line deletion + import update)

#### Goal

`decode_schema_ipc` is defined identically in two files. Delete the copy in `schema_evolution.rs` and import the one from `helpers.rs`.

#### Representative Code Snippets

**Current (both are identical):**

```rust
// rust/datafusion_python/src/codeanatomy_ext/helpers.rs:104
fn decode_schema_ipc(schema_ipc: &[u8]) -> PyResult<SchemaRef> {
    schema_from_ipc(schema_ipc)
        .map_err(|err| PyValueError::new_err(format!("Failed to decode schema IPC: {err}")))
}

// rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:38
fn decode_schema_ipc(schema_ipc: &[u8]) -> PyResult<SchemaRef> {
    schema_from_ipc(schema_ipc)
        .map_err(|err| PyValueError::new_err(format!("Failed to decode schema IPC: {err}")))
}
```

**After:**

Change `decode_schema_ipc` in `helpers.rs` from `fn` (private) to `pub(crate) fn`. Delete the copy in `schema_evolution.rs` and add it to the `use super::helpers::{ ... }` import block.

#### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs` — change `fn decode_schema_ipc` to `pub(crate) fn decode_schema_ipc`.
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs` — delete local `decode_schema_ipc`, add `decode_schema_ipc` to `use super::helpers::{ ... }`.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

**D11.** Delete `fn decode_schema_ipc` from `schema_evolution.rs`.

---

### S8. Fix IntervalAlignProvider bare SessionContext::new()

**Priority:** HIGH (P6)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** medium (requires API signature change)

#### Goal

`IntervalAlignProvider::build_dataframe` and `execute_interval_align` both create an unconfigured `SessionContext::new()` internally. This bypasses configured planner/runtime surfaces and violates the provider contract boundary.

Replace nested internal session construction with a provider-native plan assembly path:

1. Build a logical plan directly for interval alignment (no ad hoc internal SQL context).
2. Apply filter/projection/limit in `scan` against that logical plan.
3. Compile with the caller-provided `state.create_physical_plan(...)`.

This item also introduces pushdown-contract hardening: classify supported filters precisely (Exact/Inexact/Unsupported), and evaluate adopting `scan_with_args` once the direct plan path lands.

#### Representative Code Snippets

**Current (bypasses session adapter):**

```rust
// rust/codeanatomy_engine/src/providers/interval_align_provider.rs:102
async fn build_dataframe(&self) -> Result<datafusion::prelude::DataFrame> {
    let sql = build_interval_align_sql(...)?;
    let ctx = SessionContext::new();   // <-- bare nested context
    ...
}
```

**After (direct logical-plan path, caller state owns physical planning):**

```rust
// rust/codeanatomy_engine/src/providers/interval_align_provider.rs
fn build_logical_plan(&self) -> Result<LogicalPlan> {
    // Build interval-alignment plan without constructing a new SessionContext.
    // Use LogicalPlanBuilder / provider-native plan construction.
    let base = /* interval align plan root */;
    Ok(base)
}

async fn scan(
    &self,
    state: &dyn Session,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut plan = LogicalPlanBuilder::from(self.build_logical_plan()?);
    if let Some(filter_expr) = filters.iter().cloned().reduce(|acc, expr| acc.and(expr)) {
        plan = plan.filter(filter_expr)?;
    }
    if let Some(indices) = projection {
        let projected: Vec<Expr> = indices
            .iter()
            .map(|index| Expr::Column(Column::from(plan.schema().qualified_field(*index))))
            .collect();
        plan = plan.project(projected)?;
    }
    if let Some(row_limit) = limit {
        plan = plan.limit(0, Some(row_limit))?;
    }
    state.create_physical_plan(&plan.build()?).await
}
```

Also remove the second bare `SessionContext::new()` in `execute_interval_align` by routing execution through the same plan/provider path used by `scan`.

#### Files to Edit

- `rust/codeanatomy_engine/src/providers/interval_align_provider.rs` — replace nested-context `build_dataframe` with direct logical-plan builder, keep `scan` as the only physical-plan compilation point, update filter pushdown classification, fix `execute_interval_align`.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

Delete nested-context plan path:

- `IntervalAlignProvider::build_dataframe` (nested `SessionContext::new()` path) from `interval_align_provider.rs`.
- Any helper code that exists only to register `__ca_left` / `__ca_right` into an internal throwaway context.

#### Tests

Add tests in `rust/codeanatomy_engine/tests/interval_align_provider.rs`:

1. Regression test proving no nested session is created in scan path (caller session config governs plan).
2. Pushdown-classification test for supported/unsupported predicate families.
3. Projection/filter/limit behavior test validating provider contract in scan output.

---

### S9. Add always-on log crate observability

**Priority:** HIGH (P24)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** medium (many files, but each edit is small)

#### Goal

All OTEL instrumentation in `executor/` is gated behind `#[cfg(feature = "tracing")]`. In the default build, there is zero observable output at pipeline stage boundaries. Add `log::info!` calls (unconditional, using the `log` crate) at key boundaries in `execute_pipeline` and `execute_and_materialize_with_plans`.

The `log` crate should be added as an unconditional dependency to `codeanatomy_engine/Cargo.toml`. Log output is controlled by the environment (`RUST_LOG`) at zero cost when disabled.

#### Representative Code Snippets

**Add to Cargo.toml:**

```toml
# rust/codeanatomy_engine/Cargo.toml
[dependencies]
log = "0.4"
```

**In executor/pipeline.rs (execute_pipeline body):**

```rust
// Step 1: Compilation start
log::info!(
    "pipeline compilation start spec_hash={} view_count={} output_count={}",
    hex::encode(spec.spec_hash),
    spec.view_definitions.len(),
    spec.output_targets.len()
);

// After compilation completes:
log::info!(
    "pipeline compilation complete spec_hash={}",
    hex::encode(spec.spec_hash)
);

// Step 3: Per-output materialization start
for (target, df) in &output_plans {
    log::info!(
        "materialization start table_name={}",
        target.table_name
    );
}

// After materialization:
log::info!(
    "materialization complete table_name={} rows_written={}",
    result.table_name,
    result.rows_written
);

// Step 5: Maintenance start/complete
log::info!("Post-materialization maintenance start");
log::info!(
    "post-materialization maintenance complete report_count={}",
    maintenance_reports.len()
);
```

**In executor/runner.rs (execute_and_materialize_with_plans):**

```rust
log::info!(
    "write path selected table_name={} use_native_delta={}",
    target.table_name,
    use_native_delta_writer
);
log::info!(
    "write complete table_name={} rows={}",
    target.table_name,
    rows_written
);
```

**Warning emission (executor/warnings.rs or run site):**

```rust
// When a RunWarning is appended:
log::warn!(
    "run warning emitted code={} stage={} message={}",
    warning.code.as_str(),
    warning.stage.as_str(),
    warning.message
);
```

#### Files to Edit

- `rust/codeanatomy_engine/Cargo.toml` — add `log = "0.4"`.
- `rust/codeanatomy_engine/src/executor/pipeline.rs` — add `log::info!` at steps 1, 3, and 5.
- `rust/codeanatomy_engine/src/executor/runner.rs` — add `log::info!` at write-path selection and write completion.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

None. The existing `#[cfg(feature = "tracing")]` OTEL spans are preserved alongside the new log calls.

---

### S10. Add bridge layer tracing instrumentation

**Priority:** HIGH (P24)
**Crate:** `rust/datafusion_python/`
**Effort:** medium (additive only — each edit is a one-line attribute addition)

#### Goal

Only the four delta mutation bridge functions carry `#[instrument]`. Nine additional high-value entry points have no structured tracing spans, making production failures in UDF registration, plugin loading, and plan capture impossible to correlate from telemetry.

Add `#[instrument(level = "info", skip_all, fields(...))]` to each of the nine listed functions.

#### Files and Functions

| File | Function | Fields |
|---|---|---|
| `udf_registration.rs` | `register_codeanatomy_udfs` | `enable_async` |
| `udf_registration.rs` | `install_codeanatomy_runtime` | `enable_async_udfs` |
| `udf_registration.rs` | `capabilities_snapshot` | (none) |
| `plugin_bridge.rs` | `load_df_plugin` | `path` |
| `plugin_bridge.rs` | `register_df_plugin` | — |
| `plugin_bridge.rs` | `register_df_plugin_udfs` | — |
| `session_utils.rs` | `build_extraction_session` | — |
| `session_utils.rs` | `capture_plan_bundle_runtime` | — |
| `session_utils.rs` | `register_dataset_provider` | `table_name`, `table_uri` |
| `schema_evolution.rs` | `parquet_listing_table_provider` | `table_name`, `path` |

#### Representative Code Snippet

```rust
// Before (no tracing):
#[pyfunction]
pub(crate) fn register_codeanatomy_udfs(
    ctx: &Bound<'_, PyAny>,
    enable_async: bool,
) -> PyResult<()> { ... }

// After:
#[pyfunction]
#[instrument(level = "info", skip_all, fields(enable_async))]
pub(crate) fn register_codeanatomy_udfs(
    ctx: &Bound<'_, PyAny>,
    enable_async: bool,
) -> PyResult<()> { ... }
```

Ensure `use tracing::instrument;` is present at the top of each modified file. The `tracing` crate is already a workspace dependency.

#### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs` — add `#[instrument]` to 3 functions.
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs` — add `#[instrument]` to 3 functions.
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — add `#[instrument]` to 3 functions.
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs` — add `#[instrument]` to 1 function.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

None.

---

### S11. Make install_policy_rules and install_physical_rules idempotent

**Priority:** HIGH (P17)
**Crate:** `rust/datafusion_ext/`
**Effort:** small

#### Goal

`install_policy_rules` adds `CodeAnatomyPolicyRule` to the session's analyzer rules without checking for duplicates. Called twice on the same session, it registers the rule twice, causing double validation passes. The existing `ensure_policy_config` function uses a config-extension flag as the idempotency guard — apply the same pattern.

The physical rules file (`rust/datafusion_ext/src/physical_rules.rs`) has an analogous `install_physical_rules` function that must also be guarded.

#### Representative Code Snippets

**Current (not idempotent):**

```rust
// rust/datafusion_ext/src/planner_rules.rs:100
pub fn install_policy_rules(ctx: &SessionContext) -> Result<()> {
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    state.add_analyzer_rule(Arc::new(CodeAnatomyPolicyRule));
    Ok(())
}
```

**After (idempotent via config extension flag):**

```rust
/// Marker type recorded in `SessionConfig` extensions after rule installation.
/// Presence indicates `CodeAnatomyPolicyRule` has already been added; prevents
/// duplicate registration on repeated calls.
#[derive(Debug, Clone, Default)]
pub struct CodeAnatomyPolicyRuleInstalled;

impl ConfigExtension for CodeAnatomyPolicyRuleInstalled {
    const PREFIX: &'static str = "codeanatomy_policy.rule_installed";
}

pub fn install_policy_rules(ctx: &SessionContext) -> Result<()> {
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    let already_installed = state
        .config()
        .options()
        .extensions
        .get::<CodeAnatomyPolicyRuleInstalled>()
        .is_some();
    if already_installed {
        return Ok(());
    }
    state.add_analyzer_rule(Arc::new(CodeAnatomyPolicyRule));
    state
        .config_mut()
        .options_mut()
        .extensions
        .insert(CodeAnatomyPolicyRuleInstalled);
    Ok(())
}
```

Apply the same pattern to `install_physical_rules` in `physical_rules.rs` using a `CodeAnatomyPhysicalRuleInstalled` marker type.

#### Files to Edit

- `rust/datafusion_ext/src/planner_rules.rs` — add `CodeAnatomyPolicyRuleInstalled` marker struct + `ConfigExtension` impl, update `install_policy_rules`.
- `rust/datafusion_ext/src/physical_rules.rs` — add `CodeAnatomyPhysicalRuleInstalled` marker struct, update `install_physical_rules`.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

None.

#### Tests

Add tests to `rust/datafusion_ext/tests/registry_metadata_tests.rs`:

- Call `install_policy_rules(ctx)` twice; assert analyzer rule list length does not increase on second call.
- Call `install_physical_rules(ctx)` twice; same assertion.

---

### S12. Remove spec/hashing.rs::DeterminismContract

**Priority:** MEDIUM (P20 / YAGNI)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** small

#### Goal

`spec/hashing.rs:68-78` defines a two-field `DeterminismContract { spec_hash, envelope_hash }` with an `is_replay_valid` method. This is a strict subset of the authoritative three-field `executor/result.rs::DeterminismContract { spec_hash, envelope_hash, rulepack_fingerprint }`. The spec version is unused at the public API level. Remove it.

#### Representative Code Snippets

**To delete from spec/hashing.rs:**

```rust
/// Determinism contract for replay validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismContract {
    pub spec_hash: [u8; 32],
    pub envelope_hash: [u8; 32],
}

impl DeterminismContract {
    pub fn is_replay_valid(&self, other: &DeterminismContract) -> bool {
        self.spec_hash == other.spec_hash && self.envelope_hash == other.envelope_hash
    }
}
```

Before deleting, verify with `grep -r "spec::hashing::DeterminismContract"` across the workspace to ensure there are no external consumers.

#### Files to Edit

- `rust/codeanatomy_engine/src/spec/hashing.rs` — delete `DeterminismContract` struct and its `impl` block.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

**D12.** Delete `pub struct DeterminismContract` from `spec/hashing.rs` (lines 68-78).
**D13.** Delete the `impl DeterminismContract` block (lines 74-78).

---

### S13. Rename ensure_source_registered

**Priority:** LOW (P21)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** small (rename only)

#### Goal

`SemanticPlanCompiler::ensure_source_registered` at `plan_compiler.rs:373` is a command (it calls `ctx.register_table()`) but is named like a predicate or idempotent state assertion. Rename to `register_inline_source` to communicate the command nature. No behavior change.

The function is called 7 times within `compile_view`, all via `self.ensure_source_registered(source, inline_cache)`.

#### Files to Edit

- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs` — rename `ensure_source_registered` to `register_inline_source` in the definition and all 7 call sites within `compile_view`.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

None (name change only, not a deletion).

---

### S14. Implement DF52 RelationPlanner end-to-end (replace phantom capability)

**Priority:** HIGH (P20 / P10)
**Crates/Packages:** `rust/df_plugin_api/`, `rust/df_plugin_codeanatomy/`, `rust/datafusion_ext/`, `rust/datafusion_python/`, `src/datafusion_engine/`
**Effort:** medium

#### Goal

Replace the phantom `RELATION_PLANNER` capability with a real end-to-end relation-planner surface. Capability bits, plugin exports, Rust registration, and Python runtime installation must all be truthful and executable.

This is a hard cutover: after S14 lands, relation-planner support is either fully implemented and tested, or the capability is not advertised.

#### Representative Code Snippets

**Current (capability bit without executable export path):**

```rust
// rust/df_plugin_api/src/manifest.rs
pub const RELATION_PLANNER: u64 = 1 << 5;

// rust/df_plugin_codeanatomy/src/lib.rs
capabilities: caps::TABLE_PROVIDER
    | caps::SCALAR_UDF
    | caps::AGG_UDF
    | caps::WINDOW_UDF
    | caps::TABLE_FUNCTION
    | caps::RELATION_PLANNER,
```

**After (native relation planner registration + truthful capability/export wiring):**

```rust
// rust/datafusion_ext/src/lib.rs
use datafusion::logical_expr::planner::RelationPlanner;
use datafusion::prelude::SessionContext;

pub fn install_relation_planner_native(
    ctx: &SessionContext,
    planner: Arc<dyn RelationPlanner>,
) -> Result<()> {
    ctx.register_relation_planner(planner);
    Ok(())
}
```

```python
# src/datafusion_engine/session/runtime_extensions.py
if isinstance(planner_hook, PlannerExtensionPort):
    planner_hook.install_expr_planners(ctx)
    planner_hook.install_relation_planner(ctx)
elif planner_hook is None:
    install_expr_planners(ctx, planner_names=profile.policies.expr_planner_names)
    install_relation_planner(ctx)  # new native entrypoint-backed default
```

#### Files to Edit

- `rust/df_plugin_api/src/lib.rs` — add relation-planner export field(s) and compatibility notes for `DfPluginExportsV1`.
- `rust/df_plugin_api/src/manifest.rs` — keep `caps::RELATION_PLANNER` only when export surface is present and validated.
- `rust/df_plugin_codeanatomy/src/lib.rs` — wire relation-planner exports and keep capability truthful.
- `rust/datafusion_ext/src/lib.rs` — add native relation-planner install function.
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs` — add Python bridge entrypoint for relation-planner installation.
- `src/datafusion_engine/session/runtime_extensions.py` — invoke default/native relation-planner installation when enabled.
- `src/datafusion_engine/session/protocols.py` — keep `PlannerExtensionPort.install_relation_planner` as required runtime surface.
- `src/datafusion_engine/expr/relation_planner.py` — replace no-op default with executable implementation contract.
- `src/datafusion_engine/extensions/required_entrypoints.py` — include relation-planner install entrypoint in required runtime entrypoints.

#### New Files to Create

- `rust/datafusion_ext/src/relation_planner.rs` — relation-planner implementation(s) and registration utilities.
- `rust/datafusion_ext/tests/relation_planner_install_tests.rs` — install/plan execution tests.
- `tests/unit/datafusion_engine/session/test_relation_planner_runtime_extensions.py` — runtime-install and capability-parity tests.

#### Legacy Decommission/Delete Scope

**D14.** Delete no-op `CodeAnatomyRelationPlanner` fallback behavior in `src/datafusion_engine/expr/relation_planner.py` once native/default relation planner path is executable.

**D15.** Delete any relation-planner capability advertisement path that is not backed by plugin exports and host registration.

---

### S15. Expand ABI and runtime contract compatibility policy

**Priority:** HIGH (P22)
**Crates/Packages:** `rust/df_plugin_api/`, `rust/datafusion_python/`, `rust/codeanatomy_engine_py/`, `src/datafusion_engine/`
**Effort:** medium (documentation, constants, and runtime contract checks)

#### Goal

Define an explicit compatibility policy for plugin ABI and runtime install contracts, including planner-extension surfaces introduced by S14. Remove magic literals, version contract payloads, and enforce required entrypoint/version checks at runtime bootstrap.

#### Representative Code Snippets

```rust
// rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs
/// Version of install/runtime capability payload contract.
pub(crate) const RUNTIME_INSTALL_CONTRACT_VERSION: u32 = 4;

fn capabilities_snapshot(...) -> PyResult<Py<PyAny>> {
    ...
    payload.insert("runtime_install_contract_version", RUNTIME_INSTALL_CONTRACT_VERSION);
    ...
}
```

```python
# src/datafusion_engine/extensions/required_entrypoints.py
REQUIRED_RUNTIME_ENTRYPOINTS: tuple[str, ...] = (
    ...,
    "install_relation_planner",
)
```

```python
# src/datafusion_engine/session/runtime_extensions.py
if payload.get("runtime_install_contract_version") != EXPECTED_RUNTIME_INSTALL_CONTRACT_VERSION:
    raise RuntimeError("Incompatible runtime install contract version")
```

#### Files to Edit

- `rust/df_plugin_api/src/manifest.rs` — document capability compatibility rules for planner-extension fields.
- `rust/df_plugin_api/src/lib.rs` — document additive-only policy for `DfPluginExportsV1` evolution and S14 fields.
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs` — add `RUNTIME_INSTALL_CONTRACT_VERSION`, replace magic literals in capability/runtime payloads.
- `rust/datafusion_python/src/context.rs` — add `#[deprecated]` to `register_table_provider` with migration note.
- `src/datafusion_engine/extensions/required_entrypoints.py` — include new required planner-runtime entrypoints.
- `src/datafusion_engine/session/runtime_extensions.py` — enforce runtime install contract version and entrypoint parity checks.

#### New Files to Create

- `rust/df_plugin_api/CHANGELOG.md` — ABI version history and compatibility policy, including planner-extension compatibility notes.

#### Legacy Decommission/Delete Scope

**D16.** Delete magic runtime contract literals in `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs` and replace with named version constants.

**D17.** Delete undocumented/implicit ABI assumptions by codifying compatibility rules in `rust/df_plugin_api/CHANGELOG.md`.

---

### S16. Add Rust unit tests for pure binding functions

**Priority:** HIGH (P23)
**Crates:** `rust/codeanatomy_engine_py/`, `rust/datafusion_ext_py/`
**Effort:** medium

#### Goal

`codeanatomy_engine_py` has zero `#[cfg(test)]` blocks across all six source files. `datafusion_ext_py` has no tests at all. Add unit tests for the pure functions that require no IO and no external dependencies.

#### Target Functions and Test Cases

**`rust/codeanatomy_engine_py/src/compiler.rs` — `parse_and_validate_spec`:**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_spec_returns_ok() {
        let spec_json = r#"{
            "version": 4,
            "input_relations": [],
            "view_definitions": [],
            "join_graph": {},
            "output_targets": [],
            "rule_intents": [],
            "rulepack_profile": "Default"
        }"#;
        let result = parse_and_validate_spec(spec_json.to_string());
        assert!(result.is_ok(), "valid spec should parse: {:?}", result);
    }

    #[test]
    fn parse_spec_below_min_version_returns_err() {
        let spec_json = r#"{"version": 0, "input_relations": [], ...}"#;
        let result = parse_and_validate_spec(spec_json.to_string());
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_view_definitions_is_valid() {
        // Spec with no views is schema-valid (may fail at execution, not compilation)
        let result = parse_and_validate_spec(minimal_spec_json(4));
        assert!(result.is_ok());
    }
}
```

**`rust/codeanatomy_engine_py/src/session.rs` — `from_class_name`:**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_class_name_small_is_ok() {
        assert!(SessionFactory::from_class_name("Small".to_string()).is_ok());
    }

    #[test]
    fn from_class_name_medium_is_ok() {
        assert!(SessionFactory::from_class_name("Medium".to_string()).is_ok());
    }

    #[test]
    fn from_class_name_large_is_ok() {
        assert!(SessionFactory::from_class_name("Large".to_string()).is_ok());
    }

    #[test]
    fn from_class_name_unknown_returns_err() {
        let result = SessionFactory::from_class_name("XLarge".to_string());
        assert!(result.is_err());
    }
}
```

**`rust/datafusion_ext_py/src/lib.rs` — `collect_public_names`:**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_public_names_excludes_dunder() {
        let names = vec![
            "__module__".to_string(),
            "my_function".to_string(),
            "_private".to_string(),
            "another_func".to_string(),
        ];
        let public = collect_public_names(&names);
        // Must not include dunder names
        assert!(!public.contains(&"__module__".to_string()));
        // Must include regular public names
        assert!(public.contains(&"my_function".to_string()));
        assert!(public.contains(&"another_func".to_string()));
    }

    #[test]
    fn collect_public_names_is_sorted() {
        let names = vec!["z_func".to_string(), "a_func".to_string(), "m_func".to_string()];
        let public = collect_public_names(&names);
        let sorted: Vec<String> = {
            let mut v = public.clone();
            v.sort();
            v
        };
        assert_eq!(public, sorted, "collect_public_names must return sorted names");
    }
}
```

Note: `collect_public_names` may need to be extracted into a testable pure function if it is currently inlined inside the PyO3 module initializer.

#### Files to Edit

- `rust/codeanatomy_engine_py/src/compiler.rs` — add `#[cfg(test)] mod tests { ... }` block.
- `rust/codeanatomy_engine_py/src/session.rs` — add `#[cfg(test)] mod tests { ... }` block.
- `rust/datafusion_ext_py/src/lib.rs` — extract `collect_public_names` as a testable free function (if not already), add `#[cfg(test)] mod tests { ... }` block.

#### New Files to Create

None (tests are inline `#[cfg(test)]` blocks).

---

### S17. Introduce IntervalAlignContext::prepare()

**Priority:** LOW (P19 / KISS)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** small

#### Goal

Both `build_output_schema` and `build_interval_align_sql` in `interval_align_provider.rs` begin with the same four operations: call `schema_columns(left_schema)`, call `schema_columns(right_schema)`, check emptiness, call `validate_required_columns`, call `select_columns`, resolve right-side aliases. Extract this shared setup into `IntervalAlignContext::prepare(config, left_schema, right_schema) -> Result<Self>`.

#### Representative Code Snippet

```rust
/// Pre-computed column selections and alias maps shared between
/// `build_output_schema` and `build_interval_align_sql`.
struct IntervalAlignContext {
    left_keep: Vec<String>,
    right_keep: Vec<String>,
    right_aliases: Vec<(String, String)>,
    join_mode: String,
}

impl IntervalAlignContext {
    fn prepare(
        config: &IntervalAlignProviderConfig,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<Self> {
        let left_cols = schema_columns(left_schema);
        let right_cols = schema_columns(right_schema);
        if left_cols.is_empty() {
            return Err(DataFusionError::Plan("Left schema has no columns".to_string()));
        }
        validate_required_columns(config, &left_cols, &right_cols)?;
        let left_keep = select_columns(&left_cols, &config.select_left);
        let right_keep = select_columns(&right_cols, &config.select_right);
        let right_aliases = resolve_right_aliases(&right_keep, &config.right_suffix);
        Ok(Self {
            left_keep,
            right_keep,
            right_aliases,
            join_mode: config.how.clone(),
        })
    }
}
```

`build_output_schema` and `build_interval_align_sql` each become:

```rust
fn build_output_schema(config: &IntervalAlignProviderConfig, left: &Schema, right: &Schema) -> Result<SchemaRef> {
    let ctx = IntervalAlignContext::prepare(config, left, right)?;
    // ... use ctx.left_keep, ctx.right_keep, ctx.right_aliases ...
}

fn build_interval_align_sql(config: &IntervalAlignProviderConfig, left: &Schema, right: &Schema) -> Result<String> {
    let ctx = IntervalAlignContext::prepare(config, left, right)?;
    // ... use ctx fields ...
}
```

#### Files to Edit

- `rust/codeanatomy_engine/src/providers/interval_align_provider.rs` — add `IntervalAlignContext` struct with `prepare` constructor, refactor `build_output_schema` and `build_interval_align_sql` to use it.

#### New Files to Create

None.

---

### S18. Introduce SessionBuildParams wrapper

**Priority:** MEDIUM (P12)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** medium

#### Goal

`SessionFactory::build_session_state_internal` takes 9 parameters (`#[allow(clippy::too_many_arguments)]`). Group `profile_name`, `spec_hash`, `tracing_config`, and `build_warnings` into a `SessionBuildParams` struct to reduce the parameter count to 5 and enable removal of the `#[allow]` suppression.

Similarly, `compile_artifacts_phase` in `compile_phases.rs` has 8 parameters with the same suppression. Group `stats_quality`, `provider_identities`, and `planning_surface_hash` into `ArtifactPhaseContext`.

#### Representative Code Snippet

**SessionBuildParams:**

```rust
// rust/codeanatomy_engine/src/session/factory.rs (new type)
pub struct SessionBuildParams {
    pub profile_name: String,
    pub spec_hash: [u8; 32],
    pub tracing_config: Option<TracingConfig>,
    pub build_warnings: Vec<RunWarning>,
}
```

```rust
// Before:
fn build_session_state_internal(
    &self,
    profile_name: &str,
    memory_pool_bytes: u64,
    config: &SessionConfig,
    runtime: Arc<RuntimeEnv>,
    ruleset: &CpgRuleSet,
    spec_hash: [u8; 32],
    tracing_config: Option<&TracingConfig>,
    overrides: &SessionOverrides,
    build_warnings: &mut Vec<RunWarning>,
) -> Result<SessionState> { ... }

// After:
fn build_session_state_internal(
    &self,
    params: &SessionBuildParams,
    memory_pool_bytes: u64,
    config: &SessionConfig,
    runtime: Arc<RuntimeEnv>,
    ruleset: &CpgRuleSet,
    overrides: &SessionOverrides,
) -> Result<SessionState> { ... }
```

**ArtifactPhaseContext:**

```rust
// rust/codeanatomy_engine/src/compiler/compile_phases.rs (new type)
pub(crate) struct ArtifactPhaseContext {
    pub stats_quality: Option<StatsQuality>,
    pub provider_identities: Vec<ProviderIdentity>,
    pub planning_surface_hash: [u8; 32],
}
```

```rust
// compile_artifacts_phase signature before:
pub(crate) async fn compile_artifacts_phase(
    ctx: &SessionContext,
    spec: &SemanticExecutionSpec,
    ruleset: &CpgRuleSet,
    output_plans: &[(OutputTarget, DataFrame)],
    pushdown_probe_map: &BTreeMap<String, PushdownProbe>,
    stats_quality: Option<StatsQuality>,
    provider_identities: &[ProviderIdentity],
    planning_surface_hash: [u8; 32],
) -> ArtifactPhaseResult

// After:
pub(crate) async fn compile_artifacts_phase(
    ctx: &SessionContext,
    spec: &SemanticExecutionSpec,
    ruleset: &CpgRuleSet,
    output_plans: &[(OutputTarget, DataFrame)],
    pushdown_probe_map: &BTreeMap<String, PushdownProbe>,
    artifact_ctx: &ArtifactPhaseContext,
) -> ArtifactPhaseResult
```

#### Files to Edit

- `rust/codeanatomy_engine/src/session/factory.rs` — add `SessionBuildParams` struct, update `build_session_state_internal` signature and all callers within the file.
- `rust/codeanatomy_engine/src/compiler/compile_phases.rs` — add `ArtifactPhaseContext` struct, update `compile_artifacts_phase` signature. Remove `#[allow(clippy::too_many_arguments)]`.
- Any callers of `compile_artifacts_phase` in `pipeline.rs` or `runner.rs` — update to construct `ArtifactPhaseContext`.

#### New Files to Create

None.

---

### S19. Fix build_udf_bundle error propagation

**Priority:** HIGH (P24)
**Crate:** `rust/df_plugin_codeanatomy/`
**Effort:** small

#### Goal

`build_udf_bundle` in `udf_bundle.rs:114-126` swallows errors and returns an empty bundle. `exports()` which calls it has no way to distinguish an empty bundle from an error. Change the return type to propagate errors, and update `exports()` to handle the failure explicitly.

#### Representative Code Snippets

**Current (silent failure):**

```rust
// rust/df_plugin_codeanatomy/src/udf_bundle.rs:114
fn build_udf_bundle() -> DfUdfBundleV1 {
    match build_udf_bundle_with_options(PluginUdfOptions::default()) {
        Ok(bundle) => bundle,
        Err(err) => {
            tracing::error!(error = %err, "Failed to build UDF bundle");
            DfUdfBundleV1 { scalar: RVec::new(), aggregate: RVec::new(), window: RVec::new() }
        }
    }
}

pub(crate) fn exports() -> DfPluginExportsV1 {
    DfPluginExportsV1 {
        table_provider_names: ...,
        udf_bundle: build_udf_bundle(),  // silently empty on error
        ...
    }
}
```

**After (explicit error propagation):**

```rust
// Option A: propagate to exports() and panic with a meaningful message
fn build_udf_bundle() -> Result<DfUdfBundleV1, String> {
    build_udf_bundle_with_options(PluginUdfOptions::default())
}

pub(crate) fn exports() -> DfPluginExportsV1 {
    let udf_bundle = build_udf_bundle().unwrap_or_else(|err| {
        // A plugin that fails to produce UDFs must be visible at load time,
        // not silently return an empty bundle. Panic here is intentional:
        // the host should fail-fast rather than operate with missing UDFs.
        panic!("Fatal: failed to build CodeAnatomy UDF bundle: {err}")
    });
    DfPluginExportsV1 {
        table_provider_names: ...,
        udf_bundle,
        ...
    }
}
```

Alternatively (Option B): expose the error through the plugin ABI by returning `DfResult<DfPluginExportsV1>` from `exports()` — but this would be an ABI-breaking change. Option A (panic with message) is preferred because it makes failures visible at plugin load time without breaking the ABI.

#### Files to Edit

- `rust/df_plugin_codeanatomy/src/udf_bundle.rs` — change `fn build_udf_bundle() -> DfUdfBundleV1` to `fn build_udf_bundle() -> Result<DfUdfBundleV1, String>`.
- `rust/df_plugin_codeanatomy/src/udf_bundle.rs` or `lib.rs` — update `exports()` to call `build_udf_bundle().unwrap_or_else(|err| panic!(...))`.

#### New Files to Create

None.

#### Legacy Decommission/Delete Scope

None.

---

### S20. Split session_utils.rs and udf_registration.rs

**Priority:** MEDIUM (P3 / SRP)
**Crate:** `rust/datafusion_python/`
**Effort:** medium (structural reorganization)

#### Goal

`session_utils.rs` (794 lines, 8 concerns) and `udf_registration.rs` (572 lines, 6 concerns) accumulate too many responsibilities. Split into focused files per the concern breakdown from the reviews.

This is a pure file reorganization with no behavioral changes. The public PyO3 function signatures must remain identical; only internal module structure changes.

#### Target Module Structure

```
rust/datafusion_python/src/codeanatomy_ext/
├── delta_mutations.rs          (unchanged)
├── helpers.rs                  (unchanged)
├── schema_evolution.rs         (unchanged)
├── session_utils.rs            (REDUCED: session creation only)
├── substrait_bridge.rs         (NEW: Substrait replay, lineage extraction)
├── plan_bundle_bridge.rs       (NEW: plan bundle capture, artifact building)
├── delta_session_bridge.rs     (NEW: Delta session construction, codec install, factory)
├── rules_bridge.rs             (NEW: install planner/physical rules, policy config)
├── udf_registration.rs         (REDUCED: UDF install + snapshot only)
├── docs_bridge.rs              (NEW: udf_docs_snapshot, documentation utilities)
├── factory_policy.rs           (NEW: derive_function_factory_policy)
├── plugin_bridge.rs            (unchanged)
├── rust_pivot.rs               (unchanged)
└── mod.rs                      (updated to include new modules)
```

#### Function Migration Table

| Current file | Function | Target file |
|---|---|---|
| `session_utils.rs` | `replay_substrait_plan`, `lineage_from_substrait`, `extract_lineage_json` | `substrait_bridge.rs` |
| `session_utils.rs` | `capture_plan_bundle_runtime`, `build_plan_bundle_artifact_with_warnings` | `plan_bundle_bridge.rs` |
| `session_utils.rs` | `install_delta_table_factory`, `install_delta_plan_codecs`, `delta_session_context` | `delta_session_bridge.rs` |
| `session_utils.rs` | `TracingMarkerRule`, `install_tracing` | `rules_bridge.rs` |
| `session_utils.rs` | `arrow_stream_to_batches` | `helpers.rs` (or `plan_bundle_bridge.rs`) |
| `udf_registration.rs` | `install_planner_rules`, `install_physical_rules`, `install_expr_planners`, config installs | `rules_bridge.rs` |
| `udf_registration.rs` | `udf_docs_snapshot` | `docs_bridge.rs` |
| `udf_registration.rs` | `derive_function_factory_policy` | `factory_policy.rs` |

#### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — remove migrated functions.
- `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs` — remove migrated functions.
- `rust/datafusion_python/src/codeanatomy_ext/mod.rs` — add `pub(crate) mod substrait_bridge;` etc. for each new module; update `register_functions` and `register_internal_functions` to import from new homes.

#### New Files to Create

- `rust/datafusion_python/src/codeanatomy_ext/substrait_bridge.rs`
- `rust/datafusion_python/src/codeanatomy_ext/plan_bundle_bridge.rs`
- `rust/datafusion_python/src/codeanatomy_ext/delta_session_bridge.rs`
- `rust/datafusion_python/src/codeanatomy_ext/rules_bridge.rs`
- `rust/datafusion_python/src/codeanatomy_ext/docs_bridge.rs`
- `rust/datafusion_python/src/codeanatomy_ext/factory_policy.rs`
- `rust/datafusion_python/tests/substrait_bridge_tests.rs`
- `rust/datafusion_python/tests/plan_bundle_bridge_tests.rs`
- `rust/datafusion_python/tests/delta_session_bridge_tests.rs`
- `rust/datafusion_python/tests/rules_bridge_tests.rs`
- `rust/datafusion_python/tests/docs_bridge_tests.rs`
- `rust/datafusion_python/tests/factory_policy_tests.rs`

#### Legacy Decommission/Delete Scope

None — no deletions, only moves. `session_utils.rs` and `udf_registration.rs` remain but are reduced.

---

### S21. Add TypePlanner support to planning surfaces

**Priority:** HIGH (DF52 alignment)
**Crates/Packages:** `rust/codeanatomy_engine/`, `rust/datafusion_ext/`, `src/datafusion_engine/`
**Effort:** medium

#### Goal

Extend planning-surface construction to support DataFusion TypePlanner registration in the same first-class way as ExprPlanner and RelationPlanner. Ensure type-planner configuration is captured in planning manifests and fingerprints.

#### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/session/planning_surface.rs
use datafusion::logical_expr::planner::TypePlanner;

#[derive(Clone, Default)]
pub struct PlanningSurfaceSpec {
    ...
    pub type_planner: Option<Arc<dyn TypePlanner>>,
}

pub fn apply_to_builder(
    mut builder: SessionStateBuilder,
    spec: &PlanningSurfaceSpec,
) -> SessionStateBuilder {
    ...
    if let Some(planner) = spec.type_planner.clone() {
        builder = builder.with_type_planner(planner);
    }
    builder
}
```

```python
# src/datafusion_engine/session/runtime_profile_config.py
class PolicyBundleConfig(...):
    ...
    type_planner_hook: Callable[[SessionContext], None] | None = None
```

#### Files to Edit

- `rust/codeanatomy_engine/src/session/planning_surface.rs` — add TypePlanner field and builder application.
- `rust/codeanatomy_engine/src/session/planning_manifest.rs` — include type-planner identity/version in manifest payload.
- `rust/codeanatomy_engine/src/session/factory.rs` — wire type-planner from runtime/policy surface.
- `rust/datafusion_ext/src/lib.rs` — expose native type-planner install helper.
- `src/datafusion_engine/session/runtime_profile_config.py` — add type-planner policy hook/config fields.
- `src/datafusion_engine/session/runtime_extensions.py` — install type planner during runtime extension bootstrap.

#### New Files to Create

- `rust/datafusion_ext/src/type_planner.rs` — canonical TypePlanner implementation(s).
- `rust/datafusion_ext/tests/type_planner_install_tests.rs` — registration and planning tests.
- `tests/unit/datafusion_engine/session/test_type_planner_installation.py` — Python runtime install parity test.

#### Legacy Decommission/Delete Scope

None.

---

### S22. Migrate provider capsule creation to session-aware FFI contract

**Priority:** HIGH (DF52 FFI contract)
**Crates/Packages:** `rust/datafusion_python/`, `src/datafusion_engine/`
**Effort:** medium

#### Goal

Adopt session-aware TaskContextProvider/LogicalExtensionCodec wiring for all provider capsule paths and remove global fallback context usage. Runtime invocation should use one canonical context shape without multi-candidate probing in steady state.

#### Representative Code Snippets

```rust
// rust/datafusion_python/src/codeanatomy_ext/helpers.rs
pub(crate) fn provider_capsule_from_session(
    py: Python<'_>,
    session: &SessionContext,
    provider: Arc<dyn TableProvider>,
    logical_codec: Option<Arc<dyn LogicalExtensionCodec>>,
) -> PyResult<Py<PyAny>> {
    let ffi_provider = FFI_TableProvider::new(provider, true, None, session, logical_codec);
    ...
}
```

```python
# src/datafusion_engine/extensions/context_adaptation.py
invoke_entrypoint_with_adapted_context(
    ...,
    ExtensionEntrypointInvocation(
        ctx=ctx,
        internal_ctx=getattr(ctx, "ctx", None),
        allow_fallback=False,
    ),
)
```

#### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs` — replace global task-context provider path with session-aware provider capsule helper.
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs` — pass session/codec to provider capsule creation.
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs` — enforce session-aware provider capsule path.
- `src/datafusion_engine/extensions/context_adaptation.py` — remove fallback mode from normal runtime path.
- `src/datafusion_engine/extensions/datafusion_ext.py` — tighten context normalization to canonical session contract.
- `src/datafusion_engine/session/runtime_extensions.py` — enforce session-contract probe and fail-fast on mismatch.

#### New Files to Create

- `rust/datafusion_python/src/codeanatomy_ext/provider_capsule_contract.rs` — session/codec capsule construction helpers.
- `rust/datafusion_python/tests/provider_capsule_contract_tests.rs` — capsule contract tests.
- `tests/unit/datafusion_engine/extensions/test_context_adaptation_contract.py` — fallback removal regression tests.

#### Legacy Decommission/Delete Scope

**D18.** Delete `global_task_ctx_provider` from `rust/datafusion_python/src/codeanatomy_ext/helpers.rs` after all provider capsule callers are migrated.

**D19.** Delete fallback context candidate execution path in `src/datafusion_engine/extensions/context_adaptation.py` for required runtime entrypoints.

---

### S23. Unify typed planning policy compilation across `src` and `rust`

**Priority:** HIGH (cross-layer determinism)
**Crates/Packages:** `src/datafusion_engine/`, `rust/codeanatomy_engine/`
**Effort:** medium

#### Goal

Replace stringly planning-policy translation with a typed policy contract that compiles deterministically into Rust `PlanningSurfaceSpec` and Python runtime installation payloads.

#### Representative Code Snippets

```python
# src/datafusion_engine/session/planning_surface_policy.py
class PlanningSurfacePolicyV1(msgspec.Struct, frozen=True):
    enable_default_features: bool = True
    expr_planner_names: tuple[str, ...] = ()
    relation_planner_enabled: bool = False
    type_planner_enabled: bool = False
```

```rust
// rust/codeanatomy_engine/src/session/planning_surface.rs
impl PlanningSurfaceSpec {
    pub fn from_policy(policy: PlanningSurfacePolicyV1) -> Self {
        ...
    }
}
```

#### Files to Edit

- `src/datafusion_engine/session/runtime_config_policies.py` — route planning settings through typed policy object.
- `src/datafusion_engine/session/delta_session_builder.py` — stop duplicating planning-key parsing.
- `src/datafusion_engine/session/context_pool.py` — consume typed planning policy during SessionContext construction.
- `rust/codeanatomy_engine/src/session/planning_surface.rs` — add typed policy ingestion path.
- `rust/codeanatomy_engine/src/session/planning_manifest.rs` — include typed policy payload in manifest.

#### New Files to Create

- `src/datafusion_engine/session/planning_surface_policy.py` — typed policy contract and serialization helpers.
- `tests/unit/datafusion_engine/session/test_planning_surface_policy.py` — policy compilation tests.
- `rust/codeanatomy_engine/tests/planning_surface_policy_tests.rs` — policy-to-surface mapping tests.

#### Legacy Decommission/Delete Scope

**D20.** Delete duplicated planning setting translation branches in `src/datafusion_engine/session/delta_session_builder.py` once `PlanningSurfacePolicyV1` is the canonical source.

---

### S24. Pushdown-first provider architecture for interval alignment

**Priority:** HIGH (planning/perf correctness)
**Crate:** `rust/codeanatomy_engine/`
**Effort:** medium

#### Goal

Upgrade IntervalAlign provider contracts from blanket `Inexact` pushdown and internal SQL execution toward explicit predicate capability classification and direct logical-plan assembly.

#### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/providers/interval_align_provider.rs
fn supports_filters_pushdown(
    &self,
    filters: &[&Expr],
) -> Result<Vec<TableProviderFilterPushDown>> {
    Ok(filters
        .iter()
        .map(|expr| classify_interval_align_filter(expr))
        .collect())
}
```

#### Files to Edit

- `rust/codeanatomy_engine/src/providers/interval_align_provider.rs` — add filter classification and direct plan path.
- `rust/codeanatomy_engine/src/providers/pushdown_contract.rs` — encode provider contract checks for interval predicates.
- `rust/codeanatomy_engine/tests/provider_pushdown_contract.rs` — expand contract tests.

#### New Files to Create

- `rust/codeanatomy_engine/tests/interval_align_pushdown_tests.rs` — pushdown matrix tests.

#### Legacy Decommission/Delete Scope

**D21.** Delete blanket `TableProviderFilterPushDown::Inexact` return path in interval align provider once predicate classification lands.

---

### S25. Align Delta mutation bridge with DF52 DML contracts

**Priority:** HIGH (mutation contract correctness)
**Crates/Packages:** `rust/datafusion_python/`, `rust/datafusion_ext/`, `src/datafusion_engine/`
**Effort:** medium

#### Goal

Align delete/update mutation execution semantics with DataFusion TableProvider DML hooks and count-contract behavior. Keep bespoke payloads as transport-only envelopes, not semantic execution engines.

#### Representative Code Snippets

```rust
// DF52 TableProvider contract surface to align with
fn delete_from<'a>(
    &'a self,
    state: &'a dyn Session,
    filters: Vec<Expr>,
) -> Future<Result<Arc<dyn ExecutionPlan>>>;

fn update<'a>(
    &'a self,
    state: &'a dyn Session,
    assignments: Vec<(String, Expr)>,
    filters: Vec<Expr>,
) -> Future<Result<Arc<dyn ExecutionPlan>>>;
```

#### Files to Edit

- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs` — route execution contract toward provider DML semantics and explicit count behavior.
- `rust/datafusion_ext/src/delta_mutations.rs` — align mutation primitives to DML hook contracts.
- `src/datafusion_engine/delta/control_plane_provider.py` — keep request envelope mapping transport-only.
- `src/datafusion_engine/delta/service.py` — enforce mutation response contract parity.

#### New Files to Create

- `rust/datafusion_ext/tests/delta_dml_contract_tests.rs` — delete/update contract tests.
- `tests/unit/datafusion_engine/delta/test_dml_contract_alignment.py` — Python-level contract tests.

#### Legacy Decommission/Delete Scope

**D22.** Delete bespoke mutation branches that bypass provider DML contract hooks once aligned execution path is complete.

---

### S26. Consolidate cache control plane on CacheManagerConfig

**Priority:** MEDIUM-HIGH
**Crates/Packages:** `src/datafusion_engine/`, `rust/datafusion_python/`
**Effort:** medium

#### Goal

Converge cache policy and runtime settings onto typed CacheManagerConfig-style contracts, and standardize cache introspection payloads across runtime and observability surfaces.

#### Representative Code Snippets

```rust
use datafusion::execution::cache::cache_manager::{CacheManager, CacheManagerConfig};

let cfg = CacheManagerConfig::default()
    .with_list_files_cache_limit(128 * 1024 * 1024)
    .with_list_files_cache_ttl(Some(Duration::from_secs(120)));
let cache_manager = CacheManager::try_new(&cfg)?;
let runtime = RuntimeEnvBuilder::default().with_cache_manager(cache_manager);
```

#### Files to Edit

- `src/datafusion_engine/session/runtime_hooks.py` — centralize cache manager attachment from typed config.
- `src/datafusion_engine/session/runtime.py` — consume typed cache policy in runtime builder.
- `src/datafusion_engine/session/runtime_config_policies.py` — unify cache settings projection.
- `rust/datafusion_python/src/codeanatomy_ext/cache_tables.rs` — normalize introspection payload schema.
- `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` — remove duplicate cache metrics assembly where superseded.

#### New Files to Create

- `src/datafusion_engine/session/cache_manager_contract.py` — typed cache policy contract.
- `tests/unit/datafusion_engine/session/test_cache_manager_contract.py` — contract mapping tests.
- `rust/datafusion_python/tests/cache_manager_contract_tests.rs` — runtime/cache bridge tests.

#### Legacy Decommission/Delete Scope

**D23.** Delete duplicate cache-limit parsing paths in `runtime_config_policies.py` and `delta_session_builder.py` after typed cache contract adoption.

---

### S27. Introduce standalone logical optimizer harness

**Priority:** MEDIUM-HIGH
**Crate:** `rust/codeanatomy_engine/`
**Effort:** medium

#### Goal

Create a standalone optimizer harness for deterministic compile-only experiments and diagnostics, with rule-by-rule observer snapshots.

#### Representative Code Snippets

```rust
use datafusion::optimizer::{Optimizer, OptimizerContext};

let optimizer = Optimizer::with_rules(rules);
let config = OptimizerContext::new().with_max_passes(16);
let optimized = optimizer.optimize(plan, &config, |plan, rule| {
    tracing::debug!(rule = rule.name(), "optimizer step");
    let _ = plan.display_indent();
})?;
```

#### Files to Edit

- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs` — integrate standalone optimizer entrypoint.
- `rust/codeanatomy_engine/src/stability/optimizer_lab.rs` — route compile-only diagnostics through harness.
- `rust/codeanatomy_engine/src/compiler/compile_phases.rs` — consume unified optimizer trace output.

#### New Files to Create

- `rust/codeanatomy_engine/src/compiler/standalone_optimizer_harness.rs`
- `rust/codeanatomy_engine/tests/standalone_optimizer_harness.rs`

#### Legacy Decommission/Delete Scope

**D24.** Delete duplicated compile-only optimizer trace wrappers once standalone harness is canonical.

---

### S28. Harden plan artifact serialization policy

**Priority:** MEDIUM-HIGH
**Crates/Packages:** `src/datafusion_engine/`, `rust/codeanatomy_engine/`
**Effort:** medium

#### Goal

Make Substrait the default cross-process interchange format while retaining DataFusion proto for same-version internal replay only. Enforce this policy in artifact contracts and fingerprinting.

#### Representative Code Snippets

```python
# src/datafusion_engine/plan/contracts.py
class PlanArtifactPolicyV1(msgspec.Struct, frozen=True):
    cross_process_format: str = "substrait"
    allow_proto_internal: bool = True
```

```python
# src/datafusion_engine/plan/plan_fingerprint.py
if artifact_kind == "cross_process" and payload.format == "proto":
    raise ValueError("cross-process artifacts must use Substrait")
```

#### Files to Edit

- `src/datafusion_engine/plan/contracts.py` — add artifact serialization policy contract.
- `src/datafusion_engine/plan/artifact_serialization.py` — enforce policy routing for proto/substrait.
- `src/datafusion_engine/plan/plan_fingerprint.py` — prevent proto bytes from acting as cross-process canonical hash basis.
- `src/datafusion_engine/plan/substrait_artifacts.py` — make Substrait default for cross-process bundles.
- `rust/codeanatomy_engine/src/compiler/plan_codec.rs` — align codec capture flags to policy contract.

#### New Files to Create

- `tests/unit/datafusion_engine/plan/test_plan_artifact_policy.py` — serialization policy tests.
- `rust/codeanatomy_engine/tests/plan_codec_policy_tests.rs` — Rust codec policy tests.

#### Legacy Decommission/Delete Scope

**D25.** Delete proto-as-cross-process-fingerprint code paths once policy enforcement lands.

---

### S29. Establish planning-surface manifest parity across `src` and `rust`

**Priority:** HIGH (determinism and DRY)
**Crates/Packages:** `src/datafusion_engine/`, `rust/codeanatomy_engine/`
**Effort:** medium

#### Goal

Define one versioned planning-surface manifest that is generated consistently in Python and Rust and used as the canonical determinism/fingerprint boundary for planner/runtime configuration.

#### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/session/planning_manifest.rs
pub struct PlanningSurfaceManifestV2 {
    pub expr_planners: Vec<String>,
    pub relation_planners: Vec<String>,
    pub type_planners: Vec<String>,
    pub table_factories: Vec<String>,
    pub planning_config_keys: BTreeMap<String, String>,
}
```

```python
# src/datafusion_engine/plan/contracts.py
class PlanningSurfaceManifestV2(msgspec.Struct, frozen=True):
    expr_planners: tuple[str, ...]
    relation_planners: tuple[str, ...]
    type_planners: tuple[str, ...]
    table_factories: tuple[str, ...]
    planning_config_keys: Mapping[str, str]
```

#### Files to Edit

- `rust/codeanatomy_engine/src/session/planning_manifest.rs` — upgrade manifest schema and hashing inputs.
- `rust/codeanatomy_engine/src/session/capture.rs` — capture relation/type planner identities.
- `src/datafusion_engine/plan/contracts.py` — add matching manifest contract.
- `src/datafusion_engine/session/runtime_extensions.py` — publish manifest payload from runtime install.
- `src/datafusion_engine/plan/planning_env.py` — consume shared manifest contract in artifact assembly.

#### New Files to Create

- `tests/unit/datafusion_engine/plan/test_planning_surface_manifest_v2.py` — manifest parity tests.
- `rust/codeanatomy_engine/tests/planning_surface_manifest_v2.rs` — Rust manifest stability tests.

#### Legacy Decommission/Delete Scope

**D26.** Delete duplicated planning-fingerprint payload assembly logic once `PlanningSurfaceManifestV2` is canonical across both layers.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S2, S3, S4, S5, S6, S7, S12)

- Delete private `fn enforce_pushdown_contracts` copies from `rust/codeanatomy_engine/src/compiler/compile_phases.rs` and `rust/codeanatomy_engine/src/executor/pipeline.rs` because `pushdown_contract.rs` becomes canonical.
- Delete inline view/scan/output dependency extraction blocks from `compile_phases.rs` and `pipeline.rs` because `build_task_graph_from_spec` becomes canonical.
- Delete `SemanticPlanCompiler::topological_sort` from `rust/codeanatomy_engine/src/compiler/plan_compiler.rs` because `TaskGraph::topological_sort` becomes canonical.
- Delete `pub(crate) fn runtime()` and its import remnants from `rust/datafusion_python/src/codeanatomy_ext/helpers.rs` after all callers migrate to shared runtime.
- Delete `SessionContextContract`, `session_context_contract`, and `optional_session_context_contract` from `rust/datafusion_python/src/codeanatomy_ext/session_utils.rs` because `extract_session_ctx` becomes canonical.
- Delete local `decode_schema_ipc` copy from `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs` because helper export is canonical.
- Delete the 2-field determinism contract type + impl from `rust/codeanatomy_engine/src/spec/hashing.rs`.

### Batch D2 (after S14, S15, S21, S22, S23)

- Delete no-op relation planner fallback behavior from `src/datafusion_engine/expr/relation_planner.py` once native/default relation planner path is executable.
- Delete relation-planner capability advertisement paths that are not backed by plugin export + host registration in `rust/df_plugin_api/` and `rust/df_plugin_codeanatomy/`.
- Delete magic runtime install contract literals from `rust/datafusion_python/src/codeanatomy_ext/udf_registration.rs`.
- Delete `global_task_ctx_provider` from `rust/datafusion_python/src/codeanatomy_ext/helpers.rs` once provider capsule creation is session-aware.
- Delete fallback context candidate execution for required entrypoints in `src/datafusion_engine/extensions/context_adaptation.py`.
- Delete duplicated planning setting translation branches in `src/datafusion_engine/session/delta_session_builder.py` once typed planning policy is canonical.

### Batch D3 (after S24, S25, S26, S27, S28, S29)

- Delete blanket `TableProviderFilterPushDown::Inexact` return path from `rust/codeanatomy_engine/src/providers/interval_align_provider.rs`.
- Delete bespoke mutation branches that bypass provider DML hooks in `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs`.
- Delete duplicate cache-limit parsing paths in `src/datafusion_engine/session/runtime_config_policies.py` and `src/datafusion_engine/session/delta_session_builder.py`.
- Delete duplicated compile-only optimizer trace wrappers in `rust/codeanatomy_engine/src/compiler/compile_phases.rs` and `rust/codeanatomy_engine/src/stability/optimizer_lab.rs`.
- Delete proto-as-cross-process-fingerprint code paths in `src/datafusion_engine/plan/plan_fingerprint.py`.
- Delete duplicated planning-fingerprint payload assembly logic across `src/datafusion_engine/` and `rust/codeanatomy_engine/src/session/`.

---

## Implementation Sequence

Execute scope items in this order. Items within a phase can be parallelized across crates since they have no interdependencies.

### Phase 0 — Correctness and Contract Guards

| Order | Item | Rationale |
|---|---|---|
| 0.1 | **S1** — extra_constraints validation | Critical data-integrity fix; fail-fast behavior |
| 0.2 | **S11** — idempotent rule installation | Prevents repeated rule registration drift |
| 0.3 | **S12** — determinism contract dedup | Removes contract split before further planning work |
| 0.4 | **S19** — UDF bundle error propagation | Converts silent plugin degradation to explicit failure |
| 0.5 | **S9** — safe log syntax baseline | Avoids invalid macro-style guidance in follow-on edits |

### Phase 1 — Deduplication and Runtime Hygiene

| Order | Item | Rationale |
|---|---|---|
| 1.1 | **S3** — task graph extraction dedup | Prerequisite for S4 simplification |
| 1.2 | **S4** — topological sort unification | Deterministic ordering before wider refactors |
| 1.3 | **S2** — pushdown enforcement dedup | Single-source contract behavior |
| 1.4 | **S7** — decode_schema_ipc dedup | Removes copy-paste boundary logic |
| 1.5 | **S6** — session extraction dedup | Enables cleaner bridge-level contracts |
| 1.6 | **S5** — shared runtime adoption | Runtime lifecycle hygiene after helper dedup |
| 1.7 | **S20** — bridge module split | Improves SRP before cross-layer expansion |

### Phase 2 — Provider Architecture and Pushdown Contracts

| Order | Item | Rationale |
|---|---|---|
| 2.1 | **S17** — IntervalAlignContext::prepare | Structural prework in same file |
| 2.2 | **S8** — remove nested SessionContext path | Correct provider/session boundary |
| 2.3 | **S24** — pushdown-first provider behavior | Formalize Exact/Inexact/Unsupported semantics |
| 2.4 | **S13** — command naming cleanup | Low-risk API clarity after provider refactor |
| 2.5 | **S18** — parameter object reductions | Cleanup once core behavior settles |

### Phase 3 — Planner Extension Modernization

| Order | Item | Rationale |
|---|---|---|
| 3.1 | **S14** — relation planner end-to-end | Replaces phantom capability with real implementation |
| 3.2 | **S21** — type planner support | Completes DF52 planner hook parity |
| 3.3 | **S15** — ABI/runtime contract policy expansion | Version and contract guardrails after hook additions |
| 3.4 | **S10** — bridge tracing coverage | Instrument expanded planner/runtime surfaces |

### Phase 4 — Cross-Layer `src` + `rust` Convergence

| Order | Item | Rationale |
|---|---|---|
| 4.1 | **S22** — session-aware FFI provider contract | Removes global/fallback context anti-patterns |
| 4.2 | **S23** — typed planning policy compilation | Canonical policy mapping across layers |
| 4.3 | **S29** — manifest parity and fingerprint unification | Determinism contract closure |

### Phase 5 — System-Level Feature Alignment

| Order | Item | Rationale |
|---|---|---|
| 5.1 | **S25** — DML contract alignment | Aligns mutation execution semantics to DF52 |
| 5.2 | **S26** — cache control-plane consolidation | Aligns runtime/cache policy implementation |
| 5.3 | **S27** — standalone optimizer harness | Canonical compile-only optimization diagnostics |
| 5.4 | **S28** — serialization policy hardening | Substrait-first cross-process artifact policy |

### Phase 6 — Test and Documentation Completion

| Order | Item | Rationale |
|---|---|---|
| 6.1 | **S16** — Rust unit tests for pure bindings | Validates unchanged pure surfaces |
| 6.2 | Cross-layer regression suite | Confirms new planner/provider/runtime contracts |

---

## Implementation Checklist

### Scope Items

- [ ] S1 — Fix silent `extra_constraints` drop on delete/update.
- [ ] S2 — Deduplicate `enforce_pushdown_contracts`.
- [ ] S3 — Deduplicate task graph construction.
- [ ] S4 — Unify topological sort implementations.
- [ ] S5 — Replace per-call `Runtime::new()` with shared runtime.
- [ ] S6 — Consolidate duplicate session extraction.
- [ ] S7 — Consolidate duplicate `decode_schema_ipc`.
- [ ] S8 — Remove bare nested SessionContext use in IntervalAlign provider path.
- [ ] S9 — Add always-on log observability with safe `log` macro syntax.
- [ ] S10 — Add bridge layer tracing instrumentation.
- [ ] S11 — Make planner/physical rule installation idempotent.
- [ ] S12 — Remove duplicate determinism contract in `spec/hashing.rs`.
- [ ] S13 — Rename `ensure_source_registered` to command-style `register_inline_source`.
- [ ] S14 — Implement DF52 RelationPlanner end-to-end.
- [ ] S15 — Expand ABI and runtime contract compatibility policy.
- [ ] S16 — Add Rust unit tests for pure binding functions.
- [ ] S17 — Introduce `IntervalAlignContext::prepare()`.
- [ ] S18 — Introduce `SessionBuildParams` / `ArtifactPhaseContext` wrappers.
- [ ] S19 — Fix `build_udf_bundle` error propagation.
- [ ] S20 — Split `session_utils.rs` and `udf_registration.rs`.
- [ ] S21 — Add TypePlanner support to planning surfaces.
- [ ] S22 — Migrate provider capsule creation to session-aware FFI contract.
- [ ] S23 — Unify typed planning policy compilation across `src` and `rust`.
- [ ] S24 — Implement pushdown-first provider architecture for interval alignment.
- [ ] S25 — Align Delta mutation bridge with DF52 DML contracts.
- [ ] S26 — Consolidate cache control plane on CacheManagerConfig.
- [ ] S27 — Introduce standalone logical optimizer harness.
- [ ] S28 — Harden plan artifact serialization policy (Substrait-first cross-process).
- [ ] S29 — Establish planning-surface manifest parity across `src` and `rust`.

### Decommission Batches

- [ ] Batch D1 — land post-dedup/post-contract deletions (S2, S3, S4, S5, S6, S7, S12).
- [ ] Batch D2 — land planner/runtime contract cleanups (S14, S15, S21, S22, S23).
- [ ] Batch D3 — land provider/runtime/serialization cleanup deletions (S24, S25, S26, S27, S28, S29).

### Final Gate

- [ ] `cargo build --workspace` — clean build across all crates
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no new warnings (especially no `too_many_arguments` suppressions remaining after S18)
- [ ] `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q` for affected Python integration surfaces
- [ ] Verify no `#[pyfunction]` signatures were changed (FFI contract preservation)
- [ ] Verify no `extern "C"` function signatures were changed (plugin ABI preservation)
