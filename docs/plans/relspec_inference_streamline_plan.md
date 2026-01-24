# Relspec Inference Streamline Plan

> Goal: fully pivot relspec to an inference-first task/plan pipeline, remove legacy rule-era modules, and consolidate plan identity, execution, and schema validation on top of DataFusion, Ibis, SQLGlot, and Delta capabilities.

---

## 0) Target Architecture Snapshot

### Core Flow (updated)
```python
# plan_catalog.py
checkpoint = compile_checkpoint(
    plan.expr,
    backend=compiler_backend,
    schema_map=schema_map,
    dialect="datafusion",
)

artifact = PlanArtifact(
    task=task,
    plan=plan,
    sqlglot_ast=checkpoint.normalized,
    deps=infer_deps_from_sqlglot_expr(
        checkpoint.normalized,
        task_name=task.name,
        output=task.output,
    ),
    plan_fingerprint=checkpoint.plan_hash,
)
```

### Execution (single path)
```python
# relspec/execution.py
runtime = RuntimeArtifacts(execution=ibis_execution)
result = execute_plan_artifact(TaskExecutionRequest(artifact=artifact, runtime=runtime))
```

### Evidence/Schema (DataFusion-first)
```python
# relspec/evidence.py
schema = schema_registry.schema_for(source)
metadata = table_provider_metadata(session_id, table_name=source)
```

---

## Scope 1 - Decommission Legacy and Unused Modules

### Why
Remove stale rule-era helpers and unreferenced components that duplicate engine/runtime capabilities.

### Status
**Completed**

### Representative Code Pattern
```python
# relspec/__init__.py
_EXPORT_MAP = {
    # remove entries for deleted modules and re-export only active APIs
}
```

### Target Files
- Remove: `src/relspec/execution_bundle.py`
- Remove: `src/relspec/execution_lanes.py`
- Remove: `src/relspec/contract_catalog.py`
- Remove: `src/relspec/capabilities.py`
- Remove: `src/relspec/schema_context.py`
- Remove: `src/relspec/list_filter_gate.py`
- Remove: `src/relspec/task_registry.py`
- Remove: `src/relspec/config.py`
- Remove or replace: `src/relspec/param_deps.py` (see Scope 7)
- Update: `src/relspec/__init__.py` (exports)
- Update: `src/relspec/pipeline_policy.py` (trim to diagnostics only or move to engine)

### Implementation Checklist
- [x] Delete the unused modules listed above.
- [x] Remove all exports of deleted symbols from `src/relspec/__init__.py`.
- [x] Remove unused imports in Hamilton modules that referenced deleted config/policy types.
- [x] Ensure no runtime or test modules import deleted files.

---

## Scope 2 - Simplify InferredDeps (inference only)

### Why
Declared input comparisons are legacy. Inference-first means dependencies are derived solely from the plan AST.

### Status
**Completed**

### Representative Code Pattern
```python
# relspec/inferred_deps.py
@dataclass(frozen=True)
class InferredDeps:
    task_name: str
    output: str
    inputs: tuple[str, ...]
    required_columns: Mapping[str, tuple[str, ...]]
    required_types: Mapping[str, tuple[tuple[str, str], ...]]
    required_metadata: Mapping[str, tuple[tuple[bytes, bytes], ...]]
    plan_fingerprint: str
```

### Target Files
- Update: `src/relspec/inferred_deps.py`
- Update: `src/relspec/plan_catalog.py`
- Update: `src/relspec/rustworkx_graph.py`

### Implementation Checklist
- [x] Remove `declared_inputs`, `inputs_match`, and comparison helper APIs.
- [x] Remove `compare_deps`, `summarize_inferred_deps`, and `log_inferred_deps_comparison`.
- [x] Ensure all call sites use inference-only fields.
- [x] Update any diagnostics to log inferred dependencies directly.

---

## Scope 3 - Unify Plan Identity and Fingerprints

### Why
Plan identity is duplicated in relspec. Consolidate on SQLGlot compiler checkpoints for canonical fingerprints and policy hashes.

### Status
**Completed**

### Representative Code Pattern
```python
# relspec/plan_catalog.py
checkpoint = compile_checkpoint(
    plan.expr,
    backend=compiler_backend,
    schema_map=schema_map,
)
return PlanArtifact(
    task=task,
    plan=plan,
    sqlglot_ast=checkpoint.normalized,
    deps=infer_deps_from_sqlglot_expr(
        checkpoint.normalized,
        task_name=task.name,
        output=task.output,
    ),
    plan_fingerprint=checkpoint.plan_hash,
)
```

### Target Files
- Update: `src/relspec/plan_catalog.py`
- Update: `src/relspec/inferred_deps.py` (SQLGlot-only inference path)
- Remove: `src/relspec/execution_bundle.py`
- Consider: `src/ibis_engine/compiler_checkpoint.py` (source of truth)

### Implementation Checklist
- [x] Replace ad-hoc `plan_fingerprint` usage with `compile_checkpoint` output.
- [x] Use checkpoint.normalized AST for lineage inference.
- [x] Store policy/schema hash in artifact metadata if needed for caching.
- [x] Remove `ExecutionBundle` and related signature helpers.

---

## Scope 4 - Align Task Identity in Outputs

### Why
`task_name` in relation output data should match `TaskSpec.name` everywhere to maintain consistent identity across diagnostics and downstream tables.

### Status
**Completed**

### Representative Code Pattern
```python
# relationship_plans.py
output = output.mutate(
    task_name=ibis.literal(task_name),
    task_priority=ibis.literal(task_priority),
)
```

### Target Files
- Update: `src/relspec/relationship_plans.py`
- Update: `src/relspec/relationship_task_catalog.py`
- Update: `src/cpg/emit_edges_ibis.py` (ensure literal matches task name)

### Implementation Checklist
- [x] Pass task identity into plan builders (via TaskBuildContext or builder closure).
- [x] Remove hard-coded `infer.*` task names in relation output tables.
- [x] Ensure `task_priority` is derived from TaskSpec metadata.

---

## Scope 5 - Consolidate Execution Path

### Why
Execution logic should be unified through `ibis_engine` and `datafusion_engine`, not duplicated in relspec.

### Status
**Completed** — lane helpers removed; diagnostics (metrics/traces/explain) now flow from runtime policy.

### Representative Code Pattern
```python
# relspec/execution.py
return materialize_ibis_plan(
    request.artifact.plan,
    execution=request.runtime.execution,
)
```

### Target Files
- Remove: `src/relspec/execution_lanes.py`
- Update: `src/relspec/execution.py`
- Update: `src/ibis_engine/runner.py` or `src/datafusion_engine/bridge.py` (single execution path)

### Implementation Checklist
- [x] Remove relspec lane selection helpers.
- [x] Route execution through the engine-native runner (already via `materialize_ibis_plan`).
- [x] Centralize diagnostics (explain, substrait capture) in DataFusion runtime profile.

---

## Scope 6 - Evidence and Schema Validation via DataFusion

### Why
Evidence catalogs should be derived from the same schema and provider metadata used by the engine.

### Status
**Completed** — evidence now hydrates from schema registry and provider metadata.

### Representative Code Pattern
```python
# relspec/evidence.py
schema = schema_for(name)
provider = table_provider_metadata(session_id, table_name=name)
```

### Target Files
- Update: `src/relspec/evidence.py`
- Update: `src/relspec/graph_edge_validation.py`
- Update: `src/relspec/rustworkx_schedule.py`
- Reference: `src/datafusion_engine/schema_registry.py`, `src/datafusion_engine/table_provider_metadata.py`

### Implementation Checklist
- [x] Use DataFusion schema registry for evidence schemas.
- [x] Populate evidence metadata from provider metadata when available.
- [x] Validate column requirements with authoritative schema metadata (via evidence catalog).

---

## Scope 7 - Param Table Dependencies (Relocation + Inference)

### Why
Param dependencies are currently typed but never produced. Replace `relspec.param_deps` with inference integrated into the plan compilation layer.

### Status
**Completed** — inference moved to Hamilton params module with types relocated.

### Representative Code Pattern
```python
# new location (ibis_engine or hamilton_pipeline)
refs = referenced_tables(checkpoint.normalized)
param_tables = tuple(
    ref.name for ref in refs if ref.name.startswith(policy.prefix)
)
report = TaskDependencyReport(task_name=task.name, param_tables=param_tables)
```

### Target Files
- Remove: `src/relspec/param_deps.py`
- Update: `src/hamilton_pipeline/modules/params.py`
- Update: `src/hamilton_pipeline/pipeline_types.py` (relocated `TaskDependencyReport`, `ActiveParamSet`)

### Implementation Checklist
- [x] Compute param dependencies from plan inputs during Hamilton param wiring.
- [x] Feed `ActiveParamSet` from plan-derived reports.
- [x] Remove relspec module and re-home data structures.

---

## Scope 8 - Policy Simplification

### Why
Most of `PipelinePolicy` is unused. Keep only diagnostics settings in engine runtime.

### Status
**Completed**

### Representative Code Pattern
```python
# engine/runtime.py
if diagnostics_policy is not None:
    profile = _apply_diagnostics_policy(profile, diagnostics_policy)
```

### Target Files
- Update: `src/relspec/pipeline_policy.py` (trim or delete)
- Update: `src/engine/runtime.py`
- Update: `src/engine/session_factory.py`
- Update: `src/hamilton_pipeline/modules/inputs.py` (remove relspec config usage)

### Implementation Checklist
- [x] Keep minimal relspec policy (diagnostics + param table policy).
- [x] Remove `PolicyRegistry`, `KernelLanePolicy`, and list-filter policy.
- [x] Remove `RelspecConfig` and related config plumbing.

---

## Scope 9 - Incremental Planning Improvements

### Why
Current diff logic only checks plan hash; integrate semantic diffs and canonical fingerprints.

### Status
**Completed**

### Representative Code Pattern
```python
# relspec/incremental.py
prev_hashes = plan_fingerprint_map(prev)
curr_hashes = plan_fingerprint_map(curr)
return diff_plan_fingerprints(prev_hashes, curr_hashes)
```

### Target Files
- Update: `src/relspec/incremental.py`
- Consider: `src/ibis_engine/plan_diff.py`

### Implementation Checklist
- [x] Ensure plan hashes come from compiler checkpoints in incremental diff paths.
- [x] Expose semantic diff metadata for diagnostics.

---

## Scope 10 - Docs and API Surface Cleanup

### Why
Public documentation and exports should match the inference-first surface area.

### Status
**Completed**

### Representative Code Pattern
```python
# relspec/__init__.py
__all__ = (
    "PlanCatalog",
    "PlanArtifact",
    "TaskCatalog",
    "TaskSpec",
    "TaskGraph",
    "schedule_tasks",
)
```

### Target Files
- Update: `src/relspec/__init__.py`
- Update: `docs/architecture/GRAPH_RELATIONSHIP_COMPILATION_GUIDE.md`
- Add: `docs/plans/relspec_inference_streamline_plan.md`

### Implementation Checklist
- [x] Remove exported symbols for deleted modules.
- [x] Update architecture docs to reflect inference-only dependency model.
- [x] Document the compiler checkpoint based fingerprinting pipeline.

---

## Decommission and Delete List (Summary)

### Files to Remove
- `src/relspec/execution_bundle.py`
- `src/relspec/execution_lanes.py`
- `src/relspec/contract_catalog.py`
- `src/relspec/capabilities.py`
- `src/relspec/schema_context.py`
- `src/relspec/list_filter_gate.py`
- `src/relspec/task_registry.py`
- `src/relspec/config.py`
- `src/relspec/param_deps.py` (after relocation)
- `src/relspec/policies/__init__.py`
- `src/relspec/policies/model.py`
- `src/relspec/policies/registry.py`

### Functions/Types to Remove or Relocate
- `ExecutionBundle`, `execution_bundle_signature`, `sqlglot_plan_signature`
- `DataFusionLaneOptions`, `DataFusionLaneInputs`, `ExecutionLaneRecord`, `execute_plan_datafusion`
- `ContractCatalog.from_spec`, `ContractCatalog.contract`
- `runtime_capabilities_for_profile`
- `RelspecSchemaContext` and helpers
- `ListFilterGatePolicy`, `validate_no_inline_inlists`
- `TaskRegistry`
- `InferredDepsComparison`, `compare_deps`, `summarize_inferred_deps`, `log_inferred_deps_comparison`
- `InferredDepsConfig`, `get_inferred_deps_config`
- `ParamDep` (removed)
- `TaskDependencyReport`, `ActiveParamSet` (relocated to `src/hamilton_pipeline/pipeline_types.py`)

---

## Execution Order (Recommended)
1) Scope 1 (decommission) + Scope 10 (exports/docs) to clear the surface area.
2) Scope 2 (inference-only deps) + Scope 3 (compiler checkpoints).
3) Scope 4 (task identity alignment) + Scope 6 (evidence/schema integration).
4) Scope 5 (execution consolidation).
5) Scope 7 (param dependency relocation) + Scope 8 (policy simplification).
6) Scope 9 (incremental improvements).

---

## Completion Criteria
- No remaining references to deleted rule-era modules or configs.
- Plan identity and fingerprints come from compiler checkpoints.
- `task_name` and `task_priority` are uniform across plans and outputs.
- Evidence validation uses DataFusion schema and provider metadata.
- Execution path is unified through DataFusion/Ibis runtime surfaces.
