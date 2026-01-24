# Hamilton Inference-Driven Excellence Plan

Date: January 24, 2026

Goals
- Make Hamilton a first-class projection of the inferred rustworkx task graph.
- Align Hamilton versioning, telemetry, and caching with inference-driven planning.
- Improve observability and materialization with explicit I/O nodes and lifecycle hooks.

----------------------------------------------------------------------
Scope 1: Generate Hamilton task nodes from the inferred plan catalog
----------------------------------------------------------------------
Status: Completed
Why
- A single monolithic task executor hides dependencies and reduces observability.
- Hamilton should surface task-level nodes to match the rustworkx inference graph.

Representative pattern
```python
from hamilton.function_modifiers import parameterize_sources

@parameterize_sources(
    task_alpha={"artifact": "plan_artifact__task_alpha"},
    task_beta={"artifact": "plan_artifact__task_beta"},
)
def task_execute(artifact: PlanArtifact, runtime: RuntimeArtifacts) -> TableLike:
    return execute_plan_artifact(TaskExecutionRequest(artifact=artifact, runtime=runtime))
```

Target files
- src/hamilton_pipeline/modules/task_execution.py
- src/hamilton_pipeline/modules/plan_catalog.py
- src/relspec/plan_catalog.py
- src/relspec/graph_inference.py
- src/hamilton_pipeline/driver_factory.py (if builder config enables node generation)

Implementation checklist
- [x] Add a compiler step to emit per-task Hamilton nodes based on PlanCatalog artifacts.
- [x] Ensure node names are stable and derived from task_name.
- [x] Preserve rustworkx dependency ordering by wiring dependencies to the generated nodes.
- [x] Remove compatibility mode to fully pivot to per-task nodes.
- [x] Add tests for generated node naming and dependency wiring.

Decommission candidates
- src/hamilton_pipeline/modules/task_execution.py: task_outputs (replaced by per-task nodes).

----------------------------------------------------------------------
Scope 2: Tie Hamilton DAG versioning and tags to inferred graph signatures
----------------------------------------------------------------------
Status: Completed
Why
- Hamilton UI uses driver build for versioning; we should key that to the inferred graph.
- Tags should expose plan fingerprints and task metadata for filtering.

Representative pattern
```python
from relspec.rustworkx_graph import task_graph_signature

signature = task_graph_signature(snapshot)
tracker = HamiltonTracker(
    project_id=..., username=..., dag_name=f"codeintel_{signature}",
    tags={"plan_signature": signature, "environment": env_name},
)
```

Target files
- src/hamilton_pipeline/driver_factory.py
- src/hamilton_pipeline/modules/task_catalog.py
- src/hamilton_pipeline/modules/plan_catalog.py
- src/hamilton_pipeline/modules/task_execution.py

Implementation checklist
- [x] Compute task graph signature in the Hamilton graph.
- [x] Use signature to set dag_name and tracker tags.
- [x] Add per-task tags: plan_fingerprint, output_name, priority.
- [x] Ensure tags are attached to generated nodes and outputs.

Decommission candidates
- src/hamilton_pipeline/modules/inputs.py: hamilton_tags (replace with computed tag node).

----------------------------------------------------------------------
Scope 3: Materialization-first I/O with DataLoaders and DataSavers
----------------------------------------------------------------------
Status: Completed
Why
- Hamilton materializers improve lineage, caching, and UI artifacts.
- We should avoid stub outputs and replace them with real save nodes.

Representative pattern
```python
from hamilton.function_modifiers import datasaver
from hamilton.io import utils as io_utils

@datasaver()
def write_cpg_nodes_delta(cpg_nodes: TableLike, output_dir: str) -> dict[str, object]:
    path = Path(output_dir) / "cpg_nodes"
    save_table(cpg_nodes, path)
    return io_utils.get_file_metadata(str(path))
```

Target files
- src/hamilton_pipeline/modules/outputs.py
- src/hamilton_pipeline/modules/inputs.py
- src/hamilton_pipeline/execution.py
- src/storage/* (materializer helpers)

Implementation checklist
- [x] Replace stub write_* nodes with datasaver/materializer nodes.
- [x] Add dataloader nodes for repo scan inputs and SCIP index sources.
- [x] Move materialization wiring to Builder.with_materializers for environment-specific sinks.
- [x] Ensure materializers produce metadata for UI catalog.

Decommission candidates
- src/hamilton_pipeline/modules/outputs.py: _finalize_stub, _table_stub, write_*_delta stubs.

----------------------------------------------------------------------
Scope 4: Opt-in caching with explicit formats and dependency boundaries
----------------------------------------------------------------------
Status: Completed
Why
- The inferred graph is deterministic; caching can speed repeat runs when scoped.
- We should avoid accidental cache hits by marking non-deterministic nodes.

Representative pattern
```python
from hamilton.function_modifiers import cache

@cache(format="parquet", behavior="default")
def cpg_nodes(cpg_nodes_final: TableLike) -> TableLike:
    return cpg_nodes_final
```

Target files
- src/hamilton_pipeline/driver_factory.py
- src/hamilton_pipeline/modules/outputs.py
- src/hamilton_pipeline/modules/inputs.py

Implementation checklist
- [x] Enable opt-in caching via Builder.with_cache(default_behavior="disable").
- [x] Mark deterministic, expensive nodes with @cache(format=...).
- [x] Mark connection/config nodes as IGNORE to avoid cache key churn.
- [x] Add tests that verify cache behaviors for key nodes.

Decommission candidates
- None.

----------------------------------------------------------------------
Scope 5: Data validation + schema metadata for core outputs
----------------------------------------------------------------------
Status: Completed
Why
- Validation captures data quality in the Hamilton UI.
- Schema metadata is essential for cataloging and change detection.

Representative pattern
```python
from hamilton.function_modifiers import check_output, schema

@schema.output(("id", "string"), ("kind", "string"))
@check_output(allow_nans=False, importance="fail")
def cpg_nodes(cpg_nodes_final: TableLike) -> TableLike:
    return cpg_nodes_final
```

Target files
- src/hamilton_pipeline/modules/outputs.py
- src/cpg/schemas.py
- src/schema_spec/*

Implementation checklist
- [x] Add @schema.output and @check_output to critical tables (nodes/edges/props).
- [x] Add custom validators for row counts, empty outputs, and schema drift.
- [x] Tag validation nodes to keep UI readable.

Decommission candidates
- None.

----------------------------------------------------------------------
Scope 6: Lifecycle hooks for telemetry and policy enforcement
----------------------------------------------------------------------
Status: Completed
Why
- Lifecycle hooks provide node-level timings and failure reporting.
- Integrates with existing obs stack without changing business logic.

Representative pattern
```python
from hamilton.lifecycle import base as lifecycle

hooks = [
    lifecycle.FunctionInputOutputTypeChecker(),
    CustomObsHook(collector=DiagnosticsCollector()),
]

builder = driver.Builder().with_modules(*modules).with_adapters(*hooks)
```

Target files
- src/hamilton_pipeline/driver_factory.py
- src/obs/*
- src/hamilton_pipeline/modules/inputs.py

Implementation checklist
- [x] Add lifecycle hooks for timing, errors, and custom diagnostics emission.
- [x] Wire hook configuration via pipeline config.
- [x] Ensure hooks are compatible with dynamic execution.

Decommission candidates
- None.

----------------------------------------------------------------------
Scope 7: Execution alignment with rustworkx generations
----------------------------------------------------------------------
Status: Completed
Why
- We need parallel execution that mirrors rustworkx-derived generations.
- Hamilton dynamic execution offers Parallelizable/Collect and grouping hooks.

Representative pattern
```python
from hamilton.htypes import Parallelizable, Collect

def task_wave(task_names: list[str]) -> Parallelizable[str]:
    for name in task_names:
        yield name

def execute_task(task_wave: str, runtime: RuntimeArtifacts) -> TableLike:
    ...

def gather_results(execute_task: Collect[TableLike]) -> list[TableLike]:
    return list(execute_task)
```

Target files
- src/hamilton_pipeline/driver_factory.py
- src/hamilton_pipeline/modules/task_execution.py
- src/relspec/rustworkx_schedule.py

Implementation checklist
- [x] Expose rustworkx generations as a Hamilton node.
- [x] Use Parallelizable/Collect to execute per-generation tasks.
- [x] Apply generation-aware grouping via GroupNodesByLevel and task generation tags.

Decommission candidates
- None.

----------------------------------------------------------------------
Scope 8: Pipe family for explicit multi-step transformations
----------------------------------------------------------------------
Status: In progress
Why
- Pipe decorators make intermediate transforms visible and cachable.
- Improves lineage clarity in Hamilton UI.

Representative pattern
```python
from hamilton.function_modifiers import pipe_input

@pipe_input(clean_step, enrich_step, on_input="raw_df")
def processed_df(raw_df: TableLike) -> TableLike:
    return raw_df
```

Target files
- src/normalize/*
- src/extract/*
- src/cpg/*

Implementation checklist
- [ ] Identify multi-step transforms that should be split into pipe stages.
- [ ] Convert inline transformations into pipe_input/pipe_output chains across normalize/extract/cpg (outputs and task finalizers use pipe_input).
- [ ] Tag intermediate nodes for readability across added stages.

Decommission candidates
- Inline multi-step transform helpers that become explicit pipe stages.

----------------------------------------------------------------------
Decommission and delete list (post-implementation)
----------------------------------------------------------------------
- [x] src/hamilton_pipeline/modules/task_execution.py: task_outputs (replaced by per-task nodes).
- [x] src/hamilton_pipeline/modules/outputs.py: _finalize_stub, _table_stub, write_*_delta stubs.
- [x] src/hamilton_pipeline/modules/inputs.py: hamilton_tags (replace with computed signature tags).
- [ ] Any legacy inline transform helpers replaced by pipe stages (case-by-case).
