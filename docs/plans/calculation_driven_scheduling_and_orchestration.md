# Calculation-Driven Scheduling and Orchestration

This repo now uses an inference-first scheduling model. Dependency graphs are built from
compiled Ibis/SQLGlot plans (not from declared rule inputs). The scheduler operates on a
bipartite evidence <-> task graph, and validates readiness using column-level edge
requirements derived from SQLGlot lineage.

## Current Architecture

1. **Task Catalog**
   - Define tasks with `TaskSpec` builders that return Ibis plans.
2. **Plan Catalog**
   - Compile each task to SQLGlot and extract lineage + plan fingerprints.
3. **Task Graph**
   - Build rustworkx graphs from inferred dependencies only.
4. **Scheduling**
   - Use edge requirements to determine readiness from available evidence.
5. **Execution**
   - Materialize plans and register outputs in the runtime evidence catalog.

## Representative Code Pattern

```python
from relspec.plan_catalog import compile_task_catalog
from relspec.graph_inference import task_graph_from_catalog
from relspec.rustworkx_schedule import schedule_rules
from relspec.evidence import EvidenceCatalog

plan_catalog = compile_task_catalog(task_catalog, backend=backend, ctx=ctx)
task_graph = task_graph_from_catalog(plan_catalog)
evidence = EvidenceCatalog.from_sources(source_tables)
schedule = schedule_rules(task_graph.graph, evidence=evidence)
```

## Inference Pipeline Summary

- Compile Ibis plans to SQLGlot (`ibis_to_sqlglot`).
- Extract referenced tables and required columns.
- Build a bipartite rustworkx graph with edges annotated by required columns.
- Schedule tasks only when their upstream evidence requirements are satisfied.

## Key Modules

- `src/relspec/task_catalog.py` - TaskSpec and TaskCatalog definitions
- `src/relspec/plan_catalog.py` - Plan compilation + fingerprints
- `src/relspec/inferred_deps.py` - SQLGlot lineage inference
- `src/relspec/graph_inference.py` - TaskGraph construction
- `src/relspec/graph_edge_validation.py` - Edge requirement validation
- `src/relspec/rustworkx_schedule.py` - Deterministic scheduling
- `src/relspec/execution.py` - Plan artifact execution helpers

## Scheduler Invariant

- **Readiness is edge-based**: a task is ready when all predecessor evidence
  nodes exist and their required columns are present in the evidence catalog.

## Hamilton Output (Optional)

Hamilton DAG generation remains optional and can be driven from the inferred
TaskGraph when enabled via `HAMILTON_DAG_OUTPUT`.
