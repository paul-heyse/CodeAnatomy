# CPG Migration Notes

This document summarizes the design-phase migration to the unified CPG build
architecture introduced in `cpg_dedup_acero_plan.md`.

## Key changes

- **Plan-first pipeline:** node/edge/prop builders emit Acero-backed plans and
  materialize only at the finalize boundary.
- **Unified family specs:** node/prop specs are generated from a single
  `EntityFamilySpec` registry to prevent drift.
- **Plan catalog:** CPG builders assemble inputs via `PlanCatalog` and `PlanRef`,
  including derived plan sources (file nodes, symbol nodes, qname fallback).
- **Vectorized emission:** nodes, edges, and props are projected with plan-lane
  expressions (`pc.coalesce`, `hash_expression`, `projection_for_schema`).
- **UDF-backed transforms:** `expr_context` normalization and JSON serialization
  run through Arrow compute UDFs instead of row-level loops.
- **Quality artifacts:** invalid IDs are captured in plan-lane quality tables and
  materialized alongside finalized outputs.

## API changes

The `build_cpg_*_plan` helpers return `IbisPlan` instances that are consumed
by the task pipeline:

- `build_cpg_nodes_plan(catalog, ctx, backend) -> IbisPlan`
- `build_cpg_edges_plan(catalog, ctx, backend) -> IbisPlan`
- `build_cpg_props_plan(catalog, ctx, backend, options=...) -> IbisPlan`

Downstream consumers should materialize these plans through the task/plan
catalog (or the Hamilton outputs) rather than expecting a finalize wrapper.

## Hamilton artifacts

New pipeline artifacts are available:

- `cpg_nodes_quality`
- `cpg_edges_quality`
- `cpg_props_quality`

These can be materialized alongside the finalized CPG tables.
