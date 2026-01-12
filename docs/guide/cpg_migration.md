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

The `build_cpg_*` functions now return `CpgBuildArtifacts`, which contains:

- `finalize`: the `FinalizeResult` (good/errors/stats/alignment)
- `quality`: quality artifact table

Downstream consumers should access the final tables via
`result.finalize.good`.

Plan-returning surfaces now require an execution context:

- `build_cpg_nodes_raw(ctx=..., ...) -> Plan`
- `build_cpg_edges_raw(ctx=..., ...) -> Plan`
- `build_cpg_props_raw(ctx=..., ...) -> Plan`

## Hamilton artifacts

New pipeline artifacts are available:

- `cpg_nodes_quality`
- `cpg_edges_quality`
- `cpg_props_quality`

These can be materialized alongside the finalized CPG tables.
