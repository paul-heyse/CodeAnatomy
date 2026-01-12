# CPG Migration Notes

This document summarizes the design-phase migration to the unified CPG build
architecture introduced in `cpg_dedup_acero_plan.md`.

## Key changes

- **Unified family specs:** node/prop specs are generated from a single
  `EntityFamilySpec` registry to prevent drift.
- **Table catalog:** CPG builders now assemble input tables via a shared
  `TableCatalog`, including derived tables.
- **Relation registry:** edge relations are built from `EDGE_RELATION_SPECS`
  instead of per-file ad hoc helpers.
- **Defaults + schema unification:** edge defaults are filled per-row and schema
  unification is centralized.
- **Quality artifacts:** invalid IDs are captured in dedicated quality tables.

## API changes

The `build_cpg_*` functions now return `CpgBuildArtifacts`, which contains:

- `finalize`: the `FinalizeResult` (good/errors/stats/alignment)
- `quality`: quality artifact table

Downstream consumers should access the final tables via
`result.finalize.good`.

## Hamilton artifacts

New pipeline artifacts are available:

- `cpg_nodes_quality`
- `cpg_edges_quality`
- `cpg_props_quality`

These can be materialized alongside the finalized CPG tables.
