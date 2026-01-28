# DataFusion Plan Artifacts v4 Migration Notes (Design)

Status: design-only guidance (no production migration executed)

## Summary of changes
- Plan artifacts table name: `datafusion_plan_artifacts_v4`.
- Removed redundant plan display columns (logical/optimized/execution plan display, pgjson, graphviz).
- Explain outputs, information_schema snapshots, and substrait validation payloads remain stored.
- Detailed plan displays are now available in `plan_details_json`.

## Migration strategy (recommended)
- Rebuild plan artifacts by re-running the planning pipeline so the new explain and
  information_schema payloads are generated from the current DataFusion runtime.
- Treat v4 as a clean rebuild to ensure deterministic fingerprints and diagnostics.

## Backfill outline (if rebuild is not possible)
1. Read the v3 Delta table.
2. Drop the removed plan display columns.
3. Ensure these columns are present:
   - `information_schema_json`
   - `information_schema_hash`
   - `explain_tree_json`
   - `explain_verbose_json`
   - `explain_analyze_json`
   - `substrait_validation_json`
4. For rows missing new payloads, re-run planning for the view name to regenerate
   plan artifacts and replace the row.
5. Write the transformed rows into a new Delta table named `datafusion_plan_artifacts_v4`.

## Validation checklist
- Compare row counts between v3 and v4 per view name.
- Ensure `plan_identity_hash` stability for unchanged inputs.
- Spot-check `plan_details_json` for logical/optimized/physical plan text.
- Confirm explain payloads are present for sampled rows.
