# DataFusion Plan Artifacts v5 Migration Notes (Design)

Status: design-only guidance (no production migration executed)

## Summary of changes
- Plan artifacts table name: `datafusion_plan_artifacts_v5`.
- Added planning environment provenance (`planning_env_json`, `planning_env_hash`).
- Added rulepack provenance (`rulepack_json`, `rulepack_hash`).
- Added EXPLAIN row payloads (`explain_tree_rows_json`, `explain_verbose_rows_json`).
- Added DataFusion proto payloads (`logical_plan_proto_b64`, `optimized_plan_proto_b64`, `execution_plan_proto_b64`).
- Added UDF planner metadata snapshot (`udf_planner_snapshot_json`).

## Migration strategy (recommended)
- Rebuild plan artifacts by re-running the planning pipeline so the new
  planning env, rulepack, explain-row, and proto payloads are generated
  from the current DataFusion runtime.
- Treat v5 as a clean rebuild to ensure deterministic fingerprints and diagnostics.

## Backfill outline (if rebuild is not possible)
1. Read the v4 Delta table.
2. Add the new v5 columns with null/default values.
3. For rows missing new payloads, re-run planning for the view name to regenerate
   plan artifacts and replace the row.
4. Write the transformed rows into a new Delta table named `datafusion_plan_artifacts_v5`.

## Validation checklist
- Compare row counts between v4 and v5 per view name.
- Ensure `plan_identity_hash` stability for unchanged inputs.
- Spot-check new provenance payloads (`planning_env_json`, `rulepack_json`).
- Confirm EXPLAIN row payloads and proto payloads are present for sampled rows.
