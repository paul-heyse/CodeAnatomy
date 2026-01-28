# DataFusion Plan Artifacts v6 Migration Notes

Date: 2026-01-28

## Summary
- Plan artifacts table name: `datafusion_plan_artifacts_v6`.
- Removed redundant explain JSON payload columns now superseded by row payloads + plan_details:
  - `explain_tree_json`
  - `explain_verbose_json`
  - `explain_analyze_json`
- The canonical EXPLAIN row payloads remain in:
  - `explain_tree_rows_json`
  - `explain_verbose_rows_json`
- EXPLAIN ANALYZE metrics are retained in:
  - `explain_analyze_duration_ms`
  - `explain_analyze_output_rows`

## Migration outline
1. Read from the previous plan artifacts table (currently `datafusion_plan_artifacts_v5`).
2. Drop the removed explain JSON columns listed above.
3. Write the transformed rows into a new Delta table named `datafusion_plan_artifacts_v6`.

## Notes
- Downstream consumers should use `plan_details_json` for any retained explain text.
- Row-form EXPLAIN payloads are now the canonical structured explain artifacts.
