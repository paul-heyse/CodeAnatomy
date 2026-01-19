# Fallback Coverage Report

Generated: 2026-01-19T06:07:02.480387+00:00

## Summary
- arrowdsl_lane_fallback_failures: 2
- datafusion_policy_hooks_failures: 0
- datafusion_sql_fallback_failures: 22
- ibis_backend_fallback_failures: 0
- incremental_data_fallback_failures: 0
- policy_driven_fallback_failures: 0
- total_failures: 24
- udf_kernel_fallback_failures: 0

## datafusion_sql_fallback
- static_keys: 1
- dynamic_failures: 22
- dynamic_skipped: 17
- sample_failures:
  - ts_nodes_v1 | ts_nodes_v1 | extract_query | DataFusion fallback blocked for rule 'ts_nodes_v1' output 'ts_nodes_v1' (translation_error): Select
  - ts_errors_v1 | ts_errors_v1 | extract_query | DataFusion fallback blocked for rule 'ts_errors_v1' output 'ts_errors_v1' (translation_error): Select
  - ts_missing_v1 | ts_missing_v1 | extract_query | DataFusion fallback blocked for rule 'ts_missing_v1' output 'ts_missing_v1' (translation_error): Select
  - rt_objects_v1 | rt_objects_v1 | extract_query | DataFusion fallback blocked for rule 'rt_objects_v1' output 'rt_objects_v1' (translation_error): Select
  - rt_signatures_v1 | rt_signatures_v1 | extract_query | DataFusion fallback blocked for rule 'rt_signatures_v1' output 'rt_signatures_v1' (translation_error): Select
  - rt_signature_params_v1 | rt_signature_params_v1 | extract_query | DataFusion fallback blocked for rule 'rt_signature_params_v1' output 'rt_signature_params_v1' (translation_error): Select
  - rt_members_v1 | rt_members_v1 | extract_query | DataFusion fallback blocked for rule 'rt_members_v1' output 'rt_members_v1' (translation_error): Select
  - repo_files_v1 | repo_files_v1 | extract_query | DataFusion fallback blocked for rule 'repo_files_v1' output 'repo_files_v1' (translation_error): Select
  - py_cst_name_refs_v1 | py_cst_name_refs_v1 | extract_query | DataFusion fallback blocked for rule 'py_cst_name_refs_v1' output 'py_cst_name_refs_v1' (translation_error): Select
  - py_cst_imports_v1 | py_cst_imports_v1 | extract_query | DataFusion fallback blocked for rule 'py_cst_imports_v1' output 'py_cst_imports_v1' (translation_error): Select

## datafusion_policy_hooks
- static_keys: 4
- dynamic_failures: 0

## arrowdsl_lane_fallback
- static_keys: 2
- dynamic_failures: 2
- sample_failures:
  - callsite_qname_candidates__join__dim_qualified_names | unknown_dataset | hash_join | selected lane: acero
  - qname_fallback_calls | unknown_dataset | hash_join | selected lane: acero

## ibis_backend_fallback
- static_keys: 4
- dynamic_failures: 0

## udf_kernel_fallback
- static_keys: 2
- dynamic_failures: 0

## incremental_data_fallback
- static_keys: 1
- dynamic_failures: 0
- dynamic_skipped: 1

## policy_driven_fallback
- static_keys: 4
- dynamic_failures: 0
