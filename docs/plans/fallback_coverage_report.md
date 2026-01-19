# Fallback Coverage Report

Generated: 2026-01-19T06:42:21.770976+00:00

## Summary
- arrowdsl_lane_fallback_failures: 2
- datafusion_policy_hooks_failures: 0
- datafusion_sql_fallback_failures: 0
- ibis_backend_fallback_failures: 0
- incremental_data_fallback_failures: 0
- policy_driven_fallback_failures: 0
- total_failures: 2
- udf_kernel_fallback_failures: 0

## datafusion_sql_fallback
- static_keys: 1
- dynamic_failures: 0
- dynamic_skipped: 17

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
