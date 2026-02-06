# Integration Testing v2 Implementation Findings

Date: 2026-02-06

Inputs:
- `docs/plans/expanded_integration_testing_v2.md` (implementation plan)
- `docs/plans/expanded_integration_testing_v2_review.md` (review commentary)
- Test execution results across 11 new test files

## Executive Summary

Of the 15 planned suites (3.1-3.15), 11 were implemented across 70 tests:
- **43 passing** - Exercising real subsystem boundaries with real imports
- **21 skipped** - Documented placeholders requiring infrastructure not yet available
- **6 failing** - All in Suite 3.1, require production test helper for `compile_execution_plan`
- **4 suites not implemented** (3.12-3.15) - Documented below with implementation guidance

Quality gates:
- `ruff check`: All checks passed
- `pyrefly check`: 0 errors (1 suppressed)
- `pytest`: 43 passed, 21 skipped, 6 failed

---

## 1. Failing Tests (6) - Production Changes Required

All 6 failures are in **Suite 3.1** (`tests/integration/relspec/test_compile_execution_plan.py`).

### Root Cause

`compile_execution_plan()` calls `dataset_schema_from_context()` early in its path, which does `session_ctx.table("rel_name_symbol").schema()`. This fails with `KeyError: "No table named 'rel_name_symbol'"` because the test's `SessionRuntime` has a bare `DataFusionRuntimeProfile()` session context without registered dataset tables.

The test creates mock `ViewNode` objects with builders that return `ctx.sql("SELECT 1 as value")`, but `compile_execution_plan` internally resolves dataset schemas from the session context registry (not from the view builders), requiring pre-registered tables for `rel_name_symbol` and likely other semantic datasets.

### Affected Tests

| Test | What It Validates |
|------|-------------------|
| `test_compile_execution_plan_basic_roundtrip` | Full compile -> ExecutionPlan with task_graph, task_schedule, plan_signature |
| `test_compile_determinism` | Same inputs produce identical plan_signature and task_dependency_signature |
| `test_runtime_profile_mismatch_raises` | request.runtime_profile != session_runtime.profile raises ValueError |
| `test_requested_task_filtering` | Only requested_task_names appear in active_tasks |
| `test_plan_artifacts_store_failure_continues` | Compilation succeeds despite persist_plan_artifacts failure |
| `test_plan_signature_changes_with_runtime_config` | Different UDF hashes produce different plan_signatures |

### Recommended Production Changes

**Option A (Preferred): Add a test helper for minimal session setup**

Create a `test_support` helper that registers minimal empty tables for the semantic datasets that `compile_execution_plan` resolves during early compilation. This would be a function like:

```python
# src/test_support/session_helpers.py
def register_minimal_semantic_tables(ctx: SessionContext) -> None:
    """Register empty tables with correct schemas for semantic datasets.

    Enables compile_execution_plan tests without full pipeline infrastructure.
    """
```

This function would register empty DataFusion tables for `rel_name_symbol` and any other tables resolved by `dataset_schema_from_context()` during the plan compilation path. The schemas can be derived from `schema_spec/` definitions.

**Option B: Lazy schema resolution**

Modify `compile_execution_plan` to defer `dataset_schema_from_context()` calls until they are actually needed during task graph construction, rather than resolving them eagerly. This would allow early validation tests (profile mismatch, missing plan_bundle) to pass without registered tables. This is a larger change with broader impact.

**Option C: Skip and document**

Mark these 6 tests as `@pytest.mark.skip(reason="Requires registered semantic tables")` with the understanding that they serve as executable documentation of the compile_execution_plan API contract and will be enabled when test helpers are available.

---

## 2. Skipped Tests (21) - Infrastructure Requirements

### Category A: Full Pipeline Infrastructure (7 tests)

These require the complete extraction/normalization/pipeline stack with UDFs, extractors, and normalization context.

| Suite | Test | Infrastructure Needed |
|-------|------|-----------------------|
| 3.2 | `test_materialize_basic_roundtrip` | Full extraction pipeline with registered UDFs and normalization context |
| 3.5 | `test_plan_execution_diff_emitted_on_success` | Full pipeline execution (slow, complex) |
| 3.5 | `test_pipeline_partial_output_handling` | Full pipeline with partial failure injection |
| 3.7 | `test_byte_span_consistency_cst_vs_ast` | CST + AST extractor coordination with `scip_to_byte_offsets` |
| 3.7 | `test_byte_span_consistency_cst_vs_tree_sitter` | Tree-sitter + CST extractor coordination |
| 3.10 | `test_file_id_consistency_across_extractors` | CST, AST, and symtable extractors running in coordination |
| 3.10 | `test_parallel_extraction_partial_failure_resilience` | Parallel extraction infrastructure (`extract.infrastructure.parallel`) |

**Recommended approach:** These are valuable E2E-level tests. Build a shared `ExtractorTestHarness` fixture that:
1. Creates a minimal `ExtractSession` with registered UDFs
2. Provides sample source files with known byte offsets
3. Runs extractors and collects outputs for assertion

### Category B: Semantic Compiler Setup (6 tests)

These require `SemanticCompiler` configuration with type annotations, join strategy inference, and relationship compilation.

| Suite | Test | Infrastructure Needed |
|-------|------|-----------------------|
| 3.8 | `test_join_key_type_mismatch_error_quality` | SemanticCompiler-level join validation with typed schemas |
| 3.8 | `test_udf_alias_resolution` | UDF alias registry setup |
| 3.8 | `test_relationship_failure_does_not_abort_pipeline` | SemanticCompiler with multiple relationships + diagnostics |
| 3.8 | `test_infer_join_strategy_basic` | `AnnotatedSchema.wrap()` with semantic type metadata |
| 3.9 | `test_wrong_column_type_error_message_quality` | Semantic input registry schema validation paths |
| 3.9 | `test_missing_required_typed_column_cross_module_error` | Cross-module input validation error reporting |

**Recommended approach:** Create a `SemanticCompilerTestFixture` that:
1. Provides pre-configured `AnnotatedSchema` objects with semantic annotations
2. Wraps `SemanticCompiler` initialization with minimal config
3. Captures diagnostics from relationship compilation failures

### Category C: Delta/Storage Integration (4 tests)

These require actual Delta Lake table operations and storage pipeline wiring.

| Suite | Test | Infrastructure Needed |
|-------|------|-----------------------|
| 3.6 | `test_idempotent_write_deduplication` | Investigation of actual dedup behavior in production Delta writes |
| 3.6 | `test_write_pipeline_propagates_idempotent` | WritePipeline integration with idempotent commit properties |
| 3.6 | `test_incremental_delete_propagates_idempotent` | Incremental delete flow with idempotent properties |
| 3.6 | `test_schema_evolution_with_idempotent_write` | Schema evolution during idempotent Delta writes |

**Recommended approach:** These require understanding of how `idempotent_commit_properties` flows through `WritePipeline._delta_write_spec`, `semantics/incremental/delta_updates.py`, and `semantics/incremental/snapshot.py`. Start with a focused investigation of the actual dedup path in `storage/deltalake/delta.py`.

### Category D: Production API Exposure (2 tests)

| Suite | Test | Infrastructure Needed |
|-------|------|-----------------------|
| 3.4 | `test_signal_handler_captures_correct_state` | Production code needs to expose `signal_state` dict for testing |
| 3.5 | `test_plan_execution_diff_blocked_datasets` | Complex mock of scan unit metadata mapping |

**Recommended approach for signal handler test:** Add a `_signal_state` attribute on the handler closure or pass it through a diagnostics channel, then assert on `run_id` and `run_bundle_dir` in the captured state.

### Category E: Parallel Infrastructure (2 tests)

| Suite | Test | Infrastructure Needed |
|-------|------|-----------------------|
| 3.10 | `test_parallel_extraction_result_order_independence` | `extract.infrastructure.parallel` module understanding |
| 3.11 | `test_schedule_with_selective_column_requirements` | `schedule_tasks` auto-registers output evidence nodes, making column-level blocking hard to isolate |

---

## 3. Unimplemented Suites (4)

The P3 implementation agent ran out of context after completing Suite 3.11. Suites 3.12-3.15 are documented in the plan but have no test files.

### Suite 3.12: CDF Cursor Lifecycle

**File:** `tests/integration/storage/test_cdf_cursor_lifecycle.py`

**Planned tests:**
| Test | What It Validates |
|------|-------------------|
| `test_cursor_state_progression_through_cdf_reads` | Delta CDF read -> cursor update -> state progression |
| `test_cursor_recovery_after_corruption` | Invalid JSON recovery path |
| `test_concurrent_cursor_updates` | Thread-safe last-write-wins |

**Implementation notes:** `CdfCursorStore` is at `semantics/incremental/cdf_cursors.py:96` (StructBaseStrict with `cursors_path` field). `CdfCursor` is a msgspec.Struct with `dataset_name`, `last_version`, `last_timestamp`. Unit tests already cover `get_start_version` and path sanitization - focus only on runtime interaction with Delta CDF reads.

### Suite 3.13: Dataset Resolution Contracts

**File:** `tests/integration/storage/test_dataset_resolution_contracts.py`

**Planned tests:**
| Test | What It Validates |
|------|-------------------|
| `test_storage_options_merge_request_overrides_policy` | Request storage options override policy defaults |
| `test_provider_artifacts_include_scan_metadata` | Provider artifacts include `delta_scan_identity_hash` |

**Implementation notes:** Suite 3.3 already covers the core `resolve_dataset_provider` paths (7/7 passing). These additional tests cover edge cases around storage option merging (via `utils/storage_options.py`) and artifact metadata completeness.

### Suite 3.14: Extraction to CPG Output Chain

**File:** `tests/e2e/test_extraction_to_cpg.py` (note: `tests/e2e/` per review correction 3)

**Planned tests:**
| Test | What It Validates |
|------|-------------------|
| `test_single_file_repo_produces_cpg_nodes` | Full pipeline on 1-file repo produces valid CPG nodes |
| `test_entity_ids_are_deterministic` | Build twice -> identical entity_id values |
| `test_byte_spans_within_file_bounds` | All bstart >= 0 and bend <= file size |
| `test_graceful_degradation_empty_file` | Empty .py file -> correct-schema empty output |

**Implementation notes:** This is the highest-value E2E test. It requires calling `build_graph_product(GraphProductBuildRequest(repo_root=tmp_path))` on a minimal Python repo. Mark with `@pytest.mark.e2e`. Expect ~30s execution time.

### Suite 3.15: Hamilton Materializer Contracts

**File:** `tests/integration/pipeline/test_materializer_contracts.py`

**Planned tests:**
| Test | What It Validates |
|------|-------------------|
| `test_table_summary_saver_metadata_payload` | `save_data()` returns all 9 metadata keys |
| `test_table_summary_saver_row_count_accuracy` | 100-row table -> `rows == 100` |
| `test_param_table_summary_saver_mapping_required` | Non-mapping input -> TypeError |
| `test_materializer_factory_count` | `build_hamilton_materializers()` returns 7 factories |
| `test_materializer_factory_wiring_against_delta_specs` | All factory dataset_names match `delta_output_specs()` |
| `test_validate_delta_output_payload_accepts_valid` | Valid payload passes validation |
| `test_validate_delta_output_payload_rejects_missing_key` | Missing key raises ValueError |

**Implementation notes:** `TableSummarySaver.save_data()` at `hamilton_pipeline/materializers.py:53` returns metadata dict (NOT a Delta writer - per review correction 2). `build_hamilton_materializers()` at line 126 produces 7 factories. `validate_delta_output_payload()` at `hamilton_pipeline/io_contracts.py:169`. These tests are straightforward to implement with Arrow table fixtures.

---

## 4. Passing Tests (43) - Coverage Provided

### Suite 3.1: compile_execution_plan (4 of 10 passing)

| Test | Coverage |
|------|----------|
| `test_missing_plan_bundle_for_requested_task_raises` | Validates early guard: requested task without plan_bundle raises ValueError |
| `test_no_plan_bundles_raises` | Validates early guard: all view nodes without plan_bundle raises ValueError |
| `test_delta_pin_conflict_raises` | `_scan_unit_delta_pins()` detects conflicting versions for same dataset |
| `test_delta_pin_conflict_same_version_ok` | `_scan_unit_delta_pins()` accepts identical pins for same dataset |

### Suite 3.2: materialize_extract_plan (5 of 6 passing)

| Test | Coverage |
|------|----------|
| `test_evidence_plan_compilation` | `compile_evidence_plan()` with default, extended, restricted, and columnar plans |
| `test_extract_plan_options_construction` | `ExtractPlanOptions` API: evidence_plan, resolved_repo_id |
| `test_artifact_emissions` | Diagnostics collector captures `extract_plan_compile_v1` and `extract_plan_execute_v1` |
| `test_extract_materialize_options_construction` | `ExtractMaterializeOptions` API: prefer_reader, apply_post_kernels, normalize |
| `test_record_batch_reader_construction` | `record_batch_reader_from_rows()` produces valid schema-aligned reader |

### Suite 3.3: resolve_dataset_provider (7 of 7 passing)

| Test | Coverage |
|------|----------|
| `test_resolve_delta_provider_roundtrip` | Delta provider resolution produces valid ScanUnit |
| `test_resolve_delta_cdf_provider` | CDF provider resolution path |
| `test_resolution_captures_snapshot_key` | Snapshot key propagation |
| `test_scan_identity_hash_deterministic` | Hash stability across calls |
| `test_invalid_predicate_captured` | Invalid filter handling |
| `test_control_plane_failure_wraps_error` | Error wrapping on provider failure |
| `test_scan_files_override` | File override mechanism |

### Suite 3.4: build_graph_product orchestration (5 of 6 passing)

| Test | Coverage |
|------|----------|
| `test_heartbeat_starts_before_execution` | Heartbeat lifecycle: start before _execute_build, stop in finally |
| `test_signal_handlers_installed_and_restored` | SIGTERM/SIGINT handler install during build, restore after |
| `test_build_phase_start_and_end_events` | `build.phase.start` / `build.phase.end` diagnostics events |
| `test_build_failure_records_diagnostics` | `build.failure` event with error_type and error message |
| `test_build_success_records_diagnostics` | `build.success` event with run_id and output_dir |

### Suite 3.5: pipeline diagnostics (3 of 6 passing)

| Test | Coverage |
|------|----------|
| `test_plan_execution_diff_missing_tasks` | `_emit_plan_execution_diff` reports missing tasks (planned but not executed) |
| `test_plan_execution_diff_unexpected_tasks` | Reports unexpected tasks (executed but not planned) |
| `test_plan_execution_diff_payload_shape` | Validates all 15 required fields with correct types |

### Suite 3.6: idempotent write contracts (4 of 8 passing)

| Test | Coverage |
|------|----------|
| `test_idempotent_different_versions_both_commit` | Different idempotent_key versions produce distinct properties |
| `test_commit_metadata_includes_operation_and_mode` | `ca_operation` and `ca_write_mode` keys present |
| `test_extra_metadata_propagated` | Extra metadata merged into commit properties |
| `test_no_idempotent_options_still_requires_metadata` | Minimal commit always includes ca_operation, ca_write_mode, ca_commit_timestamp |

### Suite 3.7: byte span canonicalization (4 of 6 passing)

| Test | Coverage |
|------|----------|
| `test_col_unit_utf32_to_byte_conversion` | UTF-32 column offset to byte offset conversion for multi-byte chars |
| `test_span_normalization_with_missing_udf` | `validate_required_udfs` raises when required UDF missing |
| `test_span_normalization_crlf_vs_lf` | CRLF vs LF produces different byte offsets for same logical position |
| `test_file_line_index_alignment` | Line-to-byte-offset index consistency |

### Suite 3.8: compiler type validation (2 of 6 passing)

| Test | Coverage |
|------|----------|
| `test_missing_udf_signature_metadata` | `validate_required_udfs` for missing UDFs with signature metadata |
| `test_missing_udf_return_metadata` | Validates return type metadata in UDF validation errors |

### Suite 3.9: input schema contracts (3 of 5 passing)

| Test | Coverage |
|------|----------|
| `test_extra_columns_accepted_silently` | Extra columns in input don't cause validation errors |
| `test_schema_validation_basic_types` | Basic PyArrow type validation (int, string, float, bool) |
| `test_nested_struct_schema_validation` | Nested struct field validation |

### Suite 3.10: file identity coordination (4 of 7 passing)

| Test | Coverage |
|------|----------|
| `test_file_sha256_consistency` | SHA256 hash is deterministic for same content |
| `test_file_context_attributes` | FileContext attributes: file_id, path, size_bytes, content |
| `test_file_sha256_computation` | `bytes_from_file_ctx()` -> hashlib.sha256 roundtrip |
| `test_file_context_with_unicode_content` | FileContext handles multi-byte unicode correctly |

### Suite 3.11: schedule edge validation (2 of 3 passing)

| Test | Coverage |
|------|----------|
| `test_catalog_mutation_unblocks_dependent` | Adding missing catalog entry unblocks dependent edge |
| `test_validation_summary_reflects_blocked_edges` | validation_summary correctly reports blocked edge counts |

---

## 5. Priority Recommendations for Enabling Failing/Skipped Tests

### P0: Enable Suite 3.1 failures (6 tests)

**Action:** Create `src/test_support/session_helpers.py` with `register_minimal_semantic_tables(ctx)` that registers empty DataFusion tables with schemas from `schema_spec/`. This is the single highest-value change - it unlocks 6 integration tests for the `compile_execution_plan` choke-point (1 callsite, zero previous direct integration coverage).

### P1: Implement Suite 3.15 (7 new tests)

**Action:** Implement `tests/integration/pipeline/test_materializer_contracts.py`. This requires no production changes - just `TableSummarySaver`, `ParamTableSummarySaver`, `build_hamilton_materializers()`, and `validate_delta_output_payload()` are all testable with Arrow table fixtures. Highest ROI of the unimplemented suites.

### P2: Implement Suite 3.12 (3 new tests) and Suite 3.13 (2 new tests)

**Action:** Implement CDF cursor lifecycle and dataset resolution edge cases. Suite 3.12 needs actual Delta CDF table fixtures. Suite 3.13 extends the already-passing Suite 3.3.

### P3: Enable semantic compiler tests (6 skipped tests in Suites 3.8-3.9)

**Action:** Create a `SemanticCompilerTestFixture` that provides pre-configured `AnnotatedSchema` objects and wraps `SemanticCompiler` initialization. Enables testing join strategy inference, type validation error messages, and relationship failure resilience.

### P4: Implement Suite 3.14 (4 E2E tests)

**Action:** Implement `tests/e2e/test_extraction_to_cpg.py`. Requires full pipeline (slow). Start with `test_single_file_repo_produces_cpg_nodes` as the minimal smoke test.

### P5: Build extraction test harness (7 skipped tests across Suites 3.2, 3.7, 3.10)

**Action:** Create an `ExtractorTestHarness` fixture that runs real extractors on sample source files. Enables the cross-extractor consistency tests for byte spans, file IDs, and parallel extraction.

---

## 6. New Test File Inventory

| File | Suite | Pass | Skip | Fail | Total |
|------|-------|------|------|------|-------|
| `tests/integration/relspec/test_compile_execution_plan.py` | 3.1 | 4 | 0 | 6 | 10 |
| `tests/integration/extraction/test_materialize_extract_plan.py` | 3.2 | 5 | 1 | 0 | 6 |
| `tests/integration/storage/test_resolve_dataset_provider.py` | 3.3 | 7 | 0 | 0 | 7 |
| `tests/integration/pipeline/test_build_orchestration.py` | 3.4 | 5 | 1 | 0 | 6 |
| `tests/integration/pipeline/test_pipeline_diagnostics.py` | 3.5 | 3 | 3 | 0 | 6 |
| `tests/integration/storage/test_idempotent_write_contracts.py` | 3.6 | 4 | 4 | 0 | 8 |
| `tests/integration/extraction/test_byte_span_canonicalization.py` | 3.7 | 4 | 2 | 0 | 6 |
| `tests/integration/semantics/test_compiler_type_validation.py` | 3.8 | 2 | 4 | 0 | 6 |
| `tests/integration/semantics/test_input_schema_contracts.py` | 3.9 | 3 | 2 | 0 | 5 |
| `tests/integration/extraction/test_file_identity_coordination.py` | 3.10 | 4 | 3 | 0 | 7 |
| `tests/integration/relspec/test_schedule_edge_validation.py` | 3.11 | 2 | 1 | 0 | 3 |
| **Total** | | **43** | **21** | **6** | **70** |

### Unimplemented files:

| File | Suite | Planned Tests |
|------|-------|---------------|
| `tests/integration/storage/test_cdf_cursor_lifecycle.py` | 3.12 | 3 |
| `tests/integration/storage/test_dataset_resolution_contracts.py` | 3.13 | 2 |
| `tests/e2e/test_extraction_to_cpg.py` | 3.14 | 4 |
| `tests/integration/pipeline/test_materializer_contracts.py` | 3.15 | 7 |
| **Total unimplemented** | | **16** |

**Grand total planned:** 86 tests across 15 suites
**Implemented:** 70 tests across 11 suites
**Not implemented:** 16 tests across 4 suites
