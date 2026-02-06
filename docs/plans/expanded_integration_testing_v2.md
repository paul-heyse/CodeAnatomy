# Expanded Integration Testing Proposal (v2)

Builds on the initial integration testing proposal (`docs/plans/integration_testing_proposal.md`) by identifying additional cross-system boundaries that require integration coverage as the codebase approaches end-to-end build validation readiness.

## 1. Coverage Audit: Current State

### 1.1 Implemented Test Suites (from Proposal v1)

The following suites from the original proposal have been implemented and are running:

| Suite | Directory | Tests | Status |
|-------|-----------|-------|--------|
| Semantic Runtime Adapter Round-Trip | `integration/adapters/` | 11 | Complete |
| Plan Determinism | `integration/boundaries/` | 7 | Complete |
| Evidence Plan to Extractor | `integration/boundaries/` | 8 | Complete |
| Immutability Contracts | `integration/contracts/` | 12 parametrized | Complete |
| Extract Postprocess Resilience | `integration/error_handling/` | 11 | Complete |
| Evidence Plan Gating | `integration/error_handling/` | 15 | Complete |
| Schedule Generation | `integration/scheduling/` | 9 | Complete |
| Bipartite Graph Structure | `integration/scheduling/` | 5 | Complete |
| Object Store Registration | `integration/runtime/` | 7 | Complete |
| Cache Introspection | `integration/runtime/` | 4 | Complete |
| Delta Provider Panic Containment | `integration/runtime/` | 3 | Complete |
| Capability Negotiation | `integration/runtime/` | 4 | Complete |

### 1.2 Additional Top-Level Integration Tests (Outside Proposal)

These tests exist outside the structured proposal framework:

| Test File | Scope | Tests |
|-----------|-------|-------|
| `test_semantic_pipeline.py` | Input validation, join inference, naming | ~12 |
| `test_evidence_semantic_catalog.py` | IR spec registration, view nodes | 1 |
| `test_df_delta_smoke.py` | Delta round-trip write/query/provider | 1 |
| `test_semantic_incremental_overwrite.py` | Overwrite dataset writes | 1 |
| `test_delta_protocol_and_schema_mode.py` | Schema evolution, merge policies | varies |
| `test_delta_cache_alignment.py` | Cache invalidation | varies |
| `test_incremental_partitioned_updates.py` | Incremental writes | varies |
| `test_driver_factory_integration.py` | Hamilton driver building | varies |
| `test_engine_session_smoke.py` | Session creation | varies |
| `test_engine_session_semantic_config.py` | Semantic config wiring | varies |
| `test_substrait_cross_validation.py` | Substrait plan validation | varies |
| `test_expr_planner_hooks.py` | Expression planner | varies |

### 1.3 Unimplemented Sections from Proposal v1

The following areas from the original proposal remain unimplemented:

- Delta read-after-write schema contracts
- CDF merge strategy edge cases
- Impact closure strategies (`merge_impacted_files()` with closure modes)
- Multi-source byte-span canonicalization
- Expression validation graceful skip
- Streaming row tracking at scale (>100k rows)
- Delta pin conflicts (conflicting versions for same dataset)
- Bytecode integration (bytecode extractor with semantic pipeline)

---

## 2. Identified Coverage Gaps

Analysis using CQ structural queries across the full codebase identified six major untested cross-system boundaries. Each gap is classified by risk level and mapped to specific code locations.

### 2.1 Gap Summary

| # | Integration Boundary | Risk | Code Location | Current Coverage |
|---|---------------------|------|---------------|-----------------|
| G1 | Pipeline Execution Chain | HIGH | `hamilton_pipeline/execution.py` | None |
| G2 | Hamilton DAG Materializers to Delta | HIGH | `hamilton_pipeline/materializers.py` | None |
| G3 | Execution Plan Active Task Filtering | MEDIUM | `relspec/execution_plan.py:452` | Partial |
| G4 | Multi-Source Byte-Span Canonicalization | HIGH | `semantics/span_normalize.py` | None |
| G5 | Storage Layer Cross-Boundary Contracts | MEDIUM | `storage/deltalake/`, `datafusion_engine/delta/` | Partial |
| G6 | Extraction to Normalization Pipeline | MEDIUM | `extract/` to `semantics/` | Partial |

---

## 3. Proposed Test Suites

### Phase 1: Pipeline Orchestration (G1, G2)

#### Suite 3.1: Pipeline Execution Chain

**Boundary:** `build_graph_product()` -> `_execute_build()` -> `execute_pipeline()` -> Hamilton `driver.materialize()`

**File:** `tests/integration/pipeline/test_execution_chain.py`

**Key code references:**
- `execute_pipeline()` in `hamilton_pipeline/execution.py:256`
- `_resolve_driver_instance()` in `hamilton_pipeline/driver_factory.py:332`
- `_execute_build()` in `graph/product_build.py:282`

**Tests:**

| Test | Description |
|------|-------------|
| `test_execute_pipeline_with_minimal_repo` | Execute pipeline on a minimal Python repo (single file), verify it returns a valid output mapping with expected keys. |
| `test_execute_pipeline_records_diagnostics_on_success` | Verify `emit_diagnostics_event("build.phase.end", ...)` is called with `status=ok` on success. |
| `test_execute_pipeline_records_diagnostics_on_failure` | Inject a failure into pipeline execution; verify diagnostics event has `status=error` and exception is recorded on the OTel span. |
| `test_execute_pipeline_partial_output_handling` | When some Hamilton output nodes fail, verify error handling records partial results rather than discarding all. |
| `test_execute_pipeline_respects_requested_outputs` | Pass a restricted set of output names; verify only those are materialized. |
| `test_execute_pipeline_heartbeat_lifecycle` | Verify `start_build_heartbeat()` starts before execution and `heartbeat.stop()` runs in the finally block. |
| `test_execute_pipeline_signal_handler_registration` | Verify SIGTERM/SIGINT handlers are installed before execution and restored in the finally block. |

**Fixture strategy:** Use a `tmp_path` as repo root containing a minimal Python file. Create a lightweight `GraphProductBuildRequest` with constrained outputs to keep test fast.

---

#### Suite 3.2: Hamilton Materializer Integration

**Boundary:** Hamilton DAG output nodes -> `TableSummarySaver` -> Delta Lake writes

**File:** `tests/integration/pipeline/test_materializer_integration.py`

**Key code references:**
- `build_hamilton_materializers()` in `hamilton_pipeline/materializers.py:126`
- `TableSummarySaver` in `hamilton_pipeline/materializers.py`
- `delta_output_specs()` in `hamilton_pipeline/materializers.py`

**Tests:**

| Test | Description |
|------|-------------|
| `test_materializer_writes_delta_table` | Create a `TableSummarySaver`, invoke its `save_data()` with an Arrow table, verify Delta table is readable and schema matches. |
| `test_materializer_records_row_count_and_columns` | Verify the saver captures `num_rows`, column names, and `plan_signature` in its output metadata. |
| `test_materializer_schema_mismatch_handling` | Supply an Arrow table with extra/missing columns vs. expected schema; verify behavior (error or graceful skip). |
| `test_materializer_idempotent_write` | Write same data twice with identical `IdempotentWriteOptions`; verify Delta table does not duplicate. |
| `test_materializer_output_dir_creation` | Verify materializer creates the output directory if it does not exist, and fails clearly if the parent path is invalid. |

---

### Phase 2: Scheduling and Plan Compilation (G3)

#### Suite 3.3: Execution Plan Compilation End-to-End

**Boundary:** `ViewNode` sequence -> `InferredDeps` -> `TaskGraph` -> `TaskSchedule` -> `ExecutionPlan`

**File:** `tests/integration/relspec/test_execution_plan_compilation.py`

**Key code references:**
- `compile_execution_plan()` in `relspec/execution_plan.py:452`
- `infer_deps_from_plan_bundle()` in `relspec/inferred_deps.py:103`
- `build_task_graph_from_inferred_deps()` in `relspec/rustworkx_graph.py:264`
- `schedule_tasks()` in `relspec/rustworkx_schedule.py:60`

**Tests:**

| Test | Description |
|------|-------------|
| `test_inferred_deps_to_task_graph_to_schedule` | Build `InferredDeps` from realistic fixtures, construct graph, schedule, and verify topological order respects all dependencies end-to-end. |
| `test_execution_plan_compilation_determinism` | Compile same plan twice with identical inputs; compare all signatures, costs, schedule order. Verify full determinism. |
| `test_active_task_filtering_preserves_topology` | Set `requested_task_names` to a subset; verify pruned graph still respects topological order and remains acyclic. |
| `test_impacted_task_filtering_with_nonexistent_tasks` | Pass task names not in the graph as `impacted_task_names`; verify graceful handling (ignored or reported). |
| `test_empty_schedule_when_all_tasks_filtered` | Filter all tasks via `requested_task_names`; verify an empty but valid `TaskSchedule` is returned. |
| `test_plan_signature_changes_with_session_config` | Compile plans with different `SemanticRuntimeConfig`; verify `plan_signature` differs while `task_dependency_signature` is stable. |

---

#### Suite 3.4: Edge Validation in Scheduling Context

**Boundary:** `EvidenceCatalog` -> `validate_edge_requirements()` -> `schedule_tasks()` ready-task filtering

**File:** `tests/integration/relspec/test_schedule_edge_validation.py`

**Key code references:**
- `validate_edge_requirements()` in `relspec/graph_edge_validation.py:114`
- `schedule_tasks()` in `relspec/rustworkx_schedule.py:60`
- `EvidenceCatalog` in `relspec/evidence.py`

**Tests:**

| Test | Description |
|------|-------------|
| `test_schedule_with_selective_column_requirements` | Task A requires `col_1` from ev_a (available); task B requires `col_2` (unavailable). Verify A is scheduled, B is in `missing_tasks`. |
| `test_schedule_with_catalog_mutation_and_validation` | After task A runs, its output columns are registered in catalog. Verify dependent task B can now pass edge validation and is scheduled. |
| `test_validation_summary_reflects_blocked_edges` | Blocked task's edge validation failures must appear in `TaskSchedule.validation_summary`. |
| `test_partial_column_match_blocks_task` | Task requires 3 columns, evidence has 2. Verify task is blocked, not scheduled. |

---

#### Suite 3.5: Cost Context and Graph Reduction

**Boundary:** `TaskGraph` -> `task_dependency_reduction()` -> `bottom_level_costs()` -> `ScheduleCostContext` -> `schedule_tasks()`

**File:** `tests/integration/relspec/test_cost_context_integration.py`

**Key code references:**
- `task_dependency_reduction()` in `relspec/rustworkx_graph.py:718`
- `bottom_level_costs()` in `relspec/execution_plan.py`
- `task_slack_by_task()` in `relspec/execution_plan.py`

**Tests:**

| Test | Description |
|------|-------------|
| `test_cost_context_affects_ready_task_ordering` | Diamond graph with cost context; verify higher-cost tasks are prioritized in ready-task sorting. |
| `test_transitive_reduction_correctness` | Linear chain A->B->C with shortcut A->C; verify reduction removes the shortcut edge. |
| `test_pruned_graph_remains_acyclic` | Prune subset of tasks from graph; verify reduced graph is still a DAG. |
| `test_cost_context_with_transitive_reduction` | Verify cost calculations on reduced graph are consistent (no cost inflation from removed edges). |
| `test_bottom_level_costs_on_empty_graph` | Verify cost functions handle empty graph gracefully. |

---

### Phase 3: Multi-Source Extraction and Canonicalization (G4, G6)

#### Suite 3.6: Cross-Extractor Byte-Span Consistency

**Boundary:** Multiple extractors (CST, AST, tree-sitter) -> `normalize_byte_span_df()` -> canonical `bstart`/`bend`

**File:** `tests/integration/extraction/test_byte_span_canonicalization.py`

**Key code references:**
- `normalize_byte_span_df()` in `semantics/span_normalize.py`
- `CstExtractOptions` (col_unit: "utf32") in `extract/extractors/cst_extract.py`
- `AstExtractOptions` (line/col) in `extract/extractors/ast_extract.py`
- `file_line_index_v1` table for line offset mapping

**Tests:**

| Test | Description |
|------|-------------|
| `test_byte_span_consistency_cst_vs_ast` | Extract same Python file via CST and AST extractors; normalize both; verify `bstart`/`bend` match for overlapping entities. |
| `test_byte_span_consistency_cst_vs_tree_sitter` | Extract same file via CST and tree-sitter; verify byte spans are identical after normalization. |
| `test_mixed_source_normalization_direct_and_line` | Register CST table with direct byte spans and AST table with line/col; normalize both using shared `file_line_index_v1`; verify aligned byte offsets. |
| `test_col_unit_alignment_utf32_to_byte` | CST emits col_unit="utf32"; verify normalization converts to canonical byte offsets for files with multi-byte characters. |
| `test_span_normalization_with_missing_line_index` | Attempt normalization when `file_line_index_v1` is missing; verify fallback behavior or clear error. |
| `test_span_normalization_crlf_vs_lf` | Create files with different line ending styles; verify byte offset calculations produce correct spans. |

**Fixture strategy:** Create a multi-byte Python file with Unicode identifiers. Run each extractor independently, collect Arrow tables, then apply span normalization.

---

#### Suite 3.7: File Identity and Parallel Extraction

**Boundary:** `FileContext` -> parallel extractors -> consistent `file_id`/`path` across outputs

**File:** `tests/integration/extraction/test_file_identity_coordination.py`

**Key code references:**
- `file_identity_row()` in `extract/coordination/context.py`
- `parallel_map()` in `extract/infrastructure/parallel.py`
- `bytes_from_file_ctx()` in `extract/coordination/context.py`

**Tests:**

| Test | Description |
|------|-------------|
| `test_file_id_consistency_across_extractors` | Extract same file via CST, AST, and symtable; verify `file_id` and `path` values are identical in all output tables. |
| `test_file_sha256_consistency` | Verify `file_sha256` matches across all extractors for the same file content. |
| `test_no_orphan_rows_after_parallel_extraction` | After parallel extraction, verify every `file_id` in extraction output tables exists in `repo_files_v1`. |
| `test_parallel_extraction_partial_failure_resilience` | Configure one extractor to fail on specific file; verify other extractors still produce valid output and error is recorded. |
| `test_parallel_extraction_result_order_independence` | Run parallel extraction twice; verify output schemas and row counts are identical regardless of worker scheduling. |

---

#### Suite 3.8: Evidence Plan Materialization Gating

**Boundary:** `EvidencePlan` -> `plan_feature_flags()` -> extractor execution -> materialization

**File:** `tests/integration/extraction/test_evidence_plan_materialization.py`

**Key code references:**
- `compile_evidence_plan()` in `extract/coordination/evidence_plan.py`
- `materialize_extract_plan()` in `extract/coordination/materialization.py`
- `plan_feature_flags()` in `extract/coordination/spec_helpers.py`

**Tests:**

| Test | Description |
|------|-------------|
| `test_gated_datasets_not_materialized` | Create evidence plan requiring only CST; verify AST and bytecode tables are NOT materialized. |
| `test_required_columns_projection_enforced` | Create plan with column restrictions; verify materialized table has only the required columns. |
| `test_full_plan_materializes_all_sources` | Use `compile_evidence_plan()` with all defaults; verify all extraction sources produce output. |
| `test_empty_plan_produces_no_output` | Create empty evidence plan; verify no datasets are materialized. |

---

### Phase 4: Semantic Input Validation and Compilation (G6)

#### Suite 3.9: Semantic Input Schema Contracts

**Boundary:** Extraction outputs -> `validate_semantic_inputs()` -> `validate_semantic_input_columns()` -> `SemanticCompiler`

**File:** `tests/integration/semantics/test_input_schema_contracts.py`

**Key code references:**
- `validate_semantic_inputs()` in `semantics/input_registry.py`
- `validate_semantic_input_columns()` in `semantics/validation.py`
- `require_semantic_inputs()` in `semantics/input_registry.py`

**Tests:**

| Test | Description |
|------|-------------|
| `test_partial_table_presence_reports_missing` | Register `cst_refs` but not `cst_defs`; verify validation reports `cst_defs` as missing. |
| `test_wrong_column_types_detected` | Register `cst_refs` with `bstart` as string instead of int64; verify column validation fails. |
| `test_missing_required_columns_detected` | Register `cst_refs` without `bstart` column; verify column validation fails with clear error. |
| `test_legacy_suffixed_names_not_resolved` | Register `cst_refs_v1`; verify it is NOT resolved as `cst_refs`. |
| `test_all_semantic_inputs_valid_with_correct_schemas` | Register all required tables with correct schemas; verify full validation passes. |

---

#### Suite 3.10: Semantic Compiler Type Validation

**Boundary:** `SemanticCompiler` join operations -> DataFusion type system -> UDF availability

**File:** `tests/integration/semantics/test_compiler_type_validation.py`

**Key code references:**
- `SemanticCompiler` in `semantics/compiler.py:193`
- `compile_relationship()` in `semantics/compiler.py`
- `infer_join_strategy()` in `semantics/joins/inference.py`
- `validate_required_udfs()` in `datafusion_engine/udf/runtime.py`

**Tests:**

| Test | Description |
|------|-------------|
| `test_join_key_type_mismatch_raises` | Left table has `file_id` as string, right table has `file_id` as int; verify `SemanticSchemaError` raised. |
| `test_relationship_compilation_with_span_overlap` | Compile a SPAN_OVERLAP relationship between two tables with proper span columns; verify DataFusion plan is valid. |
| `test_relationship_compilation_with_missing_column` | Attempt relationship compilation when right table is missing a join key; verify clear error. |
| `test_udf_availability_validation` | Verify `validate_required_udfs()` succeeds when Rust UDFs are loaded and fails with clear error when they are not. |
| `test_relationship_failure_does_not_abort_pipeline` | When one relationship compilation fails, verify the semantic pipeline records a diagnostic and continues with remaining relationships. |

---

### Phase 5: Storage Cross-Boundary Contracts (G5)

#### Suite 3.11: Idempotent Write Contracts

**Boundary:** `IdempotentWriteOptions` -> `idempotent_commit_properties()` -> Delta commit deduplication

**File:** `tests/integration/storage/test_idempotent_write_contracts.py`

**Key code references:**
- `IdempotentWriteOptions` in `storage/deltalake/delta.py:499`
- `idempotent_commit_properties()` in `storage/deltalake/delta.py:3281`

**Tests:**

| Test | Description |
|------|-------------|
| `test_idempotent_write_deduplication` | Write Arrow table with `app_id=run_1, version=1`; write again with same options; verify Delta table has only one version increment. |
| `test_idempotent_write_different_versions_both_commit` | Write with `version=1` then `version=2`; verify both commits are recorded. |
| `test_commit_metadata_normalization` | Pass `extra_metadata` with reserved keys; verify `codeanatomy_` prefix escaping works correctly. |
| `test_idempotent_write_with_schema_evolution` | Write table with columns A,B; write again with columns A,B,C and schema evolution enabled; verify schema evolves. |

---

#### Suite 3.12: CDF Cursor Lifecycle Integration

**Boundary:** `CdfCursorStore` -> `DeltaCdfOptions` -> `read_delta_cdf()` -> cursor update

**File:** `tests/integration/storage/test_cdf_cursor_lifecycle.py`

**Key code references:**
- `CdfCursorStore` in `semantics/incremental/cdf_cursors.py:96`
- `DeltaCdfOptions` in `storage/deltalake/delta.py`

**Tests:**

| Test | Description |
|------|-------------|
| `test_cursor_tracks_incremental_reads` | Create cursor at version=5; call `get_start_version()` -> 6; update to version=10; next `get_start_version()` -> 11. |
| `test_cursor_corruption_recovery` | Write invalid JSON to cursor file; verify `load_cursor()` returns None; call `update_version()` to overwrite with valid cursor. |
| `test_concurrent_cursor_updates` | Two threads call `update_version()` simultaneously; verify final state is valid (last write wins). |
| `test_cursor_file_path_sanitization` | Use dataset name with `/` and `\` characters; verify file paths are sanitized correctly. |
| `test_cursor_list_skips_invalid_files` | Place both valid and invalid cursor files in directory; verify `list_cursors()` returns only valid cursors. |

---

#### Suite 3.13: Dataset Resolution and Provider Contracts

**Boundary:** `register_dataset()` -> `resolve_dataset_provider()` -> `DeltaService` operations

**File:** `tests/integration/storage/test_dataset_resolution_contracts.py`

**Key code references:**
- `resolve_dataset_provider()` in `datafusion_engine/dataset/resolution.py:74`
- `DatasetResolution` in `datafusion_engine/dataset/resolution.py:56`
- `DeltaService` in `datafusion_engine/delta/service.py`

**Tests:**

| Test | Description |
|------|-------------|
| `test_register_then_resolve_roundtrip` | Register a Delta table, resolve provider, verify `DatasetResolution` contains valid provider and metadata. |
| `test_resolution_captures_delta_snapshot` | Resolve Delta provider; verify `delta_snapshot` includes protocol version and snapshot key. |
| `test_invalid_predicate_error_recorded` | Pass invalid SQL predicate to resolution request; verify `predicate_error` field is populated. |
| `test_storage_options_merge_request_overrides_policy` | Set conflicting storage options in policy and request; verify request value wins in merged result. |
| `test_provider_artifacts_include_scan_metadata` | Resolve dataset; verify provider artifacts include `delta_scan_identity_hash` and `snapshot_key`. |

---

### Phase 6: End-to-End Cross-System Validation

#### Suite 3.14: Extraction to CPG Output Chain

**Boundary:** Full pipeline from repo scan through CPG node/edge emission

**File:** `tests/integration/e2e/test_extraction_to_cpg.py`

**Key code references:**
- `build_graph_product()` in `graph/product_build.py:131`
- `build_cpg()` in `semantics/pipeline.py`
- Canonical schemas: `cpg_nodes`, `cpg_edges`

**Tests:**

| Test | Description |
|------|-------------|
| `test_single_file_repo_produces_cpg_nodes` | Build graph product for a repo with one Python file; verify `cpg_nodes` output has rows with valid `entity_id`, `path`, `bstart`, `bend`. |
| `test_single_file_repo_produces_cpg_edges` | Build graph product; verify `cpg_edges` output has rows with valid `entity_id`, `symbol`, `origin`. |
| `test_entity_ids_are_deterministic` | Build graph product twice; verify `entity_id` values are identical across runs. |
| `test_byte_spans_are_within_file_bounds` | Build graph product; verify all `bstart` >= 0 and `bend` <= file size for every CPG node. |
| `test_graceful_degradation_with_empty_file` | Include an empty Python file in repo; verify pipeline completes and produces correct-schema empty output for that file. |

**Marker:** `@pytest.mark.e2e` (these are slow tests requiring full pipeline execution)

---

#### Suite 3.15: Incremental Build Validation

**Boundary:** Full incremental build cycle: initial build -> file change -> re-build with CDF cursors

**File:** `tests/integration/e2e/test_incremental_build.py`

**Key code references:**
- `CdfCursorStore` in `semantics/incremental/cdf_cursors.py`
- `SemanticRuntimeConfig.cdf_enabled` in `semantics/runtime.py`
- `impacted_tasks()` in `relspec/rustworkx_schedule.py:134`

**Tests:**

| Test | Description |
|------|-------------|
| `test_incremental_build_only_processes_changed_files` | Initial build on 3-file repo; modify one file; incremental build processes only the changed file's evidence. |
| `test_incremental_build_cursor_advances` | After incremental build, verify CDF cursor version advanced to new Delta version. |
| `test_incremental_build_determinism` | Run same incremental build twice (no changes between); verify no new processing occurs. |
| `test_incremental_build_with_new_file_added` | Add a new file to repo; verify incremental build processes only the new file. |

**Marker:** `@pytest.mark.e2e`

---

## 4. Test Infrastructure Requirements

### 4.1 New Fixtures

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `minimal_python_repo` | function | `tmp_path` directory containing 1-3 Python files for extraction tests |
| `multi_file_python_repo` | function | `tmp_path` with 5+ files including edge cases (empty, Unicode, large) |
| `datafusion_session_with_extracts` | function | DataFusion session pre-loaded with extraction output tables |
| `realistic_inferred_deps` | function | `InferredDeps` tuple built from actual DataFusion plan lineage |
| `delta_table_with_cdf` | function | Delta table with CDF enabled and multiple versions |

### 4.2 Test Helpers

| Helper | Purpose |
|--------|---------|
| `register_arrow_table()` | Already exists in `tests/test_helpers/arrow_seed.py` |
| `df_profile()` | Already exists in `tests/test_helpers/datafusion_runtime.py` |
| `assert_immutable_assignment()` | Already exists in `tests/test_helpers/immutability.py` |
| `run_delta_smoke_round_trip()` | Already exists in `tests/harness/delta_smoke.py` |
| `build_minimal_extraction()` | NEW: Run extraction on minimal repo, return Arrow tables |
| `build_minimal_semantic_session()` | NEW: DataFusion session with extraction + normalization applied |

### 4.3 Directory Structure

```
tests/integration/
├── adapters/                   # (existing)
├── boundaries/                 # (existing)
├── contracts/                  # (existing)
├── error_handling/             # (existing)
├── runtime/                    # (existing)
├── scheduling/                 # (existing)
├── pipeline/                   # NEW: Phase 1
│   ├── test_execution_chain.py
│   └── test_materializer_integration.py
├── relspec/                    # NEW: Phase 2
│   ├── test_execution_plan_compilation.py
│   ├── test_schedule_edge_validation.py
│   └── test_cost_context_integration.py
├── extraction/                 # NEW: Phase 3
│   ├── test_byte_span_canonicalization.py
│   ├── test_file_identity_coordination.py
│   └── test_evidence_plan_materialization.py
├── semantics/                  # NEW: Phase 4
│   ├── test_input_schema_contracts.py
│   └── test_compiler_type_validation.py
├── storage/                    # NEW: Phase 5
│   ├── test_idempotent_write_contracts.py
│   ├── test_cdf_cursor_lifecycle.py
│   └── test_dataset_resolution_contracts.py
└── e2e/                        # NEW: Phase 6
    ├── test_extraction_to_cpg.py
    └── test_incremental_build.py
```

---

## 5. Implementation Priority

### 5.1 Phase Ordering

| Phase | Suite Count | Risk Addressed | Effort | Priority |
|-------|------------|----------------|--------|----------|
| Phase 3: Multi-Source Extraction | 3 suites | G4, G6 (byte-span correctness) | Medium | P0 |
| Phase 2: Scheduling and Plans | 3 suites | G3 (execution plan integrity) | Medium | P0 |
| Phase 1: Pipeline Orchestration | 2 suites | G1, G2 (pipeline reliability) | High | P1 |
| Phase 4: Semantic Validation | 2 suites | G6 (schema contract enforcement) | Low | P1 |
| Phase 5: Storage Contracts | 3 suites | G5 (storage reliability) | Medium | P2 |
| Phase 6: End-to-End | 2 suites | All gaps (system validation) | High | P2 |

### 5.2 Rationale

**P0 - Extraction and Scheduling** are prioritized because:
- Byte-span canonicalization is the foundational invariant; incorrect spans cascade to all downstream CPG outputs.
- Execution plan compilation is the scheduling authority; incorrect plans produce wrong or incomplete builds.

**P1 - Pipeline and Semantics** follow because:
- Pipeline orchestration errors are observable (crashes/diagnostics) and partially covered by existing smoke tests.
- Semantic schema validation has some coverage via `test_semantic_pipeline.py`.

**P2 - Storage and E2E** are lower priority because:
- Storage operations have unit test coverage and the `test_df_delta_smoke.py` smoke test.
- E2E tests are slow and should be added after the component-level integration gaps are closed.

---

## 6. Markers and CI Integration

All new tests should use:

```python
@pytest.mark.integration  # Phases 1-5
@pytest.mark.e2e          # Phase 6 only
```

Phase 6 tests should be excluded from default CI runs via `-m "not e2e"` and run in a dedicated nightly or pre-release job.

---

## 7. Success Criteria

The expanded integration test suite is considered complete when:

1. All 15 suites are implemented with passing tests.
2. No cross-system boundary identified in this document lacks at least one integration test.
3. The byte-span canonicalization invariant is explicitly validated across all extraction sources.
4. End-to-end build produces CPG outputs on a minimal repo with deterministic `entity_id` values.
5. Incremental build processes only changed files and advances CDF cursors correctly.
6. All new tests run in CI with `@pytest.mark.integration` and complete within the medium-speed tier.
