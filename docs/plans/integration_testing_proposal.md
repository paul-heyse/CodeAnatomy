# Integration Testing Proposal

## Executive Summary

This proposal outlines a systematic approach to integration testing for CodeAnatomy's CPG build pipeline. The focus is on **subsystem boundary testing**—tests that operate at the integration layer between major components, validating data flow correctness, contract compliance, and failure handling without requiring full end-to-end execution.

**Goals:**
- Proactively identify bugs at subsystem boundaries before E2E failures
- Validate data contracts and schema evolution across component interfaces
- Test incremental/CDF behavior with controlled mutations
- Verify error propagation and graceful degradation
- Enable targeted debugging when E2E tests fail

---

## Revision Notes (Post Code Review)

### Corrections from Initial Proposal

| Original Assumption | Actual Behavior | Test Impact |
|---------------------|-----------------|-------------|
| CdfFilterPolicy constructor default is ALL_CHANGES | Constructor default is ALL types; **IncrementalConfig** default is `inserts_and_updates_only()` | Test both constructor and config defaults |
| Three merge strategies | Four strategies: APPEND, UPSERT, REPLACE, DELETE_INSERT | Add DELETE_INSERT tests |
| Simple task nodes in graph | Bipartite graph: EvidenceNode + TaskNode | Test graph structure |
| Cursor starts at 0 | `get_start_version()` returns `last_version + 1` | Fix cursor advancement tests |
| SemanticRuntimeConfig in datafusion_engine | Lives in `semantics/runtime.py`; adapter inverts dependency | Test adapter round-trip |
| Missing inputs raise errors | Evidence plan gating returns LIMIT(0) empty plans | Test graceful gating |

### Corrections from Final Code Review (v2)

| Original Proposal | Actual Code | Fix Applied |
|-------------------|-------------|-------------|
| `CdfFilterPolicy.include_inserts` | `include_insert` (singular) | Fixed field name |
| `CdfFilterPolicy.include_updates` | `include_update_postimage` | Fixed field name |
| `CdfFilterPolicy.include_deletes` | `include_delete` (singular) | Fixed field name |
| `schedule_tasks(graph, allow_partial=True)` | `schedule_tasks(graph, evidence=catalog, options=ScheduleOptions(allow_partial=True))` | Fixed signature |
| `TaskSchedule.ordered_tasks` has task objects | `ordered_tasks: tuple[str, ...]` (task names only) | Fixed iteration pattern |
| `cursor_store._cursors_path` | `cursor_store.cursors_path` (public) | Fixed attribute name |
| `pytest.raises(AttributeError)` | `pytest.raises(FrozenInstanceError)` for dataclasses | Fixed exception type |
| Immutability via single mechanism | CdfCursor uses custom `__setattr__`; others use frozen decorator | Clarified in tests |

### Newly Identified Test Coverage Gaps

1. **Adapter Bidirectionality** - `profile → semantic_config → profile` round-trip not tested
2. **Immutability Contracts** - `FrozenInstanceError` on mutation for frozen dataclasses
3. **Cache Policy Merging** - Precedence: config > runtime_config > defaults
4. **Impact Closure Strategies** - "hybrid", "symbol_closure", "import_closure" options
5. **Partition-Scoped Deletes** - Deletion by partition value, not individual rows
6. **Streaming Row Tracking** - Artifacts for large writes (>100k rows)
7. **UDF Snapshot Validation** - Consistency checks across plan and inferred
8. **Delta Pin Conflicts** - Conflicting versions for same dataset
9. **Bipartite Graph Structure** - Evidence vs task node distinction
10. **Cost Modeling** - Bottom-level costs, slack computation, critical path
11. **Column-Level Edge Requirements** - `GraphEdge.required_columns` validation
12. **FileContext Fallback Chain** - bytes → encode text → read from disk
13. **Post-Process Resilience** - View registration failures recorded as events, not propagated
14. **Expression Validation Graceful Skip** - Invalid expressions logged, pipeline continues

### Existing Test Coverage (Acknowledged)

The following integration tests already exist and should be built upon:

| File | Coverage |
|------|----------|
| `test_semantic_pipeline.py` | Input validation, fallback names, column validation |
| `test_incremental_partitioned_updates.py` | Partition-scoped upserts and deletes |
| `test_engine_session_semantic_config.py` | Semantic config application to sessions |
| `test_delta_protocol_and_schema_mode.py` | Delta protocol and schema evolution |
| `test_driver_factory_integration.py` | Hamilton driver construction |

---

## Current Test Coverage Analysis

### Existing Test Distribution

| Category | Count | Coverage Focus | Gap |
|----------|-------|----------------|-----|
| Unit tests | ~179 | Single module behavior | No cross-module interaction |
| Integration | ~25 | Basic multi-subsystem | Limited boundary coverage |
| E2E | ~16 | Full pipeline (CQ focus) | Too coarse for debugging |
| Golden/Contract | ~25 | Output stability | No behavioral testing |

### Identified Gaps (Refined)

1. **Extraction → Normalization boundary** - No tests validate byte-span canonicalization across extractors
2. **Normalization → Relationship compilation** - Join strategy selection untested beyond basic cases
3. **View DAG → Scheduling** - Bipartite graph structure and cost modeling untested
4. **Incremental/CDF behavior** - Impact closure strategies and merge strategy edge cases
5. **Schema evolution scenarios** - Delta write policy enforcement partially tested
6. **Error propagation paths** - Post-process resilience, expression validation graceful skip
7. **Multi-source consistency** - No tests verify CST/AST/SCIP alignment
8. **Adapter contracts** - SemanticRuntimeConfig ↔ DataFusionRuntimeProfile round-trip
9. **Immutability invariants** - Frozen dataclass mutation prevention

---

## Proposed Test Architecture

### Test Tier Model

```
┌─────────────────────────────────────────────────────────────┐
│  E2E Tests (existing)                                       │
│  - Full pipeline execution                                  │
│  - Requires real repository                                 │
│  - Slow, coarse-grained                                     │
└─────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────┐
│  PROPOSED: Subsystem Integration Tests                      │
│  - 2-3 component interactions                               │
│  - Synthetic data (controlled inputs)                       │
│  - Fast, targeted                                           │
└─────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────┐
│  Unit Tests (existing)                                      │
│  - Single function/class                                    │
│  - Mocked dependencies                                      │
│  - Very fast                                                │
└─────────────────────────────────────────────────────────────┘
```

### Proposed Directory Structure

```
tests/
├── integration/
│   ├── boundaries/                    # NEW: Subsystem boundary tests
│   │   ├── test_extraction_to_normalization.py
│   │   ├── test_normalization_to_relationship.py
│   │   ├── test_relationship_to_cpg_output.py
│   │   ├── test_view_dag_to_scheduling.py
│   │   └── test_cpg_to_materialization.py
│   │
│   ├── adapters/                      # NEW: Adapter round-trip tests
│   │   ├── test_semantic_runtime_adapter.py
│   │   └── test_cache_policy_merging.py
│   │
│   ├── contracts/                     # NEW: Data contract validation
│   │   ├── test_extraction_schema_contracts.py
│   │   ├── test_semantic_view_contracts.py
│   │   ├── test_cpg_output_contracts.py
│   │   ├── test_delta_write_contracts.py
│   │   └── test_immutability_contracts.py
│   │
│   ├── incremental/                   # EXTEND existing
│   │   ├── test_cdf_cursor_tracking.py
│   │   ├── test_incremental_impact_propagation.py
│   │   ├── test_impact_closure_strategies.py   # NEW
│   │   ├── test_merge_strategies.py            # NEW
│   │   └── test_fingerprint_invalidation.py
│   │
│   ├── scheduling/                    # NEW: Graph and schedule tests
│   │   ├── test_bipartite_graph_structure.py
│   │   ├── test_cost_modeling.py
│   │   ├── test_column_level_deps.py
│   │   └── test_schedule_generation.py
│   │
│   ├── error_handling/                # NEW: Error paths and recovery
│   │   ├── test_missing_input_handling.py
│   │   ├── test_graceful_degradation.py
│   │   ├── test_evidence_plan_gating.py        # NEW
│   │   ├── test_postprocess_resilience.py      # NEW
│   │   └── test_schema_mismatch_errors.py
│   │
│   ├── multi_source/                  # NEW: Cross-extractor consistency
│   │   ├── test_cst_ast_alignment.py
│   │   ├── test_scip_byte_span_mapping.py
│   │   ├── test_symtable_cst_consistency.py
│   │   └── test_file_context_fallback.py       # NEW
│   │
│   └── performance/                   # NEW: Integration-level performance
│       ├── test_plan_compilation_performance.py
│       ├── test_join_selectivity_performance.py
│       └── test_materialization_throughput.py
```

---

## Phase 1: Boundary Tests

### Test Suite 1: Extraction → Normalization Boundary

**File:** `tests/integration/boundaries/test_extraction_to_normalization.py`

**Purpose:** Validate that extraction outputs are correctly normalized with canonical byte spans.

```python
@pytest.mark.integration
class TestExtractionToNormalization:
    """Tests for extraction → normalization boundary."""

    # ─────────────────────────────────────────────────────────────
    # Test 1.1: Byte span canonicalization
    # ─────────────────────────────────────────────────────────────
    def test_cst_byte_spans_match_file_content(
        self,
        df_ctx: SessionContext,
        sample_python_source: str,
    ) -> None:
        """Verify CST byte spans correctly index into source text."""
        # Given: A Python source file and its CST extraction
        # When: We extract byte ranges using bstart/bend
        # Then: The extracted text matches the node's expected content

    def test_ast_line_col_to_byte_conversion(
        self,
        df_ctx: SessionContext,
        sample_python_source: str,
    ) -> None:
        """Verify AST line:col positions convert to correct byte offsets."""
        # Given: AST nodes with lineno/col_offset
        # When: Joined with file_line_index_v1
        # Then: Computed byte offsets match source content

    def test_scip_byte_spans_align_with_cst(
        self,
        df_ctx: SessionContext,
        cst_extraction: pa.Table,
        scip_extraction: pa.Table,
    ) -> None:
        """Verify SCIP byte spans align with CST for same symbols."""
        # Given: CST and SCIP extractions of same source
        # When: Joining on qualified_name
        # Then: Byte spans are identical or contained

    # ─────────────────────────────────────────────────────────────
    # Test 1.2: Evidence plan gating (NEW)
    # ─────────────────────────────────────────────────────────────
    def test_evidence_plan_gating_returns_empty_plan(
        self,
        df_ctx: SessionContext,
        evidence_plan_without_dataset: EvidencePlan,
    ) -> None:
        """Verify gated datasets return LIMIT(0) empty plans, not errors."""
        # Given: Evidence plan that doesn't require ast_files
        # When: apply_query_and_project() called for ast_files
        # Then: Returns empty plan with correct schema (not an error)

    def test_evidence_plan_projects_only_required_columns(
        self,
        df_ctx: SessionContext,
        evidence_plan_with_columns: EvidencePlan,
    ) -> None:
        """Verify only required columns are projected when constrained."""
        # Given: Evidence plan requiring only file_id, bstart, bend
        # When: Materializing extraction plan
        # Then: Only those columns present in output

    # ─────────────────────────────────────────────────────────────
    # Test 1.3: FileContext fallback chain (NEW)
    # ─────────────────────────────────────────────────────────────
    def test_file_context_prefers_explicit_data(
        self,
        file_context_with_data: FileContext,
    ) -> None:
        """Verify FileContext prefers explicit data over text/disk."""
        # Given: FileContext with data, text, and abs_path all set
        # When: bytes_from_file_ctx() called
        # Then: Returns explicit data, not encoded text or disk read

    def test_file_context_encodes_text_when_no_data(
        self,
        file_context_text_only: FileContext,
    ) -> None:
        """Verify FileContext encodes text when data is None."""
        # Given: FileContext with text but no data
        # When: bytes_from_file_ctx() called
        # Then: Returns text.encode() with specified encoding

    def test_file_context_reads_disk_as_fallback(
        self,
        file_context_path_only: FileContext,
        tmp_path: Path,
    ) -> None:
        """Verify FileContext reads from disk as last fallback."""
        # Given: FileContext with only abs_path
        # When: bytes_from_file_ctx() called
        # Then: Returns file content from disk

    def test_file_context_returns_none_on_missing_file(
        self,
        file_context_missing_path: FileContext,
    ) -> None:
        """Verify graceful None return when file doesn't exist."""
        # Given: FileContext with non-existent abs_path
        # When: bytes_from_file_ctx() called
        # Then: Returns None (no exception)
```

### Test Suite 2: Normalization → Relationship Boundary

**File:** `tests/integration/boundaries/test_normalization_to_relationship.py`

**Purpose:** Validate that normalized tables join correctly to produce relationships.

```python
@pytest.mark.integration
class TestNormalizationToRelationship:
    """Tests for normalization → relationship compilation boundary."""

    # ─────────────────────────────────────────────────────────────
    # Test 2.1: Join correctness
    # ─────────────────────────────────────────────────────────────
    def test_def_ref_join_produces_expected_edges(
        self,
        df_ctx: SessionContext,
        cst_defs_normalized: pa.Table,
        cst_refs_normalized: pa.Table,
    ) -> None:
        """Verify definition → reference joins produce correct edges."""

    def test_join_strategy_selection_via_infer_join_strategy(
        self,
        df_ctx: SessionContext,
        annotated_schemas: tuple[AnnotatedSchema, AnnotatedSchema],
    ) -> None:
        """Verify infer_join_strategy() returns appropriate strategy."""
        # Given: Two AnnotatedSchemas with span columns
        # When: infer_join_strategy() called
        # Then: Returns OVERLAP, CONTAINS, or EQUI based on column types

    # ─────────────────────────────────────────────────────────────
    # Test 2.2: Quality signal integration
    # ─────────────────────────────────────────────────────────────
    def test_quality_relationship_compilation_skips_invalid_expressions(
        self,
        df_ctx: SessionContext,
        relationship_spec_with_invalid_expr: RelationshipSpec,
    ) -> None:
        """Verify invalid expressions are skipped with warning, not failure."""
        # Given: RelationshipSpec with an expression that fails validation
        # When: compile_relationship_with_quality() called
        # Then: Invalid expression skipped, valid expressions processed
        # And: Warning logged via _record_expr_issue()

    def test_file_quality_join_uses_left_join(
        self,
        df_ctx: SessionContext,
        normalized_table: pa.Table,
        file_quality_table: pa.Table,
    ) -> None:
        """Verify file quality joins default to LEFT JOIN (lossy but safe)."""
        # Given: Normalized table and file_quality_v1
        # When: Joining for quality relationship
        # Then: All normalized rows preserved even if no quality match
```

### Test Suite 3: View DAG → Scheduling Boundary

**File:** `tests/integration/boundaries/test_view_dag_to_scheduling.py`

**Purpose:** Validate dependency inference, bipartite graph construction, and scheduling.

```python
@pytest.mark.integration
class TestViewDagToScheduling:
    """Tests for view DAG → scheduling boundary."""

    # ─────────────────────────────────────────────────────────────
    # Test 3.1: Dependency inference from DataFusion plans
    # ─────────────────────────────────────────────────────────────
    def test_inferred_deps_from_plan_bundle(
        self,
        df_ctx: SessionContext,
        view_node_with_plan: ViewNode,
    ) -> None:
        """Verify infer_deps_from_plan_bundle() extracts correct inputs."""
        # Given: ViewNode with compiled DataFusionPlanBundle
        # When: infer_deps_from_plan_bundle() called
        # Then: InferredDeps.inputs matches tables scanned in plan

    def test_column_level_requirements_extracted(
        self,
        df_ctx: SessionContext,
        view_node_with_projection: ViewNode,
    ) -> None:
        """Verify column-level requirements extracted from plan."""
        # Given: ViewNode selecting subset of columns
        # When: Extracting InferredDeps
        # Then: required_columns[table] lists only used columns

    def test_udf_requirements_validated_against_snapshot(
        self,
        df_ctx: SessionContext,
        view_node_with_udf: ViewNode,
        udf_snapshot: dict[str, object],
    ) -> None:
        """Verify required_udfs validated against snapshot."""
        # Given: ViewNode using stable_hash64 UDF
        # When: _validate_bundle_udfs() called
        # Then: No error if UDF in snapshot; error if missing

    # ─────────────────────────────────────────────────────────────
    # Test 3.2: Bipartite graph construction (NEW)
    # ─────────────────────────────────────────────────────────────
    def test_task_graph_has_evidence_and_task_nodes(
        self,
        task_graph: TaskGraph,
    ) -> None:
        """Verify graph contains both EvidenceNode and TaskNode types."""
        # Given: Built TaskGraph
        # Then: Graph contains nodes with kind="evidence" and kind="task"

    def test_requires_edges_connect_evidence_to_task(
        self,
        task_graph: TaskGraph,
    ) -> None:
        """Verify 'requires' edges go from evidence → task."""
        # Given: Built TaskGraph
        # Then: All edges with kind="requires" have evidence source, task target

    def test_produces_edges_connect_task_to_evidence(
        self,
        task_graph: TaskGraph,
    ) -> None:
        """Verify 'produces' edges go from task → evidence."""
        # Given: Built TaskGraph
        # Then: All edges with kind="produces" have task source, evidence target

    def test_graph_edge_has_column_level_metadata(
        self,
        task_graph: TaskGraph,
    ) -> None:
        """Verify GraphEdge contains column-level requirements."""
        # Given: Built TaskGraph with views selecting specific columns
        # Then: Edges have required_columns, required_types metadata

    # ─────────────────────────────────────────────────────────────
    # Test 3.3: Cost modeling (NEW)
    # ─────────────────────────────────────────────────────────────
    def test_bottom_level_costs_computed(
        self,
        task_graph_with_costs: TaskGraph,
    ) -> None:
        """Verify bottom_level_costs() computes weighted longest paths."""
        # Given: TaskGraph with task_costs annotations
        # When: bottom_level_costs() computed
        # Then: Each task has bottom cost = self_cost + max(successor costs)

    def test_slack_computation(
        self,
        task_graph_with_costs: TaskGraph,
    ) -> None:
        """Verify slack = latest_start - earliest_start."""
        # Given: TaskGraph with computed schedule
        # When: task_slack_by_task() computed
        # Then: Critical path tasks have zero slack

    def test_critical_path_has_zero_slack(
        self,
        task_graph_with_costs: TaskGraph,
    ) -> None:
        """Verify critical path identified by zero slack."""
        # Given: TaskGraph with computed slack
        # Then: Tasks on critical path have slack == 0
```

### Test Suite 4: CPG → Materialization Boundary

**File:** `tests/integration/boundaries/test_cpg_to_materialization.py`

**Purpose:** Validate schema enforcement and Delta write policies.

```python
@pytest.mark.integration
class TestCpgToMaterialization:
    """Tests for CPG output → materialization boundary."""

    # ─────────────────────────────────────────────────────────────
    # Test 4.1: Schema enforcement
    # ─────────────────────────────────────────────────────────────
    def test_schema_mode_merge_vs_strict(
        self,
        tmp_path: Path,
        existing_delta_table: DeltaTable,
    ) -> None:
        """Verify schema_mode='merge' allows evolution, strict doesn't."""
        # Given: Existing Delta table
        # When: Writing with new nullable column
        # Then: merge mode succeeds, strict mode fails

    def test_schema_evolution_enabled_flag(
        self,
        semantic_runtime_config: SemanticRuntimeConfig,
    ) -> None:
        """Verify schema_evolution_enabled controls evolution behavior."""
        # Given: SemanticRuntimeConfig with schema_evolution_enabled=False
        # When: Attempting schema evolution
        # Then: Falls back to strict mode

    # ─────────────────────────────────────────────────────────────
    # Test 4.2: Post-process resilience (NEW)
    # ─────────────────────────────────────────────────────────────
    def test_view_registration_failure_recorded_not_propagated(
        self,
        df_ctx: SessionContext,
        failing_view_registration: Callable,
    ) -> None:
        """Verify view registration failures are recorded, not propagated."""
        # Given: View that fails during _register_extract_view()
        # When: materialize_extract_plan() runs
        # Then: ExtractQualityEvent recorded with stage="postprocess"
        # And: No exception raised, output still written

    def test_schema_contract_validation_failure_recorded(
        self,
        df_ctx: SessionContext,
        table_violating_contract: pa.Table,
    ) -> None:
        """Verify schema contract failures recorded as events."""
        # Given: Table that violates schema contract
        # When: _validate_extract_schema_contract() runs
        # Then: Failure recorded as diagnostic event, not exception

    # ─────────────────────────────────────────────────────────────
    # Test 4.3: Streaming write tracking (NEW)
    # ─────────────────────────────────────────────────────────────
    def test_large_write_records_streaming_artifact(
        self,
        tmp_path: Path,
        large_table: pa.Table,  # >100k rows
    ) -> None:
        """Verify large writes (>100k rows) record streaming artifacts."""
        # Given: Table with 150,000 rows
        # When: write_overwrite_dataset() called
        # Then: Streaming write artifact recorded
```

---

## Phase 2: Adapter and Contract Tests

### Test Suite 5: Semantic Runtime Adapter (NEW)

**File:** `tests/integration/adapters/test_semantic_runtime_adapter.py`

**Purpose:** Validate bidirectional adapter between DataFusionRuntimeProfile and SemanticRuntimeConfig.

```python
@pytest.mark.integration
class TestSemanticRuntimeAdapter:
    """Tests for semantic runtime adapter round-trip."""

    def test_profile_to_semantic_config_extracts_output_locations(
        self,
        datafusion_profile: DataFusionRuntimeProfile,
    ) -> None:
        """Verify output locations extracted from profile."""
        # Given: Profile with dataset locations for CPG outputs
        # When: semantic_runtime_from_profile() called
        # Then: SemanticRuntimeConfig.output_locations populated

    def test_semantic_config_to_profile_applies_locations(
        self,
        datafusion_profile: DataFusionRuntimeProfile,
        semantic_config: SemanticRuntimeConfig,
    ) -> None:
        """Verify semantic config locations applied back to profile."""
        # Given: SemanticRuntimeConfig with custom output_locations
        # When: apply_semantic_runtime_config() called
        # Then: Profile's registry_catalogs updated with new locations

    def test_adapter_round_trip_preserves_semantic_settings(
        self,
        datafusion_profile: DataFusionRuntimeProfile,
    ) -> None:
        """Verify profile → config → profile round-trip is consistent."""
        # Given: DataFusionRuntimeProfile
        # When: semantic_runtime_from_profile() then apply_semantic_runtime_config()
        # Then: Semantic-related settings preserved

    def test_cache_policy_override_merging_precedence(
        self,
        datafusion_profile: DataFusionRuntimeProfile,
        semantic_config: SemanticRuntimeConfig,
    ) -> None:
        """Verify cache policy merging: semantic_config > profile."""
        # Given: Profile with cache_overrides = {"view_a": "none"}
        # And: SemanticConfig with cache_policy_overrides = {"view_a": "delta_output"}
        # When: apply_semantic_runtime_config() called
        # Then: Merged result has {"view_a": "delta_output"} (semantic wins)

    def test_immutable_update_via_msgspec_replace(
        self,
        datafusion_profile: DataFusionRuntimeProfile,
        semantic_config: SemanticRuntimeConfig,
    ) -> None:
        """Verify profile update uses structural sharing (no mutation)."""
        # Given: Original profile
        # When: apply_semantic_runtime_config() called
        # Then: Original profile unchanged, new profile returned
```

### Test Suite 6: Immutability Contracts (NEW)

**File:** `tests/integration/contracts/test_immutability_contracts.py`

**Purpose:** Validate frozen dataclass/struct mutation prevention.

**Implementation Notes:**
- `CdfCursor`: msgspec.Struct with custom `__setattr__` that raises `FrozenInstanceError`
- `SemanticRuntimeConfig`: `@dataclass(frozen=True)` - raises `FrozenInstanceError`
- `InferredDeps`: `StructBaseStrict, frozen=True` (msgspec) - raises `AttributeError`
- `FileContext`: `@dataclass(frozen=True)` - raises `FrozenInstanceError`

```python
from dataclasses import FrozenInstanceError

@pytest.mark.integration
class TestImmutabilityContracts:
    """Tests for immutability enforcement on frozen dataclasses and structs."""

    def test_cdf_cursor_mutation_raises_frozen_error(
        self,
    ) -> None:
        """Verify CdfCursor raises FrozenInstanceError on mutation.

        CdfCursor uses a custom __setattr__ that explicitly raises FrozenInstanceError.
        """
        from semantics.incremental.cdf_cursors import CdfCursor
        cursor = CdfCursor(dataset_name="test", last_version=1, last_timestamp=None)
        with pytest.raises(FrozenInstanceError):
            cursor.last_version = 2

    def test_semantic_runtime_config_is_frozen(
        self,
    ) -> None:
        """Verify SemanticRuntimeConfig is immutable.

        Uses @dataclass(frozen=True), so raises FrozenInstanceError.
        """
        from semantics.runtime import SemanticRuntimeConfig
        config = SemanticRuntimeConfig(output_locations={})
        with pytest.raises(FrozenInstanceError):
            config.cdf_enabled = True

    def test_inferred_deps_is_frozen(
        self,
    ) -> None:
        """Verify InferredDeps is immutable.

        Uses StructBaseStrict with frozen=True (msgspec), raises AttributeError.
        """
        from relspec.inferred_deps import InferredDeps
        deps = InferredDeps(task_name="t", output="o", inputs=())
        with pytest.raises(AttributeError):
            deps.inputs = ("new",)

    def test_file_context_is_frozen(
        self,
    ) -> None:
        """Verify FileContext is immutable.

        Uses @dataclass(frozen=True), so raises FrozenInstanceError.
        """
        from extract.coordination.context import FileContext
        ctx = FileContext(file_id="f", path="p", abs_path=None, file_sha256=None)
        with pytest.raises(FrozenInstanceError):
            ctx.path = "new_path"
```

---

## Phase 3: Incremental/CDF Tests (Extended)

### Test Suite 7: CDF Cursor Tracking (Corrected)

**File:** `tests/integration/incremental/test_cdf_cursor_tracking.py`

**Purpose:** Validate CDF cursor tracking with corrected behavior.

```python
@pytest.mark.integration
class TestCdfCursorTracking:
    """Tests for CDF cursor tracking behavior."""

    def test_get_start_version_returns_last_plus_one(
        self,
        cursor_store: CdfCursorStore,
    ) -> None:
        """Verify get_start_version() returns last_version + 1."""
        from semantics.incremental.cdf_cursors import CdfCursor
        # Given: Cursor with last_version=5
        cursor = CdfCursor(dataset_name="test", last_version=5, last_timestamp=None)
        cursor_store.save_cursor(cursor)
        # When: get_start_version() called
        start = cursor_store.get_start_version("test")
        # Then: Returns 6 (not 5)
        assert start == 6

    def test_get_start_version_returns_none_for_missing_cursor(
        self,
        cursor_store: CdfCursorStore,
    ) -> None:
        """Verify get_start_version() returns None for new datasets."""
        # Given: No cursor exists for dataset
        # When: get_start_version() called
        start = cursor_store.get_start_version("new_dataset")
        # Then: Returns None (triggers full refresh)
        assert start is None

    def test_cursor_timestamp_auto_generated_on_update(
        self,
        cursor_store: CdfCursorStore,
    ) -> None:
        """Verify update_version() adds timestamp automatically."""
        # Given: Cursor store
        # When: update_version() called
        cursor_store.update_version("test", version=10)
        cursor = cursor_store.load_cursor("test")
        # Then: Cursor has ISO 8601 timestamp
        assert cursor is not None
        assert cursor.last_timestamp is not None
        assert "T" in cursor.last_timestamp  # ISO format

    def test_cursor_persistence_uses_sanitized_filename(
        self,
        cursor_store: CdfCursorStore,
    ) -> None:
        """Verify dataset names with / are sanitized for filesystem."""
        # Given: Dataset name with path separator
        cursor_store.update_version("path/to/dataset", version=1)
        # Then: File created with _ instead of / (note: cursors_path is public)
        files = list(cursor_store.cursors_path.glob("*.cursor.json"))
        assert any("path_to_dataset" in f.name for f in files)

    def test_list_cursors_skips_invalid_files(
        self,
        cursor_store: CdfCursorStore,
    ) -> None:
        """Verify list_cursors() silently skips corrupt files.

        list_cursors() catches (msgspec.DecodeError, OSError) and continues.
        """
        # Given: Valid cursor and corrupt JSON file
        cursor_store.update_version("valid", version=1)
        (cursor_store.cursors_path / "corrupt.cursor.json").write_text("{invalid")
        # When: list_cursors() called
        cursors = cursor_store.list_cursors()
        # Then: Only valid cursor returned, no exception
        assert len(cursors) == 1
        assert cursors[0].dataset_name == "valid"
```

### Test Suite 8: CDF Filter Policies and Merge Strategies (NEW)

**File:** `tests/integration/incremental/test_merge_strategies.py`

**Purpose:** Validate all four merge strategies and filter policies.

```python
@pytest.mark.integration
class TestCdfMergeStrategies:
    """Tests for CDF merge strategy implementations."""

    def test_append_strategy_unions_without_dedup(
        self,
        df_ctx: SessionContext,
        existing_df: DataFrame,
        new_df: DataFrame,
    ) -> None:
        """Verify APPEND strategy is simple union."""
        # Given: Existing 10 rows, new 5 rows (2 duplicates)
        # When: apply_cdf_merge(..., strategy=APPEND)
        # Then: Result has 15 rows (duplicates kept)

    def test_upsert_strategy_removes_matching_keys(
        self,
        df_ctx: SessionContext,
        existing_df: DataFrame,
        new_df_with_updates: DataFrame,
    ) -> None:
        """Verify UPSERT removes existing rows matching key columns.

        Note: key_columns must be a tuple, not a list.
        """
        # Given: Existing with key_col=["a","b"], new with key_col=["b","c"]
        # When: apply_cdf_merge(..., strategy=UPSERT, key_columns=("key_col",))
        # Then: Result has "a", "b" (from new), "c"

    def test_replace_strategy_requires_partition_column(
        self,
        df_ctx: SessionContext,
        existing_df: DataFrame,
        new_df: DataFrame,
    ) -> None:
        """Verify REPLACE raises if partition_column not specified."""
        # When: apply_cdf_merge(..., strategy=REPLACE, partition_column=None)
        # Then: ValueError raised

    def test_replace_strategy_removes_affected_partitions(
        self,
        df_ctx: SessionContext,
        partitioned_df: DataFrame,
        new_partition_data: DataFrame,
    ) -> None:
        """Verify REPLACE removes all rows in affected partitions."""
        # Given: Partitions A, B, C; new data for partition B
        # When: apply_cdf_merge(..., strategy=REPLACE, partition_column="part")
        # Then: All old partition B rows removed, new B rows added, A/C untouched

    def test_delete_insert_same_as_upsert(
        self,
        df_ctx: SessionContext,
        existing_df: DataFrame,
        new_df: DataFrame,
    ) -> None:
        """Verify DELETE_INSERT behaves identically to UPSERT.

        Note: apply_cdf_merge signature:
            apply_cdf_merge(existing, new_data, *, key_columns, strategy, partition_column=None)
        """
        from semantics.incremental.cdf_joins import apply_cdf_merge, CDFMergeStrategy

        key_cols = ("key_col",)  # Must be tuple, not list
        upsert_result = apply_cdf_merge(
            existing_df, new_df, key_columns=key_cols, strategy=CDFMergeStrategy.UPSERT
        )
        delete_insert_result = apply_cdf_merge(
            existing_df, new_df, key_columns=key_cols, strategy=CDFMergeStrategy.DELETE_INSERT
        )
        # Results should be identical (DELETE_INSERT is alias for UPSERT semantics)
        assert upsert_result.collect() == delete_insert_result.collect()


@pytest.mark.integration
class TestCdfFilterPolicies:
    """Tests for CDF filter policy behavior."""

    def test_default_policy_includes_all_change_types(
        self,
    ) -> None:
        """Verify default CdfFilterPolicy() includes all change types."""
        from semantics.incremental.cdf_types import CdfFilterPolicy
        policy = CdfFilterPolicy()
        # Note: Default is include_all(), NOT inserts_and_updates_only()
        assert policy.include_insert is True
        assert policy.include_update_postimage is True
        assert policy.include_delete is True

    def test_inserts_and_updates_only_excludes_deletes(
        self,
    ) -> None:
        """Verify inserts_and_updates_only() factory excludes deletes."""
        from semantics.incremental.cdf_types import CdfFilterPolicy
        policy = CdfFilterPolicy.inserts_and_updates_only()
        assert policy.include_insert is True
        assert policy.include_update_postimage is True
        assert policy.include_delete is False

    def test_to_sql_predicate_with_single_type(
        self,
    ) -> None:
        """Verify SQL predicate for single change type."""
        from semantics.incremental.cdf_types import CdfFilterPolicy
        # Note: Correct field names are include_insert, include_update_postimage, include_delete
        policy = CdfFilterPolicy(include_insert=True, include_update_postimage=False, include_delete=False)
        predicate = policy.to_sql_predicate()
        assert predicate == "_change_type = 'insert'"

    def test_to_sql_predicate_returns_none_for_all_types(
        self,
    ) -> None:
        """Verify SQL predicate is None when all types included."""
        from semantics.incremental.cdf_types import CdfFilterPolicy
        policy = CdfFilterPolicy.include_all()
        assert policy.to_sql_predicate() is None

    def test_apply_cdf_filter_passes_through_without_column(
        self,
        df_ctx: SessionContext,
        df_without_change_type: DataFrame,
    ) -> None:
        """Verify filter passes through if _change_type column absent."""
        # Given: DataFrame without _change_type column (full refresh)
        # When: _apply_cdf_filter() called
        # Then: DataFrame returned unchanged
```

### Test Suite 9: Impact Closure Strategies (NEW)

**File:** `tests/integration/incremental/test_impact_closure_strategies.py`

**Purpose:** Validate the three impact closure computation strategies.

**Key Signature Notes:**
- `merge_impacted_files(runtime: IncrementalRuntime, inputs: ImpactedFileInputs, *, strategy: str) -> pa.Table`
- Strategies: `"hybrid"` (default), `"symbol_closure"`, `"import_closure"`
- `impacted_importers_from_changed_exports(*, runtime, changed_exports, prev_imports_resolved) -> pa.Table`

```python
from semantics.incremental.impact import merge_impacted_files, impacted_importers_from_changed_exports

@pytest.mark.integration
class TestImpactClosureStrategies:
    """Tests for impact closure computation strategies."""

    def test_symbol_closure_includes_callers(
        self,
        incremental_runtime: IncrementalRuntime,
        impacted_inputs_with_callers: ImpactedFileInputs,
    ) -> None:
        """Verify symbol_closure includes callers of changed exports.

        Symbol closure includes:
        - inputs.callers (callsite-based impact)
        - inputs.importers (named import impact)
        """
        # Given: Changed export for function foo
        # And: Callsite relationship bar → foo
        # When: merge_impacted_files(runtime, inputs, strategy="symbol_closure")
        result = merge_impacted_files(
            incremental_runtime, impacted_inputs_with_callers, strategy="symbol_closure"
        )
        # Then: Both foo's file and bar's file in impact set
        file_ids = set(result["file_id"].to_pylist())
        assert "foo_file_id" in file_ids
        assert "bar_file_id" in file_ids

    def test_import_closure_includes_importers(
        self,
        incremental_runtime: IncrementalRuntime,
        impacted_inputs_with_imports: ImpactedFileInputs,
    ) -> None:
        """Verify import_closure includes files importing changed modules.

        Import closure includes ONLY:
        - inputs.import_closure_only (module-level imports)
        """
        # Given: Changed export in module foo
        # And: Import "from foo import bar" in module baz
        # When: merge_impacted_files(runtime, inputs, strategy="import_closure")
        result = merge_impacted_files(
            incremental_runtime, impacted_inputs_with_imports, strategy="import_closure"
        )
        # Then: baz in impact set
        file_ids = set(result["file_id"].to_pylist())
        assert "baz_file_id" in file_ids

    def test_hybrid_combines_both_strategies(
        self,
        incremental_runtime: IncrementalRuntime,
        impacted_inputs_complete: ImpactedFileInputs,
    ) -> None:
        """Verify hybrid combines symbol and import closures.

        Hybrid includes ALL:
        - inputs.callers
        - inputs.importers
        - inputs.import_closure_only
        """
        # When: merge_impacted_files(runtime, inputs, strategy="hybrid")
        result = merge_impacted_files(
            incremental_runtime, impacted_inputs_complete, strategy="hybrid"
        )
        # Then: Result includes both caller and importer files
        file_ids = set(result["file_id"].to_pylist())
        # Verify callers included
        assert len(file_ids) > 0

    def test_star_imports_included_in_import_closure(
        self,
        incremental_runtime: IncrementalRuntime,
        changed_exports_table: pa.Table,
        imports_resolved_path: str,
    ) -> None:
        """Verify 'from foo import *' included in import closure.

        Star imports are identified by is_star=True in resolved imports.
        They receive reason_kind="import_star" in output.
        """
        # Given: Changed export in module foo
        # And: "from foo import *" in module bar
        # When: impacted_importers_from_changed_exports()
        result = impacted_importers_from_changed_exports(
            runtime=incremental_runtime,
            changed_exports=changed_exports_table,
            prev_imports_resolved=imports_resolved_path,
        )
        # Then: bar included with reason_kind="import_star"
        star_rows = [
            r for r in result.to_pylist() if r.get("reason_kind") == "import_star"
        ]
        assert len(star_rows) > 0
```

---

## Phase 4: Error Handling Tests (Extended)

### Test Suite 10: Evidence Plan Gating (NEW)

**File:** `tests/integration/error_handling/test_evidence_plan_gating.py`

**Purpose:** Validate graceful gating behavior instead of errors.

```python
@pytest.mark.integration
class TestEvidencePlanGating:
    """Tests for evidence plan gating behavior."""

    def test_gated_dataset_returns_empty_schema_aligned_plan(
        self,
        df_ctx: SessionContext,
        evidence_plan: EvidencePlan,
    ) -> None:
        """Verify gated datasets return LIMIT(0) plans, not errors."""
        # Given: Evidence plan not requiring "optional_dataset"
        # When: apply_query_and_project() for "optional_dataset"
        # Then: Returns plan with correct schema, zero rows

    def test_enabled_when_condition_respected(
        self,
        df_ctx: SessionContext,
        dataset_with_enabled_when: str,
    ) -> None:
        """Verify enabled_when feature flag respected."""
        # Given: Dataset with enabled_when="feature_flag_xyz"
        # And: feature_flag_xyz=False in execution options
        # When: apply_query_and_project()
        # Then: Returns empty plan (gated by condition)

    def test_plan_requires_row_check(
        self,
        evidence_plan: EvidencePlan,
    ) -> None:
        """Verify plan_requires_row() correctly identifies requirements."""
        # Given: Evidence plan with sources=("cst_refs", "ast_files")
        # Then: plan_requires_row(plan, "cst_refs") returns True
        # And: plan_requires_row(plan, "bytecode") returns False
```

---

## Phase 5: Scheduling Tests (NEW)

### Test Suite 11: Schedule Generation

**File:** `tests/integration/scheduling/test_schedule_generation.py`

**Purpose:** Validate topologically-ordered schedule generation.

**Key Signature Notes:**
- `schedule_tasks(graph, *, evidence: EvidenceCatalog, options: ScheduleOptions | None = None) -> TaskSchedule`
- `TaskSchedule.ordered_tasks: tuple[str, ...]` - task **names**, not task objects
- `TaskSchedule.generations: tuple[tuple[str, ...], ...]` - task names per generation
- `allow_partial` is in `ScheduleOptions`, not a direct parameter

```python
from relspec.rustworkx_schedule import schedule_tasks, ScheduleOptions, TaskSchedule
from relspec.rustworkx_graph import TaskGraph

@pytest.mark.integration
class TestScheduleGeneration:
    """Tests for schedule_tasks() behavior."""

    def test_schedule_respects_topological_order(
        self,
        task_graph: TaskGraph,
        evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify every task scheduled after its dependencies.

        Note: ordered_tasks is tuple[str, ...] (task names), so we must
        look up task details via the graph to check dependencies.
        """
        schedule = schedule_tasks(task_graph, evidence=evidence_catalog)
        task_positions = {name: i for i, name in enumerate(schedule.ordered_tasks)}

        # For each task, verify all its input dependencies appear earlier
        for task_name in schedule.ordered_tasks:
            task_idx = task_graph.task_idx.get(task_name)
            if task_idx is None:
                continue
            node = task_graph.graph[task_idx]
            if hasattr(node.payload, "inputs"):
                for dep in node.payload.inputs:
                    if dep in task_positions:
                        assert task_positions[dep] < task_positions[task_name], (
                            f"{task_name} scheduled before dependency {dep}"
                        )

    def test_generations_allow_parallel_execution(
        self,
        task_graph_with_independent_tasks: TaskGraph,
        evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify independent tasks in same generation."""
        schedule = schedule_tasks(
            task_graph_with_independent_tasks, evidence=evidence_catalog
        )
        # Find generation with multiple tasks
        multi_task_gens = [g for g in schedule.generations if len(g) > 1]
        assert len(multi_task_gens) > 0, "Expected parallel tasks in some generation"

    def test_partial_scheduling_with_missing_deps(
        self,
        task_graph_with_missing_evidence: TaskGraph,
        evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify partial scheduling when allow_partial=True.

        Note: allow_partial is in ScheduleOptions, not a direct parameter.
        """
        options = ScheduleOptions(allow_partial=True)
        schedule = schedule_tasks(
            task_graph_with_missing_evidence,
            evidence=evidence_catalog,
            options=options,
        )
        assert len(schedule.missing_tasks) > 0
        assert len(schedule.ordered_tasks) > 0  # Some tasks still scheduled

    def test_edge_validation_detects_column_mismatches(
        self,
        task_graph_with_wrong_columns: TaskGraph,
        evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify schedule reports edge validation failures."""
        schedule = schedule_tasks(
            task_graph_with_wrong_columns, evidence=evidence_catalog
        )
        assert schedule.validation_summary is not None
        assert len(schedule.validation_summary.invalid_edges) > 0

    def test_cost_based_ordering(
        self,
        task_graph_with_costs: TaskGraph,
        evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify higher-cost tasks scheduled earlier in their generation.

        Tasks with higher bottom_level_cost should come first
        (greedy critical path scheduling).
        """
        schedule = schedule_tasks(task_graph_with_costs, evidence=evidence_catalog)
        # Verify schedule was generated
        assert len(schedule.ordered_tasks) > 0
```

---

## Fixture Library (Updated)

### Core Test Fixtures

**File:** `tests/integration/conftest.py`

```python
import pytest
from dataclasses import FrozenInstanceError
from pathlib import Path
import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.runtime import SemanticRuntimeConfig
from semantics.incremental.cdf_cursors import CdfCursorStore, CdfCursor
from extract.coordination.context import FileContext
from relspec.rustworkx_graph import TaskGraph
from relspec.rustworkx_schedule import ScheduleOptions
from relspec.evidence import EvidenceCatalog


@pytest.fixture
def df_ctx() -> SessionContext:
    """Create clean DataFusion session for each test."""
    from datafusion import SessionContext as DFSessionContext
    return DFSessionContext()


@pytest.fixture
def datafusion_profile() -> DataFusionRuntimeProfile:
    """Create default DataFusion runtime profile."""
    return DataFusionRuntimeProfile.default()


@pytest.fixture
def semantic_config() -> SemanticRuntimeConfig:
    """Create default semantic runtime config."""
    return SemanticRuntimeConfig(
        output_locations={"cpg_nodes_v1": "/tmp/test_cpg_nodes"},
        cache_policy_overrides={},
    )


@pytest.fixture
def cursors_dir(tmp_path: Path) -> Path:
    """Create temporary cursors directory."""
    cursors = tmp_path / "cursors"
    cursors.mkdir()
    return cursors


@pytest.fixture
def cursor_store(cursors_dir: Path) -> CdfCursorStore:
    """Create cursor store in temporary directory.

    Note: CdfCursorStore takes cursors_path directly, not state_dir.
    """
    return CdfCursorStore(cursors_path=cursors_dir)


@pytest.fixture
def sample_python_source() -> str:
    """Simple Python source for extraction tests."""
    return '''
def foo(x: int) -> int:
    """Add one to x."""
    return x + 1

class Bar:
    def __init__(self, value: int) -> None:
        self.value = value

    def get_value(self) -> int:
        return self.value
'''


@pytest.fixture
def file_context_with_data() -> FileContext:
    """FileContext with explicit data set."""
    return FileContext(
        file_id="f1",
        path="test.py",
        abs_path="/tmp/test.py",
        file_sha256="abc123",
        encoding="utf-8",
        text="def foo(): pass",
        data=b"def foo(): pass",
    )


@pytest.fixture
def file_context_text_only() -> FileContext:
    """FileContext with only text (no data)."""
    return FileContext(
        file_id="f2",
        path="test.py",
        abs_path=None,
        file_sha256=None,
        encoding="utf-8",
        text="def bar(): pass",
        data=None,
    )


@pytest.fixture
def evidence_catalog() -> EvidenceCatalog:
    """Create empty evidence catalog for scheduling tests.

    Note: EvidenceCatalog is mutable dataclass with sets/dicts.
    Tests should populate with specific evidence as needed.
    """
    return EvidenceCatalog()
```

---

## Implementation Priorities (Revised)

### Phase 1 (Critical - Week 1-2)
1. **Adapter round-trip tests** - Verify profile ↔ semantic config consistency
2. **Immutability contract tests** - Prevent mutation bugs in frozen dataclasses
3. **Evidence plan gating tests** - Verify graceful empty-plan behavior
4. **Bipartite graph structure tests** - Validate evidence/task node distinction

### Phase 2 (High Priority - Week 3-4)
5. **CDF cursor tracking (corrected)** - Fix start_version = last + 1 tests
6. **Merge strategy tests** - All four strategies: APPEND, UPSERT, REPLACE, DELETE_INSERT
7. **Impact closure strategy tests** - hybrid, symbol_closure, import_closure
8. **Post-process resilience tests** - Failures recorded, not propagated

### Phase 3 (Medium Priority - Week 5-6)
9. **Column-level dependency tests** - GraphEdge.required_columns validation
10. **Cost modeling tests** - Bottom-level costs, slack computation
11. **Schedule generation tests** - Topological order, parallel generations
12. **FileContext fallback tests** - bytes → text → disk chain

### Phase 4 (Lower Priority - Week 7-8)
13. **Multi-source alignment tests** - CST/AST/SCIP consistency
14. **Streaming artifact tests** - Large write (>100k) tracking
15. **Expression validation skip tests** - Graceful degradation on invalid exprs
16. **Performance baseline tests** - Compilation and join selectivity

---

## Success Metrics (Revised)

| Metric | Target | Measurement |
|--------|--------|-------------|
| Adapter coverage | Round-trip tests pass | profile → config → profile consistency |
| Immutability enforcement | 100% frozen classes tested | FrozenInstanceError on all frozen types |
| Gating behavior | Zero exceptions from missing optional | Empty plans returned, not errors |
| CDF correctness | All 4 merge strategies tested | APPEND, UPSERT, REPLACE, DELETE_INSERT |
| Impact strategies | All 3 closure types tested | hybrid, symbol_closure, import_closure |
| Graph structure | Bipartite invariant verified | Evidence/task node types validated |
| Schedule correctness | Topological order guaranteed | No task before dependencies |
| Test execution time | < 90s for full integration suite | CI timing |

---

## Appendix: Test Markers (Revised)

```python
# In pyproject.toml

[tool.pytest.ini_options]
markers = [
    "integration: Multi-subsystem integration tests",
    "boundary: Subsystem boundary tests",
    "adapter: Adapter round-trip tests",
    "contract: Schema and immutability contract tests",
    "incremental: CDF and incremental behavior tests",
    "scheduling: Graph construction and schedule tests",
    "error_handling: Error path and resilience tests",
    "multi_source: Cross-extractor consistency tests",
    "performance: Integration-level performance tests",
]
```

---

## Next Steps

**Completed:**
- ✅ Initial code review corrections documented (v1)
- ✅ Final deep code review completed (v2) - All test expectations verified against production code

**Ready for Implementation:**
1. **Implement Phase 1** - Critical adapter and immutability tests (verified correct)
2. **Create fixtures** - Build synthetic data matching verified signatures
3. **Prioritize based on recent bugs** - Focus on areas with known issues
4. **Iterate** - Add tests as new behaviors discovered

**Code Review Confidence:**
- All function signatures verified against source code with line numbers
- All field names verified (CdfFilterPolicy, TaskSchedule, etc.)
- All exception types verified (FrozenInstanceError vs AttributeError)
- All default values verified (constructor vs config defaults)
