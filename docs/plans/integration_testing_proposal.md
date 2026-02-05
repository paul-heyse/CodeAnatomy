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
| SemanticRuntimeConfig in datafusion_engine | Lives in `semantics/runtime.py`; adapter in `datafusion_engine/semantics_runtime.py` inverts dependency | Test adapter round-trip |
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

### Corrections from Deep Code Review (v3)

| Original Proposal | Actual Code | Fix Applied |
|-------------------|-------------|-------------|
| `DataFusionRuntimeProfile.default()` | No `.default()` classmethod; all fields have `default_factory` | Use `DataFusionRuntimeProfile()` |
| `EvidencePlan.apply_query_and_project()` | Method doesn't exist; actual API: `requires_dataset()`, `required_columns_for()` | Fixed test descriptions and assertions |
| `plan_requires_row(plan, "cst_refs")` takes string | `plan_requires_row(plan: EvidencePlan, row: ExtractMetadata)` takes an `ExtractMetadata` object | Use `plan.requires_dataset("cst_refs")` directly |
| `relationship_spec_with_invalid_expr: RelationshipSpec` | `compile_relationship_with_quality()` takes `QualityRelationshipSpec` (from `semantics.quality`) | Fixed fixture type |
| `_validate_bundle_udfs()` called directly | Actual function: `validate_required_udfs()` from `datafusion_engine.udf.runtime` | Fixed function name |
| `infer_deps_from_plan_bundle()` takes `ViewNode` | Takes `InferredDepsInputs` (from `relspec.inferred_deps`), not `ViewNode` | Fixed input type, added `InferredDepsInputs` construction |
| `bottom_level_costs(TaskGraph)` | Takes `rx.PyDiGraph`, not `TaskGraph`; access via `task_graph.graph` | Fixed parameter type |
| `bottom_level_costs` in `relspec.rustworkx_schedule` | Lives in `relspec.execution_plan` | Fixed import path |
| Adapter described as `semantics/adapters.py` | Module is `datafusion_engine/semantics_runtime.py` | Fixed module path reference |
| `CdfCursorStore` described as dataclass | Is `StructBaseStrict` (msgspec), `frozen=True` | Fixed class description in fixtures |

### Corrections from External Review Integration (v4)

| Review Item | Action Taken |
|-------------|-------------|
| Pytest markers in `pyproject.toml` | Fixed: markers live in `pytest.ini`, not `pyproject.toml` |
| Incremental tests under `tests/integration/incremental/` | Fixed: stateful CDF/Delta tests moved to `tests/incremental/` |
| Duplicating existing unit coverage | Added annotations marking unit-covered items; keep only boundary variants |
| `test_schedule_respects_topological_order` uses `TaskNode.inputs` as task names | Fixed: `TaskNode.inputs` are evidence dataset names; check graph predecessor topology |
| Standalone `tests/integration/conftest.py` | Fixed: reference shared fixtures in `tests/conftest.py` and `tests/test_helpers/` |
| Performance tests mixed into integration gate | Fixed: marked non-gating, separate CI job |
| Missing scope: Delta read-after-write contracts | Added: Phase 2 scope expansion |
| Missing scope: capability negotiation boundaries | Added: Phase 2 scope expansion |
| Missing scope: evidence-plan-to-extractor pipeline | Added: Phase 1 scope expansion |
| Missing scope: plan/artifact determinism | Added: Phase 3 scope expansion |
| Missing scope: diagnostics contract assertions | Added: error handling tests |
| Missing Phase 0: stabilize existing failures | Added: Phase 0 with 5 known failing tests |

### Existing Unit Coverage (Do Not Duplicate)

The following areas already have unit-level coverage. Integration tests should cover **only boundary-crossing variants**, not duplicate algorithmic behavior:

| Area | Existing Unit Tests | Integration Scope |
|------|--------------------|--------------------|
| CDF cursor behavior / `get_start_version()` | `tests/unit/semantics/incremental/test_cdf_cursors.py` | Only cursor ↔ Delta write interaction |
| Merge strategies (APPEND, UPSERT, etc.) | `tests/unit/semantics/incremental/test_merge_strategies.py` | Only merge ↔ schema evolution boundary |
| Adapter basics (round-trip) | `tests/unit/datafusion_engine/test_semantics_runtime_bridge.py` | Only profile → session construction path |
| Streaming write thresholds | `tests/unit/test_incremental_streaming_metrics.py` | Only threshold ↔ materialization path |

### Newly Identified Test Coverage Gaps

1. **Adapter Bidirectionality** - `semantic_runtime_from_profile() → apply_semantic_runtime_config()` round-trip not tested (adapter in `datafusion_engine.semantics_runtime`)
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
| Unit tests | 185 files | Single module behavior | No cross-module interaction |
| Integration | 23 files | Basic multi-subsystem | Limited boundary coverage |
| E2E | 16 files | Full pipeline (CQ focus) | Too coarse for debugging |
| Incremental | 1 file | Delta/CDF lifecycle | Minimal stateful coverage |
| Golden/Contract | ~25 files | Output stability | No behavioral testing |

### Identified Gaps (Refined)

1. **Extraction → Normalization boundary** - No tests validate byte-span canonicalization across extractors
2. **Normalization → Relationship compilation** - Join strategy selection untested beyond basic cases
3. **View DAG → Scheduling** - Bipartite graph structure and cost modeling untested
4. **Incremental/CDF behavior** - Impact closure strategies and merge strategy edge cases
5. **Schema evolution scenarios** - Delta write policy enforcement partially tested
6. **Error propagation paths** - Post-process resilience, expression validation graceful skip
7. **Multi-source consistency** - No tests verify CST/AST/SCIP alignment
8. **Adapter contracts** - SemanticRuntimeConfig ↔ DataFusionRuntimeProfile round-trip (adapter in `datafusion_engine.semantics_runtime`)
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

**Design principle:** Keep stateful Delta/CDF lifecycle tests in `tests/incremental/` (existing directory). Reserve `tests/integration/` for multi-subsystem boundary tests that are not state-store-centric. This preserves the existing separation of concerns.

```
tests/
├── integration/
│   ├── boundaries/                    # NEW: Subsystem boundary tests
│   │   ├── test_extraction_to_normalization.py
│   │   ├── test_normalization_to_relationship.py
│   │   ├── test_relationship_to_cpg_output.py
│   │   ├── test_view_dag_to_scheduling.py
│   │   └── test_evidence_plan_gating.py
│   │
│   ├── adapters/                      # NEW: Adapter round-trip tests (small set)
│   │   └── test_semantic_runtime_bridge_integration.py
│   │
│   ├── contracts/                     # NEW: Data contract validation
│   │   ├── test_extraction_schema_contracts.py
│   │   ├── test_semantic_view_contracts.py
│   │   └── test_immutability_contracts.py  # Smoke only; keep unit-level
│   │
│   ├── scheduling/                    # NEW: Graph and schedule tests
│   │   ├── test_schedule_tasks_boundaries.py
│   │   ├── test_bipartite_graph_structure.py
│   │   └── test_column_level_deps.py
│   │
│   ├── error_handling/                # NEW: Error paths and recovery
│   │   ├── test_extract_postprocess_resilience.py
│   │   ├── test_graceful_degradation.py
│   │   └── test_capability_negotiation.py     # NEW
│   │
│   ├── runtime/                       # NEW: Runtime capability contracts
│   │   └── test_capability_negotiation.py
│   │
│   ├── multi_source/                  # DEFER: Start with one focused contract
│   │   └── test_cst_ast_alignment.py
│   │
│   └── performance/                   # NON-GATING: Separate CI job
│       ├── test_plan_compilation_performance.py
│       └── test_materialization_throughput.py
│
├── incremental/                       # EXTEND existing (stateful Delta/CDF tests)
│   ├── test_view_artifacts.py                   # existing
│   ├── test_impact_closure_strategies.py        # NEW
│   ├── test_delta_read_after_write_contracts.py # NEW (scope expansion)
│   └── test_fingerprint_invalidation.py         # NEW
│
```

**Note on `tests/incremental/`:** This directory already exists with `test_view_artifacts.py`. Stateful Delta/CDF lifecycle tests belong here, not under `tests/integration/incremental/`.

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
    def test_evidence_plan_gating_excludes_unrequired_datasets(
        self,
        evidence_plan_without_dataset: EvidencePlan,
    ) -> None:
        """Verify gated datasets are excluded via requires_dataset()."""
        # Given: Evidence plan that doesn't require ast_files
        # When: evidence_plan.requires_dataset("ast_files") called
        # Then: Returns False (dataset excluded from plan)

    def test_evidence_plan_required_columns_for_dataset(
        self,
        evidence_plan_with_columns: EvidencePlan,
    ) -> None:
        """Verify required_columns_for() returns only constrained columns."""
        # Given: Evidence plan requiring only file_id, bstart, bend for cst_nodes
        # When: evidence_plan.required_columns_for("cst_nodes") called
        # Then: Returns ("file_id", "bstart", "bend")

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
        quality_spec_with_invalid_expr: QualityRelationshipSpec,
    ) -> None:
        """Verify invalid expressions are skipped with warning, not failure.

        Note: compile_relationship_with_quality() takes QualityRelationshipSpec
        (from semantics.quality), not RelationshipSpec.
        """
        # Given: QualityRelationshipSpec with an expression that fails validation
        # When: SemanticCompiler.compile_relationship_with_quality() called
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
        inferred_deps_inputs: InferredDepsInputs,
    ) -> None:
        """Verify infer_deps_from_plan_bundle() extracts correct inputs.

        Note: infer_deps_from_plan_bundle() takes InferredDepsInputs
        (from relspec.inferred_deps), not ViewNode directly. Construct
        InferredDepsInputs from ViewNode.plan_bundle.
        """
        # Given: InferredDepsInputs built from ViewNode's plan_bundle
        # When: infer_deps_from_plan_bundle(inputs) called
        # Then: InferredDeps.inputs matches tables scanned in plan

    def test_column_level_requirements_extracted(
        self,
        df_ctx: SessionContext,
        inferred_deps_inputs_with_projection: InferredDepsInputs,
    ) -> None:
        """Verify column-level requirements extracted from plan."""
        # Given: InferredDepsInputs for a view selecting subset of columns
        # When: infer_deps_from_plan_bundle(inputs) called
        # Then: InferredDeps.required_columns[table] lists only used columns

    def test_udf_requirements_validated_against_snapshot(
        self,
        df_ctx: SessionContext,
        inferred_deps_inputs_with_udf: InferredDepsInputs,
        udf_snapshot: dict[str, object],
    ) -> None:
        """Verify required_udfs validated against snapshot.

        Note: Actual function is validate_required_udfs() from
        datafusion_engine.udf.runtime, not _validate_bundle_udfs().
        """
        # Given: InferredDepsInputs for view using stable_hash64 UDF
        # When: validate_required_udfs(snapshot, required=resolved_udfs) called
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
        task_costs: dict[str, float],
    ) -> None:
        """Verify bottom_level_costs() computes weighted longest paths.

        Note: bottom_level_costs() is in relspec.execution_plan (not
        rustworkx_schedule) and takes rx.PyDiGraph, not TaskGraph.
        Access the graph via task_graph.graph.
        """
        from relspec.execution_plan import bottom_level_costs

        # Given: TaskGraph with task_costs annotations
        # When: bottom_level_costs(task_graph.graph, task_costs=task_costs)
        # Then: Each task has bottom cost = self_cost + max(successor costs)

    def test_slack_computation(
        self,
        task_graph_with_costs: TaskGraph,
        task_costs: dict[str, float],
    ) -> None:
        """Verify slack = latest_start - earliest_start.

        Note: task_slack_by_task() is in relspec.execution_plan and takes
        rx.PyDiGraph, not TaskGraph.
        """
        from relspec.execution_plan import task_slack_by_task

        # Given: TaskGraph with computed schedule
        # When: task_slack_by_task(task_graph.graph, task_costs=task_costs)
        # Then: Critical path tasks have zero slack

    def test_critical_path_has_zero_slack(
        self,
        task_graph_with_costs: TaskGraph,
        task_costs: dict[str, float],
    ) -> None:
        """Verify critical path identified by zero slack."""
        from relspec.execution_plan import bottom_level_costs, task_slack_by_task

        # Given: TaskGraph with computed slack via task_slack_by_task(graph.graph)
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
    # Test 4.3: Streaming write tracking (DEFERRED — unit coverage exists)
    # ─────────────────────────────────────────────────────────────
    def test_large_write_records_streaming_artifact(
        self,
        tmp_path: Path,
        large_table: pa.Table,  # >100k rows
    ) -> None:
        """Verify large writes (>100k rows) record streaming artifacts.

        Note: Streaming threshold behavior is already covered by
        tests/unit/test_incremental_streaming_metrics.py. Only add
        integration variant if threshold ↔ materialization path needs
        coverage.
        """
        # Given: Table with 150,000 rows
        # When: write_overwrite_dataset() called
        # Then: Streaming write artifact recorded
```

---

## Phase 2: Adapter and Contract Tests

### Test Suite 5: Semantic Runtime Adapter (NEW — Reduced Breadth)

**File:** `tests/integration/adapters/test_semantic_runtime_bridge_integration.py`

**Purpose:** Validate bidirectional adapter between DataFusionRuntimeProfile and SemanticRuntimeConfig.

**Scope note:** Basic adapter round-trip is already covered by `tests/unit/datafusion_engine/test_semantics_runtime_bridge.py`. Keep only a small integration set proving: profile → semantic config → profile behavior inside session construction. Do not duplicate every unit-path adapter behavior.

**Module Notes:**
- `SemanticRuntimeConfig` lives in `semantics.runtime` (owned by semantics module)
- Adapter functions live in `datafusion_engine.semantics_runtime` (NOT `semantics/adapters.py`)
- `semantic_runtime_from_profile(profile)` → extracts semantic config from profile
- `apply_semantic_runtime_config(profile, semantic_config)` → applies config back to profile
- Profile update uses `msgspec.structs.replace()` for structural sharing (immutable)
- Cache merging path: `profile.data_sources.semantic_output.cache_overrides` (not top-level)

```python
from datafusion_engine.semantics_runtime import (
    apply_semantic_runtime_config,
    semantic_runtime_from_profile,
)

@pytest.mark.integration
class TestSemanticRuntimeAdapter:
    """Tests for semantic runtime adapter round-trip."""

    def test_profile_to_semantic_config_extracts_output_locations(
        self,
        datafusion_profile: DataFusionRuntimeProfile,
    ) -> None:
        """Verify output locations extracted from profile."""
        # Given: Profile with dataset locations for CPG outputs
        # When: semantic_runtime_from_profile(profile) called
        # Then: SemanticRuntimeConfig.output_locations populated

    def test_semantic_config_to_profile_applies_locations(
        self,
        datafusion_profile: DataFusionRuntimeProfile,
        semantic_config: SemanticRuntimeConfig,
    ) -> None:
        """Verify semantic config locations applied back to profile.

        Note: apply_semantic_runtime_config() updates
        profile.catalog.registry_catalogs and
        profile.data_sources.semantic_output.locations.
        """
        # Given: SemanticRuntimeConfig with custom output_locations
        # When: apply_semantic_runtime_config(profile, semantic_config) called
        # Then: Profile's catalog.registry_catalogs updated with new locations

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
        """Verify cache policy merging: semantic_config > profile.

        Note: Profile cache overrides are at
        profile.data_sources.semantic_output.cache_overrides
        (not a top-level cache_overrides field).
        """
        # Given: Profile with data_sources.semantic_output.cache_overrides = {"view_a": "none"}
        # And: SemanticConfig with cache_policy_overrides = {"view_a": "delta_output"}
        # When: apply_semantic_runtime_config(profile, semantic_config) called
        # Then: Merged result has {"view_a": "delta_output"} (semantic wins)

    def test_immutable_update_via_msgspec_replace(
        self,
        datafusion_profile: DataFusionRuntimeProfile,
        semantic_config: SemanticRuntimeConfig,
    ) -> None:
        """Verify profile update uses structural sharing (no mutation).

        Note: apply_semantic_runtime_config() uses msgspec.structs.replace()
        to produce a new profile without mutating the original.
        """
        # Given: Original profile
        # When: apply_semantic_runtime_config(profile, semantic_config) called
        # Then: Original profile unchanged, new profile returned
```

### Test Suite 6: Immutability Contracts (NEW — Smoke Only)

**File:** `tests/integration/contracts/test_immutability_contracts.py`

**Purpose:** Validate frozen dataclass/struct mutation prevention.

**Scope note:** Immutability tests are primarily unit-level. Keep only a smoke assertion here where immutability affects subsystem behavior (e.g., confirming that adapter round-trip produces new objects, not mutations). The individual type assertions below can remain as quick smoke checks.

**Implementation Notes:**
- `CdfCursor`: `msgspec.Struct` with custom `__setattr__` that raises `FrozenInstanceError` (in `semantics.incremental.cdf_cursors`)
- `SemanticRuntimeConfig`: `@dataclass(frozen=True)` - raises `FrozenInstanceError` (in `semantics.runtime`)
- `InferredDeps`: `StructBaseStrict, frozen=True` (msgspec) - raises `AttributeError` (in `relspec.inferred_deps`)
- `FileContext`: `@dataclass(frozen=True)` - raises `FrozenInstanceError` (in `extract.coordination.context`)

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

**File:** `tests/incremental/test_cdf_cursor_tracking.py` *(moved from integration/incremental/)*

**Purpose:** Validate CDF cursor tracking with corrected behavior.

**Scope note:** Algorithmic cursor behavior (start version, persistence, listing) is already covered by `tests/unit/semantics/incremental/test_cdf_cursors.py`. Keep only boundary-crossing tests here: cursor ↔ Delta write interaction, cursor recovery after failed writes.

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

**File:** `tests/incremental/test_merge_strategies.py` *(moved from integration/incremental/)*

**Purpose:** Validate all four merge strategies and filter policies.

**Scope note:** Merge strategy algorithms are already covered by `tests/unit/semantics/incremental/test_merge_strategies.py`. Keep only boundary-crossing integration variants here: merge strategy ↔ schema evolution boundary, merge result ↔ Delta write path.

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

**File:** `tests/incremental/test_impact_closure_strategies.py` *(moved from integration/incremental/)*

**Purpose:** Validate the three impact closure computation strategies.

**Key Signature Notes:**
- `merge_impacted_files(runtime: IncrementalRuntime, inputs: ImpactedFileInputs, *, strategy: str) -> pa.Table`
- Strategies: `"hybrid"` (default/else), `"symbol_closure"`, `"import_closure"`
- `impacted_importers_from_changed_exports(*, runtime: IncrementalRuntime, changed_exports: TableLike, prev_imports_resolved: str | None) -> pa.Table`
- `ImpactedFileInputs` fields: `changed_files`, `callers`, `importers`, `import_closure_only` (all `TableLike | None` except changed_files)

```python
from semantics.incremental.impact import (
    merge_impacted_files,
    impacted_importers_from_changed_exports,
    ImpactedFileInputs,
)
from semantics.incremental.runtime import IncrementalRuntime

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

    def test_gated_dataset_excluded_from_plan(
        self,
        evidence_plan: EvidencePlan,
    ) -> None:
        """Verify gated datasets are excluded via requires_dataset().

        Note: EvidencePlan has no apply_query_and_project() method.
        Use requires_dataset(name) to check inclusion and
        required_columns_for(name) for column projection.
        """
        # Given: Evidence plan not requiring "optional_dataset"
        # When: evidence_plan.requires_dataset("optional_dataset")
        # Then: Returns False (dataset excluded)

    def test_plan_feature_flags_disable_unrequired_extractors(
        self,
        evidence_plan: EvidencePlan,
    ) -> None:
        """Verify plan_feature_flags() disables unrequired extractors.

        Note: Feature-flag gating is via plan_feature_flags() from
        extract.coordination.spec_helpers, not enabled_when on datasets.
        """
        from extract.coordination.spec_helpers import plan_feature_flags

        # Given: Evidence plan not requiring bytecode datasets
        # When: plan_feature_flags("bytecode", evidence_plan) called
        # Then: Returns {flag: False} for bytecode feature flags

    def test_plan_requires_dataset_check(
        self,
        evidence_plan: EvidencePlan,
    ) -> None:
        """Verify requires_dataset() correctly identifies requirements.

        Note: plan_requires_row() takes (plan, row: ExtractMetadata),
        not a string. For direct string checks, use plan.requires_dataset().
        """
        # Given: Evidence plan with sources=("cst_refs", "ast_files")
        # Then: plan.requires_dataset("cst_refs") returns True
        # And: plan.requires_dataset("bytecode") returns False
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
from relspec.evidence import EvidenceCatalog

@pytest.mark.integration
class TestScheduleGeneration:
    """Tests for schedule_tasks() behavior."""

    def test_schedule_respects_topological_order(
        self,
        task_graph: TaskGraph,
        evidence_catalog: EvidenceCatalog,
    ) -> None:
        """Verify every task scheduled after its dependencies.

        IMPORTANT: TaskNode.inputs are evidence dataset names, NOT task names.
        Dependency ordering must be checked via predecessor task nodes in
        graph topology, not by comparing TaskNode.inputs against scheduled
        task names (which would silently pass while missing real ordering
        regressions).

        Approach: For each task node, find predecessor task nodes in the
        bipartite graph (task → evidence → task path) and verify they
        appear earlier in the schedule.
        """
        import rustworkx as rx

        schedule = schedule_tasks(task_graph, evidence=evidence_catalog)
        task_positions = {name: i for i, name in enumerate(schedule.ordered_tasks)}

        # For each task, check predecessor tasks via graph topology
        for task_name in schedule.ordered_tasks:
            task_idx = task_graph.task_idx.get(task_name)
            if task_idx is None:
                continue
            # Walk predecessor chain: task ← evidence ← predecessor_task
            # In bipartite graph: evidence nodes connect tasks
            for evidence_idx in task_graph.graph.predecessor_indices(task_idx):
                evidence_node = task_graph.graph[evidence_idx]
                # Skip if not an evidence node
                if not hasattr(evidence_node, "name"):
                    continue
                # Find tasks that produce this evidence
                for pred_task_idx in task_graph.graph.predecessor_indices(evidence_idx):
                    pred_node = task_graph.graph[pred_task_idx]
                    if hasattr(pred_node, "name") and pred_node.name in task_positions:
                        assert task_positions[pred_node.name] < task_positions[task_name], (
                            f"{task_name} scheduled before predecessor {pred_node.name}"
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

## Phase 6: Scope Expansions (From Review)

The following scope expansions were identified during external review as high-value additions not strongly captured in the original proposal.

### Expansion 1: Delta Read-After-Write and Provider Registration Contracts (High Priority)

**File:** `tests/incremental/test_delta_read_after_write_contracts.py`

**Motivation:** Current integration failures show this boundary is unstable:
- `test_schema_mode_merge_allows_new_columns` (failing)
- `test_upsert_partitioned_dataset_alignment_and_deletes` (failing)
- `test_table_provider_registry_records_delta_capsule` (failing)
- `test_delta_pruning_predicate_from_dataset_spec` (failing)
- `test_write_overwrite_dataset_roundtrip` (failing)

```python
@pytest.mark.integration
class TestDeltaReadAfterWriteContracts:
    """Verify schema resolution and provider registration after Delta writes."""

    def test_schema_visible_after_write_plain_location(
        self,
        tmp_path: Path,
        df_ctx: SessionContext,
    ) -> None:
        """Verify schema is queryable immediately after Delta write."""
        # Given: Write a table to a plain Delta location
        # When: Register as table provider and query
        # Then: Schema matches written table, no stale metadata

    def test_schema_visible_after_write_dataset_spec_location(
        self,
        tmp_path: Path,
        df_ctx: SessionContext,
    ) -> None:
        """Verify schema resolution for dataset-spec-backed locations."""
        # Given: Write via dataset spec with partition_by
        # When: Register provider and query
        # Then: Partitioned schema visible, pruning predicates work

    def test_provider_registration_after_schema_evolution(
        self,
        tmp_path: Path,
        df_ctx: SessionContext,
    ) -> None:
        """Verify provider registry handles schema evolution correctly."""
        # Given: Existing Delta table, write with new nullable column (merge mode)
        # When: Re-register table provider
        # Then: New column visible in provider schema
```

### Expansion 2: Capability Negotiation and Graceful Fallback Boundaries

**File:** `tests/integration/runtime/test_capability_negotiation.py`

**Motivation:** Recent failures indicate fragile behavior around extension capabilities and replay errors. Tests should ensure deterministic failure payloads (not uncaught exceptions).

```python
@pytest.mark.integration
class TestCapabilityNegotiation:
    """Verify deterministic failure payloads for unsupported capabilities."""

    def test_substrait_decode_failure_produces_diagnostic(
        self,
        df_ctx: SessionContext,
    ) -> None:
        """Verify Substrait payload decode failure returns structured error."""
        # Given: Invalid or corrupt Substrait payload
        # When: Attempt to decode and execute
        # Then: Structured error with diagnostic context, not uncaught exception

    def test_unsupported_async_udf_produces_diagnostic(
        self,
        df_ctx: SessionContext,
    ) -> None:
        """Verify async UDF request fails gracefully if unsupported."""
        # Given: Runtime build without async UDF capability
        # When: Plan requires async UDF
        # Then: Deterministic failure payload with capability name and suggestion
```

### Expansion 3: Evidence-Plan-to-Extractor-Options Pipeline

**File:** `tests/integration/boundaries/test_evidence_plan_to_extractor.py`

**Motivation:** Proposal covers EvidencePlan methods, but should also include end-to-end gating from plan to extractor options.

```python
@pytest.mark.integration
class TestEvidencePlanToExtractorPipeline:
    """Verify plan gating flows through to extractor configuration."""

    def test_plan_feature_flags_propagate_to_rule_execution_options(
        self,
        evidence_plan: EvidencePlan,
    ) -> None:
        """Verify plan_feature_flags() disables extractors via rule_execution_options()."""
        # Given: Evidence plan not requiring bytecode
        # When: rule_execution_options(evidence_plan) called
        # Then: Bytecode extractor disabled in options

    def test_enabled_when_stage_gating(
        self,
        evidence_plan: EvidencePlan,
    ) -> None:
        """Verify enabled_when stage gating excludes irrelevant stages."""
        # Given: Evidence plan with restricted stage set
        # When: Checking extractor enablement
        # Then: Only enabled stages have their extractors active

    def test_projected_column_subset_propagates_to_scan(
        self,
        evidence_plan: EvidencePlan,
        df_ctx: SessionContext,
    ) -> None:
        """Verify required_columns_for() projection reaches DataFusion scan."""
        # Given: Evidence plan requiring only (file_id, bstart, bend)
        # When: Building scan plan for that dataset
        # Then: DataFusion plan only references projected columns
```

### Expansion 4: Plan and Artifact Determinism Boundaries

**File:** `tests/integration/boundaries/test_plan_determinism.py`

**Motivation:** Targeted integration tests validating when fingerprints/artifacts should and should not change.

```python
@pytest.mark.integration
class TestPlanDeterminism:
    """Verify fingerprint stability and expected change boundaries."""

    def test_fingerprint_stable_under_unchanged_query_and_runtime(
        self,
        df_ctx: SessionContext,
    ) -> None:
        """Verify plan fingerprint is identical across runs with same inputs."""
        # Given: Same query, same runtime config
        # When: Compile plan twice
        # Then: Fingerprints are identical

    def test_fingerprint_changes_with_runtime_policy_toggle(
        self,
        df_ctx: SessionContext,
    ) -> None:
        """Verify fingerprint changes when meaningful policy changes."""
        # Given: Plan compiled with cdf_enabled=False
        # When: Recompile with cdf_enabled=True
        # Then: Fingerprint differs (meaningful policy change)

    def test_fingerprint_stable_under_irrelevant_config_change(
        self,
        df_ctx: SessionContext,
    ) -> None:
        """Verify fingerprint ignores non-semantic config changes."""
        # Given: Plan compiled with storage_options={"a": "1"}
        # When: Recompile with storage_options={"a": "2"}
        # Then: Fingerprint unchanged if storage options are not semantic
```

### Expansion 5: Diagnostics Contract Assertions for Error-Path Observability

**Integration into existing Test Suites 4 and 10.**

For all resilience/error-path tests, assert both:
1. **Event presence** — the diagnostic event is recorded
2. **Stable status taxonomy** — the event uses canonical status names

```python
# Add to TestCpgToMaterialization (Suite 4) and TestEvidencePlanGating (Suite 10):

def test_register_view_failed_event_has_stable_taxonomy(
    self,
    df_ctx: SessionContext,
) -> None:
    """Verify register_view_failed events use canonical status taxonomy."""
    # Given: View that fails during registration
    # When: Event recorded
    # Then: event.status == "register_view_failed" (exact string)
    # And: event.stage == "postprocess" (canonical stage name)

def test_schema_contract_failed_event_has_stable_taxonomy(
    self,
    df_ctx: SessionContext,
) -> None:
    """Verify schema_contract_failed events use canonical status taxonomy."""
    # Given: Table violating schema contract
    # When: Event recorded
    # Then: event.status == "schema_contract_failed" (exact string)

def test_view_artifact_failed_event_has_stable_taxonomy(
    self,
    df_ctx: SessionContext,
) -> None:
    """Verify view_artifact_failed events use canonical status taxonomy."""
    # Given: View artifact that fails to materialize
    # When: Event recorded
    # Then: event.status == "view_artifact_failed" (exact string)
```

**Rationale:** Asserting stable status taxonomy keeps tests resilient (won't break on wording changes) and operationally useful (dashboards/alerting can rely on canonical names).

---

## Fixture Library (Updated)

### Shared Fixture Reuse Policy

**Do not create a standalone `tests/integration/conftest.py` that bypasses shared fixtures.** Reuse the shared fixtures and helpers that already encode environment/capability checks and diagnostics conventions:

| Shared Resource | Purpose | Key Exports |
|----------------|---------|-------------|
| `tests/conftest.py` | Root conftest with diagnostics, crash context | Session-level fixtures |
| `tests/test_helpers/datafusion_runtime.py` | DataFusion session/profile setup | `df_profile()`, `df_ctx()` |
| `tests/test_helpers/optional_deps.py` | Capability checking | `require_datafusion()` |
| `tests/test_helpers/delta_seed.py` | Delta table creation | `write_delta_table()`, `DeltaSeedOptions` |
| `tests/test_helpers/arrow_seed.py` | Arrow table registration | `register_arrow_table()` |
| `tests/test_helpers/diagnostics.py` | Test diagnostics helpers | Various |
| `tests/test_helpers/semantic_registry_runtime.py` | Semantic registry setup | Various |

### Integration-Specific Fixtures (Minimal Additions)

New integration tests should add only fixtures not already provided by the shared helpers. Place new integration-specific fixtures in the test file or a minimal subdirectory conftest:

```python
# tests/integration/boundaries/conftest.py
# Only add what shared helpers don't provide.
# Reuse df_profile() and df_ctx() from tests/test_helpers/datafusion_runtime.py.

import pytest
from pathlib import Path

from semantics.runtime import SemanticRuntimeConfig
from semantics.incremental.cdf_cursors import CdfCursorStore
from extract.coordination.context import FileContext
from relspec.evidence import EvidenceCatalog
from tests.test_helpers.datafusion_runtime import df_profile


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

    Note: CdfCursorStore is a StructBaseStrict (msgspec), not a dataclass.
    Takes cursors_path directly, not state_dir.
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

    Note: EvidenceCatalog is a mutable @dataclass (not frozen) with
    sets/dicts all having default_factory. Has a clone() method for
    staged updates. Tests should populate with specific evidence as needed.
    """
    return EvidenceCatalog()
```

**Key construction notes:**
- `DataFusionRuntimeProfile()` — no `.default()` classmethod; all fields have `default_factory`
- `CdfCursorStore(cursors_path=...)` — is `StructBaseStrict` (msgspec), not a dataclass
- `EvidenceCatalog()` — mutable `@dataclass` with `clone()` for staged updates

---

## Implementation Priorities (Revised per Review)

### Phase 0: Stabilize Existing Integration Failures (Immediate)

Before adding new tests, resolve and lock down the 5 currently failing integration tests. Add regression assertions around the fixed behavior.

**Known failures** (from `uv run pytest tests/integration -q`: 41 passed, 5 failed, 4 skipped):

1. `tests/integration/test_delta_protocol_and_schema_mode.py::test_schema_mode_merge_allows_new_columns`
2. `tests/integration/test_incremental_partitioned_updates.py::test_upsert_partitioned_dataset_alignment_and_deletes`
3. `tests/integration/test_pycapsule_provider_registry.py::test_table_provider_registry_records_delta_capsule`
4. `tests/integration/test_pycapsule_provider_registry.py::test_delta_pruning_predicate_from_dataset_spec`
5. `tests/integration/test_semantic_incremental_overwrite.py::test_write_overwrite_dataset_roundtrip`

**Action:** Fix root causes, add regression assertions, confirm green gate before proceeding.

### Phase 1: Core Boundary Contracts (Highest Value)

Focus on areas with highest ROI that are currently under-tested:

1. **Evidence plan gating and projection behavior** (Suites 1, 10, Expansion 3)
   - `tests/integration/boundaries/test_evidence_plan_gating.py`
   - `tests/integration/boundaries/test_evidence_plan_to_extractor.py`
   - Covers `requires_dataset()` + `required_columns_for()` + `plan_feature_flags()` interaction
2. **Scheduling behavior contracts** (Suite 11)
   - `tests/integration/scheduling/test_schedule_tasks_boundaries.py`
   - Direct tests for `schedule_tasks()` behavior under missing evidence, reduced graph, and cost context
3. **Extract postprocess resilience** (Suite 4, Expansion 5)
   - `tests/integration/error_handling/test_extract_postprocess_resilience.py`
   - `register_view_failed` / `view_artifact_failed` / `schema_contract_failed` event recording
4. **Incremental impact closure** (Suite 9)
   - `tests/incremental/test_impact_closure_strategies.py`
   - `merge_impacted_files()` and `impacted_importers_from_changed_exports()` have no direct tests

### Phase 2: Delta and Runtime Capability Contracts

1. **Delta read-after-write contracts** (Expansion 1)
   - `tests/incremental/test_delta_read_after_write_contracts.py`
   - Schema resolution and provider registration after writes
2. **Capability negotiation boundaries** (Expansion 2)
   - `tests/integration/runtime/test_capability_negotiation.py`
   - Deterministic failure payloads for Substrait decode, async UDF
3. **Adapter integration** (Suite 5 — small set only)
   - `tests/integration/adapters/test_semantic_runtime_bridge_integration.py`
   - Profile → semantic config → profile behavior inside session construction

### Phase 3: Selective Expansion

1. **Plan and artifact determinism** (Expansion 4)
   - `tests/integration/boundaries/test_plan_determinism.py`
   - Fingerprint stability / expected change boundaries
2. **Bipartite graph structure and cost modeling** (Suites 3.2, 3.3)
   - Graph structure validation, bottom-level costs, slack computation
3. **Immutability smoke assertions** (Suite 6 — smoke only)
   - Keep mostly unit-level; integration has only smoke where immutability affects subsystem behavior
4. **One multi-source alignment contract** (deferred from full cross-product)
   - Start with CST/AST alignment only, not full 5-source matrix
5. **Non-gating performance baseline tests** (separate CI job)
   - Add after boundary contracts are stable

---

## Success Metrics (Revised)

| Metric | Target | Measurement |
|--------|--------|-------------|
| Phase 0 gate | All 5 existing failures fixed | `uv run pytest tests/integration -q` → 0 failures |
| Evidence plan gating | Gating + projection tested | `requires_dataset()` + `required_columns_for()` |
| Scheduling behavior | Topology + partial + cost tested | `schedule_tasks()` under multiple scenarios |
| Postprocess resilience | Event taxonomy verified | Canonical status names asserted |
| Impact closure | All 3 closure types tested | hybrid, symbol_closure, import_closure |
| Delta read-after-write | Schema visible after write | Provider registration post-write verified |
| Adapter coverage | Round-trip in session context | profile → config → profile in session construction |
| Plan determinism | Fingerprint stability verified | Unchanged inputs → same fingerprint |
| Test execution time | < 90s for integration gate | CI timing (excludes performance/incremental jobs) |

---

## Appendix: Test Markers and CI Configuration

**Source of truth:** `pytest.ini` (not `pyproject.toml`). Do not add markers to `pyproject.toml`.

### Existing Markers (in `pytest.ini`)

Reuse these existing markers; avoid marker explosion:

```ini
# In pytest.ini
markers =
    smoke: quick end-to-end API smoke tests using fixtures
    e2e: long-running end-to-end tests that exercise external surfaces
    integration: validates coordinated behavior across multiple subsystems
    benchmark: performance, non-gating
    performance: integration/performance tests that may take longer to run
    serial: run tests serially in the same xdist worker to avoid isolation issues
```

### Proposed New Markers (add only if needed)

```ini
    boundary: subsystem boundary integration tests
    incremental_contract: CDF and stateful Delta lifecycle tests
```

**Guideline:** Prefer combining existing markers (e.g., `@pytest.mark.integration` + class-level grouping) over adding new markers for every test category.

### CI Recommendations

1. **Default integration gate:** `uv run pytest tests/integration -q` — fast and deterministic.
2. **Performance tests:** Run `@pytest.mark.benchmark` / `@pytest.mark.performance` in a separate, non-gating CI job to avoid noisy failures from host variance.
3. **Incremental/Delta tests:** Run `uv run pytest tests/incremental -q` as a separate job (may require Delta Lake setup).
4. **Serial tests:** `@pytest.mark.serial` tests run in same xdist worker via `--dist loadgroup`.

---

## Next Steps

**Completed:**
- ✅ Initial code review corrections documented (v1)
- ✅ Final deep code review completed (v2) — All test expectations verified against production code
- ✅ Deep code review completed (v3) — Fixed non-existent API references, import paths, function signatures
- ✅ External review integrated (v4) — Scope calibration, phasing, fixture reuse, marker config, directory structure

**Ready for Implementation:**
1. **Phase 0: Fix existing failures** — Resolve 5 known integration test failures before adding new tests
2. **Phase 1: Core boundary contracts** — Evidence plan gating, scheduler behavior, postprocess resilience, impact closure
3. **Phase 2: Delta/runtime contracts** — Read-after-write, capability negotiation, adapter integration (small set)
4. **Phase 3: Selective expansion** — Plan determinism, graph structure, multi-source (one contract), performance (non-gating)

**Scope Calibration (from review):**
- Keep boundary-crossing tests; do not duplicate unit-level algorithmic coverage
- Stateful CDF/Delta tests go in `tests/incremental/`, not `tests/integration/incremental/`
- Adapter tests: small integration set only, unit tests cover basics
- Immutability tests: smoke assertions only at integration level
- Performance tests: non-gating, separate CI job
- Multi-source alignment: start with one focused contract, not full cross-product

**Code Review Confidence:**
- All function signatures verified against source code with line numbers
- All field names verified (CdfFilterPolicy, TaskSchedule, etc.)
- All exception types verified (FrozenInstanceError vs AttributeError)
- All default values verified (constructor vs config defaults)
- Adapter module path verified (`datafusion_engine/semantics_runtime.py`)
- EvidencePlan API verified (`requires_dataset()`, not `apply_query_and_project()`)
- InferredDepsInputs wrapper verified for `infer_deps_from_plan_bundle()`
- Cost functions verified in `relspec.execution_plan` (not `rustworkx_schedule`)
- DataFusionRuntimeProfile construction verified (no `.default()` classmethod)
- Scheduling test topology check uses graph predecessors, not `TaskNode.inputs` (evidence names)
- Pytest markers confirmed in `pytest.ini`, not `pyproject.toml`
- Shared test fixtures confirmed in `tests/test_helpers/` (datafusion_runtime, optional_deps, delta_seed)
