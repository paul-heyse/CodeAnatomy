# Expanded Integration Testing Implementation Plan (v2)

Builds on the initial integration testing proposal (`docs/plans/integration_testing_proposal.md`) and integrates corrections from the v2 review (`docs/plans/expanded_integration_testing_v2_review.md`).

---

## 1. Coverage Audit: Current State

### 1.1 Implemented Test Suites (from Proposal v1)

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

### 1.2 Additional Integration Tests (Outside Proposal)

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

- Delta read-after-write schema contracts
- CDF merge strategy edge cases
- Impact closure strategies (`merge_impacted_files()` with closure modes)
- Multi-source byte-span canonicalization
- Expression validation graceful skip
- Streaming row tracking at scale (>100k rows)
- Delta pin conflicts (conflicting versions for same dataset)
- Bytecode integration (bytecode extractor with semantic pipeline)

---

## 2. Identified Coverage Gaps (Revised)

Analysis using CQ structural queries across the full codebase, cross-referenced against the v2 review findings. Each gap is classified by risk and mapped to verified code locations.

### 2.1 Gap Summary (Updated per Review)

| # | Integration Boundary | Risk | Primary Code Location | Current Coverage |
|---|---------------------|------|----------------------|-----------------|
| G1 | `compile_execution_plan` end-to-end boundary | **CRITICAL** | `relspec/execution_plan.py:452` (1 caller: `driver_factory.py:527`) | **None** |
| G2 | `materialize_extract_plan` boundary | **CRITICAL** | `extract/coordination/materialization.py:595` (24 callsites / 13 files) | **None** |
| G3 | `resolve_dataset_provider` boundary | **CRITICAL** | `datafusion_engine/dataset/resolution.py:74` (7 callsites / 7 files) | **Partial** (smoke only) |
| G4 | Build orchestration lifecycle | HIGH | `graph/product_build.py:131` (heartbeat/signal/phase) | **None** |
| G5 | `execute_pipeline` diagnostics | HIGH | `hamilton_pipeline/execution.py:367` (`plan_execution_diff_v1`) | **None** |
| G6 | Idempotent commit propagation | HIGH | `storage/deltalake/delta.py:3281` (3 callsites / 3 files) | **Partial** |
| G7 | Multi-source byte-span canonicalization | HIGH | `semantics/scip_normalize.py` | **None** |
| G8 | Semantic compiler + UDF availability | MEDIUM | `semantics/compiler.py`, `datafusion_engine/udf/runtime.py` | **Partial** |
| G9 | Extraction to normalization pipeline | MEDIUM | `extract/` to `semantics/` | **Partial** |

**Review-driven changes:**
- G1 elevated to CRITICAL: single choke-point, zero integration tests.
- G2 added as CRITICAL: high fan-out (24 callsites), no direct boundary tests.
- G3 added as CRITICAL: critical provider resolution choke-point.
- G4 split from old G1: pipeline ownership correction per review Section 3.1.
- G5 split from old G1: `plan_execution_diff_v1` has no test coverage.
- G6 expanded per review Addition F: multi-operation propagation.

---

## 3. Proposed Test Suites

### Phase 1: P0 - Critical Choke-Points (G1, G2, G3)

These suites target the highest-risk, highest-fan-out boundaries with zero current coverage.

---

#### Suite 3.1: `compile_execution_plan` End-to-End Boundary

**Boundary:** `ViewNode` sequence -> `InferredDeps` -> `TaskGraph` -> `TaskSchedule` -> `ExecutionPlan`

**File:** `tests/integration/relspec/test_compile_execution_plan.py`

**Verified code references (via CQ):**
- `compile_execution_plan(*, session_runtime, request)` at `relspec/execution_plan.py:452`
- `ExecutionPlanRequest` at `relspec/execution_plan.py:129` (7 fields: `view_nodes`, `snapshot`, `runtime_profile`, `requested_task_names`, `impacted_task_names`, `allow_partial`, `enable_metric_scheduling`)
- `ExecutionPlan` at `relspec/execution_plan.py:84` (39 fields, frozen dataclass)
- Single caller: `_compile_plan` in `hamilton_pipeline/driver_factory.py:527`
- Delta pin conflict: `_scan_unit_delta_pins()` at `relspec/execution_plan.py:299` (function def; first body line at :313)
- Plan bundle validation: `_validated_view_nodes()` at `relspec/execution_plan.py:529`
- Plan artifact storage failure: inline block at `relspec/execution_plan.py:678` calls `persist_plan_artifacts_for_views()` (from `datafusion_engine.plan.artifact_store`) at :685; emits `plan_artifacts_store_failed_v1` on failure

**Error paths tested:**
1. Runtime-profile mismatch (`relspec/execution_plan.py:633`): `ValueError("ExecutionPlanRequest runtime_profile must match the SessionRuntime profile.")`
2. Missing `plan_bundle` for requested tasks (`relspec/execution_plan.py:535`): `ValueError("Requested tasks are missing plan_bundle: [...].")`
3. No plan bundles at all (`relspec/execution_plan.py:542`): `ValueError("Execution plan requires view nodes with plan_bundle.")`
4. Delta pin conflict (`relspec/execution_plan.py:323`): `ValueError("Conflicting Delta pins for {dataset!r}: {a} vs {b}")` (also checked in `datafusion_engine/plan/bundle.py:255`)
5. Plan artifact store failure (`relspec/execution_plan.py:685`): caught internally via `persist_plan_artifacts_for_views()`, recorded as `plan_artifacts_store_failed_v1` artifact, compilation continues.

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_compile_execution_plan_basic_roundtrip` | Compile with 3 realistic view nodes (with plan bundles), verify full `ExecutionPlan` structure. | `plan.task_graph` has 3 nodes, `plan.task_schedule.waves` >= 1, `plan.plan_signature` is non-empty string, all 39 fields populated. |
| `test_compile_determinism` | Compile same inputs twice, verify identical output. | `plan1.plan_signature == plan2.plan_signature`, `plan1.task_dependency_signature == plan2.task_dependency_signature`. |
| `test_runtime_profile_mismatch_raises` | Pass `ExecutionPlanRequest.runtime_profile` != `session_runtime.profile`. | `pytest.raises(ValueError, match="runtime_profile must match")`. |
| `test_missing_plan_bundle_for_requested_task_raises` | Include task in `requested_task_names` whose view node lacks `plan_bundle`. | `pytest.raises(ValueError, match="missing plan_bundle")`. |
| `test_no_plan_bundles_raises` | All view nodes have `plan_bundle=None`. | `pytest.raises(ValueError, match="requires view nodes with plan_bundle")`. |
| `test_delta_pin_conflict_raises` | Two scan units for same dataset with different `(delta_version, timestamp)` tuples. | `pytest.raises(ValueError, match="Conflicting Delta pins")`. |
| `test_delta_pin_conflict_same_version_ok` | Two scan units for same dataset with identical pin tuples. | No error; `plan.scan_unit_delta_pins` has one entry. |
| `test_requested_task_filtering` | Set `requested_task_names` to subset; verify pruned plan. | Only requested tasks in `plan.active_tasks`, `plan.task_schedule` only schedules those tasks. |
| `test_plan_artifacts_store_failure_continues` | Mock `persist_plan_artifacts_for_views` to raise; verify compilation still succeeds and `plan_artifacts_store_failed_v1` artifact is emitted. | `plan` is valid `ExecutionPlan`, diagnostics contain `plan_artifacts_store_failed_v1`. |
| `test_plan_signature_changes_with_runtime_config` | Compile with two different `SemanticRuntimeConfig` values. | `plan1.plan_signature != plan2.plan_signature`, but `plan1.task_dependency_signature == plan2.task_dependency_signature`. |

**Representative fixture pattern:**

```python
@pytest.fixture
def realistic_view_nodes(
    datafusion_session_with_udfs: SessionContext,
) -> tuple[ViewNode, ...]:
    """Build 3 view nodes with plan bundles from a real DataFusion session."""
    from datafusion_engine.plan.bundle import compile_plan_bundle
    from relspec.task_catalog import TaskSpec

    specs = [
        TaskSpec(name="cst_refs", builder=_cst_refs_builder),
        TaskSpec(name="ast_defs", builder=_ast_defs_builder),
        TaskSpec(name="symtable_scopes", builder=_symtable_scopes_builder),
    ]
    nodes = []
    for spec in specs:
        df = spec.builder(datafusion_session_with_udfs)
        bundle = compile_plan_bundle(df, task_name=spec.name)
        nodes.append(ViewNode(name=spec.name, plan_bundle=bundle, ...))
    return tuple(nodes)
```

**Representative test pattern:**

```python
@pytest.mark.integration
def test_delta_pin_conflict_raises(
    session_runtime: SessionRuntime,
    realistic_view_nodes: tuple[ViewNode, ...],
) -> None:
    """Conflicting scan-unit Delta pins for one dataset raise ValueError."""
    from relspec.execution_plan import (
        ExecutionPlanRequest,
        compile_execution_plan,
    )

    # Inject two scan units with conflicting pins for same dataset
    conflicting_units = [
        ScanUnit(dataset_name="raw_files", delta_version=5, ...),
        ScanUnit(dataset_name="raw_files", delta_version=7, ...),
    ]
    request = ExecutionPlanRequest(
        view_nodes=realistic_view_nodes,
        snapshot=session_runtime.snapshot,
        runtime_profile=session_runtime.profile,
    )
    # Inject scan units into view nodes' plan bundles
    with pytest.raises(ValueError, match="Conflicting Delta pins"):
        compile_execution_plan(
            session_runtime=session_runtime,
            request=request,
        )
```

---

#### Suite 3.2: `materialize_extract_plan` Boundary

**Boundary:** `DataFusionPlanBundle` -> `materialize_extract_plan()` -> Arrow output + artifact emissions

**File:** `tests/integration/extraction/test_materialize_extract_plan.py`

**Verified code references (via CQ):**
- `materialize_extract_plan(name, plan, *, runtime_profile, determinism_tier, options)` at `extract/coordination/materialization.py:595`
- `apply_query_and_project(request)` at `extract/coordination/materialization.py:293`
- `_ExtractProjectionRequest` at `extract/coordination/materialization.py:78` (6 fields: `name`, `table`, `session`, `normalize`, `evidence_plan`, `repo_id`)
- `EvidencePlan` at `extract/coordination/evidence_plan.py:20` (API: `requires_dataset()`, `required_columns_for()`, `requires_template()`)
- `plan_requires_row()` in `extract/coordination/spec_helpers.py:62` (takes `EvidencePlan` and `ExtractMetadata` objects; re-exported via `extract/coordination/__init__.py`)
- `compile_evidence_plan()` at `extract/coordination/evidence_plan.py:74`
- Artifact emissions: `extract_plan_compile_v1`, `extract_plan_execute_v1`, `extract_udf_parity_v1`
- 24 callsites across 13 files (CQ `calls materialize_extract_plan`)

**Key behaviors tested:**
1. **Projection/gating via evidence plan**: `apply_query_and_project` calls `plan_requires_row()` to gate datasets, then `required_columns_for()` to project columns.
2. **Streaming vs non-streaming**: Controlled by `_streaming_supported_for_extract()` + `options.prefer_reader`.
3. **Artifact emission**: Three diagnostics artifacts emitted per extract.
4. **Empty plan handling**: When evidence plan gates a dataset out, returns empty plan bundle.
5. **Normalization context**: `_build_normalization_context()` creates per-extract normalization.

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_materialize_basic_roundtrip` | Materialize a single extract plan (e.g. `cst_refs`), verify Arrow output. | Result is `pa.Table` or `pa.RecordBatchReader`, has expected columns (`bstart`, `bend`, `file_id`, etc.), row count > 0. |
| `test_gated_dataset_returns_empty` | Create `EvidencePlan` that excludes a dataset, verify empty output. | Result table has 0 rows, schema matches expected output schema. |
| `test_column_projection_enforced` | Create `EvidencePlan` with restricted `required_columns`; verify only those columns in output. | `set(result.column_names) == required_columns ∪ join_keys ∪ derived`. |
| `test_artifact_emissions` | Capture diagnostics events during materialization. | Events include `extract_plan_compile_v1` (with plan fingerprint), `extract_plan_execute_v1` (with row count), `extract_udf_parity_v1`. |
| `test_streaming_vs_table_materialization` | Materialize with `prefer_reader=True` and `prefer_reader=False`. | Reader path returns `RecordBatchReader`, table path returns `pa.Table`; both produce identical data. |
| `test_normalization_applied` | Verify normalization context transforms column values. | Byte-span columns are in canonical byte offsets after normalization. |

**Representative test pattern:**

```python
@pytest.mark.integration
def test_gated_dataset_returns_empty(
    datafusion_session: SessionContext,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Evidence plan gating excludes a dataset -> empty table returned."""
    from extract.coordination.evidence_plan import compile_evidence_plan
    from extract.coordination.materialization import (
        apply_query_and_project,
    )

    # Compile an evidence plan that only requires "ast_defs"
    plan = compile_evidence_plan(
        rules={"ast_defs": {"enabled": True}},
        extra_sources=frozenset(),
    )
    # "cst_refs" is NOT in the plan's sources
    assert not plan.requires_dataset("cst_refs")

    # apply_query_and_project for "cst_refs" should return empty
    request = _ExtractProjectionRequest(
        name="cst_refs",
        table=cst_refs_table,
        session=session,
        normalize=None,
        evidence_plan=plan,
        repo_id="test-repo",
    )
    bundle = apply_query_and_project(request)
    # Empty plan bundle indicates gated-out dataset
    result = _execute_plan_bundle(bundle)
    assert result.num_rows == 0
```

---

#### Suite 3.3: `resolve_dataset_provider` Boundary

**Boundary:** `DatasetResolutionRequest` -> `resolve_dataset_provider()` -> `DatasetResolution`

**File:** `tests/integration/storage/test_resolve_dataset_provider.py`

**Verified code references (via CQ):**
- `resolve_dataset_provider(request)` at `datafusion_engine/dataset/resolution.py:74`
- `DatasetResolutionRequest` at `datafusion_engine/dataset/resolution.py:44` (6 fields: `ctx`, `location`, `runtime_profile`, `name`, `predicate`, `scan_files`)
- `DatasetResolution` at `datafusion_engine/dataset/resolution.py:56` (13 fields: `name`, `location`, `provider`, `provider_kind`, `delta_snapshot`, `delta_scan_config`, `delta_scan_effective`, `delta_scan_snapshot`, `delta_scan_identity_hash`, `delta_scan_options`, `add_actions`, `predicate_error`, `cdf_options`)
- `DatasetProviderKind = Literal["delta", "delta_cdf"]` at `datafusion_engine/dataset/resolution.py:40`
- `_resolve_delta_table()` at line 103: builds contract, requests provider bundle, validates gate
- `_resolve_delta_cdf()` at line 153: builds CDF contract, requests CDF provider
- Error wrapping at line 150 (raise) inside `_delta_provider_bundle`: catches `DataFusionEngineError|RuntimeError|TypeError|ValueError` at line 146, wraps as `DataFusionEngineError(kind=ErrorKind.PLUGIN)` at line 150; second occurrence at line 163 inside `_resolve_delta_cdf`
- 7 callsites across 7 files (CQ `calls resolve_dataset_provider`)

**Key behaviors tested:**
1. **Delta provider path**: `provider_kind="delta"` -> `_resolve_delta_table` -> contract + provider bundle + gate validation.
2. **Delta CDF provider path**: `provider_kind="delta_cdf"` -> `_resolve_delta_cdf` -> CDF contract + CDF provider.
3. **Error wrapping**: All control-plane errors wrapped as `DataFusionEngineError(kind=ErrorKind.PLUGIN)`.
4. **Predicate error capture**: Invalid SQL predicate captured in `resolution.predicate_error`.
5. **Scan identity hash**: `delta_scan_identity_hash` is deterministic for same table state.
6. **Snapshot key**: Resolution captures Delta protocol version and snapshot metadata.

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_resolve_delta_provider_roundtrip` | Write Delta table, resolve as `provider_kind="delta"`. | `resolution.provider_kind == "delta"`, `resolution.provider` is `TableProviderCapsule`, `resolution.delta_snapshot` is dict with protocol version. |
| `test_resolve_delta_cdf_provider` | Write Delta table with CDF enabled, resolve as `provider_kind="delta_cdf"`. | `resolution.provider_kind == "delta_cdf"`, `resolution.cdf_options` is not None. |
| `test_resolution_captures_snapshot_key` | Resolve provider, verify snapshot metadata. | `resolution.delta_scan_identity_hash` is non-empty string, `resolution.delta_snapshot` contains version info. |
| `test_scan_identity_hash_deterministic` | Resolve same table twice, verify hash stability. | `resolution1.delta_scan_identity_hash == resolution2.delta_scan_identity_hash`. |
| `test_invalid_predicate_captured` | Pass invalid SQL predicate (`"not_a_column > 5"`). | `resolution.predicate_error` is non-None string describing the error. |
| `test_control_plane_failure_wraps_error` | Corrupt Delta table location, attempt resolution. | `pytest.raises(DataFusionEngineError)`, `exc.kind == ErrorKind.PLUGIN`. |
| `test_scan_files_override` | Pass explicit `scan_files` list; verify provider uses file pruning. | Resolution succeeds, `resolution.add_actions` reflects pruned file set. |

**Representative test pattern:**

```python
@pytest.mark.integration
def test_resolve_delta_provider_roundtrip(
    tmp_path: Path,
    datafusion_ctx: SessionContext,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Write a Delta table and resolve it through the provider pipeline."""
    from datafusion_engine.dataset.resolution import (
        DatasetResolutionRequest,
        resolve_dataset_provider,
    )

    # Write a small Delta table
    table_path = tmp_path / "test_table"
    _write_delta_table(table_path, rows=[{"id": 1, "label": "a"}])

    location = DatasetLocation(
        path=str(table_path),
        format="delta",
        resolved=_resolved_spec(table_path),
    )
    request = DatasetResolutionRequest(
        ctx=datafusion_ctx,
        location=location,
        runtime_profile=runtime_profile,
        name="test_table",
    )
    resolution = resolve_dataset_provider(request)

    assert resolution.provider_kind == "delta"
    assert resolution.provider is not None
    assert resolution.delta_snapshot is not None
    assert isinstance(resolution.delta_scan_identity_hash, str)
    assert resolution.delta_scan_identity_hash  # non-empty
```

---

### Phase 2: P1 - Build Orchestration and Diagnostics (G4, G5, G6)

---

#### Suite 3.4: Build Orchestration Lifecycle

> **Review correction (Section 3.1):** Heartbeat/signal/build-phase assertions belong to `build_graph_product()` / `_execute_build()`, NOT `execute_pipeline()`. `execute_pipeline()` handles Hamilton execute/materialize and plan diff artifacts only. CQ confirms: heartbeat start at `product_build.py:159`, stop at `product_build.py:193`; signal handlers at lines 163-164/191; phase events at lines 296/309/326.

**Boundary:** `GraphProductBuildRequest` -> `build_graph_product()` -> heartbeat + signal handlers + build-phase diagnostics

**File:** `tests/integration/pipeline/test_build_orchestration.py`

**Verified code references (via CQ):**
- `build_graph_product(request)` at `graph/product_build.py:131` -> `GraphProductBuildResult` (16 fields)
- `GraphProductBuildRequest` at `graph/product_build.py:98` (21 fields)
- Heartbeat: `start_build_heartbeat(run_id=..., interval_s=5.0)` at line 159, `heartbeat.stop(timeout_s=2.0)` at line 193
- Signal handlers: `signal.signal(signal.SIGTERM, handler)` at line 163, `signal.signal(signal.SIGINT, handler)` at line 164, restore at line 191
- `_signal_handler_for_build._handler` closure captures: `signal_state`, `run_bundle_dir`, `run_id` (via CQ `scopes`)
- Diagnostics events: `build.start` (line 147), `build.failure` (line 168), `build.success` (line 180), `build.phase.start` (line 296), `build.phase.end` (lines 309/326)
- `_execute_build()` at line 282: wraps `execute_pipeline()` call at line 306

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_heartbeat_starts_before_execution` | Verify heartbeat is active during build. | `start_build_heartbeat` called before `_execute_build`, heartbeat.stop() in finally. |
| `test_signal_handlers_installed_and_restored` | Verify SIGTERM/SIGINT handlers are installed during build and restored after. | Before build: default handlers. During: custom `_handler`. After: original handlers restored. |
| `test_signal_handler_captures_correct_state` | Verify the signal handler closure captures `run_id` and `run_bundle_dir`. | Handler's `signal_state` dict, `run_bundle_dir`, `run_id` match build request values. |
| `test_build_phase_start_and_end_events` | Verify `build.phase.start` and `build.phase.end` events emitted for each execution phase. | Diagnostics events captured with correct phase names and timing. |
| `test_build_failure_records_diagnostics` | Inject failure into build; verify `build.failure` event with exception info. | Event payload has `status="error"`, exception type and message recorded. |
| `test_build_success_records_diagnostics` | Successful build emits `build.success` event. | Event payload has `status="ok"`, timing fields present. |

**Representative test pattern:**

```python
@pytest.mark.integration
def test_signal_handlers_installed_and_restored(
    minimal_python_repo: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Signal handlers are installed during build and restored after."""
    import signal

    from graph.product_build import (
        GraphProductBuildRequest,
        build_graph_product,
    )

    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)

    captured_handlers: dict[str, object] = {}

    # Monkeypatch _execute_build to capture handlers mid-build
    original_execute = _execute_build

    def capturing_execute(*args, **kwargs):
        captured_handlers["sigterm"] = signal.getsignal(signal.SIGTERM)
        captured_handlers["sigint"] = signal.getsignal(signal.SIGINT)
        return original_execute(*args, **kwargs)

    monkeypatch.setattr("graph.product_build._execute_build", capturing_execute)

    request = GraphProductBuildRequest(repo_root=str(minimal_python_repo))
    build_graph_product(request)

    # During build: custom handlers installed
    assert captured_handlers["sigterm"] is not original_sigterm
    assert captured_handlers["sigint"] is not original_sigint
    # After build: original handlers restored
    assert signal.getsignal(signal.SIGTERM) is original_sigterm
    assert signal.getsignal(signal.SIGINT) is original_sigint
```

---

#### Suite 3.5: `execute_pipeline` Diagnostics and `plan_execution_diff_v1`

> **Review Addition C:** `plan_execution_diff_v1` is emitted in pipeline execution but has no test coverage. CQ confirms: no test references found for `plan_execution_diff_v1` in tests.

**Boundary:** `execute_pipeline()` results -> `_emit_plan_execution_diff()` -> `plan_execution_diff_v1` artifact

**File:** `tests/integration/pipeline/test_pipeline_diagnostics.py`

**Verified code references (via CQ):**
- `execute_pipeline(*, repo_root, options)` at `hamilton_pipeline/execution.py:256`
- `PipelineExecutionOptions` at `hamilton_pipeline/execution.py:55` (14 fields including `use_materialize: bool = True`)
- `_emit_plan_execution_diff(results)` at `hamilton_pipeline/execution.py:367`
- Payload has 16 fields: `run_id`, `timestamp_ns`, `plan_signature`, `task_dependency_signature`, `reduced_task_dependency_signature`, `expected_task_count`, `executed_task_count`, `missing_task_count`, `unexpected_task_count`, `missing_tasks`, `unexpected_tasks`, `missing_task_fingerprints`, `missing_task_signatures`, `blocked_datasets`, `blocked_scan_units`
- `_base_task_name()` at line 408: strips `:variant` suffix from task names

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_plan_execution_diff_emitted_on_success` | Run pipeline to completion; verify `plan_execution_diff_v1` artifact emitted. | Artifact present in diagnostics, `missing_task_count == 0`, `unexpected_task_count == 0`. |
| `test_plan_execution_diff_missing_tasks` | Inject partial execution (some tasks not executed); verify missing tasks reported. | `missing_tasks` list is non-empty, `missing_task_fingerprints` has entries for each missing task. |
| `test_plan_execution_diff_unexpected_tasks` | Inject extra task execution not in plan; verify unexpected tasks reported. | `unexpected_tasks` list is non-empty. |
| `test_plan_execution_diff_payload_shape` | Verify all 16 payload fields present with correct types. | All fields present, `run_id` is string, `timestamp_ns` is int, task counts are ints, task lists are lists of strings. |
| `test_plan_execution_diff_blocked_datasets` | Missing task has associated scan units; verify `blocked_datasets` and `blocked_scan_units` populated. | `blocked_datasets` is non-empty list, `blocked_scan_units` reflects affected scan units. |
| `test_pipeline_partial_output_handling` | Pipeline with some failing nodes; verify partial results recorded. | Results mapping has entries for successful nodes, failing nodes recorded in diagnostics. |

**Representative test pattern:**

```python
@pytest.mark.integration
def test_plan_execution_diff_payload_shape(
    minimal_python_repo: Path,
    captured_diagnostics: list[dict],
) -> None:
    """plan_execution_diff_v1 has all 16 expected fields."""
    from hamilton_pipeline.execution import execute_pipeline

    execute_pipeline(repo_root=minimal_python_repo)

    diff_events = [
        e for e in captured_diagnostics
        if e.get("event_name") == "plan_execution_diff_v1"
    ]
    assert len(diff_events) == 1
    payload = diff_events[0]["payload"]

    # Verify all 16 fields present
    required_fields = {
        "run_id", "timestamp_ns", "plan_signature",
        "task_dependency_signature", "reduced_task_dependency_signature",
        "expected_task_count", "executed_task_count",
        "missing_task_count", "unexpected_task_count",
        "missing_tasks", "unexpected_tasks",
        "missing_task_fingerprints", "missing_task_signatures",
        "blocked_datasets", "blocked_scan_units",
    }
    assert required_fields <= set(payload.keys())
    assert isinstance(payload["run_id"], str)
    assert isinstance(payload["timestamp_ns"], int)
    assert isinstance(payload["missing_tasks"], list)
```

---

#### Suite 3.6: Idempotent Write Propagation

> **Review Addition F:** `idempotent_commit_properties` is reused by write, delete, and snapshot merge flows.

**Boundary:** `IdempotentWriteOptions` -> `idempotent_commit_properties()` -> Delta commit deduplication across write/delete/merge flows

**File:** `tests/integration/storage/test_idempotent_write_contracts.py`

**Verified code references (via CQ):**
- `idempotent_commit_properties(*, operation, mode, idempotent, extra_metadata)` at `storage/deltalake/delta.py:3281`
- Returns `CommitProperties` with `app_transactions` (for dedup) and `custom_metadata` (for audit)
- `IdempotentWriteOptions` at `storage/deltalake/delta.py` (fields: `app_id`, `version`)
- 3 callsites:
  - `WritePipeline._delta_write_spec` in `datafusion_engine/io/write.py` (write path)
  - `incremental/delta_updates.py` (delete path)
  - `incremental/snapshot.py` (merge path)
- Error: `RuntimeError("idempotent_commit_properties requires commit metadata.")` at line 3313

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_idempotent_write_deduplication` | Write with `app_id="run_1", version=1` twice; verify single Delta version increment. | After two writes, `delta_table.version()` incremented only once. |
| `test_idempotent_different_versions_both_commit` | Write `version=1` then `version=2`; verify both commits recorded. | `delta_table.version()` incremented twice, history shows both app transactions. |
| `test_commit_metadata_includes_operation_and_mode` | Verify `codeanatomy_operation` and `codeanatomy_mode` keys in commit metadata. | Commit info metadata contains expected keys with correct values. |
| `test_extra_metadata_propagated` | Pass `extra_metadata={"run_id": "abc"}`; verify in commit. | Commit metadata contains `run_id: "abc"`. |
| `test_no_idempotent_options_still_requires_metadata` | Call with `idempotent=None` but valid operation/mode. | Returns valid `CommitProperties` (no app_transaction, but metadata present). |
| `test_write_pipeline_propagates_idempotent` | Use `WritePipeline` with `IdempotentWriteOptions`; verify commit properties reach Delta. | Written table's commit history includes app transaction with expected `app_id` and `version`. |
| `test_incremental_delete_propagates_idempotent` | Incremental delete with idempotent options; verify commit dedup. | Delete commit includes app transaction for deduplication. |
| `test_schema_evolution_with_idempotent_write` | Write A,B then A,B,C with schema evolution + idempotent; verify schema evolves. | Final schema has columns A,B,C; idempotent dedup still works. |

**Representative test pattern:**

```python
@pytest.mark.integration
def test_idempotent_write_deduplication(tmp_path: Path) -> None:
    """Duplicate idempotent write produces only one version increment."""
    from storage.deltalake.delta import (
        IdempotentWriteOptions,
        idempotent_commit_properties,
    )

    table_path = tmp_path / "idempotent_table"
    data = pa.table({"id": [1, 2], "value": ["a", "b"]})

    idempotent = IdempotentWriteOptions(app_id="test_run", version=1)
    props = idempotent_commit_properties(
        operation="write",
        mode="overwrite",
        idempotent=idempotent,
    )

    # First write
    write_deltalake(str(table_path), data, mode="overwrite", commit_properties=props)
    v1 = DeltaTable(str(table_path)).version()

    # Second write with same idempotent options
    write_deltalake(str(table_path), data, mode="overwrite", commit_properties=props)
    v2 = DeltaTable(str(table_path)).version()

    # Only one version increment due to app_transaction dedup
    assert v2 == v1
```

---

### Phase 3: P2 - Semantic Boundaries and Span Normalization (G7, G8, G9)

---

#### Suite 3.7: Cross-Extractor Byte-Span Consistency

**Boundary:** Multiple extractors (CST, AST, tree-sitter) -> `scip_to_byte_offsets()` -> canonical `bstart`/`bend`

**File:** `tests/integration/extraction/test_byte_span_canonicalization.py`

**Verified code references (via CQ):**
- `scip_to_byte_offsets(ctx, *, occurrences_table, line_index_table)` at `semantics/scip_normalize.py:35`
- UDF requirements: `col_to_byte`, `span_make` (validated via `validate_required_udfs` at line 87)
- CST extractor: `col_unit="utf32"` in `extract/extractors/cst_extract.py`
- AST extractor: line/col offsets in `extract/extractors/ast_extract.py`
- `file_line_index_v1` table provides line offset mapping for normalization

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_byte_span_consistency_cst_vs_ast` | Extract same Python file via CST and AST; normalize both; compare overlapping entities. | For shared entities (function defs), `bstart`/`bend` match within tolerance. |
| `test_byte_span_consistency_cst_vs_tree_sitter` | Extract same file via CST and tree-sitter; verify identical byte spans. | Byte spans identical for same-entity rows. |
| `test_col_unit_utf32_to_byte_conversion` | File with multi-byte Unicode chars; CST `col_unit="utf32"`; verify byte offset conversion. | Byte offsets account for multi-byte encoding. |
| `test_span_normalization_with_missing_udf` | Attempt span normalization without `col_to_byte` UDF. | `ValueError` with clear UDF-missing message. |
| `test_span_normalization_crlf_vs_lf` | Files with `\r\n` and `\n` line endings; verify byte offsets correct. | Byte offsets correctly account for CR characters. |
| `test_file_line_index_alignment` | Verify `file_line_index_v1` line boundaries match actual file content. | Line start byte offsets match `content.split(b'\n')` boundaries. |

---

#### Suite 3.8: Semantic Compiler and UDF Availability

> **Review Optimization 1:** Keep only cases not covered by existing `test_semantic_pipeline.py` (lines 44, 58, 79, 198). Focus on type mismatch, missing typed columns, and error payload quality.

**Boundary:** `SemanticCompiler` join operations -> DataFusion type system -> UDF availability -> error quality

**File:** `tests/integration/semantics/test_compiler_type_validation.py`

**Verified code references (via CQ):**
- `SemanticCompiler._require_udfs(self, required)` at `semantics/compiler.py:215`
- `validate_required_udfs(snapshot, *, required)` at `datafusion_engine/udf/runtime.py:665`
- Error: `ValueError("Missing Rust UDF signature metadata for: [...].")` at `udf/runtime.py:692`
- Error: `ValueError("Missing Rust UDF return metadata for: [...].")` at `udf/runtime.py:695`
- 10 callsites for `validate_required_udfs` across 8 files (CQ `calls validate_required_udfs`)
- `infer_join_strategy()` at `semantics/joins/inference.py` (standalone function)

**Tests (non-overlapping with existing coverage):**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_join_key_type_mismatch_error_quality` | Left table `file_id: string`, right `file_id: int64`; verify error message quality. | Error message names both tables, the join key, and the mismatched types. |
| `test_missing_udf_signature_metadata` | UDF name in snapshot but missing from `signature_inputs`; verify error. | `ValueError` with "Missing Rust UDF signature metadata" naming the UDF. |
| `test_missing_udf_return_metadata` | UDF name in snapshot but missing from `return_types`; verify error. | `ValueError` with "Missing Rust UDF return metadata" naming the UDF. |
| `test_udf_alias_resolution` | UDF registered under alias; verify alias-to-canonical lookup. | `validate_required_udfs` succeeds when alias resolves to canonical name. |
| `test_relationship_failure_does_not_abort_pipeline` | One relationship fails; verify pipeline continues with remaining. | Failed relationship recorded in diagnostics; other relationships compiled successfully. |

---

#### Suite 3.9: Semantic Input Validation (Focused)

> **Review Optimization 1:** Merge/downscope to avoid overlap with `test_semantic_pipeline.py:44,58,79,198`.

**Boundary:** Extraction outputs -> `validate_semantic_inputs()` -> column validation -> `SemanticCompiler`

**File:** `tests/integration/semantics/test_input_schema_contracts.py`

**Existing coverage in `test_semantic_pipeline.py`:**
- `validate_semantic_inputs` at lines 44, 58, 79 (presence validation)
- `validate_semantic_input_columns` at line 198 (basic column validation)

**New tests (non-overlapping):**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_wrong_column_type_error_message_quality` | Register `cst_refs` with `bstart: string` instead of `int64`. | Error message names the table, column, expected type, and actual type. |
| `test_missing_required_typed_column_cross_module_error` | Missing column in semantic input from different module boundary. | Error payload includes source module and expected column spec. |
| `test_extra_columns_accepted_silently` | Register table with extra columns beyond spec. | Validation passes; extra columns not rejected. |

---

#### Suite 3.10: File Identity and Parallel Extraction

**Boundary:** `FileContext` -> parallel extractors -> consistent `file_id`/`path` across outputs

**File:** `tests/integration/extraction/test_file_identity_coordination.py`

**Verified code references (via CQ):**
- `FileContext` at `extract/coordination/context.py`
- `bytes_from_file_ctx()` at `extract/coordination/context.py`
- `parallel_map()` at `extract/infrastructure/parallel.py`

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_file_id_consistency_across_extractors` | Extract same file via CST, AST, symtable; verify identical `file_id`. | All output tables have matching `file_id` for same file. |
| `test_file_sha256_consistency` | Verify `file_sha256` matches across all extractors. | SHA256 values identical for same file content. |
| `test_parallel_extraction_partial_failure_resilience` | One extractor fails on specific file; others still produce output. | Successful extractors have valid output; error recorded in diagnostics. |
| `test_parallel_extraction_result_order_independence` | Two parallel runs produce identical schemas and row counts. | Output schemas match; row counts match. |

---

### Phase 4: P3 - Scheduling Refinement and End-to-End (existing gaps)

> **Review Optimization 2:** Avoid duplicating current scheduling coverage at `test_schedule_generation.py:222,277,316,350`.

---

#### Suite 3.11: Edge Validation in Scheduling Context

**Boundary:** `EvidenceCatalog` -> `validate_edge_requirements()` -> schedule ready-task filtering

**File:** `tests/integration/relspec/test_schedule_edge_validation.py`

**Tests (non-overlapping with existing scheduling tests):**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_schedule_with_selective_column_requirements` | Task A has required columns available; task B does not. | A scheduled, B in `missing_tasks`. |
| `test_catalog_mutation_unblocks_dependent` | After task A runs, its output columns unblock task B. | B schedulable in second wave. |
| `test_validation_summary_reflects_blocked_edges` | Blocked task's edge failures in `TaskSchedule.validation_summary`. | Summary has entries for each blocked edge. |

---

#### Suite 3.12: CDF Cursor Lifecycle (Focused)

> **Review Correction 4:** Keep only cursor <-> Delta/CDF runtime interaction. Unit coverage already exists for `get_start_version` and path sanitization at `test_cdf_cursors.py:239,268`.

**Boundary:** `CdfCursorStore` -> Delta CDF reads -> cursor update -> state progression

**File:** `tests/integration/storage/test_cdf_cursor_lifecycle.py`

**Tests (non-overlapping with unit coverage):**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_cursor_state_progression_through_cdf_reads` | Write Delta + CDF data; read via cursor; update; re-read; verify progression. | First read starts at `get_start_version()`; after update, next read starts at new version. |
| `test_cursor_recovery_after_corruption` | Write invalid cursor JSON; verify `load_cursor()` returns None; re-establish cursor. | Recovery path works; new cursor valid. |
| `test_concurrent_cursor_updates` | Two threads update same cursor simultaneously. | Final state is valid (last write wins). |

---

#### Suite 3.13: Dataset Resolution Contracts

> **Merged with Suite 3.3 content for non-P0 behaviors.**

**File:** `tests/integration/storage/test_dataset_resolution_contracts.py`

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_storage_options_merge_request_overrides_policy` | Conflicting storage options; verify request wins. | Merged options use request value. |
| `test_provider_artifacts_include_scan_metadata` | Provider artifacts have `delta_scan_identity_hash`. | Artifacts present and non-empty. |

---

#### Suite 3.14: Extraction to CPG Output Chain

> **Review Correction 3:** E2E tests go in `tests/e2e/`, NOT `tests/integration/e2e/`.

**File:** `tests/e2e/test_extraction_to_cpg.py`

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_single_file_repo_produces_cpg_nodes` | Full pipeline on 1-file repo. | `cpg_nodes` has rows with valid `entity_id`, `path`, `bstart`, `bend`. |
| `test_entity_ids_are_deterministic` | Build twice; verify identical `entity_id` values. | Sorted entity_id lists match. |
| `test_byte_spans_within_file_bounds` | All `bstart` >= 0 and `bend` <= file size. | No out-of-bounds spans. |
| `test_graceful_degradation_empty_file` | Empty `.py` file included; pipeline completes. | Correct-schema empty output for that file. |

**Marker:** `@pytest.mark.e2e`

---

#### Suite 3.15: Hamilton Materializer Contracts

> **Review Correction 2:** `TableSummarySaver` is metadata capture, NOT a Delta writer. CQ confirms: `save_data()` returns dict with `dataset_name`, `materialization`, `materialized_name`, `path`, `rows`, `columns`, `column_count`, `plan_signature`, `run_id`. It does NOT call any Delta write API.

**File:** `tests/integration/pipeline/test_materializer_contracts.py`

**Verified code references (via CQ):**
- `TableSummarySaver.save_data(data)` at `hamilton_pipeline/materializers.py:53` returns metadata dict
- `ParamTableSummarySaver.save_data(data)` at `hamilton_pipeline/materializers.py:71` (class def) returns param summary
- `build_hamilton_materializers()` at `hamilton_pipeline/materializers.py:126` produces 7 factories (6 `TableSummarySaver` + 1 `ParamTableSummarySaver`)
- `delta_output_specs()` returns 6 specs: `cpg_nodes`, `cpg_edges`, `cpg_props`, `cpg_props_map`, `cpg_edges_by_src`, `cpg_edges_by_dst`
- `validate_delta_output_payload()` at `hamilton_pipeline/io_contracts.py:169` validates 6 required keys

**Tests:**

| Test | Behavior Under Test | Key Assertions |
|------|-------------------|----------------|
| `test_table_summary_saver_metadata_payload` | Call `save_data()` with Arrow table; verify metadata dict. | Payload has all 9 keys: `dataset_name`, `materialization`, `materialized_name`, `path`, `rows`, `columns`, `column_count`, `plan_signature`, `run_id`. |
| `test_table_summary_saver_row_count_accuracy` | Pass 100-row table; verify `rows == 100`. | `payload["rows"] == 100`. |
| `test_param_table_summary_saver_mapping_required` | Pass non-mapping to `ParamTableSummarySaver.save_data()`; verify `TypeError`. | `pytest.raises(TypeError, match="expected a mapping")`. |
| `test_materializer_factory_count` | `build_hamilton_materializers()` returns exactly 7 factories. | `len(factories) == 7`. |
| `test_materializer_factory_wiring_against_delta_specs` | Each `TableSummarySaver` factory references a valid `delta_output_specs()` entry. | All 6 factory `dataset_name` values match a spec in `delta_output_specs()`. |
| `test_validate_delta_output_payload_accepts_valid` | Valid payload passes `validate_delta_output_payload()`. | No exception raised. |
| `test_validate_delta_output_payload_rejects_missing_key` | Payload missing `rows` key rejected. | `ValueError` raised naming missing key. |

**Representative test pattern:**

```python
@pytest.mark.integration
def test_table_summary_saver_metadata_payload() -> None:
    """TableSummarySaver.save_data returns correct metadata dict."""
    from hamilton_pipeline.materializers import TableSummarySaver

    data = pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]})
    saver = TableSummarySaver(
        dataset_name="cpg_nodes",
        materialization="delta",
        materialized_name="materialize_cpg_nodes",
        output_runtime_context=mock_runtime_ctx,
        output_plan_context=mock_plan_ctx,
    )
    payload = saver.save_data(data)

    assert payload["dataset_name"] == "cpg_nodes"
    assert payload["rows"] == 3
    assert payload["columns"] == ["id", "label"]
    assert payload["column_count"] == 2
    assert isinstance(payload["plan_signature"], str)
    assert isinstance(payload["run_id"], str)
```

---

## 4. Evidence Plan Gating Consolidation

> **Review Optimization 3:** Consolidate gating suites. Keep one gating suite (existing `test_evidence_plan_gating.py`) + one materialization suite (new Suite 3.2 above).

The existing suites at:
- `tests/integration/boundaries/test_evidence_plan_to_extractor.py`
- `tests/integration/error_handling/test_evidence_plan_gating.py`

cover flag/template projection logic. The new Suite 3.2 covers `apply_query_and_project` + `materialize_extract_plan` effects. No additional gating suite needed.

---

## 5. Test Infrastructure

### 5.1 New Fixtures

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `minimal_python_repo` | function | `tmp_path` with 1-3 Python files for extraction |
| `datafusion_session_with_udfs` | session | Session with Rust UDFs registered (for span normalization, plan compilation) |
| `realistic_view_nodes` | function | 3 `ViewNode` instances with real `DataFusionPlanBundle` |
| `session_runtime` | function | `SessionRuntime` for `compile_execution_plan` tests |
| `captured_diagnostics` | function | List capturing all `emit_diagnostics_event` calls |
| `delta_table_with_cdf` | function | Delta table with CDF enabled and multiple versions |

### 5.2 Test Helpers

| Helper | Status | Purpose |
|--------|--------|---------|
| `register_arrow_table()` | Exists | `tests/test_helpers/arrow_seed.py` |
| `df_profile()` | Exists | `tests/test_helpers/datafusion_runtime.py` |
| `run_delta_smoke_round_trip()` | Exists | `tests/harness/delta_smoke.py` |
| `build_minimal_extraction()` | NEW | Run extraction on minimal repo, return Arrow tables |
| `build_minimal_semantic_session()` | NEW | DataFusion session with extraction + normalization applied |
| `mock_diagnostics_collector()` | NEW | Captures diagnostics events for assertion |

### 5.3 Directory Structure

```
tests/integration/
├── adapters/                   # (existing)
├── boundaries/                 # (existing)
├── contracts/                  # (existing)
├── error_handling/             # (existing)
├── runtime/                    # (existing)
├── scheduling/                 # (existing)
├── pipeline/                   # NEW
│   ├── test_build_orchestration.py     # Suite 3.4
│   ├── test_pipeline_diagnostics.py    # Suite 3.5
│   └── test_materializer_contracts.py  # Suite 3.15
├── relspec/                    # NEW
│   ├── test_compile_execution_plan.py  # Suite 3.1
│   └── test_schedule_edge_validation.py # Suite 3.11
├── extraction/                 # NEW
│   ├── test_byte_span_canonicalization.py   # Suite 3.7
│   ├── test_file_identity_coordination.py  # Suite 3.10
│   └── test_materialize_extract_plan.py    # Suite 3.2
├── semantics/                  # NEW
│   ├── test_input_schema_contracts.py      # Suite 3.9
│   └── test_compiler_type_validation.py    # Suite 3.8
└── storage/                    # NEW
    ├── test_idempotent_write_contracts.py   # Suite 3.6
    ├── test_cdf_cursor_lifecycle.py         # Suite 3.12
    ├── test_resolve_dataset_provider.py     # Suite 3.3
    └── test_dataset_resolution_contracts.py # Suite 3.13
tests/e2e/
└── test_extraction_to_cpg.py   # Suite 3.14
```

---

## 6. Implementation Priority (Revised per Review)

### P0 (highest risk / highest fan-out / currently untested)

| Suite | Target | Tests | Effort |
|-------|--------|-------|--------|
| 3.1 | `compile_execution_plan` boundary | 10 | Medium |
| 3.2 | `materialize_extract_plan` boundary | 6 | Medium |
| 3.3 | `resolve_dataset_provider` boundary | 7 | Medium |

### P1

| Suite | Target | Tests | Effort |
|-------|--------|-------|--------|
| 3.4 | Build orchestration lifecycle | 6 | High |
| 3.5 | `execute_pipeline` diagnostics / `plan_execution_diff_v1` | 6 | Medium |
| 3.6 | Idempotent commit propagation | 8 | Medium |

### P2

| Suite | Target | Tests | Effort |
|-------|--------|-------|--------|
| 3.7 | Cross-extractor byte-span consistency | 6 | High |
| 3.8 | Semantic compiler / UDF validation | 5 | Low |
| 3.9 | Semantic input validation (focused) | 3 | Low |
| 3.10 | File identity / parallel extraction | 4 | Medium |

### P3

| Suite | Target | Tests | Effort |
|-------|--------|-------|--------|
| 3.11 | Schedule edge validation | 3 | Low |
| 3.12 | CDF cursor lifecycle (focused) | 3 | Low |
| 3.13 | Dataset resolution contracts | 2 | Low |
| 3.14 | E2E extraction to CPG | 4 | High |
| 3.15 | Hamilton materializer contracts | 7 | Low |

**Total: 15 suites, 80 tests.**

### Rationale

**P0** targets the three critical choke-points identified by CQ fan-out analysis:
- `compile_execution_plan`: 1 caller, 0 tests, controls all plan compilation.
- `materialize_extract_plan`: 24 callsites, 0 direct tests, every extractor depends on it.
- `resolve_dataset_provider`: 7 callsites, only smoke coverage, every read path depends on it.

**P1** covers lifecycle and diagnostics that are observable but currently untested:
- Build orchestration (heartbeat/signals) has production implications if broken.
- `plan_execution_diff_v1` is the primary build-health diagnostic with zero tests.
- Idempotent writes are relied on by write, delete, and merge flows.

**P2** covers correctness invariants that have partial coverage:
- Byte-span canonicalization is the foundational invariant but has partial extractor-level tests.
- Semantic compiler type validation has some coverage in `test_semantic_pipeline.py`.

**P3** covers refinements and end-to-end validation that build on P0-P2 foundations.

---

## 7. Markers and CI Integration

```python
@pytest.mark.integration  # All Phase 1-3 suites (P0, P1, P2)
@pytest.mark.e2e          # Suite 3.14 only (P3)
```

Phase 4 (P3) E2E tests excluded from default CI via `-m "not e2e"` and run in nightly/pre-release.

---

## 8. Success Criteria

1. All 15 suites implemented with passing tests.
2. Every cross-system boundary in this document has at least one integration test.
3. `compile_execution_plan` has direct boundary tests including all 5 error paths.
4. `materialize_extract_plan` has direct tests for gating, projection, and streaming vs table paths.
5. `resolve_dataset_provider` has direct tests for both `"delta"` and `"delta_cdf"` paths.
6. Byte-span canonicalization validated across CST, AST, and tree-sitter extraction sources.
7. `plan_execution_diff_v1` payload shape and content validated.
8. Idempotent write deduplication verified through write, delete, and merge paths.
9. All new tests run in CI with `@pytest.mark.integration` and complete within medium-speed tier.
10. E2E tests produce CPG outputs on minimal repo with deterministic `entity_id` values.

---

## 9. Review Integration Changelog

This document integrates all findings from `docs/plans/expanded_integration_testing_v2_review.md`:

| Review Item | Status | Action Taken |
|-------------|--------|-------------|
| Correction 1: Pipeline ownership | **Applied** | Split Suite 3.1 (old) into Suite 3.4 (build orchestration) + Suite 3.5 (pipeline diagnostics). Heartbeat/signal assertions moved to `build_graph_product` scope. |
| Correction 2: Materializer boundary | **Applied** | Suite 3.15 tests `TableSummarySaver` metadata payload (not Delta writes). Factory wiring validated against `delta_output_specs()`. |
| Correction 3: E2E directory | **Applied** | Suite 3.14 placed at `tests/e2e/test_extraction_to_cpg.py`. |
| Correction 4: CDF cursor dedup | **Applied** | Suite 3.12 scoped to cursor <-> Delta/CDF runtime only; unit-covered behaviors removed. |
| Optimization 1: Semantic input overlap | **Applied** | Suite 3.9 reduced to 3 tests not covered by `test_semantic_pipeline.py`. |
| Optimization 2: Scheduling overlap | **Applied** | Suite 3.11 reduced to 3 tests not covered by `test_schedule_generation.py`. |
| Optimization 3: Evidence gating consolidation | **Applied** | No new gating suite; existing suites + Suite 3.2 cover materialization effects. |
| Optimization 4: Boundary over smoke assertions | **Applied** | All new tests bias toward payload assertions, contract fields, and failure semantics. |
| Addition A: `compile_execution_plan` | **Applied** | Suite 3.1 (P0) with 10 tests including all error paths. |
| Addition B: Delta pin conflict | **Applied** | Tests in Suite 3.1 (`test_delta_pin_conflict_raises`, `test_delta_pin_conflict_same_version_ok`). |
| Addition C: `plan_execution_diff_v1` | **Applied** | Suite 3.5 with 6 tests covering payload shape and content. |
| Addition D: `materialize_extract_plan` | **Applied** | Suite 3.2 (P0) with 6 tests. |
| Addition E: `resolve_dataset_provider` | **Applied** | Suite 3.3 (P0) with 7 tests for both provider kinds. |
| Addition F: Idempotent write propagation | **Applied** | Suite 3.6 (P1) with 8 tests covering write/delete/merge flows. |
| Revised priorities | **Applied** | P0: compile_execution_plan + materialize_extract_plan + resolve_dataset_provider. |

### CQ Verification Pass (Post-Review Integration)

All code references verified via CQ search, CQ calls, and direct file reads. The following corrections were applied:

| Item | Original Claim | CQ-Verified Actual | Correction |
|------|---------------|-------------------|------------|
| `GraphProductBuildRequest` fields | 22 | 21 (lines 101-128 of `product_build.py`) | Field count fixed |
| `ParamTableSummarySaver` class line | :97 | :71 | Line number fixed |
| `PipelineExecutionOptions` class line | :54 | :55 (decorator at 54, class at 55) | Line number fixed |
| `DatasetResolution` fields | 12 | 13 | Field count fixed |
| `ErrorKind.PLUGIN` raise | :146 | :150 (except clause at 146, raise at 150) | Line reference updated to raise site |
| `_store_plan_artifacts()` function | claimed at :678 | **Does not exist** - inline block calls `persist_plan_artifacts_for_views()` at :685 | Function name corrected to actual API |
| `_scan_unit_delta_pins()` def | :313 | :299 (def), :313 (first body line after docstring) | Line reference updated to def site |
| Delta pin conflict ValueError | :322 | :323 | Line number fixed |
| `plan_requires_row()` file | implied evidence_plan.py | `spec_helpers.py:62` (re-exported via `__init__.py`) | File location corrected |
| `plan_artifacts_store_failed_v1` emission | :695 | :700 (inside except block starting at :695) | Line reference updated |
| Review claim: `heartbeat.stop()` line | review said :190 | :193 (line 190 is `finally:`) | Plan was correct; review note acknowledged |

All other code references (45+ checks) confirmed accurate.
