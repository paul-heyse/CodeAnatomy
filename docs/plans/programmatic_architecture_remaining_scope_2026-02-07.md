# Programmatic Architecture: Remaining Scope Assessment

Last updated: 2026-02-07

This document captures the precise remaining implementation scope across all ten phases
(A through J) of the programmatic architecture plan, based on deep cross-referencing of
`docs/plans/programmatic_architecture_continuation_consolidated_2026-02-07.md` against the
actual codebase state.

---

## Executive Summary

The programmatic architecture is substantially complete. Seven of ten phases are at 90%+
completion. Remaining work concentrates in four areas:

| Priority | Phase | Gap | Effort |
|----------|-------|-----|--------|
| **P0** | B.2 | Cache heuristic cutover | Medium |
| **P1** | D.1 | Join key inference wiring | Medium |
| **P2** | B.3 | Confidence model threading | Small |
| **P3** | C | Richer scan signals + calibrator | Large |
| **P4** | G | Operational logic (provenance, replay, quarantine) | Large |

### Completion Summary

| Phase | Description | Completion | Status |
|-------|-------------|------------|--------|
| **A** | Correctness & Integration Closure | ~95% | Done (minor test gaps) |
| **B** | Policy Compilation & Evidence Model | ~80% | **Cache cutover blocking** |
| **C** | Adaptive Optimization | ~50% | Infrastructure exists, signals shallow |
| **D** | Semantic Inference Activation | ~40% | **Join key wiring blocking** |
| **E** | Entity-Centric Data Modeling | ~95% | Done |
| **F** | Pipeline-as-Specification | ~90% | Done |
| **G** | Advanced Acceleration & Decommission | ~25% | Structs exist, logic unbuilt |
| **H** | Schema Derivation | ~95% | Done |
| **I** | Semantic Type System | ~95% | Done |
| **J** | Registry Consolidation | ~95% | Done |

---

## Critical Path

```
B.2 (cache cutover) --> B.3 (confidence threading) --> D.1 (join key wiring) --> C (calibrator)
```

---

## Phase A: Correctness & Integration Closure (~95%)

### Completed

- **Drift surface enforcement**: `scripts/check_drift_surfaces.py` (490 LOC, 8 AST-backed
  checks) is CI-gated via `.github/workflows/check_drift_surfaces.yml`.
- **Resolver identity**: `ResolverIdentityTracker` in `src/semantics/resolver_identity.py`
  (182 lines) with 4 boundary scopes (view_registration, scan_override, cdf_registration,
  dataset_readiness). Wired into `driver_factory.py:build_view_graph_context()` and
  `plan/pipeline.py:plan_with_delta_pins()`.
- **Capability detection**: `DataFusionPlanCapabilities` + `RuntimeCapabilitiesSnapshot` in
  `src/datafusion_engine/extensions/runtime_capabilities.py` (360 lines). `capability_snapshot`
  field on `ExecutionAuthorityContext`.
- **Outcome-based maintenance**: `WriteOutcomeMetrics` + `resolve_maintenance_from_execution()`
  in `src/datafusion_engine/delta/maintenance.py` (690 lines) with deterministic reason codes.
  `DELTA_MAINTENANCE_DECISION_SPEC` registered. Production callsites in `io/write.py` and
  `incremental/delta_context.py`.
- **Deprecation cleanup**: `dataset_bindings_for_profile()` and `_ensure_dataset_resolver()`
  removed. `CompileContext.dataset_bindings()` has deprecation warning with zero callers.

### Remaining

1. **Integration tests** for resolver identity boundary violations (test gap, not
   infrastructure gap).
2. **Drift surface expansion**: Consider adding checks for newly consolidated modules
   (view_kinds.py authority, builder dispatch table completeness).

### Effort: Minimal (test-only work)

---

## Phase B: Policy Compilation & Evidence Model (~80%)

### Completed

- **CompiledExecutionPolicy**: `src/relspec/compiled_policy.py` (70 lines) — msgspec struct
  with `cache_policy_by_view`, `scan_policy_overrides`, `maintenance_policy_by_dataset`,
  `udf_requirements_by_view`, `materialization_strategy`, `diagnostics_flags`,
  `validation_mode`, `policy_fingerprint`.
- **Policy compiler**: `src/relspec/policy_compiler.py` (214 lines) —
  `compile_execution_policy()` orchestrates from task_graph + output_locations +
  runtime_profile. `_derive_cache_policies()` uses rustworkx `out_degree()` for
  topology-based cache decisions.
- **Compilation wiring**: Called from `_compile_authority_policy()` in `driver_factory.py`
  (lines 651-666). Result stored on `ExecutionAuthorityContext.compiled_policy` (line 67 of
  `execution_authority.py`). Artifact recorded via `_record_compiled_policy_artifact()`.
- **Policy validation**: `validate_policy_bundle()` in `src/relspec/policy_validation.py`
  with 9 validators (6 original + 3 new: `_compiled_policy_consistency_issues`,
  `_statistics_availability_issues`, `_evidence_coherence_issues`).
- **InferenceConfidence struct**: `src/relspec/inference_confidence.py` (117 lines) —
  `InferenceConfidence(StructBaseStrict, frozen=True)` with `confidence_score`,
  `evidence_sources`, `fallback_reason`, `decision_type`, `decision_value`. Helpers:
  `high_confidence()` and `low_confidence()`.

### Remaining

#### B.2: Cache Heuristic Cutover (P0 — Highest Value)

**Problem:** `_default_semantic_cache_policy()` at `src/semantics/pipeline.py:262-288`
is the last major naming-convention heuristic. It assigns cache policy based on string
prefixes (`cpg_*`, `rel_*`, `*_norm`, `join_*`) instead of graph topology.

**Current heuristic (pipeline.py:262-288):**
```python
def _default_semantic_cache_policy(*, view_names, output_locations):
    for name in view_names:
        if name.startswith("cpg_"):  # naming convention
            ...
        if name.startswith("rel_") or name.endswith("_norm"):  # naming convention
            ...
```

**Compiled replacement (policy_compiler.py:84-119):**
```python
def _derive_cache_policies(task_graph, output_locations):
    for task_name, node_idx in task_graph.task_idx.items():
        out_degree = task_graph.graph.out_degree(node_idx)  # topology
        if task_name in output_locations:
            policies[task_name] = "delta_output"
        elif out_degree > _HIGH_FANOUT_THRESHOLD:
            policies[task_name] = "delta_staging"
        ...
```

**Gap:** `CompiledExecutionPolicy.cache_policy_by_view` is built and recorded as an
artifact, but the semantic pipeline at `pipeline.py:1253-1258` does not consume it:

```python
# pipeline.py:1253 — still falls back to naming heuristic
resolved_cache = request.cache_policy
if resolved_cache is None:
    resolved_cache = _default_semantic_cache_policy(...)  # <-- THIS
```

**Integration path:**
- `CpgBuildOptions` already has `cache_policy: Mapping[str, CachePolicy] | None = None`
- `ExecutionAuthorityContext` already carries `compiled_policy`
- Need to thread `compiled_policy.cache_policy_by_view` from the execution authority
  context into `CpgBuildOptions.cache_policy` at the call site
- Keep `_default_semantic_cache_policy()` as a fallback when compiled policy is unavailable
- Runtime `cache_overrides` from `runtime_profile` continue to apply as highest priority

**Files to modify:**
- `src/semantics/pipeline.py` — `_view_nodes_for_cpg()` (lines 1239-1281) and possibly
  `build_cpg()` signature/options
- Caller that constructs `CpgBuildOptions` — needs to extract compiled policy

**Tests needed:**
- Unit test for compiled policy → `CpgBuildOptions` threading
- Integration test comparing heuristic vs. topology-based policies for known view graphs
- No existing tests for `_default_semantic_cache_policy()` (coverage gap)

**Effort:** Medium (2-3 files, ~50-100 lines of change)

#### B.3: Confidence Model Threading (P2)

**Problem:** `InferenceConfidence` struct exists but is NOT threaded into any inference
functions.

**Current state:**
- `src/relspec/inference_confidence.py` — struct + `high_confidence()`/`low_confidence()` helpers
- `src/datafusion_engine/delta/scan_policy_inference.py` — `derive_scan_policy_overrides()`
  assigns `confidence` as a raw float (0.7-0.9), not using `InferenceConfidence`
- `src/semantics/joins/inference.py` — `infer_join_strategy()` returns strategy without
  confidence metadata

**Required wiring:**
- `derive_scan_policy_overrides()` → return `InferenceConfidence` alongside each decision
- `infer_join_strategy()` → return `InferenceConfidence` alongside strategy
- `_infer_view_properties()` in `ir_pipeline.py` → attach confidence to inferred properties

**Missing validator:** `_scan_policy_compatibility_issues()` mentioned in plan but not
implemented in `policy_validation.py`.

**Effort:** Small-Medium (3-4 files, ~80-120 lines)

---

## Phase C: Adaptive Optimization (~50%)

### Completed

- **Scan policy infrastructure**: `src/datafusion_engine/delta/scan_policy_inference.py` —
  `derive_scan_policy_overrides()` with 2 signal types:
  1. Small table detection (`_is_small_table()` vs `_SMALL_TABLE_ROW_THRESHOLD`)
  2. Pushed filters (`scan.pushed_filters` → `enable_parquet_pushdown = True`)
- **PlanSignals**: `src/datafusion_engine/plan/signals.py` — dataclass with `schema`,
  `lineage`, `stats` (num_rows, total_bytes, partition_count), `scan_compat`,
  `plan_fingerprint`, `repartition_count`.
- **ScheduleCostContext**: `src/relspec/rustworkx_schedule.py` — struct with `task_costs`,
  `bottom_level_costs`, `slack_by_task`, `betweenness_centrality`, `articulation_tasks`,
  `bridge_tasks`.
- **Cost derivation**: `derive_task_costs_from_plan()` at `src/relspec/execution_plan.py:1965`
  derives costs from TaskPlanMetrics. `_base_cost_from_metrics()` uses duration_ms →
  output_rows → stats_row_count → total_bytes → partition_count → 1.0 fallback chain.
  `_apply_physical_adjustments()` adds partition/repartition adjustments.
- **Scheduling integration**: Cost context passed to `schedule_tasks()` at
  `execution_plan.py:374`.
- **Artifact governance**: 175/175 `record_artifact` callsites typed per
  `scripts/audit_artifact_callsites.py`. CI enforced.

### Remaining

#### C.1: Richer Scan Signals (P3)

**Missing fields on `PlanSignals`:**
- `sort_keys: tuple[str, ...] | None` — for sort-order exploitation
- `predicate_selectivity_estimate: float | None` — for predicate cost modeling
- `projection_ratio: float | None` — for column pruning effectiveness

**Missing signal types in scan policy:**
- Sort-order exploitation (can skip sorting if already sorted)
- Partition pruning effectiveness metrics
- Filter push-down depth measurement

**Effort:** Medium (2 files, ~60-80 lines)

#### C.2: Calibration Feedback Loop (P3)

**Missing entirely:**
- `src/relspec/policy_calibrator.py` does NOT exist
- `src/relspec/calibration_bounds.py` does NOT exist
- No `PolicyCalibrationResult` or `PolicyCalibrationArtifact` types
- No adaptive threshold learning from execution feedback
- No per-workload cost adjustments
- Cost model is uniform, not specialized by workload class

**Design notes:**
- Calibrator would compare predicted vs. actual task costs from execution metrics
- Adaptive thresholds for `_HIGH_FANOUT_THRESHOLD`, `_SMALL_TABLE_ROW_THRESHOLD`
- Feedback loop: execution metrics → calibration artifact → next compilation cycle

**Effort:** Large (new module, ~200-400 lines)

---

## Phase D: Semantic Inference Activation (~40%)

### Completed

- **Join strategy inference**: `infer_join_strategy()` in
  `src/semantics/joins/inference.py` is in production via `require_join_strategy()` at
  `compiler.py:806`. `JoinCapabilities.from_schema()` for structured capability detection.
- **Join key inference**: `infer_join_keys()` method EXISTS at
  `src/semantics/types/annotated_schema.py:268` using semantic type system.
- **Quality templates**: `src/semantics/quality_specs.py` (338 lines) — 9 quality
  relationship specs, 5 using `entity_symbol_relationship()` template factory.

### Remaining

#### D.1: Wire `infer_join_keys()` into Compiler (P1 — High Value)

**Problem:** `infer_join_keys()` exists on `AnnotatedSchema` but the compiler still uses
18 hardcoded join keys.

**Current hardcoding (quality_specs.py):**
```python
entity_symbol_relationship(
    ...
    left_on=["file_id"],     # hardcoded
    right_on=["file_id"],    # hardcoded
    ...
)
```

**Available inference (annotated_schema.py:268):**
```python
def infer_join_keys(self, other: AnnotatedSchema) -> tuple[tuple[str, str], ...]:
    # Uses compatibility groups to find matching columns
```

**Integration path:**
1. In compiler's join handling (around `compiler.py:806`), call
   `left_schema.infer_join_keys(right_schema)` when explicit keys not provided
2. Use inferred keys as default, allow spec-level overrides
3. Remove 18 hardcoded `left_on`/`right_on` from quality specs
4. Verify via parity tests that inferred keys match previously hardcoded ones

**IR pipeline integration:**
- `_infer_view_properties()` in `ir_pipeline.py:942` already infers join keys but
  the result (`InferredViewProperties.inferred_join_keys`) is not consumed downstream

**Files to modify:**
- `src/semantics/compiler.py` — join handling logic
- `src/semantics/quality_specs.py` — remove 18 hardcoded join key pairs
- `src/semantics/ir_pipeline.py` — ensure inferred keys flow to consumers

**Tests needed:**
- Parity test: inferred keys == previously hardcoded keys for all 18 specs
- Regression test: join output correctness with inferred keys

**Effort:** Medium (3-4 files, ~100-150 lines)

---

## Phase E: Entity-Centric Data Modeling (~95%)

### Completed

- **Entity declarations**: `src/semantics/entity_model.py` — `EntityDeclaration`,
  `IdentitySpec`, `LocationSpec`, `ForeignKeySpec` structs.
- **Entity registry**: `src/semantics/entity_registry.py` — 7 `ENTITY_DECLARATIONS`
  (cst_ref, cst_def, cst_import, cst_call, cst_call_arg, cst_docstring, cst_decorator).
  `entity_to_table_spec()`, `entity_to_normalization_spec()`, `generate_table_specs()`,
  `generate_normalization_specs()` converters. Parity tests pass.
- **ViewKind consolidation**: `src/semantics/view_kinds.py` — single authoritative source
  with `ViewKind` enum (14 values), `VIEW_KIND_ORDER` (execution tiers 0-11). Triple
  definitions eliminated via aliases (`SemanticIRKind = ViewKindStr` in `ir.py:18`,
  `SpecKind = ViewKindStr` in `spec_registry.py:37`). 14-to-6 consolidated mapping
  included as roadmap (`ConsolidatedKind` + `CONSOLIDATED_KIND` mapping).
- **Builder dispatch factory**: `_dispatch_from_registry()` at `pipeline.py:874-914`.
  14-entry `_BUILDER_HANDLERS` dispatch table at `pipeline.py:1090-1107`. 11 custom
  per-kind handler functions.

### Remaining

1. **Entity registry consumption**: `SEMANTIC_TABLE_SPECS` in `registry.py` still uses
   static dict rather than consuming generated specs from `generate_table_specs()`.
   Parity tests confirm equivalence, but the actual cutover hasn't happened.
2. **14-to-6 ViewKind consolidation**: The roadmap mapping exists in `view_kinds.py:103`
   but the actual cutover (reducing 14 kinds to 6 consolidated categories) is deferred.

### Effort: Small (registry cutover is safe given parity tests)

---

## Phase F: Pipeline-as-Specification (~90%)

### Completed

- **Semantic IR pipeline**: `src/semantics/ir_pipeline.py` — complete
  compile → infer → optimize → emit pipeline:
  - `compile_semantics()` (line 530): Builds IR from SemanticModel
  - `infer_semantics()` (line 1014): Enriches with inferred properties (join strategy,
    join keys, cache policy hints, graph position)
  - `optimize_semantics()` (line 768): Prunes/optimizes IR by outputs
  - `emit_semantics()` (line 828): Final shape transformation
- **InferredViewProperties**: `src/semantics/ir.py:40` — `inferred_join_strategy`,
  `inferred_join_keys`, `inferred_cache_policy`, `graph_position`.
- **ExecutionPackage**: `src/relspec/execution_package.py` —
  `ExecutionPackageArtifact` struct with composite fingerprint from manifest, policy,
  capability snapshot, plan bundle fingerprints, session config. `build_execution_package()`
  factory. Registered and called from `driver_factory.py`.

### Remaining

1. **Inferred property consumption**: `InferredViewProperties.inferred_cache_policy` and
   `inferred_join_keys` are computed but not yet consumed by downstream pipeline stages.
   Resolving B.2 (cache cutover) and D.1 (join key wiring) would activate these.
2. **Cache policy vocabulary mismatch**: IR pipeline uses `"eager"`/`"lazy"` strings
   while the execution layer uses `CachePolicy = Literal["none", "delta_staging",
   "delta_output"]`. Need a mapping between the two vocabularies.

### Effort: Small (vocabulary mapping, property consumption)

---

## Phase G: Advanced Acceleration & Decommission (~25%)

### Completed

- **Workload classification**: `src/serde_artifacts.py:712` — `WorkloadClassificationArtifact`
  struct. `src/datafusion_engine/workload/classifier.py` — `classify_workload()` function.
- **Pruning metrics**: `src/serde_artifacts.py:739` — `PruningMetricsArtifact` struct.
- **Artifact specs registered**: `WORKLOAD_CLASSIFICATION_SPEC`, `PRUNING_METRICS_SPEC` in
  `serde_artifact_specs.py`.

### Remaining (mostly unbuilt)

1. **Workload-to-session-config mapping**: `classify_workload()` exists but there is no
   mapping from workload class to DataFusion session configuration adjustments.
2. **Decision provenance graph**: `DecisionProvenanceGraphArtifact` struct exists but no
   operational logic to build the provenance graph.
3. **Execution replay**: No infrastructure for deterministic re-execution from
   `ExecutionPackage` snapshots.
4. **Quarantine/rollback**: No quarantine mechanism for failed or suspect executions.
5. **Pruning metrics operational logic**: Struct exists but no pipeline to compute
   actual pruning effectiveness metrics.

### Effort: Large (multiple new modules, ~500+ lines total)

---

## Phase H: Schema Derivation (~95%)

### Completed

- **Schema derivation**: `src/datafusion_engine/schema/derivation.py` —
  `derive_extract_schema()` from `ExtractMetadata`.
- **Field type resolution**: `src/datafusion_engine/schema/field_types.py` —
  `resolve_field_type()` with three-stage resolution (identity → scalar/map → composite).
- **Template metadata**: `src/datafusion_engine/extract/templates.py` (1451 lines) —
  all templates have populated `fields` (no `None` values). Convention-based discovery
  via `_discover_templates()` / `_discover_configs()`.
- **Schema registry**: `src/datafusion_engine/schema/registry.py` (4139 lines) —
  `_BASE_EXTRACT_SCHEMA_BY_NAME` ImmutableRegistry, `NESTED_DATASET_INDEX`.
- **Extract metadata**: `src/datafusion_engine/extract/metadata.py` — `ExtractMetadata`
  with `field_types`, `nested_shapes`, `fields`, `derived`, `row_fields`, `row_extras`.

### Remaining

1. **`schema_from_struct()` utility**: Plan mentions this but it does not exist. Would
   generate PyArrow schema from msgspec struct definitions.
2. **2 untyped artifact specs**: Per audit, 2 specs still have `payload_type=None`.

### Effort: Minimal

---

## Phase I: Semantic Type System (~95%)

### Completed

- **Core types**: `src/semantics/types/core.py` (304 lines) — `SemanticType` enum (11 types),
  `CompatibilityGroup` enum (4 groups), `STANDARD_COLUMNS` registry, `infer_semantic_type()`.
- **Annotated schema**: `src/semantics/types/annotated_schema.py` (370 lines) —
  `has_semantic_type()`, `has_compatibility_group()`, `columns_by_semantic_type()`,
  `columns_by_compatibility_group()`, `join_key_columns()`, `partition_key_columns()`,
  `common_compatibility_groups()`, `infer_join_keys()`.

### Remaining

1. **2 contextual hardcoded checks**: `deltas.py:67` (`if "symbol" in schema.names`) and
   `projections.py:82-86` (`if "edge_owner_file_id" in names and "file_id" in names`).
   These are limited and contextual but could be replaced with semantic type queries.

### Effort: Minimal (2 small replacements)

---

## Phase J: Registry Consolidation (~95%)

### Completed

- **Naming policy**: `src/semantics/naming.py` (96 lines) — `canonical_output_name()` uses
  identity mapping with manifest-backed override support. `SEMANTIC_OUTPUT_NAMES` dict
  removed.
- **Convention-based discovery**: `_discover_templates()` / `_discover_configs()` in
  `templates.py`. Module-level constants follow `_<NAME>_TEMPLATE` / `_<NAME>_CONFIG`
  pattern.
- **Table size tiers**: `src/relspec/table_size_tiers.py` (85 lines) —
  `TableSizeThresholds`, `classify_table_size()`. Used by 3 subsystems
  (scan_policy_inference, io/write, delta_updates).
- **ViewKind authority**: Single source in `view_kinds.py` (completed via Phase E.2).

### Remaining

Nothing material. Phase J is complete.

---

## Decommission Tracker

Items that have been removed or remain:

| Target | Status |
|--------|--------|
| `dataset_bindings_for_profile()` | **REMOVED** |
| `_ensure_dataset_resolver()` | **REMOVED** |
| `CompileContext.dataset_bindings()` | Deprecated, zero callers |
| `SEMANTIC_OUTPUT_NAMES` dict | **REMOVED** |
| Triple ViewKind definitions | **REMOVED** (aliases to `view_kinds.py`) |
| `_default_semantic_cache_policy()` | **STILL ACTIVE** — Phase B.2 |
| 18 hardcoded join keys in quality_specs | **STILL ACTIVE** — Phase D.1 |
| Static `SEMANTIC_TABLE_SPECS` dict | **STILL ACTIVE** — Phase E.1 cutover |

---

## Recommended Implementation Order

```
1. B.2  Cache heuristic cutover         (unblocks F property consumption)
2. D.1  Join key inference wiring        (unblocks compiler from hardcoded keys)
3. B.3  Confidence model threading       (adds governance/observability)
4. E.1  Entity registry cutover          (safe given parity tests)
5. C.1  Richer scan signals              (enriches plan signals)
6. F.2  Cache vocabulary mapping         (connects IR hints to execution)
7. I.1  Replace 2 hardcoded checks       (completes type system migration)
8. C.2  Calibration feedback loop        (requires execution data)
9. G.*  Operational logic                (lowest priority, largest scope)
```

---

## Key Architectural Observations

1. **Infrastructure leads activation**: Most remaining work is "wire existing
   infrastructure into its consumers" — the hard design/implementation work is done.
   The gap is threading completed components through the existing call chains.

2. **Compiled policy is the pivotal artifact**: `CompiledExecutionPolicy` is built,
   recorded, and fingerprinted, but its `cache_policy_by_view` field is not yet
   consumed by the semantic pipeline. Completing B.2 is the single highest-leverage
   change.

3. **Inference confidence is ready to attach**: The `InferenceConfidence` struct and
   its `high_confidence()`/`low_confidence()` helpers are fully designed. Attaching
   them to scan policy and join strategy inference functions is straightforward.

4. **Parity tests de-risk cutovers**: Entity declarations generate specs identical
   to hand-written ones (verified by parity tests). This makes the E.1 registry
   cutover low-risk.

5. **IR pipeline has unused outputs**: `infer_semantics()` computes
   `inferred_cache_policy` and `inferred_join_keys` per view, but nothing downstream
   reads them. Completing B.2 and D.1 would activate this inference pipeline.
