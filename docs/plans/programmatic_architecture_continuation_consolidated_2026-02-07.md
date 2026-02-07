# Programmatic Architecture: Continuation Plan (Consolidated)

**Date:** 2026-02-07
**Consolidates:** `programmatic_architecture_continuation_v1_2026-02-07.md` + review feedback from `programmatic_architecture_continuation_review_v3_2026-02-07.md`
**Prerequisite:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md`

---

## 1. Executive Summary

### 1.1 Architecture Vision

The target architecture has two core inputs that programmatically determine everything:

1. **Extracted data** - Evidence tables from multi-source extractors
2. **Data operations mapping** - DataFusion view definitions mapping datasets to CPG outputs

From these two inputs, the system derives scheduling, validation, caching, UDF
requirements, and maintenance decisions with zero static declarations that could drift.

> **The DataFusion plan is the single source of truth for what the pipeline needs.**
> Everything else - dependencies, scheduling, UDFs, schema contracts, maintenance,
> scan policies, cache strategies - is derived from plan introspection products.
> Runtime configuration sets boundaries and thresholds; the plan determines behavior
> within those boundaries.

### 1.2 Review Reframing

The continuation plan is directionally strong, but the key correction from the V3
review is: **foundations exist; integration coverage is uneven.** The repo is now at
the stage where best-in-class trajectory depends on:

1. Making compile artifacts the sole source of runtime truth
2. Making policy decisions evidence-backed and replayable
3. Making optimization behavior measurable and self-improving under strict invariants

The framing shifts from "add more static declarations" to "compile policy and behavior
from plan/capability evidence." Most components already exist - the work is integration,
coverage, and closing feedback loops.

### 1.3 Operational Core

The inference-driven core is fully functional:

```
SemanticIR (declarative view graph)
  -> compile_semantic_program() -> SemanticProgramManifest (immutable contract)
    -> build_plan_bundle() -> DataFusionPlanBundle (per-view plan artifact)
      -> extract_lineage() -> LineageReport (scans, joins, UDFs, columns)
        -> infer_deps_from_plan_bundle() -> InferredDeps (typed dependency contract)
          -> build_task_graph_from_inferred_deps() -> TaskGraph (rustworkx)
            -> schedule_tasks() -> TaskSchedule (cost-aware ordering)
              -> build_task_execution_module() -> Hamilton DAG (dynamic codegen)
```

Every link derives its output from the plan, not from static declarations.

---

## 2. Current State Assessment

### 2.1 Wave Completion Status (Updated from V3 Review)

| Wave | Goal | Status | Remaining |
|------|------|--------|-----------|
| **Wave 0** | Reality alignment | **Complete** | - |
| **Wave 1+2** | Resolver + compile boundary closure | **~Complete** | V3 drift checks show 0 violations; reframe as regression prevention |
| **Wave 3** | Extract executor binding | **95%** | Deprecated global registry still functional; test-oriented only |
| **Wave 4A** | Maintenance from execution | **Integrated** | Production callsites exist at `io/write.py:1790` and `delta_context.py:238`; remaining work is threshold tuning and diagnostics quality |
| **Wave 4B** | Scan policy from plan | **Infrastructure wired** | `derive_scan_policy_overrides` + `_apply_inferred_scan_policy_overrides` active; needs richer signals + measured payoff |
| **Wave 5** | Artifact contract governance | **Complete** | 175/175 `record_artifact` callsites typed; 0 raw-string callsites |
| **8.7** | ExecutionAuthorityContext | **Complete** | Production construction at `driver_factory.py:665` |
| **8.8** | PlanSignals | **Complete** | - |
| **8.9** | Delta compatibility reuse | **Complete** | - |
| **10.1** | Output naming from manifest | **Complete** | Identity naming deployed |
| **10.8** | Hamilton DAG generation | **Complete** | Dynamic module builder operational |
| **10.10** | Policy bundle validation | **Operational** | `validate_policy_bundle()` active at `driver_factory.py:2065`; expand rule quality |

### 2.2 Drift Surface Scorecard (Updated from V3 Review)

`uv run scripts/check_drift_surfaces.py` reports all checks passing:

| Metric | V1 Estimate | V3 Verified | Target |
|--------|-------------|-------------|--------|
| `CompileContext(...)` outside boundary | 6 | **0** | 0 |
| `dataset_bindings_for_profile()` fallback usage | 5 | **0** | 0 |
| `compile_tracking(...)` missing instrumentation | unknown | **0** | 0 |
| `resolver_identity_tracking(...)` missing instrumentation | unknown | **0** | 0 |
| `record_resolver_if_tracking(...)` missing instrumentation | unknown | **0** | 0 |
| `record_artifact(` string-literal names | ~165 | **0** | 0 |
| `record_artifact(` inline `ArtifactSpec(...)` | unknown | **0** | 0 |

**Implication:** Phases framed as broad fallback cleanup should be reframed as
**regression prevention and hardening**. The drift surface CI gates are the deliverable.

### 2.3 What Flows Programmatically vs. What Remains Bridged

**Fully programmatic** (derived from plans, no static declarations):
- Task dependencies (from DataFusion plan lineage)
- Column-level requirements (from plan projection)
- UDF requirements (from plan expression walking)
- Required rewrite tags (from UDF metadata)
- Plan fingerprints (from plan content hashing)
- Task scheduling order (from rustworkx graph analysis)
- Hamilton DAG topology (from execution plan)
- Schema contracts (from DataFusion schema introspection)
- Write policy adaptation (from plan statistics via PlanSignals)
- Scan unit file pruning (from Delta add-actions + pushed predicates)
- Protocol compatibility (from `delta_protocol_compatibility()`)
- Artifact naming (all typed ArtifactSpec, 175/175)

**Still bridged** (heuristic or static):
- **Cache policy assignment** - derived from naming conventions (`_default_semantic_cache_policy` at `pipeline.py:262`), not from plan or graph properties. **This is the highest-value remaining non-programmatic decision.**
- **Extract executor availability** - deprecated global registry still functional (test-oriented)
- **Scan policy signal richness** - infrastructure wired but signal depth is basic (`small_table`, `has_pushed_filters`)

**Partially bridged** (infrastructure exists, integration uneven):
- Scan policy per table (wired, but narrow signals and no payoff measurement)
- Capability detection (wired, but not all consumers gate on it)

### 2.4 DataFusion Plan Introspection Depth

```
DataFusionPlanBundle
  ├── logical_plan (unoptimized)
  ├── optimized_logical_plan
  ├── execution_plan (physical)
  ├── substrait_bytes (serialized, cross-system)
  ├── plan_fingerprint (deterministic hash)
  ├── delta_input_pins (version-locked inputs)
  ├── udf_requirements (from expression walking)
  ├── scan_units (file-level, per-dataset)
  └── plan_details (statistics, partitioning, explain trees)

PlanSignals
  ├── schema: pa.Schema
  ├── lineage: LineageReport (scans, joins, filters, UDFs, columns)
  ├── stats: NormalizedPlanStats (num_rows, total_bytes, partition_count)
  ├── scan_compat: tuple[ScanUnitCompatSummary, ...]
  ├── plan_fingerprint: str
  └── repartition_count: int
```

**DataFusion version note:** DataFusion 51.0.0 (Python bindings) does not expose
`ExecutionPlan.statistics()` or `ExecutionPlan.schema()` as callable methods. The
existing plan statistics extraction uses defensive extraction from `plan_details`
and `explain_analyze` output. Any new stats-dependent code must use capability
detection, not version assumptions.

---

## 3. Phase A: Correctness and Integration Closure

**Goal:** Eliminate drift surfaces. Ensure manifest-backed execution context flows through
all production paths, capability detection is consistent, identity invariants are enforced,
and maintenance is outcome-driven.

**V3 reframing:** The V3 review shows drift checks at 0 violations. Phase A is now
**regression prevention and hardening**, not fallback cleanup. Keep completed invariants
closed and add CI gates to prevent regression.

### 3.1 Harden CompileContext Boundary as CI Gate (Wave 1+2)

**Rationale:** The 6 `CompileContext` fallbacks that were the primary drift surface are
now eliminated (V3-verified: 0 violations). The remaining work is ensuring this stays
closed via CI enforcement and deprecation of the fallback API.

**Current state (V3-verified):**
- `CompileContext(...)` outside boundary: **0** violations
- `dataset_bindings_for_profile()` fallback usage: **0**
- All 6 sites now receive `execution_context` from upstream callers

**Key architectural elements:**

```python
# src/semantics/compile_context.py:109-118
@dataclass(frozen=True)
class SemanticExecutionContext:
    manifest: SemanticProgramManifest
    dataset_resolver: ManifestDatasetResolver
    runtime_profile: DataFusionRuntimeProfile
    ctx: SessionContext
    facade: DataFusionExecutionFacade | None = None

# src/semantics/program_manifest.py:118-160
class ManifestDatasetResolver(Protocol):
    def location(self, name: str) -> DatasetLocation | None: ...
    def has_location(self, name: str) -> bool: ...
    def names(self) -> Sequence[str]: ...
```

**Target files:**

| File | Action |
|------|--------|
| `scripts/check_drift_surfaces.py` | Ensure this runs in CI as a hard gate |
| `src/semantics/compile_context.py` | Add deprecation warnings to `CompileContext.dataset_bindings()` |
| `tests/integration/test_runtime_semantic_locations.py` | Verify 0-fallback invariant in integration tests |

**Implementation checklist:**

- [ ] Verify `scripts/check_drift_surfaces.py` is wired as a CI gate (not just a manual check)
- [ ] Add deprecation warning to `CompileContext.dataset_bindings()` for any future callers
- [ ] Add integration test asserting zero `CompileContext` fallbacks via the same check script
- [ ] Keep the 6 former fallback sites documented as regression risk areas
- [ ] Run: `uv run scripts/check_drift_surfaces.py` to confirm 0 violations

**Decommission targets (now achievable):**

| Target | Location | Reason |
|--------|----------|--------|
| `CompileContext.dataset_bindings()` method | `src/semantics/compile_context.py` | No production callers remain |
| `dataset_bindings_for_profile()` function | `src/semantics/compile_context.py` | Wrapper with no callers |
| `_ensure_dataset_resolver()` function | `src/extract/coordination/materialization.py` | Simplified to direct context access |

---

### 3.2 Resolver Identity Assertions (Section 8.1)

**Rationale:** With CompileContext fallbacks eliminated, enforce that all subsystems
in a pipeline run reference the same `ManifestDatasetResolver` instance (identity
check, not equality). This prevents regression as the architecture evolves.

**Key architectural elements (already exist):**

```python
# src/semantics/resolver_identity.py
@dataclass
class ResolverIdentityTracker:
    label: str = "pipeline"
    def record_resolver(self, resolver: object, *, label: str = "unknown") -> None: ...
    def distinct_resolvers(self) -> int: ...
    def verify_identity(self) -> list[str]: ...
    def assert_identity(self) -> None: ...

@contextmanager
def resolver_identity_tracking(*, label: str = "pipeline", strict: bool = False) -> Iterator[ResolverIdentityTracker]: ...

def record_resolver_if_tracking(resolver: object, *, label: str = "unknown") -> None: ...
```

**V3-verified:** `resolver_identity_tracking(...)` entrypoint instrumentation shows 0 missing, and `record_resolver_if_tracking(...)` boundary instrumentation shows 0 missing. This is already deployed.

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/pipeline.py` | Verify: `build_cpg_from_inferred_deps()` uses `resolver_identity_tracking()` |
| `src/hamilton_pipeline/modules/task_execution.py` | Verify: calls `record_resolver_if_tracking()` at resolver access points |
| `src/engine/materialize_pipeline.py` | Verify: calls `record_resolver_if_tracking()` at resolver access |
| `tests/integration/test_resolver_identity.py` | Ensure: integration test for resolver identity invariant |

**Implementation checklist:**

- [ ] Verify resolver identity tracking is wired in pipeline entry points (V3 says 0 missing)
- [ ] Ensure integration test asserts `tracker.distinct_resolvers() == 1` after pipeline run
- [ ] Confirm `strict=True` mode raises on identity violation
- [ ] Run: `uv run pytest tests/integration/ -k resolver -v`

**Decommission:** None (additive invariant enforcement).

---

### 3.3 Harden DataFusion Capability Adapter (Section 8.2)

**Rationale:** Consistent capability gating is a prerequisite for reliable stats-dependent
scheduling and adaptive scan policies. The infrastructure exists and is wired; this is
coverage expansion, not greenfield.

**Current state (V3-verified):**
- `detect_plan_capabilities()`: `src/datafusion_engine/extensions/runtime_capabilities.py:78`
- Snapshot builder includes plan capabilities: `runtime_capabilities.py:213`
- Execution authority wiring: `src/hamilton_pipeline/driver_factory.py:665`
- Capability snapshot capture: `build_runtime_capabilities_snapshot()` at `:213`
- Artifact spec: `DATAFUSION_RUNTIME_CAPABILITIES_SPEC` at `serde_artifact_specs.py:473`

**Key architectural elements:**

```python
# src/datafusion_engine/extensions/runtime_capabilities.py:42-60
@dataclass(frozen=True)
class DataFusionPlanCapabilities:
    has_execution_plan_statistics: bool
    has_execution_plan_schema: bool
    datafusion_version: str
    has_dataframe_execution_plan: bool

# src/datafusion_engine/extensions/runtime_capabilities.py:63-76
@dataclass(frozen=True)
class RuntimeCapabilitiesSnapshot:
    event_time_unix_ms: int
    profile_name: str | None
    settings_hash: str
    strict_native_provider_enabled: bool
    delta: DeltaCompatibilitySnapshot
    extension_capabilities: Mapping[str, object]
    plugin: ExtensionPluginSnapshot
    execution_metrics: Mapping[str, object] | None
    plan_capabilities: DataFusionPlanCapabilities | None = None
```

**Remaining hardening work:**

| File | Action |
|------|--------|
| `src/hamilton_pipeline/driver_factory.py` | Verify: `capability_snapshot` populated in all `ExecutionAuthorityContext` constructions |
| `src/datafusion_engine/plan/pipeline.py` | Verify: stats-dependent code paths gate on `plan_capabilities` |
| `src/datafusion_engine/extensions/runtime_capabilities.py` | Verify: diagnostic artifact recording is active |

**Implementation checklist:**

- [ ] Verify `capability_snapshot` is non-None in all `ExecutionAuthorityContext` constructions
- [ ] Verify all `PlanSignals.stats` consumers gate on `plan_capabilities.has_execution_plan_statistics`
- [ ] Add diagnostic log entries explaining why features are enabled/disabled
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ -v`

**Decommission:** None (hardening change).

---

### 3.4 Tune Outcome-Based Delta Maintenance (Wave 4A)

**Rationale:** Outcome-based maintenance is the last major bridge between static policy
and programmatic behavior. The V3 review corrects the V1 claim: this is **already
integrated into production paths**, not unwired. The remaining work is threshold quality
tuning and diagnostics quality, not first-time wiring.

**Current state (V3-verified production callsites):**
- `src/datafusion_engine/io/write.py:1790` - production write path
- `src/semantics/incremental/delta_context.py:238` - incremental context
- Internal fallback: `src/datafusion_engine/delta/maintenance.py:323` (`resolve_delta_maintenance_plan` used inside `resolve_maintenance_from_execution`)

**Key architectural elements:**

```python
# src/datafusion_engine/delta/maintenance.py
@dataclass(frozen=True)
class WriteOutcomeMetrics:
    files_created: int | None = None
    total_file_count: int | None = None
    version_delta: int | None = None
    final_version: int | None = None

def build_write_outcome_metrics(
    write_result: Mapping[str, object],
    *, initial_version: int | None = None,
) -> WriteOutcomeMetrics: ...

def resolve_maintenance_from_execution(
    metrics: WriteOutcomeMetrics,
    *, plan_input: DeltaMaintenancePlanInput,
) -> DeltaMaintenancePlan | None: ...
```

**Remaining work (tuning, not wiring):**

| File | Action |
|------|--------|
| `src/datafusion_engine/delta/maintenance.py` | Tune: threshold values for optimize/vacuum/checkpoint decisions |
| `src/serde_artifact_specs.py` | Add: `MAINTENANCE_DECISION_SPEC` typed spec with cause fields |
| `src/serde_artifacts.py` | Add: `MaintenanceDecisionArtifact` with threshold-exceeded cause fields |
| `tests/unit/datafusion_engine/delta/` | Add: tests for threshold-based maintenance decisions |

**Implementation checklist:**

- [ ] Review current threshold values in `resolve_maintenance_from_execution()` for production appropriateness
- [ ] Add maintenance decision artifact recording with cause fields (which threshold exceeded)
- [ ] Add diagnostics: log which outcome metrics triggered which maintenance action
- [ ] Add unit tests for edge cases in threshold-based decisions
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/delta/ -v`

**Decommission targets:**

| Target | Location | Reason |
|--------|----------|--------|
| Direct `resolve_delta_maintenance_plan()` calls outside `resolve_maintenance_from_execution` | If any remain | Should route through outcome-based resolver |

---

### 3.5 Phase A Exit Criteria and Metrics

| Metric | Current (V3) | Target |
|--------|-------------|--------|
| `CompileContext(...)` outside boundary | 0 | 0 (CI-gated) |
| `dataset_bindings_for_profile()` fallback usage | 0 | 0 (CI-gated) |
| `record_artifact(` string-literal names | 0 | 0 (CI-gated) |
| Resolver identity tracking instrumentation gaps | 0 | 0 (CI-gated) |
| Capability snapshot populated in ExecutionAuthority | partial | consistent |
| Maintenance decisions with cause-field artifacts | partial | 100% of Delta writes |

---

## 4. Phase B: Policy Compilation and Evidence Model

**Goal:** Extend the programmatic model from "derive decisions from plans" to "compile
all policy into a single artifact consumed unchanged at runtime." Runtime does not
re-derive policy heuristically.

**V3 reframing:** This is the highest-leverage remaining work. The cache policy heuristic
(`_default_semantic_cache_policy` at `pipeline.py:262`) is the most acute remaining
non-programmatic decision. The `CompiledExecutionPolicy` artifact is the structural
mechanism that converts it.

### 4.1 Introduce CompiledExecutionPolicy Artifact

**Rationale:** A canonical compile-time policy artifact eliminates all remaining runtime
heuristic re-derivation. Runtime consumes the policy artifact unchanged.

**Existing policy objects to consolidate:**

| Policy | Location | Coverage |
|--------|----------|----------|
| `DeltaWritePolicy` | `storage/deltalake/config.py` | Write behavior |
| `DiagnosticsPolicy` | `relspec/pipeline_policy.py` | 7 boolean diagnostic flags |
| `MaterializationPolicy` | `datafusion_engine/materialize_policy.py` | Writer strategy |
| `ScanPolicyConfig` | `schema_spec/system.py` | Scan behavior |

**New artifact structure:**

```python
# src/relspec/compiled_policy.py (NEW FILE)
class CompiledExecutionPolicy(StructBaseStrict, frozen=True):
    """Compile-time-resolved execution policy artifact.

    Runtime consumes this artifact unchanged; it does not
    re-derive policy heuristically.
    """
    scan_policy_by_dataset: Mapping[str, ScanPolicyConfig] = {}
    cache_policy_by_view: Mapping[str, CachePolicy] = {}
    maintenance_policy_by_dataset: Mapping[str, DeltaMaintenancePolicy | None] = {}
    udf_requirements_by_view: Mapping[str, tuple[str, ...]] = {}
    materialization_policy: MaterializationPolicy | None = None
    diagnostics_policy: DiagnosticsPolicy | None = None
    validation_mode: Literal["off", "warn", "error"] = "warn"
    policy_fingerprint: str | None = None
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/compiled_policy.py` | Create: `CompiledExecutionPolicy` struct |
| `src/relspec/policy_compiler.py` | Create: `compile_execution_policy()` that builds artifact from plan signals + manifest |
| `src/relspec/execution_authority.py` | Edit: add `compiled_policy` field to `ExecutionAuthorityContext` |
| `src/hamilton_pipeline/driver_factory.py` | Edit: call `compile_execution_policy()` during plan-context build |
| `src/serde_artifact_specs.py` | Edit: register `COMPILED_EXECUTION_POLICY_SPEC` |
| `src/serde_artifacts.py` | Edit: add `CompiledExecutionPolicyArtifact` payload type |
| `tests/unit/relspec/test_compiled_policy.py` | Create: unit tests for policy compilation |

**Implementation checklist:**

- [ ] Define `CompiledExecutionPolicy` as frozen `StructBaseStrict` with per-dataset and per-view policy sections
- [ ] Implement `compile_execution_policy(manifest, plan_bundle, plan_signals)` that populates each section
- [ ] Add `compiled_policy` field to `ExecutionAuthorityContext`
- [ ] Wire `compile_execution_policy()` into `driver_factory.py` plan-context build phase
- [ ] Record compiled policy as typed artifact
- [ ] Add fingerprint computation (`policy_fingerprint` field)
- [ ] Create unit tests validating policy compilation from test manifests
- [ ] Run: `uv run pytest tests/unit/relspec/ -v`

**Decommission:** None immediately (existing policy objects remain as components).

---

### 4.2 Replace Cache Naming Heuristic with Compiled Policy

**Rationale:** The cache naming heuristic is the highest-value remaining non-programmatic
decision (V3-confirmed). It assigns cache policies based on syntactic name patterns
(`cpg_*` -> delta_output, `rel_*` -> delta_staging, `*_norm` -> delta_staging). This
drifts when naming conventions change.

**Current heuristic to replace:**

```python
# src/semantics/pipeline.py:262 (V3-verified location)
def _default_semantic_cache_policy(*, view_names, output_locations):
    for name in view_names:
        if name.startswith("cpg_"):        # Naming convention
            resolved[name] = "delta_output"
        elif name.startswith("rel_"):       # Naming convention
            resolved[name] = "delta_staging"
        elif name.endswith("_norm"):        # Naming convention
            resolved[name] = "delta_staging"
```

**Target: graph-derived cache policy in CompiledExecutionPolicy:**

| Graph Property | Inferred Cache Policy |
|----------------|----------------------|
| Terminal node + in output_locations | `delta_output` |
| Terminal node + not in output_locations | `none` |
| High fan-out (out_degree > 2) | `delta_staging` |
| Single consumer + low estimated cost | `none` |
| All other intermediate nodes | `delta_staging` |

The `TaskGraph` (rustworkx) already computes `out_degree`, `betweenness_centrality`,
and `articulation_tasks`. A `_derive_cache_policies(task_graph, output_locations)` function
replaces the entire naming-convention heuristic.

```python
# In src/relspec/policy_compiler.py
def _derive_cache_policies(
    task_graph: TaskGraph,
    output_locations: Mapping[str, DatasetLocation],
) -> dict[str, CachePolicy]:
    policies: dict[str, CachePolicy] = {}
    for node_idx in task_graph.graph.node_indices():
        node = task_graph.graph[node_idx]
        name = node.task_name
        out_degree = task_graph.graph.out_degree(node_idx)
        if name in output_locations:
            policies[name] = "delta_output"
        elif out_degree > 2:
            policies[name] = "delta_staging"
        elif out_degree == 0 and name not in output_locations:
            policies[name] = "none"
        else:
            policies[name] = "delta_staging" if out_degree > 0 else "none"
    return policies
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/policy_compiler.py` | Edit: add `_derive_cache_policies()` using `TaskGraph` topology |
| `src/semantics/pipeline.py` | Edit: replace `_default_semantic_cache_policy()` with compiled policy consumption |
| `tests/unit/relspec/test_cache_policy_derivation.py` | Create: test graph-derived cache policies |

**Implementation checklist:**

- [ ] Implement `_derive_cache_policies()` using rustworkx graph `out_degree()`
- [ ] Wire into `compile_execution_policy()` to populate `cache_policy_by_view`
- [ ] Replace `_default_semantic_cache_policy()` callers with `compiled_policy.cache_policy_by_view`
- [ ] Verify cache policy assignments match current naming-convention heuristic for existing views
- [ ] Add edge case handling for disconnected nodes
- [ ] Run: `uv run pytest tests/unit/relspec/ tests/unit/semantics/ -v`

**Decommission targets:**

| Target | Location | Reason |
|--------|----------|--------|
| `_default_semantic_cache_policy()` | `src/semantics/pipeline.py:262+` | Replaced by graph-derived policy in `CompiledExecutionPolicy` |

---

### 4.3 Expand Policy Bundle Validation (Section 10.10)

**Rationale:** With `CompiledExecutionPolicy` as a new artifact, validation should
verify compiled-policy-to-manifest consistency. The validation infrastructure is already
operational; this is rule expansion.

**Current state (already operational):**

```python
# src/relspec/policy_validation.py:155-198
def validate_policy_bundle(
    execution_plan: ExecutionPlan,
    *, runtime_profile, udf_snapshot, capability_snapshot, semantic_manifest,
) -> PolicyValidationResult: ...

# Current validators (6):
# 1. _udf_feature_gate_issues()
# 2. _udf_availability_issues()
# 3. _delta_protocol_issues()
# 4. _manifest_alignment_issues()
# 5. _small_scan_policy_issues()
# 6. _capability_issues()
```

**New validators to add:**

| Validator | Purpose |
|-----------|---------|
| `_compiled_policy_consistency_issues()` | Compiled policy sections match manifest datasets |
| `_scan_policy_compatibility_issues()` | Scan overrides compatible with dataset characteristics |
| `_statistics_availability_issues()` | Warn when stats-dependent policies lack plan statistics |
| `_evidence_coherence_issues()` | Compiled-policy vs runtime-policy coherence (from V3) |

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/policy_validation.py` | Edit: add new validators |
| `src/serde_artifact_specs.py` | Edit: add `POLICY_VALIDATION_RESULT_SPEC` typed spec |
| `src/serde_artifacts.py` | Edit: add `PolicyValidationResultArtifact` with machine-readable error codes |
| `tests/unit/relspec/test_policy_validation.py` | Edit: add tests for new validation rules |

**Implementation checklist:**

- [ ] Add new validators for compiled policy consistency, scan compatibility, statistics availability
- [ ] Emit validation results as typed `PolicyValidationResultArtifact` with machine-readable codes
- [ ] Wire new validators into `validate_policy_bundle()` pipeline
- [ ] Add capability-gated policy assertions (strict when capability absent, from V3 §4.4)
- [ ] Add machine-readable remediation hints in artifacts (from V3 §4.4)
- [ ] Run: `uv run pytest tests/unit/relspec/test_policy_validation.py -v`

**Decommission:** None (additive expansion).

---

### 4.4 Add Confidence Model to Inference Decisions

**Rationale:** Attach confidence and rationale to inferred decisions so the system can
fall back conservatively when evidence is insufficient, and governance dashboards can
track policy maturity.

**Existing partial infrastructure:**

```python
# src/semantics/quality.py - confidence already exists in SignalsSpec
class SignalsSpec:
    base_confidence: float = 0.5    # <-- confidence exists here
```

**New confidence structure:**

```python
# src/relspec/inference_confidence.py (NEW FILE)
class InferenceConfidence(StructBaseStrict, frozen=True):
    confidence_score: float          # 0.0-1.0
    evidence_sources: tuple[str, ...]  # ("lineage", "stats", "capabilities")
    fallback_reason: str | None = None  # When confidence insufficient
    decision_type: str               # "scan_policy", "cache_policy", "join_strategy"
    decision_value: str              # The actual decision made
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/inference_confidence.py` | Create: `InferenceConfidence` struct and helpers |
| `src/datafusion_engine/delta/scan_policy_inference.py` | Edit: attach confidence to scan policy overrides |
| `src/semantics/joins/inference.py` | Edit: attach confidence to `JoinStrategy` results |
| `src/relspec/compiled_policy.py` | Edit: add `confidence_by_decision` to `CompiledExecutionPolicy` |
| `tests/unit/relspec/test_inference_confidence.py` | Create: unit tests |

**Implementation checklist:**

- [ ] Define `InferenceConfidence` struct
- [ ] Add confidence scoring to `derive_scan_policy_overrides()` based on evidence completeness
- [ ] Add confidence scoring to `infer_join_strategy()` based on schema match quality
- [ ] Thread confidence into `CompiledExecutionPolicy` artifact
- [ ] Use confidence for conservative fallback behavior when confidence is insufficient
- [ ] Run: `uv run pytest tests/unit/relspec/ -v`

**Decommission:** None (additive).

---

### 4.5 Phase B Exit Criteria and Metrics

| Metric | Current | Target |
|--------|---------|--------|
| `policy_decisions_backed_by_compiled_policy` (V3 KPI) | 0% | 100% |
| `inference_decisions_with_confidence_payload` (V3 KPI) | 0% | 100% |
| Cache policy derived from naming conventions | yes | no (graph-derived) |
| Policy validation rule count | 6 | 10+ |
| Runtime policy re-derivation points | multiple heuristics | 0 unmanaged heuristics |

---

## 5. Phase C: Adaptive Optimization with Bounded Feedback

**Goal:** Measurable, self-improving optimization behavior under strict invariants.
Bounded, deterministic feedback loop.

### 5.1 Enrich Adaptive Scan Policy Signals (Wave 4B)

**Rationale:** Adaptive scan-policy infrastructure is already wired. The next step is
richer signals and measured payoff tracking, not initial construction.

**Current state (already wired):**
- Inference function: `src/datafusion_engine/delta/scan_policy_inference.py:42`
- Pipeline application: `src/datafusion_engine/plan/pipeline.py:131` and `:173`
- Current signals: small table detection (`stats.num_rows < 10,000`) and pushed filter detection

**New signal sources to add:**

| Signal | Source | Policy Impact |
|--------|--------|---------------|
| Sort-order exploitation | Plan `SortExec` nodes | Enable sort-merge for matching join/sort keys |
| Predicate selectivity | Pushed predicate analysis | Enable page-index pruning for selective predicates |
| Column pruning efficiency | Plan projection vs full schema | Flag narrow projections for pushdown |
| Partition pruning | Filter on partition column | Optimize partition listing |

**Enriched PlanSignals fields:**

```python
# Additions to PlanSignals:
sort_keys: tuple[str, ...] = ()
predicate_selectivity_estimate: float | None = None
projection_ratio: float | None = None  # projected_cols / total_cols
```

**Target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/delta/scan_policy_inference.py` | Edit: add new signal handlers |
| `src/datafusion_engine/plan/signals.py` | Edit: add `sort_keys`, `predicate_selectivity_estimate`, `projection_ratio` fields |
| `src/datafusion_engine/plan/pipeline.py` | Edit: pass enriched signals to scan policy inference |
| `tests/unit/datafusion_engine/test_scan_policy_inference.py` | Edit: tests for new signal-based overrides |

**Implementation checklist:**

- [ ] Add new fields to `PlanSignals`
- [ ] Extract sort-key information from DataFusion plan `SortExec` nodes
- [ ] Implement sort-order match detection in `_infer_override_for_scan()`
- [ ] Add column-pruning ratio from plan projection vs full schema
- [ ] Record enriched signal sources in scan policy override artifacts
- [ ] Add payoff measurement: record scan volume before/after overrides as artifact
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ -v`

**Decommission:** None (enrichment of existing infrastructure).

---

### 5.2 Build Artifact Calibrator (Closed-Loop Optimization)

**Rationale:** Move from one-way planning to a learning loop. Post-run calibrator reads
execution outcome artifacts and updates policy defaults for future runs using bounded,
deterministic update rules. This is "inference-driven" without unsafe online adaptation.

**Calibration loop:**

```python
# src/relspec/policy_calibrator.py (NEW FILE)
class PolicyCalibrationResult(StructBaseStrict, frozen=True):
    policy_domain: str           # "scan", "cache", "maintenance"
    previous_threshold: float
    calibrated_threshold: float
    evidence_count: int
    confidence: float
    bounded: bool                # True if bounded by min/max constraints

def calibrate_scan_thresholds(
    outcome_artifacts: Sequence[ScanPolicyOutcomeArtifact],
    *, current_thresholds, bounds: CalibrationBounds,
) -> PolicyCalibrationResult: ...
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/policy_calibrator.py` | Create: calibration logic with bounded update rules |
| `src/relspec/calibration_bounds.py` | Create: min/max bounds for each policy domain |
| `src/serde_artifacts.py` | Edit: add `PolicyCalibrationArtifact` payload type |
| `src/serde_artifact_specs.py` | Edit: register calibration spec |
| `tests/unit/relspec/test_policy_calibrator.py` | Create: unit tests |

**Implementation checklist:**

- [ ] Define `CalibrationBounds` struct with min/max for each tunable parameter
- [ ] Implement `calibrate_scan_thresholds()` using exponential moving average bounded by min/max
- [ ] Implement `calibrate_maintenance_thresholds()` for maintenance interval calibration
- [ ] Record calibration results as typed artifacts
- [ ] Add rollout controls: `CalibrationMode = Literal["off", "warn", "enforce"]`
- [ ] Require measurable payoff gates: reject calibration if no improvement measured
- [ ] Run: `uv run pytest tests/unit/relspec/test_policy_calibrator.py -v`

**Decommission:** None (new capability).

---

### 5.3 Phase C Exit Criteria and Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Scan policy signal sources | 2 (small table, pushed filters) | 6+ |
| Scan volume payoff measurement | unmeasured | per-query artifact |
| `pruning_effectiveness_score` by workload class (V3 KPI) | unmeasured | monotonic improvement |
| Calibration rollout controls | none | off/warn/enforce per domain |
| Measurable payoff gates | none | required for policy promotion |

---

## 6. Phase C' (Parallel): Infrastructure Hardening

**Goal:** Independent tracks that don't block the critical path. Typed artifact
governance, plan-statistics scheduling, and invariant enforcement.

### 6.1 Artifact Governance (Wave 5) - COMPLETE

**V3-verified:** `uv run scripts/audit_artifact_callsites.py` reports **175/175**
`record_artifact` callsites typed, **0** raw-string callsites.

**Remaining work:** Shift from "string to spec migration" (done) to **payload schema
strengthening and governance automation** (V3 §2.4, §4.5):

- Ensure high-value specs always declare `payload_type` (typed msgspec Struct validation)
- Enforce schema-level compatibility in CI
- Track typed vs untyped spec ratio as a governance metric

| File | Action |
|------|--------|
| `src/serde_artifact_specs.py` | Audit: ensure all registered specs have `payload_type` |
| CI pipeline | Add: artifact governance check as CI gate |

---

### 6.2 Plan-Statistics-Driven Scheduling

**Rationale:** The scheduling infrastructure is complete but uses uniform costs.
Plan-statistics-driven costs enable the scheduler to prioritize large, critical-path
tasks and schedule small tasks to fill gaps (HEFT-style scheduling).

**Current state (cost model exists, uniform defaults):**

```python
# src/relspec/execution_plan.py:37-45
class ScheduleCostContext(StructBaseStrict, frozen=True):
    task_costs: Mapping[str, float] | None = None
    bottom_level_costs: Mapping[str, float] | None = None
    # ... betweenness_centrality, articulation_tasks, bridge_tasks

# When task_costs is None, defaults to priority or 1.0 per task
```

**Statistics-based cost estimation:**

```python
def derive_task_costs_from_plan(
    plan_metrics: Mapping[str, TaskPlanMetrics],
) -> dict[str, float]:
    costs: dict[str, float] = {}
    for task_name, metrics in plan_metrics.items():
        row_cost = metrics.stats_row_count or 1000
        byte_cost = metrics.stats_total_bytes or 1_000_000
        costs[task_name] = (row_cost / 1000) + (byte_cost / 1_000_000)
    return costs
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/execution_plan.py` | Edit: add `derive_task_costs_from_plan()` |
| `src/relspec/rustworkx_schedule.py` | Edit: thread `task_costs` from plan metrics into `ScheduleOptions.cost_context` |
| `src/hamilton_pipeline/driver_factory.py` | Edit: compute task costs from plan metrics and pass to scheduler |
| `tests/unit/relspec/test_execution_plan.py` | Edit: tests for plan-statistics-derived costs |

**Implementation checklist:**

- [ ] Implement `derive_task_costs_from_plan()` using `stats_row_count` and `stats_total_bytes`
- [ ] Thread derived costs into `ScheduleCostContext.task_costs`
- [ ] Verify `bottom_level_costs()` uses derived costs (note: takes `rx.PyDiGraph`, not `TaskGraph` - access via `task_graph.graph`)
- [ ] Verify `task_slack_by_task()` uses derived costs
- [ ] Add integration test comparing schedule order with uniform vs statistics-based costs
- [ ] Run: `uv run pytest tests/unit/relspec/ tests/integration/relspec/ -v`

**Prerequisite:** Section 3.3 (capability adapter hardening) for reliable stats access.

**Decommission:** None (enrichment).

---

### 6.3 Phase C' Exit Criteria and Metrics

| Metric | Current | Target |
|--------|---------|--------|
| `record_artifact(` with raw strings | 0 | 0 (CI-gated) |
| Artifact specs with `payload_type` | partial | 100% for high-value specs |
| Scheduling cost source | uniform defaults | plan-statistics-derived |
| Schedule order correlation with actual execution time | unmeasured | improving |

---

## 7. First-Half Dependency Graph

```
Phase A: Correctness and Integration Closure
  3.1 CI-gate drift checks (already at 0)
    |
    +---> 3.2 Verify resolver identity (already instrumented)
    |
    +---> 3.3 Harden capability adapter (coverage expansion)
    |
    +---> 3.4 Tune outcome maintenance (threshold + diagnostics quality)
    |
    v
Phase B: Policy Compilation and Evidence Model
    |
    +---> 4.1 CompiledExecutionPolicy artifact
    |       |
    +---> 4.2 Graph-derived cache policy (replaces naming heuristic)
    |       |
    +---> 4.3 Expand policy validation rules
    |       |
    +---> 4.4 Confidence model for inference decisions
    |
    v
Phase C: Adaptive Optimization with Bounded Feedback
    |
    +---> 5.1 Enrich scan policy signals + payoff measurement
    |
    +---> 5.2 Artifact calibrator (closed-loop)

Phase C' (parallel with B and C):
    +---> 6.1 Artifact governance strengthening (Wave 5 complete; payload schema work)
    +---> 6.2 Plan-statistics-driven scheduling
```

**Critical path:** Phase A (verify gates) -> Phase B (CompiledExecutionPolicy) -> Phase C (calibrator)

**Independent tracks:**
- Phase C' (any time; no blocking dependencies)
- 3.4 maintenance tuning (after 3.1, low risk)

---

## 8. V3 Review: Revised Execution Order Summary

The V3 review proposes a simplified 5-phase execution order that maps to the phases above:

| V3 Phase | Maps To | Focus |
|----------|---------|-------|
| **R0** (Immediate) | Phase A | Rebaseline: remove completed tasks, keep drift checks as CI gates |
| **R1** | Phase B (4.1 + 4.2) | Compile policy authority; replace `_default_semantic_cache_policy` |
| **R2** | Phase C + C' (5.1 + 6.2) | Evidence depth: enrich `PlanSignals`, plan-stats costs, evidence coherence |
| **R3** | Phase H (second half) | Corrected schema derivation (H.1 with metadata enrichment gates) |
| **R4** | Phase C (5.2) | Closed-loop: confidence model + calibrator + counterfactual replay |

---

## 9. Phase D: Semantic Inference Activation

**Goal:** Activate underused inference infrastructure to reduce per-view specification
boilerplate. Relationship specs shrink from ~40 lines to ~5 lines. Cache policy
becomes self-maintaining. Redundant manual join declarations eliminated.

**Parallelism:** Phase D can start in parallel with Phase B. It has no dependency on
policy compilation - the inference infrastructure is already built and used in production
via `require_join_strategy()`.

### 9.1 Expand `infer_join_strategy()` Coverage in IR Compilation

**Rationale:** `infer_join_strategy()` is already called in production via
`require_join_strategy()` at `compiler.py:806` (V3-confirmed), but many relationship
specs still declare join strategies and join keys manually. All 9 quality specs hardcode
`left_on=["file_id"], right_on=["file_id"]` when this is inferable from schemas.

**Key architectural elements:**

```python
# src/semantics/joins/inference.py:372-440
def infer_join_strategy(
    left_schema: AnnotatedSchema, right_schema: AnnotatedSchema,
    *, hint: JoinStrategyType | None = None,
) -> JoinStrategy | None: ...

# src/semantics/joins/inference.py:36-94
@dataclass(frozen=True)
class JoinCapabilities:
    has_file_identity: bool
    has_spans: bool
    has_entity_id: bool
    has_symbol: bool
    fk_columns: tuple[str, ...]

    @classmethod
    def from_schema(cls, schema: AnnotatedSchema) -> JoinCapabilities: ...

# src/semantics/types/annotated_schema.py:268-298
# ALREADY EXISTS but UNUSED:
def infer_join_keys(self, other: AnnotatedSchema) -> list[tuple[str, str]]:
    """Infer potential join keys using compatibility groups."""
```

**Target: remove hardcoded join keys from quality specs:**

```python
# BEFORE (all 9 specs):
QualityRelationshipSpec(left_on=["file_id"], right_on=["file_id"], ...)

# AFTER (join keys inferred at compile time):
QualityRelationshipSpec(...)  # left_on/right_on omitted
# Compiler calls: join_keys = left_schema.infer_join_keys(right_schema)
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/compiler.py` | Edit: use `infer_join_keys()` when `left_on`/`right_on` empty |
| `src/semantics/quality.py` | Edit: make `left_on` and `right_on` optional (default `()`) |
| `src/semantics/quality_specs.py` | Edit: remove hardcoded join keys from all 9 specs |
| `src/semantics/types/annotated_schema.py` | Edit: ensure `infer_join_keys()` robust for all pairs |
| `tests/unit/semantics/test_join_inference.py` | Edit: tests for schema-inferred join keys |

**Implementation checklist:**

- [ ] Make `left_on` and `right_on` default to `()` in `QualityRelationshipSpec`
- [ ] In compiler `relate()`: when `left_on` is empty, call `left_schema.infer_join_keys(right_schema)`
- [ ] Verify inferred keys match current hardcoded keys (`["file_id"]`) for all 9 specs
- [ ] Remove hardcoded join keys from all 9 quality specs
- [ ] Add assertion: inferred keys must be non-empty for relationship compilation to succeed
- [ ] Run: `uv run pytest tests/unit/semantics/ tests/integration/ -v`

**Decommission targets:**

| Target | Location | Reason |
|--------|----------|--------|
| 18 hardcoded `left_on`/`right_on` entries | `src/semantics/quality_specs.py` | Schema inference |

---

### 9.2 Implement Relationship Spec Templates

**Rationale:** 5 of 9 relationship specs follow an identical structural template
(entity-to-symbol via span geometry). A template factory reduces each from ~40 lines
to ~5 lines, eliminating ~150 lines of boilerplate.

**Repeated pattern (5 specs):**
- Join on file_id (always, and inferable per 9.1)
- Hard predicate: span overlap or span contains
- Features: exact span start/end match
- Rank: partition by left entity ID, order by score desc, keep best 1
- Output: entity_id, symbol, path, bstart, bend

**Template factory:**

```python
# src/semantics/quality_templates.py (NEW FILE)
def entity_symbol_relationship(
    name: str,
    left_view: str,
    right_view: str,
    *,
    span_strategy: Literal["overlap", "contains"] = "overlap",
    additional_hard: Sequence[HardPredicate] = (),
    base_score: int = 2000,
    base_confidence: float = 0.95,
    additional_features: Sequence[Feature] = (),
    provider: str = "unknown",
    origin: str = "semantic_compiler",
) -> QualityRelationshipSpec:
    """Build entity-to-symbol spec from minimal parameters.
    Infers: join keys, join type, ranking, file quality join, output projection.
    """
    ...
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/quality_templates.py` | Create: `entity_symbol_relationship()` factory |
| `src/semantics/quality_specs.py` | Edit: replace 5 specs with template calls (~200 → ~50 lines) |
| `tests/unit/semantics/test_quality_templates.py` | Create: unit tests |

**Implementation checklist:**

- [ ] Implement `entity_symbol_relationship()` factory encapsulating shared fields
- [ ] Define `_span_predicate()` helper for overlap vs contains
- [ ] Replace 5 entity-to-symbol specs with template calls
- [ ] Verify output specs structurally identical to current specs
- [ ] Keep non-template specs (e.g., ID-based matching) as-is
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission:** ~150 lines of boilerplate in `quality_specs.py`.

---

### 9.3 Phase D Exit Criteria

| Metric | Current | After Phase D |
|--------|---------|---------------|
| Lines per relationship spec | ~40 | ~5 |
| Manual join key declarations | 18 | 0 |
| `infer_join_keys()` callers | 0 | compiler (all relationships) |

---

## 10. Phase E: Entity-Centric Data Modeling

**Goal:** Elevate the semantic model from schema-centric table specs to entity
declarations that encode identity, location, and content. All normalization,
join strategy, ranking, and output schema follow from entity properties.

### 10.1 Define Entity Declaration Format

**Rationale:** Current `SemanticTableSpec` requires ~15 lines per entity with redundant
column mappings. An entity declaration captures the *concept* (identity, location,
content), and the system derives everything else.

**Current model (7 entries in `SEMANTIC_TABLE_SPECS`):**

```python
# src/semantics/registry.py:31-139
"cst_refs": SemanticTableSpec(
    table="cst_refs",
    primary_span=SpanBinding("bstart", "bend"),
    entity_id=IdDerivation(out_col="ref_id", namespace="cst_ref"),
    text_cols=("ref_text",),
)
```

**Target model:**

```python
# src/semantics/entity_model.py (NEW FILE)
class EntityDeclaration(StructBaseStrict, frozen=True):
    name: str                            # "cst_ref"
    source_table: str                    # "cst_refs"
    identity: IdentitySpec               # How entity_id is derived
    location: LocationSpec               # Span or point location
    content: tuple[str, ...] = ()        # Content columns ("ref_text",)
    relationships: tuple[str, ...] = ()  # Relationship capabilities
    span_unit: SpanUnit = "byte"         # "byte" or "line_column"
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/entity_model.py` | Create: `EntityDeclaration`, `IdentitySpec`, `LocationSpec` |
| `src/semantics/entity_registry.py` | Create: registry; generate `SemanticTableSpec` for backward compat |
| `src/semantics/registry.py` | Edit: replace `SEMANTIC_TABLE_SPECS` with generated versions |
| `tests/unit/semantics/test_entity_model.py` | Create: test declarations and spec generation |

**Implementation checklist:**

- [ ] Define structs for entity declarations
- [ ] Implement `entity_to_table_spec(decl) -> SemanticTableSpec` converter
- [ ] Create entity declarations for all 7 current entries
- [ ] Verify generated specs match current static versions exactly
- [ ] Add `span_unit` property for SCIP/bytecode entities (enables IR kind consolidation)
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission:** Manual `SEMANTIC_TABLE_SPECS` and `SEMANTIC_NORMALIZATION_SPECS` entries.

---

### 10.2 Consolidate IR View Kinds (14 → ~6)

**Rationale:** 14 `SemanticIRKind` values are defined identically in 3 locations
(`ir.py`, `spec_registry.py`, `ir_pipeline.py`). Many are bespoke variants of the
same pattern (e.g., `normalize`, `scip_normalize`, `bytecode_line_index` all normalize
with different span units).

**Consolidation mapping:**

| Current Kinds | Consolidated Kind | Parameterized By |
|---------------|------------------|------------------|
| normalize, scip_normalize, bytecode_line_index | `normalize` | `span_unit` (byte/line_column) |
| span_unnest | `normalize` | `structure` (flat/nested) |
| symtable | `derive` | derivation spec |
| join_group, relate | `relate` | join strategy + quality signals |
| union_nodes, union_edges | `union` | target schema |
| projection, finalize, export | `project` | output contract |
| diagnostic | `diagnostic` | (unchanged) |
| artifact | (remove) | (unused stub) |

**Single authority:**

```python
# src/semantics/view_kinds.py (NEW FILE)
class ViewKind(StrEnum):
    NORMALIZE = "normalize"
    DERIVE = "derive"
    RELATE = "relate"
    UNION = "union"
    PROJECT = "project"
    DIAGNOSTIC = "diagnostic"

VIEW_KIND_ORDER: Final[dict[ViewKind, int]] = {
    ViewKind.NORMALIZE: 0, ViewKind.DERIVE: 1, ViewKind.RELATE: 2,
    ViewKind.UNION: 3, ViewKind.PROJECT: 4, ViewKind.DIAGNOSTIC: 5,
}

class ViewKindParams(StructBaseStrict, frozen=True):
    span_unit: SpanUnit = "byte"
    structure: Literal["flat", "nested"] = "flat"
    derivation_spec: str | None = None
    union_target: Literal["nodes", "edges"] | None = None
    output_contract: str | None = None
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/view_kinds.py` | Create: single `ViewKind` enum with ordering and params |
| `src/semantics/ir.py` | Edit: replace `SemanticIRKind` with `ViewKind` import |
| `src/semantics/spec_registry.py` | Edit: replace `SpecKind` with `ViewKind` import |
| `src/semantics/ir_pipeline.py` | Edit: replace `_KIND_ORDER` with `VIEW_KIND_ORDER` import |
| `src/semantics/pipeline.py` | Edit: update builder dispatch |
| `tests/unit/semantics/test_view_kinds.py` | Create: consolidation correctness tests |

**Implementation checklist:**

- [ ] Define `ViewKind` enum with 6 values and `ViewKindParams` for parameterization
- [ ] Create backward-compat mapping: `old_kind_to_new(old) -> tuple[ViewKind, ViewKindParams]`
- [ ] Replace all 3 definitions with single `ViewKind` import
- [ ] Update builder dispatch to use consolidated kinds
- [ ] Verify pipeline produces identical outputs
- [ ] Run: `uv run pytest tests/unit/semantics/ tests/integration/ -v`

**Decommission:** `SemanticIRKind`, `SpecKind`, `_KIND_ORDER` (3 definitions → 1).

---

### 10.3 Builder Dispatch Factory

**Rationale:** 15 `_builder_for_*` dispatch functions in `pipeline.py` route view kinds
to builders. 7 of 15 follow an identical dict-lookup pattern. A generic dispatch factory
eliminates ~80 lines of boilerplate.

```python
# Replace 7 dict-lookup functions with:
def _dispatch_from_registry(
    registry: Mapping[str, DataFrameBuilder],
    context_label: str,
) -> Callable[[SemanticSpecIndex, _SemanticSpecContext], DataFrameBuilder]:
    def _handler(spec, context):
        builder = registry.get(spec.name)
        if builder is None:
            raise KeyError(f"Missing {context_label} for {spec.name!r}")
        return builder
    return _handler
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/pipeline.py` | Edit: replace 7 dict-lookup builders with factory; keep 4 unique-logic functions |
| `tests/unit/semantics/test_builder_dispatch.py` | Create: test dispatch factory |

**Implementation checklist:**

- [ ] Implement `_dispatch_from_registry()` factory
- [ ] Replace 7 boilerplate functions with factory instances
- [ ] Keep unique-logic functions as-is
- [ ] Verify all builders produce identical outputs
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission:** 7 dict-lookup `_builder_for_*` functions (~80 lines).

---

## 11. Phase F: Pipeline-as-Specification

**Goal:** Adding a new evidence source requires only an entity declaration and
(optionally) relationship quality signals. All normalization, join strategy, caching,
scheduling, and output projection are derived. Execution is reproducible and replayable.

### 11.1 Schema-Aware Inference Phase in Compile-Optimize-Emit Pipeline

**Rationale:** The current `compile → optimize → emit` pipeline builds IR from static
declarations. A new `infer` phase between compile and optimize resolves all derivable
properties from schemas, making view specs contain only irreducible content.

**New phase:**

```python
# src/semantics/ir_pipeline.py (addition)
def infer_semantics(ir: SemanticIR) -> SemanticIR:
    """Schema-aware inference: resolve derived properties.

    1. Resolve AnnotatedSchemas for all views
    2. Infer join strategies using infer_join_strategy()
    3. Validate declared join keys against inferred columns
    4. Derive cache policies from graph properties
    5. Derive output projections from standard templates
    """

# Updated pipeline:
def build_semantic_ir(outputs=None) -> SemanticIR:
    ir = compile_semantics(model)
    ir = infer_semantics(ir)      # NEW PHASE
    ir = optimize_semantics(ir)
    ir = emit_semantics(ir)
    return ir
```

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/ir_pipeline.py` | Edit: add `infer_semantics()` phase |
| `src/semantics/ir.py` | Edit: add `inferred_properties` field to `SemanticIRView` |
| `tests/unit/semantics/test_ir_inference_phase.py` | Create: test the inference phase |

**Implementation checklist:**

- [ ] Implement `infer_semantics()` as new IR pipeline phase
- [ ] For relate/join_group views: resolve schemas, infer strategy, validate keys
- [ ] For all views: compute graph-derived cache policy, derive output projection
- [ ] Update `build_semantic_ir()` to include new phase
- [ ] Verify identical outputs before and after
- [ ] Run: `uv run pytest tests/unit/semantics/ tests/integration/ -v`

---

### 11.2 Reproducible Execution Package

**Rationale:** Define a replayable "execution package" keyed by fingerprint for
deterministic replay, performance regression bisecting, and policy-impact attribution.

```python
# src/relspec/execution_package.py (NEW FILE)
class ExecutionPackage(StructBaseStrict, frozen=True):
    package_fingerprint: str
    manifest_hash: str
    policy_artifact_hash: str
    capability_snapshot_hash: str
    plan_bundle_fingerprints: Mapping[str, str]
    session_config_hash: str
    created_at_unix_ms: int

    @classmethod
    def from_execution(cls, *, manifest, compiled_policy,
                       capability_snapshot, plan_bundles, session_config,
    ) -> ExecutionPackage: ...
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/execution_package.py` | Create: struct and `from_execution()` factory |
| `src/serde_artifacts.py` | Edit: add payload type |
| `src/serde_artifact_specs.py` | Edit: register spec |
| `src/hamilton_pipeline/driver_factory.py` | Edit: build and record at pipeline start |
| `tests/unit/relspec/test_execution_package.py` | Create: deterministic fingerprinting tests |

**V3 KPI:** `artifact_replay_success_rate` target >99% deterministic replay parity.

---

### 11.3 Phase F Specification Density Metrics

| Metric | Current | After Phase D | After Phase E | After Phase F |
|--------|---------|---------------|---------------|---------------|
| Lines per relationship spec | ~40 | ~5 | ~3 | ~3 |
| Lines per normalization spec | ~15 | ~10 | ~1 | ~1 |
| `_builder_for_*` functions | 15 | 15 | 8 | 4 |
| `SemanticIRKind` values | 14 | 14 | 6 | 6 |
| Manual join key declarations | 18 | 0 | 0 | 0 |
| Total pipeline spec lines | ~800 | ~350 | ~200 | ~120 |

---

## 12. Phase G: Advanced Acceleration Track

**Goal:** Performance optimization is workload-aware, measured, and self-improving.
This phase is fully independent and can start any time, but benefits from Phase C's
adaptive optimization infrastructure.

### 12.1 Workload Classification and Session Profiles

```python
# src/datafusion_engine/workload/classifier.py (NEW FILE)
class WorkloadClass(StrEnum):
    BATCH_INGEST = "batch_ingest"
    INTERACTIVE_QUERY = "interactive_query"
    COMPILE_REPLAY = "compile_replay"
    INCREMENTAL_UPDATE = "incremental_update"

def classify_workload(plan_signals: PlanSignals) -> WorkloadClass: ...
```

Map workload classes to DataFusion session configurations (memory limits, parallelism,
cache sizes). Wire classification into session builder.

### 12.2 Pruning-Ladder Optimization Program (V3 §4.9)

Track pruning efficacy as a first-class product metric:

```python
# src/datafusion_engine/pruning/metrics.py (NEW FILE)
class PruningMetrics(StructBaseStrict, frozen=True):
    row_groups_total: int
    row_groups_pruned: int
    pages_total: int
    pages_pruned: int
    filters_pushed: int
    statistics_available: bool
    pruning_effectiveness: float  # 0.0-1.0
```

Extract from DataFusion `explain analyze` output. Record as typed artifact per execution.

### 12.3 Decision Provenance Graph (NEW from V3 §5.1)

Add a first-class artifact graph that links:
- Compile-time decision
- Evidence used (plan/capabilities/runtime)
- Runtime outcome metrics
- Confidence and fallback reasons

Creates direct explainability for "why policy X was chosen" and "whether it helped."

### 12.4 Counterfactual Policy Replay (NEW from V3 §5.2)

Use stored plan bundles + artifacts to evaluate "what if policy B was applied" without
mutating production policy. Benefits: safer optimization iteration, deterministic
performance attribution, regression-proof policy evolution.

### 12.5 Workload-Class Policy Compiler (NEW from V3 §5.3)

Compile distinct policy bundles for workload classes (ingest-heavy, relationship-heavy,
incremental replay, ad-hoc diagnostics) and choose deterministically at orchestration
start.

### 12.6 Fallback Quarantine Model (NEW from V3 §5.4)

Track any compatibility fallback as a first-class debt event:
- Classify fallback type
- Enforce SLO per fallback class
- Escalate from warn → fail in strict mode once migration window closes

### 12.7 External Index Acceleration (V3 §4.6 / V1 §4.11)

For dominant semantic query shapes (span-range, symbol lookup, file-scoped traversals),
evaluate external-index-assisted file pruning with handoff to DataFusion parquet
row-group/page pruning for in-file refinement.

### 12.8 Runtime Cache Stack Observability (V1 §4.10)

Beyond `df.cache()` (already integrated at `registration.py:3176`), add observability:
listing/file-metadata/statistics cache configuration, cache hit/miss telemetry, and
per-workload cache profile selection.

### 12.9 Semantic Object Model and Relation Templates as Compiled Schema (V1 §4.12)

Elevate relationship/object templates to typed, compile-validated schema layer:
template registry with versioned contracts, static validation at compile stage,
generated relation builders from templates. Aligns with entity declarations (Phase E)
and relationship spec templates (Phase D).

---

## 13. Phase H: Schema Derivation

**Goal:** Schema declarations drop from ~2,500 LOC to ~400 LOC (external boundaries
only). Schema evolution becomes automatic when struct or extractor definitions change.

**V3 critical correction on H.1:** The V3 review identifies a **primary blocker** for
schema derivation: `ExtractMetadata` has incomplete field lists for core extract datasets.

### 13.1 Extract Schema from ExtractMetadata (CORRECTED per V3 §3)

**V3 blocker: metadata incompleteness.** Template expansion currently leaves key dataset
`fields` as `None` for 5 of the core extract datasets:

| Dataset | Template Location | Fields Status |
|---------|-------------------|---------------|
| `ast_files_v1` | `extract/templates.py:537-539` | `None` |
| `libcst_files_v1` | `extract/templates.py:563-565` | `None` |
| `bytecode_files_v1` | `extract/templates.py:589-591` | `None` |
| `symtable_files_v1` | `extract/templates.py:615-617` | `None` |
| `tree_sitter_files_v1` | `extract/templates.py:755-757` | `None` |
| `scip_index_v1` | - | Complete |

V3 metadata probe: 13 total metadata specs, but only a subset have complete field lists.

**V3 blocker: static registry carries richer semantics.**

```python
# src/datafusion_engine/schema/registry.py - these encode types not in metadata:
_BASE_EXTRACT_SCHEMA_BY_NAME     # :1739 - typed PyArrow schemas
NESTED_DATASET_INDEX             # :1751 - nested struct types and protocol shapes
```

These encode nested struct types and protocol-level shapes not currently represented
in `ExtractMetadata`. Immediate decommission risks type regressions.

**Corrected H.1 migration strategy (V3 §3.3 - 5 explicit gates):**

| Gate | Name | Description |
|------|------|-------------|
| **Gate 1** | Metadata enrichment | Add explicit field-type and nested-shape descriptors to template-expanded metadata |
| **Gate 2** | Derivation parity | Generate derived schemas; assert fingerprint equality against registry schemas for all extract + nested datasets |
| **Gate 3** | Dual-authority | Run registry and derived schemas in parallel with drift artifacts |
| **Gate 4** | Cutover | Switch runtime reads to derived authority only after sustained parity |
| **Gate 5** | Decommission | Remove static constants in bounded slices, not all-at-once |

**Gate 1 target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/extract/templates.py` | Edit: populate `fields` for the 5 templates with `None` fields |
| `src/datafusion_engine/extract/metadata.py` | Edit: add `field_types` and `nested_shapes` to `ExtractMetadata` |

**Gate 2 target files:**

| File | Action |
|------|--------|
| `src/datafusion_engine/schema/derivation.py` | Create: `derive_extract_schema()` utility |
| `src/datafusion_engine/schema/field_types.py` | Create: standard field type registry |
| `tests/unit/datafusion_engine/test_schema_derivation.py` | Create: parity tests against static schemas |

**Implementation checklist (Gate 1-2 only - do not proceed to Gates 3-5 until parity proven):**

- [ ] Populate `fields` in the 5 templates that currently have `None`
- [ ] Add `field_types: Mapping[str, str]` to `ExtractMetadata` for explicit type information
- [ ] Add `nested_shapes: Mapping[str, NestedShapeSpec]` for nested struct type information
- [ ] Build standard field type registry mapping known names to PyArrow types
- [ ] Implement `derive_extract_schema()` using enriched metadata
- [ ] Write parity test: `assert derive_extract_schema(metadata) == static_schema` for all 7 base schemas
- [ ] **STOP if parity fails.** Fix metadata enrichment before proceeding.
- [ ] Run: `uv run pytest tests/unit/datafusion_engine/ -v`

**Decommission (only after Gate 4 passes):**

| Target | Location | Reason |
|--------|----------|--------|
| ~800 LOC of schema constants | `schema/registry.py` | Derived from enriched metadata |
| ~30 nested type constants | `schema/registry.py` | Derived from parent struct introspection |

---

### 13.2 Generate Serialization Schemas from Structs

**Rationale:** ~400 LOC of `_*_SCHEMA` constants mirror msgspec `StructBase` classes.
A `schema_from_struct()` utility eliminates this manual duplication.

```python
# src/utils/schema_from_struct.py (NEW FILE)
def schema_from_struct(struct_type: type[msgspec.Struct]) -> pa.Schema:
    """Generate PyArrow schema from msgspec Struct definition.
    Handles: primitives, optionals, tuples, nested structs, mappings.
    """
```

**Target files:**

| File | Action |
|------|--------|
| `src/utils/schema_from_struct.py` | Create: utility |
| `src/datafusion_engine/schema/registry.py` | Edit: replace serialization schema constants |
| `tests/unit/utils/test_schema_from_struct.py` | Create: test against known struct/schema pairs |

**Decommission:** ~15 serialization schema constants across `schema/registry.py`,
`session/runtime.py`, `obs/metrics.py`.

---

### 13.3 Unify Schema Authority

**Target:** DataFusion `information_schema` as the single schema source, populated by
extractors and view builders at registration time. The static registry becomes
unnecessary for internal schemas.

**Current competing sources:**

| Source | Coverage | After Unification |
|--------|----------|-------------------|
| Static registry (`schema/registry.py`) | All extraction + semantic | Eliminated for internals |
| `ExtractMetadata` fields | Extract datasets | Primary for extraction |
| DataFusion `information_schema` | Registered tables | Primary for all |
| View builder `df.schema()` | View outputs | Primary for views |
| Explicit boundary contracts | Pipeline IO, CPG output | **Kept explicit** |

**Key principle:** Only external boundary schemas remain explicit. All internal schemas
are derived from their authoritative source.

---

### 13.4 Schema Evidence Lattice (NEW from V3 §5.5)

Instead of a binary "registry vs metadata" authority, define ordered evidence sources:

1. **Explicit boundary contracts** (highest authority)
2. **Derived metadata contracts** (from enriched ExtractMetadata)
3. **Runtime catalog observations** (from DataFusion information_schema)

Compile a schema authority decision with confidence and provenance. This is the
principled resolution of the H.1 dual-authority problem.

---

### 13.5 Phase H Exit Criteria

| Metric | Current | After Phase H |
|--------|---------|---------------|
| Schema registry LOC | ~1,400 | ~200 (external boundaries only) |
| Serialization schema constants | ~15 | 0 (generated from structs) |
| `schema_derivation_parity_rate` (V3 KPI) | unmeasured | 100% before decommission |
| ExtractMetadata specs with complete field lists | partial | 100% |

---

## 14. Phase I: Semantic Type System Activation

**Goal:** Column name changes require zero manual updates. Routing logic queries
structural properties, not string patterns.

### 14.1 Deploy AnnotatedSchema for Join Key Selection

**Overlap with Phase D (9.1):** This item and D.1 are the same work. If D.1 is
completed first (recommended), I.1 is already done. Included here for completeness
since the V1 document listed them separately.

Deploy `AnnotatedSchema.infer_join_keys()` and
`columns_by_compatibility_group(FILE_IDENTITY)` in the compiler to replace all 9
hardcoded `left_on=["file_id"]` declarations.

---

### 14.2 Deploy Semantic Type Queries for Routing

**Rationale:** Several code paths use string name patterns to infer properties that
should come from structured metadata. The `AnnotatedSchema` and `SemanticSchema`
infrastructure exists but is underused.

**Current fragile patterns:**

| Pattern | Location | Better Source |
|---------|----------|---------------|
| `"file_id" in join_keys` | `catalog/tags.py` | `CompatibilityGroup.FILE_IDENTITY` |
| `"symbol" in name` | `catalog/tags.py` | `SemanticType.SYMBOL` |
| `"symbol" in schema.names` | `incremental/deltas.py` | `annotated.has_semantic_type(SYMBOL)` |
| `col("file_id")` | `catalog/projections.py` | `col(schema.file_identity_column())` |
| `"cpg_*"` prefix | `_default_semantic_cache_policy` | IR view kind + output locations |
| `"rel_*"` prefix | `_default_semantic_cache_policy` | IR view kind |
| `"*_norm"` suffix | `_default_semantic_cache_policy` | IR view kind |

**Impact of a column rename (e.g., `file_id` → `file_identity`):**
- 9 relationship specs break (quality_specs.py)
- 11 symtable join definitions break
- Multiple filter/select points in incremental modules break
- Join inference would adapt automatically (already uses semantic types)

**Target files:**

| File | Action |
|------|--------|
| `src/semantics/catalog/tags.py` | Edit: replace string checks with compatibility group queries |
| `src/semantics/incremental/deltas.py` | Edit: replace `"symbol" in schema.names` with `has_semantic_type()` |
| `src/semantics/catalog/projections.py` | Edit: replace `col("file_id")` with semantic type queries |
| `tests/unit/semantics/test_semantic_routing.py` | Create: test semantic-type-based routing |

**Implementation checklist:**

- [ ] Replace `"file_id" in join_keys` with `annotated.has_compatibility_group(FILE_IDENTITY)`
- [ ] Replace `"symbol" in schema.names` with `annotated.has_semantic_type(SemanticType.SYMBOL)`
- [ ] Replace `"file" in name` substring checks with view kind or schema classification
- [ ] Replace hardcoded `col("file_id")` with `col(schema.file_identity_column_name())`
- [ ] Run: `uv run pytest tests/unit/semantics/ -v`

**Decommission:** ~40 hardcoded column name references across `tags.py`, `deltas.py`,
`projections.py`.

---

### 14.3 Consolidate Threshold Constants

**Rationale:** 4 row-count thresholds across 3 files serve the same purpose (classify
tables as small/medium/large) but are defined independently and could drift.

| Constant | File | Value |
|----------|------|-------|
| `_SMALL_TABLE_ROW_THRESHOLD` | `scan_policy_inference.py` | 10,000 |
| `_ADAPTIVE_SMALL_TABLE_THRESHOLD` | `io/write.py` | 10,000 |
| `_ADAPTIVE_LARGE_TABLE_THRESHOLD` | `io/write.py` | 1,000,000 |
| `_STREAMING_ROW_THRESHOLD` | `incremental/delta_updates.py` | 100,000 |

**Consolidated replacement:**

```python
# src/relspec/table_size_tiers.py (NEW FILE)
class TableSizeTier(StrEnum):
    SMALL = "small"      # < small_threshold
    MEDIUM = "medium"    # < large_threshold
    LARGE = "large"      # >= large_threshold

class TableSizeThresholds(StructBaseStrict, frozen=True):
    small_threshold: int = 10_000
    large_threshold: int = 1_000_000

def classify_table_size(
    row_count: int | None, thresholds: TableSizeThresholds = TableSizeThresholds(),
) -> TableSizeTier: ...
```

**Target files:**

| File | Action |
|------|--------|
| `src/relspec/table_size_tiers.py` | Create: tier definition and classifier |
| `src/datafusion_engine/delta/scan_policy_inference.py` | Edit: replace threshold constant |
| `src/datafusion_engine/io/write.py` | Edit: replace 2 threshold constants |
| `src/semantics/incremental/delta_updates.py` | Edit: replace threshold constant |
| `tests/unit/relspec/test_table_size_tiers.py` | Create: unit tests |

**Decommission:** 4 scattered threshold constants.

---

## 15. Phase J: Registry Consolidation

**Goal:** Each concept has exactly one authoritative definition. No duplicate registries.

### 15.1 Single View Kind Authority

After Phase E (10.2), `ViewKind` from `view_kinds.py` is the sole import source.
Remove `SemanticIRKind` from `ir.py`, `SpecKind` from `spec_registry.py`, and
`_KIND_ORDER` from `ir_pipeline.py`.

### 15.2 Eliminate Output Naming Identity Map

`SEMANTIC_OUTPUT_NAMES` in `naming.py` maps each view name to itself (identity mapping).
Both `canonical_output_name()` and `internal_name()` are no-ops. Remove the dict;
simplify both functions to use manifest map when available, otherwise return input
unchanged.

**Target:** Remove `SEMANTIC_OUTPUT_NAMES` dict (22 identity entries), simplify
`canonical_output_name()` and `internal_name()` in `src/semantics/naming.py`.

### 15.3 Convention-Based Extractor Discovery

Replace hardcoded `TEMPLATES` and `CONFIGS` dicts in `extract/templates.py` with
convention-based module attribute discovery. Each extractor module exports standard
`TEMPLATE` and `CONFIG` attributes.

**Target:** Remove ~130 lines of static `TEMPLATES` dict and ~175 lines of static
`CONFIGS` dict.

---

## 16. Unified Dependency Graph

```
Phase A: Correctness and Integration Closure
  3.1 CI-gate drift (0 violations) → 3.2 Resolver identity (instrumented)
    → 3.3 Capability hardening → 3.4 Maintenance tuning
    |
    v
Phase B: Policy Compilation and Evidence Model
  4.1 CompiledExecutionPolicy → 4.2 Graph-derived cache → 4.3 Validation → 4.4 Confidence
    |
    v
Phase C: Adaptive Optimization
  5.1 Scan signals + payoff → 5.2 Calibrator (closed-loop)
    |
Phase C': Infrastructure Hardening (parallel with B/C)
  6.1 Artifact governance (complete; payload strengthening)
  6.2 Plan-statistics scheduling
    |
    ======== CONSOLIDATION PHASES (parallel with B) ========
    |
Phase D: Semantic Inference Activation (NO dependency on B)
  9.1 Expand join inference → 9.2 Spec templates
    |
    v
Phase E: Entity-Centric Data Modeling
  10.1 Entity declarations → 10.2 Consolidate IR kinds → 10.3 Builder factory
    |
    v
Phase F: Pipeline-as-Specification
  11.1 Schema-aware inference phase → 11.2 Reproducible execution package
    |
    ======== SCHEMA AND TYPE PHASES ========
    |
Phase H: Schema Derivation (can start in parallel with D; blocked by metadata enrichment)
  13.1 Metadata enrichment (Gate 1-2) → 13.2 Struct-to-schema → 13.3 Unified authority
    → 13.4 Schema evidence lattice
    |
Phase I: Semantic Type System Activation (partial dependency on D)
  14.1 AnnotatedSchema for joins (= D.1) → 14.2 Semantic type routing → 14.3 Thresholds
    |
Phase J: Registry Consolidation (can start in parallel with H)
  15.1 Single view kind authority (after E.2) → 15.2 Naming map → 15.3 Extractor discovery
    |
    ======== ADVANCED (independent) ========
    |
Phase G: Advanced Acceleration (any time; benefits from C)
  12.1-12.9 Workload classes, pruning, provenance, replay, fallback quarantine
```

**Critical path:** A → B (CompiledExecutionPolicy) → C (calibrator)

**Parallel opportunities:**
- D starts immediately (no dependency on A-C)
- H starts immediately but blocked by metadata enrichment gate
- J starts immediately
- G starts any time

---

## 17. Unified Metrics and KPIs

### 17.1 Drift Surface Metrics (Phase A)

| Metric | V3 Verified | Target | Gate |
|--------|-------------|--------|------|
| `CompileContext(...)` outside boundary | 0 | 0 | CI |
| `dataset_bindings_for_profile()` fallback | 0 | 0 | CI |
| `record_artifact(` string-literal names | 0 | 0 | CI |
| Resolver identity instrumentation gaps | 0 | 0 | CI |
| `compile_tracking(...)` missing | 0 | 0 | CI |

### 17.2 Policy Convergence KPIs (V3 §7)

| KPI | Current | Target |
|-----|---------|--------|
| `policy_decisions_backed_by_compiled_policy` | 0% | 100% |
| `inference_decisions_with_confidence_payload` | 0% | 100% |
| `runtime_policy_derivation_points` | multiple heuristics | 0 unmanaged |
| `writes_using_outcome_based_maintenance` | production (V3) | 100% |

### 17.3 Schema Derivation KPIs (V3 §7)

| KPI | Current | Target |
|-----|---------|--------|
| `schema_derivation_parity_rate` | unmeasured | 100% before decommission |
| `ExtractMetadata` specs with complete fields | partial | 100% |

### 17.4 Optimization KPIs (V3 §7)

| KPI | Current | Target |
|-----|---------|--------|
| `pruning_effectiveness_score` by workload class | unmeasured | monotonic improvement |
| `counterfactual_replay_match_rate` | unmeasured | >99% |
| `fallback_events_per_run` by class | unmeasured | monotonic decline to near-zero |

### 17.5 Specification Density Metrics

| Metric | Current | After D | After E | After F |
|--------|---------|---------|---------|---------|
| Lines per relationship spec | ~40 | ~5 | ~3 | ~3 |
| Lines per normalization spec | ~15 | ~10 | ~1 | ~1 |
| Manual join key declarations | 18 | 0 | 0 | 0 |
| `_builder_for_*` functions | 15 | 15 | 8 | 4 |
| Total pipeline spec lines | ~800 | ~350 | ~200 | ~120 |

### 17.6 Static Surface Metrics

| Metric | Current | After H | After I | After J |
|--------|---------|---------|---------|---------|
| Schema registry LOC | ~1,400 | ~200 | ~200 | ~200 |
| Serialization schema constants | ~15 | 0 | 0 | 0 |
| Hardcoded column name call sites | ~60 | ~60 | ~5 | ~5 |
| Row-count threshold constants | 4 | 4 | 1 | 1 |
| Output naming map entries | ~25 | ~25 | ~25 | 0 |
| View name string constants | ~30 | ~30 | ~30 | 0 |

### 17.7 Inference Coverage

| Decision | Currently Inferred | After Consolidation |
|----------|-------------------|-------------------|
| Task dependencies | Yes (plan lineage) | Yes |
| Join keys | Partially (many manual) | Yes (schema inference) |
| Join strategy | Partially (production, many overrides) | Yes (full coverage) |
| Cache policy | No (naming convention) | Yes (graph → CompiledExecutionPolicy) |
| Output projection | No (explicit) | Yes (standard templates) |
| Ranking partition key | No (explicit) | Yes (left entity ID) |
| Normalization columns | Partially | Yes (entity declaration) |
| Span unit conversion | No (bespoke IR kind) | Yes (entity span_unit) |
| Maintenance policy | Yes (V3: production) | Yes (compiled, outcome-based) |
| Scan policy | Partially (wired, basic) | Yes (enriched, feedback-calibrated) |

---

## 18. Cross-Scope Decommission Items

Items that can only be decommissioned after multiple phases complete:

### 18.1 CompileContext Class (After Phase A fully closed)

| Item | Location | Condition |
|------|----------|-----------|
| `CompileContext` class | `compile_context.py` | Zero production callers |
| `CompileContext.dataset_bindings()` | `compile_context.py` | All fallbacks removed (V3: done) |
| `CompileContext.compile()` | `compile_context.py` | All compile paths use `build_semantic_execution_context()` |

### 18.2 Static Schema Registry (After H.1 Gate 4 + H.2 + H.3)

| Item | Location | Condition |
|------|----------|-----------|
| ~800 LOC extraction schemas | `schema/registry.py` | H.1 derivation parity proven |
| ~400 LOC serialization schemas | Various | H.2 struct-to-schema validated |
| `_BASE_EXTRACT_SCHEMA_BY_NAME` | `schema/registry.py` | H.3 unified authority |

### 18.3 Triple View Kind Definition (After E.2 + J.1)

| Item | Location | Condition |
|------|----------|-----------|
| `SemanticIRKind` Literal | `ir.py` | `ViewKind` is sole import |
| `SpecKind` Literal | `spec_registry.py` | `ViewKind` is sole import |
| `_KIND_ORDER` dict | `ir_pipeline.py` | `VIEW_KIND_ORDER` is sole import |

### 18.4 Naming Convention Cache Policy (After B.4 + D.3)

| Item | Location | Condition |
|------|----------|-----------|
| `_default_semantic_cache_policy()` | `pipeline.py:262+` | `CompiledExecutionPolicy.cache_policy_by_view` consumed everywhere |

### 18.5 Hardcoded Column Names (After D.1 + I.1 + I.2)

| Item | Location | Condition |
|------|----------|-----------|
| 18 `left_on`/`right_on` entries | `quality_specs.py` | Schema inference |
| ~40 column name references | `tags.py`, `deltas.py`, `projections.py` | Semantic type routing |
| 4 threshold constants | Various | `TableSizeTier` consolidation |

### 18.6 Builder Dispatch Boilerplate (After E.2 + E.3)

| Item | Location | Condition |
|------|----------|-----------|
| 7 `_builder_for_*` functions | `pipeline.py` | Generic factory |
| 14-entry dispatch table | `pipeline.py` | Consolidated `ViewKind` |

### 18.7 Template Registry Statics (After D.2 + J.2 + J.3)

| Item | Location | Condition |
|------|----------|-----------|
| ~150 lines spec boilerplate | `quality_specs.py` | Template factory |
| `TEMPLATES` dict (~130 lines) | `extract/templates.py` | Convention discovery |
| `CONFIGS` dict (~175 lines) | `extract/templates.py` | Convention discovery |
| `SEMANTIC_OUTPUT_NAMES` dict | `naming.py` | Identity map elimination |

---

## End of Document
