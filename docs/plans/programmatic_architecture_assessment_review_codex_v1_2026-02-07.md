# Programmatic Architecture Assessment Review (Codex)

**Date:** 2026-02-07  
**Reviewed document:** `docs/plans/programmatic_architecture_assessment_v1_2026-02-07.md`

## 1. Scope and Method

This review was performed after environment setup with:

```bash
bash scripts/bootstrap_codex.sh && uv sync
```

Analysis sources:
- `./cq search`, `./cq calls`, and targeted source reads for all high-impact claims.
- Local DataFusion API probe (`uv run python`) to verify runtime capabilities in this repo environment.

## 2. Executive Assessment

The architecture direction is strong, but the current plan mixes three categories that should be separated:

1. Already implemented capabilities (or partially implemented scaffolding).
2. Valid improvements that are mis-scoped or mis-prioritized.
3. Net-new architectural work.

The biggest correction: the primary drift surface is broader than the current Wave A/C framing. It is not only `datafusion_engine/*`; multiple orchestration entrypoints still instantiate `CompileContext(runtime_profile=...)` directly.

## 3. Verified Corrections to the Proposed Plan

| Plan claim | Verified code state | Correction |
|---|---|---|
| `dataset_bindings_for_profile()` has 6 consumer callsites in `datafusion_engine/` | There is an additional production callsite in `src/hamilton_pipeline/driver_factory.py:632` | Wave A scope must include `hamilton_pipeline` pruning path, not only `datafusion_engine` |
| `semantics/incremental/runtime.py` still has fallback resolver logic | `IncrementalRuntime` uses injected resolver and builds registry facade from `_dataset_resolver` (`src/semantics/incremental/runtime.py:85`) | Remove fallback claim for incremental runtime; this part is already aligned |
| `SemanticRuntimeConfig` exists as a runtime surface | No production symbol usage; only string guardrail in cutover checker (`scripts/check_semantic_compiled_cutover.py:77`) | Treat as historical/stale reference, not active architecture |
| Wave C describes underuse of `SemanticExecutionContext` mainly in materialization and driver | `build_semantic_execution_context` already used in planning/bootstrap (`src/datafusion_engine/plan/pipeline.py:78`, `src/datafusion_engine/session/runtime.py:4559`) | Reframe as partial adoption and focus on remaining compile-context hot spots |
| Wave E proposes introducing DataFusion-native scheduling signals | `TaskPlanMetrics` already captures stats/partition/repartition and influences cost (`src/relspec/execution_plan.py:141`, `src/relspec/execution_plan.py:1461`) | Reframe Wave E as calibration/coverage hardening, not greenfield |
| Plan bundle needs physical-plan stats extraction | Already present via `_plan_statistics_section` and `_plan_statistics_payload` (`src/datafusion_engine/plan/bundle.py:2071`, `src/datafusion_engine/plan/bundle.py:2274`) | Keep, but harden compatibility strategy |
| 9.8 proposes dynamic Hamilton task module generation | Already implemented via `build_task_execution_module()` (`src/hamilton_pipeline/task_module_builder.py:104`) | Convert 9.8 from proposal to extension/optimization track |
| 9.7 proposes deriving normalize locations from manifest/IR | Already partially implemented via `normalize_dataset_locations_for_profile()` (`src/datafusion_engine/session/runtime.py:590`) and catalog integration (`src/datafusion_engine/dataset/registry.py:332`) | Narrow 9.7 to remaining manual data source surfaces |
| 9.2 proposes plan-derived contracts as missing capability | Contracts already derive from dataset specs and are enforced during registration (`src/datafusion_engine/views/registry_specs.py:280`, `src/datafusion_engine/views/graph.py:353`) | Recast 9.2 as contract tightening and divergence telemetry |
| 9.3 proposes plan-driven write policy from scratch | Existing `_delta_policy_context` already merges options + dataset policy + schema/lineage stats columns (`src/datafusion_engine/io/write.py:288`) | Recast as policy enrichment from plan statistics, not replacement |
| 9.6 implies phase DAG is fully static | `_view_graph_phases` is static order but conditionally inserts `scan_overrides` (`src/datafusion_engine/views/registration.py:125`) | Position as optional flexibility improvement, lower priority |
| Artifact registry scope estimated at ~65 calls | Current `record_artifact(` usage is much larger (`./cq search "record_artifact(" --literal` returned 183 matches across 41 files) | Expansion is larger and needs staged migration strategy |

## 4. DataFusion Reality Check (Environment-Specific)

Local probe results:
- `datafusion_version = 51.0.0`
- `DataFrame.execution_plan()` exists.
- Returned `ExecutionPlan` in this environment does **not** expose callable `statistics()` or `schema()` directly.

Implication:
- Keep defensive stats extraction (already present in `plan/bundle.py`).
- Any new scheduling/stats work must use capability detection, not version assumptions.

## 5. Re-Prioritized Convergence Plan

### Wave 0: Reality Alignment (Immediate)

**Goal:** Correct plan baseline before further design work.

Actions:
1. Update drift surface inventory to include `hamilton_pipeline` and `engine` compile-context paths.
2. Split items into: implemented, partially implemented, net-new.
3. Replace grep-based metric commands with `./cq` equivalents.

Exit criteria:
- A corrected baseline document exists (this file).
- All future implementation tasks reference corrected statuses.

### Wave 1: Resolver Threading Closure (Highest Priority)

**Goal:** Remove runtime re-derivation of manifest bindings from production execution paths.

Primary targets:
- `src/datafusion_engine/registry_facade.py:388`
- `src/datafusion_engine/plan/bundle.py:1907`
- `src/datafusion_engine/session/facade.py:830`
- `src/datafusion_engine/session/runtime.py:4079`
- `src/datafusion_engine/session/runtime.py:4127`
- `src/datafusion_engine/session/runtime.py:7616`
- `src/hamilton_pipeline/driver_factory.py:632`

Design note:
- Prefer passing a single resolver reference from boundary context; avoid opportunistic helper calls.

### Wave 2: Compile Boundary Convergence (High Priority)

**Goal:** Reduce repeated `CompileContext(runtime_profile=...)` assembly across orchestration.

Remaining hotspots include:
- `src/engine/materialize_pipeline.py:308`
- `src/extract/coordination/materialization.py:457`
- `src/hamilton_pipeline/modules/task_execution.py:497`
- `src/hamilton_pipeline/modules/task_execution.py:523`
- `src/semantics/pipeline.py:1394`

Recommendation:
- Thread a manifest-backed execution context and resolver through these paths.
- Add identity assertions for resolver reuse inside a single pipeline run.

### Wave 3: Extract Executor Programmatic Binding (Medium Priority)

**Goal:** Replace global mutable registration reliance with plan-derived executor mapping.

Current registry remains mutable global (`src/hamilton_pipeline/modules/extract_execution_registry.py:27`).

Recommendation:
- Build immutable executor map from `EvidencePlan` requirements.
- Preserve current registry as compatibility layer behind feature flag until migration completes.

### Wave 4: Policy and Maintenance Intelligence (Medium Priority)

**Goal:** Improve policy adaptivity based on execution outcomes and plan signals.

Keep and refine:
- Outcome-driven Delta maintenance trigger decisions (augment `resolve_delta_maintenance_plan`, `src/datafusion_engine/delta/maintenance.py:68`).
- Plan-informed scan policy tuning via existing scan override infrastructure.

Do not replace:
- Dataset-level policy declarations should remain authoritative defaults; plan outcomes should tune/override where safe.

### Wave 5: Artifact Contract Governance (Medium-Large)

**Goal:** Centralize artifact naming/version/schema rules without duplicating existing serde contracts.

Current reusable substrate already exists:
- `src/serde_schema_registry.py`
- `src/serde_artifacts.py`

Recommendation:
- Introduce artifact key registry + version governance layered on top of existing msgspec schema exports.
- Migrate by subsystem (planning -> write path -> hamilton lifecycle -> diagnostics).

## 6. Proposal Disposition (Section 9.x)

| Proposal | Disposition | Reason |
|---|---|---|
| 9.1 Output naming derivation | **Keep (narrow)** | Static map exists (`src/semantics/naming.py:12`), but migration should be manifest-backed with compatibility shim |
| 9.2 Plan-derived schema contracts | **Revise** | Contract derivation/enforcement already present; focus on divergence detection and telemetry |
| 9.3 Manifest-driven write policies | **Revise** | Policy layering exists; enrich with plan statistics, do not replace location policy model |
| 9.4 Manifest-driven maintenance | **Keep** | High-value gap: currently policy presence based, not outcome-threshold based |
| 9.5 Unified artifact schema registry | **Keep (expand scope)** | Real scale is larger (183 matches), requires phased migration and reuse of existing serde registry |
| 9.6 Registration phase derivation | **Defer** | Existing orchestration is stable and partly conditional; low immediate ROI |
| 9.7 Programmatic DataSourceConfig | **Revise** | Normalize and semantic-root derivations already in place; target only remaining manual seams |
| 9.8 Hamilton DAG generation from plan | **Mark as implemented** | Dynamic task module generation is already active |
| 9.9 Plan-derived scan policy overrides | **Keep** | Strong fit with existing scan unit framework |
| 9.10 Compile-time policy validation | **Keep (promote)** | High leverage once compile-boundary convergence lands |

## 7. Expanded Scope Recommendations

### 7.1 Add Resolver Identity Invariants

The architecture goal is not only value equality but single-authority identity. Add explicit identity checks in integration tests to ensure one resolver instance is threaded through view registration, scan overrides, CDF, and readiness diagnostics.

### 7.2 Add a DataFusion Capability Adapter

Create a small compatibility layer for plan statistics capability detection so code does not depend on assumed `ExecutionPlan` methods. This should gate metric scheduling fields and annotate diagnostics with capability status.

### 7.3 Introduce Drift Audits as CI Guardrails

Use `./cq`-based checks for:
- `dataset_bindings_for_profile` production callsites.
- direct `CompileContext(runtime_profile` usage outside compile boundary modules.
- global extract registry usage surfaces.

### 7.4 Stage Artifact Registry by Blast Radius

Given current call volume, migrate artifact identifiers in phases with an adapter that accepts both legacy string names and typed specs during transition.

## 8. Corrected Progress Metrics

Prefer `./cq`-driven metrics:

```bash
./cq search dataset_bindings_for_profile
./cq search "CompileContext(runtime_profile" --literal
./cq search _EXTRACT_ADAPTER_EXECUTORS
./cq search "record_artifact(" --literal
./cq search build_task_execution_module
./cq search TaskPlanMetrics
```

Interpretation guidance:
- Separate `src/` production matches from test/script matches.
- Track trend by artifact snapshots in `.cq/artifacts/` between milestones.

## 9. Implementation Risk Notes

1. Resolver threading changes affect many integration seams; use temporary compatibility overloads before deleting fallback helpers.
2. Compile-boundary convergence can increase object lifetime coupling; keep context immutable/frozen where possible.
3. Artifact registry migration should not block core pipeline modernization; run as parallel track with adapters.

## 10. Final Recommendation

Proceed with a corrected critical path:

1. Wave 1 resolver-threading closure (including `hamilton_pipeline` path).  
2. Wave 2 compile-boundary convergence across orchestration modules.  
3. Wave 3 extract executor binding and Wave 4 policy/maintenance intelligence in parallel.  
4. Wave 5 artifact governance as a staged infrastructure program.

This ordering maximizes correctness gains first (single manifest authority), then performance/policy sophistication, while keeping higher-cost platform cleanup decoupled from core execution correctness.
