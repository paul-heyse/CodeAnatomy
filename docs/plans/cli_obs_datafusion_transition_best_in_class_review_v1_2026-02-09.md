# CLI + OBS DataFusion Transition Best-in-Class Review (v1)

Date: 2026-02-09

Reviewed inputs:
- `docs/plans/cli_datafusion_transition_plan_v1_2026-02-09.md`
- `docs/plans/datafusion_planning_best_in_class_architecture_plan_v1_2026-02-09.md`

## 0) Intent

This review tightens the CLI/OBS transition plan so `src/cli` and `src/obs` can fully pivot to the Rust/DataFusion execution basis, while preserving critical public contracts and preventing hidden Hamilton/rustworkx coupling from blocking deletion of:
- `src/relspec`
- `src/hamilton_pipeline`
- legacy `src/graph` Hamilton execution path

## 1) Evidence Snapshot (Current Codebase Reality)

Key inventory:
- `src/hamilton_pipeline`: 33 Python files
- `src/obs`: 27 Python files
- `src/cli`: 26 Python files

Hamilton import coupling outside `src/hamilton_pipeline` currently exists in:
- `src/cli/commands/build.py`
- `src/cli/commands/plan.py`
- `src/cli/config_models.py`
- `src/extract/extractors/scip/setup.py`
- `src/graph/product_build.py`
- `src/runtime_models/root.py`
- `src/semantics/incremental/config.py`
- `src/obs/otel/hamilton.py`

Test blast radius (pattern-level references):
- Hamilton-related references: 29 test files
- rustworkx references: 16 test files

## 2) Mandatory Corrections To The Existing CLI Transition Plan

## 2.1 Output Contract Mismatch (High Severity)

Existing plan issue:
- `docs/plans/cli_datafusion_transition_plan_v1_2026-02-09.md:484`-`docs/plans/cli_datafusion_transition_plan_v1_2026-02-09.md:487` lists outputs (`write_semantic_normalize_delta`, `write_semantic_relate_delta`, `write_evidence_summary_delta`) that are not current Hamilton outputs.

Current code reality:
- Canonical full output set is `FULL_PIPELINE_OUTPUTS` in `src/hamilton_pipeline/execution.py:36`.
- `GraphProductBuildRequest` path currently requests a subset in `_outputs_for_request` in `src/graph/product_build.py:375`.
- Current request set excludes `write_normalize_outputs_delta`.

Required correction:
- Re-baseline transition deliverables to actual output keys and real parse points.
- Do not plan migration work against non-existent output names.

## 2.2 Extraction DAG Flatness Is Overstated (High Severity)

Existing plan issue:
- `docs/plans/cli_datafusion_transition_plan_v1_2026-02-09.md:298`-`docs/plans/cli_datafusion_transition_plan_v1_2026-02-09.md:302` and repeated sections assert "7/9 independent" with only `python_imports -> python_external`.

Current code reality:
- `repo_files` is required by AST/CST/tree-sitter/bytecode/symtable extractors:
  - `src/hamilton_pipeline/modules/task_execution.py:708`
  - `src/hamilton_pipeline/modules/task_execution.py:807`
  - `src/hamilton_pipeline/modules/task_execution.py:823`
  - `src/hamilton_pipeline/modules/task_execution.py:839`
  - `src/hamilton_pipeline/modules/task_execution.py:855`
  - `src/hamilton_pipeline/modules/task_execution.py:872`
- `python_imports` depends on `ast_imports`, `cst_imports`, `ts_imports`:
  - `src/hamilton_pipeline/modules/task_execution.py:774`
  - `src/hamilton_pipeline/modules/task_execution.py:775`
  - `src/hamilton_pipeline/modules/task_execution.py:776`
- `python_external` depends on `python_imports`:
  - `src/hamilton_pipeline/modules/task_execution.py:790`

Required correction:
- Model extraction execution as staged barriers (not one flat parallel wave).
- Update estimates and risk assumptions for Phase 1 and Phase 2.

## 2.3 Plan Command Parity Assumption Is Incomplete (High Severity)

Existing plan issue:
- Plan assumes direct migration of `plan` command semantics to Rust compiler path without explicitly adding missing APIs.

Current code reality:
- `plan` command currently depends on Hamilton/relspec plan objects and artifact bundles:
  - `src/cli/commands/plan.py:108`
  - `src/cli/commands/plan.py:109`
  - `src/cli/commands/plan.py:119`
  - `src/cli/commands/plan.py:122`
- Rust `SemanticPlanCompiler` validates and hashes specs but does not expose schedule/graph/validation bundles:
  - `rust/codeanatomy_engine/src/python/compiler.rs:31`
  - `rust/codeanatomy_engine/src/python/compiler.rs:44`

Required correction:
- Define explicit parity tiers for `plan` command.
- Add Rust/Python introspection API if full parity is required.

## 2.4 Warning Payload Shape Assumptions Are Stale (Medium Severity)

Existing plan issue:
- `docs/plans/cli_datafusion_transition_plan_v1_2026-02-09.md:654` models warnings as string tuples.
- `docs/plans/cli_datafusion_transition_plan_v1_2026-02-09.md:1290` flags a `Vec<RunWarning>` vs `Vec<String>` mismatch as unresolved.

Current code reality:
- `RunResult.warnings` is `Vec<RunWarning>`:
  - `rust/codeanatomy_engine/src/executor/result.rs:36`
- Warning aggregation is integrated into trace summary:
  - `rust/codeanatomy_engine/src/executor/pipeline.rs:227`
  - `rust/codeanatomy_engine/src/executor/pipeline.rs:228`
- Summary payload includes warning counters and code map:
  - `rust/codeanatomy_engine/src/executor/metrics_collector.rs:29`
  - `rust/codeanatomy_engine/src/executor/metrics_collector.rs:30`

Required correction:
- Update plan assumptions and downstream schema mappings to structured warnings.

## 2.5 Config Migration Scope Is Broader Than Stated (High Severity)

Existing plan issue:
- Config migration section underestimates Hamilton-specific config surface and validation touchpoints.

Current code reality:
- Hamilton config type remains in msgspec models:
  - `src/cli/config_models.py:177`
  - `src/cli/config_models.py:214`
- Hamilton config runtime model remains:
  - `src/runtime_models/root.py:138`
  - `src/runtime_models/root.py:183`
- Config template still advertises `[hamilton]`:
  - `src/cli/commands/config.py:71`
- Config loader still validates `hamilton.tags`:
  - `src/cli/config_loader.py:193`
  - `src/cli/config_loader.py:194`

Required correction:
- Add explicit config compatibility plan: parse old section, map/deprecate keys, warn, and remove over a defined window.

## 2.6 Deletion Blockers Extend Beyond `src/cli` + `src/obs` (High Severity)

Existing plan issue:
- Transition scope is too narrow for full Hamilton/rustworkx deletion.

Current code reality:
- Hamilton coupling remains in runtime/engine layers:
  - `src/engine/telemetry/hamilton.py`
  - `src/engine/runtime_profile.py:17`
  - `src/engine/runtime_profile.py:49`
- Diagnostics lineage still records Hamilton-specific artifacts/events:
  - `src/obs/diagnostics.py:320`
  - `src/datafusion_engine/lineage/diagnostics.py:656`
  - `src/datafusion_engine/lineage/diagnostics.py:673`
- Artifact spec registry still exports many Hamilton-named specs:
  - `src/serde_artifact_specs.py:241`
  - `src/serde_artifact_specs.py:1022`
  - `src/serde_artifact_specs.py:1421`

Required correction:
- Expand transition plan into adjacent modules required for true deletion readiness.

## 2.7 Public API Compatibility Risk Is Underspecified (High Severity)

Existing plan issue:
- Plan focuses on CLI path but does not treat graph public API as a compatibility boundary.

Current code reality:
- Public API still centers on:
  - `src/graph/product_build.py:98`
  - `src/graph/product_build.py:131`
- Current implementation still routes through Hamilton execution:
  - `src/graph/product_build.py:306`

Required correction:
- Preserve `GraphProductBuildRequest` + `build_graph_product` behavior (or provide a compatibility shim with deprecation policy).

## 2.8 rustworkx Removal Is Incomplete (Medium Severity)

Current code reality:
- CLI version output still reports rustworkx dependency:
  - `src/cli/commands/version.py:39`
- Optional rustworkx topo-sort path still exists:
  - `src/datafusion_engine/views/graph.py:1417`

Required correction:
- Add explicit rustworkx deprecation/removal steps and lock deterministic Kahn fallback as canonical behavior.

## 2.9 Error Classification Is Too Brittle For Production Cutover (Medium Severity)

Current code reality:
- Engine facade maps errors via message substring matching:
  - `src/engine/facade.py:34`
  - `src/engine/facade.py:37`
  - `src/engine/facade.py:40`

Required correction:
- Introduce stable Rust error codes at the PyO3 boundary and map codes in Python, not free-form message text.

## 3) Optimizations For A Best-in-Class Final Implementation

## 3.1 Introduce A Single Execution Gateway In Python

Create `ExecutionGateway` abstraction with two adapters:
- Legacy adapter (Hamilton path) for rollback window
- Rust adapter (DataFusion engine) as target default

Benefits:
- isolates cutover logic from `build` command
- enables deterministic side-by-side validation
- reduces "if legacy/if new" spread across CLI code

## 3.2 Add Shadow Mode Before Hard Cutover

For selected repos/workloads:
- run legacy and Rust paths
- compare output table schemas, row counts, key diagnostics artifacts, manifest semantics, and determinism hashes
- block rollout on parity thresholds

This should be first-class in CI and in release checklists.

## 3.3 Define Plan Command Parity Tiers Up Front

Recommended tiers:
- Tier 1 (minimum): spec hash, output target list, high-level graph summary from Rust introspection
- Tier 2: dependency edges and schedule groups
- Tier 3: validation envelopes equivalent to legacy `PlanArtifactBundle`

Do not promise Tier 3 behavior until APIs exist.

## 3.4 Stabilize Observability Contract With Versioned Events/Artifacts

Use explicit schema versioning for renamed artifacts:
- preserve old names via temporary compatibility emission
- introduce canonical DataFusion execution event namespace
- add one release of dual-emission, then remove old names

## 3.5 Make Warning And Metrics Contracts First-Class

Leverage existing Rust fields:
- `RunResult.warnings` structured warnings
- `trace_metrics_summary` warning counts by code
- `collected_metrics.operator_metrics` for per-operator analysis

Map these directly to `src/obs/otel/metrics.py` and diagnostics report sections.

## 3.6 Preserve Stable Top-Level Build API

Keep:
- `GraphProductBuildRequest` shape
- `build_graph_product(...)` entrypoint contract

Internally replace execution backend only. This avoids ecosystem churn for non-CLI callers.

## 3.7 Remove String-Matched Error Taxonomy

Add Rust-side `error_code` + `error_stage` fields and surface them in Python exceptions.
Maintain clear machine-readable categories for CLI exit-code mapping.

## 4) Additional Scope Items Missing From The Current Plan

| Item | Why It Is Required | Deliverables |
|---|---|---|
| Rust plan introspection API | Needed for `plan` parity without Hamilton/relspec | PyO3-exported plan summary/schedule/validation metadata |
| Config compatibility translator | Prevent hard break for existing `codeanatomy.toml` | old `[hamilton]` support with warnings + mapped engine fields |
| Artifact/event migration bridge | Preserve diagnostics consumers | dual-write old/new names + schema version policy |
| Engine error code taxonomy | Eliminate brittle message parsing | Rust enum + Python exception mapping + tests |
| Shadow-mode parity harness | Safe production cutover | comparator tooling + CI job + threshold gates |
| Deletion preflight scanner | Ensure no residual imports | automated scan gate failing on Hamilton/rustworkx references |
| Rollback kill-switch | Operational safety | runtime flag to temporarily force legacy backend |
| Runtime profile cleanup | Remove Hamilton knobs in profile path | migrate `tracker_config`/`hamilton_telemetry` fields and env controls |
| Version/dependency surface cleanup | Avoid stale dependency messaging | remove rustworkx from CLI version dependency payload |
| Docs and SDK contract update | Keep user-facing API accurate | architecture docs + migration docs + release notes |

## 5) Revised Execution Plan (Recommended)

## Phase 0: Contract Freeze And Acceptance Definition

Goals:
- Freeze output/diagnostic/API contracts to preserve during backend swap.

Exit criteria:
- Signed contract doc for build outputs, run manifest, diagnostics payloads, CLI JSON fields.

## Phase 1: Build Backend Seam + Shadow Capability

Goals:
- Introduce `ExecutionGateway` and route current `build` through gateway.
- Keep Hamilton as default initially.

Exit criteria:
- Shadow mode runs both backends and produces structured diff report.

## Phase 2: Rust Backend Functional Parity For `build`

Goals:
- Rust path can produce required outputs currently parsed by `build_graph_product`.
- Wire structured warnings and trace metrics into obs pipeline.

Exit criteria:
- Parity thresholds met on golden repos.
- No severity-1 output contract diffs.

## Phase 3: `plan` Command Migration (Tiered)

Goals:
- Implement Tier 1 parity at minimum.
- Extend to Tier 2/3 only when Rust introspection APIs exist.

Exit criteria:
- `plan` command works without Hamilton/relspec imports.
- Explicitly documented parity tier.

## Phase 4: Observability + Artifact Taxonomy Migration

Goals:
- Replace Hamilton-named runtime events/artifacts with DataFusion-named equivalents.
- Provide compatibility bridge during migration window.

Exit criteria:
- Diagnostics report and OTEL metrics fully populated from Rust/DataFusion runtime data.

## Phase 5: Config Model + Runtime Profile Migration

Goals:
- Add canonical engine-centric config sections.
- Keep backward-compat parser for old Hamilton keys (deprecation warnings only).

Exit criteria:
- Existing configs still load.
- New configs have no Hamilton keys.

## Phase 6: Remove rustworkx/Hamilton Residuals

Goals:
- Remove remaining rustworkx optional path and dependency references.
- Remove Hamilton-specific telemetry/profile/config artifacts.

Exit criteria:
- Static scan finds zero Hamilton/rustworkx runtime references in supported paths.

## Phase 7: Full Legacy Deletion

Goals:
- Delete `src/relspec`, `src/hamilton_pipeline`, and legacy graph execution path once all gates pass.

Exit criteria:
- CI green with legacy modules deleted.
- Documentation and release notes complete.

## 6) Non-Functional Release Gates (Best-in-Class Bar)

Required before deleting legacy stack:
- Determinism: stable spec/envelope hash behavior for fixed inputs.
- Correctness: output schema/row-count parity within approved tolerance.
- Performance: cold/warm run budgets defined and met.
- Observability: metrics, warnings, and diagnostics completeness validated.
- Safety: rollback flag tested in staging.

## 7) Recommended Immediate Edits To The Existing Transition Plan

Apply these first so downstream implementation does not drift:
- Replace stale output key table in Section 6.4 with actual live keys.
- Rewrite extraction dependency narrative to staged DAG model.
- Add explicit `plan` parity-tier section and Rust API dependency note.
- Update warning model from strings to structured warning payloads.
- Add cross-scope migration workstream (`engine/*`, `runtime_models/*`, `serde_artifact_specs.py`, `datafusion_engine/lineage/*`).
- Add API compatibility requirement for `build_graph_product`.
- Add rollout mechanics: shadow mode, rollback switch, deletion preflight scanner.

## 8) Final Recommendation

The transition is viable, but only if treated as a contract-preserving backend replacement with observability/schema migration discipline, not a narrow `src/cli` + `src/obs` refactor. The fastest safe path is:
- gateway seam first,
- shadow parity second,
- plan/obs/config migration third,
- deletion last.
