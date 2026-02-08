# Blank-Page DataFusion + DeltaLake Set-and-Forget Semantic Execution Plan v1

Date: 2026-02-07

## 1) Objective

Design a blank-page, Rust-first execution system between:

- Upstream boundary: extraction outputs + semantically compiled view definitions.
- Downstream boundary: CPG outputs.

Design target:

- Maximally deterministic behavior.
- Robust autonomous optimization with minimal custom control-plane overhead.
- Policy represented as planner-visible Rust rulepacks, not external services.
- Near-zero dependence on bespoke protocol/artifact subsystems.
- Optional observability/compliance overlays that can be switched on without changing core execution.

---

## 2) Core Hypothesis

Given:

- A strong semantic model,
- DataFusion’s full logical/physical planning pipeline,
- DeltaLake’s snapshot/transaction semantics,
- Rust execution determinism and typed interfaces,

we can replace heavy custom orchestration/governance services with:

- A deterministic session contract,
- A semantic-to-relational compiler,
- A semantic-intent to rulepack compiler,
- A single combined LogicalPlan DAG execution strategy,
- Analyzer/logical/physical optimizer rulepacks registered in deterministic order,
- Delta-native providers and commit semantics,
- Built-in optimizer/metrics/metadata capabilities.

---

## 3) Non-Goals (By Default)

The default platform does not require:

- A bespoke policy engine service.
- A bespoke protocol validator service.
- A large persistent run-artifact subsystem.

These are replaced by engine-native contracts:

- SessionContext contract,
- analyzer/optimizer/physical planning invariants with explicit custom rules,
- Delta snapshot + commit consistency,
- deterministic schema/registration contracts,
- rule-by-rule explainability through `EXPLAIN VERBOSE`.

Optional governance and artifact packs are layered later and are non-blocking.

---

## 4) Blank-Page Architecture

## 4.1 Runtime Topology

1. Extraction Loader:
   - Reads extractor Delta outputs as canonical input tables.
   - No planning logic.
2. Semantic Compiler:
   - Converts semantic view definitions to a normalized relational intent graph.
3. Rulepack Compiler:
   - Compiles semantic constraints into analyzer/logical/physical rulepacks.
   - Emits deterministic rule registration order and activation conditions.
4. Plan Combiner:
   - Produces one global LogicalPlan DAG (or minimal DAG partition set) using joins/unions/subqueries/CTEs.
5. Delta-Aware Physical Planner:
   - Uses Delta providers, scan config, pruning, and partitioning controls.
6. Execution Core:
   - Executes physical plans and writes CPG outputs to Delta.
7. Optional Compliance Overlay:
   - Can collect explain/metrics snapshots if required.

## 4.2 Language Split

- Rust owns: planning, optimization hooks, provider lifecycle, execution, commit path.
- Python owns: CLI/API ergonomics and semantic spec submission.

Default posture: Rust-heavy hot path, thin Python shell.

---

## 5) Deterministic-by-Construction Contract

## 5.1 Session Determinism Envelope

Every run is defined by a deterministic envelope:

- Engine versions (DataFusion/delta-rs/module ABI).
- Full SessionConfig snapshot.
- RuntimeEnv settings (memory/spill/object store wiring).
- Catalog/schema/table registrations.
- UDF/UDAF/UDWF/UDTF registrations.
- Semantic spec hash.
- Rulepack hash (analyzer/logical/physical rule IDs + versions + order).

No plan execution is allowed outside this envelope.

## 5.2 One-Context Rule

All plan fragments used in a run must be built in a single SessionContext universe.

Rationale:

- Prevent incompatible plan merges across function/catalog environments.
- Ensure stable analyzer/optimizer behavior.

## 5.3 Stable Naming and Type Rules

- Canonical unquoted lowercase naming for internal relations.
- Explicit quoting policy for externally mixed-case schemas.
- Canonical Arrow schema mapping and explicit casts with `arrow_cast` where needed.

## 5.4 Rulepack Determinism Contract

- Register rules once at session construction; no mid-query mutation.
- Rule ordering is explicit and stable across runs.
- Rule activation is driven by deterministic predicates over spec/session/workload class.
- The effective rule graph is fingerprinted and returned with run outputs.

---

## 6) Semantic-to-Relational Compiler Spec

## 6.1 Input

- Semantically compiled view graph from upstream compiler.
- Extracted dataset catalog (logical name -> Delta location).
- Output target contract (CPG tables/views).

## 6.2 Output

A `SemanticExecutionSpec` object with:

- Canonical relation definitions.
- Join graph + key constraints inferred from semantic model.
- Required derived views.
- Materialization endpoints.
- Parameter template definitions (where needed).
- Rule intents (semantic constraints, optimization intents, allowed rewrites).
- Rulepack activation profile (default, low-latency, replay, strict).

## 6.3 Compiler Rules

1. Build global dependency graph from semantic definitions.
2. Collapse into maximal composable relational DAG blocks.
3. Prefer single-DAG execution unless memory or latency profile mandates partition.
4. Use CTE/subquery structure for reusable subplans.
5. Use union-by-name semantics in Rust for schema drift-safe unions.
6. Compile semantic constraints into planning-visible rule intents.
7. Map intents to concrete analyzer/logical/physical rule instances.

---

## 7) DataFusion Planning and Combination Strategy

## 7.1 Combination Model

- Never concatenate explain text or physical plans.
- Compose at LogicalPlan level only.
- Use:
  - DataFrame chaining,
  - SQL CTEs/subqueries,
  - Rust `LogicalPlanBuilder` for explicit plan surgery.

## 7.2 Parameterization

Use stable plan shapes with runtime values:

- SQL prepared statements and `EXECUTE`.
- Rust `with_param_values` for compiled templates.
- Python named parameters only as outer API convenience.

## 7.3 Plan Stages

1. Parse/bind/analyze.
2. Analyzer rules.
3. Logical optimization (built-in + custom `OptimizerRule`s).
4. Physical plan generation.
5. Physical optimization (built-in + custom `PhysicalOptimizerRule`s).
6. Execution.

System design uses this pipeline directly rather than re-implementing planning logic in application code.

## 7.4 Rules-as-Policy Architecture (Rust-First)

Policy is encoded as rewrite rules and planner hooks, not external runtime checks.

Registration and control surfaces:

- `SessionContext.add_analyzer_rule(...)` for semantic and contract normalization.
- `SessionContext.add_optimizer_rule(...)` and `remove_optimizer_rule(...)` for logical rewrites.
- `SessionStateBuilder.with_physical_optimizer_rule(...)` for physical topology control.
- `SessionState.add_physical_optimizer_rule(...)` for controlled runtime augmentation when needed.

Rule classes:

1. Semantic integrity rules:
   - Enforce join-key completeness, required columns, cardinality-safe rewrites.
2. Delta-aware scan rules:
   - Ensure scans remain on native Delta providers and preserve pushdown opportunities.
3. Cost-shape rules:
   - Normalize repartition/sort/limit shapes for workload-specific latency targets.
4. Safety rules:
   - Reject or rewrite non-deterministic constructs outside approved envelopes.

Traceability model:

- `EXPLAIN VERBOSE` is the canonical per-rule trace surface.
- Capture the ordered `plan_type` transitions (`initial_*`, `after <rule>`, final).
- Persist only compact rule-impact digests by default; full explain capture remains optional.

---

## 8) DeltaLake-Native Execution Design

## 8.1 Provider Default

Always register native Delta providers, not Arrow dataset fallback.

- Preferred: `DeltaTableProvider::try_new(...)`.
- Optional curated scan sets: `.with_files(...)`.
- For CDF workloads: `DeltaCdfTableProvider`.

## 8.2 Scan Config Standard

Standardize `DeltaScanConfig` use:

- `enable_parquet_pushdown` enabled.
- `schema_force_view_types` aligned with runtime requirements.
- `wrap_partition_values` enabled unless proven harmful for workload.
- `file_column_name` enabled only for lineage/provenance-required tasks.

## 8.3 Snapshot Selection

Default mode:

- Latest consistent snapshot at run start.

Deterministic replay mode:

- Explicit version pin.

No external protocol service is required; snapshot resolution is handled in Delta/provider lifecycle.

## 8.4 Writes

Primary write modes:

- Append,
- Overwrite.

Schema evolution:

- Controlled via `schema_mode=merge/overwrite` only where explicitly needed.

## 8.5 Caching Hierarchy (Delta-First)

Caching strategy is layered and explicit:

1. Primary cache layer: Delta snapshot/log state.
   - Determines authoritative table membership and versioned schema.
   - Reused across planning/execution within the active run envelope.
2. Secondary cache layer: Parquet metadata/statistics cache.
   - Accelerates row-group/page pruning and repeated footer/stat reads.
   - Complements, but does not replace, Delta snapshot-driven file selection.
3. Optional tertiary layer: targeted file-subset scans (`with_files`) for curated workloads.

---

## 9) Set-and-Forget Self-Optimization Loop

## 9.1 Optimization Baseline

Enable and rely on engine-native optimizers:

- Projection/filter pushdown.
- Join optimization and repartition choices.
- Dynamic filters.
- TopK-aware optimizations.
- Predicate pushdown into Parquet scans.

## 9.2 Metadata and Statistics

Treat metadata/statistics optimization as Delta-first:

- Delta snapshot/log reuse is the authoritative membership and schema cache.
- Parquet metadata/statistics caching remains enabled as a complementary acceleration layer.
- Runtime metadata cache limits are tuned once per environment class.
- Table statistics remain available for planner decisions.

## 9.3 Automatic Runtime Tuning (Minimal Surface)

Implement one Rust auto-tuner with narrow scope:

Inputs:

- Query latency class,
- scan pruning effectiveness,
- spill/memory pressure,
- repartition overhead signals.

Outputs (bounded):

- `target_partitions`,
- repartition toggles,
- parquet pushdown toggles (only if regressions detected),
- rulepack activation profile (`default`, `low_latency`, `replay`, `strict`),
- batch-size bounds.

The tuner updates a bounded set of session knobs and active rule profiles; policy remains embedded in planner-visible rules.

## 9.4 Stability Guardrails

- Config changes are monotonic and bounded per run window.
- Rollback to previous stable config on regression threshold breach.
- Keep adaptation local to workload profiles, not global blind mutation.

---

## 10) Schema and Catalog Strategy

## 10.1 Schema Source of Truth

Source of truth is runtime DataFusion + Delta schema state:

- `information_schema`,
- provider schemas,
- Delta log schema metadata.

No parallel custom schema registry is required for core runtime behavior.

## 10.2 Schema Evolution Behavior

- Prefer additive evolution.
- Use explicit cast normalization where heterogeneous inputs exist.
- Use provider-level defaults only when backward compatibility is required.

## 10.3 Schema Debug Surface

Built-in tools only:

- `DESCRIBE`,
- `SHOW COLUMNS`,
- `SHOW FUNCTIONS`,
- `arrow_typeof`,
- explain plan schema rendering.

---

## 11) Minimal Control Plane (No Heavy Services)

Default control plane contains only:

- Session factory,
- semantic-spec compiler,
- rulepack compiler/registrar,
- planner/executor invocation,
- commit coordinator.

No dedicated:

- policy compiler microservice,
- protocol governance microservice,
- artifact warehouse.

Optional extensions can be attached later without changing the execution core.

---

## 12) Optional Compliance and Observability Overlay

Default off. Enable only when needed.

## 12.1 Optional Captures

- Explain rows (`EXPLAIN`, `EXPLAIN VERBOSE`, optionally `ANALYZE`).
- Effective SessionConfig snapshot.
- Effective rulepack snapshot (rule IDs, order, activation profile).
- Rule-impact digest (`before`/`after` plan fingerprints per applied rule).
- Commit summary (table/version/rows/files).

## 12.2 Optional Retention

- Short retention by default.
- Long retention only for regulatory scopes.

This keeps core execution lightweight while preserving future auditability.

---

## 13) Implementation Workstreams (Granular)

## WS1: SemanticExecutionSpec Contract

Deliverables:

- Rust struct + serde contract.
- Python msgspec mirror.
- Canonical hashing method for spec identity.

Tasks:

1. Define relation, join, output, parameter, materialization, and rule-intent sections.
2. Add schema for versioned backward-compatible parsing.
3. Add strict unknown-field rejection in Rust.
4. Add rulepack activation profile and deterministic hashing fields.

## WS2: Rust Session Factory

Deliverables:

- Single deterministic `SessionContext` constructor API.

Tasks:

1. Implement config/runtime builder in Rust.
2. Register all required functions/providers in deterministic order.
3. Register analyzer/logical/physical rules in deterministic order.
4. Add environment + effective-rulepack fingerprint function.

## WS3: Delta Provider Manager

Deliverables:

- Native provider registration module.

Tasks:

1. Implement provider construction for latest and version-pinned snapshots.
2. Add optional `with_files` narrowed registration path.
3. Add CDF provider registration path.
4. Enforce no-fallback mode by default.

## WS4: Global Plan Combiner

Deliverables:

- Rust planner that builds combined DAG from semantic spec and emits rule-intent anchors.

Tasks:

1. Build relation map and dependency graph.
2. Emit CTE/subquery/join/union blocks.
3. Normalize unions using by-name semantics.
4. Validate logical plan completeness before optimization.
5. Emit deterministic rule attachment points for downstream rulepack compiler.

## WS5: Parameterized Plan Templates and Rulepack Activation

Deliverables:

- Prepared-template execution module + rulepack activation runtime.

Tasks:

1. Support prepared SQL plans and parameter execution.
2. Support Rust `with_param_values` for plan reuse.
3. Ensure parameter binding preserves plan shape identity.
4. Compile rule-intents into concrete rule instances for analyzer/logical/physical stages.
5. Activate/deactivate rule profiles deterministically by workload class.

## WS6: Execution Engine

Deliverables:

- Stream-first execution + CPG materialization writer.

Tasks:

1. Execute physical plans via Rust runtime.
2. Materialize CPG outputs to Delta with deterministic naming.
3. Return compact run result object (success, output locations, commit versions, rulepack fingerprint).

## WS7: Adaptive Tuner

Deliverables:

- Bounded auto-tuning subsystem.

Tasks:

1. Capture minimal runtime metrics needed for tuning.
2. Implement bounded knob adjustment policy.
3. Add bounded rule-profile adaptation policy.
4. Add rollback-on-regression logic.

## WS8: Schema Runtime Utilities

Deliverables:

- Runtime schema introspection helpers.

Tasks:

1. Build info-schema query helpers.
2. Add schema diff utility for additive evolution detection.
3. Add explicit cast-injection helper for known mismatches.

## WS9: Python API Shell

Deliverables:

- Thin Python facade calling Rust engine.

Tasks:

1. Accept extraction inputs + semantic view definitions.
2. Build `SemanticExecutionSpec`.
3. Call Rust planner/executor.
4. Return CPG outputs and minimal status payload.

## WS10: Optional Compliance Pack

Deliverables:

- Toggleable module, disabled by default.

Tasks:

1. Add explain capture toggles.
2. Add per-rule impact digest capture from `EXPLAIN VERBOSE`.
3. Add compact run envelope serialization.
4. Add retention controls.

---

## 14) Delivery Phases

## Phase 1: Deterministic Core

Implement WS1-WS5.

Exit:

- Combined logical plans and deterministic rulepacks execute reproducibly for target workloads.

## Phase 2: Native Delta Execution

Implement WS6.

Exit:

- End-to-end CPG output generation with native Delta providers only.

## Phase 3: Self-Optimization

Implement WS7-WS8.

Exit:

- Auto-tuning stabilizes and improves baseline without manual tuning loops.

## Phase 4: Thin Product Surface

Implement WS9.

Exit:

- Production API path is Rust-core with Python shell.

## Phase 5: Optional Governance

Implement WS10 only if needed.

Exit:

- Compliance/observability available without altering core execution behavior.

---

## 15) Determinism and Robustness Acceptance Criteria

1. Same semantic spec + same snapshot versions + same session envelope => same CPG outputs.
2. All production runs use native Delta providers; no dataset fallback path.
3. Same semantic spec + same profile => same effective rulepack fingerprint and order.
4. Global plan combination is stable and reproducible.
5. Auto-tuning never violates bounded knobs/rule-profile limits and self-recovers from regressions.
6. Rule effects are attributable via `EXPLAIN VERBOSE` stage transitions.
7. Core runtime succeeds without requiring custom policy/protocol/artifact services.

---

## 16) Feature Utilization Matrix (Doc-to-Implementation)

## 16.1 Plan Combination Features

Use directly:

- Single DAG composition via chaining, joins, unions, CTEs, subqueries.
- Rust `LogicalPlanBuilder` for explicit structural composition.
- Prepared statements and parameter binding for stable plan shapes.
- Optional Substrait for portable logical plan interchange.
- Rule-intent anchors attached to plan nodes for deterministic downstream rewrites.

Implementation usage:

- WS4 (Global Plan Combiner) and WS5 (Parameterized Plan Templates and Rulepack Activation).

## 16.2 Planning Pipeline Features

Use directly:

- Analyzer/optimizer/physical-planner pipeline.
- Rule registration through Rust hooks:
  - `add_analyzer_rule`,
  - `add_optimizer_rule` / `remove_optimizer_rule`,
  - `with_physical_optimizer_rule` / `add_physical_optimizer_rule`.
- `EXPLAIN VERBOSE` as canonical rule-by-rule trace.

Implementation usage:

- WS2 (Session Factory), WS4 (Combiner), WS5 (Rulepack Activation), WS10 (Compliance Pack).

## 16.3 Schema and Catalog Features

Use directly:

- `information_schema` and `SHOW` surfaces for runtime truth.
- `DESCRIBE`, `arrow_typeof`, `arrow_cast`, `arrow_metadata` for schema correctness/debug.
- TableProvider schema/default/constraint hooks where evolution requires it.

Implementation usage:

- WS8 (Schema Runtime Utilities), WS3 (Delta Provider Manager).

## 16.4 Delta Integration Features

Use directly:

- Native `DeltaTableProvider`.
- `DeltaScanConfig` and session-derived scan config.
- Optional `.with_files(...)` for curated file sets.
- `DeltaCdfTableProvider` for change-centric flows.
- Provider-supported append/overwrite write modes.

Implementation usage:

- WS3 (Provider Manager), WS6 (Execution Engine), WS7 (Adaptive Tuner).

## 16.5 Rust Packaging and Runtime Delivery Features

Use directly:

- PyO3 + maturin packaging path.
- `abi3` where feasible.
- manylinux/musllinux portable wheel strategy.

Implementation usage:

- WS2 + WS9 deployment envelope.

---

## 17) Canonical Runtime Defaults (Set-and-Forget Baseline)

## 17.1 Session Defaults

1. One SessionContext per run.
2. information_schema enabled.
3. target partitions initialized from environment profile (small/medium/large repo class).
4. repartition joins/aggregations/windows enabled.
5. parquet pushdown enabled.

## 17.2 Delta Defaults

1. Native Delta provider path required.
2. Delta scan config derived from session config.
3. Delta snapshot/log state reused as the primary cache layer within the run envelope.
4. latest snapshot at run start for default mode.
5. file-subset scans disabled by default and enabled only for explicit workflows.
6. explicit version pin for replay mode.
7. Parquet metadata/statistics cache enabled as secondary acceleration.

## 17.3 Tuning Defaults

1. Auto-tuner starts in observe-only mode.
2. After stability window, bounded adaptation is enabled.
3. Any detected regression reverts to last stable configuration.

## 17.4 Rulepack Defaults

1. Rulepack registration is static per run and fingerprinted.
2. Default profile is `default`; `strict` is used for replay/regulatory runs.
3. `low_latency` profile may relax non-correctness rewrites only.
4. Safety/integrity rules are always-on and non-disableable.

---

## 18) Concrete Interface and Module Blueprint (Blank-Page)

## 18.1 Rust Modules

1. `semantic_spec`:
   - Spec structs, schema versioning, strict decode.
2. `session_factory`:
   - Session construction, registration ordering, environment fingerprint.
3. `delta_provider_manager`:
   - Provider build/registration, snapshot mode, CDF path.
4. `semantic_plan_compiler`:
   - Semantic spec -> combined logical plan builder.
5. `rulepack_compiler`:
   - Rule-intent lowering to analyzer/logical/physical rule objects.
6. `rulepack_registry`:
   - Deterministic registration/order/fingerprint of active rules.
7. `plan_executor`:
   - Physical plan execution + CPG output writes.
8. `adaptive_tuner`:
   - Bounded tuning logic.
9. `compliance_overlay`:
   - Optional explain/config/commit capture.

## 18.2 Python Modules

1. `semantic_request_api`:
   - Accepts extraction dataset pointers + semantic view definitions.
2. `semantic_spec_builder`:
   - Produces validated `SemanticExecutionSpec` payload.
3. `rust_engine_facade`:
   - Single call boundary into Rust runtime.

---

## 19) Deterministic Execution Algorithm (Reference)

1. Receive extraction dataset map + semantic view definitions.
2. Build `SemanticExecutionSpec` and hash it.
3. Create deterministic SessionContext via Rust session factory.
4. Register all Delta inputs as native providers in deterministic order.
5. Compile semantic spec to combined LogicalPlan DAG.
6. Lower rule intents to analyzer/logical/physical rulepacks and register deterministically.
7. Bind parameters and finalize plan.
8. Run analyzer/optimizer/physical planning with active rule profile.
9. Execute physical plan and materialize CPG outputs to Delta.
10. Return compact result envelope:
   - output tables,
   - commit versions,
   - spec hash,
   - session fingerprint,
   - rulepack fingerprint.
11. Optionally run compliance capture if enabled.

---

## 20) Immediate Next Actions

1. Approve `SemanticExecutionSpec` field schema including rule-intent payloads.
2. Build Rust session/provider/planner/rulepack skeleton (WS1-WS5).
3. Create one vertical slice:
   - extraction Delta inputs -> semantic spec -> combined plan -> rulepack execution -> CPG Delta outputs.
4. Add bounded auto-tuner skeleton with profile switching but no-op thresholds.
5. Keep compliance overlay out-of-path until explicitly requested.
