# Implementation Gap Audit: `plan_control_plane_upgrade_v1_2026-02-08`

Date: 2026-02-08  
Plan audited: `docs/plans/plan_control_plane_upgrade_v1_2026-02-08.md`  
Code audited: current workspace state in `rust/codeanatomy_engine`, `rust/datafusion_ext`, `src/engine`, and `.github/workflows`.

## Verdict

The implementation is **not closed** against the plan.  
Most workstreams have meaningful groundwork, but core runtime wiring remains missing and two defects are currently execution blockers.

## Workstream Status

| Workstream | Status | Notes |
|---|---|---|
| WS-P1 PlanBundle artifacts | Partial | Artifact model exists; runtime path still returns empty bundles. |
| WS-P2 Substrait | Partial | Substrait helpers exist; not integrated into execution result path. |
| WS-P3 Typed parameters | Partial | Typed parameter module exists; root spec/compiler/Python still on legacy templates. |
| WS-P4 Rule overlay | Partial | Overlay module exists; not invoked by spec/runtime pipeline. |
| WS-P5 Delta UDTF planning primitives | Partial + error | New transform variants exist; `read_delta_cdf` call shape is currently incompatible. |
| WS-P6 Advanced UDF hooks governance | Partial | Snapshot infra exists; no CI governance hook. |
| WS-P7 Real physical metrics | Partial | Collector exists; runtime still emits synthetic metrics. |
| WS-P8 Cross-view optimization / inlining | Partial + error | Inline policy exists; downstream source resolution ignores inline cache. |
| WS-P9 Runtime profiles | Partial | Runtime profile types exist; runtime path still bypasses them. |
| WS-P10 Plan-aware cache boundaries | Partial | Policy module exists; compiler still uses legacy non-policy insertion path. |
| WS-P11 Delta maintenance integration | Partial | Maintenance module exists but executor remains stubbed and uncalled. |
| WS-P12 UDAF/UDWF optimizer integration | Mostly complete | `datafusion_ext` hook implementations are substantial; engine-side evidence integration remains thin. |
| WS-P13 OpenTelemetry tracing | Partial | Tracing module exists; no runtime wiring, and Cargo tracing feature is empty. |
| WS-P14 Named args + expression planning | Partial | Function/planner hooks exist; runtime path does not activate profile-based builder. |

## Critical Errors (Functional Breaks)

1. **ERR-01: `IncrementalCdf` compiles a 5-argument call but UDTF enforces 1 argument.**  
Evidence:
- `rust/codeanatomy_engine/src/compiler/udtf_builder.rs:33`
- `rust/datafusion_ext/src/udtf_sources.rs:278`

Impact: CDF-backed transform paths can fail at runtime.

2. **ERR-02: Inlined views are cached but never resolved during downstream compilation.**  
Evidence:
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:81`
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:357`
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:467`

Impact: Optimizer-visible inlining can produce unresolved source references in multi-view graphs.

## Partially Completed Scope (Implemented but Not Closed)

### WS-P1 / WS-P2: Plan artifacts + Substrait portability

Implemented:
- Plan bundle runtime/artifact model and helpers are present.
  - `rust/codeanatomy_engine/src/compiler/plan_bundle.rs:162`
  - `rust/codeanatomy_engine/src/compiler/plan_bundle.rs:196`
- Substrait helper module exists.
  - `rust/codeanatomy_engine/src/compiler/substrait.rs:1`

Not closed:
- Compile path returns only `Vec<(OutputTarget, DataFrame)>`.
  - `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:56`
- Materializer hardcodes empty bundle output.
  - `rust/codeanatomy_engine/src/python/materializer.rs:243`

### WS-P3: Typed parametric planning

Implemented:
- Typed parameter module and application helpers exist.
  - `rust/codeanatomy_engine/src/spec/parameters.rs:1`
  - `rust/codeanatomy_engine/src/compiler/param_compiler.rs:94`

Not closed:
- Root spec still exposes only legacy `parameter_templates`.
  - `rust/codeanatomy_engine/src/spec/execution_spec.rs:24`
- Compiler still uses env-var template substitution path.
  - `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:304`
- Python mirror still only exposes `parameter_templates`.
  - `src/engine/spec_builder.py:195`

### WS-P4: Dynamic rule overlay composition

Implemented:
- Overlay profile and session/ruleset builders exist.
  - `rust/codeanatomy_engine/src/rules/overlay.rs:38`
  - `rust/codeanatomy_engine/src/rules/overlay.rs:95`
  - `rust/codeanatomy_engine/src/rules/overlay.rs:153`

Not closed:
- No root-spec field for overlay profile.
  - `rust/codeanatomy_engine/src/spec/execution_spec.rs:16`
- No runtime callsites outside `overlay.rs` tests.
  - `rust/codeanatomy_engine/src/rules/overlay.rs:444`

### WS-P5: Delta UDTF primitives

Implemented:
- New transform variants and compiler dispatch exist.
  - `rust/codeanatomy_engine/src/spec/relations.rs:65`
  - `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:276`

Not closed:
- ERR-01 arity mismatch (above).
- Python `ViewTransform` union does not mirror new variants.
  - `src/engine/spec_builder.py:99`

### WS-P6: UDF hook governance

Implemented:
- Hook capability snapshot tooling exists.
  - `rust/datafusion_ext/src/registry_snapshot.rs:87`

Not closed:
- No CI workflow currently runs capability snapshot drift checks.
  - `.github/workflows/check_drift_surfaces.yml:1`
  - `.github/workflows/semantic_compiled_cutover.yml:1`
  - `.github/workflows/wheels.yml:1`

### WS-P7: Real physical metrics

Implemented:
- Metrics collector and bounded store exist.
  - `rust/codeanatomy_engine/src/executor/metrics_collector.rs:68`
  - `rust/codeanatomy_engine/src/tuner/metrics_store.rs:17`

Not closed:
- Collector not called by runtime pipeline.
  - `rust/codeanatomy_engine/src/executor/metrics_collector.rs:68`
  - `rust/codeanatomy_engine/src/python/materializer.rs:215`
- Synthetic metrics still emitted.
  - `rust/codeanatomy_engine/src/python/materializer.rs:220`
- Run result still ships `collected_metrics: None`.
  - `rust/codeanatomy_engine/src/python/materializer.rs:244`

### WS-P8: Cross-view inlining + source resolution

Implemented:
- Inline policy and decision computation exist.
  - `rust/codeanatomy_engine/src/compiler/inline_policy.rs:1`

Not closed:
- ERR-02 unresolved inline cache path (above).
- Planned source-resolution integration into builders is not present; builders still call `ctx.table(...)` directly.
  - `rust/codeanatomy_engine/src/compiler/view_builder.rs:26`
  - `rust/codeanatomy_engine/src/compiler/join_builder.rs:93`
  - `rust/codeanatomy_engine/src/compiler/union_builder.rs:33`

### WS-P9: Runtime profiles

Implemented:
- Runtime profile spec and profile-based session builder exist.
  - `rust/codeanatomy_engine/src/session/runtime_profiles.rs:22`
  - `rust/codeanatomy_engine/src/session/factory.rs:170`

Not closed:
- No `runtime_profile` field on root execution spec.
  - `rust/codeanatomy_engine/src/spec/execution_spec.rs:16`
- Runtime path still calls non-profile session build.
  - `rust/codeanatomy_engine/src/python/materializer.rs:115`

### WS-P10: Plan-aware cache boundaries

Implemented:
- Policy-aware cache module and policy insertion function exist.
  - `rust/codeanatomy_engine/src/compiler/cache_policy.rs:22`
  - `rust/codeanatomy_engine/src/compiler/cache_boundaries.rs:123`

Not closed:
- Compiler still calls legacy `insert_cache_boundaries`.
  - `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:91`
- No root spec cache policy contract.
  - `rust/codeanatomy_engine/src/spec/execution_spec.rs:16`

### WS-P11: Delta maintenance integration

Implemented:
- Maintenance policy/report structs and retention floor logic exist.
  - `rust/codeanatomy_engine/src/executor/maintenance.rs:20`
  - `rust/codeanatomy_engine/src/executor/maintenance.rs:116`

Not closed:
- Maintenance executor path is still documented as stub/deferred integration.
  - `rust/codeanatomy_engine/src/executor/maintenance.rs:8`
- `execute_maintenance` has no runtime callsite outside maintenance tests.
  - `rust/codeanatomy_engine/src/executor/maintenance.rs:151`
- Materializer still emits empty reports.
  - `rust/codeanatomy_engine/src/python/materializer.rs:245`

### WS-P12: UDAF/UDWF optimizer hooks

Implemented:
- `GroupsAccumulator` and retract coverage are present.
  - `rust/datafusion_ext/src/udaf_builtin.rs:147`
  - `rust/datafusion_ext/src/udaf_builtin.rs:510`
- Window function hook delegation exists.
  - `rust/datafusion_ext/src/udwf_builtin.rs:53`

Residual scope:
- Engine runtime does not yet expose explicit validation/evidence path proving WS-P12 impact in compile/execute output artifacts.

### WS-P13: Tracing

Implemented:
- Tracing scaffolding module exists.
  - `rust/codeanatomy_engine/src/executor/tracing.rs:83`

Not closed:
- Runtime path does not call `build_tracing_state` or `execution_span`.
  - `rust/codeanatomy_engine/src/executor/tracing.rs:83`
  - `rust/codeanatomy_engine/src/executor/tracing.rs:110`
- Cargo feature `tracing` is currently empty.
  - `rust/codeanatomy_engine/Cargo.toml:53`
- Planned optional dependency wiring for tracing is not present.
  - `rust/codeanatomy_engine/Cargo.toml:15`

### WS-P14: Named args + expression planning

Implemented:
- Runtime config contains function-factory/domain-planner toggles on Rust side.
  - `rust/codeanatomy_engine/src/spec/runtime.rs:49`
  - `rust/codeanatomy_engine/src/spec/runtime.rs:52`
- Session factory has install logic in profile-based path.
  - `rust/codeanatomy_engine/src/session/factory.rs:241`
  - `rust/codeanatomy_engine/src/session/factory.rs:244`

Not closed:
- Runtime path does not invoke profile-based session builder.
  - `rust/codeanatomy_engine/src/python/materializer.rs:115`
- Python runtime config mirror lacks these flags.
  - `src/engine/spec_builder.py:178`

## Entirely Neglected Scope (No Effective Delivery Evidence)

1. **Root contract expansion promised in plan Section 15 (spec additions) is not delivered.**  
No root fields for `typed_parameters`, `rule_overlay`, `runtime_profile`, `maintenance`, or `cache_policy`.  
Evidence: `rust/codeanatomy_engine/src/spec/execution_spec.rs:16`

2. **Cross-language contract parity for new runtime/spec flags is not delivered.**  
Rust runtime has advanced flags, Python runtime mirror still includes only compliance+tuner mode.  
Evidence:
- `rust/codeanatomy_engine/src/spec/runtime.rs:49`
- `src/engine/spec_builder.py:178`

3. **End-to-end pipeline integration from plan Section 17 phases is not delivered.**  
Runner path still only compile -> execute -> build basic `RunResult`, without plan bundle capture, metrics collection, tracing, or maintenance orchestration.  
Evidence:
- `rust/codeanatomy_engine/src/executor/runner.rs:90`
- `rust/codeanatomy_engine/src/python/materializer.rs:243`

4. **Capability-governance CI step from WS-P6 is absent.**  
No workflow evidence for hook capability snapshot validation.  
Evidence:
- `.github/workflows/check_drift_surfaces.yml:1`
- `.github/workflows/semantic_compiled_cutover.yml:1`
- `.github/workflows/wheels.yml:1`

5. **Typed capability-error surface (`CapabilityUnavailable`) requested by plan is absent.**  
No implementation evidence in engine source tree for that error type.

## Cross-Cutting Contract Drift

- Rust runtime config has fields not represented in Python builder.
  - `rust/codeanatomy_engine/src/spec/runtime.rs:32`
  - `src/engine/spec_builder.py:178`
- Python transform union still omits Rust-side delta transforms.
  - `src/engine/spec_builder.py:99`
  - `rust/codeanatomy_engine/src/spec/relations.rs:65`

## Final Assessment

This delivery has substantial groundwork but is not a closed implementation of `plan_control_plane_upgrade_v1_2026-02-08`.

Blocking closure items are:
1. Fix ERR-01 and ERR-02.
2. Finish root spec + Python mirror contract expansion.
3. Wire runtime pipeline to actually execute WS-P1/P2/P4/P7/P9/P10/P11/P13/P14 capabilities (not just define modules).
