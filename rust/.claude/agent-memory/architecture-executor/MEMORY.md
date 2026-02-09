# Rust Engine Architecture Memory

## Executor Module Structure (as of Scope 13)

The executor module (`codeanatomy_engine/src/executor/`) has four layers:

1. **pipeline.rs** -- Unified orchestration: compile -> bundle_capture -> materialize -> metrics -> maintenance -> build_result. Both `runner.rs` and `materializer.rs` delegate here.
2. **runner.rs** -- Native path entry point. `run_full_pipeline()` wraps `execute_pipeline()` with tracing spans. Also houses the low-level `execute_and_materialize()` and `execute_and_materialize_with_plans()` functions.
3. **materializer.rs** (in `python/`) -- Python path. Handles session construction, input registration, envelope capture, compliance capture (EXPLAIN VERBOSE), tuner logic. Delegates core execution to `pipeline::execute_pipeline()`.
4. **result.rs** -- `RunResult`, `RunResultBuilder`, `MaterializationResult`, `TuningHint` structs.

## Key Design Decisions (Scope 13)

- `execute_pipeline()` takes `envelope_hash: [u8; 32]` not full `SessionEnvelope`, because only the hash is needed internally. This avoids forcing the runner path to construct a full envelope.
- `PipelineOutcome` exposes `collected_metrics: CollectedMetrics` separately so the materializer's tuner can inspect raw metrics without re-parsing the RunResult.
- Compliance capture (EXPLAIN VERBOSE) compiles plans independently when enabled. The pipeline recompiles internally. Double-compile only occurs when compliance is explicitly enabled, and compilation is cheap relative to materialization.
- `CollectedMetrics` derives `Clone` (confirmed), enabling the pipeline to `.clone()` for both the RunResult and the PipelineOutcome.

## Module Boundaries

- `plan_bundle` types (`ProviderIdentity`, `PlanBundleArtifact`) come from `compiler::plan_bundle`
- `SemanticPlanCompiler` comes from `compiler::plan_compiler`
- `SessionEnvelope` comes from `session::envelope`
- `SemanticExecutionSpec` comes from `spec::execution_spec`
- Tracing is feature-gated: `#[cfg(feature = "tracing")]` guards all tracing code

## Test Counts

- 299 lib tests as of Wave 5 completion, all passing
- 83 integration + doc tests as of Wave 5, all passing (86 with delta-planner,delta-codec features)
- New test: `executor::pipeline::tests::test_aggregate_physical_metrics_empty`

## Wave 5 Cross-Scope Test Assertions (Agent J)

Added cross-scope integration tests across 9 test files validating all 13 scopes:

**Modified test files:**
- `session_determinism.rs` -- Scopes 1, 2, 3, 11, 12 (planning surface stability, format policy, manifest hash, envelope field, profile coverage)
- `compat_smoke.rs` -- Scopes 2, 12 (format policy compile/link, profile coverage compile/link)
- `provider_registration.rs` -- Scopes 4, 6 (provider identity determinism, pushdown probe construction/serialization)
- `plan_compilation.rs` -- Scopes 4, 8 (plan bundle artifact fields, apply_typed_parameters callable)
- `spec_contract.rs` -- Scope 8 (dual-mode parameter rejection via validate_parameter_mode)
- `rule_registration.rs` -- Scope 5 (optimizer lab empty-rules produce empty steps)
- `rule_tracing_diff.rs` -- Scope 5 (RuleStep serialization round-trip)
- `end_to_end.rs` -- Scopes 10, 13 (RunResult.warnings accessible/empty/populatable, PipelineOutcome importable)

**New test file:**
- `provider_pushdown_contract.rs` -- Scope 6 dedicated tests (probe construction, mixed statuses, serde roundtrip, empty probe)

## Pre-existing Issues

- Clippy: 16 warnings in codeanatomy-engine lib (redundant closures, type complexity, etc.), all pre-existing
- Clippy: datafusion_ext has ~46 errors with `-D warnings` (too_many_arguments, needless_as_bytes, etc.) -- pre-existing
- Python tests: 11 pre-existing failures in unit tests (pyarrow, delta session factory)
