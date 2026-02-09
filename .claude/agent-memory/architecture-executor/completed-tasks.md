# Completed Tasks - Detailed Notes

## Dataset Resolver Threading (Wave 1 - Completed 2026-02-07)

### Resolver Flow Architecture
- `SemanticExecutionContext.dataset_resolver` is the canonical source of dataset resolvers
- `ViewGraphContext.semantic_context.dataset_resolver` carries the resolver through the view graph pipeline
- `ManifestDatasetBindings` implements `ManifestDatasetResolver` protocol
- `dataset_bindings_for_profile()` in `compile_context.py` is now deprecated (wrapped with DeprecationWarning)
- Internal version: `_dataset_bindings_for_profile()` for legitimate compile-boundary callers

### Callsite Summary
- Functions 1-5 (registry_facade_for_context, etc.): callers lack resolver access, fallbacks retained
- Functions 6-7 (record_dataset_readiness, _plan_with_incremental_pruning): resolver required, fallbacks removed
- Bundle pipeline: `PlanBundleOptions.dataset_resolver` threaded through assembly/snapshot/scan paths

### Resolver Identity Guard
- Module-level in `src/datafusion_engine/views/graph.py`
- Dict-based mutable state: `_resolver_identity_guard: dict[str, int | None]`
- Reset at start of each `register_view_graph()`, checked on first non-None resolver
- Uses `id()` comparison for identity check

## Manifest-Backed Output Naming (Proposal 10.1 - Completed 2026-02-07)
- `SemanticProgramManifest.output_name_map: Mapping[str, str] | None` added as optional field
- `output_name_map_from_views()` in `naming.py` builds map from IR views + static baseline
- `canonical_output_name()` now accepts optional `manifest` kwarg (backward compat)
- `SEMANTIC_OUTPUT_NAMES` dict fully removed (J.2 completed); no static identity map remains
- manifest param typed as `object` in naming.py to avoid circular imports

## DataSourceConfig Authority-Split Builders (Proposal 10.7 - Completed 2026-02-07)
- `datasource_config_from_manifest()`: semantic-authority mode
- `datasource_config_from_profile()`: runtime-bootstrap mode
- manifest param typed as `object` to avoid circular import with `semantics.program_manifest`
- Deferred import of `ManifestDatasetBindings` inside function body

## Compile Boundary Convergence (Wave 2 - Completed 2026-02-07)
- Threaded `execution_context` from `_execute_and_record()` to `_execute_view()` via `inputs.execution_authority_context.semantic_context`
- Extracted `_resolve_cpg_compile_artifacts()` helper in `pipeline.py` to DRY the duplicated resolver+manifest resolution pattern
- Wired `record_compile_if_tracking()` into `compile_semantic_program()` and `build_semantic_execution_context()`
- Added `_CpgCompileResolution` frozen dataclass to carry resolved compile artifacts
- Key missing thread: `_execute_and_record()` -> `_execute_view()` in task_execution.py (line ~1252)

## Write Policy Enrichment Pattern (Proposal 10.3 - Completed 2026-02-07)
- `_delta_policy_context()` in `write.py` is a pure function (no profile access)
- Artifact recording must happen at the `WritePipeline` method level where `self.runtime_profile` is available
- `_adaptive_file_size_from_bundle()` extracted as helper to keep local variable count under PLR0914 limit (15)

## Builder Dispatch Factory (Phase E 10.3 - Completed 2026-02-07)
- `_dispatch_from_registry()` in `pipeline.py`: generic factory that creates handlers from registry callables
- 4 handlers replaced: span_unnest, symtable, diagnostic, finalize (dict-lookup pattern)
- 10 handlers kept: unique logic

## Convention-Based Extractor Discovery (J.3 - Completed 2026-02-07)
- File: `src/datafusion_engine/extract/templates.py`
- Pattern: replaced monolithic TEMPLATES/CONFIGS dicts with per-extractor named constants + `_discover_templates()`/`_discover_configs()` functions

## Reproducible Execution Package (Task 11.2 - Completed 2026-02-07)
- Module: `src/relspec/execution_package.py`
- `ExecutionPackageArtifact(StructBaseCompat, frozen=True)`: package_fingerprint, manifest_hash, policy_artifact_hash, etc.
- `build_execution_package()`: all-optional kwargs, graceful degradation
- CRITICAL: use `to_builtins_mapping()` (not `to_builtins()`) when passing to `record_artifact()`

## Inference Confidence Model (Phase B.4 - Completed 2026-02-07)
- `src/relspec/inference_confidence.py`: `InferenceConfidence` (StructBaseStrict), `high_confidence()`, `low_confidence()`
- Score clamping: high >= 0.8, low < 0.5
- `ScanPolicyOverride.confidence: float = 1.0` added
- `JoinStrategy.confidence: float = 1.0` added

## CompiledExecutionPolicy (Phase B.1+B.2 - Completed 2026-02-07)
- `src/relspec/compiled_policy.py`: `CompiledExecutionPolicy` (StructBaseStrict, frozen)
- `src/relspec/policy_compiler.py`: `compile_execution_policy()`, `_derive_cache_policies()`
- `_HIGH_FANOUT_THRESHOLD = 2` is the fan-out threshold constant
- `ExecutionAuthorityContext.compiled_policy: CompiledExecutionPolicy | None = None` added

## View Kind Consolidation (Phase E 10.2 - Completed 2026-02-07)
- Pyrefly rejects `Literal["x"]` assignable to `StrEnum` field; need dual types
- `VIEW_KIND_ORDER` typed `dict[str, int]` (not `dict[ViewKind, int]`)

## Schema-Aware Inference Phase (Task 11.1 - Completed 2026-02-07)
- Added `InferredViewProperties` frozen dataclass to `src/semantics/ir.py`
- Added `infer_semantics()` to `src/semantics/ir_pipeline.py` (runs between compile and optimize)
- Key insight: bundle-implied fields (file_identity -> file_id/path, span -> bstart/bend/span) must be expanded in field index
- `_BUNDLE_IMPLIED_FIELDS` mapping handles this expansion
- Join strategy inference from field metadata: span_overlap, foreign_key, symbol_match, equi_join
- Graph position: source/terminal/intermediate/high_fan_out (threshold=3)
- Cache policy: eager for high_fan_out, lazy for terminal, None otherwise
- 21 tests in `tests/unit/semantics/test_ir_inference_phase.py`

## Pushdown Contract Modeling (Scope 6 - Completed 2026-02-09)

### Architecture
- `providers/pushdown_contract.rs`: Serializable `FilterPushdownStatus` enum mirroring `TableProviderFilterPushDown` (DF 51 enum lacks serde)
- `PushdownProbe` struct: per-filter SQL text + status pairs, with query helpers (all_exact, has_inexact, status_counts)
- `probe_pushdown()` helper: invokes `provider.supports_filters_pushdown(&refs)`, converts to serializable statuses
- `registration.rs`: `probe_provider_pushdown()` async helper retrieves provider from session catalog before probing
- `scan_config.rs`: `PushdownStatus` enum bridges boolean capability flags to richer contract model

### Key Design Decisions
- `FilterPushdownStatus` uses `#[serde(rename_all = "snake_case")]` for clean JSON output
- Bidirectional `From` impls between `FilterPushdownStatus` and `TableProviderFilterPushDown`
- `PushdownStatusCounts` aggregate helper for summary statistics
- `CapabilityPushdownSummary` maps boolean capability flags to `PushdownStatus` per dimension
- 14 new tests all pass (8 in pushdown_contract, 6 in scan_config)

## Offline Optimizer Lab (Scope Item 5 - Completed 2026-02-09)

### Files
- **Created**: `rust/codeanatomy_engine/src/stability/optimizer_lab.rs` (290 LOC)
- **Edited**: `rust/codeanatomy_engine/src/stability/mod.rs` (added module + re-exports)
- **Edited**: `rust/codeanatomy_engine/src/compliance/capture.rs` (lab_traces field + record_lab_steps method)

### API
- `run_optimizer_lab(plan, rules, max_passes, skip_failed_rules)` -> `Result<LabResult>`
- `run_lab_from_ruleset(plan, ruleset, max_passes, skip_failed_rules)` -> convenience wrapper
- `diff_lab_results(baseline, candidate)` -> step-level digest comparison
- `RuleStep` (ordinal, rule_name, plan_digest) - Serde-enabled
- `LabResult` (optimized_plan, steps, rules_with_changes)

### Key API Adaptation
- DataFusion 51 `OptimizerContext::with_max_passes()` takes `u8`, not `usize`
- Clamped via `max_passes.min(u8::MAX as usize) as u8` to avoid truncation
- Observer callback: `FnMut(&LogicalPlan, &dyn OptimizerRule)`

### ComplianceCapture Integration
- Added `lab_traces: BTreeMap<String, Vec<RuleStep>>` field with `#[serde(default, skip_serializing_if)]`
- `record_lab_steps(lab_name, steps)` method for auditing
- `is_empty()` updated to include lab_traces check
- Backward compatible: empty lab_traces skipped in serialization

### Tests: 12 new (8 optimizer_lab + 4 compliance capture)
- All 285 crate tests pass with zero regressions

## Planning Surface + Format Policy + Profile Coverage (Scopes 1+2+12, 2026-02-09)

### Architecture
- `planning_surface.rs`: `PlanningSurfaceSpec` + `apply_to_builder()` + `install_rewrites()`
- `format_policy.rs`: `FormatPolicySpec` + `build_table_options()` + `default_file_formats()`
- `profile_coverage.rs`: `RuntimeProfileCoverage` + `evaluate_profile_coverage()` (31 entries)
- `mod.rs`: exports 3 new modules alongside existing 4

### Key Design Decisions
- `datafusion_functions_nested` planners exposed via `datafusion_ext::domain_expr_planners()` helper
  (not as direct dependency of codeanatomy-engine) to keep domain knowledge in datafusion_ext
- `install_rewrites()` replaces the old `install_sql_macro_factory_native`/`install_expr_planners_native`
  post-build mutation in factory.rs. Old functions preserved for backward compat (used by tests, Python bindings)
- `build_session()` uses `PlanningSurfaceSpec::default()` (minimal, no factory/planners)
- `build_session_from_profile()` conditionally populates function_factory, expr_planners, function_rewrites
- Profile coverage has 26 Applied + 5 Reserved fields (31 total matching RuntimeProfileSpec)

### API Notes
- DataFusion 51 `SessionStateBuilder` has: `with_expr_planners`, `with_function_factory`,
  `with_file_formats`, `with_table_options`, `with_table_factory`, `with_query_planner`
- `register_function_rewrite` only available on `SessionState`/`FunctionRegistry` (no builder API)
- `TableOptions::default_from_session_config(config.options())` takes `&ConfigOptions`
- `SessionConfig` is in `datafusion::prelude` (not `datafusion_execution::config`)

### Tests: 15 new (1 planning_surface + 4 format_policy + 7 profile_coverage + 3 coverage helpers)
- All 285 crate tests pass + all integration tests pass (337 total)

## Wave 3 Agent H: Artifact Completeness + Warning Propagation + Determinism Ordering (2026-02-09)

### Scopes Implemented
- **Scope 4**: Provider identity threading from registration to plan bundle artifacts
- **Scope 10**: Warning propagation replacing silent error swallowing
- **Scope 11**: Determinism ordering (envelope capture AFTER input registration)
- **Scope 3 (partial)**: TODO comments for planning_surface_hash wiring

### Files Modified
1. `executor/result.rs` - Added `warnings: Vec<String>` to RunResult + RunResultBuilder + builder methods
2. `executor/delta_writer.rs` - Added `WriteOutcome` struct + `read_write_outcome()` best-effort metadata
3. `executor/runner.rs` - provider_identities param on run_full_pipeline, warning collection, partition_by support, read_write_outcome
4. `python/materializer.rs` - Reordered envelope after registration, provider_identities threading, warning propagation (5 sites)
5. `spec/outputs.rs` - Doc comments on OutputTarget fields

### Key Design Decisions
- `read_write_outcome()` uses same `snapshot().snapshot().log_data()` API as `providers/snapshot.rs`
- `materializer.rs` reorder: `build_session` -> `register_inputs` -> `capture_envelope` (Scope 11)
- For non-profiled path, `build_session()` returns (ctx, envelope) but we drop the envelope and re-capture after registration
- `run_full_pipeline` gains `provider_identities: Vec<ProviderIdentity>` parameter (Scope 4)
- Warning propagation converts 5 `.ok()` / `.unwrap_or_default()` patterns to `match + warnings.push()`
- `partition_by` honored via `write_options.with_partition_by()` when non-empty
- Delta `table.version()` returns `Option<u32>`, mapped to `Option<i64>` via `.map(|v| v as i64)`

### Test Results
- All 298 unit tests + 51 integration/e2e/doc tests pass (349 total, 0 failures)
