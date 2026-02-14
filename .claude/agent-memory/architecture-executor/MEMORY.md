# Architecture Executor Memory

## Index of Detailed Notes
- `completed-tasks.md` - Detailed notes on all completed architecture tasks
- `schema-derivation.md` - Schema derivation (Phase H Gates 1-2)

## Key Architectural Patterns

### Context Layering
- `SemanticExecutionContext` (frozen dataclass in `src/semantics/compile_context.py`): semantic compile artifacts only
- `ExecutionAuthorityContext` (frozen dataclass in `src/relspec/execution_authority.py`): composes `SemanticExecutionContext` + orchestration fields
- These are deliberately layered - semantic context must NOT have orchestration fields

### Feature Flag Pattern
- Flags defined as module-level constants using `env_bool()` from `utils/env_utils.py`
- Convention: `CODEANATOMY_` prefix for env var names
- Feature flags file: `src/relspec/feature_flags.py` for relspec-scoped flags

### Extract Executor Resolution
- New path: `ExecutionAuthorityContext.executor_for_adapter()` uses `extract_executor_map`
- Legacy path: `extract_execution_registry.get_extract_executor()` uses global dict
- `_resolve_extract_handler()` in `task_execution.py` gates between them via `USE_GLOBAL_EXTRACT_REGISTRY`

### Test Fixture Patterns for Execution Contexts
- Use `MagicMock()` for `DataFusionRuntimeProfile` and `SessionContext` (too heavy to construct)
- Construct real `SemanticProgramManifest` and `ManifestDatasetBindings` (simple frozen dataclasses)
- `SemanticIR` can be mocked with basic attributes: views=(), join_groups=(), model_hash, ir_hash

## Lint/Quality Notes
- Private helper functions (prefixed `_`) don't need docstrings
- Fixture functions need `Returns` section in docstrings if they have a return type annotation
- All test methods in classes need docstrings (D102)
- Multi-line docstring summaries: keep to single line to avoid D205
- Ruff uses Google-style docstrings: `Raises:` (with colon, indented items)
- PLR0914: max 15 local vars; use "extract into helper + frozen dataclass result" pattern
- PLR2004: replace magic numbers with module-level constants (e.g., `_MIN_BINARY_INPUTS = 2`)
- PLR6104: use augmented assignment (`implied |= ...` not `implied = implied | ...`)
- BLE001: never use `# noqa: BLE001`; catch specific exceptions instead
- Scripts: use `sys.stdout.write()` instead of `print()` (T20 rule)

## Artifact System Patterns
- Typed artifacts: `src/serde_artifacts.py` (msgspec Struct types)
- Spec registration: `src/serde_artifact_specs.py` (links names to payload types)
- Test `expected_names` list in `tests/unit/test_artifact_spec_registry.py` must be updated when adding specs
- CRITICAL: use `to_builtins_mapping()` (not `to_builtins()`) for `record_artifact()` payload

### serde_artifact_specs Circular Import Chain (CRITICAL)
- Chain: `serde_artifact_specs` -> `serde_schema_registry` -> `schema_spec.relationship_specs` -> `datafusion_engine.session.runtime`
- `session/runtime.py` uses deferred import at END of file to break the cycle
- E402 is globally ignored in `pyproject.toml`, so no `# noqa` needed
- PLC0414: do NOT use `X as X` pattern for deferred imports

## msgspec Serialization Behavior
- `msgspec.to_builtins` omits fields at default values (empty tuples, None)
- Tuples stay as tuples (not lists) in `to_builtins` output
- Use `to_builtins_mapping` for `Mapping[str, object]` return type

## record_artifact Callsite Architecture
- Main function: `record_artifact()` in `src/datafusion_engine/lineage/diagnostics.py`
- Accepts `ArtifactSpec | str` as name parameter via `_resolve_artifact_name()`
- 3 calling patterns: standalone, method, profile attribute
- 100+ registered specs in `src/serde_artifact_specs.py`

## Semantic IR Pipeline (Task 11.1)
- Pipeline: `compile_semantics -> infer_semantics -> optimize_semantics -> emit_semantics`
- `InferredViewProperties` on `SemanticIRView`: join_strategy, join_keys, cache_policy, graph_position, inference_confidence
- `inference_confidence: InferenceConfidence | None` added in B.3 confidence threading
- Bundle-implied fields: `file_identity` -> file_id/path; `span` -> bstart/bend/span
- `_BUNDLE_IMPLIED_FIELDS` mapping in ir_pipeline.py expands bundles in field index
- Join strategy inference from field metadata (lightweight, no DataFusion schemas needed)
- Graph position: source/terminal/intermediate/high_fan_out (threshold=3)
- Cache policy: eager for high_fan_out, lazy for terminal, None otherwise
- `infer_semantics()` is additive: never removes/changes existing view data

## Delta Protocol Module (`src/datafusion_engine/delta/protocol.py`)
- `DeltaProtocolCompatibility` is `StructBaseCompat` (frozen) with all compatibility fields
- `__all__` lists must maintain alphabetical order

## msgspec kw_only Behavior Note
- `kw_only=True` on StructBaseStrict does NOT prevent positional args at Python __init__ time
- It's a serialization/deserialization constraint only
- Test `forbid_unknown_fields` via `msgspec.json.decode` instead

## Pyrefly/Pyright Type System Notes
- Pyrefly rejects `Literal["x"]` assignable to `StrEnum` field; need dual types
- `VIEW_KIND_ORDER` typed `dict[str, int]` (not `dict[ViewKind, int]`) so both work
- manifest param typed as `object` in naming.py to avoid circular imports
- Deferred imports inside function bodies for circular import avoidance

## CQ Quirk
- `build_view_product` in `materialize_pipeline.py` has zero callsites (public API via lazy import)

## Policy Validation Module (`src/relspec/policy_validation.py`)
- `validate_policy_bundle()` composes 7 core + 3 compiled-policy validators
- New `compiled_policy: CompiledExecutionPolicy | None = None` param (backward compat)
- Core validators: udf_feature_gate, udf_availability, delta_protocol, manifest_alignment, small_scan_policy, capability, scan_policy_compatibility
- Compiled-policy validators: `_compiled_policy_consistency_issues`, `_statistics_availability_issues`, `_evidence_coherence_issues`
- Issue codes: `compiled_policy_extra_cache_views`, `compiled_policy_extra_scan_overrides`, `stats_dependent_override_without_capabilities`, `compiled_policy_missing_fingerprint`, `compiled_policy_empty_cache_section`, `scan_override_stats_without_capabilities`
- `_STATS_REASON_KEYWORDS` for compiled policy reasons, `_PLAN_STATS_DEPENDENT_REASONS` for plan-level reasons
- `_manifest_view_names()` extracts view names from `semantic_ir.views` via `getattr()` (defensive)
- Caller in `driver_factory.py:build_plan_context()` passes `compiled_policy=authority_context.compiled_policy`
- Test monkeypatch of `validate_policy_bundle` must accept `compiled_policy` kwarg
- `test_driver_factory_config.py` fake authority context needs `compiled_policy=None` attribute

## Scan Policy Inference + InferenceConfidence (Phase B.3)
- `ScanPolicyOverride` has dual confidence: `confidence: float` (raw) + `inference_confidence: InferenceConfidence | None` (structured)
- `_build_inference_confidence()` routes to `high_confidence()`/`low_confidence()` based on 0.8 threshold
- Evidence sources: "stats", "capabilities", "lineage" - tracked per-signal
- `high_confidence()` clamps to [0.8, 1.0]; `low_confidence()` clamps to [0.0, 0.49]
- Merge strategy in pipeline.py: keep inference_confidence from override with lower raw confidence
- `scan_policy_override_artifact_payload()` and `_scan_overrides_to_mapping()` both serialize inference_confidence when present
- Existing tests construct `ScanPolicyOverride` without inference_confidence (defaults to None), backward compatible

## Workload & Pruning Infrastructure (Phase G, Section 12)
- Workload classification: `src/datafusion_engine/workload/classifier.py` (WorkloadClass StrEnum + classify_workload + session_config_for_workload)
- `session_config_for_workload()` returns `Mapping[str, str]` of DataFusion config overrides; uses deferred import of session_profiles
- DataFusion config key convention: `datafusion.execution.*`, `datafusion.optimizer.*`; advisory keys: `codeanatomy.workload.*`
- Session profiles: `src/datafusion_engine/workload/session_profiles.py` (WorkloadSessionProfile + workload_session_profile)
- Pruning metrics: `src/datafusion_engine/pruning/metrics.py` (PruningMetrics StructBaseStrict)
- Explain parser: `src/datafusion_engine/pruning/explain_parser.py` (parse_pruning_metrics with multi-pattern regex)
- Pruning tracker: `src/datafusion_engine/pruning/tracker.py` (PruningTracker + PruningSummary)
- Artifact types: WorkloadClassificationArtifact, PruningMetricsArtifact in serde_artifacts.py
- Specs: WORKLOAD_CLASSIFICATION_SPEC, PRUNING_METRICS_SPEC in serde_artifact_specs.py
- Regex gotcha: `pages` as alt in `(?:total_pages|pages_total|pages)` matches `pruned_pages` - use specific alternatives only

## Decision Provenance (Phase G, Section 12.3)
- Types: `src/relspec/decision_provenance.py` (DecisionRecord, DecisionProvenanceGraph, EvidenceRecord, DecisionOutcome)
- Recorder: `src/relspec/decision_recorder.py` (DecisionRecorder mutable builder -> immutable graph)
- Factory: `build_provenance_graph(compiled_policy, confidence_records, run_id=...)` in decision_provenance.py
- Factory bridges CompiledExecutionPolicy + InferenceConfidence -> DecisionProvenanceGraph
- Cache policy entries -> "cache_policy" domain decisions; scan overrides -> "scan_policy" domain decisions
- All compiled-policy decisions are roots (no parent chain); parent_ids used by DecisionRecorder for incremental recording
- Artifact: DecisionProvenanceGraphArtifact in serde_artifacts.py; spec: DECISION_PROVENANCE_GRAPH_SPEC
- Query helpers: decisions_by_domain, decisions_above_confidence, decisions_with_fallback, decision_children, decision_chain

## Join Group Optimization with Inferred Keys (Phase D.1)
- `_build_join_groups()` in `ir_pipeline.py` now resolves empty join keys from `inferred_properties`
- `_resolve_keys_from_inferred()` extracts FILE_IDENTITY exact-name pairs only
- Mirrors compiler's `_resolve_join_keys()` filtering: FILE_IDENTITY group, exact-name matches
- `_FILE_IDENTITY_NAMES = {"file_id", "path"}` at module level in ir_pipeline.py
- Flow: compile_semantics -> infer_semantics (adds inferred_join_keys) -> optimize_semantics (uses them in _build_join_groups)
- Parity tests in `tests/unit/semantics/test_join_inference.py::TestIRInferredKeysParityWithCompiler`
- Unit tests in `tests/unit/semantics/test_ir_inference_phase.py::TestResolveKeysFromInferred` and `TestBuildJoinGroupsWithInferredKeys`
- Golden snapshot (`tests/fixtures/semantic_ir_snapshot.json`) may need updating after this change

## InferenceConfidence Threading (Phase B.3)
- `InferenceConfidence` struct: `src/relspec/inference_confidence.py` (frozen StructBaseStrict)
- Join strategy confidence: `build_join_inference_confidence()` in `semantics/joins/inference.py`
- `JoinStrategyResult` dataclass pairs `JoinStrategy` + `InferenceConfidence`
- `infer_join_strategy_with_confidence()` convenience wrapper
- IR pipeline confidence: `_build_view_inference_confidence()` in `ir_pipeline.py`
- IR pipeline selects strongest signal (join strategy vs cache policy) for representative confidence
- Confidence score mirrors: `_STRATEGY_CONFIDENCE` and `_CACHE_POLICY_CONFIDENCE` dicts in ir_pipeline.py
- Threshold: 0.8 separates high_confidence from low_confidence (matches scan policy pattern)
- `InferenceConfidence` imported as TYPE_CHECKING in `ir.py` (safe with `from __future__ import annotations`)
- `InferenceConfidence` imported as runtime import in `ir_pipeline.py` (needed for function returns)
- Golden snapshot test does NOT capture inferred_properties (safe from changes)

## Golden Snapshot Tests
- `tests/fixtures/semantic_ir_snapshot.json` must be updated when IR pipeline changes
- Use `--update-golden` flag: `uv run pytest tests/semantics/test_semantic_ir_snapshot.py --update-golden`
- Always review diff before committing updated goldens

## Typed Parameter Planning Cutover (Scope 8)
- `validate_parameter_mode()` free fn in plan_compiler.rs: rejects specs with both typed_parameters AND parameter_templates
- Graph validator also validates mode exclusivity (defense in depth)
- When typed_parameters present: skip collect_parameter_values(), pass empty HashMap to compile_view, apply typed params to output DataFrames
- Template path unchanged when typed_parameters is empty (backward compat)
- `apply_parameters()` method on SemanticPlanCompiler for external callers
- ParameterTemplate + parameter_templates field: deprecated doc comments added with migration guide
- All 352 tests pass, no regressions

## Pushdown Contract (Scope 6)
- `providers/pushdown_contract.rs`: `FilterPushdownStatus` (serde wrapper for `TableProviderFilterPushDown`)
- DF 51 `TableProviderFilterPushDown` only derives `Debug, Clone, PartialEq, Eq` -- no serde
- `probe_pushdown()` takes `&dyn TableProvider` + `&[Expr]`; `probe_provider_pushdown()` in registration.rs fetches from session catalog
- `ctx.table_provider(name).await` returns `Result<Arc<dyn TableProvider>>` in DF 51

## Rust datafusion_ext Crate Patterns
- Crate at `rust/datafusion_ext/`; check with `cargo check -p datafusion_ext`
- Downstream `codeanatomy-engine` at `rust/codeanatomy_engine/`; check with `cargo check -p codeanatomy-engine`
- `serde` with derive feature is available
- UDAFs: `aggregate_udfs![]` macro generates `AggregateUdfSpec` entries
- `ArgBestAccumulator` shared by AnyValueDet/ArgMax/ArgMin/AsofSelect
- Sliding accumulators are separate structs for retract support
- `string_agg` replaced DataFusion re-export with custom `StringAggDetUdaf` for retract (WS-P12)
- UDWFs delegate to DataFusion builtins; sort_options/reverse_expr inherited
- `RegistrySnapshot`: scalar flags detected dynamically; aggregate/window via static known-capabilities
- `FunctionHookCapabilities` + `snapshot_hook_capabilities()` added for WS-P6 governance
- Test suite: `tests/udf_conformance.rs` (~41 tests); `information_schema_*` tests are critical contracts
- UDTF arg validation: `ensure_exact_args()` for fixed-arity; `ensure_arg_range()` for variable-arity
- Optional arg extraction: `optional_int64_arg()` / `optional_string_arg()` in udtf_sources.rs
- Delta CDF UDTF accepts 1-5 args: (uri, start_version, end_version, start_timestamp, end_timestamp)
- CDF scan range args go into `DeltaCdfScanOptions` fields; `delta_cdf_provider` version/timestamp params are for table snapshot loading

## Schema Registry Decomposition (Wave 4B)
- Split `registry.py` (4167 LOC) into 3 modules + re-export shim
- `extraction_schemas.py` (1944 LOC): struct types, file schemas, schema resolution
- `nested_views.py` (1025 LOC): NESTED_DATASET_INDEX, accessors, DataFrame builders
- `observability_schemas.py` (1327 LOC): pipeline events, validation functions
- `registry.py` (249 LOC): backward-compat re-export shim
- CRITICAL: `__all__` must include ALL re-exported symbols or ruff F401 strips them
- CRITICAL: monkeypatch tests that patch via registry module must be updated to patch canonical module
  - Tests patching `registry._derived_extract_schema_for` must patch `extraction_schemas` instead
  - Tests patching `registry.extract_schema_for` for validate_nested_types must patch `nested_views`
- Private symbols `_semantic_validation_tables`, `_resolve_nested_row_schema_authority` re-exported for test access
- pyright `cast()` needed for `pa.Schema.names` access when param typed as `object` (original used it too)

## Session Runtime Decomposition (Wave 4A)
- `session/features.py`: FeatureStateSnapshot, feature_state_snapshot, named_args_supported
- `session/introspection.py`: standalone helpers taking profile as param (schema_introspector, metrics, traces, diskcache, cdf)
- `session/config.py`: RE-EXPORT module (not canonical home) - avoids circular imports since presets use types from runtime.py
- `session/__init__.py`: lazy __getattr__ re-exports from all new modules
- `runtime.py` still re-exports extracted symbols for backward compat (83+ importers)
- KEY: can't move preset constants OUT of runtime.py because they're used internally AND would create circular import
- Cleaned up 4 dead imports from runtime.py after extraction

## Rust Engine Executor Modules (WS-P11/P13/P7)
- `executor/mod.rs` registers: delta_writer, maintenance, metrics_collector, result, runner, tracing
- Pre-existing build errors in param_compiler.rs (ScalarAndMetadata) and datafusion_ext (StringAggDetUdaf)
- `tracing` feature NOT yet in Cargo.toml (deferred to integration agent) -- cfg warnings expected
- maintenance.rs: engine-level `MaintenanceReport` (stub); MIN_VACUUM_RETENTION_HOURS=168
- tracing.rs: `#[cfg(feature = "tracing")]` dual paths; ExecutionSpanInfo always available
- metrics_collector.rs (WS-P7): CollectedMetrics + collect_plan_metrics() tree walker
  - Uses `partition_statistics(None)` not deprecated `statistics()` in DF 51
  - WIRED: materializer.rs creates physical plans BEFORE execute_and_materialize() consumes output_plans
  - Physical plans are created via df.clone().create_physical_plan() pre-execution
  - Post-execution: collect_plan_metrics() walks each captured plan tree for real spill/memory/selectivity
  - Fallback: when collected values are zero, falls back to env_profile defaults for tuner metrics

## Rust Engine PlanBundle + Substrait (WS-P1/P2)
- `compiler/plan_bundle.rs`: PlanBundleRuntime (not serializable) + PlanBundleArtifact (Serialize/Deserialize)
- `compiler/substrait.rs`: feature-gated via `#[cfg(feature = "substrait")]` with stub fallbacks
- RunResult in `executor/result.rs` extended with `plan_bundles: Vec<PlanBundleArtifact>`
- RunResultBuilder extended with `with_plan_bundles()` method
- WIRED: `python/materializer.rs` now captures plan bundles when compliance_enabled (WS-P1)
- DataFusion API notes:
  - `DFSchema::as_arrow()` returns `&Schema` (not Into<Schema>)
  - `DataFrame::create_physical_plan()` consumes self (must clone)
  - `datafusion_sql::unparser::plan_to_sql` for SQL unparse
  - `displayable(plan).indent(true)` for normalized physical plan text
  - `plan.display_indent()` for normalized logical plan text
- Dependencies added: datafusion-sql (workspace), prost, optional datafusion-substrait + substrait
- Feature flag: `substrait = ["dep:datafusion-substrait", "dep:substrait"]`

## Wave 3 Spec Expansion (SemanticExecutionSpec)
- Removed `#[serde(deny_unknown_fields)]` from root spec; nested structs keep theirs
- 5 new fields added (all `#[serde(default)]`): typed_parameters, rule_overlay, runtime_profile, cache_policy, maintenance
- Struct literal construction sites that need updating when adding fields:
  - `spec/hashing.rs` test helper `create_test_spec()` (struct literal)
  - `spec/execution_spec.rs` `new()` constructor body
  - All other test helpers use `::new()` (auto-defaults)
- Cargo.toml tracing feature wired: `tracing = ["dep:tracing", "dep:tracing-subscriber"]`

## WS-P3: Typed Parametric Planning + WS-P4: Dynamic Rule Composition
- `spec/parameters.rs`: TypedParameter, ParameterTarget, ParameterValue with to_scalar_value()
- `compiler/param_compiler.rs`: compile_positional_param_values() + apply_typed_parameters()
- `rules/overlay.rs`: RuleOverlayProfile, build_overlaid_ruleset(), build_overlaid_session(), capture_per_rule_deltas()
- DataFusion 51: `ParamValues::List` takes `Vec<ScalarAndMetadata>` not `Vec<ScalarValue>` -- use `.into()`
- DataFusion 51: `SessionState::analyzer()` returns `Analyzer` struct; access rules via `.analyzer().rules`
- `ScalarAndMetadata.value` is the public field for accessing the inner `ScalarValue`
- chrono is already a workspace dependency with serde feature -- use for Date32/Timestamp parsing
- EXPLAIN VERBOSE rows have "plan_type" and "plan" columns; rule transitions follow pattern "logical_plan after <rule_name>"
- 45 tests covering all three modules pass

## WS-P7/P9: Real Metrics + Runtime Profiles
- `ExecutionMetrics` now derives `Serialize, Deserialize` (added for MetricsStore)
- `tuner/metrics_store.rs`: bounded FIFO MetricsStore with spec_hash-keyed history
- `session/runtime_profiles.rs`: RuntimeProfileSpec with fingerprint() and small/medium/large presets
- `factory.rs`: build_session_from_profile() takes (profile, ruleset, enable_function_factory, enable_domain_planner)
- DF 51 API: `with_max_temp_directory_size` takes u64; `with_repartition_sorts/file_scans/file_min_size` exist as builder methods
- DF 51 API: `metadata_size_hint` is `Option<usize>` in ParquetOptions; `max_predicate_cache_size` may not exist as typed field

## WS-P14: Named Arguments + Custom Expression Planning
- `build_session_from_profile()` extended with `enable_function_factory` and `enable_domain_planner` bool params
- Wires `install_sql_macro_factory_native(&ctx)` and `install_expr_planners_native(&ctx, &["codeanatomy_domain"])` after UDF registration
- Capability policy: installation failures are hard errors when flag is true; no-op when false
- All UDAFs already have `.with_parameter_names()` via `signature_with_names()` helper -- no udaf_builtin.rs changes needed
- `build_session_from_profile` has no external callers yet (safe to change signature)
- Both install functions are pub at `datafusion_ext::` crate root (lib.rs)

## ERR-02: Inline Cache Wiring in Plan Compiler
- Inline cache (`HashMap<String, DataFrame>`) now threaded through compile_view() and resolve_source_with_templates()
- resolve_source_with_templates() returns String (source name), not DataFrame -- builders resolve by name via ctx.table()
- KEY: when an inlined source has no templates, it must be lazily registered in SessionContext so builders can find it
- resolve_source() (cache-first DataFrame lookup) is used for output target resolution in compile() step 5
- This eliminates the dead-code warning on resolve_source() naturally by actually using it

## WS-P10: Plan-Aware Cache Boundaries
- `compiler/cache_policy.rs`: CachePlacementPolicy, CacheOverride, CacheAction structs + compute_cache_boundaries() async fn
- `cache_boundaries.rs`: Added insert_cache_boundaries_with_policy() that delegates to cache_policy module
- compute_fanout() changed from `fn` to `pub(crate) fn` to allow reuse from cache_boundaries' new function
- estimate_view_rows() uses partition_statistics(None) (DF 51+), falls back to 1000
- Priority ordering: fanout * estimated_rows, descending; ForceCache gets u64::MAX priority
- Test pattern: async test helpers calling compute_cache_boundaries need .await at callsite (easy to miss)
- SessionContext::new() in tests creates empty context where all views fall back to 1000 estimated rows

## Materializer.rs Bridge Wiring (Wave 2, Agent G)
- All 5 stubs replaced with real implementations in `python/materializer.rs`
- WS-P9: Profile-based session uses `build_session_from_profile()` + separate `SessionEnvelope::capture()`
  - `RuntimeProfileSpec.memory_pool_bytes` is `usize`, needs `as u64` cast for envelope capture
- WS-P1: Plan bundles captured in compliance path BEFORE execute_and_materialize consumes output_plans
- WS-P7: Physical plans created pre-execution via `df.clone().create_physical_plan()`
  - Metrics aggregated post-execution across all plans; tuner uses real values with env_profile fallbacks
- WS-P11: Maintenance runs post-materialization; output_locations built from materialization_results + output_targets
- Ordering constraint: plan_bundles capture + physical_plans capture MUST precede execute_and_materialize()
  - output_plans is moved by execute_and_materialize (takes Vec by value)
  - compliance explain loop already borrows &output_plans; bundles + plans captured in same region

## Runner.rs Full Pipeline Wiring (Wave 2, Agent F)
- `execute_and_materialize_with_plans()` added as new function (backward-compat; original unchanged)
- Returns `(Vec<MaterializationResult>, Vec<Arc<dyn ExecutionPlan>>)` -- plan refs for post-exec metrics
- `run_full_pipeline()` expanded with 4 integration points:
  1. WS-P1: Plan bundles captured BEFORE output_plans consumed (iterates &output_plans)
  2. WS-P7: Metrics aggregated via `collect_plan_metrics()` across all physical plans, scan_selectivity averaged
  3. WS-P11: Maintenance locations derived from results + spec.output_targets.delta_location
  4. WS-P13: Tracing span guard via `#[cfg(feature = "tracing")]` block (compile-time elimination when disabled)
- `plan_compiler.rs` cache policy switch: `spec.cache_policy.is_some()` -> `insert_cache_boundaries_with_policy()`
- All 241 unit + integration tests pass; both default and tracing-feature builds compile clean

## Wave 3 Verification Results (2026-02-08)
- All 3 compilation targets pass: default, `--features tracing`, `--features substrait`
- Substrait fix: `prost` 0.13->0.14, `substrait` 0.52->0.62 (must match datafusion-substrait transitive dep)
- 340+ tests pass across all workspace crates (241 lib + 8+5+1+8+3+6+3+1+16+5+45 = 342+ total)
- All 15 audit gap items verified closed
- Python quality gate passes (ruff format clean, ruff check 29 pre-existing errors, pyrefly 0 errors)

## Scope Item 3: Planning-Surface Manifest Hash (Wave 2, Agent E)
- `session/planning_manifest.rs`: PlanningSurfaceManifest (Serialize + hash via blake3 over sorted canonical JSON)
- `SessionEnvelope` gained `planning_surface_hash: [u8; 32]` field + parameter in `capture()`
- `PlanBundleArtifact` gained `planning_surface_hash: [u8; 32]` field + parameter in `build_plan_bundle_artifact()`
- `PlanDiff` gained `planning_surface_changed: bool` field
- All callsites pass `[0u8; 32]` default until Wave 3 wires real manifest hash
- Callsites: factory.rs (1), materializer.rs (2: envelope + bundle), runner.rs (1)
- Name vectors sorted inside `hash()` for order-independent determinism
- 10 new unit tests for hash determinism, order independence, serde round-trip

## Scope Item 7: Optional Delta Planner + Delta Codec Seams (Wave 2, Agent F)
- Feature gates: `delta-planner = []` (marker) and `delta-codec = ["dep:datafusion-proto"]` in Cargo.toml
- Plan erroneously specified `delta-codec = ["dep:datafusion-substrait"]`; codec APIs live in `datafusion-proto`
- `DeltaPlanner::new()` returns `Arc<DeltaPlanner>` -- do NOT wrap in `Arc::new()` (double-Arc error)
- `physical_plan_to_bytes_with_extension_codec()` takes `Arc<dyn ExecutionPlan>`, not `&dyn`
- `install_delta_codecs()` pattern: `ctx.state_ref().write().config_mut().set_extension(Arc::new(codec))`
- `PlanningSurfaceSpec.query_planner` already exists; feature gate just conditionally sets it
- When `mut` binding is only needed under cfg feature: use `#[allow(unused_mut)]`
- Test pattern for cfg-gated tests: put imports in test fn body to avoid unused-import warnings
- `#[cfg(not(feature))]` test functions don't see `use super::*` -- use explicit imports

## Wave 3-B: Integration Tests + Drift Surface Expansion
- Integration tests: `tests/integration/test_programmatic_architecture_parity.py`
  - 5 test classes covering cache policy hierarchy, entity registry parity, join key inference, inference confidence, calibration
  - Imports private functions from semantics.pipeline and ir_pipeline
- Drift surface: 3 new checks in `scripts/check_drift_surfaces.py`
  - `programmatic.viewkind_sole_authority`: scans StrEnums for ViewKind value overlap
  - `programmatic.builder_dispatch_coverage`: AST-parses _BUILDER_HANDLERS keys vs ViewKind values
  - `programmatic.entity_registry_derivation`: checks registry.py references generate_table_specs + entity_registry
- `_VIEW_KIND_VALUES` frozenset in drift checker must stay in sync with ViewKind enum
