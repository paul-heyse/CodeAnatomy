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

## Wave 3-B: Integration Tests + Drift Surface Expansion
- Integration tests: `tests/integration/test_programmatic_architecture_parity.py`
  - 5 test classes covering cache policy hierarchy, entity registry parity, join key inference, inference confidence, calibration
  - Imports private functions from semantics.pipeline and ir_pipeline
- Drift surface: 3 new checks in `scripts/check_drift_surfaces.py`
  - `programmatic.viewkind_sole_authority`: scans StrEnums for ViewKind value overlap
  - `programmatic.builder_dispatch_coverage`: AST-parses _BUILDER_HANDLERS keys vs ViewKind values
  - `programmatic.entity_registry_derivation`: checks registry.py references generate_table_specs + entity_registry
- `_VIEW_KIND_VALUES` frozenset in drift checker must stay in sync with ViewKind enum
