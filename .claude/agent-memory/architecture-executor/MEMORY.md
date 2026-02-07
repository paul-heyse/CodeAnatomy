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
- `InferredViewProperties` on `SemanticIRView`: join_strategy, join_keys, cache_policy, graph_position
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
- `validate_policy_bundle()` composes 6 core + 3 compiled-policy validators
- New `compiled_policy: CompiledExecutionPolicy | None = None` param (backward compat)
- New validators: `_compiled_policy_consistency_issues`, `_statistics_availability_issues`, `_evidence_coherence_issues`
- Issue codes: `compiled_policy_extra_cache_views`, `compiled_policy_extra_scan_overrides`, `stats_dependent_override_without_capabilities`, `compiled_policy_missing_fingerprint`, `compiled_policy_empty_cache_section`
- `_manifest_view_names()` extracts view names from `semantic_ir.views` via `getattr()` (defensive)
- `_STATS_REASON_KEYWORDS` at module level (N806 rule: no uppercase in function scope)
- Caller in `driver_factory.py:build_plan_context()` passes `compiled_policy=authority_context.compiled_policy`
- Test monkeypatch of `validate_policy_bundle` must accept `compiled_policy` kwarg
- `test_driver_factory_config.py` fake authority context needs `compiled_policy=None` attribute

## Workload & Pruning Infrastructure (Phase G, Section 12)
- Workload classification: `src/datafusion_engine/workload/classifier.py` (WorkloadClass StrEnum + classify_workload)
- Session profiles: `src/datafusion_engine/workload/session_profiles.py` (WorkloadSessionProfile + workload_session_profile)
- Pruning metrics: `src/datafusion_engine/pruning/metrics.py` (PruningMetrics StructBaseStrict)
- Explain parser: `src/datafusion_engine/pruning/explain_parser.py` (parse_pruning_metrics with multi-pattern regex)
- Pruning tracker: `src/datafusion_engine/pruning/tracker.py` (PruningTracker + PruningSummary)
- Artifact types: WorkloadClassificationArtifact, PruningMetricsArtifact in serde_artifacts.py
- Specs: WORKLOAD_CLASSIFICATION_SPEC, PRUNING_METRICS_SPEC in serde_artifact_specs.py
- Regex gotcha: `pages` as alt in `(?:total_pages|pages_total|pages)` matches `pruned_pages` - use specific alternatives only

## Golden Snapshot Tests
- `tests/fixtures/semantic_ir_snapshot.json` must be updated when IR pipeline changes
- Use `--update-golden` flag: `uv run pytest tests/semantics/test_semantic_ir_snapshot.py --update-golden`
- Always review diff before committing updated goldens
