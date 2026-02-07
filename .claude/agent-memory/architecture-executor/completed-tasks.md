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
