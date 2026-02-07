# Semantic Compiled Cutover Contract

## Purpose

This contract defines the migration boundary for the semantic-compiled
reengineering initiative. It prevents new control-plane drift while the
architecture is being consolidated.

## Canonical Control-Plane Entry Points

All runtime/planning/bootstrap orchestration must converge on:

1. `semantics.compile_context.compile_semantic_program(...)`
2. `semantics.validation.policy.validate_semantic_inputs(...)`
3. `datafusion_engine.dataset.registry.dataset_catalog_from_profile(...)`

## Legacy Seams Under Decommission

The following seams are migration targets and must not gain new usage:

1. direct `build_semantic_ir(...)` orchestration outside compile-context internals
2. direct `ensure_view_graph(...)` orchestration from non-compile orchestration seams
3. split `require_semantic_inputs(...)` contracts in multiple modules
4. bootstrap-local semantic location derivation helpers
5. imperative `_extract_outputs_for_template` template-switch orchestration
6. hardcoded extractor dependency maps (`_REQUIRED_INPUTS`, `_SUPPORTS_PLAN`, `_EXTRACTOR_EXTRA_INPUTS`)
7. duplicate `materialize_extract_plan` implementations
8. semantic runtime bridge adapters (`semantic_runtime_from_profile`, `apply_semantic_runtime_config`)
9. duplicate dataset-location map helpers (`_dataset_location_map`)
10. runtime `_v1` references outside `semantics.naming_compat`

## Transitional Policy

During migration, existing usages may remain until their owning wave lands.
However, new usages in production code are prohibited.

The checker script `scripts/check_semantic_compiled_cutover.py` is the
enforcement mechanism. It runs in advisory mode by default and in strict mode
with `--strict`.
