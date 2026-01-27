---
name: v2-alignment-implementation
overview: Implement the remaining v2 alignment work while preserving static extract dataset schemas as the source of truth (kept in `schema_registry.py`), then enforce delta-only IO, replace `plan_hash` artifacts, align diagnostics, and centralize UDF snapshot validation.
todos: []
isProject: false
---

# v2 Alignment Implementation Plan

## Static extract schema source of truth (Phase 2 adjustment)

- Keep `NESTED_DATASET_INDEX` in [`src/datafusion_engine/schema_registry.py`](/home/paul/CodeAnatomy/src/datafusion_engine/schema_registry.py) as the authoritative static schema source for **extract datasets plus nested views derived from them**. Make that explicit by:
- Adding/renaming helpers so callers can ask for “extract/nested” contracts without treating other datasets as static (e.g., `extract_schema_contract_for(name)`), and updating existing helpers (`nested_dataset_names`, `datasets_for_path`, `is_nested_dataset`, etc.) to clarify they only cover extract-derived nested datasets.
- Ensure any schema contract derivation for extract/nested datasets uses `SchemaContract.from_arrow_schema()` on the static schema from `NESTED_DATASET_INDEX`, and avoid introducing new static registries elsewhere.

## Remove DataFrame→AST fallback paths (Phase 2)

- Remove `_sqlglot_from_dataframe()` usage and require AST inputs for registry/spec flows:
- [`src/datafusion_engine/schema_registry.py`](/home/paul/CodeAnatomy/src/datafusion_engine/schema_registry.py)
- [`src/datafusion_engine/view_registry.py`](/home/paul/CodeAnatomy/src/datafusion_engine/view_registry.py)
- [`src/datafusion_engine/view_registry_specs.py`](/home/paul/CodeAnatomy/src/datafusion_engine/view_registry_specs.py)
- Ensure registry view/spec builders thread through AST artifacts captured at registration time, rather than extracting from `DataFrame`.

## Delta-only IO cleanup (Phase 3)

- Remove parquet defaults/branches and switch defaults to Delta across IO surfaces:
- [`src/datafusion_engine/streaming_executor.py`](/home/paul/CodeAnatomy/src/datafusion_engine/streaming_executor.py) — drop `file_format="parquet"` default and remove `pipe_to_parquet()`.
- [`src/datafusion_engine/write_pipeline.py`](/home/paul/CodeAnatomy/src/datafusion_engine/write_pipeline.py) — remove `ParquetWritePolicy`, parquet branches, parquet helper functions, and change `WriteRequest` defaults to Delta.
- [`src/engine/materialize_pipeline.py`](/home/paul/CodeAnatomy/src/engine/materialize_pipeline.py) — remove parquet payloads and parquet format mapping; update error strings to Delta-only.
- [`src/ibis_engine/io_bridge.py`](/home/paul/CodeAnatomy/src/ibis_engine/io_bridge.py) — remove parquet write options and parquet writer entrypoints.
- [`src/datafusion_engine/registry_bridge.py`](/home/paul/CodeAnatomy/src/datafusion_engine/registry_bridge.py) — remove “parquet” provider type and change default `format` to “delta”; keep parquet-related Delta optimizations (pushdown/pruning).
- [`src/datafusion_engine/runtime.py`](/home/paul/CodeAnatomy/src/datafusion_engine/runtime.py) — remove parquet policy settings; change external defaults to Delta.
- [`src/sqlglot_tools/ddl_builders.py`](/home/paul/CodeAnatomy/src/sqlglot_tools/ddl_builders.py) — default `file_format` to `DELTA` and update docstrings/examples.
- [`src/hamilton_pipeline/modules/outputs.py`](/home/paul/CodeAnatomy/src/hamilton_pipeline/modules/outputs.py) and [`src/hamilton_pipeline/task_module_builder.py`](/home/paul/CodeAnatomy/src/hamilton_pipeline/task_module_builder.py) — update `@cache(format="parquet")` to Delta-compatible format.
- Keep parquet metadata utilities in [`src/obs/metrics.py`](/home/paul/CodeAnatomy/src/obs/metrics.py) as they are still valid for Delta.

## Artifact consistency (Phase 4)

- Replace `plan_hash` with `ast_fingerprint` + `policy_hash` (or `ast_policy_fingerprint`) everywhere, updating schemas and payloads:
- [`src/datafusion_engine/execution_helpers.py`](/home/paul/CodeAnatomy/src/datafusion_engine/execution_helpers.py) — update plan artifact structures and semantic diff helpers.
- [`src/datafusion_engine/schema_registry.py`](/home/paul/CodeAnatomy/src/datafusion_engine/schema_registry.py) — update schema fields for plan artifacts/fingerprints.
- [`src/incremental/registry_rows.py`](/home/paul/CodeAnatomy/src/incremental/registry_rows.py) — update registry row specs.
- [`src/arrowdsl/schema/abi.py`](/home/paul/CodeAnatomy/src/arrowdsl/schema/abi.py) — update `dataset_fingerprint()` signature and payload fields.
- [`src/datafusion_engine/compile_options.py`](/home/paul/CodeAnatomy/src/datafusion_engine/compile_options.py) and [`src/datafusion_engine/runtime.py`](/home/paul/CodeAnatomy/src/datafusion_engine/runtime.py) — update compile options and event payloads accordingly.
- [`src/incremental/invalidations.py`](/home/paul/CodeAnatomy/src/incremental/invalidations.py) — update invalidation tracking to use new artifacts.

## Diagnostics naming alignment (Phase 4)

- Rename `policy_fingerprint` → `policy_hash` and include `policy_hash` in execution diagnostics:
- [`src/datafusion_engine/diagnostics.py`](/home/paul/CodeAnatomy/src/datafusion_engine/diagnostics.py) — update `CompilationRecord`, `ExecutionRecord`, payload keys, and docstrings/examples.

## UDF snapshot gate (Phase 5)

- Centralize UDF snapshot validation at runtime initialization and ensure all registration entrypoints use the validated snapshot:
- [`src/datafusion_engine/runtime.py`](/home/paul/CodeAnatomy/src/datafusion_engine/runtime.py) and [`src/datafusion_engine/udf_platform.py`](/home/paul/CodeAnatomy/src/datafusion_engine/udf_platform.py) — confirm validation occurs before any UDF registration or parity check; add guard if any entrypoint bypasses the centralized validation.

## Verification

- Run `ReadLints` for edited files and resolve any lint/type issues.
- If you want deeper verification after implementation, run: `uv run ruff format && uv run ruff check --fix`, `uv run pyrefly check`, and `uv run pyright --warnings --pythonversion=3.13.11`.