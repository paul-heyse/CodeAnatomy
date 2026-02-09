# Zero-Row Bootstrap Feasibility Review (Extract + DataFusion + Semantics)

Date: 2026-02-06

## Objective

Assess whether CodeAnatomy can pre-bootstrap runtime/testing using schema-correct Delta datasets with `0` rows, and determine:

1. Feasibility and required effort (built-in library capabilities vs bespoke code).
2. Architectural improvements needed for a reliable end-to-end dry run.

## Executive Conclusion

Feasibility is **high**.

- Most of the required mechanics are already present (schema registries, schema-driven semantic IR, DataFusion session/table registration, Delta write/registration paths).
- The remaining work is primarily orchestration and validation semantics, not a large new subsystem.
- Estimated split:
  - **Built-in capabilities:** ~75-85%
  - **Bespoke implementation:** ~15-25% (thin planner/orchestrator + validation fixes + consistency cleanup)

## Key Findings

### 1) The runtime already supports schema-only table registration

- `src/datafusion_engine/session/runtime.py:1662` `_register_schema_table(...)` creates an empty Arrow table from schema and registers it.
- `src/datafusion_engine/session/runtime.py:5723` `_register_schema_tables(...)` bulk-registers missing tables schema-only.
- `src/datafusion_engine/session/runtime.py:5752` `_install_schema_registry(...)` registers extract nested schemas, semantic input sources, and relationship schemas.
- `src/datafusion_engine/session/runtime.py:4322` `session_context()` calls `_install_schema_registry(...)` during context startup.
- Ephemeral contexts follow the same pattern:
  - `src/datafusion_engine/session/runtime.py:4498`
  - `src/datafusion_engine/session/runtime.py:4550`

Interpretation: the current architecture is already strongly schema-first.

### 2) Semantic pipeline gates are table/column-driven, not row-driven

- `src/semantics/pipeline.py:1337` calls `require_semantic_inputs(...)`.
- `src/semantics/validation/__init__.py:36` enforces semantic input validation.
- `src/semantics/validation/catalog_validation.py:52` / `src/semantics/validation/catalog_validation.py:73` query `information_schema.tables` and `information_schema.columns`.
- `src/semantics/validation/catalog_validation.py:116` validates required columns.

Interpretation: if required tables/columns exist, zero rows are generally acceptable for pipeline gating.

### 3) Semantic dataset shapes and DAG metadata are inferred from schemas/specs

- `src/semantics/ir_pipeline.py:474` builds semantic dataset rows from model/specs.
- `src/semantics/ir_pipeline.py:495` pulls input fields from `extract.registry.dataset_schema(...)`.
- `src/semantics/ir_pipeline.py:537` compiles semantic IR independent of row values.
- `src/semantics/catalog/dataset_rows.py:124` caches rows from compiled IR.

Interpretation: compilation/planning logic is compatible with 0-row semantics.

### 4) Extract specs are already centralized and inferential

- `src/datafusion_engine/extract/registry.py:109` `dataset_schema(name)` resolves canonical schemas.
- `src/datafusion_engine/extract/registry.py:137` `dataset_spec(name)` applies Delta policy bundles.
- `src/datafusion_engine/extract/registry.py:234` `dataset_query(name, projection=...)` derives query specs.

Interpretation: a bootstrap planner can reuse existing registries instead of re-specifying schemas.

### 5) There is already a test helper pattern for schema-only registration

- `tests/test_helpers/semantic_registry_runtime.py:19` registers semantic datasets as empty tables using `empty_table_for_schema(...)`.

Interpretation: the codebase already uses this approach for isolated runtime setup.

### 6) Proven local probe: Delta can be materialized with 0 rows

Local `uv run` probe wrote an empty Arrow table to Delta and produced:

- `_delta_log` exists
- commit JSON count = 1
- version = 0
- row count = 0

This validates that 0-row Delta table bootstrap is not theoretical; it works in practice with current environment/dependencies.

### 7) Important caveat: semantic-type validation is row-sensitive today

`validate_semantic_types(...)` currently uses a row probe:

- `src/datafusion_engine/schema/registry.py:3100-3113` reads semantic metadata via:
  - `arrow_metadata(...)` expression
  - `df.limit(1).to_arrow_table().to_pylist()`

On empty tables, this yields no rows and raises “Missing semantic type metadata...”, even when field metadata exists on schema.

Local probe reproduced this with an empty table containing `node_id` metadata: validation failed until at least one row existed.

### 8) Startup validation currently escalates view errors to hard failures

- `src/datafusion_engine/session/runtime.py:5700-5720` collects `view_errors` from schema/view validators.
- `src/datafusion_engine/session/runtime.py:5742-5743` includes `view_errors` in issue set.
- `src/datafusion_engine/session/runtime.py:5803-5816` raises `ValueError` when issues are present.

Interpretation: row-sensitive validation needs adjustment for strict zero-row bootstrap mode.

### 9) Bootstrap consistency gap: some Delta auxiliary tables are skipped when `_delta_log` is missing

- Cache inventory:
  - `src/datafusion_engine/cache/inventory.py:99-111` skips bootstrap if `_delta_log` missing.
  - Has bootstrap helper at `src/datafusion_engine/cache/inventory.py:198`, but missing-log path does not use it.
- Delta observability:
  - `src/datafusion_engine/delta/observability.py:474-487` skips bootstrap if `_delta_log` missing.
  - Has bootstrap helper at `src/datafusion_engine/delta/observability.py:600`, used for schema reset path.
- In contrast, cache ledger bootstraps on missing path:
  - `src/datafusion_engine/cache/ledger.py:107-129`
  - `src/datafusion_engine/cache/ledger.py:197-219`
  - bootstrap implementation at `src/datafusion_engine/cache/ledger.py:288`

Interpretation: make bootstrap behavior uniform for deterministic zero-row setup.

### 10) Dataset location resolution seams already exist

- `src/datafusion_engine/session/runtime.py:3958` `extract_dataset_location(...)`
- `src/datafusion_engine/session/runtime.py:3984` `dataset_location(...)`

Interpretation: these are natural integration points for a bootstrap planner/materializer.

## DataFusion + Delta Built-In Leverage (From Skill References)

From `.claude/skills/dfdl_ref/reference/...`:

- Delta as DataFusion table provider is the preferred path (query planning + pushdown benefits).
- Arrow Dataset fallback exists but can lose optimization.
- Delta schema evolution supports `schema_mode="merge"` additive behavior with NULL-fill.
- Snapshot metadata defines visible schema state; planning is snapshot/schema-addressed.

Net: libraries already provide the core primitives required for a schema-first, zero-row bootstrap workflow.

## Built-In vs Bespoke Split

### Use Built-In (No Reinvention)

- Dataset schema/spec inference from extract/semantic registries.
- Empty-table registration in SessionContext.
- Delta table creation from empty Arrow tables.
- DataFusion dataset registration/resolution pathways.
- Existing runtime profile/catalog location resolution.

### Bespoke (Thin, Focused)

- A bootstrap planner that enumerates required datasets and resolves schemas/locations.
- A bootstrap materializer that creates missing Delta tables (zero-row by default, optional seeded mode).
- Validation-mode handling for row-sensitive checks (especially semantic metadata validation).
- Consistency unification for missing `_delta_log` bootstrap behavior across cache/observability surfaces.

## Recommended Architectural Improvements

### P0 (Required for robust zero-row E2E bootstrap)

1. Add `ZeroRowBootstrapPlanner` (new module under `src/datafusion_engine` or `src/semantics/bootstrap`).
2. Add `ZeroRowBootstrapExecutor` that:
   - resolves location via existing catalog/runtime methods,
   - writes empty Delta tables with canonical schema where missing,
   - registers them through existing facade paths.
3. Make semantic type validation zero-row-safe:
   - preferred: validate field metadata from schema directly (schema-level introspection), not row probe;
   - fallback option: allow empty-table bypass in explicit bootstrap mode.
4. Unify missing-log bootstrap behavior for:
   - cache inventory,
   - delta observability,
   to match cache ledger behavior.

### P1 (Hardening)

1. Add bootstrap modes:
   - `strict_zero_rows`
   - `seeded_minimal_rows` (for any unavoidable row-sensitive contracts)
2. Add explicit bootstrap report artifact:
   - planned datasets, created/skipped tables, schema fingerprints, validation outcomes.
3. Add compatibility preflight for provider path (actionable failure before deep runtime path).

### P2 (Quality/maintainability)

1. Align semantic validation table naming with current canonical output naming policy:
   - `src/datafusion_engine/schema/registry.py:3119` currently uses `cpg_nodes_v1`/`cpg_edges_v1`
   - `src/semantics/naming.py` canonical outputs are unversioned (`cpg_nodes`, `cpg_edges`).
2. Add policy-driven controls for metadata strictness (`skip_arrow_metadata`, semantic metadata enforcement).

## Practical Implementation Sketch

### Minimal Viable Bootstrap Flow

1. Build `SessionContext` and runtime profile.
2. Compile semantic IR and collect expected datasets/specs.
3. Resolve schema for each dataset from existing registries.
4. Resolve location for each dataset from existing runtime catalog methods.
5. For each missing Delta table:
   - write empty table with canonical schema (`mode=overwrite`, `schema_mode=overwrite`).
6. Register tables in DataFusion via existing facade.
7. Run schema-level validation pass and emit bootstrap report.

No large new framework is required; this is primarily orchestration and guardrails.

## Testing Strategy

1. Unit tests:
   - planner coverage for dataset discovery and schema resolution.
   - executor behavior for create/skip/register transitions.
2. Integration tests:
   - fresh temp root bootstrap creates `_delta_log` for all planned datasets.
   - semantic compile/planning runs with zero-row inputs.
   - end-to-end pipeline completes with zero-row outputs and stable schemas.
3. Contract tests:
   - semantic metadata validation succeeds on empty tables after row-sensitivity fix.
   - bootstrap modes (`strict_zero_rows` vs `seeded_minimal_rows`) behave as documented.

## Answer to the Two Requested Questions

### 1) Feasibility and built-in vs bespoke

This is feasible now with moderate effort and without building a massive bespoke subsystem. Most heavy lifting is already available in DataFusion/Delta + existing CodeAnatomy registries/runtime abstractions. Required bespoke code is mainly a focused bootstrap planner/executor and a small number of validation/consistency fixes.

### 2) Architectural improvements needed

The most important improvements are:

- make semantic metadata validation schema-based (or bootstrap-mode tolerant of empty tables),
- unify missing-log bootstrap behavior across cache/observability tables,
- add a first-class bootstrap planner/executor and report artifact,
- optionally add a seeded fallback mode for row-sensitive edges.

These changes are incremental and align with the current architecture rather than replacing it.
