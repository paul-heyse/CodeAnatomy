# CPG + schema_spec Rust/DataFusion Pivot Assessment (v1, 2026-02-11)

## Summary
This document assesses `src/cpg` and `src/schema_spec` for hard cutover toward Rust-owned planning/execution with DataFusion/Delta authority in `rust/codeanatomy_engine` + `rust/codeanatomy_engine_py`.

Assessment goals:
1. Identify dead or functionally dead code that can be removed quickly.
2. Identify low-usage modules where callsites can be migrated to reduce Python surface.
3. Identify non-removable modules and how to absorb their runtime authority into Rust.

## Method and Constraints
- Discovery used `rg`, targeted file inspection, and AST-assisted import/symbol scans.
- CQ was intentionally not used (per active repo guidance for this task).
- Rust/DataFusion direction was aligned with local `dfdl_ref` references:
  - `.claude/skills/dfdl_ref/reference/datafusion_addendum.md`
  - `.claude/skills/dfdl_ref/reference/Datafusion_logicplan_rust.md`
  - `.claude/skills/dfdl_ref/reference/deltalake_datafusion_integration.md`
  - `.claude/skills/dfdl_ref/reference/datafusion_schema.md`

## Current Usage Snapshot
Importer counts below reflect static imports from `src/` and `tests/`.

### `src/cpg` (high signal)
- `cpg.relationship_contracts`: `src:0`, `tests:1` (test-only)
- `cpg.schemas`: `src:1`, `tests:0` (single constant usage)
- `cpg.constants`: `src:1`, `tests:0`
- `cpg.scip_roles`: `src:1`, `tests:0`
- `cpg.contract_map`: `src:1`, `tests:0`
- `cpg.prop_catalog`: `src:1`, `tests:0` (via `contract_map`)
- `cpg.node_families`: `src:1`, `tests:1`
- `cpg.view_builders_df`: `src:2`, `tests:0` (runtime-critical)

### `src/schema_spec` (high signal)
- `schema_spec.relationship_specs`: `src:1`, `tests:0` (mostly serialization touchpoint)
- `schema_spec.dataset_handle`: `src:2`, `tests:0` (effectively self-referential)
- `schema_spec.nested_types`: `src:1`, `tests:1` (production path effectively absent)
- `schema_spec.registration`: `src:2`, `tests:0` (single meaningful production consumer)
- `schema_spec.policies`: `src:2`, `tests:1`
- `schema_spec.dataset_spec_ops`: `src:19`, `tests:5` (broadly used)
- `schema_spec.system`: `src:39`, `tests:15` (core authority)

## 1) Immediate Delete Candidates

## A. `src/cpg/relationship_contracts.py`
Why delete now:
- No production importers; only `tests/unit/cpg/test_relationship_contracts.py` imports it.
- Overlaps conceptually with `schema_spec.relationship_specs` and current runtime no longer depends on it.

Delete set:
- `src/cpg/relationship_contracts.py`
- `tests/unit/cpg/test_relationship_contracts.py`

## B. `src/schema_spec/dataset_handle.py` (with tiny API cleanup)
Why delete now:
- `DatasetHandle` is only reached through `dataset_spec_handle()` in `dataset_spec_ops`, and that helper has no production consumers.
- `register_views()` is already removed behavior and raises at runtime.

Delete set after tiny move:
- Remove `dataset_spec_handle()` from `src/schema_spec/dataset_spec_ops.py`
- Remove exports/import wiring from `src/schema_spec/__init__.py`
- Delete `src/schema_spec/dataset_handle.py`

## C. `src/schema_spec/nested_types.py` (if test utility not needed)
Why likely deletable now:
- No meaningful production consumers; usage is package export + tests.
- This is utility-style nested Arrow type construction, not runtime planning authority.

Delete set (hard cutover option):
- `src/schema_spec/nested_types.py`
- `tests/unit/test_nested_types.py` (or move remaining assertions to Rust/Arrow contract tests)
- Remove nested-types exports from `src/schema_spec/__init__.py`

## 2) Delete After Small Callsite Moves

## A. `src/cpg/schemas.py`
Current state:
- Contains only `SCHEMA_VERSION = 1`.
- Single consumer: `src/graph/product_build.py`.

Move:
- Inline/move schema version into Rust boundary contract (preferred), or local constant in `src/graph/product_build.py` during transition.

Then delete:
- `src/cpg/schemas.py`

## B. `src/cpg/constants.py` + `src/cpg/scip_roles.py`
Current state:
- `constants.py` is effectively a re-export shim for role flags and quality helpers.
- Only consumed by `cpg.spec_registry`.

Move:
- Inline `ROLE_FLAG_SPECS` into `src/cpg/spec_registry.py` or move role-flag prop derivation into Rust spec/compiler layer.

Then delete:
- `src/cpg/constants.py`
- `src/cpg/scip_roles.py`

## C. `src/schema_spec/registration.py`
Current state:
- Meaningful use is `src/semantics/catalog/spec_builder.py` calling `DatasetRegistration(...)` + `register_dataset(...)`.

Move:
- Replace callsite with direct `make_dataset_spec(...)` composition (already available in `schema_spec.system`).

Then delete:
- `src/schema_spec/registration.py`

## D. `src/schema_spec/relationship_specs.py` (partial shrink now, full delete later)
Current state:
- Runtime relationship constraint validation path is effectively stubbed in `datafusion_engine/session/runtime.py`.
- Production dependency is primarily `RelationshipData` in `src/serde_schema_registry.py`; optional fallback usage appears in `src/relspec/inferred_deps.py`.

Move:
- Move `RelationshipData` struct contract to a narrow contract module (or Rust/PyO3 schema contract type).
- Remove optional relationship dataset fallback from `relspec` path if hard cutover holds.

Then delete or reduce:
- Delete full module, or retain only `RelationshipData` in a minimal contract-only module.

## 3) Delete After Rust Authority Cutover
These modules are not dead today, but should be removed once Rust owns equivalent behavior.

## A. CPG runtime builder stack
- `src/cpg/view_builders_df.py`
- `src/cpg/spec_registry.py`
- `src/cpg/specs.py`
- `src/cpg/contract_map.py`
- `src/cpg/prop_catalog.py`
- `src/cpg/node_families.py`
- `src/cpg/kind_catalog.py`
- `src/cpg/emit_specs.py`
- `src/cpg/__init__.py`

Cutover condition:
- Rust compiler/orchestrator builds CPG node/edge/prop outputs directly from semantic IR and output contract descriptors.
- Python no longer instantiates these specs/builders for production execution.

## B. schema_spec runtime authority layers
- `src/schema_spec/system.py` (runtime-introspection authority functions)
- `src/schema_spec/dataset_spec_ops.py` (policy/query/finalize wrapper authority)
- `src/schema_spec/policies.py`
- `src/schema_spec/pandera_bridge.py` (if validation authority moves to Rust/DataFusion)
- `src/schema_spec/arrow_type_registry.py` and potentially parts of `arrow_types.py`

Cutover condition:
- Rust exposes typed spec/policy/session/schema contracts via PyO3.
- Python consumes dict/msgspec contracts as thin adapters only.

## 4) Functionally Dead/Prunable Exports (Safe Scope Reduction)
Even before module deletion, the following are low-value API surface and can be pruned to reduce maintenance:

- `src/schema_spec/view_specs.py`:
  - `ViewSpecInputs`, `ViewSpecSqlInputs`, `view_spec_from_builder`, `view_spec_from_sql` appear externally unused.
- `src/schema_spec/file_identity.py`:
  - `file_identity_fields`, `file_identity_fields_for_nesting`, `file_identity_struct`, `schema_with_file_identity` appear externally unused; constants and `file_identity_field_specs` are the practical live parts.
- `src/schema_spec/dataset_spec_ops.py`:
  - `dataset_spec_handle`, `dataset_spec_finalize_context`, `dataset_spec_unify_tables`, `dataset_spec_validation`, `dataset_spec_dataframe_validation`, `dataset_spec_is_streaming` show no external consumers in current static scan.
- `src/cpg/prop_catalog.py`:
  - `prop_value_type`, `resolve_prop_specs` appear externally unused; module behaves mostly as static table data.

## 5) Rust Integration Plan for Non-Removable Core
The non-removable core should not remain Python-authoritative; it should be moved behind Rust/PyO3 contract boundaries.

## A. CPG output planning/execution in Rust
Target:
- Move Python DataFrame builder logic from `cpg/view_builders_df.py` into Rust compile/execution transforms.

Recommended Rust loci:
- `rust/codeanatomy_engine/src/compiler/view_builder.rs`
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`
- `rust/codeanatomy_engine/src/spec/outputs.rs`

Representative Rust pattern (already used in engine):
```rust
let df = ctx.table(source).await?;
let joined = left.join(right, df_join_type, &left_cols, &right_cols, None)?;
let projected = df.select(columns.iter().map(col).collect::<Vec<_>>())?;
```

Direction:
- Add CPG-specific transform variants (or output materialization stages) in Rust spec/compiler.
- Eliminate Python-side CPG builder dispatch.

## B. Dataset spec/policy authority in Rust
Target:
- Replace Python `schema_spec.system`/`dataset_spec_ops` runtime authority with Rust contract APIs.

Recommended Rust loci:
- `rust/codeanatomy_engine/src/session/factory.rs`
- `rust/codeanatomy_engine/src/schema/introspection.rs`
- `rust/codeanatomy_engine/src/providers/registration.rs`
- `rust/codeanatomy_engine_py/src/session.rs`

Representative DataFusion/Delta pattern already in Rust:
```rust
let (provider, snapshot, resolved_scan_config, _, _) =
    delta_provider_from_session_request(...).await?;
ctx.register_table(&input.logical_name, Arc::new(provider))?;
```

Direction:
- Expose PyO3 APIs for schema hash, schema diff, scan-policy payloads, and dataset/open metadata.
- Keep Python only for CLI/request shaping.

## C. Validation and contracts
Target:
- Move validation-critical logic from Pandera/Python wrappers to Rust/DataFusion-native checks where possible.

Direction:
- Keep optional Python dataframe validation only as non-authoritative debug checks.
- Canonical contract checks should run in Rust and emit structured `EngineExecutionError` and result payload diagnostics.

## 6) Proposed Execution Waves

1. Wave A (fast cleanup)
- Delete `cpg/relationship_contracts.py` + test.
- Delete `schema_spec/dataset_handle.py` + remove `dataset_spec_handle()` and exports.
- Delete `schema_spec/nested_types.py` if no non-test consumer is introduced.

2. Wave B (small callsite moves)
- Inline/remove `cpg/schemas.py`.
- Inline/remove `cpg/constants.py` + `cpg/scip_roles.py`.
- Replace `schema_spec.registration` use in `semantics/catalog/spec_builder.py`; delete module.
- Shrink `schema_spec.relationship_specs.py` to contract-only or remove after moving `RelationshipData`.

3. Wave C (Rust CPG cutover)
- Implement CPG output transforms in Rust compiler/orchestrator.
- Remove Python `cpg/view_builders_df.py` and dependent spec/builder modules.

4. Wave D (Rust schema/policy cutover)
- Add Rust/PyO3 dataset policy and schema-introspection contracts.
- Remove Python runtime authority from `schema_spec.system`/`dataset_spec_ops` and related helper modules.

## 7) Conclusions
1. There is immediate deletion scope in both directories without waiting for broad architecture work (`cpg.relationship_contracts`, `schema_spec.dataset_handle`, likely `schema_spec.nested_types`).
2. Several small, isolated callsite moves unlock additional deletions quickly (`cpg/schemas`, `cpg/constants`+`scip_roles`, `schema_spec/registration`).
3. The major Python surface that remains is runtime-authoritative logic (`cpg/view_builders_df`, `schema_spec.system`, `dataset_spec_ops`); this should be migrated into existing Rust compiler/session/provider seams rather than incrementally rewritten in Python.
4. The Rust codebase already contains the right primitives (session factory, provider registration, schema introspection, orchestration); the recommended approach is to extend those primitives and delete Python authority modules in waves.
