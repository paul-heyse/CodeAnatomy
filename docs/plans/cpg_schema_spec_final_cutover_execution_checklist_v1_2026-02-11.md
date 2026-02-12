# CPG + schema_spec Final Cutover Execution Checklist (v1, 2026-02-11)

## Hard-Cut Policy
- `codeanatomy_engine` extension is mandatory for build/plan/schema runtime paths.
- No Python compatibility shims are allowed for removed CPG/schema authority modules.
- Python may retain contract models only; runtime authority must be Rust + PyO3.

## Completion Gates
1. No Python runtime construction of `cpg_nodes|cpg_edges|cpg_props|cpg_props_map|cpg_edges_by_src|cpg_edges_by_dst`.
2. No production imports of `semantics.cpg.*`.
3. No production imports of `schema_spec.system` operational helpers.
4. `schema_spec` runtime operations execute via `SchemaRuntime` with no Python fallback path.
5. Pandera is debug-only or removed from production execution paths.
6. Docs reflect Rust-first CPG/schema runtime architecture.
7. Full quality gate passes.

## Wave Checklist

### Wave 0
- [x] Guard script blocks deleted `cpg.*` imports.
- [x] Guard script blocks deleted `schema_spec.dataset_spec_ops` imports.
- [x] Guard script blocks deleted `semantics.cpg.*` imports when files are removed.
- [x] Guard script blocks `schema_spec.system` reintroduction when removed.

### Wave 1
- [ ] Remove `_cpg_output_view_specs` and dynamic CPG finalize builder path from `src/semantics/pipeline.py`.
- [ ] Ensure CPG outputs are Rust-dispatched only.

### Wave 2
- [ ] Remove `SemanticModel` hooks for CPG spec/entity generation from Python model layer.
- [ ] Remove IR payload fields carrying Python CPG spec structs.
- [ ] Remove schema registry entries for Python CPG spec classes.
- [ ] Delete `src/semantics/cpg/*.py` and `src/semantics/cpg_entity_specs.py`.

### Wave 3
- [ ] Make `SchemaRuntime` mandatory in runtime policy application path.
- [ ] Remove Python fallback behavior in scan-policy application.
- [ ] Migrate operational callsites from helper logic to Rust-backed runtime calls.

### Wave 4
- [ ] Split `schema_spec.system` into contracts and runtime-op modules.
- [ ] Move contract structs to `schema_spec/contracts.py`.
- [ ] Move runtime operations to `schema_spec/runtime_ops.py`.
- [ ] Remove compatibility exports from `schema_spec/__init__.py`.

### Wave 5
- [ ] Remove Pandera as authoritative runtime validation path.
- [ ] Keep Pandera only as explicit debug path, or delete.

### Wave 6
- [ ] Update architecture docs to remove deleted module references.
- [ ] Refresh msgspec/plan goldens after behavior review.
- [ ] Run full quality gate and resolve failures.

## Validation Commands
- `scripts/check_no_legacy_planning_imports.sh --strict-tests`
- `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`
