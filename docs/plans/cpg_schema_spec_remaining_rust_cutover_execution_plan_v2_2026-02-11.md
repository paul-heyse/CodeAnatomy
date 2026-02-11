# Remaining Rust Cutover Execution Plan (CPG + schema_spec Authority) — v2

## Scope Freeze
This plan hard-cuts Python runtime authority for:

1. CPG output construction (`src/cpg/*` runtime builder path and `semantics.pipeline` finalize builders).
2. schema_spec runtime operations (`src/schema_spec/system.py` operational helpers and `src/schema_spec/dataset_spec_ops.py`).

Python remains a thin adapter/contract layer. Rust (`codeanatomy_engine` + PyO3 bindings) is authoritative.

## Frozen Contract Changes

1. Rust relation transform contract adds `ViewTransform::CpgEmit`.
2. Rust relation transform contract adds `CpgOutputKind`:
   - `Nodes`
   - `Edges`
   - `Props`
   - `PropsMap`
   - `EdgesBySrc`
   - `EdgesByDst`
3. PyO3 compiler path emits `CpgEmit` transforms for canonical CPG output views.
4. PyO3 exposes `SchemaRuntime` JSON APIs:
   - `dataset_name(spec_json) -> str`
   - `dataset_schema_json(spec_json) -> str`
   - `dataset_policy_json(spec_json) -> str`
   - `dataset_contract_json(spec_json) -> str`
   - `apply_scan_policy_json(scan_policy_json, defaults_json) -> str`
   - `apply_delta_scan_policy_json(delta_scan_json, defaults_json) -> str`
5. Deleted-module reintroduction is blocked by `scripts/check_no_legacy_planning_imports.sh` once module files are removed.

## Delete Targets (Gate-Based)

### CPG runtime authority
- `src/cpg/view_builders_df.py`
- `src/cpg/spec_registry.py`
- `src/cpg/specs.py`
- `src/cpg/emit_specs.py`
- `src/cpg/prop_catalog.py`
- `src/cpg/node_families.py`
- `src/cpg/kind_catalog.py`
- `src/cpg/__init__.py`

### schema_spec operational authority
- `src/schema_spec/dataset_spec_ops.py`
- Operational helpers in `src/schema_spec/system.py` (types/contracts retained or split).

## Acceptance Gates

1. Rust compile/runtime path handles CPG outputs through `CpgEmit`.
2. No production imports of `cpg.view_builders_df`.
3. No production imports of `schema_spec.dataset_spec_ops`.
4. `schema_spec.system` imports are contract-only (no operational helper dependency).
5. Full gate passes:

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q
```

## Notes

- This plan is intentionally hard-cut and non-backward-compatible for deleted internal modules.
- Goldens are updated only after behavior-level parity review.
