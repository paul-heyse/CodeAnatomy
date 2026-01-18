# Legacy Decommissioning Execution Plan

Status: in progress (Scopes 0-3 complete)

## Purpose
Aggressively remove legacy, compatibility, and deprecated surfaces so the codebase is
aligned to the relspec-first, Ibis-first architecture described in
`docs/plans/compute_architecture_integrated_implementation_plan.md`. This plan prefers
deletion over preservation and removes dual-path and compatibility behavior that is
confusing during design-phase iteration.

## Alignment to compute_architecture_integrated_implementation_plan.md
- Relspec-first execution remains the single compile and execute surface.
- Ibis and DataFusion are the default execution path (streaming-first where possible).
- Determinism tiers and policy are explicit, not implicit or hidden by compat code.
- Compatibility shims and plan-lane adapters are removed, not preserved.

## Objectives
- Delete dead or unreferenced modules with no external references in tests/ or scripts/.
- Remove compatibility re-exports and adapter toggles that keep legacy APIs alive.
- Require explicit policy registry usage (no default global registries).
- Remove query/plan bridges that adapt ArrowDSL specs into Ibis paths.
- Decommission ArrowDSL plan-lane helpers and plan-lane builders in normalize/extract.
- Consolidate exports to only new architecture surfaces.

## Non-goals
- Do not remove SQLGlot DataFusion dialect usage (string name "datafusion_ext").
- Do not remove ArrowDSL compute kernels, schema helpers, or Ibis runtime surfaces.
- Do not introduce new long-lived compatibility shims; delete instead.

## Decommissioning principles
- Delete rather than deprecate. If a path is no longer intended, remove it fully.
- Prefer explicit injection (PolicyRegistry, EngineSession, ExecutionSurfacePolicy).
- One execution path only (Ibis/DataFusion). No adapter flags or dual-mode routing.
- No dynamic import compatibility layers for in-repo modules.

## Design decisions (locked)
- Hashing for derived IDs uses DataFusion UDFs (`stable_hash64`, `stable_hash128`) exposed
  through the Ibis expression registry. Build hashes via ExprIR call nodes (no Python loops).
- Schema normalization stays at the Arrow boundary: apply `align_table`/`encode_table` on
  materialized results, not inside Ibis expressions.
- Extract execution surface is Ibis-first: use `IbisPlan` with `ibis.memtable` sources and
  materialize at finalize time (prefer streaming readers when requested).

## Decision implementation details

### Decision A: Hashing via ExprIR + DataFusion UDFs
Narrative
Derived IDs must be expressed as ExprIR calls so the Ibis compiler can emit DataFusion UDF
expressions. This keeps hashing deterministic, UDF-backed, and free of Python row loops.

Code pattern
```python
# src/ibis_engine/hashing.py
from arrowdsl.core.ids import HashSpec
from arrowdsl.spec.expr_ir import ExprIR

def hash_expr_ir(*, spec: HashSpec) -> ExprIR:
    args = tuple(ExprIR(op="field", name=name) for name in spec.cols)
    fn = "stable_hash128" if spec.as_string else "stable_hash64"
    hashed = ExprIR(op="call", name=fn, args=args)
    if spec.prefix:
        prefix = ExprIR(op="literal", value=f"{spec.prefix}:")
        return ExprIR(op="call", name="concat", args=(prefix, hashed))
    return hashed
```

Target files
- (new) `src/ibis_engine/hashing.py`
- (update) `src/extract/registry_builders.py` (emit ExprIR derived IDs via HashSpec)
- (update) `src/relspec/rules/rel_ops.py` (ensure derived ExprIR paths map to UDFs)
- (update) `src/ibis_engine/expr_compiler.py` (confirm UDF names are registered)

Implementation checklist
- [ ] Add `hash_expr_ir` (and masked variant if required) to encapsulate hash UDF calls.
- [ ] Replace plan-lane hash ExprSpec usage in extract registry builders with ExprIR calls.
- [ ] Keep UDF names aligned to `stable_hash64`/`stable_hash128`.
- [ ] Add a small unit test covering prefix + null behavior for the ExprIR builder.

### Decision B: Arrow-boundary schema normalization
Narrative
Schema alignment and encoding must remain Arrow-native to preserve schema policy logic,
avoid duplicating casts in Ibis, and keep the execution boundary consistent.

Code pattern
```python
# extract schema finalization (Arrow boundary)
from arrowdsl.schema.schema import align_table, encode_table

aligned, _ = align_table(
    table,
    schema=policy.resolved_schema(),
    safe_cast=True,
    keep_extra_columns=policy.keep_extra_columns,
)
encoded = encode_table(aligned, columns=policy.encoding.columns) if policy.encoding else aligned
```

Target files
- (update) `src/extract/schema_ops.py`
- (update) `src/normalize/utils.py`
- (update) `src/engine/materialize.py`
- (update) `src/hamilton_pipeline/modules/extraction.py`

Implementation checklist
- [ ] Route schema alignment through `align_table` for extract outputs.
- [ ] Apply encoding with `encode_table` only after materialization.
- [ ] Ensure ordering metadata is preserved post-alignment.

### Decision C: Ibis-first extract surface
Narrative
Extract should produce Ibis plans as the canonical surface and only materialize at the
finalization boundary. This removes plan-lane adapters and keeps Ibis/DataFusion primary.

Code pattern
```python
import ibis
from arrowdsl.core.context import Ordering
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import apply_query_spec

expr = ibis.memtable(table)
plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
filtered = apply_query_spec(plan.expr, spec=query_spec)
plan = IbisPlan(expr=filtered, ordering=plan.ordering)
result = plan.to_reader(batch_size=batch_size) if prefer_reader else plan.to_table()
```

Target files
- (delete) `src/extract/plan_helpers.py`
- (delete) `src/extract/ibis_bridge.py`
- (update) `src/extract/registry_specs.py` (IbisQuerySpec end-to-end)
- (update) `src/extract/helpers.py`
- (update) `src/extract/ast_extract.py`
- (update) `src/extract/tree_sitter_extract.py`
- (update) `src/extract/bytecode_extract.py`
- (update) `src/extract/cst_extract.py`
- (update) `src/extract/symtable_extract.py`
- (update) `src/extract/runtime_inspect_extract.py`
- (update) `src/hamilton_pipeline/modules/extraction.py`

Implementation checklist
- [ ] Replace Plan/adapter paths with `IbisPlan` construction and `apply_query_spec`.
- [ ] Materialize to Arrow only in finalize steps; prefer readers when streaming requested.
- [ ] Remove plan-lane helper imports and modules once all call sites migrate.

## Inventory summary (legacy surfaces)
- Dead/unreferenced modules
  - `src/plan_audit.py`
  - `src/ibis_engine/hybrid.py`
- Compatibility shims
  - `src/cpg/__init__.py` (relspec.cpg re-exports)
  - `src/datafusion_ext/__init__.py` (Python shim for DataFusion extension)
  - `src/normalize/bytecode_cfg.py` wrappers: `build_cfg`, `build_cfg_streamable`
- Deprecated APIs
  - `src/relspec/policy_registry.py`
  - `src/relspec/policies.py` and `src/normalize/policies.py` default registries
  - `src/ibis_engine/param_tables.py:param_signature`
- Legacy bridges
  - `src/ibis_engine/query_bridge.py`
  - `src/ibis_engine/plan_bridge.py`
  - `src/arrowdsl/ir/expr.py:expr_from_expr_ir`
- Plan-lane surfaces
  - `src/arrowdsl/plan/*` and `src/arrowdsl/plan_helpers.py`
  - `src/normalize/plan_builders.py`, `src/normalize/*_plans.py`,
    `src/normalize/diagnostics.py`, `src/normalize/spans.py`
  - `src/extract/plan_helpers.py`, `src/extract/ibis_bridge.py`,
    plan-lane branches in extract helpers
- Adapter toggles
  - `src/config.py:AdapterMode`
  - `arrowdsl.plan.runner.AdapterRunOptions` and adapter-based execution

## Progress
- Scope 0: completed (denylist and baseline inventory)
- Scope 1: completed (dead modules removed)
- Scope 2: completed (compat shims removed)
- Scope 3: completed (policy registry and deprecated APIs removed)
- Scope 4+: in progress

## Scope 0: Inventory and denylist gates

Narrative
Create a lightweight denylist gate to prevent new legacy imports from reappearing and
confirm the current inventory before deletion begins. This establishes a clean baseline.

Code pattern
```python
# tests/test_legacy_surface.py
from pathlib import Path
import re

LEGACY_IMPORT_PATTERNS = (
    re.compile(r"^\\s*import\\s+datafusion_ext\\b", re.MULTILINE),
    re.compile(r"^\\s*from\\s+datafusion_ext\\b", re.MULTILINE),
    re.compile(r"^\\s*from\\s+ibis_engine\\.query_bridge\\b", re.MULTILINE),
    re.compile(r"^\\s*from\\s+ibis_engine\\.plan_bridge\\b", re.MULTILINE),
    re.compile(r"^\\s*from\\s+arrowdsl\\.plan\\b", re.MULTILINE),
    re.compile(r"^\\s*from\\s+arrowdsl\\.plan_helpers\\b", re.MULTILINE),
)

def test_no_legacy_imports() -> None:
    sources = Path("src").rglob("*.py")
    text = "\\n".join(path.read_text(encoding="utf-8") for path in sources)
    for pattern in LEGACY_IMPORT_PATTERNS:
        assert pattern.search(text) is None
```

Target files
- (new) `tests/test_legacy_surface.py`
- `docs/plans/legacy_decommissioning_execution_plan.md`

Implementation checklist
- [x] Run inventory scans for legacy/compat/deprecated tokens and capture a baseline.
- [x] Add denylist tests for legacy import paths only (not SQL dialect string usage).
- [x] Confirm baseline test passes before removing any code.

## Scope 1: Delete dead and unreferenced modules

Narrative
Remove modules that are no longer referenced anywhere in src/, tests/, or scripts/.
This includes plan audit helpers and hybrid query fallback utilities.

Code pattern
```python
from normalize.ibis_plan_builders import plan_builders_ibis

def list_normalize_builders() -> tuple[str, ...]:
    return tuple(sorted(plan_builders_ibis().keys()))
```

Target files
- `src/plan_audit.py`
- `src/ibis_engine/hybrid.py`

Implementation checklist
- [x] `rg -n "plan_audit|ibis_engine\\.hybrid" src tests scripts` shows no references.
- [x] Delete the modules and remove any exports or docs that mention them.

## Scope 2: Remove compatibility shims

Narrative
Eliminate compatibility re-exports and shims that hide old paths. Update call sites to
new, direct APIs.

Code pattern
```python
from relspec.cpg.build_edges import EdgeBuildConfig, EdgeBuildInputs, build_cpg_edges
from datafusion_engine.function_factory import (
    default_function_factory_policy,
    install_function_factory,
)

edge_inputs = EdgeBuildInputs(...)
edge_config = EdgeBuildConfig(inputs=edge_inputs, ibis_backend=backend)
edges = build_cpg_edges(ctx=session.ctx, config=edge_config)

install_function_factory(session.df_ctx(), policy=default_function_factory_policy())
```

Target files
- `src/cpg/__init__.py`
- `src/normalize/bytecode_cfg.py`
- `src/cpg/kinds_ultimate.py`
- `src/normalize/__init__.py`
- `src/datafusion_ext/__init__.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/datafusion_engine/function_factory.py`

Implementation checklist
- [x] Remove relspec.cpg re-exports from `src/cpg/__init__.py`.
- [x] Update all imports to reference `relspec.cpg.*` directly.
- [x] Replace `normalize.bytecode_cfg:build_cfg` usage in extractor strings with
      `build_cfg_edges` or `build_cfg_edges_streamable`.
- [x] Delete compatibility wrappers `build_cfg` and `build_cfg_streamable`.
- [x] Replace dynamic import of `datafusion_ext` with a direct extension API
      (Rust extension or new `datafusion_engine` module).
- [x] Remove `src/datafusion_ext/__init__.py` once extension integration is direct.

## Scope 3: Policy registry and deprecated API removal

Narrative
Remove deprecated registries and require explicit policy injection everywhere. Remove
deprecated param signature helpers in favor of Arrow-based signatures.

Code pattern
```python
from relspec.rules.policies import PolicyRegistry
from relspec.policies import confidence_policy_from_schema
from ibis_engine.param_tables import param_signature_from_array

registry = PolicyRegistry()
policy = confidence_policy_from_schema(schema, registry=registry)
signature = param_signature_from_array(logical_name="param_list", values=values_array)
```

Target files
- `src/relspec/policy_registry.py`
- `src/relspec/policies.py`
- `src/normalize/policies.py`
- `src/ibis_engine/param_tables.py`
- `src/hamilton_pipeline/modules/inputs.py` (inject registry from pipeline inputs)

Implementation checklist
- [x] Remove deprecated registry module and move constants into a non-legacy location.
- [x] Require `registry=` in policy helpers (no default global registry).
- [x] Replace `param_signature` call sites with `param_signature_from_array`.

## Scope 4: Remove query and expr bridges

Narrative
Eliminate legacy QuerySpec and ExprSpec bridges. Use IbisQuerySpec directly throughout
relspec, normalize, and extract paths. Derived ID hashing uses DataFusion UDFs through
ExprIR calls so the compiler can emit Ibis functions without Python loops.

Code pattern
```python
from arrowdsl.core.ids import HashSpec
from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.hashing import hash_expr_ir
from ibis_engine.query_compiler import IbisProjectionSpec, IbisQuerySpec, apply_query_spec

spec = IbisQuerySpec(
    projection=IbisProjectionSpec(
        base=("path", "lineno"),
        derived={"node_id": hash_expr_ir(spec=HashSpec(prefix="node", cols=("path", "lineno")))},
    ),
    predicate=ExprIR(op="field", name="path"),
)
filtered = apply_query_spec(table, spec=spec)
```

Target files
- `src/ibis_engine/hashing.py`
- `src/ibis_engine/query_bridge.py`
- `src/ibis_engine/plan_bridge.py`
- `src/arrowdsl/ir/expr.py`
- `src/relspec/rules/handlers/normalize.py`
- `src/extract/plan_helpers.py`
- `src/extract/helpers.py`
- `src/extract/registry_builders.py`
- `src/extract/registry_specs.py`

Implementation checklist
- [ ] Add `hash_expr_ir` (and masked variant if needed) for UDF-backed hashing.
- [ ] Replace QuerySpec conversions with direct IbisQuerySpec usage in extract/normalize.
- [ ] Remove `query_bridge` and `plan_bridge` modules once no callers remain.
- [ ] Remove ExprIR to ExprSpec conversion when plan-lane removal is complete.

## Scope 5: Normalize plan-lane decommission

Narrative
Normalize should use Ibis plan builders exclusively. Remove plan-lane builders, plan-lane
diagnostics, and plan-lane span helpers once Ibis equivalents are wired. Schema alignment
and encoding remain Arrow-boundary concerns after Ibis plan materialization.

Code pattern
```python
from normalize.ibis_plan_builders import resolve_plan_builder_ibis
from normalize.registry_specs import dataset_schema_policy
from arrowdsl.schema.schema import align_table, encode_table

builder = resolve_plan_builder_ibis("cfg_edges")
plan = builder(catalog, ctx, backend)
result = plan.to_table()
policy = dataset_schema_policy("cfg_edges_v1", ctx=ctx)
aligned, _ = align_table(
    result,
    schema=policy.resolved_schema(),
    safe_cast=True,
    keep_extra_columns=policy.keep_extra_columns,
)
final = encode_table(aligned, columns=policy.encoding.columns) if policy.encoding else aligned
```

Target files
- `src/normalize/plan_builders.py`
- `src/normalize/diagnostics.py`
- `src/normalize/diagnostics_plans.py`
- `src/normalize/bytecode_cfg_plans.py`
- `src/normalize/bytecode_dfg_plans.py`
- `src/normalize/types_plans.py`
- `src/normalize/spans.py` (remove plan-lane wrappers; keep core span logic)
- `src/normalize/__init__.py`
- `src/normalize/utils.py`
- `src/normalize/runner.py`
- `src/relspec/rules/handlers/normalize.py`

Implementation checklist
- [ ] Replace `resolve_plan_builder` with `resolve_plan_builder_ibis`.
- [ ] Remove plan-lane diagnostics builders and use Ibis diagnostics plans.
- [ ] Remove plan-lane span error plans after Ibis plan builder coverage is verified.
- [ ] Delete plan-lane builder modules and re-exports from normalize __init__.
- [ ] Apply Arrow-boundary schema alignment/encoding after Ibis plan materialization.

## Scope 6: Extract plan-lane decommission

Narrative
Extract should emit Ibis plans or Ibis tables only. Remove Plan-based helpers and
adapter routing from extract plan helpers. Use `ibis.memtable` for extracted rows,
apply IbisQuerySpec, and normalize/encode after materialization.

Code pattern
```python
import ibis
from arrowdsl.core.context import Ordering
from arrowdsl.schema.schema import align_table, encode_table
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import apply_query_spec

expr = ibis.memtable(table)
plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
filtered = apply_query_spec(plan.expr, spec=query_spec)
materialized = IbisPlan(expr=filtered, ordering=plan.ordering).to_table()
aligned, _ = align_table(materialized, schema=policy.resolved_schema(), safe_cast=True)
final = encode_table(aligned, columns=policy.encoding.columns) if policy.encoding else aligned
```

Target files
- `src/extract/plan_helpers.py`
- `src/extract/helpers.py`
- `src/extract/ibis_bridge.py`
- `src/extract/registry_specs.py`
- `src/extract/schema_ops.py`
- `src/extract/ast_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/repo_scan.py`
- `src/extract/runtime_inspect_extract.py`
- `src/extract/registry_builders.py`

Implementation checklist
- [ ] Remove plan-lane routing and adapter options in extract helpers.
- [ ] Convert extract query application to IbisQuerySpec + apply_query_spec.
- [ ] Delete extract plan helper modules after all call sites are Ibis-only.
- [ ] Materialize only at finalization; apply Arrow-boundary align/encode post-materialization.

## Scope 7: ArrowDSL plan-lane removal and adapter mode elimination

Narrative
Remove ArrowDSL plan-lane execution and adapter toggles once normalize and extract are
fully Ibis-first. Replace adapter routing with EngineSession and explicit policy, while
keeping schema alignment and encoding at the Arrow boundary.

Code pattern
```python
from engine.plan_policy import ExecutionSurfacePolicy
from engine.session import EngineSession
from arrowdsl.schema.schema import align_table, encode_table

session = EngineSession(
    ctx=ctx,
    runtime_profile=runtime_profile,
    df_profile=df_profile,
    ibis_backend=ibis_backend,
    datasets=datasets,
    surface_policy=ExecutionSurfacePolicy(prefer_streaming=True),
)
result = plan.to_reader(batch_size=batch_size) if prefer_reader else plan.to_table()
aligned, _ = align_table(result, schema=policy.resolved_schema(), safe_cast=True)
final = encode_table(aligned, columns=policy.encoding.columns) if policy.encoding else aligned
```

Target files
- `src/arrowdsl/plan_helpers.py`
- `src/arrowdsl/plan/*`
- `src/config.py`
- `src/arrowdsl/exec/runtime.py`
- `src/arrowdsl/finalize/finalize.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/normalization.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/engine/materialize.py`
- `src/relspec/engine.py`

Implementation checklist
- [ ] Remove AdapterMode and adapter execution policy toggles.
- [ ] Remove plan runner adapter code paths.
- [ ] Remove ArrowDSL Plan usage from core entrypoints.
- [ ] Verify streaming-first outputs remain the default surface.
- [ ] Keep schema alignment and encoding at the Arrow boundary post-materialization.

## Scope 8: Package exports and docs cleanup

Narrative
Trim __all__ exports and public module surfaces to only the new architecture API.
Remove references to deprecated or legacy modules in docs.

Code pattern
```python
# src/normalize/__init__.py
__all__ = [
    "NormalizeRule",
    "run_normalize_streamable",
    "run_normalize_streamable_result",
    "plan_builders_ibis",
    "resolve_plan_builder_ibis",
]
```

Target files
- `src/cpg/__init__.py`
- `src/normalize/__init__.py`
- `docs/plans/*`
- `README.md`

Implementation checklist
- [ ] Remove exports of plan-lane builders and wrappers.
- [ ] Update docs to reference only Ibis-first APIs.

## Full target file list (consolidated)

Delete
- `src/plan_audit.py`
- `src/ibis_engine/hybrid.py`
- `src/datafusion_ext/__init__.py`
- `src/relspec/policy_registry.py`
- `src/ibis_engine/query_bridge.py`
- `src/ibis_engine/plan_bridge.py`
- `src/extract/plan_helpers.py`
- `src/extract/ibis_bridge.py`
- `src/normalize/plan_builders.py`
- `src/normalize/diagnostics.py`
- `src/normalize/diagnostics_plans.py`
- `src/normalize/bytecode_cfg_plans.py`
- `src/normalize/bytecode_dfg_plans.py`
- `src/normalize/types_plans.py`
- `src/arrowdsl/plan_helpers.py`
- `src/arrowdsl/plan/catalog.py`
- `src/arrowdsl/plan/joins.py`
- `src/arrowdsl/plan/ordering_policy.py`
- `src/arrowdsl/plan/plan.py`
- `src/arrowdsl/plan/query.py`
- `src/arrowdsl/plan/runner.py`
- `src/arrowdsl/plan/runner_types.py`
- `src/arrowdsl/plan/scan_io.py`
- `src/arrowdsl/plan/schema_utils.py`
- `src/arrowdsl/plan/metrics.py`
- `src/arrowdsl/plan/scan_builder.py`
- `src/arrowdsl/plan/scan_telemetry.py`

Update
- `src/cpg/__init__.py`
- `src/cpg/kinds_ultimate.py`
- `src/normalize/bytecode_cfg.py`
- `src/normalize/__init__.py`
- `src/relspec/policies.py`
- `src/normalize/policies.py`
- `src/ibis_engine/param_tables.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/datafusion_engine/function_factory.py`
- `src/relspec/rules/handlers/normalize.py`
- `src/normalize/spans.py`
- `src/normalize/utils.py`
- `src/normalize/runner.py`
- `src/extract/helpers.py`
- `src/extract/registry_specs.py`
- `src/extract/schema_ops.py`
- `src/extract/ast_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/repo_scan.py`
- `src/extract/runtime_inspect_extract.py`
- `src/extract/registry_builders.py`
- `src/config.py`
- `src/arrowdsl/finalize/finalize.py`
- `src/arrowdsl/exec/runtime.py`
- `src/engine/materialize.py`
- `src/hamilton_pipeline/modules/normalization.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/relspec/engine.py`
- `README.md`

## Acceptance gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
- Unit tests for legacy surface removal (denylist) and Ibis plan coverage
