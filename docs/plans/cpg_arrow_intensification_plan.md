# Implementation Plan: CPG Arrow Intensification (Plan-Lane + Modularization)

This plan extends the CPG plan-lane migration by adopting the most applicable
design paradigms from `normalize_arrow_intensification_plan.md`. The focus is
deeper PyArrow integration, deduplication, and maintainability while preserving
current CPG outputs and contracts.

## Goals
- Centralize shared plan-lane expressions and ID policies.
- Move derived columns into declarative sources to reduce ad hoc logic.
- Make determinism explicit and optional at CPG build surfaces.
- Surface ordering and pipeline-breaker metadata at build boundaries.
- Drive encoding and schema metadata from dataset specs.
- Adopt QuerySpec + scan ordering options when inputs are dataset-backed.
- Reduce duplication across CPG emitters, relations, and quality capture.

## Constraints
- Preserve current CPG outputs and dataset contracts.
- Keep Acero as the required execution lane.
- Maintain strict typing and Ruff compliance (no suppressions).
- Avoid new non-Arrow dependencies.

---

## Scope 1: CPG Plan Expression Helpers (Deduplicate Patterns)

### Description
Introduce a shared CPG plan-expression module that centralizes missing-column
handling, typed nulls, coalesce patterns, and bitmask helpers used across
nodes/edges/props/quality.

### Code pattern
```python
# src/cpg/plan_exprs.py
import pyarrow as pa
from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc


def column_or_null_expr(
    name: str,
    dtype: pa.DataType,
    *,
    available: set[str],
) -> ComputeExpression:
    if name in available:
        return ensure_expression(pc.cast(pc.field(name), dtype, safe=False))
    return ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))


def coalesce_expr(
    cols: tuple[str, ...],
    *,
    dtype: pa.DataType,
    available: set[str],
) -> ComputeExpression:
    exprs = [
        ensure_expression(pc.cast(pc.field(col), dtype, safe=False))
        for col in cols
        if col in available
    ]
    if not exprs:
        return ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))
    return exprs[0] if len(exprs) == 1 else ensure_expression(pc.coalesce(*exprs))
```

### Target files
- New: `src/cpg/plan_exprs.py`
- Update: `src/cpg/emit_nodes.py`
- Update: `src/cpg/emit_edges.py`
- Update: `src/cpg/emit_props.py`
- Update: `src/cpg/relations.py`
- Update: `src/cpg/quality.py`

### Implementation checklist
- [ ] Add shared helpers for missing-column and coalesce patterns.
- [ ] Replace local `_coalesce_expr` / `_field_expr` helpers with shared versions.
- [ ] Centralize bitmask and null/zero ID checks for plan predicates.

---

## Scope 2: Declarative Derived Plan Sources (PlanRef + Derivers)

### Description
Move derived columns and computed tables into `PlanRef.derive` sources to reduce
ad hoc logic in builders and align with normalize’s declarative derived columns.

### Code pattern
```python
# src/cpg/catalog.py
def derive_cst_defs_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    defs = catalog.resolve(PlanRef("cst_defs"), ctx=ctx)
    if defs is None:
        return None
    expr = coalesce_expr(("def_kind", "kind"), dtype=pa.string(), available=set(defs.schema(ctx=ctx).names))
    return set_or_append_column(defs, name="def_kind_norm", expr=expr, ctx=ctx)


PlanRef("cst_defs_norm", derive=derive_cst_defs_norm)
```

### Target files
- Update: `src/cpg/catalog.py`
- Update: `src/cpg/spec_registry.py`
- Update: `src/cpg/build_props.py`
- Update: `src/cpg/relations.py`

### Implementation checklist
- [ ] Register derived plan sources for `cst_defs_norm` and `scip_role_flags`.
- [ ] Convert builder-local derived logic to `PlanRef.derive`.
- [ ] Keep derived plans reusable across node/prop/edge specs.

---

## Scope 3: HashSpec Registry for CPG IDs

### Description
Centralize hash ID definitions so edge IDs (and any future derived IDs) are
uniform across plan and kernel lanes.

### Code pattern
```python
# src/cpg/hash_specs.py
from arrowdsl.core.ids import HashSpec

EDGE_ID_BASE = HashSpec(
    prefix="edge",
    cols=("src", "dst"),
    extra_literals=(),
    as_string=True,
)
EDGE_ID_SPAN = HashSpec(
    prefix="edge",
    cols=("src", "dst", "path", "bstart", "bend"),
    extra_literals=(),
    as_string=True,
)
```

### Target files
- New: `src/cpg/hash_specs.py`
- Update: `src/cpg/emit_edges.py`

### Implementation checklist
- [ ] Add CPG hash spec registry with edge ID definitions.
- [ ] Use registry entries in plan-lane edge emission.
- [ ] Keep any future ID changes localized to the registry.

---

## Scope 4: Determinism Tier Hooks (Kernel-Lane Canonical Ordering)

### Description
Apply canonical ordering only at finalize boundaries using kernel-lane stable
sort indices. Avoid plan-lane `order_by` (a pipeline breaker) unless an upstream
contract explicitly requires in-plan ordering. Use plan ordering metadata to
skip redundant sorts when the plan already establishes the contract order.

### Code pattern
```python
# src/cpg/plan_helpers.py
from arrowdsl.compute.kernels import canonical_sort_if_canonical
from arrowdsl.core.context import ExecutionContext, OrderingLevel
from arrowdsl.finalize.finalize import Contract
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.core.interop import TableLike


def finalize_with_canonical_order(
    plan: Plan,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> TableLike:
    table = PlanSpec.from_plan(plan).to_table(ctx=ctx)
    explicit_keys = tuple((sk.column, sk.order) for sk in contract.canonical_sort)
    if plan.ordering.level == OrderingLevel.EXPLICIT and plan.ordering.keys == explicit_keys:
        return table
    return canonical_sort_if_canonical(table, sort_keys=contract.canonical_sort, ctx=ctx)
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`
- Update: `src/cpg/schemas.py`

### Implementation checklist
- [ ] Add a determinism-aware finalize helper using stable sort indices.
- [ ] Pull canonical sort keys from CPG contract specs.
- [ ] Skip canonical sort when the plan already establishes the contract order.
- [ ] Avoid plan-lane `order_by` unless explicitly required.

---

## Scope 5: Encoding Policy via Schema Metadata

### Description
Drive dictionary encoding from schema/field metadata rather than hard-coded
column lists, mirroring the normalize plan’s metadata-driven encoding policy.

### Code pattern
```python
# src/cpg/plan_helpers.py
def encoding_columns_from_metadata(schema: SchemaLike) -> list[str]:
    cols: list[str] = []
    for field in schema:
        meta = field.metadata or {}
        if meta.get(b"encoding") == b"dictionary":
            cols.append(field.name)
    return cols
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/schemas.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`

### Implementation checklist
- [ ] Mark dictionary-encoded fields in CPG schemas via field metadata.
- [ ] Compute encoding projections from metadata in plan helpers.
- [ ] Remove hard-coded encoding spec tuples where possible.

---

## Scope 6: Schema Metadata Control Plane

### Description
Attach schema-level metadata for CPG datasets (contract name, stage, determinism
tier) to improve observability and enforce consistent metadata propagation.

### Code pattern
```python
# src/cpg/schemas.py
SCHEMA_META = {
    b"contract_name": b"cpg_nodes_v1",
    b"stage": b"cpg",
}

CPG_NODES_SCHEMA = CPG_NODES_SPEC.table_spec.to_arrow_schema().with_metadata(SCHEMA_META)
```

### Target files
- Update: `src/cpg/schemas.py`
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [ ] Define schema metadata per CPG dataset.
- [ ] Ensure metadata survives plan alignment/finalize steps.
- [ ] Keep metadata values stable across releases.

---

## Scope 7: Metadata-Aware Schema Alignment

### Description
Add metadata-aware schema alignment helpers that unify schemas using Arrow
rules and preserve field/schema metadata before casting and concatenation.

### Code pattern
```python
# src/cpg/plan_helpers.py
import pyarrow as pa
from arrowdsl.core.interop import SchemaLike, TableLike


def unify_schema_with_metadata(
    schemas: list[SchemaLike],
    *,
    promote_options: str = "permissive",
) -> SchemaLike:
    return pa.unify_schemas(schemas, promote_options=promote_options)


def align_table_to_schema(table: TableLike, *, schema: SchemaLike) -> TableLike:
    return table.cast(schema)
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [ ] Add a metadata-aware unify helper (preserve first-schema metadata).
- [ ] Use unified schema when concatenating parts or aligning finalize outputs.
- [ ] Keep schema evolution handling centralized in plan helpers.

---

## Scope 8: Dictionary Unification at Finalize

### Description
Unify dictionaries on finalized outputs to ensure consistent dictionary pools
across concatenated parts and downstream consumers.

### Code pattern
```python
# src/cpg/plan_helpers.py
from arrowdsl.core.interop import TableLike


def finalize_table(table: TableLike, *, unify_dicts: bool = True) -> TableLike:
    if unify_dicts:
        return table.unify_dictionaries()
    return table
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [ ] Add a finalize helper that optionally unifies dictionaries.
- [ ] Apply dictionary unification after materialization, before contracts.
- [ ] Keep the behavior configurable for performance tuning.

---

## Scope 9: Deferred JSON Serialization for Heavy Props

### Description
Keep heavy property values as native list/struct types through plan-lane
processing and serialize to JSON only at the final projection step.

### Code pattern
```python
# src/cpg/emit_props.py
def json_value_expr(expr: ComputeExpression, *, dtype: DataTypeLike) -> ComputeExpression:
    if patypes.is_list(dtype) or patypes.is_struct(dtype):
        func = ensure_json_udf(dtype)
        return ensure_expression(pc.call_function(func, [expr]))
    return ensure_expression(pc.cast(expr, pa.string(), safe=False))
```

### Target files
- Update: `src/cpg/emit_props.py`
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [ ] Preserve list/struct values for heavy JSON props through the plan.
- [ ] Serialize to JSON only at the final projection boundary.
- [ ] Keep output schema unchanged (value_json remains string).

---

## Scope 10: Internal Prop Value Struct/Union Staging

### Description
Stage property values in an internal struct or union column and route them to
the five value columns (`value_*`) in a single projection, reducing duplicated
casting logic.

### Code pattern
```python
# src/cpg/emit_props.py
value_struct = pc.struct(
    [
        pc.field("value_str"),
        pc.field("value_int"),
        pc.field("value_float"),
        pc.field("value_bool"),
        pc.field("value_json"),
    ],
    ["value_str", "value_int", "value_float", "value_bool", "value_json"],
)
```

### Target files
- Update: `src/cpg/emit_props.py`
- Update: `src/cpg/plan_helpers.py`

### Implementation checklist
- [ ] Add an internal staging column for prop values.
- [ ] Route staged values to canonical columns in a single projection.
- [ ] Ensure schema alignment still matches CPG props contract.

---

## Scope 11: LargeList/ListView Tuning for Heavy Props (Optional)

### Description
Adopt `large_list` or `list_view` types for large list-valued properties to
reduce copying and avoid 32-bit offset limits.

### Code pattern
```python
# src/cpg/emit_props.py
dtype = pa.large_list(pa.string())
expr = ensure_expression(pc.cast(pc.field("qnames"), dtype, safe=False))
```

### Target files
- Update: `src/cpg/emit_props.py`
- Update: `src/cpg/schemas.py`

### Implementation checklist
- [ ] Evaluate list sizes for `qnames`, `callee_qnames`, and `details`.
- [ ] Use `large_list` or `list_view` for internal plan-lane staging.
- [ ] Keep final JSON string output stable.

---

## Scope 12: Schema Metadata Equality Checks (Optional Debug)

### Description
Provide a debug-mode assertion that verifies schema metadata is preserved after
alignment and finalize.

### Code pattern
```python
# src/cpg/plan_helpers.py
def assert_schema_metadata(table: TableLike, *, schema: SchemaLike) -> None:
    if not table.schema.equals(schema, check_metadata=True):
        msg = "Schema metadata mismatch after finalize."
        raise ValueError(msg)
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [ ] Add a metadata equality assertion helper.
- [ ] Call it when `ctx.debug` is enabled.
- [ ] Keep the check non-invasive for production runs.

---

## Scope 13: Pipeline-Breaker Aware Materialization Surfaces

### Description
Expose pipeline-breaker metadata and provide reader surfaces that are only valid
for fully streaming plans. Keep eager materialization confined to finalize
boundaries and surface pipeline-breaker diagnostics for debugging.

### Code pattern
```python
# src/cpg/plan_helpers.py
from arrowdsl.core.interop import RecordBatchReaderLike
from arrowdsl.plan.plan import Plan, PlanSpec


def plan_reader(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    return PlanSpec.from_plan(plan).to_reader(ctx=ctx)
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`
- Update: `src/cpg/artifacts.py`

### Implementation checklist
- [ ] Add reader/materialization helpers that honor `PlanSpec.pipeline_breakers`.
- [ ] Keep materialization confined to finalize boundaries.
- [ ] Surface pipeline-breaker metadata in build artifacts for debugging.

---

## Scope 14: QuerySpec Scan Integration + Ordering Options

### Description
When inputs are dataset-backed, drive scans via `QuerySpec` and `ScanContext` so
pushdown and projection are centralized. Use scan ordering options
(`implicit_ordering`, `require_sequenced_output`) to populate plan ordering
metadata and enable provenance tie-breakers under canonical determinism.

### Code pattern
```python
# src/cpg/plan_helpers.py
import pyarrow.dataset as ds

from arrowdsl.core.context import Ordering
from arrowdsl.plan.plan import Plan
from schema_spec.system import DatasetSpec


def plan_from_dataset(
    dataset: ds.Dataset,
    *,
    spec: DatasetSpec,
    ctx: ExecutionContext,
) -> Plan:
    scan_ctx = spec.scan_context(dataset, ctx)
    ordering = (
        Ordering.implicit()
        if ctx.runtime.scan.implicit_ordering or ctx.runtime.scan.require_sequenced_output
        else Ordering.unordered()
    )
    return Plan(decl=scan_ctx.acero_decl(), label=spec.name, ordering=ordering, pipeline_breakers=())
```

### Target files
- New: `src/cpg/query_specs.py`
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/catalog.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [ ] Define QuerySpec objects for dataset-backed CPG inputs.
- [ ] Compile scan plans via `ScanContext.acero_decl()` and centralized scan policy.
- [ ] Honor scan ordering options in plan ordering metadata.
- [ ] Use provenance columns as tie-breakers when canonical sorting.

---

## Acceptance Criteria
- Shared plan expression helpers replace duplicated CPG plan-lane logic.
- Derived columns are registered as declarative plan sources.
- Hash specs for CPG IDs live in a centralized registry.
- Determinism tier hooks optionally apply canonical ordering.
- Encoding policy is driven by schema metadata.
- Schema metadata is attached and preserved through finalize.
- Metadata-aware schema alignment is used where parts are merged.
- Finalized outputs unify dictionary pools when configured.
- Heavy JSON props serialize only at the finalize boundary.
- Internal prop value staging reduces duplicate casting logic.

## Acceptance Criteria
- Shared plan expression helpers replace duplicated CPG plan-lane logic.
- Derived columns are registered as declarative plan sources.
- Hash specs for CPG IDs live in a centralized registry.
- Determinism tier hooks optionally apply canonical ordering.
- Encoding policy is driven by schema metadata.
- CPG schemas carry stable metadata and preserve it through finalize.
