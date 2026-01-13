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

## Status
- Completed in code; checklists reflect implemented scope.

---

## Scope 1: CPG Plan Expression Helpers (Deduplicate Patterns)

### Description
Introduce a shared CPG plan-expression module that centralizes missing-column
handling, typed nulls, coalesce patterns, and bitmask helpers used across
nodes/edges/props/quality.

### Code pattern
```python
# src/cpg/plan_exprs.py
from collections.abc import Sequence

import pyarrow as pa

from arrowdsl.core.interop import ComputeExpression, DataTypeLike, ensure_expression, pc


def column_or_null_expr(
    name: str,
    dtype: DataTypeLike,
    *,
    available: set[str],
) -> ComputeExpression:
    if name in available:
        return ensure_expression(pc.cast(pc.field(name), dtype, safe=False))
    return ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))


def coalesce_expr(
    cols: Sequence[str],
    *,
    dtype: DataTypeLike,
    available: set[str],
) -> ComputeExpression:
    exprs = [
        ensure_expression(pc.cast(pc.field(col), dtype, safe=False))
        for col in cols
        if col in available
    ]
    if not exprs:
        return ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))
    if len(exprs) == 1:
        return exprs[0]
    return ensure_expression(pc.coalesce(*exprs))


def invalid_id_expr(values: ComputeExpression, *, dtype: DataTypeLike) -> ComputeExpression:
    return ensure_expression(pc.or_(pc.is_null(values), zero_expr(values, dtype=dtype)))


def bitmask_is_set_expr(values: ComputeExpression, *, mask: int) -> ComputeExpression:
    roles = pc.cast(values, pa.int64(), safe=False)
    hit = pc.not_equal(pc.bit_wise_and(roles, pa.scalar(mask)), pa.scalar(0))
    return ensure_expression(pc.fill_null(hit, fill_value=False))
```

### Target files
- New: `src/cpg/plan_exprs.py`
- Update: `src/cpg/emit_nodes.py`
- Update: `src/cpg/emit_edges.py`
- Update: `src/cpg/emit_props.py`
- Update: `src/cpg/relations.py`
- Update: `src/cpg/quality.py`

### Implementation checklist
- [x] Add shared helpers for missing-column and coalesce patterns.
- [x] Replace local `_coalesce_expr` / `_field_expr` helpers with shared versions.
- [x] Centralize bitmask and null/zero ID checks for plan predicates.

---

## Scope 2: Declarative Derived Plan Sources (PlanRef + Derivers)

### Description
Move derived columns and computed tables into `PlanRef.derive` sources to reduce
ad hoc logic in builders and align with normalize's declarative derived columns.

### Code pattern
```python
# src/cpg/catalog.py
import pyarrow as pa

from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc
from cpg.plan_exprs import bitmask_is_set_expr, coalesce_expr
from cpg.plan_helpers import set_or_append_column
from cpg.role_flags import ROLE_FLAG_SPECS


def derive_cst_defs_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    defs = catalog.resolve(PlanRef("cst_defs"), ctx=ctx)
    if defs is None:
        return None
    available = set(defs.schema(ctx=ctx).names)
    expr = coalesce_expr(("def_kind", "kind"), dtype=pa.string(), available=available)
    return set_or_append_column(defs, name="def_kind_norm", expr=expr, ctx=ctx)


def derive_scip_role_flags(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    occurrences = catalog.resolve(PlanRef("scip_occurrences"), ctx=ctx)
    if occurrences is None:
        return None
    flag_exprs: list[ComputeExpression] = []
    flag_names: list[str] = []
    for name, mask, _ in ROLE_FLAG_SPECS:
        hit = bitmask_is_set_expr(pc.field("symbol_roles"), mask=mask)
        flag_exprs.append(ensure_expression(pc.cast(hit, pa.int32(), safe=False)))
        flag_names.append(name)
    ...

# src/cpg/spec_registry.py
PlanRef("cst_defs_norm", derive=derive_cst_defs_norm)
PlanRef("scip_role_flags", derive=derive_scip_role_flags)
```

### Target files
- Update: `src/cpg/catalog.py`
- Update: `src/cpg/spec_registry.py`
- Update: `src/cpg/role_flags.py`
- Update: `src/cpg/build_props.py`
- Update: `src/cpg/relations.py`

### Implementation checklist
- [x] Register derived plan sources for `cst_defs_norm` and `scip_role_flags`.
- [x] Convert builder-local derived logic to `PlanRef.derive`.
- [x] Keep derived plans reusable across node/prop/edge specs.

---

## Scope 3: HashSpec Registry for CPG IDs

### Description
Centralize hash ID definitions so edge IDs (and any future derived IDs) are
uniform across plan and kernel lanes.

### Code pattern
```python
# src/cpg/hash_specs.py
from dataclasses import replace

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


def edge_hash_specs(edge_kind: str) -> tuple[HashSpec, HashSpec]:
    extra = (edge_kind,) if edge_kind else ()
    return (
        replace(EDGE_ID_BASE, extra_literals=EDGE_ID_BASE.extra_literals + extra),
        replace(EDGE_ID_SPAN, extra_literals=EDGE_ID_SPAN.extra_literals + extra),
    )
```

### Target files
- New: `src/cpg/hash_specs.py`
- Update: `src/cpg/emit_edges.py`

### Implementation checklist
- [x] Add CPG hash spec registry with edge ID definitions.
- [x] Use registry entries in plan-lane edge emission.
- [x] Keep any future ID changes localized to the registry.

---

## Scope 4: Determinism Tier Hooks (Finalize Boundary)

### Description
Apply canonical ordering only at finalize boundaries. Skip redundant canonical
sorting when plan ordering already matches the contract canonical sort keys.

### Code pattern
```python
# src/cpg/plan_helpers.py
from arrowdsl.core.context import DeterminismTier, ExecutionContext, OrderingLevel
from arrowdsl.finalize.finalize import Contract
from arrowdsl.plan.plan import Plan


def finalize_context_for_plan(
    plan: Plan,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> ExecutionContext:
    if ctx.determinism != DeterminismTier.CANONICAL:
        return ctx
    explicit_keys = tuple((sk.column, sk.order) for sk in contract.canonical_sort)
    if plan.ordering.level == OrderingLevel.EXPLICIT and plan.ordering.keys == explicit_keys:
        return ctx.with_determinism(DeterminismTier.STABLE_SET)
    return ctx
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [x] Add a determinism-aware finalize helper.
- [x] Pull canonical sort keys from CPG contract specs.
- [x] Skip canonical sort when the plan already establishes the contract order.
- [x] Avoid plan-lane `order_by` unless explicitly required.

---

## Scope 5: Encoding Policy via Schema Metadata

### Description
Drive dictionary encoding from schema/field metadata rather than hard-coded
column lists, mirroring the normalize plan's metadata-driven encoding policy.

### Code pattern
```python
# src/cpg/schemas.py
ENCODING_METADATA_KEY = "encoding"
ENCODING_DICTIONARY = "dictionary"


def _dict_metadata() -> dict[str, str]:
    return {ENCODING_METADATA_KEY: ENCODING_DICTIONARY}

ArrowFieldSpec(
    name="node_kind",
    dtype=DICT_STRING,
    nullable=False,
    metadata=_dict_metadata(),
)

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
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [x] Mark dictionary-encoded fields in CPG schemas via field metadata.
- [x] Compute encoding projections from metadata in plan helpers.
- [x] Remove hard-coded encoding spec tuples where possible.

---

## Scope 6: Schema Metadata Control Plane

### Description
Attach schema-level metadata for CPG datasets (contract name, stage, determinism
metadata) to improve observability and enforce consistent propagation.

### Code pattern
```python
# src/cpg/schemas.py
from arrowdsl.schema.schema import SchemaMetadataSpec


def _cpg_metadata(dataset_name: str, *, contract_name: str) -> SchemaMetadataSpec:
    return SchemaMetadataSpec(
        schema_metadata={
            b"cpg_stage": b"cpg",
            b"cpg_dataset": dataset_name.encode("utf-8"),
            b"contract_name": contract_name.encode("utf-8"),
            b"determinism_tier": b"best_effort",
        }
    )

CPG_NODES_SPEC = make_dataset_spec(
    table_spec=CPG_NODES_TABLE_SPEC,
    contract_spec=CPG_NODES_CONTRACT_SPEC,
    metadata_spec=_cpg_metadata("cpg_nodes_v1", contract_name="cpg_nodes_v1"),
)
```

### Target files
- Update: `src/cpg/schemas.py`
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [x] Define schema metadata per CPG dataset.
- [x] Ensure metadata survives plan alignment/finalize steps.
- [x] Keep metadata values stable across releases.

---

## Scope 7: Metadata-Aware Schema Alignment

### Description
Add metadata-aware schema alignment helpers that unify schemas using Arrow
rules and preserve field/schema metadata before casting and concatenation.

### Code pattern
```python
# src/cpg/plan_helpers.py
import pyarrow as pa


def unify_schema_with_metadata(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
) -> SchemaLike:
    if not schemas:
        return pa.schema([])
    try:
        unified = pa.unify_schemas(list(schemas), promote_options=promote_options)
    except TypeError:
        unified = pa.unify_schemas(list(schemas))

    base = schemas[0]
    field_meta = {field.name: field.metadata for field in base if field.metadata}
    fields = []
    for field in unified:
        meta = field_meta.get(field.name)
        fields.append(field.with_metadata(meta) if meta else field)
    unified = pa.schema(fields)
    if base.metadata:
        return unified.with_metadata(dict(base.metadata))
    return unified


def align_table_to_schema(table: TableLike, *, schema: SchemaLike) -> TableLike:
    return table.cast(schema)
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/merge.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [x] Add a metadata-aware unify helper (preserve first-schema metadata).
- [x] Use unified schema when concatenating parts or aligning finalize outputs.
- [x] Keep schema evolution handling centralized in plan helpers.

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
- [x] Add a finalize helper that optionally unifies dictionaries.
- [x] Apply dictionary unification after materialization, before contracts.
- [x] Keep the behavior configurable for performance tuning.

---

## Scope 9: Deferred JSON Serialization for Heavy Props

### Description
Keep heavy property values as native list/struct types through plan-lane
processing and serialize to JSON only at the final projection step.

### Code pattern
```python
# src/cpg/emit_props.py
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
import pyarrow.types as patypes

from arrowdsl.compute.udfs import ensure_json_udf
from arrowdsl.core.interop import ComputeExpression, DataTypeLike, ensure_expression, pc


@dataclass(frozen=True)
class PropValueExpr:
    expr: ComputeExpression
    value_type: PropValueType
    defer_json: bool = False
    json_dtype: DataTypeLike | None = None


def _json_value_expr(
    expr: ComputeExpression,
    *,
    dtype: DataTypeLike | None,
    ctx: ExecutionContext,
) -> PropValueExpr:
    if dtype is None or patypes.is_string(dtype) or patypes.is_large_string(dtype):
        return PropValueExpr(
            expr=ensure_expression(pc.cast(expr, pa.string(), safe=False)),
            value_type="json",
        )
    if patypes.is_list(dtype):
        list_type = cast("pa.ListType", dtype)
        target_dtype = dtype
        if not patypes.is_large_list(dtype):
            target_dtype = pa.large_list(list_type.value_type)
        coerced = ensure_expression(pc.cast(expr, target_dtype, safe=False))
        return PropValueExpr(
            expr=coerced,
            value_type="json",
            defer_json=True,
            json_dtype=target_dtype,
        )
    if patypes.is_struct(dtype):
        return PropValueExpr(
            expr=ensure_expression(expr),
            value_type="json",
            defer_json=True,
            json_dtype=dtype,
        )
    func_name = ensure_json_udf(dtype)
    serialized = ensure_expression(pc.call_function(func_name, [expr]))
    return PropValueExpr(expr=serialized, value_type="json")


def _expand_value_struct(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    defer_json: bool,
    json_dtype: DataTypeLike | None,
) -> Plan:
    value_struct = pc.field(VALUE_STRUCT_FIELD)
    value_json = ensure_expression(pc.struct_field(value_struct, "value_json"))
    if defer_json:
        value_json = _serialize_json_expr(value_json, dtype=json_dtype, ctx=ctx)
    ...
```

### Target files
- Update: `src/cpg/emit_props.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [x] Preserve list/struct values for heavy JSON props through the plan.
- [x] Serialize to JSON only at the final projection boundary.
- [x] Keep output schema unchanged (value_json remains string).

---

## Scope 10: Internal Prop Value Struct Staging

### Description
Stage property values in an internal struct column and route them to the five
value columns (`value_*`) in a single projection, reducing duplicated casting
logic.

### Code pattern
```python
# src/cpg/emit_props.py
from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc

VALUE_STRUCT_FIELD = "value_struct"


def _value_struct_expr(value_exprs: dict[str, ComputeExpression]) -> ComputeExpression:
    names = list(value_exprs.keys())
    exprs = [value_exprs[name] for name in names]
    return ensure_expression(pc.make_struct(*exprs, field_names=names))


def _expand_value_struct(...):
    value_struct = pc.field(VALUE_STRUCT_FIELD)
    value_json = ensure_expression(pc.struct_field(value_struct, "value_json"))
    ...
```

### Target files
- Update: `src/cpg/emit_props.py`
- Update: `src/arrowdsl/core/interop.py`

### Implementation checklist
- [x] Add an internal staging column for prop values.
- [x] Route staged values to canonical columns in a single projection.
- [x] Ensure schema alignment still matches CPG props contract.

---

## Scope 11: LargeList/ListView Tuning for Heavy Props (Optional)

### Description
Adopt `large_list` for list-valued properties to avoid 32-bit offset limits and
reduce copying, keeping final JSON string output stable.

### Code pattern
```python
# src/cpg/emit_props.py
if patypes.is_list(dtype):
    list_type = cast("pa.ListType", dtype)
    target_dtype = dtype
    if not patypes.is_large_list(dtype):
        target_dtype = pa.large_list(list_type.value_type)
    coerced = ensure_expression(pc.cast(expr, target_dtype, safe=False))
    return PropValueExpr(
        expr=coerced,
        value_type="json",
        defer_json=True,
        json_dtype=target_dtype,
    )
```

### Target files
- Update: `src/cpg/emit_props.py`
- Update: `src/arrowdsl/core/interop.py`

### Implementation checklist
- [x] Evaluate list sizes for `qnames`, `callee_qnames`, and `details`.
- [x] Use `large_list` for internal plan-lane staging where needed.
- [x] Keep final JSON string output stable.

---

## Scope 12: Schema Metadata Equality Checks (Optional Debug)

### Description
Provide a debug-mode assertion that verifies schema metadata is preserved after
alignment and finalize.

### Code pattern
```python
# src/cpg/plan_helpers.py
from typing import cast

import pyarrow as pa


def assert_schema_metadata(table: TableLike, *, schema: SchemaLike) -> None:
    table_schema = cast("pa.Schema", table.schema)
    expected_schema = cast("pa.Schema", schema)
    if not table_schema.equals(expected_schema, check_metadata=True):
        msg = "Schema metadata mismatch after finalize."
        raise ValueError(msg)
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [x] Add a metadata equality assertion helper.
- [x] Call it when `ctx.debug` is enabled.
- [x] Keep the check non-invasive for production runs.

---

## Scope 13: Pipeline-Breaker Aware Materialization Surfaces

### Description
Expose pipeline-breaker metadata and provide reader surfaces that are only
valid for fully streaming plans. Keep eager materialization confined to finalize
boundaries and surface pipeline-breaker diagnostics for debugging.

### Code pattern
```python
# src/cpg/plan_helpers.py
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike
from arrowdsl.plan.plan import Plan, PlanSpec


def plan_reader(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    return PlanSpec.from_plan(plan).to_reader(ctx=ctx)

# src/cpg/artifacts.py
from dataclasses import dataclass

from arrowdsl.core.interop import TableLike
from arrowdsl.finalize.finalize import FinalizeResult


@dataclass(frozen=True)
class CpgBuildArtifacts:
    finalize: FinalizeResult
    quality: TableLike
    pipeline_breakers: tuple[str, ...] = ()
```

### Target files
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`
- Update: `src/cpg/artifacts.py`

### Implementation checklist
- [x] Add reader/materialization helpers that honor `PlanSpec.pipeline_breakers`.
- [x] Keep materialization confined to finalize boundaries.
- [x] Surface pipeline-breaker metadata in build artifacts for debugging.

---

## Scope 14: QuerySpec Scan Integration + Ordering Options

### Description
When inputs are dataset-backed, drive scans via `DatasetSpec.scan_context` and
expose QuerySpec registries. Use scan ordering options to populate plan ordering
metadata when configured.

### Code pattern
```python
# src/cpg/sources.py
from dataclasses import dataclass

import pyarrow.dataset as ds

from schema_spec.system import DatasetSpec


@dataclass(frozen=True)
class DatasetSource:
    dataset: ds.Dataset
    spec: DatasetSpec

# src/cpg/query_specs.py
from arrowdsl.plan.query import QuerySpec
from cpg.schemas import CPG_EDGES_SPEC, CPG_NODES_SPEC, CPG_PROPS_SPEC

CPG_NODES_QUERY: QuerySpec = CPG_NODES_SPEC.query()
CPG_EDGES_QUERY: QuerySpec = CPG_EDGES_SPEC.query()
CPG_PROPS_QUERY: QuerySpec = CPG_PROPS_SPEC.query()

# src/cpg/plan_helpers.py
from arrowdsl.core.context import ExecutionContext, Ordering
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
    return Plan(
        decl=scan_ctx.acero_decl(),
        label=spec.name,
        ordering=ordering,
        pipeline_breakers=(),
    )
```

### Target files
- New: `src/cpg/query_specs.py`
- New: `src/cpg/sources.py`
- Update: `src/cpg/plan_helpers.py`
- Update: `src/cpg/catalog.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`
- Update: `src/cpg/builders.py`

### Implementation checklist
- [x] Define QuerySpec objects for dataset-backed CPG inputs.
- [x] Compile scan plans via `DatasetSpec.scan_context()` and centralized scan policy.
- [x] Honor scan ordering options in plan ordering metadata.

---

## Acceptance Criteria
- [x] Shared plan expression helpers replace duplicated CPG plan-lane logic.
- [x] Derived columns are registered as declarative plan sources.
- [x] Hash specs for CPG IDs live in a centralized registry.
- [x] Canonical ordering uses kernel-lane stable sort indices when required.
- [x] Encoding policy is driven by schema metadata.
- [x] CPG schemas carry stable metadata and preserve it through finalize.
- [x] Metadata-aware schema alignment is used where parts are merged.
- [x] Finalized outputs unify dictionary pools when configured.
- [x] Heavy JSON props serialize only at the final projection boundary.
- [x] Internal prop value staging reduces duplicate casting logic.
- [x] Pipeline-breaker metadata is surfaced and reader surfaces are guarded.
- [x] Dataset-backed scans use DatasetSpec scan contexts with ordering options.
- [x] Optional schema metadata equality checks are enforced under debug mode.
