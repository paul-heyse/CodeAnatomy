# Implementation Plan: Expanded PyArrow Protocols (Shared Compute + Data Operations)

This plan captures the next consolidated protocol layer for shared compute/data operations,
aligned with Acero ExecPlan semantics, Arrow ordering rules, and schema immutability. Each
scope item includes code patterns, target files, and a checklist.

---

## Scope 1: ColumnExpr + ColumnDefaultsSpec (Constants, Casts, Coalesce, Fill)

### Description
Standardize column materialization and replacement. This removes repeated inline
`pa.array([value] * n)` and ad-hoc `pc.fill_null` chains.

Status: Partial (ColumnDefaultsSpec is in `finalize`; broader rollout remains).

### Code patterns
```python
# src/arrowdsl/column_ops.py
from dataclasses import dataclass
from typing import Protocol

import arrowdsl.pyarrow_core as pa
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import ArrayLike, ComputeExpression, DataTypeLike, TableLike


class ColumnExpr(Protocol):
    def to_expression(self) -> ComputeExpression: ...
    def materialize(self, table: TableLike) -> ArrayLike: ...


@dataclass(frozen=True)
class NullFillExpr:
    expr: ColumnExpr
    fill_value: object
    dtype: DataTypeLike | None = None

    def to_expression(self) -> ComputeExpression:
        fill = pa.scalar(self.fill_value) if self.dtype is None else pa.scalar(
            self.fill_value, type=self.dtype
        )
        return pc.fill_null(self.expr.to_expression(), fill_value=fill)

    def materialize(self, table: TableLike) -> ArrayLike:
        fill = pa.scalar(self.fill_value) if self.dtype is None else pa.scalar(
            self.fill_value, type=self.dtype
        )
        return pc.fill_null(self.expr.materialize(table), fill_value=fill)


@dataclass(frozen=True)
class ColumnDefaultsSpec:
    defaults: tuple[tuple[str, ColumnExpr], ...]

    def apply(self, table: TableLike) -> TableLike:
        out = table
        for name, expr in self.defaults:
            if name in out.column_names:
                continue
            out = out.append_column(name, expr.materialize(out))
        return out
```

### Target files
- `src/arrowdsl/column_ops.py`
- `src/arrowdsl/finalize.py`
- `src/normalize/spans.py`
- `src/normalize/bytecode_anchor.py`
- `src/normalize/bytecode_dfg.py`
- `src/cpg/build_nodes.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_props.py`
- `src/relspec/compiler.py`

### Integration checklist
- [x] Add `NullFillExpr` and `ColumnDefaultsSpec` for shared column defaults.
- [ ] Replace inline `pa.array([value] * n)` patterns with `ColumnDefaultsSpec`.
- [ ] Normalize fill behavior through `NullFillExpr` or shared helpers.

---

## Scope 2: PredicateSpec + FilterSpec (Pushdown + Kernel Mask)

### Description
Unify filter logic so plan lane and kernel lane share the same predicate specs.

Status: Partial (QuerySpec/dataset_io wired; kernel-lane filters still direct).

### Code patterns
```python
# src/arrowdsl/predicates.py
from dataclasses import dataclass
from typing import Protocol

from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import ArrayLike, ComputeExpression, TableLike


class PredicateSpec(Protocol):
    def to_expression(self) -> ComputeExpression: ...
    def mask(self, table: TableLike) -> ArrayLike: ...


@dataclass(frozen=True)
class FilterSpec:
    predicate: PredicateSpec

    def apply_plan(self, plan) -> object:
        return plan.filter(self.predicate.to_expression())

    def apply_kernel(self, table: TableLike) -> TableLike:
        return table.filter(self.predicate.mask(table))
```

### Target files
- `src/arrowdsl/predicates.py`
- `src/arrowdsl/queryspec.py`
- `src/arrowdsl/plan.py`
- `src/arrowdsl/dataset_io.py`
- `src/relspec/compiler.py`

### Integration checklist
- [x] Introduce `FilterSpec` and route QuerySpec/dataset_io filters through it.
- [x] Keep plan-lane filters as pushdown expressions.
- [ ] Keep kernel-lane filters as masks.

---

## Scope 3: JoinSpec + OrderingEffect (Join Consolidation)

### Description
Standardize joins (plan lane and kernel lane) and always mark join output as unordered,
per Acero ordering rules.

Status: Complete.

### Code patterns
```python
# src/arrowdsl/specs.py
from dataclasses import dataclass


@dataclass(frozen=True)
class JoinSpec:
    join_type: str
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""
```

```python
# src/arrowdsl/joins.py
from arrowdsl.plan import Plan
from arrowdsl.runtime import Ordering
from arrowdsl.specs import JoinSpec


def hash_join(*, left: Plan, right: Plan, spec: JoinSpec, label: str = "") -> Plan:
    # build hashjoin declaration, return Plan with Ordering.unordered()
    ...
```

### Target files
- `src/arrowdsl/specs.py`
- `src/arrowdsl/joins.py`
- `src/normalize/bytecode_cfg.py`
- `src/cpg/build_edges.py`

### Integration checklist
- [x] Route join usage through `JoinSpec`.
- [x] Ensure plan-lane joins return `Ordering.unordered()`.

---

## Scope 4: AggregateSpec + AggregateNamingPolicy

### Description
Unify aggregation patterns and naming (suffix handling and stable output names).

Status: Partial (shared helper added; call sites still limited).

### Code patterns
```python
# src/arrowdsl/specs.py
from dataclasses import dataclass


@dataclass(frozen=True)
class AggregateSpec:
    keys: tuple[str, ...]
    aggs: tuple[tuple[str, str], ...]
    use_threads: bool = True
    rename_aggregates: bool = False
```

```python
# src/arrowdsl/kernels.py
def apply_aggregate(table: TableLike, *, spec: AggregateSpec) -> TableLike:
    out = table.group_by(list(spec.keys), use_threads=spec.use_threads).aggregate(spec.aggs)
    if not spec.rename_aggregates:
        return out
    rename_map = {f"{col}_{agg}": col for col, agg in spec.aggs if col not in spec.keys}
    names = [rename_map.get(name, name) for name in out.column_names]
    return out.rename_columns(names)
```

### Target files
- `src/arrowdsl/specs.py`
- `src/arrowdsl/kernels.py`
- `src/arrowdsl/plan_ops.py`
- `src/arrowdsl/finalize.py`
- `src/cpg/build_props.py`

### Integration checklist
- [x] Add a shared `AggregateSpec` with naming controls.
- [ ] Route aggregations through the shared helper.
- [ ] Ensure ordering is marked unordered after aggregation.

---

## Scope 5: DedupeSpec + CanonicalSortSpec (Finalize-Only)

### Description
Make dedupe and canonical sort a finalize-only policy. Tie-breakers must be explicit.

Status: Complete.

### Code patterns
```python
# src/arrowdsl/kernels.py
def canonical_sort(table: TableLike, *, sort_keys: Sequence[SortKey]) -> TableLike:
    idx = pc.sort_indices(table, sort_keys=[(k.column, k.order) for k in sort_keys])
    return table.take(idx)
```

### Target files
- `src/arrowdsl/specs.py`
- `src/arrowdsl/kernels.py`
- `src/arrowdsl/finalize.py`
- `src/schema_spec/contracts.py`

### Integration checklist
- [x] Keep canonical sort only in finalize path.
- [x] Ensure tie-breakers create a total order.
- [x] Ensure dedupe uses canonical sort when deterministic tier is canonical.

---

## Scope 6: SchemaTransform + SchemaMetadataSpec

### Description
Centralize schema alignment and metadata mutation using immutable schema/field updates.

Status: Partial (SchemaTransform adopted; metadata spec still to define).

### Code patterns
```python
# src/arrowdsl/schema_ops.py
from dataclasses import dataclass

from arrowdsl.schema import align_to_schema
from arrowdsl.pyarrow_protocols import SchemaLike, TableLike


@dataclass(frozen=True)
class SchemaTransform:
    schema: SchemaLike
    safe_cast: bool = True
    keep_extra_columns: bool = False
    on_error: str = "unsafe"

    def apply(self, table: TableLike) -> TableLike:
        aligned, _ = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            on_error=self.on_error,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned
```

### Target files
- `src/arrowdsl/schema_ops.py`
- `src/arrowdsl/schema.py`
- `src/schema_spec/core.py`
- `src/schema_spec/pandera_adapter.py`
- `src/arrowdsl/finalize.py`

### Integration checklist
- [x] Route all alignment through `SchemaTransform`.
- [ ] Centralize metadata mutations (new schema + cast).

---

## Scope 7: EncodingSpec + EncodingPolicy (Dictionary Encoding)

### Description
Define a schema-driven dictionary encoding policy and apply it consistently.

Status: Partial (encoding spec used in diagnostics/edges/nodes/stats/scip).

### Code patterns
```python
# src/arrowdsl/encoding.py
from dataclasses import dataclass

import pyarrow.types as patypes
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import TableLike


@dataclass(frozen=True)
class EncodingSpec:
    column: str


def encode_dictionary(table: TableLike, spec: EncodingSpec) -> TableLike:
    if spec.column not in table.column_names:
        return table
    col = table[spec.column]
    if patypes.is_dictionary(col.type):
        return table
    return table.set_column(
        table.schema.get_field_index(spec.column),
        spec.column,
        pc.dictionary_encode(col),
    )
```

### Target files
- `src/arrowdsl/encoding.py`
- `src/extract/scip_extract.py`
- `src/normalize/diagnostics.py`
- `src/cpg/builders.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_nodes.py`
- `src/cpg/schemas.py`
- `src/obs/stats.py`

### Integration checklist
- [ ] Replace bespoke dictionary encoding with `EncodingSpec`.
- [x] Add schema-level encoding policies for core tables.

---

## Scope 8: NestedArraySpec (struct/list/list_view/map/union)

### Description
Standardize nested array creation with list, list_view, struct, map, and union builders.

Status: Complete.

### Code patterns
```python
# src/arrowdsl/nested_ops.py
import pyarrow as pa
from arrowdsl.pyarrow_protocols import ArrayLike, ListArrayLike, StructArrayLike


def build_struct(fields: dict[str, ArrayLike]) -> StructArrayLike:
    names = list(fields.keys())
    arrays = [fields[name] for name in names]
    return pa.StructArray.from_arrays(arrays, names=names)


def build_list(offsets: ArrayLike, values: ArrayLike) -> ListArrayLike:
    return pa.ListArray.from_arrays(offsets, values)


def build_list_view(offsets: ArrayLike, sizes: ArrayLike, values: ArrayLike) -> ListArrayLike:
    return pa.ListViewArray.from_arrays(offsets, sizes, values)
```

### Target files
- `src/arrowdsl/nested_ops.py`
- `src/arrowdsl/nested.py`
- `src/arrowdsl/finalize.py`
- `src/normalize/diagnostics.py`

### Integration checklist
- [x] Add map/union builders as needed for complex nested fields.
- [x] Route list<struct> creation through shared builders.

---

## Scope 9: ErrorArtifactSpec (Errors, Stats, Alignment)

### Description
Normalize error table schemas and stats aggregation for finalize and diagnostics.

Status: Partial (finalize uses ErrorArtifactSpec; other layers pending).

### Code patterns
```python
# src/arrowdsl/finalize.py
from dataclasses import dataclass
from arrowdsl.pyarrow_protocols import TableLike


@dataclass(frozen=True)
class ErrorArtifactSpec:
    detail_field_names: tuple[str, ...]
    detail_field_types: tuple[object, ...]

    def build_error_table(self, table: TableLike) -> TableLike:
        ...
```

### Target files
- `src/arrowdsl/finalize.py`
- `src/normalize/diagnostics.py`
- `src/obs/stats.py`

### Integration checklist
- [ ] Use a single error detail struct layout across components.
- [ ] Aggregate error stats via shared helper.

---

## Scope 10: HashSpec + SpanSpec (IDs + Span Ops)

### Description
Keep ID generation and span columns fully vectorized and shared.

Status: Partial (HashSpec used in repo_scan/finalize; more call sites pending).

### Code patterns
```python
# src/arrowdsl/id_specs.py
from dataclasses import dataclass


@dataclass(frozen=True)
class HashSpec:
    prefix: str
    cols: tuple[str, ...]
    as_string: bool = True
    null_sentinel: str = "None"
```

```python
# src/normalize/ids.py
@dataclass(frozen=True)
class SpanIdSpec:
    path_col: str = "path"
    bstart_col: str = "bstart"
    bend_col: str = "bend"
    kind: str | None = None
    out_col: str = "span_id"
```

### Target files
- `src/arrowdsl/ids.py`
- `src/arrowdsl/id_specs.py`
- `src/normalize/ids.py`
- `src/normalize/spans.py`
- `src/extract/*.py`
- `src/cpg/builders.py`

### Integration checklist
- [ ] Route all hash IDs through `HashSpec`.
- [x] Centralize span_id generation with `SpanIdSpec`.

---

## Scope 11: PlanOp/KernelOp + ExecutionProfile (Ordering, Pipeline Breakers)

### Description
Make ordering and pipeline breaker metadata explicit on every plan/kernel op and tie
execution knobs to `ExecutionContext`.

Status: Partial (ExecutionProfile added; plan/kernel op wiring pending).

### Code patterns
```python
# src/arrowdsl/runtime.py
from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionProfile:
    use_threads: bool = True
    scan_implicit_ordering: bool = False
    scan_require_sequenced_output: bool = False
```

```python
# src/arrowdsl/plan_ops.py
class ScanOp:
    def apply_ordering(self, ordering: Ordering) -> Ordering:
        if ordering.level == Ordering.unordered().level:
            return Ordering.implicit()
        return ordering
```

### Target files
- `src/arrowdsl/runtime.py`
- `src/arrowdsl/ops.py`
- `src/arrowdsl/plan_ops.py`
- `src/arrowdsl/plan.py`
- `src/arrowdsl/dataset_io.py`

### Integration checklist
- [ ] Add explicit ordering effects to all plan ops.
- [ ] Record pipeline breakers (order_by, aggregate).
- [ ] Surface scan ordering knobs in `ExecutionContext`.
