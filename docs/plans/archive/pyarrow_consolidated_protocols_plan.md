# Implementation Plan: Consolidated PyArrow Protocols for Shared Compute + Data Operations

This plan consolidates repeated Arrow compute/data patterns into shared PyArrow-based
protocols and helpers, aligned with Acero’s ExecPlan model (plan lane vs kernel lane,
streaming boundaries, and ordering/pipeline-breaker semantics) and PyArrow’s advanced
type system (nested layouts, schema evolution, metadata). Each scope item includes
patterns, targets, and a checklist.

---

## Progress Summary
- Scopes 1-9 completed in code.
- Scope 10 pending: pytest still fails because `EDGE_DERIVATIONS` is missing for the SCIP symbol
  relationship edges (see Scope 10).

---

## Scope 1: Column Expression Protocol (Shared Column Ops)

### Description
Unify repeated column operations (add/replace columns, constants, coalesce, cast, null
fill) into a single protocol-driven layer. This removes duplicated `_set_or_append_column`
and ad‑hoc column materialization across builders and normalization.

### Code patterns
```python
# src/arrowdsl/column_ops.py
from dataclasses import dataclass
from typing import Protocol

import arrowdsl.pyarrow_core as pa
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import (
    ArrayLike,
    ComputeExpression,
    DataTypeLike,
    TableLike,
    ensure_expression,
)


class ColumnExpr(Protocol):
    def to_expression(self) -> ComputeExpression: ...
    def materialize(self, table: TableLike) -> ArrayLike: ...


@dataclass(frozen=True)
class ConstExpr:
    value: object
    dtype: DataTypeLike | None = None

    def to_expression(self) -> ComputeExpression:
        scalar = self.value if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        return ensure_expression(pc.scalar(scalar))

    def materialize(self, table: TableLike) -> ArrayLike:
        scalar = (
            pa.scalar(self.value) if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        )
        return pa.array([self.value] * table.num_rows, type=scalar.type)


@dataclass(frozen=True)
class FieldExpr:
    name: str

    def to_expression(self) -> ComputeExpression:
        return pc.field(self.name)

    def materialize(self, table: TableLike) -> ArrayLike:
        return table[self.name]


@dataclass(frozen=True)
class CastExpr:
    expr: ColumnExpr
    dtype: DataTypeLike
    safe: bool = True

    def to_expression(self) -> ComputeExpression:
        return ensure_expression(pc.cast(self.expr.to_expression(), self.dtype, safe=self.safe))

    def materialize(self, table: TableLike) -> ArrayLike:
        return pc.cast(self.expr.materialize(table), self.dtype, safe=self.safe)


@dataclass(frozen=True)
class CoalesceExpr:
    exprs: tuple[ColumnExpr, ...]

    def to_expression(self) -> ComputeExpression:
        return ensure_expression(pc.coalesce(*(expr.to_expression() for expr in self.exprs)))

    def materialize(self, table: TableLike) -> ArrayLike:
        out = self.exprs[0].materialize(table)
        for expr in self.exprs[1:]:
            out = pc.coalesce(out, expr.materialize(table))
        return out


def const_array(n: int, value: object, *, dtype: DataTypeLike | None = None) -> ArrayLike:
    scalar = pa.scalar(value) if dtype is None else pa.scalar(value, type=dtype)
    return pa.array([value] * n, type=scalar.type)


def set_or_append_column(table: TableLike, name: str, values: ArrayLike) -> TableLike:
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)
```

### Target files
- `src/arrowdsl/column_ops.py` (new)
- `src/arrowdsl/columns.py` (wrap/redirect to column_ops)
- `src/cpg/build_edges.py`
- `src/cpg/build_nodes.py`
- `src/cpg/build_props.py`
- `src/normalize/spans.py`
- `src/normalize/bytecode_anchor.py`
- `src/normalize/bytecode_dfg.py`
- `src/normalize/diagnostics.py`
- `src/relspec/compiler.py`

### Integration checklist
- [x] Add `ColumnExpr` + concrete expressions (const, field, coalesce, cast).
- [x] Replace `_set_or_append_column` helpers with `set_or_append_column`.
- [x] Standardize constant column creation and null fill.
- [x] Keep runtime behavior identical; remove redundant helpers.

---

## Scope 2: Predicate Protocol (Pushdown + Kernel Mask)

### Description
Provide a predicate protocol that can compile into an Acero filter expression for plan
lane or materialize a boolean mask in kernel lane. This centralizes filter logic and
keeps pushdown explicit.

### Code patterns
```python
# src/arrowdsl/predicates.py
from dataclasses import dataclass
from typing import Protocol

from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import ArrayLike, ComputeExpression, TableLike, ensure_expression


class PredicateSpec(Protocol):
    def to_expression(self) -> ComputeExpression: ...
    def mask(self, table: TableLike) -> ArrayLike: ...


@dataclass(frozen=True)
class Equals:
    col: str
    value: object

    def to_expression(self) -> ComputeExpression:
        return ensure_expression(pc.equal(pc.field(self.col), pc.scalar(self.value)))

    def mask(self, table: TableLike) -> ArrayLike:
        return pc.equal(table[self.col], pc.scalar(self.value))


@dataclass(frozen=True)
class InSet:
    col: str
    values: tuple[object, ...]

    def to_expression(self) -> ComputeExpression:
        return ensure_expression(pc.is_in(pc.field(self.col), value_set=list(self.values)))

    def mask(self, table: TableLike) -> ArrayLike:
        return pc.is_in(table[self.col], value_set=list(self.values))


@dataclass(frozen=True)
class IsNull:
    col: str

    def to_expression(self) -> ComputeExpression:
        return ensure_expression(pc.is_null(pc.field(self.col)))

    def mask(self, table: TableLike) -> ArrayLike:
        return pc.is_null(table[self.col])


@dataclass(frozen=True)
class And:
    preds: tuple[PredicateSpec, ...]

    def to_expression(self) -> ComputeExpression:
        out = self.preds[0].to_expression()
        for pred in self.preds[1:]:
            out = ensure_expression(pc.and_(out, pred.to_expression()))
        return out

    def mask(self, table: TableLike) -> ArrayLike:
        out = self.preds[0].mask(table)
        for pred in self.preds[1:]:
            out = pc.and_(out, pred.mask(table))
        return out


@dataclass(frozen=True)
class Not:
    pred: PredicateSpec

    def to_expression(self) -> ComputeExpression:
        return ensure_expression(pc.invert(self.pred.to_expression()))

    def mask(self, table: TableLike) -> ArrayLike:
        return pc.invert(self.pred.mask(table))
```

### Target files
- `src/arrowdsl/predicates.py` (new)
- `src/arrowdsl/queryspec.py`
- `src/arrowdsl/plan.py` (filter node construction)
- `src/arrowdsl/dataset_io.py`

### Integration checklist
- [x] Introduce `PredicateSpec` and a small set of predicates (equals, in, is_null, and/or/not).
- [x] Update QuerySpec to accept predicate objects and expose both expression and mask.
- [x] Ensure plan-lane filters use `to_expression()` and kernel lane uses `mask()`.

---

## Scope 3: PlanOp / KernelOp Protocols (ExecPlan‑Aware Ops)

### Description
Formalize the “plan lane vs kernel lane” boundary. Plan ops compile to Acero
Declarations and declare ordering/pipeline breaker semantics; kernel ops are pure
table transforms with explicit ordering effects.

### Code patterns
```python
# src/arrowdsl/ops.py
from typing import Protocol

from arrowdsl.pyarrow_protocols import DeclarationLike, TableLike
from arrowdsl.runtime import ExecutionContext, Ordering, OrderingEffect


class PlanOp(Protocol):
    @property
    def ordering_effect(self) -> OrderingEffect: ...
    @property
    def is_pipeline_breaker(self) -> bool: ...
    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike: ...
    def apply_ordering(self, ordering: Ordering) -> Ordering: ...


class KernelOp(Protocol):
    @property
    def ordering_effect(self) -> OrderingEffect: ...
    def apply(self, table: TableLike, ctx: ExecutionContext) -> TableLike: ...
    def apply_ordering(self, ordering: Ordering) -> Ordering: ...
```

### Target files
- `src/arrowdsl/ops.py` (new)
- `src/arrowdsl/plan.py` (pipeline assembly)
- `src/arrowdsl/runtime.py` (ordering + breaker metadata)
- `src/relspec/compiler.py` (mixed plan + kernel execution)

### Integration checklist
- [x] Define `PlanOp` and `KernelOp` protocols with ordering metadata.
- [x] Update plan assembly to carry ordering effects.
- [x] Annotate pipeline breakers (order_by, aggregate) and preserve in execution metadata.

---

## Scope 4: JoinSpec / AggregateSpec / DedupeSpec Protocols

### Description
Centralize join and group_by semantics and tie them to determinism policy. The same
specs can be used both in Acero plan lane and kernel lane.

### Code patterns
```python
# src/arrowdsl/specs.py
from dataclasses import dataclass
from typing import Literal

type DedupeStrategy = Literal[
    "KEEP_FIRST_AFTER_SORT",
    "KEEP_BEST_BY_SCORE",
    "COLLAPSE_LIST",
    "KEEP_ARBITRARY",
]


@dataclass(frozen=True)
class SortKey:
    column: str
    order: Literal["ascending", "descending"] = "ascending"


@dataclass(frozen=True)
class JoinSpec:
    join_type: str
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
    output_suffix_for_left: str = ""
    output_suffix_for_right: str = ""


@dataclass(frozen=True)
class AggregateSpec:
    keys: tuple[str, ...]
    aggs: tuple[tuple[str, str], ...]
    use_threads: bool = True


@dataclass(frozen=True)
class DedupeSpec:
    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...] = ()
    strategy: DedupeStrategy = "KEEP_FIRST_AFTER_SORT"
```

### Target files
- `src/arrowdsl/specs.py`
- `src/arrowdsl/joins.py`
- `src/arrowdsl/kernels.py`
- `src/arrowdsl/finalize.py`
- `src/relspec/compiler.py`
- `src/cpg/build_edges.py`

### Integration checklist
- [x] Add shared specs for joins/aggregates/dedupe.
- [x] Route kernel lane dedupe through `DedupeSpec`.
- [x] Ensure canonical sort is enforced only at finalize (Tier A).

---

## Scope 5: IdSpec / HashSpec Protocol (Hash64 Consolidation)

### Description
Centralize the repeated hash64 + prefix + string formatting pattern so all ID generation
is consistent and vectorized, with a single config surface.

### Code patterns
```python
# src/arrowdsl/id_specs.py
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class HashSpec:
    prefix: str
    cols: tuple[str, ...]
    extra_literals: tuple[str, ...] = ()
    as_string: bool = True
    null_sentinel: str = "None"
    out_col: str | None = None
    missing: Literal["raise", "null"] = "raise"
```

```python
# src/arrowdsl/ids.py
from arrowdsl.id_specs import HashSpec

def add_hash_column(table: TableLike, *, spec: HashSpec) -> TableLike:
    out_col = spec.out_col or f"{spec.prefix}_id"
    if out_col in table.column_names:
        return table
    hashed = hash_column_values(table, spec=spec)
    return table.append_column(out_col, hashed)
```

### Target files
- `src/arrowdsl/id_specs.py`
- `src/arrowdsl/ids.py`
- `src/normalize/ids.py`
- `src/normalize/diagnostics.py`
- `src/normalize/types.py`
- `src/normalize/bytecode_dfg.py`
- `src/extract/repo_scan.py`
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/runtime_inspect_extract.py`
- `src/cpg/builders.py`

### Integration checklist
- [x] Introduce `HashSpec` and a single `add_hash_column` API.
- [x] Replace all inline hash64 + prefix patterns with the shared API.

---

## Scope 6: Nested Builders (list<struct>, struct, list_view)

### Description
Centralize nested array creation using PyArrow’s advanced types and buffer‑based
constructors, enabling list<struct> and list_view semantics without ad‑hoc code.

### Code patterns
```python
# src/arrowdsl/nested_ops.py
import pyarrow as pa

from arrowdsl.pyarrow_protocols import ArrayLike, DataTypeLike, ListArrayLike, StructArrayLike


def build_struct(fields: dict[str, ArrayLike], *, mask: ArrayLike | None = None) -> StructArrayLike:
    names = list(fields.keys())
    arrays = [fields[name] for name in names]
    return pa.StructArray.from_arrays(arrays, names=names, mask=mask)


def build_list(offsets: ArrayLike, values: ArrayLike) -> ListArrayLike:
    return pa.ListArray.from_arrays(offsets, values)


def build_list_view(
    offsets: ArrayLike,
    sizes: ArrayLike,
    values: ArrayLike,
    *,
    list_type: DataTypeLike | None = None,
    mask: ArrayLike | None = None,
) -> ListArrayLike:
    return pa.ListViewArray.from_arrays(
        offsets,
        sizes,
        values,
        type=list_type,
        mask=mask,
    )
```

### Target files
- `src/arrowdsl/nested_ops.py`
- `src/arrowdsl/nested.py` (migrate to nested_ops)
- `src/arrowdsl/finalize.py`
- `src/normalize/diagnostics.py`

### Integration checklist
- [x] Add shared nested builders.
- [x] Replace list<struct> construction in finalize and diagnostics.
- [x] Add list_view support when non‑monotonic offsets are required.

---

## Scope 7: Schema Transform Protocol (Metadata + Unify/Cast)

### Description
Standardize schema transformations and metadata mutation using immutable Schema and
Field semantics from PyArrow advanced docs.

### Code patterns
```python
# src/arrowdsl/schema_ops.py
from dataclasses import dataclass
from typing import Literal

from arrowdsl.schema import AlignmentInfo, align_to_schema
from arrowdsl.pyarrow_protocols import SchemaLike, TableLike


@dataclass(frozen=True)
class SchemaTransform:
    schema: SchemaLike
    safe_cast: bool = True
    keep_extra_columns: bool = False
    on_error: Literal["unsafe", "null", "drop"] = "unsafe"

    def apply(self, table: TableLike) -> TableLike:
        aligned, _ = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            on_error=self.on_error,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned

    def apply_with_info(self, table: TableLike) -> tuple[TableLike, AlignmentInfo]:
        aligned, info = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            on_error=self.on_error,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned, info
```

### Target files
- `src/arrowdsl/schema_ops.py`
- `src/arrowdsl/schema.py`
- `src/normalize/schema_infer.py`
- `src/arrowdsl/finalize.py`
- `src/schema_spec/core.py`
- `src/schema_spec/pandera_adapter.py`

### Integration checklist
- [x] Introduce `SchemaTransform` and route alignment through it.
- [x] Enforce immutable metadata update patterns (new schema + cast).
- [x] Reuse in finalize + schema inference paths.

---

## Scope 8: EncodingSpec Protocol (Dictionary Encoding)

### Description
Add a standard dictionary‑encoding spec for categorical string columns to reduce memory
and stabilize joins/aggregations.

### Code patterns
```python
# src/arrowdsl/encoding.py
from dataclasses import dataclass

import pyarrow.types as patypes

from arrowdsl.column_ops import set_or_append_column
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import TableLike


@dataclass(frozen=True)
class EncodingSpec:
    column: str


def encode_dictionary(table: TableLike, spec: EncodingSpec) -> TableLike:
    if spec.column not in table.column_names:
        return table
    column = table[spec.column]
    if patypes.is_dictionary(column.type):
        return table
    encoded = pc.dictionary_encode(column)
    return set_or_append_column(table, spec.column, encoded)
```

### Target files
- `src/arrowdsl/encoding.py` (new)
- `src/normalize/diagnostics.py`
- `src/cpg/builders.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_nodes.py`
- `src/cpg/schemas.py`
- `src/obs/stats.py`

### Integration checklist
- [x] Add encoding helpers and use them for high‑cardinality string columns.
- [x] Ensure downstream operations handle dictionary types correctly.

---

## Scope 9: Plan‑Lane Helpers for Streaming + Ordering Metadata

### Description
Provide shared plan helpers for scan/filter/project and record pipeline breaker metadata
based on Acero guidance. Preserve ordering/implicit ordering settings through `ExecutionContext`.

### Code patterns
```python
# src/arrowdsl/plan_ops.py
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow.dataset as ds

from arrowdsl.acero import acero
from arrowdsl.pyarrow_protocols import ComputeExpression, DeclarationLike
from arrowdsl.runtime import ExecutionContext, Ordering, OrderingEffect


@dataclass(frozen=True)
class ScanOp:
    dataset: ds.Dataset
    columns: Sequence[str] | Mapping[str, ComputeExpression]
    predicate: ComputeExpression | None = None

    ordering_effect: OrderingEffect = OrderingEffect.IMPLICIT
    is_pipeline_breaker: bool = False

    def to_declaration(
        self, inputs: list[DeclarationLike], ctx: ExecutionContext | None
    ) -> DeclarationLike:
        _ = inputs
        if ctx is None:
            msg = "ScanOp requires an execution context."
            raise ValueError(msg)
        opts = acero.ScanNodeOptions(
            self.dataset,
            columns=self.columns,
            filter=self.predicate,
            **ctx.runtime.scan.scan_node_kwargs(),
        )
        return acero.Declaration("scan", opts)

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        if self.ordering_effect == OrderingEffect.IMPLICIT:
            if ordering.level == Ordering.unordered().level:
                return Ordering.implicit()
            return ordering
        return ordering
```

### Target files
- `src/arrowdsl/plan_ops.py`
- `src/arrowdsl/dataset_io.py`
- `src/arrowdsl/plan.py`
- `src/arrowdsl/runtime.py`

### Integration checklist
- [x] Add plan‑lane ops (scan, filter, project, aggregate, order_by).
- [x] Annotate ordering effects and pipeline breakers.
- [x] Ensure `ExecutionContext` exposes Acero ordering knobs.

---

## Scope 10: Verification + Migration Cleanup

### Description
Ensure all migrations compile cleanly and remove any legacy helper duplication.

### Target files
- Entire repo

### Integration checklist
- [x] Remove obsolete helpers (`_set_or_append_column`, ad‑hoc coalesce/const logic).
- [x] Ensure no duplicated hash ID patterns remain.
- [x] `uv run ruff check --fix`
- [x] `uv run pyrefly check`
- [x] `uv run pyright --warnings --pythonversion=3.14`
- [ ] Fix registry completeness: add `EDGE_DERIVATIONS` entries for SCIP symbol relationship edges
  in `src/cpg/kinds_ultimate.py`.
- [ ] `uv run pytest` (blocked until registry completeness is resolved).
