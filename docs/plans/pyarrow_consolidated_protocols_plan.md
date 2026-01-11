# Implementation Plan: Consolidated PyArrow Protocols for Shared Compute + Data Operations

This plan consolidates repeated Arrow compute/data patterns into shared PyArrow-based
protocols and helpers, aligned with Acero’s ExecPlan model (plan lane vs kernel lane,
streaming boundaries, and ordering/pipeline-breaker semantics) and PyArrow’s advanced
type system (nested layouts, schema evolution, metadata). Each scope item includes
patterns, targets, and a checklist.

---

## Scope 1: Column Expression Protocol (Shared Column Ops)

### Description
Unify repeated column operations (add/replace columns, constants, coalesce, cast, null
fill) into a single protocol-driven layer. This removes duplicated `_set_or_append_column`
and ad‑hoc column materialization across builders and normalization.

### Code patterns
```python
# src/arrowdsl/column_ops.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import arrowdsl.pyarrow_core as pa
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import ArrayLike, ComputeExpression, DataTypeLike, TableLike


class ColumnExpr(Protocol):
    def to_expression(self) -> ComputeExpression: ...
    def materialize(self, table: TableLike) -> ArrayLike: ...


@dataclass(frozen=True)
class ConstExpr:
    value: object
    dtype: DataTypeLike | None = None

    def to_expression(self) -> ComputeExpression:
        return pc.scalar(self.value if self.dtype is None else pa.scalar(self.value, type=self.dtype))

    def materialize(self, table: TableLike) -> ArrayLike:
        scalar = pa.scalar(self.value) if self.dtype is None else pa.scalar(self.value, type=self.dtype)
        return pa.array([self.value] * table.num_rows, type=scalar.type)


@dataclass(frozen=True)
class FieldExpr:
    name: str

    def to_expression(self) -> ComputeExpression:
        return pc.field(self.name)

    def materialize(self, table: TableLike) -> ArrayLike:
        return table[self.name]


@dataclass(frozen=True)
class CoalesceExpr:
    exprs: tuple[ColumnExpr, ...]

    def to_expression(self) -> ComputeExpression:
        return pc.coalesce(*(expr.to_expression() for expr in self.exprs))

    def materialize(self, table: TableLike) -> ArrayLike:
        arrays = [expr.materialize(table) for expr in self.exprs]
        out = arrays[0]
        for arr in arrays[1:]:
            out = pc.coalesce(out, arr)
        return out


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
- [ ] Add `ColumnExpr` + concrete expressions (const, field, coalesce, cast).
- [ ] Replace `_set_or_append_column` helpers with `set_or_append_column`.
- [ ] Standardize constant column creation and null fill.
- [ ] Keep runtime behavior identical; remove redundant helpers.

---

## Scope 2: Predicate Protocol (Pushdown + Kernel Mask)

### Description
Provide a predicate protocol that can compile into an Acero filter expression for plan
lane or materialize a boolean mask in kernel lane. This centralizes filter logic and
keeps pushdown explicit.

### Code patterns
```python
# src/arrowdsl/predicates.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import ArrayLike, ComputeExpression, TableLike


class PredicateSpec(Protocol):
    def to_expression(self) -> ComputeExpression: ...
    def mask(self, table: TableLike) -> ArrayLike: ...


@dataclass(frozen=True)
class Equals:
    col: str
    value: object

    def to_expression(self) -> ComputeExpression:
        return pc.equal(pc.field(self.col), pc.scalar(self.value))

    def mask(self, table: TableLike) -> ArrayLike:
        return pc.equal(table[self.col], pc.scalar(self.value))
```

### Target files
- `src/arrowdsl/predicates.py` (new)
- `src/arrowdsl/queryspec.py`
- `src/arrowdsl/plan.py` (filter node construction)
- `src/arrowdsl/dataset_io.py`

### Integration checklist
- [ ] Introduce `PredicateSpec` and a small set of predicates (equals, in, is_null, and/or/not).
- [ ] Update QuerySpec to accept predicate objects and expose both expression and mask.
- [ ] Ensure plan-lane filters use `to_expression()` and kernel lane uses `mask()`.

---

## Scope 3: PlanOp / KernelOp Protocols (ExecPlan‑Aware Ops)

### Description
Formalize the “plan lane vs kernel lane” boundary. Plan ops compile to Acero
Declarations and declare ordering/pipeline breaker semantics; kernel ops are pure
table transforms with explicit ordering effects.

### Code patterns
```python
# src/arrowdsl/ops.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from arrowdsl.pyarrow_protocols import DeclarationLike, TableLike
from arrowdsl.runtime import ExecutionContext, OrderingEffect


class PlanOp(Protocol):
    ordering_effect: OrderingEffect
    is_pipeline_breaker: bool
    def to_declaration(self, inputs: list[DeclarationLike], ctx: ExecutionContext) -> DeclarationLike: ...


class KernelOp(Protocol):
    ordering_effect: OrderingEffect
    def apply(self, table: TableLike, ctx: ExecutionContext) -> TableLike: ...
```

### Target files
- `src/arrowdsl/ops.py` (new)
- `src/arrowdsl/plan.py` (pipeline assembly)
- `src/arrowdsl/runtime.py` (ordering + breaker metadata)
- `src/relspec/compiler.py` (mixed plan + kernel execution)

### Integration checklist
- [ ] Define `PlanOp` and `KernelOp` protocols with ordering metadata.
- [ ] Update plan assembly to carry ordering effects.
- [ ] Annotate pipeline breakers (order_by, aggregate) and preserve in execution metadata.

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

from arrowdsl.runtime import SortKey


@dataclass(frozen=True)
class JoinSpec:
    join_type: str
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]


@dataclass(frozen=True)
class AggregateSpec:
    keys: tuple[str, ...]
    aggs: tuple[tuple[str, str], ...]
    use_threads: bool = True


@dataclass(frozen=True)
class DedupeSpec:
    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...]
    strategy: Literal[
        "KEEP_FIRST_AFTER_SORT",
        "KEEP_BEST_BY_SCORE",
        "COLLAPSE_LIST",
        "KEEP_ARBITRARY",
    ]
```

### Target files
- `src/arrowdsl/joins.py`
- `src/arrowdsl/kernels.py`
- `src/arrowdsl/finalize.py`
- `src/relspec/compiler.py`
- `src/cpg/build_edges.py`

### Integration checklist
- [ ] Add shared specs for joins/aggregates/dedupe.
- [ ] Route kernel lane dedupe through `DedupeSpec`.
- [ ] Ensure canonical sort is enforced only at finalize (Tier A).

---

## Scope 5: IdSpec / HashSpec Protocol (Hash64 Consolidation)

### Description
Centralize the repeated hash64 + prefix + string formatting pattern so all ID generation
is consistent and vectorized, with a single config surface.

### Code patterns
```python
# src/arrowdsl/id_specs.py
from dataclasses import dataclass


@dataclass(frozen=True)
class HashSpec:
    prefix: str
    cols: tuple[str, ...]
    as_string: bool = True
    null_sentinel: str = "__NULL__"
```

```python
# src/arrowdsl/ids.py
from arrowdsl.id_specs import HashSpec

def add_hash_column(table: TableLike, *, spec: HashSpec) -> TableLike:
    arrays = [table[col] for col in spec.cols]
    hashed = hash64_from_arrays(arrays, prefix=spec.prefix, null_sentinel=spec.null_sentinel)
    if spec.as_string:
        hashed = pc.cast(hashed, pa.string())
        hashed = pc.binary_join_element_wise(pa.scalar(spec.prefix), hashed, ":")
    return table.append_column(f"{spec.prefix}_id", hashed)
```

### Target files
- `src/arrowdsl/ids.py`
- `src/normalize/ids.py`
- `src/normalize/diagnostics.py`
- `src/normalize/types.py`
- `src/normalize/bytecode_dfg.py`
- `src/extract/repo_scan.py`
- `src/extract/scip_extract.py`
- `src/cpg/builders.py`

### Integration checklist
- [ ] Introduce `HashSpec` and a single `add_hash_column` API.
- [ ] Replace all inline hash64 + prefix patterns with the shared API.

---

## Scope 6: Nested Builders (list<struct>, struct, list_view)

### Description
Centralize nested array creation using PyArrow’s advanced types and buffer‑based
constructors, enabling list<struct> and list_view semantics without ad‑hoc code.

### Code patterns
```python
# src/arrowdsl/nested_ops.py
from dataclasses import dataclass

import arrowdsl.pyarrow_core as pa
from arrowdsl.pyarrow_protocols import ArrayLike, ListArrayLike, StructArrayLike


def build_struct(fields: dict[str, ArrayLike], *, mask: ArrayLike | None = None) -> StructArrayLike:
    names = list(fields.keys())
    arrays = list(fields.values())
    return pa.StructArray.from_arrays(arrays, names=names, mask=mask)


def build_list(offsets: ArrayLike, values: ArrayLike) -> ListArrayLike:
    return pa.ListArray.from_arrays(offsets, values)
```

### Target files
- `src/arrowdsl/nested.py` (migrate to nested_ops)
- `src/arrowdsl/finalize.py`
- `src/normalize/diagnostics.py`

### Integration checklist
- [ ] Add shared nested builders.
- [ ] Replace list<struct> construction in finalize and diagnostics.
- [ ] Add list_view support when non‑monotonic offsets are required.

---

## Scope 7: Schema Transform Protocol (Metadata + Unify/Cast)

### Description
Standardize schema transformations and metadata mutation using immutable Schema and
Field semantics from PyArrow advanced docs.

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

    def apply(self, table: TableLike) -> TableLike:
        aligned, _ = align_to_schema(
            table,
            schema=self.schema,
            safe_cast=self.safe_cast,
            keep_extra_columns=self.keep_extra_columns,
        )
        return aligned
```

### Target files
- `src/arrowdsl/schema.py`
- `src/normalize/schema_infer.py`
- `src/arrowdsl/finalize.py`
- `src/schema_spec/core.py`
- `src/schema_spec/pandera_adapter.py`

### Integration checklist
- [ ] Introduce `SchemaTransform` and route alignment through it.
- [ ] Enforce immutable metadata update patterns (new schema + cast).
- [ ] Reuse in finalize + schema inference paths.

---

## Scope 8: EncodingSpec Protocol (Dictionary Encoding)

### Description
Add a standard dictionary‑encoding spec for categorical string columns to reduce memory
and stabilize joins/aggregations.

### Code patterns
```python
# src/arrowdsl/encoding.py
from dataclasses import dataclass

import arrowdsl.pyarrow_core as pa
from arrowdsl.pyarrow_protocols import TableLike
from arrowdsl.compute import pc


@dataclass(frozen=True)
class EncodingSpec:
    column: str


def encode_dictionary(table: TableLike, spec: EncodingSpec) -> TableLike:
    encoded = pc.dictionary_encode(table[spec.column])
    return table.append_column(spec.column, encoded)
```

### Target files
- `src/arrowdsl/encoding.py` (new)
- `src/normalize/diagnostics.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_nodes.py`
- `src/obs/stats.py`

### Integration checklist
- [ ] Add encoding helpers and use them for high‑cardinality string columns.
- [ ] Ensure downstream operations handle dictionary types correctly.

---

## Scope 9: Plan‑Lane Helpers for Streaming + Ordering Metadata

### Description
Provide shared plan helpers for scan/filter/project and record pipeline breaker metadata
based on Acero guidance. Preserve ordering/implicit ordering settings through `ExecutionContext`.

### Code patterns
```python
# src/arrowdsl/plan_ops.py
from dataclasses import dataclass

from arrowdsl.acero import acero
from arrowdsl.pyarrow_protocols import DeclarationLike
from arrowdsl.runtime import ExecutionContext, OrderingEffect


@dataclass(frozen=True)
class ScanOp:
    ordering_effect: OrderingEffect
    is_pipeline_breaker: bool = False

    def to_declaration(self, dataset, ctx: ExecutionContext) -> DeclarationLike:
        opts = acero.ScanNodeOptions(
            dataset,
            **ctx.runtime.scan.scan_node_kwargs(),
        )
        return acero.Declaration("scan", opts)
```

### Target files
- `src/arrowdsl/dataset_io.py`
- `src/arrowdsl/plan.py`
- `src/arrowdsl/runtime.py`

### Integration checklist
- [ ] Add plan‑lane ops (scan, filter, project, aggregate, order_by).
- [ ] Annotate ordering effects and pipeline breakers.
- [ ] Ensure `ExecutionContext` exposes Acero ordering knobs.

---

## Scope 10: Verification + Migration Cleanup

### Description
Ensure all migrations compile cleanly and remove any legacy helper duplication.

### Target files
- Entire repo

### Integration checklist
- [ ] Remove obsolete helpers (`_set_or_append_column`, ad‑hoc coalesce/const logic).
- [ ] Ensure no duplicated hash ID patterns remain.
- [ ] `uv run ruff check --fix`
- [ ] `uv run pyrefly check`
- [ ] `uv run pyright --warnings --pythonversion=3.14`
- [ ] `uv run pytest`
