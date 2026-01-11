# Implementation Plan: Local Protocols for PyArrow Compute + Acero Typing

This plan removes the `typings/pyarrow/compute.pyi` and `typings/pyarrow/acero.pyi`
stubs by introducing local Protocols that model the small subset of APIs used in
this repo. Runtime still uses `pyarrow.compute` and `pyarrow.acero`, but all static
typing relies on local protocols and typed wrappers.

Status: Scopes 1-6 completed. Scopes 7-9 planned for removing
`typings/pyarrow/__init__.pyi`.

---

## Completed Work (Scopes 1-6)

- [x] Added protocol definitions and runtime guards in `src/arrowdsl/pyarrow_protocols.py`.
- [x] Added typed wrappers for compute and acero in `src/arrowdsl/compute.py` and
  `src/arrowdsl/acero.py`.
- [x] Replaced compute expression annotations with `ComputeExpression` and routed
  expression helpers through `ensure_expression`.
- [x] Replaced acero declaration annotations with `DeclarationLike` and moved
  imports to `arrowdsl.acero`.
- [x] Deleted `typings/pyarrow/compute.pyi` and `typings/pyarrow/acero.pyi`.
- [x] Verified `ruff`, `pyrefly`, and `pyright` for the changes.
- [ ] `pytest` (not run in this pass).

---

## Scope 1: Define local compute + acero Protocols (Completed)

### Description
Create a small local protocol layer that captures the minimal surface needed by
our code for compute expressions and acero declarations. This becomes the only
typing dependency for these modules.

### Code patterns
```python
# src/arrowdsl/pyarrow_protocols.py
from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol, runtime_checkable

import pyarrow as pa


@runtime_checkable
class ComputeExpression(Protocol):
    def __hash__(self) -> int: ...
    def isin(self, values: Sequence[object]) -> ComputeExpression: ...
    def is_null(self) -> ComputeExpression: ...
    def is_valid(self) -> ComputeExpression: ...


class UdfContext(Protocol):
    """Protocol for pyarrow.compute.UdfContext."""


def ensure_expression(value: object) -> ComputeExpression:
    if isinstance(value, ComputeExpression):
        return value
    raise TypeError("Expected a compute expression.")


@runtime_checkable
class DeclarationLike(Protocol):
    def to_table(self, *, use_threads: bool | None = None) -> pa.Table: ...
    def to_reader(self, *, use_threads: bool | None = None) -> pa.RecordBatchReader: ...


class ComputeModule(Protocol):
    field: Callable[[str], ComputeExpression]
    scalar: Callable[[object], ComputeExpression]
    equal: Callable[..., pa.Array]
    not_equal: Callable[..., pa.Array]
    less: Callable[..., pa.Array]
    less_equal: Callable[..., pa.Array]
    greater: Callable[..., pa.Array]
    greater_equal: Callable[..., pa.Array]
    and_: Callable[..., pa.Array]
    or_: Callable[..., pa.Array]
    invert: Callable[..., pa.Array]
    cast: Callable[..., pa.Array]
    coalesce: Callable[..., pa.Array]
```

### Target files
- `src/arrowdsl/pyarrow_protocols.py`

### Integration checklist
- [x] Add protocol module with `ComputeExpression`, `DeclarationLike`, `ComputeModule`, and `UdfContext`.
- [x] Add `ensure_expression` to guard runtime expression casts.

---

## Scope 2: Route expression helpers through Protocols (Completed)

### Description
Update `arrowdsl/expr.py` to export type aliases that reference the new
protocols and normalize compute output via `ensure_expression`.

### Code patterns
```python
# src/arrowdsl/expr.py
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import ComputeExpression, ensure_expression

type ExpressionLike = str | ComputeExpression

class E:
    @staticmethod
    def eq(col: str, value: ScalarValue) -> ComputeExpression:
        return ensure_expression(pc.equal(E.field(col), E.scalar(value)))
```

### Target files
- `src/arrowdsl/expr.py`

### Integration checklist
- [x] Replace `pc.Expression` annotations with `ComputeExpression`.
- [x] Route compute calls through `ensure_expression`.

---

## Scope 3: Replace compute type hints across the codebase (Completed)

### Description
Replace all compute expression annotations with `ComputeExpression` from the
local protocol. Use `arrowdsl.compute.pc` as the canonical compute import.

### Code patterns
```python
from arrowdsl.compute import pc
from arrowdsl.pyarrow_protocols import ComputeExpression

def filter(self, predicate: ComputeExpression, *, label: str = "") -> Plan:
    ...
```

### Target files
- `src/arrowdsl/plan.py`
- `src/arrowdsl/queryspec.py`
- `src/arrowdsl/expr.py`
- `src/relspec/model.py`
- `src/relspec/compiler.py`

### Integration checklist
- [x] Replace `pc.Expression` in type aliases and function signatures.
- [x] Remove direct type imports from `pyarrow.compute`.

---

## Scope 4: Replace acero declaration type hints (Completed)

### Description
Replace `acero.Declaration` annotations with the `DeclarationLike` protocol
while keeping runtime usage of `pyarrow.acero` through a typed wrapper.

### Code patterns
```python
# src/arrowdsl/acero.py
from typing import Protocol, cast
from pyarrow import acero as _acero

from arrowdsl.pyarrow_protocols import DeclarationLike

class AceroModule(Protocol):
    Declaration: callable[..., DeclarationLike]
    ScanNodeOptions: callable[..., object]
    FilterNodeOptions: callable[..., object]
    ProjectNodeOptions: callable[..., object]

acero = cast(AceroModule, _acero)
```

```python
# src/arrowdsl/plan.py
from arrowdsl.acero import acero
from arrowdsl.pyarrow_protocols import DeclarationLike
```

### Target files
- `src/arrowdsl/acero.py`
- `src/arrowdsl/plan.py`
- `src/arrowdsl/dataset_io.py`
- `src/arrowdsl/joins.py`

### Integration checklist
- [x] Replace `acero.Declaration` in annotations with `DeclarationLike`.
- [x] Route runtime imports through `arrowdsl.acero`.

---

## Scope 5: Remove compute/acero stubs and update config (Completed)

### Description
Delete the compute/acero stub files and keep pyright/pyrefly relying on local
protocols for these APIs.

### Code patterns
```text
typings/pyarrow/compute.pyi  -> deleted
typings/pyarrow/acero.pyi    -> deleted
```

### Target files
- `typings/pyarrow/compute.pyi`
- `typings/pyarrow/acero.pyi`

### Integration checklist
- [x] Delete the compute/acero stubs.
- [x] Validate typing passes without these stubs.
- [x] Keep `typings/pyarrow/__init__.pyi` for now.

---

## Scope 6: Verification (Completed)

### Target files
- Entire repo

### Integration checklist
- [x] `uv run ruff check --fix`
- [x] `uv run pyrefly check`
- [x] `uv run pyright --warnings --pythonversion=3.14`
- [ ] `uv run pytest`

---

## Scope 7: Add core Arrow Protocols and a typed Arrow module wrapper (Planned)

### Description
Introduce protocol definitions for core Arrow types (`Table`, `Array`, `Schema`,
`Field`, `Scalar`, `DataType`, `RecordBatchReader`, `NativeFile`) and a typed
wrapper for the `pyarrow` module to eliminate reliance on `typings/pyarrow/__init__.pyi`.

### Code patterns
```python
# src/arrowdsl/pyarrow_protocols.py
from collections.abc import Iterator, Mapping, Sequence
from typing import Protocol, runtime_checkable

@runtime_checkable
class ArrayLike(Protocol):
    null_count: int
    type: DataTypeLike
    def field(self, index: int | str) -> ArrayLike: ...
    def to_pylist(self) -> list[object]: ...
    def __iter__(self) -> Iterator[object]: ...
    def __len__(self) -> int: ...

@runtime_checkable
class ChunkedArrayLike(ArrayLike, Protocol):
    def combine_chunks(self) -> ArrayLike: ...

@runtime_checkable
class ScalarLike(Protocol):
    type: DataTypeLike
    def as_py(self) -> object: ...

class DataTypeLike(Protocol):
    ...

@runtime_checkable
class FieldLike(Protocol):
    name: str
    type: DataTypeLike
    metadata: Mapping[bytes, bytes] | None
    nullable: bool

@runtime_checkable
class SchemaLike(Protocol):
    names: list[str]
    metadata: Mapping[bytes, bytes] | None
    def with_metadata(self, metadata: Mapping[bytes, bytes]) -> SchemaLike: ...
    def field(self, name_or_index: str | int) -> FieldLike: ...
    def get_field_index(self, name: str) -> int: ...
    def __iter__(self) -> Iterator[FieldLike]: ...

@runtime_checkable
class RecordBatchReaderLike(Protocol):
    schema: SchemaLike
    def read_all(self) -> TableLike: ...

@runtime_checkable
class NativeFileLike(Protocol):
    def __enter__(self) -> NativeFileLike: ...
    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: object | None) -> None: ...

@runtime_checkable
class TableGroupByLike(Protocol):
    def aggregate(self, aggs: Sequence[tuple[str, str]]) -> TableLike: ...

@runtime_checkable
class TableLike(Protocol):
    num_rows: int
    column_names: list[str]
    schema: SchemaLike
    def append_column(self, name: str, data: ArrayLike | ChunkedArrayLike) -> TableLike: ...
    def drop(self, columns: Sequence[str]) -> TableLike: ...
    def rename_columns(self, names: Sequence[str]) -> TableLike: ...
    def to_pylist(self) -> list[dict[str, object]]: ...
    def to_pydict(self) -> dict[str, list[object]]: ...
    def filter(self, mask: ArrayLike) -> TableLike: ...
    def take(self, indices: ArrayLike) -> TableLike: ...
    def group_by(self, keys: Sequence[str], *, use_threads: bool = True) -> TableGroupByLike: ...
    def join(self, right: TableLike, keys: Sequence[str], right_keys: Sequence[str] | None = None, join_type: str = "left outer", left_suffix: str | None = None, right_suffix: str | None = None, coalesce_keys: bool = True, use_threads: bool = True, filter_expression: object | None = None) -> TableLike: ...
    def select(self, names: Sequence[str]) -> TableLike: ...
    def set_column(self, i: int, name: str, data: ArrayLike | ChunkedArrayLike) -> TableLike: ...
    def to_reader(self) -> RecordBatchReaderLike: ...
    def __getitem__(self, key: str) -> ChunkedArrayLike: ...
```

```python
# src/arrowdsl/pyarrow_core.py
from __future__ import annotations

from typing import Protocol, cast
import pyarrow as _pa

from arrowdsl.pyarrow_protocols import ArrayLike, ChunkedArrayLike, DataTypeLike, FieldLike, ScalarLike, SchemaLike, TableLike

class ArrowModule(Protocol):
    Array: type[ArrayLike]
    ChunkedArray: type[ChunkedArrayLike]
    Scalar: type[ScalarLike]
    DataType: type[DataTypeLike]
    Field: type[FieldLike]
    Schema: type[SchemaLike]
    Table: type[TableLike]
    array: callable[..., ArrayLike]
    scalar: callable[..., ScalarLike]
    table: callable[..., TableLike]
    schema: callable[..., SchemaLike]
    field: callable[..., FieldLike]
    concat_tables: callable[..., TableLike]
    unify_schemas: callable[..., SchemaLike]
    list_: callable[..., DataTypeLike]
    struct: callable[..., DataTypeLike]
    string: callable[[], DataTypeLike]
    int32: callable[[], DataTypeLike]
    int64: callable[[], DataTypeLike]
    bool_: callable[[], DataTypeLike]
    OSFile: callable[..., object]
    memory_map: callable[..., object]

pa = cast(ArrowModule, _pa)
```

### Target files
- `src/arrowdsl/pyarrow_protocols.py` (extend)
- `src/arrowdsl/pyarrow_core.py` (new)

### Integration checklist
- [ ] Add core Arrow protocols for the types we reference in annotations.
- [ ] Add a typed wrapper for the `pyarrow` module (`arrowdsl.pyarrow_core.pa`).
- [ ] Update `ComputeModule` signatures to use `ArrayLike`, `ChunkedArrayLike`, and `ScalarLike`.

---

## Scope 8: Migrate annotations/imports to local Arrow protocols (Planned)

### Description
Replace direct `pa.Table`, `pa.Schema`, `pa.Array`, etc. annotations with local
protocol types and route runtime calls through the typed wrapper module. This
eliminates the last typing dependency on `typings/pyarrow/__init__.pyi`.

### Code patterns
```python
from arrowdsl.pyarrow_core import pa
from arrowdsl.pyarrow_protocols import ArrayLike, DataTypeLike, SchemaLike, TableLike


def align_table(table: TableLike, schema: SchemaLike) -> TableLike:
    arrays: list[ArrayLike] = []
    ...
    return pa.Table.from_arrays(arrays, schema=schema)
```

### Target files
- `src/arrowdsl/columns.py`
- `src/arrowdsl/contracts.py`
- `src/arrowdsl/dataset_io.py`
- `src/arrowdsl/empty.py`
- `src/arrowdsl/expr.py`
- `src/arrowdsl/finalize.py`
- `src/arrowdsl/ids.py`
- `src/arrowdsl/iter.py`
- `src/arrowdsl/kernels.py`
- `src/arrowdsl/nested.py`
- `src/arrowdsl/plan.py`
- `src/arrowdsl/pyarrow_protocols.py`
- `src/arrowdsl/runner.py`
- `src/arrowdsl/runtime.py`
- `src/arrowdsl/schema.py`
- `src/cpg/build_edges.py`
- `src/cpg/builders.py`
- `src/cpg/build_nodes.py`
- `src/cpg/build_props.py`
- `src/cpg/schemas.py`
- `src/cpg/specs.py`
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/repo_scan.py`
- `src/extract/runtime_inspect_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/hamilton_pipeline/modules/extraction.py`
- `src/hamilton_pipeline/modules/normalization.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/normalize/bytecode_anchor.py`
- `src/normalize/bytecode_cfg.py`
- `src/normalize/bytecode_dfg.py`
- `src/normalize/diagnostics.py`
- `src/normalize/ids.py`
- `src/normalize/schema_infer.py`
- `src/normalize/spans.py`
- `src/normalize/types.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`
- `src/obs/stats.py`
- `src/relspec/compiler.py`
- `src/schema_spec/core.py`
- `src/schema_spec/pandera_adapter.py`
- `src/storage/ipc.py`
- `src/storage/parquet.py`

### Integration checklist
- [ ] Replace `pa.Table`, `pa.Schema`, `pa.Array`, `pa.ChunkedArray`, `pa.Scalar`,
  `pa.Field`, `pa.DataType`, and `pa.RecordBatchReader` annotations with protocol
  equivalents.
- [ ] Replace `import pyarrow as pa` with `from arrowdsl.pyarrow_core import pa`.
- [ ] Update any `isinstance(..., pa.Schema)` checks to use runtime types from
  `arrowdsl.pyarrow_core.pa`.

---

## Scope 9: Remove `typings/pyarrow/__init__.pyi` and update configs (Planned)

### Description
Remove the remaining pyarrow stub file and remove any type checker configuration
that points at `typings/`.

### Code patterns
```text
typings/pyarrow/__init__.pyi -> delete
```

```json
// pyrightconfig.json (if only used for pyarrow)
{
  "stubPath": null
}
```

```toml
# pyrefly.toml (if only used for pyarrow)
search-path = ["src"]
```

### Target files
- `typings/pyarrow/__init__.pyi`
- `pyrightconfig.json`
- `pyrefly.toml`

### Integration checklist
- [ ] Delete `typings/pyarrow/__init__.pyi`.
- [ ] Remove `typings` from `pyrightconfig.json` and `pyrefly.toml` if no longer needed.
- [ ] Run ruff, pyrefly, and pyright to verify no Any leaks.
- [ ] Run pytest if applicable.
