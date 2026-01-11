# Implementation Plan: Local Protocols for PyArrow Compute + Acero Typing

This plan removes the `typings/pyarrow/compute.pyi` and `typings/pyarrow/acero.pyi`
stubs by introducing local Protocols that model the small subset of APIs used in
this repo. Runtime still uses `pyarrow.compute` and `pyarrow.acero`, but all static
typing relies on local protocols.

---

## Scope 1: Define local compute + acero Protocols

### Description
Create a small local protocol layer that captures the minimal surface needed by
our code for compute expressions and acero declarations. This becomes the only
typing dependency for these modules.

### Code patterns
```python
# src/arrowdsl/pyarrow_protocols.py
from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable


@runtime_checkable
class ComputeExpression(Protocol):
    def __and__(self, other: ComputeExpression) -> ComputeExpression: ...
    def __or__(self, other: ComputeExpression) -> ComputeExpression: ...
    def __invert__(self) -> ComputeExpression: ...
    def __eq__(self, other: object) -> ComputeExpression: ...
    def __ne__(self, other: object) -> ComputeExpression: ...
    def __lt__(self, other: object) -> ComputeExpression: ...
    def __le__(self, other: object) -> ComputeExpression: ...
    def __gt__(self, other: object) -> ComputeExpression: ...
    def __ge__(self, other: object) -> ComputeExpression: ...
    def isin(self, values: Sequence[object]) -> ComputeExpression: ...
    def is_null(self) -> ComputeExpression: ...
    def is_valid(self) -> ComputeExpression: ...


@runtime_checkable
class RecordBatchReaderLike(Protocol):
    def read_all(self) -> object: ...


@runtime_checkable
class DeclarationLike(Protocol):
    def to_table(self, *, use_threads: bool | None = None) -> object: ...
    def to_reader(self, *, use_threads: bool | None = None) -> RecordBatchReaderLike: ...
```

### Target files
- `src/arrowdsl/pyarrow_protocols.py` (new)

### Integration checklist
- [ ] Add protocol module with `ComputeExpression`, `RecordBatchReaderLike`, `DeclarationLike`.
- [ ] Keep protocol surfaces minimal and aligned with actual usage in the repo.

---

## Scope 2: Route expression helpers through Protocols

### Description
Update `arrowdsl/expr.py` to export type aliases that reference the new protocols.
This gives a single, stable import path used across the repo.

### Code patterns
```python
# src/arrowdsl/expr.py
from arrowdsl.pyarrow_protocols import ComputeExpression

type ExpressionLike = str | ComputeExpression

def field(name: str) -> ComputeExpression:
    return pc.field(name)
```

### Target files
- `src/arrowdsl/expr.py`

### Integration checklist
- [ ] Replace `pc.Expression` annotations with `ComputeExpression`.
- [ ] Keep runtime behavior unchanged (still uses `pyarrow.compute`).

---

## Scope 3: Replace compute type hints across the codebase

### Description
Replace all `pc.Expression` annotations with `ComputeExpression` from the local protocol.
This removes any dependency on compute stubs for typing.

### Code patterns
```python
# Example update
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
- [ ] Replace `pc.Expression` in type aliases and function signatures.
- [ ] Ensure no type hints import from `pyarrow.compute` directly.

---

## Scope 4: Replace acero declaration type hints

### Description
Replace `acero.Declaration` annotations with the `DeclarationLike` protocol while
keeping runtime usage of `pyarrow.acero`.

### Code patterns
```python
from arrowdsl.pyarrow_protocols import DeclarationLike

class Plan:
    decl: DeclarationLike | None = None
```

### Target files
- `src/arrowdsl/plan.py`
- `src/arrowdsl/dataset_io.py`
- `src/arrowdsl/joins.py`

### Integration checklist
- [ ] Replace `acero.Declaration` in annotations.
- [ ] Keep runtime calls to `acero.Declaration(...)` unchanged.

---

## Scope 5: Remove compute/acero stubs and update config

### Description
Delete the stub files and ensure type checkers rely on local protocols instead.

### Code patterns
```text
typings/pyarrow/compute.pyi  -> delete
typings/pyarrow/acero.pyi    -> delete
```

### Target files
- `typings/pyarrow/compute.pyi` (remove)
- `typings/pyarrow/acero.pyi` (remove)
- `pyrightconfig.json` (optional: remove `stubPath` if we later remove `typings/pyarrow/__init__.pyi`)
- `pyrefly.toml` (optional: remove `typings` from `search-path` if we later remove `typings/pyarrow/__init__.pyi`)

### Integration checklist
- [ ] Delete the compute/acero stubs.
- [ ] Run pyrefly/pyright to ensure Protocols cover all typed uses.
- [ ] Decide whether to retain `typings/pyarrow/__init__.pyi` (recommended for now).

---

## Scope 6: Verification

### Description
Validate that the codebase compiles, tests run, and no new type errors appear.

### Target files
- Entire repo

### Integration checklist
- [ ] `uv run ruff check --fix`
- [ ] `uv run pyrefly check`
- [ ] `uv run pyright --warnings --pythonversion=3.14`
- [ ] `uv run pytest`

---

## Optional Extension: Fully remove `typings/pyarrow/__init__.pyi`

If you want *zero* local pyarrow stubs, add local protocols for `Table`, `Array`,
`ChunkedArray`, `Scalar`, `Schema`, and `Field` and migrate annotations accordingly.
This is a larger change and can be planned as a separate phase.
