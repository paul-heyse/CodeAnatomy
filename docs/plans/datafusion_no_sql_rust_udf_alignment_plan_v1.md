# DataFusion No-SQL + Rust-Only UDF Alignment Plan v1

## Goal
Eliminate **all SQL strings** from view construction and **remove all Python UDFs**, replacing them with **Rust extension functions** and Python-side **native Expr builders** only. This yields a single, deterministic, schema-driven DataFusion architecture with no string-based execution and no Python UDF runtime variability.

## Best-in-class end-state
- View registry builds **only** from DataFusion `Expr` and `DataFrame` APIs.
- All function surface (map/union/metadata/ids) is provided by **`rust/datafusion_ext`** and exposed as Python Expr constructors.
- Python UDF registry is fully removed.
- Runtime validation ensures all required Rust functions exist before view registration.
- View definitions are **schema-derived** and **deterministic**, with no embedded SQL and no Python UDFs.

---

# Scope Items

## 1) Rust extension function surface (core operators + id functions)

**Why**
The DataFusion Python API does not expose several required SQL-only functions (map/union/metadata). We must provide them as native Rust functions and export Python bindings that return `Expr`.

**Representative pattern (target)**
```rust
// rust/datafusion_ext/src/lib.rs (conceptual)
#[pyfunction]
fn map_entries(expr: PyExpr) -> PyExpr { /* wraps DF scalar function */ }

#[pyfunction]
fn map_keys(expr: PyExpr) -> PyExpr { /* wraps DF scalar function */ }

#[pyfunction]
fn map_values(expr: PyExpr) -> PyExpr { /* wraps DF scalar function */ }

#[pyfunction]
fn map_extract(map_expr: PyExpr, key_expr: PyExpr) -> PyExpr { /* wraps DF scalar */ }

#[pyfunction]
fn arrow_metadata(expr: PyExpr, key: &str) -> PyExpr { /* wraps DF scalar */ }

#[pyfunction]
fn union_tag(expr: PyExpr) -> PyExpr { /* wraps DF scalar */ }

#[pyfunction]
fn union_extract(expr: PyExpr, tag: &str) -> PyExpr { /* wraps DF scalar */ }

#[pyfunction]
fn stable_id(prefix: &str, value: PyExpr) -> PyExpr { /* native hash */ }

#[pyfunction]
fn prefixed_hash64(prefix: &str, value: PyExpr) -> PyExpr { /* native hash */ }
```

**Target files**
- `rust/datafusion_ext/src/lib.rs`
- `rust/datafusion_ext/src/...` (internal modules as needed)

**Deletes**
- None in this scope (adds new Rust APIs)

**Implementation checklist**
- [ ] Export Rust functions for: `map_entries`, `map_keys`, `map_values`, `map_extract`.
- [ ] Export Rust functions for: `arrow_metadata`, `union_tag`, `union_extract`.
- [ ] Port Python UDFs to Rust: `stable_hash64`, `stable_hash128`, `prefixed_hash64`, `stable_id`, `col_to_byte`, `list_unique`, `first_value_agg`, `last_value_agg`, `count_distinct_agg`, `string_agg`, `row_index`, `running_count`, `running_total`, `row_number_window`, `lag_window`, `lead_window`, `range_table`.
- [ ] Add DataFusion registration + Python binding exposure.
- [ ] Validate that `datafusion_ext` exposes all new functions to Python.

---

## 2) Python Expr wrapper module (no SQL strings)

**Why**
The view registry must only use Python Expr constructors. We need a stable wrapper layer for Rust-exposed functions.

**Representative pattern (target)**
```python
# src/datafusion_engine/expr_functions.py
from __future__ import annotations

from datafusion import Expr
from datafusion_ext import (
    arrow_metadata as _arrow_metadata,
    map_entries as _map_entries,
    map_extract as _map_extract,
    map_keys as _map_keys,
    map_values as _map_values,
    prefixed_hash64 as _prefixed_hash64,
    stable_id as _stable_id,
    union_extract as _union_extract,
    union_tag as _union_tag,
)


def map_entries(expr: Expr) -> Expr:
    return _map_entries(expr)


def map_keys(expr: Expr) -> Expr:
    return _map_keys(expr)


def map_values(expr: Expr) -> Expr:
    return _map_values(expr)


def map_extract(expr: Expr, key: str) -> Expr:
    return _map_extract(expr, key)


def arrow_metadata(expr: Expr, key: str) -> Expr:
    return _arrow_metadata(expr, key)


def union_tag(expr: Expr) -> Expr:
    return _union_tag(expr)


def union_extract(expr: Expr, tag: str) -> Expr:
    return _union_extract(expr, tag)


def stable_id(prefix: str, value: Expr) -> Expr:
    return _stable_id(prefix, value)


def prefixed_hash64(prefix: str, value: Expr) -> Expr:
    return _prefixed_hash64(prefix, value)
```

**Target files**
- New: `src/datafusion_engine/expr_functions.py`

**Deletes**
- None in this scope

**Implementation checklist**
- [ ] Add wrapper module for Rust functions.
- [ ] Ensure zero SQL string usage in this module.
- [ ] Add `__all__` exports.

---

## 3) View registry: pure DataFrame + Expr builders

**Why**
This removes all `select_exprs`/`parse_sql_expr` usage and ensures view definitions are strictly expression-constructed.

**Representative pattern (target)**
```python
from datafusion import col, lit
from datafusion_engine.expr_functions import map_entries, map_extract

base_df = ctx.table("ast_calls")
kv = map_entries(col("attrs"))
expanded = base_df.with_column("kv", kv).unnest_columns("kv")
view_df = expanded.select(
    col("file_id"),
    col("path"),
    col("ast_id"),
    map_extract(col("attrs"), "arg_count").alias("arg_count"),
    col("kv")["key"].alias("attr_key"),
    col("kv")["value"].alias("attr_value"),
)
```

**Target files**
- Update: `src/datafusion_engine/view_registry.py`
- Update: `src/datafusion_engine/schema_registry.py` (if registry helpers are reused)

**Deletes**
- `src/datafusion_engine/view_registry_exprs.py`

**Implementation checklist**
- [ ] Replace all `select_exprs` / `parse_sql_expr` with explicit `Expr` builders.
- [ ] Use Rust-backed wrappers for map/union/metadata functions.
- [ ] Ensure derived views (attrs, spans, diagnostics) are pure `Expr` pipelines.
- [ ] Maintain view names and output column order.

---

## 4) Remove Python UDF registry (Rust only)

**Why**
We are eliminating Python UDFs entirely. All UDFs become Rust extension functions.

**Representative pattern (target)**
```python
# src/datafusion_engine/runtime.py
# replace Python UDF registration with rust extension install
from datafusion_ext import register_udfs

if self.enable_udfs:
    register_udfs(ctx)
```

**Target files**
- Update: `src/datafusion_engine/runtime.py`
- Update/Remove: `src/datafusion_engine/udf_registry.py`
- Update: `src/datafusion_engine/function_factory.py` (if needed for registry policy)

**Deletes**
- Python UDF definitions and registration paths in `src/datafusion_engine/udf_registry.py`

**Implementation checklist**
- [ ] Remove all Python UDF definitions.
- [ ] Remove UDF registration in Python runtime.
- [ ] Replace with Rust extension UDF install hook.
- [ ] Update any catalog or diagnostics code that relies on Python UDF metadata.

---

## 5) Validation and required-function enforcement

**Why**
We must fail fast if required Rust functions are missing.

**Representative pattern (target)**
```python
# src/datafusion_engine/schema_registry.py
ENGINE_REQUIRED_FUNCTIONS += (
    "map_entries",
    "map_keys",
    "map_values",
    "map_extract",
    "arrow_metadata",
    "union_tag",
    "union_extract",
)
```

**Target files**
- Update: `src/datafusion_engine/schema_registry.py`
- Update: `src/datafusion_engine/runtime.py`

**Deletes**
- None

**Implementation checklist**
- [ ] Add new Rust functions to required-function lists.
- [ ] Ensure validation paths check function presence and signature.
- [ ] Confirm diagnostics report missing Rust bindings clearly.

---

## 6) Tests + regression gates

**Why**
Ensure no SQL strings and no Python UDFs are used anywhere.

**Representative pattern (target)**
```python
def test_view_registry_uses_expr_only() -> None:
    src = Path("src/datafusion_engine/view_registry.py").read_text()
    assert "select_exprs" not in src
    assert "parse_sql_expr" not in src
```

**Target files**
- Update: `tests/unit/test_datafusion_query_fragments.py`
- Add: new unit tests for registry behavior

**Deletes**
- Tests tied to static SQL payloads

**Implementation checklist**
- [ ] Add tests ensuring no SQL string usage in view registry.
- [ ] Add tests ensuring no Python UDF registry usage.
- [ ] Update any view coverage tests to use view specs + schemas.

---

# Deletions Summary

**Files**
- `src/datafusion_engine/view_registry_exprs.py`

**Modules/paths**
- Python UDF definitions in `src/datafusion_engine/udf_registry.py`

---

# Verification Checklist
- [ ] `rg "select_exprs|parse_sql_expr" src/datafusion_engine` returns empty.
- [ ] `rg "ScalarUDF|udf_registry" src/datafusion_engine` returns empty (or only Rust-facing shim usage).
- [ ] All view registry specs register successfully.
- [ ] `uv run ruff check --fix`, `uv run pyrefly check`, `uv run pyright --warnings --pythonversion=3.13` pass.

---

# Implementation Order (Recommended)
1) Rust extension function surface
2) Python Expr wrappers
3) View registry conversion to pure Expr
4) Remove Python UDF registry
5) Validation + tests

