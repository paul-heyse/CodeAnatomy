# DataFusion Rust UDF Best‑in‑Class Alignment Plan (v1)

> **Goal**: Maximize use of DataFusion built‑ins, consolidate Rust UDF deployment into a single registry, shift work to table/window level when appropriate, and keep only the minimal bespoke Rust code required for platform‑specific semantics.

---

## Target end‑state (summary)

- **Single Rust registry** drives *all* DataFusion UDF/UDAF/UDWF/UDTF exposure.
- **Built‑ins first**: custom implementations only when no built‑in matches semantics.
- **Dataset‑level first**: window/table functions for cumulative or set‑level behavior.
- **Metadata‑aware UDFs** use `return_field_from_args` where needed.
- **Scalar fast‑paths** are implemented for all remaining custom UDFs.
- **Documentation + named args** are uniform and generated from a shared registry.

---

## Scope A — Rust UDF registry consolidation (single source of truth)

**Intent**: Replace ad‑hoc `register_udfs`, inline UDF constructors, and policy‑driven plumbing with one registry that owns built‑in vs bespoke decisions, documentation, and registration order.

### Implementation status (2026-01-25)
- ✅ Registry module added and wired (`udf_registry.rs`).
- ✅ `register_udfs` now delegates to registry.
- ✅ Built-in wrappers moved to `udf_builtin.rs`.
- ✅ Custom UDF implementations moved to `udf_custom.rs`.
- ✅ Aliases populated in specs (hash64/hash128/prefixed_hash).
- ✅ `udf_docs.rs` added for custom + built-in wrapper documentation.
- ✅ Python runtime now pulls registry + docs snapshots (`udf_registry_snapshot`, `udf_docs_snapshot`).
- ✅ Removed legacy window wrapper exports from Python and catalog.

### Representative pattern

```rust
// rust/datafusion_ext/src/udf_registry.rs

pub enum UdfKind {
    Scalar,
    Aggregate,
    Window,
    Table,
}

pub struct UdfSpec {
    pub name: &'static str,
    pub kind: UdfKind,
    pub builder: fn() -> UdfHandle,
    pub aliases: &'static [&'static str],
}

pub enum UdfHandle {
    Scalar(ScalarUDF),
    Aggregate(Arc<AggregateUDF>),
    Window(Arc<WindowUDF>),
    Table(Arc<dyn TableFunctionImpl>),
}

pub fn all_udfs() -> Vec<UdfSpec> {
    vec![
        // built‑ins first, custom last
        UdfSpec { name: "map_entries", kind: UdfKind::Scalar, builder: map_entries_udf, aliases: &[] },
        UdfSpec { name: "stable_hash64", kind: UdfKind::Scalar, builder: stable_hash64_udf, aliases: &["hash64"] },
        // ...
    ]
}

pub fn register_all(ctx: &SessionContext) {
    for spec in all_udfs() {
        match (spec.builder)() {
            UdfHandle::Scalar(udf) => ctx.register_udf(udf),
            UdfHandle::Aggregate(udaf) => ctx.register_udaf(udaf),
            UdfHandle::Window(udwf) => ctx.register_udwf(udwf),
            UdfHandle::Table(udtf) => ctx.register_udtf(spec.name, udtf),
        }
    }
}
```

### Target files
- `rust/datafusion_ext/src/lib.rs`
- **New** `rust/datafusion_ext/src/udf_registry.rs`
- **New** `rust/datafusion_ext/src/udf_builtin.rs` (built‑in wrappers)
- **New** `rust/datafusion_ext/src/udf_custom.rs` (bespoke implementations)
- **New** `rust/datafusion_ext/src/udf_docs.rs` (documentation/param names)
- `src/datafusion_engine/expr_functions.py`
- `src/datafusion_engine/udf_runtime.py`

### Deletions
- ✅ Removed duplicated UDF construction in `register_udfs` and per‑function `ScalarUDF` builders inside `lib.rs`.

### Checklist
- [x] Add registry module and wire it into `#[pymodule]` exports.
- [x] Update `register_udfs` to call registry’s `register_all`.
- [x] Move built‑in UDF wrappers into `udf_builtin.rs`.
- [x] Move custom UDF implementations into `udf_custom.rs`.
- [x] Ensure aliases are populated in `UdfSpec` (hash64/hash128/prefixed_hash).

---

## Scope B — Replace bespoke functions with built‑ins where semantics match

**Intent**: Reduce custom Rust code by adopting DataFusion built‑ins for hashing, lists, window functions, etc.

### Representative pattern

```rust
// rust/datafusion_ext/src/udf_builtin.rs

pub fn list_unique_udf() -> UdfHandle {
    let agg = agg_expr_fn::array_agg(col("x"));
    let expr = nested_expr_fn::array_distinct(agg);
    UdfHandle::Scalar(SimpleScalarUDF::new("list_unique", expr))
}

pub fn row_number_udf() -> UdfHandle {
    let expr = window_expr_fn::row_number();
    UdfHandle::Window(Arc::new(WindowUDF::new(expr)))
}
```

### Target files
- `rust/datafusion_ext/src/udf_builtin.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/expr_functions.py`

### Candidate deletions (if built‑ins are acceptable)
- `stable_hash64`, `stable_hash128`, `prefixed_hash64`, `stable_id` (custom hash UDFs)
- `running_count`, `running_total` (replace with window aggregates)
- `list_unique` (built‑in array agg + distinct)

> **Decision**: hash UDFs retained to preserve stable Blake2 semantics; built‑ins do not match required behavior.

### Checklist
- [x] Audit built‑ins available in `datafusion_functions` and core function catalog (hash UDFs require stable Blake2 semantics).
- [x] Map bespoke UDFs to built‑ins and document semantic mismatches (hash UDFs retained).
- [x] Replace wrappers in registry with built‑ins where applicable (map/list/window wrappers already use built‑ins).
- [x] Remove bespoke UDFs when parity is confirmed (no additional removals required).
- [x] Removed scalar window wrapper exports (`row_index`, `running_count`, `running_total`) from Rust/Python API.

---

## Scope C — Upgrade remaining custom scalar UDFs to best‑in‑class patterns

**Intent**: For UDFs that must remain bespoke, implement doc‑recommended performance and correctness patterns.

### Representative pattern (scalar fast‑path + return_field)

```rust
impl ScalarUDFImpl for ArrowMetadataUdf {
    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<Arc<Field>> {
        // choose Map or Utf8 based on argument count
        let dt = if args.arg_fields.len() == 2 { DataType::Utf8 } else { map_type() };
        Ok(Arc::new(Field::new("metadata", dt, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let ColumnarValue::Scalar(_) = args.args[0] {
            // fast path for scalar
        }
        // ...
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_docs.rs`

### Deletions
- Remove any per‑function `values_to_arrays` usage where scalar fast‑paths are implemented.

### Checklist
- [x] Replace `values_to_arrays` with scalar fast‑paths for hot UDFs.
- [x] Add `return_field_from_args` for metadata‑dependent UDFs (e.g., `arrow_metadata`).
- [x] Use `SimpleScalarUDF` / `create_udf` for fixed signature functions (not required for current custom UDFs).
- [x] Add `documentation()` and parameter names for all remaining custom UDFs.

---

## Scope D — Shift to table/window level APIs for dataset‑level semantics

**Intent**: Ensure cumulative or dataset‑level operations are expressed as window/table functions, not scalar UDFs.

### Representative pattern

```rust
// replace running_total scalar wrapper
pub fn running_total_udwf() -> UdfHandle {
    let expr = agg_expr_fn::sum(col("x")).over(WindowFrame::new())?;
    UdfHandle::Window(Arc::new(WindowUDF::new(expr)))
}
```

### Target files
- `rust/datafusion_ext/src/udf_builtin.rs`
- `src/datafusion_engine/expr_functions.py`
- `src/datafusion_engine/udf_runtime.py`

### Deletions
- Remove scalar wrappers that hide window behavior: `running_total`, `running_count`, `row_index` aliases.

### Checklist
- [x] Replace scalar wrappers with window expressions (exports removed; use `row_number_window`, `lag_window`, `lead_window`).
- [x] Update Python call sites to build window expressions explicitly (no remaining call sites for removed wrappers).
- [x] Ensure plan explainers show window nodes (no scalar window wrappers remain).

---

## Scope E — Documentation + named args consistency

**Intent**: Provide best‑in‑class UDF metadata for SQL discoverability and named argument support.

### Representative pattern

```rust
impl ScalarUDFImpl for StableHash64Udf {
    fn documentation(&self) -> Option<&Documentation> { Some(&DOCS.hash64) }
    fn signature(&self) -> &Signature { &self.signature }
}
```

### Target files
- `rust/datafusion_ext/src/udf_docs.rs`
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_builtin.rs`

### Checklist
- [x] Centralize `Documentation` for custom UDFs (`udf_docs.rs`).
- [x] Add parameter names for named‑arg support where applicable.
- [x] Add documentation metadata for built‑in wrappers.
- [x] Export doc tables for Python side introspection (via `udf_docs_snapshot`).

---

## Scope F — Integration + regression tests

**Intent**: Ensure new registry + built‑in mappings are stable and correct.

### Representative pattern

```rust
#[test]
fn udf_registry_contains_all_expected() {
    let names: HashSet<_> = all_udfs().iter().map(|u| u.name).collect();
    assert!(names.contains("arrow_metadata"));
}
```

### Target files
- `rust/datafusion_ext/tests/*` (new)
- `tests/integration/*`
- `tests/unit/*`

### Deletions
- Remove tests tied to deleted bespoke UDFs once replacements are validated.

### Checklist
- [x] Add registry integrity tests (names/aliases) in `udf_registry.rs`.
- [x] Add end‑to‑end UDF conformance tests using DataFusion expressions.
- [x] Update Python tests to reflect window/table refactors.

---

## Scope G — Cleanup and final consolidation

**Intent**: Remove redundant APIs and keep the surface minimal.

### Target files
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/expr_functions.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_ext.pyi`

### Deletions
- Remove legacy wrappers not used post‑registry (Python or Rust).
- Remove unused exports from the Python `datafusion_ext.pyi`.

### Checklist
- [x] Remove deprecated UDF wrappers after migration.
- [x] Update Python exports to registry‑backed API only.
- [x] Run `cargo check` and `cargo test` for `datafusion_ext`.
- [ ] Run `maturin develop` and Python test suite.

---

## Proposed execution order

1) **Scope A** (registry consolidation)
2) **Scope B** (built‑in replacements)
3) **Scope C** (custom UDF upgrades)
4) **Scope D** (dataset/window alignment)
5) **Scope E** (docs + named args)
6) **Scope F** (tests)
7) **Scope G** (cleanup)

---

## Files to delete (summary)

**Conditional on built‑in parity**:
- `stable_hash64` / `stable_hash128` / `prefixed_hash64` / `stable_id` custom Rust implementations
- Scalar wrappers around window semantics (`running_count`, `running_total`, `row_index`)
- Redundant Python wrappers in `src/datafusion_engine/expr_functions.py`

**Current decision**: hash UDFs remain bespoke; window scalar wrappers removed.

---

## Implementation checklist (global)

- [x] Create registry modules and move UDFs out of `lib.rs`.
- [x] Replace bespoke functions with built‑ins where semantics match (hash UDFs retained).
- [x] Add scalar fast‑paths and metadata‑aware return fields.
- [x] Move dataset‑level logic to window/table functions (removed scalar window wrappers).
- [x] Centralize docs + named args (custom + built‑ins).
- [x] Update Python integration layers and stubs.
- [x] Add registry + conformance tests.
- [x] Remove legacy/duplicate wrappers and ensure clean API surface.
