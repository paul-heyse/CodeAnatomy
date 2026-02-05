# Rust Architecture & UDFs

## Purpose

The Rust subsystem provides high-performance DataFusion extensions, custom UDFs, Delta Lake integration, and plugin infrastructure. It delivers native performance for compute-intensive operations while maintaining type safety and ABI stability for Python integration.

## Key Concepts

- **Custom UDF System** - 28+ scalar, 11+ aggregate, and 3+ window functions implemented in Rust
- **Plugin Architecture** - ABI-stable dynamic loading via `abi_stable` crate
- **PyO3 Bindings** - Zero-copy Arrow IPC bridge between Python and Rust
- **Delta Lake Native** - Rust-native Delta operations (CDF, mutations, maintenance)
- **Function Factory** - SQL macro support via `CREATE FUNCTION` statements

---

## Workspace Structure

```
rust/
├── Cargo.toml                   # Workspace manifest with shared dependencies
├── datafusion_ext/              # Core UDF library (rlib)
├── datafusion_ext_py/           # Thin PyO3 wrapper (cdylib)
├── datafusion_python/           # DataFusion Python bindings (cdylib + rlib)
├── df_plugin_api/               # ABI-stable plugin interface definitions
├── df_plugin_host/              # Plugin loading and validation
└── df_plugin_codeanatomy/       # CodeAnatomy plugin implementation (cdylib)
```

### Crate Responsibilities

| Crate | Type | Purpose |
|-------|------|---------|
| `datafusion_ext` | rlib | Pure Rust UDFs, Delta ops, DataFusion extensions |
| `datafusion_ext_py` | cdylib | Thin PyO3 wrapper exposing `datafusion_ext` |
| `datafusion_python` | cdylib+rlib | Apache DataFusion Python bindings + CodeAnatomy |
| `df_plugin_api` | rlib | ABI-stable interface definitions |
| `df_plugin_host` | rlib | Plugin loading with version validation |
| `df_plugin_codeanatomy` | cdylib | Plugin bundling all UDFs and Delta providers |

---

## Custom UDF System

### UDF Categories (28 Scalar UDFs)

| Category | Functions | Description |
|----------|-----------|-------------|
| **Hashing & IDs** | `stable_hash64`, `stable_hash128`, `prefixed_hash64`, `stable_id`, `stable_id_parts`, `span_id` | BLAKE2b-based deterministic hashing |
| **Span Arithmetic** | `span_make`, `span_len`, `span_overlaps`, `span_contains`, `interval_align_score` | Byte span operations |
| **String Normalization** | `utf8_normalize`, `utf8_null_if_blank`, `qname_normalize` | Unicode and qualified name handling |
| **Collections** | `list_compact`, `list_unique_sorted`, `map_get_default`, `map_normalize` | List and map operations |
| **Metadata** | `arrow_metadata`, `semantic_tag`, `cpg_score` | Arrow field metadata extraction |
| **Delta CDF** | `cdf_change_rank`, `cdf_is_upsert`, `cdf_is_delete` | Change Data Feed utilities |
| **Type Conversion** | `col_to_byte`, `struct_pick` | Type conversion and struct operations |

### UDF Implementation Pattern

Every custom UDF follows this structure:

```rust
#[derive(Debug, PartialEq, Eq, Hash)]
struct StableHash64Udf {
    signature: Signature,
}

impl ScalarUDFImpl for StableHash64Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "stable_hash64" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> { ... }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ArrayRef> { ... }

    // Optional optimizations
    fn simplify(&self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> { ... }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> { ... }
}

pub fn stable_hash64_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(StableHash64Udf {
        signature: Signature::variadic_any(Volatility::Immutable),
    })
}
```

### API Compliance

All UDFs use modern DataFusion APIs:

| API Feature | Coverage | Notes |
|-------------|----------|-------|
| `invoke_with_args` | 100% | Primary execution method |
| `return_field_from_args` | 100% | Dynamic return type support |
| `coerce_types` | ~60% | For multi-typed inputs |
| `simplify` | ~25% | Literal folding opportunity |
| `ColumnarValue::Scalar` fast-path | Good | Scalar optimization |

### Aggregate UDFs (11+)

| UDAF | Features |
|------|----------|
| `list_unique`, `collect_set` | GroupsAccumulator, null handling |
| `count_distinct_agg`, `count_if` | Sliding accumulator support |
| `any_value_det`, `arg_max`, `arg_min` | Deterministic value selection |
| `asof_select`, `first_value`, `last_value` | Ordered value selection |
| `string_agg` | String concatenation |

All aggregate UDFs implement `groups_accumulator_supported() -> true` and proper state field definitions.

### Window UDFs

Re-exports from `datafusion_functions_window` with aliases:
- `row_number`, `lag`, `lead`

---

## UDF Quality Gate

### Required Invariants

1. **Row-count invariants** - Scalar UDFs must return arrays with same row count as input
2. **Metadata-aware output** - Implement `return_field_from_args` when output depends on input metadata
3. **Short-circuit correctness** - Implement `short_circuits()` for null/constant optimization
4. **Type coercion** - Implement `coerce_types` for multi-typed inputs
5. **Named arguments** - Register parameter names in snapshot for `information_schema` parity

### Code Review Checklist

- [ ] Scalar UDF output length == input batch length
- [ ] `return_field_from_args` used when output metadata depends on input
- [ ] `coerce_types` implemented for multi-typed inputs
- [ ] `simplify` implemented for deterministic expressions
- [ ] Snapshot metadata updated and parity tests pass

---

## Delta Lake Integration

### Control Plane

```rust
pub async fn load_delta_table(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    session_ctx: Option<&SessionContext>,
) -> Result<DeltaTable, DeltaTableError>

pub async fn delta_provider_from_session(
    table_uri: &str,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    overrides: DeltaScanOverrides,
    gate: Option<DeltaFeatureGate>,
    session_ctx: &SessionContext,
) -> Result<DeltaTableProvider, DeltaTableError>
```

### Scan Overrides

```rust
pub struct DeltaScanOverrides {
    pub file_column_name: Option<String>,          // Add _file column
    pub enable_parquet_pushdown: Option<bool>,     // Pushdown filters
    pub schema_force_view_types: Option<bool>,     // Force logical types
    pub wrap_partition_values: Option<bool>,       // Wrap partitions
    pub schema: Option<SchemaRef>,                 // Override schema
}
```

### Mutations

| Operation | Description |
|-----------|-------------|
| `delta_write_ipc` | Write Arrow IPC data to Delta |
| `delta_merge` | MERGE INTO with match/not-match operations |
| `delta_update` | UPDATE with predicate |
| `delta_delete` | DELETE with predicate |

### Maintenance

| Operation | Description |
|-----------|-------------|
| `delta_vacuum` | Remove old files |
| `delta_optimize_compact` | Compact small files |
| `delta_create_checkpoint` | Create checkpoint |
| `delta_restore` | Restore to version/timestamp |
| `delta_add_features` | Add protocol features |

---

## Plugin Architecture

### Design Goals

1. **Independent Deployment** - Update UDFs without rebuilding application
2. **Version Isolation** - Multiple plugin versions coexist
3. **Language Interop** - C ABI allows non-Rust extensions
4. **Sandboxing** - Plugins cannot corrupt host memory

### Plugin ABI Types

```rust
#[repr(C)]
#[derive(StableAbi)]
pub struct DfPluginManifestV1 {
    pub struct_size: u32,
    pub plugin_abi_major: u16,
    pub plugin_abi_minor: u16,
    pub df_ffi_major: u16,
    pub datafusion_major: u16,
    pub arrow_major: u16,
    pub plugin_name: RString,
    pub plugin_version: RString,
    pub build_id: RString,
    pub capabilities: u64,
    pub features: RVec<RString>,
}

#[repr(C)]
#[derive(StableAbi)]
pub struct DfPluginMod {
    pub manifest: extern "C" fn() -> DfPluginManifestV1,
    pub exports: extern "C" fn() -> DfPluginExportsV1,
    pub udf_bundle_with_options: extern "C" fn(...) -> DfResult<DfUdfBundleV1>,
    pub create_table_provider: extern "C" fn(...) -> DfResult<FFI_TableProvider>,
}
```

### Capability Flags

- `TABLE_PROVIDER` - Plugin can create table providers
- `SCALAR_UDF` - Plugin exports scalar UDFs
- `AGG_UDF` - Plugin exports aggregate UDFs
- `WINDOW_UDF` - Plugin exports window UDFs
- `TABLE_FUNCTION` - Plugin exports table functions

### Version Compatibility

| Host ABI | Plugin ABI | Compatible? |
|----------|------------|-------------|
| 1.0 | 1.0 | ✓ Exact match |
| 1.0 | 1.1 | ✓ Minor forward compat |
| 1.1 | 1.0 | ✓ Host supports older minor |
| 1.0 | 2.0 | ✗ Major version mismatch |

---

## PyO3 Bindings

### Architecture

```
Python Process
├── datafusion (Python package)
│   └── _internal.so  ←─ datafusion_python (cdylib)
│       └── datafusion_ext (rlib)
└── datafusion_ext.so  ←─ datafusion_ext_py (cdylib)
    └── datafusion_ext (rlib)
```

### Key Patterns

**GIL Management:**
```rust
#[pyfunction]
fn delta_snapshot_info(py: Python, table_uri: String, ...) -> PyResult<Py<PyBytes>> {
    py.allow_threads(|| {
        let runtime = runtime()?;
        runtime.block_on(async { snapshot_info_with_gate(...).await })
    })
}
```

**Arrow IPC Bridge:**
```rust
fn record_batch_to_ipc(batch: RecordBatch) -> PyResult<Py<PyBytes>> {
    let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    Python::with_gil(|py| Ok(PyBytes::new(py, &writer.into_inner()?).into()))
}
```

### Python Usage

```python
from datafusion import SessionContext
from datafusion_ext import install_function_factory, install_expr_planners

ctx = SessionContext()
install_function_factory(ctx)
install_expr_planners(ctx, ["codeanatomy_domain"])

# Custom functions now available
df = ctx.sql("SELECT stable_hash64(col1, col2) FROM table")
```

---

## Expression Planner

Custom SQL operator overloading:

| Operator | Translation | Example |
|----------|-------------|---------|
| `->`, `->>` | `get_field()` | `struct_col -> 'field'` |
| `#>`, `#>>` | `get_field()` | `map_col #> 'tags' -> 'key'` |
| `@>` | `array_has_all()` | `array1 @> array2` |
| `<@` | `array_has_all()` | `array1 <@ array2` |

---

## Function Factory

Supports `CREATE FUNCTION` SQL statements:

```sql
-- Scalar macro
CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE sql AS 'x + 1';

-- Aggregate macro
CREATE FUNCTION sum_squares(x DOUBLE) RETURNS DOUBLE LANGUAGE aggregate AS 'SUM(x * x)';
```

---

## UDF Deprecation Strategy

### Deprecation Levels

| Level | Behavior |
|-------|----------|
| **Soft** | Flagged in docs, replacement available |
| **Hard** | Emits warning, replacement is default |
| **Removal** | Removed from all registration paths |

### Compatibility Windows

- Soft deprecation: minimum 2 minor releases
- Hard deprecation: minimum 1 minor release after soft
- Removal: minimum 1 minor release after hard

---

## Build Configuration

### Workspace Dependencies

```toml
[workspace.dependencies]
arrow = "57.1.0"
datafusion = { version = "51.0.0", features = ["parquet"] }
deltalake = { version = "0.30.1", features = ["datafusion"] }
pyo3 = "0.26"
tokio = { version = "1.49.0", features = ["rt-multi-thread"] }
blake2 = "0.10"
```

### Feature Flags

| Crate | Feature | Purpose |
|-------|---------|---------|
| `datafusion_ext` | `async-udf` | Enable async UDF support |
| `datafusion_python` | `mimalloc` | Fast allocator |
| `datafusion_python` | `substrait` | Substrait support |

### Build Commands

```bash
# Build all crates
cd rust && cargo build --release

# Build with async UDF
cargo build --release --features async-udf

# Run tests
cargo test --workspace

# Build Python wheels
cd datafusion_python && maturin build --release
```

---

## Packaging Contract

### Artifact Strategy

- **Default**: ABI-stable wheels (PyO3 `abi3`)
- **Baseline**: manylinux2014 for Linux portability
- **Module name**: Importable as `datafusion._internal`

### Required Smoke Tests

1. Import `datafusion._internal` from wheel
2. Validate `datafusion.substrait.Producer.to_substrait_plan` exists
3. Validate `datafusion_ext.capabilities_snapshot()` returns payload

---

## Performance Characteristics

| Operation | Python (ms) | Rust UDF (ms) | Speedup |
|-----------|-------------|---------------|---------|
| `stable_hash64` (1M rows) | 1250 | 12 | 104x |
| `utf8_normalize` (1M strings) | 890 | 35 | 25x |
| `span_overlaps` (1M pairs) | 2100 | 18 | 117x |
| `list_compact` (1M lists) | 1500 | 22 | 68x |

### Memory Efficiency

- **Zero-Copy IPC**: PyArrow C Data Interface avoids serialization
- **Streaming Execution**: Arrow RecordBatch streaming prevents full materialization
- **Columnar Layout**: SIMD-friendly data structures

---

## Cross-References

- **[04_datafusion_integration.md](04_datafusion_integration.md)** - DataFusion engine details
- **[07_storage_and_incremental.md](07_storage_and_incremental.md)** - Delta Lake storage

**Source Files:**
- `rust/datafusion_ext/src/` - Core Rust UDF implementations
- `rust/datafusion_python/` - Python bindings
- `rust/df_plugin_*/` - Plugin system
- `src/datafusion_engine/udf/` - Python UDF integration
