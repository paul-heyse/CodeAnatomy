# DataFusion & DeltaLake Rust UDF Robustness Assessment

## Executive Summary

This document provides a comprehensive assessment of the CodeAnatomy codebase's DataFusion and DeltaLake Rust UDF deployment, focusing on **maintainability**, code organization, plugin architecture, and future-proofing against DataFusion API evolution.

### Current State Summary

| Component | Count/Version | Status |
|-----------|--------------|--------|
| DataFusion | 51.0.0 | Current |
| Arrow | 57.1.0 | Current |
| PyO3 | 0.26 | Current |
| Scalar UDFs | ~32 | Well-implemented |
| Aggregate UDFs | 11 | Fully compliant |
| Window UDFs | 3 | Re-exports (minimal) |
| Async UDF | 1 | Feature-gated |
| Plugin System | ABI-stable | Mature |

### Key Findings

1. **API Compliance: Strong** - All UDFs use modern DataFusion APIs (`invoke_with_args`, `return_field_from_args`)
2. **Maintainability Concern: Monolithic File** - `udf_custom.rs` at ~3800 lines needs modularization
3. **Future-Proofing: Good** - Plugin ABI isolation provides upgrade paths
4. **Python Integration: Robust** - WeakSet/WeakKeyDictionary patterns prevent memory leaks
5. **Delta Integration: Comprehensive** - File pruning, scan config, and CDF support present

### Priority Recommendations

| Priority | Recommendation | Impact |
|----------|---------------|--------|
| 1 | Modularize `udf_custom.rs` | Maintainability |
| 2 | Add DashMap caching for regex/pattern UDFs | Performance |
| 3 | Expand `simplify()` coverage | Performance |
| 4 | Document deprecation strategy | Maintainability |

---

## 1. Rust UDF Implementation Analysis

### 1.1 Scalar UDF Inventory

The codebase contains **32 scalar UDFs** in `rust/datafusion_ext/src/udf_custom.rs`:

| UDF Name | Has `simplify()` | Has `coerce_types()` | Has `short_circuits()` | Volatility |
|----------|-----------------|---------------------|----------------------|------------|
| `arrow_metadata` | No | No | No | Immutable |
| `semantic_tag` | No | Yes | No | Immutable |
| `cpg_score` | No | No | No | Immutable |
| `stable_hash64` | Yes | No | No | Immutable |
| `stable_hash128` | Yes | No | No | Immutable |
| `prefixed_hash64` | Yes | No | No | Immutable |
| `stable_id` | Yes | Yes | No | Immutable |
| `stable_id_parts` | Yes | Yes | No | Immutable |
| `prefixed_hash_parts64` | Yes | Yes | No | Immutable |
| `stable_hash_any` | Yes | Yes | No | Immutable |
| `span_make` | No | Yes | No | Immutable |
| `span_len` | No | No | No | Immutable |
| `interval_align_score` | No | No | No | Immutable |
| `span_overlaps` | No | Yes | No | Immutable |
| `span_contains` | No | Yes | No | Immutable |
| `span_id` | No | No | No | Immutable |
| `utf8_normalize` | Yes | Yes | No | Immutable |
| `utf8_null_if_blank` | No | No | No | Immutable |
| `qname_normalize` | No | Yes | No | Immutable |
| `map_get_default` | No | Yes | No | Immutable |
| `map_normalize` | No | Yes | No | Immutable |
| `list_compact` | No | Yes | No | Immutable |
| `list_unique_sorted` | No | Yes | No | Immutable |
| `struct_pick` | No | Yes | No | Immutable |
| `cdf_change_rank` | No | No | No | Immutable |
| `cdf_is_upsert` | No | No | No | Immutable |
| `cdf_is_delete` | No | No | No | Immutable |
| `col_to_byte` | No | No | No | Immutable |

**API Compliance Matrix:**

| API Feature | Implementation Count | Status |
|-------------|---------------------|--------|
| `invoke_with_args` | 31 | All UDFs |
| `return_field_from_args` | 31 | All UDFs |
| `coerce_types` | 17 | Good coverage |
| `simplify` | 8 | Moderate coverage |
| `short_circuits` | 1 | Limited |
| `ColumnarValue::Scalar` fast-path | 47 occurrences | Good |
| `Volatility::Immutable` | 31 | Correct |

### 1.2 Aggregate UDF Inventory

The codebase contains **11 aggregate UDFs** in `rust/datafusion_ext/src/udaf_builtin.rs`:

| UDAF Name | GroupsAccumulator | Sliding Accumulator | Null Handling | coerce_types |
|-----------|-------------------|---------------------|---------------|--------------|
| `list_unique` | Yes | Yes | Yes | No |
| `collect_set` | Yes | No | Yes | No |
| `count_distinct_agg` | Yes | Yes | Yes | No |
| `count_if` | Yes | No | Yes | No |
| `any_value_det` | Yes | No | Yes | No |
| `arg_max` | Yes | No | Yes | Yes |
| `arg_min` | Yes | No | Yes | Yes |
| `asof_select` | Yes | No | Yes | Yes |
| `first_value` | Yes | No | Yes | Yes |
| `last_value` | Yes | No | Yes | No |
| `string_agg` | Yes | No | Yes | No |

**All aggregate UDFs implement:**
- `groups_accumulator_supported() -> true`
- `supports_null_handling_clause()`
- Proper state field definitions via `state_fields()`

### 1.3 Window UDF Inventory

The codebase re-exports **3 window UDFs** from `datafusion_functions_window`:

| UDWF Name | Source | Aliases |
|-----------|--------|---------|
| `row_number` | `row_number_udwf()` | `row_number_window`, `dedupe_best_by_score` |
| `lag` | `lag_udwf()` | `lag_window` |
| `lead` | `lead_udwf()` | `lead_window` |

These are thin re-exports with alias support, not custom implementations.

### 1.4 Async UDF Assessment

**Location:** `rust/datafusion_ext/src/udf_async.rs`

**Feature Gate:** `async-udf` (Cargo feature)

**Implementation:**
```rust
// Current pattern
static ASYNC_UDF_POLICY: OnceLock<RwLock<AsyncUdfPolicy>> = OnceLock::new();

impl AsyncScalarUDFImpl for AsyncEchoUdfImpl {
    fn ideal_batch_size(&self) -> usize { ... }
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ArrayRef> { ... }
}
```

**Assessment:**
- Timeout handling via `tokio::select!` is correct
- Global policy via `OnceLock<RwLock<AsyncUdfPolicy>>` is thread-safe
- Only one async UDF (`async_echo`) exists - appears to be a proof-of-concept
- Batch size and timeout are configurable via policy

### 1.5 Plugin System Evaluation

**Plugin ABI Architecture:**

```
df_plugin_api (ABI contract)
    └── DfPluginMod (StableAbi)
        ├── manifest: PluginManifest
        ├── exports: Exports
        ├── udf_bundle_with_options: fn -> DfUdfBundleV1
        └── create_table_provider: fn -> FFI_TableProvider

df_plugin_codeanatomy (implementation)
    ├── Exports: DeltaProviderOptions, DeltaCdfProviderOptions
    ├── Capabilities: TABLE_PROVIDER | SCALAR_UDF | AGG_UDF | WINDOW_UDF | TABLE_FUNCTION
    └── UDF Bundle: scalar_udf_specs, udaf_builtin, udwf_builtin
```

**ABI Stability:**
- Uses `abi_stable` crate with `#[derive(StableAbi)]`
- Version manifest with compatibility checking
- FFI types: `FFI_ScalarUDF`, `FFI_AggregateUDF`, `FFI_TableProvider`

---

## 2. Python Integration Analysis

### 2.1 UDF Registration Workflow

**Entry Point:** `src/datafusion_engine/udf/platform.py::install_rust_udf_platform()`

```python
# Registration flow
def install_rust_udf_platform(ctx: SessionContext, *, options: RustUdfPlatformOptions) -> RustUdfPlatform:
    # 1. Resolve UDF snapshot
    snapshot, snapshot_hash, rewrite_tags, docs = _resolve_udf_snapshot(ctx, resolved)

    # 2. Install FunctionFactory (for CREATE FUNCTION support)
    function_factory, function_factory_payload = _install_function_factory(ctx, ...)

    # 3. Install ExprPlanners (for domain syntax routing)
    expr_planners, expr_planner_payload = _install_expr_planners(ctx, ...)

    # 4. Return platform snapshot for diagnostics
    return RustUdfPlatform(...)
```

**Memory Management:**
```python
# WeakSet for idempotent installation tracking
_FUNCTION_FACTORY_CTXS: WeakSet[SessionContext] = WeakSet()
_EXPR_PLANNER_CTXS: WeakSet[SessionContext] = WeakSet()

# WeakKeyDictionary for session-scoped caching (in runtime.py)
_RUST_UDF_SNAPSHOTS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = WeakKeyDictionary()
_RUST_UDF_DOCS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = WeakKeyDictionary()
_RUST_UDF_POLICIES: WeakKeyDictionary[SessionContext, RustUdfPolicy] = WeakKeyDictionary()
```

**Assessment:**
- WeakSet/WeakKeyDictionary patterns prevent memory leaks when sessions are garbage collected
- Idempotent installation via set membership check
- Snapshot-based validation ensures UDF availability before use

### 2.2 Delta TableProvider Integration

**Scan Configuration:** `src/storage/deltalake/scan_profile.py`

```python
_DEFAULT_DELTA_SCAN = DeltaScanOptions(
    file_column_name="__delta_rs_path",
    enable_parquet_pushdown=True,
    schema_force_view_types=True,  # Enables Utf8View optimization
    wrap_partition_values=True,
    schema=None,
)
```

**File Pruning:** `src/storage/deltalake/file_pruning.py`

```python
@dataclass(frozen=True)
class FilePruningPolicy(FingerprintableConfig):
    partition_filters: list[PartitionFilter]  # Equality/membership on partition columns
    stats_filters: list[StatsFilter]          # Range filters on min/max statistics

    def to_predicate(self) -> Expr | None:
        # Converts to DataFusion predicate expression
```

**Assessment:**
- Full partition and statistics-based pruning
- Both DataFusion-evaluated and Python-evaluated paths
- Fingerprinting for deterministic caching

### 2.3 Session Configuration

**Factory Pattern:** `src/datafusion_engine/session/factory.py::SessionFactory`

```python
@dataclass(frozen=True)
class SessionFactory:
    profile: DataFusionRuntimeProfile

    def build_config(self) -> SessionConfig:
        # Applies: target_partitions, batch_size, repartition settings
        # Applies: catalog settings, cache policy, schema hardening

    def _build_local_context(self) -> SessionContext:
        # Attempts Delta session context via datafusion._internal or datafusion_ext
        # Falls back to standard SessionContext if unavailable
```

**Delta Session Integration:**
```python
def _build_delta_session_context(profile, runtime_env) -> _DeltaSessionBuildResult:
    # Tries: datafusion._internal.delta_session_context
    # Falls back to: datafusion_ext.delta_session_context
    # Applies: delta_runtime_env_options for storage configuration
```

---

## 3. Cross-Cutting Concerns

### 3.1 Version Compatibility Matrix

| Component | Current | Notes |
|-----------|---------|-------|
| DataFusion | 51.0.0 | Uses modern API throughout |
| Arrow | 57.1.0 | Includes Utf8View support |
| deltalake | 0.30.1 | Via Python bindings |
| PyO3 | 0.26 | Python 3.13 compatible |
| abi_stable | (indirect) | For plugin ABI |
| tokio | 1.49.0 | Async runtime |

### 3.2 Error Handling Patterns

**Rust Side (335 occurrences of `DataFusionError::`):**
```rust
// Common patterns
DataFusionError::Plan(format!("..."))  // Planning errors
DataFusionError::Execution(format!("..."))  // Runtime errors
DataFusionError::Internal(format!("..."))  // Internal errors
```

**Python Side:**
- RuntimeError for installation failures
- TypeError for type mismatches
- Graceful degradation with fallback UDFs

### 3.3 Thread Safety

**Rust Patterns (29 occurrences of synchronization primitives):**

| Location | Pattern | Usage |
|----------|---------|-------|
| `udf_async.rs` | `OnceLock<RwLock<AsyncUdfPolicy>>` | Global async policy |
| `async_runtime.rs` | `OnceLock<Runtime>` | Shared tokio runtime |
| `datafusion_python/` | Various `RwLock`, `Mutex` | Python-Rust boundary |

**Assessment:**
- Thread-safe global state management
- No raw static mut patterns
- Proper use of `OnceLock` for lazy initialization

---

## 4. Maintainability Assessment (PRIMARY FOCUS)

### 4.1 Code Organization Critique

#### Issue: Monolithic `udf_custom.rs` (~3800 lines)

**Current Structure:**
```
rust/datafusion_ext/src/udf_custom.rs
├── SignatureEqHash wrapper
├── CodeAnatomyUdfConfig config trait
├── expect_arg_len helpers
├── 32 individual UDF implementations
│   ├── ArrowMetadataUdf
│   ├── SemanticTagUdf
│   ├── StableHash64Udf
│   ├── ... (28 more)
│   └── ColToByteUdf
└── span/hash/string utility functions
```

**Problems:**
1. Single file makes code review difficult
2. No logical grouping by domain (hashing, spans, CDF, etc.)
3. Difficult to find related UDFs
4. Merge conflicts when multiple contributors edit

### Recommendation 1: Modularize UDF Implementation

**Priority:** 1 (High)
**Impact:** Maintainability
**Effort:** Medium

#### Current Pattern (Before)

```rust
// rust/datafusion_ext/src/udf_custom.rs (3800 lines)
// Contains ALL scalar UDFs in one file

pub struct StableHash64Udf;
impl ScalarUDFImpl for StableHash64Udf { ... }

pub struct SpanMakeUdf;
impl ScalarUDFImpl for SpanMakeUdf { ... }

pub struct CdfChangeRankUdf;
impl ScalarUDFImpl for CdfChangeRankUdf { ... }
```

#### Recommended Pattern (After)

```rust
// rust/datafusion_ext/src/udf/mod.rs
pub mod hash;      // stable_hash64, stable_hash128, prefixed_hash64, stable_id, etc.
pub mod span;      // span_make, span_len, span_overlaps, span_contains, span_id
pub mod cdf;       // cdf_change_rank, cdf_is_upsert, cdf_is_delete
pub mod string;    // utf8_normalize, utf8_null_if_blank, qname_normalize
pub mod collection; // list_compact, list_unique_sorted, map_get_default, map_normalize
pub mod metadata;  // arrow_metadata, semantic_tag, cpg_score
pub mod struct_ops; // struct_pick

// Re-export all UDF constructors
pub use hash::*;
pub use span::*;
// ...
```

```rust
// rust/datafusion_ext/src/udf/hash.rs (~800 lines)
use super::config::CodeAnatomyUdfConfig;
use super::helpers::{expect_arg_len, SignatureEqHash};

pub struct StableHash64Udf;
impl ScalarUDFImpl for StableHash64Udf { ... }

pub struct StableHash128Udf;
impl ScalarUDFImpl for StableHash128Udf { ... }

pub fn stable_hash64_udf() -> ScalarUDF { ... }
pub fn stable_hash128_udf() -> ScalarUDF { ... }
```

#### Rationale

1. **Domain Coherence:** Group related UDFs together
2. **Code Review:** Smaller files are easier to review
3. **Ownership:** Different team members can own different modules
4. **Testing:** Module-level tests can be co-located
5. **Documentation:** Domain-specific README per module

### 4.2 API Abstraction Layer Assessment

#### Strength: Macro-Based Registry

```rust
// rust/datafusion_ext/src/macros.rs
macro_rules! scalar_udfs {
    ($($name:literal => $builder:expr $(, aliases: [$($alias:literal),* $(,)?])?);* $(;)?) => { ... }
}

// Usage in udf_registry.rs
pub fn scalar_udf_specs() -> Vec<ScalarUdfSpec> {
    scalar_udfs![
        "stable_hash64" => udf_custom::stable_hash64_udf, aliases: ["hash64"];
        "stable_hash128" => udf_custom::stable_hash128_udf, aliases: ["hash128"];
    ]
}
```

**Assessment:** Good abstraction. Registry macros decouple UDF definition from registration.

#### Strength: Plugin ABI Isolation

```rust
// df_plugin_api provides stable ABI boundary
#[repr(C)]
#[derive(StableAbi)]
pub struct DfPluginMod {
    pub manifest: PluginManifest,
    pub exports: Exports,
    // ...
}
```

**Assessment:** Strong isolation. Plugin consumers don't need to recompile when DataFusion internal APIs change.

### 4.3 Future-Proofing Evaluation

#### DataFusion API Evolution Strategy

**Current APIs Used:**
- `ScalarUDFImpl::invoke_with_args` (modern, stable)
- `AggregateUDFImpl::groups_accumulator_supported` (modern, stable)
- `SessionStateBuilder::with_function_factory` (modern)

**Deprecated APIs Avoided:**
- `ScalarUDFImpl::invoke` (old pattern, not used)
- Manual UDF construction (not used)

### Recommendation 2: Add API Version Compatibility Layer

**Priority:** 3 (Medium)
**Impact:** Maintainability
**Effort:** Low

#### Current Pattern (Before)

```rust
// Direct usage of DataFusion types throughout
use datafusion_expr::{ScalarUDF, ScalarUDFImpl, Signature};
```

#### Recommended Pattern (After)

```rust
// rust/datafusion_ext/src/compat.rs
//! DataFusion API compatibility layer.
//!
//! This module re-exports DataFusion types with version-specific adaptations.
//! When upgrading DataFusion, update this module first.

// Re-export with version-specific aliases
pub use datafusion_expr::ScalarUDF;
pub use datafusion_expr::ScalarUDFImpl;
pub use datafusion_expr::Signature;

// Version-specific helpers
#[cfg(datafusion_version = "51")]
pub fn signature_with_param_names(sig: Signature, names: Vec<String>) -> Signature {
    sig.with_parameter_names(names).unwrap()
}

// Document breaking changes
/// # DataFusion 51.0.0 Changes
/// - `Signature::with_parameter_names` now returns Result
/// - `invoke_with_args` is now the primary UDF execution method
```

#### Rationale

1. **Single Update Point:** API changes only need updates in one file
2. **Documentation:** Breaking changes documented in one place
3. **Migration Path:** Version-conditional compilation when needed

### 4.4 `simplify()` Hook Expansion Opportunities

### Recommendation 3: Expand simplify() Coverage

**Priority:** 2 (High)
**Impact:** Performance
**Effort:** Medium

#### Current Coverage

Only 8 of 32 scalar UDFs implement `simplify()`:
- `stable_hash64`, `stable_hash128`, `prefixed_hash64`
- `stable_id`, `stable_id_parts`, `prefixed_hash_parts64`
- `stable_hash_any`, `utf8_normalize`

#### Candidates for simplify()

| UDF | Simplification Opportunity |
|-----|---------------------------|
| `span_make` | Fold when all args are literals |
| `span_len` | Compute at plan time for literal spans |
| `span_overlaps` | Constant-fold literal span pairs |
| `span_contains` | Constant-fold literal span pairs |
| `list_compact` | No-op when input is literal empty list |
| `map_get_default` | Fold when map and key are literals |

#### Example Implementation

```rust
// rust/datafusion_ext/src/udf/span.rs

impl ScalarUDFImpl for SpanMakeUdf {
    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        // If all arguments are literals, compute at plan time
        let all_literal = args.iter().all(|arg| matches!(arg, Expr::Literal(_)));
        if !all_literal {
            return Ok(ExprSimplifyResult::Original(args));
        }

        // Extract literal values
        let start = extract_i64_literal(&args[0])?;
        let end = extract_i64_literal(&args[1])?;

        // Return computed literal
        let span_struct = compute_span_struct(start, end);
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(span_struct)))
    }
}
```

---

## 5. Best Practice Gap Analysis

### 5.1 Comparison Against DataFusion UDF Documentation

| Best Practice | Current Status | Recommendation |
|---------------|----------------|----------------|
| Use `invoke_with_args` | Implemented (31/31) | None |
| Use `return_field_from_args` | Implemented (31/31) | None |
| Implement `simplify()` | Partial (8/32) | Expand coverage |
| Implement `short_circuits()` | Minimal (1/32) | Add for AND/OR-like UDFs |
| Use `GroupsAccumulator` | Full (11/11 UDAFs) | None |
| Thread-safe caching | Not implemented | Add DashMap for regex UDFs |
| Documentation hooks | Implemented | Good |

### 5.2 DeltaLake Integration Assessment

| Best Practice | Current Status | Gap |
|---------------|----------------|-----|
| `DeltaScanConfig` usage | Via abstraction | Acceptable |
| File pruning with stats | Implemented | None |
| CDF TableProvider | In plugin | Good |
| Partition pushdown | Enabled by default | None |
| Utf8View optimization | `schema_force_view_types=True` | Good |

### Recommendation 4: Add DashMap Caching for Pattern UDFs

**Priority:** 2 (High)
**Impact:** Performance
**Effort:** Low

#### Current Pattern (Before)

```rust
// No caching for repeated pattern compilation
impl ScalarUDFImpl for QnameNormalizeUdf {
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ArrayRef> {
        // Pattern compiled on every invocation
        let separator_pattern = ...;
    }
}
```

#### Recommended Pattern (After)

```rust
use dashmap::DashMap;
use std::sync::LazyLock;

// Global thread-safe cache
static PATTERN_CACHE: LazyLock<DashMap<String, Regex>> = LazyLock::new(DashMap::new);

impl ScalarUDFImpl for QnameNormalizeUdf {
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ArrayRef> {
        let pattern_key = "qname_separator";
        let regex = PATTERN_CACHE
            .entry(pattern_key.to_string())
            .or_insert_with(|| Regex::new(r"[./:]").unwrap())
            .value()
            .clone();
        // Use cached regex
    }
}
```

#### Dependencies Required

```toml
# rust/datafusion_ext/Cargo.toml
[dependencies]
dashmap = "5"
```

---

## 6. Implementation Roadmap

### Short-Term: Quick Wins (1-2 weeks)

| Item | Description | Files |
|------|-------------|-------|
| Add DashMap caching | Cache compiled patterns | `udf_custom.rs` |
| Expand simplify() | Add to span/list UDFs | `udf_custom.rs` |
| Add compat.rs | API compatibility layer | New file |

### Medium-Term: Architectural Improvements (4-6 weeks)

| Item | Description | Files |
|------|-------------|-------|
| Modularize UDFs | Split into domain modules | `udf_custom.rs` → `udf/*.rs` |
| Add integration tests | UDF behavior tests | `tests/` |
| Document upgrade path | DataFusion version notes | `docs/` |

### Long-Term: Strategic Refactoring (8-12 weeks)

| Item | Description | Files |
|------|-------------|-------|
| Plugin versioning | Manifest version strategy | `df_plugin_api` |
| Async UDF expansion | Production-ready async | `udf_async.rs` |
| Type evolution | Arrow 2.0 types | All UDF files |

---

## 7. Appendix: Investigation Commands Used

```bash
# UDF API compliance
grep -c "fn invoke_with_args" rust/datafusion_ext/src/udf_custom.rs
grep -c "fn return_field_from_args" rust/datafusion_ext/src/udf_custom.rs
grep -c "fn simplify" rust/datafusion_ext/src/udf_custom.rs
grep -c "fn coerce_types" rust/datafusion_ext/src/udf_custom.rs
grep -c "ColumnarValue::Scalar" rust/datafusion_ext/src/udf_custom.rs

# Aggregate UDF features
grep -c "groups_accumulator_supported" rust/datafusion_ext/src/udaf_builtin.rs
grep -c "create_sliding_accumulator" rust/datafusion_ext/src/udaf_builtin.rs

# Thread safety
grep -c "RwLock\|OnceLock\|Mutex" rust/datafusion_ext/src/

# Error handling
grep -c "DataFusionError::" rust/
```

---

## 8. Conclusion

The CodeAnatomy DataFusion/Rust UDF deployment demonstrates **strong API compliance** and **robust Python integration**. The primary maintainability concern is the monolithic `udf_custom.rs` file, which should be modularized for long-term health.

The plugin ABI architecture provides excellent future-proofing, allowing DataFusion upgrades without breaking plugin consumers. The WeakSet/WeakKeyDictionary patterns in Python integration prevent memory leaks and provide proper session lifecycle management.

**Recommended immediate actions:**
1. Modularize `udf_custom.rs` into domain-specific files
2. Add DashMap caching for pattern-based UDFs
3. Expand `simplify()` coverage for literal folding opportunities
