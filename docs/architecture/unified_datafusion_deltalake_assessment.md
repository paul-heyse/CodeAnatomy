# Unified DataFusion + DeltaLake + Semantic Architecture Assessment

**Generated:** 2026-02-03
**Scope:** Comprehensive investigation of CodeAnatomy's DataFusion, DeltaLake, and Semantic layers
**Breaking Changes:** Acceptable (design phase)

---

## Executive Summary

This assessment evaluates the CodeAnatomy codebase's three integration layers—**Semantics**, **DataFusion Engine**, and **Delta Storage**—across API compliance, architectural unification, performance optimization, and maintainability.

### Key Findings Summary

| Area | Status | Priority Items |
|------|--------|----------------|
| **UDF API Compliance** | Excellent | 100% modern API usage, minor simplify() coverage expansion opportunity |
| **Configuration Architecture** | Needs Work | 60+ config classes, severe duplication in DataFusionRuntimeProfile |
| **Schema Contracts** | Needs Work | ContractSpec and SchemaContract have near-total duplication |
| **Observability** | Critical Gap | Storage layer has ZERO OpenTelemetry instrumentation |
| **File Size/Maintainability** | Needs Work | 4 files exceed 1,600 lines, `udf_custom.rs` at ~5K lines |
| **Plan Caching** | Strong | Two-tier caching, deterministic fingerprinting, Substrait replay stubbed |
| **CDF Integration** | Good | Incremental processing works, minor architectural coupling |

### Priority Recommendations

1. **Critical**: Add `SCOPE_STORAGE` observability instrumentation
2. **High**: Modularize `udf_custom.rs` into 7 domain modules
3. **High**: Consolidate ContractSpec and SchemaContract
4. **Medium**: Split `runtime.py` into 4 modules (profile, introspection, telemetry, validation)
5. **Medium**: Fix LargeList gap in `ensure_list_arg()` validation

---

## Phase 1: API Compliance & Best Practices Audit

### 1.1 UDF API Pattern Verification

**Status: Excellent - 100% Modern API Compliance**

All 55+ scalar UDFs in the codebase use modern DataFusion patterns:

| Pattern | Coverage | Notes |
|---------|----------|-------|
| `invoke_with_args()` | 55/55 (100%) | Zero deprecated `invoke()` patterns |
| `return_field_from_args()` | 55/55 (100%) | Full dynamic return type support |
| `ColumnarValue::Scalar` fast-path | 47+ UDFs | Comprehensive scalar optimization |
| `simplify()` literal folding | 8 UDFs | Expansion opportunity |
| `evaluate_bounds()` | 3 UDFs | Interval arithmetic support |
| `with_updated_config()` | 7 UDFs | Session config awareness |

**UDF Distribution by Module:**

```
rust/datafusion_ext/src/
├── udf_custom.rs      (30 UDFs, 4983 lines) - NEEDS MODULARIZATION
├── udf/hash.rs        (7 UDFs) - Well-organized
├── udf/span.rs        (6 UDFs) - Well-organized
├── udf/string.rs      (4 UDFs) - Well-organized
├── udf/collection.rs  (4 UDFs) - Well-organized
├── udf/metadata.rs    (2 UDFs) - Well-organized
├── udf_async.rs       (1 async UDF)
└── udaf_builtin.rs    (11 aggregate UDFs)
```

**Optimization Opportunities:**

1. **Expand `simplify()` coverage** to span/list UDFs for literal folding
2. **Add DashMap caching** for regex-based UDFs (pattern compilation)
3. **Increase interval arithmetic** coverage for hash functions

### 1.2 Arrow Type Handling Assessment

**Status: Good with Specific Gaps**

**Utf8View Support: Comprehensive**

The `expand_string_signatures()` function in `udf/common.rs:68-89` systematically handles all three string types:
- `DataType::Utf8`
- `DataType::LargeUtf8`
- `DataType::Utf8View`

**Critical Gap: LargeList Validation**

```rust
// Current implementation in udf/common.rs:141-147
pub(crate) fn ensure_list_arg(name: &str, arg_type: &DataType) -> Result<()> {
    if matches!(arg_type, DataType::List(_)) {  // MISSING: LargeList
        Ok(())
    } else {
        Err(DataFusionError::Plan(...))
    }
}
```

**Recommendation:** Update to:
```rust
if matches!(arg_type, DataType::List(_) | DataType::LargeList(_)) {
    Ok(())
}
```

**Nested Type Support:**
- ✅ Struct: Proper validation and field extraction
- ✅ Map: Correct structure with keys/values
- ⚠️ List: Missing LargeList variant
- ✅ Complex nesting: Recursive type parsing works

### 1.3 Delta Integration Pattern Assessment

**Status: Good with Architectural Coupling**

**DeltaScanConfig Usage:**

The codebase properly uses `DeltaScanConfig` with builder pattern:
- Storage options separated (data vs log)
- CDF providers integrated with semantic incremental processing
- File pruning uses dual-path evaluation (DataFusion + Python fallback)

**Identified Gap: Synthetic DatasetLocation Reconstruction**

CDF reads reconstruct `DatasetLocation` synthetically rather than passing through the original:

```python
# In cdf_runtime.py - synthetic reconstruction
location = DatasetLocation(
    table_uri=table_uri,
    dataset_name=dataset_name,
    # ... reconstructed fields
)
```

**Impact:** Risk of inconsistency if original location had custom options.

**Recommendation:** Pass original `DatasetLocation` through CDF pipeline.

---

## Phase 2: Architectural Unification Assessment

### 2.1 Configuration Sprawl Analysis

**Status: Critical - 60+ Configuration Classes**

| Layer | Config Classes | Notes |
|-------|----------------|-------|
| Semantics | 15+ | SemanticConfig, CpgBuildOptions, etc. |
| DataFusion Engine | 35+ | DataFusionRuntimeProfile (170+ fields!), policies, settings |
| Storage | 10+ | DeltaWritePolicy, DeltaScanConfig, etc. |

**Critical Finding: DataFusionRuntimeProfile**

Located at `src/datafusion_engine/session/runtime.py:3139`, this class has **170+ fields** including:
- Execution settings (batch_size, target_partitions, memory_limit)
- Feature gates (40+ boolean toggles)
- Policy objects (join, SQL, cache)
- External catalog configurations

**Duplication Examples:**

| Config A | Config B | Overlap |
|----------|----------|---------|
| `CachePolicyConfig` | `DiskCacheSettings` | Cache sizing |
| `DataFusionJoinPolicy` | Session config | Join algorithm selection |
| `SemanticConfig.enable_cdf` | `IncrementalConfig.enabled` | CDF enablement |

**Recommendation: Hierarchical Configuration**

```python
@dataclass(frozen=True)
class UnifiedPipelineConfig:
    """Top-level pipeline configuration."""

    # Composed sub-configs
    execution: ExecutionConfig          # batch_size, partitions, memory
    storage: StorageConfig              # Delta settings, cache policies
    semantic: SemanticConfig            # CPG rules, quality settings
    observability: ObservabilityConfig  # OTel, diagnostics

    def fingerprint(self) -> str:
        """Composite fingerprint from all sub-configs."""
        ...
```

### 2.2 Schema Contract Consolidation

**Status: Critical - Near-Total Duplication**

**Finding:** `ContractSpec` (schema_spec) and `SchemaContract` (datafusion_engine) are functionally identical:

| Feature | ContractSpec | SchemaContract |
|---------|--------------|----------------|
| Schema definition | ✅ PyArrow schema | ✅ PyArrow schema |
| Constraint validation | ✅ | ✅ |
| Fingerprinting | ✅ | ✅ |
| Nullable handling | ✅ | ✅ |

**DatasetLocation Redundancy:**

```python
@dataclass
class DatasetLocation:
    table_uri: str
    dataset_name: str
    schema: pa.Schema          # Redundant with contract
    contract: SchemaContract   # Contains schema
    spec: DatasetSpec          # Contains contract which contains schema
```

**Recommendation:**

1. Merge `SchemaContract` into `ContractSpec`
2. Remove redundant `schema` field from `DatasetLocation`
3. Use `ContractSpec` as the single source of truth

### 2.3 Fingerprinting Strategy Assessment

**Status: Strong - Well-Designed Multi-Layer System**

**Hash Functions Used:**

| Function | Algorithm | Use Case |
|----------|-----------|----------|
| `hash_msgpack_canonical()` | msgpack + SHA256 | Config fingerprints |
| `hash_json_canonical()` | JSON (sorted) + SHA256 | Plan fingerprints |
| `hash64_from_text()` | BLAKE2b 64-bit | Stable IDs |
| `hash128_from_text()` | BLAKE2b 128-bit | Collision-resistant IDs |

**Plan Cache Key (10 components):**
```
profile_hash : substrait_hash : plan_fingerprint : udf_snapshot_hash :
function_registry_hash : information_schema_hash : required_udfs_hash :
required_rewrite_tags_hash : settings_hash : delta_inputs_hash
```

**Collision Risk: Low**
- SHA256 provides strong collision resistance
- Multi-factor keys add orthogonal entropy
- Version components prevent stale cache hits

**Minor Improvement:** Use structured representation (msgpack) instead of colon-delimited strings for cache keys.

### 2.4 Registry Pattern Assessment

**Status: Mixed Protocol Adherence**

**Registry Types Found:**

| Registry | Protocol | Notes |
|----------|----------|-------|
| `DatasetCatalog` | Custom | `register()`, `get()`, `contains()` |
| `ProviderRegistry` | Custom | `register_provider()`, `resolve()` |
| `UdfCatalog` | Custom | `get_udf()`, `refresh()` |
| `ImmutableRegistry` | Protocol | Static module-level registries |
| `MutableRegistry` | Protocol | Runtime-modifiable registries |

**Inconsistencies:**
- Registration signatures vary (`register()` vs `register_provider()` vs `add()`)
- No atomic cross-registry registration
- Mixed use of `Protocol` types vs concrete classes

**Recommendation:** Unified `PipelineRegistry` facade:

```python
class PipelineRegistry:
    """Unified registry coordinating all subsystem registrations."""

    def register_dataset(self, location: DatasetLocation) -> None:
        """Atomically register dataset across catalog and provider registries."""
        with self._registration_lock:
            self.dataset_catalog.register(location)
            self.provider_registry.register_provider(location)
```

### 2.5 Observability Gap Analysis

**Status: Critical - Storage Layer Uninstrumented**

**Current Scope Coverage:**

| Scope | Defined | Instrumented |
|-------|---------|--------------|
| `SCOPE_SEMANTICS` | ✅ | ✅ High coverage |
| `SCOPE_DATAFUSION` | ✅ | ✅ High coverage |
| `SCOPE_HAMILTON` | ✅ | ✅ Moderate coverage |
| `SCOPE_EXTRACTION` | ✅ | ✅ High coverage |
| `SCOPE_STORAGE` | ❌ | ❌ **ZERO instrumentation** |

**Critical Finding:** Storage operations (Delta reads, writes, CDF, file pruning) have no OpenTelemetry spans.

**Recommendation:**

```python
# Add to src/obs/otel/scopes.py
SCOPE_STORAGE: Final = "codeanatomy.storage"

# Instrument storage operations
@stage_span(SCOPE_STORAGE, "delta_read")
def read_delta_table(...):
    ...

@stage_span(SCOPE_STORAGE, "file_pruning")
def prune_files(...):
    ...
```

---

## Phase 3: Performance Optimization Assessment

### 3.1 Caching Effectiveness Analysis

**Status: Strong - Multi-Layer Architecture**

**Cache Tiers:**

| Tier | Type | Purpose | Size |
|------|------|---------|------|
| L1 | In-memory (LRU) | Tree-sitter parse cache | Bounded |
| L2 | DiskCache | Plan/extract/schema caches | 512MB-8GB |
| L3 | Delta-backed | Cache inventory, run summaries | Persistent |

**Per-Kind Configuration:**

| Kind | Size Limit | Shards | TTL | Policy |
|------|-----------|--------|-----|--------|
| `plan` | 512 MB | 1 | None | LRU |
| `extract` | 8 GB | 8 | 24 hrs | LRU |
| `schema` | 256 MB | 1 | 5 min | LRU |
| `repo_scan` | 512 MB | 1 | 30 min | LRU |

**Optimization Opportunities:**

1. **PlanProtoCache hit/miss tracking**: Add counters for proto rehydration effectiveness
2. **Extract TTL granularity**: Consider per-extractor TTL (AST vs bytecode)
3. **Plan cache key simplification**: Use msgpack instead of colon-delimited strings

### 3.2 Plan Optimization Analysis

**Status: Strong - Complete Infrastructure**

**Plan Walking:**
- Complete graph traversal with cycle detection (`walk_logical_complete()`)
- Embedded subplan discovery via `to_variant()` introspection
- Graceful error handling (non-fatal degradation)

**Optimizer Control Layers:**

1. **Feature Gates**: 40+ boolean toggles (dynamic filter pushdown, join strategies)
2. **Join Policy**: Algorithm selection (hash, sort-merge, nested-loop)
3. **Rust Rules**: `install_function_factory_native()` for custom optimization

**Two-Pass Planning:**
1. **Pass 1**: Baseline planning (cold, without scan pins)
2. **Pass 2**: Pinned planning (Delta versions locked)

**Gap: Substrait Replay Stubbed**

```python
def replay_substrait_bytes(ctx: SessionContext, payload: bytes) -> DataFrame:
    raise ValueError("Substrait replay is unavailable in this build.")
```

**Impact:** Cache hits still require re-optimization. Implementing replay could yield 60-80% improvement on repeated queries.

### 3.3 Memory Management Assessment

**Status: Good - Configurable Tiered System**

**Memory Pool Options:**

| Pool | Description | Use Case |
|------|-------------|----------|
| `greedy` | Single consumer takes all | Single-query workloads |
| `fair` | Proportional allocation | Multi-tenant/concurrent |
| `unbounded` | No limits | Development only |

**Environment Presets:**

| Preset | Memory | Temp Dir | Concurrency |
|--------|--------|----------|-------------|
| DEV | 4 GB | 50 GB | 2-4 |
| DEFAULT | 8 GB | 100 GB | 8 |
| PROD | 16 GB | 200 GB | 16 |

**Optimization Opportunities:**

1. **Batch size tuning**: Current default is DataFusion's default; consider workload-specific settings
2. **Write parallelism**: All settings None; configure for high-throughput writes
3. **Spill directory**: Always configure to SSD for predictable behavior

---

## Phase 4: Maintainability Assessment

### 4.1 Large File Decomposition Analysis

**Status: Critical - 4 Files Require Splitting**

| File | Lines | Classes | Functions | Priority |
|------|-------|---------|-----------|----------|
| `runtime.py` | 6,693 | 15 | 130+ | HIGH |
| `udf_custom.rs` | 4,983 | 32 | ~40 | HIGH |
| `pipeline.py` | 2,075 | 4 | 60+ | HIGH |
| `compiler.py` | 1,695 | 1 | 35+ | MEDIUM |

**Recommended Decomposition:**

#### `runtime.py` → 4 Modules
```
src/datafusion_engine/session/
├── runtime.py          (core: DataFusionRuntimeProfile, SessionRuntime)
├── profile.py          (~550 lines: policy classes)
├── introspection.py    (~450 lines: catalog snapshots)
├── telemetry.py        (~600 lines: telemetry payloads)
└── validation.py       (~400 lines: schema/constraint validation)
```

#### `udf_custom.rs` → 7 Modules
```
rust/datafusion_ext/src/udf/
├── mod.rs              (re-exports)
├── hash.rs             (~900 lines: stable_hash64, etc.)
├── span_ops.rs         (~650 lines: span_make, span_overlaps)
├── text_normalize.rs   (~450 lines: utf8_normalize, qname_normalize)
├── containers.rs       (~400 lines: map_get_default, list_compact)
├── cdf.rs              (~200 lines: cdf_change_rank, etc.)
├── position.rs         (~200 lines: position_encoding, col_to_byte)
└── policy.rs           (~1100 lines: install_function_factory_native)
```

#### `pipeline.py` → 3 Modules
```
src/semantics/pipeline/
├── __init__.py         (re-exports)
├── builders.py         (~500 lines: _builder_for_* functions)
├── orchestration.py    (~600 lines: build_cpg, _materialize_semantic_outputs)
└── diagnostics.py      (~250 lines: quality emission)
```

### 4.2 Plugin Architecture Assessment

**Status: Strong with Minor Gaps**

**ABI Versioning:**
- Multi-level: plugin ABI major/minor, DataFusion FFI, DataFusion semver, Arrow semver
- Struct size validation for forward compatibility
- Uses `abi_stable` crate for proven ABI safety

**Native vs Plugin Parity:**

| Feature | Native | Plugin | Gap |
|---------|--------|--------|-----|
| Scalar UDFs | 32 | 32 | ✅ None |
| Aggregate UDFs | 11 | 11 | ✅ None |
| Window UDFs | 3 | 3 | ✅ None |
| SQL Macros | ✅ | ❌ | ⚠️ Native only |
| Expression Planners | ✅ | ❌ | ⚠️ Native only |

**Recommendation:** Document SQL macro and expression planner limitations in plugin compatibility notes.

### 4.3 Semantic Pipeline Architecture

**Status: Well-Designed with Decomposition Opportunities**

**10 Semantic Rules:**

| # | Rule | Implementation |
|---|------|----------------|
| 1 | Derive Entity ID | `stable_id_parts` Rust UDF |
| 2 | Derive Span | `span_make` Rust UDF |
| 3 | Normalize Text | `utf8_normalize` Rust UDF |
| 4 | Path Join | `DataFrame.join()` on path |
| 5 | Span Overlap | `span_overlaps` filter |
| 6 | Span Contains | `span_contains` filter |
| 7 | Relation Project | `.select()` to 6-column edge schema |
| 8 | Union | `.union()` with discriminator |
| 9 | Aggregate | `.aggregate()` with `array_agg` |
| 10 | Dedupe | `row_number()` window function |

**Quality-Aware Compilation:**
- Three-tier signals: Hard predicates, soft features, file quality
- Deterministic ranking via `row_number()` window
- Confidence computation with quality adjustment

**CDF Integration:**
- Cursor-based version tracking
- Multiple merge strategies (append, upsert, replace)
- Automatic incremental output detection

---

## Implementation Roadmap

### Short-Term: Quick Wins (1-2 weeks)

| Item | Impact | Effort | Files |
|------|--------|--------|-------|
| Add `SCOPE_STORAGE` instrumentation | Critical | Low | `scopes.py`, storage modules |
| Fix `ensure_list_arg()` LargeList gap | High | Low | `udf/common.rs` |
| Add DashMap caching for regex UDFs | Medium | Low | `udf_custom.rs` |
| Document plugin SQL macro limitations | Low | Low | Docs |

### Medium-Term: Architectural Consolidation (1-2 months)

| Item | Impact | Effort | Files |
|------|--------|--------|-------|
| Modularize `udf_custom.rs` | High | Medium | Rust UDF layer |
| Split `runtime.py` into 4 modules | High | Medium | Session layer |
| Merge ContractSpec/SchemaContract | High | Medium | Schema layer |
| Implement hierarchical config | Medium | Medium | Config layer |
| Expand `simplify()` coverage | Medium | Medium | UDF layer |

### Long-Term: Major Refactoring (3-6 months)

| Item | Impact | Effort | Files |
|------|--------|--------|-------|
| Implement Substrait replay | High | High | Plan execution |
| Unified PipelineRegistry | Medium | High | Registry layer |
| Decompose quality compilation | Medium | High | Compiler |
| CDF logic centralization | Low | Medium | Incremental layer |

---

## Appendix A: Critical Files Reference

### Rust Layer
- `rust/datafusion_ext/src/udf_custom.rs` - All scalar UDFs (4,983 lines)
- `rust/datafusion_ext/src/udaf_builtin.rs` - Aggregate UDFs
- `rust/datafusion_ext/src/compat.rs` - Version compatibility
- `rust/df_plugin_api/src/lib.rs` - Plugin ABI

### Python - DataFusion Engine
- `src/datafusion_engine/session/runtime.py` - Master runtime profile (6,772 lines)
- `src/datafusion_engine/plan/bundle.py` - Plan bundling with Substrait
- `src/datafusion_engine/plan/cache.py` - Two-tier plan caching
- `src/datafusion_engine/udf/platform.py` - Rust UDF platform installation
- `src/datafusion_engine/dataset/registry.py` - DatasetLocation + DatasetCatalog

### Python - Semantics
- `src/semantics/compiler.py` - SemanticCompiler with 10 rules
- `src/semantics/pipeline.py` - CPG build orchestration
- `src/semantics/quality.py` - Quality-aware relationship building
- `src/semantics/incremental/cdf_runtime.py` - CDF incremental processing

### Python - Storage
- `src/storage/deltalake/file_pruning.py` - File pruning dual-path evaluation
- `src/storage/deltalake/delta.py` - Core Delta operations

### Shared Utilities
- `src/utils/hashing.py` - Canonical hashing utilities
- `src/utils/registry_protocol.py` - Registry protocols
- `src/core/config_base.py` - FingerprintableConfig protocol

---

## Appendix B: Configuration Class Inventory

| Module | Class | Fields | Fingerprinted |
|--------|-------|--------|---------------|
| `session/runtime.py` | `DataFusionRuntimeProfile` | 170+ | Yes |
| `session/runtime.py` | `DataFusionConfigPolicy` | 12 | Yes |
| `session/runtime.py` | `DataFusionFeatureGates` | 40+ | Yes |
| `session/runtime.py` | `DataFusionJoinPolicy` | 7 | Yes |
| `session/cache_policy.py` | `CachePolicyConfig` | 3 | No |
| `compile/options.py` | `DataFusionSqlPolicy` | 3 | Yes |
| `storage/deltalake/config.py` | `DeltaWritePolicy` | 8 | Yes |
| `semantics/config.py` | `SemanticConfig` | 15+ | Yes |
| `cache/diskcache_factory.py` | `DiskCacheSettings` | 10 | No |
| `cache/diskcache_factory.py` | `DiskCacheProfile` | 4 | Yes |

---

## Appendix C: Metrics Collection Points

### Recommended Metrics

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `plan_cache_hit_total` | Counter | kind, profile | Cache effectiveness |
| `plan_cache_miss_total` | Counter | kind, profile | Cache miss rate |
| `delta_files_scanned` | Counter | table | File pruning effectiveness |
| `delta_files_pruned` | Counter | table | Pruning ratio |
| `udf_execution_seconds` | Histogram | udf_name | UDF performance |
| `semantic_join_rows` | Histogram | join_type | Join cardinality |
| `storage_operation_seconds` | Histogram | operation | Storage latency |

---

*End of Assessment*
