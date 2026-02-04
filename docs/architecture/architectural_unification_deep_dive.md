# Architectural Unification Deep-Dive Assessment

> **Document Type**: Architecture Assessment
> **Status**: Complete Investigation
> **Breaking Changes**: Acceptable (design phase)
> **Last Updated**: 2026-02-03

---

## Executive Summary

This deep-dive investigation analyzed 6 critical architectural unification opportunities across the CodeAnatomy codebase. The findings reveal significant complexity hotspots and consolidation opportunities that can reduce technical debt while improving maintainability, observability, and developer experience.

### Key Metrics

| Area | Current State | Proposed State | Complexity Reduction |
|------|---------------|----------------|---------------------|
| Configuration Classes | **183 classes** across 107 files | Hierarchical system with 6 sub-configs | ~60% field reduction |
| Schema Contract Types | **6 types** with 9 redundant fields | 3-tier resolution architecture | Elimination of duplication |
| Fingerprinting | **28 implementations**, 5 hash strategies | Unified `CompositeFingerprint` framework | Single composition pattern |
| Registry/Catalog | **20+ registries** with inconsistent APIs | RegistryFacade + adapters (capability-aware) | Protocol compliance |
| Delta Integration | **Split across 4 modules** | Unified DeltaService boundary | Clear ownership |
| Storage Observability | **54 functions**, ZERO OTel spans (delta artifacts exist) | Full OpenTelemetry + artifact linkage | Complete visibility |

### Critical Findings

1. **DataFusionRuntimeProfile** (158 fields) lacks fingerprinting - breaks reproducibility (despite existing settings/telemetry hashes)
2. **RootConfig** has 4x flattening duplication (94 fields: 6 nested + 88 flat)
3. **PlanCacheKey** uses string concatenation - collision risk at scale
4. **UdfCatalog** breaks Registry protocol - cannot use polymorphically (protocol also requires `__iter__` / `__len__`)
5. **Storage layer** has zero OTel observability - delta artifact tables exist but no trace spans
6. **Delta integration** is split across `datafusion_engine.delta` vs `storage.deltalake`, with no unified service boundary

### Implementation Priority

| Priority | Area | Effort | Impact |
|----------|------|--------|--------|
| **P0** | Storage Observability | Low | High (production visibility) |
| **P1** | Fingerprinting Collision Fixes | Medium | High (reproducibility) |
| **P2** | Delta Integration Boundary | Medium | High (consistency + ownership) |
| **P3** | Registry Protocol Compliance | Medium | Medium (maintainability) |
| **P4** | Configuration Hierarchical Design | High | High (DX improvement) |
| **P5** | Schema Contract Unification | High | Medium (complexity reduction) |

---

## Table of Contents

1. [Configuration Architecture Unification](#1-configuration-architecture-unification)
2. [Schema Contract Consolidation](#2-schema-contract-consolidation)
3. [Fingerprinting Framework](#3-fingerprinting-framework)
4. [Registry Pattern Unification](#4-registry-pattern-unification)
5. [Storage Observability](#5-storage-observability)
6. [DataFusion + Delta Integration Unification](#6-datafusion--delta-integration-unification)
7. [Implementation Roadmap](#7-implementation-roadmap)
8. [Appendices](#appendices)

---

## 1. Configuration Architecture Unification

### 1.1 Current State Analysis

**Total Configuration Classes: 183** across 107 files

#### Distribution by Subsystem

| Layer | Count | Percentage | Notable Classes |
|-------|-------|------------|-----------------|
| DataFusion Engine | 46 | 25% | DataFusionRuntimeProfile (158 fields) |
| Extract | 34 | 19% | RepoOptions, WorklistQueueOptions |
| CLI | 26 | 14% | RootConfig (94 fields) |
| Hamilton Pipeline | 21 | 11% | ExecutorConfig, DAG configs |
| Semantics | 13 | 7% | SemanticConfig, CpgBuildOptions |
| Storage | 11 | 6% | DeltaWritePolicy, DeltaScanConfig |
| Schema Spec | 7 | 4% | Arrow/Parquet/Delta specs |
| Observability | 7 | 4% | OtelConfig, DiagnosticsPolicy |
| RelSpec | 5 | 3% | Task graph inference |
| Engine | 5 | 3% | Session factories |
| CPG | 3 | 2% | Schema definitions |
| Cache | 2 | 1% | DiskCacheSettings |
| Core | 1 | <1% | FingerprintableConfig |

#### Type Distribution

| Type Suffix | Count | Purpose |
|-------------|-------|---------|
| `*Options` | 71 | Optional parameter bundles |
| `*Config` | 46 | Request/command parameters |
| `*Policy` | 28 | Runtime behavior control |
| `*Settings` | 5 | Initialization parameters |
| `*Profile` | 4 | Comprehensive runtime profiles |
| `*Spec` | 29 | Declarative schema definitions |

### 1.2 Critical Hotspots

#### DataFusionRuntimeProfile (158 fields)

**Location**: `src/datafusion_engine/session/runtime.py:3139-3318`

This class is the largest configuration object in the codebase, combining concerns that should be separated.

**Field Groupings by Prefix**:

| Prefix | Count | Purpose |
|--------|-------|---------|
| `ast_*` | 20 | AST catalog/external/delta configurations |
| `bytecode_*` | 20 | Bytecode catalog/external/delta configurations |
| `enable_*` | 16 | Feature flags for components |
| `delta_*` | 8 | Delta Lake specific settings |
| `explain_*` | 5 | Query explanation configuration |
| `cache_*` | 5 | Cache management settings |
| `semantic_*` | 4 | Semantic output locations |
| `repartition_*` | 4 | DataFusion partitioning |
| `plan_*` | 4 | Plan artifact collection |
| `capture_*` | 3 | Diagnostic capture |
| Other | ~69 | Various mixed concerns |

**Critical Issue**: DataFusionRuntimeProfile does NOT implement `fingerprint_payload()` despite being the most complex config class. This breaks reproducibility tracking. **Note**: the profile already emits stable `settings_hash()` and `telemetry_payload_hash()` values, so any new fingerprinting must compose these existing hashes rather than replace them.

#### RootConfig (94 fields)

**Location**: `src/cli/config_models.py:99-193`

**Problem**: 4x flattening duplication
- 6 nested config objects
- 88 flattened scalar fields that mirror the nested objects

**Example Duplication**:
```python
# Nested (clean)
plan: PlanConfig | None = None

# Flattened (duplicates nested fields)
plan_allow_partial: bool | None = None
plan_requested_tasks: tuple[str, ...] | None = None
enable_metric_scheduling: bool | None = None  # NOT prefixed!
```

Users can set the same config via TWO paths, creating silent conflicts.

**Additional Constraint**: `cli/config_loader.normalize_config_contents()` currently flattens nested sections into flat keys used by the driver. Removing flattened fields must include a legacy normalization layer (or explicit deprecation window) to avoid breaking existing config files.

### 1.3 Duplicate Class Names

| Class Name | Locations | Issue |
|------------|-----------|-------|
| `GraphAdapterConfig` | cli/config_models.py:32, hamilton_pipeline/types/execution.py:44 | Type mismatch (str vs Literal) |
| `IncrementalConfig` | cli/config_models.py:39, semantics/incremental/config.py:22 | Different field counts, methods |
| `ExtractPlanOptions` | extract/helpers.py:164, extract/coordination/materialization.py:69 | **Exact duplicate code** |
| `ExtractMaterializeOptions` | extract/helpers.py:332, extract/coordination/materialization.py:92 | Code duplication |

### 1.4 Proposed Hierarchical Design

#### DataFusionRuntimeProfile Decomposition

Extract 6 logical sub-configs:

```python
@msgspec.Struct(frozen=True)
class ExecutionConfig:
    """DataFusion execution parameters."""
    target_partitions: int
    batch_size: int
    repartition_aggregations: bool
    repartition_joins: bool
    repartition_windows: bool
    memory_limit: int | None
    spill_dir: Path | None

@msgspec.Struct(frozen=True)
class CatalogConfig:
    """Catalog and schema configuration."""
    default_catalog: str
    default_schema: str
    view_catalog_name: str
    registry_catalogs: tuple[str, ...]
    catalog_auto_load_enabled: bool
    enable_information_schema: bool

@msgspec.Struct(frozen=True)
class TableSourceConfig:
    """Reusable table source configuration template."""
    catalog_path: Path | None
    catalog_scan_options: ScanOptions | None
    external_table_location: Path | None
    external_table_format: str
    delta_table_path: Path | None
    delta_scan_options: DeltaScanOptions | None

@msgspec.Struct(frozen=True)
class DataSourceConfig:
    """All data source configurations."""
    ast: TableSourceConfig
    bytecode: TableSourceConfig
    extract_output: ExtractOutputConfig
    semantic_output: SemanticOutputConfig

@msgspec.Struct(frozen=True)
class FeatureGatesConfig:
    """Feature flag configuration."""
    enable_information_schema: bool
    enable_ident_normalization: bool
    enable_url_table: bool
    enable_cache_refresh: bool
    enable_function_caching: bool
    enable_schema_caching: bool
    enable_udfs: bool
    enable_delta_cdf: bool
    enable_metrics: bool
    enable_tracing: bool

@msgspec.Struct(frozen=True)
class DiagnosticsConfig:
    """Diagnostics and explanation configuration."""
    explain_verbose: bool
    explain_analyze: bool
    explain_analyze_threshold_ms: int
    explain_collector: ExplainCollector | None
    capture_logical_plan: bool
    capture_physical_plan: bool
    validate_schemas: bool

@msgspec.Struct(frozen=True, kw_only=True)
class DataFusionRuntimeProfile(FingerprintableConfig):
    """Unified DataFusion runtime configuration."""
    execution: ExecutionConfig
    catalog: CatalogConfig
    data_sources: DataSourceConfig
    features: FeatureGatesConfig
    diagnostics: DiagnosticsConfig
    policies: PolicyBundleConfig

    def fingerprint_payload(self) -> Mapping[str, object]:
        return {
            "execution": self.execution.fingerprint_payload(),
            "catalog": self.catalog.fingerprint_payload(),
            "features": self.features.fingerprint_payload(),
            "policies": self.policies.fingerprint_payload(),
        }
```

**Target**: Reduce from 158 flat fields to ~70 fields across 6 structured sub-configs.

**Compatibility Layer (Required)**:
- Preserve `telemetry_payload_hash()` and `settings_hash()` by delegating their payload construction to the new sub-configs.
- Add a temporary adapter that synthesizes legacy flat fields (e.g., `ast_*`, `bytecode_*`) from structured sub-configs to avoid a wide break during migration.

#### RootConfig Simplification

Remove flattened fields, force nested access:

```python
@msgspec.Struct(frozen=True)
class RootConfig:
    """CLI root configuration."""
    # ONLY nested configs - no flattened mirrors
    plan: PlanConfig
    cache: CacheConfig
    graph_adapter: GraphAdapterConfig
    incremental: IncrementalConfig
    delta: DeltaConfig
    docstrings: DocstringsConfig
    otel: OtelConfig  # NEW: was flat
    hamilton: HamiltonConfig  # NEW: was flat
```

### 1.5 Migration Path

**Phase 1**: Fix duplicate names
1. Rename `semantics/incremental/config.py:IncrementalConfig` → `SemanticIncrementalConfig`
2. Unify `GraphAdapterConfig` - keep Hamilton version, adapt CLI to use it
3. Delete duplicate `ExtractPlanOptions` in coordination/materialization.py

**Phase 2**: Add missing fingerprinting
1. Implement `fingerprint_payload()` on DataFusionRuntimeProfile
2. Add fingerprint methods to all Policy classes that lack them

**Phase 3**: Hierarchical refactor
1. Extract sub-configs from DataFusionRuntimeProfile
2. Remove flattened fields from RootConfig
3. Add a legacy normalization layer for flattened RootConfig keys (deprecate in stages)
4. Update all call sites (estimated 50-100 files)

---

## 2. Schema Contract Consolidation

### 2.1 Current State Analysis

**6 Schema Contract Types** with overlapping responsibilities:

| Type | Location | Fields | Purpose |
|------|----------|--------|---------|
| `TableSchemaSpec` | schema_spec/specs.py | 5 | Core schema definition |
| `ContractSpec` | schema_spec/system.py | 10 | Runtime validation wrapper |
| `SchemaContract` | datafusion_engine/schema/contracts.py | 9 | Independent validation |
| `DatasetSpec` | schema_spec/system.py | 18 | Complete IO configuration |
| `DatasetLocation` | datafusion_engine/dataset/registry.py | 21 | Registration + overrides |
| `TableSchemaContract` | schema_spec/system.py | 2 | File schema + partitions |

### 2.2 DatasetLocation Redundancy

**9 fields duplicate DatasetSpec** for override purposes:

| Field | In DatasetSpec | In DatasetLocation | Purpose |
|-------|----------------|-------------------|---------|
| `delta_scan` | ✓ | ✓ | Override scan options |
| `delta_cdf_policy` | ✓ | ✓ | Override CDF settings |
| `delta_maintenance_policy` | ✓ | ✓ | Override maintenance |
| `delta_write_policy` | ✓ | ✓ | Override write policy |
| `delta_schema_policy` | ✓ | ✓ | Override schema policy |
| `delta_feature_gate` | ✓ | ✓ | Override feature flags |
| `delta_constraints` | ✓ | ✓ | Override constraints |
| `datafusion_scan` | ✓ | ✓ | Override scan options |
| `table_spec` | ✓ | ✓ | Override table spec |

**The override mechanism is intentional** - DatasetLocation allows overriding dataset-level defaults without mutating the underlying DatasetSpec.

### 2.3 Resolution Logic

**9 `resolve_*()` functions** implement two-tier precedence:

```python
def resolve_delta_write_policy(location: DatasetLocation) -> DeltaWritePolicy | None:
    # Level 1: Check location override
    if location.delta_write_policy is not None:
        return location.delta_write_policy
    # Level 2: Fall back to dataset_spec
    if location.dataset_spec is not None:
        return location.dataset_spec.delta_write_policy
    # Level 3: Return None (use system defaults)
    return None
```

**Exception**: `resolve_dataset_schema()` has non-standard 4-level priority:
1. DataFusionScanOptions.table_schema_contract.file_schema
2. DatasetLocation.table_spec
3. DatasetLocation.dataset_spec.schema()
4. Delta table introspection

### 2.4 ContractSpec vs SchemaContract

| Aspect | ContractSpec | SchemaContract |
|--------|--------------|----------------|
| Coupling | Tightly coupled to TableSchemaSpec | Independent |
| Focus | Runtime execution validation | Compile-time validation |
| Unique Features | dedupe, canonical_sort, virtual_fields | evolution_policy, semantic_types |
| Conversion | Part of spec hierarchy | One-way: DatasetSpec → SchemaContract |

### 2.5 Proposed Consolidation

**Option: Lazy Resolution with Cached Views**

Simplify DatasetLocation to store only raw overrides:

```python
@msgspec.Struct(frozen=True)
class DatasetLocationOverrides:
    """Override-only fields for DatasetLocation."""
    delta_scan: DeltaScanOptions | None = None
    delta_cdf_policy: DeltaCdfPolicy | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    # ... other override fields

@msgspec.Struct(frozen=True)
class DatasetLocation:
    """Dataset registration location."""
    name: str
    path: Path
    dataset_spec: DatasetSpec
    overrides: DatasetLocationOverrides | None = None

    @cached_property
    def resolved_delta_write_policy(self) -> DeltaWritePolicy | None:
        if self.overrides and self.overrides.delta_write_policy:
            return self.overrides.delta_write_policy
        return self.dataset_spec.delta_write_policy
```

**Benefits**:
- Clearer separation of base spec vs overrides
- Computed properties trigger resolution on-demand
- Less redundancy in the data model

### 2.6 Best-in-Class Adjustment: ResolvedDatasetLocation View Model

The codebase already has a dataset resolution pipeline (`DatasetResolution`) that materializes provider-level metadata. We should extend this to a *resolved view model* for dataset configuration, so all `resolve_*()` logic is centralized and cached:

```python
@dataclass(frozen=True)
class ResolvedDatasetLocation:
    """Resolved view of DatasetLocation + DatasetSpec precedence."""
    location: DatasetLocation
    dataset_spec: DatasetSpec | None
    datafusion_scan: DataFusionScanOptions | None
    delta_write_policy: DeltaWritePolicy | None
    delta_schema_policy: DeltaSchemaPolicy | None
    delta_feature_gate: DeltaFeatureGate | None
    schema: SchemaLike | None
```

**Why**:
- Avoid repeated `resolve_*()` calls across the codebase
- Make precedence explicit, testable, and fingerprintable
- Align with `DatasetResolution` (provider-level) to reduce duplicate logic

### 2.7 Migration Path

**Phase 1**: Document current resolution rules
1. Add docstrings to all `resolve_*()` functions
2. Create resolution precedence diagram

**Phase 2**: Introduce override wrapper
1. Create `DatasetLocationOverrides` struct
2. Migrate override fields incrementally

**Phase 3**: Simplify DatasetLocation
1. Replace direct override fields with overrides bundle
2. Add cached property accessors
3. Deprecate direct field access

---

## 3. Fingerprinting Framework

### 3.1 Current State Analysis

**28 distinct fingerprinting implementations** across 27 files

#### Hash Function Inventory

| Function | Algorithm | Use Case | Truncation |
|----------|-----------|----------|------------|
| `hash_sha256_hex()` | SHA-256 | General-purpose | Optional (16 chars in plans) |
| `hash64_from_text()` | BLAKE2b-8 | Compact 64-bit | Fixed 8 bytes |
| `hash128_from_text()` | BLAKE2b-16 | 128-bit | Fixed 16 bytes |
| `hash_msgpack_canonical()` | SHA-256 of msgpack | Deterministic structs | None |
| `hash_json_canonical()` | SHA-256 of sorted JSON | Canonical JSON | None |
| `hash_settings()` | SHA-256 of sorted msgpack | Settings-specific | None |
| `hash_file_sha256()` | SHA-256 (chunked) | File content | None |
| `hash_storage_options()` | SHA-256 of JSON | Storage options | None |
| `CacheKeyBuilder` | SHA-256 of msgpack components | Structured keys | None |

### 3.2 FingerprintableConfig Protocol

**Location**: `src/core/config_base.py`

```python
@runtime_checkable
class FingerprintableConfig(Protocol):
    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return a canonical payload for fingerprinting."""
        ...

    def fingerprint(self) -> str:
        """Return a deterministic fingerprint."""
        return config_fingerprint(self.fingerprint_payload())
```

**27 implementations** follow this protocol consistently.

### 3.3 Critical Collision Risks

#### Risk 1: PlanCacheKey String Concatenation

**Location**: `src/datafusion_engine/plan/cache.py:161-181`

```python
def as_key(self) -> str:
    parts = (
        self.profile_hash,
        self.substrait_hash,
        self.plan_fingerprint,
        # ... 7 more components
    )
    return "plan:" + ":".join(parts)  # COLLISION RISK
```

**Problem**: Colon-separated strings can collide when component boundaries align ambiguously.

#### Risk 2: Plan Fingerprint Truncation

**Location**: `src/semantics/plans/fingerprints.py`

```python
def _hash_string(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]  # 64 bits only!
```

Plan fingerprints use **16-character truncation** (64 bits), approaching birthday-problem collision territory for large plan populations.

#### Collision Risk Matrix

| Component | Full Hash | Actual Length | Entropy | Risk |
|-----------|-----------|---------------|---------|------|
| Plan logical_plan_hash | SHA256 (256 bits) | 16 chars | 64 bits | **MEDIUM** |
| Plan schema_hash | SHA256 (256 bits) | 16 chars | 64 bits | **MEDIUM** |
| Plan substrait_hash | SHA256 (256 bits) | 16 chars | 64 bits | **MEDIUM** |
| Config fingerprint | SHA256 (256 bits) | 64 chars | 256 bits | LOW |
| Cache key (10 components) | String concat | Variable | Variable | **HIGH** |

### 3.4 Proposed CompositeFingerprint Design

```python
@msgspec.Struct(frozen=True)
class FingerprintComponent:
    """Single component of a composite fingerprint."""
    name: str
    value: str
    algorithm: str = "sha256"
    truncated: bool = False

@msgspec.Struct(frozen=True)
class CompositeFingerprint:
    """Type-safe composite fingerprint with collision resistance."""
    version: int
    components: tuple[FingerprintComponent, ...]

    @classmethod
    def from_components(
        cls,
        version: int,
        **components: str,
    ) -> CompositeFingerprint:
        """Create from named components."""
        return cls(
            version=version,
            components=tuple(
                FingerprintComponent(name=k, value=v)
                for k, v in sorted(components.items())
            ),
        )

    def as_cache_key(self, prefix: str = "") -> str:
        """Generate collision-resistant cache key."""
        # Use msgpack canonical encoding, NOT string concatenation
        payload = {
            "version": self.version,
            "components": {c.name: c.value for c in self.components},
        }
        digest = hash_msgpack_canonical(payload)
        return f"{prefix}:{digest}" if prefix else digest

    def extend(self, **additional: str) -> CompositeFingerprint:
        """Add components, returning new fingerprint."""
        new_components = self.components + tuple(
            FingerprintComponent(name=k, value=v)
            for k, v in sorted(additional.items())
        )
        return CompositeFingerprint(
            version=self.version,
            components=new_components,
        )
```

**Implementation Notes**:
- Prefer the existing `CacheKeyBuilder` (msgpack-canonical) as the engine for composite key materialization.
- For runtime profiles, compose fingerprints from `settings_hash()` and `telemetry_payload_hash()` rather than re-hashing the raw profile fields.
- Keep a read-path for legacy cache keys while writing new composite keys (dual-read, single-write).

### 3.5 Migration Path

**Phase 1**: Fix immediate collision risks
1. Extend plan fingerprint truncation from 16 to 32 chars (128 bits)
2. Replace `PlanCacheKey.as_key()` string concatenation with msgpack encoding (via CacheKeyBuilder)

**Phase 2**: Introduce CompositeFingerprint
1. Create `src/core/fingerprinting.py` with new types
2. Migrate PlanFingerprint to use CompositeFingerprint
3. Migrate PlanCacheKey to use CompositeFingerprint with dual-read compatibility

**Phase 3**: Standardize all fingerprinting
1. Update FingerprintableConfig to return CompositeFingerprint (or a stable payload that composes existing hashes)
2. Add collision risk audit trail
3. Document fingerprint schema evolution policy

---

## 4. Registry Pattern Unification

### 4.1 Current State Analysis

**20+ registries/catalogs** with inconsistent interfaces

#### Registry Inventory

| Registry | Location | Protocol Compliance | Interface |
|----------|----------|---------------------|-----------|
| `DatasetCatalog` | datafusion_engine/dataset/registry.py | ✓ Full | register, get, __contains__ |
| `ProviderRegistry` | datafusion_engine/catalog/provider_registry.py | ✓ Full | register, get, snapshot |
| `UdfCatalog` | datafusion_engine/udf/catalog.py | ✗ **Breaks** | register_udf, resolve_function |
| `FunctionCatalog` | datafusion_engine/udf/catalog.py (+ schema/registry.py) | ✗ Custom | get_function, list_functions |
| `SemanticModel` | semantics/registry.py | ✗ Singleton | get_spec, get_all |
| `ViewGraph` | datafusion_engine/views/graph.py | ✗ Custom | register_view, get_view |
| `CacheRegistry` | cache/registry.py | ✗ Query-only | lookup, invalidate |
| `SerdeSchemaRegistry` | serde_schema_registry.py | ✗ Immutable | get_schema |

**Clarification**: `FunctionCatalog` lives in `datafusion_engine/udf/catalog.py` (and another in `datafusion_engine/schema/registry.py`), not `datafusion_engine/functions/catalog.py`. Any unification effort should reference the actual definitions.

#### Protocol Definition

**Location**: `src/utils/registry_protocol.py`

```python
class Registry(Protocol[K, V]):
    def register(self, key: K, value: V) -> None: ...
    def get(self, key: K) -> V | None: ...
    def __contains__(self, key: K) -> bool: ...
    def __iter__(self) -> Iterator[K]: ...
    def __len__(self) -> int: ...

class MutableRegistry(Registry[K, V], Protocol):
    def remove(self, key: K) -> V | None: ...
    def clear(self) -> None: ...

class ImmutableRegistry(Registry[K, V], Protocol):
    pass
```

### 4.2 Interface Inconsistencies

| Method | DatasetCatalog | UdfCatalog | ProviderRegistry | ViewGraph |
|--------|---------------|------------|------------------|-----------|
| Register | `register()` | `register_udf()` | `register()` | `register_view()` |
| Get | `get()` | `resolve_function()` | `get()` | `get_view()` |
| Contains | `__contains__` | `is_registered()` | `__contains__` | N/A |
| Snapshot | N/A | N/A | `snapshot()` | N/A |

**UdfCatalog is the worst offender** - cannot use polymorphically with Registry protocol (no `register/get/contains` parity, and missing `__iter__` / `__len__`).

### 4.3 Registration Flow Analysis

Dataset registration involves **8 implicit phases**:

```
Phase 1: ExtractMetadata loaded (global singleton)
    ↓ implicit dependency
Phase 2: SEMANTIC_TABLE_SPECS loaded (immutable tuple)
    ↓ implicit dependency
Phase 3: ViewGraph constructed (view_graph_nodes)
    ↓ depends on Phase 1 & 2
Phase 4: DatasetCatalog built (dataset_catalog_from_profile)
    ↓ depends on Phase 3
Phase 5: ProviderRegistry populated (register table providers)
    ↓ depends on Phase 4
Phase 6: UdfCatalog refreshed (refresh_from_session)
    ↓ depends on Phase 5
Phase 7: Validation passes
    ↓ depends on all prior phases
Phase 8: Ready for execution
```

**Problems**:
- No enforcement of phase ordering
- Partial failures leave system inconsistent
- No rollback mechanism

### 4.4 Proposed RegistryFacade Design (Capability-Aware)

```python
@runtime_checkable
class SnapshotRegistry(Protocol[K, V]):
    def snapshot(self) -> Mapping[K, V]: ...
    def restore(self, snapshot: Mapping[K, V]) -> None: ...


@msgspec.Struct
class RegistrationResult:
    """Result of a registration operation."""
    success: bool
    key: str
    phase: str
    timestamp: datetime
    error: str | None = None


class RegistryFacade:
    """Unified registration facade with optional transaction semantics."""

    def __init__(
        self,
        *,
        dataset_catalog: Registry[str, DatasetLocation],
        provider_registry: Registry[str, TableRegistration],
        udf_registry: Registry[str, DataFusionUdfSpec],
        view_registry: Registry[str, ViewNode],
    ) -> None:
        self._datasets = dataset_catalog
        self._providers = provider_registry
        self._udfs = udf_registry
        self._views = view_registry
        self._checkpoints: dict[str, dict[str, Mapping[object, object]]] = {}

    def register_dataset(self, name: str, location: DatasetLocation) -> RegistrationResult:
        checkpoint = self._checkpoint_optional()
        try:
            self._datasets.register(name, location)
            # cascade to provider registry
            self._providers.register(name, self._build_provider(location))
            return RegistrationResult(success=True, key=name, phase="dataset")
        except Exception as exc:
            self._rollback_optional(checkpoint)
            return RegistrationResult(success=False, key=name, phase="dataset", error=str(exc))

    def lookup_dataset(self, name: str) -> DatasetLocation | None:
        return self._datasets.get(name)

    def lookup_udf(self, func_id: str) -> DataFusionUdfSpec | None:
        return self._udfs.get(func_id)

    def _checkpoint_optional(self) -> str | None:
        snapshot: dict[str, Mapping[object, object]] = {}
        for label, registry in {
            "datasets": self._datasets,
            "providers": self._providers,
            "udfs": self._udfs,
            "views": self._views,
        }.items():
            if isinstance(registry, SnapshotRegistry):
                snapshot[label] = registry.snapshot()
        if not snapshot:
            return None
        checkpoint_id = str(uuid.uuid4())
        self._checkpoints[checkpoint_id] = snapshot
        return checkpoint_id

    def _rollback_optional(self, checkpoint_id: str | None) -> None:
        if checkpoint_id is None:
            return
        snapshot = self._checkpoints.get(checkpoint_id)
        if snapshot is None:
            return
        # Restore only registries that support snapshot/restore.
        for label, registry in {
            "datasets": self._datasets,
            "providers": self._providers,
            "udfs": self._udfs,
            "views": self._views,
        }.items():
            if isinstance(registry, SnapshotRegistry) and label in snapshot:
                registry.restore(snapshot[label])
```

**Key Adjustments**:
- Use **capability-aware** checkpoints; not every registry can (or should) support restore.
- Introduce a **UdfCatalogAdapter** that exposes a strict Registry surface for custom UDF specs, while builtin resolution remains in the runtime FunctionCatalog.
- UDF expression construction resolves against the **SessionContext FunctionRegistry** (via `udf_expr` or SQL), not the UdfCatalogAdapter.

### 4.5 Migration Path

**Phase 1**: Protocol adoption
1. Add a `UdfCatalogAdapter` that implements Registry for custom UDF specs
2. Extend registries that can support it with SnapshotRegistry (snapshot/restore)
3. Standardize method names across registries (including `__iter__` / `__len__`)

**Phase 2**: Create RegistryFacade
1. Implement RegistryFacade with capability-aware checkpoints
2. Route all registrations through facade
3. Add optional checkpoint/rollback where SnapshotRegistry is supported

**Phase 3**: Enforce phase ordering
1. Create `RegistrationPhaseOrchestrator`
2. Add validation gates between phases
3. Implement partial failure recovery

---

## 5. Storage Observability

### 5.1 Current State Analysis

**54 public functions** across storage/deltalake with **ZERO OTel span instrumentation**

**Important Clarification**: Delta operations already emit **artifact records** via
`datafusion_engine.delta.observability` (snapshots, mutations, maintenance). The gap
is the absence of trace spans and consistent OTel attributes that tie these artifacts
to runtime traces.

#### Function Inventory by Category

| Category | Count | Examples |
|----------|-------|----------|
| Read Operations | 4 | read_delta_table, query_delta_sql |
| Metadata | 7 | delta_table_version, delta_table_schema |
| Write/Mutation | 4 | delta_merge_arrow, delta_delete_where |
| Enable Features | 15 | enable_delta_cdf, enable_delta_deletion_vectors |
| Disable Features | 13 | disable_delta_cdf, disable_delta_row_tracking |
| Maintenance | 4 | vacuum_delta, create_delta_checkpoint |
| File Pruning | 3 | evaluate_and_select_files |
| Configuration | 4 | delta_write_configuration |

### 5.2 Available Infrastructure

**Location**: `src/obs/otel/`

**Existing Scopes**:
```python
class ScopeName(StrEnum):
    ROOT = "codeanatomy"
    PIPELINE = "codeanatomy.pipeline"
    EXTRACT = "codeanatomy.extract"
    DATAFUSION = "codeanatomy.datafusion"
    SEMANTICS = "codeanatomy.semantics"
    # STORAGE is MISSING
```

**Available Utilities**:
- `get_tracer(scope_name)` - OpenTelemetry tracer
- `stage_span(name, stage, scope_name, attributes)` - Span decorator
- `record_stage_duration()` - Histogram metric
- `record_exception()` - Exception recording

### 5.3 Proposed SCOPE_STORAGE Design

**Add to constants.py**:
```python
STORAGE = "codeanatomy.storage"
```

**Span Naming Convention**:
```
codeanatomy.storage.<operation_type>
```

| Operation | Span Name |
|-----------|-----------|
| Read table | `codeanatomy.storage.read_table` |
| Merge | `codeanatomy.storage.merge` |
| Delete | `codeanatomy.storage.delete` |
| Read CDF | `codeanatomy.storage.read_cdf` |
| File pruning | `codeanatomy.storage.file_pruning` |
| Vacuum | `codeanatomy.storage.vacuum` |
| Feature enable | `codeanatomy.storage.feature_enable` |
| Feature disable | `codeanatomy.storage.feature_disable` |

### 5.4 Span Attributes

| Category | Attribute | Example |
|----------|-----------|---------|
| Identity | `codeanatomy.table` | "/data/tables/ast_nodes" |
| Operation | `codeanatomy.operation` | "read", "merge", "vacuum" |
| Version | `codeanatomy.version_before` | 123 |
| Version | `codeanatomy.version_after` | 124 |
| Metrics | `codeanatomy.rows_affected` | 5000 |
| Metrics | `codeanatomy.files_scanned` | 42 |
| Metrics | `codeanatomy.files_selected` | 8 |
| Filter | `codeanatomy.has_filters` | true |
| CDF | `codeanatomy.starting_version` | 100 |
| Feature | `codeanatomy.feature_name` | "deletion_vectors" |

### 5.5 Implementation Checklist

#### Tier 1 (Critical - Implement First)

| Function | Span Name | Key Attributes |
|----------|-----------|----------------|
| `read_delta_table()` | `storage.read_table` | table, version, rows |
| `delta_merge_arrow()` | `storage.merge` | table, rows_affected, version_after |
| `delta_delete_where()` | `storage.delete` | table, predicate, rows_deleted |
| `read_delta_cdf()` | `storage.read_cdf` | table, starting_version, ending_version |
| `evaluate_and_select_files()` | `storage.file_pruning` | candidates_before, candidates_after |
| `query_delta_sql()` | `storage.sql_query` | sql_kind, tables |
| `vacuum_delta()` | `storage.vacuum` | table, retention_hours, files_removed |
| `delta_table_version()` | `storage.metadata` | table, version |

#### Tier 2 (Standard)

All `enable_delta_*()` and `disable_delta_*()` functions with:
- Span: `storage.feature_control`
- Attributes: feature_name, table, success

#### Tier 3 (Utility)

Configuration and helper functions with lightweight spans.

### 5.6 Artifact Linkage

Delta mutations and maintenance already emit artifact rows. The new OTel spans should:
- Emit `codeanatomy.run_id` (when available) for cross-system correlation
- Add trace/span identifiers to artifact payloads (if feasible) or vice versa
- Ensure span names/attributes align with artifact `operation` values

### 5.7 Example Implementation

```python
from obs.otel import stage_span, SCOPE_STORAGE

def read_delta_table(request: DeltaReadRequest) -> pa.Table:
    """Read a Delta table with full observability."""
    with stage_span(
        "read_delta_table",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes={
            "codeanatomy.table": str(request.path),
            "codeanatomy.operation": "read",
        },
    ) as span:
        table = DeltaTable(str(request.path), storage_options=request.storage_options)
        version = table.version()
        span.set_attribute("codeanatomy.version", version)

        result = table.to_pyarrow_table()
        span.set_attribute("codeanatomy.rows", result.num_rows)

        return result
```

---

## 6. DataFusion + Delta Integration Unification

### 6.1 Current Surface Area

The Delta/DataFusion integration is split across multiple modules:
- **Provider construction & scan config**: `datafusion_engine.delta.*` (control plane, plugin options)
- **Dataset resolution**: `datafusion_engine.dataset.*`
- **Mutations & maintenance**: `storage.deltalake.*`
- **Observability artifacts**: `datafusion_engine.delta.observability`

This split makes it hard to enforce consistent configuration, observability, and fingerprinting.

### 6.2 Proposed DeltaService (Unified Boundary)

Introduce a single service boundary that owns Delta access end-to-end.

```python
@dataclass(frozen=True)
class DeltaService:
    profile: DataFusionRuntimeProfile

    def provider(self, *, location: DatasetLocation, name: str | None = None) -> DatasetResolution:
        """Resolve a Delta provider with consistent scan config + observability."""

    def read_table(self, request: DeltaReadRequest) -> pa.Table:
        """Read with OTel spans + artifact linkage."""

    def mutate(self, request: DeltaMutationRequest) -> DeltaWriteResult:
        """Merge/append/delete with unified commit metadata and tracing."""
```

**Goals**:
- Single place for storage option resolution, runtime profile use, and observability
- One canonical location for Delta scan configuration defaults
- Clear extension point for caching/statistics policy

### 6.3 Dataset Template Map (Runtime Profile Simplification)

Replace ad-hoc `ast_*` / `bytecode_*` fields in `DataFusionRuntimeProfile` with a typed
dataset template map:

```python
dataset_templates: Mapping[str, DatasetLocation]
```

Each template can embed `DataFusionScanOptions`, `DeltaScanOptions`, and overrides.
Legacy fields should be synthesized from this map during migration.

### 6.4 FunctionFactory as the Canonical UDF Entry Point

Treat FunctionFactory (Rust + Python policy) as the canonical source of UDF/UDTF
registration. Align UdfCatalog snapshots with the FunctionFactory policy hash, so
the runtime UDF surface is deterministic and part of the profile fingerprint.

**UDF Expression Surface (Target State)**:
- Python exposes a **single** generic helper: `udf_expr(name, *args, ctx=None)`.
- Per-UDF Python wrappers are removed to avoid drift from the registry snapshot.
- UDF expression construction resolves against the **SessionContext
  FunctionRegistry** when a context is provided, otherwise it falls back to the
  canonical registry specs. The UdfCatalogAdapter is metadata-only (custom spec registry),
  not the execution source.

**UDTF/Table Function Surface (Target State)**:
- Replace minimal `read_parquet` / `read_csv` UDTFs with **full-fidelity provider
  factories** (ListingTable) that preserve pushdown and options parity.
- Rely on DataFusion built-ins (`range`, `generate_series`) instead of a
  `range_table` alias.

### 6.5 Caching + Statistics as First-Class Policy

Tie `DataFusionScanOptions` and runtime settings to explicit cache/statistics policy:
- Listing cache TTL/limit, metadata cache limit
- `collect_statistics` defaults for large datasets
- Policy-aware defaults for delta vs non-delta datasets

## 7. Implementation Roadmap

### Phase 1: Quick Wins (Week 1-2)

**Storage Observability** (Low effort, High impact)
- [ ] Add `SCOPE_STORAGE` to constants.py
- [ ] Instrument Tier 1 functions (8 functions)
- [ ] Add span attributes per design
- [ ] Link OTel spans to existing Delta artifact records
- [ ] Verify traces in local dev

**Fingerprinting Fixes** (Medium effort, High impact)
- [ ] Extend plan fingerprint truncation to 32 chars
- [ ] Replace `PlanCacheKey.as_key()` with msgpack encoding (CacheKeyBuilder)
- [ ] Add dual-read compatibility for legacy cache keys

**Registry Fixes** (Low effort, Medium impact)
- [ ] Add UdfCatalogAdapter to expose a strict Registry for custom UDF specs
- [ ] Document registration phase dependencies

### Phase 2: Breaking Changes (Week 3-4)

**Configuration Consolidation**
- [ ] Fix duplicate class names (4 collisions)
- [ ] Add `fingerprint_payload()` to DataFusionRuntimeProfile
- [ ] Remove flattened fields from RootConfig (88 fields) with legacy normalization
- [ ] Introduce ResolvedDatasetLocation view model + tests

**Registry Unification**
- [ ] Create RegistryFacade (capability-aware)
- [ ] Add checkpoint/rollback only for registries that support SnapshotRegistry
- [ ] Route critical registrations through facade

**Delta Integration Boundary**
- [ ] Draft DeltaService/DeltaGateway API (read, provider, mutate)
- [x] Canonicalize delta plugin options via a single `scan_config` payload from the host

**UDF/UDTF Surface Cleanup**
- [x] Add `udf_expr(name, *args, ctx=None)` as the only Python UDF expression helper
- [x] Remove per-UDF Python wrappers and update call sites
- [x] Replace minimal `read_parquet/read_csv` UDTFs with full-fidelity provider factories
- [x] Remove `range_table` alias; use built-in `range` / `generate_series`
- [x] Update registry snapshots, docs, and parity tests for the new surfaces

### Phase 3: Full Alignment (Week 5-8)

**DataFusionRuntimeProfile Decomposition**
- [ ] Extract 6 sub-configs
- [ ] Update all call sites
- [ ] Add hierarchical fingerprinting
- [ ] Replace `ast_*`/`bytecode_*` fields with dataset template map

**Schema Contract Simplification**
- [ ] Create DatasetLocationOverrides
- [ ] Migrate override fields
- [ ] Add cached property accessors

**Fingerprinting Framework**
- [ ] Create CompositeFingerprint class
- [ ] Migrate PlanFingerprint
- [ ] Document fingerprint schema evolution

### Phase 4: Completion (Week 9-10)

**Storage Observability Complete**
- [ ] Instrument Tier 2 functions (28 functions)
- [ ] Add metrics collection
- [ ] Create observability dashboard

**Registry Full Adoption**
- [ ] Enforce phase ordering
- [ ] Add validation gates
- [ ] Implement full transaction semantics

**DeltaService Adoption**
- [ ] Route all delta provider/mutation flows through DeltaService
- [ ] Remove legacy direct calls to `storage.deltalake.*` where possible

---

## 8. Appendices

### Appendix A: Configuration Class Inventory (183 classes)

<details>
<summary>Click to expand full list</summary>

**DataFusion Engine (46)**
- DataFusionRuntimeProfile (158 fields)
- DeltaStorePolicy, DataFusionSqlPolicy, SchemaPolicy
- NormalizePolicy, MaterializationPolicy, ParamTablePolicy
- DeltaScanOptions, DeltaCdfPolicy, DeltaMaintenancePolicy
- ... (36 more)

**Extract (34)**
- RepoOptions, WorklistQueueOptions, ScanOptions
- ExtractPlanOptions, ExtractMaterializeOptions
- ... (29 more)

**CLI (26)**
- RootConfig (94 fields)
- PlanConfig, CacheConfig, GraphAdapterConfig
- IncrementalConfig, DeltaConfig, DocstringsConfig
- ... (20 more)

**Hamilton Pipeline (21)**
- ExecutorConfig, GraphAdapterConfig
- CachePolicyProfile, TagPolicy
- ... (17 more)

**Semantics (13)**
- SemanticConfig, CpgBuildOptions
- IncrementalConfig, PipelinePolicy
- ... (9 more)

**Storage (11)**
- DeltaWritePolicy, FilePruningPolicy
- DeltaLakeConfig, DiskCacheSettings
- ... (7 more)

**Other (32)**
- Schema Spec: 7
- Observability: 7
- RelSpec: 5
- Engine: 5
- CPG: 3
- Cache: 2
- Core: 1

</details>

### Appendix B: Schema Type Feature Matrix

| Feature | TableSchemaSpec | ContractSpec | SchemaContract | DatasetSpec | DatasetLocation |
|---------|----------------|--------------|----------------|-------------|-----------------|
| Column definitions | ✓ | Via table_spec | ✓ | Via table_spec | Via overrides |
| Validation rules | - | ✓ | ✓ | Via contract | - |
| Dedupe settings | - | ✓ | - | Via contract | - |
| Evolution policy | - | - | ✓ | - | - |
| Semantic types | - | - | ✓ | - | - |
| Delta policies | - | - | - | ✓ | ✓ (overrides) |
| Storage options | - | - | - | ✓ | ✓ |

### Appendix C: Registry Interface Comparison

| Operation | Protocol | DatasetCatalog | UdfCatalog | ProviderRegistry |
|-----------|----------|---------------|------------|------------------|
| Register | `register(k, v)` | ✓ | `register_udf()` | ✓ |
| Get | `get(k)` | ✓ | `resolve_function()` | ✓ |
| Contains | `__contains__` | ✓ | `is_registered()` | ✓ |
| Iter | `__iter__` | ✓ | - | ✓ |
| Len | `__len__` | ✓ | - | ✓ |
| Remove | `remove(k)` | - | - | - |
| Clear | `clear()` | - | - | - |
| Snapshot | N/A | - | - | ✓ |
| Items | `items()` | ✓ | - | ✓ |

### Appendix D: Storage Operation Catalog (54 functions)

| File | Function | Category | Tier |
|------|----------|----------|------|
| delta.py | read_delta_table | Read | 1 |
| delta.py | delta_merge_arrow | Write | 1 |
| delta.py | delta_delete_where | Write | 1 |
| delta.py | read_delta_cdf | Read | 1 |
| delta.py | vacuum_delta | Maintenance | 1 |
| delta.py | query_delta_sql | Read | 1 |
| delta.py | delta_table_version | Metadata | 1 |
| delta.py | delta_table_schema | Metadata | 2 |
| delta.py | enable_delta_cdf | Feature | 2 |
| delta.py | disable_delta_cdf | Feature | 2 |
| file_pruning.py | evaluate_and_select_files | Pruning | 1 |
| ... | ... (43 more) | ... | ... |

---

## Summary

This deep-dive investigation reveals significant opportunities for architectural simplification and unification:

1. **Configuration**: 183 classes can be reduced through hierarchical design and duplicate elimination
2. **Schema Contracts**: 9 redundant fields in DatasetLocation can be consolidated via lazy resolution
3. **Fingerprinting**: 28 implementations need unification under CompositeFingerprint with collision fixes
4. **Registries**: 20+ registries need a RegistryFacade + adapters with capability-aware transactions
5. **Observability**: 54 storage functions need comprehensive OpenTelemetry instrumentation and artifact linkage
6. **Delta Integration**: Consolidate provider + mutation surfaces behind a single DeltaService boundary

The implementation roadmap provides a phased approach, starting with high-impact quick wins (storage observability, fingerprint fixes) before tackling breaking changes (configuration hierarchy, registry unification).

**Estimated Total Effort**: 8-10 weeks for full implementation
**Estimated Complexity Reduction**: ~40% reduction in configuration surface area
**Estimated OTel Observability Coverage**: 0% → 100% for storage operations
