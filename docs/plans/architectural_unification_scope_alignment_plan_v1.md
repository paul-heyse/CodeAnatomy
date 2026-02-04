# Architectural Unification Scope Alignment Plan (v1)

> **Purpose**: Capture the remaining architectural-unification work (per `docs/architecture/architectural_unification_deep_dive.md`) and add the missing **msgspec deployment** requirements. Each scope item includes representative code snippets, target files, deprecations, and a concrete checklist.
> 
> **Assumptions**: Design-phase, breaking changes are acceptable. Legacy compatibility layers are not retained.

---

## Scope 0 — Msgspec Deployment Audit + Conversion (Required)

**Goal**: Ensure all config/contract/registry surfaces specified in the design doc are **msgspec-backed** (or use `StructBaseStrict` / `StructBaseCompat` when required by repo patterns). This includes DatasetLocation, overrides/resolved views, new config sub-classes, and registry metadata payloads.

**Status**: Completed.

### Representative code patterns

```python
import msgspec

@msgspec.Struct(frozen=True)
class DatasetLocationOverrides:
    delta_scan: DeltaScanOptions | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    # ... other overrides

@msgspec.Struct(frozen=True)
class ResolvedDatasetLocation:
    location: DatasetLocation
    dataset_spec: DatasetSpec | None
    datafusion_scan: DataFusionScanOptions | None
    delta_schema_policy: DeltaSchemaPolicy | None
```

```python
from serde_msgspec import StructBaseStrict

class ExecutionConfig(StructBaseStrict, frozen=True):
    target_partitions: int | None = None
    batch_size: int | None = None
    # ...
```

### Target files
- `src/datafusion_engine/dataset/registry.py` (DatasetLocationOverrides, DatasetLocation, ResolvedDatasetLocation)
- `src/datafusion_engine/session/runtime.py` (ExecutionConfig, CatalogConfig, DataSourceConfig, FeatureGatesConfig, DiagnosticsConfig)
- `src/cli/config_models.py` (RootConfig + nested configs already msgspec; verify use is consistent)
- `src/datafusion_engine/registry_facade.py` (RegistrationResult payload if msgspec is preferred)

### Deprecate/delete after completion
- Any remaining dataclass-based config/override types that are still used as config surfaces (convert rather than keep duplicates).

### Implementation checklist
- [x] Inventory all config/override/resolution classes and mark msgspec coverage.
- [x] Convert any remaining `@dataclass` config types to msgspec-backed types where required by the plan.
- [x] Update serialization / fingerprint payloads to use msgspec-safe payloads.
- [x] Validate msgspec-encoded round-trip for config payloads (where serialization is used).

---

## Scope 1 — Configuration Architecture Unification Completion

**Goal**: Finish the hierarchical config model as described in section 1 of the deep-dive, and reconcile any residual flat or legacy patterns.

**Status**: Completed.

### Representative code patterns

```python
class DataFusionRuntimeProfile(StructBaseStrict, frozen=True):
    execution: ExecutionConfig = msgspec.field(default_factory=ExecutionConfig)
    catalog: CatalogConfig = msgspec.field(default_factory=CatalogConfig)
    data_sources: DataSourceConfig = msgspec.field(default_factory=DataSourceConfig)
    features: FeatureGatesConfig = msgspec.field(default_factory=FeatureGatesConfig)
    diagnostics: DiagnosticsConfig = msgspec.field(default_factory=DiagnosticsConfig)
    policies: PolicyBundleConfig = msgspec.field(default_factory=PolicyBundleConfig)
```

```python
class RootConfig(StructBaseStrict, frozen=True):
    plan: PlanConfig | None = None
    cache: CacheConfig | None = None
    # nested only; no flattened mirrors
```

### Target files
- `src/datafusion_engine/session/runtime.py`
- `src/cli/config_models.py`
- `src/cli/config_loader.py`
- `src/hamilton_pipeline/*` (config ingestion paths)
- `src/semantics/*` (call sites relying on old flat fields)

### Deprecate/delete after completion
- Any remaining references to legacy flat config keys (if any resurface in config loading).

### Implementation checklist
- [x] Validate no call sites rely on removed flat RootConfig fields.
- [x] Align execution/catalog/data_sources/feature/diagnostics fields with doc expectations.
- [x] Ensure `fingerprint_payload()` uses settings/telemetry hashes as canonical inputs.

---

## Scope 2 — Schema Contract Consolidation (Completion)

**Goal**: Continue consolidation of schema contract types and use ResolvedDatasetLocation as the authoritative resolved view.

**Status**: Completed.

### Representative code patterns

```python
def resolve_dataset_location(location: DatasetLocation) -> ResolvedDatasetLocation:
    overrides = location.overrides
    # centralize resolve_* logic here
    return ResolvedDatasetLocation(
        location=location,
        dataset_spec=location.dataset_spec,
        datafusion_scan=_resolve_datafusion_scan(location, overrides),
        delta_schema_policy=_resolve_override(location, overrides, "delta_schema_policy"),
        # ...
    )
```

### Target files
- `src/datafusion_engine/dataset/registry.py`
- `src/datafusion_engine/dataset/registration.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/schema/contracts.py`

### Deprecate/delete after completion
- Any direct override fields left on DatasetLocation (all override access should go through `overrides` or `resolved`).

### Implementation checklist
- [x] Ensure all `resolve_*` helpers route through `ResolvedDatasetLocation`.
- [x] Create/update resolution precedence documentation.
- [x] Add tests covering override precedence + resolved schema behavior.

---

## Scope 3 — Fingerprinting Standardization (Completion)

**Goal**: Finish CompositeFingerprint rollout and ensure remaining hashes are standardized and documented.

**Status**: Completed.

### Representative code patterns

```python
class PlanCacheKey(StructBaseStrict, frozen=True):
    def as_key(self) -> str:
        return self.composite_fingerprint().as_cache_key(prefix="plan")
```

```python
@dataclass(frozen=True)
class CompositeFingerprint:
    def as_cache_key(self, *, prefix: str = "") -> str:
        builder = CacheKeyBuilder(prefix=prefix)
        builder.add("version", self.version)
        builder.add("components", {c.name: c.value for c in self.components})
        return builder.build()
```

### Target files
- `src/core/fingerprinting.py`
- `src/datafusion_engine/plan/cache.py`
- `src/semantics/plans/fingerprints.py`
- `src/core/config_base.py` (protocol docs and usage guidance)

### Deprecate/delete after completion
- Legacy hash utilities that are no longer referenced (evaluate for removal once all call sites moved).

### Implementation checklist
- [x] Verify plan fingerprint truncation at 32 chars (128 bits).
- [x] Confirm dual-read cache compatibility for legacy plan cache keys.
- [x] Document fingerprint schema evolution (location TBD in docs).

---

## Scope 4 — Registry Pattern Unification (Completion)

**Goal**: Expand RegistryFacade use and unify registry protocol across remaining registries.

**Status**: Completed.

### Representative code patterns

```python
class UdfCatalogAdapter:
    def register(self, key: str, value: DataFusionUdfSpec) -> None:
        if key != value.func_id:
            raise ValueError("...")
        self.catalog.register_udf(value)
```

```python
class RegistryFacade:
    def register_dataset_df(...):
        checkpoint = self._checkpoint_optional()
        try:
            self._datasets.register(name, location, overwrite=overwrite)
            return self._providers.register_location(...)
        except Exception:
            self._rollback_optional(checkpoint)
            raise
```

### Target files
- `src/datafusion_engine/registry_facade.py`
- `src/datafusion_engine/udf/catalog.py`
- `src/semantics/registry.py`
- `src/datafusion_engine/views/graph.py`
- `src/cache/registry.py`
- `src/serde_schema_registry.py`

### Deprecate/delete after completion
- Ad-hoc registry method names that duplicate `register/get/__contains__/__iter__/__len__`.

### Implementation checklist
- [x] Ensure remaining registries implement Registry protocol or provide adapters.
- [x] Add phase ordering/orchestration layer if required by plan (not required after review).
- [x] Route critical registrations through RegistryFacade.

---

## Scope 5 — Storage Observability (Tier 2/3 Completion)

**Goal**: Finish OTel spans for remaining Delta storage functions and align artifact linkage where feasible.

**Status**: Completed.

### Representative code patterns

```python
with stage_span(
    "storage.feature_control",
    stage="storage",
    scope_name=SCOPE_STORAGE,
    attributes={"codeanatomy.table": path, "codeanatomy.feature_name": feature},
):
    enable_delta_feature(...)
```

### Target files
- `src/storage/deltalake/delta.py`
- `src/storage/deltalake/file_pruning.py`
- `src/obs/otel/constants.py`

### Deprecate/delete after completion
- None; add spans without removing functionality.

### Implementation checklist
- [x] Instrument Tier 2 enable/disable feature functions.
- [x] Add Tier 3 spans for any remaining configuration utilities.
- [x] Evaluate trace/span ID capture in artifact payloads (if feasible).

---

## Scope 6 — DataFusion + Delta Integration Unification (Completion)

**Goal**: Route *all* Delta provider/read/mutation flows through DeltaService and remove direct storage calls where possible.

**Status**: Completed.

### Representative code patterns

```python
class DeltaService:
    def read_table(self, request: DeltaReadRequest) -> pa.Table:
        resolved = replace(request, runtime_profile=self.profile)
        return read_delta_table(resolved)
```

```python
delta_service = runtime.profile.delta_service()
return delta_service.read_table(DeltaReadRequest(path=..., ...))
```

### Target files
- `src/datafusion_engine/delta/service.py`
- `src/semantics/incremental/*`
- `src/datafusion_engine/dataset/registry.py`
- `src/datafusion_engine/plan/artifact_store.py`
- `src/datafusion_engine/io/write.py`
- `src/cli/commands/diag.py`
- `src/engine/delta_tools.py`

### Deprecate/delete after completion
- Direct call sites of `storage.deltalake.delta_*` in application code (retain storage module but route via service).

### Implementation checklist
- [x] Migrate read/provider flows to DeltaService.
- [x] Centralize storage option resolution in DeltaService.
- [x] Verify call sites no longer import storage delta functions directly.

---

## Scope 7 — UDF/UDTF Surface and Doc Alignment

**Goal**: Update docs and tests to reflect `udf_expr` as the canonical expression helper and the Rust-backed UDTF surfaces.

**Status**: Completed.

### Representative code patterns

```python
from datafusion_engine.udf.expr import udf_expr

expr = udf_expr("span_start", col("span"))
```

### Target files
- `docs/architecture/semantic_compiler_design.md`
- `docs/plans/semantic_approach_review.md`
- `docs/plans/datafusion_rust_udf_architecture_cleanup_migration_notes.md`
- `docs/plans/end_to_end_codebase_review_implementation_plan_v1.md`

### Deprecate/delete after completion
- Any documentation referencing legacy per-UDF shim helpers.

### Implementation checklist
- [x] Replace legacy shim references with `udf_expr` examples.
- [x] Verify test stubs (e.g., `src/test_support/datafusion_ext_stub.py`) align with new UDF surface.

---

## Scope 8 — Test + Validation Coverage

**Goal**: Ensure tests cover the newly finalized architecture.

**Status**: Completed.

### Representative test patterns

```python
location = DatasetLocation(
    path=...,
    overrides=DatasetLocationOverrides(delta_schema_policy=policy),
)
assert resolve_dataset_location(location).delta_schema_policy == policy
```

### Target files
- `tests/integration/*`
- `tests/unit/*`

### Deprecate/delete after completion
- Legacy tests that assert behavior from deprecated config/registry patterns.

### Implementation checklist
- [x] Add test coverage for DeltaService read/provider usage.
- [x] Add tests for RegistryFacade rollback semantics.
- [x] Add tests for CompositeFingerprint serialization/stability.
- [x] Add tests for msgspec round-trip for config classes.

---

## Completion Definition

The plan is complete when:
- All configuration and resolution classes specified by the design are msgspec-backed.
- No application code directly calls `storage.deltalake.*` for Delta operations; DeltaService is the boundary.
- Registry interfaces are standardized or adapter-wrapped.
- Storage observability spans cover Tier 1–3 functions.
- Docs reflect the new UDF surface and legacy references are removed.
- Tests validate the new architecture surface and override resolution precedence.
