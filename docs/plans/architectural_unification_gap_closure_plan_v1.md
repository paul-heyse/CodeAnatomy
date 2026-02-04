# Architectural Unification Gap Closure Plan (v1)

> **Purpose**: Close the remaining gaps between `architectural_unification_deep_dive.md` and the current implementation.
> 
> **Status**: Implementation mostly complete; remaining items called out below.
> 
> **Assumptions**: Design‑phase, breaking changes are acceptable; remove legacy surfaces rather than shim.

---

## Scope 1 — Cached ResolvedDatasetLocation View Model

**Goal**: Make resolution deterministic + memoized, eliminating repeated `resolve_*` logic and repeated resolution work.

### Representative code patterns

```python
from functools import cached_property

class DatasetLocation(StructBaseStrict, frozen=True):
    # ...
    @cached_property
    def resolved(self) -> ResolvedDatasetLocation:
        return resolve_dataset_location(self)
```

```python
def resolve_dataset_location(location: DatasetLocation) -> ResolvedDatasetLocation:
    overrides = location.overrides
    return ResolvedDatasetLocation(
        location=location,
        dataset_spec=location.dataset_spec,
        datafusion_scan=_resolve_override(location, overrides, "datafusion_scan"),
        delta_write_policy=_resolve_override(location, overrides, "delta_write_policy"),
        # ...
    )
```

### Target files
- `src/datafusion_engine/dataset/registry.py`
- `src/datafusion_engine/dataset/resolution.py`
- `src/datafusion_engine/dataset/registration.py`
- `tests/unit/test_dataset_resolution_precedence.py` (extend)

### Deprecate/delete after completion
- Any direct ad‑hoc `resolve_*` calls outside `ResolvedDatasetLocation` (convert to `location.resolved.*`).

### Implementation checklist
- [x] Add `cached_property` for `DatasetLocation.resolved`.
- [x] Update call sites to use `location.resolved` for resolved values.
- [x] Ensure resolution precedence remains unchanged (tests updated/added).

---

## Scope 2 — Msgspec CompositeFingerprint

**Goal**: Align fingerprint primitives with msgspec in the same style as the rest of the config surface.

### Representative code patterns

```python
@msgspec.Struct(frozen=True)
class FingerprintComponent:
    name: str
    value: str
    algorithm: str = "sha256"
    truncated: bool = False
```

```python
@msgspec.Struct(frozen=True)
class CompositeFingerprint:
    version: int
    components: tuple[FingerprintComponent, ...]
```

### Target files
- `src/core/fingerprinting.py`
- `tests/unit/test_composite_fingerprint.py`

### Deprecate/delete after completion
- Dataclass versions of `FingerprintComponent` / `CompositeFingerprint`.

### Implementation checklist
- [x] Convert dataclasses to msgspec structs.
- [x] Ensure existing `CacheKeyBuilder` semantics are preserved.
- [x] Update tests for msgspec round‑trip if needed (no changes required).

---

## Scope 3 — Registration Phase Orchestrator

**Goal**: Enforce registration phase ordering and validation gates described in §4.3/§4.5.

### Representative code patterns

```python
@dataclass(frozen=True)
class RegistrationPhase:
    name: str
    requires: tuple[str, ...] = ()
    validate: Callable[[], None] | None = None

class RegistrationPhaseOrchestrator:
    def run(self, phases: Sequence[RegistrationPhase]) -> None:
        # validate ordering + call validation hooks
        ...
```

### Target files
- `src/datafusion_engine/registry_facade.py`
- `src/datafusion_engine/session/runtime.py`
- `src/datafusion_engine/views/registration.py`
- `src/semantics/registry.py`
- `tests/unit/test_registry_facade_rollback.py` (extend)

### Deprecate/delete after completion
- Implicit phase‑ordering in call‑site chains without validation gates.

### Implementation checklist
- [x] Implement `RegistrationPhaseOrchestrator` with ordering + validation.
- [x] Integrate into RegistryFacade/registration entry points.
- [x] Add tests for ordering violations and validation failures.

---

## Scope 4 — Registry Adapters for Remaining Custom Registries

**Goal**: Provide Registry/SnapshotRegistry adapters for remaining custom registries.

### Representative code patterns

```python
class ViewGraphRegistryAdapter(Registry[str, ViewNode]):
    def register(self, key: str, value: ViewNode) -> None: ...
    def get(self, key: str) -> ViewNode | None: ...
    def __contains__(self, key: str) -> bool: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
```

### Target files
- `src/datafusion_engine/views/graph.py`
- `src/semantics/registry.py`
- `src/datafusion_engine/schema/registry.py`
- `src/datafusion_engine/registry_facade.py`

### Deprecate/delete after completion
- Direct use of bespoke registry APIs in favor of adapters or RegistryFacade.

### Implementation checklist
- [x] Add adapters for view graph + semantic registries.
- [x] Integrate adapters into RegistryFacade construction.
- [x] Update call sites to use registry protocol interfaces where feasible.

---

## Scope 5 — UDTF Surface Cleanup (read_csv/read_parquet)

**Goal**: Remove or replace minimal UDTFs with full‑fidelity provider factories.

### Representative code patterns

```rust
// Either remove the UDTF registration entirely:
// ctx.register_udtf("read_csv", ...)
```

```rust
// Or replace with provider‑factory‑based registration:
ctx.register_table_provider("read_csv", Arc::new(ListingTableProvider::new(...)));
```

### Target files
- `rust/datafusion_ext/src/udtf_sources.rs`
- `rust/datafusion_ext/src/udf_docs.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `docs/architecture/semantic_compiler_design.md` (if references remain)

### Deprecate/delete after completion
- `read_csv` / `read_parquet` UDTFs and their docs/registry snapshot entries.

### Implementation checklist
- [x] Remove UDTF registrations (or replace with provider factories).
- [x] Update Rust doc registry snapshots accordingly.
- [x] Ensure parity tests/doc references are updated (rust UDTF conformance tests updated).

---

## Scope 6 — FunctionFactory ↔ UdfCatalog Snapshot Alignment

**Goal**: Bind UDF catalog snapshots to FunctionFactory policy hash for deterministic runtime UDF surfaces.

### Representative code patterns

```python
@msgspec.Struct(frozen=True)
class UdfCatalogSnapshot:
    specs: Mapping[str, DataFusionUdfSpec]
    function_factory_hash: str | None = None
```

```python
def snapshot(self) -> Mapping[str, object]:
    return {
        "specs": self._custom_specs,
        "function_factory_hash": self._function_factory_hash(),
    }
```

### Target files
- `src/datafusion_engine/udf/catalog.py`
- `src/datafusion_engine/udf/factory.py`
- `src/datafusion_engine/session/runtime.py`

### Deprecate/delete after completion
- Snapshot payloads that omit the FunctionFactory policy hash.

### Implementation checklist
- [x] Extend UdfCatalog snapshot payload with function factory hash.
- [x] Validate hash equality on restore/refresh.
- [x] Add tests for deterministic snapshots.

---

## Scope 7 — Policy‑Driven Scan Defaults (Caching + Stats)

**Goal**: Centralize defaults for `DataFusionScanOptions` based on policy and dataset type.

### Representative code patterns

```python
def apply_scan_policy(
    options: DataFusionScanOptions | None,
    policy: ScanPolicyConfig,
) -> DataFusionScanOptions:
    return replace(
        options or DataFusionScanOptions(),
        collect_statistics=policy.collect_statistics,
        list_files_cache_ttl=policy.list_files_cache_ttl,
        list_files_cache_limit=policy.list_files_cache_limit,
    )
```

### Target files
- `src/schema_spec/system.py`
- `src/datafusion_engine/dataset/registry.py`
- `src/datafusion_engine/session/runtime.py`
- `src/datafusion_engine/delta/scan_config.py`

### Deprecate/delete after completion
- Hard‑coded defaults spread across call sites.

### Implementation checklist
- [x] Introduce `ScanPolicyConfig` (or extend existing policy bundle).
- [x] Apply policy defaults during dataset resolution.
- [x] Add coverage for delta vs non‑delta defaults.

---

## Scope 8 — Storage Observability Metrics + Dashboard Hooks

**Goal**: Complete Phase‑4 observability: add metrics collection and dashboard‑ready payloads.

### Representative code patterns

```python
from obs.otel.metrics import record_stage_duration

with stage_span(...):
    # operation
    record_stage_duration("storage.vacuum.duration_ms", elapsed_ms)
```

### Target files
- `src/storage/deltalake/delta.py`
- `src/storage/deltalake/file_pruning.py`
- `src/obs/otel/metrics.py`
- `src/datafusion_engine/delta/observability.py`

### Deprecate/delete after completion
- Ad‑hoc metrics payloads without OTel integration (if any).

### Implementation checklist
- [x] Add metric emission for Tier‑1/Tier‑2 storage ops.
- [x] Extend artifact payloads for dashboard correlation.
- [x] Document metric names + units in docs.

---

## Remaining Scope

All scope items are implemented. Pending only repo-wide validation runs (ruff/pyrefly/pyright).

---

## Completion Definition

The gap‑closure work is complete when:
- `DatasetLocation.resolved` is cached and all resolution flows use it.
- Composite fingerprint types are msgspec‑backed.
- Registration phases are enforced by an orchestrator.
- All remaining custom registries have Registry adapters (or are removed).
- `read_csv` / `read_parquet` UDTFs are removed or replaced with provider factories.
- UDF snapshots include and validate FunctionFactory policy hash.
- Scan‑option defaults are policy‑driven and centralized.
- Storage observability includes metrics + dashboard‑ready payloads.
