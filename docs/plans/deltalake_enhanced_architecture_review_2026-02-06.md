# DeltaLake Consolidated Architecture Review (2026-02-06)

## Scope and Method

This document is the consolidated net set of all improvement opportunities for the DeltaLake integration stack, merging and superseding the initial review (`deltalake_legacy_deprecation_architecture_review_2026-02-06.md`).

It covers three categories:

1. **Legacy architecture elements to deprecate, delete, or explicitly retain.**
2. **Function-level design enhancements** (parameter reduction, error handling, monolith decomposition, write-path hardening).
3. **Class and helper consolidation** with spec-vs-runtime separation and msgspec adoption.

Grounding: CQ-first discovery (`./cq calls` for all key functions), DataFusion/DeltaLake reference material probing, full module reads of `src/datafusion_engine/delta/` (16 modules, ~8.5K LOC) and `src/storage/deltalake/` (7 modules, ~3.5K LOC). msgspec reference (`docs/python_library_reference/msgspec.md`) used for model policy recommendations.

---

## 1) Legacy Elements: Deprecate, Delete, or Retain

### 1.1 QueryBuilder path (delete-ready)

- **Candidates**:
  - `src/storage/deltalake/query_builder.py` (entire module)
  - `query_delta_sql(..., use_querybuilder=True)` branch in `src/storage/deltalake/delta.py`
- **Evidence**:
  - `query_delta_sql` has a single callsite (`tests/harness/delta_smoke.py:92`).
  - `open_delta_table` has one callsite (`src/storage/deltalake/delta.py:483`).
  - `execute_query` has one callsite (`src/storage/deltalake/delta.py:486`).
  - `query_builder.py` itself warns this is ad-hoc-only (`src/storage/deltalake/query_builder.py:33-38`).
- **Action**: Migrate `tests/harness/delta_smoke.py` off `query_delta_sql` first, then delete QueryBuilder surface completely. SessionContext + Delta TableProvider is the only production query path (see Section 2.1).

### 1.2 `storage/deltalake/scan_profile.py` (delete candidate)

- **Evidence**: The module is a single-function shim (`build_delta_scan_config`) that delegates entirely to `datafusion_engine.dataset.policies.resolve_delta_scan_options`. Zero additional logic.
- **Action**: Inline the call at usage sites, then delete the file. Update `storage/deltalake/__init__.py` to remove the re-export.

### 1.3 Bidirectional CDF conversion helpers (delete after unification)

- **Candidates**:
  - `cdf_options_to_spec()` (`storage/deltalake/delta.py:602-623`)
  - `cdf_options_from_spec()` (`storage/deltalake/delta.py:626-651`)
- **Evidence**: These exist solely because `DeltaCdfOptions` (dataclass, `delta.py:590`) and `DeltaCdfOptionsSpec` (StructBaseStrict, `specs.py:29`) duplicate the same concept. The polymorphic adapter `cdf_options_payload()` in `payload.py` already accepts both types, proving they share identical fields.
- **Action**: Unify to a single canonical type (see Section 3.4), then delete both converters (~50 lines).

### 1.4 Feature-toggle functions in `storage/deltalake/delta.py` (consolidate)

- **Candidates**: 10 `disable_delta_*` functions and 12 `enable_delta_*` functions (lines 1300-2226), each following an identical pattern:
  1. Open a tracing span.
  2. Build a `DeltaFeatureEnableRequest`.
  3. Call the corresponding control-plane function inside `try/except (ImportError, RuntimeError, TypeError, ValueError)`.
  4. Record a feature mutation artifact.
- **Evidence**: CQ analysis shows `total_sites: 0` for most `disable_*` functions (confirmed in initial review). The `enable_*` functions all have exactly one caller each in `DeltaFeatureOps` (`service.py:149-314`).
- **Action**: Replace all 22 functions with a single polymorphic `_feature_toggle(options, *, operation, control_plane_fn, **kwargs)` dispatcher. Expose the current signatures as thin wrappers if external compatibility is needed, but the storage layer should not own this logic.

### 1.5 `_DeltaFeatureMutationRecord` and `_DeltaMaintenanceRecord` (fold)

- **Location**: `storage/deltalake/delta.py:151-175`
- **Evidence**: These internal dataclasses are constructed only to be immediately destructured into observability payloads. They carry no behavior.
- **Action**: Fold fields directly into the observability call sites or replace with `DeltaMutationArtifact` / `DeltaMaintenanceArtifact` (StructBaseStrict types that already exist in `observability.py`).

### 1.6 `DeltaSnapshotLookup` (fold)

- **Location**: `storage/deltalake/delta.py:523-557`
- **Evidence**: A frozen dataclass whose sole purpose is grouping `DeltaTable` + version + storage options for a single internal helper. Only constructed and destructured within the same module.
- **Action**: Replace with a local tuple or inline at the call site.

### 1.7 `storage/__init__.py` and `storage/io.py` (staged delete)

- **Evidence**: No internal `from storage.io import ...` usage found (confirmed via CQ). `storage/__init__.py` is a pure re-export surface.
- **Action**: If no external dependents exist, delete both. If external dependents are unknown, emit a deprecation warning for one release cycle, then delete.

### 1.8 Explicit retain list (not deletable yet)

The following items were evaluated and should be **kept** in their current form:

**A) Degraded fallback seams**
- `_degraded_delta_provider_bundle` and `_degraded_delta_cdf_bundle`
- **Evidence**: `_degraded_delta_provider_bundle` still has one live callsite via strictness opt-out (`src/datafusion_engine/dataset/resolution.py:167`). `_degraded_delta_cdf_bundle` is still wired in CDF resolution degraded path.
- **Disposition**: Do not delete unless degraded mode is removed entirely.

**B) File index/pruning stack**
- `build_delta_file_index_from_add_actions` and `evaluate_and_select_files`
- **Evidence**: Both are actively used by lineage scan (`src/datafusion_engine/lineage/scan.py:357` and `scan.py:359`).
- **Disposition**: Not legacy dead code; active and required for pruning behavior.

**C) Service wrapper functions with single callsites**
- `read_delta_table_eager`, `read_delta_cdf_eager`, `delta_history_snapshot`, `delta_protocol_snapshot`, `delta_cdf_enabled`
- **Evidence**: Each has one callsite, all in `DeltaService` methods.
- **Disposition**: Thin but useful as service boundary points. Fold only as part of a wider API simplification.

---

## 2) Function-Level Design Enhancements

### 2.1 Enforce provider-first, SessionContext-first execution

- Keep `SessionContext + Delta TableProvider` as the only production query path.
- Remove QueryBuilder path entirely after harness migration (Section 1.1).
- **Rationale**: DataFusion integration guidance favors direct provider registration for pushdown/planning quality. QueryBuilder is a convenience path and should remain non-production.

### 2.2 Decompose the `delta.py` monolith (3,539 lines, 105 top-level declarations)

The monolith mixes five distinct concerns. Proposed split:

| Concern | Current lines (approx.) | Target module |
|---------|------------------------|---------------|
| Read operations | 818-1017 | `storage/deltalake/read_ops.py` |
| Metadata queries | 1019-1194 | `storage/deltalake/metadata_ops.py` |
| Feature mutations | 1300-2226 | `storage/deltalake/feature_ops.py` |
| Maintenance (vacuum/checkpoint/cleanup) | 2490-2642 | `storage/deltalake/maintenance_ops.py` |
| Write/Mutation orchestration | 2644-3182 | `storage/deltalake/write_ops.py` |
| Data models (dataclasses) | 116-777 | `storage/deltalake/models.py` (temporary; see Section 3) |

Keep `delta.py` as a compatibility re-export shim during migration, then delete.

### 2.3 Extract repeated feature-mutation error handling

**Current pattern** (repeated 22+ times in `delta.py:1300-2226`):
```python
with _feature_control_span(options, operation="enable_X"):
    ctx, request = _feature_enable_request(options)
    try:
        from datafusion_engine.delta.control_plane import delta_enable_X
        report = delta_enable_X(ctx, request=request, **kwargs)
    except (ImportError, RuntimeError, TypeError, ValueError) as exc:
        msg = f"Failed to enable Delta X: {exc}"
        raise RuntimeError(msg) from exc
    _record_delta_feature_mutation(...)
return report
```

**Problems**:
- Broad exception union `(ImportError, RuntimeError, TypeError, ValueError)` catches unrelated errors.
- Re-raising as `RuntimeError` loses the original exception type.
- No classification between recoverable (ImportError) and permanent (schema error) failures.

**Proposed fix**: Extract to a single dispatcher:
```python
def _execute_feature_operation(
    options: DeltaFeatureMutationOptions,
    *,
    operation: str,
    control_plane_fn: Callable[..., Mapping[str, object]],
    **kwargs: object,
) -> Mapping[str, object]:
    with _feature_control_span(options, operation=operation):
        ctx, request = _feature_enable_request(options)
        report = _invoke_control_plane(control_plane_fn, ctx, request=request, **kwargs)
        _record_delta_feature_mutation(...)
    return report
```

### 2.4 Unify retry and fallback logic

**Current state**: Two independent mechanisms exist:
- **Retry** (`_delta_retry_classification` + `_delta_retry_delay` in `delta.py:186-264`) used by `delta_delete_where`.
- **Fallback** (`_should_fallback_delta_merge` in `delta.py:~3053`) used by `delta_merge_arrow`.

**Problems**:
- Different exception classification criteria.
- No shared observability for retry/fallback decisions.
- Bare `except Exception` in both paths risks masking programming errors.

**Proposed fix**: Create a unified `DeltaOperationOutcome` enum:
```python
class DeltaOperationOutcome(StrEnum):
    COMMITTED = "committed"
    CONFLICT_RETRIABLE = "conflict_retriable"
    CONFLICT_REQUIRES_VERIFICATION = "conflict_requires_verification"
    FALLBACK_REQUIRED = "fallback_required"
    FATAL = "fatal"
```
Classification function accepts exception + policy, returns outcome. Both retry and fallback paths use the same classifier. Persist verification traces tied to table history and snapshot key where retries are attempted.

### 2.5 Reduce `DeltaMergeArrowRequest` parameter count (20 params)

**Current state**: `storage/deltalake/delta.py:725-748` defines a frozen dataclass with 20 fields conflating three concerns:
1. Merge semantics (predicates, updates, inserts, aliases).
2. Infrastructure (storage_options, log_storage_options, runtime_profile).
3. Commit metadata (commit_properties, commit_metadata, extra_constraints).

**Proposed decomposition**:
```python
class DeltaMergeOperations(StructBaseStrict, frozen=True):
    matched_predicate: str | None = None
    matched_updates: Mapping[str, str] | None = None
    not_matched_predicate: str | None = None
    not_matched_inserts: Mapping[str, str] | None = None
    not_matched_by_source_predicate: str | None = None
    delete_not_matched_by_source: bool = False
    update_all: bool = False
    insert_all: bool = False

class DeltaMergeArrowRequest(StructBaseStrict, frozen=True):
    path: str
    source: object  # TableLike | RecordBatchReaderLike (runtime handle)
    predicate: str
    operations: DeltaMergeOperations
    source_alias: str | None = "source"
    target_alias: str | None = "target"
    # Infrastructure fields via DeltaTableRef composition
    table_ref: DeltaTableRefFields  # (see Section 3.2)
    commit: DeltaCommitOptionsSpec | None = None
```

### 2.6 Make `DeltaMutationRequest.validate()` automatic

**Current state**: `service.py:70-88` defines a dataclass with `.validate()` that callers must remember to invoke.

**Fix**: Move validation to `__post_init__` so invariant is always enforced at construction time.

### 2.7 Introduce a first-class provider build contract artifact

Add a single typed artifact for provider construction (e.g. `DeltaProviderBuildResult`), capturing:
- Canonical URI, resolved version/timestamp, and snapshot identity.
- Entrypoint/module/ctx-kind/probe-result.
- Scan config and effective options.
- Pushdown classification summary.
- Object store registration outcome.
- Storage profile fingerprint and provenance.

This removes repeated ad-hoc dict payloads and gives deterministic diagnostics and plan artifacts. Subsumes the untyped `DeltaProviderBundle` fields (see 2.8 below).

### 2.8 Strengthen `DeltaProviderBundle` response typing

**Current state** (`control_plane.py:73-82`):
```python
class DeltaProviderBundle(StructBaseStrict, frozen=True):
    provider: object
    snapshot: Mapping[str, object]      # Untyped
    scan_config: Mapping[str, object]   # Untyped
    scan_effective: dict[str, object]   # Untyped
    add_actions: Sequence[Mapping[str, object]] | None = None  # Untyped
```

**Proposed fix**: Decode Rust responses into typed structs immediately:
```python
class DeltaScanConfigPayload(StructBaseCompat, frozen=True):
    file_column_name: str | None = None
    enable_parquet_pushdown: bool | None = None
    schema_force_view_types: bool | None = None
    wrap_partition_values: bool | None = None
    has_schema: bool | None = None
    schema_ipc: str | None = None  # Base64

class DeltaSnapshotPayload(StructBaseCompat, frozen=True):
    version: int | None = None
    snapshot_timestamp: int | None = None
    min_reader_version: int | None = None
    min_writer_version: int | None = None
    reader_features: tuple[str, ...] = ()
    writer_features: tuple[str, ...] = ()
    table_properties: Mapping[str, str] = msgspec.field(default_factory=dict)
    schema_json: str | None = None
    partition_columns: tuple[str, ...] = ()
```
Use `StructBaseCompat` (not Strict) because Rust may add fields across versions.

### 2.9 Replace ad-hoc observability dict construction

**Current state**: `service.py:424-449` and `delta.py:3074-3103` hand-build `dict[str, object]` payloads for diagnostics with no schema enforcement.

**Fix**: Use the existing StructBaseStrict artifact types in `observability.py` (which already exist for snapshot, mutation, scan plan, maintenance, and feature state). For provider artifacts, add a new type:
```python
class DeltaProviderArtifact(StructBaseStrict, frozen=True):
    event_time_unix_ms: int
    run_id: str | None = None
    dataset_name: str | None = None
    table_uri: str = ""
    provider_kind: str | None = None
    module: str | None = None
    entrypoint: str | None = None
    ctx_kind: str | None = None
    compatible: bool = False
    available: bool = False
    error: str | None = None
```

### 2.10 Add pushdown/pruning observability to provider construction

**Current state**: Provider construction (`control_plane.py:698-773`) extracts `scan_config` and `add_actions` but does not classify pushdown behavior.

**Proposed enhancement**: After provider construction, emit:
- Exact vs inexact pushdown classification.
- `files_scanned` / `files_pruned` counts.
- Predicate normalization artifacts.

Use `DeltaScanPlanArtifact` (already defined in `observability.py:131-143`) for this purpose.

### 2.11 Harden `schema_guard.py` nullability checks

**Current state** (`schema_guard.py:53-71`): `_ensure_additive` compares field types via `updated_field.type != field.type` but does not check `field.nullable`. A non-nullable to nullable change (or vice versa) passes silently.

**Fix**: Add `if updated_field.nullable != field.nullable: mismatched.append(name)` and optionally distinguish "type mismatch" from "nullability mismatch" in error reporting.

### 2.12 Strengthen write-path contract around schema and CDF semantics

Add explicit write boundary checks (before commit):
- Dictionary-to-physical schema normalization where needed.
- Strict handling of injected partition/path metadata columns (must be dropped/renamed before persistence unless part of target schema).
- Explicit `_change_type` split policy (normal vs CDF batches).
- Constraints/invariants batch validation before commit.

This aligns with deltalake-core write execution patterns and avoids implicit behavior drift.

### 2.13 Add checkpoint/cleanup guardrails

Given known checkpoint/cleanup hazards documented in Delta integration references:
- Gate checkpoint and cleanup with explicit policy constraints.
- Require safe retention windows and checkpoint validity checks.
- Fail-safe behavior for partial snapshots / aggressive cleanup scenarios.

### 2.14 Collapse remaining mixed authority for object-store settings

`StorageProfile` is good progress (see Section 3.5). Complete it by:
- Making it the **only** emitter for both DataFusion object-store config and delta-rs storage options.
- Adding immutable profile provenance/fingerprint into all provider/write artifacts.
- Forbidding any direct ad-hoc options dict construction outside a single adapter layer.

---

## 3) Class Consolidation and Spec-vs-Runtime Separation

### 3.1 Model policy (three categories only)

Following the existing `serde_msgspec.py` base hierarchy:

| Category | Base | `frozen` | `forbid_unknown_fields` | Use case |
|----------|------|----------|------------------------|----------|
| **Spec (boundary + persisted)** | `StructBaseStrict` | True | True | API requests, control-plane payloads, observability artifacts |
| **Spec (forward-compatible)** | `StructBaseCompat` | True | False | Persisted artifacts that must tolerate additive fields (Rust response decoding, protocol snapshots) |
| **Spec (hot path)** | `StructBaseHotPath` | True | False | High-volume, immutable artifacts (currently unused; candidate: file index entries) |
| **Runtime handle** | `@dataclass(frozen=True)` | True | N/A | Non-serializable in-memory objects holding `SessionContext`, `DeltaTable`, `DataFrame`, `pa.Table` |

### 3.2 Extract `DeltaTableRefFields` mixin to eliminate field repetition

**Current problem**: 15+ request types in `control_plane.py` repeat `table_uri`, `storage_options`, `version`, `timestamp`, and a `.table_ref` property.

**Proposed solution**: Define a common field group:
```python
class DeltaTableRefFields(StructBaseStrict, frozen=True):
    table_uri: str
    storage_options: Mapping[str, str] | None = None
    version: int | None = None
    timestamp: str | None = None
```

Then compose via inheritance or embedding. Since msgspec supports struct inheritance, each request type inherits from this:
```python
class DeltaWriteRequest(DeltaTableRefFields, frozen=True):
    data_ipc: msgspec.Raw
    mode: str
    schema_mode: str | None = None
    partition_columns: Sequence[str] | None = None
    # ... write-specific fields
    gate: DeltaFeatureGate | None = None
    commit_options: DeltaCommitOptionsSpec | None = None
```

The `.table_ref` property moves to `DeltaTableRefFields` once and is inherited by all 15 types.

### 3.3 Concrete model unification plan

| Storage-layer dataclass | Control-plane spec | Action |
|------------------------|--------------------|--------|
| `DeltaCdfOptions` (`delta.py:590`) | `DeltaCdfOptionsSpec` (`specs.py:29`) | **Unify**: Keep `DeltaCdfOptionsSpec`, delete dataclass + converters |
| `DeltaVacuumOptions` (`delta.py:655`) | `DeltaVacuumRequest` (`control_plane.py:288`) | **Unify**: Keep `DeltaVacuumRequest` as canonical spec. `DeltaVacuumOptions` becomes a thin runtime-only adapter if needed |
| `DeltaSchemaRequest` (`delta.py:683`) | `DeltaSnapshotRequest` (`control_plane.py:92`) | **Evaluate**: `DeltaSchemaRequest` has `path` + version/storage fields. Could use `DeltaTableRefFields` |
| `DeltaReadRequest` (`delta.py:695`) | No spec equivalent | **Migrate to spec**: Create `DeltaReadSpec(DeltaTableRefFields)` |
| `DeltaDeleteWhereRequest` (`delta.py:710`) | `DeltaDeleteRequest` (`control_plane.py:186`) | **Unify**: Keep `DeltaDeleteRequest`. `DeltaDeleteWhereRequest` wraps runtime handles (ctx, commit_properties) |
| `DeltaMergeArrowRequest` (`delta.py:725`) | `DeltaMergeRequest` (`control_plane.py:233`) | **Unify**: Decompose per Section 2.5. Keep `DeltaMergeRequest` for Rust FFI |
| `DeltaFeatureMutationOptions` (`delta.py:136`) | `DeltaFeatureMutationRequest` (`service.py:90`) | **Unify**: Keep `DeltaFeatureMutationRequest` (already StructBaseStrict). `DeltaFeatureMutationOptions` adds `runtime_profile` (runtime handle) |
| `DeltaWriteResult` (`delta.py:561`) | No spec equivalent | **Migrate to spec**: Create `DeltaWriteResultSpec(StructBaseCompat)` |
| `DeltaDataCheckRequest` (`delta.py:668`) | No spec equivalent | **Keep as dataclass**: Contains `ctx` and `data` (runtime handles) |
| `SnapshotKey` (`delta.py:117`) | No spec equivalent | **Migrate to spec**: Pure value type, ideal for `StructBaseStrict` |
| `IdempotentWriteOptions` (`delta.py:571`) | No spec equivalent | **Migrate to spec**: Pure value type |
| `DeltaInput` (`delta.py:2670`) | No spec equivalent | **Keep as dataclass**: Contains `data` (runtime handle) |

### 3.4 Unified CDF options (detailed walkthrough)

**Before** (3 types + 2 converters):
```
DeltaCdfOptions (dataclass, storage/deltalake/delta.py:590)
   columns: list[str] | None
DeltaCdfOptionsSpec (StructBaseStrict, datafusion_engine/delta/specs.py:29)
   columns: tuple[str, ...] | None
cdf_options_to_spec() - converts list -> tuple
cdf_options_from_spec() - converts tuple -> list
cdf_options_payload() - polymorphic adapter for both
```

**After** (1 type):
```
DeltaCdfOptionsSpec (StructBaseStrict, datafusion_engine/delta/specs.py:29)
   columns: tuple[str, ...] | None  # canonical
```

Migration steps:
1. Replace all `DeltaCdfOptions` imports with `DeltaCdfOptionsSpec`.
2. Update call sites that pass `columns=list(...)` to pass `columns=tuple(...)`.
3. Delete `cdf_options_to_spec()`, `cdf_options_from_spec()`.
4. Remove `DeltaCdfOptions` from `storage/deltalake/delta.py`.
5. Simplify `cdf_options_payload()` to accept only `DeltaCdfOptionsSpec | None`.

### 3.5 Consolidate `DeltaStorePolicy` and `StorageProfile` to msgspec

**Current state**: Both are frozen dataclasses in `store_policy.py`. `DeltaStorePolicy` extends `FingerprintableConfig` (a protocol for deterministic hashing).

**Proposed**: Migrate `StorageProfile` to `StructBaseStrict`. `DeltaStorePolicy` can remain a dataclass if `FingerprintableConfig` is a protocol that requires methods, but the value fields should be typed:
```python
class StorageProfile(StructBaseStrict, frozen=True):
    table_uri: str
    canonical_uri: str
    scheme: str | None = None
    storage_options: Mapping[str, str] = msgspec.field(default_factory=dict)
    log_storage_options: Mapping[str, str] = msgspec.field(default_factory=dict)
```

For `DeltaStorePolicy`, consider extracting the fingerprint computation to a standalone function rather than inheriting from `FingerprintableConfig`, which would allow it to become a pure `StructBaseStrict`.

### 3.6 Introduce `StorageOptionsSpec` to replace raw `Mapping[str, str]`

**Current state**: `type StorageOptions = Mapping[str, str]` is a bare alias in `delta.py:56`. No validation of keys or values.

**Proposed**: A lightweight typed wrapper:
```python
class StorageOptionsSpec(StructBaseStrict, frozen=True):
    options: Mapping[str, str] = msgspec.field(default_factory=dict)
```

This enables:
- JSON Schema generation for documentation (`msgspec.json.schema(StorageOptionsSpec)`).
- Validation hooks for credential masking in diagnostic output.
- Future extension (add `scheme` field, per-provider key validation).

### 3.7 msgspec practices to adopt across all new spec types

From `docs/python_library_reference/msgspec.md` and current `serde_msgspec` policy:

| Practice | Status | Action |
|----------|--------|--------|
| `kw_only=True` | Already set via base classes | Maintain |
| `frozen=True` | Already set | Maintain |
| `omit_defaults=True` | Already set | Maintain |
| `forbid_unknown_fields=True` (Strict) / `False` (Compat) | Already set | Maintain |
| Reuse `Encoder`/`Decoder` instances | Not done | Add module-level instances for hot-path serialization |
| `encode_into(bytearray)` for buffer reuse | Not done | Adopt for observability row serialization |
| JSON Schema generation in CI | Not done | Add `msgspec.json.schema()` snapshot tests for boundary specs |
| Schema evolution rules (add-only, defaults) | Implicit | Document explicitly per-type |
| `StructBaseHotPath` (`gc=False`, `cache_hash=True`) | Defined but unused | Adopt for `FileIndexEntry`, `SnapshotKey` |

---

## 4) Proposed Module Layout (target state)

```
src/datafusion_engine/delta/
    __init__.py               # Public API
    control_plane.py          # Rust FFI entrypoints (keep as-is, well-structured)
    specs.py                  # Shared spec types (DeltaCdfOptionsSpec, DeltaCommitOptionsSpec, DeltaAppTransactionSpec)
    protocol.py               # Protocol snapshot/compatibility (keep)
    capabilities.py           # Extension compatibility probing (keep)
    contracts.py              # Provider/CDF contracts (migrate dataclasses to specs)
    store_policy.py           # Storage profile resolution (migrate StorageProfile to spec)
    scan_config.py            # Scan config identity/snapshot (keep)
    object_store.py           # Object store registration (keep)
    payload.py                # Rust payload adapters (simplify after type unification)
    observability.py          # Typed observability artifacts (add DeltaProviderArtifact)
    schema_guard.py           # Schema evolution enforcement (harden nullability)
    service.py                # DeltaService facade (simplify after monolith decomposition)
    maintenance.py            # Maintenance plan resolution (keep)
    cdf.py                    # CDF registration (keep)
    plugin_options.py         # Plugin option helpers (keep)

src/storage/deltalake/
    __init__.py               # Re-export surface (reduce, deprecate unused)
    config.py                 # Policy types (DeltaWritePolicy, DeltaMutationPolicy etc - already StructBaseStrict)
    models.py                 # Canonical data models (unified specs, temporary; contents migrate to specs.py over time)
    read_ops.py               # Delta read operations (from delta.py:818-1017)
    metadata_ops.py           # Metadata queries (from delta.py:1019-1194)
    feature_ops.py            # Feature toggle dispatcher (from delta.py:1300-2226, consolidated)
    maintenance_ops.py        # Vacuum/checkpoint/cleanup (from delta.py:2490-2642)
    write_ops.py              # Write/mutation orchestration (from delta.py:2644-3182)
    file_index.py             # File index construction (keep)
    file_pruning.py           # File pruning policies (keep)
    delta.py                  # Compatibility shim (temporary, then delete)
```

---

## 5) Recommended Test Additions

### 5.1 Legacy deletion/deprecation safety tests
- Add a deprecation test gate ensuring QueryBuilder path is not used in production code.
- Add coverage for deprecation warnings on compatibility facades (`storage/`, `storage/io.py`) if kept temporarily.
- Add a static import test proving no internal imports rely on compatibility facades.

### 5.2 Type unification safety tests
- Roundtrip serialization test for `DeltaCdfOptionsSpec` replacing `DeltaCdfOptions`.
- Test that `DeltaTableRefFields` inheritance produces correct `__struct_fields__`.
- Test that `DeltaScanConfigPayload` / `DeltaSnapshotPayload` decode correctly from actual Rust response dicts.
- Strict decode tests (`forbid_unknown_fields`) for runtime API specs.
- Compat decode tests for persisted artifacts (unknown-field tolerant where intended).

### 5.3 Monolith decomposition import tests
- Static import test: no internal code imports from `storage/deltalake/delta.py` directly (all via `__init__.py` or new submodules).
- Circular dependency test: verify no import cycles between `control_plane.py`, `contracts.py`, `service.py`, `schema_guard.py`.

### 5.4 Feature dispatch consolidation tests
- Parametrized test for every feature toggle operation through the unified dispatcher.
- Test that exception classification distinguishes ImportError (plugin missing) from ValueError (schema error).

### 5.5 JSON Schema snapshot tests for boundary specs
- `msgspec.json.schema(DeltaCdfOptionsSpec)` snapshot.
- `msgspec.json.schema(DeltaCommitOptionsSpec)` snapshot.
- `msgspec.json.schema(DeltaProviderRequest)` snapshot.
- Add to CI as golden-file tests.

### 5.6 Provider build artifact contract tests
- Verify `DeltaProviderBundle` populated with typed `DeltaScanConfigPayload` and `DeltaSnapshotPayload`.
- Verify pushdown classification and files scanned/pruned metrics are emitted.
- Verify canonical payload contains entrypoint/module/ctx-kind/probe-result + storage profile fingerprint.

### 5.7 Write contract tests
- Dictionary-to-physical schema normalization correctness.
- Partition metadata column handling (drop/rename before persistence).
- `_change_type` split policy (normal vs CDF batches).
- Constraint/invariant pre-commit batch validation.

### 5.8 Concurrency and conflict tests
- Deterministic handling for `DeltaOperationOutcome` conflict outcomes.
- Retry/verification behavior with persisted traces tied to snapshot key.
- Verification that `CONFLICT_REQUIRES_VERIFICATION` outcome produces an audit record.

---

## 6) Decision-Ready Conclusions

1. **Immediate cleanup path exists**: Remove the QueryBuilder production branch and its module after migrating one test harness callsite. Remove compatibility facades (`storage/__init__.py`, `storage/io.py`) if no external consumers require them.

2. **The `delta.py` monolith is the highest-priority structural debt.** Decomposition into 5 focused modules is safe because internal coupling is already organized by concern. The `__init__.py` re-export surface absorbs the break.

3. **Type duplication between storage-layer dataclasses and control-plane specs is the primary source of unnecessary adapter code.** Unifying `DeltaCdfOptions` alone eliminates ~50 lines and 2 converter functions. The same pattern applies to at least 5 other type pairs.

4. **All 15 control-plane request types share a 4-field common base.** Extracting `DeltaTableRefFields` removes ~120 lines of duplicated field declarations and `.table_ref` properties.

5. **Feature mutation code can be reduced by ~80%** by replacing 22 near-identical functions with a single parameterized dispatcher.

6. **Best-in-class next step is architectural hardening, not feature expansion:**
   - Provider-build contract artifact (2.7).
   - Explicit pushdown/pruning observability (2.10).
   - Strict write contract and CDF semantics (2.12).
   - Checkpoint/cleanup guardrails (2.13).
   - Commit conflict semantics as first-class outcomes (2.4).

7. **Observability payloads should use typed specs.** The infrastructure already exists (`observability.py` has 5 StructBaseStrict artifact types). Extending to provider artifacts and ensuring all diagnostic recording uses these types closes the last untyped boundary.

8. **The Rust FFI response boundary is the next hardening target.** `DeltaProviderBundle.snapshot` and `.scan_config` should decode into `StructBaseCompat` types immediately, providing schema enforcement without blocking Rust-side evolution.

9. **Model layer should be unified around msgspec:** One canonical spec per boundary concept, no duplicated dataclass/spec model pairs, clear spec-vs-runtime separation enforced by module boundaries.

10. **msgspec `StructBaseHotPath` is available but unused.** `FileIndexEntry` and `SnapshotKey` are high-volume, immutable value types that would benefit from `gc=False` + `cache_hash=True`.

11. **`StorageProfile` should become the sole emitter** for both DataFusion object-store config and delta-rs storage options, with ad-hoc options dict construction forbidden outside a single adapter layer.
