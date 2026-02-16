# Design Review: src/storage/, src/relspec/, src/schema_spec/

**Date:** 2026-02-16
**Scope:** `src/storage/`, `src/relspec/`, `src/schema_spec/`
**Focus:** All principles (1-24), with emphasis on: Ports & Adapters, DRY, Dependency Direction, Information Hiding
**Depth:** Deep (all files)
**Files reviewed:** 38

## Executive Summary

These three modules form the infrastructure backbone of CodeAnatomy's data layer: `storage` handles Delta Lake and IPC persistence, `relspec` handles task/plan scheduling and policy compilation, and `schema_spec` provides dataset specifications and Arrow schema contracts. Overall alignment is solid in `relspec` (well-decomposed, good Protocol usage for rustworkx hiding) and `storage` (clean protocol abstractions for external index providers). The primary structural concerns are: (1) `schema_spec/contracts.py` at ~1860 lines is a monolith combining struct definitions, factory functions, accessor functions, and DataFusion introspection wrappers -- it has at least four distinct reasons to change; (2) `storage/deltalake/delta.py` at ~1700+ lines bundles snapshot management, reads, writes, merges, feature control, vacuum, retry logic, and CDF into a single file; (3) `schema_spec/contracts.py` lines 1550-1663 create fresh `DataFusionRuntimeProfile` instances inline for every introspection call, violating dependency direction (infrastructure depending on runtime construction); and (4) `_decode_metadata` is duplicated verbatim in two `schema_spec` files.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | medium | medium | `CompiledExecutionPolicy` uses `Mapping[str, object]` for 3 fields, leaking untyped internals |
| 2 | Separation of concerns | 1 | large | high | `schema_spec/contracts.py` and `storage/deltalake/delta.py` each mix 4+ concerns |
| 3 | SRP | 1 | large | high | `contracts.py` has specs, factories, accessors, and DataFusion introspection |
| 4 | High cohesion, low coupling | 2 | medium | medium | `schema_spec` has heavy coupling to `datafusion_engine` sub-packages |
| 5 | Dependency direction | 1 | medium | high | `ipc_utils.py` imports `extraction.plan_product`; `contracts.py` constructs RuntimeProfiles |
| 6 | Ports & Adapters | 2 | medium | medium | Good protocols in `storage/external_index` and `cdf_cursor_protocol`; missing port for Delta ops |
| 7 | DRY | 2 | small | low | `_decode_metadata` duplicated in `arrow_types.py:208` and `specs.py:69` |
| 8 | Design by contract | 2 | small | low | `__post_init__` validators on key structs; some docstrings lack Raises |
| 9 | Parse, don't validate | 2 | small | low | `__post_init__` normalizers on `TableSchemaContract` and `DataFusionScanOptions` |
| 10 | Make illegal states unrepresentable | 2 | small | medium | `CompileExecutionPolicyRequestV1` uses `Any` for 5 of 8 fields |
| 11 | CQS | 2 | small | low | Most functions are pure queries or commands; minor `enable_delta_features` returns value and mutates |
| 12 | DI + explicit composition | 2 | medium | medium | `_runtime_profile_for_delta` creates default instances; `inferred_deps.py` uses `importlib` |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance hierarchies; composition used throughout |
| 14 | Law of Demeter | 1 | medium | medium | `policy_compiler.py:459-462` chains through `runtime_profile.policies.write_policy` and `.features.enable_delta_cdf` |
| 15 | Tell, don't ask | 2 | medium | medium | 15+ `dataset_spec_*` accessor functions externalize logic from `DatasetSpec` |
| 16 | Functional core, imperative shell | 2 | small | low | Policy compilation is pure; Delta writes are imperative shell |
| 17 | Idempotency | 3 | - | - | `IdempotentWriteOptions` explicitly supports idempotent Delta writes |
| 18 | Determinism / reproducibility | 3 | - | - | `fingerprint()` and `policy_fingerprint` throughout; `ExecutionPackageArtifact` |
| 19 | KISS | 2 | medium | medium | Large files increase cognitive load; otherwise straightforward |
| 20 | YAGNI | 2 | small | low | Some fields appear unused (`ValidationMode` literal defined but `validation_mode` is `str`) |
| 21 | Least astonishment | 2 | small | low | `delta_commit_metadata` always returns None (line 1047); some docstrings say "Description." |
| 22 | Declare and version public contracts | 2 | small | low | `__all__` in every module; version in `_v1` suffixes; `ContractSpec.version` |
| 23 | Design for testability | 2 | medium | medium | `delta.py` functions create runtime profiles inline; `importlib` lookups in `inferred_deps.py` |
| 24 | Observability | 3 | - | - | Consistent `stage_span` in storage ops; structured attributes |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`relspec` hides rustworkx graph internals well behind `_TaskGraphLike` and `_OutDegreeGraph` Protocol types (`policy_compiler.py:28-34`). However, `CompiledExecutionPolicy` uses weakly-typed `Mapping[str, object]` for `scan_policy_overrides`, `maintenance_policy_by_dataset`, and `inference_confidence_by_view` (`compiled_policy.py:63-67`), exposing untyped internal structure to consumers.

**Findings:**
- `src/relspec/compiled_policy.py:63`: `scan_policy_overrides: Mapping[str, object]` -- callers must know the internal dict shape to use it.
- `src/relspec/compiled_policy.py:64`: `maintenance_policy_by_dataset: Mapping[str, object]` -- same issue.
- `src/relspec/compiled_policy.py:67`: `inference_confidence_by_view: Mapping[str, object]` -- same issue.
- `src/relspec/execution_package.py:44-88`: Five `_hash_*` functions use `object` types and `getattr` duck-typing instead of typed protocols, hiding the required shape from callers.
- `src/relspec/policy_compiler.py:28-34`: Good -- `_TaskGraphLike` and `_OutDegreeGraph` protocols properly hide rustworkx internals.

**Suggested improvement:**
Define typed struct contracts for `ScanPolicyOverride`, `MaintenancePolicyEntry`, and `InferenceConfidencePayload` in `relspec/compiled_policy.py`. Replace `Mapping[str, object]` with `Mapping[str, ScanPolicyOverride]` etc. For `execution_package.py`, introduce Protocol types like `HasModelHash` and `HasPolicyFingerprint` instead of using `object` + `getattr`.

**Effort:** medium
**Risk if unaddressed:** medium -- consumers silently depend on untyped dict shapes, making changes fragile.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
Two files severely violate separation of concerns. `schema_spec/contracts.py` (~1860 lines) contains: (a) struct definitions (`DatasetSpec`, `ContractSpec`, `DataFusionScanOptions`, `DeltaScanOptions`, `DeltaCdfPolicy`, `DeltaMaintenancePolicy`, etc.), (b) factory functions (`make_dataset_spec`, `make_table_spec`, `make_contract_spec`), (c) accessor functions (15+ `dataset_spec_*` functions), (d) DataFusion introspection wrappers (`dataset_table_definition`, `dataset_table_constraints`, `schema_from_datafusion_catalog`), and (e) scan policy application functions (`apply_scan_policy`, `apply_delta_scan_policy`).

Similarly, `storage/deltalake/delta.py` (~1700+ lines) bundles: (a) snapshot management, (b) table reads, (c) table writes (not shown in sampled lines but present), (d) merge operations, (e) feature control (enable CDF, column mapping, deletion vectors, row tracking), (f) vacuum/maintenance, (g) retry logic, and (h) commit metadata normalization.

**Findings:**
- `src/schema_spec/contracts.py:1-1858`: Single file with 5 distinct concern areas (structs, factories, accessors, introspection, policy application).
- `src/storage/deltalake/delta.py:1-1700+`: Single file with 8 distinct concern areas.
- `src/schema_spec/contracts.py:1550-1663`: DataFusion introspection functions (`dataset_table_definition`, `dataset_table_constraints`, `dataset_table_column_defaults`, `dataset_table_logical_plan`, `schema_from_datafusion_catalog`) all create fresh `DataFusionRuntimeProfile` instances and `SchemaIntrospector` objects -- this is runtime orchestration mixed into a contract module.

**Suggested improvement:**
Split `schema_spec/contracts.py` into: (1) `schema_spec/dataset_spec.py` for `DatasetSpec`, `ContractSpec`, and related structs; (2) `schema_spec/factories.py` for `make_*` functions; (3) `schema_spec/accessors.py` for `dataset_spec_*` accessor functions; (4) `schema_spec/introspection.py` for DataFusion catalog wrappers. Split `storage/deltalake/delta.py` into: (1) `delta_read.py` for reads; (2) `delta_write.py` for writes/merges; (3) `delta_features.py` for feature control; (4) `delta_maintenance.py` for vacuum; (5) `delta_snapshot.py` for snapshot management.

**Effort:** large
**Risk if unaddressed:** high -- any change to Delta feature control risks breaking write logic; any change to DataFusion introspection wrappers risks breaking dataset spec factories.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`schema_spec/contracts.py` changes for at least four reasons: (1) new struct fields on `DatasetSpec`/`ContractSpec`, (2) new factory convenience functions, (3) new DataFusion introspection queries, (4) scan policy application changes. `storage/deltalake/delta.py` changes for: (1) Delta read API changes, (2) write/merge behavior, (3) feature control additions, (4) retry policy changes, (5) maintenance operations.

**Findings:**
- `src/schema_spec/contracts.py`: 58 items in `__all__` -- a strong indicator of multiple responsibilities.
- `src/storage/deltalake/delta.py`: Contains 20+ public functions spanning reads, writes, merges, features, maintenance.
- `src/storage/deltalake/__init__.py:1-224`: 67 lazy exports from 4 submodules, suggesting the package surface is broad.

**Suggested improvement:**
Same as P2 decomposition. The `__init__.py` lazy export map can continue to re-export for backward compatibility.

**Effort:** large
**Risk if unaddressed:** high -- broad blast radius for any change to these files.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
`schema_spec/contracts.py` imports from 14 different modules including `datafusion_engine.arrow.*`, `datafusion_engine.delta.*`, `datafusion_engine.expr.*`, `datafusion_engine.kernels`, `datafusion_engine.schema.*`, `storage.dataset_sources`, `storage.deltalake`, and more. This creates a wide coupling surface. `relspec` modules are well-focused with narrow imports. `storage/deltalake/delta.py` imports from 11 modules.

**Findings:**
- `src/schema_spec/contracts.py:25-66`: 25 import statements from 14+ modules.
- `src/storage/deltalake/delta.py:1-55`: Imports from `datafusion_engine.arrow.*`, `datafusion_engine.encoding`, `datafusion_engine.errors`, `datafusion_engine.schema.*`, `datafusion_engine.session.*`, `obs.otel.*`.
- `src/relspec/policy_compiler.py:1-26`: Clean -- only 6 imports, 4 in TYPE_CHECKING.
- `src/relspec/decision_provenance.py:1-18`: Clean -- only 2 imports, 2 in TYPE_CHECKING.

**Suggested improvement:**
Reduce `schema_spec/contracts.py` coupling by extracting introspection functions (which pull in `datafusion_engine.runtime`) and scan policy functions (which pull in `datafusion_engine.extensions.schema_runtime`) into separate modules.

**Effort:** medium
**Risk if unaddressed:** medium -- import changes in any coupled module propagate broadly.

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
Three dependency direction violations:

1. `storage/ipc_utils.py:14` imports `extraction.plan_product.PlanProduct`. Storage is infrastructure; extraction is a higher-level pipeline stage. Storage should not depend on extraction.

2. `schema_spec/contracts.py:1566-1567` creates fresh `DataFusionRuntimeProfile()` instances via `importlib.import_module("datafusion_engine.runtime")`. Schema spec is a declarative contract module; constructing runtime sessions is an operational concern that belongs to callers.

3. `relspec/inferred_deps.py:280-314` uses `importlib.import_module()` to optionally load `datafusion_engine.extract_registry` and `semantics.catalog.dataset_specs`. While this is done to avoid hard dependencies, it creates hidden runtime coupling.

**Findings:**
- `src/storage/ipc_utils.py:14`: `from extraction.plan_product import PlanProduct` -- storage depends on extraction (wrong direction).
- `src/schema_spec/contracts.py:1566-1567`: `module = importlib.import_module("datafusion_engine.runtime"); runtime = module.DataFusionRuntimeProfile()` -- repeated identically 5 times at lines 1566-1570, 1589-1593, 1612-1616, 1635-1639, 1658-1662.
- `src/relspec/inferred_deps.py:280-314`: Dynamic `importlib` for `datafusion_engine.extract_registry` and `semantics.catalog.dataset_specs`.
- `src/storage/deltalake/delta.py:176-183`: `_runtime_profile_for_delta` creates default `DataFusionRuntimeProfile()` instances.

**Suggested improvement:**
(1) Move `PlanProduct` handling out of `ipc_utils.py` -- accept only `TableLike | RecordBatchReaderLike` and let callers resolve `PlanProduct`. (2) Extract the 5 DataFusion introspection functions from `schema_spec/contracts.py` into a separate module (e.g., `schema_spec/introspection.py`) that accepts a `SessionContext` parameter instead of constructing its own. (3) In `inferred_deps.py`, replace `importlib` lookups with Protocol-typed parameters or optional dependencies injected at call sites.

**Effort:** medium
**Risk if unaddressed:** high -- violating dependency direction creates circular import risks and makes infrastructure modules untestable without heavyweight runtime setup.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
Good examples of Ports & Adapters exist:
- `storage/cdf_cursor_protocol.py` defines clean Protocol types `CdfCursorLike` and `CdfCursorStoreLike`.
- `storage/external_index/provider.py` defines `ExternalIndexProvider` Protocol with `ExternalIndexRequest`/`ExternalIndexSelection` value types.
- `relspec/policy_compiler.py:28-34` defines `_TaskGraphLike`/`_OutDegreeGraph` Protocols to abstract rustworkx.

Missing: Delta Lake operations in `storage/deltalake/delta.py` directly depend on `deltalake` library types (`DeltaTable`, `Transaction`, `CommitProperties`) without a port abstraction. This makes the module untestable without a real Delta Lake installation.

**Findings:**
- `src/storage/cdf_cursor_protocol.py:8-27`: Clean Protocol pair for cursor persistence.
- `src/storage/external_index/provider.py:1-103`: Clean Protocol with request/response value types.
- `src/storage/deltalake/delta.py:15-16`: Direct import of `deltalake.DeltaTable`, `deltalake.Transaction`, `deltalake.CommitProperties` with no port abstraction.
- `src/relspec/policy_compiler.py:28-40`: Three Protocol types for graph and scan override abstraction.

**Suggested improvement:**
Introduce a `DeltaTableProviderLike` Protocol in `storage/deltalake/` that abstracts the core `DeltaTable` operations (version, schema, read, write). This would enable testing Delta operations without a real Delta Lake backend. Given the heavy integration with `deltalake`, this is a medium-effort improvement.

**Effort:** medium
**Risk if unaddressed:** medium -- testing Delta operations requires real filesystem and `deltalake` library.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 2/3

**Current state:**
One clear duplication of knowledge: `_decode_metadata` is defined identically in `schema_spec/arrow_types.py:208-214` and `schema_spec/specs.py:69-82`. Both convert `Mapping[bytes, bytes]` to `dict[str, str]` with `utf-8` decode and `errors="replace"`.

The `RELATION_OUTPUT_NAME` re-export chain (`semantics.output_names` -> `relspec/view_defs.py` -> `relspec/contracts.py`) is intentional re-exporting, not knowledge duplication. Similarly, `DeltaWritePolicy`/`DeltaSchemaPolicy` are defined once in `storage/deltalake/config.py` and imported (not duplicated) in `schema_spec/contracts.py`.

**Findings:**
- `src/schema_spec/arrow_types.py:208-214` and `src/schema_spec/specs.py:69-82`: Identical `_decode_metadata` function.
- `src/schema_spec/contracts.py:1566-1662`: The 4-line pattern `module = importlib.import_module("datafusion_engine.runtime"); runtime = module.DataFusionRuntimeProfile(); ctx = runtime.session_context(); introspector = SchemaIntrospector(ctx, sql_options=...)` is repeated 5 times.

**Suggested improvement:**
(1) Move `_decode_metadata` to a single location (e.g., `schema_spec/arrow_types.py`) and import it in `specs.py`. (2) Extract the repeated introspection setup into a single private helper function.

**Effort:** small
**Risk if unaddressed:** low -- but the 5x repeated block is a maintenance hazard.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Several structs use `__post_init__` to validate invariants:
- `ContractSpec.__post_init__` validates `virtual_field_docs` against `virtual_fields` (`contracts.py:791-801`).
- `ContractCatalogSpec.__post_init__` validates key-name matches (`contracts.py:1769-1778`).
- `TableSchemaContract.__post_init__` normalizes partition types (`contracts.py:181-193`).
- `DataFusionScanOptions.__post_init__` normalizes partition types (`contracts.py:332-344`).

Some docstrings use placeholder text like "Description." for parameters and "If the operation cannot be completed." for raises, reducing contract clarity.

**Findings:**
- `src/schema_spec/contracts.py:757-758`: Docstring says `view_specs: Description.` and `label: Description.` -- not helpful.
- `src/schema_spec/contracts.py:948-955`: `DatasetOpenSpec.open` docstring says `path: Description.` and raises `ValueError: If the operation cannot be completed.`
- `src/storage/deltalake/delta.py:729-736`: `delta_table_version` docstring uses `path: Description.` etc.

**Suggested improvement:**
Replace placeholder "Description." docstrings with actual parameter descriptions. This is a documentation quality issue affecting ~10 docstrings across the three modules.

**Effort:** small
**Risk if unaddressed:** low -- reduces developer clarity but no runtime impact.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Several structs normalize inputs in `__post_init__`, converting `pa.DataType` to `ArrowTypeSpec` at construction time:
- `TableSchemaContract.__post_init__` (`contracts.py:181-193`): normalizes `partition_cols` types.
- `DataFusionScanOptions.__post_init__` (`contracts.py:332-344`): same pattern.

`DeltaReadRequest` validates mutual exclusivity of `version` and `timestamp` at function entry rather than at construction, but this is acceptable given `dataclass(frozen=True)` doesn't support `__post_init__` validation well.

**Findings:**
- `src/schema_spec/contracts.py:181-193`: Good -- normalizes `pa.DataType` to `ArrowTypeSpec` at boundary.
- `src/schema_spec/contracts.py:332-344`: Good -- same normalization pattern.
- `src/storage/deltalake/delta.py:694-696`: Validates `version`/`timestamp` mutual exclusivity at function entry (acceptable).

**Suggested improvement:**
No urgent changes needed. The current approach of normalizing in `__post_init__` is sound.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`CompileExecutionPolicyRequestV1` in `relspec/contracts.py:31-41` uses `Any` for 5 of 8 fields (`task_graph`, `output_locations`, `runtime_profile`, `view_nodes`, `semantic_ir`). This allows any value where specific types are required.

`execution_package.py:116-123` accepts `object | None` for `compiled_policy`, `capability_snapshot`, and `session_config` -- callers can pass anything.

On the positive side, `CachePolicyValue` and `ValidationMode` are properly constrained to `Literal` types in `compiled_policy.py:20-21`.

**Findings:**
- `src/relspec/contracts.py:33-41`: `task_graph: Any`, `output_locations: dict[str, Any]`, `runtime_profile: Any`, `view_nodes: tuple[Any, ...] | None`, `semantic_ir: Any | None` -- 5 weakly typed fields.
- `src/relspec/execution_package.py:117-122`: Parameters typed as `object | None` instead of specific types.
- `src/relspec/compiled_policy.py:20-21`: Good -- `CachePolicyValue` and `ValidationMode` use `Literal`.

**Suggested improvement:**
Replace `Any` types in `CompileExecutionPolicyRequestV1` with TYPE_CHECKING imports of the actual types. Use Protocol types in `build_execution_package` instead of `object`.

**Effort:** small
**Risk if unaddressed:** medium -- `Any` types bypass type checking, allowing invalid inputs at runtime.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS well. Pure query functions (all `dataset_spec_*` accessors, `decisions_by_domain`, `decisions_above_confidence`) return values without side effects. Command functions (`enable_delta_features`, `delta_add_constraints`) modify state.

Minor violation: `enable_delta_features` (`delta.py:1153-1221`) both mutates the Delta table and returns the applied properties dict. Similarly, `delta_add_constraints` returns the report while modifying state.

**Findings:**
- `src/storage/deltalake/delta.py:1153`: `enable_delta_features` returns `dict[str, str]` while mutating the Delta table.
- `src/storage/deltalake/delta.py:1258`: `delta_add_constraints` returns report while mutating.
- `src/relspec/decision_provenance.py:161-288`: Pure query functions -- good CQS.

**Suggested improvement:**
The return-value-from-mutations pattern is a common and acceptable convention for recording operation results. No change needed unless strict CQS is desired.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`storage/deltalake/delta.py:176-183` creates default `DataFusionRuntimeProfile()` instances when callers don't provide one. This hides dependency creation inside the module rather than requiring explicit injection.

`relspec/inferred_deps.py:280-314` uses `importlib.import_module()` to dynamically resolve `datafusion_engine.extract_registry` and `semantics.catalog.dataset_specs`. This is effectively service location rather than dependency injection.

On the positive side, `policy_compiler.py` accepts all dependencies through the `CompileExecutionPolicyRequestV1` request object, following explicit composition.

**Findings:**
- `src/storage/deltalake/delta.py:176-183`: `_runtime_profile_for_delta` creates default `DataFusionRuntimeProfile()`.
- `src/relspec/inferred_deps.py:280-314`: Dynamic `importlib` for optional module resolution.
- `src/relspec/policy_compiler.py:64`: Good -- accepts request object with all dependencies.

**Suggested improvement:**
Make `runtime_profile` a required parameter (not optional with default construction) in Delta operations that need it. Replace `importlib` lookups in `inferred_deps.py` with optional callable parameters.

**Effort:** medium
**Risk if unaddressed:** medium -- hidden construction makes testing harder and behavior less predictable.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies found across all 38 files. All struct types use composition (nested structs, field bundles) rather than inheritance. `DatasetSpec` composes `TableSchemaSpec`, `ContractSpec`, `DatasetPolicies`, etc. `DeltaPolicyBundle` composes `DeltaScanOptions`, `DeltaCdfPolicy`, `DeltaMaintenancePolicy`, etc.

**Findings:**
- All struct types use `StructBaseStrict` or `StructBaseCompat` as frozen base, which is a serialization concern, not behavioral inheritance.
- `DatasetPolicies` composes multiple policy types (`contracts.py:897-904`).
- `DeltaPolicyBundle` composes multiple Delta-specific policies (`contracts.py:829-838`).

**Suggested improvement:**
No action needed.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Several functions reach through multiple levels of nested objects:

`policy_compiler.py:459` chains `runtime_profile.policies.write_policy` and `runtime_profile.features.enable_delta_cdf` (three levels deep). Multiple `dataset_spec_*` accessor functions in `contracts.py` chain through `spec.policies.delta.scan`, `spec.policies.delta.cdf_policy`, etc.

**Findings:**
- `src/relspec/policy_compiler.py:459`: `getattr(getattr(runtime_profile, "policies", None), "write_policy", None)` -- double `getattr` chain.
- `src/relspec/policy_compiler.py:461`: `bool(getattr(getattr(runtime_profile, "features", None), "enable_delta_cdf", False))` -- double `getattr` chain.
- `src/schema_spec/contracts.py:1329-1334`: `dataset_spec_delta_scan` reaches through `spec.policies.delta.scan`.
- `src/schema_spec/contracts.py:1337-1342`: `dataset_spec_delta_cdf_policy` reaches through `spec.policies.delta.cdf_policy`.
- `src/schema_spec/contracts.py:1345-1350`: `dataset_spec_delta_maintenance_policy` reaches through `spec.policies.delta.maintenance_policy`.
- `src/schema_spec/contracts.py:1369-1374`: Same pattern for `delta_write_policy`.
- `src/schema_spec/contracts.py:1377-1382`: Same pattern for `delta_schema_policy`.
- `src/schema_spec/contracts.py:1385-1390`: Same pattern for `delta_feature_gate`.

**Suggested improvement:**
Add convenience accessor methods on `DatasetPolicies` itself (e.g., `DatasetPolicies.delta_scan() -> DeltaScanOptions | None`) so callers can use `spec.policies.delta_scan()` instead of `spec.policies.delta.scan` with null checks. For `policy_compiler.py`, accept typed sub-objects rather than reaching through the runtime profile.

**Effort:** medium
**Risk if unaddressed:** medium -- long chains are fragile when intermediate objects change shape.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The 15+ `dataset_spec_*` accessor functions in `contracts.py` (lines 1319-1489) expose raw data from `DatasetSpec` for external logic. This is "ask" rather than "tell" -- callers extract data and make decisions externally.

However, `DatasetSpec` is a frozen data struct, not a behavior-rich object, so accessor functions are a reasonable pattern for value types.

**Findings:**
- `src/schema_spec/contracts.py:1319-1489`: 15+ `dataset_spec_*` functions that extract and return sub-fields.
- `src/relspec/policy_compiler.py:425-431`: `_derive_maintenance_policies` uses `getattr(location, "resolved", None)` then `getattr(resolved, "delta_maintenance_policy", None)` -- asking rather than telling.

**Suggested improvement:**
For frozen data structs, accessor functions are acceptable. The `policy_compiler.py:425-431` pattern using `getattr` chains should instead accept properly typed objects or use Protocol types.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Good separation overall. `relspec/` is almost entirely a functional core: `policy_compiler.py` performs pure policy derivation, `decision_provenance.py` is pure graph construction and query. `schema_spec/` is mostly pure struct definitions and factories.

The imperative shell is in `storage/deltalake/delta.py` for actual Delta operations. The concern is that `schema_spec/contracts.py:1550-1663` contains imperative shell code (DataFusion session creation) mixed into what should be a pure contract module.

**Findings:**
- `src/relspec/policy_compiler.py`: Entirely pure functional -- derives policy from inputs.
- `src/relspec/decision_provenance.py`: Entirely pure functional.
- `src/schema_spec/contracts.py:1550-1663`: Imperative shell (DataFusion session creation) in a contract module.
- `src/storage/deltalake/delta.py`: Correctly placed in imperative shell.

**Suggested improvement:**
Move DataFusion introspection functions out of `schema_spec/contracts.py` into a separate imperative module.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
`IdempotentWriteOptions` (`delta.py:498-510`) explicitly supports idempotent Delta writes using `CommitProperties` with `app_id` and `version`. The retry logic in `_execute_delta_merge` (`delta.py:315-339`) correctly retries on transient errors. All fingerprint computation functions are idempotent.

**Findings:**
- `src/storage/deltalake/delta.py:498-510`: `IdempotentWriteOptions` with `app_id` and `version`.
- `src/storage/deltalake/delta.py:315-339`: Retry logic with classification (fatal/retryable/unknown).
- `src/relspec/execution_package.py:98-113`: `_composite_fingerprint` is deterministic and idempotent.

**Suggested improvement:**
No action needed.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Excellent determinism support throughout:
- `ExecutionPackageArtifact` captures composite fingerprint from all pipeline inputs.
- `CompiledExecutionPolicy` includes `policy_fingerprint` for reproducibility.
- All policy structs in `storage/deltalake/config.py` implement `fingerprint_payload()` and `fingerprint()`.
- `_normalize_plan_bundle_fingerprints` sorts keys for deterministic hashing.

**Findings:**
- `src/relspec/execution_package.py:98-113`: Deterministic composite fingerprint.
- `src/relspec/compiled_policy.py:72`: `policy_fingerprint` field.
- `src/storage/deltalake/config.py`: All policy structs have `fingerprint_payload()` and `fingerprint()`.
- `src/relspec/policy_compiler.py:497-511`: Deterministic policy fingerprint via canonical JSON encoding.

**Suggested improvement:**
No action needed.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The large file sizes of `contracts.py` (~1860 lines) and `delta.py` (~1700+ lines) increase cognitive load. Individual functions within these files are generally straightforward, but finding the right function in a 1800+ line file is not simple.

`schema_spec/contracts.py` also has a large `__all__` list (58 items), indicating the module's surface area is too broad for easy comprehension.

**Findings:**
- `src/schema_spec/contracts.py`: 1858 lines, 58 exports.
- `src/storage/deltalake/delta.py`: ~1700+ lines, 20+ public functions.
- `src/relspec/policy_compiler.py`: 517 lines -- reasonable for its scope.
- `src/relspec/decision_provenance.py`: 445 lines -- clean, focused.

**Suggested improvement:**
Decompose the two large files as described in P2/P3.

**Effort:** medium
**Risk if unaddressed:** medium -- cognitive load increases maintenance burden.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
`ValidationMode` literal type is defined in `compiled_policy.py:21` as `Literal["off", "warn", "error"]`, but `validation_mode` field uses `str` type (`compiled_policy.py:71`), not the `ValidationMode` literal. This suggests the literal was defined speculatively.

`delta_commit_metadata` function (`delta.py:1012-1047`) always returns `None` (line 1047), suggesting it's a stub for future functionality.

**Findings:**
- `src/relspec/compiled_policy.py:21`: `ValidationMode = Literal["off", "warn", "error"]` defined but not used as type.
- `src/relspec/compiled_policy.py:71`: `validation_mode: str = "warn"` -- uses `str`, not `ValidationMode`.
- `src/storage/deltalake/delta.py:1047`: `return None` -- `delta_commit_metadata` always returns None.

**Suggested improvement:**
(1) Either use `ValidationMode` as the type for `validation_mode` or remove the literal definition. (2) Either implement `delta_commit_metadata` or mark it as a stub with a `TODO`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
`delta_commit_metadata` (`delta.py:1012-1047`) has a docstring saying it returns commit metadata, but it always returns `None`. A caller would be surprised that a function named "delta_commit_metadata" never returns metadata.

Several docstrings use placeholder text "Description." for parameters, which is surprising for a documented codebase.

**Findings:**
- `src/storage/deltalake/delta.py:1047`: `delta_commit_metadata` always returns None despite docstring promising metadata.
- `src/schema_spec/contracts.py:757-758`: `view_specs: Description.` and `label: Description.` in `_validate_view_specs`.
- `src/storage/deltalake/delta.py:729-736`: `path: Description.` etc. in `delta_table_version`.

**Suggested improvement:**
Fix `delta_commit_metadata` to either extract commit metadata from the snapshot or document that it is unimplemented. Replace all "Description." placeholder docstrings.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules have explicit `__all__` lists defining their public surface. Version information is embedded in naming conventions (`_v1` suffixes) and `ContractSpec.version` fields. `SCHEMA_META_VERSION` constants enable runtime version discovery.

The `schema_spec/__init__.py` lazy-loading facade (~295 lines) provides a stable re-export surface.

**Findings:**
- All 38 files define `__all__` lists.
- `src/relspec/contracts.py:31`: `CompileExecutionPolicyRequestV1` uses `V1` suffix.
- `src/schema_spec/contracts.py:784`: `ContractSpec.version` field.
- `src/schema_spec/__init__.py`: Large facade re-exporting ~100+ names.

**Suggested improvement:**
Consider adding `__version__` or changelog metadata to the package-level `__init__.py` files.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
`relspec/` is highly testable -- pure functions, no hidden state. `storage/deltalake/delta.py` is harder to test because functions like `_runtime_profile_for_delta` create `DataFusionRuntimeProfile()` instances internally. `schema_spec/contracts.py` introspection functions (`dataset_table_definition` etc.) are untestable without a real DataFusion session.

`inferred_deps.py:280-314` uses `importlib` to load optional modules, making it hard to mock without monkeypatching.

**Findings:**
- `src/storage/deltalake/delta.py:176-183`: Creates `DataFusionRuntimeProfile()` internally.
- `src/schema_spec/contracts.py:1566-1662`: Creates `DataFusionRuntimeProfile()` and `SchemaIntrospector` internally.
- `src/relspec/inferred_deps.py:280-314`: `importlib.import_module` for optional dependencies.
- `src/relspec/policy_compiler.py`: Fully testable -- accepts all inputs via request object.
- `src/relspec/decision_provenance.py`: Fully testable -- pure functions on data structs.

**Suggested improvement:**
Accept `SessionContext` or `SchemaIntrospector` as parameters in `schema_spec` introspection functions instead of constructing them. Accept optional resolver callables in `inferred_deps.py` instead of using `importlib`.

**Effort:** medium
**Risk if unaddressed:** medium -- untestable code paths reduce confidence in correctness.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Excellent observability in `storage/deltalake/delta.py`:
- All Delta operations use `stage_span` from `obs.otel.tracing`.
- Structured attributes include `codeanatomy.operation`, `codeanatomy.table`, `codeanatomy.run_id`, `codeanatomy.query_id`, `codeanatomy.dataset_name`.
- `_storage_span_attributes` helper (`delta.py:404-424`) standardizes attribute construction.
- Retry attempts are recorded via `span.set_attribute("codeanatomy.retry_attempt", attempts)`.

`relspec/` uses `logging.getLogger(__name__)` consistently.

**Findings:**
- `src/storage/deltalake/delta.py:404-424`: Centralized span attribute builder.
- `src/storage/deltalake/delta.py:336`: Retry attempt tracking on spans.
- `src/storage/deltalake/delta.py:33-35`: Imports from `obs.otel` for structured tracing.
- `src/relspec/policy_compiler.py:44`: `_LOGGER = logging.getLogger(__name__)`.
- `src/relspec/inferred_deps.py:27`: `_LOGGER = logging.getLogger(__name__)`.

**Suggested improvement:**
No action needed.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

## Cross-Cutting Themes

### Theme 1: Two God Files Need Decomposition

**Root cause:** `schema_spec/contracts.py` (~1860 lines) and `storage/deltalake/delta.py` (~1700+ lines) have accumulated responsibilities over time without decomposition.

**Affected principles:** P2 (Separation of Concerns), P3 (SRP), P4 (Coupling), P5 (Dependency Direction), P19 (KISS), P23 (Testability).

**Suggested approach:** Decompose each file into 4-5 focused modules as detailed in P2. Use the existing `__init__.py` lazy-loading pattern to maintain backward-compatible imports.

### Theme 2: Infrastructure Modules Constructing Runtime Objects

**Root cause:** `schema_spec/contracts.py` and `storage/deltalake/delta.py` both construct `DataFusionRuntimeProfile()` instances internally instead of receiving them as parameters.

**Affected principles:** P5 (Dependency Direction), P12 (DI), P23 (Testability).

**Suggested approach:** Make runtime profile a required parameter in all functions that need it. The 5 DataFusion introspection functions in `contracts.py` should accept a `SessionContext` parameter. `_runtime_profile_for_delta` should not have a default construction fallback.

### Theme 3: Weak Typing at Module Boundaries

**Root cause:** `CompileExecutionPolicyRequestV1` uses `Any` for most fields. `CompiledExecutionPolicy` uses `Mapping[str, object]` for several fields. `build_execution_package` accepts `object | None` parameters.

**Affected principles:** P1 (Information Hiding), P8 (Design by Contract), P10 (Make Illegal States Unrepresentable).

**Suggested approach:** Introduce typed Protocol or struct types for the weakly-typed boundaries. Use TYPE_CHECKING imports to avoid circular dependencies while maintaining type safety.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Deduplicate `_decode_metadata` from `arrow_types.py:208` and `specs.py:69` | small | Eliminates identical code duplication |
| 2 | P7 (DRY) | Extract repeated 4-line introspection setup in `contracts.py:1566-1662` into helper | small | Eliminates 5x repeated block |
| 3 | P10 | Replace `Any` types in `CompileExecutionPolicyRequestV1` with TYPE_CHECKING imports | small | Restores type safety at key boundary |
| 4 | P20/P21 | Fix `delta_commit_metadata` stub (always returns None) or mark as TODO | small | Eliminates surprising behavior |
| 5 | P8 | Replace "Description." placeholder docstrings (~10 occurrences) | small | Improves contract clarity |

## Recommended Action Sequence

1. **Deduplicate `_decode_metadata`** (P7) -- Move to `schema_spec/arrow_types.py`, import in `specs.py`. Zero risk, immediate clarity gain.

2. **Extract introspection setup helper** (P7) -- Create `_introspection_context()` helper in `schema_spec/contracts.py` that returns `(ctx, introspector)` tuple. Reduces 5x repetition to 1.

3. **Type `CompileExecutionPolicyRequestV1` fields** (P10) -- Add TYPE_CHECKING imports for `TaskGraph`, `DatasetLocation`, `DataFusionRuntimeProfile`, `ViewNode`, `SemanticIR`. Replace `Any` with proper types.

4. **Define typed structs for `CompiledExecutionPolicy` weak fields** (P1) -- Create `ScanPolicyOverrideEntry`, `MaintenancePolicyEntry`, `InferenceConfidenceEntry` structs. Replace `Mapping[str, object]` fields.

5. **Fix `delta_commit_metadata`** (P21) -- Either implement the extraction or add a deprecation notice/TODO.

6. **Extract DataFusion introspection from `schema_spec/contracts.py`** (P2, P5, P23) -- Create `schema_spec/introspection.py` with functions accepting `SessionContext` parameter. This unblocks testability and fixes dependency direction.

7. **Remove `extraction.plan_product` import from `ipc_utils.py`** (P5) -- Accept only `TableLike | RecordBatchReaderLike`. Move `PlanProduct` resolution to callers.

8. **Decompose `schema_spec/contracts.py`** (P2, P3) -- Split into `dataset_spec.py`, `factories.py`, `accessors.py`, and retain policy/scan structs in `contracts.py`. Update `__init__.py` re-exports.

9. **Decompose `storage/deltalake/delta.py`** (P2, P3) -- Split into `delta_read.py`, `delta_write.py`, `delta_features.py`, `delta_maintenance.py`, `delta_snapshot.py`. Update `__init__.py` lazy exports.

10. **Add convenience accessors to `DatasetPolicies`** (P14) -- Reduce Law of Demeter violations by adding `delta_scan()`, `delta_cdf_policy()`, etc. methods on the policies struct.
