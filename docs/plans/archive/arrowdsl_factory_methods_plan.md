## ArrowDSL Factory Methods Plan

### Goals
- Centralize ArrowDSL construction through factory methods for compute, schema, plan, and IO.
- Ensure every entrypoint shares consistent performance, ordering, and policy defaults.
- Leverage advanced PyArrow/Acero capabilities to reduce bespoke logic and improve modularity.
- Preserve strict typing and avoid hidden runtime differences between call sites.

### Constraints
- Keep plan lane (Acero) and kernel lane responsibilities separated.
- Preserve schema metadata and ordering semantics; avoid silent behavior drift.
- No relative imports; keep modules fully typed and pyright clean.
- Avoid suppressed lint/type errors; prefer structural consolidation.

---

### Scope 1: Compute Kernel Resolver Factory
**Description**
Introduce a compute kernel resolver that selects native `pc.*` kernels when available
and falls back to UDFs only when necessary. This becomes the single path for hash,
string, and predicate kernels.

**Code patterns**
```python
# src/arrowdsl/compute/registry.py
def resolve_kernel(name: str, *, fallbacks: Sequence[str] = ()) -> str | None:
    for candidate in (name, *fallbacks):
        if pc.get_function(candidate):
            return candidate
    return None
```

**Target files**
- Update: `src/arrowdsl/compute/registry.py`
- Update: `src/arrowdsl/core/ids.py`
- Update: `src/arrowdsl/compute/udfs.py`

**Implementation checklist**
- [x] Add resolver for native vs UDF kernels.
- [x] Route hash/normalize kernels through resolver.
- [x] Remove per-call site kernel availability checks.

**Status**
Completed.

---

### Scope 2: Expr/Predicate Spec Factory
**Description**
Unify expression and predicate construction behind a single spec factory so plan-lane
and kernel-lane share identical materialization and scalar-safety behavior.

**Code patterns**
```python
# src/arrowdsl/compute/macros.py
def predicate_spec(
    kind: PredicateKind,
    *,
    col: str | None = None,
    values: Sequence[object] = (),
    predicate: ExprSpec | None = None,
) -> ExprSpec:
    ...
```

**Target files**
- Update: `src/arrowdsl/compute/macros.py`
- Update: `src/arrowdsl/compute/expr.py`
- Update: `src/arrowdsl/compute/predicates.py`

**Implementation checklist**
- [x] Add canonical predicate factory entrypoints.
- [x] Ensure materializers are shared across plan/kernel lanes.
- [x] Consolidate scalar-safety checks in one place.

**Status**
Completed.

---

### Scope 3: Hash Spec Factory
**Description**
Provide a hash spec factory that standardizes prefix/null handling and picks the
best hash kernel path. This removes ad hoc hash spec assembly in call sites.

**Code patterns**
```python
# src/arrowdsl/core/ids_registry.py
def hash_spec_factory(*, prefix: str, cols: Sequence[str]) -> HashSpec:
    return HashSpec(prefix=prefix, cols=tuple(cols), as_string=True)
```

**Target files**
- Update: `src/arrowdsl/core/ids_registry.py`
- Update: `src/arrowdsl/core/ids.py`

**Implementation checklist**
- [x] Standardize null sentinel + prefix handling.
- [x] Route hash expression/array builders through the factory.
- [x] Remove redundant HashSpec construction patterns.

**Status**
Completed.

---

### Scope 4: Nested Array Builder Factory
**Description**
Expose a single nested array factory for list/list_view/map/union/dictionary/struct
builds so all nested conversions flow through one entrypoint.

**Code patterns**
```python
# src/arrowdsl/schema/nested_builders.py
def nested_array_factory(field: FieldLike, values: Sequence[object | None]) -> ArrayLike:
    return array_from_values(values, field)
```

**Target files**
- Update: `src/arrowdsl/schema/nested_builders.py`
- Update: `src/arrowdsl/schema/arrays.py`

**Implementation checklist**
- [x] Add a single entrypoint for nested array builds.
- [x] Use list_view/map/union/dictionary handling consistently.
- [x] Remove duplicated nested conversion logic.

**Status**
Completed.

---

### Scope 5: ExecutionContext + Scan Profile Factories
**Description**
Create `ExecutionContextFactory` helpers to build named profiles (streaming/bulk/deterministic)
with consistent scan batch/readahead/threading options.

**Code patterns**
```python
# src/arrowdsl/core/context.py
def execution_context_factory(
    profile: str,
    *,
    options: ExecutionContextOptions | None = None,
) -> ExecutionContext:
    return ExecutionContext(runtime=runtime_profile_factory(profile))
```

**Target files**
- Update: `src/arrowdsl/core/context.py`
- Update: `src/arrowdsl/plan/source.py`

**Implementation checklist**
- [x] Add profile factory for common runtime profiles.
- [x] Ensure scan settings are centralized.
- [x] Route plan creation through the factory where appropriate.

**Status**
Completed.

---

### Scope 6: Scan/Scanner Factory
**Description**
Provide a single scan context factory that returns dataset, scanner, and plan with
consistent ordering and telemetry logic.

**Code patterns**
```python
# src/arrowdsl/plan/query.py
def scan_context_factory(spec: ScanSpec, *, ctx: ExecutionContext) -> ScanContext:
    return scan_context_from_spec(spec, ctx=ctx)
```

**Target files**
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/arrowdsl/plan/source.py`
- Update: `src/arrowdsl/plan/scan_specs.py`

**Implementation checklist**
- [x] Expose a single scan context factory.
- [x] Use scanner tasks for telemetry by default.
- [x] Remove parallel scan construction code paths.

**Status**
Completed.

---

### Scope 7: IPC Options Factory
**Description**
Add a shared IPC options factory for read/write helpers to standardize compression,
compatibility, and dictionary options across spec IO.

**Code patterns**
```python
# src/arrowdsl/spec/io.py
def ipc_write_options_factory(config: IpcWriteConfig | None = None) -> ipc.IpcWriteOptions:
    return ipc.IpcWriteOptions(compression=config.compression)
```

**Target files**
- Update: `src/arrowdsl/spec/io.py`
- Update: `src/arrowdsl/spec/tables/base.py`

**Implementation checklist**
- [x] Add read/write options factory helpers.
- [x] Use options defaults consistently in codec IO.
- [x] Avoid per-call site option construction.

**Status**
Completed.

---

### Scope 8: Dataset Metadata Factory
**Description**
Create a Parquet metadata factory to standardize sidecar metadata generation
for datasets and shard collections.

**Code patterns**
```python
# src/arrowdsl/plan/fragments.py
def parquet_metadata_factory(dataset: ds.Dataset) -> ParquetMetadataSpec:
    return ParquetMetadataSpec(schema=dataset.schema, file_metadata=...)
```

**Target files**
- Update: `src/arrowdsl/plan/fragments.py`
- Update: `src/storage/parquet.py`

**Implementation checklist**
- [x] Add helpers for `_metadata` / `_common_metadata`.
- [x] Use centralized schema metadata collection logic.
- [x] Avoid ad hoc metadata sidecar writes.

**Status**
Completed.

---

### Scope 9: Plan Builder Factory
**Description**
Provide a plan builder factory that assembles scan/filter/project/join/order_by
with consistent ordering + pipeline-breaker metadata.

**Code patterns**
```python
# src/arrowdsl/plan/plan.py
class PlanFactory:
    def scan(self, dataset: ds.Dataset, *, columns: ColumnsSpec) -> Plan: ...
```

**Target files**
- Update: `src/arrowdsl/plan/plan.py`
- Update: `src/arrowdsl/plan/ops.py`
- Update: `src/arrowdsl/plan/query.py`

**Implementation checklist**
- [x] Add PlanFactory with scan/filter/project/join methods.
- [x] Keep ordering metadata in a single path.
- [x] Replace ad hoc plan assembly in call sites.

**Status**
Completed.

---

### Scope 10: Schema Policy Factory
**Description**
Introduce a schema policy factory that applies metadata, encoding, and validation
defaults in one place.

**Code patterns**
```python
# src/arrowdsl/schema/policy.py
def schema_policy_factory(
    spec: TableSchemaSpec,
    *,
    ctx: ExecutionContext,
    options: SchemaPolicyOptions | None = None,
) -> SchemaPolicy:
    return SchemaPolicy(schema=options.schema or spec.to_arrow_schema())
```

**Target files**
- Update: `src/arrowdsl/schema/policy.py`
- Update: `src/schema_spec/system.py`

**Implementation checklist**
- [x] Centralize schema policy construction.
- [x] Use factory in finalize + validation paths.
- [x] Remove ad hoc policy assembly.

**Status**
Completed.
