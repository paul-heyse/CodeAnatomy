# Architectural Unification Msgspec Expansion Plan (v1)

> **Purpose**: Convert remaining config/spec/registry surfaces to msgspec, introduce spec/runtime splits where required, and implement structural refactors that unlock best-in-class msgspec usage (schema export, deterministic encoding, strict validation).
>
> **Status**: Draft for implementation
>
> **Assumptions**: Design-phase, breaking changes are acceptable; remove legacy surfaces rather than shim.
>
> **References**:
> - `docs/architecture/architectural_unification_deep_dive.md`
> - `docs/plans/architectural_unification_scope_alignment_plan_v1.md`
> - `docs/plans/architectural_unification_gap_closure_plan_v1.md`

---

## Implementation Status (as of 2026-02-05)

- **Scope 0**: Complete
- **Scope 1**: Complete
- **Scope 2**: Complete
- **Scope 3**: Complete
- **Scope 4**: Complete
- **Scope 5**: Complete
- **Scope 6**: Partial (Pandera validation not yet wired at CPG materialization boundaries)
- **Scope 7**: Complete
- **Scope 8**: Complete
- **Scope 9**: Complete
- **Scope 10**: Complete
- **Scope 11**: Complete
- **Scope 12**: Complete (goldens updated; verification pending via cq pytest/quality gate)

---

## Scope 0 — Schema + Query Contracts as msgspec (P0)

**Goal**: Make schema and query specifications first-class msgspec contracts with JSON Schema export. This unlocks schema registry coverage and deterministic serialization for dataset contracts.

### Representative code patterns

```python
from serde_msgspec import StructBaseStrict

class FieldSpec(StructBaseStrict, frozen=True):
    name: str
    dtype: ArrowTypeSpec
    nullable: bool = True
    metadata: dict[str, str] = msgspec.field(default_factory=dict)
    default_value: str | None = None
    encoding: Literal["dictionary"] | None = None
```

```python
class QuerySpec(StructBaseStrict, frozen=True):
    projection: ProjectionSpec
    predicate: ExprSpec | None = None
    pushdown_predicate: ExprSpec | None = None
```

### Target files
- `src/schema_spec/field_spec.py`
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/schema_spec/view_specs.py`
- `src/schema_spec/relationship_specs.py`
- `src/datafusion_engine/expr/spec.py`
- `src/datafusion_engine/expr/query_spec.py`
- `src/datafusion_engine/kernels.py`
- `src/datafusion_engine/schema/validation.py`

### Deprecate/delete after completion
- Dataclass variants of the above spec types.
- Any ad-hoc schema payload dicts emitted from these types (replace with `msgspec.to_builtins`).

### Implementation checklist
- [ ] Convert all schema/query spec dataclasses to `StructBaseStrict`.
- [ ] Update any `.replace`/construction call sites to use `msgspec.structs.replace`.
- [ ] Register new msgspec schema types in `src/serde_schema_registry.py` if intended as exported contracts.
- [ ] Add msgpack round-trip tests for the converted specs.

---

## Scope 1 — Arrow Type IR (P0)

**Goal**: Replace `DataTypeLike` in schema contracts with a serializable Arrow type IR so schema specs are fully msgspec-serializable and JSON-schema exportable.

### Representative code patterns

```python
class ArrowTypeSpec(StructBaseStrict, frozen=True, tag=True):
    type: str  # tag field

class ArrowPrimitive(ArrowTypeSpec):
    type: Literal["i32", "i64", "f64", "string", "bool", "binary"]

class ArrowList(ArrowTypeSpec):
    type: Literal["list"]
    item: ArrowTypeSpec

class ArrowStruct(ArrowTypeSpec):
    type: Literal["struct"]
    fields: tuple[FieldSpec, ...]
```

```python
class FieldSpec(StructBaseStrict, frozen=True):
    name: str
    dtype: ArrowTypeSpec
    nullable: bool = True
```

### Target files
- `src/schema_spec/field_spec.py`
- `src/schema_spec/specs.py`
- `src/schema_spec/nested_types.py`
- `src/datafusion_engine/arrow/interop.py` (adapters to/from pyarrow)

### Deprecate/delete after completion
- Direct `DataTypeLike` usage in schema contracts.
- Any schema serialization paths that embed `pyarrow.DataType` directly.

### Implementation checklist
- [ ] Implement Arrow type IR (primitive/list/map/struct/dictionary/large_list) with tagged unions.
- [ ] Add conversion helpers `arrow_type_from_pyarrow` / `arrow_type_to_pyarrow`.
- [ ] Update `FieldSpec.to_arrow_field()` to use IR conversion.
- [ ] Update any schema inference functions to emit IR instead of pyarrow types.

---

## Scope 2 — DatasetSpec Nesting + Policy Bundles (P0)

**Goal**: Reshape `DatasetSpec` into nested, msgspec-friendly bundles (datafusion, delta, validation) and align with `DatasetLocationOverrides`.

### Representative code patterns

```python
class DatasetPolicies(StructBaseStrict, frozen=True):
    datafusion_scan: DataFusionScanOptions | None = None
    delta: DeltaPolicyBundle | None = None
    validation: ArrowValidationOptions | None = None

class DatasetSpec(StructBaseStrict, frozen=True):
    table_spec: TableSchemaSpec
    contract_spec: ContractSpec | None = None
    query_spec: QuerySpec | None = None
    view_specs: tuple[ViewSpec, ...] = ()
    policies: DatasetPolicies = msgspec.field(default_factory=DatasetPolicies)
```

### Target files
- `src/schema_spec/system.py`
- `src/datafusion_engine/dataset/registry.py`
- `src/datafusion_engine/dataset/registration.py`
- `src/datafusion_engine/dataset/resolution.py`

### Deprecate/delete after completion
- Flat `DatasetSpec.delta_*` and `DatasetSpec.datafusion_scan` fields.
- Redundant resolve helpers that only exist to navigate flat fields.

### Implementation checklist
- [ ] Introduce nested policy bundle types.
- [ ] Update `DatasetLocationOverrides` to mirror nested bundles.
- [ ] Migrate all `resolve_*` accessors to use nested policies.
- [ ] Update tests for precedence and resolved schema logic.

---

## Scope 3 — Runtime Config Surfaces to msgspec (P0)

**Goal**: Ensure `DataFusionRuntimeProfile` is fully msgspec-backed, including nested config types and cache/profile objects.

### Representative code patterns

```python
class DiskCacheSettings(StructBaseStrict, frozen=True):
    size_limit_bytes: int
    cull_limit: int = 10

class DiskCacheProfile(StructBaseStrict, frozen=True):
    root: str
    base_settings: DiskCacheSettings
    overrides: Mapping[DiskCacheKind, DiskCacheSettings] = msgspec.field(default_factory=dict)
```

```python
class PreparedStatementSpec(StructBaseStrict, frozen=True):
    name: str
    sql: str
    param_types: tuple[str, ...] = ()
```

### Target files
- `src/cache/diskcache_factory.py`
- `src/datafusion_engine/session/cache_policy.py`
- `src/datafusion_engine/session/runtime.py`

### Deprecate/delete after completion
- Dataclass versions of runtime config types (DiskCache*, CachePolicyConfig, PreparedStatementSpec).

### Implementation checklist
- [ ] Convert runtime config dataclasses to `StructBaseStrict`.
- [ ] Update construction/replace call sites to use msgspec utilities.
- [ ] Ensure fingerprint payloads remain stable post-migration.

---

## Scope 4 — Spec/Runtime Split for Non-Serializable Types (P0)

**Goal**: Introduce msgspec specs for config types that currently embed runtime objects or callables (SQLOptions, samplers, hooks), and pair them with Pydantic runtime models for derived settings and invariants.

### Representative code patterns

```python
class DataFusionCompileOptionsSpec(StructBaseStrict, frozen=True):
    cache: bool | None = None
    cache_max_columns: int | None = 64
    prepared_statements: bool = True
    sql_policy_name: str | None = None
    enforce_sql_policy: bool = True
```

```python
from pydantic import BaseModel, ConfigDict, model_validator

class RuntimeBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

class DataFusionCompileOptionsRuntime(RuntimeBase):
    cache: bool | None
    cache_max_columns: int | None
    sql_policy_name: str | None

    @model_validator(mode="after")
    def _validate_cache(self) -> "Self":
        if self.cache is False and self.cache_max_columns is not None:
            raise ValueError("cache_max_columns requires cache=True")
        return self
```

```python
def resolve_compile_options(spec: DataFusionCompileOptionsSpec) -> DataFusionCompileOptionsRuntime:
    payload = msgspec.to_builtins(spec)
    return DataFusionCompileOptionsRuntime.model_validate(payload)
```

### Target files
- `src/datafusion_engine/compile/options.py`
- `src/obs/otel/config.py`
- `src/semantics/config.py`
- `src/engine/runtime_profile.py`
- `src/runtime_models/` (new runtime model package)

### Deprecate/delete after completion
- Any config loaders that directly instantiate runtime objects from TOML/JSON.
- Ad-hoc validation logic embedded in config loaders (move to runtime models).

### Implementation checklist
- [ ] Define `*Spec` msgspec types for compile, otel, and semantic config.
- [ ] Create Pydantic runtime models for each spec with cross-field validators.
- [ ] Add conversion bridges `resolve_*` or `from_spec`/`to_spec`.
- [ ] Update CLI/config loading to decode specs, then validate via runtime models.

---

## Scope 5 — Runtime Validation Layer (Pydantic) (P1)

**Goal**: Establish a shared runtime validation infrastructure (base model policy, trust-boundary entrypoints, and reusable validation types) for all runtime-derived configurations.

### Representative code patterns

```python
from pydantic import BaseModel, ConfigDict

class RuntimeBase(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_default=True)
```

```python
from pydantic import TypeAdapter

COMPILE_ADAPTER = TypeAdapter(DataFusionCompileOptionsRuntime)

# Reuse the adapter to avoid repeated schema builds
runtime = COMPILE_ADAPTER.validate_python(payload)
```

```python
# Trust-boundary entrypoints
runtime = RuntimeModel.model_validate(payload)          # Python dict
runtime = RuntimeModel.model_validate_strings(payload)  # env/CLI string dicts
runtime = RuntimeModel.model_construct(**trusted)       # only after hash/contract checks
```

### Target files
- `src/runtime_models/base.py` (new)
- `src/runtime_models/types.py` (new reusable Annotated validators)
- `src/runtime_models/compile.py` (new)
- `src/runtime_models/otel.py` (new)
- `src/runtime_models/semantic.py` (new)
- `src/cli/config_loader.py`

### Deprecate/delete after completion
- Duplicate per-module validation functions that re-check the same invariants.
- Runtime object construction without Pydantic validation.

### Implementation checklist
- [ ] Add `RuntimeBase` with strict config policy.
- [ ] Implement `TypeAdapter` caches for hot-path runtime models.
- [ ] Introduce reusable `Annotated` validation types in `types.py`.
- [ ] Route all config loading through runtime model validation entrypoints.

---

## Scope 6 — DataFrame Validation Layer (Pandera) (P1)

**Goal**: Add Pandera-driven runtime validation for DataFrame outputs, with schemas derived from msgspec schema specs.

### Representative code patterns

```python
import pandera.pandas as pa

def to_pandera_schema(spec: TableSchemaSpec) -> pa.DataFrameSchema:
    columns = {
        field.name: pa.Column(dtype=field.to_pandera_dtype(), nullable=field.nullable)
        for field in spec.fields
    }
    return pa.DataFrameSchema(columns, strict=True, ordered=True)
```

```python
@pa.check_types(lazy=True)
def normalize_table(df: pa.typing.DataFrame[MySchema]) -> pa.typing.DataFrame[MySchema]:
    ...
```

```python
class ValidationPolicySpec(StructBaseStrict, frozen=True):
    enabled: bool = True
    lazy: bool = True
    sample: int | None = None
    head: int | None = None
    tail: int | None = None
```

### Target files
- `src/schema_spec/pandera_bridge.py` (new)
- `src/schema_spec/field_spec.py` (pandera dtype adapters)
- `src/semantics/pipeline.py`
- `src/extract/*` (validation hooks at extractor boundaries)
- `src/cpg/*` (validation for materialized outputs)
- `src/obs/diagnostics.py` (validation error reporting)

### Deprecate/delete after completion
- Ad-hoc DataFrame shape/type checks scattered across pipeline stages.
- Redundant schema assertions that can be expressed by Pandera.

### Implementation checklist
- [ ] Implement Pandera schema bridge from msgspec TableSchemaSpec.
- [ ] Add `ValidationPolicySpec` and runtime policy resolution.
- [ ] Integrate Pandera validation at key pipeline boundaries (extract, semantics, CPG).
- [ ] Add diagnostics surfacing for Pandera SchemaErrors.

---

## Scope 7 — Delta Control-Plane Requests/Bundles (P1)

**Goal**: Move Delta control-plane request/response types to msgspec for deterministic encoding and contract export.

### Representative code patterns

```python
class DeltaTableRef(StructBaseStrict, frozen=True):
    table_uri: str
    storage_options: Mapping[str, str] | None
    version: int | None
    timestamp: str | None

class DeltaWriteRequest(StructBaseStrict, frozen=True):
    table: DeltaTableRef
    data_ipc: msgspec.Raw
    mode: str
    schema_mode: str | None
```

### Target files
- `src/datafusion_engine/delta/control_plane.py`
- `src/datafusion_engine/delta/observability.py`
- `src/storage/deltalake/delta.py`

### Deprecate/delete after completion
- Dataclass versions of Delta request/bundle types.
- Any dict-based payloads that duplicate these request types.

### Implementation checklist
- [ ] Introduce `DeltaTableRef` and reuse across all requests.
- [ ] Convert requests/bundles to msgspec structs.
- [ ] Replace raw bytes with `msgspec.Raw` where appropriate.
- [ ] Update any msgpack/json payloads to use `msgspec.to_builtins`.

---

## Scope 8 — Plan/Cache/Registry msgspec Contracts (P1)

**Goal**: Ensure cache keys and plan registry payloads are msgspec-serializable and schema-exportable.

### Representative code patterns

```python
class PlanCacheKey(StructBaseCompat, frozen=True):
    profile_hash: str
    substrait_hash: str
    plan_fingerprint: str
    # ...
```

```python
class UdfCatalogSnapshot(StructBaseStrict, frozen=True):
    specs: Mapping[str, DataFusionUdfSpec]
    function_factory_hash: str | None = None
```

### Target files
- `src/datafusion_engine/plan/cache.py`
- `src/datafusion_engine/plan/result_types.py`
- `src/datafusion_engine/plan/diagnostics.py`
- `src/datafusion_engine/udf/catalog.py`
- `src/serde_schema_registry.py`

### Deprecate/delete after completion
- Dataclass cache key/entry types.
- Legacy snapshot dict structures where a msgspec contract exists.

### Implementation checklist
- [ ] Convert plan cache keys/entries to `StructBaseCompat`.
- [ ] Add msgspec snapshots for UDF registry and plan diagnostics.
- [ ] Register new snapshot types in `serde_schema_registry.py`.

---

## Scope 9 — Semantics / CPG / Relspec Contracts (P2)

**Goal**: Make declarative specs in semantics, CPG, and relspec msgspec to align with system-wide contract export and validation.

### Representative code patterns

```python
class SemanticTableSpec(StructBaseStrict, frozen=True):
    table: str
    primary_span: SpanBinding = msgspec.field(default_factory=...)
```

### Target files
- `src/semantics/specs.py`
- `src/semantics/config.py`
- `src/relspec/*`
- `src/cpg/specs.py`
- `src/cpg/emit_specs.py`

### Deprecate/delete after completion
- Dataclass versions of semantic/cpg/relspec specs.

### Implementation checklist
- [ ] Convert spec dataclasses to msgspec.
- [ ] Update any dict snapshots to use msgspec serialization.
- [ ] Add msgpack round-trip tests for key spec groups.

---

## Scope 10 — OTel Config Spec + Runtime Resolution (P2)

**Goal**: Introduce `OtelConfigSpec` (msgspec) and decouple runtime SDK objects from config decoding, using Pydantic runtime validation.

### Representative code patterns

```python
class OtelConfigSpec(StructBaseStrict, frozen=True):
    enable_traces: bool
    enable_metrics: bool
    sampler: Literal["always_on", "always_off", "ratio"]
    sampler_arg: float | None = None
```

```python
class OtelConfigRuntime(RuntimeBase):
    enable_traces: bool
    enable_metrics: bool
    sampler: str
```

### Target files
- `src/obs/otel/config.py`
- `src/obs/otel/bootstrap.py`
- `src/runtime_models/otel.py`

### Deprecate/delete after completion
- Config decoding paths that instantiate SDK objects directly from TOML/JSON.

### Implementation checklist
- [ ] Add `OtelConfigSpec` msgspec struct.
- [ ] Implement `OtelConfigRuntime` Pydantic model with validators.
- [ ] Implement `resolve_otel_config(spec) -> OtelConfig`.
- [ ] Update config loading to decode spec then validate/runtime-resolve.

---

## Scope 11 — CLI/Config Loading Updates (P2)

**Goal**: Ensure TOML/JSON config parsing uses msgspec specs only, then resolves runtime objects via Pydantic models.

### Representative code patterns

```python
raw = msgspec.toml.decode(text, type=object, strict=True)
spec = msgspec.convert(raw, type=RootConfigSpec, strict=True)
runtime = RootConfigRuntime.model_validate(msgspec.to_builtins(spec))
```

### Target files
- `src/cli/config_loader.py`
- `src/cli/config_models.py`
- `src/engine/runtime_profile.py`
- `src/runtime_models/root.py` (new)

### Deprecate/delete after completion
- Flat config fields and legacy normalization code.
- Any direct conversions into runtime-only types.

### Implementation checklist
- [ ] Define spec types for root config and nested sections.
- [ ] Add Pydantic runtime model for root config.
- [ ] Update loader to decode to spec, then validate and resolve runtime objects.
- [ ] Update CLI overrides to apply to spec objects first.

---

## Scope 12 — Contract Registry + Tests (P0–P2)

**Goal**: Keep msgspec contract registry authoritative and ensure round-trip and schema evolution tests cover new contracts.

### Representative code patterns

```python
_SCHEMA_TYPES: tuple[type[msgspec.Struct], ...] = (
    TableSchemaSpec,
    DatasetSpec,
    QuerySpec,
    # ...
)
```

### Target files
- `src/serde_schema_registry.py`
- `tests/msgspec_contract/`
- `tests/unit/test_msgspec_roundtrip.py`

### Deprecate/delete after completion
- Duplicate schema export helpers for types now in registry.

### Implementation checklist
- [ ] Register new msgspec contract types.
- [ ] Update schema registry golden snapshots.
- [ ] Add/update round-trip tests for new msgspec contracts.

---

## Milestone Ordering

1. **Schema/Query Contracts + Arrow Type IR** (Scopes 0–1)
2. **DatasetSpec Nesting + Runtime Configs** (Scopes 2–3)
3. **Spec/Runtime Splits + Runtime Models** (Scopes 4–5)
4. **Pandera DataFrame Validation** (Scope 6)
5. **Delta + Plan/Registry Contracts** (Scopes 7–8)
6. **Semantics/CPG/Relspec + OTel + CLI** (Scopes 9–11)
7. **Registry + Tests** (Scope 12)

---

## Global Notes

- Favor `StructBaseStrict` for config/spec surfaces and `StructBaseCompat` for persisted artifacts.
- Prefer `msgspec.UNSET` for tri-state overrides.
- Use tagged unions for multi-shape payloads and Arrow IR.
- Treat Pydantic runtime models as the strict boundary for derived configs.
- Use Pandera at runtime boundaries with configurable sampling and lazy error reporting.
- All breaking changes should update call sites rather than add compatibility shims.
