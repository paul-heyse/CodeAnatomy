# Msgspec/Pydantic/Pandera Consolidation Plan (v1)

> **Purpose**: Implement the 10 consolidation recommendations to reduce duplication, strengthen validation boundaries, and expand msgspec/pydantic/pandera usage.
>
> **Status**: Implemented (scopes 1–10 complete; updated 2026-02-05)
>
> **Assumptions**: Design-phase. Breaking changes acceptable when they simplify architecture.
>
> **References**:
> - `docs/python_library_reference/msgspec.md`
> - `docs/python_library_reference/pydantic.md`
> - `docs/python_library_reference/pandera.md`

---

## Scope 1 — Shared Constrained Types (msgspec.Meta + Pydantic Annotated)

**Goal**: Centralize string/id/hash constraints so they are used consistently across msgspec and Pydantic models, with JSON schema metadata baked in.

**Status**: Complete. Shared aliases exist in `src/core_types.py` and are applied to key specs/runtime models. Target files with no applicable pattern fields (e.g., `src/serde_schema_registry.py`, `src/obs/otel/config.py`) required no changes.

### Representative code patterns

```python
from typing import Annotated
from msgspec import Meta

HashStr = Annotated[
    str,
    Meta(
        pattern=HASH_PATTERN,
        title="Hash Value",
        description="Deterministic hash value.",
        examples=["sha256:4a7f2b1c9e4d5a6f7b8c9d0e1f2a3b4c"],
    ),
]
```

```python
from pydantic import Field
from typing import Annotated

RunIdStr = Annotated[str, Field(pattern=RUN_ID_PATTERN, min_length=8, max_length=64)]
```

### Target files
- `src/core_types.py`
- `src/serde_schema_registry.py`
- `src/semantics/config.py`
- `src/obs/otel/config.py`
- `src/datafusion_engine/compile/options.py`
- `src/schema_spec/system.py`

### Deprecate/delete after completion
- Ad-hoc regex constants used only in single modules (replace with shared aliases).
- Duplicate pattern validation helpers in downstream config parsing.

### Implementation checklist
- [x] Add shared `Annotated[...]` aliases in `src/core_types.py`.
- [x] Apply aliases to msgspec specs for schema export.
- [x] Apply aliases to Pydantic runtime models for consistent validation.
- [x] Remove module-local duplicate pattern validators (no additional duplicates found).

---

## Scope 2 — Unify Scalar Literal Specs for Expression IR

**Goal**: Consolidate scalar literal definitions so expression specs, hashing helpers, and other expression surfaces all use the same msgspec tagged literal spec (no parallel union types).

**Status**: Complete. `ScalarLiteralInput` is canonical in `expr/spec.py`; `ScalarValue` is a direct alias to it and hashing uses the shared alias.

### Representative code patterns

```python
class ScalarStringLiteral(ScalarLiteralBase, tag="string", frozen=True):
    value: str

ScalarLiteralSpec: TypeAlias = (
    ScalarNullLiteral
    | ScalarBoolLiteral
    | ScalarIntLiteral
    | ScalarFloatLiteral
    | ScalarStringLiteral
    | ScalarBytesLiteral
)
```

```python
def scalar_literal(value: ScalarLike | bool | int | float | str | bytes | None) -> ScalarLiteralSpec:
    ...
```

### Target files
- `src/datafusion_engine/expr/spec.py`
- `src/datafusion_engine/hashing.py`
- `src/arrow_utils/core/expr_types.py`

### Deprecate/delete after completion
- `ScalarValue` unions that duplicate literal typing rules.
- Any local “literal coercion” helpers not using `scalar_literal` / `scalar_literal_value`.

### Implementation checklist
- [x] Export scalar literal spec from `expr/spec.py` as the canonical type.
- [x] Replace `ScalarValue` usage with the literal spec (or alias to it).
- [x] Normalize all literal creation through `scalar_literal`.

---

## Scope 3 — Centralize Pandera Validation Entry Points

**Goal**: Replace repeated validation wiring (policy resolution, diagnostics, sampling) with a shared helper so all call sites behave identically.

**Status**: Complete. `validate_with_policy` exists and call sites in semantics, extract, and write paths have been migrated.

### Representative code patterns

```python
def validate_with_policy(
    df: object,
    *,
    schema_spec: TableSchemaSpec,
    policy: ValidationPolicySpec | None,
    diagnostics: DiagnosticsSink | None,
    name: str,
) -> object:
    if policy is None or not policy.enabled:
        return df
    try:
        validate_dataframe(df, schema_spec=schema_spec, policy=policy)
    except Exception as exc:
        if diagnostics is not None:
            record_dataframe_validation_error(diagnostics, name=name, error=exc, policy=policy)
        raise
    return df
```

### Target files
- `src/schema_spec/pandera_bridge.py`
- `src/semantics/pipeline.py`
- `src/datafusion_engine/io/write.py`
- `src/extract/coordination/schema_ops.py`
- `src/obs/diagnostics.py`

### Deprecate/delete after completion
- Inline validation+diagnostics blocks in each call site.

### Implementation checklist
- [x] Implement a shared `validate_with_policy` helper.
- [x] Update all call sites to use the helper.
- [x] Remove duplicated error reporting logic.

---

## Scope 4 — Expand Pandera Schema Coverage from Spec Semantics

**Goal**: Map more schema semantics to Pandera (unique constraints, required non-null, dedupe, constraints) using Pandera checks and DataFrameSchema features.

**Status**: Complete. `key_fields`, `required_non_null`, and minimal constraint checks (`IS NOT NULL` / `IS NULL`) are enforced, with `strict/coerce` policy support.

### Representative code patterns

```python
schema = pa.DataFrameSchema(
    columns=columns,
    strict=True,
    ordered=True,
    unique=tuple(table_spec.key_fields) if table_spec.key_fields else None,
)
```

```python
column = pa.Column(
    dtype=field.to_pandera_dtype(),
    nullable=field.nullable,
    checks=[pa.Check.not_null()] if field.name in required_non_null else None,
)
```

### Target files
- `src/schema_spec/pandera_bridge.py`
- `src/schema_spec/system.py`
- `src/schema_spec/specs.py`

### Deprecate/delete after completion
- Ad-hoc data validation that duplicates constraints already in specs.

### Implementation checklist
- [x] Map key-fields to `DataFrameSchema.unique`.
- [x] Map `required_non_null` and constraint expressions to Pandera checks.
- [x] Add optional coercion/strictness tuning via policy.

---

## Scope 5 — DataFrameModel + check_types at High-Value Boundaries

**Goal**: Use Pandera `DataFrameModel` + `@check_types` where function boundaries produce critical datasets (e.g., CPG/semantic outputs).

**Status**: Complete. High-value outputs route through `validate_with_policy`, which enforces DataFrameModel + `@check_types` without per-function decorators.

### Representative code patterns

```python
class CpgNodesSchema(pa.DataFrameModel):
    node_id: pa.typing.Series[str]
    node_kind: pa.typing.Series[str]

    class Config:
        strict = True
        coerce = True

@pa.check_types(lazy=True)
def build_cpg_nodes_df(...) -> pa.typing.DataFrame[CpgNodesSchema]:
    ...
```

### Target files
- `src/cpg/view_builders_df.py`
- `src/semantics/pipeline.py`
- `src/schema_spec/pandera_bridge.py`

### Deprecate/delete after completion
- Manual output-shape assertions in view builders.

### Implementation checklist
- [x] Add generated DataFrameModels from TableSchemaSpec.
- [x] Route CPG and semantic output builders through `validate_with_policy` (centralized DataFrameModel + `@check_types` path).
- [x] Align `ValidationPolicySpec` with `@check_types` sampling knobs.

---

## Scope 6 — Tighten Pydantic Trust Boundaries

**Goal**: Strengthen runtime validation policy to prevent stale/invalid models from being re-used silently.

**Status**: Complete. Runtime base config enforces `revalidate_instances="always"` and env/CLI payloads now use `validate_strings`/`model_validate_json`.

### Representative code patterns

```python
class RuntimeBase(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_default=True,
        frozen=True,
        revalidate_instances="always",
    )
```

```python
runtime = ROOT_CONFIG_ADAPTER.validate_strings(env_payload)
```

### Target files
- `src/runtime_models/base.py`
- `src/runtime_models/root.py`
- `src/cli/config_loader.py`
- `src/engine/runtime_profile.py`

### Deprecate/delete after completion
- Any runtime construction paths that bypass Pydantic validation.

### Implementation checklist
- [x] Enforce `revalidate_instances="always"` where appropriate.
- [x] Route env/CLI inputs through `validate_strings`.
- [x] Prefer `model_validate_json` for JSON payloads.

---

## Scope 7 — Numeric Bounds via msgspec.Meta

**Goal**: Use `msgspec.Meta` constraints to enforce numeric bounds and generate richer JSON schemas.

**Status**: Complete. Shared numeric aliases exist and are applied to the target specs.

### Representative code patterns

```python
PositiveInt = Annotated[int, Meta(gt=0)]
NonNegativeInt = Annotated[int, Meta(ge=0)]
```

```python
class DiskCacheSettings(StructBaseStrict, frozen=True):
    size_limit_bytes: NonNegativeInt
    cull_limit: PositiveInt = 10
```

### Target files
- `src/cache/diskcache_factory.py`
- `src/datafusion_engine/compile/options.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/schema/validation.py`

### Deprecate/delete after completion
- Manual “if < 0” checks that duplicate schema constraints.

### Implementation checklist
- [x] Introduce numeric aliases with `Meta(ge=..., gt=...)`.
- [x] Apply aliases to spec fields with bounds.
- [x] Remove redundant runtime validation code where safe.

---

## Scope 8 — Hot-Path Struct Optimization

**Goal**: Identify and move high-volume internal artifacts to `StructBaseHotPath` and/or `array_like=True` for performance, within schema evolution constraints.

**Status**: Complete. Hot structs moved to `StructBaseHotPath`/`array_like=True` and schema evolution documentation updated with ordering implications.

### Representative code patterns

```python
class PlanCacheKey(StructBaseHotPath, frozen=True, array_like=True):
    profile_hash: str
    substrait_hash: str
    plan_fingerprint: str
```

### Target files
- `src/datafusion_engine/plan/cache.py`
- `src/datafusion_engine/plan/diagnostics.py`
- `src/relspec/runtime_artifacts.py`

### Deprecate/delete after completion
- Legacy dict-based cache keys/entries.

### Implementation checklist
- [x] Identify hot structs with stable schema evolution.
- [x] Apply `StructBaseHotPath` or `array_like=True` where safe.
- [x] Update schema evolution documentation if array-like ordering changes.

---

## Scope 9 — Consolidate Runtime Adapters

**Goal**: Consolidate `TypeAdapter` instances and runtime model conversion entrypoints into a single module to avoid duplication and drift.

**Status**: Complete. Centralized adapters module exists and import sites have been migrated.

### Representative code patterns

```python
# runtime_models/adapters.py
COMPILE_ADAPTER = TypeAdapter(DataFusionCompileOptionsRuntime)
OTEL_ADAPTER = TypeAdapter(OtelConfigRuntime)
ROOT_ADAPTER = TypeAdapter(RootConfigRuntime)
```

### Target files
- `src/runtime_models/compile.py`
- `src/runtime_models/otel.py`
- `src/runtime_models/semantic.py`
- `src/runtime_models/root.py`
- `src/runtime_models/__init__.py` or new `src/runtime_models/adapters.py`

### Deprecate/delete after completion
- Per-module adapter globals that can be consolidated.

### Implementation checklist
- [x] Create a unified adapters module.
- [x] Update import sites to use centralized adapters.
- [x] Remove duplicate adapter declarations.

---

## Scope 10 — Consolidate Dataset Policy Resolution

**Goal**: Centralize policy resolution and precedence rules for datafusion scan + delta policies to eliminate divergent logic in registry/scan/profile helpers.

**Status**: Complete. Centralized resolver is used across registry/registration, and legacy delta/scan helpers were removed.

### Representative code patterns

```python
def resolve_dataset_policies(
    location: DatasetLocation,
    overrides: DatasetLocationOverrides | None,
) -> ResolvedDatasetPolicies:
    ...
```

### Target files
- `src/datafusion_engine/dataset/registry.py`
- `src/storage/deltalake/scan_profile.py`
- `src/datafusion_engine/schema/contracts.py`
- `src/datafusion_engine/dataset/registration.py`

### Deprecate/delete after completion
- Duplicate local merge/resolve helpers scattered across modules.

### Implementation checklist
- [x] Introduce a centralized policy resolver.
- [x] Migrate callers to use it.
- [x] Remove redundant resolution helpers.

---

## Milestone Ordering

1) **Shared constrained types + scalar literal consolidation** (Scopes 1–2)
2) **Pandera consolidation + schema expansion** (Scopes 3–5)
3) **Pydantic trust-boundary hardening + Meta bounds** (Scopes 6–7)
4) **Performance + adapters + policy resolution** (Scopes 8–10)

---

## Global Notes

- Prefer `StructBaseStrict` for spec/config; use `StructBaseCompat` or `StructBaseHotPath` only when justified.
- Keep JSON Schema output stable; use `Meta` to enrich schema descriptions and constraints.
- Treat runtime models as trust boundaries; avoid bypassing Pydantic validation.
- Pandera validations should be policy-gated and diagnostics-integrated.
