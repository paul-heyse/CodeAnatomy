## PyArrow Validation + Inference Migration Plan

### Scope 1: Arrow Validation Core (Options + Report)

### Description
Introduce Arrow-native validation types and a single entrypoint that returns a structured
report (valid flag, error stats, and optional invalid rows). This replaces the Pandera
options surface and becomes the canonical validation API used across schema_spec and finalize.

### Code patterns
```python
# src/arrowdsl/schema/validation.py
from dataclasses import dataclass
from typing import Literal

from arrowdsl.core.interop import TableLike


@dataclass(frozen=True)
class ArrowValidationOptions:
    strict: bool | Literal["filter"] = "filter"
    coerce: bool = False
    max_errors: int | None = None
    emit_invalid_rows: bool = True
    emit_error_table: bool = True


@dataclass(frozen=True)
class ValidationReport:
    valid: bool
    errors: TableLike
    stats: TableLike
    invalid_rows: TableLike | None


def validate_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions,
) -> ValidationReport:
    ...
```

### Target files
- `src/arrowdsl/schema/validation.py` (new)
- `src/arrowdsl/schema/__init__.py`
- `src/arrowdsl/core/context.py` (policy-to-options mapping)

### Implementation checklist
- [x] Add `ArrowValidationOptions` and `ValidationReport`.
- [x] Add `validate_table` entrypoint and signature in ArrowDSL.
- [x] Add helper to translate `SchemaValidationPolicy` into Arrow options.

---

### Scope 2: Plan-Lane Validation Expressions (Required + Keys + Type Checks)

### Description
Compile `TableSchemaSpec` constraints into compute expressions and Acero plans. Required
fields and key-uniqueness checks are emitted as plan-lane filters, with optional coercion
via `pc.cast(..., safe=False)` when configured.

### Code patterns
```python
# src/arrowdsl/schema/validation.py
from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc
from arrowdsl.plan.plan import Plan


def required_non_null_mask(spec: TableSchemaSpec) -> ComputeExpression:
    exprs = [
        ensure_expression(pc.invert(pc.is_valid(pc.field(name))))
        for name in spec.required_non_null
    ]
    if not exprs:
        return ensure_expression(pc.scalar(False))
    return ensure_expression(pc.or_(*exprs))


def invalid_rows_plan(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    ctx: ExecutionContext,
) -> Plan:
    mask = required_non_null_mask(spec)
    return Plan.table_source(table, label=f"{spec.name}_validate").filter(mask, ctx=ctx)


def duplicate_key_rows(
    table: TableLike,
    *,
    keys: Sequence[str],
    ctx: ExecutionContext,
) -> TableLike:
    count_col = f"{keys[0]}_count"
    plan = Plan.table_source(table, label="validate_keys").aggregate(
        group_keys=keys,
        aggs=[(keys[0], "count")],
        ctx=ctx,
    )
    dupes = plan.filter(ensure_expression(pc.greater(pc.field(count_col), pc.scalar(1))), ctx=ctx)
    return dupes.to_table(ctx=ctx)
```

### Target files
- `src/arrowdsl/schema/validation.py`
- `src/arrowdsl/compute/predicates.py` (optional: shared masks)
- `src/arrowdsl/plan_helpers.py` (optional: reuse column-or-null helpers)

### Implementation checklist
- [x] Add required-non-null and key-duplicate expression builders.
- [x] Build invalid-row plans using `Plan.table_source` + `filter`.
- [x] Add type-mismatch checks via `pc.cast` when `options.coerce` is enabled.

---

### Scope 3: SchemaSpec Integration (Remove Pandera/Polars)

### Description
Replace `pandera.polars` + `polars` validation with ArrowDSL validation. Remove dtype
conversion helpers and update schema_spec exports to expose Arrow validation types.

### Code patterns
```python
# src/schema_spec/system.py
from arrowdsl.schema.validation import ArrowValidationOptions, validate_table


def validate_arrow_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions | None = None,
) -> TableLike:
    options = options or ArrowValidationOptions()
    report = validate_table(table, spec=spec, options=options)
    return table if report.valid else report.invalid_rows or table
```

### Target files
- `src/schema_spec/system.py`
- `src/schema_spec/__init__.py`
- `src/schema_spec/specs.py` (if new constraints are added)

### Implementation checklist
- [x] Remove pandera/polars imports and dtype conversion tables.
- [x] Replace `PanderaValidationOptions` with `ArrowValidationOptions`.
- [x] Update `ContractSpecKwargs`/`DatasetSpecKwargs` to use new options.
- [x] Update `schema_spec.__init__` exports for the new API.

---

### Scope 4: Finalize Integration (Arrow Validation in Contracts)

### Description
Replace `_maybe_validate_with_pandera` with an Arrow-native validator, wired through
`ExecutionContext.schema_validation`. Ensure strict/filter semantics are preserved
while keeping output tables in Arrow form.

### Code patterns
```python
# src/arrowdsl/finalize/finalize.py
from arrowdsl.schema.validation import ArrowValidationOptions, validate_table


def _maybe_validate_with_arrow(
    table: TableLike,
    *,
    contract: Contract,
    ctx: ExecutionContext,
) -> TableLike:
    if not ctx.schema_validation.enabled or contract.schema_spec is None:
        return table
    options = contract.validation or ArrowValidationOptions.from_policy(ctx.schema_validation)
    report = validate_table(table, spec=contract.schema_spec, options=options)
    return table if report.valid else report.invalid_rows or table
```

### Target files
- `src/arrowdsl/finalize/finalize.py`
- `src/arrowdsl/core/context.py` (policy mapping helper)

### Implementation checklist
- [x] Update `Contract.validation` type to `ArrowValidationOptions | None`.
- [x] Replace pandera-based validation with Arrow validation entrypoint.
- [x] Preserve strict vs filter behavior via `SchemaValidationPolicy`.

---

### Scope 5: Arrow-Native Schema Inference (Remove Pandera/Pandas)

### Description
Replace pandas/pandera inference with Arrow-driven inference using unified schemas and
compute-based type probing. Keep metadata preservation and `SchemaEvolutionSpec` as
the primary schema alignment path.

### Code patterns
```python
# src/arrowdsl/schema/infer.py
import pyarrow as pa

from arrowdsl.core.interop import ArrayLike, TableLike, pc
from arrowdsl.schema.schema import SchemaEvolutionSpec


def best_fit_type(array: ArrayLike, candidates: Sequence[pa.DataType]) -> pa.DataType:
    for dtype in candidates:
        casted = pc.cast(array, dtype, safe=False)
        if int(pc.sum(pc.is_valid(casted)).as_py()) == array.length():
            return dtype
    return array.type


def infer_schema_from_tables(
    tables: Sequence[TableLike],
    *,
    promote_options: str = "permissive",
) -> pa.Schema:
    evolution = SchemaEvolutionSpec(promote_options=promote_options)
    base = evolution.unify_schema([t for t in tables if t is not None])
    return base
```

### Target files
- `src/arrowdsl/schema/infer.py` (new)
- `src/arrowdsl/schema/__init__.py`
- `src/normalize/schema_infer.py`

### Implementation checklist
- [x] Add Arrow inference helpers (`best_fit_type`, `infer_schema_from_tables`).
- [x] Remove pandas/pandera inference functions and options.
- [x] Update `SchemaInferOptions` to drop `use_pandera_infer`.

---

### Scope 6: Dependency Cleanup + Repo Sweep

### Description
Remove pandera/pandas/polars dependencies and ensure no remaining imports or exports
reference them. Keep all remaining validation and inference logic Arrow-native.

### Code patterns
```toml
# pyproject.toml
dependencies = [
    "pyarrow",
    "pydantic",
    "ruff",
    # pandas/pandera/polars removed after migration
]
```

### Target files
- `pyproject.toml`
- `uv.lock`
- `src/schema_spec/__init__.py`

### Implementation checklist
- [x] Remove `pandas`, `pandera`, and `polars` from dependency lists.
- [x] Remove remaining imports/re-exports referencing removed libraries.
- [x] Run `rg "pandera|pandas|polars"` to confirm zero usage.
