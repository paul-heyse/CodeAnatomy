## SchemaSpec ArrowDSL Intensification Plan

### Goals
- Make schema_spec a thin, declarative layer over ArrowDSL compute + plan helpers.
- Encode schema constraints and ordering semantics in Arrow metadata for round-trip fidelity.
- Push validation and projection logic into Acero plan lane wherever possible.
- Keep schema inference, alignment, and encoding policies fully Arrow-native.

### Constraints
- Preserve existing dataset schemas and contract semantics unless explicitly updated.
- Keep plan-lane operations inside Acero; kernel-lane operations inside ArrowDSL helpers.
- Avoid introducing new non-Arrow runtime dependencies.
- Maintain strict typing and Ruff compliance (no suppressions).

---

### Scope 1: Schema Metadata Round-Trip (Constraints + Ordering)

### Description
Encode schema constraints and ordering metadata into Arrow schema metadata and provide
round-trip helpers that can reconstruct `TableSchemaSpec` and `DatasetSpec` without
losing key_fields/required_non_null/canonical ordering semantics.

### Code patterns
```python
# src/arrowdsl/schema/spec_roundtrip.py
from arrowdsl.schema.metadata import ordering_metadata_spec
from schema_spec.specs import schema_metadata


def schema_metadata_for_spec(spec: TableSchemaSpec) -> dict[bytes, bytes]:
    meta = schema_metadata(spec.name, spec.version or 0)
    meta[b"required_non_null"] = ",".join(spec.required_non_null).encode("utf-8")
    meta[b"key_fields"] = ",".join(spec.key_fields).encode("utf-8")
    return meta


def apply_spec_metadata(spec: TableSchemaSpec) -> SchemaMetadataSpec:
    return SchemaMetadataSpec(schema_metadata=schema_metadata_for_spec(spec))
```

### Target files
- `src/arrowdsl/schema/spec_roundtrip.py` (new)
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/arrowdsl/schema/metadata.py`

### Implementation checklist
- [ ] Add schema metadata encode/decode helpers for required/key fields.
- [ ] Extend `table_spec_from_schema` to rehydrate constraints from metadata.
- [ ] Merge ordering metadata into `DatasetSpec.schema()` when contract ordering exists.

---

### Scope 2: Canonical Constraint Compiler in ArrowDSL

### Description
Centralize constraint-to-expression compilation in ArrowDSL so schema_spec can reuse
the same primitives for validation, plan-lane masks, and error reporting.

### Code patterns
```python
# src/arrowdsl/schema/constraints.py
from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc


def required_non_null_mask(
    required: Sequence[str],
    *,
    available: set[str],
) -> ComputeExpression:
    exprs = [
        ensure_expression(pc.invert(pc.is_valid(pc.field(name))))
        for name in required
        if name in available
    ]
    if not exprs:
        return ensure_expression(pc.scalar(pa.scalar(False)))
    return ensure_expression(pc.or_(*exprs))
```

### Target files
- `src/arrowdsl/schema/constraints.py` (new)
- `src/arrowdsl/schema/validation.py`
- `src/schema_spec/system.py`

### Implementation checklist
- [ ] Add constraint compiler helpers for required/key checks.
- [ ] Update validation to use centralized constraint helpers.
- [ ] Add constraint helpers to ArrowDSL exports for reuse.

---

### Scope 3: Plan-Lane Validation Entry Points

### Description
Expose validation entry points that accept a `DatasetSource` or `Plan` and return a
plan-lane validation pipeline (invalid rows + duplicate keys) without materializing
tables before the finalize boundary.

### Code patterns
```python
# src/schema_spec/system.py
from arrowdsl.plan.source import DatasetSource, plan_from_source
from arrowdsl.schema.validation import invalid_rows_plan, duplicate_key_rows


def validate_dataset_source(
    spec: DatasetSpec,
    source: TableLike | DatasetSource,
    *,
    ctx: ExecutionContext,
) -> tuple[Plan, TableLike]:
    plan = plan_from_source(source, ctx=ctx, label=f"{spec.name}_validate")
    invalid_plan = invalid_rows_plan(plan.to_table(ctx=ctx), spec=spec.table_spec, ctx=ctx)
    dupes = duplicate_key_rows(plan.to_table(ctx=ctx), keys=spec.table_spec.key_fields, ctx=ctx)
    return invalid_plan, dupes
```

### Target files
- `src/schema_spec/system.py`
- `src/arrowdsl/schema/validation.py`
- `src/arrowdsl/plan/source.py`

### Implementation checklist
- [ ] Add DatasetSpec-level validation helpers for plan sources.
- [ ] Keep invalid-row checks in plan lane where possible.
- [ ] Return validation artifacts in Arrow tables for finalize integration.

---

### Scope 4: Derived Columns + Pushdown Predicates in Specs

### Description
Allow schema specs to define derived columns and predicates using ArrowDSL `ExprSpec`,
so dataset scans can push projections and filters into Acero/Scanner.

### Code patterns
```python
# src/schema_spec/specs.py
class DerivedFieldSpec(BaseModel):
    name: str
    expr: ExprSpec


# src/schema_spec/system.py
def query(self) -> QuerySpec:
    base = tuple(field.name for field in self.table_spec.fields)
    derived = {df.name: df.expr for df in self.derived_fields}
    return QuerySpec(projection=ProjectionSpec(base=base, derived=derived))
```

### Target files
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/arrowdsl/compute/expr_specs.py`
- `src/arrowdsl/plan/query.py`

### Implementation checklist
- [ ] Add `DerivedFieldSpec` to specs with ArrowDSL `ExprSpec`.
- [ ] Build `QuerySpec` using derived expressions for scan projections.
- [ ] Add optional pushdown predicates in `DatasetSpec`.

---

### Scope 5: Encoding Policy From SchemaSpec

### Description
Extend `ArrowFieldSpec` with encoding hints (e.g., dictionary encode) and build an
ArrowDSL `EncodingPolicy` directly from `TableSchemaSpec`. Apply this policy in
finalize contexts and dataset writes.

### Code patterns
```python
# src/schema_spec/specs.py
class ArrowFieldSpec(BaseModel):
    ...
    encoding: Literal["dictionary"] | None = None


# src/arrowdsl/schema/encoding.py
def encoding_policy_from_spec(spec: TableSchemaSpec) -> EncodingPolicy:
    specs = [EncodingSpec(col=field.name, encoding="dictionary") for field in spec.fields
             if field.encoding == "dictionary"]
    return EncodingPolicy(specs=tuple(specs))
```

### Target files
- `src/schema_spec/specs.py`
- `src/arrowdsl/schema/encoding.py` (new)
- `src/schema_spec/system.py`
- `src/storage/parquet.py`

### Implementation checklist
- [ ] Add `encoding` hint to `ArrowFieldSpec`.
- [ ] Build `EncodingPolicy` from specs and apply during finalize/write paths.
- [ ] Ensure dictionary unification before Parquet writes.

---

### Scope 6: Nested Type Helpers in ArrowDSL

### Description
Move list_view/map/struct helpers out of schema_spec into ArrowDSL so nested type
construction is canonical and consistent with Arrow best practices.

### Code patterns
```python
# src/arrowdsl/schema/arrays.py
def list_view_type(value_type: DataTypeLike, *, large: bool = False) -> DataTypeLike:
    return pa.large_list_view(value_type) if large else pa.list_view(value_type)
```

### Target files
- `src/arrowdsl/schema/arrays.py`
- `src/arrowdsl/schema/__init__.py`
- `src/schema_spec/specs.py`

### Implementation checklist
- [ ] Move `list_view_type` to ArrowDSL and re-export.
- [ ] Add map/struct helpers for explicit nested type selection.
- [ ] Update schema_spec to use ArrowDSL nested helpers.

---

### Scope 7: Ordering Metadata Integration

### Description
Ensure ordering semantics are encoded in schema metadata using ArrowDSL ordering helpers.
This provides deterministic ordering metadata across scans, joins, and finalize boundaries.

### Code patterns
```python
# src/schema_spec/system.py
from arrowdsl.schema.metadata import ordering_metadata_spec, merge_metadata_specs

def schema(self) -> SchemaLike:
    base = self.metadata_spec
    ordering = ordering_metadata_spec(level=OrderingLevel.IMPLICIT, keys=self.contract_keys())
    merged = merge_metadata_specs(base, ordering)
    return merged.apply(self.table_spec.to_arrow_schema())
```

### Target files
- `src/schema_spec/system.py`
- `src/arrowdsl/schema/metadata.py`

### Implementation checklist
- [ ] Derive ordering keys from contract canonical_sort or key fields.
- [ ] Apply ordering metadata in `DatasetSpec.schema`.
- [ ] Ensure ordering metadata round-trips via `table_spec_from_schema`.

---

### Scope 8: Dataset Discovery + Registry Helpers

### Description
Add helpers to build DatasetSpecs from Arrow datasets and register them in the schema
registry. Use dataset factories for schema inspection and optional mmap policies.

### Code patterns
```python
# src/schema_spec/system.py
def dataset_spec_from_dataset(
    name: str,
    dataset: ds.Dataset,
    *,
    version: int | None = None,
) -> DatasetSpec:
    table_spec = table_spec_from_schema(name, dataset.schema, version=version)
    return make_dataset_spec(table_spec=table_spec)
```

### Target files
- `src/schema_spec/system.py`
- `src/schema_spec/__init__.py`
- `src/arrowdsl/plan/query.py`

### Implementation checklist
- [ ] Add DatasetSpec factories for ds.Dataset and dataset paths.
- [ ] Add SchemaRegistry helpers to register datasets from filesystem scans.
- [ ] Preserve schema metadata and constraints on discovery.
