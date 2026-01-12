## PyArrow Schema + Compute Unification Plan

### Scope 1: DatasetSpec as the Single Source of Truth

### Description
Unify schema + contract + query + evolution + metadata into a single DatasetSpec that can emit
ScanContext, Plan, and FinalizeContext. This reduces duplicated wiring and makes spec objects
the sole source of behavior.

### Code patterns
```python
# src/schema_spec/core.py
@dataclass(frozen=True)
class DatasetSpec:
    table_spec: TableSchemaSpec
    contract_spec: ContractSpec | None = None
    query_spec: QuerySpec | None = None
    evolution_spec: SchemaEvolutionSpec = dataclass_field(default_factory=SchemaEvolutionSpec)
    metadata_spec: SchemaMetadataSpec = dataclass_field(default_factory=SchemaMetadataSpec)
    validation: PanderaValidationOptions | None = None

    def scan_context(self, dataset: ds.Dataset, ctx: ExecutionContext) -> ScanContext:
        return ScanContext(dataset=dataset, spec=self.query(), ctx=ctx)

    def finalize_context(self, ctx: ExecutionContext) -> FinalizeContext:
        transform = SchemaTransform(
            schema=self.schema(),
            safe_cast=ctx.safe_cast,
            keep_extra_columns=ctx.provenance,
            on_error="unsafe" if ctx.safe_cast else "raise",
        )
        return FinalizeContext(contract=self.contract(), transform=transform)
```

### Target files
- `src/schema_spec/core.py`
- `src/schema_spec/contracts.py`
- `src/schema_spec/factories.py`
- `src/schema_spec/registry.py`
- `src/arrowdsl/queryspec.py`
- `src/arrowdsl/scan_context.py`
- `src/arrowdsl/finalize_context.py`
- `src/arrowdsl/runner.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_nodes.py`
- `src/cpg/build_props.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/relspec/compiler.py`

### Integration checklist
- [x] Introduce DatasetSpec and migrate existing TableSchemaSpec/ContractSpec creators.
- [x] Update factory helpers to return DatasetSpec (or a bundle of specs).
- [x] Route scan + finalize context creation through DatasetSpec methods (runner, cpg/build_*, outputs, relspec/compiler).
- [x] Keep ExecutionContext the single runtime knob for behavior.

---

### Scope 2: Registry Consolidation into DatasetSpec

### Description
Replace separate table/contract registration with DatasetSpec registration and retrieval,
ensuring a unified catalog for all dataset behavior.

### Code patterns
```python
# src/schema_spec/registry.py
@dataclass(frozen=True)
class SchemaRegistry:
    dataset_specs: dict[str, DatasetSpec] = field(default_factory=dict)

    def register_dataset(self, spec: DatasetSpec) -> DatasetSpec:
        existing = self.dataset_specs.get(spec.name)
        if existing is not None:
            return existing
        self.dataset_specs[spec.name] = spec
        return spec
```

### Target files
- `src/schema_spec/registry.py`
- `src/schema_spec/catalogs.py`
- `src/cpg/schemas.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/schema_spec/__init__.py`

### Integration checklist
- [x] Add DatasetSpec-aware registry methods.
- [x] Convert CPG and relationship specs to DatasetSpec entries.
- [x] Replace register_table/register_contract usage with register_dataset.

---

### Scope 3: Unify Expression + Predicate + Column Ops into ExprSpec

### Description
Collapse separate predicate and column-expression layers into one ExprSpec model that can emit
plan-lane expressions and kernel-lane materializations from the same object.

### Code patterns
```python
# src/arrowdsl/expr.py
class ExprSpec(Protocol):
    def to_expression(self) -> ComputeExpression: ...
    def materialize(self, table: TableLike) -> ArrayLike: ...
    def is_scalar(self) -> bool: ...

@dataclass(frozen=True)
class EqExpr(ExprSpec):
    col: str
    value: ScalarValue

    def to_expression(self) -> ComputeExpression:
        return ensure_expression(pc.equal(pc.field(self.col), pc.scalar(self.value)))

    def materialize(self, table: TableLike) -> ArrayLike:
        return pc.equal(table[self.col], pc.scalar(self.value))

    def is_scalar(self) -> bool:
        return True
```

### Target files
- `src/arrowdsl/expr.py`
- `src/arrowdsl/predicates.py`
- `src/arrowdsl/column_ops.py`
- `src/arrowdsl/queryspec.py`
- `src/arrowdsl/dataset_io.py`

### Integration checklist
- [x] Replace PredicateSpec and ColumnExpr usage with ExprSpec objects.
- [x] Ensure QuerySpec stores ExprSpec for predicate and derived projection.
- [x] Remove duplicate pc calls in nodes and use ExprSpec helpers everywhere.

---

### Scope 4: Plan/Kernel Lane Unification with Pipeline-Breaker Metadata

### Description
Make plan operations and kernel operations explicit about pipeline-breaker semantics and
streaming safety, per Acero guidance. The Plan object should expose streaming-safe execution
only when no pipeline breakers are present.

### Code patterns
```python
# src/arrowdsl/plan.py
@dataclass(frozen=True)
class PlanSpec:
    plan: Plan
    pipeline_breakers: tuple[str, ...] = ()

    def to_reader(self, ctx: ExecutionContext) -> RecordBatchReaderLike:
        if self.pipeline_breakers:
            msg = f"Plan contains pipeline breakers: {self.pipeline_breakers}"
            raise ValueError(msg)
        return self.plan.to_reader(ctx=ctx)
```

### Target files
- `src/arrowdsl/plan_ops.py`
- `src/arrowdsl/plan.py`
- `src/arrowdsl/ops.py`
- `src/arrowdsl/runner.py`

### Integration checklist
- [x] Add pipeline-breaker metadata to all plan ops (order_by/aggregate).
- [x] Provide PlanSpec wrapper that enforces streaming-safe behavior.
- [x] Align Plan execution surfaces with ExecutionContext threading policy.

---

### Scope 5: Schema Metadata + Evolution Consolidation

### Description
Normalize all schema edits through SchemaMetadataSpec + SchemaEvolutionSpec, and keep
schema versioning attached as metadata (not as columns).

### Code patterns
```python
# src/schema_spec/core.py
def to_arrow_schema(self) -> SchemaLike:
    schema = pa.schema([field.to_arrow_field() for field in self.fields])
    if self.version is None:
        return schema
    meta = schema_metadata(self.name, self.version)
    return SchemaMetadataSpec(schema_metadata=meta).apply(schema)
```

### Target files
- `src/schema_spec/core.py`
- `src/schema_spec/metadata.py`
- `src/arrowdsl/schema_ops.py`
- `src/arrowdsl/schema.py`
- `src/arrowdsl/finalize.py`

### Integration checklist
- [x] Route all schema metadata edits through SchemaMetadataSpec.
- [x] Centralize schema evolution through SchemaEvolutionSpec (apply evolution_spec in alignment/finalize).
- [x] Keep schema versioning metadata consistent across datasets.

---

### Scope 6: Pandera Validation as Spec Policy

### Description
Move validation policy to DatasetSpec/ContractSpec so finalize is fully spec-driven and
consistent across callers.

### Code patterns
```python
# src/schema_spec/contracts.py
class ContractSpec(BaseModel):
    ...
    validation: PanderaValidationOptions | None = None

    def to_contract(self) -> Contract:
        contract = Contract(...)
        contract.validation = self.validation
        return contract
```

### Target files
- `src/schema_spec/contracts.py`
- `src/schema_spec/pandera_adapter.py`
- `src/arrowdsl/finalize.py`

### Integration checklist
- [x] Add validation policy to ContractSpec/DatasetSpec.
- [x] Update finalize to use spec-level validation policy consistently.
- [x] Ensure nested types default to safe validation handling.

---

### Scope 7: Nested Type Builders as Spec-Driven API

### Description
Use schema-defined nested type specs to drive list/struct/map/union construction
via arrowdsl/nested. Avoid row-wise Python list materialization when Arrow buffers
can be built directly.

### Code patterns
```python
# src/schema_spec/fields.py
@dataclass(frozen=True)
class NestedFieldSpec:
    name: str
    dtype: DataTypeLike
    builder: Callable[..., ArrayLike]

# src/arrowdsl/finalize.py
def _build_error_detail_list(errors: TableLike) -> ArrayLike:
    detail = build_struct_array({"code": errors["error_code"], "message": errors["error_message"]})
    offsets = pa.array(range(errors.num_rows + 1), type=pa.int32())
    return build_list_array(offsets, detail)

ERROR_DETAIL_SPEC = NestedFieldSpec(
    name="error_detail",
    dtype=pa.list_(ERROR_DETAIL_STRUCT),
    builder=_build_error_detail_list,
)
```

### Target files
- `src/arrowdsl/nested.py`
- `src/arrowdsl/nested_ops.py`
- `src/schema_spec/fields.py`
- `src/arrowdsl/finalize.py`
- `src/normalize/diagnostics.py`
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/symtable_extract.py`

### Integration checklist
- [x] Add nested field specs for list/struct (diagnostics + finalize).
- [x] Extend nested field specs for list-valued columns in CST/SCIP/Symtable extraction.
- [x] Replace ad hoc nested construction with nested builders outside diagnostics/finalize.
- [ ] Extend nested field specs for map/union if those column types are introduced.
- [x] Keep list_view and map offsets support for advanced usage.

---

### Scope 8: ExecutionContext as the Single Runtime Override

### Description
Ensure all variable deployment demands (determinism, provenance, threading,
strictness) flow through ExecutionContext and are applied consistently across
scan, plan, kernel, and finalize.

### Code patterns
```python
# src/arrowdsl/runtime.py
def with_execution_profile(self, profile: ExecutionProfile) -> ExecutionContext:
    runtime = profile.apply(self.runtime)
    return ExecutionContext(
        runtime=runtime,
        mode=self.mode,
        provenance=self.provenance,
        safe_cast=self.safe_cast,
        debug=self.debug,
        schema_validation=self.schema_validation,
    )
```

### Target files
- `src/arrowdsl/runtime.py`
- `src/arrowdsl/dataset_io.py`
- `src/arrowdsl/plan_ops.py`
- `src/arrowdsl/finalize.py`

### Integration checklist
- [x] Centralize runtime overrides in ExecutionContext only.
- [x] Use ExecutionContext for scan, plan, and finalize threading decisions.
- [x] Keep determinism tier handling consistent for canonical sort.

---

## Global Guardrails

- All plan-lane operations are declared via Acero declarations, no ad hoc `pc` usage in nodes.
- Kernel-lane transforms handle row-count changing logic and finalize only.
- Streaming surfaces (`to_reader`) are only used when no pipeline breakers are present.

---

## Further Application Opportunities (Implemented)

### 1) Relspec UNION_ALL evolution-aware unions
**Why**: `_compile_union_all` currently uses `pa.concat_tables`, which ignores the
`SchemaEvolutionSpec` path used elsewhere and can drift when inputs vary.

**Pattern**
```python
dataset_spec = registry.dataset_specs.get(rule.output_dataset)
unioned = (
    dataset_spec.unify_tables(parts, ctx=ctx)
    if dataset_spec is not None
    else SchemaEvolutionSpec().unify_and_cast(parts, safe_cast=ctx.safe_cast)
)
```

**Target files**
- `src/relspec/compiler.py` (RelationshipRuleCompiler._compile_union_all)
- `src/relspec/model.py` (optional: carry output dataset name on rules explicitly)

**Status**
- [x] Implemented in `src/relspec/compiler.py` using DatasetSpec when available.

---

### 2) Schema inference unification via SchemaEvolutionSpec
**Why**: `normalize/schema_infer.py` duplicates schema-unification logic that now exists
in `SchemaEvolutionSpec`. Consolidating reduces divergence and keeps align/unify logic
consistent across the pipeline.

**Pattern**
```python
evolution = SchemaEvolutionSpec(promote_options=opts.promote_options)
schema = evolution.unify_schema(tables)
aligned = evolution.unify_and_cast(tables, safe_cast=opts.safe_cast)
```

**Target files**
- `src/normalize/schema_infer.py`
- `src/arrowdsl/schema_ops.py` (optional: expose helpers for inference-mode alignment)

**Status**
- [x] Implemented via `SchemaEvolutionSpec.unify_schema_from_schemas` and updated inference flow.

---

### 3) DatasetLocation carrying DatasetSpec (not just TableSchemaSpec)
**Why**: DatasetSpec already packages query, evolution, metadata, and validation policy.
Embedding it into dataset registries removes ad hoc query/scan logic in resolvers.

**Pattern**
```python
@dataclass(frozen=True)
class DatasetLocation:
    dataset_spec: DatasetSpec | None = None

# resolver
spec = loc.dataset_spec or make_dataset_spec(table_spec=loc.table_spec)
decl = spec.scan_context(dataset, ctx).acero_decl()
```

**Target files**
- `src/relspec/registry.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/relspec/compiler.py`

**Status**
- [x] Implemented with `DatasetLocation.dataset_spec` and resolver wiring.
