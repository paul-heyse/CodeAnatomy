## ArrowDSL Unification Plan

### Scope 1: Canonical Plan Runner + Finalize Surface

### Description
Unify plan execution, finalize semantics, and determinism handling currently split
across extract/normalize/cpg into a single ArrowDSL plan runner. This consolidates
materialization rules, metadata application, and canonical ordering behavior.

### Code patterns
```python
# src/arrowdsl/plan/runner.py

@dataclass(frozen=True)
class PlanRunResult:
    value: TableLike | RecordBatchReaderLike
    kind: Literal["table", "reader"]


def run_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
    metadata_spec: SchemaMetadataSpec | None = None,
) -> PlanRunResult:
    spec = PlanSpec.from_plan(plan)
    if prefer_reader and not spec.pipeline_breakers and ctx.determinism != DeterminismTier.CANONICAL:
        reader = spec.to_reader(ctx=ctx)
        return PlanRunResult(value=_apply_metadata(reader, metadata_spec), kind="reader")
    table = spec.to_table(ctx=ctx)
    table = _apply_canonical_sort(table, ctx=ctx)
    return PlanRunResult(value=_apply_metadata(table, metadata_spec), kind="table")
```

### Target files
- `src/arrowdsl/plan/runner.py` (new)
- `src/extract/tables.py`
- `src/normalize/plan_helpers.py`
- `src/cpg/plan_helpers.py`

### Implementation checklist
- [x] Introduce `PlanRunResult` and `run_plan` with metadata-aware finalize logic.
- [ ] Migrate `finalize_plan`/`materialize_plan`/`stream_plan` calls to `run_plan`.
- [ ] Remove duplicated finalize helpers from extract/normalize/cpg once migrated.

---

### Scope 2: Canonical Plan Source + Dataset Scan Helpers

### Description
Move dataset scan/plan-source logic (Acero scan, dataset ordering semantics, scanner conversion)
into ArrowDSL so extract/normalize/cpg use a single scan surface.

### Code patterns
```python
# src/arrowdsl/plan/source.py

PlanSource = TableLike | RecordBatchReaderLike | ds.Dataset | ds.Scanner | Plan


def plan_from_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    columns: Sequence[str] | None = None,
    label: str = "",
) -> Plan:
    if isinstance(source, Plan):
        return source
    if isinstance(source, ds.Dataset):
        scan = ScanOp(dataset=source, columns=list(columns or source.schema.names))
        decl = scan.to_declaration([], ctx=ctx)
        return Plan(decl=decl, label=label, ordering=scan.apply_ordering(Ordering.unordered()))
    if isinstance(source, ds.Scanner):
        return Plan.table_source(source.to_table(), label=label)
    if isinstance(source, RecordBatchReaderLike):
        return Plan.table_source(source.read_all(), label=label)
    return Plan.table_source(source, label=label)
```

### Target files
- `src/arrowdsl/plan/source.py` (new)
- `src/arrowdsl/plan_helpers.py`
- `src/cpg/plan_helpers.py`
- `src/cpg/sources.py`

### Implementation checklist
- [x] Add `plan_from_source` and `PlanSource` to ArrowDSL.
- [x] Replace `ensure_plan`/`plan_source`/`plan_from_dataset` usage with the canonical helper.
- [x] Remove duplicated scan logic in CPG plan helpers once migrated.

---

### Scope 3: Unify ExprSpec Surfaces (extract/normalize/cpg)

### Description
Consolidate expression spec classes into ArrowDSL so all plan-lane expression helpers live in
one place. This reduces duplicated Hash/MaskedHash/Coalesce/Trim logic across modules.

### Code patterns
```python
# src/arrowdsl/compute/expr_specs.py

@dataclass(frozen=True)
class HashExprSpec:
    spec: HashSpec
    def to_expression(self) -> ComputeExpression:
        return hash_expression(self.spec)
    def materialize(self, table: TableLike) -> ArrayLike:
        return hash_column_values(table, spec=self.spec)
    def is_scalar(self) -> bool:
        return self is not None


@dataclass(frozen=True)
class MaskedHashExprSpec:
    spec: HashSpec
    required: Sequence[str]
    def to_expression(self) -> ComputeExpression:
        return masked_hash_expression(self.spec, required=tuple(self.required))
```

### Target files
- `src/arrowdsl/compute/expr_specs.py` (new)
- `src/extract/plan_exprs.py`
- `src/normalize/plan_exprs.py`
- `src/cpg/plan_exprs.py`

### Implementation checklist
- [x] Introduce canonical ExprSpec helpers in ArrowDSL (hash, masked hash, trim, coalesce).
- [x] Update extract/normalize/cpg to import from ArrowDSL instead of local modules.
- [x] Delete redundant plan_exprs modules once consumers are migrated.

---

### Scope 4: Canonical Predicate + Validation Helpers

### Description
Move repeated predicate logic (zero/empty checks, invalid id tests, bitmask flags,
trimmed-non-empty checks) into ArrowDSL compute predicates.

### Code patterns
```python
# src/arrowdsl/compute/predicates.py

def null_if_empty_or_zero(expr: ComputeExpression) -> ComputeExpression:
    empty = ensure_expression(pc.equal(expr, pc.scalar("")))
    zero = ensure_expression(pc.equal(expr, pc.scalar("0")))
    return ensure_expression(pc.if_else(pc.or_(empty, zero), pc.scalar(None), expr))


def bitmask_is_set_expr(values: ComputeExpression, *, mask: int) -> ComputeExpression:
    roles = pc.cast(values, pa.int64(), safe=False)
    hit = pc.not_equal(pc.bit_wise_and(roles, pa.scalar(mask)), pa.scalar(0))
    return ensure_expression(pc.fill_null(hit, fill_value=False))
```

### Target files
- `src/arrowdsl/compute/predicates.py`
- `src/cpg/plan_exprs.py`
- `src/cpg/quality.py`
- `src/normalize/arrow_utils.py`

### Implementation checklist
- [x] Add predicate helpers to ArrowDSL compute predicates.
- [ ] Replace local implementations in CPG/normalize with ArrowDSL helpers.
- [ ] Remove duplicated predicate helpers after migration.

---

### Scope 5: Canonical Column Helpers and Schema-Aligned Table Builders

### Description
Unify typed-null column selection and schema-driven table creation into ArrowDSL.
This replaces `column_or_null`, `_pick_first`, and schema-based `Table.from_arrays` boilerplate.

### Code patterns
```python
# src/arrowdsl/schema/columns.py

def column_or_null(table: TableLike, col: str, dtype: DataTypeLike) -> ArrayLike:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=dtype)


def table_from_schema(
    schema: SchemaLike,
    *,
    columns: Mapping[str, ArrayLike],
    num_rows: int,
) -> TableLike:
    arrays = [columns.get(f.name, pa.nulls(num_rows, type=f.type)) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)
```

### Target files
- `src/arrowdsl/schema/columns.py` (new)
- `src/normalize/arrow_utils.py`
- `src/cpg/builders.py`
- `src/cpg/schemas.py`
- `src/cpg/quality.py`

### Implementation checklist
- [x] Add canonical column/table helpers under ArrowDSL schema utilities.
- [x] Replace `column_or_null` and table-builder boilerplate usages.
- [x] Remove redundant helpers from normalize/cpg once migrated.

---

### Scope 6: Schema Metadata + Ordering Utilities

### Description
Consolidate schema metadata helpers (options hash, ordering metadata, merge specs, ordering
key inference) into ArrowDSL to keep ordering/determinism policy centralized.

### Code patterns
```python
# src/arrowdsl/schema/metadata.py

def ordering_metadata_spec(
    level: OrderingLevel,
    *,
    keys: Sequence[OrderingKey] = (),
    extra: dict[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    meta = {b"ordering_level": level.value.encode("utf-8")}
    if keys:
        key_text = ",".join(f"{col}:{order}" for col, order in keys)
        meta[b"ordering_keys"] = key_text.encode("utf-8")
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)
```

### Target files
- `src/arrowdsl/schema/metadata.py` (new)
- `src/extract/spec_helpers.py`
- `src/extract/tables.py`
- `src/normalize/schema_infer.py`

### Implementation checklist
- [x] Move ordering/metadata helpers into ArrowDSL schema metadata module.
- [x] Update extract/normalize to import from ArrowDSL.
- [x] Remove duplicated metadata helpers from extract once migrated.

---

### Scope 7: Schema Unification + Metadata Preservation

### Description
Unify schema unification and metadata preservation logic across extract/normalize/cpg so
schema evolution is handled consistently in ArrowDSL.

### Code patterns
```python
# src/arrowdsl/schema/unify.py

def unify_schemas(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
) -> SchemaLike:
    if not schemas:
        return pa.schema([])
    evolution = SchemaEvolutionSpec(promote_options=promote_options)
    unified = evolution.unify_schema_from_schemas(schemas)
    return _metadata_spec_from_schema(schemas[0]).apply(unified)
```

### Target files
- `src/arrowdsl/schema/unify.py` (new)
- `src/extract/tables.py`
- `src/normalize/schema_infer.py`
- `src/cpg/plan_helpers.py`

### Implementation checklist
- [x] Introduce `unify_schemas` and metadata-aware helpers in ArrowDSL.
- [x] Replace local schema unify helpers in extract/normalize/cpg.
- [x] Delete redundant schema unification code after migration.

---

### Scope 8: Nested List/Struct Accumulators

### Description
Promote nested list/list_view/struct accumulators to ArrowDSL so extract and normalize
share canonical builders for nested arrays.

### Code patterns
```python
# src/arrowdsl/schema/nested.py

@dataclass
class LargeListViewAccumulator[T]:
    offsets: list[int] = field(default_factory=list)
    sizes: list[int] = field(default_factory=list)
    values: list[T | None] = field(default_factory=list)

    def build(self, *, value_type: DataTypeLike) -> ArrayLike:
        offsets = pa.array(self.offsets, type=pa.int64())
        sizes = pa.array(self.sizes, type=pa.int64())
        values = pa.array(self.values, type=value_type)
        return build_list_view(offsets, sizes, values, list_type=pa.large_list_view(value_type))
```

### Target files
- `src/arrowdsl/schema/nested.py` (new)
- `src/extract/nested_lists.py`
- `src/normalize/diagnostics.py`
- `src/extract/scip_extract.py`

### Implementation checklist
- [x] Move list/list_view accumulators to ArrowDSL schema utilities.
- [ ] Update extract/normalize consumers to import from ArrowDSL.
- [x] Remove `extract/nested_lists.py` once fully migrated.

---

### Scope 9: Encoding + Dictionary Utilities

### Description
Standardize dictionary-encoding helpers for both plan and table flows. Consolidate
encoding projection and metadata-driven encoding across extract and cpg.

### Code patterns
```python
# src/arrowdsl/plan_helpers.py

def encode_plan(plan: Plan, *, columns: Sequence[str], ctx: ExecutionContext) -> Plan:
    exprs, names = encoding_projection(columns, available=plan.schema(ctx=ctx).names)
    return plan.project(exprs, names, ctx=ctx)


def encode_table(table: TableLike, *, columns: Sequence[str]) -> TableLike:
    specs = tuple(EncodingSpec(column=col) for col in columns)
    return encode_columns(table, specs=specs)
```

### Target files
- `src/arrowdsl/plan_helpers.py`
- `src/extract/postprocess.py`
- `src/cpg/plan_helpers.py`

### Implementation checklist
- [x] Add `encode_plan` and `encode_table` helpers to ArrowDSL.
- [ ] Replace encoding helpers in extract/cpg with ArrowDSL versions.
- [ ] Remove encoding-only wrappers in extract once migrated.

---

### Scope 10: Join Helpers for Plan and Table Lanes

### Description
Consolidate join utilities (plan-lane joins and kernel-lane joins) into ArrowDSL so
join configuration and output shaping are consistently applied.

### Code patterns
```python
# src/arrowdsl/plan/joins.py

@dataclass(frozen=True)
class JoinConfig:
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
    output_suffix_for_right: str = ""


def left_join(
    left: TableLike | Plan,
    right: TableLike | Plan,
    *,
    config: JoinConfig,
    ctx: ExecutionContext | None = None,
) -> TableLike | Plan:
    spec = JoinSpec(...)
    if isinstance(left, Plan) or isinstance(right, Plan):
        return Plan.table_source(left).join(Plan.table_source(right), spec=spec, ctx=ctx)
    return apply_join(left, right, spec=spec, use_threads=True)
```

### Target files
- `src/arrowdsl/plan/joins.py` (new)
- `src/extract/join_helpers.py`
- `src/normalize/arrow_utils.py`

### Implementation checklist
- [x] Move JoinConfig and left_join into ArrowDSL plan helpers.
- [x] Update extract/normalize to import join helpers from ArrowDSL.
- [x] Remove duplicated join helpers in extract/normalize.

---

### Scope 11: Property Transform + JSON Helper Consolidation

### Description
Standardize property transforms and JSON expression handling so CPG and other
pipelines can reuse ArrowDSL compute utilities for UDF-backed transforms.

### Code patterns
```python
# src/arrowdsl/compute/transforms.py

def expr_context_expr(expr: ComputeExpression) -> ComputeExpression:
    func_name = ensure_expr_context_udf()
    return ensure_expression(pc.call_function(func_name, [expr]))


def flag_to_bool_expr(expr: ComputeExpression) -> ComputeExpression:
    casted = pc.cast(expr, pa.int64(), safe=False)
    hit = pc.equal(casted, pa.scalar(1))
    return ensure_expression(pc.if_else(hit, pc.scalar(True), pc.scalar(None)))
```

### Target files
- `src/arrowdsl/compute/transforms.py` (new)
- `src/cpg/prop_transforms.py`
- `src/cpg/emit_props.py`

### Implementation checklist
- [x] Move transform helpers into ArrowDSL compute utilities.
- [x] Update CPG to import transforms from ArrowDSL.
- [x] Remove `cpg/prop_transforms.py` once fully migrated.

---

### Scope 12: ArrowDSL Public API and Exports

### Description
Expose canonical helpers in `arrowdsl.__init__` and trim legacy exports so consumers
use a single, well-defined API surface.

### Code patterns
```python
# src/arrowdsl/__init__.py
from arrowdsl.plan.runner import run_plan, PlanRunResult
from arrowdsl.plan.source import plan_from_source, PlanSource
from arrowdsl.compute.expr_specs import HashExprSpec, MaskedHashExprSpec, TrimExprSpec
from arrowdsl.schema.metadata import ordering_metadata_spec, options_metadata_spec
```

### Target files
- `src/arrowdsl/__init__.py`
- `src/arrowdsl/plan_helpers.py`
- `src/arrowdsl/schema/schema.py`

### Implementation checklist
- [ ] Export canonical helpers from ArrowDSL top-level.
- [ ] Remove exports for deprecated helpers once migration is complete.
- [ ] Audit imports in extract/normalize/cpg to ensure consistent ArrowDSL usage.
