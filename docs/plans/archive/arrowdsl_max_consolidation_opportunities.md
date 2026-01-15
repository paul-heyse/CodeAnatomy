# ArrowDSL Max Consolidation Opportunities (Internal Only)

## Context
This document enumerates **every consolidation opportunity** inside `src/arrowdsl` that
leverages the capabilities described in:

- `docs/python_library_reference/pyarrow-advanced.md`
- `docs/python_library_reference/arrow_acero_dsl_guide.md`

Constraints:
- **No external changes** outside `src/arrowdsl`.
- **Public entry points remain stable** (for `Plan`, `QuerySpec`, `ExprIR`, `plan_helpers`,
  and `run_plan`).
- Consolidation is internal only: no new compatibility layers, no adapter surfaces.

Each scope item includes:
- Code patterns and functions to deploy.
- Target file list.
- An implementation checklist.

---

## Scope 1: Canonical Scan Builder (QuerySpec as the single scan source)

### Pattern and Functions to Deploy
Use a single builder that creates both Scanner and Acero scan declarations from
`QuerySpec` to eliminate duplicated scan logic.

```python
# src/arrowdsl/plan/scan_builder.py
@dataclass(frozen=True)
class ScanBuildSpec:
    dataset: ds.Dataset
    query: QuerySpec
    ctx: ExecutionContext

    def scan_columns(self) -> ColumnsSpec:
        return self.query.scan_columns(
            provenance=self.ctx.provenance,
            scan_provenance=self.ctx.runtime.scan.scan_provenance_columns,
        )

    def scanner(self) -> ds.Scanner:
        return self.dataset.scanner(
            columns=self.scan_columns(),
            filter=self.query.pushdown_expression(),
            **self.ctx.runtime.scan.scanner_kwargs(),
        )

    def acero_decl(self) -> DeclarationLike:
        builder = PlanBuilder()
        builder.scan(
            dataset=self.dataset,
            columns=self.scan_columns(),
            predicate=self.query.pushdown_expression(),
            ordering_effect=scan_ordering_effect(self.ctx),
        )
        if (predicate := self.query.predicate_expression()) is not None:
            builder.filter(predicate=predicate)
        plan_ir, _, _ = builder.build()
        return PlanCompiler(catalog=OP_CATALOG).to_acero(plan_ir, ctx=self.ctx)
```

### Target Files
- `src/arrowdsl/plan/query.py`
- `src/arrowdsl/plan/scan_io.py`
- `src/arrowdsl/plan/scan_builder.py` (new)

### Implementation Checklist
- [ ] Route all scan creation through `ScanBuildSpec` (Scanner + Acero).
- [ ] Remove duplicated scan creation in `ScanContext.scanner` and `compile_to_acero_scan`.
- [ ] Preserve `QuerySpec` API and scan semantics.

---

## Scope 2: Dataset Source Normalization via ds.dataset

### Pattern and Functions to Deploy
Normalize all input sources using `pyarrow.dataset.dataset(...)`, including
`RecordBatchReader` and iterables (InMemoryDataset behavior), so plan building is uniform.

```python
# src/arrowdsl/plan/source_normalize.py
def normalize_dataset_source(
    source: DatasetSourceLike,
    *,
    dataset_format: str,
    filesystem: pafs.FileSystem | None,
    partitioning: str | None,
    schema: SchemaLike | None,
) -> ds.Dataset:
    return ds.dataset(
        source,
        format=dataset_format,
        filesystem=filesystem,
        partitioning=partitioning,
        schema=schema,
    )
```

### Target Files
- `src/arrowdsl/plan/query.py`
- `src/arrowdsl/plan/scan_io.py`

### Implementation Checklist
- [ ] Replace custom dataset factory branches with `ds.dataset(...)`.
- [ ] Remove one-off `RowSource`/`InMemoryDatasetSource` handling when ds.dataset
      provides the same semantics.
- [ ] Keep PlanSource API stable.

---

## Scope 3: Unified Scan Telemetry and Provenance

### Pattern and Functions to Deploy
Centralize telemetry using `Dataset.get_fragments(filter=...)` and
`Scanner.scan_batches()` (TaggedRecordBatch). This replaces duplicate telemetry
code paths.

```python
# src/arrowdsl/plan/scan_telemetry.py
@dataclass(frozen=True)
class ScanTelemetry:
    fragment_count: int
    row_group_count: int
    count_rows: int | None
    estimated_rows: int | None
    file_hints: tuple[str, ...]


def scan_telemetry(dataset: ds.Dataset, *, predicate: ComputeExpression | None) -> ScanTelemetry:
    fragments = list(dataset.get_fragments(filter=predicate))
    count_rows = dataset.count_rows(filter=predicate)
    estimated_rows = _estimate_rows(fragments)
    return ScanTelemetry(
        fragment_count=len(fragments),
        row_group_count=_row_group_count(fragments),
        count_rows=int(count_rows) if count_rows is not None else None,
        estimated_rows=estimated_rows,
        file_hints=_fragment_file_hints(fragments),
    )
```

### Target Files
- `src/arrowdsl/plan/metrics.py`
- `src/arrowdsl/plan/query.py`
- `src/arrowdsl/plan/scan_io.py`

### Implementation Checklist
- [ ] Consolidate fragment counting and row-group estimation into one helper.
- [ ] Use `scan_batches()` for provenance when requested (no duplicate paths).
- [ ] Keep the external telemetry table API unchanged.

---

## Scope 4: Acero Compilation Handler Registry

### Pattern and Functions to Deploy
Replace the large conditional in `PlanCompiler` with a handler registry keyed
by op name to eliminate duplication and improve extensibility.

```python
# src/arrowdsl/compile/plan_compiler.py
Handler = Callable[[OpNode, DeclarationLike | None], DeclarationLike]

_ACERO_HANDLERS: dict[str, Handler] = {
    "scan": lambda node, _: _scan_decl(node, ctx=ctx),
    "table_source": lambda node, _: _table_source_decl(node),
    "filter": lambda node, inp: _filter_decl(node, input_decl=_require(inp)),
    "project": lambda node, inp: _project_decl(node, input_decl=_require(inp)),
    "order_by": lambda node, inp: _order_by_decl(node, input_decl=_require(inp)),
    "aggregate": lambda node, inp: _aggregate_decl(node, input_decl=_require(inp)),
    "hash_join": lambda node, inp: _hash_join_decl(
        node, input_decl=_require(inp), ctx=ctx, compile_ir=compile_ir
    ),
    "union_all": lambda node, inp: _union_all_decl(node, ctx=ctx, compile_ir=compile_ir),
    "winner_select": lambda node, inp: _winner_select_decl(node, input_decl=_require(inp)),
}
```

### Target Files
- `src/arrowdsl/compile/plan_compiler.py`

### Implementation Checklist
- [ ] Replace conditional chain with `_ACERO_HANDLERS`.
- [ ] Keep op-specific validation inside handlers.
- [ ] Preserve PlanCompiler API and errors.

---

## Scope 5: Shared FunctionOptions Serialization

### Pattern and Functions to Deploy
Centralize FunctionOptions serialization/deserialization so `ExprIR` and
`ExprCompiler` do not duplicate logic.

```python
# src/arrowdsl/compute/options.py
class FunctionOptionsProto(Protocol):
    def serialize(self) -> bytes: ...
    @classmethod
    def deserialize(cls, payload: bytes) -> FunctionOptionsProto: ...


def serialize_options(value: FunctionOptionsProto | bytes | bytearray | None) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, bytes):
        return value
    return value.serialize()


def deserialize_options(payload: bytes | None) -> FunctionOptionsProto | None:
    if payload is None:
        return None
    options_type = cast("type[FunctionOptionsProto] | None", getattr(pc, "FunctionOptions", None))
    if options_type is None:
        raise TypeError("Arrow compute FunctionOptions is unavailable.")
    return options_type.deserialize(payload)
```

### Target Files
- `src/arrowdsl/compute/options.py` (new)
- `src/arrowdsl/spec/expr_ir.py`
- `src/arrowdsl/compile/expr_compiler.py`
- `src/arrowdsl/ir/expr.py`

### Implementation Checklist
- [ ] Replace all local options serialization logic with shared helpers.
- [ ] Keep error types and messages stable.
- [ ] Preserve existing ExprIR JSON encoding.

---

## Scope 6: Expression Macro Consolidation

### Pattern and Functions to Deploy
Use `ColumnOrNullExpr` and `CoalesceExpr` for plan and kernel lanes. Remove
duplicated helper logic in `plan_helpers`.

```python
# src/arrowdsl/compute/macros.py
expr = ColumnOrNullExpr(name=field.name, dtype=field.type, available=available)
return expr.to_expression()

# src/arrowdsl/plan_helpers.py
# Replace custom coalesce_expr with CoalesceExpr
exprs = tuple(FieldExpr(name=col) for col in cols if col in available)
return CoalesceExpr(exprs=exprs).to_expression()
```

### Target Files
- `src/arrowdsl/compute/macros.py`
- `src/arrowdsl/plan_helpers.py`
- `src/arrowdsl/schema/build.py`

### Implementation Checklist
- [ ] Remove custom `column_or_null_expr` and `coalesce_expr` from plan_helpers.
- [ ] Reuse `ColumnOrNullExpr` and `CoalesceExpr` everywhere.
- [ ] Keep public `plan_helpers` exports stable via re-exports.

---

## Scope 7: Predicate and Mask Utilities Unification

### Pattern and Functions to Deploy
Consolidate predicate spec builders and mask helpers into one module so the
same logic is used across plan and kernel lanes.

```python
# src/arrowdsl/compute/predicates.py
@dataclass(frozen=True)
class PredicateSpec:
    expr: ExprSpec

    def to_expression(self) -> ComputeExpression:
        return self.expr.to_expression()

    def materialize(self, table: TableLike) -> ArrayLike:
        return self.expr.materialize(table)


def valid_mask(values: Sequence[ArrayLike | ChunkedArrayLike]) -> ArrayLike | ChunkedArrayLike:
    mask = pc.is_valid(values[0])
    for value in values[1:]:
        mask = pc.and_(mask, pc.is_valid(value))
    return mask
```

### Target Files
- `src/arrowdsl/compute/filters.py`
- `src/arrowdsl/compute/macros.py`
- `src/arrowdsl/compute/ids.py`

### Implementation Checklist
- [ ] Merge `valid_mask_array`, `valid_mask_for_columns`, `valid_mask_expr` into a
      shared utility module.
- [ ] Keep `FilterSpec` and `ExprSpec` interfaces unchanged.
- [ ] Remove duplicated predicate spec constructors.

---

## Scope 8: Join Spec and Output Resolution Single Source

### Pattern and Functions to Deploy
Use `JoinSpec` only, with shared output resolution for plan and kernel lanes.
Leverage `Table.join(filter_expression=...)` to avoid post-join filters.

```python
# src/arrowdsl/plan/joins.py
spec = JoinSpec(
    join_type="left outer",
    left_keys=tuple(left_keys),
    right_keys=tuple(right_keys),
    left_output=tuple(left_output),
    right_output=tuple(right_output),
    output_suffix_for_left=left_suffix,
    output_suffix_for_right=right_suffix,
)

# src/arrowdsl/compute/kernels.py
joined = left.join(
    right,
    keys=list(spec.left_keys),
    right_keys=list(spec.right_keys),
    join_type=spec.join_type,
    left_suffix=spec.output_suffix_for_left,
    right_suffix=spec.output_suffix_for_right,
    filter_expression=residual_filter,
)
return _select_join_outputs(joined, spec=spec)
```

### Target Files
- `src/arrowdsl/plan/joins.py`
- `src/arrowdsl/plan/join_compiler.py`
- `src/arrowdsl/compute/kernels.py`

### Implementation Checklist
- [ ] Collapse `JoinConfig` and output selection into `JoinSpec` + `resolve_join_outputs`.
- [ ] Use `Table.join(filter_expression=...)` when post-join predicates exist.
- [ ] Keep public join helpers (`left_join`, `join_plan`) stable.

---

## Scope 9: As-Of Join Consolidation for Interval Matching

### Pattern and Functions to Deploy
Where interval matching is "nearest by key", use `Table.join_asof` instead of
custom interval candidate joins.

```python
# src/arrowdsl/compute/kernels.py
out = left.join_asof(
    right,
    on=spec.on,
    by=spec.by,
    tolerance=spec.tolerance,
    right_on=spec.right_on,
    right_by=spec.right_by,
)
```

### Target Files
- `src/arrowdsl/compute/kernels.py`
- `src/arrowdsl/plan/joins.py`
- `src/arrowdsl/plan/ops.py`

### Implementation Checklist
- [ ] Add a dedicated `JoinSpec` variant (or options struct) for as-of joins.
- [ ] Replace interval candidate logic where semantics match join_asof.
- [ ] Keep existing join helpers stable and feature-gated.

---

## Scope 10: Dedupe and Winner-Select Consolidation

### Pattern and Functions to Deploy
Express winner selection as a unified strategy: order_by + aggregate in Acero,
`Table.group_by().aggregate` in kernel lane, with explicit ordering policy
from `OrderingEffect`.

```python
# plan lane (Acero)
order_decl = acero.Declaration(
    "order_by",
    acero.OrderByNodeOptions(sort_keys=list(sort_keys)),
    inputs=[input_decl],
)
agg_decl = acero.Declaration(
    "aggregate",
    acero.AggregateNodeOptions(agg_specs, keys=list(spec.keys)),
    inputs=[order_decl],
)

# kernel lane
sorted_table = table.sort_by(sort_keys)
agg = sorted_table.group_by(list(spec.keys)).aggregate(aggs)
```

### Target Files
- `src/arrowdsl/plan/ops.py`
- `src/arrowdsl/compile/plan_compiler.py`
- `src/arrowdsl/compute/kernels.py`
- `src/arrowdsl/kernel/specs.py`

### Implementation Checklist
- [ ] Use one `DedupeSpec` strategy table across plan and kernel lanes.
- [ ] Ensure pipeline breaker flags are applied via OP_CATALOG only.
- [ ] Preserve existing dedupe public APIs.

---

## Scope 11: Schema Unification and Evolution

### Pattern and Functions to Deploy
Use `pa.unify_schemas(..., promote_options=...)` as the core implementation,
wrap it once to preserve metadata, and reuse everywhere.

```python
# src/arrowdsl/schema/ops.py
def unify_schemas(
    schemas: Sequence[SchemaLike],
    *,
    promote_options: str = "permissive",
) -> SchemaLike:
    if not schemas:
        return pa.schema([])
    unified = pa.unify_schemas(list(schemas), promote_options=promote_options)
    return metadata_spec_from_schema(schemas[0]).apply(unified)
```

### Target Files
- `src/arrowdsl/schema/ops.py`
- `src/arrowdsl/schema/schema.py`

### Implementation Checklist
- [ ] Remove duplicate unify logic between `SchemaEvolutionSpec` and ops helpers.
- [ ] Keep metadata preservation in one place.
- [ ] Preserve behavior for nested types with `prefer_nested` policy.

---

## Scope 12: Nested Array Builders Using Arrow Constructors

### Pattern and Functions to Deploy
Replace custom offset/size handling with Arrow constructors:
- `pa.array(..., type=pa.list_view(...))`
- `pa.ListViewArray.from_arrays(...)`
- `pa.MapArray.from_arrays(...)`
- `pa.UnionArray.from_dense/from_sparse(...)`

```python
# src/arrowdsl/schema/nested_builders.py
values = [1, 2, 3, 4]
offsets = [0, 2, 4]
sizes = [2, 2]
arr = pa.ListViewArray.from_arrays(offsets, sizes, values)
```

### Target Files
- `src/arrowdsl/schema/nested_builders.py`
- `src/arrowdsl/schema/build.py`
- `src/arrowdsl/schema/types.py`

### Implementation Checklist
- [ ] Replace manual list_view and union array assembly with Arrow factories.
- [ ] Use `pa.array(..., type=...)` for inferred struct/list/map cases.
- [ ] Preserve existing builder function signatures.

---

## Scope 13: Dictionary Encoding and Chunk Normalization

### Pattern and Functions to Deploy
Unify dictionary encoding and chunk handling into a single normalization pass.

```python
# src/arrowdsl/schema/normalize.py
@dataclass(frozen=True)
class NormalizePolicy:
    encoding: EncodingPolicy
    chunk: ChunkPolicy = field(default_factory=ChunkPolicy)

    def apply(self, table: TableLike) -> TableLike:
        encoded = apply_encoding(table, policy=self.encoding)
        return self.chunk.apply(encoded)
```

### Target Files
- `src/arrowdsl/compute/kernels.py`
- `src/arrowdsl/schema/encoding_policy.py`
- `src/arrowdsl/schema/schema.py`

### Implementation Checklist
- [ ] Replace `ChunkPolicy` use sites with `NormalizePolicy`.
- [ ] Centralize dictionary unification and chunk combining.
- [ ] Keep per-table encoding policies stable.

---

## Scope 14: Ordering Metadata and Pipeline Breakers

### Pattern and Functions to Deploy
Drive ordering decisions solely from `OP_CATALOG` and `OrderingEffect` and
apply metadata consistently in one place.

```python
# src/arrowdsl/plan/builder.py
op_def = OP_CATALOG[name]
self.ordering = _apply_ordering_effect(self.ordering, op_def.ordering_effect, ordering_keys)
if op_def.pipeline_breaker:
    self.pipeline_breakers.append(name)
```

### Target Files
- `src/arrowdsl/plan/builder.py`
- `src/arrowdsl/plan/ordering_policy.py`
- `src/arrowdsl/ops/catalog.py`

### Implementation Checklist
- [ ] Remove duplicate ordering rules from plan helpers and kernels.
- [ ] Ensure pipeline breaker metadata is attached once at plan build.
- [ ] Keep ordering metadata emission stable.

---

## Scope 15: Rows-to-Table and Rows-to-Reader Consolidation

### Pattern and Functions to Deploy
Expose one set of row helpers and reuse everywhere.

```python
# src/arrowdsl/schema/build.py
def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    if not rows:
        return empty_table(schema)
    return table_from_rows(schema, rows)

# src/arrowdsl/plan/scan_io.py
def reader_from_rows(...):
    batches = record_batches_from_rows(...)
    return pa.RecordBatchReader.from_batches(schema, batches)
```

### Target Files
- `src/arrowdsl/schema/build.py`
- `src/arrowdsl/plan/scan_io.py`
- `src/arrowdsl/spec/io.py`

### Implementation Checklist
- [ ] Replace duplicate `table_from_rows` wrappers in spec/io and scan_io.
- [ ] Keep `rows_to_table` in schema.build as the single implementation.
- [ ] Preserve existing signatures in external modules.

---

## Scope 16: Kernel Capability Registry Unification

### Pattern and Functions to Deploy
Use one kernel registry that includes lane, ordering requirements, and
implementation name to avoid duplication between `compute/kernels.py`
and `kernel/specs.py`.

```python
# src/arrowdsl/kernel/registry.py
@dataclass(frozen=True)
class KernelDef:
    name: str
    lane: KernelLane
    requires_ordering: bool
    impl_name: str

KERNEL_REGISTRY: dict[str, KernelDef] = {
    "dedupe": KernelDef(...),
    "winner_select": KernelDef(...),
}
```

### Target Files
- `src/arrowdsl/compute/kernels.py`
- `src/arrowdsl/kernel/specs.py`

### Implementation Checklist
- [ ] Move kernel metadata into one registry.
- [ ] Keep runtime lane selection in one place.
- [ ] Preserve kernel invocation API.

---

## Scope 17: plan_helpers as Thin Re-exports

### Pattern and Functions to Deploy
Minimize duplicated helper logic by re-exporting canonical implementations.

```python
# src/arrowdsl/plan_helpers.py
from arrowdsl.compute.ids import hash_projection
from arrowdsl.compute.macros import ColumnOrNullExpr
from arrowdsl.schema.ops import align_plan, encode_plan, encode_table
```

### Target Files
- `src/arrowdsl/plan_helpers.py`

### Implementation Checklist
- [ ] Remove duplicate helper implementations.
- [ ] Re-export canonical functions with stable names.
- [ ] Keep public API compatibility.

---

## Scope 18: Join Output Projection Reuse for Plan and Kernel Lanes

### Pattern and Functions to Deploy
Make join output projection resolve once and reuse in both plan and kernel
lanes, avoiding duplicated suffix logic.

```python
# src/arrowdsl/plan/join_compiler.py
plan = resolve_join_outputs(joined.column_names, spec=spec)
return joined.select(list(plan.output_names))
```

### Target Files
- `src/arrowdsl/plan/join_compiler.py`
- `src/arrowdsl/compute/kernels.py`

### Implementation Checklist
- [ ] Use `resolve_join_outputs` in all join execution paths.
- [ ] Remove local suffix handling duplicates.
- [ ] Keep join output defaults stable.

---

## Scope 19: Arrow IPC Read/Write Options Single Source

### Pattern and Functions to Deploy
Use one IPC read/write options factory and reuse everywhere.

```python
# src/arrowdsl/spec/io.py
options = ipc.IpcWriteOptions(
    metadata_version=ipc.MetadataVersion.V5,
    compression="zstd",
    unify_dictionaries=True,
    use_threads=True,
)
```

### Target Files
- `src/arrowdsl/spec/io.py`
- `src/arrowdsl/io/ipc.py`

### Implementation Checklist
- [ ] Use one read/write options factory and re-export it.
- [ ] Remove duplicated options configuration.
- [ ] Keep public functions unchanged.

---

## Scope 20: External Interface Stability Guardrails

### Pattern and Functions to Deploy
Add small, internal-only guardrails to assert that public entry points have
not changed while internal consolidation progresses.

```python
# src/arrowdsl/__init__.py
__all__ = [
    "Plan",
    "QuerySpec",
    "ExprIR",
    "run_plan",
    "plan_helpers",
]
```

### Target Files
- `src/arrowdsl/__init__.py`
- `src/arrowdsl/plan/__init__.py`

### Implementation Checklist
- [ ] Keep public module exports stable across consolidation steps.
- [ ] Avoid introducing new entry points during refactors.
- [ ] Confirm external imports remain valid.

---

## Suggested Sequencing
If we implement this in one pass, a safe and efficient order is:

1. Scan builder + dataset normalization (Scopes 1-2).
2. Telemetry + provenance consolidation (Scope 3).
3. Expression and options helpers (Scopes 5-7).
4. Join and dedupe consolidation (Scopes 8-10, 18).
5. Schema + nested builder consolidation (Scopes 11-12, 15).
6. Encoding/ordering/kernel registry consolidation (Scopes 13-16).
7. plan_helpers thinning + IPC options + guardrails (Scopes 17, 19-20).

This keeps plan and scan lanes stable while the internal helpers consolidate.
