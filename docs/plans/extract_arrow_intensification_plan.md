## Extract Arrow Intensification Plan (Normalize-Style)

### Goals
- Apply normalize-style plan-lane patterns in `src/extract`.
- Centralize hash/id definitions and reduce duplicated plan helpers.
- Move derived columns into `QuerySpec.derived` where possible.
- Use field metadata to drive dictionary encoding decisions.
- Keep extract APIs stable while improving modularity and performance.

### Constraints
- Preserve extract output schemas and dataset names.
- Keep deterministic IDs stable.
- Avoid new runtime dependencies.
- Maintain strict typing and Ruff compliance (no suppressions).

---

### Scope 1: Centralize extract HashSpec registry
**Description**
Move inline `HashSpec(...)` literals into a shared registry so IDs are defined once and reused
across extract modules and query specs.

**Code pattern**
```python
# src/extract/hash_specs.py
from arrowdsl.core.ids import HashSpec

CST_DEF_ID_SPEC = HashSpec(prefix="cst_def", cols=("file_id", "bstart", "bend"), as_string=True)
CST_CALL_ID_SPEC = HashSpec(prefix="cst_call", cols=("file_id", "bstart", "bend"), as_string=True)
TS_NODE_ID_SPEC = HashSpec(prefix="ts_node", cols=("file_id", "start_byte", "end_byte"), as_string=True)
REPO_FILE_ID_SPEC = HashSpec(prefix="repo_file", cols=("path",), as_string=True)
# ...and so on for bytecode/symtable/runtime/sym
```

**Target files**
- New: `src/extract/hash_specs.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/repo_scan.py`

**Integration checklist**
- [ ] Add `HashSpec` constants for all extract ID columns.
- [ ] Replace inline `HashSpec(...)` literals with registry imports.
- [ ] Keep ID prefixes and column inputs unchanged.

---

### Scope 2: Add extract plan-expr helpers (normalize-style ExprSpec)
**Description**
Introduce extract-local `ExprSpec` helpers (hash, masked hash, coalesce) so
`QuerySpec.derived` can express derived columns without manual `apply_hash_projection` calls.

**Code pattern**
```python
# src/extract/plan_exprs.py
import pyarrow as pa
from arrowdsl.core.ids import HashSpec, hash_column_values, hash_expression
from arrowdsl.core.interop import ArrayLike, ComputeExpression, TableLike, ensure_expression, pc

class HashExprSpec:
    def __init__(self, spec: HashSpec) -> None:
        self._spec = spec

    def to_expression(self) -> ComputeExpression:
        return hash_expression(self._spec)

    def materialize(self, table: TableLike) -> ArrayLike:
        return hash_column_values(table, spec=self._spec)

    def is_scalar(self) -> bool:
        return True

class MaskedHashExprSpec:
    def __init__(self, spec: HashSpec, required: tuple[str, ...]) -> None:
        self._spec = spec
        self._required = required

    def to_expression(self) -> ComputeExpression:
        expr = hash_expression(self._spec)
        mask = pc.is_valid(pc.field(self._required[0]))
        for name in self._required[1:]:
            mask = pc.and_(mask, pc.is_valid(pc.field(name)))
        null_value = pa.scalar(None, type=pa.string() if self._spec.as_string else pa.int64())
        return ensure_expression(pc.if_else(mask, expr, null_value))
```

**Target files**
- New: `src/extract/plan_exprs.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`

**Integration checklist**
- [ ] Add extract-local ExprSpec helpers for hash/masked hash/coalesce.
- [ ] Use these helpers in `QuerySpec.derived` for ID columns.
- [ ] Remove duplicate `apply_hash_projection` blocks where derived projections suffice.

---

### Scope 3: Attach QuerySpec + ContractSpec to extract DatasetSpecs
**Description**
Extend `register_dataset(...)` in extract to accept optional `query_spec` and `contract_spec`
so dataset specs are self-describing (schema + query + contract), matching normalize style.

**Code pattern**
```python
# src/extract/spec_helpers.py
from arrowdsl.plan.query import QuerySpec
from schema_spec.system import ContractSpec

def register_dataset(..., query_spec: QuerySpec | None = None,
                     contract_spec: ContractSpec | None = None) -> DatasetSpec:
    return GLOBAL_SCHEMA_REGISTRY.register_dataset(
        make_dataset_spec(
            table_spec=make_table_spec(...),
            query_spec=query_spec,
            contract_spec=contract_spec,
        )
    )
```

**Target files**
- Update: `src/extract/spec_helpers.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/repo_scan.py`

**Integration checklist**
- [ ] Add optional `query_spec`/`contract_spec` params in `register_dataset`.
- [ ] Define QuerySpec per dataset (use `query_for_schema` for simple cases).
- [ ] Apply `apply_query_spec(...)` using dataset QuerySpec.

---

### Scope 4: Move ID/derived columns into QuerySpec.derived
**Description**
Replace per-module `apply_hash_projection` and ad-hoc derived-column logic with
`QuerySpec.derived` using extract ExprSpec helpers and centralized HashSpec constants.

**Code pattern**
```python
# src/extract/cst_extract.py
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from extract.hash_specs import CST_CALL_ID_SPEC
from extract.plan_exprs import HashExprSpec

CALLSITES_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=("file_id", "path", "call_bstart", "call_bend", "callee_text", ...),
        derived={
            "call_id": HashExprSpec(spec=CST_CALL_ID_SPEC),
        },
    )
)
```

**Target files**
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/repo_scan.py`

**Integration checklist**
- [ ] Add QuerySpec.derived entries for each ID column.
- [ ] Remove manual `apply_hash_projection` calls after projection.
- [ ] Keep masking semantics for nullable ID inputs.

---

### Scope 5: Metadata-driven encoding for extract (SCIP)
**Description**
Drive dictionary encoding from field metadata instead of hard-coded column lists.
Attach `encoding=dictionary` on SCIP fields and use metadata helpers to select columns.

**Code pattern**
```python
# src/extract/postprocess.py
from arrowdsl.core.interop import SchemaLike

def encoding_columns_from_metadata(schema: SchemaLike) -> list[str]:
    cols: list[str] = []
    for field in schema:
        if field.metadata and field.metadata.get(b"encoding") == b"dictionary":
            cols.append(field.name)
    return cols

# src/extract/scip_extract.py
encode_cols = encoding_columns_from_metadata(SCIP_OCCURRENCES_SCHEMA)
exprs, names = encoding_projection(encode_cols, available=schema.names)
plan = plan.project(exprs, names, ctx=ctx)
```

**Target files**
- Update: `src/extract/postprocess.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/extract/spec_helpers.py` (ensure ArrowFieldSpec metadata is propagated)

**Integration checklist**
- [ ] Add field metadata for dictionary-encoded columns in SCIP specs.
- [ ] Replace hard-coded encode lists with metadata-driven selection.
- [ ] Preserve existing encoding behavior when metadata is absent.

---

### Scope 6: Unify extract/normalize plan helpers (shared module)
**Description**
Consolidate shared plan helper logic (apply_query_spec, append_projection, finalize_plan)
into a single shared module under `src/arrowdsl` to eliminate drift.

**Code pattern**
```python
# src/arrowdsl/plan_helpers.py
from arrowdsl.plan.plan import Plan, PlanSpec
from arrowdsl.plan.query import QuerySpec
from arrowdsl.core.interop import ComputeExpression, TableLike, pc

def apply_query_spec(plan: Plan, *, spec: QuerySpec, ctx: ExecutionContext) -> Plan:
    predicate = spec.predicate_expression()
    if predicate is not None:
        plan = plan.filter(predicate, ctx=ctx)
    cols = spec.scan_columns()
    names = list(cols) if not isinstance(cols, dict) else list(cols.keys())
    exprs = [pc.field(n) for n in cols] if not isinstance(cols, dict) else list(cols.values())
    return plan.project(exprs, names, ctx=ctx)
```

**Target files**
- New: `src/arrowdsl/plan_helpers.py`
- Update: `src/extract/tables.py` (import shared helpers)
- Update: `src/normalize/plan_helpers.py` (import shared helpers)

**Integration checklist**
- [ ] Move shared helper implementations to `arrowdsl`.
- [ ] Keep extract/normalize wrappers as thin re-exports (if desired).
- [ ] Remove duplicate helper implementations to avoid drift.

---

### Scope 7: Adopt large_list / large_list_view for high-cardinality lists
**Description**
Upgrade list-typed extract columns that can exceed 32-bit offsets to `large_list` and introduce
`large_list_view` where re-slicing shared value buffers reduces copying.

**Code pattern**
```python
# schema: large_list
ArrowFieldSpec(name="tags", dtype=pa.large_list(pa.string()))

# builder: large_list offsets
offsets = pa.array(offsets_int64, type=pa.int64())
values = pa.array(values_list, type=pa.string())
arr = pa.LargeListArray.from_arrays(offsets, values)
```

**Target files**
- Update: `src/extract/nested_lists.py` (support int64 offsets and list_view helpers)
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/scip_extract.py`

**Integration checklist**
- [ ] Identify list columns with potential 32-bit overflow risk.
- [ ] Update schema definitions to `pa.large_list(...)` where needed.
- [ ] Extend list accumulators to build `LargeListArray` with int64 offsets.

---

### Scope 8: Add list_view helpers for shared buffer reuse
**Description**
Provide `list_view` utilities so derived list columns can reuse shared value buffers without
extra copies, improving memory efficiency for extract outputs with repeated list slicing.

**Code pattern**
```python
# list_view construction
values = pa.array(shared_values, type=pa.string())
offsets = pa.array(offsets_int64, type=pa.int64())
sizes = pa.array(sizes_int64, type=pa.int64())
arr = pa.ListViewArray.from_arrays(offsets, sizes, values)
```

**Target files**
- Update: `src/extract/nested_lists.py` (add ListViewAccumulator)
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/extract/symtable_extract.py`

**Integration checklist**
- [ ] Add list_view builders for cases with shared value buffers.
- [ ] Prefer list_view when slicing is frequent or reuses shared buffers.
- [ ] Keep list_ fallback for smaller, simple list columns.

---

### Scope 9: Introduce map_ types for dynamic metadata payloads
**Description**
Use `map_<string, string>` (or `map_<string, binary>`) to capture flexible metadata without
column explosion, especially for runtime inspection or extractor metadata payloads.

**Code pattern**
```python
meta_type = pa.map_(pa.string(), pa.string())
meta_field = ArrowFieldSpec(name="meta", dtype=meta_type)

# data-time: must pass explicit type for map
data = [[("key", "value")], [("flag", "true")]]
arr = pa.array(data, type=meta_type)
```

**Target files**
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/scip_extract.py` (optional metadata fields)
- Update: `src/extract/cst_extract.py` (optional metadata fields)

**Integration checklist**
- [ ] Identify dynamic metadata that would otherwise become sparse columns.
- [ ] Add `map_` fields to schemas where appropriate.
- [ ] Build MapArray with explicit type (avoid implicit inference).

---

### Scope 10: Attach extract schema metadata at source
**Description**
Attach schema metadata for extractor version, repo id, and parse options using Arrow metadata
rules (immutable schema/field objects + cast).

**Code pattern**
```python
# attach metadata at finalize
meta = SchemaMetadataSpec(schema_metadata={b"extractor": b"cst", b"version": b"v1"})
schema = meta.apply(table.schema)
table = table.cast(schema)
```

**Target files**
- Update: `src/extract/spec_helpers.py` (optional metadata_spec parameter)
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/repo_scan.py`

**Integration checklist**
- [ ] Add metadata_spec support in dataset registration helpers.
- [ ] Populate schema metadata with extractor name/version and options hash.
- [ ] Ensure metadata is preserved through materialization and finalize.

---

### Scope 11: Metadata-aware schema unify helpers for extract merges
**Description**
Provide schema unify + cast helpers for extract pipelines that merge partial tables or
per-file batches, preserving first-schema metadata rules.

**Code pattern**
```python
def unify_tables(tables: Sequence[TableLike]) -> TableLike:
    unified = pa.unify_schemas([t.schema for t in tables], promote_options="permissive")
    aligned = [t.cast(unified) for t in tables]
    return pa.concat_tables(aligned)
```

**Target files**
- Update: `src/extract/tables.py` (add unify helpers)
- Update: `src/extract/scip_extract.py` (where per-doc tables merge)
- Update: `src/extract/runtime_inspect_extract.py`

**Integration checklist**
- [ ] Add unify+cast helper that preserves first-schema metadata.
- [ ] Use helper when concatenating extract fragments.
- [ ] Avoid manual per-column alignment when unify is sufficient.

---

### Scope 12: Struct flatten helpers for list<struct> projections
**Description**
Add utilities to flatten struct/list<struct> fields to simplify filtering and joining against
nested columns (e.g., CST qnames, SCIP occurrences).

**Code pattern**
```python
def flatten_struct_field(field: pa.Field) -> list[pa.Field]:
    return list(field.flatten())
```

**Target files**
- Update: `src/extract/tables.py` (add flatten helper)
- Update: `src/extract/cst_extract.py` (qnames list<struct>)
- Update: `src/extract/scip_extract.py` (occurrences list<struct>)

**Integration checklist**
- [ ] Add flatten helper for struct/list<struct> fields.
- [ ] Use flattened fields when building projections or joins on nested data.
- [ ] Keep base nested columns intact for downstream use.

---

### Scope 13: Ordering + determinism metadata for extract outputs
**Description**
Expose ordering semantics on extract plan outputs and enforce canonical ordering at finalize when
`ctx.determinism` is `CANONICAL`, consistent with the Acero ordering guidance.

**Code pattern**
```python
# finalize step for canonical determinism
if ctx.determinism.is_canonical():
    sort_keys = [("file_id", "ascending"), ("path", "ascending"), ("bstart", "ascending")]
    idx = pc.sort_indices(table, sort_keys=sort_keys)
    table = table.take(idx)
```

**Target files**
- Update: `src/extract/tables.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`

**Integration checklist**
- [ ] Mark plan ordering effects explicitly (unordered after joins/aggregates).
- [ ] Add canonical sort enforcement at finalize when required.
- [ ] Keep non-canonical paths streaming-friendly where possible.

---

### Scope 14: Scan provenance options for stable tie-breakers
**Description**
When using Acero scan nodes, set `implicit_ordering=True` and `require_sequenced_output=True` to
generate stable fragment/batch ordering for simple plans, and plumb into extract metadata.

**Code pattern**
```python
scan_opts = ctx.runtime.scan.to_acero_options()
scan_opts = scan_opts.with_options(
    implicit_ordering=True,
    require_sequenced_output=True,
)
```

**Target files**
- Update: `src/arrowdsl/core/context.py`
- Update: `src/extract/tables.py` (scan helper)

**Integration checklist**
- [ ] Extend scan helpers to pass implicit ordering flags.
- [ ] Expose provenance columns as tie-breakers when available.
- [ ] Document ordering implications in extract specs.

---

### Scope 15: Pipeline breaker aware finalize paths
**Description**
Ensure `prefer_reader` only returns readers when no pipeline breakers exist and determinism tier
allows streaming; otherwise fall back to materialization and record breaker metadata.

**Code pattern**
```python
spec = PlanSpec.from_plan(plan)
if prefer_reader and not spec.pipeline_breakers and not ctx.determinism.is_canonical():
    return spec.to_reader(ctx=ctx)
return spec.to_table(ctx=ctx)
```

**Target files**
- Update: `src/extract/tables.py`
- Update: `src/extract/spec_helpers.py`

**Integration checklist**
- [ ] Gate `prefer_reader` on pipeline breakers + determinism tier.
- [ ] Persist pipeline breaker metadata on outputs for observability.
- [ ] Keep behavior backward compatible for existing callers.

---

### Scope 16: Avoid plan-lane order_by/aggregate in extract
**Description**
Keep pipeline-breaker operations (order_by, aggregate) out of extract plan-lane; enforce any
needed ordering in kernel lane at finalize to preserve streaming behavior where feasible.

**Code pattern**
```python
# kernel lane canonical sort
idx = pc.sort_indices(table, sort_keys=sort_keys)
table = table.take(idx)
```

**Target files**
- Update: `src/extract/tables.py`
- Update: `src/extract/*_extract.py` (where ordering is enforced)

**Integration checklist**
- [ ] Remove/avoid plan-lane order_by in extract pipelines.
- [ ] Centralize ordering in finalize helpers.
- [ ] Document any exceptions where full sort is required.

---

### Scope 17: Ordering metadata in dataset specs
**Description**
Attach ordering metadata to extract dataset specs (unordered/implicit/explicit + keys) so
downstream consumers can reason about stability and dedupe behavior.

**Code pattern**
```python
metadata_spec = SchemaMetadataSpec(
    schema_metadata={b"ordering_level": b"unordered"}
)
```

**Target files**
- Update: `src/extract/spec_helpers.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`

**Integration checklist**
- [ ] Add ordering metadata to dataset registration.
- [ ] Declare explicit ordering keys when available.
- [ ] Keep ordering metadata aligned with plan ordering effects.

---

### Scope 18: Standardized plan execution helper for extract
**Description**
Create a single extract plan runner that applies `use_threads`, streaming rules, and ordering
policy consistently, mirroring the Acero DSL guidance.

**Code pattern**
```python
def run_extract_plan(plan: Plan, *, ctx: ExecutionContext, prefer_reader: bool) -> TableLike | Reader:
    spec = PlanSpec.from_plan(plan)
    if prefer_reader and not spec.pipeline_breakers and not ctx.determinism.is_canonical():
        return spec.to_reader(ctx=ctx)
    return spec.to_table(ctx=ctx)
```

**Target files**
- Update: `src/extract/tables.py`
- Update: `src/extract/*_extract.py` (use helper)

**Integration checklist**
- [ ] Add a single plan runner in `extract.tables`.
- [ ] Route all extract table helpers through it.
- [ ] Ensure ordering + determinism policies are centralized.

---

### Acceptance criteria
- Extract HashSpecs are centralized and reused consistently.
- Extract dataset specs carry QuerySpec + ContractSpec where applicable.
- Derived ID columns are implemented via QuerySpec.derived in extract.
- SCIP encoding is driven by field metadata, not hard-coded lists.
- Extract and normalize plan helpers share a single implementation.
- High-cardinality list columns use large_list where needed.
- list_view helpers exist for buffer reuse without copies.
- Dynamic metadata can be captured via map_ fields.
- Extract outputs attach schema metadata at source.
- Extract merges use metadata-aware schema unify helpers.
- Struct/list<struct> flatten helpers are available for joins/projections.
- Extract ordering metadata is explicit and canonical ordering is enforced when required.
- Scan provenance options are used where applicable.
- Pipeline breakers are recorded and streaming is gated appropriately.
- Extract avoids plan-lane order_by/aggregate when possible.
- All changes preserve existing output schemas and IDs.
