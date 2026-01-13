## Normalize Arrow Opportunities Plan (Post-Intensification)

### Goals
- Extend metadata-driven encoding and dictionary unification across normalize outputs.
- Adopt advanced Arrow nested types (list_view, map) where they reduce copy and improve clarity.
- Make normalize pipelines stream-first when inputs permit it.
- Avoid redundant canonical sorts by honoring plan ordering metadata.
- Deduplicate plan helpers across normalize/extract/cpg by centralizing in arrowdsl.

### Constraints
- Preserve normalized dataset contracts unless a scope explicitly redefines a schema.
- Keep plan-lane work inside Acero and kernel-lane work inside arrowdsl compute helpers.
- Maintain strict typing and Ruff compliance (no suppressions).
- Avoid adding new non-Arrow runtime dependencies.

---

### Scope 1: Metadata-driven encoding policy and dictionary unification
**Description**
Extend schema metadata to drive dictionary encoding consistently (including index type and
ordered flags). Centralize encoding policy construction and apply it during finalize so all
normalize outputs are encoded and unified the same way.

**Code pattern**
```python
# src/normalize/encoding.py
import pyarrow as pa
from arrowdsl.compute.kernels import ChunkPolicy
from arrowdsl.schema.schema import EncodingPolicy, EncodingSpec
from arrowdsl.core.interop import SchemaLike

ENCODING_META = "encoding"
ENCODING_DICTIONARY = "dictionary"
DICT_INDEX_META = "dictionary_index_type"
DICT_ORDERED_META = "dictionary_ordered"


def encoding_policy_from_schema(schema: SchemaLike) -> EncodingPolicy:
    specs = tuple(
        spec for field in schema if (spec := _encoding_spec_from_field(field)) is not None
    )
    return EncodingPolicy(specs=specs, chunk_policy=ChunkPolicy())


# src/normalize/schemas.py
ENCODING_META = "encoding"

def dict_field(name: str, *, ordered: bool = False) -> ArrowFieldSpec:
    return ArrowFieldSpec(
        name=name,
        dtype=pa.dictionary(pa.int32(), pa.string(), ordered=ordered),
        metadata={
            ENCODING_META: "dictionary",
            DICT_INDEX_META: "int32",
            DICT_ORDERED_META: "1" if ordered else "0",
        },
    )
```

**Target files**
- New: `src/normalize/encoding.py`
- Update: `src/arrowdsl/finalize/finalize.py`
- Update: `src/arrowdsl/schema/schema.py`
- Update: `src/normalize/schemas.py`
- Update: `src/normalize/runner.py`
- Update: `src/normalize/plan_helpers.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Add encoding metadata helpers and a normalize-local EncodingPolicy factory.
- [x] Mark high-cardinality string columns with dictionary encoding metadata.
- [x] Apply EncodingPolicy during finalize (after query spec projection).
- [x] Unify dictionaries and combine chunks using ChunkPolicy.

---

### Scope 2: ListView and nested type modernization (diagnostics and list fields)
**Description**
Adopt `list_view` / `large_list_view` types for nested list columns that are frequently sliced
or re-batched (diagnostic details, tags, qname lists). Use view types to reduce copying and
enable more flexible offsets. Optionally migrate diagnostic detail collections to `map<key, value>`
when key/value semantics are more appropriate.

**Code pattern**
```python
# src/normalize/schemas.py
DIAG_TAGS_TYPE = list_view_type(pa.string(), large=True)
DIAG_DETAILS_TYPE = list_view_type(DIAG_DETAIL_STRUCT, large=True)

# src/normalize/diagnostics.py
def _build_diag_details(buffers: _DiagBuffers) -> ArrayLike:
    offsets = pa.array(buffers.detail_offsets, type=pa.int64())
    sizes = pa.array(buffers.detail_sizes, type=pa.int64())
    detail_struct = build_struct_array(...)
    return pa.LargeListViewArray.from_arrays(offsets, sizes, detail_struct)
```

**Target files**
- Update: `src/normalize/schemas.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/schema_infer.py`
- Update: `src/schema_spec/specs.py`

**Implementation checklist**
- [x] Add list view types for nested list columns that are frequently projected.
- [x] Extend diagnostic detail builder to emit ListView arrays.
- [x] Add metadata hints for list view types (not required once inference preserves them).
- [x] Preserve list view types during schema unification.

---

### Scope 3: Stream-first normalize entrypoints (RecordBatchReader, Dataset, Scanner)
**Description**
Allow normalize entrypoints to accept streaming inputs (`RecordBatchReader`, `Dataset`,
`Scanner`) and return readers when the plan has no pipeline breakers. This keeps normalize
streamable end-to-end without forcing table materialization.

**Code pattern**
```python
# src/normalize/runner.py
from pyarrow.dataset import Dataset, Scanner
from arrowdsl.plan.plan import Plan
from arrowdsl.plan_helpers import PlanSource, plan_source

def run_normalize_streamable(...):
    plan = plan_source(source)
    plan = apply_query_spec(plan, spec=spec, ctx=ctx)
    return finalize_plan(plan, ctx=ctx, prefer_reader=True)
```

**Target files**
- New: `src/arrowdsl/plan_helpers.py`
- Update: `src/normalize/runner.py`
- Update: `src/normalize/plan_helpers.py`
- Update: `src/normalize/__init__.py`
- Update: `src/normalize/pipeline.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/bytecode_dfg.py`

**Implementation checklist**
- [x] Add a unified source-to-plan adapter that accepts Dataset/Scanner/Reader.
- [x] Keep streamable paths returning RecordBatchReader when no pipeline breakers exist.
- [ ] Extend finalize helpers to surface reader vs table in results metadata.
- [ ] Ensure safe casting and schema alignment work on streaming sources.

---

### Scope 4: Ordering-aware finalize (skip redundant canonical sorts)
**Description**
Use plan ordering metadata plus contract canonical sort keys to avoid redundant sorts.
If a plan already guarantees the contract order, skip the kernel-lane canonical sort and
only apply it when ordering is absent or incomplete.

**Code pattern**
```python
# src/normalize/runner.py
from arrowdsl.core.context import OrderingLevel
from arrowdsl.compute.kernels import canonical_sort_if_canonical
from arrowdsl.finalize.finalize import Contract

def finalize_with_contract(plan: Plan, *, contract: Contract, ctx: ExecutionContext) -> TableLike:
    table = PlanSpec.from_plan(plan).to_table(ctx=ctx)
    if plan.ordering.level == OrderingLevel.EXPLICIT:
        keys = tuple((key.column, key.order) for key in plan.ordering.keys)
        if keys == contract.canonical_sort:
            return table
    return canonical_sort_if_canonical(table, sort_keys=contract.canonical_sort, ctx=ctx)
```

**Target files**
- Update: `src/normalize/runner.py`
- Update: `src/normalize/plan_helpers.py`
- Update: `src/arrowdsl/finalize/finalize.py`

**Implementation checklist**
- [x] Compare plan ordering metadata to contract canonical sort keys.
- [x] Skip canonical sort when ordering already matches contract requirements.
- [x] Avoid inserting `order_by` in plan-lane unless explicitly required.

---

### Scope 5: Shared plan helpers (normalize/extract/cpg dedupe)
**Description**
Consolidate common plan helpers (encoding projection, metadata-driven encoding, column-or-null,
masked hash, coalesce) into `arrowdsl` so normalize, extract, and cpg share identical logic.

**Code pattern**
```python
# src/arrowdsl/plan_helpers.py
from arrowdsl.core.interop import ComputeExpression, SchemaLike, pc
from arrowdsl.schema.schema import encode_expression

def encoding_columns_from_metadata(schema: SchemaLike) -> list[str]:
    cols: list[str] = []
    for field in schema:
        meta = field.metadata or {}
        if meta.get(b"encoding") == b"dictionary":
            cols.append(field.name)
    return cols

def encoding_projection(columns: Sequence[str], *, available: Sequence[str]) -> tuple[list[ComputeExpression], list[str]]:
    expressions = [encode_expression(name) if name in set(columns) else pc.field(name) for name in available]
    return expressions, list(available)
```

**Target files**
- New: `src/arrowdsl/plan_helpers.py`
- Update: `src/normalize/plan_helpers.py`
- Update: `src/extract/postprocess.py`
- Update: `src/cpg/plan_helpers.py`
- Update: `src/normalize/plan_exprs.py`
- Update: `src/cpg/plan_exprs.py`

**Implementation checklist**
- [x] Move shared helpers into arrowdsl and re-export in normalize.
- [x] Replace local copies in normalize/extract/cpg with the shared helpers.
- [x] Keep helper APIs stable (or update call sites in one batch).

---

### Scope 6: Schema inference upgrades for advanced types
**Description**
Preserve dictionary, list_view, map, and struct types during schema unification to avoid
regressing advanced types back to simpler list or string types. Extend metadata inheritance
to nested children for list and map types.

**Code pattern**
```python
# src/normalize/schema_infer.py
def _prefer_arrow_nested(base: SchemaLike, merged: SchemaLike) -> SchemaLike:
    fields: list[FieldLike] = []
    base_map = {field.name: field for field in base}
    for field in merged:
        base_field = base_map.get(field.name)
        if base_field and (
            pa.types.is_dictionary(base_field.type)
            or pa.types.is_list_view(base_field.type)
            or pa.types.is_large_list_view(base_field.type)
            or pa.types.is_map(base_field.type)
            or pa.types.is_struct(base_field.type)
        ):
            fields.append(base_field)
        else:
            fields.append(field)
    return pa.schema(fields)
```

**Target files**
- Update: `src/normalize/schema_infer.py`
- Update: `src/normalize/schemas.py`
- Update: `src/arrowdsl/schema/schema.py`

**Implementation checklist**
- [x] Prefer list_view/map/dictionary types when the base schema includes them.
- [x] Extend metadata inheritance to nested list/map children.
- [x] Align schema inference with list view usage in diagnostics and other lists.
