## Extract Acero Pipeline Plan (PyArrow/Acero/DSL)

### Goals
- Convert extractor pipelines to Acero plan‑lane where feasible (filter/project/join/aggregate).
- Keep Python parsing/collection only where Acero cannot express the operation.
- Replace static, bespoke steps with programmatic QuerySpec/ProjectionSpec‑driven plans.
- Preserve current extractor outputs and schemas.

### Constraints
- Do not touch `src/normalize` (parallel work in progress).
- Keep extract‑specific parsing logic intact; only refactor orchestration and post‑processing.

---

### Scope 1: Plan‑lane hash join support
**Description**
Add a PlanOp for hash joins and route extract joins through Acero when both sides are plan‑backed.

**Code pattern**
```python
# src/arrowdsl/plan/ops.py
@dataclass(frozen=True)
class JoinOp:
    spec: JoinSpec
    ordering_effect: OrderingEffect = OrderingEffect.UNORDERED
    is_pipeline_breaker: bool = False

    def to_declaration(self, inputs: list[DeclarationLike], ctx: ExecutionContext | None) -> DeclarationLike:
        _ = ctx
        opts = acero.HashJoinNodeOptions(
            self.spec.join_type,
            list(self.spec.left_keys),
            list(self.spec.right_keys),
            left_output=list(self.spec.left_output),
            right_output=list(self.spec.right_output),
            output_suffix_for_left=self.spec.output_suffix_for_left,
            output_suffix_for_right=self.spec.output_suffix_for_right,
        )
        return acero.Declaration("hashjoin", opts, inputs=inputs)
```

**Target files**
- Update: `src/arrowdsl/plan/ops.py`
- Update: `src/arrowdsl/plan/plan.py` (add `join(...)` convenience)
- Update: `src/extract/join_helpers.py` (prefer plan‑lane join when inputs are Plans)
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`

**Integration checklist**
- [ ] Add `JoinOp` and `Plan.join(...)`.
- [ ] Extend `left_join(...)` to accept `Plan` inputs and produce a `Plan`.
- [ ] Keep kernel‑lane join fallback for non‑plan tables.
- [ ] Mark output ordering as unordered post‑join.

---

### Scope 2: HashSpec → plan expression
**Description**
Move hash ID generation into `project` expressions so hashing is plan‑lane by default.

**Code pattern**
```python
# src/arrowdsl/core/ids.py
def hash_expression(spec: HashSpec) -> ComputeExpression:
    exprs = [FieldExpr(col).to_expression() for col in spec.cols]
    if spec.extra_literals:
        exprs = [pc.scalar(lit) for lit in spec.extra_literals] + exprs
    hashed = pc.hash64(exprs)  # or hash64_from_arrays equivalent
    if spec.as_string:
        hashed = pc.binary_join_element_wise(pc.scalar(spec.prefix), pc.cast(hashed, pa.string()), ":")
    return hashed
```

**Target files**
- Update: `src/arrowdsl/core/ids.py`
- Update: `src/arrowdsl/compute/expr.py` (if needed for FieldExpr helpers)
- Update: `src/extract/hashing.py` (add plan‑lane variant or adapt to expressions)
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/repo_scan.py`
- Update: `src/extract/scip_extract.py`

**Integration checklist**
- [ ] Add a `hash_expression(...)` adapter.
- [ ] Use `Plan.project(...)` to materialize hash IDs instead of kernel‑lane append.
- [ ] Keep `apply_hash_column(...)` for non‑plan tables or as a fallback.

---

### Scope 3: Schema alignment as a plan projection
**Description**
Express schema alignment (cast + missing columns) as a `project` plan step.

**Code pattern**
```python
# src/arrowdsl/schema/schema.py
def projection_for_schema(schema: SchemaLike) -> tuple[list[ComputeExpression], list[str]]:
    exprs: list[ComputeExpression] = []
    names: list[str] = []
    for field in schema:
        col = FieldExpr(field.name).to_expression()
        expr = pc.cast(col, field.type, safe=True)
        exprs.append(expr)
        names.append(field.name)
    return exprs, names
```

**Target files**
- Update: `src/arrowdsl/schema/schema.py`
- Update: `src/extract/tables.py` (add `align_plan(...)` helper)
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`

**Integration checklist**
- [ ] Add a projection builder for schema alignment.
- [ ] Replace `align_table(...)` with `Plan.project(...)` where a plan is used.
- [ ] Keep `align_table(...)` for kernel‑lane fallback.

---

### Scope 4: Dictionary encoding as a plan projection
**Description**
Move dictionary encoding into plan‑lane `project` expressions.

**Code pattern**
```python
# src/arrowdsl/schema/schema.py
def encode_expression(col: str) -> ComputeExpression:
    return pc.dictionary_encode(FieldExpr(col).to_expression())
```

**Target files**
- Update: `src/arrowdsl/schema/schema.py`
- Update: `src/extract/postprocess.py` (plan‑lane alternative)
- Update: `src/extract/scip_extract.py`

**Integration checklist**
- [ ] Add `encode_expression(...)`.
- [ ] Use `Plan.project(...)` to apply encoding where configured.
- [ ] Keep `apply_encoding(...)` for table‑only fallbacks.

---

### Scope 5: Streaming row emission to RecordBatchReader
**Description**
Replace list accumulation with `RecordBatchReader` construction and feed into plans.

**Code pattern**
```python
# src/extract/tables.py
def iter_record_batches(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
) -> Iterator[pa.RecordBatch]:
    buffer: list[Mapping[str, object]] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield pa.RecordBatch.from_pylist(buffer, schema=schema)
            buffer.clear()
    if buffer:
        yield pa.RecordBatch.from_pylist(buffer, schema=schema)
```

**Target files**
- Update: `src/extract/tables.py`
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/repo_scan.py`

**Integration checklist**
- [ ] Add `iter_record_batches(...)` helper.
- [ ] Convert extractor row builders to yield batches (or iterables of rows).
- [ ] Build `RecordBatchReader` via `pa.RecordBatchReader.from_batches(...)`.
- [ ] Use `Plan.from_reader(...)` as the plan source.

---

### Scope 6: Extractor plan refactors (per module)
**Description**
Refactor each extractor to a three‑stage flow: parse → plan‑lane transforms → finalize.

**Code pattern**
```python
# Example: AST extraction
reader = pa.RecordBatchReader.from_batches(AST_NODES_SCHEMA, iter_record_batches(rows, schema=AST_NODES_SCHEMA))
plan = Plan.from_reader(reader).filter(predicate, ctx=ctx).project(exprs, names, ctx=ctx)
table_or_reader = plan.to_reader(ctx=ctx)  # streaming surface
```

**Target files**
- `src/extract/ast_extract.py` (filter defs in plan‑lane)
- `src/extract/cst_extract.py` (hash IDs + derived fields in plan‑lane)
- `src/extract/symtable_extract.py` (join in plan‑lane)
- `src/extract/bytecode_extract.py` (def/use + IDs in plan‑lane)
- `src/extract/tree_sitter_extract.py` (missing/error filters in plan‑lane)
- `src/extract/scip_extract.py` (encoding in plan‑lane)
- `src/extract/runtime_inspect_extract.py` (joins in plan‑lane)
- `src/extract/repo_scan.py` (hash IDs in plan‑lane)

**Integration checklist**
- [ ] Introduce `ExecutionContext` usage in extractors where missing.
- [ ] Prefer `Plan.to_reader()` unless a pipeline breaker is declared.
- [ ] Use `PlanSpec` to record pipeline breaker metadata (order_by/aggregate).

---

### Scope 7: Programmatic QuerySpec/ProjectionSpec for extract outputs
**Description**
Make extract pipelines declarative by defining `QuerySpec`/`ProjectionSpec` per dataset.

**Code pattern**
```python
# src/extract/ast_extract.py
AST_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=("file_id", "path", "kind", "span_start", "span_end"),
        derived={"span_len": ExprSpec("span_end - span_start")},
    ),
)
```

**Target files**
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/repo_scan.py`
- Update: `src/arrowdsl/plan/query.py` (if additional helpers are required)

**Integration checklist**
- [ ] Define `QuerySpec` for each dataset table.
- [ ] Use `QuerySpec.scan_columns(...)` for plan‑lane projections.
- [ ] Keep schema specs as the source of truth for output fields.

---

### Execution sequencing
1) Add plan‑lane hash join support (Scope 1).
2) Add plan‑lane hash ID expressions (Scope 2).
3) Add schema alignment projections (Scope 3).
4) Add encoding expressions (Scope 4).
5) Add streaming batch emission (Scope 5).
6) Refactor extractors to plan‑lane flows (Scope 6).
7) Make datasets declarative via `QuerySpec` (Scope 7).

---

### Acceptance criteria
- All extractors can return a `RecordBatchReader` without full materialization.
- Plan‑lane filters/projects/joins replace bespoke kernel‑lane steps where possible.
- Hash IDs and dictionary encoding are expressed as plan projections.
- Ordering metadata is preserved via `ExecutionContext` and `PlanSpec`.
- Extractor outputs match existing schemas and tests.
