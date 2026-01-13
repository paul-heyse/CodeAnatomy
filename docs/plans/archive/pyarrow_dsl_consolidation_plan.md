# Implementation Plan: PyArrow DSL Consolidation (Acero + Protocols)

This plan consolidates repeating data/compute patterns into shared PyArrow protocols,
Acero plan ops, and Arrow DSL helpers. Each scope item includes code patterns,
concrete target files, and an integration checklist.

---

## Scope 1: Plan Lane Ordering + ExecutionProfile Wiring

### Description
Make ordering effects and pipeline breakers explicit on every plan op, and wire
ExecutionProfile/ScanProfile controls through plan compilation and execution.
This aligns plan behavior with the Acero ordering rules in the DSL guide.

### Code patterns
```python
# src/arrowdsl/plan.py

def _apply_plan_op(
    self,
    op: PlanOp,
    *,
    ctx: ExecutionContext | None = None,
    label: str = "",
) -> Plan:
    if self.decl is None:
        msg = "Plan operation requires an Acero-backed Plan (decl is None)."
        raise TypeError(msg)
    decl = op.to_declaration([self.decl], ctx=ctx)
    ordering = op.apply_ordering(self.ordering)
    return Plan(decl=decl, label=label or self.label, ordering=ordering)
```

```python
# src/arrowdsl/plan_ops.py
@dataclass(frozen=True)
class OrderByOp:
    sort_keys: Sequence[tuple[str, str]]
    ordering_effect: OrderingEffect = OrderingEffect.EXPLICIT
    is_pipeline_breaker: bool = True

    def apply_ordering(self, ordering: Ordering) -> Ordering:
        _ = ordering
        return Ordering.explicit(tuple(self.sort_keys))
```

### Target files
- `src/arrowdsl/plan.py`
- `src/arrowdsl/plan_ops.py`
- `src/arrowdsl/ops.py`
- `src/arrowdsl/runtime.py`
- `src/arrowdsl/dataset_io.py`
- `src/relspec/compiler.py`
- `src/arrowdsl/joins.py`

### Integration checklist
- [x] Thread `ExecutionContext` through plan op compilation (Scan/Filter/Project).
- [x] Ensure every plan op declares ordering effect + pipeline breaker status.
- [x] Carry ordering metadata across Plan transformations.
- [x] Apply scan ordering knobs via `ExecutionProfile` in scan nodes.

---

## Scope 2: QuerySpec + Expression Protocol Consolidation

### Description
Make `QuerySpec` the single source of truth for scan columns, pushdown predicate,
semantic predicate, and projection. Derive `QuerySpec` from `TableSchemaSpec`
via `schema_spec/factories.py` so scan columns remain aligned with schema specs.
Centralize expression construction in `arrowdsl/expr.py`, and convert
`PredicateSpec`/`PredicateExpr` inputs consistently for plan and pushdown.

### Code patterns
```python
# src/arrowdsl/queryspec.py
from schema_spec.fields import PROVENANCE_COLS

@dataclass(frozen=True)
class QuerySpec:
    predicate: PredicateSpec | None
    pushdown_predicate: PredicateSpec | None
    projection: ProjectionSpec

    def scan_columns(self, *, provenance: bool) -> list[str] | dict[str, ComputeExpression]:
        cols = list(self.projection.base)
        if provenance:
            cols += list(PROVENANCE_COLS)
        return cols
```

```python
# src/arrowdsl/expr.py
class E:
    @staticmethod
    def eq(col: str, value: object) -> ComputeExpression:
        return ensure_expression(pc.equal(pc.field(col), pc.scalar(value)))
```

```python
# src/schema_spec/factories.py
def query_spec_for_table(
    table_spec: TableSchemaSpec,
    *,
    projection: ProjectionSpec,
    predicate: PredicateSpec | None,
    pushdown: PredicateSpec | None,
) -> QuerySpec:
    base_cols = tuple(field.name for field in table_spec.fields)
    return QuerySpec(
        predicate=predicate,
        pushdown_predicate=pushdown,
        projection=ProjectionSpec(base=base_cols, derived=projection.derived),
    )
```

### Target files
- `src/arrowdsl/expr.py`
- `src/arrowdsl/predicates.py`
- `src/arrowdsl/queryspec.py`
- `src/schema_spec/factories.py`
- `src/arrowdsl/dataset_io.py`
- `src/relspec/compiler.py`
- `src/normalize/bytecode_dfg.py`
- `src/cpg/build_props.py`
- `src/cpg/build_edges.py`

### Integration checklist
- [ ] Move expression building into `expr.py` macros (pc/predicate usage remains in build_edges/build_props/bytecode_dfg).
- [x] Make QuerySpec drive scan columns and plan predicates.
- [x] Build QuerySpec from TableSchemaSpec via factory helpers.
- [x] Pass PredicateSpec/PredicateExpr directly for plan and pushdown predicates.
- [ ] Eliminate duplicated predicate logic in nodes/modules (pending cleanup in CPG/normalize helpers).
- [x] Keep pushdown predicate and semantic predicate distinct.

---

## Scope 3: Join/Aggregate/Dedupe Specs (Plan + Kernel)

### Description
Unify join/aggregate/dedupe logic across plan and kernel lanes via shared specs.
Ordering effects must be explicit (joins/aggregates mark unordered).

### Code patterns
```python
# src/arrowdsl/specs.py
@dataclass(frozen=True)
class JoinSpec:
    join_type: str
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
```

```python
# src/arrowdsl/kernels.py

def apply_aggregate(table: TableLike, *, spec: AggregateSpec) -> TableLike:
    out = table.group_by(list(spec.keys), use_threads=spec.use_threads).aggregate(list(spec.aggs))
    if not spec.rename_aggregates:
        return out
    rename_map = {f"{col}_{agg}": col for col, agg in spec.aggs if col not in spec.keys}
    names = [rename_map.get(name, name) for name in out.schema.names]
    return out.rename_columns(names)
```

### Target files
- `src/arrowdsl/specs.py`
- `src/arrowdsl/joins.py`
- `src/arrowdsl/kernels.py`
- `src/arrowdsl/finalize.py`
- `src/normalize/bytecode_cfg.py`
- `src/cpg/build_props.py`
- `src/relspec/compiler.py`

### Integration checklist
- [x] Drive all plan lane joins through `JoinSpec`.
- [x] Drive kernel lane joins/aggregates through shared helpers.
- [x] Ensure join/aggregate outputs mark ordering as unordered.
- [x] Keep canonical sort + dedupe finalize-only.

---

## Scope 4: SchemaTransform + SchemaMetadataSpec + SchemaEvolutionSpec

### Description
Centralize schema alignment and metadata mutation using immutable schema/field
operations. Add a SchemaEvolutionSpec for unify/cast/concat workflows. Default
to schema metadata versioning; only add schema_version columns when required.

### Code patterns
```python
# src/arrowdsl/schema_ops.py
@dataclass(frozen=True)
class SchemaMetadataSpec:
    schema_metadata: dict[bytes, bytes] = field(default_factory=dict)
    field_metadata: dict[str, dict[bytes, bytes]] = field(default_factory=dict)

    def apply(self, schema: SchemaLike) -> SchemaLike:
        fields = []
        for field in schema:
            meta = self.field_metadata.get(field.name)
            fields.append(field.with_metadata(meta) if meta is not None else field)
        updated = pa.schema(fields)
        return updated.with_metadata(self.schema_metadata) if self.schema_metadata else updated
```

```python
# src/arrowdsl/schema_ops.py
@dataclass(frozen=True)
class SchemaEvolutionSpec:
    promote_options: str = "permissive"

    def unify_and_cast(self, tables: Sequence[TableLike]) -> TableLike:
        unified = pa.unify_schemas([t.schema for t in tables], promote_options=self.promote_options)
        aligned = [t.cast(unified) for t in tables]
        return pa.concat_tables(aligned, promote=True)
```

### Target files
- `src/arrowdsl/schema_ops.py`
- `src/arrowdsl/schema.py`
- `src/schema_spec/core.py`
- `src/schema_spec/metadata.py`
- `src/normalize/schema_infer.py`
- `src/arrowdsl/finalize.py`

### Integration checklist
- [x] Add SchemaMetadataSpec to mutate schema/field metadata.
- [x] Route metadata edits through SchemaMetadataSpec + cast.
- [x] Add SchemaEvolutionSpec for unify/cast/concat.
- [ ] Replace ad hoc metadata edits in schema specs (TableSchemaSpec still uses schema_metadata helpers).
- [x] Prefer metadata-based versioning over a schema_version column.

---

## Scope 5: Nested Array Builders (List/Struct/Map/Union)

### Description
Standardize nested array creation through `arrowdsl/nested.py`. Map and union
builders should be used wherever schemas already declare map/union types.

### Code patterns
```python
# src/arrowdsl/nested.py

def build_map_array(offsets: ArrayLike, keys: ArrayLike, items: ArrayLike) -> ArrayLike:
    return build_map(offsets, keys, items)


def build_sparse_union_array(type_ids: ArrayLike, children: list[ArrayLike]) -> ArrayLike:
    return build_sparse_union(type_ids, children)
```

### Target files
- `src/arrowdsl/nested.py`
- `src/arrowdsl/nested_ops.py`
- `src/arrowdsl/finalize.py`
- `src/normalize/diagnostics.py`

### Integration checklist
- [x] Use shared list/struct builders for nested columns.
- [ ] Use map/union builders where schemas declare those types (no map/union fields wired yet).
- [x] Avoid ad hoc `pa.MapArray.from_arrays` outside DSL helpers.

---

## Scope 6: Encoding + Chunk Normalization Policy

### Description
Add a shared encoding policy for dictionary encoding and chunk normalization
(`unify_dictionaries` + `combine_chunks`) prior to joins/sorts and before export.

### Code patterns
```python
# src/arrowdsl/kernels.py
@dataclass(frozen=True)
class ChunkPolicy:
    unify_dictionaries: bool = True
    combine_chunks: bool = True

    def apply(self, table: TableLike) -> TableLike:
        out = table
        if self.unify_dictionaries:
            out = out.unify_dictionaries()
        if self.combine_chunks:
            out = out.combine_chunks()
        return out
```

### Target files
- `src/arrowdsl/encoding.py`
- `src/arrowdsl/kernels.py`
- `src/arrowdsl/finalize.py`
- `src/cpg/builders.py`
- `src/obs/stats.py`

### Integration checklist
- [x] Centralize dictionary encoding via EncodingSpec.
- [x] Add ChunkPolicy for unify/combine normalization.
- [x] Apply ChunkPolicy at join/sort/finalize boundaries.

---

## Scope 7: ID + Span Protocol (HashSpec, SpanIdSpec)

### Description
Replace row-wise `prefixed_hash_id_from_parts` usage with vectorized HashSpec-
based ID generation, preserving cross-table references.

### Code patterns
```python
# src/arrowdsl/ids.py
spec = HashSpec(
    prefix="bc_insn",
    cols=("code_unit_id", "instr_index", "offset"),
    out_col="instr_id",
)
with_ids = add_hash_column(table, spec=spec)
```

### Target files
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/runtime_inspect_extract.py`
- `src/normalize/ids.py`
- `src/arrowdsl/ids.py`

### Integration checklist
- [x] Replace `prefixed_hash_id_from_parts` with HashSpec + add_hash_column.
- [x] Keep prefixes identical to preserve stable ID namespaces.
- [x] Ensure IDs are computed after all input columns exist.

---

## Scope 8: Observability + Provenance Protocol

### Description
Standardize provenance columns in scans and ensure error artifacts include
provenance when enabled. Centralize run metadata and error stats generation.

### Code patterns
```python
# src/schema_spec/fields.py
PROVENANCE_COLS: tuple[str, ...] = (
    "prov_filename",
    "prov_fragment_index",
    "prov_batch_index",
    "prov_last_in_fragment",
)

# src/arrowdsl/queryspec.py
from schema_spec.fields import PROVENANCE_COLS

def scan_columns(self, *, provenance: bool) -> list[str]:
    cols = list(self.projection.base)
    if provenance:
        cols += list(PROVENANCE_COLS)
    return cols
```

```python
# src/arrowdsl/finalize.py
errors = ERROR_ARTIFACT_SPEC.build_error_table(table, results)
```

### Target files
- `src/arrowdsl/queryspec.py`
- `src/arrowdsl/dataset_io.py`
- `src/arrowdsl/finalize.py`
- `src/schema_spec/fields.py`
- `src/arrowdsl/kernels.py`
- `src/obs/stats.py`
- `src/obs/repro.py`
- `src/arrowdsl/runtime.py`
- `tests/test_provenance_constants.py`

### Integration checklist
- [ ] Standardize provenance columns across scans and plan outputs (scan columns include provenance, but contract schemas drop them).
- [ ] Include provenance in error tables when enabled (blocked on provenance columns in schemas).
- [x] Centralize stats + alignment artifacts via shared helpers.
- [x] Add guard tests to keep provenance constants aligned.

---

## Scope 9: Schema Bundles + Registry (FieldBundle + SchemaRegistry)

### Description
Replace scattered schema literals with FieldBundle definitions and a central
SchemaRegistry that owns TableSchemaSpec/ContractSpec registration and metadata
versioning. Use bundles to compose schema columns consistently across
extract/normalize/CPG.

### Code patterns
```python
# src/schema_spec/fields.py
@dataclass(frozen=True)
class FieldBundle:
    name: str
    fields: tuple[ArrowFieldSpec, ...]
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()
```

```python
# src/schema_spec/registry.py
@dataclass(frozen=True)
class SchemaRegistry:
    table_specs: dict[str, TableSchemaSpec]
    contract_specs: dict[str, ContractSpec]

    def register_table(self, spec: TableSchemaSpec) -> TableSchemaSpec:
        return self.table_specs.setdefault(spec.name, spec)
```

### Target files
- `src/schema_spec/fields.py`
- `src/schema_spec/registry.py`
- `src/schema_spec/core.py`
- `src/schema_spec/metadata.py`
- `src/cpg/schemas.py`
- `src/cpg/catalog.py`
- `src/normalize/diagnostics.py`
- `src/normalize/schema_infer.py`
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/runtime_inspect_extract.py`

### Integration checklist
- [x] Define FieldBundle families for repeated column sets.
- [ ] Register all TableSchemaSpec/ContractSpec in SchemaRegistry (only CPG wired).
- [ ] Replace module-level schema literals with bundle + registry lookups (extract/normalize still local).
- [ ] Apply schema metadata from the registry when emitting Arrow schemas.

---

## Scope 10: Spec Factories (TableSchemaSpec / ContractSpec / QuerySpec)

### Description
Create factory helpers for building table specs, contracts, and query specs so
spec construction is programmatic and consistent. QuerySpec must be derived
from the input TableSchemaSpec, not from output contracts.

### Code patterns
```python
# src/schema_spec/factories.py
def make_table_spec(
    name: str,
    *,
    version: int | None,
    bundles: Iterable[FieldBundle],
    fields: Iterable[ArrowFieldSpec],
) -> TableSchemaSpec:
    ...


def make_contract_spec(*, table_spec: TableSchemaSpec, version: int | None) -> ContractSpec:
    ...


def query_spec_for_table(table_spec: TableSchemaSpec) -> QuerySpec:
    cols = tuple(field.name for field in table_spec.fields)
    return QuerySpec(projection=ProjectionSpec(base=cols))
```

### Target files
- `src/schema_spec/factories.py`
- `src/schema_spec/core.py`
- `src/schema_spec/contracts.py`
- `src/schema_spec/fields.py`
- `src/arrowdsl/queryspec.py`
- `src/cpg/schemas.py`
- `src/extract/ast_extract.py`
- `src/extract/scip_extract.py`
- `src/normalize/diagnostics.py`
- `tests/test_schema_spec_builders.py`

### Integration checklist
- [x] Implement factory helpers for table specs, contracts, and query specs.
- [x] Replace manual TableSchemaSpec/ContractSpec construction with factories.
- [x] Ensure QuerySpec is built from the input TableSchemaSpec only.
- [x] Add guard tests for factory outputs and schema bundle usage.

---

## Scope 11: Shared Context Objects (FileContext / ScanContext / FinalizeContext)

### Description
Standardize extractor, scan, and finalize context objects to avoid ad hoc
parameter passing. Thread contexts through dataset I/O and pipeline modules.

### Code patterns
```python
# src/extract/file_context.py
@dataclass(frozen=True)
class FileContext:
    file_id: str
    path: str
    file_sha256: str | None
    encoding: str | None = None
```

```python
# src/arrowdsl/scan_context.py
@dataclass(frozen=True)
class ScanContext:
    query: QuerySpec
    execution: ExecutionContext
```

```python
# src/arrowdsl/finalize_context.py
@dataclass(frozen=True)
class FinalizeContext:
    contract: ContractSpec
    schema: TableSchemaSpec
    execution: ExecutionContext
```

### Target files
- `src/extract/file_context.py`
- `src/arrowdsl/scan_context.py`
- `src/arrowdsl/finalize_context.py`
- `src/arrowdsl/dataset_io.py`
- `src/arrowdsl/runtime.py`
- `src/arrowdsl/finalize.py`
- `src/hamilton_pipeline/modules/extraction.py`
- `src/hamilton_pipeline/modules/normalization.py`

### Integration checklist
- [x] Adopt FileContext in all extractors.
- [x] Use ScanContext for dataset scans and plan compilation.
- [x] Use FinalizeContext for contract validation and error artifacts.
- [ ] Update pipeline modules to construct contexts consistently (extraction/normalization still pass raw inputs).

---

## Global Guardrails (apply across all scopes)

- No raw `pc.*` in nodes; route compute through `arrowdsl/expr.py` or
  `arrowdsl/kernels.py`.
- Use plan lane for scan/filter/project/join/aggregate; kernel lane for
  row-count changing transforms and finalize.
- Enforce canonical ordering only at finalize when determinism is canonical.
