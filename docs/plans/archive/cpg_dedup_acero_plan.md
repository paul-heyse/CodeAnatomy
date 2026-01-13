# Implementation Plan: CPG Dedup + Acero/DSL Consolidation

This plan consolidates repeated CPG patterns into shared helpers, spec objects, and
Arrow DSL plan plumbing. It integrates the confirmed behavior decisions:

- Numeric IDs should never be 0 in valid inputs (treat 0 as invalid).
- Missing ID columns in node sources must be captured (not dropped silently).
- Fill `resolution_method` when null.
- Use a unified `EntityFamilySpec` to generate both node and prop specs.

Each scope item includes code patterns, target files, and a checklist.

---

## Scope 1: Unified EntityFamilySpec (Nodes + Props)

### Description
Create a single spec that can emit both `NodePlanSpec` and `PropTableSpec` without
loss of fidelity. This becomes the canonical registry for node-kind families and
their property mappings, eliminating drift between `build_nodes` and `build_props`.

### Code patterns
```python
# src/cpg/specs.py
@dataclass(frozen=True)
class EntityFamilySpec:
    name: str
    option_flag: str
    node_kind: NodeKind
    id_cols: tuple[str, ...]
    table: TableRef
    path_cols: tuple[str, ...] = ()
    bstart_cols: tuple[str, ...] = ()
    bend_cols: tuple[str, ...] = ()
    file_id_cols: tuple[str, ...] = ()
    prop_fields: tuple[PropFieldSpec, ...] = ()
    include_props: bool = True

    def to_node_plan(self) -> NodePlanSpec:
        return NodePlanSpec(
            name=self.name,
            option_flag=self.option_flag,
            table_getter=self.table.getter(),
            emit=NodeEmitSpec(
                node_kind=self.node_kind,
                id_cols=self.id_cols,
                path_cols=self.path_cols,
                bstart_cols=self.bstart_cols,
                bend_cols=self.bend_cols,
                file_id_cols=self.file_id_cols,
            ),
        )

    def to_prop_table(self) -> PropTableSpec:
        return PropTableSpec(
            name=self.name,
            option_flag=self.option_flag,
            table_getter=self.table.getter(),
            entity_kind=EntityKind.NODE,
            id_cols=self.id_cols,
            node_kind=self.node_kind,
            fields=self.prop_fields,
            include_if=None if self.include_props else (lambda _: False),
        )
```

### Target files
- `src/cpg/specs.py`
- `src/cpg/build_nodes.py`
- `src/cpg/build_props.py`
- `src/cpg/__init__.py`
- `tests/test_schema_spec_builders.py`

### Implementation checklist
- [ ] Add `EntityFamilySpec` and replace duplicated node/prop specs with a single registry.
- [ ] Preserve existing per-family overrides (extra id/path columns, heavy-json gating).
- [ ] Update `NodeBuilder`/`PropBuilder` wiring to consume generated specs.
- [ ] Add tests that ensure every `EntityFamilySpec` yields both node + prop specs.

---

## Scope 2: Table Catalog + Shared Getters (Dedup Table Wiring)

### Description
Unify `_table_getter` and the per-module table mapping logic into a shared catalog
that also supports derived tables. This removes repeated table assembly in nodes,
edges, and props, and makes derived tables discoverable to the DSL plan layer.

### Code patterns
```python
# src/cpg/catalog.py
@dataclass(frozen=True)
class TableRef:
    name: str
    derive: Callable[[TableCatalog, ExecutionContext], TableLike | None] | None = None

    def getter(self) -> TableGetter:
        def _get(tables: Mapping[str, TableLike]) -> TableLike | None:
            return tables.get(self.name)
        return _get


@dataclass
class TableCatalog:
    tables: dict[str, TableLike] = field(default_factory=dict)

    def add_if(self, name: str, table: TableLike | None) -> None:
        if table is not None:
            self.tables[name] = table

    def resolve(self, ref: TableRef, *, ctx: ExecutionContext) -> TableLike | None:
        if ref.name in self.tables:
            return self.tables[ref.name]
        if ref.derive is None:
            return None
        table = ref.derive(self, ctx)
        if table is not None:
            self.tables[ref.name] = table
        return table
```

### Target files
- `src/cpg/catalog.py` (new)
- `src/cpg/build_nodes.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_props.py`
- `src/cpg/specs.py`

### Implementation checklist
- [ ] Introduce `TableCatalog` and migrate `_node_tables`, `_edge_tables`, `_prop_tables`.
- [ ] Register derived tables (file nodes, SCIP role flags, qname fallback).
- [ ] Replace `_table_getter` with `TableRef.getter()` for all specs.
- [ ] Add catalog-focused tests to ensure derived tables resolve once and are cached.

---

## Scope 3: Relation Plans via Arrow DSL (Acero + Kernel Hybrid)

### Description
Express edge relation transforms as Arrow DSL plans where possible, using
Acero for project/filter/join/aggregate and kernel lane helpers for the rest.
This centralizes relation logic in a single registry and makes execution mode
explicit (streaming vs materializing).

### Code patterns
```python
# src/cpg/relations.py
@dataclass(frozen=True)
class EdgeRelationSpec:
    name: str
    option_flag: str
    emit: EdgeEmitSpec
    plan_builder: Callable[[TableCatalog, ExecutionContext], PlanSpec]


def qname_fallback_plan(tables: TableCatalog, ctx: ExecutionContext) -> PlanSpec:
    rel = tables.resolve(TableRef("rel_callsite_qname"), ctx=ctx)
    if rel is None:
        return PlanSpec.from_plan(Plan.table(rel))
    plan = Plan.table(rel).filter(Not(InValues(col="call_id", values=...)))
    return PlanSpec.from_plan(plan).with_kernel(_ensure_ambiguity_group_id)
```

```python
# src/cpg/build_edges.py
plans = [spec.plan_builder(catalog, ctx) for spec in EDGE_RELATION_SPECS if enabled]
tables = [plan.to_table(ctx=ctx) for plan in plans if plan is not None]
```

### Target files
- `src/cpg/relations.py` (new)
- `src/cpg/build_edges.py`
- `src/arrowdsl/plan/plan.py`
- `src/arrowdsl/plan/ops.py`
- `src/arrowdsl/compute/kernels.py`

### Implementation checklist
- [ ] Define `EdgeRelationSpec` registry and migrate existing edge relation helpers.
- [ ] Prefer Acero plan ops (project/filter/join/aggregate) with kernel fallbacks.
- [ ] Track ordering + pipeline breakers using `PlanSpec` and DSL guide semantics.
- [ ] Add plan-level tests for a sample relation (diagnostics, qname fallback).

---

## Scope 4: Defaults, Encoding, and Schema Evolution Consolidation

### Description
Centralize default fill logic, dictionary encoding, and schema evolution. This
eliminates repeated per-column logic and ensures `resolution_method` is always
filled when null. Use `pyarrow.unify_schemas` + `Table.cast` for safe merging.

### Code patterns
```python
# src/cpg/defaults.py
def apply_edge_defaults(table: TableLike, *, spec: EdgeEmitSpec, schema: SchemaLike) -> TableLike:
    defaults = ColumnDefaultsSpec(
        defaults=(
            ("origin", ConstExpr(spec.origin, dtype=pa.string())),
            ("resolution_method", ConstExpr(spec.default_resolution_method, dtype=pa.string())),
        ),
    )
    table = defaults.apply(table)
    table = set_or_append_column(table, "origin", pc.fill_null(table["origin"], spec.origin))
    table = set_or_append_column(
        table,
        "resolution_method",
        pc.fill_null(table["resolution_method"], spec.default_resolution_method),
    )
    return encode_columns(table, specs=EDGE_ENCODING_SPECS)
```

```python
# src/cpg/merge.py
def unify_tables(tables: Sequence[TableLike], *, ctx: ExecutionContext) -> TableLike:
    schema = pa.unify_schemas([t.schema for t in tables], promote_options="permissive")
    aligned = [t.cast(schema) for t in tables]
    return pa.concat_tables(aligned)
```

### Target files
- `src/cpg/defaults.py` (new)
- `src/cpg/builders.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_nodes.py`
- `src/cpg/schemas.py`
- `src/arrowdsl/schema/schema.py`

### Implementation checklist
- [ ] Add `apply_edge_defaults` and apply in edge emission pipeline.
- [ ] Ensure `resolution_method` is filled per-row (null-safe).
- [ ] Centralize schema unification and apply in nodes/edges builders.
- [ ] Keep dictionary encoding in one place (edges/nodes defaults helper).

---

## Scope 5: Quality Capture for Missing/Invalid IDs

### Description
Do not drop rows when ID columns are missing. Instead, capture invalid IDs in a
quality artifact so the pipeline remains observable. Invalid IDs include:
null IDs, missing id columns, or numeric IDs equal to 0.

### Code patterns
```python
# src/cpg/quality.py
def build_quality_issues(table: TableLike, *, id_col: str, issue: str) -> TableLike:
    mask = pc.or_(
        pc.is_null(table[id_col]),
        pc.equal(table[id_col], pa.scalar(0, type=table.schema.field(id_col).type)),
    )
    return table.filter(mask).select([id_col]).append_column(
        "issue", const_array(mask.sum().as_py(), issue, dtype=pa.string())
    )
```

```python
# src/cpg/build_nodes.py
raw = build_cpg_nodes_raw(...)
issues = build_quality_issues(raw, id_col="node_id", issue="missing_node_id")
```

### Target files
- `src/cpg/quality.py` (new)
- `src/cpg/build_nodes.py`
- `src/cpg/build_props.py`
- `src/arrowdsl/finalize/finalize.py`
- `docs/guide/cpg_quality.md` (new)

### Implementation checklist
- [ ] Add a `cpg_quality` artifact (table or finalize bundle) for invalid IDs.
- [ ] Treat numeric zero IDs as invalid and capture in quality output.
- [ ] Keep raw rows for diagnostics, but surface issues via `FinalizeResult.errors` or a new table.
- [ ] Add tests for missing/zero IDs to ensure they are captured.

---

## Scope 6: Registry Alignment with `kinds_ultimate`

### Description
Use `kinds_ultimate` contracts to drive property mappings and ensure CPG specs
stay consistent with the canonical kind registry. This prevents drift between
contracts and property emission.

### Code patterns
```python
# src/cpg/contract_map.py
def prop_specs_from_contract(
    props: Mapping[str, PropSpec],
    source_map: Mapping[str, str],
) -> tuple[PropFieldSpec, ...]:
    return tuple(
        PropFieldSpec(prop_key=key, source_col=source_map.get(key, key))
        for key in props.keys()
    )
```

```python
# src/cpg/specs.py
ENTITY_FAMILY_SPECS = (
    EntityFamilySpec(
        name="scip_symbol",
        option_flag="include_symbol_nodes",
        node_kind=NodeKind.SCIP_SYMBOL,
        id_cols=("symbol",),
        table=TableRef("scip_symbols"),
        prop_fields=prop_specs_from_contract(
            NODE_KIND_CONTRACTS[NodeKind.SCIP_SYMBOL].required_props,
            source_map={"display_name": "display_name"},
        ),
    ),
)
```

### Target files
- `src/cpg/contract_map.py` (new)
- `src/cpg/specs.py`
- `src/cpg/kinds_ultimate.py`
- `tests/test_schema_spec_builders.py`

### Implementation checklist
- [ ] Add contract-driven prop field generation.
- [ ] Map contract property keys to source columns explicitly.
- [ ] Ensure every `EntityFamilySpec` has a contract-backed prop map.
- [ ] Add tests validating coverage and contract alignment.

---

## Scope 7: Migration + Backward Compatibility

### Description
Provide a staged migration that keeps existing APIs and raw builders available
while the unified spec layer and plan registry are adopted.

### Code patterns
```python
# src/cpg/build_nodes.py
def build_cpg_nodes_raw(..., *, use_unified_specs: bool = True) -> TableLike:
    specs = ENTITY_FAMILY_SPECS if use_unified_specs else NODE_PLAN_SPECS
    parts = NodeBuilder(emitters=tuple(spec.to_node_plan() for spec in specs)).build(...)
```

### Target files
- `src/cpg/build_nodes.py`
- `src/cpg/build_props.py`
- `src/cpg/build_edges.py`
- `src/cpg/__init__.py`
- `docs/guide/cpg_migration.md` (new)

### Implementation checklist
- [ ] Add feature flag to select unified spec path.
- [ ] Keep legacy specs available until parity tests pass.
- [ ] Document migration path and deprecation timeline.

