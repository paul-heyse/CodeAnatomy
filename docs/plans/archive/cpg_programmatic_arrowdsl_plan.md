## CPG Programmatic ArrowDSL Refactor Plan

### Goals
- Replace static CPG schema/spec declarations with programmatic, row-driven registries.
- Centralize CPG configuration into ArrowDSL-backed catalogs and tables.
- Keep CPG builders focused on execution while specs live in reusable registries.
- Reuse ArrowDSL routines wherever possible; add new helpers only when needed.

### Constraints
- Preserve output schemas, column names, and metadata semantics.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Maintain compatibility with existing ArrowDSL plan/contract mechanics.

---

### Scope 1: CPG Field + Bundle Catalogs
**Description**
Create centralized catalogs for reusable `ArrowFieldSpec` entries and `FieldBundle`s.
CPG schema builders reference catalog keys instead of inline field lists.

**Code patterns**
```python
# src/cpg/registry_fields.py
FIELD_CATALOG = {
    "node_id": ArrowFieldSpec(name="node_id", dtype=pa.string(), nullable=False),
    "node_kind": ArrowFieldSpec(name="node_kind", dtype=DICT_STRING, nullable=False),
    # ...
}

def field(name: str) -> ArrowFieldSpec:
    return FIELD_CATALOG[name]


# src/cpg/registry_bundles.py
BUNDLE_CATALOG = {
    "file_identity": file_identity_bundle(include_sha256=False),
    "span": span_bundle(),
}

def bundle(name: str) -> FieldBundle:
    return BUNDLE_CATALOG[name]
```

**Target files**
- Add: `src/cpg/registry_fields.py`
- Add: `src/cpg/registry_bundles.py`
- Update: `src/cpg/schemas.py`

**Implementation checklist**
- [x] Define `FIELD_CATALOG` entries for all CPG fields.
- [x] Define `BUNDLE_CATALOG` for standard bundles.
- [x] Replace inline `ArrowFieldSpec` lists in CPG schemas.

**Status**
Completed.

---

### Scope 2: Dataset Rows + Templates
**Description**
Define each CPG dataset as a compact row spec referencing bundles/fields and
inheriting defaults from templates (ordering + metadata + constraints).

**Code patterns**
```python
# src/cpg/registry_rows.py
@dataclass(frozen=True)
class DatasetRow:
    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    constraints: TableSpecConstraints | None = None
    contract: ContractRow | None = None
    template: str | None = None


# src/cpg/registry_templates.py
TEMPLATES = {
    "cpg": RegistryTemplate(
        stage="cpg",
        ordering_level=OrderingLevel.IMPLICIT,
        determinism_tier="best_effort",
    )
}
```

**Target files**
- Add: `src/cpg/registry_rows.py`
- Add: `src/cpg/registry_templates.py`
- Update: `src/cpg/schemas.py`

**Implementation checklist**
- [x] Define `DatasetRow` and contract row helpers.
- [x] Add dataset rows for nodes/edges/props.
- [x] Encode template defaults (ordering + metadata + determinism).

**Status**
Completed.

---

### Scope 3: Programmatic Dataset Spec Builder + Registry API
**Description**
Build `TableSpec`, `ContractSpec`, and `DatasetSpec` from dataset rows using
ArrowDSL and schema_spec helpers to avoid inline declarations.

**Code patterns**
```python
# src/cpg/registry_builders.py
def build_dataset_spec(row: DatasetRow) -> DatasetSpec:
    table_spec = make_table_spec(
        name=row.name,
        version=row.version,
        bundles=tuple(bundle(name) for name in row.bundles),
        fields=tuple(field(name) for name in row.fields),
        constraints=row.constraints,
    )
    contract_spec = build_contract_spec(row.contract, table_spec=table_spec)
    registration = DatasetRegistration(
        contract_spec=contract_spec,
        metadata_spec=_metadata_spec(row),
    )
    return register_dataset(table_spec=table_spec, registration=registration)


# src/cpg/registry_specs.py
def dataset_spec(name: str) -> DatasetSpec:
    return DATASET_SPECS[name]
```

**Target files**
- Add: `src/cpg/registry_builders.py`
- Add: `src/cpg/registry_specs.py`
- Update: `src/cpg/schemas.py`

**Implementation checklist**
- [x] Implement builders for table/contract/metadata specs.
- [x] Expose dataset accessors (`dataset_spec`, `dataset_schema`, `dataset_contract_spec`).
- [x] Replace static `CPG_*_SPEC` declarations with registry lookups.

**Status**
Completed.

---

### Scope 4: Plan Specs As Arrow Tables (Node/Edge/Prop)
**Description**
Store node/edge/prop plan specs in Arrow tables and decode them using
`arrowdsl.spec.tables.cpg` utilities. This makes plan specs data-driven.

**Code patterns**
```python
# src/cpg/spec_tables.py
NODE_PLAN_TABLE = node_plan_table(node_plan_specs())
EDGE_PLAN_TABLE = edge_plan_table(edge_plan_specs())
PROP_TABLE_TABLE = prop_table_table(prop_table_specs())

NODE_PLAN_SPECS = node_plan_specs_from_table(NODE_PLAN_TABLE)
```

**Target files**
- Add: `src/cpg/spec_tables.py`
- Update: `src/cpg/spec_registry.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

**Implementation checklist**
- [x] Build plan spec tables with `arrowdsl/spec/tables/cpg.py`.
- [x] Use `*_from_table` decoders as the source of truth.
- [x] Wire builders to accept optional spec tables for overrides.

**Status**
Completed.

---

### Scope 5: Contracts + Derivations from Registry Tables
**Description**
Treat contract + derivation tables as canonical, feeding property validation and
spec generation instead of static maps.

**Code patterns**
```python
# src/cpg/contract_registry.py
NODE_CONTRACT_TABLE = node_contract_table()
EDGE_CONTRACT_TABLE = edge_contract_table()
DERIVATION_TABLE = derivation_table()

NODE_CONTRACTS = node_contracts_from_table(NODE_CONTRACT_TABLE)
```

**Target files**
- Add: `src/cpg/contract_registry.py`
- Update: `src/cpg/contract_map.py`
- Update: `src/cpg/spec_registry.py`

**Implementation checklist**
- [x] Decode node/edge contracts from `cpg_registry` tables.
- [x] Use contract tables as the validation source for prop mappings.
- [x] Expose derivation table for registry inspection + audit.

**Status**
Completed.

---

### Scope 6: Relation Rules As Data (ExprIR + Relspec Tables)
**Description**
Move relation definitions to Arrow tables using relspec rule schemas and ExprIR.
Compile rules to `Plan`s instead of static Python in `cpg/relations.py`.

**Code patterns**
```python
# src/cpg/relation_registry.py
RULE_TABLE = relationship_rule_table(RULE_ROWS)
RULES = relationship_rules_from_table(RULE_TABLE)

def build_relations(catalog: PlanCatalog, *, ctx: ExecutionContext) -> tuple[Plan, ...]:
    return tuple(rule.build_plan(catalog=catalog, ctx=ctx) for rule in RULES)
```

**Target files**
- Add: `src/cpg/relation_registry.py`
- Remove: `src/cpg/relations.py`
- Update: `src/cpg/build_edges.py`

**Implementation checklist**
- [x] Encode relation kernels in `relspec` table format.
- [x] Use ExprIR for derived columns, filters, and scalars.
- [x] Replace static relation builders with rule-driven compilation.

**Status**
Completed.

---

### Scope 7: OO Registry Facade
**Description**
Provide a `CpgRegistry` class that owns spec tables, resolves lookups, and
exposes typed accessors. This centralizes configuration without static globals.

**Code patterns**
```python
# src/cpg/registry.py
@dataclass(frozen=True)
class CpgRegistry:
    dataset_specs: Mapping[str, DatasetSpec]
    node_plan_spec_table: pa.Table
    edge_plan_spec_table: pa.Table
    prop_table_spec_table: pa.Table

    def dataset_spec(self, name: str) -> DatasetSpec:
        return self.dataset_specs[name]
```

**Target files**
- Add: `src/cpg/registry.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

**Implementation checklist**
- [x] Add registry facade and default builder.
- [x] Route builders through registry accessors.
- [x] Keep global constants only as thin defaults.

**Status**
Completed.
