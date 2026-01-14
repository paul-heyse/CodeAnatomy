## CPG Kind Registry Programmatic Refactor Plan

### Goals
- Replace static, repetitive registry entries with programmatic templates and catalogs.
- Preserve the current public API and semantics for kind contracts and derivations.
- Keep the core registry portable (stdlib-only) while enabling ArrowDSL exports.
- Improve maintainability and prevent enum/prop drift across kinds.

### Constraints
- Keep `NodeKind`, `EdgeKind`, `SourceKind`, and contract/derivation dataclasses stable.
- Preserve `NODE_KIND_CONTRACTS`, `EDGE_KIND_CONTRACTS`, `NODE_DERIVATIONS`, `EDGE_DERIVATIONS` exports.
- Avoid relative imports; keep modules fully typed (pyright strict + pyrefly).
- Ensure any ArrowDSL/pyarrow usage lives outside the stdlib-only core registry.

---

### Scope 1: Property Catalog + Enum Bundles
**Description**
Create a canonical property spec catalog and reusable bundles for repeated prop groups.

**Code patterns**
```python
PROP_ENUMS = {
    "def_kind": ("function", "class", "lambda"),
    "expr_context": ("LOAD", "STORE", "DEL", "UNKNOWN"),
    "use_kind": ("read", "write", "del", "call", "import", "other"),
}

PROP_SPECS = {
    "name": p_str("Identifier name"),
    "def_kind": p_str(enum=PROP_ENUMS["def_kind"]),
    "expr_context": p_str(enum=PROP_ENUMS["expr_context"]),
}

PROP_BUNDLES = {
    "anchor": ("line_start", "col_start", "line_end", "col_end"),
    "sym_flags": (
        "is_local",
        "is_global",
        "is_nonlocal",
        "is_free",
        "is_parameter",
        "is_imported",
        "is_assigned",
        "is_referenced",
        "is_annotated",
        "is_namespace",
    ),
}
```

**Target files**
- Update: `src/cpg/kinds_ultimate.py`
- Add: `src/cpg/kinds_registry_props.py`

**Implementation checklist**
- [ ] Define canonical enum tuples for repeated `enum=` lists.
- [ ] Create a `PROP_SPECS` registry for all property keys.
- [ ] Add `PROP_BUNDLES` for frequently reused prop groupings.
- [ ] Replace inline `p_str/p_int/p_bool` blocks with catalog lookups.

**Status**
Planned.

---

### Scope 2: Contract Templates + Row-Driven Builders
**Description**
Define reusable contract templates and build contracts from compact row specs.

**Code patterns**
```python
@dataclass(frozen=True)
class NodeContractRow:
    kind: NodeKind
    required: tuple[str, ...]
    optional: tuple[str, ...] = ()
    template: str = ""
    description: str = ""

NODE_TEMPLATES = {
    "cst": NodeContractTemplate(
        requires_anchor=True,
        allowed_sources=(SourceKind.LIBCST,),
        optional=PROP_BUNDLES["anchor"],
    ),
}

NODE_CONTRACT_ROWS = (
    NodeContractRow(
        kind=NodeKind.CST_DEF,
        required=("name", "def_kind"),
        optional=("is_async", "docstring", "container_def_id", "qnames"),
        template="cst",
        description="High-signal definition node.",
    ),
)

NODE_KIND_CONTRACTS = build_node_contracts(
    rows=NODE_CONTRACT_ROWS,
    templates=NODE_TEMPLATES,
    props=PROP_SPECS,
)
```

**Target files**
- Update: `src/cpg/kinds_ultimate.py`
- Add: `src/cpg/kinds_registry_builders.py`

**Implementation checklist**
- [ ] Add `NodeContractTemplate` and `EdgeContractTemplate` dataclasses.
- [ ] Define row specs for each kind group (CST/AST/SCIP/etc.).
- [ ] Implement `build_node_contracts`/`build_edge_contracts` utilities.
- [ ] Replace static dict blocks with row-driven builders.

**Status**
Planned.

---

### Scope 3: Derivation Templates + Generator Rows
**Description**
Replace repeated derivation entries with template-driven generators.

**Code patterns**
```python
@dataclass(frozen=True)
class DerivationRow:
    kind: NodeKind | EdgeKind
    provider: str
    id_prefix: str
    join_keys: tuple[str, ...] = ()
    status: DerivationStatus = "implemented"
    notes: str = ""

CST_DERIVATION = DerivationTemplate(
    extractor="extract.cst_extract:extract_cst_tables",
    confidence_policy="confidence=1.0",
    ambiguity_policy="none",
)

NODE_DERIVATIONS = build_derivations(
    rows=(
        DerivationRow(NodeKind.CST_DEF, "LibCST FunctionDef/ClassDef/Lambda", "CST_DEF"),
        DerivationRow(NodeKind.CST_NAME_REF, "LibCST Name + ExprContext", "CST_NAME_REF"),
    ),
    template=CST_DERIVATION,
)
```

**Target files**
- Update: `src/cpg/kinds_ultimate.py`
- Add: `src/cpg/kinds_registry_derivations.py`

**Implementation checklist**
- [ ] Add `DerivationTemplate` with defaults per extractor family.
- [ ] Define `DerivationRow` lists per group (CST/AST/SCIP/symtable/dis/runtime).
- [ ] Generate `NODE_DERIVATIONS`/`EDGE_DERIVATIONS` from rows.
- [ ] Preserve current status/notes text and join key semantics.

**Status**
Planned.

---

### Scope 4: Declarative ID + Join-Key Helpers
**Description**
Standardize ID recipes and join key specs through shared helpers.

**Code patterns**
```python
def node_id_recipe(prefix: str) -> str:
    return f"node_id = stable_id(path, bstart, bend, '{prefix}')"

EDGE_JOIN_KEYS = {
    "span_contained": (
        "interval_align: left.path == right.path",
        "interval_align: right.bstart>=left.bstart AND right.bend<=left.bend (CONTAINED_BEST)",
    ),
}
```

**Target files**
- Update: `src/cpg/kinds_ultimate.py`
- Add: `src/cpg/kinds_registry_ids.py`

**Implementation checklist**
- [ ] Add helpers for standard node/edge ID recipe strings.
- [ ] Define shared join-key macros for interval alignment patterns.
- [ ] Replace repeated join/id strings with helpers.

**Status**
Planned.

---

### Scope 5: ArrowDSL Registry Export Tables
**Description**
Expose contracts and derivations as Arrow tables for downstream analysis and validation.

**Code patterns**
```python
NODE_CONTRACT_SCHEMA = pa.schema(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("requires_anchor", pa.bool_(), nullable=False),
        pa.field("required_props", pa.list_(pa.string()), nullable=False),
        pa.field("optional_props", pa.list_(pa.string()), nullable=False),
    ],
    metadata={b"spec_kind": b"cpg_node_contracts"},
)

def node_contract_table() -> pa.Table:
    rows = [
        {
            "kind": kind.value,
            "requires_anchor": contract.requires_anchor,
            "required_props": list(contract.required_props.keys()),
            "optional_props": list(contract.optional_props.keys()),
        }
        for kind, contract in NODE_KIND_CONTRACTS.items()
    ]
    return pa.Table.from_pylist(rows, schema=NODE_CONTRACT_SCHEMA)
```

**Target files**
- Add: `src/arrowdsl/spec/tables/cpg_registry.py`
- Update: `src/arrowdsl/spec/__init__.py`
- Update: `src/cpg/kinds_ultimate.py`

**Implementation checklist**
- [ ] Define Arrow schemas for node/edge contracts and derivations.
- [ ] Export `node_contract_table`/`edge_contract_table`/`derivation_table` helpers.
- [ ] Keep ArrowDSL exports optional to preserve stdlib-only registry core.

**Status**
Planned.

---

### Scope 6: Compatibility + Validation Harness
**Description**
Ensure the programmatic registry matches existing behavior and validations.

**Code patterns**
```python
validate_registry_completeness()
validate_derivations_implemented_only(allow_planned=True)
```

**Target files**
- Update: `src/cpg/kinds_ultimate.py`
- Update: `src/relspec/edge_contract_validator.py`

**Implementation checklist**
- [ ] Preserve `registry_to_jsonable` output shape.
- [ ] Keep import-time completeness checks and derivation validation.
- [ ] Ensure downstream validators consume identical contract/derivation data.

**Status**
Planned.
