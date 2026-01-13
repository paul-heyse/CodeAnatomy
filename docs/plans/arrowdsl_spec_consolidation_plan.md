## ArrowDSL Spec Consolidation Plan

### Goals
- Consolidate duplicated PyArrow spec-table logic from `src/relspec`, `src/schema_spec`, and
  `src/cpg` into ArrowDSL as canonical helpers.
- Standardize spec encoding/decoding, schema structs, and validation across domains.
- Preserve existing behavior while reducing repeated parse/codec logic.
- Keep downstream APIs stable via thin re-exports in domain modules.

### Constraints
- Maintain strict typing and Ruff compliance (no suppressions).
- Preserve spec table schemas and metadata semantics (no breaking format changes).
- Keep domain-specific models in their current packages; only move Arrow plumbing.
- No relative imports; keep modules fully typed and mypy/pyright clean.

---

### Scope 1: Shared Spec Codec Utilities
**Description**
Create ArrowDSL utilities for common parsing and encoding behaviors shared across
spec tables (sort order parsing, list/tuple parsing, dedupe strategy parsing, and
scalar literal JSON encoding).

**Code patterns**
```python
# src/arrowdsl/spec/codec.py
def parse_sort_order(value: object) -> Literal["ascending", "descending"]:
    normalized = "ascending" if value is None else str(value).lower()
    if normalized in ("ascending", "descending"):
        return cast("Literal['ascending', 'descending']", normalized)
    raise ValueError(f"Unsupported sort order: {value!r}")


def encode_scalar_json(value: ScalarValue | None) -> str | None:
    payload = encode_scalar_payload(value)
    return None if payload is None else json.dumps(payload, ensure_ascii=True)
```

**Target files**
- Add: `src/arrowdsl/spec/codec.py`
- Update: `src/relspec/spec_tables.py`
- Update: `src/schema_spec/spec_tables.py`
- Update: `src/cpg/spec_tables.py`
- Update: `src/arrowdsl/spec/expr_ir.py` (reuse scalar codec)

**Implementation checklist**
- [ ] Add shared parse helpers (`parse_sort_order`, `parse_string_tuple`, `parse_mapping_sequence`).
- [ ] Add scalar literal JSON codec helpers and reuse in relspec + cpg spec tables.
- [ ] Replace local parsing/encoding helpers in each domain spec table module.

**Status**
Not started.

---

### Scope 2: Canonical Spec Structs
**Description**
Centralize shared struct definitions used across spec tables (SortKey, Dedupe,
Validation, and DatasetRef) so all spec tables reference a single ArrowDSL source.

**Code patterns**
```python
# src/arrowdsl/spec/structs.py
SORT_KEY_STRUCT = pa.struct(
    [
        pa.field("column", pa.string(), nullable=False),
        pa.field("order", pa.string(), nullable=False),
    ]
)

DEDUPE_STRUCT = pa.struct(
    [
        pa.field("keys", pa.list_(pa.string()), nullable=False),
        pa.field("tie_breakers", pa.list_(SORT_KEY_STRUCT), nullable=True),
        pa.field("strategy", pa.string(), nullable=False),
    ]
)
```

**Target files**
- Add: `src/arrowdsl/spec/structs.py`
- Update: `src/relspec/spec_tables.py`
- Update: `src/schema_spec/spec_tables.py`
- Update: `src/cpg/spec_tables.py`

**Implementation checklist**
- [ ] Define shared struct schemas in ArrowDSL.
- [ ] Replace duplicate struct definitions in domain modules.
- [ ] Keep schema metadata identical to current outputs.

**Status**
Not started.

---

### Scope 3: Spec Table Codec Base
**Description**
Introduce a reusable spec-table codec that handles table building, JSON/IPC IO,
and validation using `SpecTableSpec`.

**Code patterns**
```python
# src/arrowdsl/spec/tables/base.py
@dataclass(frozen=True)
class SpecTableCodec(Generic[T]):
    schema: pa.Schema
    spec: SpecTableSpec
    encode_row: Callable[[T], dict[str, object]]
    decode_row: Callable[[Mapping[str, object]], T]

    def to_table(self, values: Sequence[T]) -> pa.Table:
        rows = [self.encode_row(value) for value in values]
        return pa.Table.from_pylist(rows, schema=self.schema)
```

**Target files**
- Add: `src/arrowdsl/spec/tables/base.py`
- Update: `src/arrowdsl/spec/core.py`
- Update: `src/arrowdsl/spec/io.py`

**Implementation checklist**
- [ ] Add a generic codec layer for spec tables.
- [ ] Integrate ArrowDSL validation (`SpecTableSpec.validate`) into the base.
- [ ] Provide common JSON/IPC IO helpers for any spec table.

**Status**
Not started.

---

### Scope 4: Migrate Schema Spec Tables into ArrowDSL
**Description**
Move Arrow schema spec tables into ArrowDSL as canonical modules, then re-export
from `src/schema_spec/spec_tables.py` to preserve existing import surfaces.

**Code patterns**
```python
# src/arrowdsl/spec/tables/schema.py
FIELD_SPEC_SCHEMA = pa.schema([...], metadata={b"spec_kind": b"schema_fields"})

def field_spec_table(specs: Sequence[TableSchemaSpec]) -> pa.Table:
    rows = [...]
    return pa.Table.from_pylist(rows, schema=FIELD_SPEC_SCHEMA)
```

**Target files**
- Add: `src/arrowdsl/spec/tables/schema.py`
- Update: `src/schema_spec/spec_tables.py` (re-export + thin wrappers)

**Implementation checklist**
- [ ] Move schema spec table schemas and codecs into ArrowDSL.
- [ ] Replace local helper functions with `arrowdsl.spec.codec` utilities.
- [ ] Keep `schema_spec/spec_tables.py` as a compatibility layer.

**Status**
Not started.

---

### Scope 5: Migrate Relationship Spec Tables into ArrowDSL
**Description**
Consolidate relspec spec-table schemas and codecs into ArrowDSL while keeping
relspec domain logic in `src/relspec/model.py`.

**Code patterns**
```python
# src/arrowdsl/spec/tables/relspec.py
RULES_SCHEMA = pa.schema([...], metadata={b"spec_kind": b"relationship_rules"})

def relationship_rule_table(rules: Sequence[RelationshipRule]) -> pa.Table:
    rows = [encode_rule(rule) for rule in rules]
    return pa.Table.from_pylist(rows, schema=RULES_SCHEMA)
```

**Target files**
- Add: `src/arrowdsl/spec/tables/relspec.py`
- Update: `src/relspec/spec_tables.py` (re-export + thin wrappers)

**Implementation checklist**
- [ ] Move relspec spec-table schemas and row codecs into ArrowDSL.
- [ ] Reuse shared sort/dedupe/literal codecs from `arrowdsl.spec.codec`.
- [ ] Keep re-exports in `src/relspec/spec_tables.py`.

**Status**
Not started.

---

### Scope 6: Migrate CPG Spec Tables into ArrowDSL
**Description**
Consolidate CPG spec-table schemas and codecs into ArrowDSL and reuse shared
literal codecs and struct definitions.

**Code patterns**
```python
# src/arrowdsl/spec/tables/cpg.py
NODE_PLAN_SCHEMA = pa.schema([...], metadata={b"spec_kind": b"cpg_node_specs"})

def node_plan_table(specs: Sequence[NodePlanSpec]) -> pa.Table:
    rows = [encode_node(spec) for spec in specs]
    return pa.Table.from_pylist(rows, schema=NODE_PLAN_SCHEMA)
```

**Target files**
- Add: `src/arrowdsl/spec/tables/cpg.py`
- Update: `src/cpg/spec_tables.py` (re-export + thin wrappers)

**Implementation checklist**
- [ ] Move CPG spec-table schemas and codecs into ArrowDSL.
- [ ] Reuse ArrowDSL scalar literal codec for `literal_json`.
- [ ] Keep `cpg/spec_tables.py` as a thin compatibility layer.

**Status**
Not started.

---

### Scope 7: Plan/Schema Helper Consolidation
**Description**
Deduplicate shared plan and schema helper logic currently implemented in
`src/cpg/plan_helpers.py` by moving canonical versions into ArrowDSL.

**Code patterns**
```python
# src/arrowdsl/plan_helpers.py
def ensure_plan(source: PlanSource, *, ctx: ExecutionContext | None, label: str = "") -> Plan:
    if isinstance(source, Plan):
        return source
    if ctx is None:
        raise ValueError("ensure_plan requires ctx for dataset-backed sources.")
    return plan_from_source(source, ctx=ctx, label=label)
```

**Target files**
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/arrowdsl/schema/unify.py`
- Update: `src/cpg/plan_helpers.py` (re-export + wrappers)

**Implementation checklist**
- [ ] Move `ensure_plan`, `empty_plan`, `align_plan`, `finalize_plan` into ArrowDSL.
- [ ] Reuse `arrowdsl.schema.unify` for schema merging with metadata.
- [ ] Keep CPG helpers as wrappers to avoid breaking imports.

**Status**
Not started.

---

### Scope 8: Column Resolver / Builder Utilities
**Description**
Deduplicate generic column selection and dictionary handling utilities currently
in `src/cpg/builders.py` into ArrowDSL schema helpers.

**Code patterns**
```python
# src/arrowdsl/schema/builders.py
def pick_first(table: TableLike, cols: Sequence[str], *, default_type: DataTypeLike) -> ArrayLike:
    for col in cols:
        if col in table.column_names:
            return table[col]
    return pa.nulls(table.num_rows, type=default_type)
```

**Target files**
- Update: `src/arrowdsl/schema/builders.py`
- Update: `src/cpg/builders.py`

**Implementation checklist**
- [ ] Add generic helpers (`pick_first`, `resolve_string_col`, `resolve_float_col`).
- [ ] Add dictionary-encoding helper with type-aware fallback.
- [ ] Replace CPG local helpers with ArrowDSL equivalents.

**Status**
Not started.

---

### Scope 9: Compatibility and Re-Export Layer
**Description**
Keep domain modules stable by re-exporting ArrowDSL spec helpers and tables, so
call sites do not need immediate refactors.

**Code patterns**
```python
# src/schema_spec/spec_tables.py
from arrowdsl.spec.tables.schema import (FIELD_SPEC_SCHEMA, field_spec_table, ...)
```

**Target files**
- Update: `src/schema_spec/spec_tables.py`
- Update: `src/relspec/spec_tables.py`
- Update: `src/cpg/spec_tables.py`
- Update: `src/arrowdsl/spec/__init__.py`

**Implementation checklist**
- [ ] Replace direct implementations with imports from ArrowDSL.
- [ ] Preserve public constants and function names.
- [ ] Add deprecation notes in docstrings where appropriate.

**Status**
Not started.
