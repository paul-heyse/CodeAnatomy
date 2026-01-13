## Pydantic to ArrowDSL Spec Migration Plan

### Goals
- Remove Pydantic usage and model validation in favor of Arrow-native spec tables.
- Keep spec validation fully vectorized and Acero-friendly with plan-lane checks.
- Use ArrowDSL as the canonical spec runtime (schemas, constraints, query specs, registries).
- Leverage advanced PyArrow features (union/struct types, dictionary encoding, options objects).
- Preserve existing behavior while enabling streaming validation and error artifacts.

### Constraints
- Do not store docstrings in Arrow metadata.
- Keep spec IO and validation Arrow-native (no new non-Arrow dependencies).
- Maintain strict typing and Ruff compliance (no suppressions).
- Preserve existing contract semantics and schema metadata round-trip behavior.

---

### Scope 1: ArrowDSL Spec Table Foundation

### Description
Introduce a canonical spec-table layer in ArrowDSL to replace Pydantic models. This
layer defines spec schemas, validation rules, and a uniform error-reporting surface.

### Code patterns
```python
# src/arrowdsl/spec/core.py
from dataclasses import dataclass

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.schema import SchemaTransform
from arrowdsl.schema.validation import ArrowValidationOptions, ValidationReport, validate_table


@dataclass(frozen=True)
class SpecTableSpec:
    name: str
    schema: SchemaLike
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()

    def align(self, table: TableLike) -> TableLike:
        transform = SchemaTransform(schema=self.schema, safe_cast=True, keep_extra_columns=False)
        return transform.apply(table)

    def validate(self, table: TableLike, *, ctx: ExecutionContext) -> ValidationReport:
        options = ArrowValidationOptions.from_policy(ctx.schema_validation)
        return validate_table(table, spec=self.to_table_spec(), options=options, ctx=ctx)
```

### Target files
- `src/arrowdsl/spec/core.py` (new)
- `src/arrowdsl/spec/validators.py` (new)
- `src/arrowdsl/spec/__init__.py` (new)

### Implementation checklist
- [x] Add `SpecTableSpec` and `SpecValidationSuite` types.
- [x] Provide `align()` and `validate()` helpers using ArrowDSL validation.
- [x] Encode required/key constraints into spec-table metadata (no docstrings).

---

### Scope 2: Expression IR + Registry Integration

### Description
Replace Pydantic-held expressions and callables with Arrow IR columns and registries.
Compile IR to `ExprSpec`/`ComputeExpression` using ArrowDSL registries.

### Code patterns
```python
# src/arrowdsl/spec/expr_ir.py
from arrowdsl.compute.expr import ExprSpec
from arrowdsl.compute.expr_specs import ComputeExprSpec
from arrowdsl.core.interop import pc


def expr_from_ir(row: dict[str, object]) -> ExprSpec:
    op = row["op"]
    if op == "field":
        return ComputeExprSpec(expression=pc.field(row["name"]))
    if op == "literal":
        return ComputeExprSpec(expression=pc.scalar(row["value"]))
    raise ValueError(f"Unknown expr op: {op}")
```

### Target files
- `src/arrowdsl/spec/expr_ir.py` (new)
- `src/arrowdsl/compute/registry.py` (existing)
- `src/arrowdsl/plan/catalog.py` (existing)

### Implementation checklist
- [x] Define JSON-based expression IR with compile helpers.
- [ ] Define a compact expression IR schema (struct columns).
- [ ] Compile IR to `ExprSpec` using ArrowDSL compute registries.
- [x] Replace callables in specs with registry IDs.

---

### Scope 3: SchemaSpec Migration (schema_spec)

### Description
Replace Pydantic schema spec models with Arrow spec tables and dataclasses. Ensure
constraints and ordering metadata round-trip through Arrow metadata.

### Code patterns
```python
# src/schema_spec/spec_tables.py
SPEC_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("field_name", pa.string(), nullable=False),
        pa.field("field_type", pa.string(), nullable=False),
        pa.field("nullable", pa.bool_(), nullable=False),
        pa.field("encoding", pa.dictionary(pa.int8(), pa.string())),
    ],
    metadata={b"spec_kind": b"schema_fields"},
)
```

### Target files
- `src/schema_spec/specs.py` (refactor out Pydantic)
- `src/schema_spec/system.py`
- `src/schema_spec/spec_tables.py` (new)
- `src/arrowdsl/spec/core.py`

### Implementation checklist
- [x] Introduce Arrow spec tables for field specs, constraints, and contracts.
- [x] Compile spec tables to `TableSchemaSpec` and `DatasetSpec`.
- [x] Preserve metadata round-trip for required/key fields and ordering.

---

### Scope 4: Relationship Spec Migration (relspec)

### Description
Replace Pydantic relationship rules with Arrow spec tables. Model discriminated
config variants using struct columns (or union types where appropriate).

### Code patterns
```python
# src/relspec/spec_tables.py
RULES_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("kind", pa.dictionary(pa.int8(), pa.string()), nullable=False),
        pa.field("output_dataset", pa.string(), nullable=False),
        pa.field("hash_join", pa.struct([...]), nullable=True),
        pa.field("interval_align", pa.struct([...]), nullable=True),
        pa.field("winner_select", pa.struct([...]), nullable=True),
    ],
    metadata={b"spec_kind": b"relationship_rules"},
)
```

### Target files
- `src/relspec/model.py` (replace Pydantic models)
- `src/relspec/spec_tables.py` (new)
- `src/relspec/compiler.py` (compile from spec tables)

### Implementation checklist
- [x] Define Arrow schemas for relationship rule tables.
- [ ] Add validators enforcing exactly one variant per rule kind.
- [x] Compile spec tables to JoinSpec/Plan pipelines.

---

### Scope 5: CPG Spec Migration (cpg/specs)

### Description
Replace CPG Pydantic specs with Arrow spec tables and registry IDs. Express all
callables as registry keys and compile to runtime plan specs.

### Code patterns
```python
# src/cpg/spec_tables.py
EDGE_SPEC_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("edge_kind", pa.string(), nullable=False),
        pa.field("relation_getter_id", pa.string(), nullable=False),
        pa.field("filter_id", pa.string()),
        pa.field("src_cols", pa.list_(pa.string()), nullable=False),
        pa.field("dst_cols", pa.list_(pa.string()), nullable=False),
    ]
)
```

### Target files
- `src/cpg/specs.py` (replace Pydantic)
- `src/cpg/spec_tables.py` (new)
- `src/cpg/registry.py` (registry resolution)

### Implementation checklist
- [x] Convert plan getter/filter/transform callables to registry IDs.
- [x] Build Arrow spec tables for node/edge/prop specs.
- [ ] Add spec validators for literal vs source column constraints.

---

### Scope 6: Spec IO + Serialization (Arrow-First)

### Description
Provide a uniform IO surface for spec tables using Arrow IPC/Parquet/JSON. All IO
should be schema-driven and Arrow-native.

### Code patterns
```python
# src/arrowdsl/spec/io.py
import pyarrow.ipc as ipc


def write_spec_table(path: str, table: pa.Table) -> None:
    with ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)
```

### Target files
- `src/arrowdsl/spec/io.py` (new)
- `src/arrowdsl/spec/__init__.py`

### Implementation checklist
- [x] Add IPC writer/reader helpers for spec tables.
- [x] Add JSON ingestion helpers with explicit schema when needed.
- [ ] Provide stable ordering helpers for deterministic spec outputs.

---

### Scope 7: Migration Adapters + Pydantic Removal

### Description
Add adapters that convert existing Pydantic configs to spec tables, then remove
Pydantic imports and dependencies once parity is reached.

### Code patterns
```python
# src/arrowdsl/spec/adapters.py
def contract_specs_to_table(specs: Sequence[object]) -> pa.Table:
    rows = [{"name": spec.name, "version": spec.version} for spec in specs]
    return pa.Table.from_pylist(rows, schema=CONTRACT_SCHEMA)
```

### Target files
- `src/arrowdsl/spec/adapters.py` (new)
- `src/schema_spec/system.py`
- `src/relspec/compiler.py`
- `src/cpg/specs.py`

### Implementation checklist
- [ ] Provide adapter functions for each spec domain.
- [ ] Switch spec-loading paths to use spec tables and registries.
- [x] Remove Pydantic imports and dependency from the project.
