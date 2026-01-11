# Implementation Plan: Pydantic + Pandera Schema System (Arrow-Centric)

This plan replaces bespoke schema generation, transformation, inference, and validation
with Pydantic v2 specs and Pandera validation while keeping Arrow as the execution core.
It also refactors CPG builders (edges/nodes/props) into spec-driven OO engines with
minimal bespoke logic.

## Status Summary
- Scope 1: Completed
- Scope 2: Completed
- Scope 3: Completed (Pandera inference uses pandas backend)
- Scope 4: Completed
- Scope 5: Completed
- Scope 6: Completed
- Scope 7: Completed
- Scope 8: Completed
- Scope 9: Completed (tests added; obs schema serialization reviewed, no changes needed)

---

## Scope 1: Schema Spec Layer (Pydantic v2)

### Description
Introduce a canonical schema spec layer using Pydantic models. These specs generate
Arrow schemas, enforce definition integrity, and become the single source of truth
for contracts and validation policies.

### Code patterns (implemented)
```python
# src/schema_spec/core.py
from __future__ import annotations

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


class ArrowFieldSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)
    name: str
    dtype: pa.DataType
    nullable: bool = True
    metadata: dict[str, str] = Field(default_factory=dict)

    def to_arrow_field(self) -> pa.Field:
        metadata = {str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in self.metadata.items()}
        return pa.field(self.name, self.dtype, nullable=self.nullable, metadata=metadata)


class TableSchemaSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)
    name: str
    fields: list[ArrowFieldSpec]
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()

    @field_validator("fields")
    @classmethod
    def _unique_field_names(cls, fields: list[ArrowFieldSpec]) -> list[ArrowFieldSpec]:
        names = [f.name for f in fields]
        if len(set(names)) != len(names):
            raise ValueError("duplicate field names detected.")
        return fields

    @field_validator("required_non_null", "key_fields")
    @classmethod
    def _validate_field_refs(cls, v: tuple[str, ...], info: ValidationInfo):
        names = {f.name for f in info.data["fields"]}
        missing = [c for c in v if c not in names]
        if missing:
            raise ValueError(f"unknown fields: {missing}")
        return v

    def to_arrow_schema(self) -> pa.Schema:
        return pa.schema([field.to_arrow_field() for field in self.fields])
```

```python
# src/schema_spec/contracts.py
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator

from arrowdsl.contracts import Contract, DedupeSpec, SortKey
from schema_spec.core import TableSchemaSpec


class ContractSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    table_schema: TableSchemaSpec
    dedupe: DedupeSpec | None = None
    canonical_sort: tuple[SortKey, ...] = ()
    version: int | None = None
    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None

    @field_validator("virtual_field_docs")
    @classmethod
    def _docs_match_virtual_fields(
        cls,
        value: dict[str, str] | None,
        info: ValidationInfo,
    ) -> dict[str, str] | None:
        if value is None:
            return None
        virtual_fields: tuple[str, ...] = info.data.get("virtual_fields", ())
        missing = [key for key in value if key not in virtual_fields]
        if missing:
            raise ValueError(f"virtual_field_docs keys missing in virtual_fields: {missing}")
        return value

    def to_contract(self) -> Contract:
        return Contract(
            name=self.name,
            schema=self.table_schema.to_arrow_schema(),
            schema_spec=self.table_schema,
            key_fields=self.table_schema.key_fields,
            required_non_null=self.table_schema.required_non_null,
            dedupe=self.dedupe,
            canonical_sort=self.canonical_sort,
            version=self.version,
            virtual_fields=self.virtual_fields,
            virtual_field_docs=self.virtual_field_docs,
        )
```

### Target files
- `src/schema_spec/core.py` (new)
- `src/schema_spec/contracts.py` (new)
- `src/arrowdsl/contracts.py` (consume ContractSpec output)
- `src/cpg/schemas.py` (replace static schemas/contracts with specs)
- `src/hamilton_pipeline/modules/cpg_build.py` (relationship schemas as specs)
- `src/extract/*` and `src/normalize/*` (schema constants replaced by specs)

### Integration checklist
- [x] Add schema spec models with Pydantic validators.
- [x] Convert CPG schemas/contracts to spec instances + `to_contract`.
- [x] Add helpers to emit Arrow schemas from spec objects.
- [x] Ensure contract metadata (name/version) preserved in `to_contract`.

---

## Scope 2: Pandera Adapters (Arrow <-> Pandera)

### Description
Add conversion utilities between `TableSchemaSpec` and Pandera schemas, and provide
Arrow validation via Polars backend.

### Code patterns (implemented)
```python
# src/schema_spec/pandera_adapter.py
from __future__ import annotations

import pandera.polars as pa_pl
import polars as pl
import pyarrow as pa

from schema_spec.core import TableSchemaSpec


def table_spec_to_pandera(
    spec: TableSchemaSpec,
    *,
    strict: bool | Literal["filter"] = "filter",
    coerce: bool = False,
) -> pa_pl.DataFrameSchema:
    columns = {
        f.name: pa_pl.Column(
            dtype=_arrow_dtype_to_pandera(f.dtype),
            nullable=f.nullable,
            required=True,
        )
        for f in spec.fields
    }
    return pa_pl.DataFrameSchema(columns=columns, strict=strict, coerce=coerce)


def validate_arrow_table(
    table: pa.Table,
    *,
    spec: TableSchemaSpec,
    strict: bool | Literal["filter"] = "filter",
    coerce: bool = False,
    lazy: bool = True,
) -> pa.Table:
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        raise TypeError("Expected a polars DataFrame from Arrow input.")
    schema = table_spec_to_pandera(spec, strict=strict, coerce=coerce)
    validated = schema.validate(df, lazy=lazy)
    if isinstance(validated, pl.LazyFrame):
        validated = validated.collect()
    if not isinstance(validated, pl.DataFrame):
        raise TypeError("Expected a polars DataFrame after validation.")
    return validated.to_arrow()
```

### Target files
- `src/schema_spec/pandera_adapter.py` (new)
- `src/arrowdsl/finalize.py` (optional: validate at contract boundary)
- `src/normalize/schema_infer.py` (use Pandera inference to augment)

### Integration checklist
- [x] Implement Arrow -> Polars -> Pandera -> Arrow pipeline.
- [x] Provide dtype conversion helper for Arrow to Pandera (DataTypeClass mapping, list -> List/Object).
- [x] Validate on finalize (configurable via ExecutionContext).

---

## Scope 3: Schema Inference Augmentation (Pandera + Arrow)

### Description
Use Pandera inference (via pandas backend) to augment Arrow schema inference and then unify
schemas with Arrow-native logic for nested types.

### Code patterns (implemented)
```python
# src/normalize/schema_infer.py
import pandas as pd
import pandera.pandas as pa_pd
import pyarrow as pa


def infer_schema_from_tables(tables: Sequence[pa.Table], opts: SchemaInferOptions | None = None) -> pa.Schema:
    arrow_schema = unify_schemas([t.schema for t in tables if t is not None], opts=opts)
    if not tables:
        return arrow_schema

    sample = next((t for t in tables if t is not None and t.num_rows), None)
    if sample is None:
        return arrow_schema

    df = pd.DataFrame(sample.to_pydict())
    pan_schema = pa_pd.infer_schema(df)
    pan_arrow = pandera_schema_to_arrow(pan_schema)
    merged = pa.unify_schemas([arrow_schema, pan_arrow], promote_options=opts.promote_options)
    return _prefer_arrow_nested(arrow_schema, merged)
```

### Target files
- `src/normalize/schema_infer.py`
- `src/schema_spec/pandera_adapter.py` (add `pandera_schema_to_arrow`)

### Integration checklist
- [x] Add Pandera inference path with pandas backend (Polars inference not available).
- [x] Merge inferred schema with Arrow unified schema.
- [x] Keep Arrow nested types authoritative when conflicts arise.

---

## Scope 4: Contract Validation as OO (Pydantic Catalogs)

### Description
Replace contract registry validation and rule checks with Pydantic catalog models.
This unifies schema/contract validation with rule config validation.

### Code patterns
```python
# src/schema_spec/catalogs.py
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator
from schema_spec.contracts import ContractSpec


class ContractCatalogSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    contracts: dict[str, ContractSpec]

    @field_validator("contracts")
    @classmethod
    def _names_match(cls, v):
        for name, spec in v.items():
            if name != spec.name:
                raise ValueError(f"contract key mismatch: {name} != {spec.name}")
        return v
```

### Target files
- `src/relspec/registry.py` (consume ContractCatalogSpec)
- `src/relspec/edge_contract_validator.py` (use spec-derived contracts)
- `src/relspec/model.py` (optionally move to Pydantic models)

### Integration checklist
- [x] Add Pydantic contract catalog spec.
- [x] Replace registry validation with Pydantic validation.
- [x] Ensure edge-kind contract checks operate on spec-derived contracts.

---

## Scope 5: OO Builder System for Nodes/Edges/Props

### Description
Refactor CPG builders into a spec-driven OO system that emits Arrow tables using
generic emit engines.

### Code patterns (implemented)
```python
# src/cpg/specs.py
from __future__ import annotations

from pydantic import BaseModel, ConfigDict
from cpg.kinds import EdgeKind, NodeKind


class EdgeEmitSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    edge_kind: EdgeKind
    src_cols: tuple[str, ...]
    dst_cols: tuple[str, ...]
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)
    origin: str
    default_resolution_method: str


class NodeEmitSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    node_kind: NodeKind
    id_cols: tuple[str, ...]
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)
    file_id_cols: tuple[str, ...] = ("file_id",)
```

```python
# src/cpg/builders.py
class EdgeBuilder:
    def __init__(self, *, emitters: Sequence[EdgePlanSpec], schema_version: int, edge_schema: pa.Schema) -> None:
        self._emitters = tuple(emitters)
        self._schema_version = schema_version
        self._edge_schema = edge_schema

    def build(self, *, tables: Mapping[str, pa.Table], options: object) -> list[pa.Table]:
        parts = []
        for emitter in self._emitters:
            if not getattr(options, emitter.option_flag, False):
                continue
            rel = emitter.relation_getter(tables)
            if rel is None or rel.num_rows == 0:
                continue
            parts.append(emit_edges_from_relation(rel, spec=emitter.emit, schema_version=self._schema_version))
        return parts
```

### Target files
- `src/cpg/build_edges.py` (replace ad-hoc emitters with spec engine)
- `src/cpg/build_nodes.py` (use NodeEmitSpec list + engine)
- `src/cpg/build_props.py` (PropEmitSpec list + engine)
- `src/cpg/specs.py` (new)
- `src/cpg/builders.py` (new)

### Integration checklist
- [x] Define Pydantic specs for nodes/edges/props emission.
- [x] Build generic emit engines that map specs to Arrow kernels.
- [x] Replace per-edge and per-node bespoke logic with spec lists.
- [x] Keep Arrow performance-critical pieces in engine layer.

---

## Scope 6: Finalize + Validation Integration

### Description
Integrate Pandera validation at contract boundaries with configurable trust modes.

### Code patterns (implemented)
```python
# src/arrowdsl/finalize.py
from schema_spec.pandera_adapter import validate_arrow_table

def finalize(table: pa.Table, *, contract: Contract, ctx: ExecutionContext) -> FinalizeResult:
    if ctx.schema_validation.enabled and contract.schema_spec is not None:
        table = validate_arrow_table(
            table,
            spec=contract.schema_spec,
            strict=ctx.schema_validation.strict,
            coerce=ctx.schema_validation.coerce,
            lazy=ctx.schema_validation.lazy,
        )
    # existing align/dedupe/canonical sort logic...
```

### Target files
- `src/arrowdsl/finalize.py`
- `src/schema_spec/pandera_adapter.py`

### Integration checklist
- [x] Add validation hook with configuration in ExecutionContext.
- [x] Ensure nested Arrow types are skipped or treated as object in Pandera.
- [x] Keep strict/lax validation knobs explicit in config.

---

## Scope 7: Migration of Schema Constants (Extraction + Normalize)

### Description
Replace the scattered `pa.schema(...)` constants with Pydantic specs that generate Arrow
schemas and Pandera schemas.

### Target files
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/runtime_inspect_extract.py`
- `src/normalize/*` (all schema constants)

### Integration checklist
- [x] Define TableSchemaSpec per dataset.
- [x] Replace schema constants with spec-derived Arrow schemas.
- [x] Adjust table construction to reference spec output (no inline schema).

---

## Scope 8: Rule Model OO Validation (Optional)

### Description
Migrate `relspec.model` dataclasses to Pydantic for automatic validation, config boundary
enforcement, and JSON serialization.

### Code patterns
```python
# src/relspec/specs.py
class RelationshipRuleSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    kind: RuleKind
    output_dataset: str
    contract_name: str | None = None
```

### Target files
- `src/relspec/model.py`
- `src/relspec/registry.py`

### Integration checklist
- [x] Replace dataclasses with Pydantic models.
- [x] Ensure validation matches current behavior.
- [x] Update registry usage for Pydantic objects.

---

## Scope 9: Testing + Schema Snapshot Updates

### Description
Add tests to ensure schema generation, validation, and OO builders behave correctly.

### Target files
- `tests/` (new schema validation tests)
- `obs/manifest.py` and `obs/repro.py` (ensure schema serialization still stable)

### Integration checklist
- [x] Tests for `TableSchemaSpec -> Arrow schema` conversions.
- [x] Tests for Pandera validation on Arrow tables.
- [x] Tests for EdgeBuilder/NodeBuilder/PropBuilder outputs.
- [x] Reviewed obs schema serialization paths (no changes required).

---

## Integration Sequencing (Suggested)

1. Implement schema spec layer and adapters (Scopes 1-2).
2. Migrate CPG schemas/contracts to specs (Scope 1 + 7 for CPG).
3. Add Pandera validation hook in finalize (Scope 6).
4. Refactor CPG builders into spec engines (Scope 5).
5. Migrate extraction/normalize schemas (Scope 7).
6. Optional: rule model migration (Scope 8).
7. Tests and snapshot updates (Scope 9).
