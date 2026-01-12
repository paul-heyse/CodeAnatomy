# Plan: PyArrow Spec + Context Consolidation

## Goals
- Eliminate schema drift by centralizing field bundles, schema metadata, and registries.
- Make scan/plan/finalize paths single-source-of-truth via spec and context factories.
- Align DSL + relspec configuration models to avoid duplicate semantics.
- Keep all operations PyArrow-first (Acero plan lane + compute kernel lane).

## Design decisions (from open questions)
- **Schema versioning**: Use `Schema.metadata` as the canonical version registry; keep `schema_version` as a physical column only where mixed-version rows are expected or row-level lineage is required.
- **QuerySpec source**: Generate `QuerySpec` from `TableSchemaSpec` (input shape), not `ContractSpec` (output correctness).
- **Extraction contexts**: Introduce a shared `FileContext` and wrap it in extractor-specific contexts.
- **Provenance**: Allow `scan_batches()`/`TaggedRecordBatch` only under debug/repro modes; production uses provenance columns from `QuerySpec`.

## Status Summary (current codebase)
- Scope 1: Complete
- Scope 2: Complete
- Scope 3: Complete
- Scope 4: Complete
- Scope 5: Complete
- Scope 6: Complete
- Scope 7: Complete
- Scope 8: Complete
- Scope 9: Complete
- Scope 10: Complete
- Scope 11: Complete
- Scope 12: Complete
- Scope 13: Complete
- Scope 14: Complete

---

## Scope 1: Shared field bundles + schema registry + metadata versioning

### Pattern snippet
```python
# src/schema_spec/metadata.py
from __future__ import annotations

SCHEMA_META_NAME = b"schema_name"
SCHEMA_META_VERSION = b"schema_version"


def schema_metadata(name: str, version: int) -> dict[bytes, bytes]:
    return {
        SCHEMA_META_NAME: name.encode("utf-8"),
        SCHEMA_META_VERSION: str(version).encode("utf-8"),
    }


# src/schema_spec/fields.py
from __future__ import annotations

from dataclasses import dataclass

import arrowdsl.pyarrow_core as pa
from schema_spec.core import ArrowFieldSpec

DICT_STRING = pa.dictionary(pa.int32(), pa.string())


@dataclass(frozen=True)
class FieldBundle:
    name: str
    fields: tuple[ArrowFieldSpec, ...]
    required_non_null: tuple[str, ...] = ()
    key_fields: tuple[str, ...] = ()


def file_identity_bundle(*, include_sha256: bool = True) -> FieldBundle:
    ...


def span_bundle() -> FieldBundle:
    ...
```

```python
# src/schema_spec/registry.py
from __future__ import annotations

from dataclasses import dataclass

from schema_spec.contracts import ContractSpec
from schema_spec.core import TableSchemaSpec

@dataclass(frozen=True)
class SchemaRegistry:
    table_specs: dict[str, TableSchemaSpec]
    contract_specs: dict[str, ContractSpec]

    def register_table(self, spec: TableSchemaSpec) -> TableSchemaSpec:
        return self.table_specs.setdefault(spec.name, spec)

    def register_contract(self, spec: ContractSpec) -> ContractSpec:
        return self.contract_specs.setdefault(spec.name, spec)
```

### Target files
- `src/schema_spec/fields.py` (new)
- `src/schema_spec/registry.py` (new)
- `src/schema_spec/core.py`
- `src/cpg/schemas.py`
- `src/normalize/diagnostics.py`
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/symtable_extract.py`

### Implementation checklist
- [x] Add `FieldBundle` definitions for file identity, span, schema_version, provenance, and dictionary string types.
- [x] Add `schema_metadata()` and wire metadata into `TableSchemaSpec.to_arrow_schema()` when version is present.
- [x] Introduce `SchemaRegistry` for centralized spec registration.
- [x] Migrate one extractor (AST) and one output (CPG) to use bundles and metadata as a pilot.
- [x] Expand migration to remaining extractors/normalizers once validated.

---

## Scope 2: Spec factories (TableSchemaSpec / ContractSpec / QuerySpec)

### Pattern snippet
```python
# src/schema_spec/factories.py
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from schema_spec.core import ArrowFieldSpec, TableSchemaSpec
from schema_spec.contracts import ContractSpec, DedupeSpecSpec, SortKeySpec
from schema_spec.fields import FieldBundle
from arrowdsl.queryspec import ProjectionSpec, QuerySpec


@dataclass(frozen=True)
class TableSpecConstraints:
    required_non_null: Iterable[str] = ()
    key_fields: Iterable[str] = ()


@dataclass(frozen=True)
class VirtualFieldSpec:
    fields: tuple[str, ...] = ()
    docs: Mapping[str, str] | None = None


def make_table_spec(
    name: str,
    *,
    version: int | None,
    bundles: Iterable[FieldBundle],
    fields: Iterable[ArrowFieldSpec],
    constraints: TableSpecConstraints | None = None,
) -> TableSchemaSpec:
    ...


def make_contract_spec(
    *,
    table_spec: TableSchemaSpec,
    dedupe: DedupeSpecSpec | None = None,
    canonical_sort: Iterable[SortKeySpec] = (),
    virtual: VirtualFieldSpec | None = None,
    version: int | None = None,
) -> ContractSpec:
    ...


def query_spec_for_table(table_spec: TableSchemaSpec) -> QuerySpec:
    cols = tuple(field.name for field in table_spec.fields)
    return QuerySpec(projection=ProjectionSpec(base=cols))
```

### Target files
- `src/schema_spec/factories.py` (new)
- `src/schema_spec/core.py`
- `src/schema_spec/contracts.py`
- `src/arrowdsl/queryspec.py`
- `src/extract/scip_extract.py`

### Implementation checklist
- [x] Add `TableSchemaSpec.version` (optional) and propagate metadata via `schema_metadata()`.
- [x] Implement factory helpers for table specs, contracts, and query specs.
- [x] Replace manual spec construction in at least one extractor and CPG schemas with factories.
- [x] Ensure `QuerySpec` construction uses input `TableSchemaSpec` only (not output contracts).

---

## Scope 3: Provenance + dictionary dtype centralization + guard tests

### Pattern snippet
```python
# src/schema_spec/fields.py
PROVENANCE_COLS: tuple[str, ...] = (
    "prov_filename",
    "prov_fragment_index",
    "prov_batch_index",
    "prov_last_in_fragment",
)

PROVENANCE_FIELDS = FieldBundle(
    name="provenance",
    fields=(
        ArrowFieldSpec(name="prov_filename", dtype=pa.string()),
        ArrowFieldSpec(name="prov_fragment_index", dtype=pa.int32()),
        ArrowFieldSpec(name="prov_batch_index", dtype=pa.int32()),
        ArrowFieldSpec(name="prov_last_in_fragment", dtype=pa.bool_()),
    ),
)
```

```python
# tests/test_provenance_constants.py
from schema_spec.fields import PROVENANCE_COLS
from arrowdsl.kernels import provenance_sort_keys


def test_provenance_column_names_are_shared() -> None:
    assert tuple(col for col in PROVENANCE_COLS) == tuple(col for col in provenance_sort_keys())
```

### Target files
- `src/schema_spec/fields.py`
- `src/arrowdsl/queryspec.py`
- `src/arrowdsl/kernels.py`
- `src/normalize/diagnostics.py`
- `src/cpg/schemas.py`
- `tests/test_provenance_constants.py` (new)

### Implementation checklist
- [x] Define `PROVENANCE_COLS` and `DICT_STRING` once and reuse everywhere.
- [x] Update `QuerySpec.scan_columns()` to reference `PROVENANCE_COLS` instead of hardcoding.
- [x] Update `canonical_sort_if_canonical()` to pull provenance keys from the shared constant.
- [x] Add a guard test to prevent renames from silently drifting.

---

## Scope 4: Shared FileContext for extractors

### Pattern snippet
```python
# src/extract/file_context.py
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping

@dataclass(frozen=True)
class FileContext:
    file_id: str
    path: str
    file_sha256: str | None
    encoding: str | None = None
    text: str | None = None
    data: bytes | None = None

    @classmethod
    def from_repo_row(cls, row: Mapping[str, object]) -> FileContext:
        return cls(
            file_id=str(row.get("file_id") or ""),
            path=str(row.get("path") or ""),
            file_sha256=row.get("file_sha256") if isinstance(row.get("file_sha256"), str) else None,
            encoding=row.get("encoding") if isinstance(row.get("encoding"), str) else None,
            text=row.get("text") if isinstance(row.get("text"), str) else None,
            data=row.get("bytes") if isinstance(row.get("bytes"), (bytes, bytearray, memoryview)) else None,
        )
```

### Target files
- `src/extract/file_context.py` (new)
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/cst_extract.py`

### Implementation checklist
- [x] Introduce `FileContext` and update extractors to build it from repo file rows.
- [x] Update extractor-specific contexts to embed `FileContext` rather than duplicating fields.
- [x] Update helper functions to accept `FileContext` where feasible.
- [x] Keep extractor options local and unchanged to minimize migration churn.

---

## Scope 5: ScanContext + scan telemetry

### Pattern snippet
```python
# src/arrowdsl/scan_context.py
from __future__ import annotations

from dataclasses import dataclass

import pyarrow.dataset as ds

from arrowdsl.dataset_io import compile_to_acero_scan, make_scanner
from arrowdsl.queryspec import QuerySpec
from arrowdsl.runtime import ExecutionContext

@dataclass(frozen=True)
class ScanTelemetry:
    fragment_count: int
    estimated_rows: int | None

@dataclass(frozen=True)
class ScanContext:
    dataset: ds.Dataset
    spec: QuerySpec
    ctx: ExecutionContext

    def telemetry(self) -> ScanTelemetry:
        fragments = list(self.dataset.get_fragments(filter=self.spec.pushdown_expression()))
        rows = self.dataset.count_rows(filter=self.spec.pushdown_expression())
        return ScanTelemetry(fragment_count=len(fragments), estimated_rows=int(rows))

    def scanner(self) -> ds.Scanner:
        return make_scanner(self.dataset, spec=self.spec, ctx=self.ctx)

    def acero_decl(self) -> object:
        return compile_to_acero_scan(self.dataset, spec=self.spec, ctx=self.ctx)
```

### Target files
- `src/arrowdsl/scan_context.py` (new)
- `src/arrowdsl/dataset_io.py`
- `src/relspec/compiler.py`
- `src/hamilton_pipeline/modules/extraction.py`

### Implementation checklist
- [x] Add `ScanContext` to standardize scanner creation, Acero compilation, and telemetry.
- [x] Switch pipeline and relspec compilation to use `ScanContext` instead of direct helpers (relspec done).
- [x] Plumb telemetry into observability outputs where appropriate.
- [x] Gate `scan_batches()` usage behind `ExecutionContext.debug` or an explicit debug profile (no usage found).

---

## Scope 6: FinalizeContext (contract + schema alignment + error artifacts)

### Pattern snippet
```python
# src/arrowdsl/finalize_context.py
from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.contracts import Contract
from arrowdsl.finalize import ErrorArtifactSpec, ERROR_ARTIFACT_SPEC, finalize
from arrowdsl.schema_ops import SchemaTransform
from arrowdsl.runtime import ExecutionContext

@dataclass(frozen=True)
class FinalizeContext:
    contract: Contract
    error_spec: ErrorArtifactSpec = ERROR_ARTIFACT_SPEC
    transform: SchemaTransform | None = None

    def run(self, table, ctx: ExecutionContext):
        return finalize(table, contract=self.contract, ctx=ctx)
```

### Target files
- `src/arrowdsl/finalize_context.py` (new)
- `src/arrowdsl/finalize.py`
- `src/relspec/compiler.py`

### Implementation checklist
- [x] Add `FinalizeContext` and use it in relspec and pipeline finalize flows.
- [x] Derive `SchemaTransform` from `Contract.schema_spec` when present.
- [x] Ensure schema validation policy flows from `ExecutionContext.schema_validation`.

---

## Scope 7: Join/Dedupe spec unification

### Pattern snippet
```python
# src/relspec/model.py
from arrowdsl.specs import JoinSpec

class HashJoinConfig(BaseModel):
    ...
    def to_join_spec(self) -> JoinSpec:
        return JoinSpec(
            join_type=self.join_type,
            left_keys=self.left_keys,
            right_keys=self.right_keys,
            left_output=self.left_output,
            right_output=self.right_output,
            output_suffix_for_left=self.output_suffix_for_left,
            output_suffix_for_right=self.output_suffix_for_right,
        )
```

```python
# src/relspec/compiler.py
spec = rule.hash_join.to_join_spec()
plan = hash_join(left=left_plan, right=right_plan, spec=spec)
```

### Target files
- `src/arrowdsl/specs.py`
- `src/relspec/model.py`
- `src/relspec/compiler.py`
- `src/arrowdsl/joins.py`

### Implementation checklist
- [x] Add conversion methods to map Pydantic configs to dataclass specs.
- [x] Normalize join_type defaults and key validations in one place (JoinSpec).
- [x] Ensure dedupe and sort specs reuse the same `SortKey` and `DedupeSpec` types everywhere.

---

## Scope 8: Predicate/Projection API consolidation

### Pattern snippet
```python
# src/arrowdsl/predicates.py
from dataclasses import dataclass
from collections.abc import Callable

@dataclass(frozen=True)
class PredicateExpr:
    expr: ComputeExpression
    mask_fn: Callable[[TableLike], ArrayLike]

    def to_expression(self) -> ComputeExpression:
        return self.expr

    def mask(self, table: TableLike) -> ArrayLike:
        return self.mask_fn(table)
```

```python
# src/arrowdsl/expr.py
class E:
    @staticmethod
    def eq(col: str, value: ScalarValue) -> PredicateExpr:
        expr = ensure_expression(pc.equal(E.field(col), E.scalar(value)))
        return PredicateExpr(expr=expr, mask_fn=lambda table: pc.equal(table[col], pc.scalar(value)))
```

### Target files
- `src/arrowdsl/predicates.py`
- `src/arrowdsl/expr.py`
- `src/arrowdsl/queryspec.py`
- `src/arrowdsl/column_ops.py`

### Implementation checklist
- [x] Introduce `PredicateExpr` and return it from expression macros.
- [x] Update `QuerySpec` to accept `PredicateExpr` (or a unified `PredicateSpec`) consistently.
- [x] Ensure plan-lane and kernel-lane behaviors are generated from one source per predicate.
- [x] Keep projection expressions scalar-only per Acero rules (explicit in docs/tests).

---

## Scope 9: Migration + cleanup

### Pattern snippet
```python
# Example migration: use bundles and factories in extractors
AST_NODES_SPEC = make_table_spec(
    name="py_ast_nodes_v1",
    version=1,
    bundles=(file_identity_bundle(),),
    fields=(
        ArrowFieldSpec(name="ast_idx", dtype=pa.int32()),
        ArrowFieldSpec(name="parent_ast_idx", dtype=pa.int32()),
        ...
    ),
)
```

### Target files
- `src/extract/ast_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`
- `src/normalize/diagnostics.py`
- `src/cpg/schemas.py`
- `tests/test_schema_spec_builders.py`

### Implementation checklist
- [x] Migrate extractors and normalizers to use bundles and spec factories.
- [x] Consolidate common constants and remove duplicate definitions.
- [x] Update tests to cover metadata, bundles, and shared constants.
- [x] Remove deprecated helpers or fields once all call sites are migrated (e.g., unused `schema_version_bundle`).

---

## Scope 10: FileContext adoption in tree-sitter extraction

### Pattern snippet
```python
# src/extract/tree_sitter_extract.py
from extract.file_context import FileContext


def _row_bytes(file_ctx: FileContext) -> bytes | None:
    if file_ctx.data is not None:
        return file_ctx.data
    if not file_ctx.text:
        return None
    enc = file_ctx.encoding or "utf-8"
    return file_ctx.text.encode(enc, errors="replace")


def _extract_ts_for_row(rf: dict[str, object], ...) -> None:
    file_ctx = FileContext.from_repo_row(rf)
    if not file_ctx.file_id or not file_ctx.path:
        return
    data = _row_bytes(file_ctx)
    ...
```

### Target files
- `src/extract/tree_sitter_extract.py`

### Implementation checklist
- [x] Add `FileContext` usage to tree-sitter extraction and byte/text decoding.
- [x] Remove direct `rf["bytes"]`/`rf["text"]` access in favor of `FileContext`.
- [x] Keep row emission identical (only identity/decoding refactor).

---

## Scope 11: Additional FieldBundles for repeated span families

### Pattern snippet
```python
# src/schema_spec/fields.py
def call_span_bundle() -> FieldBundle:
    return FieldBundle(
        name="call_span",
        fields=(
            ArrowFieldSpec(name="call_bstart", dtype=pa.int64()),
            ArrowFieldSpec(name="call_bend", dtype=pa.int64()),
        ),
    )


def scip_range_bundle() -> FieldBundle:
    return FieldBundle(
        name="scip_range",
        fields=(
            ArrowFieldSpec(name="start_line", dtype=pa.int32()),
            ArrowFieldSpec(name="start_char", dtype=pa.int32()),
            ArrowFieldSpec(name="end_line", dtype=pa.int32()),
            ArrowFieldSpec(name="end_char", dtype=pa.int32()),
        ),
    )
```

### Target files
- `src/schema_spec/fields.py`
- `src/extract/cst_extract.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/extract/scip_extract.py`
- `src/normalize/spans.py`

### Implementation checklist
- [x] Add span bundles for call spans and SCIP range columns.
- [x] Replace repeated call span fields in CST and relationship specs with bundles.
- [x] Replace SCIP range field definitions with bundle usage where possible.
- [x] Keep field ordering aligned with existing schemas.

---

## Scope 12: SchemaRegistry integration into catalogs

### Pattern snippet
```python
# src/cpg/schemas.py
from schema_spec.registry import SchemaRegistry

REGISTRY = SchemaRegistry()
REGISTRY.register_table(CPG_NODES_SPEC)
REGISTRY.register_contract(CPG_NODES_CONTRACT_SPEC)
```

### Target files
- `src/schema_spec/registry.py`
- `src/cpg/schemas.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/schema_spec/catalogs.py`

### Implementation checklist
- [x] Introduce a shared registry for CPG and relationship specs.
- [x] Register all table specs and contract specs when they are constructed.
- [x] Expose registry entries for metadata export or validation reporting.

---

## Scope 13: QuerySpec factory usage at scan entrypoints

### Pattern snippet
```python
# src/relspec/compiler.py
from schema_spec.factories import query_spec_for_table

if ref.query is None and loc.table_spec is not None:
    ref_query = query_spec_for_table(loc.table_spec)
```

### Target files
- `src/relspec/compiler.py`
- `src/relspec/registry.py`
- `src/schema_spec/factories.py`

### Implementation checklist
- [x] Store `TableSchemaSpec` in dataset catalog entries where available.
- [x] Use `query_spec_for_table()` when query specs are missing.
- [x] Keep QuerySpec source of truth tied to input table schemas.

---

## Scope 14: FinalizeContext adoption for pipeline writers

### Pattern snippet
```python
# src/hamilton_pipeline/modules/cpg_build.py
finalizer = FinalizeContext(contract=contract, transform=transform)
result = finalizer.run(table, ctx=ctx)
```

### Target files
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/storage/parquet.py`

### Implementation checklist
- [x] Run `FinalizeContext` before writing datasets to storage.
- [x] Ensure contract metadata is attached to schemas on output.
- [x] Emit error artifacts alongside data outputs where applicable.

---

## Phased delivery
- **Phase 1 (Scaffolding)**: Complete.
- **Phase 2 (Pilot)**: Complete.
- **Phase 3 (Expansion)**: Complete.
- **Phase 4 (Spec unification)**: Complete.
- **Phase 5 (Contexts)**: Complete.

## Validation gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.14`
- Focused tests: schema metadata, provenance constants, and factory conversions.
