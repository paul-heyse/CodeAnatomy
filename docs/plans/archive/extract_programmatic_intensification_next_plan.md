## Extract Programmatic Intensification (Next) Plan

### Goals
- Make extract registries fully table-driven with minimal bespoke Python lists.
- Push all feasible extract execution into ArrowDSL plan lanes.
- Derive extractor options and evidence requirements from schema metadata by default.
- Unify extract pipelines (query, kernels, policies) under a single spec registry.
- Improve auditability via extract-level validation and error artifacts.

### Constraints
- Preserve output schemas, column names, and metadata semantics for extract tables.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Keep changes in `src/normalize` and `src/cpg` limited to planned scope only.

---

### Scope 1: Table-Driven Extract Specs
**Description**
Move dataset and extractor definitions to spec tables so registries are compiled
from Arrow tables rather than static Python lists.

**Code patterns**
```python
# src/arrowdsl/spec/tables/extract.py
EXTRACT_DATASET_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("version", pa.int32(), nullable=False),
        pa.field("template", pa.string(), nullable=True),
        pa.field("fields", pa.list_(pa.string()), nullable=True),
        pa.field("row_fields", pa.list_(pa.string()), nullable=True),
        pa.field("feature_flag", pa.string(), nullable=True),
        pa.field("postprocess", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"extract_datasets"},
)


def dataset_rows_from_table(table: pa.Table) -> tuple[DatasetRow, ...]:
    return tuple(_row_from_record(rec) for rec in table.to_pylist())
```

**Target files**
- Add: `src/arrowdsl/spec/tables/extract.py`
- Add: `src/extract/registry_tables.py`
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/registry_extractors.py`

**Implementation checklist**
- [x] Define extract spec schemas and loaders.
- [x] Compile `DatasetRow` entries from spec tables.
- [x] Remove static `DATASET_ROWS` once parity is reached.

**Status**
Completed.

---

### Scope 2: Template + Macro Expansion for Dataset Families
**Description**
Introduce template specs that expand into multiple dataset rows, replacing
repeated blocks in registry files.

**Code patterns**
```python
# src/extract/registry_templates.py
@dataclass(frozen=True)
class DatasetTemplateSpec:
    name: str
    template: str
    params: Mapping[str, object] = field(default_factory=dict)


def expand_dataset_templates(
    specs: Sequence[DatasetTemplateSpec],
) -> tuple[DatasetRow, ...]:
    return tuple(row for spec in specs for row in TEMPLATE_REGISTRY[spec.name](spec.params))
```

**Target files**
- Add: `src/extract/registry_template_specs.py`
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/registry_templates.py`

**Implementation checklist**
- [x] Define template specs for common dataset families.
- [x] Expand templates into dataset rows at registry build time.
- [x] Replace repeated dataset row definitions with templates.

**Status**
Completed (tree_sitter + runtime_inspect template expansion; add more as needed).

---

### Scope 3: Metadata-Driven Option Defaults
**Description**
Infer extractor options (feature flags, defaults) from schema metadata and
evidence requirements, minimizing bespoke option logic.

**Code patterns**
```python
# src/extract/spec_helpers.py
def infer_feature_flags(schema: SchemaLike, *, plan: EvidencePlan | None) -> dict[str, bool]:
    required = schema.metadata.get(b"evidence_required_columns", b"")
    return {"include_callsites": b"callsite" in required}
```

**Target files**
- Update: `src/extract/spec_helpers.py`
- Update: `src/extract/registry_specs.py`
- Update: `src/arrowdsl/schema/metadata.py`

**Implementation checklist**
- [x] Define metadata keys for feature defaults.
- [x] Drive `extractor_option_values` from metadata + evidence.
- [x] Remove bespoke option derivation logic where possible.

**Status**
Completed.

---

### Scope 4: Unified Extract Pipeline Specs
**Description**
Define a single pipeline spec (query ops, kernels, policies) per dataset so all
extractors share a consistent plan execution path.

**Code patterns**
```python
# src/extract/plan_helpers.py
plan = dataset_query(name).apply_to_plan(plan, ctx=ctx)
plan = _apply_pipeline_query_ops(name, plan, ctx=ctx)
# src/extract/schema_ops.py
processed = apply_pipeline_kernels(name, table)
```

**Target files**
- Add: `src/extract/registry_pipelines.py`
- Update: `src/extract/schema_ops.py`
- Update: `src/extract/plan_helpers.py`

**Implementation checklist**
- [x] Define pipeline specs per dataset (query ops + kernels).
- [x] Apply pipeline query ops in plan lane and post-kernels in schema ops.
- [x] Centralize postprocess hooks into pipeline specs.

**Status**
Completed.

---

### Scope 5: Plan-First Extraction Lanes
**Description**
Standardize on plan lanes for all extract outputs; Python stays limited to row
builders and IO boundaries.

**Code patterns**
```python
# src/extract/plan_helpers.py
plan = plan_from_rows_for_dataset(name, rows, row_schema=schema, ctx=ctx)
plan = apply_query_and_normalize(name, plan, ctx=ctx, evidence_plan=evidence_plan)
```

**Target files**
- Update: `src/extract/*_extract.py`
- Update: `src/extract/plan_helpers.py`

**Implementation checklist**
- [x] Convert ad-hoc plan wiring to shared helpers.
- [x] Apply pipeline query ops + evidence projection in plan lane.
- [x] Minimize per-extractor materialization.

**Status**
Completed.

---

### Scope 6: Programmatic Bundle Registry
**Description**
Extend output bundle specs to include ordering and dataset policies, making
Hamilton bundle wiring fully registry-driven.

**Code patterns**
```python
# src/extract/registry_bundles.py
@dataclass(frozen=True)
class OutputBundleSpec:
    name: str
    outputs: tuple[str, ...]
    template: str | None = None
    ordering: tuple[str, ...] = ()
    dataset_map: Mapping[str, str | None] = field(default_factory=dict)
```

**Target files**
- Update: `src/extract/registry_bundles.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`

**Implementation checklist**
- [x] Add ordering and output-to-dataset hints to bundle specs.
- [x] Drive bundle ordering from registry metadata.
- [x] Remove manual output ordering where possible.

**Status**
Completed (dataset policies remain in `registry_policies`).

---

### Scope 7: Evidence-Minimized Projections
**Description**
Use evidence requirements to project only needed columns early in plan lanes.

**Code patterns**
```python
# src/extract/plan_helpers.py
plan = apply_evidence_projection(name, plan, ctx=ctx, evidence_plan=evidence_plan)
```

**Target files**
- Update: `src/extract/plan_helpers.py`
- Update: `src/extract/evidence_plan.py`

**Implementation checklist**
- [x] Derive required columns per dataset from evidence plan.
- [x] Apply early projections to extractor plans.
- [x] Preserve join keys and required schema fields.

**Status**
Completed.

---

### Scope 8: Extract Validation + Error Artifacts
**Description**
Apply contract-style validation to extract outputs, emitting error tables for
invalid rows to improve auditability.

**Code patterns**
```python
# src/extract/schema_ops.py
result = validate_extract_output(name, table, ctx=ctx)
error_rows = int(result.errors.num_rows)
```

**Target files**
- Update: `src/extract/schema_ops.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`
- Update: `src/obs/manifest.py`

**Implementation checklist**
- [x] Add extract-level validation hooks to schema ops.
- [x] Emit error artifacts for extract datasets when enabled.
- [x] Record extract error counts in the manifest.

**Status**
Completed.
