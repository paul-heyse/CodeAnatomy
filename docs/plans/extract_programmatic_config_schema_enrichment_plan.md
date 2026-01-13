## Extract Programmatic Configuration + Schema Enrichment Plan

### Goals
- Make extract configuration, schema, and metadata fully programmatic and registry-driven.
- Encode evidence semantics (meaning + confidence + ordering) directly in Arrow schema metadata.
- Centralize join/ordering keys and derived IDs to avoid ad-hoc logic in extractors.
- Use ArrowDSL schema policies for consistent alignment/encoding across extract outputs.

### Constraints
- Preserve existing output schemas and column names unless explicitly extended.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Keep extractor logic focused on parsing/row emission; move configuration to registries.

---

### Scope 1: Evidence Semantics Metadata (Templates → Schema)
**Description**
Attach evidence semantics (family, coordinate system, ambiguity policy, superior rank) to schema
metadata via extractor templates so downstream relationship compilation has uniform context.

**Code patterns**
```python
# src/extract/registry_templates.py
@dataclass(frozen=True)
class ExtractorTemplate:
    extractor_name: str
    evidence_rank: int
    ordering_level: OrderingLevel = OrderingLevel.IMPLICIT
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)
    confidence: float = 1.0

TEMPLATES = {
    "cst": ExtractorTemplate(
        extractor_name="cst",
        evidence_rank=3,
        metadata_extra={
            b"evidence_family": b"cst",
            b"coordinate_system": b"bytes",
            b"ambiguity_policy": b"preserve",
            b"superior_rank": b"3",
            b"streaming_safe": b"true",
            b"pipeline_breaker": b"false",
        },
    ),
}


# src/extract/registry_builders.py
extractor_meta = extractor_metadata_spec(
    extractor_name,
    row.version,
    extra=extractor_extra,
)
```

**Target files**
- Update: `src/extract/registry_templates.py`
- Update: `src/extract/registry_builders.py`
- Update: `src/extract/registry_rows.py` (optional per-dataset overrides)

**Implementation checklist**
- [x] Add semantic metadata keys to templates (`evidence_family`, `coordinate_system`,
      `ambiguity_policy`, `superior_rank`).
- [x] Allow dataset rows to extend/override template metadata.
- [x] Ensure metadata is applied to schema via `SchemaMetadataSpec`.

**Status**
Completed.

---

### Scope 2: Canonical Join Keys + Derived Span IDs
**Description**
Make canonical join keys explicit and programmatic for each dataset, and use them to
derive stable span IDs and ordering keys without inline logic.

**Code patterns**
```python
# src/extract/registry_rows.py
@dataclass(frozen=True)
class DatasetRow:
    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    join_keys: tuple[str, ...] = ()
    derived: tuple[DerivedIdSpec, ...] = ()


DatasetRow(
    name="py_cst_name_refs_v1",
    version=1,
    bundles=("file_identity",),
    fields=("name_ref_id", "bstart", "bend", "name", "expr_ctx", "qnames"),
    join_keys=("file_id", "bstart", "bend"),
    derived=(DerivedIdSpec(name="span_id", spec="span_id"),),
)
```

**Target files**
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/registry_builders.py`
- Update: `src/extract/registry_ids.py` (add span hash spec where needed)

**Implementation checklist**
- [x] Add `join_keys` to dataset rows.
- [x] Derive ordering keys from `join_keys` when not set explicitly.
- [x] Add a `span_id` hash spec and wire into derived IDs when applicable.

**Status**
Completed.

---

### Scope 3: Encoding Policy + SchemaPolicy Integration
**Description**
Drive encoding (e.g., dictionary encoding) from schema specs and apply it consistently
through a registry `SchemaPolicy`, avoiding per-extractor encode logic.

**Code patterns**
```python
# src/extract/registry_specs.py
def dataset_schema_policy(
    name: str,
    *,
    ctx: ExecutionContext,
    options: object | None = None,
    enable_encoding: bool = True,
) -> SchemaPolicy:
    ...


# src/extract/scip_extract.py
policy = dataset_schema_policy(
    "scip_occurrences_v1",
    ctx=exec_ctx,
    options=parse_opts,
    enable_encoding=parse_opts.dictionary_encode_strings,
)
plan = _finalize_plan(plan, policy=policy, query=SCIP_OCCURRENCES_QUERY, exec_ctx=exec_ctx)
```

**Target files**
- Update: `src/extract/registry_specs.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/extract/bytecode_extract.py` (and other extractors with manual encoding)
- Update: `src/arrowdsl/schema/policy.py` (if new helpers are needed)

**Implementation checklist**
- [x] Add registry accessor for `SchemaPolicy`.
- [x] Replace manual encoding logic with `SchemaPolicy` + encode plan flow.
- [x] Ensure encoding derives from `ArrowFieldSpec` metadata.

**Status**
Completed.

---

### Scope 4: ExtractorConfigSpec + Dataset Enablement
**Description**
Centralize extractor option handling in registry configs and programmatic
dataset enablement, rather than per-extractor branching.

**Code patterns**
```python
# src/extract/registry_rows.py
@dataclass(frozen=True)
class DatasetRow:
    ...
    enabled_when: Callable[[object | None], bool] | None = None


# src/extract/registry_specs.py
def dataset_enabled(name: str, options: object | None) -> bool:
    ...
```

**Target files**
- Update: `src/extract/registry_templates.py`
- Update: `src/extract/registry_rows.py` (add `enabled_when`)
- Update: `src/extract/registry_specs.py`
- Update: `src/extract/*_extract.py` (use `enabled_datasets`)

**Implementation checklist**
- [x] Add `enabled_when` hooks for datasets tied to options.
- [x] Replace extractor branching with registry-driven enablement.
- [ ] (Optional) Add `ExtractorConfigSpec` if you want to centralize defaults beyond flags.

**Status**
Completed (ExtractorConfigSpec deferred).

---

### Scope 5: Dataset SchemaPolicy Overrides
**Description**
Allow per-dataset overrides (safe_cast, keep_extra_columns, on_error) using a
programmatic policy registry.

**Code patterns**
```python
# src/extract/registry_policies.py
@dataclass(frozen=True)
class DatasetPolicyRow:
    name: str
    safe_cast: bool | None = None
    keep_extra_columns: bool | None = None
    on_error: CastErrorPolicy | None = None


# src/extract/registry_specs.py
def dataset_schema_policy(name: str, *, ctx: ExecutionContext) -> SchemaPolicy:
    policy_row = POLICY_ROWS.get(name)
    options = SchemaPolicyOptions(
        safe_cast=policy_row.safe_cast if policy_row else None,
        keep_extra_columns=policy_row.keep_extra_columns if policy_row else None,
        on_error=policy_row.on_error if policy_row else None,
    )
    return schema_policy_factory(dataset_spec(name).table_spec, ctx=ctx, options=options)
```

**Target files**
- Add: `src/extract/registry_policies.py`
- Update: `src/extract/registry_specs.py`
- Update: `src/extract/*_extract.py`

**Implementation checklist**
- [x] Add policy registry plumbing.
- [x] Apply policy registry in extract materialization paths.
- [ ] Define overrides in `registry_policies.py` when needed.

**Status**
Completed (no overrides configured yet).

---

### Scope 6: Evidence Rank + Confidence Defaults
**Description**
Standardize ambiguity handling by adding evidence rank/confidence fields with
default values provided programmatically.

**Code patterns**
```python
# src/extract/registry_fields.py
_register("evidence_rank", _spec("evidence_rank", pa.int16()))
_register("confidence", _spec("confidence", pa.float32()))


# src/extract/registry_builders.py
row_extras = ("evidence_rank", "confidence")
```

**Target files**
- Update: `src/extract/registry_fields.py`
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/registry_builders.py`
- Update: `src/extract/*_extract.py` (if defaults are materialized per table)

**Implementation checklist**
- [x] Add evidence columns to registry field catalog.
- [x] Apply template-level defaults for rank/confidence.
- [x] Ensure null/default handling is consistent across extractors.

**Status**
Completed.

---

### Scope 7: Streaming + Pipeline-Breaker Metadata
**Description**
Capture whether a dataset/plan is streaming-safe or a pipeline breaker in metadata
for reproducible ordering/determinism decisions.

**Code patterns**
```python
# src/extract/registry_templates.py
metadata_extra={
    b"streaming_safe": b"true",
    b"pipeline_breaker": b"false",
}
```

**Target files**
- Update: `src/extract/registry_templates.py`
- Update: `src/extract/registry_builders.py`

**Implementation checklist**
- [x] Add streaming/pipeline-breaker metadata keys.
- [x] Set defaults per extractor template.
- [x] Preserve metadata through schema application.

**Status**
Completed.
