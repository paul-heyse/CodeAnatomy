## Extract Programmatic Intensification Plan

### Goals
- Make extraction fully programmatic: schemas, options, outputs, and compute pipelines derive from registries/specs.
- Drive extraction via evidence + rule requirements instead of per-extractor toggles.
- Push more extract work into ArrowDSL plan/compute lanes to reduce Python loops.
- Improve determinism, lineage, and auditability for extract outputs.

### Constraints
- Preserve output schemas, column names, and metadata semantics for extract tables.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Keep changes in `src/normalize` and `src/cpg` limited to planned scope only.

---

### Scope 1: Programmatic Extractor Registry + Capabilities
**Description**
Define a single registry that describes each extractor’s outputs, required inputs,
optional outputs, and template. This becomes the source of truth for routing and
feature flags.

**Code patterns**
```python
# src/extract/registry_extractors.py
@dataclass(frozen=True)
class ExtractorSpec:
    name: str
    template: str
    outputs: tuple[str, ...]
    required_inputs: tuple[str, ...] = ()
    optional_outputs: tuple[str, ...] = ()
    supports_plan: bool = False
```

**Target files**
- Add: `src/extract/registry_extractors.py`
- Update: `src/extract/helpers.py`
- Update: `src/extract/registry_templates.py`

**Implementation checklist**
- [x] Define ExtractorSpec registry for ast/cst/scip/symtable/bytecode/tree_sitter/runtime.
- [x] Encode required inputs, optional outputs, and plan support per extractor.
- [x] Replace ad-hoc feature flags with registry capabilities.
- [x] Preserve extractor template defaults in registry templates (no changes required).

**Status**
Completed.

---

### Scope 2: Evidence Plan → Extractor Routing
**Description**
Use `EvidencePlan` to select required datasets and compute the minimal set of
extractor outputs, then route extraction through the registry.

**Code patterns**
```python
# src/extract/helpers.py
outputs = template_outputs(plan, "cst")
extractors = required_extractors(plan)
```

**Target files**
- Update: `src/extract/evidence_plan.py`
- Update: `src/extract/helpers.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`

**Implementation checklist**
- [x] Derive required extractor outputs from `EvidencePlan.sources`.
- [x] Route extractor invocation through registry selectors.
- [x] Drive bundle gating via `template_outputs` in the Hamilton layer.
- [x] Keep extractor modules option-driven for direct invocation.

**Status**
Completed.

---

### Scope 3: Registry-Driven Schemas + Metadata
**Description**
Ensure all extract outputs are aligned to registry schemas and metadata, with
evidence semantics captured as schema metadata (rank, coordinate system, policy).

**Code patterns**
```python
# src/extract/schema_ops.py
meta = metadata_spec_for_dataset(name, options=options, repo_id=repo_id)
plan = normalize_extract_plan(name, plan, ctx=ctx, options=options)
```

**Target files**
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/registry_specs.py`
- Update: `src/extract/*_extract.py`

**Implementation checklist**
- [x] Use registry-derived schemas and metadata specs across extractors.
- [x] Apply registry-aligned ordering + encoding policies via schema ops.
- [x] Keep dataset_schema constants for empty-table construction.

**Status**
Completed.

---

### Scope 4: Extractor Config Factories
**Description**
Replace per-extractor option branching with programmatic config factories driven
by templates + evidence plans.

**Code patterns**
```python
# src/extract/spec_helpers.py
def extractor_option_values(
    template_name: str,
    plan: EvidencePlan | None,
    *,
    overrides: Mapping[str, object] | None = None,
) -> dict[str, object]:
    ...
```

**Target files**
- Update: `src/extract/registry_templates.py`
- Update: `src/extract/spec_helpers.py`
- Update: `src/extract/*_extract.py`

**Implementation checklist**
- [x] Implement config factories per extractor template.
- [x] Replace include_* branching with factory outputs in Hamilton routing.
- [x] Keep default values in template metadata (not in extractor code).

**Status**
Completed.

---

### Scope 5: Shared Extract Normalization Helpers
**Description**
Centralize normalization (span canonicalization, hashing, column alignment) in
shared helpers, and apply them uniformly across extractors.

**Code patterns**
```python
# src/extract/schema_ops.py
def normalize_extract_output(name: str, table: TableLike, *, ctx: ExecutionContext) -> TableLike:
    policy = schema_policy_for_dataset(name, ctx=ctx, options=options)
    return policy.apply(postprocess_table(name, table))
```

**Target files**
- Add: `src/extract/schema_ops.py`
- Update: `src/extract/helpers.py`
- Update: `src/extract/*_extract.py`

**Implementation checklist**
- [x] Add canonical normalize/align helpers.
- [x] Apply shared normalization in all extractors.
- [x] Keep hash/span derivations in registry specs (no extractor duplication).

**Status**
Completed.

---

### Scope 6: Bundle Definitions as Registry Specs
**Description**
Define extractor bundles (CST bundle, SCIP bundle, etc.) in registry specs and
use them to drive Hamilton bundle outputs.

**Code patterns**
```python
# src/extract/registry_bundles.py
OutputBundleSpec(name="cst_bundle", outputs=("cst_parse_manifest", ...))
```

**Target files**
- Update: `src/extract/registry_bundles.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`

**Implementation checklist**
- [x] Add bundle specs with output ordering.
- [x] Drive bundle field lists from registry.
- [x] Remove manual output lists in Hamilton nodes.

**Status**
Completed.

---

### Scope 7: ArrowDSL Plan/Compute Intensification
**Description**
Convert repeated Python loops and row-wise conversions into ArrowDSL plan/compute
operations and shared helpers.

**Code patterns**
```python
# src/extract/plan_helpers.py
plan = plan_from_rows_for_dataset(name, rows, row_schema=schema, label=label, ctx=ctx)
```

**Target files**
- Update: `src/extract/*_extract.py`
- Add: `src/extract/plan_helpers.py`

**Implementation checklist**
- [x] Replace repeated query/align wiring with shared plan helpers.
- [x] Reuse existing ArrowDSL compute helpers (no new changes required).
- [x] Minimize materialization in extract execution paths.

**Status**
Completed.

---

### Scope 8: Extract Lineage + Manifest Expansion
**Description**
Emit lineage records for extract outputs (dataset, template, metadata, sources)
so evidence selection is auditable and reproducible.

**Code patterns**
```python
# src/obs/manifest.py
ExtractRecord(
    name, alias, template, evidence_family, coordinate_system, evidence_rank,
    ambiguity_policy, required_columns, sources, schema_fingerprint
)
```

**Target files**
- Update: `src/obs/manifest.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`

**Implementation checklist**
- [x] Emit extract manifest records from registry + evidence plan.
- [x] Include evidence metadata + schema fingerprints.
- [x] Make manifest stable across debug/non-debug runs.

**Status**
Completed.
