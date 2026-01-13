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
- [ ] Define ExtractorSpec registry for ast/cst/scip/symtable/bytecode/tree_sitter/runtime.
- [ ] Encode required inputs and optional outputs per extractor.
- [ ] Replace ad-hoc feature flags with registry capabilities.

**Status**
Planned.

---

### Scope 2: Evidence Plan → Extractor Routing
**Description**
Use `EvidencePlan` to select required datasets and compute the minimal set of
extractor outputs, then route extraction through the registry.

**Code patterns**
```python
# src/extract/evidence_plan.py
plan = compile_evidence_plan(rules)
extractors = select_extractors(plan.sources, registry=EXTRACTOR_SPECS)
```

**Target files**
- Update: `src/extract/evidence_plan.py`
- Update: `src/extract/helpers.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`

**Implementation checklist**
- [ ] Derive required extractor outputs from `EvidencePlan.sources`.
- [ ] Route extractor invocation through registry selectors.
- [ ] Remove manual per-function gating where registry can determine routing.

**Status**
Planned.

---

### Scope 3: Registry-Driven Schemas + Metadata
**Description**
Ensure all extract outputs are aligned to registry schemas and metadata, with
evidence semantics captured as schema metadata (rank, coordinate system, policy).

**Code patterns**
```python
# src/extract/registry_specs.py
schema = dataset_schema(name)
meta = dataset_metadata_spec(name)
```

**Target files**
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/registry_specs.py`
- Update: `src/extract/*_extract.py`

**Implementation checklist**
- [ ] Remove inline schema definitions from extractors.
- [ ] Apply metadata specs to all extract outputs.
- [ ] Enforce registry-aligned ordering + encoding policies.

**Status**
Planned.

---

### Scope 4: Extractor Config Factories
**Description**
Replace per-extractor option branching with programmatic config factories driven
by templates + evidence plans.

**Code patterns**
```python
# src/extract/spec_helpers.py
def extractor_options(name: str, *, plan: EvidencePlan | None) -> Mapping[str, object]:
    ...
```

**Target files**
- Update: `src/extract/registry_templates.py`
- Update: `src/extract/spec_helpers.py`
- Update: `src/extract/*_extract.py`

**Implementation checklist**
- [ ] Implement config factories per extractor template.
- [ ] Replace include_* branching with factory outputs.
- [ ] Keep default values in template metadata (not in extractor code).

**Status**
Planned.

---

### Scope 5: Shared Extract Normalization Helpers
**Description**
Centralize normalization (span canonicalization, hashing, column alignment) in
shared helpers, and apply them uniformly across extractors.

**Code patterns**
```python
# src/extract/schema_ops.py
def normalize_extract_output(name: str, table: TableLike) -> TableLike:
    spec = dataset_spec(name)
    return spec.finalize_context(ctx).run(table, ctx=ctx).good
```

**Target files**
- Add: `src/extract/schema_ops.py`
- Update: `src/extract/helpers.py`
- Update: `src/extract/*_extract.py`

**Implementation checklist**
- [ ] Add canonical normalize/align helpers.
- [ ] Apply shared normalization in all extractors.
- [ ] Remove duplicated hash/span logic in extractor modules.

**Status**
Planned.

---

### Scope 6: Bundle Definitions as Registry Specs
**Description**
Define extractor bundles (CST bundle, SCIP bundle, etc.) in registry specs and
use them to drive Hamilton bundle outputs.

**Code patterns**
```python
# src/extract/registry_bundles.py
BundleSpec(name="cst_bundle", outputs=("cst_parse_manifest", ...))
```

**Target files**
- Update: `src/extract/registry_bundles.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`

**Implementation checklist**
- [ ] Add bundle specs with output ordering.
- [ ] Drive bundle field lists from registry.
- [ ] Remove manual output lists in Hamilton nodes.

**Status**
Planned.

---

### Scope 7: ArrowDSL Plan/Compute Intensification
**Description**
Convert repeated Python loops and row-wise conversions into ArrowDSL plan/compute
operations and shared helpers.

**Code patterns**
```python
# src/extract/plan_helpers.py
plan = plan_from_source(source, ctx=ctx).project(exprs, names, ctx=ctx)
```

**Target files**
- Update: `src/extract/*_extract.py`
- Update: `src/extract/plan_helpers.py`
- Update: `src/arrowdsl/compute/*`

**Implementation checklist**
- [ ] Replace list building with plan projections where possible.
- [ ] Centralize compute helpers for hashing/filtering/joins.
- [ ] Minimize materialization in extract execution paths.

**Status**
Planned.

---

### Scope 8: Extract Lineage + Manifest Expansion
**Description**
Emit lineage records for extract outputs (dataset, template, metadata, sources)
so evidence selection is auditable and reproducible.

**Code patterns**
```python
# src/obs/manifest.py
ExtractRecord(name, template, metadata, sources)
```

**Target files**
- Update: `src/obs/manifest.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/extract/registry_specs.py`

**Implementation checklist**
- [ ] Emit extract manifest records from registry + evidence plan.
- [ ] Include evidence metadata and schema versions.
- [ ] Make manifest stable across debug/non-debug runs.

**Status**
Planned.
