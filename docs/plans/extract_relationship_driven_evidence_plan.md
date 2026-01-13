## Extract Relationship-Driven Evidence Plan

### Goals
- Make extract outputs driven by relationship rules and evidence semantics, not per-extractor toggles.
- Compile an evidence plan from relationship requirements (sources + columns + normalization ops).
- Centralize evidence semantics (coordinate system, rank, ambiguity policy) as data.
- Keep extraction deterministic and reuseable across rule families.

### Constraints
- Preserve output schemas, column names, and metadata semantics for extract tables.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Reuse ArrowDSL plan/contract mechanics where possible.

---

### Scope 1: Evidence Semantics Registry
**Description**
Define evidence semantics as first-class data. Each evidence table declares meaning,
required columns, coordinate system, evidence rank, and ambiguity policy.

**Code patterns**
```python
# src/extract/evidence_specs.py
@dataclass(frozen=True)
class EvidenceSpec:
    name: str
    template: str
    coordinate_system: str
    evidence_rank: int
    required_columns: tuple[str, ...]
    ambiguity_policy: str | None = None

EVIDENCE_SPECS: dict[str, EvidenceSpec] = {
    "py_cst_name_refs_v1": EvidenceSpec(
        name="py_cst_name_refs_v1",
        template="cst",
        coordinate_system="bytes",
        evidence_rank=3,
        required_columns=("file_id", "bstart", "bend", "name"),
        ambiguity_policy="preserve",
    ),
}
```

**Target files**
- Add: `src/extract/evidence_specs.py`
- Update: `src/extract/registry_specs.py`
- Update: `src/extract/registry_rows.py`

**Implementation checklist**
- [ ] Create EvidenceSpec dataclass and registry mapping.
- [ ] Map each extract dataset to evidence semantics and required columns.
- [ ] Ensure evidence metadata is merged into dataset metadata specs.

**Status**
Planned.

---

### Scope 2: Evidence-Driven Extraction Plan
**Description**
Compile a minimal extraction plan from relationship rule requirements. The plan
selects which evidence tables to materialize and which normalization operators
must run before relationship compilation.

**Code patterns**
```python
# src/extract/evidence_plan.py
@dataclass(frozen=True)
class EvidenceRequirement:
    tables: tuple[str, ...]
    required_columns: tuple[str, ...]


def compile_evidence_plan(rules: Sequence[RelationshipRule]) -> EvidenceRequirement:
    tables = tuple(sorted({t for r in rules for t in r.evidence_sources}))
    cols = tuple(sorted({c for r in rules for c in r.required_columns}))
    return EvidenceRequirement(tables=tables, required_columns=cols)
```

**Target files**
- Add: `src/extract/evidence_plan.py`
- Update: `src/relspec/model.py`
- Update: `src/relspec/compiler_graph.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`

**Implementation checklist**
- [ ] Encode evidence requirements on relationship rules.
- [ ] Compile a minimal evidence plan from the active rule DAG.
- [ ] Drive extract execution from the evidence plan (not ad-hoc toggles).

**Status**
Planned.

---

### Scope 3: Normalization Operators as Named Steps
**Description**
Make normalization operations explicit, composable operators referenced by
relationship rules. This replaces implicit per-extractor normalization logic.

**Code patterns**
```python
# src/extract/normalize_ops.py
@dataclass(frozen=True)
class NormalizeOp:
    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]

NORMALIZE_OPS: dict[str, NormalizeOp] = {
    "byte_span_normalize": NormalizeOp(
        name="byte_span_normalize",
        inputs=("py_cst_name_refs_v1",),
        outputs=("py_cst_name_refs_v1",),
    ),
}
```

**Target files**
- Add: `src/extract/normalize_ops.py`
- Update: `src/extract/helpers.py`
- Update: `src/normalize/runner.py`

**Implementation checklist**
- [ ] Define NormalizeOp registry for all required operators.
- [ ] Reference normalization operators from relationship rule specs.
- [ ] Run only required operators from the evidence plan.

**Status**
Planned.

---

### Scope 4: Evidence Gating + Fallbacks
**Description**
Gate evidence on availability and define fallbacks via rule selection. This
ensures the pipeline runs the best-available evidence and deterministic
fallbacks without bespoke conditionals.

**Code patterns**
```python
# src/relspec/compiler_graph.py
@dataclass(frozen=True)
class EvidenceCatalog:
    available: frozenset[str]

    def has(self, name: str) -> bool:
        return name in self.available


def select_rules(rules: Sequence[RelationshipRule], catalog: EvidenceCatalog) -> list[RuleNode]:
    return [node for node in compile_rule_graph(rules) if catalog.has_all(node.rule.evidence)]
```

**Target files**
- Update: `src/relspec/compiler_graph.py`
- Update: `src/cpg/relation_registry.py`
- Update: `src/cpg/build_edges.py`

**Implementation checklist**
- [ ] Build evidence availability catalog from extraction outputs.
- [ ] Gate rules by evidence availability.
- [ ] Define deterministic fallbacks when superior evidence is missing.

**Status**
Planned.

---

### Scope 5: Semantics-Driven Metadata + Provenance
**Description**
Attach evidence semantics (coordinate system, rank, ambiguity policy) to
extracted tables so relationship compilation and edge emission can rely on
metadata instead of ad-hoc logic.

**Code patterns**
```python
# src/extract/registry_specs.py
meta = dataset_metadata_spec(name)
meta = merge_metadata_specs(meta, evidence_metadata_spec(name))
```

**Target files**
- Update: `src/extract/registry_specs.py`
- Update: `src/arrowdsl/schema/metadata.py`
- Update: `src/cpg/emit_edges.py`

**Implementation checklist**
- [ ] Add evidence metadata to schemas for all evidence tables.
- [ ] Use evidence metadata in relationship compilation policies.
- [ ] Ensure edge emission honors evidence rank + ambiguity policy.

**Status**
Planned.
