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
    alias: str
    template: str | None
    evidence_family: str | None
    coordinate_system: str | None
    evidence_rank: int | None
    required_columns: tuple[str, ...]
    ambiguity_policy: str | None = None
```

**Target files**
- Add: `src/extract/evidence_specs.py`
- Update: `src/extract/registry_specs.py`
- Update: `src/extract/registry_rows.py`

**Implementation checklist**
- [x] Create EvidenceSpec dataclass and registry mapping.
- [x] Map each extract dataset to evidence semantics and required columns.
- [x] Ensure evidence metadata is merged into dataset metadata specs.

**Status**
Completed.

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
class EvidencePlan:
    sources: tuple[str, ...]
    normalize_ops: tuple[str, ...]
    requirements: tuple[EvidenceRequirement, ...]
```

**Target files**
- Add: `src/extract/evidence_plan.py`
- Update: `src/relspec/model.py`
- Update: `src/relspec/compiler_graph.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`

**Implementation checklist**
- [x] Encode evidence sources on relationship rules (or fall back to inputs).
- [ ] Extend rule specs to supply required_columns/required_types where needed.
- [x] Compile a minimal evidence plan from the active rule DAG.
- [x] Drive extract execution from the evidence plan (not ad-hoc toggles).

**Status**
Mostly complete (missing rule-level required_columns/required_types coverage).

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
```

**Target files**
- Add: `src/extract/normalize_ops.py`
- Update: `src/extract/helpers.py`
- Update: `src/normalize/runner.py`

**Implementation checklist**
- [x] Define NormalizeOp registry for required operators.
- [x] Expand normalization requirements from required outputs in evidence plans.
- [ ] Wire evidence-plan normalize_ops (or required outputs) into normalize execution.

**Status**
Partially complete (normalize execution still runs full rule set by default).

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
- [x] Build evidence availability catalog from extraction outputs.
- [x] Gate rules by evidence availability.
- [x] Define deterministic fallbacks via rule ordering/priority.

**Status**
Completed.

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
meta = merge_metadata_specs(meta, extract_evidence_metadata_spec(name))
```

**Target files**
- Update: `src/extract/registry_specs.py`
- Update: `src/arrowdsl/schema/metadata.py`
- Update: `src/cpg/emit_edges.py`

**Implementation checklist**
- [x] Add evidence metadata to schemas for all evidence tables.
- [ ] Use evidence metadata in relationship compilation defaults beyond ambiguity_policy.
- [ ] Incorporate evidence_rank into edge scoring/tie-breakers when present.

**Status**
Partially complete (ambiguity_policy metadata is honored; evidence_rank unused).
