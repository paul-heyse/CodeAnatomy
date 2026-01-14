## Normalize Evidence Compiler Intensification Plan

### Goals
- Move normalize from static per-module pipelines to a rule-driven, evidence-aware compiler model.
- Make normalized outputs explicitly typed, contracted, ambiguity-preserving, and deterministic.
- Centralize evidence semantics (source family, confidence, ambiguity policy) in programmatic metadata.
- Enable rule gating and fallback strategies based on evidence availability.
- Produce relationship-ready evidence tables rather than one-off settings.

### Constraints
- Preserve output schemas, column names, and metadata semantics (unless extended explicitly).
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Keep kernel-lane conversions where required (text index lookups, span derivation).

---

### Scope 1: Normalize Rule + Evidence Metadata Model
**Description**
Define rule specs for normalize that encode inputs, outputs, derived columns, and evidence
requirements. Extend the normalize registry to represent rules as data rather than hard-coded
pipelines.

**Code patterns**
```python
# src/normalize/rule_model.py
@dataclass(frozen=True)
class EvidenceSpec:
    sources: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class NormalizeRule:
    name: str
    inputs: tuple[str, ...]
    output: str
    query: QuerySpec
    evidence: EvidenceSpec | None = None
    metadata_extra: Mapping[bytes, bytes] | None = None
```

**Target files**
- Add: `src/normalize/rule_model.py`
- Add: `src/normalize/rule_registry.py`
- Update: `src/normalize/registry_specs.py`

**Implementation checklist**
- [ ] Define `EvidenceSpec` and `NormalizeRule`.
- [ ] Add rule registry entries for all current normalize outputs.
- [ ] Validate required inputs/columns per rule.

**Status**
Planned.

---

### Scope 2: Canonical Normalized Evidence Contracts
**Description**
Define canonical evidence output tables (relationship-ready facts) that every rule emits to.
These contracts are shared across evidence sources and consumed by relationship compilation.

**Code patterns**
```python
# src/normalize/contracts.py
EVIDENCE_FIELDS = (
    "file_id",
    "path",
    "span_id",
    "bstart",
    "bend",
    "evidence_family",
    "source",
    "role",
    "confidence",
    "ambiguity_group_id",
    "rule_name",
)

EVIDENCE_SPEC = dataset_spec_from_schema(
    "normalize_evidence_v1",
    schema=build_evidence_schema(EVIDENCE_FIELDS),
)
```

**Target files**
- Add: `src/normalize/contracts.py`
- Update: `src/normalize/schemas.py`
- Update: `src/normalize/registry_rows.py`

**Implementation checklist**
- [ ] Define canonical evidence contracts for normalized outputs.
- [ ] Ensure all normalize rules emit to these contracts.
- [ ] Update schema registry exports for canonical evidence tables.

**Status**
Planned.

---

### Scope 3: Evidence Semantics Metadata (Templates → Schema)
**Description**
Attach evidence semantics (family, coordinate system, ambiguity policy, superior rank,
determinism tier) to normalize schema metadata via templates, so downstream relationship
compilation can reason about evidence uniformly.

**Code patterns**
```python
# src/normalize/registry_templates.py
RegistryTemplate(
    stage="normalize",
    metadata_extra={
        b"evidence_family": b"cst",
        b"coordinate_system": b"bytes",
        b"ambiguity_policy": b"preserve",
        b"superior_rank": b"2",
    },
    determinism_tier=DeterminismTier.BEST_EFFORT,
)
```

**Target files**
- Update: `src/normalize/registry_templates.py`
- Update: `src/normalize/registry_builders.py`
- Update: `src/normalize/registry_rows.py`

**Implementation checklist**
- [ ] Add evidence semantics metadata to templates.
- [ ] Allow per-dataset overrides via row metadata.
- [ ] Ensure metadata is applied through schema policies.

**Status**
Planned.

---

### Scope 4: Rule DAG + Evidence Gating
**Description**
Compile normalize rules into a DAG with evidence-aware gating. Only execute rules when
inputs are available, and materialize fallback rules when superior evidence is missing.

**Code patterns**
```python
# src/normalize/compiler_graph.py
@dataclass(frozen=True)
class RuleNode:
    name: str
    rule: NormalizeRule
    requires: tuple[str, ...]


def compile_rule_graph(
    rules: Sequence[NormalizeRule],
    *,
    evidence: EvidenceCatalog,
) -> list[RuleNode]:
    return [node for node in nodes if evidence.satisfies(node.rule.evidence)]
```

**Target files**
- Add: `src/normalize/compiler_graph.py`
- Add: `src/normalize/evidence_catalog.py`
- Update: `src/normalize/runner.py`

**Implementation checklist**
- [ ] Model evidence availability in a catalog.
- [ ] Build rule DAG with gating + fallback logic.
- [ ] Cache intermediate rule outputs for reuse.

**Status**
Planned.

---

### Scope 5: Confidence + Ambiguity Policies As Kernels
**Description**
Turn confidence scoring and ambiguity resolution into explicit, reusable kernels so
normalize rules express policy rather than embed logic.

**Code patterns**
```python
# src/normalize/policies.py
def confidence_expr(policy: ConfidencePolicy, *, source_col: str) -> ExprIR:
    return ExprIR(op="call", name="confidence_score", args=(ExprIR(op="field", name=source_col),))


def ambiguity_kernels(policy: AmbiguityPolicy) -> tuple[KernelSpecT, ...]:
    if policy.winner_select is None:
        return ()
    return (WinnerSelectKernelSpec(config=policy.winner_select),)
```

**Target files**
- Add: `src/normalize/policies.py`
- Update: `src/normalize/rule_model.py`
- Update: `src/normalize/rule_registry.py`

**Implementation checklist**
- [ ] Implement confidence expression factories.
- [ ] Implement ambiguity/winner-select kernel factories.
- [ ] Wire policy kernels into rule compilation.

**Status**
Planned.

---

### Scope 6: Normalize Rule Families (Evidence-Driven)
**Description**
Generate normalize rules from semantic templates (symbol references, definitions, calls,
bindings, diagnostics, flow). This replaces one-off logic with rule families.

**Code patterns**
```python
# src/normalize/rule_factories.py
def symbol_reference_rules() -> tuple[NormalizeRule, ...]:
    return build_symbol_rules(
        source="scip_occurrences",
        policy=CONFIDENCE_SCIP,
    )
```

**Target files**
- Add: `src/normalize/rule_factories.py`
- Update: `src/normalize/rule_registry.py`

**Implementation checklist**
- [ ] Add factory helpers per evidence family.
- [ ] Generate rule sets from templates + policies.
- [ ] Remove one-off normalization rules.

**Status**
Planned.

---

### Scope 7: Normalize Compiler Runner + Debug Materializers
**Description**
Expose rule-level metadata, optional rule output materialization, and deterministic ordering
to make normalize outputs traceable and debuggable.

**Code patterns**
```python
# src/normalize/runner.py
if ctx.debug:
    plan = plan.annotate(metadata={"rule": rule.name, "priority": str(rule.priority)})
```

**Target files**
- Update: `src/normalize/runner.py`
- Add: `src/normalize/materializers.py`
- Update: `src/obs/manifest.py`

**Implementation checklist**
- [ ] Attach rule metadata to intermediate outputs.
- [ ] Add optional debug materializers for rule outputs.
- [ ] Enforce deterministic ordering/tie-breakers where defined.

**Status**
Planned.

---

### Scope 8: Migrate Existing Normalize Outputs To Rule Compiler
**Description**
Replace current normalize pipelines with rule-compiled outputs that emit canonical evidence
tables and apply policies consistently.

**Code patterns**
```python
# src/normalize/runner.py
def run_normalize_rules(
    rules: Sequence[NormalizeRule],
    *,
    ctx: ExecutionContext,
) -> dict[str, TableLike]:
    return compile_and_run_rules(rules, ctx=ctx)
```

**Target files**
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/spans.py`

**Implementation checklist**
- [ ] Convert existing normalize outputs to rule-driven emissions.
- [ ] Ensure canonical evidence contracts are used everywhere.
- [ ] Remove static pipeline wiring in favor of rule compilation.

**Status**
Planned.
