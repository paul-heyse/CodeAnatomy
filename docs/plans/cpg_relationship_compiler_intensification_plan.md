## CPG Relationship Compiler Intensification Plan

### Goals
- Move CPG relationships from static mappings to a logic-driven, evidence-aware compiler model.
- Make relationships explicitly typed, contracted, ambiguity-preserving, and deterministic.
- Centralize relationship semantics so new evidence sources can be added without bespoke wiring.

### Constraints
- Preserve output schemas, column names, and metadata semantics for CPG tables.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Maintain compatibility with ArrowDSL plan/contract mechanics.

---

### Scope 1: Relationship Rule Metadata Model (Evidence + Policy)
**Description**
Extend relationship rule specs to include evidence sources, confidence policy, ambiguity policy,
and preconditions (required columns and evidence availability). Encode/deserialize these fields
in relspec tables.

**Code patterns**
```python
# src/relspec/model.py
@dataclass(frozen=True)
class EvidenceSpec:
    sources: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class ConfidencePolicy:
    base: float = 0.5
    source_weight: Mapping[str, float] = field(default_factory=dict)
    penalty: float = 0.0


@dataclass(frozen=True)
class AmbiguityPolicy:
    winner_select: WinnerSelectConfig | None = None
    tie_breakers: tuple[SortKey, ...] = ()


@dataclass(frozen=True)
class RelationshipRule:
    ...
    evidence: EvidenceSpec | None = None
    confidence_policy: ConfidencePolicy | None = None
    ambiguity_policy: AmbiguityPolicy | None = None
```

**Target files**
- Update: `src/relspec/model.py`
- Update: `src/arrowdsl/spec/tables/relspec.py`
- Update: `src/relspec/compiler.py`

**Implementation checklist**
- [x] Add evidence/confidence/ambiguity models to relspec.
- [x] Encode/decode new metadata in relspec tables.
- [x] Validate preconditions in compiler (required columns/evidence).

**Status**
Completed.

---

### Scope 2: Canonical Relationship Output Contract
**Description**
Define a single canonical relationship output schema/contract and require all rules
to emit to that contract before edge emission. Edge emission becomes fully generic.

**Code patterns**
```python
# src/relspec/contracts.py
RELATION_OUTPUT_FIELDS = (
    "src",
    "dst",
    "path",
    "bstart",
    "bend",
    "origin",
    "resolution_method",
    "confidence",
    "score",
    "ambiguity_group_id",
    "rule_name",
    "rule_priority",
)

RELATION_OUTPUT_SPEC = dataset_spec_from_schema(
    "relation_output_v1",
    schema=build_relation_schema(RELATION_OUTPUT_FIELDS),
)
```

**Target files**
- Add: `src/relspec/contracts.py`
- Update: `src/cpg/emit_edges.py`
- Update: `src/relspec/edge_contract_validator.py`

**Implementation checklist**
- [x] Define a canonical relation output schema/contract.
- [x] Enforce alignment before edge emission.
- [x] Validate relation outputs against the contract.

**Status**
Completed.

---

### Scope 3: Rule DAG + Evidence Gating
**Description**
Compile rules as a DAG with evidence-aware gating (SCIP available → high-confidence rules;
otherwise fallback rules). Order and execute only viable rules; reuse intermediate outputs.

**Code patterns**
```python
# src/relspec/compiler_graph.py
@dataclass(frozen=True)
class RuleNode:
    name: str
    rule: RelationshipRule
    requires: tuple[str, ...]


def compile_rule_graph(
    rules: Sequence[RelationshipRule],
    *,
    evidence: EvidenceCatalog,
) -> list[RuleNode]:
    return [node for node in nodes if evidence.satisfies(node.rule.evidence)]
```

**Target files**
- Add: `src/relspec/compiler_graph.py`
- Update: `src/cpg/relation_registry.py`
- Update: `src/cpg/build_edges.py`

**Implementation checklist**
- [x] Model evidence availability in a catalog.
- [x] Build rule DAG with gating + fallback logic.
- [x] Execute only eligible rules and cache intermediate outputs.

**Status**
Completed.

---

### Scope 4: Confidence + Ambiguity Kernels as First-Class Policies
**Description**
Turn confidence scoring and ambiguity resolution into explicit, reusable kernels
so the rules express policy rather than inline logic.

**Code patterns**
```python
# src/relspec/policies.py
def confidence_expr(policy: ConfidencePolicy, *, source_col: str) -> ExprIR:
    return ExprIR(op="call", name="confidence_score", args=(ExprIR(op="field", name=source_col),))


def ambiguity_kernels(policy: AmbiguityPolicy) -> tuple[KernelSpecT, ...]:
    if policy.winner_select is None:
        return ()
    return (WinnerSelectKernelSpec(config=policy.winner_select),)
```

**Target files**
- Add: `src/relspec/policies.py`
- Update: `src/relspec/model.py`
- Update: `src/relspec/compiler.py`
- Update: `src/cpg/relation_registry.py`

**Implementation checklist**
- [x] Implement confidence expression factories.
- [x] Implement ambiguity/winner-select kernel factories.
- [x] Wire policy-driven kernels into rule compilation.

**Status**
Completed.

---

### Scope 5: Relationship Families via Rule Factories
**Description**
Replace flat rule lists with “rule families” generated from semantic templates
(symbol resolution, call resolution, binding rules, diagnostics, runtime overlays).

**Code patterns**
```python
# src/cpg/relation_registry.py
def symbol_resolution_rules() -> tuple[RelationshipRule, ...]:
    return build_symbol_rules(
        source="scip_symbol_relationships",
        policy=CONFIDENCE_SCIP,
    )
```

**Target files**
- Update: `src/cpg/relation_registry.py`
- Add: `src/cpg/relation_factories.py`

**Implementation checklist**
- [x] Add factory helpers per relationship family.
- [x] Generate rule sets from templates + policy inputs.
- [x] Remove one-off rule definitions.

**Status**
Completed.

---

### Scope 6: Deterministic Ordering + Debuggability
**Description**
Expose run metadata, intermediate materializers, and traceable rule outputs
to ensure determinism and make failures diagnosable.

**Code patterns**
```python
# src/relspec/compiler.py
if ctx.debug:
    plan = plan.annotate(metadata={"rule": rule.name, "priority": str(rule.priority)})
```

**Target files**
- Update: `src/relspec/compiler.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/obs/manifest.py`

**Implementation checklist**
- [x] Attach rule metadata to intermediate outputs.
- [x] Optional debug materialization for rule outputs.
- [x] Deterministic sort + tie-breaker enforcement.

**Status**
Completed.
