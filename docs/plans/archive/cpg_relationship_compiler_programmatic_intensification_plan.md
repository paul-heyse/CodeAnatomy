## CPG Relationship Compiler Programmatic Intensification Plan

### Goals
- Make relationship rules fully data-driven via spec tables and registries.
- Push more rule execution into ArrowDSL plan lanes (minimize materialization).
- Derive policies, ordering, and schema alignment from contracts and metadata.
- Improve determinism, lineage, and auditability for relationship outputs.

### Constraints
- Preserve output schemas, column names, and metadata semantics for CPG tables.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Maintain compatibility with ArrowDSL plan/contract mechanics.

---

### Scope 1: Spec-Table Driven Rule Families + Registry
**Description**
Move relationship families into spec tables and use a registry of factory
callables, so new families can be added without editing Python code.

**Code patterns**
```python
# src/cpg/relation_registry_specs.py
@dataclass(frozen=True)
class RuleFamilySpec:
    name: str
    factory: str
    inputs: tuple[str, ...]
    policy: str | None = None
    option_flag: str | None = None


# src/cpg/relation_factories.py
RULE_FAMILY_FACTORIES: dict[str, RuleFamilyFactory] = {
    "symbol_role": build_symbol_role_rules,
    "scip_symbol": build_scip_symbol_rules,
}


def build_rules_from_specs(specs: Sequence[RuleFamilySpec]) -> tuple[RelationshipRule, ...]:
    rules: list[RelationshipRule] = []
    for spec in specs:
        factory = RULE_FAMILY_FACTORIES[spec.factory]
        rules.extend(factory(spec))
    return tuple(rules)
```

**Target files**
- Add: `src/cpg/relation_registry_specs.py`
- Update: `src/arrowdsl/spec/tables/relspec.py`
- Update: `src/cpg/relation_factories.py`
- Update: `src/cpg/relation_registry.py`
- Update: `src/cpg/spec_tables.py`

**Implementation checklist**
- [x] Define a RuleFamily spec table and its Arrow encoding.
- [x] Add a registry mapping spec factory names to callables.
- [x] Build relationship rules from specs rather than hardcoded lists.
- [x] Wire registry table into `relation_rule_table_cached`.

**Status**
Completed.

---

### Scope 2: Policy Derivation from Schema Metadata
**Description**
Derive confidence and ambiguity policies from contract metadata so rules
inherit consistent defaults without bespoke wiring.

**Code patterns**
```python
# src/relspec/policies.py
def confidence_policy_from_contract(contract: Contract) -> ConfidencePolicy:
    meta = contract.schema.metadata or {}
    base = float(meta.get(b"confidence_base", b"1.0"))
    penalty = float(meta.get(b"confidence_penalty", b"0.0"))
    return ConfidencePolicy(base=base, penalty=penalty)


def rule_with_default_policies(rule: RelationshipRule, contract: Contract) -> RelationshipRule:
    if rule.confidence_policy is None:
        rule = replace(rule, confidence_policy=confidence_policy_from_contract(contract))
    return rule
```

**Target files**
- Update: `src/relspec/policies.py`
- Update: `src/relspec/model.py`
- Update: `src/relspec/compiler.py`
- Update: `src/relspec/contracts.py`

**Implementation checklist**
- [x] Define policy metadata keys in contract schema metadata.
- [x] Derive default policies from contract metadata.
- [x] Apply defaults when rules omit explicit policies.

**Status**
Completed.

---

### Scope 3: Arrow-First Compilation Modes
**Description**
Add explicit execution modes so rule compilation can be plan-only or allow
fallback to table execution when plan-lane support is unavailable.

**Code patterns**
```python
# src/relspec/model.py
type ExecutionMode = Literal["auto", "plan", "table"]


@dataclass(frozen=True)
class RelationshipRule:
    ...
    execution_mode: ExecutionMode = "auto"


# src/relspec/compiler.py
if rule.execution_mode == "plan" and compiled.plan is None:
    raise ValueError(f"Rule {rule.name!r} requires plan execution.")
```

**Target files**
- Update: `src/relspec/model.py`
- Update: `src/relspec/compiler.py`
- Update: `src/cpg/relation_registry.py`

**Implementation checklist**
- [x] Add execution mode to rule model.
- [x] Enforce plan-only mode in the compiler.
- [x] Allow table fallback when execution mode is `auto`.

**Status**
Completed.

---

### Scope 4: Typed Evidence Gating + Virtual Outputs
**Description**
Expand evidence gating to check column types and build a virtual output
graph so downstream rules can reason about schemas without re-scanning.

**Code patterns**
```python
# src/relspec/model.py
@dataclass(frozen=True)
class EvidenceSpec:
    sources: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    required_types: dict[str, str] = field(default_factory=dict)


# src/relspec/compiler_graph.py
def satisfies(self, spec: EvidenceSpec | None, *, inputs: Sequence[str]) -> bool:
    ...
    for col, dtype in spec.required_types.items():
        if dtype not in self.column_types.get(source, {}).get(col, set()):
            return False
```

**Target files**
- Update: `src/relspec/model.py`
- Update: `src/relspec/compiler_graph.py`
- Update: `src/cpg/relation_registry.py`

**Implementation checklist**
- [x] Track column types in the evidence catalog.
- [x] Enforce column type requirements in evidence gating.
- [x] Emit virtual output schema nodes for downstream dependency checks.

**Status**
Completed.

---

### Scope 5: Rule Metadata + Lineage Manifest
**Description**
Make rule metadata a stable schema metadata artifact (not just debug-only)
and generate a lineage manifest derived from compiled outputs.

**Code patterns**
```python
# src/relspec/compiler.py
meta = SchemaMetadataSpec(schema_metadata={
    b"rule_name": rule.name.encode("utf-8"),
    b"rule_priority": str(rule.priority).encode("utf-8"),
})
table = meta.apply(table.schema).cast(table.schema)


# src/obs/manifest.py
RuleRecord(
    name=rule.name,
    output_dataset=rule.output_dataset,
    evidence=asdict(rule.evidence) if rule.evidence else None,
)
```

**Target files**
- Update: `src/relspec/compiler.py`
- Update: `src/obs/manifest.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`

**Implementation checklist**
- [x] Apply rule metadata in all execution modes.
- [x] Emit lineage/metadata details into manifest records.
- [x] Ensure rule metadata is stable across debug and non-debug runs.

**Status**
Completed.

---

### Scope 6: Ambiguity Policy Registry + Plan-Lane Winner Select
**Description**
Centralize ambiguity handling in a policy registry and enable plan-lane
winner selection to avoid materialization.

**Code patterns**
```python
# src/relspec/policy_registry.py
AMBIGUITY_POLICIES: dict[str, AmbiguityPolicy] = {
    "qname_fallback": AmbiguityPolicy(
        winner_select=WinnerSelectConfig(keys=("call_id",), score_col="score"),
    ),
}


# src/arrowdsl/plan/ops.py
@dataclass(frozen=True)
class WinnerSelectOp(PlanOp):
    spec: DedupeSpec
```

**Target files**
- Add: `src/relspec/policy_registry.py`
- Update: `src/arrowdsl/plan/ops.py`
- Update: `src/arrowdsl/compute/kernels.py`
- Update: `src/relspec/compiler.py`
- Update: `src/cpg/relation_factories.py`

**Implementation checklist**
- [x] Add a named policy registry for ambiguity policies.
- [x] Introduce a plan-lane winner-select operation.
- [x] Use policy names in rule specs/factories instead of inline config.

**Status**
Completed.

---

### Scope 7: Schema Evolution + Validation Automation
**Description**
Derive the relation output schema from contract specs and enforce policy
requirements (e.g., ambiguity group columns when ambiguity is enabled).

**Code patterns**
```python
# src/relspec/contracts.py
def relation_output_schema() -> pa.Schema:
    return GLOBAL_SCHEMA_REGISTRY.contract("relation_output_v1").schema


# src/relspec/compiler.py
if rule.ambiguity_policy and "ambiguity_group_id" not in schema.names:
    raise ValueError("Ambiguity policy requires ambiguity_group_id column.")
```

**Target files**
- Update: `src/relspec/contracts.py`
- Update: `src/relspec/compiler.py`
- Update: `src/schema_spec/system.py`

**Implementation checklist**
- [x] Resolve relation output schema from contract registry.
- [x] Enforce policy-specific schema requirements.
- [x] Validate outputs against the derived schema spec.

**Status**
Completed.

---

### Scope 8: Deterministic Tie-Breakers Derived from Schema
**Description**
Generate tie-breaker ordering from schema metadata and ID conventions so
determinism is consistent without manual configuration.

**Code patterns**
```python
# src/relspec/policies.py
def default_tie_breakers(schema: pa.Schema) -> tuple[SortKey, ...]:
    keys = infer_ordering_keys(schema.names)
    return tuple(SortKey(name, "ascending") for name, _ in keys)
```

**Target files**
- Update: `src/relspec/policies.py`
- Update: `src/relspec/compiler.py`
- Update: `src/arrowdsl/schema/metadata.py`

**Implementation checklist**
- [x] Derive default ordering keys from schema names/metadata.
- [x] Inject deterministic tie-breakers into ambiguity policies.
- [x] Ensure stable ordering across plan and table execution paths.

**Status**
Completed.
