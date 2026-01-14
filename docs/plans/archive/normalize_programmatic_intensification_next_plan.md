## Normalize Programmatic Intensification (Next) Plan

### Goals
- Make normalize rules fully table-driven with minimal bespoke Python factories.
- Derive evidence, policies, and execution modes from schema metadata by default.
- Push more normalize execution into ArrowDSL plan lanes for determinism and auditability.
- Enable graph-level normalize plan compilation with shared scans and ordering.
- Remove remaining static alias mappings in pipeline wiring.

### Constraints
- Preserve current output schemas, column names, and metadata semantics.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Keep kernel-only operations (text index lookups, span conversion) isolated.

---

### Scope 1: Fully Table-Driven Normalize Rule Definitions
**Description**
Replace bespoke rule-family factories with a rule-definition spec table that encodes
inputs, outputs, query ops, evidence requirements, and policy overrides.

**Code patterns**
```python
# src/arrowdsl/spec/tables/normalize.py
NORMALIZE_RULE_DEF_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("inputs", pa.list_(pa.string()), nullable=False),
        pa.field("output", pa.string(), nullable=False),
        pa.field("execution_mode", pa.string(), nullable=True),
        pa.field("query_ops", pa.list_(QUERY_OP_STRUCT), nullable=True),
        pa.field("evidence", EVIDENCE_STRUCT, nullable=True),
        pa.field("evidence_output", EVIDENCE_OUTPUT_STRUCT, nullable=True),
        pa.field("policy_overrides", POLICY_OVERRIDE_STRUCT, nullable=True),
    ],
    metadata={b"spec_kind": b"normalize_rule_definitions"},
)


def normalize_rules_from_table(table: pa.Table) -> tuple[NormalizeRule, ...]:
    rules: list[NormalizeRule] = []
    for row in table.to_pylist():
        query = _query_from_row(row.get("query_ops"))
        evidence = _evidence_from_row(row.get("evidence"))
        evidence_output = _evidence_output_from_row(row.get("evidence_output"))
        policies = _policy_overrides_from_row(row.get("policy_overrides"))
        rules.append(
            NormalizeRule(
                name=str(row["name"]),
                inputs=tuple(row["inputs"] or ()),
                output=str(row["output"]),
                query=query,
                evidence=evidence,
                evidence_output=evidence_output,
                confidence_policy=policies.confidence_policy,
                ambiguity_policy=policies.ambiguity_policy,
                execution_mode=_mode_from_row(row.get("execution_mode")),
            )
        )
    return tuple(rules)
```

**Target files**
- Add/Update: `src/arrowdsl/spec/tables/normalize.py`
- Update: `src/normalize/spec_tables.py`
- Update: `src/normalize/rule_registry.py`
- Update: `src/normalize/rule_factories.py`

**Implementation checklist**
- [x] Add normalize rule definition schema + row encoders/decoders.
- [x] Compile rule rows into NormalizeRule instances (minimal Python logic).
- [x] Replace family factories with table-driven rule construction.

**Status**
Completed.

---

### Scope 2: Rule Templates + Macro Expansion
**Description**
Introduce template specs that expand into multiple normalize rules (e.g., repeated
bytecode or type rules), reducing duplication and keeping patterns declarative.

**Code patterns**
```python
# src/normalize/rule_template_specs.py
@dataclass(frozen=True)
class RuleTemplateSpec:
    name: str
    factory: str
    inputs: tuple[str, ...]
    params: Mapping[str, ScalarValue] = field(default_factory=dict)


def expand_rule_templates(specs: Sequence[RuleTemplateSpec]) -> tuple[NormalizeRuleFamilySpec, ...]:
    expanded: list[NormalizeRuleFamilySpec] = []
    for spec in specs:
        for row in TEMPLATE_REGISTRY[spec.factory](spec.params):
            expanded.append(
                NormalizeRuleFamilySpec(
                    name=f"{spec.name}_{row.name}",
                    factory=row.factory,
                    inputs=spec.inputs,
                    execution_mode=row.execution_mode,
                )
            )
    return tuple(expanded)
```

**Target files**
- Add: `src/normalize/rule_template_specs.py`
- Update: `src/normalize/rule_registry_specs.py`
- Update: `src/normalize/rule_registry.py`

**Implementation checklist**
- [x] Define template spec dataclass + template registry.
- [x] Expand templates into rule-family specs at registry build time.
- [x] Replace repeated rule families with templates.

**Status**
Completed.

---

### Scope 3: Metadata-Driven Evidence + Output Mapping Defaults
**Description**
Derive evidence requirements and evidence-output mappings from schema metadata so
rule rows can omit them when defaults are encoded in contracts.

**Code patterns**
```python
# src/normalize/policies.py or src/normalize/evidence_specs.py
EVIDENCE_REQUIRED_COLS_META = b"evidence_required_columns"
EVIDENCE_REQUIRED_TYPES_META = b"evidence_required_types"
EVIDENCE_OUTPUT_MAP_META = b"evidence_output_map"


def evidence_spec_from_schema(schema: SchemaLike) -> EvidenceSpec:
    cols = _meta_list(schema.metadata, EVIDENCE_REQUIRED_COLS_META)
    types = _meta_map(schema.metadata, EVIDENCE_REQUIRED_TYPES_META)
    return EvidenceSpec(required_columns=tuple(cols), required_types=types)


def evidence_output_from_schema(schema: SchemaLike) -> EvidenceOutput | None:
    mapping = _meta_map(schema.metadata, EVIDENCE_OUTPUT_MAP_META)
    if not mapping:
        return None
    return EvidenceOutput(column_map=mapping)
```

**Target files**
- Add: `src/normalize/evidence_specs.py`
- Update: `src/normalize/registry_templates.py`
- Update: `src/normalize/runner.py`

**Implementation checklist**
- [x] Add metadata keys for evidence requirements and evidence output mappings.
- [x] Decode defaults from schema metadata when rule rows omit them.
- [x] Apply defaults during rule compilation.

**Status**
Completed.

---

### Scope 4: Normalize Ops Registry For Extraction Evidence Plans
**Description**
Generate `extract.normalize_ops` from normalize rule specs to keep evidence planning
fully aligned with normalize rule outputs.

**Code patterns**
```python
# src/extract/normalize_ops.py
def normalize_ops() -> tuple[NormalizeOp, ...]:
    rows = normalize_rule_spec_rows()
    ops: list[NormalizeOp] = []
    for row in rows:
        ops.append(
            NormalizeOp(
                name=row.name,
                inputs=tuple(row.inputs),
                outputs=(row.output,),
            )
        )
    return tuple(ops)
```

**Target files**
- Update: `src/normalize/spec_tables.py`
- Update: `src/extract/normalize_ops.py`

**Implementation checklist**
- [x] Export normalize rule spec rows from normalize registry tables.
- [x] Generate normalize ops from spec rows (input -> output).
- [x] Remove static normalize-op lists once parity is reached.

**Status**
Completed.

---

### Scope 5: Plan-Lane Kernel Coverage Expansion
**Description**
Expand ArrowDSL plan-lane op coverage to reduce table materialization for normalize
rules (e.g., rename/alias, list explode, flatten list-struct fields).

**Code patterns**
```python
# src/arrowdsl/plan/ops.py
@dataclass(frozen=True)
class RenameColumnsOp(PlanOp):
    mapping: Mapping[str, str]


# src/arrowdsl/plan/plan.py
def rename_columns(self, mapping: Mapping[str, str], *, ctx: ExecutionContext) -> Plan:
    return self._apply_plan_op(RenameColumnsOp(mapping=mapping), ctx=ctx)
```

**Target files**
- Update: `src/arrowdsl/plan/ops.py`
- Update: `src/arrowdsl/plan/plan.py`
- Update: `src/normalize/diagnostics_plans.py`
- Update: `src/normalize/spans.py`
- Update: `src/normalize/bytecode_anchor.py`

**Implementation checklist**
- [x] Identify kernel-only steps still used by normalize plans.
- [x] Add plan-lane ops + Plan helpers for those kernels.
- [x] Enforce plan-only execution where coverage is complete.

**Status**
Completed.

---

### Scope 6: Graph-Level Normalize Plan Compilation
**Description**
Compile eligible normalize rules into a graph-level plan with shared scans and
per-output subplans, mirroring relationship compiler behavior.

**Code patterns**
```python
# src/normalize/compiler_graph.py
@dataclass(frozen=True)
class NormalizeGraphPlan:
    plan: Plan
    outputs: dict[str, Plan]


def compile_graph_plan(compilation: NormalizeRuleCompilation, *, ctx: ExecutionContext) -> NormalizeGraphPlan:
    ordered = compilation.rules
    plans = {rule.output: compilation.plans[rule.output] for rule in ordered}
    union = union_all_plans(tuple(plans.values()), label="normalize_graph")
    return NormalizeGraphPlan(plan=union, outputs=plans)
```

**Target files**
- Update: `src/normalize/compiler_graph.py`
- Update: `src/normalize/runner.py`

**Implementation checklist**
- [x] Add graph-level plan container with per-output subplans.
- [x] Support execution of full graph plan or single-output plans.
- [x] Keep evidence gating and ordering intact.

**Status**
Completed.

---

### Scope 7: Registry/Contract Validation Automation
**Description**
Add validation that rule specs, evidence metadata, and contracts are consistent,
failing early when mismatches are introduced.

**Code patterns**
```python
# src/normalize/registry_validation.py
def validate_rule_specs(rules: Sequence[NormalizeRule]) -> None:
    for rule in rules:
        schema = dataset_schema(rule.output)
        _ensure_evidence_columns(rule, schema)
        _ensure_policy_columns(rule, normalize_evidence_schema())
```

**Target files**
- Add: `src/normalize/registry_validation.py`
- Add: `tests/normalize/test_registry_validation.py`
- Update: `src/normalize/rule_registry.py`

**Implementation checklist**
- [x] Validate rule outputs against dataset registry.
- [x] Validate evidence requirements against schema metadata.
- [x] Validate ambiguity policy keys against evidence schema.

**Status**
Completed.

---

### Scope 8: Alias/Name Normalization For Pipeline Outputs
**Description**
Replace static alias maps in the pipeline with registry-driven name resolution
to ensure consistent versioned dataset usage.

**Code patterns**
```python
# src/normalize/registry_specs.py
def dataset_alias(name: str) -> str:
    row = dataset_row(name)
    return row.alias or row.name


# src/hamilton_pipeline/modules/normalization.py
def _required_rule_outputs(plan: EvidencePlan | None) -> tuple[str, ...] | None:
    if plan is None:
        return None
    return tuple(dataset_name_from_alias(alias) for alias in plan.sources)
```

**Target files**
- Update: `src/normalize/registry_specs.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`

**Implementation checklist**
- [x] Add dataset alias helpers to the normalize registry.
- [x] Resolve pipeline output names via registry aliases.
- [x] Remove static alias maps once registry resolution is complete.

**Status**
Completed.
