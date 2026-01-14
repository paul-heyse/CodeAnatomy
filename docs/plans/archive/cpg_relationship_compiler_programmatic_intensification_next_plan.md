## CPG Relationship Compiler Programmatic Intensification (Next) Plan

### Goals
- Make relationship rules fully table-driven, minimizing bespoke Python factories.
- Push all feasible rule execution into ArrowDSL plan lanes.
- Derive evidence and policy requirements from schema metadata by default.
- Consolidate relationship + edge emission specs into a single registry.
- Enable graph-level plan compilation for more end-to-end Arrow execution.

### Constraints
- Preserve output schemas, column names, and metadata semantics for CPG tables.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Maintain compatibility with ArrowDSL plan/contract mechanics.

---

### Scope 1: Fully Table-Driven Rule Definitions
**Description**
Extend the rule spec table to directly encode predicates, projections, and
post-kernel pipelines so rule factories become thin row-to-rule compilers.

**Code patterns**
```python
# src/arrowdsl/spec/tables/relspec.py
RULE_DEFINITION_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("inputs", pa.list_(DATASET_REF_STRUCT), nullable=False),
        pa.field("predicate_expr", pa.string(), nullable=True),
        pa.field("project_select", pa.list_(pa.string()), nullable=True),
        pa.field("project_exprs", pa.list_(PROJECT_EXPR_STRUCT), nullable=True),
        pa.field("post_kernels", pa.list_(KERNEL_SPEC_STRUCT), nullable=True),
        pa.field("evidence", EVIDENCE_STRUCT, nullable=True),
    ],
    metadata={b"spec_kind": b"relationship_rule_definitions"},
)


def relationship_rules_from_table(table: pa.Table) -> tuple[RelationshipRule, ...]:
    rules: list[RelationshipRule] = []
    for row in table.to_pylist():
        predicate = _decode_expr(row["predicate_expr"]) if row.get("predicate_expr") else None
        project = _project_from_row(row)
        post_kernels = tuple(_kernel_from_row(item) for item in row.get("post_kernels") or ())
        rules.append(
            RelationshipRule(
                name=str(row["name"]),
                kind=RuleKind(str(row["kind"])),
                inputs=tuple(_dataset_ref_from_row(item) for item in row.get("inputs") or ()),
                project=project,
                post_kernels=post_kernels,
                evidence=_evidence_from_row(row.get("evidence")),
            )
        )
    return tuple(rules)
```

**Target files**
- Update: `src/arrowdsl/spec/tables/relspec.py`
- Update: `src/cpg/relation_factories.py`
- Update: `src/cpg/relation_registry.py`
- Update: `src/cpg/relation_registry_specs.py`

**Implementation checklist**
- [x] Extend the rule spec table schema with predicate/projection columns.
- [x] Implement row-to-rule compilation for predicates and projections.
- [x] Remove bespoke predicate logic from Python factories where possible.
- [x] Ensure evidence and post-kernel definitions are fully table-driven.

**Status**
Completed. Rule definition tables now encode predicates, projections, and kernel
pipelines, and rules are compiled directly from the table representation.

---

### Scope 2: Rule Templates and Macro Expansion
**Description**
Introduce template specs that expand into multiple rules (e.g., role-based
rules) so repeated patterns are generated programmatically.

**Code patterns**
```python
# src/cpg/relation_registry_specs.py
@dataclass(frozen=True)
class RuleTemplateSpec:
    name: str
    factory: str
    inputs: tuple[str, ...]
    params: Mapping[str, ScalarValue] = field(default_factory=dict)


def expand_rule_templates(specs: Sequence[RuleTemplateSpec]) -> tuple[RuleFamilySpec, ...]:
    expanded: list[RuleFamilySpec] = []
    for spec in specs:
        for rule_name, param in TEMPLATE_REGISTRY[spec.factory](spec.params):
            expanded.append(
                RuleFamilySpec(
                    name=f"{spec.name}_{rule_name}",
                    factory=spec.factory,
                    inputs=spec.inputs,
                    option_flag=param.get("option_flag"),
                )
            )
    return tuple(expanded)
```

**Target files**
- Add: `src/cpg/relation_template_specs.py`
- Update: `src/cpg/relation_registry_specs.py`
- Update: `src/cpg/relation_registry.py`

**Implementation checklist**
- [x] Define template specs and a registry of template expanders.
- [x] Expand templates into rule-family specs at registry build time.
- [x] Replace repeated rule families (e.g., symbol roles) with templates.

**Status**
Completed. Implemented in `src/cpg/relation_template_specs.py` and consumed via
`src/cpg/relation_registry_specs.py`.

---

### Scope 3: Metadata-Driven Evidence + Policy Defaults
**Description**
Infer evidence requirements and policy defaults directly from schema metadata
so rule rows stay minimal and coherent with contracts.

**Code patterns**
```python
# src/relspec/policies.py
EVIDENCE_REQUIRED_COLS_META = b"evidence_required_columns"
EVIDENCE_REQUIRED_TYPES_META = b"evidence_required_types"


def evidence_spec_from_schema(schema: SchemaLike) -> EvidenceSpec:
    required_cols = _meta_list(schema.metadata, EVIDENCE_REQUIRED_COLS_META)
    required_types = _meta_map(schema.metadata, EVIDENCE_REQUIRED_TYPES_META)
    return EvidenceSpec(required_columns=tuple(required_cols), required_types=required_types)


# src/relspec/compiler.py
schema = _schema_for_rule(rule, contracts=contract_catalog)
if schema is not None and rule.evidence is None:
    rule = replace(rule, evidence=evidence_spec_from_schema(schema))
```

**Target files**
- Update: `src/relspec/policies.py`
- Update: `src/relspec/compiler.py`
- Update: `src/arrowdsl/schema/metadata.py`

**Implementation checklist**
- [x] Define schema metadata keys for evidence requirements.
- [x] Add helpers to derive EvidenceSpec from schema metadata.
- [x] Apply inferred evidence and policy defaults during compilation.

**Status**
Completed. Evidence inference now flows through `src/relspec/policies.py`,
`src/relspec/compiler.py`, and `src/arrowdsl/schema/metadata.py`.

---

### Scope 4: Unified Relationship + Edge Emission Specs
**Description**
Consolidate relationship rule specs and edge emission specs into a single
registry table to eliminate parallel configuration.

**Code patterns**
```python
# src/cpg/spec_tables.py
EDGE_RULE_SCHEMA = pa.schema(
    [
        pa.field("rule_name", pa.string(), nullable=False),
        pa.field("edge_kind", pa.string(), nullable=False),
        pa.field("src_cols", pa.list_(pa.string()), nullable=False),
        pa.field("dst_cols", pa.list_(pa.string()), nullable=False),
        pa.field("origin", pa.string(), nullable=False),
        pa.field("resolution_method", pa.string(), nullable=False),
    ],
    metadata={b"spec_kind": b"relationship_edge_specs"},
)


def edge_plan_specs_from_rules(rules: Sequence[RelationshipRule]) -> tuple[EdgePlanSpec, ...]:
    return tuple(_edge_spec_from_rule(rule) for rule in rules if rule.emit_edge)
```

**Target files**
- Update: `src/cpg/spec_tables.py`
- Update: `src/cpg/relation_factories.py`
- Update: `src/cpg/relation_registry.py`
- Update: `src/cpg/spec_registry.py`

**Implementation checklist**
- [x] Define a combined rule+edge spec table.
- [x] Compile EdgePlanSpec entries from the unified spec table.
- [x] Remove standalone edge-plan spec tables once parity is reached.

**Status**
Completed. Edge plan specs are derived from the unified rule table and the
standalone edge spec table has been removed from the core registry flow.

---

### Scope 5: Plan-Lane Kernel Coverage Expansion
**Description**
Extend plan-lane kernel compilation to cover more kernel specs so
`execution_mode="plan"` is possible for more rules.

**Code patterns**
```python
# src/relspec/compiler.py
if isinstance(spec, ExplodeListSpec):
    plan = plan.explode_list(spec.list_col, out_value_col=spec.out_value_col)
    continue
if isinstance(spec, RenameColumnsSpec):
    plan = _apply_rename_columns_to_plan(plan, spec, ctx=ctx, rule=rule)
    continue
```

**Target files**
- Update: `src/relspec/compiler.py`
- Update: `src/arrowdsl/plan/plan.py`
- Update: `src/arrowdsl/plan/ops.py`

**Implementation checklist**
- [x] Identify kernel specs still table-only.
- [x] Add plan-lane equivalents (ops + Plan methods).
- [x] Enforce plan-only execution where coverage is complete.

**Status**
Completed. Canonical sort and explode-list now have plan-aware handling, with
plan-only execution preserved when supported kernels are covered.

---

### Scope 6: Graph-Level Plan Compilation
**Description**
Compile the entire relationship rule graph into a single ArrowDSL plan when
dependencies allow, minimizing per-rule materialization and enabling unified
optimization.

**Code patterns**
```python
# src/relspec/compiler_graph.py
@dataclass(frozen=True)
class GraphPlan:
    plan: Plan
    outputs: dict[str, Plan]


def compile_graph_plan(rules: Sequence[RelationshipRule], resolver: PlanResolver) -> GraphPlan:
    ordered = order_rules(rules, evidence=EvidenceCatalog.from_plan_catalog(...))
    plans = {rule.output_dataset: compile_rule_plan(rule, resolver) for rule in ordered}
    union = union_all_plans(tuple(plans.values()), label="relspec_graph")
    return GraphPlan(plan=union, outputs=plans)
```

**Target files**
- Update: `src/relspec/compiler_graph.py`
- Update: `src/relspec/compiler.py`
- Update: `src/cpg/relation_registry.py`

**Implementation checklist**
- [x] Build a graph-level Plan container that keeps per-output subplans.
- [x] Ensure dependency ordering uses evidence gating.
- [x] Allow execution of either the full graph plan or per-output plans.

**Status**
Completed. Implemented graph plan compilation in `src/relspec/compiler_graph.py`
and wired usage in `src/cpg/relation_registry.py`.
