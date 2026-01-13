## Normalize Programmatic Intensification Plan

### Goals
- Make all normalize behavior contract/spec-driven across types, bytecode, spans, diagnostics, and evidence.
- Push as much execution into ArrowDSL plan lanes as possible with deterministic, auditable outputs.
- Derive policies, ordering, and schema alignment from registry metadata, not per-module wiring.
- Replace static pipelines with rule-driven compilation that is data- and metadata-gated.
- Preserve current output schemas, column names, and semantics unless explicitly extended.

### Constraints
- Preserve current normalized outputs and dataset contracts.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Keep kernel-lane conversions (text index lookups, span derivation) in kernel lane.

---

### Scope 1: Spec-Table Driven Rule Families + Registry
**Description**
Define normalize rule families as spec table rows and construct rules programmatically via a
registry of factory callables, mirroring the CPG relationship compiler pattern.

**Code patterns**
```python
# src/normalize/rule_registry_specs.py
@dataclass(frozen=True)
class NormalizeRuleFamilySpec:
    name: str
    factory: str
    inputs: tuple[str, ...] = ()
    output: str | None = None
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None
    option_flag: str | None = None
    execution_mode: str | None = None


# src/normalize/rule_factories.py
RULE_FAMILY_FACTORIES: dict[str, RuleFamilyFactory] = {
    "types": build_type_rules,
    "bytecode_cfg": build_bytecode_cfg_rules,
    "bytecode_dfg": build_bytecode_dfg_rules,
    "diagnostics": build_diagnostics_rules,
    "span_errors": build_span_error_rules,
}


def build_rules_from_specs(
    specs: Sequence[NormalizeRuleFamilySpec],
) -> tuple[NormalizeRule, ...]:
    rules: list[NormalizeRule] = []
    for spec in specs:
        factory = RULE_FAMILY_FACTORIES.get(spec.factory)
        if factory is None:
            msg = f"Unknown normalize rule factory: {spec.factory!r}."
            raise KeyError(msg)
        rules.extend(factory(spec))
    return tuple(rules)
```

**Target files**
- Add: `src/normalize/rule_registry_specs.py`
- Add: `src/arrowdsl/spec/tables/normalize.py`
- Update: `src/normalize/rule_factories.py`
- Update: `src/normalize/rule_registry.py`
- Update: `src/normalize/spec_tables.py`

**Implementation checklist**
- [x] Define normalize rule family spec dataclass + Arrow encoding table.
- [x] Add a factory registry and build rules from spec rows.
- [x] Wire spec tables into normalize rule registry caches.

**Status**
Completed.

---

### Scope 2: Contract-First Schema + Metadata Derivation
**Description**
Make all normalize schemas, input projections, and metadata derive from contracts and
template metadata, removing inline schema/query declarations.

**Code patterns**
```python
# src/normalize/registry_specs.py
def dataset_input_columns(name: str) -> tuple[str, ...]:
    return tuple(dataset_input_schema(name).names)


def dataset_schema_policy(name: str, *, ctx: ExecutionContext) -> SchemaPolicy:
    spec = dataset_spec(name)
    contract = spec.contract()
    options = SchemaPolicyOptions(
        schema=contract.with_versioned_schema(),
        encoding=spec.encoding_policy(),
        metadata=dataset_metadata_spec(name),
        validation=contract.validation,
    )
    return schema_policy_factory(spec.table_spec, ctx=ctx, options=options)
```

**Target files**
- Update: `src/normalize/contracts.py`
- Update: `src/normalize/registry_specs.py`
- Update: `src/normalize/registry_templates.py`
- Update: `src/normalize/registry_rows.py`
- Update: `src/normalize/schemas.py`

**Implementation checklist**
- [x] Centralize contract schema derivation for all normalize datasets.
- [x] Encode default metadata in templates (coordinate system, evidence family, tier).
- [x] Ensure schema policies apply metadata and encoding consistently.

**Status**
Completed.

---

### Scope 3: Policy + Determinism Defaults From Metadata
**Description**
Derive confidence, ambiguity policies, and deterministic tie-breakers from contract metadata
when rule configs do not specify them explicitly.

**Code patterns**
```python
# src/normalize/policies.py
def confidence_policy_from_schema(schema: SchemaLike) -> ConfidencePolicy | None:
    meta = schema.metadata or {}
    name = _meta_str(meta, CONFIDENCE_POLICY_META)
    if name:
        return resolve_confidence_policy(name)
    base = _meta_float(meta, CONFIDENCE_BASE_META)
    penalty = _meta_float(meta, CONFIDENCE_PENALTY_META)
    if base is None and penalty is None:
        return None
    return ConfidencePolicy(base=base or 0.5, penalty=penalty or 0.0)


def default_tie_breakers(schema: pa.Schema) -> tuple[SortKey, ...]:
    keys = infer_ordering_keys(schema.names)
    return tuple(SortKey(name, order) for name, order in keys)
```

**Target files**
- Update: `src/normalize/policies.py`
- Update: `src/normalize/policy_registry.py`
- Update: `src/normalize/rule_model.py`
- Update: `src/normalize/runner.py`
- Update: `src/normalize/registry_templates.py`

**Implementation checklist**
- [x] Define metadata keys for confidence/ambiguity defaults.
- [x] Apply defaults during rule compilation when policies are missing.
- [x] Derive deterministic tie-breakers from schema metadata/naming conventions.

**Status**
Completed.

---

### Scope 4: Typed Evidence Gating + Virtual Output Schemas
**Description**
Extend evidence gating to validate required types and metadata, and register virtual outputs
so downstream rules can resolve schemas without re-scanning inputs.

**Code patterns**
```python
# src/normalize/rule_model.py
@dataclass(frozen=True)
class EvidenceSpec:
    sources: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    required_types: Mapping[str, str] = field(default_factory=dict)
    required_metadata: Mapping[bytes, bytes] = field(default_factory=dict)


# src/normalize/evidence_catalog.py
def satisfies(self, spec: EvidenceSpec | None, *, inputs: Sequence[str]) -> bool:
    ...
    for col, dtype in spec.required_types.items():
        if self.types_by_dataset.get(source, {}).get(col) != dtype:
            return False
```

**Target files**
- Update: `src/normalize/rule_model.py`
- Update: `src/normalize/evidence_catalog.py`
- Update: `src/normalize/compiler_graph.py`
- Update: `src/normalize/runner.py`

**Implementation checklist**
- [x] Track column types and metadata in evidence catalog.
- [x] Enforce type/metadata requirements in evidence gating.
- [x] Register virtual output schemas for compiled rules.

**Status**
Completed.

---

### Scope 5: Rule Families For All Normalize Domains
**Description**
Make all normalize domains (types, bytecode, spans, diagnostics, CST/SCIP) into rule families
driven by spec rows and plan builders.

**Code patterns**
```python
# src/normalize/rule_factories.py
def build_type_rules(spec: NormalizeRuleFamilySpec) -> tuple[NormalizeRule, ...]:
    execution_mode = _execution_mode_from_spec(spec)
    return (
        _rule(
            RuleSpec(
                name="type_exprs_norm",
                output="type_exprs_norm_v1",
                inputs=("cst_type_exprs",),
                derive=_derive_type_exprs,
                execution_mode=execution_mode,
            ),
            config=_rule_config(spec, evidence_sources=("cst_type_exprs",)),
        ),
    )
```

**Target files**
- Update: `src/normalize/types_plans.py`
- Update: `src/normalize/bytecode_cfg_plans.py`
- Update: `src/normalize/bytecode_dfg_plans.py`
- Update: `src/normalize/diagnostics_plans.py`
- Update: `src/normalize/spans.py`
- Update: `src/normalize/rule_factories.py`

**Implementation checklist**
- [x] Define rule family builders per domain.
- [x] Move one-off rules into family builders driven by specs.
- [x] Ensure all family outputs are contracted and evidence-aware.

**Status**
Completed.

---

### Scope 6: ArrowDSL Plan-Lane Ops + Kernel Integration
**Description**
Promote ambiguity resolution and confidence scoring to plan-lane ops, falling back to kernels
only when required, minimizing table materialization.

**Code patterns**
```python
# src/arrowdsl/plan/ops.py
@dataclass(frozen=True)
class WinnerSelectOp(PlanOp):
    spec: DedupeSpec


# src/normalize/runner.py
for spec in ambiguity_kernels(rule.ambiguity_policy):
    plan = plan.winner_select(spec, columns=columns, ctx=ctx)
```

**Target files**
- Update: `src/arrowdsl/plan/ops.py`
- Update: `src/arrowdsl/compute/kernels.py`
- Update: `src/normalize/runner.py`
- Update: `src/normalize/materializers.py`

**Implementation checklist**
- [x] Add plan-lane winner selection and confidence ops.
- [x] Use plan-lane ops where available; materialize only when required.
- [x] Keep kernel-lane conversions isolated and explicit.

**Status**
Completed.

---

### Scope 7: Catalog + Pipeline Unification
**Description**
Unify normalize plan catalog creation and pipeline outputs so all callers request rule outputs
by dataset name rather than module-specific wiring.

**Code patterns**
```python
# src/normalize/catalog.py
def normalize_plan_catalog(inputs: NormalizeCatalogInputs) -> PlanCatalog:
    tables = inputs.as_tables()
    return NormalizePlanCatalog(tables=tables, repo_text_index=inputs.repo_text_index)


# src/hamilton_pipeline/modules/normalization.py
def normalize_outputs(catalog: PlanCatalog, ctx: ExecutionContext) -> dict[str, Plan]:
    return compile_normalize_plans(catalog, ctx=ctx)
```

**Target files**
- Update: `src/normalize/catalog.py`
- Update: `src/normalize/registry_plans.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`
- Update: `src/hamilton_pipeline/modules/cpg_build.py`

**Implementation checklist**
- [x] Provide a catalog factory that attaches repo text index.
- [x] Ensure pipeline modules request outputs by dataset name.
- [x] Remove static pipeline wiring for normalize outputs.

**Status**
Completed.

---

### Scope 8: Manifest Lineage + Validation Automation
**Description**
Emit normalize rule lineage into manifests and enforce contract-derived validation, with
tests that assert policy and schema invariants.

**Code patterns**
```python
# src/obs/manifest.py
RuleRecord(
    name=rule.name,
    output_dataset=rule.output,
    evidence=asdict(rule.evidence) if rule.evidence else None,
)


# tests/normalize/test_rule_policies.py
def test_rule_policies_have_contract_defaults() -> None:
    for rule in normalize_rules():
        assert rule.confidence_policy is not None
```

**Target files**
- Update: `src/obs/manifest.py`
- Update: `src/normalize/runner.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Add: `tests/normalize/test_rule_policies.py`
- Add: `tests/normalize/test_rule_families.py`

**Implementation checklist**
- [x] Emit normalize rule metadata into manifests.
- [x] Enforce policy-required schema columns at compile time.
- [x] Add tests covering rule family construction and policy defaults.

**Status**
Completed.
