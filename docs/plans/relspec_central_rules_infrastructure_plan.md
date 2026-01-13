## Relspec Central Rules Infrastructure Plan

### Goals
- Centralize relationship, normalize, and extract rule definitions into a single rule engine under `src/relspec`.
- Replace duplicated registries, evidence catalogs, and policy handling across CPG/normalize/extract.
- Support external extractors plus plan-lane operations with staged execution and gating.
- Preserve ArrowDSL plan-lane execution, evidence gating, and graph-level planning.
- Remove redundant rule table formats by converging on one canonical spec table.

### Constraints
- Preserve output schemas, column names, and metadata semantics for CPG, normalize, and extract outputs.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Maintain execution-mode semantics (plan/table/auto/external/hybrid) across domains.
- Allow zero-input rules and optional/gated outputs (feature flags, enabled_when).
- Keep external extraction work isolated to IO boundaries.

---

### Scope 1: Canonical RuleDefinition + Payload Model (CPG/Normalize/Extract)
**Description**
Create a shared rule definition model with a domain tag and payload union so all rules
flow through a single registry and compiler entrypoint, including extract datasets.

**Code patterns**
```python
# src/relspec/rules/definitions.py
ExecutionMode = Literal["auto", "plan", "table", "external", "hybrid"]


@dataclass(frozen=True)
class RuleDefinition:
    name: str
    domain: str  # "cpg" | "normalize" | "extract"
    kind: str
    inputs: tuple[str, ...]
    output: str
    execution_mode: ExecutionMode = "auto"
    priority: int = 100
    evidence: EvidenceSpec | None = None
    evidence_output: EvidenceOutput | None = None
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None
    emit_rule_meta: bool = True
    payload: RulePayload | None = None


@dataclass(frozen=True)
class ExtractPayload:
    template: str | None = None
    bundles: tuple[str, ...] = ()
    row_fields: tuple[str, ...] = ()
    row_extras: tuple[str, ...] = ()
    ordering_keys: tuple[OrderingKey, ...] = ()
    join_keys: tuple[str, ...] = ()
    derived_ids: tuple[ExtractDerivedIdSpec, ...] = ()
    enabled_when: str | None = None
    feature_flag: str | None = None
    postprocess: str | None = None
    metadata_extra: dict[bytes, bytes] | None = None
    pipeline_name: str | None = None
```

**Target files**
- Add: `src/relspec/rules/definitions.py`
- Update: `src/relspec/model.py`
- Update: `src/normalize/rule_model.py`
- Update: `src/cpg/specs.py`
- Update: `src/arrowdsl/spec/tables/extract.py`

**Implementation checklist**
- [ ] Add `RuleDefinition` and payload dataclasses with domain-aware validation.
- [ ] Extend `ExecutionMode` to include `external` and `hybrid`.
- [ ] Add `ExtractPayload` that mirrors extract dataset registry requirements.
- [ ] Keep legacy models intact during migration with explicit adapters.

**Status**
Pending.

---

### Scope 2: Canonical Spec Table + Codec Layer (Including Extract Payload)
**Description**
Introduce a single Arrow spec table format that encodes relationship, normalize, and
extract rules, including extract dataset payloads and pipeline hooks.

**Code patterns**
```python
# src/relspec/rules/spec_tables.py
RULE_DEF_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("domain", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("inputs", pa.list_(pa.string()), nullable=True),
        pa.field("output", pa.string(), nullable=False),
        pa.field("execution_mode", pa.string(), nullable=True),
        pa.field("priority", pa.int32(), nullable=True),
        pa.field("evidence", EVIDENCE_STRUCT, nullable=True),
        pa.field("evidence_output", EVIDENCE_OUTPUT_STRUCT, nullable=True),
        pa.field("policy_overrides", POLICY_OVERRIDE_STRUCT, nullable=True),
        pa.field("relationship_payload", RELATIONSHIP_STRUCT, nullable=True),
        pa.field("normalize_payload", NORMALIZE_STRUCT, nullable=True),
        pa.field("extract_payload", EXTRACT_STRUCT, nullable=True),
        pa.field("pipeline_ops", pa.list_(QUERY_OP_STRUCT), nullable=True),
        pa.field("post_kernels", pa.list_(KERNEL_SPEC_STRUCT), nullable=True),
    ],
    metadata={b"spec_kind": b"relspec_rule_definitions"},
)
```

**Target files**
- Add: `src/relspec/rules/spec_tables.py`
- Update: `src/arrowdsl/spec/io.py`
- Update: `src/arrowdsl/spec/tables/relspec.py`
- Update: `src/arrowdsl/spec/tables/normalize.py`
- Update: `src/arrowdsl/spec/tables/extract.py`
- Update: `src/extract/registry_tables.py`

**Implementation checklist**
- [ ] Define the canonical rule spec schema + payload structs.
- [ ] Add codec helpers to encode/decode extract payloads and pipeline ops.
- [ ] Add conversion helpers for legacy extract/normalize/relspec tables.
- [ ] Switch registries to emit only the canonical rule table.

**Status**
Pending.

---

### Scope 3: Central Registry + Template Expansion (Including Extract)
**Description**
Move rule registry assembly into `src/relspec`, including rule template expansion for
all domains. Extract dataset templates become adapter inputs.

**Code patterns**
```python
# src/relspec/adapters/extract.py
class ExtractRuleAdapter(RuleAdapter):
    def rule_specs(self) -> Sequence[RuleDefinition]:
        rows = extract_dataset_rows()
        return tuple(_rule_from_dataset_row(row) for row in rows)

    def templates(self) -> Sequence[DatasetTemplateSpec]:
        return extract_dataset_templates()
```

**Target files**
- Add: `src/relspec/rules/registry.py`
- Add: `src/relspec/adapters/cpg.py`
- Add: `src/relspec/adapters/normalize.py`
- Add: `src/relspec/adapters/extract.py`
- Update: `src/cpg/relation_registry.py`
- Update: `src/normalize/rule_registry.py`
- Update: `src/extract/registry_tables.py`
- Update: `src/extract/registry_templates.py`
- Update: `src/extract/registry_template_specs.py`

**Implementation checklist**
- [ ] Define a `RuleAdapter` protocol with `rule_specs()` + `templates()` hooks.
- [ ] Route extract template expansion through the central registry.
- [ ] Convert extract dataset rows into rule definitions with extract payloads.
- [ ] Emit a single canonical rule table across all adapters.

**Status**
Pending.

---

### Scope 4: Shared Evidence + Policy Infrastructure (Including Extract Evidence)
**Description**
Unify evidence catalogs, evidence plans, and policy resolution so all domains use
one shared implementation with metadata-driven defaults.

**Code patterns**
```python
# src/relspec/rules/evidence.py
@dataclass(frozen=True)
class EvidenceRequirement:
    name: str
    alias: str
    template: str | None
    required_columns: tuple[str, ...]


def compile_evidence_plan(rules: Sequence[RuleDefinition]) -> EvidencePlan:
    ...
```

**Target files**
- Add: `src/relspec/rules/evidence.py`
- Add: `src/relspec/rules/policies.py`
- Update: `src/relspec/policies.py`
- Update: `src/relspec/policy_registry.py`
- Update: `src/normalize/policies.py`
- Update: `src/normalize/policy_registry.py`
- Update: `src/normalize/evidence_catalog.py`
- Update: `src/extract/evidence_plan.py`
- Update: `src/extract/spec_helpers.py`

**Implementation checklist**
- [ ] Move evidence plan compilation into `relspec` and add extract requirements.
- [ ] Centralize evidence metadata decoding and schema-derived defaults.
- [ ] Add domain-aware policy registry resolution for CPG/normalize.
- [ ] Update extract feature flag derivations to use shared evidence plan data.

**Status**
Pending.

---

### Scope 5: Central Compiler + Domain Handlers (Including Extract Stages)
**Description**
Introduce a single `RuleCompiler` that dispatches to domain handlers for plan
compilation, including staged extract execution (external source, plan ops,
post-kernels).

**Code patterns**
```python
# src/relspec/rules/compiler.py
class RuleCompiler:
    def __init__(self, handlers: Mapping[str, RuleHandler]) -> None:
        self._handlers = handlers

    def compile(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> RuleCompilation:
        handler = self._handlers[rule.domain]
        return handler.compile(rule, ctx=ctx)


# src/relspec/rules/handlers/extract.py
class ExtractRuleHandler(RuleHandler):
    def compile(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> RuleCompilation:
        plan = self._maybe_external_extract(rule, ctx=ctx)
        plan = self._apply_pipeline_ops(rule, plan, ctx=ctx)
        return RuleCompilation(plan=plan, post_kernels=self._post_kernels(rule))
```

**Target files**
- Add: `src/relspec/rules/compiler.py`
- Add: `src/relspec/rules/compiler_graph.py`
- Add: `src/relspec/rules/handlers/extract.py`
- Update: `src/relspec/compiler.py`
- Update: `src/relspec/compiler_graph.py`
- Update: `src/normalize/compiler_graph.py`
- Update: `src/extract/plan_helpers.py`
- Update: `src/extract/registry_pipelines.py`

**Implementation checklist**
- [ ] Define `RuleHandler` protocols for CPG/normalize/extract.
- [ ] Implement `ExtractRuleHandler` with staged execution and gating support.
- [ ] Centralize evidence ordering and graph plan construction.
- [ ] Keep plan-only vs table-only vs external execution semantics intact.

**Status**
Pending.

---

### Scope 6: Pipeline Integration + Removal of Redundant Modules
**Description**
Wire CPG, normalize, and extract pipelines to use the centralized registry/compiler and
retire duplicated registries, graphs, and policy resolvers.

**Code patterns**
```python
# src/hamilton_pipeline/modules/extraction.py
registry = RuleRegistry(adapters=(ExtractRuleAdapter(), NormalizeRuleAdapter(), CpgRuleAdapter()))
extract_rules = registry.rules_for_domain("extract")
compiled = compiler.compile_all(extract_rules, ctx=ctx)
```

**Target files**
- Update: `src/hamilton_pipeline/modules/cpg_build.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`
- Update: `src/extract/*_extract.py`
- Update: `src/extract/registry_bundles.py`
- Remove/Replace: `src/normalize/compiler_graph.py`
- Remove/Replace: `src/normalize/evidence_catalog.py`
- Remove/Replace: `src/normalize/policy_registry.py`

**Implementation checklist**
- [ ] Use the centralized registry + compiler in all three pipelines.
- [ ] Replace local evidence catalogs with shared `relspec` catalogs.
- [ ] Migrate policy resolution to shared registry where applicable.
- [ ] Convert extract bundle definitions into canonical registry/table form.

**Status**
Pending.

---

### Scope 7: Validation + Backward Compatibility (Including Extract Validation)
**Description**
Add validation helpers to ensure rule specs remain consistent, and provide
compatibility helpers to convert existing legacy tables to the new format
while migration is in progress.

**Code patterns**
```python
# src/relspec/rules/validation.py
def validate_rule_definitions(rules: Sequence[RuleDefinition]) -> None:
    for rule in rules:
        _validate_inputs(rule)
        _validate_payload(rule)


def _validate_extract_payload(payload: ExtractPayload) -> None:
    _ensure_join_keys(payload.join_keys)
    _ensure_derived_ids(payload.derived_ids)
```

**Target files**
- Add: `src/relspec/rules/validation.py`
- Update: `src/relspec/registry.py`
- Update: `src/normalize/registry_validation.py`
- Add: `src/extract/registry_validation.py`

**Implementation checklist**
- [ ] Add domain-aware validation for rule payloads and evidence.
- [ ] Validate extract join keys, derived IDs, and pipeline kernel names.
- [ ] Provide table conversion helpers for legacy extract/normalize/relspec tables.
- [ ] Enforce validation at registry build time.

**Status**
Pending.

---

### Scope 8: Execution Stages + Gating for Optional Outputs
**Description**
Make rule execution stages explicit and allow per-stage gating so optional extract
outputs can compile to empty plans with correct schemas when disabled.

**Code patterns**
```python
# src/relspec/rules/definitions.py
@dataclass(frozen=True)
class RuleStage:
    name: str
    mode: Literal["source", "plan", "post_kernel", "finalize"]
    enabled_when: str | None = None


def stage_enabled(stage: RuleStage, options: Mapping[str, object]) -> bool:
    ...
```

**Target files**
- Update: `src/relspec/rules/definitions.py`
- Update: `src/relspec/rules/compiler.py`
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/spec_helpers.py`
- Update: `src/arrowdsl/schema/metadata.py`

**Implementation checklist**
- [ ] Model per-rule stages and optional gating in the rule definition model.
- [ ] Add enablement checks driven by feature flags and metadata defaults.
- [ ] Compile disabled rules into empty plans with correct schemas.
- [ ] Keep stage ordering deterministic for graph compilation.

**Status**
Pending.
