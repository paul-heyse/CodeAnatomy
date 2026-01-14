## Relspec Central Rules Infrastructure Plan

### Goals
- Centralize all relationship, normalize, and extract rule definitions into a single rule engine under `src/relspec`.
- Replace duplicated registries, evidence catalogs, spec tables, and policy handling across CPG/normalize/extract.
- Provide one canonical rule table format and a single compilation path for plan/table/external execution.
- Support external extractors plus plan-lane operations with staged execution and gating.
- Preserve ArrowDSL plan-lane execution, evidence gating, and graph-level planning while improving cohesion.
- Remove all legacy rule registries and spec tables once the centralized path is complete.

### Constraints
- Preserve output schemas, column names, and metadata semantics for CPG, normalize, and extract outputs.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Maintain execution-mode semantics (plan/table/auto/external/hybrid) across domains.
- Allow zero-input rules and optional/gated outputs (feature flags, enabled_when).
- Keep external extraction work isolated to IO boundaries.
- Breaking changes are acceptable; remove legacy APIs when a strictly better centralized replacement exists.

---

### Scope 1: Canonical RuleDefinition + Payload Model (CPG/Normalize/Extract)
**Description**
Create a shared rule definition model with a domain tag and payload union so all rules
flow through a single registry and compiler entrypoint, including extract datasets.
Treat legacy rule models as internal-only conversions and remove them from orchestration.

**Code patterns**
```python
# src/relspec/rules/definitions.py
ExecutionMode = Literal["auto", "plan", "table", "external", "hybrid"]


@dataclass(frozen=True)
class RuleDefinition:
    name: str
    domain: RuleDomain  # "cpg" | "normalize" | "extract"
    kind: str
    inputs: tuple[str, ...]
    output: str
    execution_mode: ExecutionMode = "auto"
    priority: int = 100
    evidence: EvidenceSpec | None = None
    evidence_output: EvidenceOutput | None = None
    policy_overrides: PolicyOverrides = field(default_factory=PolicyOverrides)
    emit_rule_meta: bool = True
    pipeline_ops: tuple[PipelineOp, ...] = ()
    post_kernels: tuple[KernelSpecT, ...] = ()
    stages: tuple[RuleStage, ...] = ()
    payload: RulePayload | None = None


@dataclass(frozen=True)
class ExtractPayload:
    version: int = 1
    template: str | None = None
    bundles: tuple[str, ...] = ()
    fields: tuple[str, ...] = ()
    derived_ids: tuple[ExtractDerivedIdSpec, ...] = ()
    row_fields: tuple[str, ...] = ()
    row_extras: tuple[str, ...] = ()
    ordering_keys: tuple[OrderingKey, ...] = ()
    join_keys: tuple[str, ...] = ()
    enabled_when: str | None = None
    feature_flag: str | None = None
    postprocess: str | None = None
    metadata_extra: Mapping[bytes, bytes] = field(default_factory=dict)
    evidence_required_columns: tuple[str, ...] = ()
    pipeline_name: str | None = None
```

**Target files**
- Add: `src/relspec/rules/definitions.py`
- Update: `src/relspec/model.py`
- Update: `src/normalize/rule_model.py`
- Update: `src/cpg/specs.py`
- Update: `src/arrowdsl/spec/tables/extract.py`

**Implementation checklist**
- [x] Add `RuleDefinition` and payload dataclasses with domain-aware validation.
- [x] Extend `ExecutionMode` to include `external` and `hybrid`.
- [x] Add `ExtractPayload` that mirrors extract dataset registry requirements (version, fields, bundles, evidence requirements).
- [x] Keep legacy models intact during migration with explicit adapters.
- [x] Remove legacy rule models from orchestration once central compilation is fully wired.

**Status**
Completed. Normalize rule definitions are generated directly from rule family specs (no legacy normalize rule models in orchestration), `src/normalize/rule_registry.py` is removed, and adapters now consume canonical RuleDefinition generation.

---

### Scope 2: Canonical Spec Table + Codec Layer (Including Extract Payload)
**Description**
Introduce a single Arrow spec table format that encodes relationship, normalize, and
extract rules, including extract dataset payloads and pipeline hooks. This becomes the
only supported spec table for rule registries (legacy tables must be converted or removed).

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

EXTRACT_STRUCT = pa.struct(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("template", pa.string(), nullable=True),
        pa.field("bundles", pa.list_(pa.string()), nullable=True),
        pa.field("fields", pa.list_(pa.string()), nullable=True),
        pa.field("derived", pa.list_(DERIVED_ID_STRUCT), nullable=True),
        pa.field("row_fields", pa.list_(pa.string()), nullable=True),
        pa.field("row_extras", pa.list_(pa.string()), nullable=True),
        pa.field("ordering_keys", pa.list_(ORDERING_KEY_STRUCT), nullable=True),
        pa.field("join_keys", pa.list_(pa.string()), nullable=True),
        pa.field("enabled_when", pa.string(), nullable=True),
        pa.field("feature_flag", pa.string(), nullable=True),
        pa.field("postprocess", pa.string(), nullable=True),
        pa.field("metadata_extra", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("evidence_required_columns", pa.list_(pa.string()), nullable=True),
        pa.field("pipeline_name", pa.string(), nullable=True),
    ]
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
- [x] Define the canonical rule spec schema + payload structs.
- [x] Add codec helpers to encode/decode extract payloads and pipeline ops.
- [x] Ensure extract payload structs fully match `EXTRACT_DATASET_SCHEMA` parity (incl `pipeline_name`).
- [x] Remove legacy conversion helpers once canonical rule tables are exclusive.
- [x] Switch registries to emit only the canonical rule table and remove legacy tables.

**Status**
Completed. Canonical schema/codec live in `src/relspec/rules/spec_tables.py`, extract payload parity is complete, all registries emit canonical rule tables, and legacy conversion helpers were removed to enforce canonical-only decoding.

---

### Scope 3: Central Registry + Template Expansion (Including Extract)
**Description**
Move rule registry assembly into `src/relspec`, including rule template expansion for
all domains. Extract dataset templates and CPG relationship templates flow through the
central registry, and legacy registry modules are removed.

**Code patterns**
```python
# src/relspec/rules/registry.py
registry = RuleRegistry(
    adapters=(CpgRuleAdapter(), NormalizeRuleAdapter(), ExtractRuleAdapter())
)
rule_table = registry.rule_table()

# src/extract/registry_definitions.py
def extract_rule_definitions() -> tuple[RuleDefinition, ...]:
    return tuple(_rule_from_spec(spec) for spec in dataset_row_specs())
```

**Target files**
- Add: `src/relspec/rules/registry.py`
- Add: `src/relspec/adapters/cpg.py`
- Add: `src/relspec/adapters/normalize.py`
- Add: `src/relspec/adapters/extract.py`
- Add: `src/extract/registry_definitions.py`
- Update: `src/relspec/adapters/relationship_rules.py`
- Update: `src/cpg/relation_registry.py`
- Update: `src/normalize/rule_registry.py`
- Update: `src/extract/registry_tables.py`
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/registry_specs.py`
- Update: `src/extract/registry_templates.py`
- Update: `src/extract/registry_template_specs.py`

**Implementation checklist**
- [x] Define a `RuleAdapter` protocol with `rule_definitions()` + `templates()` hooks.
- [x] Route extract template expansion through the central registry for all families.
- [x] Convert extract dataset rows into rule definitions with extract payloads.
- [x] Add parity checks to ensure template expansion reproduces registry rows.
- [x] Emit a single canonical rule table across all adapters and remove legacy registries.

**Status**
Completed. Rule adapters + registry live in `src/relspec/rules/registry.py` and `src/relspec/adapters/*`. Extract template expansion is centralized via `src/extract/registry_definitions.py`, parity checks are enforced, and canonical rule tables are emitted across adapters. Legacy registries have been trimmed (`src/normalize/rule_registry.py` removed); remaining compatibility surfaces are confined to CPG.

---

### Scope 4: Shared Evidence + Policy Infrastructure (Including Extract Evidence)
**Description**
Unify evidence catalogs, evidence plans, and policy resolution so all domains use
one shared implementation with metadata-driven defaults. Remove normalize/relspec policy
registries from orchestration and route all policy resolution through `relspec`.

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

# src/relspec/rules/handlers/normalize.py
handler = NormalizeRuleHandler(policies=PolicyRegistry())
```

**Target files**
- Add: `src/relspec/rules/evidence.py`
- Add: `src/relspec/rules/policies.py`
- Update: `src/relspec/policies.py`
- Update: `src/relspec/policy_registry.py`
- Update: `src/relspec/rules/handlers/cpg.py`
- Update: `src/relspec/rules/handlers/normalize.py`
- Update: `src/normalize/policies.py`
- Update: `src/normalize/policy_registry.py`
- Update: `src/normalize/evidence_catalog.py`
- Update: `src/extract/evidence_plan.py`
- Update: `src/extract/normalize_ops.py`
- Update: `src/extract/spec_helpers.py`

**Implementation checklist**
- [x] Move evidence plan compilation into `relspec` and add extract requirements.
- [x] Centralize evidence metadata decoding and schema-derived defaults.
- [x] Add domain-aware policy registry resolution for CPG/normalize.
- [x] Expand evidence plan compilation with normalize-op dependency expansion.
- [x] Update extract feature flag derivations to use shared evidence plan data.
- [x] Remove legacy policy registries from orchestration and re-export only as compatibility if needed.

**Status**
Completed. Shared evidence plan/catalog exists in `src/relspec/rules/evidence.py`, and `src/extract/evidence_plan.py` wraps it. Domain-aware policy resolution is wired for CPG/normalize handlers and legacy policy registries are no longer used in orchestration.

---

### Scope 5: Central Compiler + Domain Handlers (Including Extract Stages)
**Description**
Introduce a single `RuleCompiler` that dispatches to domain handlers for plan
compilation, including staged extract execution (external source, plan ops,
post-kernels). Extract handler outputs become the only input for extract execution.

**Code patterns**
```python
# src/relspec/rules/handlers/extract.py
@dataclass(frozen=True)
class ExtractRuleCompilation:
    definition: RuleDefinition
    payload: ExtractPayload
    pipeline_ops: tuple[Mapping[str, object], ...]
    post_kernels: tuple[Callable[[TableLike], TableLike], ...]
    stages: tuple[RuleStage, ...]


class ExtractRuleHandler(RuleHandler):
    def compile_rule(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> ExtractRuleCompilation:
        payload = _extract_payload(rule)
        return ExtractRuleCompilation(
            definition=rule,
            payload=payload,
            pipeline_ops=rule.pipeline_ops,
            post_kernels=post_kernels_for_postprocess(payload.postprocess),
            stages=_default_stages(payload),
        )
```

**Target files**
- Add: `src/relspec/rules/compiler.py`
- Add: `src/relspec/rules/compiler_graph.py`
- Add: `src/relspec/rules/handlers/extract.py`
- Update: `src/relspec/rules/handlers/__init__.py`
- Update: `src/relspec/rules/handlers/cpg.py`
- Update: `src/relspec/rules/handlers/normalize.py`
- Update: `src/relspec/compiler.py`
- Update: `src/relspec/compiler_graph.py`
- Update: `src/normalize/compiler_graph.py`
- Update: `src/extract/plan_helpers.py`
- Update: `src/extract/registry_pipelines.py`

**Implementation checklist**
- [x] Define `RuleHandler` protocols for CPG/normalize/extract.
- [x] Implement `ExtractRuleHandler` with staged execution metadata and kernel resolution.
- [x] Return a typed extract compilation artifact and export it for pipeline use.
- [x] Centralize evidence ordering and graph plan construction.
- [x] Resolve extract postprocess kernels via registry hooks.
- [x] Apply extract postprocess kernels after plan ops in pipeline execution.
- [x] Keep plan-only vs table-only vs external execution semantics intact.

**Status**
Completed. `RuleCompiler` + domain handlers exist in `src/relspec/rules/compiler.py` and `src/relspec/rules/handlers/*`, graph ordering now uses centralized evidence catalogs, extract bundles apply postprocess kernels from compilation metadata, and `relspec` graph-plan assembly now unions per-output contributions safely. Pipeline orchestration unification is tracked in Scope 6.

---

### Scope 6: Pipeline Integration + Removal of Redundant Modules
**Description**
Wire CPG, normalize, and extract pipelines to use the centralized registry/compiler and
retire duplicated registries, graphs, and policy resolvers. All orchestration should
flow through `relspec` registries/handlers and the canonical rule table, with legacy
modules removed (or left as thin compatibility re-exports).

**Code patterns**
```python
# src/hamilton_pipeline/modules/extraction.py
registry = RuleRegistry(adapters=(ExtractRuleAdapter(), NormalizeRuleAdapter(), CpgRuleAdapter()))
compiler = RuleCompiler(
    handlers={
        "extract": ExtractRuleHandler(),
        "normalize": NormalizeRuleHandler(),
        "cpg": RelationshipRuleHandler(compiler=RelationshipRuleCompiler(resolver=resolver)),
    }
)
extract_compiled = compiler.compile_rules(registry.rules_for_domain("extract"), ctx=ctx)

# src/hamilton_pipeline/modules/outputs.py
@tag(layer="materialize", artifact="extract_errors_parquet", kind="side_effect")
def write_extract_error_artifacts_parquet(
    output_dir: str | None,
    extract_error_artifacts: ExtractErrorArtifacts,
) -> JsonDict | None:
    if not output_dir:
        return None
    base = Path(output_dir) / "extract_errors"
    ...
```

**Target files**
- Update: `src/hamilton_pipeline/modules/cpg_build.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/extract/*_extract.py`
- Update: `src/extract/registry_bundles.py`
- Remove/Replace: `src/normalize/compiler_graph.py`
- Remove/Replace: `src/normalize/evidence_catalog.py`
- Remove/Replace: `src/normalize/policy_registry.py`

**Implementation checklist**
- [x] Use the centralized registry + compiler in all three pipelines.
- [x] Replace local evidence catalogs with shared `relspec` catalogs.
- [x] Migrate policy resolution to shared registry where applicable.
- [x] Convert extract bundle definitions into canonical registry/table form.
- [x] Wire extract error artifact materializers to centralized registry outputs.
- [x] Remove legacy normalize and extract registries from orchestration paths.
- [x] Remove/replace normalize compatibility wrappers (`normalize.compiler_graph`, `normalize.evidence_catalog`, `normalize.policy_registry`) if full consolidation is required.

**Status**
Completed. Hamilton modules use the central registry for normalization/extraction and centralized evidence/policy helpers. CPG compilation is evidence-ordered with shared policy defaults, extract bundles apply postprocess kernels from centralized compilation metadata, and CPG/extract execution consumes RuleCompiler-derived artifacts. Normalize compatibility wrappers have been removed in favor of the centralized rule graph helpers.

---

### Scope 7: Validation + Backward Compatibility (Including Extract Validation)
**Description**
Add validation helpers to ensure rule specs remain consistent, including extract
error artifacts (errors/stats/alignment). Backward compatibility helpers are optional
and can be removed once canonical tables are the only supported format.

**Code patterns**
```python
# src/relspec/rules/validation.py
def validate_rule_definitions(rules: Sequence[RuleDefinition]) -> None:
    seen = set()
    for rule in rules:
        if rule.name in seen:
            raise ValueError(f"Duplicate rule definition name: {rule.name!r}.")
        seen.add(rule.name)
        _validate_payload(rule)
        _validate_stages(rule)


def _validate_extract_payload(name: str, payload: ExtractPayload) -> None:
    _validate_join_keys(name, payload.join_keys)
    _validate_derived_ids(name, payload.derived_ids)
```

**Target files**
- Add: `src/relspec/rules/validation.py`
- Update: `src/relspec/registry.py`
- Update: `src/normalize/registry_validation.py`
- Add: `src/extract/registry_validation.py`
- Update: `src/extract/schema_ops.py`
- Update: `src/obs/manifest.py`

**Implementation checklist**
- [x] Add domain-aware validation for rule payloads and evidence.
- [x] Validate extract join keys and derived IDs in centralized validation.
- [x] Validate extract pipeline kernel names.
- [x] Validate extract outputs and capture errors/stats/alignment artifacts.
- [x] Surface extract error counts in the manifest/reporting layer.
- [x] Remove legacy conversion helpers once canonical rule tables are exclusive.
- [x] Enforce validation at registry build time.
- [x] Decide whether to keep or remove compatibility helpers once full migration completes.

**Status**
Completed. Central validation lives in `src/relspec/rules/validation.py` and extract registry validation in `src/extract/registry_validation.py`; manifest already carries extract error counts, and compatibility helpers have been removed.

---

### Scope 8: Execution Stages + Gating for Optional Outputs
**Description**
Make rule execution stages explicit and allow per-stage gating so optional extract
outputs can compile to empty plans with correct schemas when disabled, including
evidence-plan-driven gating. Stage metadata drives pipeline planning and ordering.

**Code patterns**
```python
# src/relspec/rules/options.py
@dataclass(frozen=True)
class RuleExecutionOptions:
    module_allowlist: tuple[str, ...] = ()
    feature_flags: Mapping[str, bool] = field(default_factory=dict)
    metadata_defaults: Mapping[str, object] = field(default_factory=dict)

    def as_mapping(self) -> Mapping[str, object]:
        return {
            "module_allowlist": self.module_allowlist,
            **self.metadata_defaults,
            **self.feature_flags,
        }


# src/relspec/rules/definitions.py
@dataclass(frozen=True)
class RuleStage:
    name: str
    mode: Literal["source", "plan", "post_kernel", "finalize"]
    enabled_when: str | None = None


def stage_enabled(stage: RuleStage, options: Mapping[str, object]) -> bool:
    ...

# src/extract/plan_helpers.py
options = rule_execution_options(template_name, evidence_plan, overrides=overrides)
if evidence_plan and not evidence_plan.requires_dataset(rule.output):
    return empty_plan_for_dataset(rule.output)
if not stage_enabled(rule_stage, options.as_mapping()):
    return empty_plan_for_dataset(rule.output)
```

**Target files**
- Update: `src/relspec/rules/definitions.py`
- Add: `src/relspec/rules/options.py`
- Update: `src/relspec/rules/compiler.py`
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/spec_helpers.py`
- Update: `src/extract/plan_helpers.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`
- Update: `src/hamilton_pipeline/modules/inputs.py`
- Update: `src/hamilton_pipeline/pipeline_types.py`
- Update: `src/arrowdsl/schema/metadata.py`

**Implementation checklist**
- [x] Model per-rule stages and optional gating in the rule definition model.
- [x] Add centralized `RuleExecutionOptions` and template-aware option derivation.
- [x] Use evidence plan requirements to disable unused templates and datasets.
- [x] Apply stage gating to extract/normalize plan helpers using shared options.
- [x] Compile disabled rules into empty plans with correct schemas.
- [x] Keep stage ordering deterministic for graph compilation.

**Status**
Completed. Stage metadata, execution options, evidence-based pruning, and empty-plan gating are wired into extract/normalize helpers.

---

### Scope 9: Central Rule Snapshots + Spec IO
**Description**
Unify rule snapshotting and reproducibility around the canonical rule table. Remove
`RelationshipRegistry`-specific serialization and emit one centralized snapshot format
for all domains.

**Code patterns**
```python
# src/obs/repro.py
registry = RuleRegistry(adapters=(CpgRuleAdapter(), NormalizeRuleAdapter(), ExtractRuleAdapter()))
rule_table = registry.rule_table()
write_spec_table(output_path, rule_table)
```

**Target files**
- Update: `src/obs/repro.py`
- Update: `src/obs/__init__.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/hamilton_pipeline/modules/cpg_build.py`

**Implementation checklist**
- [x] Serialize centralized rule tables in `obs` snapshots.
- [x] Remove `RelationshipRegistry`-specific snapshot payloads.
- [x] Emit canonical rule tables from pipeline outputs where snapshotting is required.

**Status**
Completed. Rule tables, template tables, and template diagnostics are captured in `obs` snapshots, and `RelationshipRegistry` serialization has been removed.

---

### Scope 10: Legacy Module Removal + API Consolidation
**Description**
Remove legacy registries/spec tables from orchestration paths and consolidate public APIs
around the centralized rule infrastructure. Compatibility wrappers should be thin and
optional, and removed once consumers migrate.

**Code patterns**
```python
# Canonical-only: generate definitions from rule family specs
def normalize_rule_definitions() -> tuple[RuleDefinition, ...]:
    return build_rule_definitions_from_specs(rule_family_specs())
```

**Target files**
- Remove: `src/normalize/rule_registry.py`
- Update/Remove: `src/normalize/spec_tables.py`
- Update/Remove: `src/extract/registry_tables.py`
- Update/Remove: `src/cpg/spec_tables.py`
- Update/Remove: `src/arrowdsl/spec/tables/relspec.py`

**Implementation checklist**
- [x] Replace orchestration entrypoints with centralized registry/compilers.
- [x] Remove legacy spec table emission in normalize/extract/cpg.
- [x] Remove normalize rule registry modules once canonical RuleDefinition generation is direct.
- [x] Remove remaining compatibility registries (`cpg.relation_registry`, legacy spec-table decoders).

**Status**
Completed. Legacy rule table emitters have been removed, `RelationshipRegistry` eliminated, `src/normalize/rule_registry.py` and `src/cpg/relation_registry.py` removed, and legacy spec-table decoders have been deleted in favor of canonical-only decoding.

---

### Scope 11: Template Catalog + Expansion Diagnostics
**Description**
Introduce a canonical template catalog for all domains (CPG/normalize/extract) so
template expansion is modeled, audited, and snapshot-ready. Provide a standard
diagnostics table for template expansion errors and rule-level diagnostics, and
thread those diagnostics through `obs` outputs for reproducibility.

**Code patterns**
```python
# src/relspec/rules/templates.py
@dataclass(frozen=True)
class RuleTemplateSpec:
    name: str
    domain: RuleDomain
    template: str
    outputs: tuple[str, ...]
    feature_flags: tuple[str, ...] = ()
    metadata: Mapping[str, str] = field(default_factory=dict)


def rule_template_table(specs: Sequence[RuleTemplateSpec]) -> pa.Table:
    return table_from_rows(TEMPLATE_SCHEMA, [spec.to_row() for spec in specs])


# src/relspec/rules/diagnostics.py
@dataclass(frozen=True)
class RuleDiagnostic:
    domain: RuleDomain
    template: str | None
    rule_name: str | None
    severity: Literal["error", "warning"]
    message: str
    metadata: Mapping[str, str] = field(default_factory=dict)
```

**Target files**
- Add: `src/relspec/rules/templates.py`
- Add: `src/relspec/rules/diagnostics.py`
- Update: `src/relspec/rules/spec_tables.py`
- Update: `src/relspec/rules/registry.py`
- Update: `src/extract/registry_template_specs.py`
- Update: `src/extract/registry_templates.py`
- Update: `src/cpg/relation_template_specs.py`
- Update: `src/obs/repro.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`

**Implementation checklist**
- [x] Define `RuleTemplateSpec` + template table schema for centralized snapshotting.
- [x] Convert CPG and extract template specs into the canonical template model.
- [x] Emit template diagnostics for expansion failures and invalid specs.
- [x] Persist diagnostics alongside rule snapshots in `obs` outputs.

**Status**
Completed. Template catalogs and diagnostics are centralized and emitted alongside rule snapshots.

---

### Progress Summary (Full Migration)
- Canonical RuleDefinition models, adapters, tables, and template diagnostics are centralized and wired.
- Extract definitions, evidence planning, stage gating, and snapshots emit canonical rule + template tables.
- Legacy rule table emission, conversion helpers, `RelationshipRegistry`, `src/normalize/rule_registry.py`, and `src/cpg/relation_registry.py` have been removed.
- CPG relationship compilation is evidence-ordered and consumes RuleCompiler artifacts; extract bundles apply centralized post-kernels.
- All scoped migration items are complete, including removal of normalize compatibility wrappers.

### Next Actions (Full Migration)
- None required; scope is fully implemented. Optional follow-up is a regression sweep for any downstream code importing deleted wrapper modules.
