# Relspec Programmatic Extension Implementation Plan

## Goals
- Make `src/relspec/` the single source of truth for rule definitions, templates,
  coverage, and validation.
- Replace static rule lists and adapter wiring with programmatic discovery and
  metadata-driven synthesis.
- Ensure deterministic, reproducible registries and snapshots across domains.
- Enable extensibility without editing central registries.
- Centralize incremental scoping metadata (file-id columns, output contracts)
  so incremental behavior is runtime-driven instead of module static.

## Non-goals
- No changes to ArrowDSL internals or execution semantics.
- No new runtime features beyond relspec consolidation and wiring.

## Execution order
- Bundle discovery and registration, then decorator migration, then
  contract-driven synthesis, then registry wiring, then compiler unification,
  then spec-table codecs, then validation and coverage, then incremental
  integration and query predicate strategy, then runtime and snapshots, then
  entry-point extensions.

## Scope item 1: Rule bundle registry and discovery
### Rationale
Static tuples in rule modules force edits in many places. A bundle registry makes
rule collection programmatic while retaining deterministic order.

### Representative pattern
```python
# src/relspec/rules/bundles.py
from dataclasses import dataclass
from collections.abc import Callable, Sequence

@dataclass(frozen=True)
class RuleBundle:
    name: str
    domain: RuleDomain
    rules: Callable[[], Sequence[RuleDefinition]]
    templates: Callable[[], Sequence[RuleTemplateSpec]] = lambda: ()
    diagnostics: Callable[[], Sequence[RuleDiagnostic]] = lambda: ()

_BUNDLES: dict[str, RuleBundle] = {}

def register_bundle(bundle: RuleBundle) -> None:
    if bundle.name in _BUNDLES:
        msg = f"Duplicate rule bundle: {bundle.name!r}."
        raise ValueError(msg)
    _BUNDLES[bundle.name] = bundle

def iter_bundles() -> tuple[RuleBundle, ...]:
    return tuple(sorted(_BUNDLES.values(), key=lambda bundle: bundle.name))
```

### Target files
- `src/relspec/rules/bundles.py` (new)
- `src/relspec/rules/discovery.py` (new)
- `src/relspec/registry/rules.py`
- `src/relspec/rules/cache.py`

### Implementation checklist
- [ ] Add `RuleBundle` registry with deterministic ordering.
- [ ] Add a discovery module that imports known relspec bundle modules.
- [ ] Update `RuleRegistry` to source bundles from discovery.
- [ ] Update cache helpers to call discovery once and reuse bundles.

## Scope item 2: Decorator-based rule and template declarations
### Rationale
Use decorators to register rule factories in place, eliminating static global
tuples and reducing registration drift.

### Representative pattern
```python
# src/relspec/rules/decorators.py
from collections.abc import Callable, Sequence

def rule_bundle(name: str, domain: RuleDomain) -> Callable[[RuleFactory], RuleFactory]:
    def _wrap(factory: RuleFactory) -> RuleFactory:
        register_bundle(RuleBundle(name=name, domain=domain, rules=factory))
        return factory
    return _wrap

# src/relspec/rules/relationship_specs.py
@rule_bundle(name="relspec.relationships", domain="cpg")
def relationship_rule_definitions() -> tuple[RuleDefinition, ...]:
    ...
```

### Target files
- `src/relspec/rules/decorators.py` (new)
- `src/relspec/rules/relationship_specs.py`
- `src/relspec/rules/cpg_relationship_specs.py`
- `src/relspec/normalize/rule_registry_specs.py`
- `src/relspec/extract/registry_template_specs.py`

### Implementation checklist
- [ ] Introduce decorator helpers for rule and template bundles.
- [ ] Migrate rule modules to decorators and remove static tuples.
- [ ] Ensure bundle registration remains deterministic (sorting by name).
- [ ] Add tests asserting bundle discovery order and content.

## Scope item 3: Contract-driven rule synthesis
### Rationale
Let schema metadata and contracts generate rule definitions, eliminating manual
rule wiring for contract-bound outputs.

### Representative pattern
```python
# src/relspec/rules/contract_rules.py
def rules_from_contracts(registry: SchemaRegistry) -> tuple[RuleDefinition, ...]:
    rules: list[RuleDefinition] = []
    for spec in registry.dataset_specs.values():
        meta = spec.schema.metadata or {}
        rule_meta = parse_rule_metadata(meta)
        if rule_meta is None:
            continue
        rules.append(rule_definition_from_contract(spec, rule_meta))
    return tuple(rules)
```

### Target files
- `src/relspec/rules/contract_rules.py` (new)
- `src/relspec/contracts.py`
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/relspec/rules/relationship_specs.py`

### Implementation checklist
- [ ] Define metadata keys for rule synthesis on dataset schemas.
- [ ] Implement contract-to-rule translation helpers.
- [ ] Replace manual rule wiring where a contract already encodes rule intent.
- [ ] Add tests covering metadata parsing and rule emission.

## Scope item 4: Dynamic registry wiring by domain
### Rationale
The rule registry should be built from discovered bundles, not static adapters.
This keeps new domains or bundles from requiring central edits.

### Representative pattern
```python
# src/relspec/registry/rules.py
@dataclass(frozen=True)
class RuleRegistry:
    bundles: Sequence[RuleBundle]

    def rule_definitions(self) -> tuple[RuleDefinition, ...]:
        return tuple(rule for bundle in self.bundles for rule in bundle.rules())

def default_rule_registry() -> RuleRegistry:
    discover_bundles()
    return RuleRegistry(bundles=iter_bundles())
```

### Target files
- `src/relspec/registry/rules.py`
- `src/relspec/adapters/factory.py`
- `src/relspec/rules/cache.py`
- `src/relspec/adapters/__init__.py`

### Implementation checklist
- [ ] Replace adapter lists with bundle-driven registry construction.
- [ ] Keep adapters as thin shims or remove where no longer needed.
- [ ] Add a domain map for bundle metadata to support filtering.
- [ ] Update any registry consumers to use `default_rule_registry()`.

## Scope item 5: Unified rule compilation pipeline
### Rationale
Consolidate compilation into `RuleHandler` implementations so relspec has a
single compiler entrypoint with clear domain ownership.

### Representative pattern
```python
# src/relspec/rules/handlers/cpg.py
@dataclass(frozen=True)
class CpgRuleHandler(RuleHandler):
    domain: RuleDomain = "cpg"

    def compile_rule(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> CompiledRule:
        ...

# src/relspec/runtime.py
compiler = RuleCompiler(handlers={handler.domain: handler for handler in handlers})
compiled = compiler.compile_rules(registry.rule_definitions(), ctx=ctx)
```

### Target files
- `src/relspec/rules/compiler.py`
- `src/relspec/rules/handlers/cpg.py`
- `src/relspec/rules/handlers/normalize.py`
- `src/relspec/rules/handlers/extract.py`
- `src/relspec/compiler.py`

### Implementation checklist
- [ ] Move domain-specific compilation into `RuleHandler` classes.
- [ ] Convert `relspec/compiler.py` to a handler implementation.
- [ ] Add a single compiler entrypoint for all rule domains.
- [ ] Add tests for handler selection and error handling.

## Scope item 6: Schema-derived spec tables and codecs
### Rationale
Spec table schemas are hand-maintained and drift-prone. A codec layer can derive
PyArrow schemas directly from dataclasses or structured metadata.

### Representative pattern
```python
# src/relspec/rules/spec_codec.py
class RuleDefinitionCodec(SpecCodec[RuleDefinition]):
    schema = dataclass_schema(RuleDefinition)

    def encode(self, rule: RuleDefinition) -> dict[str, object]:
        return dataclass_to_payload(rule)

    def decode(self, payload: Mapping[str, object]) -> RuleDefinition:
        return payload_to_dataclass(RuleDefinition, payload)
```

### Target files
- `src/relspec/rules/spec_codec.py` (new)
- `src/relspec/rules/spec_tables.py`
- `src/relspec/rules/definitions.py`
- `src/arrowdsl/spec/infra.py`

### Implementation checklist
- [ ] Add codec utilities to generate schemas and encode/decode payloads.
- [ ] Replace hand-rolled schema definitions with codec-derived schemas.
- [ ] Keep backward-compatible field names and encoding rules.
- [ ] Add tests for round-trip encoding across rule payload types.

## Scope item 7: Programmatic coverage and validation gates
### Rationale
Coverage and validation logic should be driven by registries and known runtime
capabilities rather than static lists.

### Representative pattern
```python
# src/relspec/validate.py
@dataclass(frozen=True)
class ValidationReport:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

def validate_registry(registry: RuleRegistry) -> ValidationReport:
    errors = _validate_rule_uniqueness(registry)
    errors += _validate_contracts(registry)
    return ValidationReport(errors=errors, warnings=())
```

### Target files
- `src/relspec/rules/coverage.py`
- `src/relspec/rules/validation.py`
- `src/relspec/validate.py` (new)
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Implementation checklist
- [ ] Replace static coverage lists with registry-driven inventories.
- [ ] Create a unified validation report for registry+policy+contracts.
- [ ] Invoke validation in pipeline entrypoints.
- [ ] Add tests for validation failures and coverage summaries.

## Scope item 8: Incremental integration and file-id scoping
### Rationale
Incremental runs should not rely on module-level static constants or large
literal predicates. File-id scoping must scale to large inputs and be derived
from relspec runtime/snapshot metadata.

### Representative pattern
```python
# src/relspec/incremental.py
@dataclass(frozen=True)
class RelspecIncrementalSpec:
    relation_output_contracts: Mapping[str, str]
    normalize_alias_overrides: Mapping[str, str]
    file_id_columns: Mapping[str, str]
    scoped_datasets: frozenset[str]
    file_id_param_name: str = "file_ids"

def build_incremental_spec(snapshot: RelspecSnapshot) -> RelspecIncrementalSpec:
    ...

# src/ibis_engine/query_compiler.py
def dataset_query_for_file_ids(
    file_ids: Sequence[str],
    *,
    schema: SchemaLike | None = None,
    columns: Sequence[str] | None = None,
    file_id_column: str = "file_id",
    param_tables: ParamTableRegistry | None = None,
) -> IbisQuerySpec:
    if param_tables is not None and len(file_ids) > FILE_ID_PARAM_THRESHOLD:
        param_tables.register_values("file_ids", file_ids)
        return IbisQuerySpec(
            projection=IbisProjectionSpec(base=tuple(columns)),
            macros=(FileIdJoinMacro(file_id_column),),
        )
    predicate = _in_set_expr(file_id_column, tuple(file_ids))
    return IbisQuerySpec(
        projection=IbisProjectionSpec(base=tuple(columns)),
        predicate=predicate,
        pushdown_predicate=predicate,
    )
```

### Target files
- `src/relspec/incremental.py` (new)
- `src/relspec/registry/snapshot.py`
- `src/incremental/relspec_update.py`
- `src/ibis_engine/query_compiler.py`
- `src/ibis_engine/param_tables.py`
- `src/ibis_engine/macros.py`

### Implementation checklist
- [ ] Add `RelspecIncrementalSpec` with explicit output+alias+scoping metadata.
- [ ] Move `SCOPED_SITE_DATASETS` to runtime/snapshot-driven metadata.
- [ ] Include `RELATION_OUTPUT_NAME` mapping explicitly or validate its usage.
- [ ] Add param-table-backed file-id predicates for large lists.
- [ ] Add tests for both small and large file-id cases.

## Scope item 9: Relspec runtime composition root
### Rationale
Centralize relspec configuration, registries, and compiler wiring in a single
runtime object to make integration consistent and explicit.

### Representative pattern
```python
# src/relspec/runtime.py
@dataclass(frozen=True)
class RelspecRuntime:
    config: RelspecConfig
    registry: RuleRegistry
    compiler: RuleCompiler
    policies: PolicyRegistry

def compose_relspec(config: RelspecConfig) -> RelspecRuntime:
    discover_bundles()
    registry = default_rule_registry()
    compiler = RuleCompiler(handlers=default_rule_handlers(config))
    return RelspecRuntime(
        config=config,
        registry=registry,
        compiler=compiler,
        policies=PolicyRegistry(),
    )
```

### Target files
- `src/relspec/runtime.py` (new)
- `src/relspec/config.py`
- `src/relspec/pipeline_policy.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Implementation checklist
- [ ] Add a runtime composition root for relspec.
- [ ] Thread `RelspecRuntime` through pipeline modules.
- [ ] Remove ad-hoc registry/policy wiring in pipeline modules.
- [ ] Add tests covering runtime composition defaults.

## Scope item 10: Snapshot v2 for unified artifacts
### Rationale
Snapshots should include all rule-derived artifacts (tables, signatures,
coverage, graph metadata) so downstream consumers are fully programmatic.

### Representative pattern
```python
# src/relspec/registry/snapshot.py
@dataclass(frozen=True)
class RelspecSnapshot:
    rule_table: pa.Table
    template_table: pa.Table
    rule_diagnostics: pa.Table
    template_diagnostics: pa.Table
    plan_signatures: dict[str, str]
    graph_signature: str
    coverage: RuleCoverageAssessment
    bundle_inventory: dict[str, str]
    incremental_spec: RelspecIncrementalSpec
```

### Target files
- `src/relspec/registry/snapshot.py`
- `src/relspec/rules/cache.py`
- `src/obs/manifest.py`
- `src/storage/deltalake/relspec_registry.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Implementation checklist
- [ ] Extend snapshot with graph signature and coverage payloads.
- [ ] Add bundle inventory and policy hash metadata.
- [ ] Add incremental spec (file-id scoping + output contracts + aliases).
- [ ] Update consumers to use the expanded snapshot.
- [ ] Add parity tests against existing cache helpers.

## Scope item 11: Extension packs via entry points
### Rationale
Enable external bundles without editing core modules, supporting a plug-in model
for relspec rule sources.

### Representative pattern
```python
# src/relspec/rules/discovery.py
from importlib import metadata

def load_entrypoint_bundles() -> None:
    for entry in metadata.entry_points(group="codeanatomy.relspec_bundles"):
        bundle = entry.load()()
        register_bundle(bundle)
```

### Target files
- `src/relspec/rules/discovery.py`
- `pyproject.toml` (entry-point definitions)
- `docs/plans/relspec_programmatic_extension_implementation_plan.md`

### Implementation checklist
- [ ] Define an entry-point group for relspec bundles.
- [ ] Load entry-point bundles during discovery.
- [ ] Add smoke tests for entry-point registration ordering.

## Acceptance gates
- `uv run ruff format && uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13.11`
- `uv run pytest -q`
