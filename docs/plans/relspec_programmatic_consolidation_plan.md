# Relspec Programmatic Consolidation Plan

## Goals
- Make RuleDefinition the canonical rule spec across CPG, normalize, and extract.
- Consolidate relspec registries, policies, and graph utilities into cohesive packages.
- Drive programmatic rule generation and registry snapshots through a single API.
- Reduce drift by removing duplicated models and redundant adapters.

## Non-goals
- No changes to ArrowDSL internals.
- No feature additions beyond relspec consolidation and programmatic wiring.

## Execution order
- Canonical rule spec consolidation first, then registry/policy/graph packages, then
  snapshot API, then adapter refactor, then config injection and cleanup.

## Scope item 1: Canonical RuleDefinition everywhere
### Rationale
Multiple rule model definitions (relspec core + normalize domain) increase drift and
force duplicate adapters. Centralizing on RuleDefinition keeps rules programmatic
and consistent.

### Representative pattern
```python
# src/relspec/rules/definitions.py
from dataclasses import dataclass
from typing import Literal

RuleDomain = Literal["cpg", "normalize", "extract"]


@dataclass(frozen=True)
class RuleDefinition:
    name: str
    domain: RuleDomain
    kind: str
    inputs: tuple[str, ...]
    output: str
    payload: object | None = None
```

### Target files
- `src/relspec/rules/definitions.py`
- `src/relspec/normalize/rule_model.py`
- `src/relspec/normalize/rule_specs.py`
- `src/relspec/normalize/rule_template_specs.py`
- `src/normalize/rule_factories.py`
- `src/normalize/runner.py`
- `src/relspec/rules/handlers/normalize.py`
- `src/normalize/rule_defaults.py`

### Implementation checklist
- [x] Replace NormalizeRule usages with RuleDefinition + NormalizePayload.
- [x] Convert NormalizeRuleFamilySpec expansion to emit RuleDefinition rows.
- [x] Remove or shim NormalizeRule class after adapters are updated.
- [x] Update normalize rule factories to return RuleDefinition instances.
- [x] Add unit tests for normalize RuleDefinition round-trips.
- [x] Run `uv run ruff check --fix`, `uv run pyrefly check`, `uv run pyright --warnings --pythonversion=3.13`.

## Scope item 2: Consolidate registries into relspec/registry package
### Rationale
Dataset/contract and rule registries live in separate modules with overlapping
responsibilities. A single package makes registry wiring explicit and reusable.

### Representative pattern
```python
# src/relspec/registry/rules.py
from dataclasses import dataclass

from relspec.registry.rules import RuleRegistry


@dataclass(frozen=True)
class RelspecRegistry:
    rules: RuleRegistry

    def rule_table(self) -> "pa.Table":
        return self.rules.rule_table()
```

### Target files
- `src/relspec/registry.py` (move to `src/relspec/registry/datasets.py`)
- `src/relspec/rules/registry.py` (move to `src/relspec/registry/rules.py`)
- `src/relspec/__init__.py`
- `src/relspec/rules/__init__.py`
- `src/storage/deltalake/relspec_registry.py`
- `src/hamilton_pipeline/modules/outputs.py`

### Implementation checklist
- [x] Create `src/relspec/registry/__init__.py` with curated exports.
- [x] Move dataset/contract registry to `registry/datasets.py`.
- [x] Move rule registry to `registry/rules.py`.
- [x] Update all imports to the new registry paths.
- [x] Keep re-exports in `relspec/__init__.py` for compatibility.
- [x] Update documentation references to the new registry package.

## Scope item 3: Unify policy registry + schema policy helpers
### Rationale
Policy resolution is split between `relspec/rules/policies.py` and
`relspec/policies.py`. Combining them avoids divergent behavior.

### Representative pattern
```python
# src/relspec/policies/registry.py
from dataclasses import dataclass


@dataclass(frozen=True)
class PolicyRegistry:
    confidence: dict[str, object]
    ambiguity: dict[str, object]

    def resolve_confidence(self, name: str | None) -> object | None:
        return None if name is None else self.confidence[name]
```

### Target files
- `src/relspec/policies.py` (move to `src/relspec/policies/schema.py`)
- `src/relspec/rules/policies.py` (move to `src/relspec/policies/registry.py`)
- `src/relspec/pipeline_policy.py`
- `src/relspec/rules/handlers/cpg.py`
- `src/relspec/rules/handlers/normalize.py`
- `src/normalize/policies.py`

### Implementation checklist
- [x] Create `src/relspec/policies/__init__.py` exporting registry + schema helpers.
- [x] Update policy resolution call sites to use the new policy registry.
- [x] Migrate schema metadata parsing into the new policy module.
- [x] Add tests for confidence/ambiguity resolution across domains.

## Scope item 4: Merge relspec graph utilities
### Rationale
We currently have `compiler_graph.py` and `rules/graph.py` with overlapping
responsibilities (ordering and signatures). A single graph module clarifies the
canonical workflow.

### Representative pattern
```python
# src/relspec/graph.py
def order_rules(rules: "Sequence[RuleDefinition]") -> tuple["RuleDefinition", ...]:
    return tuple(sorted(rules, key=lambda rule: rule.priority))


def rule_graph_signature(rules: "Sequence[RuleDefinition]", label: str) -> str:
    return _signature_from_rules(rules, label=label)
```

### Target files
- `src/relspec/compiler_graph.py`
- `src/relspec/rules/graph.py`
- `src/relspec/rules/cache.py`
- `src/cpg/relationship_plans.py`
- `src/normalize/runner.py`
- `src/hamilton_pipeline/modules/cpg_build.py`

### Implementation checklist
- [x] Create `src/relspec/graph.py` with unified ordering + signature utilities.
- [x] Remove or alias `compiler_graph.py` and `rules/graph.py`.
- [x] Update all imports to `relspec.graph`.
- [x] Add tests for ordering and signature stability.

## Scope item 5: Introduce a single Relspec registry snapshot API
### Rationale
Multiple call sites independently compute tables, diagnostics, signatures, and
plan hashes. A single snapshot object makes the repo more programmatic and reduces
duplicate pipelines.

### Representative pattern
```python
# src/relspec/registry/snapshot.py
from dataclasses import dataclass


@dataclass(frozen=True)
class RelspecSnapshot:
    rule_table: "pa.Table"
    template_table: "pa.Table"
    rule_diagnostics: "pa.Table"
    template_diagnostics: "pa.Table"
    plan_signatures: dict[str, str]
```

### Target files
- `src/relspec/registry/snapshot.py` (new)
- `src/relspec/rules/cache.py`
- `src/storage/deltalake/relspec_registry.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/obs/manifest.py`

### Implementation checklist
- [x] Add a `build_relspec_snapshot` helper that uses the unified registries.
- [x] Replace direct cache/table helpers with snapshot usage.
- [x] Update delta registry exports to consume snapshot outputs.
- [x] Add snapshot parity tests against the existing cache helpers.

## Scope item 6: Data-driven adapters and factory registry
### Rationale
Adapters are now hand-coded per domain; a factory registry makes rule generation
programmatic and easier to extend.

### Representative pattern
```python
# src/relspec/adapters/factory.py
from dataclasses import dataclass


@dataclass(frozen=True)
class RuleFactoryRegistry:
    factories: dict[str, "Callable[[], tuple[RuleDefinition, ...]]"]

    def build(self, name: str) -> tuple["RuleDefinition", ...]:
        return self.factories[name]()
```

### Target files
- `src/relspec/adapters/__init__.py`
- `src/relspec/adapters/cpg.py`
- `src/relspec/adapters/normalize.py`
- `src/relspec/adapters/extract.py`
- `src/relspec/adapters/relationship_rules.py`
- `src/relspec/rules/cache.py`

### Implementation checklist
- [x] Introduce `RuleFactoryRegistry` and register domain factories.
- [x] Refactor adapters to be thin wrappers over the factory registry.
- [x] Update RuleRegistry construction to use factory-backed adapters.
- [x] Add unit tests to validate adapter output stability.

## Scope item 7: Centralize relspec configuration injection
### Rationale
Configuration and policy wiring is distributed across pipeline modules. A single
RelspecConfig makes programmatic integration consistent.

### Representative pattern
```python
# src/relspec/config.py
from dataclasses import dataclass


@dataclass(frozen=True)
class RelspecConfig:
    param_table_policy: "ParamTablePolicy | None"
    list_filter_gate_policy: "ListFilterGatePolicy | None"
    kernel_lane_policy: "KernelLanePolicy | None"
```

### Target files
- `src/relspec/config.py` (new)
- `src/relspec/pipeline_policy.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/relspec/rules/registry.py`

### Implementation checklist
- [x] Define `RelspecConfig` and thread it through registry + compiler layers.
- [x] Update pipeline modules to pass the config explicitly.
- [x] Remove redundant policy parameters where RelspecConfig is available.

## Scope item 8: Cleanup removed fallback coverage references
### Rationale
Fallback coverage has been removed; remaining references should be cleaned up to
avoid broken imports or stale documentation.

### Representative pattern
```python
# src/relspec/rules/__init__.py
__all__ = [
    "RuleDefinition",
    "RuleTemplateSpec",
    "RuleRegistry",
]
```

### Target files
- `src/relspec/rules/__init__.py`
- `docs/plans/fallback_missing_expr_functions.json`

### Implementation checklist
- [x] Remove exports or imports referencing fallback coverage modules.
- [x] Delete or archive stale fallback coverage plan artifacts.
- [x] Remove stale `fallback_coverage` references after cleanup.
