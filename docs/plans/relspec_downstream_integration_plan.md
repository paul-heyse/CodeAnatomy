## Relspec Downstream Integration Plan

### Goals
- Align extract/normalize/CPG downstream code with centralized RuleRegistry/RuleCompiler outputs.
- Remove duplicate or divergent logic that bypasses centralized rule definitions.
- Improve modularity, correctness, and performance by reducing drift and redundant registry builds.

### Constraints
- Preserve output schemas, column names, and metadata semantics.
- Keep strict typing and Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Breaking changes are acceptable when they improve cohesion and correctness.

---

### Scope 1: Remove Legacy CPG Rule Factories
**Description**
Eliminate local rule assembly logic that duplicates centralized rule compilation.
Ensure CPG relationship rules are always derived via RuleCompiler handlers.

**Code patterns**
```python
# src/cpg/relationship_plans.py
registry = rule_registry_cached()
definitions = registry.rules_for_domain("cpg")
compiler = RuleCompiler(handlers={"cpg": RelationshipRuleHandler()})
rules = compiler.compile_rules(definitions, ctx=ctx)
```

**Target files**
- Remove/replace: `src/cpg/relation_factories.py`
- Update: `src/cpg/relationship_plans.py`
- Update: `src/cpg/registry.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/relspec/rules/handlers/cpg.py`

**Implementation checklist**
- [x] Remove any remaining usage of local CPG rule factories.
- [x] Use RuleCompiler outputs for all relationship-rule construction.
- [x] Keep RelationshipRule conversion logic only in RuleCompiler handler.
- [x] Validate manifest and snapshot output compatibility.

**Status**
Completed. `src/cpg/relation_factories.py` is removed, relationship compilation flows through RuleCompiler handlers, and manifests use canonical rule decoding.

---

### Scope 2: Normalize Ops Registry Driven by RuleDefinitions
**Description**
Remove manual normalize-op lists and derive normalize dependency ops from canonical
rule definitions and rule metadata, keeping evidence-plan expansion accurate.

**Code patterns**
```python
# src/extract/normalize_ops.py
registry = rule_registry_cached()
definitions = registry.rules_for_domain("normalize")
ops = tuple(NormalizeOp.from_definition(defn, alias_fn=dataset_alias) for defn in definitions)
```

**Target files**
- Update: `src/extract/normalize_ops.py`
- Update: `src/normalize/rule_factories.py`
- Update: `src/relspec/rules/definitions.py`
- Update: `src/normalize/registry_specs.py`

**Implementation checklist**
- [x] Introduce rule metadata to support multi-output normalize ops if needed.
- [x] Replace `_EXTRA_OPS` and `_ORDERED_OP_NAMES` with rule-driven derivation.
- [x] Ensure evidence-plan expansion remains deterministic and equivalent.
- [x] Add validation that normalize ops cover required outputs.

**Status**
Completed. Normalize op specs are centralized in `src/normalize/op_specs.py`, and extract normalization ops now derive from RuleDefinitions.

---

### Scope 3: Derive Extract Bundle Outputs from RuleDefinitions
**Description**
Replace static extract bundle/output mappings with derivations from ExtractPayload
and canonical rule definitions, keeping bundle order and overrides consistent.

**Code patterns**
```python
# src/extract/registry_bundles.py
registry = rule_registry_cached()
definitions = registry.rules_for_domain("extract")
bundle_map = bundle_specs_from_definitions(definitions)
```

**Target files**
- Update: `src/extract/registry_bundles.py`
- Update: `src/extract/registry_rows.py`
- Update: `src/extract/registry_definitions.py`
- Update: `src/extract/registry_template_specs.py`

**Implementation checklist**
- [x] Build OutputBundleSpec entries from ExtractPayload data.
- [x] Preserve bundle ordering and dataset alias overrides.
- [x] Add validation that bundle outputs match dataset rows.
- [x] Remove hardcoded bundle maps once derived data is stable.

**Status**
Completed. Bundle mappings are derived from dataset rows with deterministic ordering and alias overrides retained.

---

### Scope 4: Central Registry + Compiler Cache
**Description**
Add a cached registry bundle to avoid repeated RuleRegistry instantiations and
ensure consistent rule snapshots across extract/normalize/CPG subsystems.

**Code patterns**
```python
# src/relspec/rules/cache.py
@cache
def rule_registry_cached() -> RuleRegistry:
    return RuleRegistry(adapters=(CpgRuleAdapter(), NormalizeRuleAdapter(), ExtractRuleAdapter()))
```

**Target files**
- Add: `src/relspec/rules/cache.py`
- Update: `src/cpg/relationship_plans.py`
- Update: `src/cpg/registry.py`
- Update: `src/extract/normalize_ops.py`
- Update: `src/extract/registry_tables.py`

**Implementation checklist**
- [x] Introduce cached registry + rule_table helpers.
- [x] Replace ad-hoc RuleRegistry instantiations with cached helpers.
- [x] Verify snapshot output stability and provenance records.

**Status**
Completed. Cached registry/table helpers live in `src/relspec/rules/cache.py`, and call sites use them.

---

### Scope 5: Normalize Policy Defaults in Handler
**Description**
Move normalize evidence/policy defaulting into the NormalizeRuleHandler so any
compiled NormalizeRule is fully decorated and consistent across call sites.

**Code patterns**
```python
# src/relspec/rules/handlers/normalize.py
def compile_rule(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> NormalizeRule:
    base = _normalize_rule_from_definition(rule)
    return _apply_normalize_defaults(base)
```

**Target files**
- Update: `src/relspec/rules/handlers/normalize.py`
- Update: `src/normalize/runner.py`
- Update: `src/normalize/policies.py`
- Update: `src/normalize/evidence_specs.py`

**Implementation checklist**
- [x] Apply policy defaults and evidence defaults during rule compilation.
- [x] Remove duplicate defaulting logic from normalize runner.
- [x] Ensure normalization graph ordering uses the same decorated rule data.

**Status**
Completed. Normalize defaults are applied in `NormalizeRuleHandler`, and runner no longer re-applies them.

---

### Progress Summary
- All scoped integration items have been implemented.

### Next Actions
- None required; scope is complete.
