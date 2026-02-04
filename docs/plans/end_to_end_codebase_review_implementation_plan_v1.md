# End-to-End Codebase Review — Implementation Plan v1

> **Purpose**: Translate the end-to-end architecture audit into a concrete, sequenced implementation plan. Each scope item contains representative code snippets, target files, deprecations, and a completion checklist.
>
> **Design principle**: The semantic registry becomes the single source of truth. All CPG outputs and contracts are generated from the semantic model, and every view builder finalizes to contract.

---

## Scope Item 1 — Resolve `cpg_nodes_v1` / `cpg_edges_v1` naming collision

### Goal
Disambiguate semantic union outputs from final CPG outputs by introducing distinct union view names and wiring CPG outputs to the dedicated CPG builders.

**Status**: Completed.

### Representative code patterns
```python
# semantics/naming.py
SEMANTIC_OUTPUT_NAMES.update(
    {
        "semantic_nodes_union": "semantic_nodes_union_v1",
        "semantic_edges_union": "semantic_edges_union_v1",
    }
)
```

```python
# semantics/spec_registry.py
specs.append(
    SemanticSpecIndex(
        name=canonical_output_name("semantic_edges_union"),
        kind="union_edges",
        inputs=tuple(spec.name for spec in RELATIONSHIP_SPECS),
        outputs=(canonical_output_name("semantic_edges_union"),),
    )
)
```

```python
# semantics/catalog/projections.py

def cpg_nodes_builder() -> DataFrameBuilder:
    def _builder(ctx: SessionContext) -> DataFrame:
        return ctx.table("semantic_nodes_union_v1")
    return _builder
```

```python
# datafusion_engine/views/registry_specs.py
# Ensure cpg_nodes_v1/cpg_edges_v1 come from cpg.view_builders_df
# (not semantic union builders).
```

### Target files
- `src/semantics/naming.py`
- `src/semantics/spec_registry.py`
- `src/semantics/pipeline.py`
- `src/semantics/catalog/projections.py`
- `src/datafusion_engine/views/registry_specs.py`
- `src/semantics/docs/graph_docs.py`

### Deprecate / delete after completion
- Any direct references assuming `cpg_nodes_v1` and `cpg_edges_v1` are semantic union outputs.
- Any legacy alias or doc text implying “union of normalized entity tables” under `cpg_nodes_v1` / `cpg_edges_v1`.

### Implementation checklist
- [x] Add canonical names for `semantic_nodes_union_v1` / `semantic_edges_union_v1`.
- [x] Update spec registry to emit union outputs under new names.
- [x] Update semantic pipeline to register union outputs under new names.
- [x] Wire CPG outputs to `cpg.view_builders_df` only.
- [x] Update projections to read from union views by new names.
- [x] Update documentation and graph docs.

---

## Scope Item 2 — Enforce contract finalization for every view

### Goal
Guarantee outputs match contract schemas by projecting, casting, and filling missing fields at a centralized finalize step.

**Status**: Completed.

### Representative code patterns
```python
# datafusion_engine/views/graph.py

def _finalize_to_contract(
    ctx: SessionContext,
    df: DataFrame,
    contract: SchemaContract,
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFrame:
    from datafusion_engine.schema.finalize import finalize_contract

    return finalize_contract(
        ctx,
        df,
        contract,
        runtime_profile=runtime_profile,
    )
```

```python
# datafusion_engine/views/graph.py
if context.options.validate_schema and node.contract_builder is not None:
    contract = node.contract_builder(schema)
    df = _finalize_to_contract(context.ctx, df, contract, runtime_profile=context.runtime.runtime_profile)
```

### Target files
- `src/datafusion_engine/views/graph.py`
- `src/datafusion_engine/schema/finalize.py`
- `src/datafusion_engine/schema/contracts.py`
- `src/semantics/compiler.py`

### Deprecate / delete after completion
- Ad hoc per-builder projection patterns or manual column pruning done for contract compliance.

### Implementation checklist
- [x] Create a contract-finalization wrapper in view registration.
- [x] Ensure finalization runs **before** schema validation.
- [x] Ensure finalization applies canonical ordering and type casting.
- [x] Update any builders relying on implicit extra columns.

---

## Scope Item 3 — Standardize span representation (struct span internal)

### Goal
Make `span` (struct) canonical in semantic views while allowing CPG outputs to expose byte offsets.

**Status**: Completed.

### Representative code patterns
```python
# semantics/schema.py
from datafusion_engine.udf.expr import udf_expr

span_expr = udf_expr("span_make", col("bstart"), col("bend"))
```

```python
# cpg/view_builders_df.py
# CPG outputs still include bstart/bend but can also carry span if desired.
result = result.with_column("span", udf_expr("span_make", col("bstart"), col("bend")))
```

### Target files
- `src/semantics/compiler.py`
- `src/semantics/schema.py`
- `src/semantics/quality_specs.py`
- `src/cpg/view_builders_df.py`
- `src/schema_spec/specs.py`

### Deprecate / delete after completion
- Direct use of `bstart`/`bend` in relationship logic when span struct is available.

### Implementation checklist
- [x] Define policy: semantic views always include `span`.
- [x] Add utilities to derive `bstart`/`bend` from `span` where needed.
- [x] Update relationship specs to use span-derived columns when possible.
- [x] Ensure CPG outputs keep `bstart`/`bend` for consumer compatibility.

---

## Scope Item 4 — Single semantic registry as source of truth

### Goal
Collapse multiple registries (semantic specs, dataset rows, CPG emission specs) into a single declarative registry that generates all downstream artifacts.

**Status**: Completed.

### Representative code patterns
```python
# semantics/registry.py
@dataclass(frozen=True)
class SemanticModel:
    inputs: tuple[ExtractionDatasetSpec, ...]
    views: tuple[SemanticViewSpec, ...]
    relationships: tuple[RelationshipSpec, ...]
    outputs: tuple[CpgOutputSpec, ...]
```

```python
# semantics/registry.py
model = build_semantic_model()
view_specs = model.to_view_specs()
contracts = model.to_dataset_rows()
node_specs = model.to_cpg_node_specs()
```

### Target files
- `src/semantics/spec_registry.py`
- `src/semantics/catalog/dataset_rows.py`
- `src/cpg/spec_registry.py`
- `src/semantics/catalog/spec_builder.py`

### Deprecate / delete after completion
- `SEMANTIC_SPEC_INDEX` as a hand‑maintained index.
- Direct manual `ENTITY_FAMILY_SPECS` entries that duplicate semantic normalization inputs.

### Implementation checklist
- [x] Introduce `SemanticModel` with normalization + relationship specs and canonical union outputs.
- [x] Extend `SemanticModel` to cover outputs and contract generation.
- [x] Generate full dataset rows/contracts from `SemanticModel` (beyond normalization/relationships).
- [x] Generate CPG prop specs from `SemanticModel` and remove duplicated registry definitions.

---

## Scope Item 5 — Auto‑generate CPG node emission specs from semantic normalization specs

### Goal
Eliminate manual CPG node specs for semantic entities by generating them from semantic normalization metadata.

**Status**: Completed.

### Representative code patterns
```python
# semantics/registry_to_cpg.py
for spec in SEMANTIC_NORMALIZATION_SPECS:
    if not spec.include_in_cpg_nodes:
        continue
    emit_specs.append(
        NodeEmitSpec(
            node_kind=...,  # from mapping
            id_cols=(spec.spec.entity_id.out_col,),
            path_cols=(spec.spec.path_col,),
            bstart_cols=(spec.spec.primary_span.start_col,),
            bend_cols=(spec.spec.primary_span.end_col,),
            file_id_cols=("file_id",),
        )
    )
```

### Target files
- `src/semantics/spec_registry.py`
- `src/cpg/spec_registry.py`
- `src/cpg/view_builders_df.py`

### Deprecate / delete after completion
- Hardcoded `EntityFamilySpec` entries that only mirror semantic normalization outputs.

### Implementation checklist
- [x] Add node_kind mapping for semantic normalization outputs.
- [x] Generate CPG `NodeEmitSpec` from normalization specs.
- [x] Remove duplicated manual specs.

---

## Scope Item 6 — Relationship diagnostics: candidates + decisions + schema anomalies

### Goal
Provide diagnostic views for relationship candidate sets, ranking decisions, and schema anomalies to enable agent‑grade introspection.

**Status**: Completed.

### Representative code patterns
```python
# semantics/diagnostics.py

def build_relationship_candidates(ctx: SessionContext, name: str) -> DataFrame:
    df = ctx.table(name)
    return df.select("src", "dst", "score", "confidence", "ambiguity_group_id", "task_name")
```

```python
# semantics/diagnostics.py

def build_schema_anomalies(ctx: SessionContext) -> DataFrame:
    return ctx.table("schema_contract_violations_v1")
```

### Target files
- `src/semantics/diagnostics.py`
- `src/semantics/pipeline.py`
- `src/semantics/catalog/dataset_rows.py`

### Deprecate / delete after completion
- Any ad hoc diagnostics tables not wired into semantic diagnostics emission.

### Implementation checklist
- [x] Add `relationship_candidates_v1` view.
- [x] Add `relationship_decisions_v1` view.
- [x] Add `schema_anomalies_v1` view.
- [x] Register diagnostic views in semantic pipeline diagnostics emission.

---

## Scope Item 7 — Operationalize hard/soft/quality tiers with explainability

### Goal
Expose explicit signal columns and weights so downstream systems can explain why a relationship was chosen.

**Status**: Completed.

### Representative code patterns
```python
# semantics/quality.py
@dataclass(frozen=True)
class SignalsSpec:
    base_score: float
    base_confidence: float
    hard: Sequence[HardPredicate]
    features: Sequence[Feature]
```

```python
# semantics/compiler.py
# Emit per-feature columns for explainability
for feature in spec.signals.features:
    df = df.with_column(feature.name, feature.expr(ctx))
```

### Target files
- `src/semantics/quality.py`
- `src/semantics/compiler.py`
- `src/semantics/quality_specs.py`

### Deprecate / delete after completion
- Opaque scoring logic that doesn’t preserve feature columns in diagnostics.

### Implementation checklist
- [x] Emit feature columns for every relationship spec.
- [x] Preserve hard predicate outcomes for diagnostics.
- [x] Include signal-level metadata in decision diagnostics.

---

## Scope Item 8 — Semantic IR: compile → optimize → emit

### Goal
Create an intermediate semantic representation that drives view graphs, contracts, and CPG outputs.

**Status**: Completed.

### Representative code patterns
```python
# semantics/ir.py
@dataclass(frozen=True)
class SemanticIR:
    nodes: tuple[IRNode, ...]
    edges: tuple[IREdge, ...]
    contracts: tuple[IRContract, ...]
```

```python
# semantics/compile.py
ir = compile_semantics(model)
optimized = optimize_semantics(ir)
artifacts = emit_semantics(optimized)
```

### Target files
- `src/semantics/ir.py`
- `src/semantics/compiler.py`
- `src/semantics/catalog/*`
- `src/cpg/*`

### Deprecate / delete after completion
- Direct cross‑module spec lookups that bypass a centralized semantic model.

### Implementation checklist
- [x] Define Semantic IR schema.
- [x] Compile IR from registry.
- [x] Optimize IR (dedupe, canonical sorting).
- [x] Add join-fusion optimization pass (grouping and shared join views executed).
- [x] Wire IR outputs to drive dataset rows and CPG specs in pipeline registration.

---

## Scope Item 9 — Versioned schema migrations

### Goal
Introduce explicit schema migration paths between versioned semantic outputs.

**Status**: Completed.

### Representative code patterns
```python
# semantics/migrations.py
MIGRATIONS = {
    ("cpg_nodes_v1", "cpg_nodes_v2"): migrate_cpg_nodes_v1_to_v2,
}
```

### Target files
- `src/semantics/migrations.py`
- `src/semantics/naming.py`
- `src/semantics/catalog/dataset_rows.py`

### Deprecate / delete after completion
- Implicit schema updates without migrations.

### Implementation checklist
- [x] Add migration registry.
- [x] Define first migration prototypes.
- [x] Enforce migrations on version bumps.

---

## Scope Item 10 — SemanticModel‑driven CPG registry (remove manual specs)

### Goal
Make the semantic model the sole source of truth for CPG node and prop specs by
replacing `ENTITY_FAMILY_SPECS` with model‑generated specs.

**Status**: Completed.

### Representative code patterns
```python
# semantics/registry.py
@dataclass(frozen=True)
class CpgEntitySpec:
    name: str
    node_kind: NodeKindId
    node_table: str
    id_cols: tuple[str, ...]
    path_cols: tuple[str, ...]
    span_cols: tuple[str, ...]
    file_id_cols: tuple[str, ...]
    prop_fields: tuple[PropFieldSpec, ...]
    prop_table: str | None = None
```

```python
# semantics/registry.py
def cpg_entity_specs(self) -> tuple[CpgEntitySpec, ...]:
    semantic_specs = tuple(_from_normalization(spec) for spec in self.normalization_specs)
    manual_specs = _explicit_cpg_specs()
    return (*semantic_specs, *manual_specs)
```

```python
# cpg/spec_registry.py
def build_node_plan_specs(model: SemanticModel) -> tuple[NodePlanSpec, ...]:
    return tuple(spec.to_node_plan() for spec in model.cpg_entity_specs())
```

### Target files
- `src/semantics/registry.py`
- `src/semantics/cpg_entity_specs.py`
- `src/cpg/spec_registry.py`
- `src/cpg/view_builders_df.py`

### Deprecate / delete after completion
- `ENTITY_FAMILY_SPECS` and all manual prop mappings in `src/cpg/spec_registry.py`.

### Implementation checklist
- [x] Introduce `CpgEntitySpec` and `SemanticModel.cpg_entity_specs()`.
- [x] Generate node/prop specs from semantic normalization specs.
- [x] Port remaining explicit CPG specs to model‑level `CpgEntitySpec`.
- [x] Delete `ENTITY_FAMILY_SPECS` and related manual prop mappings.
- [x] Ensure `node_plan_specs()` and `prop_table_specs()` are model‑driven.

---

## Scope Item 11 — IR join‑fusion execution (shared join pipeline)

### Goal
Move from join‑fusion metadata to an actual shared join execution path and
use it to compile multiple relationships from a single join.

**Status**: Completed.

### Representative code patterns
```python
# semantics/ir.py
@dataclass(frozen=True)
class SemanticIRJoinGroup:
    name: str
    left_view: str
    right_view: str
    left_on: tuple[str, ...]
    right_on: tuple[str, ...]
    how: str
    relationship_names: tuple[str, ...]
```

```python
# semantics/compiler.py
def build_join_group(self, group: SemanticIRJoinGroup) -> DataFrame:
    joined = self.ctx.table(group.left_view).join(
        self.ctx.table(group.right_view),
        left_on=group.left_on,
        right_on=group.right_on,
        how=group.how,
    )
    return joined
```

```python
# semantics/pipeline.py
for group in ir.join_groups:
    joined = compiler.build_join_group(group)
    for rel_name in group.relationship_names:
        df = compiler.compile_relationship_from_join(joined, rel_specs[rel_name])
        register_view(rel_name, df)
```

### Target files
- `src/semantics/ir.py`
- `src/semantics/ir_pipeline.py`
- `src/semantics/compiler.py`
- `src/semantics/pipeline.py`

### Deprecate / delete after completion
- Repeated per‑relationship join execution paths in the relationship compiler.

### Implementation checklist
- [x] Add `SemanticIRJoinGroup` to IR schema.
- [x] Build join groups during IR optimization.
- [x] Implement shared join builder in `SemanticCompiler`.
- [x] Compile relationships from shared joins in pipeline registration.
- [x] Validate diagnostics and projections still align with join‑fused outputs.

---

# Cross‑Cutting Acceptance Gates

- [x] Semantic union outputs are renamed and no longer collide with CPG outputs.
- [x] Every view builder is finalized to contract schema.
- [x] Semantic spans are canonical (`span` struct) and byte offsets are derived.
- [x] Single semantic registry generates view specs, contracts, and CPG emission specs.
- [x] Relationship diagnostics tables are registered and emitted.
- [x] Hard/soft/quality signals are explainable in diagnostics output.
- [x] IR compile → optimize → emit pipeline is in place.
- [x] Versioned schema migrations are defined and enforceable.

---

## Execution Order (Suggested)

1. Resolve cpg union naming collision.
2. Implement contract finalization wrapper in view registration.
3. Standardize span representation + helpers.
4. Introduce single semantic registry + generation of dataset rows/specs.
5. Auto‑generate CPG emission specs.
6. Add diagnostics tables.
7. Emit signal‑level explainability.
8. Add semantic IR pipeline.
9. Add schema migration framework.
