# Semantic-Compile First — Best-in-Class Implementation Plan v1

> **Purpose**: Extend the semantic-compile architecture to a fully IR-driven, cost-aware, and contract-deterministic system across all pipelines.
>
> **Design principle**: The Semantic IR becomes the single source of truth for orchestration, optimization, and artifact generation.

---

## Scope Item 1 — IR as the single orchestration surface (all pipelines)

### Goal
Move diagnostics, quality signals, exports, and auxiliary pipelines into first-class IR nodes so all outputs are compiled, cached, and introspected consistently.

### Representative code patterns
```python
# semantics/ir.py
@dataclass(frozen=True)
class SemanticIRArtifact:
    name: str
    kind: Literal["diagnostic", "metric", "export"]
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
```

```python
# semantics/ir_pipeline.py
views.extend(
    SemanticIRView(
        name="relationship_candidates_v1",
        kind="diagnostic",
        inputs=("rel_name_symbol_v1",),
        outputs=("relationship_candidates_v1",),
    )
)
```

```python
# semantics/pipeline.py
if spec.kind == "diagnostic":
    return _diagnostic_builder(spec)
```

### Target files
- `src/semantics/ir.py`
- `src/semantics/ir_pipeline.py`
- `src/semantics/pipeline.py`
- `src/semantics/diagnostics.py`
- `src/semantics/catalog/dataset_rows.py`

### Deprecate / delete after completion
- Manual registration of diagnostics or exports outside IR.
- Any per-pipeline orchestration not reflected in IR.

### Implementation checklist
- [ ] Add IR artifact node types for diagnostics/exports.
- [ ] Emit those nodes from IR compile.
- [ ] Register artifact nodes via pipeline builder.
- [ ] Route artifact outputs through contract finalization.

---

## Scope Item 2 — Cost-based IR optimization and pruning

### Goal
Add a semantic-aware optimizer that reorders joins, prunes unused branches, and plans only requested outputs.

### Representative code patterns
```python
# semantics/ir_optimize.py
@dataclass(frozen=True)
class IRCost:
    row_count: int | None
    selectivity: float | None


def plan_outputs(ir: SemanticIR, outputs: set[str]) -> SemanticIR:
    return prune_ir(ir, outputs=outputs)
```

```python
# semantics/ir_pipeline.py
optimized = optimize_semantics(ir, outputs=requested_outputs)
```

### Target files
- `src/semantics/ir_pipeline.py`
- `src/semantics/ir_optimize.py` (new)
- `src/semantics/pipeline.py`

### Deprecate / delete after completion
- Fixed, non-pruned IR execution regardless of requested outputs.

### Implementation checklist
- [ ] Introduce IR cost model inputs (row counts, selectivity).
- [ ] Add join reordering for join groups.
- [ ] Add IR pruning by requested outputs.

---

## Scope Item 3 — Semantic types drive join selection and column pruning

### Goal
Use semantic types at compile time to select valid joins, reject invalid specs, and drop unused columns by default.

### Representative code patterns
```python
# semantics/schema.py
if left_sem.entity_id_col() is None:
    raise SemanticSchemaError("Missing entity_id for left view")
```

```python
# semantics/compiler.py
required_cols = semantic_required_columns(spec)
joined = joined.select(*[col(c) for c in required_cols])
```

### Target files
- `src/semantics/schema.py`
- `src/semantics/compiler.py`
- `src/semantics/quality.py`

### Deprecate / delete after completion
- Join rules that allow ambiguous or semantically invalid inputs.

### Implementation checklist
- [ ] Add semantic type validations during IR compile.
- [ ] Enforce column pruning based on semantic usage.
- [ ] Validate join keys against semantic types.

---

## Scope Item 4 — Join groups as materializable artifacts

### Goal
Make join-group outputs cacheable, versioned, and explainable with fingerprints and diagnostics.

### Representative code patterns
```python
# semantics/pipeline.py
if spec.kind == "join_group":
    node.cache_policy = "delta_staging"
```

```python
# datafusion_engine/views/artifacts.py
record_artifact(runtime_profile, "join_group_stats_v1", payload)
```

### Target files
- `src/semantics/ir_pipeline.py`
- `src/semantics/pipeline.py`
- `src/datafusion_engine/views/artifacts.py`

### Deprecate / delete after completion
- Hidden join-fusion logic without surfaced plans/metrics.

### Implementation checklist
- [ ] Emit join-group artifacts with fingerprints.
- [ ] Cache join-group outputs as first-class views.
- [ ] Add join-group diagnostics (row count, selectivity).

---

## Scope Item 5 — Projection & contract finalization as IR stages

### Goal
Make projections and contract finalization explicit IR stages to guarantee deterministic outputs.

### Representative code patterns
```python
# semantics/ir.py
@dataclass(frozen=True)
class SemanticIRProjection:
    name: str
    source: str
    contract: str
```

```python
# semantics/pipeline.py
if spec.kind == "projection":
    df = builder(ctx)
    return finalize_contract(ctx, df, contract)
```

### Target files
- `src/semantics/ir.py`
- `src/semantics/ir_pipeline.py`
- `src/datafusion_engine/schema/finalize.py`
- `src/semantics/catalog/projections.py`

### Deprecate / delete after completion
- Ad-hoc projections outside the IR.

### Implementation checklist
- [ ] Add projection nodes to IR.
- [ ] Wire projections through finalization.
- [ ] Ensure contract ordering + nullability deterministic.

---

## Scope Item 6 — Expand SemanticModel outputs beyond CPG

### Goal
Centralize all output definitions (analytics, diagnostics, exports) in SemanticModel so IR drives everything.

### Representative code patterns
```python
# semantics/registry.py
@dataclass(frozen=True)
class SemanticOutputSpec:
    name: str
    kind: Literal["table", "diagnostic", "export"]
    contract_ref: str

class SemanticModel:
    outputs: tuple[SemanticOutputSpec, ...]
```

### Target files
- `src/semantics/registry.py`
- `src/semantics/ir_pipeline.py`
- `src/semantics/catalog/dataset_rows.py`

### Deprecate / delete after completion
- Separate registries for outputs outside SemanticModel.

### Implementation checklist
- [ ] Add output specs to SemanticModel.
- [ ] Emit output specs from IR.
- [ ] Align dataset rows with output specs.

---

## Scope Item 7 — Schema evolution enforcement at IR compile time

### Goal
Detect incompatible schema changes during IR compile and generate migration stubs when required.

### Representative code patterns
```python
# semantics/migrations.py
if diff.is_breaking:
    raise SchemaMigrationRequired(diff)
```

```python
# semantics/ir_pipeline.py
validate_schema_migrations(ir, contracts=current_contracts)
```

### Target files
- `src/semantics/migrations.py`
- `src/semantics/ir_pipeline.py`
- `src/datafusion_engine/schema/contracts.py`

### Deprecate / delete after completion
- Implicit schema changes without explicit migrations.

### Implementation checklist
- [ ] Add compile-time schema diffing.
- [ ] Generate migration skeletons from diffs.
- [ ] Enforce migration policies per output.

---

## Scope Item 8 — Semantic-compile caching keys and provenance metadata

### Goal
Cache keys and artifact metadata should encode SemanticModel + IR fingerprints for determinism and traceability.

### Representative code patterns
```python
# semantics/ir_pipeline.py
ir_hash = hash_ir(ir)
model_hash = hash_semantic_model(model)
```

```python
# datafusion_engine/views/artifacts.py
record_artifact(runtime_profile, "semantic_ir_fingerprint_v1", payload)
```

### Target files
- `src/semantics/ir_pipeline.py`
- `src/datafusion_engine/views/artifacts.py`
- `src/datafusion_engine/views/graph.py`

### Deprecate / delete after completion
- Cache keys that omit semantic model/IR fingerprints.

### Implementation checklist
- [ ] Add IR + SemanticModel fingerprints.
- [ ] Attach fingerprints to output artifacts.
- [ ] Use fingerprints for cache eligibility.

---

## Scope Item 9 — Output slicing & incremental compilation

### Goal
Allow building only requested outputs and incremental updates based on input diffs rather than full recompute.

### Representative code patterns
```python
# semantics/pipeline.py
build_cpg(requested_outputs={"cpg_nodes_v1"})
```

```python
# semantics/ir_pipeline.py
optimized = optimize_semantics(ir, outputs=requested_outputs)
```

### Target files
- `src/semantics/pipeline.py`
- `src/semantics/ir_pipeline.py`
- `src/semantics/incremental/*`

### Deprecate / delete after completion
- Full IR execution when only a subset of outputs is needed.

### Implementation checklist
- [ ] Add requested output filtering to IR.
- [ ] Wire output slicing into pipeline entrypoints.
- [ ] Connect incremental diff engine to IR pruning.

---

## Scope Item 10 — Semantic testing harness and golden outputs

### Goal
Provide robust, deterministic tests for normalization, joins, and join groups.

### Representative code patterns
```python
# tests/semantics/test_ir_golden.py
ir = build_semantic_ir()
assert snapshot(ir) == load_golden("semantic_ir.json")
```

### Target files
- `tests/semantics/*`
- `tests/fixtures/*`

### Deprecate / delete after completion
- Ad-hoc tests that do not validate semantic IR or join groups.

### Implementation checklist
- [ ] Add golden IR snapshots.
- [ ] Add join-group equivalence tests.
- [ ] Add contract-finalization tests.

---

## Scope Item 11 — Semantic explain plan UI & artifacts

### Goal
Expose IR graphs, join-group plans, and compile decisions for usability and debugging.

### Representative code patterns
```python
# semantics/diagnostics.py
record_artifact(runtime_profile, "semantic_explain_plan_v1", explain_payload)
```

### Target files
- `src/semantics/diagnostics.py`
- `src/datafusion_engine/views/artifacts.py`
- `src/semantics/ir_pipeline.py`

### Deprecate / delete after completion
- Unstructured logs without explainable semantic plan artifacts.

### Implementation checklist
- [ ] Export IR graph + join group membership.
- [ ] Export per-view plan stats.
- [ ] Add UIs or markdown reports to surface explain plans.

---

# Cross‑Cutting Acceptance Gates

- [ ] IR is the single orchestration source for all semantic outputs.
- [ ] Cost-based optimization + output pruning is in place.
- [ ] Semantic types are enforced at compile time.
- [ ] Join groups are materialized, cached, and explainable.
- [ ] Projections/finalization are explicit IR nodes.
- [ ] Schema evolution requires explicit migrations.
- [ ] Semantic fingerprints drive caching and provenance.
- [ ] Output slicing + incremental compilation are available.
- [ ] Golden tests verify IR + join-group correctness.
- [ ] Explain plans are exported as artifacts.

---

## Execution Order (Suggested)

1. IR orchestration for diagnostics/exports
2. Cost-based optimization + pruning
3. Semantic-type validation + pruning
4. Join group artifactization
5. Projection/finalization IR stages
6. Expand SemanticModel output specs
7. Schema evolution enforcement
8. Semantic fingerprints for caching
9. Output slicing + incremental compile
10. Testing harness + goldens
11. Explain plan artifacts/UI
