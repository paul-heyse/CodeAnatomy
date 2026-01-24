# Inference-Pure Pipe Stage Alignment Plan

Date: January 24, 2026

Goals
- Preserve a maximally inference-driven execution model based on rustworkx scheduling.
- Surface intermediate transforms as explicit Hamilton pipe stages without introducing new rule logic.
- Keep the Hamilton DAG a faithful projection of inferred task dependencies.

Guiding principles
- Only introduce pipe stages that are derived from inferred plan artifacts and existing calculation outputs.
- Do not add new scheduling logic; all ordering stays anchored to rustworkx generations and inferred deps.
- Keep stage boundaries deterministic and purely functional (no side effects inside stages).

----------------------------------------------------------------------
Scope 1: Normalize pipeline pipe stages (inference-derived transforms only)
----------------------------------------------------------------------
Why
- Normalize steps currently bundle multiple transforms into single nodes.
- Exposing transforms as stages improves lineage without changing inference.

Representative pattern
```python
from hamilton.function_modifiers import pipe_input, source, step, tag

@tag(layer="normalize", artifact="normalize_inputs", kind="stage")
def _normalize_inputs_stage(table: TableLike) -> TableLike:
    return table

@tag(layer="normalize", artifact="normalize_enrich", kind="stage")
def _normalize_enrich_stage(table: TableLike) -> TableLike:
    return table

@pipe_input(
    step(_normalize_inputs_stage),
    step(_normalize_enrich_stage),
    on_input="raw_inputs",
    namespace="normalize_inputs",
)
@tag(layer="normalize", artifact="normalized_inputs", kind="table")
def normalized_inputs(raw_inputs: TableLike) -> TableLike:
    return raw_inputs
```

Target files
- src/normalize/ibis_plan_builders.py
- src/normalize/dataset_builders.py
- src/normalize/output_writes.py
- src/normalize/dataset_rows.py

Implementation checklist
- [ ] Identify multi-step normalization transforms that are strictly derived from inferred task outputs.
- [ ] Split these transforms into pipe stages with `@pipe_input`/`@pipe_output`.
- [ ] Tag each stage with `layer="normalize"` and a stable `artifact` value.
- [ ] Keep stage outputs fully deterministic; no IO or side effects in stages.

Decommission candidates
- Inline helper functions that become explicit pipe stages (case-by-case).

----------------------------------------------------------------------
Scope 2: Extract pipeline pipe stages (parse/normalize/materialize)
----------------------------------------------------------------------
Why
- Extract steps mix parsing, normalization, and table assembly in single nodes.
- Staging preserves inference while improving observability and cache boundaries.

Representative pattern
```python
from hamilton.function_modifiers import pipe_output, step, tag

@tag(layer="extract", artifact="parse_stage", kind="stage")
def _parse_stage(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return rows

@tag(layer="extract", artifact="normalize_stage", kind="stage")
def _normalize_stage(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return rows

@pipe_output(
    step(_parse_stage),
    step(_normalize_stage),
)
@tag(layer="extract", artifact="ast_files", kind="table")
def ast_files(raw_rows: list[dict[str, object]]) -> TableLike:
    return build_table(raw_rows)
```

Target files
- src/extract/ast_extract.py
- src/extract/cst_extract.py
- src/extract/tree_sitter_extract.py
- src/extract/symtable_extract.py

Implementation checklist
- [ ] Isolate parse vs normalize vs materialize phases into stage functions.
- [ ] Use `@pipe_output` for node outputs that already exist in the inferred graph.
- [ ] Tag each stage with `layer="extract"` and `kind="stage"`.
- [ ] Avoid adding new outputs; stages must feed existing inferred nodes.

Decommission candidates
- Inline parsing/normalization helpers replaced by stages (case-by-case).

----------------------------------------------------------------------
Scope 3: CPG pipeline pipe stages (relation normalization, id/key derivation)
----------------------------------------------------------------------
Why
- CPG edge/prop emitters perform multiple steps in one node.
- Splitting into stages aligns with inference-driven execution and improves lineage.

Representative pattern
```python
from hamilton.function_modifiers import pipe_input, step, tag

@tag(layer="cpg", artifact="normalize_relation", kind="stage")
def _normalize_relation_stage(rel: TableLike) -> TableLike:
    return rel

@tag(layer="cpg", artifact="assign_ids", kind="stage")
def _assign_ids_stage(rel: TableLike) -> TableLike:
    return rel

@pipe_input(
    step(_normalize_relation_stage),
    step(_assign_ids_stage),
    on_input="relation_output",
    namespace="cpg_edges",
)
@tag(layer="cpg", artifact="cpg_edges", kind="table")
def cpg_edges(relation_output: TableLike) -> TableLike:
    return relation_output
```

Target files
- src/cpg/emit_edges_ibis.py
- src/cpg/emit_props_ibis.py
- src/cpg/prop_transforms.py

Implementation checklist
- [ ] Split relation normalization, id/key derivation, and final select into pipe stages.
- [ ] Tag each stage with `layer="cpg"` and stable artifact names.
- [ ] Keep stage boundaries aligned with inferred task outputs; do not add rule logic.

Decommission candidates
- Inline helper functions (e.g., normalization/selection helpers) replaced by stages.

----------------------------------------------------------------------
Scope 4: Evidence-pure stage policy enforcement
----------------------------------------------------------------------
Why
- Stages must remain inference-driven and strictly derived from inferred artifacts.
- Prevents accidental drift into rule-based or ad-hoc transforms.

Representative pattern
```python
from hamilton.function_modifiers import tag

@tag(layer="policy", artifact="stage_policy", kind="guard")
def stage_policy_guard(stage_name: str) -> str:
    return stage_name
```

Target files
- src/hamilton_pipeline/modules/task_execution.py
- src/hamilton_pipeline/modules/task_graph.py
- src/relspec/evidence.py

Implementation checklist
- [ ] Validate that stage inputs are part of inferred task outputs or evidence catalog.
- [ ] Reject or log stage additions that introduce non-inferred dependencies.
- [ ] Ensure stage tags include task_name/task_output for traceability.

Decommission candidates
- None (policy additions only).

----------------------------------------------------------------------
Scope 5: Documentation + observability alignment
----------------------------------------------------------------------
Why
- Stage additions must be discoverable and aligned with inference semantics.

Representative pattern
```python
@tag(layer="obs", artifact="stage_tags", kind="metadata")
def stage_tag_policy() -> dict[str, str]:
    return {"stage_policy": "inference_only"}
```

Target files
- docs/plans/hamilton_inference_streamline_plan.md
- docs/architecture/Starting_Architecture.md
- src/obs/* (if stage telemetry is added)

Implementation checklist
- [ ] Document new stage boundaries and their inferred provenance.
- [ ] Ensure tags reflect task_name and plan_fingerprint when applicable.
- [ ] Add or update telemetry to track stage timing if needed.

Decommission candidates
- None.

----------------------------------------------------------------------
Decommission and delete list (post-implementation)
----------------------------------------------------------------------
- Inline normalize/extract/cpg helpers replaced by explicit pipe stages (case-by-case).
- Any redundant intermediate transform wrappers that become pipe stages.
