# Extract ↔ Semantics Integration & Parallelization Plan

This plan consolidates the extraction layer (`src/extract`) with the semantic-first pipeline by:
1) registering extract outputs as first‑class datasets,
2) validating semantic inputs against extract outputs,
3) enabling rustworkx scheduling of extract scopes where appropriate, and
4) ensuring Delta/DataFusion materialization uses explicit dataset locations.

The goal is to make extract outputs discoverable, parallelizable, and strongly typed for
semantic consumption and downstream planning.

---

## Scope A — Extract Output Catalog & Runtime Profile Wiring

**Goal:** Register all extract outputs as explicit dataset locations so they are
discoverable by DataFusion, materializable to Delta, and available for semantic input validation.

### Representative implementation

```python
# src/datafusion_engine/extract/output_catalog.py (NEW)
"""Build dataset locations for extract outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation
from datafusion_engine.extract.metadata import extract_metadata_specs
from datafusion_engine.extract.registry import dataset_spec as extract_dataset_spec


def build_extract_output_catalog(
    *,
    output_root: str,
) -> DatasetCatalog:
    """Return a catalog of DatasetLocation entries for extract outputs."""
    root = Path(output_root)
    catalog = DatasetCatalog()
    for meta in extract_metadata_specs():
        name = meta.name  # use canonical dataset name, not alias
        catalog.register(
            name,
            DatasetLocation(
                path=str(root / name),
                format="delta",
                dataset_spec=extract_dataset_spec(name),
            ),
        )
    return catalog
```

```python
# src/datafusion_engine/session/runtime.py (UPDATE)
# Add new profile fields:
extract_output_root: str | None = None
extract_output_catalog_name: str | None = None

# Add helper:
def extract_output_locations_for_profile(profile: DataFusionRuntimeProfile) -> Mapping[str, DatasetLocation]:
    if profile.extract_dataset_locations:
        return profile.extract_dataset_locations
    if profile.extract_output_catalog_name is not None:
        catalog = profile.registry_catalogs.get(profile.extract_output_catalog_name)
        return {name: catalog.get(name) for name in catalog.names()} if catalog else {}
    if profile.extract_output_root is None:
        return {}
    return build_extract_output_catalog(output_root=profile.extract_output_root)
```

### Target files
- `src/datafusion_engine/extract/output_catalog.py` (NEW)
- `src/datafusion_engine/session/runtime.py` (ADD: extract output root/catalog hooks)
- `src/datafusion_engine/dataset/registry.py` (include extract output locations in catalog)
- `src/datafusion_engine/io/write.py` (allow path binding from extract output catalog)
- `src/hamilton_pipeline/types/output_config.py` (config for extract output root/catalog)
- `src/hamilton_pipeline/modules/inputs.py` (wire into runtime profile)

### Deprecate/delete after completion
- None (additive change; old extract location config can remain during transition)

### Implementation checklist
- [x] Add `extract_output_root` / `extract_output_catalog_name` to runtime profile
- [x] Add `build_extract_output_catalog()` helper
- [x] Include extract output locations in dataset catalog and dataset_location resolution
- [x] Wire output config to pass extract output root/catalog into runtime profile
- [x] Add tests for catalog construction + dataset lookup

---

## Scope B — Semantic Input Registration Gate for Extract Outputs

**Goal:** Ensure semantic pipeline fails fast if required extract outputs are missing
or not registered, and that extraction aliases resolve properly.

### Representative implementation

```python
# src/semantics/input_registry.py (UPDATE)
from datafusion_engine.extract.bundles import dataset_name_for_output

# For each semantic input, add fallback names derived from extract output aliases
def _fallback_names(output: str) -> tuple[str, ...]:
    try:
        dataset = dataset_name_for_output(output)
    except KeyError:
        return ()
    if dataset is None or dataset == output:
        return ()
    return (dataset,)

SemanticInputSpec(
    canonical_name="cst_refs",
    extraction_source="cst_refs",
    required=True,
    fallback_names=_fallback_names("cst_refs"),
)
```

```python
# src/semantics/pipeline.py (UPDATE)
# Enforce semantic input validation before planning semantic views:
from semantics.input_registry import require_semantic_inputs

resolved_inputs = require_semantic_inputs(ctx)
```

### Target files
- `src/semantics/input_registry.py` (fallbacks from extract output aliases)
- `src/semantics/pipeline.py` (enforce semantic input availability)
- `src/datafusion_engine/views/registry_specs.py` (schema column validation)
- `src/semantics/validation/catalog_validation.py` (ensure required columns exist)

### Deprecate/delete after completion
- None

### Implementation checklist
- [x] Map extract output aliases into semantic input fallback names
- [x] Enforce semantic input validation in view registration
- [x] Add tests covering missing extract outputs

---

## Scope C — Rustworkx Scheduling for Extract Scopes

**Goal:** Treat extract scopes as first‑class tasks in the rustworkx scheduler so
they can be parallelized and orchestrated with the rest of the pipeline.

### Representative implementation

```python
# src/relspec/extract_plan.py (NEW)
"""Construct extract tasks for rustworkx scheduling."""

from __future__ import annotations

from dataclasses import dataclass
from datafusion_engine.extract.extractors import extractor_specs, ExtractorSpec


@dataclass(frozen=True)
class ExtractTaskSpec:
    name: str
    outputs: tuple[str, ...]
    required_inputs: tuple[str, ...]
    supports_plan: bool


def build_extract_tasks() -> tuple[ExtractTaskSpec, ...]:
    tasks: list[ExtractTaskSpec] = []
    for spec in extractor_specs():
        tasks.append(
            ExtractTaskSpec(
                name=spec.name,
                outputs=spec.outputs,
                required_inputs=spec.required_inputs,
                supports_plan=spec.supports_plan,
            )
        )
    return tuple(tasks)
```

```python
# src/relspec/execution_plan.py (UPDATE)
# Add extract tasks to the task graph when enabled.
# Model them as scan tasks or a new task kind "extract".
```

### Target files
- `src/relspec/extract_plan.py` (NEW)
- `src/relspec/execution_plan.py` (add extract tasks to the DAG)
- `src/relspec/rustworkx_graph.py` (support extract task edges if needed)

### Deprecate/delete after completion
- None (additive task type)

### Implementation checklist
- [x] Build extract task specs from `extractor_specs()`
- [x] Add extract tasks into rustworkx graph
- [x] Decide task kind semantics (`scan` vs `extract`)
- [x] Add tests for extract DAG edges and dependencies

---

## Scope D — Hamilton Orchestration for Extract Tasks

**Goal:** Execute extract tasks in parallel via Hamilton/rustworkx, and
register their outputs into DataFusion before semantic views compile.

### Representative implementation

```python
# src/hamilton_pipeline/modules/task_execution.py (UPDATE)
# Handle task_kind == "extract" by running the extractor, registering output tables,
# and materializing via write_extract_outputs(...).
```

```python
# src/hamilton_pipeline/modules/execution_plan.py (UPDATE)
# Include extract tasks in the generated module outputs and scheduling metadata.
```

### Target files
- `src/hamilton_pipeline/modules/task_execution.py` (execute extract tasks)
- `src/hamilton_pipeline/modules/execution_plan.py` (expose extract tasks)
- `src/engine/materialize_pipeline.py` (already handles `write_extract_outputs`)
- `src/extract/helpers.py` (use existing plan/materialization helpers)

### Deprecate/delete after completion
- Potentially retire ad‑hoc extract‑only entrypoints that bypass scheduling

### Implementation checklist
- [x] Add extract task execution path in Hamilton execution node
- [x] Ensure extract outputs are registered before semantic planning
- [x] Record extract artifacts (schemas, row counts, errors)
- [x] Add integration test covering extract → semantic build

---

## Scope E — Delta/DataFusion Integration for Extract Outputs

**Goal:** Ensure extract outputs are materialized to Delta with enforced schema
and are discoverable for planning and worklist generation.

### Representative implementation

```python
# src/engine/materialize_pipeline.py (ALREADY EXISTS)
# write_extract_outputs(...) writes to Delta if dataset_location exists.
# Ensure extract output locations are populated (Scope A).
```

```python
# src/extract/infrastructure/worklists.py (UPDATE)
# If output_location exists, use DataFusion scan overrides to avoid full scans.
```

### Target files
- `src/engine/materialize_pipeline.py` (existing write path)
- `src/extract/infrastructure/worklists.py` (scan override optimization)
- `src/datafusion_engine/session/runtime.py` (dataset registration)

### Deprecate/delete after completion
- None

### Implementation checklist
- [x] Verify all extract outputs have explicit dataset locations
- [x] Ensure Delta write policy from extract registry applies
- [x] Add schema identity artifacts for extract outputs

---

## Implementation Order

1. **Scope A** — Extract output catalog & runtime profile wiring  
2. **Scope B** — Semantic input registration gate  
3. **Scope C** — Rustworkx extract task graph  
4. **Scope D** — Hamilton orchestration of extract tasks  
5. **Scope E** — Delta/DataFusion integration hardening  

---

## Success Criteria

1. Extract outputs are registered as datasets (not only AST/bytecode/SCIP).
2. Semantic pipeline fails fast on missing extract outputs.
3. Extract scopes can be scheduled as parallel tasks by rustworkx.
4. Extract outputs are materialized to Delta with enforced schema policy.
5. All semantic inputs resolve from registered extract datasets or explicit fallbacks.

---

## Notes & Risks

- **Nested schemas**: Delta and DataFusion support nested types. The key risk is not schema
  complexity, but missing dataset locations. Scope A resolves this.
- **Pipeline ordering**: semantic compilation must occur *after* extract tasks are registered.
- **Incremental/CDF**: extract outputs already enforce CDF features; ensure CDF policies
  remain consistent when wiring new catalogs.
