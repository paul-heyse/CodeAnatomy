# Hamilton Best-in-Class Semantic Alignment Plan (v1)

## Purpose
Deliver a Hamilton deployment that is (1) fully aligned to the semantic architecture in `src/semantics`, (2) UI/telemetry ready with stable semantic contracts, and (3) operationally best-in-class for caching, lineage, and reproducibility.

## Success Criteria
- Semantic outputs derive tags directly from the semantic catalog/specs and pass strict registry validation.
- Every public semantic output has consistent, machine-readable tags, schema references, and join/entity keys.
- UI/telemetry surfaces show clean lineage and stable naming; registry artifacts are emitted per plan signature.
- Cache policy is predictable and aligned to semantic runtime policies, with safe data-versioning for Arrow/DataFusion outputs.
- Column-level lineage and validation are available for core semantic datasets.

---

## Scope 1 — Semantic Tag Canon (Catalog-Driven Tagging)

### Goal
Replace hand-authored tag payloads with tags derived from the semantic catalog/specs. This ensures semantic tags are authoritative, consistent, and versioned in one place.

### Key Architectural Elements
- Translate `semantics.catalog.dataset_rows` and `semantics.catalog.dataset_specs` into Hamilton tag payloads.
- Tagging helpers should enforce required keys for semantic outputs (`layer`, `semantic_id`, `kind`, `entity`, `grain`, `version`, `stability`, plus `schema_ref` / `entity_keys` / `join_keys` for tables).

### Representative Code Snippet (proposed)
```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from hamilton_pipeline.tag_policy import TagPolicy
from semantics.catalog.dataset_rows import SemanticDatasetRow
from semantics.catalog.dataset_specs import dataset_spec

@dataclass(frozen=True)
class SemanticTagPayload:
    policy: TagPolicy


def tag_policy_from_dataset_row(row: SemanticDatasetRow) -> TagPolicy:
    spec = dataset_spec(row.name)
    return TagPolicy(
        layer="semantic",
        kind="table",
        artifact=row.name,
        semantic_id=row.name,
        entity=spec.entity,
        grain=spec.grain,
        version=str(row.version),
        stability=spec.stability,
        schema_ref=row.name,
        materialization=spec.materialization,
        materialized_name=spec.materialized_name,
        entity_keys=spec.entity_keys,
        join_keys=spec.join_keys,
    )
```

### Target Files
- `src/hamilton_pipeline/tag_policy.py`
- `src/hamilton_pipeline/modules/subdags.py`
- `src/hamilton_pipeline/io_contracts.py`
- `src/semantics/catalog/dataset_rows.py`
- `src/semantics/catalog/dataset_specs.py`

### Deprecate / Delete
- `_CPG_OUTPUT_POLICIES` in `src/hamilton_pipeline/modules/subdags.py` (manual tag payloads)
- Any ad-hoc semantic tag literals for semantic table outputs in Hamilton modules

### Implementation Checklist
- [x] Add a catalog-to-tag policy helper (semantic dataset row/spec -> TagPolicy).
- [x] Replace manual semantic tag payloads in `subdags.py` with generated tags.
- [x] Ensure `semantic_id` and `schema_ref` map to canonical dataset names.
- [x] Validate tags at build time (fail fast on missing required keys).

---

## Scope 2 — Semantic Registry Compiler (UI Contract + Validation)

### Goal
Create a strict semantic registry compiler that validates required tags and writes a canonical registry artifact per plan signature (for UI + tooling).

### Key Architectural Elements
- Compile semantic registry via `Driver.list_available_variables(tag_filter=...)`.
- Enforce required tag keys and unique `semantic_id` per output.
- Emit a registry artifact alongside other diagnostics (plan artifacts / run logs).

### Representative Code Snippet (proposed)
```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

REQUIRED_BASE = {"layer", "semantic_id", "kind", "entity", "grain", "version", "stability"}

@dataclass(frozen=True)
class SemanticRegistryEntry:
    semantic_id: str
    node_name: str
    kind: str
    entity: str
    grain: str
    version: str
    schema_ref: str | None
    entity_keys: tuple[str, ...]
    join_keys: tuple[str, ...]
    stability: str
    tags: Mapping[str, object]


def compile_semantic_registry(driver) -> dict[str, SemanticRegistryEntry]:
    nodes = driver.list_available_variables(tag_filter={"layer": "semantic"})
    registry: dict[str, SemanticRegistryEntry] = {}
    for node in nodes:
        tags = dict(node.tags)
        missing = REQUIRED_BASE - tags.keys()
        if missing:
            raise ValueError(f"Missing required tags {sorted(missing)} for {node.name}")
        semantic_id = str(tags["semantic_id"])
        if semantic_id in registry:
            raise ValueError(f"Duplicate semantic_id={semantic_id}")
        registry[semantic_id] = SemanticRegistryEntry(
            semantic_id=semantic_id,
            node_name=node.name,
            kind=str(tags["kind"]),
            entity=str(tags["entity"]),
            grain=str(tags["grain"]),
            version=str(tags["version"]),
            schema_ref=str(tags.get("schema_ref")) if tags.get("schema_ref") else None,
            entity_keys=_split_keys(tags.get("entity_keys")),
            join_keys=_split_keys(tags.get("join_keys")),
            stability=str(tags["stability"]),
            tags=tags,
        )
    return registry
```

### Target Files
- `src/hamilton_pipeline/semantic_registry.py`
- `src/hamilton_pipeline/lifecycle.py`
- `src/hamilton_pipeline/structured_logs.py`

### Deprecate / Delete
- Any legacy registry output logic that doesn’t validate tag completeness or uniqueness.

### Implementation Checklist
- [x] Replace tag validation with strict semantic registry compilation logic.
- [x] Emit registry artifact per `plan_signature` into diagnostics/artifacts.
- [x] Add unit tests for missing tags and duplicate `semantic_id` failures.

---

## Scope 3 — Schema Metadata + Validation as First-Class Nodes

### Goal
Add Hamilton-level schema metadata and validation for semantic outputs using `@schema`, `@check_output`, and existing `SchemaContractValidator`.

### Key Architectural Elements
- Attach schema metadata (`@schema.output`) to DataFrame/table outputs.
- Enforce schema contracts (fail for semantic tables, warn for diagnostics).

### Representative Code Snippet (proposed)
```python
from hamilton.function_modifiers import check_output, schema

@schema.output(("entity_id", "string"), ("symbol", "string"), ("path", "string"))
@check_output_custom(SchemaContractValidator(dataset_name="rel_name_symbol_v1", importance="fail"))
def rel_name_symbol_v1_table(...) -> TableLike:
    return table
```

### Target Files
- `src/hamilton_pipeline/modules/subdags.py`
- `src/hamilton_pipeline/modules/cpg_outputs.py`
- `src/hamilton_pipeline/modules/column_features.py`
- `src/hamilton_pipeline/validators.py`

### Deprecate / Delete
- Any non-standard ad-hoc schema checks embedded in node bodies (prefer validators).

### Implementation Checklist
- [x] Add schema metadata for semantic tables (schema_ref + schema output).
- [x] Wire validators to semantic outputs with correct severity.
- [x] Add tests for schema mismatch failure behavior.

---

## Scope 4 — Column-Level Lineage (Entity/Join Keys)

### Goal
Expose key columns as first-class nodes to strengthen lineage, data quality, and UI exploration.

### Key Architectural Elements
- Use `@extract_columns` and `@tag_output` to create nodes for entity/join keys.
- Tag extracted columns as `layer="semantic"` with `kind="column"` or `kind="key"`.

### Representative Code Snippet (proposed)
```python
from hamilton.function_modifiers import extract_columns, tag_output

@extract_columns("entity_id", "symbol", "path")
@tag_output(
    entity_id={"layer": "semantic", "kind": "key", "semantic_id": "rel_name_symbol.entity_id"},
    symbol={"layer": "semantic", "kind": "key", "semantic_id": "rel_name_symbol.symbol"},
    path={"layer": "semantic", "kind": "key", "semantic_id": "rel_name_symbol.path"},
)
def rel_name_symbol_v1(...) -> TableLike:
    return table
```

### Target Files
- `src/hamilton_pipeline/modules/subdags.py`
- `src/hamilton_pipeline/modules/cpg_outputs.py`
- `src/hamilton_pipeline/tag_policy.py`

### Deprecate / Delete
- Manual lineage/inspection code outside of Hamilton metadata.

### Implementation Checklist
- [x] Identify high-value semantic tables for column extraction.
- [x] Add extract/tag decorators with stable semantic IDs.
- [x] Validate node naming and tagging in registry compiler.

---

## Scope 5 — Cross-Cutting Output Transforms (Pipe/Mutate)

### Goal
Centralize post-processing (validation, normalization, shape) using `pipe_output` or `mutate`, avoiding duplicate logic in node bodies.

### Key Architectural Elements
- Use `pipe_output` steps with `when(...)` config gates for staged transformations.
- Use `mutate` for cross-cutting adjustments on a set of outputs without changing original node definitions.

### Representative Code Snippet (proposed)
```python
from hamilton.function_modifiers import pipe_output, step, value

@pipe_output(
    step(_validate_schema).when(enable_strict_validation=True),
    step(_normalize_names, case=value("snake")),
    on_output=["cpg_nodes", "cpg_edges"],
    namespace="post",
)
def semantic_outputs_bundle(...) -> dict[str, TableLike]:
    return outputs
```

### Target Files
- `src/hamilton_pipeline/modules/cpg_outputs.py`
- `src/hamilton_pipeline/modules/subdags.py`
- `src/hamilton_pipeline/cpg_finalize_utils.py`

### Deprecate / Delete
- Duplicated validation or post-processing blocks in individual nodes.

### Implementation Checklist
- [x] Identify post-processing logic to centralize (validation/finalize).
- [x] Implement pipe steps with stable names and config gating.
- [x] Ensure steps are tagged and visible for UI lineage.

---

## Scope 6 — Cache Policy Alignment (Semantic Runtime → Hamilton Cache)

### Goal
Make Hamilton caching decisions consistent with semantic runtime cache policy, with safe data-versioning for Arrow/DataFusion outputs.

### Key Architectural Elements
- Bridge semantic `CachePolicy` into Hamilton cache behavior defaults.
- Add custom data-version hashing for Arrow tables to avoid unhashable values.

### Representative Code Snippet (proposed)
```python
from hamilton.caching import fingerprinting
import pyarrow as pa

@fingerprinting.register(pa.Table)
def _hash_arrow_table(table: pa.Table) -> str:
    return fingerprinting.hash_pandas_obj(table.to_pandas())
```

### Target Files
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/cache_lineage.py`
- `src/semantics/runtime.py`
- `src/hamilton_pipeline/modules/inputs.py`

### Deprecate / Delete
- Any ad-hoc caching policy logic outside the central cache profile.

### Implementation Checklist
- [x] Map semantic cache policy into Hamilton cache defaults.
- [x] Add Arrow/DataFusion data-version hashing.
- [x] Expand cache lineage export with plan signature + semantic IDs.

---

## Scope 7 — UI/Telemetry Snapshot Discipline

### Goal
Ensure every plan execution emits a consistent snapshot bundle for UI and CI: graph visualization, registry, cache lineage, plan artifacts.

### Key Architectural Elements
- Emit graph snapshot (`driver.visualize_execution_graph`) per plan signature.
- Persist semantic registry and cache lineage artifacts in the same run directory.

### Representative Code Snippet (proposed)
```python
from pathlib import Path

def write_graph_snapshot(driver, *, plan_signature: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    driver.visualize_execution_graph(output_file_path=str(out_dir / f"{plan_signature}.png"))
```

### Target Files
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/graph_snapshot.py`
- `src/hamilton_pipeline/semantic_registry.py`
- `src/hamilton_pipeline/cache_lineage.py`

### Deprecate / Delete
- Any legacy ad-hoc UI snapshotting or scattered graph export logic.

### Implementation Checklist
- [x] Add per-run graph snapshot emission.
- [x] Standardize artifact paths under `cache_path` or `build/structured_logs`.
- [x] Add CI check to verify snapshot + registry artifacts exist.

---

## Implementation Phasing (recommended)
1. **Phase 1**: Scope 1 + Scope 2 (semantic tag canon + registry compiler). These are dependency foundations.
2. **Phase 2**: Scope 3 + Scope 4 (schema validation + column lineage) to harden contract surface.
3. **Phase 3**: Scope 5 + Scope 6 + Scope 7 (cross-cut transforms, cache alignment, telemetry snapshots).

---

## Global Checklist (Definition of Done)
- [x] All semantic outputs satisfy the required tag taxonomy.
- [x] Semantic registry compiler passes and emits artifacts per plan signature.
- [x] Schema validation for semantic outputs has deterministic fail/warn behavior.
- [x] Cache policies are aligned to semantic runtime settings; data-versioning is deterministic.
- [x] UI snapshots and cache lineage artifacts are emitted consistently.
- [x] No deprecated manual tag payloads remain in codebase.
