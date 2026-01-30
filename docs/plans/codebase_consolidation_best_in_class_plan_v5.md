# Codebase Consolidation Plan v5 (Best-in-Class, Breaking Changes OK)

## Executive Summary

This plan extends the v4 consolidation work with a **best-in-class** follow-up focused on: (1) Hamilton metadata consistency, (2) IO/materialization uniformity, (3) cache and run artifact observability, (4) aggressive subdag reuse, and (5) DataFusion + Delta provider standardization (listing tables, plan artifacts, Delta scan config, CDF, and maintenance). It assumes the v3 and v4 scopes are already complete and intentionally pushes to a **target-state architecture**, even where breaking changes are required.

## Design Principles

1. **Single source of truth** for tags, IO contracts, scan config, and schema governance.
2. **Hamilton-first orchestration** with strict metadata discipline and predictable outputs.
3. **DataFusion-native scanning** (listing tables + explicit session config) for uniform performance and observability.
4. **Delta-as-provider** with explicit scan configuration, snapshot pinning, and CDF as first-class incremental.
5. **Plan artifacts are a product**: reproducible explain snapshots and metrics are mandatory.

## Scope Index

1. Unified Hamilton Tag Policy + Registry Enforcement (Planned)
2. Hamilton IO Contract + Materialization Unification (Planned)
3. Hamilton Cache Lineage + Structured Run Logs (Planned)
4. Parameterized Subdag Library for Reused Patterns (Planned)
5. DataFusion Listing Registration + SessionConfig Standardization (Planned)
6. Plan Artifacts + Explain Snapshot Pipeline (Planned)
7. Delta Provider Contract + ScanConfig Unification (Planned)
8. Delta CDF-First Incremental Pipeline (Planned)
9. Delta Maintenance Control Plane (Planned)
10. Schema Governance via Info Schema + Hamilton Validators (Planned)
11. Deferred Decommissioning and Cleanup (Planned)

---

## 1. Unified Hamilton Tag Policy + Registry Enforcement

### Goal
Create a single tag taxonomy and enforcement layer for Hamilton nodes, and wire tags into semantic registry compilation and UI tracker metadata.

### Status
Planned

### Representative Pattern
```python
from dataclasses import dataclass
from hamilton.function_modifiers import tag

@dataclass(frozen=True)
class TagPolicy:
    layer: str
    kind: str
    artifact: str
    semantic_id: str | None = None
    entity: str | None = None
    grain: str | None = None
    version: str | None = None

    def as_tags(self) -> dict[str, str]:
        payload = {
            "layer": self.layer,
            "kind": self.kind,
            "artifact": self.artifact,
        }
        optional = {
            "semantic_id": self.semantic_id,
            "entity": self.entity,
            "grain": self.grain,
            "version": self.version,
        }
        payload.update({key: value for key, value in optional.items() if value is not None})
        return payload


def apply_tag(policy: TagPolicy):
    return tag(**policy.as_tags())


@apply_tag(
    TagPolicy(
        layer="plan",
        kind="context",
        artifact="plan_context",
        semantic_id="plan.context",
        version="v1",
    )
)
def plan_context(...):
    ...
```

### Target Files
- Create: `src/hamilton_pipeline/tag_policy.py`
- Modify: `src/hamilton_pipeline/semantic_registry.py`
- Modify: `src/hamilton_pipeline/driver_factory.py`
- Modify: `src/hamilton_pipeline/hamilton_tracker.py`
- Modify: `src/hamilton_pipeline/modules/outputs.py`
- Modify: `src/hamilton_pipeline/modules/task_execution.py`
- Modify: `src/hamilton_pipeline/modules/execution_plan.py`
- Modify: `src/hamilton_pipeline/modules/params.py`
- Modify: `src/hamilton_pipeline/modules/inputs.py`
- Modify: `src/hamilton_pipeline/modules/column_features.py`

### Deletions
- Remove ad-hoc tag dicts once policies are in place:
  - `src/hamilton_pipeline/modules/task_execution.py` (`_CPG_FINAL_TAGS`)
  - `src/hamilton_pipeline/modules/outputs.py` (`_CPG_DELTA_WRITE_TAGS`)
  - `src/hamilton_pipeline/modules/outputs.py` (`_semantic_tag` helper if superseded)

### Checklist
- [ ] Define `TagPolicy` and `apply_tag` helpers.
- [ ] Update Hamilton modules to use the centralized policy.
- [ ] Enforce required tags in `semantic_registry` with actionable error messages.
- [ ] Ensure tracker tags include semantic + plan metadata consistently.

---

## 2. Hamilton IO Contract + Materialization Unification

### Goal
Consolidate dataloaders, datasavers, and materializers into a single IO contract library and stop embedding IO logic in pipeline modules.

### Status
Planned

### Representative Pattern
```python
from dataclasses import dataclass
from hamilton.function_modifiers import dataloader, datasaver, source
from hamilton.io import materialization

@dataclass(frozen=True)
class DatasetIO:
    name: str
    materialization: str


@dataloader
def read_dataset(io: DatasetIO):
    return load_table(io.name)


@datasaver
def write_dataset(io: DatasetIO, table: object) -> dict[str, object]:
    write_table(io.name, table)
    return {"dataset": io.name, "materialization": io.materialization}


def build_materializers(specs: list[DatasetIO]):
    return [
        materialization.to.write_dataset(
            id=f"materialize_{spec.name}",
            dependencies=[spec.name],
            io=spec,
        )
        for spec in specs
    ]
```

### Target Files
- Create: `src/hamilton_pipeline/io_contracts.py`
- Modify: `src/hamilton_pipeline/materializers.py`
- Modify: `src/hamilton_pipeline/modules/dataloaders.py`
- Modify: `src/hamilton_pipeline/modules/outputs.py`
- Modify: `src/hamilton_pipeline/modules/inputs.py`
- Modify: `src/hamilton_pipeline/task_module_builder.py`
- Modify: `src/datafusion_engine/write_pipeline.py`

### Deletions
- Remove local IO helpers embedded in Hamilton modules once `io_contracts` is adopted.

### Checklist
- [ ] Define dataset IO contracts and materializer factories.
- [ ] Move per-module IO logic into centralized contracts.
- [ ] Update task builder to inject materializers and datasavers.
- [ ] Add contract-level validation of output metadata.

---

## 3. Hamilton Cache Lineage + Structured Run Logs

### Goal
Standardize cache lineage export and introduce structured JSONL run logs for all Hamilton executions.

### Status
Planned

### Representative Pattern
```python
from dataclasses import dataclass
from hamilton.lifecycle import api as lifecycle_api

@dataclass
class StructuredLogHook(lifecycle_api.GraphExecutionHook):
    log_path: str

    def run_after_graph_execution(self, *, run_id: str, **kwargs: object) -> None:
        payload = {"run_id": run_id, "event": "graph_complete"}
        write_jsonl(self.log_path, payload)
```

### Target Files
- Create: `src/hamilton_pipeline/structured_logs.py`
- Modify: `src/hamilton_pipeline/cache_lineage.py`
- Modify: `src/hamilton_pipeline/lifecycle.py`
- Modify: `src/hamilton_pipeline/driver_factory.py`
- Modify: `src/datafusion_engine/diagnostics.py`

### Deletions
- Remove ad-hoc run logging scattered across diagnostics once structured logs are centralized.

### Checklist
- [ ] Add structured log hook and schema.
- [ ] Ensure cache lineage export is always tied to run metadata.
- [ ] Persist lineage + logs in a deterministic directory layout.
- [ ] Emit log artifacts in diagnostics store.

---

## 4. Parameterized Subdag Library for Reused Patterns

### Goal
Replace repeated Hamilton node sequences with parameterized subdags and pipe helpers.

### Status
Planned

### Representative Pattern
```python
from hamilton.function_modifiers import parameterized_subdag, pipe_input

@parameterized_subdag(
    inputs={"raw_table": "{dataset}_raw"},
    outputs={"clean_table": "{dataset}_clean"},
    config={"dataset": ["nodes", "edges", "props"]},
)
@pipe_input(step_name="normalize", input_name="raw_table")
def normalize_subdag(raw_table: object) -> object:
    return normalize_table(raw_table)
```

### Target Files
- Create: `src/hamilton_pipeline/modules/subdags.py`
- Modify: `src/hamilton_pipeline/modules/outputs.py`
- Modify: `src/hamilton_pipeline/modules/column_features.py`
- Modify: `src/hamilton_pipeline/modules/task_execution.py`
- Modify: `src/hamilton_pipeline/modules/params.py`

### Deletions
- Remove duplicated per-dataset function blocks once subdags are adopted.

### Checklist
- [ ] Identify repeated node chains and factor into subdags.
- [ ] Replace manual per-dataset variants with parameterized subdags.
- [ ] Update tests/fixtures to reference new subdag outputs.

---

## 5. DataFusion Listing Registration + SessionConfig Standardization

### Goal
Centralize table registration into listing tables with explicit partition columns and file ordering, and unify DataFusion session config in one builder.

### Status
Planned

### Representative Pattern
```python
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_listing_table(
    "dataset",
    "/data/datasets/dataset",
    file_extension=".parquet",
    table_partition_cols=[("partition_date", pa.date32())],
    file_sort_order=["partition_date", "id"],
)
```

### Target Files
- Create: `src/datafusion_engine/table_registration.py`
- Modify: `src/datafusion_engine/dataset_registration.py`
- Modify: `src/datafusion_engine/dataset_registry.py`
- Modify: `src/datafusion_engine/dataset_resolution.py`
- Modify: `src/datafusion_engine/scan_planner.py`
- Modify: `src/datafusion_engine/session_factory.py`
- Modify: `src/datafusion_engine/sql_options.py`
- Modify: `src/datafusion_engine/compile_options.py`
- Modify: `src/datafusion_engine/io_adapter.py`

### Deletions
- Remove duplicated listing-table registration logic once centralized.

### Checklist
- [ ] Create a single registration entrypoint for all table types.
- [ ] Ensure partition columns and file sort order are first-class config.
- [ ] Standardize session options (parquet pushdown, metadata cache limit).
- [ ] Emit registration diagnostics with normalized payloads.

---

## 6. Plan Artifacts + Explain Snapshot Pipeline

### Goal
Make logical, optimized, and physical plan snapshots a required artifact for every run.

### Status
Planned

### Representative Pattern
```python
plan_bundle = {
    "logical": df.logical_plan(),
    "optimized": df.optimized_logical_plan(),
    "physical": df.execution_plan(),
    "explain": df.explain(verbose=True, analyze=True),
}
record_plan_artifact(plan_bundle)
```

### Target Files
- Modify: `src/datafusion_engine/plan_artifact_store.py`
- Modify: `src/datafusion_engine/plan_bundle.py`
- Modify: `src/datafusion_engine/plan_profiler.py`
- Modify: `src/datafusion_engine/planning_pipeline.py`
- Modify: `src/hamilton_pipeline/modules/execution_plan.py`

### Deletions
- Remove ad-hoc plan debug capture once plan artifacts are always recorded.

### Checklist
- [ ] Standardize the plan artifact schema and persistence path.
- [ ] Capture explain output with run + dataset metadata.
- [ ] Wire plan artifact emission into Hamilton execution.

---

## 7. Delta Provider Contract + ScanConfig Unification

### Goal
Define a single Delta provider contract that binds scan config to session settings and normalizes snapshot pinning behavior.

### Status
Planned

### Representative Pattern
```python
from datafusion_engine.delta_scan_config import delta_scan_snapshot

scan_snapshot = delta_scan_snapshot(
    enable_parquet_pushdown=True,
    schema_force_view_types=True,
    wrap_partition_values=True,
)
provider = build_delta_provider(
    table_uri=table_uri,
    scan_snapshot=scan_snapshot,
    table_version=resolved_version,
)
```

### Target Files
- Create: `src/datafusion_engine/delta_provider_contracts.py`
- Modify: `src/datafusion_engine/delta_scan_config.py`
- Modify: `src/datafusion_engine/delta_control_plane.py`
- Modify: `src/datafusion_engine/dataset_resolution.py`
- Modify: `src/datafusion_engine/dataset_registration.py`
- Modify: `src/datafusion_engine/scan_planner.py`
- Modify: `src/datafusion_engine/table_provider_metadata.py`
- Modify: `src/datafusion_engine/plan_bundle.py`

### Deletions
- Remove inline scan config assembly once provider contracts are enforced.

### Checklist
- [ ] Define contract types for scan config + snapshot pinning.
- [ ] Enforce config derivation from session settings.
- [ ] Emit scan config identity hashes in plan artifacts.

---

## 8. Delta CDF-First Incremental Pipeline

### Goal
Make Delta Change Data Feed the default incremental path and remove duplicated diff logic.

### Status
Planned

### Representative Pattern
```python
cdf_bundle = delta_cdf_provider(
    request=DeltaCdfProviderRequest(
        table_uri=table_uri,
        options=DeltaCdfOptions(starting_version=from_version),
    )
)
register_provider(cdf_bundle.provider_capsule)
```

### Target Files
- Modify: `src/datafusion_engine/delta_control_plane.py`
- Modify: `src/datafusion_engine/dataset_registration.py`
- Modify: `src/datafusion_engine/dataset_resolution.py`
- Modify: `src/incremental/cdf_runtime.py`
- Modify: `src/incremental/cdf_cursors.py`
- Modify: `src/incremental/registry_builders.py`
- Modify: `src/incremental/plan_bundle_exec.py`
- Modify: `src/hamilton_pipeline/modules/execution_plan.py`

### Deletions
- Remove incremental diff helpers once CDF path is mandatory (see deferred deletions).

### Checklist
- [ ] Enforce CDF provider path for delta datasets requiring incremental.
- [ ] Normalize CDF cursor persistence and window validation.
- [ ] Remove alternative diff-based incremental branches.

---

## 9. Delta Maintenance Control Plane

### Goal
Centralize optimize, vacuum, and checkpoint policies into a single Delta maintenance layer.

### Status
Planned

### Representative Pattern
```python
maintenance_plan = DeltaMaintenancePlan(
    retention_days=7,
    optimize_zorder=["repo_id", "task_name"],
    keep_versions=[pinned_version],
)
run_delta_maintenance(table_uri, maintenance_plan)
```

### Target Files
- Create: `src/datafusion_engine/delta_maintenance.py`
- Modify: `src/datafusion_engine/delta_control_plane.py`
- Modify: `src/datafusion_engine/delta_store_policy.py`
- Modify: `src/datafusion_engine/delta_observability.py`
- Modify: `src/datafusion_engine/write_pipeline.py`
- Modify: `src/incremental/delta_context.py`

### Deletions
- Remove ad-hoc maintenance calls once policy is centralized.

### Checklist
- [ ] Define maintenance plan schema and default policies.
- [ ] Wire maintenance execution into write and incremental paths.
- [ ] Capture maintenance artifacts and metrics.

---

## 10. Schema Governance via Info Schema + Hamilton Validators

### Goal
Unify schema validation by combining DataFusion info schema queries with Hamilton validator hooks.

### Status
Planned

### Representative Pattern
```python
info_schema = ctx.sql(
    "SELECT table_name, column_name, data_type FROM information_schema.columns"
)
expected = load_schema_contract("cpg_nodes")
validate_schema(info_schema, expected)
```

### Target Files
- Modify: `src/datafusion_engine/schema_registry.py`
- Modify: `src/datafusion_engine/schema_introspection.py`
- Modify: `src/datafusion_engine/schema_validation.py`
- Modify: `src/datafusion_engine/schema_contracts.py`
- Modify: `src/datafusion_engine/schema_policy.py`
- Modify: `src/datafusion_engine/schema_alignment.py`
- Modify: `src/serde_schema_registry.py`
- Modify: `src/hamilton_pipeline/validators.py`
- Modify: `src/hamilton_pipeline/modules/column_features.py`

### Deletions
- Remove per-module schema checks once registry-based validation is enforced.

### Checklist
- [ ] Add info-schema based introspection utilities.
- [ ] Consolidate schema contracts into a single registry format.
- [ ] Wire schema validation into Hamilton check_output hooks.

---

## 11. Deferred Decommissioning and Cleanup

### Goal
Remove legacy paths only after all scopes above are complete and validated.

### Status
Planned

### Representative Pattern
```python
# After migration, delete legacy tag dicts and diff utilities.
# This scope is for deletion only once new contracts are fully adopted.
```

### Target Files
- Modify: `src/hamilton_pipeline/modules/outputs.py`
- Modify: `src/hamilton_pipeline/modules/task_execution.py`
- Modify: `src/incremental/diff.py`
- Modify: `src/incremental/delta_updates.py`
- Modify: `src/incremental/changes.py`

### Deletions
- Delete legacy tag dictionaries and helpers once `TagPolicy` is enforced.
- Delete diff-based incremental utilities once CDF is mandatory.
- Remove any duplicate plan-debug utilities superseded by plan artifacts.

### Checklist
- [ ] Verify all scopes 1-10 are fully migrated.
- [ ] Remove deprecated tag dicts and per-module tag helpers.
- [ ] Remove legacy incremental diff modules.
- [ ] Re-run plan artifact and schema validation checks.

