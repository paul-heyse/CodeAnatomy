# DataFusion + Delta Best-in-Class Implementation Plan (Design-Phase, Breaking Changes OK)

## Purpose
Create a single, comprehensive implementation plan that captures the existing improvement scope and the corrections/additions required for a best-in-class target architecture. This plan assumes we are in the design phase and may accept breaking changes to reach the optimal end state.

## Design Principles (Non-Negotiable)
- **Delta-first** for all persisted datasets (extract → normalize → CPG) with explicit providers, pinned versions, and protocol gating.
- **Scan-boundary schema alignment** (casts, defaults, missing columns) so downstream views stay clean and pushdown-friendly.
- **Determinism by construction**: plan fingerprinting, Substrait-first execution, and Delta input pins must be reproducible and recorded.
- **Feature gating**: Delta protocol/table features are opt-in and validated against runtime capabilities.
- **Operational transparency**: plan artifacts, storage policy, maintenance events, and schema contracts are observable artifacts.
- **Breaking changes acceptable** if they increase correctness, determinism, or long-term performance.

---

## Scope 1 — Delta Write Policy v2 (layout, stats, zorder, features)

### Goal
Replace implicit and scattered Delta write behavior with a single declarative policy (layout + stats + features) that is enforced consistently.
Status: Complete

### Representative Code Patterns
```python
@dataclass(frozen=True)
class DeltaWritePolicy:
    target_file_size: int | None = DEFAULT_TARGET_FILE_SIZE
    partition_by: tuple[str, ...] = ()
    zorder_by: tuple[str, ...] = ()
    stats_policy: Literal["off", "explicit", "auto"] = "auto"
    stats_columns: tuple[str, ...] | None = None
    stats_max_columns: int = 32
    enable_features: tuple[DeltaFeatureName, ...] = ()
```

```python
resolved_stats = resolve_stats_columns(
    policy=write_policy,
    partition_by=partition_by,
    zorder_by=zorder_by,
    schema_columns=schema_cols,
    override=stats_override,
)
if resolved_stats:
    table_properties["delta.dataSkippingStatsColumns"] = ",".join(resolved_stats)
```

### Target Files to Modify
- `src/storage/deltalake/config.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/schema_spec/system.py`
- `src/storage/deltalake/delta.py`

### Modules to Delete
- None (breaking changes are behavioral, not file removal at this scope).

### Implementation Checklist
- [x] Add layout + stats + feature fields to `DeltaWritePolicy`.
- [x] Implement `resolve_stats_columns(...)` with schema filtering + caps.
- [x] Remove implicit default feature properties from writes; require opt-in.
- [x] Thread partition + zorder + stats into table properties consistently.
- [x] Add format options overrides (`zorder_by`, `stats_columns`, `enable_features`).

---

## Scope 2 — Schema Policy Enforcement (schema_mode + column mapping)

### Goal
Ensure `DeltaSchemaPolicy.schema_mode` is actually applied during writes and not silently ignored.
Status: Complete

### Representative Code Patterns
```python
def _delta_schema_mode(options: Mapping[str, object], *, policy: DeltaSchemaPolicy | None) -> str | None:
    if options.get("schema_mode"):
        return _normalize_schema_mode(options["schema_mode"])
    if policy is not None and policy.schema_mode is not None:
        return policy.schema_mode
    return None
```

### Target Files to Modify
- `src/datafusion_engine/write_pipeline.py`
- `src/storage/deltalake/config.py`
- `src/schema_spec/system.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Add `schema_mode` propagation from `DeltaSchemaPolicy` into write options.
- [x] Add tests in plan artifacts and/or write pipeline tests if present (tracked in Scope 13).
- [x] Ensure write policy + schema policy precedence is explicit and documented.

---

## Scope 3 — Scan-Boundary Schema Alignment (projection exprs as expressions)

### Goal
Allow projection expressions at registration to include casts/defaults and not only raw column names.
Status: Complete

### Representative Code Patterns
```python
# Accept SQL expressions, not just column names.
exprs = [sql_expr(expr, ctx) for expr in projection_exprs]
projected = df.select(*exprs)
```

### Target Files to Modify
- `src/datafusion_engine/registry_bridge.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_loader.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Extend `projection_exprs` semantics to accept SQL/Expr strings.
- [x] Validate expression names and handle aliasing explicitly.
- [x] Update registry artifacts to record resolved projection expressions.

---

## Scope 4 — Lineage-Driven Stats Policy

### Goal
Use plan lineage to compute stats columns based on actual filter/join keys, not only partition/zorder,
and persist the decision as a canonical msgspec artifact payload.
Status: Complete

### Representative Code Patterns
```python
hot_cols = sorted(required_columns | join_keys | filter_columns)
resolved_stats = resolve_stats_columns(
    policy=write_policy,
    partition_by=partition_by,
    zorder_by=zorder_by,
    schema_columns=schema_cols,
    override=stats_override,
)
```

```python
# src/serde_artifacts.py (new)
class DeltaStatsDecision(StructBaseCompat, frozen=True):
    dataset_name: str
    stats_policy: str
    stats_columns: tuple[str, ...] | None
    lineage_columns: tuple[str, ...]
```

### Target Files to Modify
- `src/datafusion_engine/lineage_datafusion.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `tests/msgspec_contract/test_contract_*.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Add lineage extraction of join keys + filter columns if missing.
- [x] Compute stats columns automatically per output table.
- [x] Persist stats decision as a msgspec artifact payload (not just commit metadata).
- [x] Add msgspec contract snapshots (JSON/MsgPack/schema/errors) for the stats decision payload.

---

## Scope 5 — Delta Feature Gating + Protocol Compatibility

### Goal
Make Delta feature enablement explicit, validated, and reported. Avoid implicit feature activation.
Status: Complete

### Representative Code Patterns
```python
if "change_data_feed" in spec.enable_features:
    enable_delta_change_data_feed(...)
```

```python
properties = _strip_protocol_feature_props(spec.table_properties)
set_properties(properties)
```

### Target Files to Modify
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/delta_protocol.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Remove `DEFAULT_DELTA_FEATURE_PROPERTIES` auto-apply.
- [x] Add explicit `enable_features` handling in write pipeline.
- [x] Validate feature gates against runtime protocol support.
- [x] Record protocol compatibility and feature enablement in artifacts.

---

## Scope 6 — Ordering Metadata + Cluster-on-Write

### Goal
Write clustering should be declared as ordering metadata to unlock optimizer benefits.
Status: Complete

### Representative Code Patterns
```python
ordering = ordering_metadata_spec(
    OrderingLevel.EXPLICIT,
    keys=(("file_id", "ascending"), ("bstart", "ascending")),
)
```

```python
if zorder_by:
    df = df.sort(*[SortExpr(col(c), asc=True) for c in zorder_cols])
```

### Target Files to Modify
- `src/schema_spec/system.py`
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/registry_bridge.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Extend schema metadata generation to include ordering keys.
- [x] Implement sort-on-write when `zorder_by` is specified.
- [x] Record ordering metadata in table artifacts.

---

## Scope 7 — CDF Provider Integration (Incremental First-Class)

### Goal
Use Delta CDF as a real TableProvider and wire into incremental processing paths.
Status: Complete

### Representative Code Patterns
```python
ctx.register_table("table_cdf", DeltaCdfTableProvider::try_new(builder)?)
```

```python
if location.delta_cdf_policy.required:
    register_delta_cdf(...)
```

### Target Files to Modify
- `src/datafusion_engine/registry_bridge.py`
- `src/incremental/runtime.py`
- `src/incremental/cdf_*.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Add CDF dataset registration in registry bridge.
- [x] Prefer CDF tables in incremental scan planning.
- [x] Record CDF consumption metadata per run.

---

## Scope 8 — View Graph Cache Boundaries

### Goal
Add explicit cache boundaries to view graph registration to avoid repeated upstream scans,
and emit cache artifacts as canonical msgspec payloads.
Status: Complete

### Representative Code Patterns
```python
@dataclass(frozen=True)
class ViewNode:
    cache_policy: Literal["none", "memory", "delta_staging"] = "none"
```

```python
if node.cache_policy == "memory":
    df = df.cache()
```

```python
# src/serde_artifacts.py (new)
class ViewCacheArtifact(StructBaseCompat, frozen=True):
    view_name: str
    cache_policy: str
    cache_path: str | None
    plan_fingerprint: str | None
    hit: bool | None
```

### Target Files to Modify
- `src/datafusion_engine/view_graph_registry.py`
- `src/datafusion_engine/view_registry_specs.py`
- `src/datafusion_engine/execution_facade.py`
- `src/obs/diagnostics.py`
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `tests/msgspec_contract/test_contract_*.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Add cache policy to `ViewNode`.
- [x] Implement cache materialization with temp view or Delta staging.
- [x] Emit cache artifacts as msgspec payloads (lineage/effectiveness included).
- [x] Add msgspec contract snapshots for cache artifacts.

---

## Scope 9 — UDF Metadata + Extension Types

### Goal
Adopt Arrow extension types and UDF metadata propagation for semantic schema enforcement,
with plan-time validation captured as msgspec artifacts.
Status: Complete

### Representative Code Patterns
```rust
fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
    let field = Field::new("node_id", DataType::Utf8, false)
        .with_metadata(meta_for("NodeId"));
    Ok(Arc::new(field))
}
```

```python
# src/serde_artifacts.py (new)
class SemanticColumnSpec(StructBaseCompat, frozen=True):
    column_name: str
    semantic_type: str
    metadata_key: str
```

### Target Files to Modify
- Rust UDF crate(s) (not in repo root; add explicit subpath once finalized)
- `src/datafusion_engine/udf_*.py`
- `src/datafusion_engine/schema_contracts.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `tests/msgspec_contract/test_contract_*.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Define extension types for NodeId/EdgeId/SpanId.
- [x] Implement `return_field_from_args` in core UDFs.
- [x] Validate extension metadata at plan time and during writes, emitting msgspec artifacts.
- [x] Add msgspec contract snapshots for semantic metadata validation errors.

---

## Scope 10 — Commit Metadata + Run Manifest

### Goal
Make run-level reproducibility explicit by linking commit metadata, plan artifacts, and Delta versions,
using canonical msgspec run-manifest artifacts.
Status: Complete

### Representative Code Patterns
```python
commit_metadata.update({
    "plan_fingerprint": bundle.plan_fingerprint,
    "run_id": run_id,
    "delta_inputs": json.dumps(delta_inputs),
})
```

```python
# src/serde_artifacts.py (new)
class RunManifest(StructBaseCompat, frozen=True):
    run_id: str
    plan_signature: str | None
    plan_fingerprints: dict[str, str]
    delta_inputs: tuple[dict[str, object], ...]
    outputs: tuple[dict[str, object], ...]
    artifact_ids: dict[str, str] | None
```

### Target Files to Modify
- `src/datafusion_engine/write_pipeline.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/serde_artifacts.py`
- `src/serde_schema_registry.py`
- `tests/msgspec_contract/test_contract_*.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Add `cpg_runs` (or `graph_snapshots`) Delta table (implemented as `run_manifest`).
- [x] Link input Delta pins and output versions to run manifest.
- [x] Emit a canonical msgspec run-manifest artifact and export schema via registry.
- [x] Include plan fingerprint + artifact ids in commit metadata and run-manifest payloads.

---

## Scope 11 — Parquet Writer Properties (bloom/dict)

### Goal
Ensure scan-time bloom/dict settings are backed by writer properties for new data,
with validation and snapshots driven by msgspec contracts.
Status: Complete

### Representative Code Patterns
```python
writer_props = WriterProperties.builder()\
    .set_dictionary_enabled(True)\
    .set_bloom_filter_enabled("node_id", True)\
    .build()
```

### Target Files to Modify
- `src/storage/deltalake/delta.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/write_pipeline.py`
- `tests/msgspec_contract/test_contract_*.py`

### Modules to Delete
- None.

### Implementation Checklist
- [x] Extend schema spec to capture writer properties for Delta writes.
- [x] Thread writer properties through `DeltaWriteOptions`.
- [x] Add msgspec contract snapshots for writer policy payloads and validation errors.
- [x] Add tests verifying bloom/dict settings are applied (tracked in Scope 13).

---

## Scope 12 — CPG Data Model Enhancements (accelerators)

### Goal
Add optional graph accelerators (property maps, adjacency tables) without abandoning relational model.
Status: Complete

### Representative Code Patterns
```sql
create view edges_by_src as
select src_node_id, collect_list(named_struct('dst', dst_node_id, 'kind', edge_kind)) as edges
from cpg_edges
group by 1
```

### Target Files to Modify
- `src/cpg/view_builders_df.py`
- `src/cpg/*` (new views/tables)
- `docs/architecture/*` (update contracts)

### Modules to Delete
- None.

### Implementation Checklist
- [x] Add materialized adjacency + property bundle views.
- [x] Include Z-order / stats policies for accelerator tables.
- [x] Ensure manifest links accelerator outputs to run.

---

## Scope 13 — Testing + Plan Golden Updates

### Goal
Update tests to reflect the new deterministic, policy-driven architecture and
the msgspec contract harness.
Status: Complete

### Representative Code Patterns
```python
bundle = build_plan_bundle(...)
assert bundle.plan_fingerprint == expected
```

### Target Files to Modify
- `tests/plan_golden/test_plan_artifacts.py`
- `tests/msgspec_contract/test_contract_*.py`
- `tests/*` (add coverage for Delta policies and commit metadata)

### Modules to Delete
- None.

### Implementation Checklist
- [x] Fix plan-golden to handle missing logical-extension codecs (record msgspec error payloads instead of hard failure).
- [x] Add msgspec contract snapshots for new artifact fields and policy hashes.
- [x] Add integration tests for feature gating and schema mode.
- [x] Add regression tests for scan-boundary projection expressions.

---

## Deferred Deletions (after all scopes complete)

These removals should only occur once all above scopes are implemented and validated end-to-end.
Status: Partial (remaining cleanup tracked below)

### Deletions
- [x] Legacy implicit Delta feature auto-enable logic in `src/storage/deltalake/delta.py`.
- [ ] Any Arrow record-batch registration paths used solely for plan serialization or caching (if they remain only for tests).
- [x] Deprecated or unused `delta_*` table property passthrough paths in write pipeline once policy-driven configuration is complete.
- [ ] Any remaining ad-hoc JSON payloads for plan/write artifacts once msgspec contracts are canonical.

### Checklist
- [x] Confirm CDF + Delta provider registration paths cover all runtime use cases.
- [ ] Confirm all plan artifacts and commit metadata are persisted for each run and validated by msgspec contract tests.
- [ ] Confirm no consumers depend on the removed paths or legacy artifact shapes.

---

## Notes on Breaking Changes
- Schema policy enforcement and feature gating are intentional breaking changes.
- Any dataset relying on implicit Delta feature defaults must set explicit policies.
- Projection expressions now accept SQL/Expr strings, which may require updating existing specs.
