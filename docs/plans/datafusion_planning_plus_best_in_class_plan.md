# DataFusion Planning+ Best-in-Class Implementation Plan

Date: 2026-01-28
Owner: Codex (design phase)
Status: complete (all scoped items implemented; breaking changes applied)

## Goals
- Extend planning artifacts beyond the current plan to include rulepacks, config provenance, and proto/EXPLAIN-row artifacts.
- Make planning decisions stats-aware and explicitly reproducible across environments.
- Integrate Delta scan control, snapshot identity, and protocol compatibility into plan identity.
- Make UDFs “planner-aware” and observable (volatility, simplify, coercion, parameter names).
- Raise determinism by pinning every plan-shaping knob used by the runtime.

## Design principles
- **Deterministic first**: all plan-shaping inputs must be captured as artifacts and included in plan identity.
- **Plan artifacts are data contracts**: schema/fields are versioned; removals happen only after replacements are validated.
- **Planner/optimizer transparency**: rulepack + config provenance should be inspectable per plan.
- **Delta scans are first-class**: scan config and snapshot identity are part of plan identity.
- **UDFs are planning participants**: UDF metadata and planner hooks are exposed and verified.

---

## Scope 1: Planning config + runtime env provenance bundle

**Objective**
Capture all planning-relevant SessionConfig and RuntimeEnv settings as a structured artifact and include in plan identity.

**Status**: complete

**Representative code pattern**
```python
# src/datafusion_engine/plan_bundle.py

def _planning_env_snapshot(ctx: SessionContext, *, profile: DataFusionRuntimeProfile | None) -> dict[str, object]:
    config_snapshot = ctx.config().to_dict() if hasattr(ctx, "config") else {}
    runtime_snapshot = _runtime_env_snapshot(profile)
    return {
        "session_config": config_snapshot,
        "runtime_env": runtime_snapshot,
        "explain_controls": _explain_controls(ctx),
    }
```

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/schema_registry.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Add `planning_env_snapshot` and `planning_env_hash` to plan artifacts.
- [x] Include SessionConfig and RuntimeEnv knobs that affect planning/physical planning.
- [x] Include explain formatting controls and `target_partitions`-style knobs.
- [x] Add artifacts schema bump and identity hash inclusion.

---

## Scope 2: Rulepack + optimizer provenance capture

**Objective**
Capture analyzer/optimizer/physical optimizer rulepack ordering for each plan to enable deterministic diffs.

**Status**: complete

**Representative code pattern**
```python
# src/datafusion_engine/plan_bundle.py

def _rulepack_snapshot(ctx: SessionContext) -> dict[str, object]:
    return {
        "analyzer_rules": _list_analyzer_rules(ctx),
        "optimizer_rules": _list_optimizer_rules(ctx),
        "physical_optimizer_rules": _list_physical_rules(ctx),
    }
```

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/plan_artifact_store.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Add rulepack snapshot artifact (names + ordering).
- [x] Include rulepack hash in plan identity.
- [x] Provide a fallback for Python bindings without rulepack APIs.

---

## Scope 3: SQL EXPLAIN row artifacts + proto serialization

**Objective**
Store machine-friendly EXPLAIN row outputs and DataFusion proto plan bytes alongside Substrait.

**Status**: complete

**Representative code pattern**
```python
# src/datafusion_engine/plan_bundle.py

def _explain_rows(ctx: SessionContext, sql: str, *, verbose: bool) -> list[dict[str, object]]:
    prefix = "EXPLAIN VERBOSE" if verbose else "EXPLAIN FORMAT TREE"
    return ctx.sql(f"{prefix} {sql}").to_arrow_table().to_pylist()

# proto
proto = plan.to_proto() if hasattr(plan, "to_proto") else None
```

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/schema_registry.py`
- `tests/plan_golden/test_plan_artifacts.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Add EXPLAIN row payloads to artifacts (`explain_tree_rows`, `explain_verbose_rows`).
- [x] Add proto payloads for logical/optimized/physical plans where available.
- [x] Extend golden fixtures to include row payloads.

---

## Scope 4: Statistics-aware planning + scheduling

**Objective**
Capture plan statistics availability and use it in scheduling cost models.

**Status**: complete

**Representative code pattern**
```python
# src/datafusion_engine/plan_bundle.py
stats = execution_plan.statistics() if execution_plan is not None else None

# src/relspec/execution_plan.py
if metrics.stats_available:
    base_cost *= stats_weight
```

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/relspec/execution_plan.py`
- `src/hamilton_pipeline/scheduling_hooks.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Capture statistics availability (row counts, column stats when exposed).
- [x] Record a stats-provenance artifact (source, quality).
- [x] Incorporate stats availability into scheduling cost adjustments.
- [x] No direct changes in Hamilton scheduling hooks; they consume adjusted plan costs.

---

## Scope 5: Delta scan configuration + snapshot identity

**Objective**
Make Delta scan configuration and snapshot identity first-class plan inputs.

**Status**: complete

**Representative code pattern**
```python
# src/datafusion_engine/plan_bundle.py

def _delta_snapshot_identity(scan_unit: ScanUnit) -> dict[str, object]:
    return {
        "version": scan_unit.delta_version,
        "timestamp": scan_unit.delta_timestamp,
        "protocol": scan_unit.delta_protocol,
        "features": scan_unit.delta_features,
    }
```

**Target files to modify**
- `src/datafusion_engine/scan_planner.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/runtime.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Add Delta scan config snapshot to plan artifacts.
- [x] Add Delta snapshot identity hash to plan identity.
- [x] Capture whether FFI TableProvider vs Arrow Dataset fallback path is used.

---

## Scope 6: Delta protocol compatibility gate

**Objective**
Fail or downgrade planning when Delta protocol/features are not compatible with the runtime.

**Status**: complete

**Representative code pattern**
```python
# src/datafusion_engine/scan_planner.py
if not _is_protocol_compatible(delta_protocol, feature_gate):
    raise ValueError("Delta protocol features are incompatible with this runtime.")
```

**Target files to modify**
- `src/datafusion_engine/scan_planner.py`
- `src/datafusion_engine/runtime.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Validate reader/writer features prior to plan bundle creation.
- [x] Record compatibility outcome in plan artifacts.

---

## Scope 7: Schema diagnostics + DESCRIBE artifacts

**Objective**
Store schema diagnostics per view, including `DESCRIBE` output and schema provenance hints.

**Status**: complete

**Representative code pattern**
```python
# src/schema_spec/view_specs.py
rows = ctx.sql(f"DESCRIBE {view_name}").to_arrow_table().to_pylist()
```

**Target files to modify**
- `src/schema_spec/view_specs.py`
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/view_artifacts.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Capture DESCRIBE output as a plan or view artifact.
- [x] Add schema provenance notes (provider, source, inferred vs explicit).

---

## Scope 8: Provider schema provenance + DDL capture

**Objective**
Capture provider-level schema provenance and DDL when available.

**Status**: complete

**Representative code pattern**
```python
# src/datafusion_engine/schema_introspection.py
provider_ddl = table_provider.get_table_definition() if hasattr(table_provider, "get_table_definition") else None
```

**Target files to modify**
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/plan_bundle.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Capture `get_table_definition` where supported.
- [x] Add provenance fields to plan artifacts.

---

## Scope 9: UDF planner hooks + metadata audit

**Objective**
Make UDFs planner-aware and record their metadata/behavior in artifacts.

**Status**: complete

**Representative code pattern**
```rust
// Rust UDF (conceptual)
fn simplify(&self, args: &[Expr], info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> { ... }
```

**Target files to modify**
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/datafusion_engine/plan_bundle.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Capture UDF volatility + planner hooks in function catalog snapshot.
- [x] Add parameter name metadata where supported.
- [x] Add plan snapshot tests for UDF-heavy queries.

---

## Scope 10: Async UDF planning safeguards

**Objective**
Standardize async UDF batching/backpressure settings and capture them in plan artifacts.

**Status**: complete

**Representative code pattern**
```python
# src/datafusion_engine/runtime.py
DataFusionRuntimeProfile(async_udf_batch_size=128, async_udf_timeout_ms=5000)
```

**Target files to modify**
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/plan_bundle.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Expose async UDF settings as plan artifact fields.
- [x] Add test coverage for async UDF planning defaults.

---

## Scope 11: Plan determinism audit bundle

**Objective**
Create a single “determinism audit bundle” artifact used by CI and local diagnostics.

**Status**: complete

**Representative code pattern**
```python
# src/datafusion_engine/plan_bundle.py
bundle = {
    "plan_fingerprint": plan_fingerprint,
    "planning_env_hash": planning_env_hash,
    "rulepack_hash": rulepack_hash,
    "information_schema_hash": info_hash,
}
```

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `tests/plan_golden/test_plan_artifacts.py`

**Delete list**
- None.

**Implementation checklist**
- [x] Aggregate key hashes into a deterministic audit payload.
- [x] Add golden test coverage for the audit bundle.

---

## Scope 12: Deferred deletions (after all scopes complete)

**Objective**
Remove legacy or redundant paths once the new artifacts are fully validated.

**Status**: complete

**Removals applied**
- Dropped redundant `explain_*_json` artifact columns (replaced by row payloads + metrics).
- Bumped plan artifacts schema to v6 and documented the migration.
- Kept Substrait/proto fingerprinting paths as the canonical plan identity inputs.

**Target files to modify**
- `src/datafusion_engine/plan_bundle.py`
- `src/datafusion_engine/plan_artifact_store.py`
- `src/datafusion_engine/schema_registry.py`
- `src/incremental/registry_rows.py`
- `src/datafusion_engine/runtime.py`
- `tests/plan_golden/test_plan_artifacts.py`
- `docs/plans/datafusion_plan_artifacts_v6_migration_notes.md`

**Delete list**
- None (redundant fields removed as part of v6 migration).

**Implementation checklist**
- [x] Identify legacy plan capture paths after rollout.
- [x] Remove redundant fields and simplify artifacts.
- [x] Update tests and migration docs accordingly.

---

## Final design endpoint (planning+)
- Complete planning provenance: config, rulepack, explain rows, and plan proto bytes.
- Stats-aware scheduling informed by plan statistics availability.
- Delta scan configuration and snapshot identity fully captured in plan identity.
- UDFs treated as planner participants with metadata and simplification hooks.
- Deterministic audit bundle gating plan drift in CI and golden tests.
