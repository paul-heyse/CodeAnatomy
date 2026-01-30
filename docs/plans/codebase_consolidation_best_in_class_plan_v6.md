# Codebase Consolidation Plan v6 (Best-in-Class, Breaking Changes OK)

## Executive Summary

This plan extends v5 with additional consolidation opportunities to further reduce duplication, increase modularity, and align the codebase with DataFusion/Delta/msgspec capabilities. The v6 scope is explicitly design-phase and prefers a best-in-class target state even if it introduces breaking changes.

Key themes:
- Rust-first planning and physical optimization policy surfaces
- DDL-first catalog registration for non-Delta sources
- Delta provider-native scan overrides (file pinning)
- Constraint governance unified across DataFusion + Delta
- Delta protocol feature adoption via a single maintenance control plane
- Substrait-first plan portability
- msgspec schema registry as the canonical artifact contract
- SessionConfig policy as the single SQL policy source
- COPY/INSERT as the standard non-Delta write path

---

## Scope Index

1. Physical Optimizer Rulepack (Rust SessionState hooks)
2. DDL-First External Table Registration (Listing + Unbounded + ORDER)
3. Delta Scan Overrides via Provider File Sets (with_files)
4. Constraint Governance Unification (DataFusion constraints + Delta CHECK)
5. Delta Protocol Feature Adoption (DV, v2 checkpoints, log compaction)
6. Substrait-First Plan Portability (plan bundle + interchange)
7. msgspec Schema Registry as Canonical Artifact Contract
8. Catalog Autoload as Sole Registry for Non-Delta Sources
9. SQL Policy Single Source (SessionConfig + Rust policy rules)
10. COPY/INSERT Standardization for Non-Delta Writes
11. Deferred Decommissioning (only after scopes 1-10 complete)

---

## Status Summary (as of 2026-01-30)

1. **Physical Optimizer Rulepack** — **Partial**
   - Rust physical rule hook is installed and rulepack snapshots are captured.
   - Policy-driven physical rules and Python-side cleanup remain.
2. **DDL-First External Table Registration** — **Complete**
   - Non-Delta registration is DDL-first (DDL captured in provider artifacts).
3. **Delta Scan Overrides via Provider File Sets** — **Complete**
   - Scan unit pinning uses provider `with_files` and artifacts record file-set hashes.
4. **Constraint Governance Unification** — **Partial**
   - Delta CHECK constraints on write + information_schema fallback added.
   - Unified constraint DSL + duplicate validation cleanup remain.
5. **Delta Protocol Feature Adoption** — **Partial**
   - Policy surface + maintenance ops added; remaining integration/observability pending.
6. **Substrait-First Plan Portability** — **Partial**
   - Substrait is used for fingerprinting, but bundle still allows missing bytes.
7. **msgspec Schema Registry** — **Partial**
   - Registry exists; export automation + manual schema doc removal pending.
8. **Catalog Autoload for Non-Delta** — **Partial**
   - Registry catalogs are Delta-only; non-Delta registry snapshots removal pending.
9. **SQL Policy Single Source** — **Partial**
   - sql_guard/sql_options fallbacks removed; runtime policy resolution still uses Python fallbacks.
10. **COPY/INSERT Standardization** — **Partial**
   - COPY/INSERT paths added; legacy writers + diagnostics capture pending.
11. **Deferred Decommissioning** — **Not Started**

---

## 1. Physical Optimizer Rulepack (Rust SessionState hooks)

### Goal
Move physical plan policy and rewrites into Rust physical optimizer rules so plan topology is deterministic and policy-driven without Python-side divergence.

### Representative Pattern
```rust
use datafusion::execution::context::SessionContext;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use std::sync::Arc;

pub fn install_physical_rules(ctx: &SessionContext) {
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    state.add_physical_optimizer_rule(Arc::new(MyPhysicalRule::default()));
}
```

### Target Files
- Modify: `rust/datafusion_ext/src/lib.rs`
- Modify: `rust/datafusion_ext/src/planner_rules.rs`
- Create: `rust/datafusion_ext/src/physical_rules.rs`
- Modify: `rust/datafusion_python/src/codeanatomy_ext.rs`
- Modify: `src/datafusion_engine/runtime.py`

### Deletions
- Remove Python-side physical tuning code paths that duplicate rule behavior.

### Checklist
- [ ] Implement physical rulepack in Rust with explicit policy inputs.
- [x] Register physical rules during SessionContext initialization.
- [x] Record rulepack identity in plan artifacts.
- [ ] Remove redundant Python physical tuning branches.

---

## 2. DDL-First External Table Registration (Listing + Unbounded + ORDER)

### Goal
Replace ad-hoc registration for non-Delta sources with canonical DataFusion DDL and catalog surfaces.

### Representative Pattern
```sql
CREATE EXTERNAL TABLE events (
  user_id BIGINT NOT NULL,
  ts TIMESTAMP,
  event STRING
)
STORED AS PARQUET
LOCATION 's3://bucket/events'
PARTITIONED BY (dt)
WITH ORDER (ts)
```

### Target Files
- Modify: `src/datafusion_engine/dataset_registration.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/sql_guard.py`
- Modify: `src/datafusion_engine/catalog_provider.py`
- Modify: `src/hamilton_pipeline/modules/inputs.py`

### Deletions
- Remove bespoke listing-table registration helpers for non-Delta sources.
- Remove custom ordering/partition metadata builders for non-Delta sources.

### Checklist
- [x] Define a DDL builder for external tables with schema/partition/order options.
- [x] Route all non-Delta registrations through DDL.
- [x] Capture DDL text in plan/diagnostic artifacts.

---

## 3. Delta Scan Overrides via Provider File Sets (with_files)

### Goal
Replace scan-unit re-registration and override logic with provider-level file set pinning using DeltaTableProvider `with_files`.

### Representative Pattern
```python
resolution = resolve_dataset_provider(request)
provider = resolution.provider
provider = provider.with_files(selected_files)
ctx.register_table("delta_table", provider)
```

### Target Files
- Modify: `src/datafusion_engine/dataset_resolution.py`
- Modify: `src/datafusion_engine/delta_control_plane.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/hamilton_pipeline/modules/task_execution.py`

### Deletions
- Remove scan override re-registration paths once provider pinning is authoritative.

### Checklist
- [x] Implement file-set pinning in delta provider resolution.
- [x] Remove scan-unit re-registration loops and duplicate hashes.
- [x] Capture file-set metadata in plan artifacts.

---

## 4. Constraint Governance Unification (DataFusion constraints + Delta CHECK)

### Goal
Define constraints once and enforce them via Delta write-time checks while surfacing them through information_schema for introspection.

### Representative Pattern
```python
constraints = {
    "primary_key": ["id"],
    "checks": ["amount >= 0"],
}
write_delta_table(..., constraints=constraints)
```

### Target Files
- Modify: `src/datafusion_engine/schema_contracts.py`
- Modify: `src/datafusion_engine/schema_validation.py`
- Modify: `src/datafusion_engine/write_pipeline.py`
- Modify: `src/datafusion_engine/delta_control_plane.py`
- Modify: `src/hamilton_pipeline/modules/outputs.py`

### Deletions
- Remove duplicate constraint validation logic once Delta checks are enforced.

### Checklist
- [ ] Define constraint DSL as a single contract surface.
- [x] Emit Delta CHECK constraints on writes.
- [x] Populate information_schema constraints for introspection.

---

## 5. Delta Protocol Feature Adoption (DV, v2 checkpoints, log compaction)

### Goal
Centralize feature adoption and maintenance operations in a single Delta control plane.

### Representative Pattern
```python
DeltaMaintenancePolicy(
    enable_deletion_vectors=True,
    enable_v2_checkpoints=True,
    enable_log_compaction=True,
)
```

### Target Files
- Modify: `src/datafusion_engine/delta_control_plane.py`
- Modify: `src/datafusion_engine/delta_observability.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/write_pipeline.py`

### Deletions
- Remove scattered maintenance toggles and ad-hoc cleanup helpers.

### Checklist
- [x] Add a single Delta maintenance policy surface.
- [ ] Apply policy during table initialization and commits.
- [ ] Record feature adoption state in diagnostics.

---

## 6. Substrait-First Plan Portability (plan bundle + interchange)

### Goal
Standardize plan interchange on Substrait and treat it as the portable plan artifact.

### Representative Pattern
```python
bundle = build_plan_bundle(...)
substrait_bytes = bundle.substrait_bytes
store_artifact("substrait_plan", substrait_bytes)
```

### Target Files
- Modify: `src/datafusion_engine/plan_bundle.py`
- Modify: `src/datafusion_engine/plan_artifact_store.py`
- Modify: `src/datafusion_engine/runtime.py`

### Deletions
- Remove custom plan serialization branches that duplicate Substrait.

### Checklist
- [ ] Make Substrait bytes mandatory in plan bundles.
- [ ] Persist Substrait as the canonical portable plan artifact.
- [ ] Use Substrait for plan diffing and portability checks.

---

## 7. msgspec Schema Registry as Canonical Artifact Contract

### Goal
Use msgspec Structs + JSON Schema export as the authoritative schema registry for artifacts and diagnostics.

### Representative Pattern
```python
class PlanArtifact(msgspec.Struct, frozen=True):
    plan_identity_hash: str
    information_schema_hash: str

schema = msgspec.json.schema(PlanArtifact)
```

### Target Files
- Modify: `src/serde_artifacts.py`
- Modify: `src/serde_msgspec.py`
- Modify: `tests/msgspec_contract/*`
- Modify: `schemas/` (generated JSON Schema)

### Deletions
- Remove hand-authored schema docs for artifact payloads once msgspec export is canonical.

### Checklist
- [x] Define canonical msgspec Structs for all artifacts.
- [ ] Export JSON Schema 2020-12 for each contract.
- [ ] Replace manual schema docs with generated schema artifacts.

---

## 8. Catalog Autoload as Sole Registry for Non-Delta Sources

### Goal
Use DataFusion catalog autoloading for non-Delta sources and eliminate registry snapshots for those datasets.

### Representative Pattern
```python
config = SessionConfig()
config = config.set("datafusion.catalog.location", "s3://bucket/registry")
config = config.set("datafusion.catalog.format", "parquet")
```

### Target Files
- Modify: `src/datafusion_engine/session_factory.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/catalog_provider.py`

### Deletions
- Remove non-Delta registry snapshot tables and loaders.

### Checklist
- [ ] Route non-Delta registration through catalog autoload.
- [ ] Remove registry snapshot artifacts for non-Delta sources.

---

## 9. SQL Policy Single Source (SessionConfig + Rust policy rules)

### Goal
Make SessionConfig + Rust policy rules the only SQL policy source and eliminate Python-side policy fallback paths.

### Representative Pattern
```python
config = SessionConfig().set("codeanatomy_policy.allow_ddl", "false")
```

### Target Files
- Modify: `src/datafusion_engine/sql_options.py`
- Modify: `src/datafusion_engine/sql_guard.py`
- Modify: `src/datafusion_engine/runtime.py`

### Deletions
- Remove Python policy resolution branches that are not mirrored in Rust.

### Checklist
- [ ] Route policy through SessionConfig extension options.
- [ ] Enforce via Rust analyzer rules only.
- [ ] Remove Python policy fallbacks.

---

## 10. COPY/INSERT Standardization for Non-Delta Writes

### Goal
Standardize non-Delta materialization on DataFusion COPY/INSERT so writes are consistent and observable.

### Representative Pattern
```sql
COPY (SELECT * FROM staging) TO 's3://bucket/out/' STORED AS PARQUET
```

### Target Files
- Modify: `src/datafusion_engine/write_pipeline.py`
- Modify: `src/hamilton_pipeline/materializers.py`
- Modify: `src/hamilton_pipeline/modules/outputs.py`

### Deletions
- Remove bespoke Arrow->filesystem writers for non-Delta outputs.

### Checklist
- [x] Route non-Delta writes via COPY or INSERT.
- [ ] Capture COPY/INSERT artifacts in diagnostics.
- [ ] Remove duplicate write helpers.

---

## 11. Deferred Decommissioning (only after scopes 1-10 complete)

### Goal
Finalize deletions only after all new paths are stable and validated.

### Deletion Candidates
- Legacy non-Delta registration helpers in `dataset_registration.py`
- Python-side SQL policy fallback helpers
- Any remaining custom plan serialization artifacts (non-Substrait)
- Any duplicate constraint validators not backed by Delta CHECK

### Checklist
- [ ] Confirm scopes 1-10 complete.
- [ ] Validate no call sites remain.
- [ ] Delete deferred modules and update imports.

---

## Verification (Design-Phase)

- Run tests: `uv run pytest tests/`
- Run type checks: `uv run pyright --warnings` and `uv run pyrefly`
- Run lint/format: `uv run ruff check .` and `uv run ruff format --check .`
