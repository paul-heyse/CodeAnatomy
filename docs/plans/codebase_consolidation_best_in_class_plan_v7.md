# Codebase Consolidation Plan v7 (Best-in-Class, Breaking Changes OK)

## Executive Summary

This plan is a **revised, comprehensive consolidation roadmap** that integrates all follow-up improvements identified after v5. It assumes a **design-phase mandate** (breaking changes are expected) and drives the codebase toward a single, cohesive architecture anchored in DataFusion-native capabilities, Delta protocol features, and msgspec-first schema contracts.

Key themes:
- **Rust-first planning + policy** for deterministic optimizer behavior.
- **DDL-first external registration** for non-Delta sources (catalog-native, portable, observable).
- **Provider-level Delta scan pinning** (file set overrides, not re-registration loops).
- **Unified constraints + maintenance** across DataFusion and Delta protocol features.
- **Substrait-first plan portability** and msgspec JSON Schema registry.
- **SessionConfig-only SQL policy** and **COPY/INSERT standardization** for non-Delta writes.

---

## Design Principles

1. **Single source of truth** for catalog state, schema contracts, and SQL policy.
2. **Provider-first** integrations (Delta, listing tables, external tables) instead of ad-hoc IO paths.
3. **Rust policy surfaces** wherever execution determinism and plan stability matter.
4. **Schema governance via msgspec + DataFusion info schema** (no redundant schema systems).
5. **Observable plan + write artifacts** as a first-class product surface.

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
11. Deferred Decommissioning (after scopes 1-10 complete)

---

## 1. Physical Optimizer Rulepack (Rust SessionState hooks)

### Goal
Move physical plan policy and rewrites into Rust optimizer rules so plan topology and performance are deterministic and policy-driven without Python-side divergence.

### Representative Pattern
```rust
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use std::sync::Arc;

#[derive(Default)]
pub struct CodeAnatomyPhysicalRule;

impl PhysicalOptimizerRule for CodeAnatomyPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::execution::context::SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "codeanatomy_physical_rulepack"
    }
}
```

### Target Files
- Create: `rust/datafusion_ext/src/physical_rules.rs`
- Modify: `rust/datafusion_ext/src/lib.rs`
- Modify: `rust/datafusion_python/src/codeanatomy_ext.rs`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/plan_artifacts.py`

### Deletions
- Remove Python-side physical tuning branches that duplicate the Rust rulepack.

### Checklist
- [ ] Implement physical rulepack in Rust with explicit policy inputs.
- [ ] Register physical rules during SessionContext initialization.
- [ ] Record rulepack identity in plan artifacts.
- [ ] Remove redundant Python-side physical tuning logic.

---

## 2. DDL-First External Table Registration (Listing + Unbounded + ORDER)

### Goal
Replace bespoke non-Delta registration with canonical DataFusion DDL and catalog surfaces.

### Representative Pattern
```sql
CREATE UNBOUNDED EXTERNAL TABLE events (
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
- Modify: `src/datafusion_engine/sql_guard.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/catalog_provider.py`
- Modify: `src/datafusion_engine/table_provider_metadata.py`
- Modify: `src/hamilton_pipeline/modules/inputs.py`

### Deletions
- Remove listing-table registration helpers used only for non-Delta sources.
- Remove custom ordering/partition metadata builders for non-Delta sources.

### Checklist
- [ ] Add a DDL builder for external tables (schema, partition, order, options).
- [ ] Route all non-Delta registrations through DDL execution.
- [ ] Capture DDL text in plan/diagnostic artifacts.
- [ ] Ensure `information_schema` is enabled for introspection.

---

## 3. Delta Scan Overrides via Provider File Sets (with_files)

### Goal
Replace scan-unit re-registration and override logic with provider-level file set pinning.

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
- Modify: `src/datafusion_engine/scan_planner.py`
- Modify: `src/hamilton_pipeline/modules/task_execution.py`

### Deletions
- Remove scan override re-registration loops once file pinning is authoritative.

### Checklist
- [ ] Implement file-set pinning in delta provider resolution.
- [ ] Remove scan-unit re-registration and duplicate hash logic.
- [ ] Capture file-set metadata in plan artifacts.

---

## 4. Constraint Governance Unification (DataFusion constraints + Delta CHECK)

### Goal
Define constraints once and enforce them via Delta write-time checks while surfacing them through DataFusion information_schema.

### Representative Pattern
```python
constraints = {
    "primary_key": ["id"],
    "checks": ["amount >= 0", "status IN ('open', 'closed')"],
}
apply_delta_constraints(table_uri, constraints)
```

### Target Files
- Modify: `src/datafusion_engine/schema_contracts.py`
- Modify: `src/datafusion_engine/schema_validation.py`
- Modify: `src/datafusion_engine/write_pipeline.py`
- Modify: `src/datafusion_engine/delta_control_plane.py`
- Modify: `src/hamilton_pipeline/modules/outputs.py`

### Deletions
- Remove duplicate constraint validation paths not enforced in Delta or surfaced via info schema.

### Checklist
- [ ] Define a single constraint DSL as the contract surface.
- [ ] Emit Delta CHECK constraints during writes.
- [ ] Surface constraints via `information_schema` for introspection.

---

## 5. Delta Protocol Feature Adoption (DV, v2 checkpoints, log compaction)

### Goal
Centralize Delta protocol feature adoption and maintenance operations in a single control plane.

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
- Modify: `src/datafusion_engine/write_pipeline.py`
- Modify: `src/datafusion_engine/dataset_registry.py`

### Deletions
- Remove scattered maintenance toggles and ad-hoc cleanup helpers.

### Checklist
- [ ] Add a single Delta maintenance policy surface.
- [ ] Apply policy during initialization and after commits.
- [ ] Record feature adoption state in diagnostics.

---

## 6. Substrait-First Plan Portability (plan bundle + interchange)

### Goal
Standardize plan interchange on Substrait and treat it as the canonical portable plan artifact.

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
Use msgspec Structs and JSON Schema export as the authoritative registry for all artifacts and diagnostics.

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
- Modify: `schemas/` (generated JSON Schema)
- Modify: `tests/msgspec_contract/*`

### Deletions
- Remove hand-authored schema docs for artifact payloads once msgspec export is canonical.

### Checklist
- [ ] Define canonical msgspec Structs for all artifacts.
- [ ] Export JSON Schema 2020-12 for each contract.
- [ ] Replace manual schema docs with generated artifacts.

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
- Remove non-Delta registry snapshot artifacts and loaders.

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
- Remove Python policy resolution branches not mirrored in Rust.

### Checklist
- [ ] Route policy through SessionConfig extension options.
- [ ] Enforce policy via Rust analyzer rules only.
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
- [ ] Route non-Delta writes via COPY or INSERT.
- [ ] Capture COPY/INSERT SQL in diagnostics.
- [ ] Remove duplicate write helpers.

---

## 11. Deferred Decommissioning (after scopes 1-10 complete)

### Goal
Finalize deletions only after all new paths are stable and validated.

### Deletion Candidates
- Legacy non-Delta registration helpers in `dataset_registration.py`
- Python-side SQL policy fallback helpers
- Any remaining custom plan serialization artifacts (non-Substrait)
- Duplicate constraint validators not enforced via Delta CHECK
- Legacy non-Delta write helpers superseded by COPY/INSERT

### Checklist
- [ ] Confirm scopes 1-10 are complete.
- [ ] Validate no call sites remain.
- [ ] Delete deferred modules and update imports.

---

## Verification (Design-Phase)

- Tests: `uv run pytest tests/`
- Type checks: `uv run pyright --warnings` and `uv run pyrefly`
- Lint/format: `uv run ruff check .` and `uv run ruff format --check .`
