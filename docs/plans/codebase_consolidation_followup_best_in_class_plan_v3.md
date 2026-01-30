# Codebase Consolidation Follow‑up Plan v3 (Design‑Phase, Breaking Changes OK)

## Executive Summary

This plan builds directly on **Codebase Consolidation Plan v2** and targets a **best‑in‑class, modular, extensible** end‑state with aggressive breaking changes allowed. The new scopes focus on:

- **Native DataFusion catalog & planning surfaces** as the canonical boundary.
- **Delta Lake providers** as the authoritative data interface for read/write/CDF.
- **Single‑path configuration and scan resolution** (no parallel pipelines).
- **Rust‑first policy and UDF execution surfaces** for deterministic planning.
- **Canonical metadata and identity surfaces** across registry, plans, and artifacts.
- **Status:** All scopes complete; plan represents the current consolidated state.

Design‑phase rules are assumed:
- **No backward compatibility.**
- **All API shapes are mutable.**
- **No stable fingerprints required.**

---

## Design‑Phase Principles (Non‑Negotiable)

1. **Single source of truth**: DataFusion catalog + Delta protocol metadata define the planning universe.
2. **Minimal surface area**: one canonical path for each behavior (session creation, scan resolution, registry wiring, plan capture, write‑back).
3. **Rust‑first planning control**: analyzer/optimizer hooks define policy; Python only orchestrates.
4. **Delta provider dominance**: DeltaTableProvider is the IO boundary; no duplicated provider adapters.
5. **Identity semantics explicit**: rename/standardize all “fingerprint” to “identity hash” when it is derived.

---

## Scope 1 — Delta Scan Configuration Service (Single Canonical Pipeline)

### Goal
Replace scattered scan config snapshot/hashing logic with a single, canonical service that produces:
- a **normalized scan config**
- a **stable schema payload**
- a **scan identity hash**
- a **Rust‑compatible payload**

### Representative Pattern
```python
@dataclass(frozen=True)
class DeltaScanConfigSnapshot:
    options: Mapping[str, object]
    schema_payload: Mapping[str, object] | None


def resolve_delta_scan_config(
    location: DatasetLocation,
) -> DeltaScanConfigSnapshot | None:
    options = build_delta_scan_config(location)
    if options is None:
        return None
    return DeltaScanConfigSnapshot(
        options=delta_scan_config_payload(options),
        schema_payload=delta_scan_schema_payload(options),
    )


def delta_scan_identity_hash(snapshot: DeltaScanConfigSnapshot | None) -> str | None:
    if snapshot is None:
        return None
    payload = {
        "options": snapshot.options,
        "schema": snapshot.schema_payload,
    }
    return hash_msgpack_canonical(payload)
```

### Target Files
- Modify: `src/datafusion_engine/dataset_registry.py`
- Modify: `src/datafusion_engine/scan_planner.py`
- Modify: `src/datafusion_engine/scan_overrides.py`
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/catalog_provider.py`
- Modify: `src/datafusion_engine/plan_artifact_store.py`
- Modify: `src/datafusion_engine/plan_bundle.py`
- Create: `src/datafusion_engine/delta_scan_config.py`

### Deletions
- Remove ad‑hoc helpers: `delta_scan_config_snapshot_*` and duplicate hash helpers in
  `dataset_registry.py`, `scan_planner.py`, `scan_overrides.py`, `registry_bridge.py`.

### Status
Complete. Canonical helpers live in `delta_scan_config.py`; call sites have been rewired.

### Checklist
- [x] Create `delta_scan_config.py` with canonical payload + hash.
- [x] Replace all call sites with service functions.
- [x] Remove duplicated scan payload builders and hashers.

---

## Scope 2 — SessionFactory as the Sole SessionContext Entry Point

### Goal
Eliminate parallel SessionContext creation paths and enforce `SessionFactory` as the only entrypoint.

### Representative Pattern
```python
class SessionFactory:
    def __init__(self, profile: DataFusionRuntimeProfile) -> None:
        self._profile = profile

    def build(self) -> SessionContext:
        config = self.build_config()
        runtime = self.build_runtime_env()
        return SessionContext(config, runtime)
```

### Target Files
- Modify: `src/engine/session_factory.py` (delegate to datafusion_engine SessionFactory)
- Modify: `src/engine/runtime.py`
- Modify: `src/datafusion_engine/runtime.py`

### Deletions
- Delete legacy session factory paths in `engine/session_factory.py` if duplicated.

### Status
Complete. SessionContext creation now flows exclusively through
`datafusion_engine.session_factory.SessionFactory` and downstream helpers consume the
profile runtime instead of constructing ad-hoc contexts.

### Checklist
- [x] Route all `SessionContext` creation through `SessionFactory`.
- [x] Remove redundant config assembly paths.

---

## Scope 3 — Native DataFusion Catalog Wiring (Registry → CatalogProvider)

### Goal
Replace registry snapshots + manual table wiring with native DataFusion catalog/schema providers.

### Representative Pattern
```python
catalog = Catalog.memory_catalog()
schema = Schema.memory_schema()
for name, provider in providers.items():
    schema.register_table(name, provider)

catalog.register_schema("public", schema)
ctx.register_catalog_provider("datafusion", catalog)
```

### Target Files
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/dataset_registry.py`
- Modify: `src/datafusion_engine/catalog_provider.py`
- Modify: `src/datafusion_engine/schema_registry.py`
- Modify: `src/datafusion_engine/runtime.py`

### Deletions
- Remove custom registry snapshot tables that duplicate DataFusion’s catalog state.

### Status
Complete. Catalog wiring registers registry-backed providers once, and metadata surfaces now
source exclusively from information_schema without duplicate registry snapshots.

### Checklist
- [x] Build catalog providers from registry once.
- [x] Remove duplicate in‑Python registry “schema tables”.
- [x] Ensure plan capture references `information_schema.*`.

---

## Scope 4 — Plan Artifact Consolidation (EXPLAIN‑First)

### Goal
Standardize plan artifacts around DataFusion’s canonical surfaces:
- `logical_plan`, `optimized_logical_plan`, `execution_plan`
- `EXPLAIN VERBOSE` and `EXPLAIN ANALYZE`

### Representative Pattern
```python
plan_bundle = {
    "logical": df.logical_plan().to_variant(),
    "optimized": df.optimized_logical_plan().to_variant(),
    "physical": df.execution_plan().to_variant(),
    "explain_verbose": df.explain(verbose=True, analyze=False),
}
```

### Target Files
- Modify: `src/datafusion_engine/plan_bundle.py`
- Modify: `src/datafusion_engine/plan_artifact_store.py`
- Modify: `src/datafusion_engine/runtime.py`

### Deletions
- Remove custom plan serialization branches not based on DataFusion’s native plan surfaces.

### Status
Complete. `plan_bundle.py` uses DataFusion logical/optimized/execution plans plus explain surfaces,
and plan artifact storage consumes those canonical payloads without legacy plan serialization.

### Checklist
- [x] Use DataFusion plan APIs as canonical plan artifacts.
- [x] Remove secondary/legacy plan payloads.

---

## Scope 5 — Delta Write Path Unification (DeltaTableProvider Insert)

### Goal
Use DataFusion `INSERT` into DeltaTableProvider for append/overwrite to remove dual write paths.

### Representative Pattern
```sql
INSERT INTO delta_table
SELECT * FROM staging
```

### Target Files
- Modify: `src/engine/materialize_pipeline.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/plan_artifact_store.py`
- Modify: `src/datafusion_engine/delta_control_plane.py`

### Deletions
- Remove bespoke DataFusion→Arrow→Delta write helpers when provider supports insert.

### Status
Complete. Delta writes now flow through provider-backed INSERT paths via `WritePipeline`, and
legacy `write_delta_table` helpers have been removed.

### Checklist
- [x] Wire write paths to Delta provider `insert_into`.
- [x] Deprecate bespoke Delta write adapters.

---

## Scope 6 — Delta CDF Provider Adoption

### Goal
Use `DeltaCdfTableProvider` as the sole CDF ingestion path.

### Representative Pattern
```python
cdf_provider = DeltaCdfTableProvider::try_new(builder)
ctx.register_table("cdf", cdf_provider)
```

### Target Files
- Modify: `src/datafusion_engine/delta_control_plane.py`
- Modify: `src/datafusion_engine/scan_overrides.py`
- Modify: `src/datafusion_engine/plan_artifact_store.py`

### Deletions
- Remove custom CDF scan materialization logic once provider path is complete.

### Status
Complete. CDF ingestion uses the Delta CDF provider end‑to‑end and bespoke scan paths have been
retired in favor of provider metadata.

### Checklist
- [x] Register CDF as a provider table.
- [x] Remove bespoke CDF scan/query logic.

---

## Scope 7 — Protocol Gate Enforcement at the Provider Boundary

### Goal
Move protocol gating to provider resolution and eliminate duplicated feature‑gate payloads.

### Representative Pattern
```python
provider = delta_provider_for(location)
validate_protocol_gate(snapshot_msgpack, gate_msgpack)
```

### Target Files
- Modify: `src/datafusion_engine/delta_control_plane.py`
- Modify: `src/datafusion_engine/delta_protocol.py`
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/scan_planner.py`

### Deletions
- Delete duplicate gate payload builders in registry/scan paths.

### Status
Complete. Protocol gate validation now occurs at provider resolution and downstream payloads no
longer duplicate feature‑gate snapshots.

### Checklist
- [x] Ensure gate validation happens once during provider resolution.
- [x] Remove redundant gate payload generation elsewhere.

---

## Scope 8 — Storage Options: DeltaTable as Single Source of Truth

### Goal
Eliminate duplicated object‑store wiring and treat `DeltaTable(storage_options=...)` as canonical.

### Representative Pattern
```python
DeltaTable(path, storage_options=storage_options)
ctx.register_table("table", delta_table)
```

### Target Files
- Modify: `src/datafusion_engine/catalog_provider.py`
- Modify: `src/datafusion_engine/dataset_registry.py`
- Modify: `src/datafusion_engine/runtime.py`

### Deletions
- Remove DataFusion‑side storage option normalization once DeltaTable is the only seam.

### Status
Complete. Delta storage options now flow exclusively through DeltaTable configuration and
Python-side normalization/hashing has been removed.

### Checklist
- [x] Route all storage options through DeltaTable.
- [x] Remove duplicated object store config paths.

---

## Scope 9 — Information Schema as the Canonical Metadata Surface

### Goal
Remove custom registry metadata tables and rely on `information_schema` for introspection.

### Representative Pattern
```sql
SELECT * FROM information_schema.tables;
SELECT * FROM information_schema.columns;
```

### Target Files
- Modify: `src/datafusion_engine/schema_registry.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/plan_artifact_store.py`

### Deletions
- Delete registry snapshot tables that duplicate information_schema.

### Status
Complete. Registry metadata introspection now sources information_schema end‑to‑end
(via SchemaIntrospector + plan artifacts), and no custom registry metadata tables are
queried by runtime or artifact capture paths.

### Checklist
- [x] Replace registry metadata queries with information_schema queries.
- [x] Remove duplicated schema tables.

---

## Scope 10 — Planning Policy Enforcement in Rust (Analyzer/Optimizer Hooks)

### Goal
Move policy enforcement (join policies, plan guards, etc.) into Rust planning hooks.

### Representative Pattern
```rust
ctx.add_optimizer_rule(Arc::new(MyPolicyRule::new()));
```

### Target Files
- Modify: `rust/datafusion_ext/src/lib.rs`
- Modify: `rust/datafusion_ext/src/planner_rules.rs` (new)
- Modify: `rust/datafusion_python/src/codeanatomy_ext.rs`
- Modify: `src/datafusion_engine/runtime.py`

### Deletions
- Remove Python‑side policy enforcement once Rust rules are authoritative.

### Status
Complete. Rust planner policy rules are installed during SessionContext initialization
and the runtime now treats policy as native configuration (no parallel Python enforcement).

### Checklist
- [x] Implement Rust analyzer/optimizer rules for policy.
- [x] Register rules in SessionContext initialization.
- [x] Remove Python policy logic.

---

## Scope 11 — Rust UDF Platform as the Single UDF Surface

### Goal
Consolidate all UDF registration under the Rust UDF platform and remove Python UDF duplication.

### Representative Pattern
```rust
let udf = ScalarUDF::new_from_impl(Arc::new(MyUdfImpl::new()));
ctx.register_udf(udf);
```

### Target Files
- Modify: `rust/datafusion_ext/src/udf_platform.rs`
- Modify: `rust/datafusion_python/src/codeanatomy_ext.rs`
- Modify: `src/datafusion_engine/udf_platform.py`

### Deletions
- Remove Python‑side scattered UDF registration and policy layers.

### Status
Complete. Rust UDF platform is the sole registration surface; production call sites now
consume `rust_udf_snapshot` after platform installation, and no direct `register_rust_udfs`
usage remains outside the platform wrapper.

### Checklist
- [x] Ensure all UDFs registered in Rust.
- [x] Remove parallel Python UDF registration surfaces.

---

## Scope 12 — Rename “schema_fingerprint” → “schema_identity_hash”

### Goal
Normalize terminology now that identity hashes are canonical.

### Representative Pattern
```python
schema_identity_hash = schema_identity_hash(schema)
```

### Target Files
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/plan_bundle.py`
- Modify: `src/datafusion_engine/schema_registry.py`
- Modify: `src/hamilton_pipeline/modules/*`
- Modify: `src/incremental/*`
- Modify: `src/relspec/*`

### Deletions
- Remove old “schema_fingerprint” fields from artifacts and tables.

### Status
Complete. All `schema_fingerprint` references were renamed across `src/` to
`schema_identity_hash`.

### Checklist
- [x] Rename fields in artifacts, schema tables, and metadata payloads.
- [x] Update downstream readers accordingly.

---

## Scope 13 — Dataset Resolution Pipeline (Registry + Overrides + Provider)

### Goal
Merge registry bridge, scan overrides, and provider creation into a single pipeline module.

### Representative Pattern
```python
@dataclass(frozen=True)
class DatasetResolution:
    provider: object
    delta_scan: DeltaScanConfigSnapshot | None
    identity_hash: str | None


def resolve_dataset(location: DatasetLocation) -> DatasetResolution:
    scan = resolve_delta_scan_config(location)
    provider = build_provider(location, scan)
    return DatasetResolution(
        provider=provider,
        delta_scan=scan,
        identity_hash=delta_scan_identity_hash(scan),
    )
```

### Target Files
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/scan_overrides.py`
- Modify: `src/datafusion_engine/dataset_registry.py`
- Create: `src/datafusion_engine/dataset_resolution.py`

### Deletions
- Remove registry/scan override paths once `dataset_resolution.py` is authoritative.

### Status
Complete. `dataset_resolution.py` now owns dataset → provider resolution, and legacy
registry/scan override modules have been removed from the codebase.

### Checklist
- [x] Implement `dataset_resolution.py`.
- [x] Rewire all dataset → provider flows.
- [x] Remove duplicate resolution helpers.

---

## Scope 14 — Deferred Deletes (Only After All Scopes Complete)

These modules should **not** be deleted until every scope above is complete and validated.

### Deletion Candidates
- `src/datafusion_engine/scan_overrides.py`
- `src/datafusion_engine/registry_bridge.py` (split/absorbed functionality)
- Legacy registry snapshot tables and payload builders in `schema_registry.py`
- Any remaining custom plan artifact serializers not based on DataFusion plan surfaces
- Any Python‑side UDF registration modules once Rust UDF platform fully covers all UDFs

### Status
Complete. Deferred modules were removed once scopes 1‑13 stabilized; no call sites remain.

### Checklist
- [x] Confirm all scopes 1‑13 are complete.
- [x] Validate no call sites remain for deferred modules.
- [x] Delete deferred modules and update imports.

---

## Suggested Implementation Order (Best‑in‑Class Sequence)

1. Delta Scan Config Service (Scope 1)
2. Dataset Resolution Pipeline (Scope 13)
3. SessionFactory as sole entrypoint (Scope 2)
4. Native Catalog Wiring (Scope 3)
5. Information Schema canonicalization (Scope 9)
6. Plan Artifact Consolidation (Scope 4)
7. Delta Provider Gate Enforcement (Scope 7)
8. Delta Write Path Unification (Scope 5)
9. Delta CDF Provider Adoption (Scope 6)
10. Storage Options single source (Scope 8)
11. Rust Planning Policy Hooks (Scope 10)
12. Rust UDF Platform Unification (Scope 11)
13. Rename schema_fingerprint fields (Scope 12)
14. Deferred Deletes (Scope 14)

---

## Verification (Design‑Phase)

- Run tests: `uv run pytest tests/`
- Run type checks: `uv run pyright --warnings` and `uv run pyrefly`
- Run linting: `uv run ruff check`
- Ensure **only one** dataset resolution path exists.
- Ensure **only one** write path exists for Delta.
- Ensure **only one** UDF registration surface exists (Rust).
