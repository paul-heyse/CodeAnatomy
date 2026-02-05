# Legacy/Dead-Code Decommission Assessment (src/)

Date: 2026-02-05
Scope: `src/` only (per directive). CQ-first analysis; dynamic access treated as in-use unless proven otherwise.

## Summary
We ran a CQ-first sweep across `src/` for explicit legacy/deprecation markers, compatibility/fallback logic, dynamic access paths, and dependency health signals. The review surfaced:

- **One clear deprecation stub with no `src/` references** (`datafusion_engine/views/__init__.py`), safe to remove.
- **Legacy schema adapters actively used** in semantic outputs (`semantics/adapters.py` + projection wrappers). These are not dead, but are compatibility layers and should be removed after downstream schema migration.
- **Compatibility fallbacks** in plan cache keys, scan telemetry payloads, and semantic input name resolution.
- **Versioned dataset naming + migrations** (`_v1`/`_v2`) that are still operational and tied to migration registry; removing these is a deliberate compatibility break.
- **Dynamic access / registry hotspots** (`__getattr__`, `importlib`) that keep code reachable even when static callers are absent.

This report prioritizes aggressive decommission candidates aligned to the msgspec/pydantic/pandera consolidation and DataFusion/Delta-first architecture.

## Architecture Alignment Checklist (for decommission decisions)
Use this checklist to decide removal vs migration:

- Data-only specs: favor msgspec `Struct` models over ad-hoc dataclasses/dicts.
- Centralized validation: enforce DataFrame schema validation via pandera bridge (no scattered validators).
- Drop legacy schema adapters: remove outputs that project into legacy relationship schemas.
- DataFusion is source of truth for schema and planning; avoid fallback pipelines outside DataFusion.
- Delta is table provider; avoid optional backcompat logic unless required by current public API.

## Evidence-Backed Decommission Candidates

### 1) Immediate Deletions (no `src/` usage)

**A. Deprecated export stub**
- `src/datafusion_engine/views/__init__.py` defines `_REMOVED_EXPORTS` and `__getattr__` that raises DeprecationWarning.
- CQ evidence: the removed export names only appear as string literals in this module; no `src/` references.
  - `VIEW_SELECT_REGISTRY`, `ViewExprBuilder`, `SpanExprs`, `DslQueryBuilder` (string matches only).
- **Recommendation:** remove the entire deprecation shim (`_REMOVED_EXPORTS` + `__getattr__`) and keep the module empty (or delete file if no other exports are needed).

**B. Unused migration functions (no call sites in `src/`)**
- `src/semantics/migrations.py`
  - `migrate_cpg_nodes_v1_to_v2(ctx)` — CQ calls found **0** call sites.
  - `migrate_cpg_edges_v1_to_v2(ctx)` — CQ calls found **0** call sites.
- These are only registered in `MIGRATIONS` and not referenced directly in `src/`.
- **Recommendation:** remove these migration functions (and their registration) if we are decommissioning v1/v2 migration paths. See “Versioned Naming & Migration Framework” below for the broader compatibility tradeoff.

### 2) Legacy/Compatibility Adapters (used, but obsolete under current architecture)

**A. Legacy semantic relationship projection**
- `src/semantics/adapters.py` provides `legacy_relationship_projection`, `legacy_relationship_projection_extended`, and `project_semantic_to_legacy` for legacy schema alignment.
- CQ calls for `project_semantic_to_legacy`: **5 call sites** across `src/semantics/catalog/projections.py` and `src/semantics/pipeline.py`.
- **Recommendation:** decommission once downstream consumers accept the minimal semantic schema (no legacy rel_name_symbol_v1 projection).
- **Required migration:** update any outputs or contracts expecting legacy schema to use the new minimal schema or a msgspec contract-driven schema.

**B. Legacy projection wrappers**
- `src/semantics/catalog/projections.py` and `src/semantics/pipeline.py` wrap builders to project legacy schema (via `project_semantic_to_legacy`).
- **Recommendation:** remove these wrappers and emit canonical semantic outputs only.

### 3) Compatibility Fallbacks (not dead; removal is a break)

**A. Plan cache legacy key fallback**
- `src/datafusion_engine/plan/cache.py`:
  - `PlanCacheKey.legacy_key()` used only inside `PlanCache.get()` and `PlanCache.contains()`.
- **Recommendation:** remove legacy key fallback and require only composite fingerprints.
- **Required migration:** drop old cache entries / flush disk cache.

**B. Scan telemetry payload fallback**
- `src/obs/metrics.py`:
  - `_scan_payload_bytes(..., fallback=...)` encodes legacy JSON fields into msgpack.
- **Recommendation:** remove fallback keys (`*_json`) and keep only msgpack fields.
- **Required migration:** update telemetry producers to always emit msgpack fields.

**C. Semantic input name fallback**
- `src/semantics/input_registry.py`:
  - `_fallback_names()` uses `_v1` ↔ unversioned fallbacks.
- **Recommendation:** remove fallback lookup and require canonical names only.
- **Required migration:** ensure all inputs are registered under canonical names.

### 4) Versioned Naming & Migration Framework (compatibility layers)

**Observed:** extensive `_v1` / `_v2` naming in semantic outputs and migrations (CQ `_v1` and `_v2` scans show heavy usage). The design currently uses version suffixes to enforce schema evolution.

- `src/semantics/naming.py` establishes `_v1` suffix as canonical output mapping.
- `src/semantics/migrations.py` maintains migration registry and helpers.
- `src/semantics/ir_pipeline.py` validates migration readiness for versioned datasets.

**Recommendation (aggressive decommission):**
- If the architecture requires a single canonical naming layer with no version aliases, remove version suffix mapping and the migration registry. This is a breaking change and must be coordinated with the schema contract system.

### 5) Dynamic Access & Registry Hotspots (risk of false “dead”)

CQ shows multiple dynamic access points (`__getattr__`, `importlib`) that can keep code in use without direct static call sites:

- Lazy export modules: `src/datafusion_engine/__init__.py`, `src/schema_spec/__init__.py`, `src/storage/__init__.py`, `src/storage/deltalake/__init__.py`, `src/obs/__init__.py`, `src/hamilton_pipeline/__init__.py`, etc.
- `importlib` usage is widespread (DataFusion/Delta plugin discovery, runtime config loaders, optional dependencies).

**Recommendation:** treat these as in‑use until module-level registries are consolidated. Do not delete without explicitly removing registry entries or dynamic export maps.

### 6) DataFusion/Delta Alignment Checks (possible future decommissions)

**A. PyArrow dataset usage**
- `src/storage/dataset_sources.py` uses `pyarrow.dataset` for scanning and wrappers (`OneShotDataset`, `union_dataset`).
- These may be legacy if the target architecture requires DataFusion-only scan paths.
- **Recommendation:** evaluate if dataset scans can be fully replaced by DataFusion providers; if yes, decommission this module.

**B. Delta QueryBuilder**
- `src/storage/deltalake/query_builder.py` uses delta‑rs `QueryBuilder` (optional dependency).
- **Recommendation:** decommission if DataFusion SQL execution is the only supported query surface.

## Decommission Order (Suggested)
1) **Remove pure deprecation stubs** with no `src/` references (`datafusion_engine/views/__init__.py`).
2) **Remove legacy schema projections** (semantic adapters and wrappers) after migrating consumers to canonical schema.
3) **Remove plan cache legacy key fallback** and invalidate cache.
4) **Remove telemetry payload fallbacks** (enforce msgpack-only fields).
5) **Remove input name fallbacks** (require canonical `_v1` names).
6) **Collapse versioned naming/migration framework** if you are ready to break v1/v2 compatibility entirely.
7) **Evaluate PyArrow dataset & Delta QueryBuilder modules** for DataFusion-only replacement.

## Required Migrations / Preconditions
- Confirm downstream consumers no longer expect legacy relationship schema columns.
- Flush/rotate plan caches if removing legacy cache key support.
- Ensure telemetry producers only emit msgpack payloads.
- Ensure all semantic inputs/outputs are registered under canonical names.
- Decide whether to keep or remove versioned dataset naming and migration registry.

## CQ Evidence (Artifacts)
Primary CQ artifacts saved in `.cq/artifacts/`:
- `run_20260205_065418_019c2c94-8917-700d-aa7e-00132e1f0515.json` (legacy/deprecated search)
- `search_20260205_065554_019c2c96-02c8-7578-8482-779bf250f721.json` (legacy_ regex)
- `search_20260205_065559_019c2c96-17a0-71f4-ab77-7ca1793ec3c3.json` (_legacy regex)
- `calls_20260205_065652_019c2c96-e6fb-7fa7-9163-ffdf542d25a8.json` (project_semantic_to_legacy callers)
- `calls_20260205_065700_019c2c97-038b-73f2-8800-105ed3be6db2.json` (legacy_key callers)
- `calls_20260205_065712_019c2c97-31aa-700b-b282-f689164b9553.json` (migrate_cpg_nodes_v1_to_v2 callers)
- `calls_20260205_065718_019c2c97-4b0a-7543-8242-4b46d46c7504.json` (migrate_cpg_edges_v1_to_v2 callers)
- `q_20260205_065547_019c2c95-e78a-7c97-ab72-61ad6ff9dceb.json` (__getattr__ definitions)
- `q_20260205_065623_019c2c96-724d-7fce-83c5-61ebed8af944.json` (importlib dynamic usage)
- `report:dependency-health_20260205_065645_019c2c96-ca36-77c2-83a0-6f8f56e963bb.json`

