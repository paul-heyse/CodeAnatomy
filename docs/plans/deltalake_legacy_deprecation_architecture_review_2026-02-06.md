# DeltaLake Legacy Deletion + Architecture Review (2026-02-06)

## Scope and Method

This review covers three asks:
1. Legacy DeltaLake code that can be deprecated/deleted.
2. Additional architecture improvements for a best-in-class DataFusion-integrated DeltaLake stack.
3. Further modularization with explicit spec-vs-runtime separation, including msgspec adoption guidance.

Grounding completed before analysis:
- `bash scripts/bootstrap_codex.sh`
- `uv sync`

CQ-first discovery was used for all code-surface and caller analysis (examples):
- `./cq calls query_delta_sql`
- `./cq calls _degraded_delta_provider_bundle`
- `./cq calls open_delta_table`
- `./cq calls execute_query`
- `./cq calls read_delta_table_eager`
- `./cq calls read_delta_cdf_eager`
- `./cq calls delta_history_snapshot`
- `./cq calls delta_protocol_snapshot`
- `./cq calls delta_cdf_enabled`
- `./cq calls build_delta_file_index_from_add_actions`
- `./cq calls evaluate_and_select_files`

Reference guidance used:
- `.codex/skills/dfdl_ref/reference/deltalake.md`
- `.codex/skills/dfdl_ref/reference/deltalake_datafusionmixins.md`
- `.codex/skills/dfdl_ref/reference/deltalake_datafusion_integration.md`
- `.codex/skills/dfdl_ref/reference/datafusion_deltalake_advanced_rust_integration.md`
- `.codex/skills/dfdl_ref/reference/datafusion_rust_UDFs.md`
- `docs/python_library_reference/msgspec.md`

---

## 1) Legacy DeltaLake Code: Deprecate/Delete Recommendations

## 1.1 Delete-Ready (internal usage is effectively zero after small migration)

### A) QueryBuilder path
- Candidate:
  - `src/storage/deltalake/query_builder.py`
  - `query_delta_sql(..., use_querybuilder=True)` branch in `src/storage/deltalake/delta.py`
- Evidence:
  - `query_delta_sql` has a single callsite (`tests/harness/delta_smoke.py:92`).
  - `open_delta_table` has one callsite (`src/storage/deltalake/delta.py:483`).
  - `execute_query` has one callsite (`src/storage/deltalake/delta.py:486`).
  - `query_builder.py` itself warns this is ad-hoc-only (`src/storage/deltalake/query_builder.py:33-38`).
- Conclusion:
  - Migrate `tests/harness/delta_smoke.py` off `query_delta_sql` first, then delete QueryBuilder surface completely.

### B) Internal compatibility re-export facades (staged delete)
- Candidate:
  - `src/storage/__init__.py`
  - `src/storage/io.py` (as compatibility import surface, not core logic)
- Evidence:
  - No internal `from storage.io import ...` usage found.
  - No internal `import storage.io` usage found.
  - `from storage import ...` appears only in `src/storage/__init__.py`.
- Conclusion:
  - Keep temporarily only as an external-compat layer if needed by downstream users.
  - If there are no external dependents, remove.
  - If unknown external dependents exist, deprecate one release, then remove.

## 1.2 Strong Deprioritization/Deprecation Candidates (currently unused internally)

### C) Disable-feature helpers (external-only today)
- Candidates in `src/storage/deltalake/delta.py`:
  - `disable_delta_change_data_feed`
  - `disable_delta_deletion_vectors`
  - `disable_delta_row_tracking`
  - `disable_delta_in_commit_timestamps`
  - `disable_delta_vacuum_protocol_check`
  - `disable_delta_column_mapping`
  - `disable_delta_generated_columns`
  - `disable_delta_invariants`
  - `disable_delta_check_constraints`
  - `disable_delta_checkpoint_protection`
  - `disable_delta_v2_checkpoints`
- Evidence:
  - CQ `calls` results for these functions show `total_sites: 0` internally.
- Conclusion:
  - Remove from default public export surface first.
  - Keep behind explicit advanced API module only if you need external compatibility.

## 1.3 Keep (not deletable yet)

### D) Degraded fallback seams
- Keep for now:
  - `_degraded_delta_provider_bundle`
  - `_degraded_delta_cdf_bundle`
- Evidence:
  - `_degraded_delta_provider_bundle` still has one live callsite via strictness opt-out (`src/datafusion_engine/dataset/resolution.py:167`).
  - `_degraded_delta_cdf_bundle` is still wired in CDF resolution degraded path.
- Conclusion:
  - Do not delete unless you decide to remove degraded mode entirely.

### E) File index/pruning stack
- Keep:
  - `build_delta_file_index_from_add_actions`
  - `evaluate_and_select_files`
- Evidence:
  - Both are actively used by lineage scan (`src/datafusion_engine/lineage/scan.py:357` and `src/datafusion_engine/lineage/scan.py:359`).
- Conclusion:
  - Not legacy dead code; active and required for pruning behavior.

### F) Service wrapper functions with single callsites
- Keep for now:
  - `read_delta_table_eager`, `read_delta_cdf_eager`, `delta_history_snapshot`, `delta_protocol_snapshot`, `delta_cdf_enabled`
- Evidence:
  - Each has one callsite, all in `DeltaService` methods.
- Conclusion:
  - These are thin, but still useful as service boundary points; fold only as part of a wider API simplification.

## 1.4 Structural legacy risk that should be addressed next
- `src/storage/deltalake/delta.py` is still a monolith:
  - 3,575 lines
  - 105 top-level `def/class` declarations
- `src/storage/deltalake/__init__.py` still exports a broad, mixed surface (feature control, reads/writes, pruning/indexing, compatibility APIs).

## 1.5 Rust integration gaps discovered in follow-on review (priority additions, not deletions)

### G) External UDTF registration is a stub
- Evidence:
  - `rust/datafusion_ext/src/udtf_sources.rs` currently returns `Ok(())` in `register_external_udtfs(...)` without registering any table functions.
  - `rust/datafusion_ext/src/udf_registry.rs` still invokes this function during registry setup.
- Conclusion:
  - This is the highest-impact missing Rust integration seam for Delta-specific table functions.

### H) Custom table UDF registry is intentionally empty today
- Evidence:
  - `table_udf_specs()` returns `table_udfs![]` in `rust/datafusion_ext/src/udf_registry.rs`.
  - Existing test asserts `table_udf_specs().is_empty()`.
- Conclusion:
  - The current registry shape is compatible with expansion, but no custom external table functions are currently shipped.

### I) Plugin table-function exports are narrower than the native session surface
- Evidence:
  - `rust/df_plugin_codeanatomy/src/lib.rs` builds plugin table-function exports from `table_udf_specs()` only.
  - Native path separately registers builtins and source UDTFs via `udtf_builtin` and `udtf_sources`.
- Conclusion:
  - Plugin and native table-function capability surfaces should be unified to avoid drift.

---

## 2) Additional Architecture Improvements for Best-in-Class DataFusion + DeltaLake

These recommendations come from combining current code state with integration guidance in the Delta/DataFusion reference material.

## 2.1 Provider-first, SessionContext-first execution should be absolute
- Keep `SessionContext + Delta TableProvider` as the only production query path.
- Remove QueryBuilder path entirely after harness migration.
- Rationale:
  - DataFusion docs and integration guidance favor direct provider registration for pushdown/planning quality.
  - QueryBuilder is a convenience path and should remain non-production.

## 2.2 Introduce a first-class provider build contract object
Add a single typed artifact for provider construction, e.g. `DeltaProviderBuildResult`, capturing:
- canonical URI
- resolved version/timestamp and snapshot identity
- entrypoint/module/ctx-kind/probe-result
- scan config and effective options
- pushdown classification summary
- object store registration outcome

This removes repeated ad-hoc dict payloads and gives deterministic diagnostics and plan artifacts.

## 2.3 Make filter/pushdown behavior auditable by design
From Delta/DataFusion scan contract guidance:
- persist exact vs inexact pushdown classification
- persist `files_scanned` / `files_pruned`
- include predicate normalization artifacts in diagnostics

Use this both in plan artifacts and runtime diagnostics report.

## 2.4 Strengthen write-path contract around schema and CDC/CDF semantics
Add explicit write boundary checks (before commit):
- dictionary-to-physical schema normalization where needed
- strict handling of injected partition/path metadata columns (must be dropped/renamed before persistence unless part of target schema)
- explicit `_change_type` split policy (normal vs CDF batches)
- constraints/invariants batch validation before commit

This aligns with deltalake-core write execution patterns and avoids implicit behavior drift.

## 2.5 Treat commit conflicts as first-class outcomes
Strengthen conflict policy with explicit outcomes:
- `committed`
- `conflict_retriable`
- `conflict_requires_verification`
- `fatal`

Persist verification traces tied to table history and snapshot key where retries are attempted.

## 2.6 Add checkpoint/cleanup guardrails
Given known checkpoint/cleanup hazards documented in integration references:
- gate checkpoint and cleanup with explicit policy constraints
- require safe retention windows and checkpoint validity checks
- fail-safe behavior for partial snapshots / aggressive cleanup scenarios

## 2.7 Collapse remaining mixed authority for object-store settings
`StorageProfile` is good progress. Complete it by:
- making it the only emitter for both DataFusion object-store config and delta-rs storage options
- adding immutable profile provenance/fingerprint into all provider/write artifacts
- forbidding any direct ad-hoc options dict construction outside a single adapter layer

## 2.8 Prioritize Delta-oriented UDTF completion (before adding more scalar/UDAF functions)
Implement external table functions in `rust/datafusion_ext/src/udtf_sources.rs` as first-class registration targets:
- `read_delta(...)` (version/timestamp aware table reader)
- `read_delta_cdf(...)` (CDF relation surface)
- `delta_snapshot(...)` (snapshot metadata relation)
- `delta_add_actions(...)` (file/action metadata relation)

Design requirements:
- use strict literal/constant argument parsing and explicit coercion/validation
- return `TableProvider` implementations with clear pushdown capability declarations
- align signatures and behavior with existing native control-plane operations to avoid duplicate semantics

Rationale:
- This addresses the single largest missing Rust/DataFusion integration seam while leveraging existing control-plane primitives already implemented in Rust.

## 2.9 Add Rust-side runtime/cache policy bridge exported to Python
Extend runtime/session construction surfaces (notably `delta_session_context(...)`) to support policy-driven runtime wiring from Python:
- named cache policy/profile selection
- object-store retry/timeout/backoff policy controls where Rust-level integration is required
- immutable runtime policy snapshots emitted with diagnostics artifacts

Rationale:
- Advanced guidance shows Python-only runtime hooks are insufficient for custom cache injection and some object-store controls; this needs a Rust bridge to remain deterministic.

## 2.10 Add structured execution metrics export (Arrow-native, not text-only)
Add a Rust observability function that emits execution/operator metrics as typed rows:
- expose a stable Arrow/IPC payload for per-operator metrics and query summary
- avoid relying only on textual `EXPLAIN` output parsing for machine checks
- integrate payload into diagnostics report synthesis and failure artifacts

Rationale:
- Enables deterministic regression checks and better production debugging without brittle text parsing.

## 2.11 Unify native and plugin table-function export lanes
Ensure plugin exports and native registration produce the same intended table-function capability set:
- remove divergence between plugin `build_table_functions()` and native runtime registration
- include external/builtin table functions consistently across both paths
- add explicit capability snapshot checks for parity

Rationale:
- Prevents capability drift between deployment modes and simplifies test expectations.

---

## 3) Modularization + Spec-vs-Runtime Separation with msgspec

## 3.1 Current state (mixed)
- Control-plane requests are mostly strict msgspec structs (`StructBaseStrict`) in `src/datafusion_engine/delta/control_plane.py`.
- Storage layer still carries many runtime `@dataclass` request/option models in `src/storage/deltalake/delta.py`.
- `src/datafusion_engine/delta/service.py` mixes dataclasses and msgspec structs.
- Conversion shims (`cdf_options_to_spec`, `cdf_options_from_spec`) indicate duplicate model stacks.

## 3.2 Target model policy
Use three model categories only:
1. **Spec objects (boundary and persisted)**
   - `StructBaseStrict` for runtime API/control-plane requests.
   - `StructBaseCompat` for persisted artifacts requiring forward compatibility.
2. **Runtime handles (non-serializable)**
   - `SessionContext`, `DataFrame`, `DeltaTable`, Arrow reader/table instances.
3. **Adapters (the only conversion layer)**
   - single module family for spec -> runtime kwargs payload conversion.

## 3.3 Recommended module split
Proposed package layout:
- `src/datafusion_engine/delta/spec/`
  - `table_ref.py`
  - `read.py`
  - `write.py`
  - `feature_ops.py`
  - `maintenance.py`
  - `snapshot.py`
- `src/datafusion_engine/delta/adapters/`
  - `control_plane_payloads.py`
  - `storage_payloads.py`
  - `object_store_payloads.py`
- `src/storage/deltalake/runtime/`
  - `read_ops.py`
  - `write_ops.py`
  - `feature_ops.py`
  - `maintenance_ops.py`
  - `snapshot_ops.py`

Keep `src/storage/deltalake/delta.py` as a temporary compatibility shim during migration, then delete.

## 3.4 Concrete model unification steps
- Replace duplicated runtime dataclasses with unified specs for:
  - `DeltaReadRequest`
  - `DeltaMergeArrowRequest`
  - `DeltaDeleteWhereRequest`
  - `DeltaFeatureMutationOptions`
  - `DeltaVacuumOptions`
  - `DeltaSchemaRequest`
- Merge `DeltaCdfOptions` + `DeltaCdfOptionsSpec` into one canonical spec type.
- Convert `StorageOptions` alias to an explicit typed spec object (`StorageOptionsSpec`) where practical.
- Remove bidirectional spec conversion helpers once one canonical spec remains.

## 3.5 msgspec-specific practices to adopt
From `msgspec.md` and current `serde_msgspec` policy:
- Keep `kw_only=True`, `frozen=True`, `omit_defaults=True`.
- Use `forbid_unknown_fields=True` for strict runtime request contracts.
- Use `StructBaseCompat` only for persisted artifacts that must tolerate additive fields.
- Generate and check JSON Schemas (`msgspec.json.schema`) for major boundary specs in CI.
- Follow schema evolution rules explicitly (add-only with defaults, no type mutation of existing fields).

---

## 4) Recommended Test Additions and Updates

## 4.1 Legacy deletion/deprecation safety tests
- Add a deprecation test gate ensuring QueryBuilder path is not used in production code.
- Add coverage for deprecation warnings on compatibility facades (`storage` / `storage.io`) if kept temporarily.
- Add a static import test proving no internal imports rely on compatibility facades.

## 4.2 Architecture contract tests
- Provider-build artifact tests: verify canonical payload contains entrypoint/module/ctx-kind/probe-result + storage profile fingerprint.
- Pushdown/pruning audit tests: verify classification and files scanned/pruned metrics are emitted.
- Write contract tests: dictionary normalization, partition metadata column handling, `_change_type` split policy, constraint pre-commit checks.
- Concurrency tests: deterministic handling for conflict outcomes and retry/verification behavior.

## 4.3 msgspec model-governance tests
- Roundtrip serialization tests for all boundary spec objects.
- Strict decode tests (`forbid_unknown_fields`) for runtime APIs.
- Compat decode tests for persisted artifacts (unknown-field tolerant where intended).
- JSON Schema snapshot tests for boundary specs.

## 4.4 Rust/DataFusion integration tests to add
- UDTF conformance tests:
  - SQL + programmatic invocation coverage for `read_delta`, `read_delta_cdf`, `delta_snapshot`, `delta_add_actions`
  - argument validation/coercion failures are structured and non-panicking
  - pushdown capability assertions for returned providers
- Registry/discovery parity tests:
  - information schema / function discovery includes newly registered table functions where applicable
  - plugin export parity checks against native registration surface
- Runtime/cache policy bridge tests:
  - session construction applies named runtime/cache profiles deterministically
  - policy payloads are serialized into diagnostics artifacts
- Structured metrics contract tests:
  - Arrow/IPC metrics payload shape is stable
  - key execution counters/operators are present for representative query plans

---

## 5) Decision-Ready Conclusions

1. **Immediate cleanup path exists**: remove QueryBuilder production branch and its module after migrating one test harness callsite.
2. **Large export/facade surfaces should be reduced**: keep compatibility shims only if external consumers require them; otherwise remove.
3. **Best-in-class next step is architectural hardening, not feature expansion**:
   - provider-build contract artifact,
   - explicit pushdown/pruning observability,
   - strict write contract and conflict semantics,
   - checkpoint/cleanup guardrails.
4. **Model layer should be unified around msgspec**:
   - one canonical spec per boundary concept,
   - no duplicated dataclass/spec model pairs,
   - clear spec-vs-runtime separation enforced by module boundaries.
5. **Highest-priority Rust follow-on is UDTF surface completion**:
   - implement external Delta UDTFs first,
   - unify plugin/native table-function exports,
   - add structured metrics and runtime/cache policy bridge.
6. **Additional scalar/UDAF/UDWF expansion is lower priority**:
   - current Rust function surface is already substantial,
   - immediate ROI is in missing table-function and observability/runtime seams.
