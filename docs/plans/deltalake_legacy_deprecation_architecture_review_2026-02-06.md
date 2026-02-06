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
- `.codex/skills/datafusion-and-deltalake-stack/reference/deltalake.md`
- `.codex/skills/datafusion-and-deltalake-stack/reference/deltalake_datafusionmixins.md`
- `.codex/skills/datafusion-and-deltalake-stack/reference/deltalake_datafusion_integration.md`
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
