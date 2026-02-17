# Design Review: Storage Module

**Date:** 2026-02-17
**Scope:** `src/storage/`
**Focus:** All principles (1-24), with emphasis on Correctness (16-18) and Boundaries (1-6)
**Depth:** deep
**Files reviewed:** 17 (`storage/__init__.py`, `storage/io.py`, `storage/dataset_sources.py`, `storage/cdf_cursor_protocol.py`, `storage/ipc_utils.py`, `storage/external_index/__init__.py`, `storage/external_index/provider.py`, `storage/deltalake/__init__.py`, `storage/deltalake/config.py`, `storage/deltalake/file_index.py`, `storage/deltalake/file_pruning.py`, `storage/deltalake/delta_read.py`, `storage/deltalake/delta_write.py`, `storage/deltalake/delta_maintenance.py`, `storage/deltalake/delta_metadata.py`, `storage/deltalake/delta_runtime_ops.py`, `storage/deltalake/delta_feature_mutations.py`)

## Executive Summary

The `src/storage/` module serves as the technology-adapter layer for Delta Lake and Arrow IPC persistence. It is well-structured at the package level with clean lazy-import facades, protocol-based contracts (`CdfCursorLike`, `ExternalIndexProvider`), and comprehensive policy types (`DeltaWritePolicy`, `DeltaSchemaPolicy`, `DeltaMutationPolicy`). The correctness profile is strong: retry logic with configurable policies, fallback paths for control-plane failures, and idempotent write support. The primary concerns are: (1) `delta_runtime_ops.py` (~900 LOC) exports 26 private functions as cross-module API, (2) reverse dependency from `storage.deltalake` back to `datafusion_engine.delta` creates a bidirectional coupling between adapter and engine layers, and (3) `delta_read.py` (~713 LOC) carries a size-exception comment acknowledging it needs further decomposition. The module's policy types (`config.py`) are exemplary -- frozen msgspec structs with fingerprinting, constrained annotation types, and clean separation between write, schema, retry, and mutation policies.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | small | high | `delta_runtime_ops.py` exports 26 private functions in `__all__` |
| 2 | Separation of concerns | 2 | medium | medium | `delta_read.py` mixes read operations, type definitions, input coercion, and CDF support |
| 3 | SRP | 2 | medium | medium | `delta_runtime_ops.py` handles retry logic, merge execution, observability recording, span management, feature queries |
| 4 | High cohesion, low coupling | 2 | small | low | Well-organized subpackages; some cross-module private imports |
| 5 | Dependency direction | 1 | medium | high | `delta_runtime_ops.py` imports from `datafusion_engine.delta.*` at runtime (adapter depends on engine) |
| 6 | Ports & Adapters | 2 | small | low | Good protocol definitions; `ExternalIndexProvider` is exemplary |
| 7 | DRY | 2 | small | low | `_DeltaFeatureMutationRecord` pattern repeated 8 times in feature mutations |
| 8 | Design by contract | 2 | small | low | Frozen request dataclasses enforce input contracts; `StatsFilter.value: Any` is untyped |
| 9 | Parse, don't validate | 2 | small | low | `coerce_delta_input()` parses at boundary; report payloads parsed repeatedly |
| 10 | Make illegal states unrepresentable | 2 | small | low | `DeltaReadRequest` allows version+timestamp; `WriteMode` enum is clean |
| 11 | CQS | 2 | small | low | `delta_delete_where()` commands and returns; pragmatic design |
| 12 | DI + explicit composition | 2 | small | low | Constructor injection via request dataclasses; some hidden internal creation |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies |
| 14 | Law of Demeter | 2 | small | low | `profile.delta_ops.delta_runtime_ctx()` chains through 2 levels |
| 15 | Tell, don't ask | 2 | small | low | `_merge_rows_affected` probes 6 key variants |
| 16 | Functional core, imperative shell | 2 | small | low | Pure functions: `canonical_table_uri`, `resolve_stats_columns`, `_delta_retry_delay` |
| 17 | Idempotency | 2 | small | medium | `IdempotentWriteOptions` exists but retry paths don't use it |
| 18 | Determinism / reproducibility | 3 | - | - | Policy fingerprinting via `config_fingerprint()` throughout |
| 19 | KISS | 2 | small | low | Lazy import facades appropriate; `OneShotDataset.__getattr__` delegation is complex |
| 20 | YAGNI | 3 | - | - | No speculative generality detected |
| 21 | Least astonishment | 1 | small | medium | `delta_commit_metadata()` always returns `None` despite its name |
| 22 | Declare/version public contracts | 2 | small | low | `__all__` consistent; private names in `__all__` |
| 23 | Design for testability | 2 | small | low | Frozen dataclasses, protocols; some functions require real Delta tables |
| 24 | Observability | 3 | - | - | Comprehensive `stage_span` instrumentation with structured `codeanatomy.*` attributes |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information Hiding -- Alignment: 1/3

**Current state:**
`delta_runtime_ops.py` exports 26 private-named functions in its `__all__` list. These functions are consumed by `delta_write.py`, `delta_feature_mutations.py`, and `delta_maintenance.py` as cross-module imports. The `_` prefix convention signals "internal implementation detail," but the cross-module usage makes them effectively public API with an incorrect naming convention.

**Findings:**
- `src/storage/deltalake/delta_runtime_ops.py:865-898` -- `__all__` includes `_MutationArtifactRequest`, `_commit_metadata_from_properties`, `_constraint_status`, `_delta_cdf_table_provider`, `_delta_commit_options`, `_delta_retry_classification`, `_enforce_locking_provider`, `_execute_delta_merge`, `_record_mutation_artifact`, `_resolve_delta_mutation_policy`, `_storage_span_attributes` (26 total). All are imported by sibling modules.
- `src/storage/deltalake/delta_write.py:121-132` -- Imports 10 private functions from `delta_runtime_ops.py`: `_constraint_status`, `_delta_commit_options`, `_delta_retry_classification`, `_delta_retry_delay`, `_enforce_append_only_policy`, `_enforce_locking_provider`, `_MutationArtifactRequest`, `_record_mutation_artifact`, `_resolve_delta_mutation_policy`, `_storage_span_attributes`.
- `src/storage/deltalake/delta_feature_mutations.py:9-14` -- Imports 4 private functions from `delta_runtime_ops.py`.
- `src/storage/deltalake/delta_read.py:677-686` -- Imports private functions from `delta_runtime_ops.py` at module level (not in TYPE_CHECKING block).

**Suggested improvement:**
Promote the 26 private functions to public names by removing the `_` prefix. They are genuinely part of the internal module contract within `storage.deltalake` and are used across multiple submodules.

**Effort:** small
**Risk if unaddressed:** high -- The private naming creates a false signal about API stability. Any tooling that enforces private-function-import rules would flag all 26 usages.

---

#### P5. Dependency Direction -- Alignment: 1/3

**Current state:**
The `storage.deltalake` package, which should be a pure technology adapter, has runtime dependencies on `datafusion_engine.delta` (the engine ring). This creates a bidirectional dependency between layers: the engine depends on storage for read/write operations, and storage depends on the engine for control-plane operations and protocol types.

**Findings:**
- `src/storage/deltalake/delta_runtime_ops.py:16` -- Runtime import of `DeltaFeatureGate` and `DeltaProtocolSnapshot` from `datafusion_engine.delta.protocol`.
- `src/storage/deltalake/delta_runtime_ops.py:196` -- Runtime import of `delta_merge` from `datafusion_engine.delta.control_plane_core`.
- `src/storage/deltalake/delta_runtime_ops.py:646-647` -- Runtime import of `DeltaMutationArtifact` and `record_delta_mutation` from `datafusion_engine.delta.observability`.
- `src/storage/deltalake/delta_read.py:142-145` -- Deferred import of `DeltaSnapshotRequest` and `delta_snapshot_info` from `datafusion_engine.delta.control_plane_core`.
- `src/storage/deltalake/delta_feature_mutations.py:55-59` -- Deferred import of `DeltaCommitOptions`, `DeltaSetPropertiesRequest`, `delta_set_properties` from `datafusion_engine.delta.control_plane_core`.

Many of these imports are deferred (inside function bodies) to avoid import cycles, which is a symptom of the architectural tension.

**Suggested improvement:**
Move shared types (`DeltaFeatureGate`, `DeltaProtocolSnapshot`, `StorageOptions`) into a shared types package that both layers can depend on. Consider defining a `ControlPlanePort` protocol in the storage layer that the engine layer implements, inverting the dependency direction for control-plane operations.

**Effort:** medium
**Risk if unaddressed:** high -- The bidirectional dependency constrains refactoring freedom and risks import cycles as the codebase evolves. Deferred imports are a code smell indicating architectural tension.

---

#### P2. Separation of Concerns -- Alignment: 2/3

**Current state:**
`delta_read.py` (~713 LOC, with a size-exception comment) mixes type definitions (10+ dataclasses), read operations, input coercion, CDF support, and snapshot lookup. `delta_runtime_ops.py` (~900 LOC) mixes retry logic, merge execution, observability recording, span management, and feature query functions. Both files acknowledge their size as temporary during decomposition.

**Findings:**
- `src/storage/deltalake/delta_read.py:2-4` -- Comment: "NOTE(size-exception): This module is temporarily >800 LOC during hard-cutover decomposition."
- `src/storage/deltalake/delta_read.py` -- Contains: `DeltaWriteResult`, `DeltaCdfOptions`, `DeltaReadRequest`, `DeltaDeleteWhereRequest`, `DeltaMergeArrowRequest`, `DeltaSchemaRequest`, `DeltaVacuumOptions`, `SnapshotKey`, `IdempotentWriteOptions`, `DeltaInput` (10 dataclasses), plus read functions, coercion functions, and helper types.
- `src/storage/deltalake/delta_write.py:2-4` -- Same size-exception comment.
- `src/storage/deltalake/delta_runtime_ops.py` -- No size-exception comment but at ~900 LOC with 5+ distinct concerns.

**Suggested improvement:**
Extract type definitions from `delta_read.py` into a `delta_types.py` module. This would reduce `delta_read.py` to pure read functions and `delta_types.py` to pure type definitions. The type module could be shared by read, write, and runtime_ops without creating new dependencies.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
Protocol definitions are well-designed. `ExternalIndexProvider` in `storage/external_index/provider.py` defines a clean port with `supports()` and `select_candidates()`. `CdfCursorStoreLike` and `CdfCursorLike` in `cdf_cursor_protocol.py` are minimal structural contracts. `OneShotDataset` in `dataset_sources.py` enforces single-scan semantics as a behavioral wrapper. However, the storage layer lacks a clean port for control-plane operations -- it reaches directly into `datafusion_engine.delta`.

**Findings:**
- `src/storage/external_index/provider.py:46-62` -- `ExternalIndexProvider` protocol: clean port with `provider_name`, `supports()`, and `select_candidates()`. Exemplary.
- `src/storage/cdf_cursor_protocol.py:8-25` -- `CdfCursorLike` and `CdfCursorStoreLike` protocols: minimal, structural. Good.
- `src/storage/dataset_sources.py:25-113` -- `OneShotDataset`: behavioral wrapper enforcing single-scan semantics via `_scanned` flag. Good adapter pattern.
- No `ControlPlanePort` protocol exists -- storage functions import concrete engine modules for control-plane operations.

**Suggested improvement:**
Define a `DeltaControlPlanePort` protocol in the storage layer with methods like `snapshot_info()`, `merge()`, `delete()`, `feature_enable()`. Have the engine layer provide an implementation. This would invert the dependency and allow the storage layer to be tested independently.

**Effort:** medium (requires coordination with engine layer)
**Risk if unaddressed:** low (current pattern works; the improvement is architectural hygiene)

---

### Category: Knowledge (7-11)

#### P7. DRY (Knowledge, Not Lines) -- Alignment: 2/3

**Current state:**
The feature mutation functions in `delta_feature_mutations.py` follow a highly repetitive pattern: each function (8 total) calls `_feature_control_span`, `_feature_enable_request`, the control-plane function, and `_record_delta_feature_mutation` with a `_DeltaFeatureMutationRecord`. The structure is identical; only the control-plane function name and operation string differ. This is borderline -- the repetition is in ceremony, not knowledge.

**Findings:**
- `src/storage/deltalake/delta_feature_mutations.py:237-526` -- 7 `enable_delta_*` functions follow identical structure: (1) `_feature_control_span(options, operation=...)`, (2) `_feature_enable_request(options)`, (3) control-plane call, (4) `_record_delta_feature_mutation(...)`. Only the control-plane import and operation string differ.
- `src/storage/deltalake/delta_runtime_ops.py:280-300` -- `_storage_span_attributes()` centralizes span attribute construction. Good DRY application.
- `src/storage/deltalake/config.py` -- All policy classes implement `fingerprint_payload()` and `fingerprint()` with identical structure. This is acceptable since each fingerprint payload is distinct.

**Suggested improvement:**
Consider a generic `_enable_delta_feature()` helper that takes the operation name and control-plane function as parameters, reducing the 7 near-identical functions to 7 one-liner wrappers. This is a low-priority improvement since the current repetition is in ceremony, not business logic.

**Effort:** small
**Risk if unaddressed:** low -- The repetition is structural, not knowledge duplication.

---

#### P8. Design by Contract -- Alignment: 2/3

**Current state:**
Request dataclasses serve as implicit contracts. The `config.py` module uses `msgspec.Meta` annotations to add runtime constraints: `NonNegInt` (ge=0), `PositiveInt` (ge=1), `PositiveFloat` (gt=0), `NonEmptyStr` (min_length=1), `ColumnName` (pattern-validated). These are excellent examples of design-by-contract through type annotations.

**Findings:**
- `src/storage/deltalake/config.py:15-61` -- Annotated types with `msgspec.Meta`: `NonNegInt`, `PositiveInt`, `PositiveFloat`, `NonEmptyStr`, `ColumnName` (with pattern `^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`). Excellent contract definitions.
- `src/storage/deltalake/file_pruning.py:39` -- `StatsFilter.value: Any` -- the only use of bare `Any` in the scope. This weakens the contract for statistics filter values.
- `src/storage/deltalake/delta_read.py:357-359` -- `_open_delta_table` validates mutual exclusivity of `version` and `timestamp`. Good precondition check.

**Suggested improvement:**
Replace `StatsFilter.value: Any` with a union type: `int | float | str | bool` or a constrained annotation type that matches the valid stats value types.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional Core, Imperative Shell -- Alignment: 2/3

**Current state:**
The module contains a healthy mix of pure functions and imperative operations. Pure functions include: `canonical_table_uri()` (URI normalization), `resolve_stats_columns()` (policy resolution), `_delta_retry_delay()` (exponential backoff calculation), `_matches_partition_filters()` (filter evaluation), `_matches_stats_filters()` (statistics evaluation), `config_fingerprint()` (policy hashing). Imperative functions handle Delta table operations and span management.

**Findings:**
- `src/storage/deltalake/delta_metadata.py:20-41` -- `canonical_table_uri()` is a pure function normalizing Delta table URIs. Clean functional core.
- `src/storage/deltalake/config.py:319-344` -- `resolve_stats_columns()` is a pure function resolving stats columns from policy inputs. Good.
- `src/storage/deltalake/delta_runtime_ops.py:138-140` -- `_delta_retry_delay()` is a pure function computing exponential backoff. Good.
- `src/storage/deltalake/file_pruning.py:403-461` -- `_matches_partition_filters()` and `_matches_stats_filters()` are pure functions for file-level filter evaluation. Good.
- `src/storage/deltalake/file_index.py:96-174` -- `_normalize_file_index()` is a pure transformation function. Good.

**Suggested improvement:**
The functional core is adequate. No immediate action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Idempotent writes are supported via `IdempotentWriteOptions` with `app_id` and `version` fields, which map to Delta Lake's `CommitProperties.app_transactions`. The `build_commit_properties()` function in `delta_write.py` constructs these. However, the retry paths in `_execute_delta_merge()` and `delta_delete_where()` do not use idempotent tokens, creating a window for duplicate mutations on retry.

**Findings:**
- `src/storage/deltalake/delta_read.py:176-192` -- `IdempotentWriteOptions` dataclass with `app_id` and `version`. Clean idempotency token definition.
- `src/storage/deltalake/delta_write.py:47-69` -- `build_commit_properties()` constructs `CommitProperties` with `Transaction` entries when `app_id` and `version` are provided. Correct mapping to Delta protocol.
- `src/storage/deltalake/delta_runtime_ops.py:189-213` -- `_execute_delta_merge()` retry loop: re-executes merge on retryable errors without idempotent tokens. If the original merge committed but the response was lost, the retry creates a duplicate merge.
- `src/storage/deltalake/delta_write.py:168-194` -- `delta_delete_where()` retry loop: same concern. A successful-but-unacknowledged delete followed by retry could double-delete.

**Suggested improvement:**
Add `app_transaction` to the `DeltaCommitOptions` constructed in `_delta_commit_options()` when the caller provides idempotency tokens. For merge and delete retries, generate a unique app_id from the operation + table_uri + timestamp to enable Delta-level deduplication.

**Effort:** small
**Risk if unaddressed:** medium -- In practice, the retryable errors (`ConcurrentAppendException`, `ConcurrentTransactionException`) indicate the commit did not succeed, so duplicate commits from retries are unlikely. But edge cases exist.

---

#### P18. Determinism / Reproducibility -- Alignment: 3/3

**Current state:**
Policy fingerprinting is comprehensive and well-implemented throughout `config.py`. Every policy class (`DeltaWritePolicy`, `DeltaSchemaPolicy`, `DeltaRetryPolicy`, `DeltaMutationPolicy`, `ParquetWriterPolicy`, `FilePruningPolicy`) implements `fingerprint_payload()` and `fingerprint()` using `config_fingerprint()` from `core.config_base`. `SnapshotKey` provides deterministic Delta snapshot identity via `canonical_uri` + `version`.

**Findings:**
- `src/storage/deltalake/config.py` -- All 6 policy classes implement deterministic fingerprinting. Excellent.
- `src/storage/deltalake/delta_read.py:78-84` -- `SnapshotKey(canonical_uri, version)` provides deterministic snapshot identity.
- `src/storage/deltalake/delta_metadata.py:78-86` -- `snapshot_key_for_table()` resolves canonical URIs for deterministic keys.

No violations found. This principle is well-satisfied.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P21. Least Astonishment -- Alignment: 1/3

**Current state:**
`delta_commit_metadata()` in `delta_runtime_ops.py` is the most surprising function in the scope. Its name and docstring promise to return "custom commit metadata for the latest Delta table version." It successfully retrieves a snapshot (line 439-447) but then unconditionally returns `None` at line 449. A reader who calls this function expecting commit metadata will always get `None`.

**Findings:**
- `src/storage/deltalake/delta_runtime_ops.py:414-449` -- `delta_commit_metadata()` always returns `None`. Lines 439-447 retrieve a snapshot successfully, then line 448 `if snapshot is None: return None` and line 449 `return None`. The second `return None` fires even when the snapshot is valid.
- `src/storage/dataset_sources.py:99-113` -- `OneShotDataset.__getattr__` delegates to `self.dataset`, which is a dynamic dispatch pattern. The `__getattr__` is documented and handles `AttributeError` properly, making it less surprising.

**Suggested improvement:**
Either implement `delta_commit_metadata()` to extract metadata from the snapshot's commit info, or remove it from the public API and document that commit metadata retrieval is not yet implemented.

**Effort:** small
**Risk if unaddressed:** medium -- Any caller relying on `delta_commit_metadata()` will silently get `None` and may build incorrect logic on top of it.

---

### Category: Quality (23-24)

#### P24. Observability -- Alignment: 3/3

**Current state:**
Observability instrumentation throughout the storage module is comprehensive and consistent. Every storage operation uses `stage_span()` from `obs.otel` with structured `codeanatomy.*` attributes. The `_storage_span_attributes()` helper centralizes attribute construction. File pruning, metadata lookups, read/write operations, feature mutations, maintenance operations, and CDF reads are all instrumented.

**Findings:**
- `src/storage/deltalake/delta_runtime_ops.py:280-300` -- `_storage_span_attributes()` centralizes span attribute construction. Consistent use across all operations.
- `src/storage/deltalake/file_pruning.py:308-335` -- `evaluate_and_select_files()` instruments pruning with `total_files`, `files_selected`, `files_pruned` attributes.
- `src/storage/deltalake/delta_maintenance.py:41-55` -- `vacuum_delta()` instruments with `retention_hours`, `dry_run`, `enforce_retention_duration`.
- `src/storage/deltalake/delta_runtime_ops.py:631-660` -- `_record_delta_feature_mutation()` records feature mutations as observability artifacts.

No significant gaps found. This principle is well-satisfied.

**Effort:** -
**Risk if unaddressed:** -

---

## Cross-Cutting Themes

### Theme 1: Bidirectional Dependency with Engine Layer

The most significant architectural concern in the storage module is its runtime dependency on `datafusion_engine.delta`. The storage layer should be a pure adapter that the engine layer depends on, not the reverse. Currently, `delta_runtime_ops.py` imports from `datafusion_engine.delta.control_plane_core`, `datafusion_engine.delta.protocol`, and `datafusion_engine.delta.observability` at runtime. Many imports are deferred (inside function bodies) to avoid import cycles, which is a symptom of the architectural tension.

**Root cause:** The Rust control-plane integration was added via `datafusion_engine.delta.control_plane_core`, and storage operations need to call through it. Rather than defining a port/adapter boundary, the storage module imports the engine module directly.

**Affected principles:** P5 (Dependency Direction), P6 (Ports & Adapters), P4 (Coupling).

**Suggested approach:** Define a `ControlPlanePort` protocol in the storage layer. Have the engine layer implement it. Pass the implementation to storage functions via the `runtime_profile` parameter. This inverts the dependency direction.

### Theme 2: Acknowledged Technical Debt in File Sizes

Both `delta_read.py` and `delta_write.py` carry `NOTE(size-exception)` comments acknowledging they exceed the 800 LOC target and are undergoing decomposition. `delta_runtime_ops.py` at ~900 LOC has no such comment but has the same issue. The tracked plan (`docs/plans/src_design_improvements_implementation_plan_v1_2026-02-16.md`) addresses these.

**Affected principles:** P2 (Separation of Concerns), P3 (SRP).

### Theme 3: Exemplary Policy Design

The `config.py` module is a reference implementation for policy design in this codebase. It demonstrates: (1) frozen msgspec structs for immutability, (2) annotated types with `msgspec.Meta` for runtime validation, (3) `fingerprint_payload()` + `fingerprint()` for deterministic identity, (4) clean separation between write, schema, retry, and mutation policies. This pattern should be propagated to other policy definitions in the codebase.

**Affected principles:** P8 (Design by Contract), P10 (Illegal States), P18 (Determinism).

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P1 (Info Hiding) | Promote 26 private functions in `delta_runtime_ops.py` to public names | small | Aligns naming with actual cross-module usage |
| 2 | P21 (Least Astonishment) | Fix `delta_commit_metadata()` which always returns `None` | small | Eliminates misleading dead code |
| 3 | P8 (Contract) | Replace `StatsFilter.value: Any` with `int \| float \| str \| bool` | small | Strengthens filter value contract |
| 4 | P7 (DRY) | Extract generic `_enable_delta_feature()` helper to reduce 7 near-identical functions | small | Reduces ceremony duplication |
| 5 | P2 (SoC) | Extract 10 dataclass definitions from `delta_read.py` into `delta_types.py` | medium | Reduces delta_read.py by ~200 LOC |

## Recommended Action Sequence

1. **Fix `delta_commit_metadata()` (P21):** Either implement metadata extraction from the snapshot or remove the function. Zero-risk change.

2. **Promote private exports (P1, P22):** Rename the 26 private functions in `delta_runtime_ops.py` to public names. Update importers in `delta_write.py`, `delta_feature_mutations.py`, `delta_maintenance.py`.

3. **Replace `StatsFilter.value: Any` (P8):** Constrain to `int | float | str | bool` to eliminate the only bare `Any` in the storage module.

4. **Extract types from `delta_read.py` (P2, P3):** Move the 10 dataclass definitions into a `delta_types.py` module. This reduces `delta_read.py` and allows type reuse without circular dependencies.

5. **Define `ControlPlanePort` protocol (P5, P6):** Define a protocol in the storage layer for control-plane operations. Have the engine layer provide an implementation. This is a larger effort but addresses the fundamental dependency direction issue.

6. **Generic feature-enable helper (P7):** Reduce the 7 near-identical `enable_delta_*` functions in `delta_feature_mutations.py` to wrappers around a shared helper.
