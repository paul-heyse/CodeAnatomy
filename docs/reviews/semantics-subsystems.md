# Design Review: src/semantics/ Subsystems

**Date:** 2026-02-16
**Scope:** `src/semantics/incremental/`, `src/semantics/joins/`, `src/semantics/types/`, `src/semantics/diagnostics/`, `src/semantics/validation/`, `src/semantics/plans/`, `src/semantics/docs/`
**Focus:** All principles (1-24), with emphasis on dependency direction, information hiding, and DRY
**Depth:** Deep (all files)
**Files reviewed:** 38 files, 8,040 LOC

## Executive Summary

The seven subsystem packages under `src/semantics/` are generally well-structured with clean module boundaries, good use of frozen dataclasses, and appropriate separation of pure logic from IO. The type system (`types/`) and join inference (`joins/`) are the strongest subsystems, exhibiting high cohesion and disciplined information hiding. The incremental subsystem (`incremental/`) is the largest and most complex, with several dependency direction concerns where peripheral modules reach back into core registries. The diagnostics subsystem has meaningful DRY violations: four separate modules repeat the same `table_names_snapshot(ctx)` + `stage_span` + schema-probing pattern, and a private function (`_empty_table`) crosses module boundaries. Validation has a dead constant (`SEMANTIC_INPUT_COLUMN_SPECS`) that is exported but always empty. The highest-priority improvements are (1) extracting a shared diagnostic builder abstraction, (2) making `_empty_table` a proper internal utility, and (3) removing the dead validation constant.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_empty_table` crosses module boundary via private import |
| 2 | Separation of concerns | 2 | medium | medium | `cdf_runtime.py` mixes CDF read logic with artifact recording |
| 3 | SRP | 2 | medium | low | `metadata.py` handles both metadata writes and diagnostics snapshots |
| 4 | High cohesion, low coupling | 2 | medium | medium | diagnostics modules tightly coupled via shared private helper |
| 5 | Dependency direction | 2 | medium | high | `diagnostics/quality_metrics.py` imports `semantics.registry` (core); `docs/graph_docs.py` imports `compile_context` |
| 6 | Ports & Adapters | 3 | - | - | `DeltaCdfPort` protocol is a textbook port; `DeltaServiceCdfPort` is a clean adapter |
| 7 | DRY (knowledge) | 1 | medium | medium | `table_names_snapshot` + `stage_span` pattern duplicated across 4 diagnostic modules |
| 8 | Design by contract | 2 | small | low | `JoinStrategy.__post_init__` validates well; `CdfReadOptions` lacks cross-field validation |
| 9 | Parse, don't validate | 2 | small | low | `CdfFilterPolicy.from_cdf_column` returns `None` rather than a parsed type |
| 10 | Make illegal states unrepresentable | 2 | medium | low | `CdfReadOptions` allows `cursor_store` without `dataset_name` (caught at runtime) |
| 11 | CQS | 2 | small | low | `CdfCursorStore.update_version` mutates and returns (command+query) |
| 12 | DI + explicit composition | 2 | small | low | `IncrementalRuntime.registry_facade` lazily constructs complex object graph |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition via frozen dataclasses |
| 14 | Law of Demeter | 2 | small | low | `runtime.session_runtime().ctx` chain appears in 6+ locations |
| 15 | Tell, don't ask | 2 | small | low | Diagnostics builders ask `ctx` for schema names, then branch externally |
| 16 | Functional core, imperative shell | 2 | medium | low | Join inference is pure; incremental metadata writes mix IO throughout |
| 17 | Idempotency | 3 | - | - | Delta writes use OVERWRITE mode; cursor updates are idempotent |
| 18 | Determinism | 3 | - | - | `PlanFingerprint` hashes and `sorted()` usage ensure deterministic outputs |
| 19 | KISS | 2 | small | low | `_OverrideDeltaCdfPort` wrapper adds complexity for testing seams |
| 20 | YAGNI | 2 | small | low | `build_relationship_decisions_view` delegates entirely to `candidates` (no distinct logic) |
| 21 | Least astonishment | 2 | small | low | `SEMANTIC_INPUT_COLUMN_SPECS` is always `()` but exported as public API |
| 22 | Public contracts | 2 | small | low | All modules have `__all__`; `_empty_table` leaks across boundary despite underscore |
| 23 | Testability | 2 | small | low | `CdfReadOptions` has testing function overrides; but `_semantic_input_column_specs()` is hard to test in isolation |
| 24 | Observability | 3 | - | - | Every diagnostic builder is wrapped in `stage_span`; structured logging throughout |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Most subsystems hide their internals well. All modules declare `__all__` lists. The `types/core.py` STANDARD_COLUMNS registry is `Final` and immutable. However, there is one clear violation where a private function is imported across module boundaries.

**Findings:**
- `src/semantics/diagnostics/schema_anomalies.py:12` imports `_empty_table` from `semantics.diagnostics.quality_metrics`. This is a private helper (underscore-prefixed) being used as a shared utility across modules. The underscore convention communicates "don't import me" but the code violates that contract.
- `src/semantics/incremental/runtime.py:86-122` -- `IncrementalRuntime.registry_facade` exposes a lazily-built `RegistryFacade` as a public property, but its construction details (which registries to compose, how to handle UDF catalog errors) are leaked into the property getter rather than being encapsulated behind a factory.

**Suggested improvement:**
Extract `_empty_table` to a proper shared utility in `src/semantics/diagnostics/__init__.py` or a small `_utils.py` module within diagnostics, removing the underscore prefix. For `registry_facade`, extract the construction logic into a private `_build_registry_facade()` method or a standalone factory function.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Most modules separate domain logic from plumbing cleanly. The join inference module (`joins/inference.py`) is pure domain logic with no IO. The diagnostics builders are almost pure DataFusion query construction.

**Findings:**
- `src/semantics/incremental/cdf_runtime.py:306-421` -- `read_cdf_changes` mixes three concerns: (1) resolving CDF inputs and storage, (2) loading the CDF table with fallback, and (3) recording diagnostic artifacts. The function is 115 lines and contains nested try/except blocks with artifact recording in multiple branches.
- `src/semantics/incremental/metadata.py:42-82` and `metadata.py:172-212` -- `write_incremental_metadata` and `write_semantic_diagnostics_snapshots` mix Delta write orchestration with artifact payload construction. The payload construction (lines 57-67, 192-209) could be separated from the persistence mechanics.

**Suggested improvement:**
In `cdf_runtime.py`, separate the diagnostic artifact recording into a post-processing step that receives a structured result object, rather than recording inline in each control flow branch. In `metadata.py`, extract payload construction functions that return pure data, then have thin write orchestrators that call them.

**Effort:** medium
**Risk if unaddressed:** medium -- mixed concerns make the CDF runtime difficult to reason about and test in isolation.

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
Most files have a clear single purpose. `types/core.py` owns semantic type definitions. `joins/strategies.py` owns strategy types.

**Findings:**
- `src/semantics/incremental/metadata.py` changes for four different reasons: (1) incremental metadata snapshot schema changes, (2) CDF cursor snapshot format changes, (3) semantic diagnostics persistence changes, and (4) artifact persistence changes. It contains `write_incremental_metadata`, `write_cdf_cursor_snapshot`, `write_semantic_diagnostics_snapshots`, and `write_incremental_artifacts` -- four distinct write concerns.
- `src/semantics/incremental/state_store.py` has 19 path-returning methods on a single class. While cohesive around "filesystem layout," this class changes whenever any new artifact type is added.

**Suggested improvement:**
Consider splitting `metadata.py` into `metadata_snapshot.py`, `cursor_snapshot.py`, and `artifact_writer.py`. For `state_store.py`, the current approach is acceptable given its role as a single layout authority, but it could benefit from a declarative path registry rather than individual methods.

**Effort:** medium
**Risk if unaddressed:** low -- the current structure works but growth will increase merge conflicts and cognitive load.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The `types/` and `joins/` subsystems are highly cohesive. The `incremental/` subsystem has reasonable internal cohesion but moderate coupling to external systems.

**Findings:**
- `src/semantics/diagnostics/quality_metrics.py:45,202` imports `RELATIONSHIP_SPECS` from `semantics.registry` at function call time. This creates coupling from a diagnostic module to the core spec registry. The diagnostics should ideally receive relationship specs as parameters rather than reaching into the global registry.
- `src/semantics/diagnostics/schema_anomalies.py:12` couples to `quality_metrics._empty_table` -- a cross-module coupling on a private function.
- `src/semantics/incremental/export_builders.py:14-16` imports from both `semantics.catalog.dataset_specs` and `semantics.types`, coupling the incremental module to catalog internals and the type system.

**Suggested improvement:**
Pass `RELATIONSHIP_SPECS` as a parameter to `build_relationship_candidates_view` and `_relationship_diag_schema` rather than importing them. This would also improve testability. Extract `_empty_table` to a proper shared location.

**Effort:** medium
**Risk if unaddressed:** medium -- direct registry access makes it harder to test diagnostics modules independently and creates hidden dependencies.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The stated contract is that these subsystem modules should depend inward on compiler contracts, not vice versa. This is mostly respected, with some notable exceptions.

**Findings:**
- `src/semantics/diagnostics/quality_metrics.py:45,202` -- `build_relationship_candidates_view` and `_relationship_diag_schema` import `RELATIONSHIP_SPECS` from `semantics.registry`. The registry is a core module; diagnostics is a peripheral concern. This inverts the expected dependency direction (peripheral should not reach into core registries at import time).
- `src/semantics/docs/graph_docs.py:40` -- `_explain_payload_from_ir` imports `semantic_ir_for_outputs` from `semantics.compile_context`. The docs subsystem (peripheral) reaches into the compilation context (core). This is a lazy import inside a function, which mitigates the coupling somewhat, but the dependency direction is still inverted.
- `src/semantics/validation/catalog_validation.py:165` -- `_semantic_input_column_specs` imports `SEMANTIC_TABLE_SPECS` from `semantics.registry`. Again, validation (peripheral) reaches into the core registry.
- `src/semantics/validation/policy.py:64` -- `_validate_semantic_input_presence` imports `SEMANTIC_INPUT_SPECS` from `semantics.input_registry`.

**Suggested improvement:**
For diagnostics and validation: inject the needed specs as parameters or provide a lightweight protocol that the caller satisfies. For docs: accept the explain payload as a required parameter rather than falling back to IR construction internally, or move the fallback logic to the caller.

**Effort:** medium
**Risk if unaddressed:** high -- these inversions mean changes to the core registry or input specs can unexpectedly break diagnostic, validation, and documentation modules. As the codebase grows, these hidden dependencies become harder to trace.

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
The `DeltaCdfPort` protocol in `src/semantics/incremental/delta_port.py:16-58` is a textbook port definition. `DeltaServiceCdfPort` at `delta_port.py:62-141` is a clean adapter backed by `DeltaService`. The `_OverrideDeltaCdfPort` in `cdf_reader.py:133-197` provides a test-friendly decorator pattern.

**Findings:**
No significant gaps. The port/adapter boundary is explicit and well-defined.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
There is meaningful semantic duplication across the diagnostics subsystem where the same pattern is repeated in four modules.

**Findings:**
- The pattern `table_names_snapshot(ctx)` + check-table-existence + `stage_span` wrapper is repeated in:
  - `src/semantics/diagnostics/ambiguity.py:31-38` (`build_ambiguity_analysis`)
  - `src/semantics/diagnostics/coverage.py:32-45` (`build_file_coverage_report`)
  - `src/semantics/diagnostics/quality_metrics.py:113-121` (`build_relationship_quality_metrics`)
  - `src/semantics/diagnostics/quality_metrics.py:195-200` (`build_relationship_candidates_view`)
  - `src/semantics/diagnostics/schema_anomalies.py:37-42` (`build_schema_anomalies_view`)
  All five functions begin with `with stage_span(...)` + `available = table_names_snapshot(ctx)` + table existence check. This is the same knowledge: "diagnostic builders must discover available tables and trace their execution."
- The `SCOPE_SEMANTICS` constant is imported identically in all four diagnostics files and all five span calls use `stage="semantics"`.
- `_optional_col` in `quality_metrics.py:37-41` implements the same nullable-column-coercion pattern that exists elsewhere in the codebase, but is defined locally without reuse.

**Suggested improvement:**
Create a small shared diagnostic builder base or decorator that handles the `stage_span` + `table_names_snapshot` boilerplate. For example:
```python
def _diagnostic_builder(name: str):
    def decorator(fn):
        def wrapper(ctx, *args, **kwargs):
            with stage_span(f"semantics.{name}", stage="semantics", scope_name=SCOPE_SEMANTICS):
                available = table_names_snapshot(ctx)
                return fn(ctx, available, *args, **kwargs)
        return wrapper
    return decorator
```
This would eliminate the repeated tracing + discovery boilerplate while keeping each builder's query logic focused.

**Effort:** medium
**Risk if unaddressed:** medium -- any change to the diagnostic tracing convention (scope name, stage attribute, table discovery method) requires updates in 5+ locations. This has already led to slight inconsistencies in the `attributes=` kwargs.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Several types use `__post_init__` to enforce preconditions. `JoinStrategy` validates key presence and confidence bounds at `src/semantics/joins/strategies.py:66-80`. `SemanticIncrementalConfig` validates that `state_dir` is set when `enabled=True` at `src/semantics/incremental/config.py:104-112`.

**Findings:**
- `src/semantics/incremental/cdf_reader.py:113-123` -- `CdfReadOptions` has a deferred cross-field validation (`cursor_store` requires `dataset_name`) that is caught in `_validate_cdf_options` at line 126-129 rather than in the type's own construction. This means an invalid `CdfReadOptions` can exist until `read_cdf_changes` is called.
- `src/semantics/incremental/cdf_types.py:33-53` -- `CdfChangeType.from_cdf_column` returns `None` for unrecognized values rather than raising. This is appropriate for graceful degradation but the postcondition (may return None) is only documented, not enforced structurally.

**Suggested improvement:**
Add `__post_init__` to `CdfReadOptions` (or convert it to a `dataclass` with validation) that raises `ValueError` if `cursor_store` is set without `dataset_name`. This moves the invariant enforcement to construction time.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The semantic type system uses `infer_semantic_type()` and `AnnotatedSchema.from_arrow_schema()` to parse raw schemas into structured representations once at the boundary. This is good parse-don't-validate practice.

**Findings:**
- `src/semantics/incremental/cdf_types.py:33-53` -- `CdfChangeType.from_cdf_column` returns `CdfChangeType | None`. The caller must repeatedly handle the `None` case. A parsed enum variant would be more robust (e.g., an `UNKNOWN` variant that can be filtered structurally).
- `src/semantics/diagnostics/coverage.py:40-45` -- Fallback table name resolution is done by iterating through string names and checking existence, rather than parsing the available-table set into a structured representation once.

**Suggested improvement:**
Add an `UNKNOWN` variant to `CdfChangeType` and have `from_cdf_column` always return a valid enum value. Callers can then use pattern matching on the enum rather than `None` checks.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most types use `frozen=True` dataclasses to prevent mutation. `CdfCursor` enforces immutability via custom `__setattr__` despite being a `msgspec.Struct` with `frozen=False`. `JoinStrategy` enforces non-empty keys and bounded confidence.

**Findings:**
- `src/semantics/incremental/cdf_reader.py:73-123` -- `CdfReadOptions` allows `cursor_store=SomeStore, dataset_name=None` which is an invalid combination. This should be unrepresentable by construction. A union type or paired dataclass would prevent this.
- `src/semantics/incremental/cdf_cursors.py:26-29` -- `CdfCursor` is declared as `msgspec.Struct, frozen=False` but then overrides `__setattr__` to raise `FrozenInstanceError`. This is an unusual pattern; the intent is immutability but the mechanism fights the framework's own `frozen` parameter.
- `src/semantics/validation/catalog_validation.py:36` -- `SEMANTIC_INPUT_COLUMN_SPECS: Final[tuple[ColumnValidationSpec, ...]] = ()` is always empty. It is exported in `__all__` but never populated. This is a dead constant that suggests a state that was intended but never realized.

**Suggested improvement:**
For `CdfReadOptions`, split into two types: `CdfReadOptionsWithCursor(cursor_store: CdfCursorStore, dataset_name: str, ...)` and `CdfReadOptionsBasic(...)`, or use a `CursorConfig` sub-struct that bundles both fields. For `CdfCursor`, use `frozen=True` on the `msgspec.Struct` directly. Remove or mark `SEMANTIC_INPUT_COLUMN_SPECS` as deprecated.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions are either queries or commands. The diagnostic builders are pure queries that return DataFrames. The write helpers are pure commands.

**Findings:**
- `src/semantics/incremental/cdf_cursors.py:248-281` -- `CdfCursorStore.update_version` creates a cursor, saves it to disk (command), and returns the cursor (query). This mixes mutation with return.
- `src/semantics/incremental/cdf_runtime.py:306-421` -- `read_cdf_changes` reads CDF data, updates the cursor store (side effect), and records diagnostic artifacts (side effect), then returns a result. Three concerns in one function.

**Suggested improvement:**
Split `update_version` into `save_cursor(cursor)` (command, already exists) and a separate cursor construction factory. The caller would construct the cursor and then save it. For `read_cdf_changes`, separate cursor updates and artifact recording from the read logic.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`IncrementalRuntime.build()` is a clean factory. The `CdfReadOptions` uses callable overrides for testing seams, which is a form of DI.

**Findings:**
- `src/semantics/incremental/runtime.py:86-122` -- `IncrementalRuntime.registry_facade` lazily constructs a complex object graph inline (creating `DatasetCatalog`, `ProviderRegistry`, `UdfCatalogAdapter`, `RegistryFacade`) rather than receiving these as injected dependencies or using an explicit composition root. This makes it harder to test `IncrementalRuntime` with alternative registry configurations.

**Suggested improvement:**
Accept an optional `RegistryFacade` or `RegistryFacadeFactory` in `IncrementalRuntimeBuildRequest`, with the current lazy construction as the default.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in any of the reviewed subsystems. All behavior extension is via composition (dataclass fields, protocol implementations). `CdfFilterPolicy` uses `FingerprintableConfig` mixin but this is a lightweight `dataclass` composition, not deep inheritance.

**Findings:**
No issues.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most access paths are short. However, the pattern `runtime.session_runtime().ctx` appears frequently.

**Findings:**
- `src/semantics/incremental/write_helpers.py:67-69` -- `runtime.session_runtime()` then `session_runtime.ctx` (two-hop access)
- `src/semantics/incremental/plan_bundle_exec.py:62` -- `runtime.session_runtime().ctx`
- `src/semantics/incremental/delta_context.py:186,254` -- `context.runtime.session_runtime().ctx`
- `src/semantics/incremental/cdf_runtime.py:134` -- `runtime.session_runtime().ctx`
- The chain `context.runtime.profile.view_registry_snapshot()` appears in `metadata.py:246`.
- `runtime.profile.diagnostics.diagnostics_sink` in `runtime.py:168` is a 3-level access.

**Suggested improvement:**
`IncrementalRuntime` already provides `session_context()` as a shorthand at `runtime.py:134-142`. Callers should prefer this. For `profile.diagnostics.diagnostics_sink`, the existing `diagnostics_sink()` method at `runtime.py:166-168` already wraps it.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The diagnostics builders ask `ctx` for schema information and then make branching decisions externally. This is a natural consequence of the DataFusion API, where builders must discover the available schema before constructing queries.

**Findings:**
- `src/semantics/diagnostics/ambiguity.py:42-47` -- Fetches `rel_df.schema().names`, checks for specific columns, then branches. The relationship DataFrame is asked for its shape rather than told what to produce.
- `src/semantics/diagnostics/coverage.py:48-95` -- Repeated pattern of checking table existence and conditionally joining. The builder asks the context "what do you have?" rather than being told "here are your inputs."

**Suggested improvement:**
This is partially inherent to the DataFusion query-builder pattern. A partial mitigation would be to create typed input descriptors that pre-resolve which tables and columns are available, passed as parameters to the builders.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Join inference (`joins/inference.py`) and the type system (`types/core.py`) are purely functional with no IO. The plan fingerprinting (`plans/fingerprints.py`) is mostly pure, with IO only in the Substrait serialization fallback. The incremental subsystem mixes IO (Delta reads/writes) throughout.

**Findings:**
- `src/semantics/incremental/cdf_runtime.py` -- The `read_cdf_changes` function interleaves pure logic (cursor resolution, CDF option construction) with impure operations (Delta reads, cursor persistence, artifact recording). The pure logic could be extracted.
- `src/semantics/incremental/metadata.py` -- Write functions are inherently impure but the payload construction mixed in could be extracted as pure functions.
- `src/semantics/docs/graph_docs.py:39-67` -- `_explain_payload_from_ir` lazily imports from `compile_context` and invokes the semantic IR builder, which is a heavy operation with side effects. This is hidden inside what appears to be a documentation utility.

**Suggested improvement:**
In `cdf_runtime.py`, extract cursor resolution and CDF option construction into pure helper functions. In `graph_docs.py`, make the IR-based fallback explicit by requiring the caller to provide the payload rather than silently invoking the compiler.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Delta writes in the incremental subsystem use `WriteMode.OVERWRITE` with `schema_mode="overwrite"`, making them inherently idempotent. Cursor updates are set-last-version operations that produce the same state regardless of repetition. Plan fingerprint persistence overwrites the entire snapshot.

**Findings:**
No issues.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
`PlanFingerprint` uses SHA256 hashes of logical plans and schemas. Plan fingerprint snapshots are sorted by name before persistence at `src/semantics/incremental/plan_fingerprints.py:147`. Join strategy resolution follows a deterministic priority order. The type inference system uses a deterministic pattern-matching order.

**Findings:**
No issues.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most subsystems are straightforward. The type system is simple and effective. The join inference, while having many helper functions, follows a clear priority chain.

**Findings:**
- `src/semantics/incremental/cdf_reader.py:133-223` -- `_OverrideDeltaCdfPort` is a 90-line wrapper class that delegates to a base `DeltaCdfPort` with optional function overrides. This is used solely for testing seams. The same could be achieved more simply with `monkeypatch` or a direct test double.
- `src/semantics/plans/fingerprints.py:257-313` -- The Substrait encoding logic tries three paths (`_internal_substrait_bytes`, `_public_substrait_bytes`, direct `encode`) with extensive `getattr`/`callable` checks. This defensive approach is necessary for API compatibility but adds significant complexity.

**Suggested improvement:**
Consider whether `_OverrideDeltaCdfPort` can be replaced by passing the port directly (callers already construct the port). If testing requires overrides, create a simple `FakeDeltaCdfPort` test double instead.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most modules are lean and justified by current usage.

**Findings:**
- `src/semantics/diagnostics/quality_metrics.py:224-235` -- `build_relationship_decisions_view` is a function that wraps `build_relationship_candidates_view` with no additional logic (it calls `return build_relationship_candidates_view(ctx)` at line 235). This is a placeholder for future divergence that currently adds no value but creates the illusion of distinct functionality.
- `src/semantics/validation/catalog_validation.py:36` -- `SEMANTIC_INPUT_COLUMN_SPECS: Final[tuple[ColumnValidationSpec, ...]] = ()` is always empty. It is re-exported in `__init__.py:6` and `__all__` but never populated. This appears to be a speculative extension point.

**Suggested improvement:**
Either implement `build_relationship_decisions_view` with distinct logic or remove it and merge its registration entry with the candidates view. Remove or deprecate `SEMANTIC_INPUT_COLUMN_SPECS` if it has no current consumers.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. Factory methods like `CdfFilterPolicy.include_all()` and `SemanticIncrementalConfig.with_cdf_enabled()` are well-named.

**Findings:**
- `src/semantics/validation/catalog_validation.py:36` -- A public constant named `SEMANTIC_INPUT_COLUMN_SPECS` that is always an empty tuple is surprising. A reader would expect it to contain actual specs.
- `src/semantics/incremental/cdf_cursors.py:26-29` -- `CdfCursor` declared with `frozen=False` but actually frozen via `__setattr__` override is surprising. The `msgspec.Struct` `frozen` parameter should match the actual behavior.

**Suggested improvement:**
Remove `SEMANTIC_INPUT_COLUMN_SPECS` or populate it. Use `frozen=True` on `CdfCursor`'s `msgspec.Struct` declaration if the intent is immutability.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
All modules define `__all__`. Version constants exist (`PLAN_FINGERPRINTS_VERSION = 5`). Schema constants use `pa.schema()` definitions.

**Findings:**
- `src/semantics/diagnostics/quality_metrics.py:23` -- `_empty_table` is underscore-prefixed (indicating private) but is imported by `schema_anomalies.py:12`. This crosses the module's public contract boundary.
- `src/semantics/diagnostics/quality_metrics.py:70` -- `_RELATIONSHIP_DIAG_SCHEMA` is computed at module load time from `RELATIONSHIP_SPECS`. This means the schema is implicitly versioned by the registry's state at import time, with no explicit version marker.

**Suggested improvement:**
Make `_empty_table` public (rename to `empty_diagnostic_table` or similar) and add it to `__all__` in `quality_metrics.py`, or move it to the diagnostics `__init__.py`. Add a version constant to the diagnostic schema if it is persisted.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The `CdfReadOptions` pattern with override callables (`delta_table_version_fn`, etc.) provides good testing seams. The pure functions in `types/` and `joins/` are trivially testable.

**Findings:**
- `src/semantics/validation/catalog_validation.py:163-196` -- `_semantic_input_column_specs` accesses `SEMANTIC_TABLE_SPECS` from the global registry and calls `extract_schema_for`, making it impossible to test validation logic in isolation without the full registry being populated.
- `src/semantics/diagnostics/quality_metrics.py:70` -- `_RELATIONSHIP_DIAG_SCHEMA` is computed at import time from `RELATIONSHIP_SPECS`, making it untestable without the registry.

**Suggested improvement:**
Accept specs as parameters in `_semantic_input_column_specs` and pass `RELATIONSHIP_SPECS` as a parameter to `_relationship_diag_schema`. This allows unit tests to provide minimal test specs.

**Effort:** small
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Every diagnostic builder is instrumented with `stage_span` from `obs.otel.tracing`. The spans use structured attributes (`codeanatomy.relationship`, `codeanatomy.base_table`, `codeanatomy.relationship_count`). CDF read operations record detailed artifacts with status, version ranges, change counts, and filter policies via `_record_cdf_read` in `cdf_runtime.py:424-456`. Incremental metadata writes record fingerprints and settings hashes.

**Findings:**
No significant gaps. The observability coverage is thorough and consistent.

---

## Cross-Cutting Themes

### Theme 1: Peripheral modules reaching into core registries

**Affected principles:** P5 (dependency direction), P4 (coupling), P23 (testability)

Three subsystems (`diagnostics`, `validation`, `docs`) import from core modules (`semantics.registry`, `semantics.input_registry`, `semantics.compile_context`) to discover specs, table mappings, and IR state. This creates a bidirectional dependency pattern where peripheral modules are coupled to core internals.

**Root cause:** These modules were designed to be self-contained but need configuration data that lives in centralized registries. Rather than the registries pushing configuration outward, the peripherals pull it inward.

**Suggested approach:** Adopt a "pass the data you need" pattern: callers (pipeline orchestrators) resolve registry data and pass it as parameters to diagnostic/validation/docs functions. This preserves the one-way dependency (core -> orchestrator -> peripheral) and improves testability.

### Theme 2: Diagnostic builder boilerplate duplication

**Affected principles:** P7 (DRY), P2 (separation of concerns), P1 (information hiding)

The five diagnostic builder functions repeat the same discovery-and-tracing boilerplate. Additionally, the `_empty_table` helper crosses a module boundary via private import. This pattern suggests a missing abstraction: a diagnostic builder framework that handles discovery, tracing, and empty-table fallback.

**Root cause:** Each diagnostic builder was likely developed independently and the shared pattern was not refactored into a common abstraction.

**Suggested approach:** Create a `DiagnosticBuilderContext` or decorator that encapsulates `table_names_snapshot`, `stage_span`, and empty-table fallback. Each builder then focuses only on its unique query logic.

### Theme 3: CdfCursor immutability confusion

**Affected principles:** P10 (illegal states), P21 (least astonishment)

`CdfCursor` is declared as `msgspec.Struct, frozen=False` but manually prevents mutation via `__setattr__`. This fighting-the-framework pattern suggests either a msgspec limitation at the time of writing or an oversight.

**Root cause:** The `msgspec.Struct` may have had limitations with `frozen=True` and `kw_only=True` together, or the `forbid_unknown_fields=False` may have been incompatible. The workaround achieves the goal but is confusing.

**Suggested approach:** Test whether `frozen=True` works with the current `msgspec` version and flags. If so, switch. If not, add a clear comment explaining the workaround.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract diagnostic builder boilerplate into shared decorator/context | medium | Eliminates 5-way duplication; single tracing convention change point |
| 2 | P1/P22 (hiding/contracts) | Make `_empty_table` a proper public utility in diagnostics | small | Removes private cross-module import; clarifies contract |
| 3 | P20/P21 (YAGNI/astonishment) | Remove dead `SEMANTIC_INPUT_COLUMN_SPECS` constant and `build_relationship_decisions_view` alias | small | Removes misleading public API surface |
| 4 | P5 (dependency direction) | Pass `RELATIONSHIP_SPECS` as parameter to diagnostic builders | small | Fixes dependency inversion; improves testability |
| 5 | P10/P21 (illegal states/astonishment) | Fix `CdfCursor` `frozen` declaration to match actual behavior | small | Removes framework-fighting pattern; reduces reader confusion |

## Recommended Action Sequence

1. **Extract `_empty_table` to a shared diagnostic utility** (P1, P4, P22). Create `src/semantics/diagnostics/_utils.py` with a public `empty_diagnostic_frame` function. Update imports in `quality_metrics.py` and `schema_anomalies.py`. This is a prerequisite for the next step.

2. **Create a diagnostic builder abstraction** (P7, P2). Implement a `@diagnostic_builder` decorator or `DiagnosticBuilderContext` that handles `stage_span` + `table_names_snapshot` boilerplate. Refactor all five builder functions to use it. This depends on step 1 for the shared utility.

3. **Fix dependency direction in diagnostics** (P5, P4, P23). Change `build_relationship_candidates_view` and `_relationship_diag_schema` to accept specs as a parameter. Update callers in `__init__.py` to pass `RELATIONSHIP_SPECS`. Similarly parameterize `_semantic_input_column_specs` in validation.

4. **Clean up dead API surface** (P20, P21). Remove `SEMANTIC_INPUT_COLUMN_SPECS` or mark it deprecated. Either give `build_relationship_decisions_view` its own logic or remove it and merge its registration.

5. **Fix CdfCursor immutability** (P10, P21). Switch `CdfCursor` to `frozen=True` on the `msgspec.Struct` declaration, removing the custom `__setattr__` override.

6. **Add construction-time validation to CdfReadOptions** (P8, P10). Move the `cursor_store requires dataset_name` check from `_validate_cdf_options` into `__post_init__`.

7. **Separate concerns in `cdf_runtime.read_cdf_changes`** (P2, P16). Extract cursor resolution and CDF option construction into pure helpers. Move artifact recording to a post-processing step.

8. **Address `metadata.py` SRP** (P3). Split into focused write modules if additional artifact types are added. This can wait until the next growth episode.
