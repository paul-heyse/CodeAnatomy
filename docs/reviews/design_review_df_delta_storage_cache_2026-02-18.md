# Design Review: DataFusion Delta, Plan, Lineage, and I/O Layer

**Date:** 2026-02-18
**Scope:** `src/datafusion_engine/delta/`, `src/datafusion_engine/plan/`, `src/datafusion_engine/lineage/`, `src/datafusion_engine/io/`, `src/datafusion_engine/dataset/`, `src/datafusion_engine/arrow/`, `src/datafusion_engine/obs/`, `src/datafusion_engine/pruning/`, `src/datafusion_engine/cache/`, `src/datafusion_engine/symtable/`
**Focus:** All principles (1-24)
**Depth:** deep (all files)
**Files reviewed:** 132 Python files, ~35,900 lines of code

---

## Executive Summary

The DataFusion delta/plan/lineage/IO layer is architecturally mature. The Ports & Adapters pattern is applied deliberately across the delta control plane, the diagnostics system uses a clean protocol-based sink, and plan fingerprinting exhibits strong determinism. The most significant gaps are concentrated in three areas: (1) a predicate-correctness risk in `DeltaDeleteRequest` that allows an empty string predicate through the type system (prior flagged risk, not yet resolved), (2) DRY violations in the I/O write path where error markers and observability helpers are duplicated between `write_pipeline.py` and `write_execution.py`, and (3) the `DeltaService` class (904 lines) carrying three distinct responsibilities—provider construction, feature mutation, and observability—that should be separated. Testability is constrained by widespread `SessionContext` injection requirements and `Path.cwd()` side effects at module load time in three constants modules.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | — | — | Internals well-hidden; `_internal_ctx`, `_require_internal_entrypoint` are private |
| 2 | Separation of concerns | 2 | medium | medium | `DeltaService` (904 lines) mixes provider, feature mutation, and observability concerns |
| 3 | SRP | 2 | medium | medium | `delta/service.py` and `io/write_pipeline.py` each carry multiple orthogonal responsibilities |
| 4 | High cohesion, low coupling | 2 | small | low | `WritePipeline` delegates to handler modules, but retains 30+ forwarder methods |
| 5 | Dependency direction | 3 | — | — | Core logic (fingerprinting, lineage) depends on nothing; adapters depend on core |
| 6 | Ports & Adapters | 3 | — | — | `DiagnosticsSink`, `_WriterPort`, `DeltaServicePort`, `LineageScan` protocols are clean |
| 7 | DRY | 1 | small | medium | `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` and `_is_delta_observability_operation` duplicated between `write_pipeline.py` and `write_execution.py`; 16 identical `table_ref` property bodies in `control_plane_types.py` |
| 8 | Design by contract | 2 | small | high | `DeltaDeleteRequest.predicate: str` allows empty string; guard in mutation layer only, not at construction; `DeltaUpdateRequest.predicate: str | None` is correctly nullable by contrast |
| 9 | Parse, don't validate | 2 | small | low | `_delta_add_actions_payload` re-parses snapshot mappings every call; regex filter parsing in `scheduling.py` is ad-hoc string regex rather than structured types |
| 10 | Make illegal states unrepresentable | 1 | medium | high | `DeltaDeleteRequest.predicate` is `str` (allows `""`) but semantically must be non-empty; `DeltaMutationRequest` (merge or delete) relies on runtime `.validate()` rather than sum-type encoding |
| 11 | CQS | 2 | small | low | `delta_write_ipc`, `delta_delete` etc. return mutation reports; this is a pragmatic response payload pattern and is consistent—minor violation only |
| 12 | Dependency inversion + explicit composition | 3 | — | — | `DiagnosticsRecorder` injected; `WritePipeline` takes `runtime_profile` and `recorder`; `DeltaService` takes `profile` |
| 13 | Composition over inheritance | 3 | — | — | No deep inheritance; `DeltaFeatureOps` wraps `DeltaService` by composition |
| 14 | Law of Demeter | 2 | small | low | `WritePipeline._match_dataset_location` accesses `self.runtime_profile.policies.delta_store_policy`; `_record_scan_plan_artifact` accesses `request.runtime_profile.policies.delta_protocol_support` |
| 15 | Tell, don't ask | 2 | small | low | `_record_scan_plan_artifact` in `scheduling.py` recomputes `delta_protocol_compatibility` locally rather than asking the already-resolved `DeltaScanResolution` object to emit its observability payload |
| 16 | Functional core, imperative shell | 2 | small | low | `_scan_unit_key`, `compute_plan_fingerprint`, `extract_lineage` are pure; `plan_scan_unit` mixes span emission with pure key computation |
| 17 | Idempotency | 3 | — | — | Idempotent write options, commit key, and `IdempotentWriteOptions` are explicit and threaded correctly |
| 18 | Determinism / reproducibility | 3 | — | — | Plan fingerprinting via Substrait bytes + UDF snapshot hash is robust; scan unit keys are content-addressed |
| 19 | KISS | 2 | medium | low | `control_plane_core.py` lazy-export machinery (`_LazyExportProxy`, `_LAZY_EXPORT_MODULES`, `__getattr__`) adds ~100 lines of complexity; simpler alternatives exist |
| 20 | YAGNI | 2 | small | low | `WritePipeline.write` just calls `write_via_streaming`—the two-method split anticipates a legacy path that no longer exists |
| 21 | Least astonishment | 2 | small | medium | `DeltaDeleteRequest.predicate: str` suggests any string is acceptable; callers must know to check for empty; the mutation guard is non-obvious |
| 22 | Public contracts | 3 | — | — | `__all__` exported consistently; versioned table names (`_v2`, `_v10`) are explicit |
| 23 | Design for testability | 1 | medium | medium | `Path.cwd()` at module load in three constants files (`artifact_store_constants.py:18`, `cache/inventory.py:42`, `delta/obs_table_manager.py:38`) makes defaults non-reproducible in tests; `WritePipeline` requires live `SessionContext` for all paths |
| 24 | Observability | 2 | small | low | `DiagnosticsRecorder` is well-designed; `_OBS_SESSION_ID` in `obs/diagnostics_bridge.py:37` is a module-level UUID fixed at import time, which means all bridge calls within a session share the same ID rather than one per logical operation |

---

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding — Alignment: 3/3

The delta control-plane internals (`_internal_ctx`, `_require_internal_entrypoint`, `_LazyExportProxy`) are all named-private and not in `__all__`. The Rust FFI boundary is encapsulated behind `RustDeltaEntrypoint` and `InternalSessionContext` protocols. No findings — well aligned.

---

#### P2. Separation of concerns — Alignment: 2/3

**Current state:**
`src/datafusion_engine/delta/service.py` (904 lines) carries at least three distinct concerns: (1) Delta table provider construction (`resolve_provider`), (2) Delta feature mutation (`enable_change_data_feed`, `enable_deletion_vectors`, etc. via `DeltaFeatureOps`), and (3) observability recording (`_record_provider_artifact`, `provider_artifact_payload`). These change for different reasons: provider construction changes when the scan API changes; feature mutation changes when the Delta spec adds new features; observability changes when the artifact schema evolves.

Similarly, `src/datafusion_engine/io/write_pipeline.py` (611 lines) is partly a write orchestrator and partly a thin proxy layer that forwards calls into `delta_write_handler` and `format_write_handler`—the separation is better here since the handlers are split, but the combined surface is still broad.

**Findings:**
- `src/datafusion_engine/delta/service.py:133–296` — `DeltaFeatureOps` contains 12+ feature-enable methods all delegating to `enable_delta_*` functions. This class is a coordinator facade, not a cohesive service. Changes to any Delta feature protocol require touching this facade.
- `src/datafusion_engine/delta/service.py:425–478` — `_record_provider_artifact` and `provider_artifact_payload` embed observability assembly inside the service, mixing domain with diagnostics.

**Suggested improvement:**
Extract `DeltaProviderService` (provider construction only), leave `DeltaFeatureOps` as a standalone coordinator, and move `_record_provider_artifact` into a `DeltaProviderObservabilityAdapter` that wraps `DeltaProviderService`. Each class then changes for one reason.

**Effort:** medium
**Risk if unaddressed:** medium — new Delta features and observability schema changes both require editing the same 900-line file.

---

#### P3. SRP — Alignment: 2/3

**Current state:**
`src/datafusion_engine/delta/service.py` (discussed above) is the primary SRP offender. A secondary concern is `src/datafusion_engine/io/write_pipeline.py`, which is nominally an "orchestrator" but also contains: validation logic (`_validate_dataframe`), dataset-binding logic (`_dataset_location_for_destination`), commit-metadata logic (via delegation), adaptive file-size policy recording, row-counting, and formatting details. It has more than one reason to change.

**Findings:**
- `src/datafusion_engine/delta/service.py:100–350` — provider resolution, feature mutation, and maintenance all co-located.
- `src/datafusion_engine/io/write_pipeline.py:84–98` — `WritePipeline.__init__` accepts four optional collaborators; the class orchestrates dataset resolution, validation, and write execution simultaneously.

**Suggested improvement:**
The `WritePipeline` dataset-binding logic (`_dataset_location_for_destination`, `_match_dataset_location`, `_dataset_binding`) is separable into a `DatasetBindingResolver` helper that can be tested independently of a live session.

**Effort:** medium
**Risk if unaddressed:** medium — the 611-line file is growing as new write paths are added.

---

#### P4. High cohesion, low coupling — Alignment: 2/3

**Current state:**
`WritePipeline` in `write_pipeline.py` delegates all delta-specific logic to `delta_write_handler` and format-specific logic to `format_write_handler`, which is positive. However, the delegation is implemented as 30+ forwarder methods that merely call the same method name on the handler module, passing `self`. This is coupling in disguise: the handler modules expect to receive a `WritePipeline` instance and call methods on it, creating a circular reference pattern between orchestrator and handler.

**Findings:**
- `src/datafusion_engine/io/write_pipeline.py:466–611` — 25+ methods each consisting of a single `return delta_write_handler.<method>(self, ...)` or `format_write_handler.<method>(self, ...)`. The handler modules depend on the pipeline's internal interface.
- `src/datafusion_engine/io/delta_write_handler.py:53–80` — `_DeltaCommitFinalizeContextLike` and `_DeltaBootstrapPipeline` are Protocol definitions inside the handler that re-express `WritePipeline`'s interface, indicating the handler knows the orchestrator's shape.

**Suggested improvement:**
Replace the bidirectional pipeline/handler coupling with a data-in/data-out function design: the handler functions accept explicit parameters (spec, request, profile) rather than the full pipeline object. This eliminates the need for handler-side protocol mirrors.

**Effort:** medium
**Risk if unaddressed:** low — the pattern works, but extending write paths requires editing both the pipeline forwarder and the handler.

---

#### P5. Dependency direction — Alignment: 3/3

Core logic modules (`plan/plan_fingerprint.py`, `lineage/reporting.py`, `arrow/types.py`) depend on nothing from the outer layers. Adapter modules (`io/delta_write_handler.py`, `delta/control_plane_mutation.py`) depend on the core. No violations detected.

---

#### P6. Ports & Adapters — Alignment: 3/3

`DiagnosticsSink` (Protocol), `_WriterPort` (Protocol in `cache/_ports.py`), `DeltaServicePort` (in `delta/service_protocol.py`), `LineageScan` / `LineageRecorder` / `LineageQuery` (in `lineage/protocols.py`), and `InternalSessionContext` / `RustDeltaEntrypoint` (in `delta/protocols.py`) are explicit, typed ports. The `InMemoryDiagnosticsSink` in `lineage/diagnostics.py` is a clean test double. Well aligned.

---

### Category: Knowledge

#### P7. DRY — Alignment: 1/3

**Current state:**
Three distinct duplications exist within the scope, all in the I/O write path.

**Findings:**
1. `src/datafusion_engine/io/write_pipeline.py:56–64` and `src/datafusion_engine/io/write_execution.py:7–25` — `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` (same two-element tuple of lowercase strings) and the `_is_retryable_delta_stream_error` / `is_retryable_delta_stream_error` functions are defined identically in both files. `write_pipeline.py:64` uses its private copy; `write_execution.py:25` re-defines the same logic under a public name.

2. `src/datafusion_engine/io/write_pipeline.py:67–78` and `src/datafusion_engine/io/write_execution.py:28–40` — `_is_delta_observability_operation` (private, in pipeline) and `is_delta_observability_operation` (public, in execution) are identical implementations checking the same five operation prefixes. `delta_write_handler.py:379` imports the private version from `write_pipeline`, making the public version in `write_execution` dead code.

3. `src/datafusion_engine/delta/control_plane_types.py:59–426` — all 16 request types (e.g., `DeltaSnapshotRequest`, `DeltaProviderRequest`, `DeltaDeleteRequest`, `DeltaOptimizeRequest`, …) repeat the same `table_ref` property body verbatim:
   ```python
   @property
   def table_ref(self) -> DeltaTableRef:
       return DeltaTableRef(
           table_uri=self.table_uri,
           storage_options=self.storage_options,
           version=self.version,
           timestamp=self.timestamp,
       )
   ```
   This is the same 6 lines repeated 16 times. A base struct with `table_ref` defined once would eliminate this.

**Suggested improvement:**
1. Delete `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` and `_is_retryable_delta_stream_error` from `write_pipeline.py`; import `is_retryable_delta_stream_error` from `write_execution.py`. Update `delta_write_handler.py` to import from `write_execution` instead of `write_pipeline`.
2. Same consolidation for the observability operation helper: use `write_execution.is_delta_observability_operation` everywhere; remove the duplicate from `write_pipeline.py`.
3. For `table_ref`: introduce `_DeltaTableRefMixin(StructBaseStrict)` with the `table_ref` property, and have each request class inherit or compose it. Alternatively, write a `table_ref_from_request(req)` function at module level.

**Effort:** small
**Risk if unaddressed:** medium — the retry markers are safety-critical (they prevent bad retries on data-interface errors). If one copy is updated and the other is not, behaviour diverges silently.

---

#### P8. Design by contract — Alignment: 2/3

**Current state:**
The mutation layer has an explicit guard for empty delete predicates in `control_plane_mutation.py:23–31` (`_require_non_empty_delete_predicate`). This is good. However, the guard lives in the call-path rather than at construction time, which means `DeltaDeleteRequest` objects with `predicate=""` can be created, serialised, and passed across boundaries before the error is raised.

**Findings:**
- `src/datafusion_engine/delta/control_plane_types.py:146–166` — `DeltaDeleteRequest.predicate: str` accepts any string. An empty string (`""`) is rejected by `delta_delete()` but is constructible and msgpack-encodable. The contract that "predicate must be non-empty" is only declared in the call path, not enforced at the struct boundary.
- `src/datafusion_engine/delta/control_plane_types.py:176` — `DeltaUpdateRequest.predicate: str | None` is correctly typed: `None` means "no filter (update all)", which is semantically valid. The asymmetry between delete and update predicate semantics is correct but adds cognitive load.

**Suggested improvement:**
Add a `__post_init__` guard (following the pattern already used in `DeltaWriteRequest:131–133` for `data_ipc`) to `DeltaDeleteRequest`:
```python
def __post_init__(self) -> None:
    if not self.predicate.strip():
        msg = "DeltaDeleteRequest.predicate must be non-empty."
        raise ValueError(msg)
```
This makes the contract enforceable at construction rather than only at invocation.

**Effort:** small
**Risk if unaddressed:** high — a serialisation-deserialization round-trip of a `DeltaDeleteRequest` with `predicate=""` would pass the type checker and msgpack decode without error, reaching Rust with a semantically invalid payload.

---

#### P9. Parse, don't validate — Alignment: 2/3

**Current state:**
The lineage reporting layer (`lineage/reporting.py`) correctly parses raw Rust payloads into typed `ScanLineage`, `JoinLineage`, and `ExprInfo` structs at the boundary. The `_normalize_scans` and `_normalize_required_columns` functions are proper parse-once patterns.

The weaker point is in `scheduling.py`, which parses filter predicates as free-text strings using regex (`_EQUALS_FILTER_PATTERN`, `_COMPARISON_FILTER_PATTERN`) inside `_partition_filters_from_strings` and `_stats_filters_from_strings`. These regexes match column names like `col.qualified.name` but apply a `.split(".")[-1]` suffix hack to extract the simple column name. This means a filter string `"outer.inner.col = 1"` is silently treated as `col = 1`, which may give wrong results for nested structures.

**Findings:**
- `src/datafusion_engine/lineage/scheduling.py:52–57` — two regex patterns for filter parsing are defined; they are used to re-parse strings that arrived as structured data from the DataFusion plan walker.
- `src/datafusion_engine/lineage/scheduling.py:744` — `.split(".")[-1]` suffix extraction for qualified column names is a lossy parse.

**Suggested improvement:**
Replace the regex-based filter string parsing with structured `PartitionFilter` and `StatsFilter` construction at the lineage-extraction stage (in the Rust walker or `extract_lineage`). By the time `plan_scan_unit` is called, filters should already be structured, not re-parsed from strings.

**Effort:** medium (requires Rust walker output change)
**Risk if unaddressed:** low in normal cases, medium for deeply qualified column names.

---

#### P10. Make illegal states unrepresentable — Alignment: 1/3

**Current state:**
Two structural gaps exist, one high-risk and one moderate.

**Findings:**
1. `src/datafusion_engine/delta/control_plane_types.py:146–153` — `DeltaDeleteRequest.predicate: str` (discussed under P8). A non-empty string is a semantic precondition, not a type-level constraint.

2. `src/datafusion_engine/delta/service.py:81–99` — `DeltaMutationRequest` has two optional fields (`merge`, `delete`), both `| None`. The valid states are exactly two: `(merge=X, delete=None)` or `(merge=None, delete=X)`. The current encoding allows `(None, None)` and `(X, X)`, which are caught only by `validate()`. A union type (`MergeRequest | DeleteRequest`) or `Literal` discriminant would eliminate the invalid combinations.

**Suggested improvement:**
For `DeltaMutationRequest`: replace with a `DeltaMutation = DeltaMergeArrowRequest | DeltaDeleteWhereRequest` type alias and accept it directly at call sites that currently accept `DeltaMutationRequest`. The `.validate()` method and its branches disappear.

**Effort:** medium
**Risk if unaddressed:** high for the delete predicate; medium for the mutation union (the `.validate()` call is tested and consistent, but defensive programming is required at every consumer).

---

#### P11. CQS — Alignment: 2/3

**Current state:**
The delta mutation functions (`delta_write_ipc`, `delta_delete`, `delta_update`, `delta_merge`) return response payloads while also changing Delta table state. This is pragmatic—the response carries the new table version and commit metadata needed for downstream use—but it is a CQS violation. The pattern is consistent throughout the codebase, which limits the blast radius.

**Findings:**
- `src/datafusion_engine/delta/control_plane_mutation.py:34–118` — all four mutation functions return `Mapping[str, object]` while performing irreversible table mutations.
- `src/datafusion_engine/lineage/diagnostics.py:312` — `DiagnosticsRecorder.record_artifact` returns `None` (pure command). CQS is correctly applied here.

**Suggested improvement:**
Accept the pattern as a deliberate pragmatic choice given the Rust FFI boundary. The returned mapping is read-only response metadata. No structural change recommended; document the intent in module docstring.

**Effort:** small (documentation only)
**Risk if unaddressed:** low — the pattern is internally consistent.

---

### Category: Composition

#### P12. Dependency inversion + explicit composition — Alignment: 3/3

`WritePipeline` accepts `DiagnosticsRecorder`, `DataFusionRuntimeProfile`, and `ManifestDatasetResolver` at construction. `DeltaService` accepts `DataFusionRuntimeProfile`. `DiagnosticsRecorder` accepts `DiagnosticsSink`. No hidden singletons or module-level globals are used for wiring. The `otel_diagnostics_sink()` factory in `lineage/diagnostics.py:732` is the only static entry point and is not called inside library code. Well aligned.

---

#### P13. Composition over inheritance — Alignment: 3/3

`DeltaFeatureOps` wraps `DeltaService` by composition (`service: DeltaService`). `DiagnosticsRecorderAdapter` wraps `DiagnosticsSink` by composition. No meaningful inheritance hierarchies exist in this scope. Well aligned.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
Several sites navigate two or three levels into a dependency's internals.

**Findings:**
- `src/datafusion_engine/io/write_pipeline.py:215–216` — `self.runtime_profile.policies.delta_store_policy` traverses two levels into the profile.
- `src/datafusion_engine/lineage/scheduling.py:629–657` — `_enforce_protocol_compatibility` accesses `runtime_profile.policies.delta_protocol_mode` and `runtime_profile.record_artifact(...)`, traversing two levels.
- `src/datafusion_engine/lineage/scheduling.py:687–691` — `_record_scan_plan_artifact` accesses `request.runtime_profile.policies.delta_protocol_support`, three levels deep.
- `src/datafusion_engine/delta/service.py:435–436` — `self.profile.diagnostics.diagnostics_sink` is two levels deep.

**Suggested improvement:**
`DataFusionRuntimeProfile` should expose convenience properties for the most-commonly traversed paths: e.g., `profile.delta_store_policy` instead of `profile.policies.delta_store_policy`, and `profile.delta_protocol_support` instead of `profile.policies.delta_protocol_support`. This is a single-method addition per property, keeps the profile as the stable interface, and eliminates multi-level navigation at call sites.

**Effort:** small
**Risk if unaddressed:** low — changes to `policies` internals currently require updating every traversal site.

---

#### P15. Tell, don't ask — Alignment: 2/3

**Current state:**
`_record_scan_plan_artifact` in `scheduling.py` re-invokes `delta_protocol_compatibility(request.payload.delta_protocol, ...)` after `plan_scan_unit` has already computed `protocol_compatibility` in `_DeltaScanResolution`. The artifact-recording code "asks" for data that is already available in the resolved object.

**Findings:**
- `src/datafusion_engine/lineage/scheduling.py:687–692` — recomputes `compatibility = delta_protocol_compatibility(...)` when the result is already present in the `_DeltaScanResolution` that triggered this function.
- `src/datafusion_engine/lineage/scheduling.py:360–377` — `_DeltaAddActionsIndexProvider.select_candidates` builds an `ExternalIndexSelection.metadata` dict and the caller unpacks it with `.get(...)`. The metadata dict is loosely typed `Mapping[str, object]`; a typed `DeltaScanMetadata` struct would "tell" rather than requiring callers to "ask".

**Suggested improvement:**
Pass the already-resolved `DeltaProtocolCompatibility` into `_record_scan_plan_artifact` rather than recomputing it. Add a typed result struct for `ExternalIndexSelection.metadata` to avoid key-string access at the call site.

**Effort:** small
**Risk if unaddressed:** low — but the double computation adds latency and cognitive load.

---

### Category: Correctness

#### P16. Functional core, imperative shell — Alignment: 2/3

**Current state:**
The plan/lineage core is well-structured: `compute_plan_fingerprint`, `extract_lineage`, `_scan_unit_key`, and `_parse_filter_value` are all pure functions. The delta mutation functions are necessarily imperative.

The gap is in `plan_scan_unit` (`scheduling.py:156–223`), which wraps a `stage_span(...)` context manager (IO/tracing side effect) around what is otherwise mostly a pure key-computation and assembly operation. The span is mixed into the same function as the data transformation.

**Findings:**
- `src/datafusion_engine/lineage/scheduling.py:171–223` — `plan_scan_unit` combines: span emission, delta scan resolution (IO: calls Rust via `delta_add_actions`), config snapshot computation, key construction, and `ScanUnit` assembly. The pure assembly (`_scan_unit_key`, `ScanUnit(...)`) is entangled with span emission.
- `src/datafusion_engine/obs/metrics_bridge.py` (600 lines) — this module is largely a collection of pure schema transformations (schema computation, stats aggregation) but imports `pyarrow.dataset` and `pyarrow.parquet` at module level, mixing schema-query concerns with scan I/O.

**Suggested improvement:**
In `plan_scan_unit`, extract a pure `_assemble_scan_unit(...)` function that accepts all resolved inputs and returns a `ScanUnit`, then wrap it with the span and IO calls. This makes the assembly logic independently testable without tracing infrastructure.

**Effort:** small
**Risk if unaddressed:** low — primarily a testability issue.

---

#### P17. Idempotency — Alignment: 3/3

`IdempotentWriteOptions`, commit key derivation via `hash_sha256_hex`, and the `idempotent_commit_properties` helper are correctly wired throughout the write path. `delta_optimize_compact` and `delta_vacuum` are inherently idempotent by Delta protocol design. Well aligned.

---

#### P18. Determinism / reproducibility — Alignment: 3/3

Plan fingerprinting (`plan_fingerprint.py:58–90`) uses Substrait bytes, DataFusion session settings, UDF snapshot hash, Delta input pins, and planning env hash — all stable, content-addressed inputs. `_scan_unit_key` uses `hash_msgpack_canonical` over a deterministic payload. `extract_lineage` is deterministic given the same logical plan. Well aligned.

---

### Category: Simplicity

#### P19. KISS — Alignment: 2/3

**Current state:**
`src/datafusion_engine/delta/control_plane_core.py:286–450` implements a lazy-export proxy system (`_LazyExportProxy`, `_LAZY_EXPORT_MODULES`, module-level `__getattr__`) to defer imports of the mutation, maintenance, and provider submodules. This pattern exists to avoid circular imports or import-time cost. The implementation is ~100 lines of machinery. A simpler alternative — explicit lazy imports at function call time inside each function — would achieve the same effect without the proxy classes.

**Findings:**
- `src/datafusion_engine/delta/control_plane_core.py:341–357` — `_LazyExportProxy.__call__` and `__getattr__` redirect attribute access dynamically. Type checkers must rely on the `if TYPE_CHECKING:` block to see the right types, but the runtime object is the proxy.
- `src/datafusion_engine/io/write_core.py:83–100` — `WritePipeline` itself is a lazy-runtime proxy at module level (`_WritePipelineProxyMeta`, `_resolve_write_pipeline_class`), again to defer the import. This means `write_core.WritePipeline` is not the real class in production—it is a forwarding stub.

**Suggested improvement:**
Consider replacing both lazy-proxy patterns with explicit `from __future__ import annotations` combined with function-local imports at call time. This eliminates the proxy machinery while still avoiding circular imports and deferring import cost. It is more readable and is already the pattern used by many functions in `delta_write_handler.py`.

**Effort:** medium
**Risk if unaddressed:** low (correctness) — but the proxies add debugging friction: stack traces and repr output show proxy types, not the real ones.

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**
`WritePipeline.write` at `write_pipeline.py:359–368` is a one-line wrapper that calls `write_via_streaming`. The docstring says "best available method," implying future alternatives, but none exist. The two-method split introduces an indirection with no current value.

**Findings:**
- `src/datafusion_engine/io/write_pipeline.py:359–368` — `write` calls `write_via_streaming` unconditionally. If a non-streaming path were needed, it would be added to `write`, but currently the method just adds a level of indirection.

**Suggested improvement:**
Collapse `write` and `write_via_streaming` into a single method. If a second write strategy is genuinely anticipated, add it when required.

**Effort:** small
**Risk if unaddressed:** low — purely cosmetic.

---

#### P21. Least astonishment — Alignment: 2/3

**Current state:**
`DeltaDeleteRequest.predicate: str` is the primary astonishment risk. A caller reading the type annotation would expect any string to be valid; only reading the call path reveals the empty-string rejection. This is inconsistent with `DeltaUpdateRequest.predicate: str | None`, where `None` explicitly encodes "no filter."

**Findings:**
- `src/datafusion_engine/delta/control_plane_types.py:153` — `predicate: str` with no validation hint.
- `src/datafusion_engine/delta/control_plane_mutation.py:64–67` — guard is applied at call time, not at construction.
- `src/datafusion_engine/cache/_ports.py:10–13` — `_WriterPort` is named with a leading underscore, suggesting it is private. However, it is the canonical port used by `CacheLedgerWriter` and exported in `__all__`. The underscore naming is inconsistent with its intended role as a public protocol.

**Suggested improvement:**
Rename `_WriterPort` to `WriterPort` in `cache/_ports.py` or document it explicitly as an internal-to-cache protocol. For `DeltaDeleteRequest.predicate`, add a `__post_init__` guard (per P8 suggestion).

**Effort:** small
**Risk if unaddressed:** medium for the delete predicate (discussed under P8 and P10).

---

#### P22. Public contracts — Alignment: 3/3

All modules in scope define `__all__` explicitly. Versioned table names (`_v2`, `_v10`) are encoded as constants in `artifact_store_constants.py`. Protocol and struct types are stable and separated from implementation detail. Well aligned.

---

### Category: Quality

#### P23. Design for testability — Alignment: 1/3

**Current state:**
Three distinct testability problems exist.

**Findings:**
1. **Module-level `Path.cwd()` calls (non-injectable defaults)**:
   - `src/datafusion_engine/plan/artifact_store_constants.py:18` — `_DEFAULT_ARTIFACTS_ROOT = Path.cwd() / ".artifacts"` in the `except IndexError:` branch. This captures the working directory at import time.
   - `src/datafusion_engine/cache/inventory.py:42` — same pattern: `_DEFAULT_CACHE_ROOT = Path.cwd() / ".artifacts"`.
   - `src/datafusion_engine/delta/obs_table_manager.py:38` — `_DEFAULT_OBSERVABILITY_ROOT = Path.cwd() / ".artifacts"`.
   All three also have a `try: Path(__file__).resolve().parents[2] / ".artifacts"` primary path that is correct. The `Path.cwd()` fallback, however, captures ambient state at module import time and makes tests that change the working directory non-deterministic.

2. **`WritePipeline` requires live `SessionContext`**: every write path flows through `self.ctx.sql_with_options(...)` or `self.ctx.table(...)`. There is no seam for injecting a SQL-execution stub; tests must either use a real DataFusion session or patch `SessionContext` methods.

3. **`_record_scan_plan_artifact` has deferred imports**:
   `src/datafusion_engine/lineage/scheduling.py:663–667` — inside the function, two import statements pull from `datafusion_engine.delta.observability` and `datafusion_engine.lineage.diagnostics`. The function cannot be tested in isolation without those modules being importable.

**Suggested improvement:**
1. Replace the `Path.cwd()` fallback with `None` and propagate `None` to mean "no default root; caller must provide." This makes the defaults explicit and injectable. The functions that consume the default can accept `Path | None` and raise `ValueError` when `None` and no override is supplied.
2. Introduce a `SqlExecutor` protocol with `def sql(self, query: str) -> DataFrame: ...` and inject it into `WritePipeline` instead of accepting `SessionContext` directly. This allows test doubles to replace the SQL layer.
3. The deferred imports in `_record_scan_plan_artifact` are acceptable for optional-observability paths, but the function body should be extractable if refactored to accept the observability helpers as callable parameters (per P16 suggestion for functional core).

**Effort:** medium
**Risk if unaddressed:** medium — test suites that exercise artifact storage or cache ledger paths are sensitive to the working directory at test startup.

---

#### P24. Observability — Alignment: 2/3

**Current state:**
The observability architecture is coherent: `DiagnosticsRecorder` wraps `DiagnosticsSink`; `DiagnosticsContext` carries session/operation IDs; `DiagnosticsRecorderAdapter` bridges legacy sinks. Structured artifact specs (`SCAN_UNIT_PRUNING_SPEC`, `DELTA_SERVICE_PROVIDER_SPEC`, etc.) ensure consistent field shapes. OpenTelemetry spans are emitted at `plan_scan_unit`, write operations, and scheduling boundaries.

The gap is in `obs/diagnostics_bridge.py:37`:

```python
_OBS_SESSION_ID: Final[str] = uuid7_str()
```

This module-level UUID is shared across all bridge calls within a process lifetime. Every call to `record_view_fingerprints`, `record_rust_udf_snapshot`, `record_view_contract_violations`, etc. uses the same session ID. For a long-running process serving multiple pipeline runs, this means diagnostics artifacts from different runs share a session ID, making correlation ambiguous.

**Findings:**
- `src/datafusion_engine/obs/diagnostics_bridge.py:37` — `_OBS_SESSION_ID: Final[str] = uuid7_str()` is fixed at import time.
- `src/datafusion_engine/obs/diagnostics_bridge.py:49, 67, 82, 111, 141, 151, 161` — all bridge helpers call `ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)`, hardcoding this single ID.
- `src/datafusion_engine/lineage/diagnostics.py:724–729` — `recorder_for_profile` correctly uses `profile.context_cache_key()` as session ID, which is per-profile. The bridge does not use this mechanism.

**Suggested improvement:**
Add a `session_id: str | None = None` parameter to each bridge helper function, defaulting to `_OBS_SESSION_ID` for backward compatibility. Callers with a live `DataFusionRuntimeProfile` can pass `profile.context_cache_key()` as the session ID. Alternatively, accept a `DiagnosticsCollector | DiagnosticsRecorder` (with built-in session context) rather than a raw `DiagnosticsCollector` sink.

**Effort:** small
**Risk if unaddressed:** low (correctness) — diagnostics still record, but cross-run correlation by session ID is unreliable in long-running process contexts.

---

## Cross-Cutting Themes

### Theme 1: Request type boilerplate in `control_plane_types.py`
All 16 request types in `control_plane_types.py` repeat four fields (`table_uri`, `storage_options`, `version`, `timestamp`) and the same `table_ref` property verbatim. This is 96 lines of identical code. It is not merely cosmetic: adding or renaming a field (e.g., changing `timestamp` from `str | None` to a `datetime | None`) requires editing all 16 classes. A `_DeltaRequestBase` mixin or a `table_ref_from_request(req)` function addresses this in one place.

**Affected principles:** P7 (DRY), P10 (illegal states), P21 (astonishment)

### Theme 2: Write path duplication between `write_pipeline.py` and `write_execution.py`
The retry-error marker constants and the observability-operation detector exist in two files. `write_execution.py` was created (presumably) to extract testable pure functions, but `write_pipeline.py` still contains private copies of the same logic. The public `write_execution` versions are not yet used everywhere.

**Affected principles:** P7 (DRY), P19 (KISS), P23 (testability)

### Theme 3: `Path.cwd()` ambient capture at module load
Three constants modules capture `Path.cwd()` as a fallback default at import time. This creates an implicit dependency on the process working directory that cannot be injected or mocked. The pattern is identical in all three files and shares a common root cause: the `except IndexError:` fallback was added as a safety net but was not made injectable.

**Affected principles:** P18 (determinism), P23 (testability)

### Theme 4: `DeltaDeleteRequest` predicate correctness gap
Identified in prior review and still present: `DeltaDeleteRequest.predicate: str` allows an empty string at the type level. The guard in `control_plane_mutation.py` is the sole enforcement point. This is a high-risk gap because msgpack serialization round-trips bypass the guard.

**Affected principles:** P8 (contract), P10 (illegal states), P21 (astonishment)

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P8 / P10 | Add `__post_init__` guard to `DeltaDeleteRequest` for empty predicate | small | high — closes correctness gap at boundary |
| 2 | P7 | Delete duplicate `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS` and `_is_delta_observability_operation` from `write_pipeline.py`; import from `write_execution.py` | small | medium — eliminates silent divergence risk |
| 3 | P7 | Extract `table_ref` as a module-level function in `control_plane_types.py` to eliminate 96 lines of identical property bodies | small | medium — reduces change surface for `DeltaTableRef` field evolution |
| 4 | P24 | Add `session_id` parameter to bridge helpers in `obs/diagnostics_bridge.py` | small | low/medium — enables correct per-run session correlation |
| 5 | P23 | Replace `Path.cwd()` fallback in three constants modules with `None`; propagate through callers | small | medium — makes artifact root deterministic and injectable in tests |

---

## Recommended Action Sequence

1. **(Highest priority)** Fix `DeltaDeleteRequest.predicate` empty-string gap via `__post_init__` guard in `control_plane_types.py`. This addresses P8, P10, P21 simultaneously with a single-function change. The `_require_non_empty_delete_predicate` call in `control_plane_mutation.py` can remain as a defense-in-depth belt-and-suspenders check.

2. **Consolidate write-path duplicates**: remove `_RETRYABLE_DELTA_STREAM_ERROR_MARKERS`, `_is_retryable_delta_stream_error`, and `_is_delta_observability_operation` from `write_pipeline.py`. Import the public versions from `write_execution.py`. Update `delta_write_handler.py:379` to import from `write_execution`. This resolves P7 and reduces the surface area of `write_pipeline.py`.

3. **Consolidate `table_ref` property** in `control_plane_types.py`. Either introduce a `_DeltaRequestBase` struct with the four shared fields and the `table_ref` property, or add a module-level helper function. Resolves P7 and reduces the blast radius of future `DeltaTableRef` changes.

4. **Replace `Path.cwd()` fallbacks** in `artifact_store_constants.py`, `cache/inventory.py`, and `delta/obs_table_manager.py` with `None` defaults. Update consumers to accept `Path | None` and require explicit configuration in production entry points. Resolves P23 and P18.

5. **Add `session_id` parameter to `obs/diagnostics_bridge.py` bridge helpers**. Default to `_OBS_SESSION_ID` for backward compatibility. Callers with a profile can pass `profile.context_cache_key()`. Resolves P24.

6. **(Structural)** Split `DeltaService` into `DeltaProviderService` and `DeltaProviderObservabilityAdapter`. Extract `DeltaFeatureOps` as a standalone module-level coordinator. Resolves P2, P3. This is the most disruptive change and should follow the smaller fixes above.

7. **(Structural)** Replace `DeltaMutationRequest` runtime `.validate()` pattern with a union type. Resolves P10 cleanly but requires updating all callers.

8. **(Optional)** Evaluate whether the `_LazyExportProxy` machinery in `control_plane_core.py` and the `_WritePipelineProxyMeta` in `write_core.py` are still necessary, or whether function-local imports would suffice. If the lazy-import pattern is kept, add explicit documentation about why it exists. Resolves P19 (KISS).
