# CQ C901/PLR09 + msgspec Hard-Cutover Implementation Plan v1

**Date:** 2026-02-14  
**Status:** Design-phase implementation plan (approved architecture direction)  
**Scope:** `tools/cq` primary; selected shared/runtime modules in `src/` secondarily  
**Primary objective:** Eliminate C901/PLR09 complexity hotspots by introducing shared deterministic orchestration and strict msgspec/runtime segmentation, with a 100% architecture pivot (no compatibility shims).

---

## 1. Design-Phase Hard-Cutover Posture

This plan is intentionally non-incremental from an internal architecture perspective.

1. No compatibility shims.
2. No deprecation adapters.
3. No dual-path old/new implementations.
4. Old modules/functions replaced in a wave are deleted in that same wave.
5. New interfaces are strongly typed and msgspec-first for serialized boundaries.

---

## 2. Baseline Inventory (Inputs to This Plan)

### 2.1 `noqa` suppressions for `PLR09**` / `C901`

1. Total suppressions: `19`  
2. Production suppressions: `14`  
3. Test suppressions: `5`  
4. Code distribution:
   - `PLR0914`: 10
   - `PLR0913`: 5
   - `PLR0915`: 3
   - `C901`: 3
   - `PLR0912`: 2
   - `PLR0911`: 2
   - `PLR0904`: 2

### 2.2 Active Ruff findings (`C901`, `PLR09**`)

1. Active findings: `25` across `10` CQ files.
2. Highest concentration:
   - `tools/cq/query/executor.py`: 6
   - `tools/cq/search/smart_search.py`: 4
   - `tools/cq/neighborhood/scan_snapshot.py`: 4
   - `tools/cq/macros/calls_target.py`: 4

### 2.3 Immediate blocker

`tools/cq/core/cache/diagnostics.py` currently contains a malformed docstring section causing syntax parse errors. This must be fixed first so lint output is trustworthy.

---

## 3. Root-Cause Summary

The dominant issue is architectural duplication, not isolated “bad functions”:

1. Cache key/snapshot/tag/hit-miss/writeback pipelines are re-implemented in multiple subsystems (`search`, `query`, `pattern`, `neighborhood`, `calls_target`, `lsp`).
2. Deterministic canonicalization logic is duplicated across cache and stable-ID paths.
3. Runtime policy env parsing logic is duplicated.
4. Runtime-only dependencies leak into orchestration layers that should be serializable and deterministic.

---

## 4. Target Architecture (Best-in-Class)

## 4.1 Core principle

Split CQ execution into:

1. **Serializable planning/contracts layer** (`msgspec.Struct` only)
2. **Runtime execution layer** (I/O, diskcache backend, AST/LSP tools, session/runtime objects)

## 4.2 Hard boundary

1. Serializable layer contains no `Path`, `SessionContext`, futures, backend handles, or parser objects.
2. Runtime layer accepts typed plans/contracts and produces typed outputs/events.
3. Only msgspec-safe objects are persisted or hashed.

## 4.3 Shared orchestration substrate

A single reusable fragment-cache orchestration module will own:

1. Scope/inventory resolution handoff.
2. Snapshot fingerprint acquisition.
3. Fragment key derivation.
4. Cache probe + decode.
5. Miss execution callback.
6. Transactionized writeback.
7. Deterministic assemble/sort.

This substrate replaces duplicated logic in:

1. `tools/cq/query/executor.py`
2. `tools/cq/neighborhood/scan_snapshot.py`
3. `tools/cq/search/smart_search.py`
4. `tools/cq/macros/calls_target.py`

---

## 5. Workstream Breakdown

## WS0 — Syntax/Signal Restoration (Gate)

### Scope items

1. Repair malformed docstring in `tools/cq/core/cache/diagnostics.py`.
2. Re-run targeted Ruff check for `C901,PLR09**` to establish clean baseline signal.

### Representative snippet

```python
def snapshot_backend_metrics(*, root: Path) -> dict[str, object]:
    """Collect backend cache metrics for summary/diagnostics payloads.

    Returns:
        dict[str, object]: Backend stats, volume, and namespace-level payload.
    """
```

### File edits

1. Edit `tools/cq/core/cache/diagnostics.py`

### Checklist

1. Fix syntax.
2. Verify `uv run ruff check --select C901,PLR09 tools/cq`.

---

## WS1 — Shared Fragment-Orchestration Engine

### Scope items

1. Introduce a shared cache-fragment execution engine.
2. Encode hit/miss orchestration via typed msgspec contracts.
3. Centralize transactionized writeback + deterministic assembly.

### Representative code patterns

```python
class FragmentRequestV1(msgspec.Struct, frozen=True):
    namespace: str
    workspace: str
    language: str
    scope_hash: str | None = None
    snapshot_digest: str | None = None
    ttl_seconds: int = 0
    run_id: str | None = None
```

```python
class FragmentMissV1(msgspec.Struct, frozen=True):
    file: str
    cache_key: str
    content_hash: str
```

```python
def execute_fragment_cache_plan(
    plan: FragmentRequestV1,
    *,
    probe: FragmentProbeProtocol,
    compute_misses: FragmentComputeProtocol,
    encode: FragmentEncodeProtocol,
    decode: FragmentDecodeProtocol,
    assemble: FragmentAssembleProtocol,
) -> FragmentResultV1: ...
```

### External library functions leveraged

1. `diskcache.FanoutCache.get`
2. `diskcache.FanoutCache.set`
3. `diskcache.FanoutCache.transact`
4. `msgspec.convert`
5. `msgspec.to_builtins`
6. `msgspec.json.encode`

### New files

1. `tools/cq/core/cache/fragment_engine.py`
2. `tools/cq/core/cache/fragment_contracts.py`
3. `tools/cq/core/cache/fragment_codecs.py`

### File edits

1. `tools/cq/core/cache/__init__.py` (exports)

### Legacy deletions

1. Delete subsystem-local duplicated hit/miss/writeback helpers after migration:
   - local miss loops in `query/executor.py`
   - local miss loops in `neighborhood/scan_snapshot.py`
   - local miss loops in `search/smart_search.py`

### Checklist

1. Add contracts and engine.
2. Add engine unit tests.
3. Wire one pilot caller (`query_entity_fragment`) before full migration.

---

## WS2 — Canonical Identity and Determinism Core

### Scope items

1. Replace duplicate canonicalization functions with one shared canonicalizer.
2. Standardize stable digest derivation used by cache keys and finding IDs.

### Representative snippet

```python
def canonicalize_payload(value: object) -> object:
    if isinstance(value, Mapping):
        items = sorted((str(k), canonicalize_payload(v)) for k, v in value.items())
        return dict(items)
    if isinstance(value, (set, frozenset)):
        return sorted((canonicalize_payload(v) for v in value), key=msgspec.json.encode)
    ...
```

```python
def stable_digest24(value: object) -> str:
    payload = canonicalize_payload(value)
    return hashlib.sha256(msgspec.json.encode(payload)).hexdigest()[:24]
```

### New files

1. `tools/cq/core/id/canonical.py`
2. `tools/cq/core/id/digests.py`

### File edits

1. `tools/cq/core/cache/key_builder.py` (use shared canonicalizer)
2. `tools/cq/core/schema.py` (remove local `_canonicalize_for_id`)

### Legacy deletions

1. `tools/cq/core/schema.py::_canonicalize_for_id`

### Checklist

1. Replace both call sites with shared canonicalizer.
2. Add deterministic regression tests for equal unordered inputs.

---

## WS3 — Scope/Inventory/Snapshot Service Consolidation

### Scope items

1. Factor file inventory + snapshot acquisition into one reusable service.
2. Remove per-caller recreation of scope roots/inventory token/snapshot key logic.

### Representative snippet

```python
class ScopePlanV1(msgspec.Struct, frozen=True):
    root: str
    paths: tuple[str, ...]
    globs: tuple[str, ...] = ()
    language: str = "python"
```

```python
class ScopeResolutionV1(msgspec.Struct, frozen=True):
    files: tuple[str, ...]
    scope_hash: str | None = None
    snapshot_digest: str = ""
    inventory_token: dict[str, object] = msgspec.field(default_factory=dict)
```

```python
def resolve_scope(plan: ScopePlanV1) -> ScopeResolutionV1: ...
```

### New files

1. `tools/cq/core/cache/scope_services.py`

### File edits

1. `tools/cq/query/sg_parser.py`
2. `tools/cq/query/executor.py`
3. `tools/cq/neighborhood/scan_snapshot.py`
4. `tools/cq/search/smart_search.py`

### Legacy deletions

1. Remove `_tabulate_scan_files` after callers migrate.
2. Remove duplicate inventory-token helpers in callers.

### Checklist

1. Introduce service.
2. Migrate `sg_parser` first.
3. Migrate all scope consumers.

---

## WS4 — Query Executor Refactor (Entity + Pattern)

### Scope items

1. Replace `_scan_entity_records` monolith with fragment-engine pipeline.
2. Replace `_execute_ast_grep_rules` monolith with composable pipeline stages.
3. Reduce arg-heavy surface of `execute_plan` using typed request envelope.

### Representative snippet

```python
class ExecutePlanRequestV1(msgspec.Struct, frozen=True):
    plan: ToolPlan
    query: Query
    root: str
    argv: tuple[str, ...] = ()
    run_id: str | None = None
    query_text: str | None = None
```

```python
def execute_pattern_fragments(req: PatternFragmentRequestV1) -> PatternFragmentResultV1:
    scope = resolve_scope(...)
    return execute_fragment_cache_plan(...)
```

### File edits

1. `tools/cq/query/executor.py`
2. `tools/cq/query/execution_requests.py`
3. `tools/cq/query/batch.py` (request-object propagation)

### New files

1. `tools/cq/query/fragment_pipelines.py`
2. `tools/cq/query/execute_request.py`

### Legacy deletions

1. Delete `_scan_entity_records` after migration.
2. Delete `_execute_ast_grep_rules` after migration.
3. Delete redundant per-mode cache loops replaced by pipeline module.

### Checklist

1. Land request envelope.
2. Extract entity pipeline.
3. Extract pattern pipeline.
4. Remove old functions and update tests.

---

## WS5 — Neighborhood Snapshot Refactor

### Scope items

1. Replace `ScanSnapshot.build_from_repo` monolith with shared scope + fragment engine.
2. Split aggregate-snapshot layer from fragment-scan layer.

### Representative snippet

```python
class NeighborhoodScanRequestV1(msgspec.Struct, frozen=True):
    root: str
    language: str
    run_id: str | None = None
```

```python
def build_neighborhood_records(req: NeighborhoodScanRequestV1) -> list[SgRecord]:
    scope = resolve_scope(...)
    fragments = execute_fragment_cache_plan(...)
    return fragments.records
```

### File edits

1. `tools/cq/neighborhood/scan_snapshot.py`
2. `tools/cq/run/runner.py` (if helper signatures change)

### New files

1. `tools/cq/neighborhood/cache_pipeline.py`

### Legacy deletions

1. Delete local miss loop and local aggregate writeback block from `build_from_repo`.

### Checklist

1. Introduce neighborhood pipeline module.
2. Replace classmethod internals.
3. Re-run neighborhood unit/e2e tests.

---

## WS6 — Smart Search Partition Refactor

### Scope items

1. Split `_run_single_partition` into deterministic phases:
   - scope resolve
   - candidate cache read/compute/write
   - enrichment cache read/compute/write
   - LSP prefetch merge
2. Route candidate and enrichment fragment handling through shared engine.

### Representative snippet

```python
class SearchPartitionPlanV1(msgspec.Struct, frozen=True):
    root: str
    language: str
    query: str
    mode: str
    include_strings: bool = False
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
```

```python
def run_search_partition(plan: SearchPartitionPlanV1) -> SearchPartitionResultV1: ...
```

### File edits

1. `tools/cq/search/smart_search.py`

### New files

1. `tools/cq/search/partition_pipeline.py`
2. `tools/cq/search/partition_contracts.py`

### Legacy deletions

1. Delete `_run_single_partition` implementation body after moving logic into pipeline module.

### Checklist

1. Extract typed partition plan/result.
2. Migrate call site in `smart_search()`.
3. Update deterministic cache-matrix tests.

---

## WS7 — LSP Front-Door Pipeline Hardening

### Scope items

1. Refactor `enrich_with_language_lsp` into phase functions:
   - context/budget resolution
   - cached outcome probe
   - single-flight lock and waiter path
   - provider execution by language
   - normalized outcome persistence
2. Replace ad-hoc dict cache payload with msgspec contract.

### Representative snippet

```python
class LspOutcomeCacheV1(msgspec.Struct, frozen=True):
    payload: dict[str, object] | None = None
    timed_out: bool = False
    failure_reason: str | None = None
```

```python
def probe_lsp_cache(...) -> LspOutcomeCacheV1 | None: ...
def execute_lsp_provider(...) -> LspOutcomeCacheV1: ...
def persist_lsp_outcome(...) -> bool: ...
```

### External library functions leveraged

1. `FanoutCache.add` for lock acquisition
2. `FanoutCache.get` for waiter poll
3. `FanoutCache.delete` for lock release
4. `FanoutCache.set` for outcome persistence

### File edits

1. `tools/cq/search/lsp_front_door_adapter.py`

### New files

1. `tools/cq/search/lsp_front_door_pipeline.py`
2. `tools/cq/search/lsp_front_door_contracts.py`

### Legacy deletions

1. Remove local `_decode_cached_outcome` dict-decoding once msgspec decode path is in place.

### Checklist

1. Add cache payload contract.
2. Extract phased pipeline functions.
3. Keep same external behavior and fail-open semantics.

---

## WS8 — Runtime Policy Parsing Consolidation

### Scope items

1. Create one shared env-namespace parser utility for:
   - ttl overrides
   - enabled flags
   - ephemeral flags
2. Remove duplicate parsing loops across runtime/cache policy modules.

### Representative snippet

```python
class NamespaceEnvParseResultV1(msgspec.Struct, frozen=True):
    ttl_seconds: dict[str, int] = msgspec.field(default_factory=dict)
    enabled: dict[str, bool] = msgspec.field(default_factory=dict)
    ephemeral: dict[str, bool] = msgspec.field(default_factory=dict)
```

```python
def parse_cache_namespace_env(prefix: str) -> NamespaceEnvParseResultV1: ...
```

### File edits

1. `tools/cq/core/runtime/execution_policy.py`
2. `tools/cq/core/cache/policy.py`

### New files

1. `tools/cq/core/runtime/env_namespace.py`

### Legacy deletions

1. Delete `_env_namespace_ttls`, `_env_namespace_enabled`, `_env_namespace_ephemeral` duplicates.
2. Delete corresponding cache-policy duplicate helpers after cutover.

### Checklist

1. Implement shared parser.
2. Replace both call sites.
3. Add parser unit tests.

---

## WS9 — Strong Request Objects for Arg-Heavy Production APIs (`src/`)

### Scope items

Apply the same architecture discipline outside CQ where `PLR0913/0914` is mostly signature/config sprawl.

### Candidate migrations

1. `src/graph/build_pipeline.py::orchestrate_build`
2. `src/extraction/orchestrator.py::run_extraction`
3. `src/relspec/policy_compiler.py::compile_execution_policy`
4. `src/datafusion_engine/udf/platform.py::install_rust_udf_platform`
5. `src/datafusion_engine/plan/pipeline_runtime.py::plan_with_delta_pins`

### Representative snippet

```python
class OrchestrateBuildRequestV1(msgspec.Struct, frozen=True):
    repo_root: str
    work_dir: str
    output_dir: str
    engine_profile: str = "medium"
    rulepack_profile: str = "default"
    include_errors: bool = True
    include_manifest: bool = True
    include_run_bundle: bool = False
```

```python
def orchestrate_build(request: OrchestrateBuildRequestV1) -> BuildResult: ...
```

### File edits

1. `src/graph/build_pipeline.py`
2. `src/extraction/orchestrator.py`
3. `src/relspec/policy_compiler.py`
4. `src/datafusion_engine/udf/platform.py`
5. `src/datafusion_engine/plan/pipeline_runtime.py`

### New files

1. `src/graph/contracts.py` (or request module)
2. `src/extraction/contracts.py` (extend existing)
3. `src/relspec/contracts.py` (extend existing)
4. `src/datafusion_engine/udf/contracts.py`
5. `src/datafusion_engine/plan/contracts.py`

### Legacy deletions

1. Delete old arg-heavy function signatures after callers are updated in same wave.

### Checklist

1. Define request structs.
2. Rewrite entry signatures.
3. Update all callsites atomically.
4. Remove temporary adapters (none allowed).

---

## WS10 — Legacy Module/Function Removal List (Explicit)

These are removal targets once replacement modules are wired:

1. `tools/cq/query/executor.py::_scan_entity_records`
2. `tools/cq/query/executor.py::_execute_ast_grep_rules`
3. `tools/cq/query/sg_parser.py::_tabulate_scan_files` (replace with shared scope service)
4. `tools/cq/search/smart_search.py::_run_single_partition` monolith body
5. `tools/cq/search/lsp_front_door_adapter.py` inline monolith flow sections (replaced by pipeline module)
6. `tools/cq/core/schema.py::_canonicalize_for_id`
7. `tools/cq/core/runtime/execution_policy.py` namespace parsing helpers
8. `tools/cq/core/cache/policy.py` namespace parsing helpers

No deprecation notices or pass-through wrappers are retained.

---

## 6. Target File Plan

## 6.1 New files to create

1. `tools/cq/core/cache/fragment_engine.py`
2. `tools/cq/core/cache/fragment_contracts.py`
3. `tools/cq/core/cache/fragment_codecs.py`
4. `tools/cq/core/cache/scope_services.py`
5. `tools/cq/core/id/canonical.py`
6. `tools/cq/core/id/digests.py`
7. `tools/cq/search/partition_pipeline.py`
8. `tools/cq/search/partition_contracts.py`
9. `tools/cq/search/lsp_front_door_pipeline.py`
10. `tools/cq/search/lsp_front_door_contracts.py`
11. `tools/cq/core/runtime/env_namespace.py`
12. Targeted new test modules under `tests/unit/cq/...` for each new service

## 6.2 Existing files to edit

1. `tools/cq/core/cache/diagnostics.py`
2. `tools/cq/core/cache/__init__.py`
3. `tools/cq/core/cache/key_builder.py`
4. `tools/cq/core/schema.py`
5. `tools/cq/query/sg_parser.py`
6. `tools/cq/query/executor.py`
7. `tools/cq/neighborhood/scan_snapshot.py`
8. `tools/cq/search/smart_search.py`
9. `tools/cq/search/lsp_front_door_adapter.py`
10. `tools/cq/core/runtime/execution_policy.py`
11. `tools/cq/core/cache/policy.py`
12. Selected `src/...` modules listed in WS9

---

## 7. Implementation Sequence (No-Shim Cutover)

1. WS0 syntax gate fix.
2. WS1 shared fragment engine.
3. WS2 canonical identity core.
4. WS3 scope/snapshot service.
5. WS4 query executor migration.
6. WS5 neighborhood migration.
7. WS6 smart-search partition migration.
8. WS7 LSP front-door migration.
9. WS8 runtime policy parser consolidation.
10. WS9 non-CQ arg-heavy API request-object migration.
11. WS10 explicit deletion pass.

---

## 8. Validation Matrix

### 8.1 Lint/type/behavior targets

1. Zero active `C901` and `PLR09**` findings in CQ production modules.
2. Zero `noqa` suppressions for those codes in CQ production modules.
3. Deterministic output parity across:
   - cache miss path
   - cache hit path
   - process restart

### 8.2 Required commands

1. `uv run ruff format`
2. `uv run ruff check --fix`
3. `uv run pyrefly check`
4. `uv run pyright`
5. `uv run pytest -q tests/unit/cq tests/e2e/cq`

### 8.3 Additional targeted checks

1. `uv run ruff check --select C901,PLR09 tools/cq src`
2. Determinism rerun snapshots for `search`, `q entity`, `q pattern`, `neighborhood`, `run`.
3. Cache hit-ratio and failure telemetry sanity checks.

---

## 9. Implementation Checklist (Master)

1. [ ] Fix syntax blocker in `tools/cq/core/cache/diagnostics.py`.
2. [ ] Introduce shared fragment engine + contracts + codecs.
3. [ ] Introduce shared canonicalizer and stable digest core.
4. [ ] Introduce shared scope/inventory/snapshot service.
5. [ ] Refactor query entity and pattern execution onto shared services.
6. [ ] Refactor neighborhood snapshot build onto shared services.
7. [ ] Refactor smart-search partition execution into phased pipeline module.
8. [ ] Refactor LSP front-door into phased pipeline + msgspec cache payload.
9. [ ] Consolidate env namespace parsing into one shared utility.
10. [ ] Migrate arg-heavy production APIs in `src/` to request objects.
11. [ ] Delete replaced monolith functions/modules in same wave.
12. [ ] Remove all corresponding `noqa` suppressions in migrated areas.
13. [ ] Pass full validation matrix.

---

## 10. Success Criteria

1. CQ execution paths share one deterministic cache-fragment orchestration substrate.
2. Serialized boundaries are uniformly msgspec contracts.
3. Runtime-only objects are isolated to runtime modules.
4. `C901`/`PLR09**` hotspots are resolved by architecture, not rule suppression.
5. Legacy paths are removed without compatibility scaffolding.

