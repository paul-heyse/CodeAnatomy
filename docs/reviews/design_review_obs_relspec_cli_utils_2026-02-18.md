# Design Review: Orchestration Spine — relspec / graph / planning_engine

**Date:** 2026-02-18
**Scope:** `src/relspec/`, `src/graph/`, `src/planning_engine/`
**Focus:** All principles (1-24)
**Depth:** deep — all files
**Files reviewed:** 24 Python files

---

## Executive Summary

The orchestration spine is in genuinely strong shape. The `relspec` package has been
through a deliberate decoupling effort: all technology-specific logic is pushed behind
`LineagePort`, `DatasetSpecProvider`, and `RuntimeProfilePort` protocols; contracts are
expressed as frozen `msgspec.Struct`s; and every policy decision is front-loaded at
compile time into `CompiledExecutionPolicy` so that the execution layer consumes a
pre-baked artifact without re-deriving heuristics. The `graph/` package cleanly
separates user-facing API (`product_build.py`) from orchestration mechanics
(`build_pipeline.py`). `planning_engine/` is a disciplined, thin contract layer whose
`SemanticExecutionSpec` and `RuntimeConfig` serve as the stable serialization boundary
with the Rust engine.

The three issues worth addressing are: (1) a non-deterministic timestamp embedded in
`ExecutionPackageArtifact` that breaks full reproducibility; (2) a legacy mode aliasing
knot in `policy_calibrator.py` where the `CalibrationMode` Literal permits four values
but two are aliases whose semantics are partially collapsed; (3) the `build_pipeline.py`
function `orchestrate_build` has grown to 80 lines of procedural orchestration mixing
stage execution, observability emission, and auxiliary-output collection in a single
public function, which crosses the SRP boundary. All other principle scores are 2 or
better.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | — | low | Clean lazy-loading `__init__`, private helpers throughout |
| 2 | Separation of concerns | 2 | small | low | `orchestrate_build` mixes stage dispatch + observability + aux-output collection |
| 3 | SRP | 2 | small | low | `orchestrate_build` (graph/build_pipeline.py:120) has ≥3 reasons to change |
| 4 | High cohesion, low coupling | 3 | — | low | Port protocols + lazy-imported heavy deps keep coupling narrow |
| 5 | Dependency direction | 3 | — | low | relspec → ports; graph → planning_engine contracts only; no upward deps |
| 6 | Ports & Adapters | 3 | — | low | `ports.py` and Protocol-based adapters cleanly implemented |
| 7 | DRY | 2 | small | low | `CalibrationMode` aliases add a second encoding of the "warn/observe" equivalence |
| 8 | Design by contract | 3 | — | low | `validate_calibration_bounds`, `validate_output_targets`, msgspec frozen structs |
| 9 | Parse, don't validate | 3 | — | low | Boundary parsing at `msgspec.json.decode` / `msgspec.Struct`; scattered isinstance guards are for untyped Rust payloads only |
| 10 | Make illegal states unrepresentable | 2 | small | low | `CompiledExecutionPolicy.validation_mode` is a plain `str`, not `ValidationMode` |
| 11 | CQS | 3 | — | low | Functions are either pure transforms or IO-effecting orchestration; no mixed cases |
| 12 | Dependency inversion + explicit composition | 3 | — | low | All heavy deps injected via ports or deferred inside-function imports |
| 13 | Composition over inheritance | 3 | — | low | No inheritance hierarchies; all behavior via protocol composition |
| 14 | Law of Demeter | 2 | small | low | `inferred_deps.py:124-125` chains `getattr(getattr(bundle, "artifacts"), "udf_snapshot")` |
| 15 | Tell, don't ask | 3 | — | low | Objects encapsulate their fingerprinting; callers receive hashes, not raw fields |
| 16 | Functional core, imperative shell | 3 | — | low | Policy derivation is pure; IO lives in `graph/build_pipeline.py` |
| 17 | Idempotency | 2 | small | medium | `ExecutionPackageArtifact.created_at_unix_ms` is a wall-clock timestamp; re-running produces different `package_fingerprint` |
| 18 | Determinism / reproducibility | 2 | small | medium | Same as P17; `created_at_unix_ms` is the only non-deterministic field in an otherwise fully-fingerprinted artifact |
| 19 | KISS | 3 | — | low | No over-engineering; each module is the simplest thing that satisfies its contract |
| 20 | YAGNI | 3 | — | low | No unused abstractions; legacy aliases have explicit migration notes |
| 21 | Least astonishment | 2 | small | low | `CalibrationMode` exposes "observe" and "apply" alongside "warn" and "enforce"; `resolve_mode` logic in calibrator normalises them silently |
| 22 | Declare and version public contracts | 2 | small | low | `SemanticExecutionSpec.version` field exists but no stability-level docs or `__version__` for relspec or planning_engine packages |
| 23 | Design for testability | 3 | — | low | All policy logic is pure; ports injectable; deferred imports isolate heavy deps |
| 24 | Observability | 2 | small | low | `relspec/` logging is correct but spans are absent; `graph/` emits `codeanatomy.*` OTel events but mixes them into the orchestration function body |

---

## Detailed Findings

### Category: Boundaries (P1-P6)

#### P1. Information Hiding — Alignment: 3/3

**Current state:**
`relspec/__init__.py` uses a `make_lazy_loader` mechanism to expose 27 public symbols
across 9 submodules without triggering any submodule import at `import relspec` time.
All private helpers (`_DataFusionLineagePort`, `_DefaultDatasetSpecProvider`,
`_CompiledPolicyComponents`, `_AuxiliaryOutputOptions`) are prefixed with `_` and not
re-exported. The `policy_compiler.py` deliberately uses a private `_derive_policy_components`
pipeline builder to hide the assembly sequence.

**Findings:**
- No violations found.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P2. Separation of Concerns — Alignment: 2/3

**Current state:**
`graph/build_pipeline.py:orchestrate_build` (lines 120-201) is the main integration
point. Within a single public function body, it:
1. Dispatches three sequential pipeline stages (`_run_extraction_phase`,
   `_compile_semantic_phase`, `_execute_engine_phase`).
2. Collects CPG and auxiliary outputs from stage results.
3. Records observability events and metrics (`_record_observability`).

The three concerns—stage dispatch, output collection, and observability—are mixed in
one caller. The private helpers are well-extracted for each concern individually, but
`orchestrate_build` acts as the glue that calls all of them in sequence within the same
logical scope. When observability requirements change (e.g., adding span attributes to
the extraction phase) and when output collection contracts change (e.g., a new auxiliary
artifact type), both touch this function.

**Findings:**
- `src/graph/build_pipeline.py:120-201` — `orchestrate_build` calls `_record_observability`
  (observability concern) and `_collect_cpg_outputs` / `_collect_auxiliary_outputs`
  (output collection concern) alongside `_run_extraction_phase` (stage dispatch concern)
  without a seam between them.

**Suggested improvement:**
Extract a `_post_process_results` helper that receives the raw stage results and produces
`(cpg_outputs, auxiliary_outputs, warnings)`. `orchestrate_build` then calls
`_run_stages(request) -> RawStageResults` and `_post_process_results(raw, ...) -> ProcessedResults`
with `_record_observability` moved to the caller (`product_build.py`) after the result
is in hand. This separates "what the stages produce" from "how we record and shape the output."

**Effort:** small
**Risk if unaddressed:** low

---

#### P3. SRP — Alignment: 2/3

**Current state:**
`graph/build_pipeline.py:orchestrate_build` has three identifiable change reasons:
(a) stage sequencing (e.g., adding a new pipeline phase between semantic compilation
and engine execution), (b) output-collection contract changes (e.g., a new Rust artifact
field), and (c) observability policy changes (e.g., new span attributes or event kinds).
All three live in the same 80-line function.

The `relspec/policy_compiler.py:compile_execution_policy` function (lines 102-167) is
better structured: it delegates completely to `_derive_policy_components`, then assembles
the `CompiledExecutionPolicy`, then computes the fingerprint. Its single reason to change
is "the policy compilation contract changes." Score 3 in isolation.

**Findings:**
- `src/graph/build_pipeline.py:120-201` — `orchestrate_build` mixes three concerns (see P2).
- `src/graph/product_build.py:126-204` — `build_graph_product` is 80 lines but cleanly
  separates signal/error handling, run-ID management, OTel bootstrapping, and the
  build execution call. Its one reason to change is "the public API contract changes."
  Score 3 in isolation.

**Suggested improvement:**
Same as P2: narrow `orchestrate_build` to stage dispatch only. The output collection and
observability helpers already exist; the fix is to stop calling them from within the
same function block.

**Effort:** small
**Risk if unaddressed:** low

---

#### P4. High Cohesion, Low Coupling — Alignment: 3/3

**Current state:**
`relspec/` cohesion is high: calibration bounds + calibrator + confidence + provenance
all live in the same package and share no concepts with the Rust adapter logic in
`inferred_deps.py`. Coupling is narrow: `policy_compiler.py` receives a `TaskGraphLike`
Protocol (not a concrete rustworkx type) and a `RuntimeProfilePort` Protocol (not a
`DataFusionRuntimeProfile`). The Rust bridge is invoked via `getattr(datafusion_ext,
"derive_cache_policies", None)` which fails gracefully when the symbol is absent.

**Findings:**
- No meaningful violations.

---

#### P5. Dependency Direction — Alignment: 3/3

**Current state:**
The dependency hierarchy is clean:
```
graph/ (shell)
  -> planning_engine/ (contracts only)
  -> obs/ (structured telemetry)
  -> relspec/ (policy logic)
  -> datafusion_engine/ (TYPE_CHECKING only)
relspec/ (core policy)
  -> relspec/ports.py (protocols only)
  -> datafusion_engine/ (TYPE_CHECKING only)
planning_engine/ (contracts only)
  -> planning_engine/spec_contracts.py
```
No upward dependency violations. `relspec` modules use `TYPE_CHECKING` guards for all
heavy DataFusion imports and defer concrete imports into function bodies.

**Findings:**
- No violations.

---

#### P6. Ports & Adapters — Alignment: 3/3

**Current state:**
`relspec/ports.py` defines five Protocol ports:
- `LineagePort` — abstracts DataFusion lineage extraction
- `DatasetSpecProvider` — abstracts schema-spec resolution
- `RuntimeProfilePoliciesPort`, `RuntimeProfileFeaturesPort`, `RuntimeProfilePort` — abstract
  the runtime profile shape consumed by `policy_compiler.py`

Each port has a corresponding default adapter (`_DataFusionLineagePort`,
`_DefaultDatasetSpecProvider`) that lives in `inferred_deps.py` and can be overridden
at the `InferredDepsInputs` call site. The `CompileExecutionPolicyRequestV1` struct
accepts `RuntimeProfilePort` (a Protocol), not `DataFusionRuntimeProfile` (a concrete
type). This means the Rust engine's runtime profile, a Python port mock, and any future
implementation all satisfy the type without modification.

**Findings:**
- No violations.

---

### Category: Knowledge (P7-P11)

#### P7. DRY — Alignment: 2/3

**Current state:**
The `CalibrationMode` Literal in `policy_calibrator.py:26` defines four values:
`"off"`, `"warn"`, `"enforce"`, `"observe"`, `"apply"`. The module docstring
(lines 13-15) explains that `"observe" -> "warn"` and `"apply" -> "enforce"` are
legacy aliases. However, the aliasing is not fully collapsed: at lines 162-177, the
result `mode` field is set to either the alias or its canonical form depending on which
branch was entered, producing a `PolicyCalibrationResult` whose `.mode` may be `"observe"`
or `"warn"` for the same logical state. That means there are two representations of the
"warn" semantic in the return type.

Similarly, `CompiledExecutionPolicy.validation_mode` (compiled_policy.py:80) is typed
as `str` with default `"warn"`, but `ValidationMode = Literal["off", "warn", "error"]`
is defined at line 22 and never used as the field type. This is a second place where a
validated constraint exists in knowledge form (the Literal) but is not enforced at
the type level on the struct.

**Findings:**
- `src/relspec/policy_calibrator.py:26` — `CalibrationMode` lists both `"observe"` and
  `"warn"` as valid values for the same semantic.
- `src/relspec/policy_calibrator.py:162-177` — Returned `mode` field in
  `PolicyCalibrationResult` may be either the alias or its canonical name for identical
  behavior; callers cannot safely test `result.mode == "warn"` without also testing
  `result.mode == "observe"`.
- `src/relspec/compiled_policy.py:22,80` — `ValidationMode` Literal is defined but
  `CompiledExecutionPolicy.validation_mode: str` does not use it.

**Suggested improvement:**
1. In `policy_calibrator.py`, normalise legacy aliases at entry and remove them from the
   `Literal` type: accept `"observe"` and `"apply"` as inputs via a separate
   `_normalise_mode(mode: str) -> CalibrationMode` helper, but store only `"warn"` or
   `"enforce"` in `PolicyCalibrationResult.mode`.
2. In `compiled_policy.py`, change `validation_mode: str = "warn"` to
   `validation_mode: ValidationMode = "warn"` to enforce the Literal constraint at
   msgspec decode time.

**Effort:** small
**Risk if unaddressed:** low

---

#### P8. Design by Contract — Alignment: 3/3

**Current state:**
- `validate_calibration_bounds` (calibration_bounds.py:45) produces explicit error
  messages for each violated invariant and is called at `calibrate_from_execution_metrics`
  entry, raising before any state mutation.
- `_validate_output_targets` (build_pipeline.py:104) checks that every output target
  has a non-empty `delta_location` before invoking the Rust engine.
- `InferredDeps` and `CompiledExecutionPolicy` are `frozen=True` msgspec Structs,
  making post-construction mutation impossible.
- The `high_confidence` factory (inference_confidence.py:65) silently clamps its score
  to `[0.8, 1.0]` and the `low_confidence` factory to `[0.0, 0.49]`, encoding the
  invariant as a constructor guarantee.

**Findings:**
- No violations.

---

#### P9. Parse, Don't Validate — Alignment: 3/3

**Current state:**
All external data enters the system through `msgspec.json.decode` with explicit type
targets (`SemanticExecutionSpec`, `BuildResult`). The raw Rust response dictionary
(build_pipeline.py:401-405) undergoes explicit `isinstance` guards only because it
arrives as `object` from the opaque FFI boundary; this is the appropriate boundary
pattern. No scattered "if field missing" checks appear in domain logic.

**Findings:**
- No violations.

---

#### P10. Make Illegal States Unrepresentable — Alignment: 2/3

**Current state:**
The overwhelming majority of state is encoded correctly: frozen structs, StrEnum for
`TableSizeTier`, `Final` for view name constants. The two gaps are:

1. `CompiledExecutionPolicy.validation_mode: str` — value is restricted to
   `ValidationMode` by convention but not by type. An out-of-range string such as
   `"strict"` would survive construction and be silently ignored by downstream consumers.
2. `DecisionRecord.timestamp_ms: int = 0` — the default `0` is a sentinel for "not set"
   but is not semantically distinguishable from epoch time. There is no `None` option
   or dedicated sentinel type.

**Findings:**
- `src/relspec/compiled_policy.py:22,80` — `validation_mode: str` should be
  `ValidationMode`.
- `src/relspec/decision_provenance.py:123` — `timestamp_ms: int = 0` uses the
  zero-sentinel pattern rather than `int | None`.

**Suggested improvement:**
1. Change `validation_mode: str = "warn"` to `validation_mode: ValidationMode = "warn"`.
2. Change `timestamp_ms: int = 0` to `timestamp_ms: int | None = None`; treat `None` as
   "decision timestamp not recorded" rather than using epoch time as sentinel. Update
   any downstream comparisons accordingly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. Command–Query Separation — Alignment: 3/3

**Current state:**
All `relspec` functions are pure data transforms returning new structs; none mutates
state and returns it simultaneously. `build_graph_product` and `orchestrate_build` are
commands: they return results but the result is the record of what happened, not a
hidden mutation of existing state. The `calibrate_from_execution_metrics` function
returns a new `PolicyCalibrationResult` without modifying `current_thresholds`
in-place. `build_provenance_graph` constructs a new graph from its inputs; it does not
mutate the `compiled_policy` argument.

**Findings:**
- No violations.

---

### Category: Composition (P12-P15)

#### P12. Dependency Inversion + Explicit Composition — Alignment: 3/3

**Current state:**
All heavy dependencies are injected or deferred:
- `LineagePort` and `DatasetSpecProvider` are Protocol parameters on `InferredDepsInputs`
  with module-level default singletons (`_DEFAULT_LINEAGE_PORT`,
  `_DEFAULT_DATASET_SPEC_PROVIDER`) that callers can replace.
- `policy_compiler.py:_derive_cache_policies_rust` defers the `datafusion_ext` import
  to the function body (line 267: `from datafusion_engine.extensions import datafusion_ext`)
  and guards it with `getattr(..., None)` + `callable(...)` before invocation.
- `build_pipeline.py` defers all extraction, semantics, and engine imports to the
  `_run_*` helper bodies, keeping the module importable without triggering the full
  dependency graph.

**Findings:**
- No violations.

---

#### P13. Composition over Inheritance — Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the scope. All extension points are Protocols or
default-override patterns. `StructBaseStrict` and `StructBaseCompat` are base types
from `serde_msgspec` used only for msgspec wire compatibility, not for behavioural
inheritance.

**Findings:**
- No violations.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
The majority of attribute access is one level deep. Two violations stand out:

**Finding 1:** `inferred_deps.py:124-125`:
```python
snapshot: Mapping[str, object] | object = getattr(
    getattr(bundle, "artifacts", None),
    "udf_snapshot",
    {},
)
```
This is a two-step `getattr` chain through `bundle.artifacts.udf_snapshot`. The
function already has the `bundle` as a `DataFusionPlanArtifact` (type-annotated), so
the intermediate attribute `bundle.artifacts` is a real, typed object. The `getattr`
chain is used for defensive programming at the untyped FFI boundary, but it violates
the principle by navigating two levels from the caller's collaborator.

**Finding 2:** `execution_package.py:90-91`:
```python
semantic_ir = getattr(manifest, "semantic_ir", None)
ir_hash = getattr(semantic_ir, "ir_hash", None)
```
Again two levels: `manifest.semantic_ir.ir_hash`. This is inside `_hash_manifest` which
accepts a union Protocol type, so the double-dereference is a Protocol compliance check.
The Protocol-union approach (`ManifestHashLike | ManifestWithSemanticIr`) is the correct
direction but the chained access still violates LoD.

**Suggested improvement:**
For finding 1: add a property or method `DataFusionPlanArtifact.udf_snapshot` that
encapsulates the `artifacts.udf_snapshot` access, and call it directly. The
`_DataFusionLineagePort.resolve_required_udfs` already navigates this same path
(inferred_deps.py:124); centralising it in the type prevents two-level chains.

For finding 2: collapse the two Protocols into one that exposes `ir_or_model_hash() -> str`
and implement it on both manifest types, so `_hash_manifest` calls a single method.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, Don't Ask — Alignment: 3/3

**Current state:**
`DiagnosticsPolicy` exposes `fingerprint()` and `fingerprint_payload()`; callers do not
extract its fields to compute a hash externally. `PipelinePolicy` similarly delegates
to its children. `ExecutionPackageArtifact` is produced by `build_execution_package`
which takes protocol objects and calls their hash accessors; callers do not extract raw
fields and compute the fingerprint themselves.

**Findings:**
- No violations.

---

### Category: Correctness (P16-P18)

#### P16. Functional Core, Imperative Shell — Alignment: 3/3

**Current state:**
The split is clean. The functional core:
- `policy_compiler.py` — pure mapping from `CompileExecutionPolicyRequestV1` to
  `CompiledExecutionPolicy`; no IO.
- `policy_calibrator.py` — pure EMA math + clamping; no IO.
- `decision_provenance.py` — pure graph construction and traversal functions.
- `inference_confidence.py` — pure constructors.
- `calibration_bounds.py` — pure validation.
- `inferred_deps.py` (aside from the deferred imports in the adapter classes) — pure
  struct assembly from lineage report.

The imperative shell:
- `graph/build_pipeline.py` — all IO, subprocess-equivalent (Rust FFI), timing, logging.
- `graph/product_build.py` — OTel configuration, signal handling, UUID generation.

**Findings:**
- No violations.

---

#### P17. Idempotency — Alignment: 2/3

**Current state:**
All policy compilation and inference functions are stateless pure transforms; calling
them twice with the same inputs produces identical outputs. The exception is
`ExecutionPackageArtifact` produced by `build_execution_package`
(execution_package.py:217):

```python
created_at_unix_ms=int(time.time() * 1000),
```

This wall-clock timestamp means two calls to `build_execution_package` with all other
inputs identical produce different `package_fingerprint` values (because the timestamp
is included in `_composite_fingerprint`). A retry of a failed build that reconstructs
the execution package from the same manifest, policy, and plan-bundle fingerprints will
produce a different `package_fingerprint`, defeating its purpose as a stable cache key.

**Findings:**
- `src/relspec/execution_package.py:217` — `created_at_unix_ms=int(time.time() * 1000)`
  is injected into a struct that is otherwise fully fingerprinted from deterministic
  inputs.

**Suggested improvement:**
Move `created_at_unix_ms` out of `_composite_fingerprint`. It should be recorded as
metadata but excluded from the hash computation. The `_composite_fingerprint` call
(lines 152-161) should not include the timestamp. Alternatively, accept
`created_at_unix_ms` as an optional parameter to `build_execution_package` so that
callers who need replay stability can provide a fixed value.

**Effort:** small
**Risk if unaddressed:** medium

---

#### P18. Determinism / Reproducibility — Alignment: 2/3

**Current state:**
The `plan_fingerprint` in `InferredDeps`, `policy_fingerprint` in
`CompiledExecutionPolicy`, and `spec_hash` in `SemanticExecutionSpec` all follow the
canonical hash-from-stable-inputs pattern using `hash_msgpack_canonical` and
`hash_json_canonical`. The `RELATION_OUTPUT_ORDERING_KEYS` constant anchors sort order
for Arrow table outputs. The only non-deterministic element is the same one identified
in P17: `created_at_unix_ms` inside the composite fingerprint of
`ExecutionPackageArtifact`.

**Findings:**
- `src/relspec/execution_package.py:152-161,217` — Wall-clock time is hashed into the
  composite package fingerprint, making reproducibility impossible across retry attempts.

**Suggested improvement:**
Same as P17.

**Effort:** small
**Risk if unaddressed:** medium

---

### Category: Simplicity (P19-P22)

#### P19. KISS — Alignment: 3/3

**Current state:**
No over-engineering is present. `calibration_bounds.py` is 86 lines doing exactly one
thing. `table_size_tiers.py` is 85 lines doing exactly one thing. The `relspec/__init__.py`
lazy-loader adds no concepts — it is the standard module-facade pattern used throughout
the codebase. The calibration EMA is four lines of arithmetic.

**Findings:**
- No violations.

---

#### P20. YAGNI — Alignment: 3/3

**Current state:**
No unused abstractions are present. The `planning_engine/config.py` has `EngineConfigSpec`
and `EngineExecutionOptions` which are clearly used by the CLI and build orchestrator.
`CompiledPlanSummary` is used by the observability module. Legacy aliases in
`planning_engine/output_contracts.py` have explicit `LEGACY_CPG_OUTPUTS` tuples and
`CPG_OUTPUT_CANONICAL_TO_LEGACY` maps rather than trying to anticipate future output
kinds.

**Findings:**
- No violations.

---

#### P21. Principle of Least Astonishment — Alignment: 2/3

**Current state:**
The `CalibrationMode` Literal surprise: callers reading the public API see
`calibrate_from_execution_metrics(mode="observe")` and
`calibrate_from_execution_metrics(mode="warn")` both accepted, both producing results,
but through slightly different code paths (lines 162-163) that resolve to different
`mode` values in the returned `PolicyCalibrationResult`. A caller who tests
`result.mode == "warn"` after passing `mode="warn"` gets `True`, but a caller who
passes `mode="observe"` gets `result.mode == "observe"`, not `"warn"`, despite the
docstring saying these are equivalent. This is asymmetric: the two "equivalent" values
produce non-interchangeable results.

**Findings:**
- `src/relspec/policy_calibrator.py:162-163` — `mode="warn"` returns
  `PolicyCalibrationResult(mode="warn")` but `mode="observe"` returns
  `PolicyCalibrationResult(mode="observe")`. Callers who check `result.mode` will get
  unexpected values when using the documented alias.
- `src/relspec/policy_calibrator.py:177` — Similarly `mode="enforce"` returns `"enforce"`
  but `mode="apply"` returns `"apply"`.

**Suggested improvement:**
Canonicalise at function entry:
```python
_ALIAS_MAP: dict[str, str] = {"observe": "warn", "apply": "enforce"}
mode = _ALIAS_MAP.get(mode, mode)  # type: ignore[arg-type]
```
Then the returned `PolicyCalibrationResult.mode` is always the canonical form. Remove
`"observe"` and `"apply"` from the `CalibrationMode` Literal (or add a separate
`CalibrationModeInput` Literal for the accepted values) so the type system enforces the
canonical output.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and Version Public Contracts — Alignment: 2/3

**Current state:**
`SemanticExecutionSpec` has a `version: int` field and `spec_hash: bytes` for
content-level identity. `planning_engine/output_contracts.py` explicitly maintains
`CANONICAL_CPG_OUTPUTS`, `LEGACY_CPG_OUTPUTS`, and the bidirectional alias maps, which
is exactly the right pattern for a migration window. The `V1` suffix on
`CompileExecutionPolicyRequestV1` and `OrchestrateBuildRequestV1` signals versioned
contracts.

Gaps:
1. `relspec/__init__.py` has no `__version__` export; callers who import `relspec`
   cannot programmatically check the contract version.
2. `planning_engine/__init__.py` has no `__version__` and is effectively empty; the
   stability level of the contracts in `spec_contracts.py` is not stated anywhere.
3. No `__all__` documents a stability boundary in `planning_engine/config.py` (though
   `__all__` is present in the others).

**Findings:**
- `src/relspec/__init__.py` — No `__version__` attribute to signal contract stability.
- `src/planning_engine/__init__.py:1-9` — Empty except for a docstring; no stability
  declaration for the transitional contracts.
- `src/planning_engine/config.py` — No `__all__` to signal which items are stable
  surface vs internal helpers.

**Suggested improvement:**
Add `__version__ = "1.0.0"` or a semver constant to both `relspec/__init__.py` and
`planning_engine/__init__.py`, tied to the `pyproject.toml` version. Add `__all__` to
`planning_engine/config.py` listing only `EngineConfigSpec`, `EngineExecutionOptions`,
and `CompiledPlanSummary`.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (P23-P24)

#### P23. Design for Testability — Alignment: 3/3

**Current state:**
All `relspec` policy functions are pure transforms that accept frozen structs and return
frozen structs. No `relspec` module requires a live DataFusion session to test; the
ports pattern (`LineagePort`, `DatasetSpecProvider`) allows test doubles to be passed
directly. The `policy_calibrator.py:calibrate_from_execution_metrics` takes a plain
`ExecutionMetricsSummary` struct — testable with a single constructor call. The
`build_execution_package` function accepts Protocol types that any stub can satisfy.

The `graph/build_pipeline.py:orchestrate_build` function is harder to test in isolation
because it imports and invokes `codeanatomy_engine` (a compiled Rust extension) without
an injectable seam. The private helpers (`_run_extraction_phase`, `_compile_semantic_phase`,
`_execute_engine_phase`) are extractable individually for integration testing. The lack
of a test seam for the engine call does not affect unit-testability of the policy layer.

**Findings:**
- No critical violations; the one integration seam (Rust FFI) is inherent to the
  architecture and isolated to `build_pipeline.py`.

---

#### P24. Observability — Alignment: 2/3

**Current state:**
`graph/product_build.py` emits structured `codeanatomy.*` OTel events at key lifecycle
points (`build.start`, `build.phase.start`, `build.phase.end`, `build.success`,
`build.failure`) with stable, named fields. Stage spans use `stage_span` with explicit
`stage=` attributes. `policy_compiler.py` logs structured extras
(`codeanatomy.policy_fingerprint`, `codeanatomy.policy_components`) at DEBUG level.

Gaps:
1. The `relspec` package has no OTel spans. `policy_compiler.py:compile_execution_policy`
   is a significant computation (it calls the Rust bridge) but emits only `_LOGGER.debug`
   lines without any span boundary. If the Rust bridge call fails or is slow, the trace
   shows no evidence from this layer.
2. The `infer_deps_from_plan_bundle` function also has no span, despite being the core
   lineage extraction step.
3. Observability calls in `build_pipeline.py:_record_observability` (lines 411-454)
   are intermixed with orchestration logic rather than being a post-build side-channel.

**Findings:**
- `src/relspec/policy_compiler.py:102-167` — No OTel span around `compile_execution_policy`
  or its Rust bridge call at line 223.
- `src/relspec/inferred_deps.py:165-234` — No OTel span around
  `infer_deps_from_plan_bundle`.
- `src/graph/build_pipeline.py:411-454` — `_record_observability` is called from within
  the `stage_span("build_orchestrator")` context, so all observability events appear
  nested under the orchestrator span; this makes it hard to isolate "observability
  overhead" from "build overhead" in a trace.

**Suggested improvement:**
1. Add `with stage_span("policy_compile", stage="relspec", scope_name=SCOPE_PIPELINE):`
   around `compile_execution_policy`, and `with stage_span("lineage_infer", ...):`
   around `infer_deps_from_plan_bundle`. These are the two hottest paths in the policy
   layer and the most valuable spans for diagnosing slowdowns.
2. Move `_record_observability(spec, run_result)` outside the `stage_span("build_orchestrator")`
   context manager (i.e., after the `with` block closes) so observability emission is
   clearly outside the measured build time.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Timestamp in the Fingerprint Layer

`ExecutionPackageArtifact.created_at_unix_ms` is included in the composite fingerprint
computation (execution_package.py:154-161). This is the single thread that unravels
otherwise exemplary determinism across the policy and planning layers. The root cause is
a conflation of "when was this artifact created" (provenance metadata) with "what inputs
determined this execution" (fingerprint key). These are different concerns; the timestamp
should be a metadata field that is excluded from `_composite_fingerprint`.

Affected principles: P17, P18.

### Theme 2: Legacy Alias Leakage in CalibrationMode

The `CalibrationMode` Literal accepts both canonical names and legacy aliases, but the
normalisation happens inside the function body rather than at the API boundary. This
means the alias leaks into the return type (`PolicyCalibrationResult.mode`) and into any
downstream logic that matches on `result.mode`. The fix is to normalise at entry, use
only canonical names internally, and tighten the Literal to canonical values only.

Affected principles: P7, P21, P10 (indirectly, because `ValidationMode` is also an
under-enforced Literal).

### Theme 3: Missing OTel Spans in relspec Core

The relspec policy layer has no span instrumentation despite being on the hot path. The
three highest-value additions are:
- `compile_execution_policy` (includes a Rust FFI call)
- `infer_deps_from_plan_bundle` (includes DataFusion lineage extraction)
- `_derive_cache_policies_rust` (the Rust bridge call itself)

The logging infrastructure (`_LOGGER.debug`) is in place; adding `stage_span` wrappers
is a one-line addition per function entry.

Affected principles: P24.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P17/P18 | Remove `created_at_unix_ms` from `_composite_fingerprint` in `execution_package.py:154-161` | small | Restores replay stability for retried builds |
| 2 | P7/P21 | Normalise `CalibrationMode` aliases at function entry in `policy_calibrator.py` | small | Eliminates surprising `.mode` values in results |
| 3 | P10 | Change `compiled_policy.py:80` `validation_mode: str` to `validation_mode: ValidationMode` | small | Enforces Literal constraint at msgspec decode |
| 4 | P24 | Add `stage_span` around `compile_execution_policy` and `infer_deps_from_plan_bundle` | small | Adds visibility to the two hottest relspec paths |
| 5 | P14 | Add `udf_snapshot` property to `DataFusionPlanArtifact` to collapse two-level `getattr` chain in `inferred_deps.py:124-125` | small | Closes LoD violation and centralises attribute access |

---

## Recommended Action Sequence

1. **Remove wall-clock timestamp from composite fingerprint** (P17/P18)
   — `src/relspec/execution_package.py:_composite_fingerprint` (lines 152-161): remove
   `created_at_unix_ms` from the payload tuple. Keep the field in `ExecutionPackageArtifact`
   as provenance metadata but do not hash it. Update `build_execution_package` to accept
   optional `created_at_unix_ms` parameter (defaults to `int(time.time() * 1000)`) for
   callers who want to override during replay.

2. **Tighten CalibrationMode aliases** (P7, P21)
   — `src/relspec/policy_calibrator.py`: add `_ALIAS_MAP = {"observe": "warn", "apply": "enforce"}`
   and normalise `mode` on line 137 before any branching. Remove `"observe"` and `"apply"`
   from `CalibrationMode` (or keep a separate `CalibrationModeInput` union). Verify no
   callers check `result.mode == "observe"` or `result.mode == "apply"`.

3. **Enforce ValidationMode Literal** (P10, P7)
   — `src/relspec/compiled_policy.py:80`: change field type from `str` to `ValidationMode`.
   Run msgspec decode against existing serialized policies to confirm no out-of-range
   values exist in stored artifacts.

4. **Add OTel spans to relspec hot paths** (P24)
   — `src/relspec/policy_compiler.py:compile_execution_policy`: wrap body in
   `stage_span("policy_compile", stage="relspec", scope_name=SCOPE_PIPELINE)`.
   — `src/relspec/inferred_deps.py:infer_deps_from_plan_bundle`: wrap body in
   `stage_span("lineage_infer", stage="relspec", scope_name=SCOPE_PIPELINE)`.

5. **Collapse two-level getattr chain** (P14)
   — `src/relspec/inferred_deps.py:124-125`: add `udf_snapshot` property to
   `DataFusionPlanArtifact` that encapsulates `self.artifacts.udf_snapshot`. Call
   `bundle.udf_snapshot` directly. Similarly in `execution_package.py:90-91`, consider
   collapsing the two Manifest protocols into one with a single `ir_or_model_hash()`.

6. **Add `__all__` to `planning_engine/config.py`** (P22)
   — Declare stable public surface: `EngineConfigSpec`, `EngineExecutionOptions`,
   `CompiledPlanSummary`. Internal aliases (`EngineProfile`, `RulepackProfile`,
   `TracingPreset`, `ExtractionConfig`, `IncrementalConfig`) are type aliases only and
   may remain or be re-exported by choice.

7. **Narrow `orchestrate_build` scope** (P2, P3) — last because it requires care
   — `src/graph/build_pipeline.py:orchestrate_build`: extract `_collect_outputs` and
   `_record_observability_for_build` as two separate calls in `_execute_build` in
   `product_build.py`, after `orchestrate_build` returns `BuildResult`. This ensures
   the pipeline function is responsible only for stage sequencing.
