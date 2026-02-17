# Design Review: src/relspec

**Date:** 2026-02-17
**Scope:** `src/relspec/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 14 (2,823 LOC)

## Executive Summary

The `src/relspec/` module is well-designed and lean. It demonstrates strong adherence to most design principles: clean serializable contracts via `msgspec.Struct`, frozen immutable data structures, pure functions for policy compilation and calibration, well-separated concerns across focused modules, and explicit `__all__` declarations. The primary weaknesses are (1) coupling to `datafusion_engine` types in `contracts.py`, `policy_compiler.py`, and `inferred_deps.py` that violates dependency direction for a middle-ring module, and (2) the use of `object`-typed parameters in `execution_package.py` that relies on `getattr` duck typing instead of protocols. Overall, relspec is one of the cleanest modules in the codebase and serves as a good reference for design patterns.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | - | - | Clean module boundaries; internals hidden behind `__all__` |
| 2 | Separation of concerns | 3 | - | - | Each module has a clear single purpose |
| 3 | SRP | 3 | - | - | Every module changes for exactly one reason |
| 4 | High cohesion, low coupling | 2 | small | low | `contracts.py` mixes schema metadata with protocol defs and request envelopes |
| 5 | Dependency direction | 1 | medium | high | 19 imports from `datafusion_engine` across module; middle ring depends on engine ring |
| 6 | Ports & Adapters | 2 | medium | medium | `TaskGraphLike` and `ScanOverrideLike` protocols show the pattern; but `DatasetLocation` and `ViewNode` are concrete imports |
| 7 | DRY | 3 | - | - | Constants centralized in `view_defs.py`; calibration bounds in `calibration_bounds.py` |
| 8 | Design by contract | 3 | - | - | `validate_calibration_bounds` enforces invariants; `SemanticSchemaError` for join key validation |
| 9 | Parse, don't validate | 3 | - | - | `InferredDeps` is a parsed representation; `CompiledExecutionPolicy` is a compile-time artifact |
| 10 | Illegal states | 3 | - | - | `CachePolicyValue` is a Literal type; `CalibrationMode` is a Literal union; frozen structs |
| 11 | CQS | 3 | - | - | All policy compilation functions are pure queries returning frozen structs |
| 12 | DI + explicit composition | 2 | small | low | `compile_execution_policy` takes `CompileExecutionPolicyRequestV1` envelope; but `inferred_deps.py` uses `importlib` for optional imports |
| 13 | Composition over inheritance | 3 | - | - | No inheritance; all behavior via composition |
| 14 | Law of Demeter | 2 | small | low | `policy_compiler.py:202-203`: `task_graph.graph.out_degree(node_idx)` and `task_graph.task_idx` chains |
| 15 | Tell, don't ask | 3 | - | - | Protocols define what collaborators must provide; caller tells, doesn't interrogate |
| 16 | Functional core | 3 | - | - | Pure functions: `compile_execution_policy`, `calibrate_from_execution_metrics`, `build_provenance_graph` |
| 17 | Idempotency | 3 | - | - | All functions are stateless transforms with no side effects |
| 18 | Determinism | 3 | - | - | `hash_json_canonical` for policy fingerprints; `hash_msgpack_canonical` for execution packages |
| 19 | KISS | 3 | - | - | Small, focused modules; no unnecessary abstractions |
| 20 | YAGNI | 2 | - | low | `DecisionOutcome.notes` and `DecisionRecord.timestamp_ms` unused infrastructure |
| 21 | Least astonishment | 2 | small | low | `execution_package.py` uses `object` params with `getattr` instead of typed protocols |
| 22 | Public contracts | 3 | - | - | All modules declare `__all__`; `StructBaseStrict` enforces frozen serialization contracts |
| 23 | Testability | 3 | - | - | Pure functions with frozen inputs/outputs; no IO or global state |
| 24 | Observability | 2 | small | low | `_LOGGER` declared in `inferred_deps.py` and `policy_compiler.py` but sparse structured logging |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 3/3

**Current state:**
All modules declare explicit `__all__` exports. Internal helper functions use underscore prefixes consistently. The `__init__.py` uses a lazy `__getattr__` pattern to avoid importing everything at module load time while providing a clean public surface.

No action needed.

---

#### P2. Separation of concerns -- Alignment: 3/3

**Current state:**
Each module has a distinct, well-bounded responsibility:
- `compiled_policy.py`: Serializable policy artifact struct
- `policy_compiler.py`: Policy derivation logic
- `inferred_deps.py`: Dependency inference from plan bundles
- `execution_package.py`: Reproducibility fingerprinting
- `decision_provenance.py`: Decision audit trail
- `inference_confidence.py`: Confidence metadata construction
- `calibration_bounds.py`: Calibration range constants
- `policy_calibrator.py`: Adaptive threshold adjustment
- `table_size_tiers.py`: Table classification constants
- `contracts.py`: Schema metadata and protocol definitions
- `view_defs.py`: Canonical output name constants

No action needed.

---

#### P3. SRP -- Alignment: 3/3

**Current state:**
Every module changes for exactly one reason. `compiled_policy.py` changes only when the policy artifact schema evolves. `policy_compiler.py` changes only when derivation logic changes. `policy_calibrator.py` changes only when the calibration algorithm changes.

No action needed.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
`contracts.py` combines three distinct concerns: (1) protocol definitions for `TaskGraphLike`/`ScanOverrideLike`, (2) the `CompileExecutionPolicyRequestV1` request envelope, and (3) schema metadata spec construction functions (`relspec_metadata_spec`, `rel_name_symbol_metadata_spec`, etc.).

**Findings:**
- `contracts.py:41-47`: `TaskGraphLike` and `ScanOverrideLike` protocols (structural contracts)
- `contracts.py:57-67`: `CompileExecutionPolicyRequestV1` request envelope (request DTOs)
- `contracts.py:70-185`: Schema metadata spec functions (output metadata concerns)

**Suggested improvement:**
Move the metadata spec functions (`_schema_metadata`, `_metadata_spec`, `rel_*_metadata_spec`, `relspec_metadata_spec`) into a dedicated `relspec/metadata.py` module. Keep `contracts.py` focused on protocol definitions and request envelopes.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
`src/relspec/` is conceptually a middle-ring module (task/plan scheduling), but it imports 19+ symbols from `datafusion_engine/` (engine ring). These imports are both runtime and TYPE_CHECKING, but several are runtime imports in core logic paths.

**Findings:**
- `inferred_deps.py:13`: `from datafusion_engine.lineage.reporting import ScanLineage` -- runtime import of engine type in data contract
- `inferred_deps.py:121`: `from datafusion_engine.lineage.reporting import extract_lineage` -- runtime engine dependency in core inference
- `inferred_deps.py:140`: `from datafusion_engine.views.bundle_extraction import resolve_required_udfs_from_bundle` -- engine internal
- `inferred_deps.py:162`: `from datafusion_engine.udf.extension_core import validate_required_udfs` -- engine UDF validation
- `inferred_deps.py:188`: `from datafusion_engine.schema import nested_path_for` -- engine schema utility
- `inferred_deps.py:256`: `from schema_spec.dataset_spec import dataset_spec_schema` -- schema spec dependency
- `inferred_deps.py:336`: `from datafusion_engine.schema.contracts import schema_contract_from_dataset_spec` -- engine contract
- `contracts.py:11-16`: Arrow metadata imports from engine
- `pipeline_policy.py:10`: `from datafusion_engine.tables.param import ParamTablePolicy` -- engine param table type
- `policy_compiler.py:27-29`: TYPE_CHECKING imports from engine (acceptable but numerous)

**Suggested improvement:**
`inferred_deps.py` is the worst offender. Extract a `LineageExtractor` protocol that `datafusion_engine` implements. The `infer_deps_from_plan_bundle` function should accept a protocol-typed `lineage_extractor` instead of importing `extract_lineage` directly. Similarly, `ScanLineage` in the `InferredDeps` struct could be replaced with a relspec-owned lineage type that the engine maps to.

For `contracts.py`, the `SchemaMetadataSpec` construction functions should move to the engine side, with relspec defining only the metadata contract (names, versions).

For `pipeline_policy.py`, the `ParamTablePolicy` import should be replaced with a protocol or the field typed as a generic policy struct.

**Effort:** medium
**Risk if unaddressed:** high -- prevents independent testing and evolution of scheduling logic

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`relspec/contracts.py` defines `TaskGraphLike`, `OutDegreeGraph`, and `ScanOverrideLike` protocols, which is the right pattern. However, `DatasetLocation`, `DataFusionRuntimeProfile`, and `ViewNode` are imported as concrete types rather than through protocols.

**Findings:**
- `contracts.py:41-47`: `TaskGraphLike` protocol -- excellent example of port pattern
- `contracts.py:50-54`: `ScanOverrideLike` protocol -- well-designed
- `contracts.py:60-61`: `CompileExecutionPolicyRequestV1` uses concrete `DatasetLocation` and `DataFusionRuntimeProfile` types
- `inferred_deps.py:20-24`: TYPE_CHECKING imports of `DataFusionPlanArtifact`, `SchemaContract`, `ViewNode` -- concrete engine types

**Suggested improvement:**
Extend the protocol pattern to cover `DatasetLocation` (define a `DatasetLocationLike` protocol with the fields the compiler needs) and `RuntimeProfile` (define `RuntimeProfileLike` with the minimum required attributes). This would complete the port abstraction.

**Effort:** medium
**Risk if unaddressed:** medium

---

### Category: Knowledge (7-11)

#### P7. DRY -- Alignment: 3/3

**Current state:**
Constants are well-centralized. `view_defs.py` is the single source for output view names. `table_size_tiers.py` centralizes row-count thresholds. `calibration_bounds.py` centralizes calibration ranges. No semantic duplication detected.

No action needed.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
`validate_calibration_bounds` explicitly checks bound ordering invariants. `infer_deps_from_plan_bundle` raises `ValueError` for missing plan bundles. `_normalize_cache_policy_value` validates against an explicit allowed set.

No action needed.

---

#### P10. Illegal states unrepresentable -- Alignment: 3/3

**Current state:**
`CachePolicyValue` uses `Literal["none", "delta_staging", "delta_output"]` to constrain valid values. `CalibrationMode` uses `Literal["off", "warn", "enforce", "observe", "apply"]`. All policy structs are `frozen=True`, preventing mutation after construction. `TableSizeTier` is a `StrEnum`.

No action needed.

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`compile_execution_policy` accepts a request envelope with all dependencies, which is clean DI. However, `inferred_deps.py` uses `importlib.import_module` for optional dependency resolution (`_optional_module_attr` at line 280), which is a hidden dependency mechanism.

**Findings:**
- `policy_compiler.py:92`: `compile_execution_policy(request: CompileExecutionPolicyRequestV1)` -- clean request envelope DI
- `inferred_deps.py:280-285`: `_optional_module_attr(module, attr)` -- uses `importlib` to optionally load engine modules at runtime
- `inferred_deps.py:288-299`: `_extract_dataset_spec` uses `_optional_module_attr` to dynamically resolve `datafusion_engine.extract_registry.dataset_spec`

**Suggested improvement:**
Replace `_optional_module_attr` dynamic imports with an explicit `DatasetSpecProvider` protocol injected via the `InferredDepsInputs` dataclass. This makes the optional dependency explicit rather than hidden behind `importlib`.

**Effort:** small
**Risk if unaddressed:** low -- dynamic imports work but make dependency graph opaque

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
`policy_compiler.py` accesses `task_graph.task_idx` and `task_graph.graph.out_degree(node_idx)` through the `TaskGraphLike` protocol, which is a two-level chain. The protocol design makes this acceptable, but the chain could be simplified.

**Findings:**
- `policy_compiler.py:202-203`: `for task_name, node_idx in task_graph.task_idx.items(): out_degree = task_graph.graph.out_degree(node_idx)` -- traverses protocol chain
- `policy_compiler.py:462`: `location.delta_maintenance_policy` -- single-level access (fine)

**Suggested improvement:**
Add an `out_degree(task_name: str) -> int` method to `TaskGraphLike` to flatten the chain. The protocol can provide this as a single entry point rather than exposing `task_idx` and `graph` separately.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core -- Alignment: 3/3

**Current state:**
All core functions are pure transforms:
- `compile_execution_policy` takes a request and returns a frozen policy
- `calibrate_from_execution_metrics` takes metrics and returns calibration results
- `build_provenance_graph` takes a compiled policy and returns a frozen graph
- `build_execution_package` takes component hashes and returns a frozen artifact

No IO, no mutation, no side effects in the core logic.

No action needed.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All functions are stateless. Calling `compile_execution_policy` with the same inputs always produces the same `CompiledExecutionPolicy` (deterministic fingerprint). `build_execution_package` produces the same composite fingerprint for the same component hashes.

No action needed.

---

#### P18. Determinism -- Alignment: 3/3

**Current state:**
`_compute_policy_fingerprint` uses `hash_json_canonical` for deterministic hashing. `_composite_fingerprint` in `execution_package.py` uses `hash_msgpack_canonical`. Both produce stable outputs across runs.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 3/3

**Current state:**
Modules are small and focused. The largest module is `policy_compiler.py` at ~550 LOC, which is well within acceptable limits. The calibration logic uses a simple EMA algorithm with clear bounds clamping. No unnecessary abstractions.

No action needed.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The `DecisionOutcome` struct and `DecisionRecord.timestamp_ms` field represent infrastructure for a feedback loop that is not yet wired. The provenance graph query helpers (`decision_chain`, `decision_children`, `decisions_above_confidence`) are implemented but may not have callers yet.

**Findings:**
- `decision_provenance.py:53-74`: `DecisionOutcome` struct with `success`, `metric_name`, `metric_value`, `notes` fields
- `decision_provenance.py:109`: `timestamp_ms: int = 0` -- defaults to 0, suggesting it's not populated
- `decision_provenance.py:246-288`: `decision_chain` traversal logic -- potentially unused

**Suggested improvement:**
This is borderline YAGNI. The types are designed with clear extension seams and the code is small. Keep the types but document that `DecisionOutcome` and provenance graph queries are reserved for the upcoming calibration feedback loop. No code removal needed, but avoid building on this until the feedback loop is implemented.

**Effort:** n/a
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
`execution_package.py` uses `object`-typed parameters with `getattr` duck typing instead of the typed protocols defined in the same module. The protocols exist (`SemanticIrHashLike`, `ManifestHashLike`, etc.) but are not used in the function signatures.

**Findings:**
- `execution_package.py:159-166`: `build_execution_package(*, manifest: object | None = None, compiled_policy: object | None = None, ...)` -- uses `object` despite protocols
- `execution_package.py:77-90`: `_hash_manifest(manifest: object | None)` -- uses `getattr` duck typing
- `execution_package.py:24-57`: `SemanticIrHashLike`, `ManifestHashLike`, `PolicyFingerprintLike`, `SettingsHashValueLike`, `SettingsHashCallableLike` protocols exist but are unused in signatures

**Suggested improvement:**
Use the defined protocols in the function signatures: `manifest: ManifestWithSemanticIr | None`, `compiled_policy: PolicyFingerprintLike | None`, `capability_snapshot: SettingsHashValueLike | None`, `session_config: SettingsHashCallableLike | str | None`. This makes the expected interface explicit and enables IDE support.

**Effort:** small
**Risk if unaddressed:** low -- but `getattr` duck typing is surprising when protocols are already defined

---

### Category: Quality (23-24)

#### P23. Testability -- Alignment: 3/3

**Current state:**
All core functions are pure and accept simple data inputs. `CompiledExecutionPolicy` and `InferredDeps` are frozen structs easily constructed in tests. `PolicyCalibrationResult` is a frozen result. No IO fixtures needed for testing core logic.

No action needed.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`_LOGGER` is declared in `inferred_deps.py` and `policy_compiler.py`, but structured logging is sparse. The policy compiler produces a `CompiledExecutionPolicy` artifact for audit, which is good, but runtime diagnostic logging during compilation is minimal.

**Findings:**
- `inferred_deps.py:27`: `_LOGGER = logging.getLogger(__name__)` -- declared
- `inferred_deps.py:156-159`: Single warning log for ignored UDFs
- `policy_compiler.py:33`: `_LOGGER = logging.getLogger(__name__)` -- declared but no log calls found in the module

**Suggested improvement:**
Add structured debug logging in `_derive_cache_policies` for each task's resolved policy (useful for debugging why a view got `delta_staging` vs `none`). Add info-level logging in `calibrate_from_execution_metrics` when thresholds are adjusted.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Engine Ring Coupling (P5, P6)

The most significant architectural concern. `inferred_deps.py` has the densest coupling, with runtime imports from 6 different `datafusion_engine` submodules. The `contracts.py` module mixes protocol definitions with engine-specific metadata construction. The `pipeline_policy.py` imports `ParamTablePolicy` from the engine.

**Root cause:** The dependency inference path inherently touches engine internals (plan bundles, lineage, UDF snapshots). The boundary between "what do I need from the engine" vs "what does the engine provide" is not formalized as a port.

**Affected principles:** 5, 6, 12, 23
**Suggested approach:** Define a `LineagePort` protocol in `relspec/ports.py` with methods like `extract_lineage(plan) -> LineageResult` and `resolve_udfs(bundle) -> tuple[str, ...]`. Have the engine implement this port. `infer_deps_from_plan_bundle` should accept a `LineagePort` instead of importing engine internals.

### Theme 2: Protocol Definitions Not Fully Utilized (P6, P21)

The module defines excellent protocols (`TaskGraphLike`, `ScanOverrideLike`, `SemanticIrHashLike`, etc.) but doesn't consistently use them in function signatures. `execution_package.py` defines 5 protocols but uses `object` in all signatures. This creates a gap between documented intent and actual type safety.

**Affected principles:** 6, 21, 22
**Suggested approach:** Replace `object` parameters in `execution_package.py` with the corresponding protocols. This is a small change with high documentation value.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P21 | Use defined protocols in `execution_package.py` function signatures instead of `object` | small | Type safety + IDE support |
| 2 | P4 | Extract metadata spec functions from `contracts.py` into `relspec/metadata.py` | small | Better cohesion |
| 3 | P14 | Add `out_degree(task_name)` to `TaskGraphLike` protocol | small | Simplifies policy compiler |
| 4 | P24 | Add structured debug logging in `_derive_cache_policies` | small | Better operational visibility |
| 5 | P12 | Replace `_optional_module_attr` with explicit `DatasetSpecProvider` protocol | small | Makes optional deps explicit |

## Recommended Action Sequence

1. **P21** -- Replace `object` with protocols in `execution_package.py`. Zero-risk, immediate type safety gain.
2. **P4** -- Split `contracts.py` into protocols + metadata modules. Improves cohesion.
3. **P14** -- Simplify `TaskGraphLike` protocol with direct `out_degree(task_name)` method.
4. **P24** -- Add structured debug logging in policy compiler derivation paths.
5. **P5/P6** -- Define `LineagePort` protocol and refactor `inferred_deps.py` to use it. Largest effort, highest architectural value.
6. **P12** -- Replace `importlib`-based optional imports with protocol injection.
