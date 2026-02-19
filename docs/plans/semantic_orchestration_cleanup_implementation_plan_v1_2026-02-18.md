# Semantic Pipeline + Orchestration Spine Cleanup
## Implementation Plan v1

**Date:** 2026-02-18
**Scope:** `src/semantics/` + `src/relspec/` + `src/graph/` + `src/planning_engine/` +
`src/datafusion_engine/` + `rust/`
**Source Reviews:**
- `docs/reviews/design_review_semantic_pipeline_2026-02-18.md` (Agent 1)
- `docs/reviews/design_review_obs_relspec_cli_utils_2026-02-18.md` (Agent 3)
**Design Stance:** No compatibility shims. Hard cutover for all renames. Additive changes
only for new protocols and constants.

---

## Scope Summary

Twenty-six targeted actions across six packages. This plan keeps the original S1-S14
semantic-orchestration cleanup scope and adds S15-S26 DataFusion-native convergence scope.
The changes fall into six thematic groups:

1. **Determinism and DRY** (S1, S3, S7): Eliminate confidence-constant duplication,
   fix the plan-fingerprint fallback, and remove wall-clock time from the composite
   fingerprint computation.
2. **Contract hygiene** (S2, S9, S14): Remove the `RelationshipSpec` migration debt, enforce
   the `ValidationMode` Literal type, and declare the `planning_engine/config.py` public
   surface.
3. **CQS and naming clarity** (S4, S5, S6, S8, S13): Rename CQS-violating methods, promote
   private builder helpers, replace an ad-hoc normalization dict reconstruction, normalize
   calibration-mode aliases at entry, and document identity-function naming.
4. **Observability and code shape** (S10, S11, S12): Add OTel spans to relspec hot paths,
   collapse a two-level `getattr` chain, and narrow the `orchestrate_build` function to
   stage dispatch only.
5. **DataFusion planning convergence** (S15, S16, S17, S19, S23): unify plan fingerprint/
   identity surfaces, remove private Substrait internal usage, and make planning artifacts
   first-class orchestration inputs.
6. **Cross-language planning policy and extension discipline** (S18, S20, S21, S22, S24,
   S25, S26): deduplicate runtime-setting application, align Python/Rust planning policy,
   enforce builder-native session composition, and standardize rule installer/idempotency
   behavior.

Estimated total effort: 8–12 engineer-days. S4 and S5 are medium-risk refactors, and
S15-S26 introduce medium-risk architecture work across Python/Rust boundaries. No
schema migrations are required, but plan/cache contract version bumps may be needed.

---

## Design Principles

- **No shims.** Every rename is a hard cutover with all call sites updated in the same
  commit. Legacy names are not kept as aliases.
- **Additive for new exports.** When a new constant or property is added (`CONFIDENCE_BY_STRATEGY`,
  `udf_snapshot` property), it is exported cleanly and the old definition is deleted in
  the same change.
- **Single-file scope.** Each scope item touches at most three files. Cross-file changes
  that require coordination are sequenced so downstream consumers are updated before the
  upstream definition is removed.
- **No test-only mocks for internal methods.** The rename of `get_or_register` →
  `ensure_registered` and `resolve` → `ensure_and_get` applies to all call sites
  including tests; no compatibility stub is added.
- **DataFusion-native first for planning surfaces.** Prefer DataFusion builder/config/
  plan-artifact primitives over bespoke orchestration logic when the capability exists.
- **Full quality gate at task completion.** Run `uv run ruff format && uv run ruff check --fix
  && uv run pyrefly check && uv run pyright && uv run pytest -q` after implementation is complete.

---

## Current Baseline

The codebase is in a healthy transitional state. The semantic pipeline has an explicit
compile/infer/optimize/emit IR structure, clean ports-and-adapters boundaries, and deep
use of frozen `msgspec.Struct` and frozen dataclasses. The orchestration spine has pure
policy functions, deferred heavy imports, and a clean dependency hierarchy from `graph/`
down through `planning_engine/` and `relspec/`.

The issues addressed by this plan are residues of incremental feature additions:

- `_STRATEGY_CONFIDENCE` in `ir_pipeline.py:1088-1093` was added when IR inference was
  introduced and was never wired back to the authority in `joins/inference.py:49-52`.
- `RelationshipSpec` in `specs.py:75-147` was left live when `QualityRelationshipSpec`
  in `quality.py:236-340` replaced it in the registry, leaving dead imports in
  `pipeline_build.py:48` and `pipeline_builders.py:13`.
- `TableRegistry.resolve` (table_registry.py:42-49) was the original lazy-populate
  helper, but the CQS violation it introduces was never addressed when
  `SemanticCompiler.get_or_register` (compiler.py:615-631) was added as a wrapper.
- `CalibrationMode` (policy_calibrator.py:26) retained legacy aliases `"observe"` and
  `"apply"` in its Literal type after the normalization note was added to the docstring,
  allowing alias values to appear in `PolicyCalibrationResult.mode`.
- `created_at_unix_ms` in `ExecutionPackageArtifact` (execution_package.py:217) is
  provenance metadata and is already excluded from `_composite_fingerprint`
  (execution_package.py:154-161). The remaining work is explicit documentation and
  optional caller override for replay workflows.

---

## Per-Scope-Item Sections

---

### S1. Centralize Confidence Constants

**Goal:** Eliminate the duplicate numeric confidence values that exist in both
`joins/inference.py` and `ir_pipeline.py`. Create a single exported dict
`CONFIDENCE_BY_STRATEGY` in `joins/inference.py`, import it in `ir_pipeline.py`,
and delete the duplicate `_STRATEGY_CONFIDENCE` definition.

**Representative Code Snippets**

Current state in `src/semantics/joins/inference.py:49-52`:
```python
# Four private module-level floats
_SPAN_CONFIDENCE: float = 0.95
_FK_CONFIDENCE: float = 0.85
_SYMBOL_CONFIDENCE: float = 0.75
_FILE_EQUI_CONFIDENCE: float = 0.6
```

Current state in `src/semantics/ir_pipeline.py:1088-1093` (duplicate, different form):
```python
_STRATEGY_CONFIDENCE: dict[str, float] = {
    "span_overlap": 0.95,
    "foreign_key": 0.85,
    "symbol_match": 0.75,
    "equi_join": 0.6,
}
```

After change in `src/semantics/joins/inference.py` (insert after line 52):
```python
# Single canonical source of truth for confidence by strategy name.
# Consumed by ir_pipeline.py for lightweight field-based inference.
CONFIDENCE_BY_STRATEGY: dict[str, float] = {
    "span_overlap": _SPAN_CONFIDENCE,
    "foreign_key": _FK_CONFIDENCE,
    "symbol_match": _SYMBOL_CONFIDENCE,
    "equi_join": _FILE_EQUI_CONFIDENCE,
}
```

After change in `src/semantics/ir_pipeline.py` (import section + remove local dict body):
```python
# Import in the module import section; do not redefine confidence values locally.
from semantics.joins.inference import CONFIDENCE_BY_STRATEGY as _STRATEGY_CONFIDENCE
```

Then update the existing usage at `ir_pipeline.py:1103-1130` — the variable name
`_STRATEGY_CONFIDENCE` is unchanged so all downstream dict-lookups continue to work
without further modification.

After change in `src/semantics/joins/__init__.py` — add `CONFIDENCE_BY_STRATEGY` to
the `__all__` list and to the explicit import from `semantics.joins.inference`.

**Files to Edit**
- `src/semantics/joins/inference.py` — add `CONFIDENCE_BY_STRATEGY` dict (line 53,
  after the four private constants)
- `src/semantics/joins/__init__.py` — add `CONFIDENCE_BY_STRATEGY` to import and
  `__all__`
- `src/semantics/ir_pipeline.py` — add the import in the module import section and
  delete the local `_STRATEGY_CONFIDENCE` dict body

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- Delete the `_STRATEGY_CONFIDENCE` dict body at `ir_pipeline.py:1088-1093` (kept as an
  import alias `_STRATEGY_CONFIDENCE` pointing to the canonical dict, so all existing
  lookup calls remain valid without further edits)

---

### S2. Deprecate and Remove RelationshipSpec

**Goal:** Complete the migration from `RelationshipSpec` to `QualityRelationshipSpec`.
Remove `RelationshipSpec` from the active import surface of `pipeline_build.py` and
`pipeline_builders.py`. Confirm no runtime path feeds `RelationshipSpec` instances into
`RELATIONSHIP_SPECS` or `QUALITY_RELATIONSHIP_SPECS` in `registry.py`.

**Representative Code Snippets**

Current state — `src/semantics/pipeline_build.py:48`:
```python
from semantics.specs import RelationshipSpec
```

Current state — `src/semantics/pipeline_build.py:685` (`__all__`):
```python
    "RelationshipSpec",
```

Current state — `src/semantics/pipeline_builders.py:13`:
```python
from semantics.specs import RelationshipSpec
```

Current state — `src/semantics/pipeline_builders.py:132`:
```python
    spec: RelationshipSpec | QualityRelationshipSpec,
```

After change — `pipeline_builders.py:132` (type simplification):
```python
    spec: QualityRelationshipSpec,
```

After change — `src/semantics/specs.py` (add deprecation notice above class definition):
```python
# DEPRECATED: RelationshipSpec is superseded by QualityRelationshipSpec in
# semantics/quality.py. No active code path registers RelationshipSpec instances
# in the quality relationship registry. This class will be removed in a
# subsequent cleanup cycle.
class RelationshipSpec(StructBaseStrict, frozen=True):
```

**Verification step (must pass before commit):** Confirm that `RELATIONSHIP_SPECS` in
`src/semantics/registry.py:196-201` and `QUALITY_RELATIONSHIP_SPECS` in
`src/semantics/quality_specs.py:311` contain only `QualityRelationshipSpec` instances.
Run all of the following checks:
- `rg "RelationshipSpec\\(" src/semantics/registry.py` must return zero results
- `rg "RelationshipSpec" src/ tests/ tools/ --include "*.py"` must only return
  intentionally retained references (for example the deprecated class in `specs.py`)

**Files to Edit**
- `src/semantics/pipeline_build.py` — remove line 48 (`from semantics.specs import
  RelationshipSpec`); remove `"RelationshipSpec"` from `__all__` at line 685
- `src/semantics/pipeline_builders.py` — remove line 13 (`from semantics.specs import
  RelationshipSpec`); narrow union type at line 132 from
  `RelationshipSpec | QualityRelationshipSpec` to `QualityRelationshipSpec`
- `src/semantics/specs.py` — add deprecation comment above `RelationshipSpec` class at
  line 75; do not remove the class itself (removal is a follow-on)

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- `RelationshipSpec` import removed from `pipeline_build.py` and `pipeline_builders.py`
- `"RelationshipSpec"` removed from `pipeline_build.__all__`
- The class definition in `specs.py` is retained but marked deprecated; a follow-on
  scope item can remove it once confirmed no external consumers remain

---

### S3. Fix Plan Fingerprint Fallback

**Goal:** Replace the constant-string fallback `"unknown_plan"` in
`src/semantics/plans/fingerprints.py:223` with a view-name-qualified string to prevent
two distinct failing plans from producing the same fingerprint hash and silently
conflating plan cache keys. This is an interim guardrail pending shared DataFusion
plan-identity consolidation in S15/S16.

**Representative Code Snippets**

Current state — `src/semantics/plans/fingerprints.py:184-223`:
```python
def _compute_logical_plan_hash(df: DataFrame) -> str:
    # ... two try/except blocks for optimized and unoptimized plan ...

    # Ultimate fallback
    return _hash_string("unknown_plan")
```

After change — `_compute_logical_plan_hash` must accept a `view_name` parameter and pass
it to the fallback:
```python
def _compute_logical_plan_hash(df: DataFrame, *, view_name: str = "") -> str:
    # ... existing try/except blocks unchanged ...

    # Ultimate fallback: qualify with view_name to avoid silent conflation
    # of different failing plans under the same constant hash.
    fallback_key = f"unknown_plan:{view_name}" if view_name else "unknown_plan"
    return _hash_string(fallback_key)
```

The call site that invokes `_compute_logical_plan_hash` within `fingerprints.py` must
also be updated to pass the view name. Find the caller with:
```
rg "_compute_logical_plan_hash" src/semantics/plans/fingerprints.py
```
and add `view_name=<name>` at every call site within the same file.

**Files to Edit**
- `src/semantics/plans/fingerprints.py` — update `_compute_logical_plan_hash` signature
  to accept `view_name: str = ""` and update the fallback string at line 223; update all
  internal call sites within the same file

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- The bare `"unknown_plan"` constant string is replaced; no symbol is deleted

---

### S4. Rename CQS-Violating Methods

**Goal:** Rename `TableRegistry.resolve` to `ensure_and_get` and rename
`SemanticCompiler.get_or_register` to `ensure_registered`. Both names currently
obscure that they perform a mutation (registration) as a side effect of a query-like
call. The name `register` in `SemanticCompiler` (compiler.py:597-613) is kept and made
the explicit mutation-only path (unchanged body, name clarified to match intent).

**Representative Code Snippets**

Current state — `src/semantics/table_registry.py:42-49`:
```python
def resolve(self, name: str, factory: Callable[[], TableInfo]) -> TableInfo:
    """Return existing table info or register a newly-created one."""
    existing = self.get(name)
    if existing is not None:
        return existing
    created = factory()
    self.register(name, created)
    return created
```

After change — same body, renamed method:
```python
def ensure_and_get(self, name: str, factory: Callable[[], TableInfo]) -> TableInfo:
    """Register-if-absent and return table info.

    This is a command that may register ``name`` as a side effect.
    Use ``get`` for read-only access when the table is already known to
    be registered.
    """
    existing = self.get(name)
    if existing is not None:
        return existing
    created = factory()
    self.register(name, created)
    return created
```

Current state — `src/semantics/compiler.py:597-631` (two methods with identical bodies):
```python
def register(self, name: str) -> TableInfo:
    return self._registry.resolve(
        name,
        lambda: TableInfo.analyze(name, self.ctx.table(name), config=self._config),
    )

def get_or_register(self, name: str) -> TableInfo:
    return self._registry.resolve(
        name,
        lambda: TableInfo.analyze(name, self.ctx.table(name), config=self._config),
    )
```

After change — `get_or_register` renamed, `_registry.resolve` call updated to
`_registry.ensure_and_get`, duplicate `register` method body also updated:
```python
def register(self, name: str) -> TableInfo:
    """Register and analyze a table (command: always calls ensure_and_get)."""
    return self._registry.ensure_and_get(
        name,
        lambda: TableInfo.analyze(name, self.ctx.table(name), config=self._config),
    )

def ensure_registered(self, name: str) -> TableInfo:
    """Ensure a table is registered and return its info.

    This is a command that may register ``name`` as a side effect when
    the table is not yet in the registry.
    """
    return self._registry.ensure_and_get(
        name,
        lambda: TableInfo.analyze(name, self.ctx.table(name), config=self._config),
    )
```

All call sites of `get_or_register` within `compiler.py` must be updated to
`ensure_registered`. All call sites of `_registry.resolve` within `compiler.py` must
be updated to `_registry.ensure_and_get`.

**Files to Edit**
- `src/semantics/table_registry.py` — rename `resolve` to `ensure_and_get`; update
  docstring
- `src/semantics/compiler.py` — rename `get_or_register` to `ensure_registered`; update
  all `self._registry.resolve(...)` calls to `self._registry.ensure_and_get(...)`

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- Method `resolve` on `TableRegistry` is renamed (no alias retained)
- Method `get_or_register` on `SemanticCompiler` is renamed (no alias retained)

---

### S5. Promote Private Pipeline Builder Helpers

**Goal:** Rename the six private helper symbols consumed by `pipeline_build.py` from
`pipeline_builders.py` to public names (drop leading underscore) and declare them in
`pipeline_builders.__all__`. This eliminates the cross-module private-boundary import
documented in P1.

**Representative Code Snippets**

Current state — `src/semantics/pipeline_build.py:33-40`:
```python
from semantics.pipeline_builders import (
    _bundle_for_builder,
    _cache_policy_for,
    _finalize_output_builder,
    _normalize_cache_policy,
    _ordered_semantic_specs,
    _semantic_view_specs,
    _SemanticSpecContext,
)
```

After change — same import block with public names:
```python
from semantics.pipeline_builders import (
    bundle_for_builder,
    cache_policy_for,
    finalize_output_builder,
    normalize_cache_policy,
    ordered_semantic_specs,
    semantic_view_specs,
    SemanticSpecContext,
)
```

Current state — `src/semantics/pipeline_builders.py` (representative private symbol):
```python
def _cache_policy_for(
    name: str,
    policy: Mapping[str, CachePolicy] | None,
) -> CachePolicy:
    return _pipeline_cache.cache_policy_for(name, policy)
```

After change — leading underscore removed; added to `__all__`:
```python
def cache_policy_for(
    name: str,
    policy: Mapping[str, CachePolicy] | None,
) -> CachePolicy:
    return _pipeline_cache.cache_policy_for(name, policy)
```

The same rename applies to all seven symbols:
- `_bundle_for_builder` → `bundle_for_builder`
- `_cache_policy_for` → `cache_policy_for`
- `_finalize_output_builder` → `finalize_output_builder`
- `_normalize_cache_policy` → `normalize_cache_policy`
- `_ordered_semantic_specs` → `ordered_semantic_specs`
- `_semantic_view_specs` → `semantic_view_specs`
- `_SemanticSpecContext` → `SemanticSpecContext`

All internal usages of the private names within `pipeline_builders.py` itself must also
be updated to use the new public names.

**Files to Edit**
- `src/semantics/pipeline_builders.py` — rename all seven symbols; add `__all__` block
  if absent, or extend it to include the seven promoted names
- `src/semantics/pipeline_build.py` — update the import block at lines 33-40 to use
  public names; update all usages in the file body (function-local references to
  `_SemanticSpecContext(...)`, `_ordered_semantic_specs(...)`, etc.)

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- All seven underscore-prefixed names are removed; public names replace them directly

---

### S6. Replace Ad-Hoc Normalization Dict

**Goal:** Replace the inline dict comprehension at `pipeline_build.py:301` that
reconstructs `normalization_by_output` from `SEMANTIC_NORMALIZATION_SPECS` with a call
to the existing accessor `normalization_spec_for_output()` from `registry.py`. The
accessor already encapsulates `_NORMALIZATION_BY_OUTPUT` (defined at `registry.py:104`).

**Representative Code Snippets**

Current state — `src/semantics/pipeline_build.py:295-301`:
```python
    from semantics.registry import (
        RELATIONSHIP_SPECS,
        SEMANTIC_NORMALIZATION_SPECS,
        SemanticSpecIndex,
        normalization_spec_for_output,
    )

    normalization_by_output = {spec.output_name: spec for spec in SEMANTIC_NORMALIZATION_SPECS}
```

After change — remove the dict comprehension and the `SEMANTIC_NORMALIZATION_SPECS`
import; keep `normalization_spec_for_output` which is already imported and is now the
only accessor:
```python
    from semantics.registry import (
        RELATIONSHIP_SPECS,
        SemanticSpecIndex,
        normalization_spec_for_output,
    )
```

All subsequent uses of `normalization_by_output.get(output_name)` in the same function
body are replaced with `normalization_spec_for_output(output_name)`.

**Files to Edit**
- `src/semantics/pipeline_build.py` — remove line 301 (dict comprehension); remove
  `SEMANTIC_NORMALIZATION_SPECS` from the local import at lines 295-299; replace all
  `normalization_by_output.get(...)` call sites in the function body with
  `normalization_spec_for_output(...)`

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- Local variable `normalization_by_output` is removed
- Import of `SEMANTIC_NORMALIZATION_SPECS` from `registry` at this call site is removed

---

### S7. Clarify Timestamp as Provenance Metadata

**Goal:** Confirm that `created_at_unix_ms` in `ExecutionPackageArtifact` is metadata
only and is not fed into `_composite_fingerprint`. The review notes it at line 217; a
careful reading of the actual code shows `_composite_fingerprint` does NOT include
`created_at_unix_ms` in its payload tuple (lines 154-161). The timestamp is set directly
on the struct at line 217 after `_composite_fingerprint` has already been computed.

The fingerprint is therefore already deterministic. The action for this scope item is:

1. Add an explicit code comment at `execution_package.py:217` stating that
   `created_at_unix_ms` is provenance metadata excluded from fingerprint computation.
2. Add an optional `created_at_unix_ms: int | None = None` parameter to
   `build_execution_package` so that callers who need replay stability can provide a
   fixed value instead of wall-clock time.

**Representative Code Snippets**

Current state — `src/relspec/execution_package.py:164-218`:
```python
def build_execution_package(
    *,
    manifest: ManifestHashLike | ManifestWithSemanticIr | None = None,
    compiled_policy: PolicyFingerprintLike | None = None,
    capability_snapshot: SettingsHashValueLike | None = None,
    plan_bundle_fingerprints: Mapping[str, str] | None = None,
    session_config: SettingsHashCallableLike | SettingsHashValueLike | str | None = None,
) -> ExecutionPackageArtifact:
    # ... hashing ...
    fingerprint = _composite_fingerprint(...)

    return ExecutionPackageArtifact(
        package_fingerprint=fingerprint,
        # ...
        created_at_unix_ms=int(time.time() * 1000),   # line 217
    )
```

After change — signature extended with optional parameter; comment added:
```python
def build_execution_package(
    *,
    manifest: ManifestHashLike | ManifestWithSemanticIr | None = None,
    compiled_policy: PolicyFingerprintLike | None = None,
    capability_snapshot: SettingsHashValueLike | None = None,
    plan_bundle_fingerprints: Mapping[str, str] | None = None,
    session_config: SettingsHashCallableLike | SettingsHashValueLike | str | None = None,
    created_at_unix_ms: int | None = None,
) -> ExecutionPackageArtifact:
    # ...
    fingerprint = _composite_fingerprint(...)

    # created_at_unix_ms is provenance metadata only; it is intentionally
    # excluded from _composite_fingerprint so that retried builds with
    # identical inputs produce the same package_fingerprint.
    recorded_at = created_at_unix_ms if created_at_unix_ms is not None else int(time.time() * 1000)

    return ExecutionPackageArtifact(
        package_fingerprint=fingerprint,
        # ...
        created_at_unix_ms=recorded_at,
    )
```

**Files to Edit**
- `src/relspec/execution_package.py` — add optional `created_at_unix_ms` parameter to
  `build_execution_package`; add clarifying comment; introduce `recorded_at` local

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- Nothing removed; this is a purely additive clarification

---

### S8. Tighten CalibrationMode Aliases

**Goal:** Normalize `CalibrationMode` aliases `"observe"` and `"apply"` at function
entry in `calibrate_from_execution_metrics` so that `PolicyCalibrationResult.mode`
always contains a canonical value (`"warn"` or `"enforce"`), never the alias. The
aliases remain accepted as input but are collapsed before any branching occurs.

**Representative Code Snippets**

Current state — `src/relspec/policy_calibrator.py:26`:
```python
CalibrationMode = Literal["off", "warn", "enforce", "observe", "apply"]
```

Current state — `src/relspec/policy_calibrator.py:162-177` (aliases leak into result):
```python
    if mode in {"observe", "warn"}:
        resolved_mode = "warn" if mode == "warn" else "observe"   # alias leaks!
        ...
        return PolicyCalibrationResult(..., mode=resolved_mode, ...)

    resolved_mode = "enforce" if mode == "enforce" else "apply"   # alias leaks!
    ...
    return PolicyCalibrationResult(..., mode=resolved_mode, ...)
```

After change — add `_ALIAS_MAP` and normalize at function entry (insert before the
`mode == "off"` guard at line 138):
```python
_ALIAS_MAP: dict[str, str] = {"observe": "warn", "apply": "enforce"}
```

Then at the top of `calibrate_from_execution_metrics`, after the `validate_calibration_bounds`
check and before the `mode == "off"` guard:
```python
    # Normalize legacy aliases to canonical mode names at the API boundary.
    mode = _ALIAS_MAP.get(mode, mode)  # type: ignore[arg-type]
```

Simplified branches after normalization:
```python
    if mode in {"warn"}:
        confidence = _build_calibration_confidence(metrics=metrics, decision_value=mode)
        return PolicyCalibrationResult(..., mode=mode, ...)

    # mode must be "enforce" here
    confidence = _build_calibration_confidence(metrics=metrics, decision_value=mode)
    return PolicyCalibrationResult(..., mode=mode, ...)
```

`CalibrationMode` Literal remains as-is (still accepts `"observe"` and `"apply"` for
backward compatibility with existing callers); the output guarantee is that
`PolicyCalibrationResult.mode` is always `"warn"` or `"enforce"` or `"off"`.

**Verification step:** Search for `result.mode == "observe"` and `result.mode == "apply"`
across the codebase and confirm there are no callers that depend on the alias appearing
in the output.

**Files to Edit**
- `src/relspec/policy_calibrator.py` — add `_ALIAS_MAP` dict; add normalization line at
  function entry in `calibrate_from_execution_metrics`; simplify the branching at lines
  162-177 to use the already-normalized `mode` variable

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- `resolved_mode` local variable removed from the two branches (replaced by the
  already-normalized `mode`)

---

### S9. Enforce ValidationMode Literal

**Goal:** Change `CompiledExecutionPolicy.validation_mode` at `compiled_policy.py:80`
from `str` to `ValidationMode` so that `msgspec` enforces the `Literal["off", "warn",
"error"]` constraint at decode time. An invalid value such as `"strict"` currently
passes construction silently.

**Representative Code Snippets**

Current state — `src/relspec/compiled_policy.py:22` and `80`:
```python
ValidationMode = Literal["off", "warn", "error"]  # defined but not used as field type

class CompiledExecutionPolicy(StructBaseStrict, frozen=True):
    # ...
    validation_mode: str = "warn"   # line 80 — should be ValidationMode
```

After change:
```python
ValidationMode = Literal["off", "warn", "error"]

class CompiledExecutionPolicy(StructBaseStrict, frozen=True):
    # ...
    validation_mode: ValidationMode = "warn"
```

**Pre-commit check:** Before committing, verify that any stored `CompiledExecutionPolicy`
artifacts contain only values within `{"off", "warn", "error"}` for `validation_mode`.
If integration tests decode stored artifacts, run them against the new type to confirm no
`msgspec.ValidationError` is raised.

**Files to Edit**
- `src/relspec/compiled_policy.py` — change field type at line 80 from `str` to
  `ValidationMode`

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- Nothing removed; this is a type annotation tightening only

---

### S10. Add OTel Spans to relspec Hot Paths

**Goal:** Add `stage_span` wrappers to `compile_execution_policy` in
`policy_compiler.py` and `infer_deps_from_plan_bundle` in `inferred_deps.py`. These are
the two hottest computation paths in the policy layer and currently emit only `_LOGGER.debug`
without span boundaries.

**Representative Code Snippets**

Current state — `src/relspec/policy_compiler.py:102-167`:
```python
def compile_execution_policy(request: CompileExecutionPolicyRequestV1) -> CompiledExecutionPolicy:
    components = _derive_policy_components(request)
    _LOGGER.debug("Derived execution-policy component sets.", extra={...})
    preliminary = CompiledExecutionPolicy(...)
    fingerprint = _compute_policy_fingerprint(preliminary)
    _LOGGER.debug("Compiled execution policy fingerprint.", extra={...})
    return CompiledExecutionPolicy(..., policy_fingerprint=fingerprint)
```

After change — wrap function body in a span:
```python
def compile_execution_policy(request: CompileExecutionPolicyRequestV1) -> CompiledExecutionPolicy:
    from obs.otel import SCOPE_PIPELINE, stage_span

    with stage_span("policy_compile", stage="relspec", scope_name=SCOPE_PIPELINE):
        components = _derive_policy_components(request)
        _LOGGER.debug("Derived execution-policy component sets.", extra={...})
        preliminary = CompiledExecutionPolicy(...)
        fingerprint = _compute_policy_fingerprint(preliminary)
        _LOGGER.debug("Compiled execution policy fingerprint.", extra={...})
        return CompiledExecutionPolicy(..., policy_fingerprint=fingerprint)
```

Current state — `src/relspec/inferred_deps.py:165-234`:
```python
def infer_deps_from_plan_bundle(inputs: InferredDepsInputs) -> InferredDeps:
    plan_bundle = inputs.plan_bundle
    # ... lineage extraction, required_types, required_udfs ...
    return InferredDeps(...)
```

After change — wrap function body in a span:
```python
def infer_deps_from_plan_bundle(inputs: InferredDepsInputs) -> InferredDeps:
    from obs.otel import SCOPE_PIPELINE, stage_span

    with stage_span("lineage_infer", stage="relspec", scope_name=SCOPE_PIPELINE):
        plan_bundle = inputs.plan_bundle
        # ... unchanged body ...
        return InferredDeps(...)
```

The `obs.otel` import is deferred inside the function body (consistent with the
`inferred_deps.py` pattern of deferring all heavy imports) to avoid adding a module-level
dependency on the OTel stack from `relspec`.

**Files to Edit**
- `src/relspec/policy_compiler.py` — add deferred `from obs.otel import SCOPE_PIPELINE,
  stage_span` and `with stage_span(...)` wrapper inside `compile_execution_policy`
- `src/relspec/inferred_deps.py` — add deferred `from obs.otel import SCOPE_PIPELINE,
  stage_span` and `with stage_span(...)` wrapper inside `infer_deps_from_plan_bundle`

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- Nothing removed; purely additive instrumentation

---

### S11. Collapse Two-Level getattr Chain

**Goal:** Remove the two-level `getattr(getattr(bundle, "artifacts", None),
"udf_snapshot", {})` chain at `inferred_deps.py:124-125` by introducing a helper or
encapsulating the access. Since `DataFusionPlanArtifact` is imported only inside the
function body and is not available at module level, the cleanest fix is a module-level
private helper function.

**Representative Code Snippets**

Current state — `src/relspec/inferred_deps.py:122-132`:
```python
    if not isinstance(bundle, DataFusionPlanArtifact):
        return frozenset()
    snapshot: Mapping[str, object] | object = getattr(
        getattr(bundle, "artifacts", None),
        "udf_snapshot",
        {},
    )
    if not isinstance(snapshot, Mapping):
        snapshot = {"status": "unavailable"}
    resolved = resolve_required_udfs_from_bundle(bundle, snapshot=snapshot)
    return frozenset(str(name) for name in resolved)
```

After change — extract `_get_udf_snapshot(bundle)` free function:
```python
def _get_udf_snapshot(bundle: object) -> Mapping[str, object]:
    """Extract udf_snapshot from a plan artifact bundle.

    Encapsulates the two-level attribute access bundle.artifacts.udf_snapshot
    with safe fallbacks for missing intermediate objects.
    """
    artifacts = getattr(bundle, "artifacts", None)
    raw = getattr(artifacts, "udf_snapshot", {})
    if not isinstance(raw, Mapping):
        return {"status": "unavailable"}
    return raw
```

Updated call site:
```python
    if not isinstance(bundle, DataFusionPlanArtifact):
        return frozenset()
    snapshot = _get_udf_snapshot(bundle)
    resolved = resolve_required_udfs_from_bundle(bundle, snapshot=snapshot)
    return frozenset(str(name) for name in resolved)
```

**Files to Edit**
- `src/relspec/inferred_deps.py` — add `_get_udf_snapshot` function before the
  `_DataFusionLineagePort` class; update the call site at lines 124-131

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- Inline `getattr(getattr(...))` chain at lines 124-128 is replaced by the helper call

---

### S12. Narrow `orchestrate_build` Scope

**Goal:** Extract a `_post_process_results` helper from `orchestrate_build` in
`src/graph/build_pipeline.py` that separates output collection from stage dispatch.
Move the `_record_observability` call outside the `stage_span("build_orchestrator")`
context manager so that observability emission time is not counted against build time
in traces.

**Representative Code Snippets**

Current state — `src/graph/build_pipeline.py:120-201`:
```python
def orchestrate_build(request: OrchestrateBuildRequestV1) -> BuildResult:
    with stage_span("build_orchestrator", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        extraction_result = _run_extraction_phase(...)
        semantic_inputs = ...
        _, spec = _compile_semantic_phase(...)
        run_result, run_artifacts = _execute_engine_phase(...)

        # Output collection mixed into stage dispatch:
        cpg_outputs = _collect_cpg_outputs(run_result, output_dir=output_dir)
        auxiliary_output_options = _AuxiliaryOutputOptions(...)
        auxiliary_outputs = _collect_auxiliary_outputs(...)

        # Observability emission inside the build span:
        _record_observability(spec, run_result)
        warnings = _extract_warnings(run_result)

    return BuildResult(...)
```

After change — stage dispatch ends at `_execute_engine_phase`; post-processing is
separated:
```python
def orchestrate_build(request: OrchestrateBuildRequestV1) -> BuildResult:
    repo_root = Path(request.repo_root)
    work_dir = Path(request.work_dir)
    output_dir = Path(request.output_dir)

    with stage_span("build_orchestrator", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        extraction_result = _run_extraction_phase(
            repo_root, work_dir, extraction_config=request.extraction_config,
        )
        semantic_inputs = (
            extraction_result.semantic_input_locations
            if extraction_result.semantic_input_locations
            else extraction_result.delta_locations
        )
        _, spec = _compile_semantic_phase(
            semantic_input_locations=semantic_inputs,
            engine_profile=request.engine_profile,
            rulepack_profile=request.rulepack_profile,
            output_dir=output_dir,
            runtime_config=request.runtime_config,
        )
        run_result, run_artifacts = _execute_engine_phase(
            semantic_inputs, spec, request.engine_profile,
        )

    # Post-processing and observability run outside the measured build span.
    cpg_outputs, auxiliary_outputs, warnings = _post_process_results(
        run_result=run_result,
        run_artifacts=run_artifacts,
        extraction_result=extraction_result,
        output_dir=output_dir,
        options=_AuxiliaryOutputOptions(
            include_errors=request.include_errors,
            include_manifest=request.include_manifest,
            include_run_bundle=request.include_run_bundle,
        ),
        spec=spec,
    )

    return BuildResult(
        cpg_outputs=cpg_outputs,
        auxiliary_outputs=auxiliary_outputs,
        run_result=run_result,
        extraction_timing=extraction_result.timing,
        warnings=warnings,
    )


def _post_process_results(
    *,
    run_result: dict[str, object],
    run_artifacts: dict[str, object],
    extraction_result: ExtractionResult,
    output_dir: Path,
    options: _AuxiliaryOutputOptions,
    spec: SemanticExecutionSpec,
) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]], list[str]]:
    """Collect outputs and emit observability after stage dispatch completes."""
    cpg_outputs = _collect_cpg_outputs(run_result, output_dir=output_dir)
    auxiliary_outputs = _collect_auxiliary_outputs(
        output_dir=output_dir,
        artifacts=run_artifacts,
        extraction_result=extraction_result,
        options=options,
    )
    _record_observability(spec, run_result)
    warnings = _extract_warnings(run_result)
    return cpg_outputs, auxiliary_outputs, warnings
```

**Files to Edit**
- `src/graph/build_pipeline.py` — extract `_post_process_results` helper; update
  `orchestrate_build` to call it after the `with stage_span(...)` block closes

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- No symbols removed; the output-collection and observability logic is moved, not deleted

---

### S13. Rename Identity Functions in naming.py

**Goal:** Document `canonical_output_name` clearly as an identity-by-default function.
Remove `internal_name` which is an unconditional identity function (it returns its
argument unchanged in all cases) and is referenced only in `__all__`.

**Representative Code Snippets**

Current state — `src/semantics/naming.py:64-78`:
```python
def internal_name(output_name: str) -> str:
    """Get internal name from a canonical output name."""
    return output_name
```

After change — remove `internal_name` entirely. Update `__all__` to remove it.

Current state — `src/semantics/naming.py:33-61` (docstring does not note identity
behavior):
```python
def canonical_output_name(
    internal_name: str,
    *,
    manifest: HasOutputNameMap | None = None,
) -> str:
    """Get canonical output name for an internal view name."""
```

After change — update the docstring summary line to make identity-by-default explicit:
```python
def canonical_output_name(
    internal_name: str,
    *,
    manifest: HasOutputNameMap | None = None,
) -> str:
    """Return the canonical output name for a view, which is the view name itself by default.

    When a manifest with a populated ``output_name_map`` is provided, the
    manifest-backed map is authoritative. Otherwise the internal name is
    returned unchanged (identity behavior).
    ...
    """
```

**Verification step:** Confirm no code in `src/`, `tests/`, or `tools/` imports or calls
`internal_name` from `semantics.naming`. Use:
```bash
rg "internal_name" src/ tests/ tools/ --include "*.py"
```
Any caller must be updated to use the view name directly before `internal_name` is
removed.

**Files to Edit**
- `src/semantics/naming.py` — remove `internal_name` function (lines 64-78); remove
  `"internal_name"` from `__all__`; update `canonical_output_name` docstring to
  document identity-by-default behavior

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- `internal_name` function deleted from `naming.py` and removed from `__all__`

---

### S14. Add `__all__` to planning_engine/config.py

**Goal:** Add a `__all__` declaration to `src/planning_engine/config.py` that lists the
stable public surface. The file currently has an `__all__` block (ending at line 76) but
it is missing `RulepackProfile` and `TracingPreset` from the type alias declarations.

**Representative Code Snippets**

Current state — `src/planning_engine/config.py:69-76`:
```python
__all__ = [
    "CompiledPlanSummary",
    "EngineConfigSpec",
    "EngineExecutionOptions",
    "EngineProfile",
    "ExtractionConfig",
    "IncrementalConfig",
]
```

After verification of all type aliases in the file (lines 16-20):
```python
type EngineProfile = Literal["small", "medium", "large"]
type RulepackProfile = Literal["Default", "LowLatency", "Replay", "Strict"]
type TracingPreset = Literal["Maximal", "MaximalNoData", "ProductionLean"]
type ExtractionConfig = Mapping[str, object]
type IncrementalConfig = Mapping[str, object]
```

After change — extend `__all__` to include the two missing type aliases:
```python
__all__ = [
    "CompiledPlanSummary",
    "EngineConfigSpec",
    "EngineExecutionOptions",
    "EngineProfile",
    "ExtractionConfig",
    "IncrementalConfig",
    "RulepackProfile",
    "TracingPreset",
]
```

**Files to Edit**
- `src/planning_engine/config.py` — add `"RulepackProfile"` and `"TracingPreset"` to
  `__all__`

**New Files to Create**
None.

**Legacy Decommission / Delete Scope**
- Nothing removed; additive only

---

### S15. Consolidate Plan Fingerprint Authority

### Goal
Replace semantics-local runtime fingerprint authority with shared DataFusion
plan fingerprint/identity artifacts so cache/replay identity is computed in one place.

### Representative Code Snippets

Current canonical bundle fingerprint/identity path in `src/datafusion_engine/plan/bundle_assembly.py`:
```python
fingerprint = compute_plan_fingerprint(
    PlanFingerprintInputs(
        substrait_bytes=plan_core.substrait_bytes,
        df_settings=environment.df_settings,
        planning_env_hash=environment.planning_env_hash,
        rulepack_hash=environment.rulepack_hash,
        udf_snapshot_hash=udf_artifacts.snapshot_hash,
        required_udfs=required.required_udfs,
        required_rewrite_tags=required.required_rewrite_tags,
        delta_inputs=merged_delta_inputs,
        delta_store_policy_hash=environment.delta_store_policy_hash,
        information_schema_hash=environment.information_schema_hash,
    )
)
```

Target adapter shape in `src/semantics/plans/fingerprints.py`:
```python
# Runtime identity should prefer plan_identity_hash emitted by plan bundles.
def runtime_plan_identity(bundle: DataFusionPlanArtifact) -> str:
    if isinstance(bundle.plan_identity_hash, str) and bundle.plan_identity_hash:
        return bundle.plan_identity_hash
    return bundle.plan_fingerprint
```

### Files to Edit
- `src/semantics/plans/fingerprints.py`
- `src/semantics/plans/__init__.py`
- `src/datafusion_engine/plan/bundle_assembly.py` (adapter/export wiring only)

### New Files to Create
None.

### Legacy Decommission / Delete Scope
- Semantics-local runtime identity fallback usage is removed from
  `src/semantics/plans/fingerprints.py` once bundle identity is available.

---

### S16. Remove Private Substrait Internal Path Usage

### Goal
Eliminate dependence on `datafusion._internal` Substrait hooks from semantic
fingerprinting and use the canonical Rust bundle bridge for Substrait bytes.

### Representative Code Snippets

Current canonical Substrait bridge in `src/datafusion_engine/plan/substrait_artifacts.py`:
```python
substrait_bytes, rust_required_udfs = substrait_bytes_from_rust_bundle(
    ctx,
    df,
    session_runtime=options.session_runtime,
)
```

Target shape for semantic path:
```python
from datafusion_engine.plan.substrait_artifacts import substrait_bytes_from_rust_bundle

encoded, _required = substrait_bytes_from_rust_bundle(ctx, df, session_runtime=None)
substrait_hash = _hash_bytes(encoded)
```

### Files to Edit
- `src/semantics/plans/fingerprints.py`
- `src/datafusion_engine/plan/substrait_artifacts.py` (optional helper extraction)

### New Files to Create
None.

### Legacy Decommission / Delete Scope
- Delete `_internal_substrait_bytes` from `src/semantics/plans/fingerprints.py`.
- Delete `_public_substrait_bytes` from `src/semantics/plans/fingerprints.py`.
- Delete `_encode_substrait_plan` from `src/semantics/plans/fingerprints.py`.

---

### S17. Promote Planning-Environment Hashes to relspec Package Inputs

### Goal
Include planning environment identity (`planning_env_hash`, `rulepack_hash`,
`information_schema_hash`, function registry hash) in relspec execution package payloads.

### Representative Code Snippets

Current relspec composite payload in `src/relspec/execution_package.py`:
```python
payload = (
    ("manifest_hash", manifest_hash),
    ("policy_artifact_hash", policy_artifact_hash),
    ("capability_snapshot_hash", capability_snapshot_hash),
    ("plan_bundle_fingerprints", tuple(sorted(plan_bundle_fingerprints.items()))),
    ("session_config_hash", session_config_hash),
)
```

Target additive payload keys:
```python
payload = (
    ("manifest_hash", manifest_hash),
    ("policy_artifact_hash", policy_artifact_hash),
    ("capability_snapshot_hash", capability_snapshot_hash),
    ("plan_bundle_fingerprints", tuple(sorted(plan_bundle_fingerprints.items()))),
    ("session_config_hash", session_config_hash),
    ("planning_env_hash", planning_env_hash),
    ("rulepack_hash", rulepack_hash),
    ("information_schema_hash", information_schema_hash),
    ("function_registry_hash", function_registry_hash),
)
```

### Files to Edit
- `src/relspec/execution_package.py`
- `src/relspec/compiled_policy.py`
- `src/relspec/policy_compiler.py`
- `src/datafusion_engine/plan/bundle_assembly.py`

### New Files to Create
None.

### Legacy Decommission / Delete Scope
- Remove ad hoc environment-hash lookups at relspec/orchestrator call sites once package
  artifact includes the expanded planning hashes directly.

---

### S18. Deduplicate Scan-Setting Runtime Application

### Goal
Unify duplicated runtime setting helpers used by table/dataset registration and
eliminate drift between equivalent `SET datafusion.*` application paths.

### Representative Code Snippets

Current duplicate helper pattern:
```python
def _apply_scan_settings(ctx: SessionContext, *, scan: DataFusionScanOptions | None, ...):
    settings = [
        ("datafusion.execution.collect_statistics", scan.collect_statistics, True),
        ("datafusion.execution.meta_fetch_concurrency", scan.meta_fetch_concurrency, False),
        ("datafusion.runtime.list_files_cache_limit", scan.list_files_cache_limit, False),
        ("datafusion.runtime.list_files_cache_ttl", scan.list_files_cache_ttl, False),
    ]
```

Target shared helper:
```python
# src/datafusion_engine/session/scan_settings.py
def apply_scan_settings(
    ctx: SessionContext,
    *,
    scan: DataFusionScanOptions | None,
    sql_options: SQLOptions,
) -> None:
    ...
```

### Files to Edit
- `src/datafusion_engine/dataset/registration_validation.py`
- `src/datafusion_engine/tables/registration.py`

### New Files to Create
- `src/datafusion_engine/session/scan_settings.py`
- `tests/unit/datafusion_engine/session/test_scan_settings.py`

### Legacy Decommission / Delete Scope
- Delete `_apply_scan_settings` from `src/datafusion_engine/dataset/registration_validation.py`.
- Delete `_set_runtime_setting` from `src/datafusion_engine/dataset/registration_validation.py`.
- Delete `_apply_scan_settings` from `src/datafusion_engine/tables/registration.py`.
- Delete `_set_runtime_setting` from `src/datafusion_engine/tables/registration.py`.

---

### S19. Capture Cache Diagnostics in Plan Artifacts

### Goal
Promote cache diagnostics from ad hoc introspection into first-class plan artifact
payloads and optional relspec calibration signals.

### Representative Code Snippets

Current diagnostics source in `src/datafusion_engine/catalog/introspection.py`:
```python
def capture_cache_diagnostics(ctx: SessionContext) -> dict[str, Any]:
    ...
```

Target additive wiring in plan artifact assembly:
```python
from datafusion_engine.catalog.introspection import capture_cache_diagnostics

cache_diagnostics = capture_cache_diagnostics(ctx)
# Include in artifact payload for replay/debug policy decisions.
```

### Files to Edit
- `src/datafusion_engine/plan/bundle_assembly.py`
- `src/serde_artifacts.py` (additive `PlanArtifacts` field)
- `src/relspec/policy_calibrator.py` (optional signal consumption)

### New Files to Create
- `tests/unit/datafusion_engine/plan/test_bundle_cache_diagnostics.py`

### Legacy Decommission / Delete Scope
- Remove cache-diagnostic side channels once plan artifacts carry canonical cache snapshots.

---

### S20. Enforce Python/Rust Planning Policy Parity

### Goal
Define and validate one planning policy contract so equivalent profiles produce
hash-equivalent setting snapshots in Python and Rust session builders.

### Representative Code Snippets

Current Python policy gate surface in `src/datafusion_engine/session/runtime_config_policies.py`:
```python
class DataFusionFeatureGates(StructBaseStrict, frozen=True):
    enable_dynamic_filter_pushdown: bool = True
    enable_topk_dynamic_filter_pushdown: bool = True
    enable_sort_pushdown: bool = True
```

Current Rust builder surface in `rust/codeanatomy_engine/src/session/factory.rs`:
```rust
opts.optimizer.enable_dynamic_filter_pushdown = profile.enable_dynamic_filter_pushdown;
opts.optimizer.enable_topk_dynamic_filter_pushdown =
    profile.enable_topk_dynamic_filter_pushdown;
opts.optimizer.enable_sort_pushdown = profile.enable_sort_pushdown;
```

### Files to Edit
- `src/datafusion_engine/session/runtime_config_policies.py`
- `src/datafusion_engine/session/runtime.py`
- `rust/codeanatomy_engine/src/session/factory.rs`
- `rust/codeanatomy_engine/src/session/planning_surface.rs`

### New Files to Create
- `tests/unit/datafusion_engine/session/test_policy_parity_hash.py`
- `rust/codeanatomy_engine/tests/policy_parity_hash.rs`

### Legacy Decommission / Delete Scope
- Remove duplicate profile-specific knob tables once shared policy contract generation is in place.

---

### S21. Enforce Builder-Native Planning Surface Composition

### Goal
Require new planning-time extensions to flow through `PlanningSurfaceSpec` and
`SessionStateBuilder` APIs instead of ad hoc post-build mutation.

### Representative Code Snippets

Current canonical builder path in `rust/codeanatomy_engine/src/session/planning_surface.rs`:
```rust
if !spec.expr_planners.is_empty() {
    builder = builder.with_expr_planners(spec.expr_planners.clone());
}
if let Some(factory) = spec.function_factory.clone() {
    builder = builder.with_function_factory(Some(factory));
}
if let Some(planner) = spec.query_planner.clone() {
    builder = builder.with_query_planner(planner);
}
```

### Files to Edit
- `rust/codeanatomy_engine/src/session/planning_surface.rs`
- `rust/codeanatomy_engine/src/session/factory.rs`
- `rust/datafusion_ext/src/lib.rs` (expose builder-friendly components only)

### New Files to Create
- `rust/codeanatomy_engine/tests/planning_surface_enforcement.rs`

### Legacy Decommission / Delete Scope
- Remove new ad hoc post-build planner registration paths introduced outside
  `PlanningSurfaceSpec` after this scope item lands.

---

### S22. Pilot Relation-Planner-Based SQL Extension Path

### Goal
Prototype one bounded semantic SQL construct via planner-native extension wiring
instead of bespoke pre-planning orchestration transforms.

### Representative Code Snippets

Planning-surface integration point (existing query planner hook):
```rust
if let Some(planner) = spec.query_planner.clone() {
    builder = builder.with_query_planner(planner);
}
```

Pilot acceptance must be explain-verified against existing behavior:
```rust
let batches = df.clone().explain(true, false)?.collect().await?;
```

### Files to Edit
- `rust/datafusion_ext/src/` (new relation-planner pilot module)
- `rust/codeanatomy_engine/src/session/factory.rs`
- `rust/codeanatomy_engine/src/rules/overlay.rs` (pilot explain delta capture)

### New Files to Create
- `rust/datafusion_ext/src/relation_planner.rs`
- `rust/datafusion_ext/tests/relation_planner_pilot.rs`

### Legacy Decommission / Delete Scope
- Remove the pilot construct's bespoke orchestration rewrite path once parity is proven.

---

### S23. Adopt Plan-Combination Discipline for Semantic DAG Assembly

### Goal
Minimize avoidable materialization boundaries by composing one logical DAG per
semantic workload where feasible.

### Representative Code Snippets

DataFrame composition shape (single-DAG planning):
```python
combined = (
    left_df.join(right_df, on=["dataset_name"], how="inner")
    .filter(col("confidence") > lit(0.8))
)
```

Plan-shape validation:
```python
logical = df.logical_plan()
optimized = df.optimized_logical_plan()
physical = df.execution_plan()
```

### Files to Edit
- `src/semantics/`
- `src/datafusion_engine/session/facade.py`
- `src/datafusion_engine/plan/bundle_assembly.py`

### New Files to Create
- `tests/unit/semantics/test_plan_combination_shapes.py`

### Legacy Decommission / Delete Scope
- Remove avoidable intermediate materialization paths where outputs are only consumed
  by immediately downstream semantic plan stages.

---

### S24. Add Idempotency Guards to Rule Installers

### Goal
Ensure repeated rule installer invocations are no-ops once a rule is present,
preventing duplicate registration and non-deterministic rule ordering.

### Representative Code Snippets

Current installer pattern:
```rust
state.add_analyzer_rule(Arc::new(CodeAnatomyPolicyRule));
```

Target guarded installer:
```rust
let already_installed = state
    .analyzer_rules()
    .iter()
    .any(|rule| rule.name() == "codeanatomy_policy_rule");
if !already_installed {
    state.add_analyzer_rule(Arc::new(CodeAnatomyPolicyRule));
}
```

### Files to Edit
- `rust/datafusion_ext/src/planner_rules.rs`
- `rust/datafusion_ext/src/physical_rules.rs`

### New Files to Create
- `rust/datafusion_ext/tests/rule_installer_idempotency.rs`

### Legacy Decommission / Delete Scope
- Remove implicit repeated-installer assumptions in callers once installers are guaranteed idempotent.

---

### S25. Standardize Overlay-Session Rule Experiments

### Goal
Use overlay sessions as the default mechanism for rulepack experiments so baseline
session state is never mutated in place.

### Representative Code Snippets

Existing overlay session path in `rust/codeanatomy_engine/src/rules/overlay.rs`:
```rust
let new_state = SessionStateBuilder::new_from_existing(state.clone())
    .with_analyzer_rules(overlaid_ruleset.analyzer_rules.clone())
    .with_optimizer_rules(overlaid_ruleset.optimizer_rules.clone())
    .with_physical_optimizer_rules(overlaid_ruleset.physical_rules.clone())
    .build();
```

### Files to Edit
- `rust/codeanatomy_engine/src/rules/overlay.rs`
- `rust/codeanatomy_engine/src/session/factory.rs` (experiment routing hooks)

### New Files to Create
- `rust/codeanatomy_engine/tests/overlay_session_invariance.rs`

### Legacy Decommission / Delete Scope
- Remove in-place mutable experiment toggles that alter baseline session state.

---

### S26. Evaluate Provider-Native DML Migration Envelope

### Goal
Assess and pilot migration of selected bespoke mutation pathways to provider-native
DataFusion DML hooks where contract guarantees are acceptable.

### Representative Code Snippets

Pilot acceptance shape (SQL-level DML path):
```sql
DELETE FROM target_table WHERE run_id = 'abc123';
UPDATE target_table SET status = 'active' WHERE id = 42;
```

Gate condition for migration:
```python
# Keep fallback mutation path when provider-native guarantees are unavailable.
if not provider_supports_native_dml:
    return run_bespoke_mutation(...)
```

### Files to Edit
- `src/datafusion_engine/delta/`
- `rust/datafusion_ext/src/delta_mutations.rs`
- `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs`

### New Files to Create
- `tests/unit/datafusion_engine/delta/test_native_dml_pilot.py`
- `rust/datafusion_ext/tests/native_dml_pilot.rs`

### Legacy Decommission / Delete Scope
- Delete bespoke mutation code paths only after pilot parity and rollback guarantees are established.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S4, S5, S6, S8, S11, S13)
- Delete `_STRATEGY_CONFIDENCE` dict body from `src/semantics/ir_pipeline.py` because
  S1 introduces canonical `CONFIDENCE_BY_STRATEGY`.
- Delete `TableRegistry.resolve` and `SemanticCompiler.get_or_register` legacy names
  because S4 performs hard-cutover renames.
- Delete private helper symbol names from `src/semantics/pipeline_builders.py`
  (`_bundle_for_builder`, `_cache_policy_for`, `_finalize_output_builder`,
  `_normalize_cache_policy`, `_ordered_semantic_specs`, `_semantic_view_specs`,
  `_SemanticSpecContext`) because S5 promotes public names.
- Delete `normalization_by_output` local reconstruction and local
  `SEMANTIC_NORMALIZATION_SPECS` import in `src/semantics/pipeline_build.py` because S6
  uses canonical accessor wiring.
- Delete `resolved_mode` branch locals in `src/relspec/policy_calibrator.py` because S8
  normalizes aliases at function entry.
- Delete inline chained `getattr(getattr(...))` call site in
  `src/relspec/inferred_deps.py` because S11 introduces `_get_udf_snapshot`.
- Delete `internal_name` from `src/semantics/naming.py` because S13 confirms no callers.

### Batch D2 (after S2)
- Delete `RelationshipSpec` imports/usages from active semantics build paths:
  `src/semantics/pipeline_build.py` and `src/semantics/pipeline_builders.py`.
- Keep `RelationshipSpec` class in `src/semantics/specs.py` marked deprecated until
  external consumer audit completes.

### Batch D3 (after S15, S16)
- Delete semantics-local runtime identity authority paths in
  `src/semantics/plans/fingerprints.py` because bundle identity/fingerprint becomes the
  canonical source.
- Delete private Substrait helper functions in `src/semantics/plans/fingerprints.py`
  (`_internal_substrait_bytes`, `_public_substrait_bytes`, `_encode_substrait_plan`)
  because S16 routes through the canonical Rust bundle bridge.

### Batch D4 (after S18, S20)
- Delete duplicated scan-setting helpers from
  `src/datafusion_engine/dataset/registration_validation.py` and
  `src/datafusion_engine/tables/registration.py` because S18 centralizes helper logic.
- Delete profile-specific duplicate planning knob tables replaced by a shared policy
  parity contract once S20 parity tests pass.

### Batch D5 (after S24, S25)
- Delete repeated-installer assumptions in call sites under `rust/datafusion_ext/` and
  `rust/codeanatomy_engine/` once idempotent installers and overlay-session routing are
  default behavior.

---

## Implementation Sequence

The sequence below prioritizes deterministic guardrails first, then
DataFusion-native convergence, then higher-risk refactors.

1. **Phase 1 — Determinism quick wins:** `S1`, `S3`, `S9`, `S14`
2. **Phase 2 — Behavioral cleanup:** `S8`, `S13`
3. **Phase 3 — Prerequisite refactors:** `S5` -> `S6` -> `S2`
4. **Phase 4 — Rename + structure cleanup:** `S4`, `S11`
5. **Phase 5 — Observability + orchestration shaping:** `S10`, `S7`, `S12`
6. **Phase 6 — Core DataFusion planning convergence:** `S15`, `S16`, `S17`, `S19`, `S23`
7. **Phase 7 — Policy/extension convergence across Python+Rust:** `S18`, `S20`, `S21`, `S22`, `S24`, `S25`, `S26`
8. **Phase 8 — Decommission batches:** `D1`, `D2`, `D3`, `D4`, `D5`

Quick smoke checks during implementation are optional. The required completion gate is:
`uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`

---

## Implementation Checklist

### Completion Audit (2026-02-19)

- Completed scope items: `S1-S26`
- Partially complete scope items: none
- Completed decommission batches: `D1`, `D2`, `D3`, `D4`, `D5`
- Partially complete decommission batches: none
- Quality gate status: `ruff format`, `ruff check --fix`, `pyrefly`, `pyright`, and scoped pytest for touched Python subsystems passed. Full-repo `uv run pytest -q` plus unblocked `datafusion-python` Rust test linking remain outstanding.

### Phase 1 — Determinism quick wins

- [x] **S1** Centralize confidence constants with import-section placement.
- [x] **S3** Implement view-qualified logical-plan fallback hash.
- [x] **S9** Enforce `ValidationMode` field type.
- [x] **S14** Complete `planning_engine/config.py` public `__all__`.

### Phase 2 — Behavioral cleanup

- [x] **S8** Normalize calibration aliases at function entry.
- [x] **S13** Remove `internal_name` after caller audit.

### Phase 3 — Prerequisite refactors

- [x] **S5** Promote private pipeline builder helpers.
- [x] **S6** Remove ad hoc normalization dict reconstruction.
- [x] **S2** Remove `RelationshipSpec` imports/usages from active paths with expanded `src/`+`tests/`+`tools/` verification.

### Phase 4 — Rename + structure cleanup

- [x] **S4** Perform hard-cutover CQS renames in registry/compiler.
- [x] **S11** Replace chained `getattr` access with helper.

### Phase 5 — Observability + orchestration shaping

- [x] **S10** Add relspec OTel spans.
- [x] **S7** Add provenance clarification and optional timestamp override.
- [x] **S12** Narrow orchestrator build span and move post-processing outside.

### Phase 6 — DataFusion planning convergence

- [x] **S15** Consolidate plan fingerprint authority to shared DataFusion artifacts.
- [x] **S16** Remove private Substrait internal path usage.
- [x] **S17** Promote planning-environment hashes into relspec package inputs.
- [x] **S19** Cache diagnostics are captured in plan artifacts and consumed as optional relspec calibration signals.
- [x] **S23** Plan-shape capture/tests and inferred-deps non-materializing execution path are in place to reduce avoidable DAG boundaries.

### Phase 7 — Cross-language policy/extension convergence

- [x] **S18** Deduplicate scan-setting runtime application helpers.
- [x] **S20** Enforce Python/Rust planning policy parity contract.
- [x] **S21** Builder-native planning-surface composition is enforced and standalone relation/type post-build installers are decommissioned from the native installer path.
- [x] **S22** Complete relation-planner pilot.
- [x] **S24** Add idempotency guards to rule installers.
- [x] **S25** Overlay experiment routing is wired through orchestration with effective overlaid rulesets and invariance coverage.
- [x] **S26** Provider-native DML gate (`provider_supports_native_dml`) and explicit fallback routing are wired for mutation entrypoints.

### Decommission Batches

- [x] **D1** Execute decommissions after S1/S4/S5/S6/S8/S11/S13.
- [x] **D2** Execute decommissions after S2.
- [x] **D3** Execute decommissions after S15/S16.
- [x] **D4** Duplicate scan-setting helper and parity follow-through cleanup are complete.
- [x] **D5** Repeated-installer assumptions and overlay routing standardization decommissions are complete.

### Final Gate

- [ ] Run post-implementation quality gates in sequence:
  - `uv run ruff format`
  - `uv run ruff check --fix`
  - `uv run pyrefly check`
  - `uv run pyright`
  - `uv run pytest -q` (full repository)
  - Unblocked targeted Rust tests for `datafusion-python` once Python linker symbols are available in the environment
