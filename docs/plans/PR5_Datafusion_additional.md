Below is the **PR5 diff map** I’d put directly into GitHub as a **commit-by-commit, file-by-file checklist**. It’s structured to be maximally reviewable while moving you closer to the target architecture: **Ibis IR → DataFusion execution (with SQLGlot fallback/diagnostics) → Arrow materialization only at the boundaries**.

---

## PR5 theme

**“Turn Ibis execution into a first-class engine lane.”**

Right now, several paths still execute `IbisPlan` via `plan.to_table()` / `plan.to_reader()` directly. That bypasses the new DataFusion bridge/fallback/diagnostics tooling you’ve introduced (notably `ibis_engine.runner` + `datafusion_engine.bridge`).

PR5 makes execution consistent:

* **If `AdapterMode.use_datafusion_bridge` is enabled** and we have the backend:
  execute via `ibis_engine.runner.materialize_plan/stream_plan` (DataFusion + SQLGlot fallback + explain hooks)
* Otherwise: keep the current direct path (but start nudging with deprecations)

---

# Commit plan (reviewable slices)

## Commit 1 — Wire Ibis backend to the same DataFusion runtime profile as `ExecutionContext`

**Commit title:** `inputs: build ibis backend using ctx.runtime.datafusion`

### Why

DataFusion “views” (registered tables) must be visible to the same `SessionContext` used by the bridge execution. You already have a robust `DataFusionRuntimeProfile.session_context()` cache keyed by profile payload, but PR5 should make the “same context” relationship **explicit** in the pipeline.

### Files

* [ ] `src/hamilton_pipeline/modules/inputs.py`

### Checklist

* [ ] Change `ibis_backend_config()` to take `ctx: ExecutionContext`
* [ ] Return `IbisBackendConfig(datafusion_profile=ctx.runtime.datafusion)`
* [ ] Add a short comment: “backend shares DataFusion SessionContext with ctx runtime profile”

### Minimal diff shape

```py
# src/hamilton_pipeline/modules/inputs.py

@tag(layer="inputs", artifact="ibis_backend_config", kind="object")
def ibis_backend_config(ctx: ExecutionContext) -> IbisBackendConfig:
    return IbisBackendConfig(datafusion_profile=ctx.runtime.datafusion)
```

### Review focus

* Ensures backend & runtime profile are aligned
* Prevents “bridge ctx can’t see backend views” class of bugs

---

## Commit 2 — Add a single “execution options builder” for Ibis plans

**Commit title:** `ibis_engine: add execution options builder from ctx + backend`

### Why

You don’t want every callsite re-deriving `DataFusionExecutionOptions` and guessing which `SessionContext` to use. Centralize it.

### Files

* [ ] **NEW:** `src/ibis_engine/execution.py`
* [ ] `src/ibis_engine/__init__.py` (optional export)

### Checklist

* [ ] Add `ibis_execution_options(ctx, backend, params=None, runtime_profile=None)` returning `IbisPlanExecutionOptions`
* [ ] Prefer using a backend-owned session context if present (DataFusion backend commonly exposes `.con`)
* [ ] Fall back to `ctx.runtime.datafusion.session_context()` if needed

### Code shape

```py
# src/ibis_engine/execution.py
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from datafusion import SessionContext
from ibis.backends import BaseBackend
from ibis.expr.types import Value

from arrowdsl.core.context import ExecutionContext
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.runner import DataFusionExecutionOptions, IbisPlanExecutionOptions

def _backend_session_context(backend: BaseBackend) -> SessionContext | None:
    ctx = getattr(backend, "con", None)
    return ctx if isinstance(ctx, SessionContext) else None

def ibis_execution_options(
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    params: Mapping[Value, object] | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> IbisPlanExecutionOptions:
    df_profile = runtime_profile or ctx.runtime.datafusion
    if df_profile is None:
        return IbisPlanExecutionOptions(params=params)

    df_ctx = _backend_session_context(backend) or df_profile.session_context()
    return IbisPlanExecutionOptions(
        params=params,
        datafusion=DataFusionExecutionOptions(
            backend=cast("Any", backend),  # runner expects IbisCompilerBackend
            ctx=df_ctx,
            runtime_profile=df_profile,
        ),
    )
```

### Review focus

* One canonical place for “ctx + backend → execution options”
* Avoids session-context mismatch and keeps compile options centralized via runtime_profile

---

## Commit 3 — Teach the Plan adapter runner to use DataFusion bridge for `IbisPlan`

**Commit title:** `arrowdsl.plan.runner: execute IbisPlan via datafusion bridge when enabled`

### Why

`run_plan_adapter()` is the choke point used by extract/normalize/cpg. Once this is correct, you remove a lot of ad-hoc `plan.to_table()` calls elsewhere.

### Files

* [ ] `src/arrowdsl/plan/runner.py`

### Checklist

* [ ] Extend `AdapterRunOptions`:

  * [ ] `ibis_backend: BaseBackend | None = None`
  * [ ] `ibis_params: Mapping[Value, object] | None = None` (or looser typing if you want)
  * [ ] `ibis_batch_size: int | None = None` (for streaming)
* [ ] In `_run_ibis_plan`, if `adapter_mode.use_datafusion_bridge`:

  * [ ] require backend (or fall back, but warn)
  * [ ] use `ibis_engine.runner.materialize_plan/stream_plan` via `ibis_engine.execution.ibis_execution_options`
* [ ] Preserve:

  * [ ] canonical sort behavior (`apply_canonical_sort`)
  * [ ] ordering metadata merge logic (`ordering_metadata_for_plan` + `merge_metadata_specs`)
  * [ ] metadata_spec application

### Key diff shape

```py
# src/arrowdsl/plan/runner.py
from ibis.backends import BaseBackend
from ibis.expr.types import Value

from ibis_engine.execution import ibis_execution_options
from ibis_engine.runner import materialize_plan as materialize_ibis_plan
from ibis_engine.runner import stream_plan as stream_ibis_plan
import warnings

@dataclass(frozen=True)
class AdapterRunOptions:
    adapter_mode: AdapterMode | None = None
    prefer_reader: bool = False
    metadata_spec: SchemaMetadataSpec | None = None
    attach_ordering_metadata: bool = True
    ibis_backend: BaseBackend | None = None
    ibis_params: Mapping[Value, object] | None = None
    ibis_batch_size: int | None = None

def _run_ibis_plan(...):
    adapter_mode = adapter_mode or AdapterMode()

    use_bridge = adapter_mode.use_datafusion_bridge and ctx.runtime.datafusion is not None
    if use_bridge:
        if ibis_backend is None:
            warnings.warn(
                "AdapterMode.use_datafusion_bridge enabled but no ibis_backend provided; "
                "falling back to IbisPlan.to_table/to_reader(). This fallback will be removed.",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            execution = ibis_execution_options(ctx=ctx, backend=ibis_backend, params=ibis_params)
            if prefer_reader and ctx.determinism != DeterminismTier.CANONICAL:
                reader = stream_ibis_plan(plan, execution=execution, batch_size=ibis_batch_size)
                ...
            else:
                table = materialize_ibis_plan(plan, execution=execution)
                ...
            # then canonical sort + metadata merge as today
            ...
    # legacy path (still supported in PR5)
    ...
```

### Review focus

* This is the “spine” commit: once merged, the rest of PR5 becomes mechanical wiring.

---

## Commit 4 — Stop schema-alignment paths from materializing Ibis plans directly

**Commit title:** `arrowdsl.plan_helpers: thread ibis backend into finalize_plan_adapter`

### Why

`finalize_plan_result_adapter()` currently executes `IbisPlan` directly (`plan.to_table()`) when `schema` is passed. That bypasses the new bridge lane.

### Files

* [ ] `src/arrowdsl/plan_helpers.py`

### Checklist

* [ ] Extend `FinalizePlanAdapterOptions` to include:

  * [ ] `ibis_backend: BaseBackend | None = None`
  * [ ] `ibis_params: Mapping[Value, object] | None = None`
* [ ] Replace direct `plan.to_table()` usage with:

  * [ ] `materialize_plan_adapter(plan, ctx=ctx, options=AdapterRunOptions(...))`
  * [ ] ensure `AdapterRunOptions` carries backend/params through

### Key diff shape

```py
# src/arrowdsl/plan_helpers.py
from ibis.backends import BaseBackend
from ibis.expr.types import Value

@dataclass(frozen=True)
class FinalizePlanAdapterOptions:
    adapter_mode: AdapterMode | None = None
    prefer_reader: bool = False
    schema: SchemaLike | None = None
    keep_extra_columns: bool = False
    ibis_backend: BaseBackend | None = None
    ibis_params: Mapping[Value, object] | None = None

# inside finalize_plan_result_adapter IbisPlan path:
result = run_plan_adapter(
    plan,
    ctx=ctx,
    options=AdapterRunOptions(
        adapter_mode=options.adapter_mode,
        prefer_reader=options.prefer_reader,
        ibis_backend=options.ibis_backend,
        ibis_params=options.ibis_params,
    ),
)
table = _materialize_table(result.value)
# then align_table_to_schema(table, ...)
```

### Review focus

* Removes a “bridge bypass” that would otherwise keep surprising you later.

---

## Commit 5 — Plumb `ibis_backend` into all the existing adapter execution callsites

**Commit title:** `pipeline: pass ibis_backend into AdapterRunOptions/FinalizePlanAdapterOptions`

### Why

PR5’s runner changes only help if callsites supply `ibis_backend` at execution time.

### Files

* [ ] `src/extract/ast_extract.py`
* [ ] `src/extract/tree_sitter_extract.py`
* [ ] `src/cpg/build_edges.py`

### Checklist

* [ ] In each `run_plan_adapter(... AdapterRunOptions(...))` invocation:

  * [ ] pass `ibis_backend=exec_context.ibis_backend`
* [ ] In `build_cpg_edges()` where `finalize_plan_adapter()` is invoked on an `IbisPlan`:

  * [ ] pass `ibis_backend=config.ibis_backend`

### Review focus

* Pure wiring changes; should be a low-risk review.

---

## Commit 6 — Normalize lane: ensure rule outputs execute through bridge-aware plan adapter

**Commit title:** `normalize: run_normalize supports bridge execution via adapter options`

### Why

Hamilton normalization outputs currently don’t pass `adapter_mode` / `ibis_backend` through to `run_normalize()`, so even if PR5 adds the bridge execution path, normalization likely won’t use it.

### Files

* [ ] `src/normalize/runner.py`
* [ ] `src/hamilton_pipeline/modules/normalization.py`

### Checklist: `normalize/runner.py`

* [ ] Update `run_normalize(...)` signature to accept:

  * [ ] `adapter_mode: AdapterMode | None = None`
  * [ ] `ibis_backend: BaseBackend | None = None`
  * [ ] `params: Mapping[Value, object] | None = None` (optional; safe default)
* [ ] Pass these through to `run_plan_adapter(... AdapterRunOptions(...))`

### Checklist: Hamilton normalization module

* [ ] Update `_normalize_rule_output(...)` to accept:

  * [ ] `adapter_mode: AdapterMode`
  * [ ] `ibis_backend: BaseBackend`
* [ ] Update each output node that calls `_normalize_rule_output` to also depend on:

  * [ ] `adapter_mode`
  * [ ] `ibis_backend`

*(Yes, that’s a mechanical signature expansion — but it’s explicit and keeps caching behavior sane.)*

### Optional-but-good in the same commit

* [ ] In `compile_normalize_plans_ibis(... materialize_outputs ...)`, replace:

  * `materialized = plan.to_table()`
    with:
  * `materialized = ibis_engine.runner.materialize_plan(plan, execution=ibis_execution_options(...))`

That ensures “materialize during compilation” uses the same bridge/fallback rules.

---

## Commit 7 — Deprecate legacy relationship plan lane

**Commit title:** `deprecate: legacy compile_relation_plans ArrowDSL lane`

### Why

You’re clearly converging on `compile_relation_plans_ibis()` (design-mode / target architecture). The old ArrowDSL lane should be put on a removal track.

### Files

* [ ] `src/cpg/relationship_plans.py`
* [ ] `src/config.py` (optional: update checklist comments/docstrings)

### Checklist

* [ ] Add `warnings.warn(..., DeprecationWarning, stacklevel=2)` at the start of:

  * `compile_relation_plans(...)`
* [ ] Update docstring: “Use compile_relation_plans_ibis; legacy lane will be removed in PR7/PR8 (or your chosen milestone)”
* [ ] Keep behavior unchanged for now (PR5 should not break non-ibis mode), but make the direction unambiguous.

### Deprecation snippet

```py
import warnings

def compile_relation_plans(...):
    warnings.warn(
        "compile_relation_plans (ArrowDSL relationship lane) is deprecated; "
        "use compile_relation_plans_ibis (Ibis/DataFusion lane).",
        DeprecationWarning,
        stacklevel=2,
    )
    ...
```

---

## Commit 8 — Tests: prove the adapter chooses the bridge lane when configured

**Commit title:** `tests: adapter runner uses ibis_engine.runner when bridge enabled`

### Files

* [ ] `tests/test_plan_adapter_datafusion_bridge.py` (new)
* [ ] potentially small fixtures in `tests/conftest.py`

### Checklist

* [ ] Unit test (mock-based) verifying:

  * when `AdapterMode(use_datafusion_bridge=True)` and `ibis_backend` is provided:

    * `_run_ibis_plan` calls `ibis_engine.runner.materialize_plan` / `stream_plan`
  * when bridge enabled but backend missing:

    * it falls back to legacy path and emits `DeprecationWarning`
* [ ] Add a single “sanity” integration test if CI has ibis+datafusion available (optional)

---

# Deprecations to introduce in PR5 (explicit list)

These are the ones I’d actually implement *in PR5*:

1. **`cpg.relationship_plans.compile_relation_plans`**
   Deprecate in favor of `compile_relation_plans_ibis`.

2. **Bridge bypass fallback** when `AdapterMode.use_datafusion_bridge=True` but no backend is provided

   * Keep it working in PR5
   * Emit `DeprecationWarning`
   * Plan to make it an error once the pipeline wiring is complete (PR6)

Things I’d **call out** as “next deprecations” but *not necessarily implement in PR5* unless you want bigger churn:

* `ibis_engine.plan_bridge.plan_to_ibis(...)` as a transitional “materialize PlanIR then re-enter Ibis” bridge
* remaining direct `plan.to_table()` usage in compilation/materialization helpers outside the adapter runner

---

# What I’d expect PR5 to unlock immediately

* A single switch (`AdapterMode.use_datafusion_bridge`) now meaningfully routes execution through:

  * **DataFusion** for compute
  * **SQLGlot** fallback for unsupported Ibis ops (and diagnostics)
  * runtime hooks (explain snapshots, fallback collectors, etc.)

* Clear separation emerging:

  * **Compilation:** build Ibis plans and register views
  * **Execution:** adapter runner decides how to execute (bridge vs legacy)
  * **Materialization boundary:** only at the very end (finalize, storage, debug outputs)

---

If you want, I can also draft the **exact PR description text** (summary, motivation, risk, rollout flags, test plan) to paste into GitHub so reviewers immediately understand what to focus on.
