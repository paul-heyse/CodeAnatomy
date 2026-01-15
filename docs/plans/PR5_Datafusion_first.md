## PR5 implementation plan: “DataFusion-first execution adapter for Ibis plans”

### Why PR5 / what it unlocks

In the current codebase, you already have the *pieces* of the target architecture in place:

* A real **DataFusion session contract** (`DataFusionRuntimeProfile`, `compile_options`, explain/fallback collectors)
* A **SQLGlot→DataFusion bridge** (`datafusion_engine.bridge`) that can:

  * compile Ibis → SQLGlot → DataFusion DataFrame,
  * fall back to SQL safely,
  * emit **fallback diagnostics**,
  * optionally run **EXPLAIN / EXPLAIN ANALYZE**
* A **hybrid Ibis execution runner** (`ibis_engine.runner`) that decides when to force-bridge (e.g., when `capture_explain=True`)

But **the main plan execution pathway still bypasses these**:

* `arrowdsl.plan.runner._run_ibis_plan()` calls `plan.to_table()` / `plan.to_reader()` directly.
* `arrowdsl.plan_helpers.finalize_plan_result_adapter()` calls `plan.to_table()` directly in the schema-alignment path.
* `relspec.compiler.CompiledOutput.execute()` executes Ibis plans via `plan.to_table(params=...)`.

So PR5 should wire these together end-to-end, behind the already-existing flag:

* `AdapterMode.use_datafusion_bridge` (currently defined, but not actually used anywhere)

This PR makes DataFusion the **observable, diagnosable execution substrate** (explain plans + fallback logs), and it sets you up to retire ArrowDSL’s relational “plan lane” without losing debuggability.

---

## PR5 goals

### Primary goals

1. **Implement `AdapterMode.use_datafusion_bridge` for IbisPlan execution**

   * Use the existing `ibis_engine.runner` + `datafusion_engine.bridge` path when enabled
   * Collect explain + fallback events into `ctx.runtime.datafusion` collectors (already surfaced in `hamilton_pipeline/modules/outputs.py`)

2. **Unify the pipeline’s Ibis backend SessionContext with `ctx.runtime.datafusion`**

   * Right now the Hamilton `ctx()` and `ibis_backend()` can end up with *different* DataFusion SessionContexts, which fragments registrations and telemetry.

3. **Support scalar parameter bindings all the way through the adapter**

   * Relationship execution already passes `relspec_param_bindings: Mapping[IbisValue, object]`
   * The bridge already supports params (`DataFusionCompileOptions.params`)
   * The adapter just needs to plumb them in

### Non-goals (explicitly deferred to PR6+)

* Full migration of CPG nodes/props builders off ArrowDSL plan-lane (that’s a great next PR after this)
* IO/materialization refactor (COPY TO / write_parquet consolidation, etc.)

---

## PR5 deliverables and step-by-step implementation

# 1) Make DataFusion SessionContext *shared* between `ctx` and `ibis_backend`

### Problem

`hamilton_pipeline/modules/inputs.py` builds:

* `ctx()` via `execution_context_factory("default")` (has its own `DataFusionRuntimeProfile`)
* `ibis_backend()` via `build_backend(IbisBackendConfig())` which creates **a different DataFusionRuntimeProfile** unless explicitly provided

That means:

* datasets/param tables registered in one context may not exist in the other
* DataFusion explain/fallback collectors attached to `ctx.runtime.datafusion` won’t see queries executed on the backend’s context

### Change

Update `ibis_backend_config()` to depend on `ctx` and re-use its runtime profile.

**File:** `hamilton_pipeline/modules/inputs.py`

```python
@tag(layer="inputs", kind="runtime")
def ibis_backend_config(ctx: ExecutionContext) -> IbisBackendConfig:
    # Reuse the SessionContext owned by the pipeline ExecutionContext runtime profile.
    # This ensures one catalog, one set of registrations, one telemetry stream.
    return IbisBackendConfig(datafusion_profile=ctx.runtime.datafusion)
```

Everything else can remain the same:

```python
def ibis_backend(ibis_backend_config: IbisBackendConfig) -> BaseBackend:
    return build_backend(ibis_backend_config)
```

### Add a runtime “sanity check” (optional but very useful)

Create a tiny helper (or log) that confirms the contexts match at startup in debug mode.

---

# 2) Promote `_datafusion_context()` helper to a public function

You already have a private helper in `ibis_engine/registry.py`:

```python
def _datafusion_context(backend: BaseBackend) -> object | None: ...
```

PR5 should turn this into a supported API because the execution adapter will need it too.

**File:** `ibis_engine/registry.py`

```python
def datafusion_context(backend: ibis.backends.BaseBackend) -> SessionContext | None:
    if SessionContext is None:
        return None
    for attr in ("con", "_context", "_ctx", "ctx", "session_context"):
        ctx = getattr(backend, attr, None)
        if isinstance(ctx, SessionContext):
            return ctx
    return None
```

Then:

* Update internal uses (registry registration) to call `datafusion_context`
* Keep `_datafusion_context` temporarily, but mark it deprecated

### Deprecate in PR5

* `ibis_engine.registry._datafusion_context` (private API)

---

# 3) Extend the Plan adapter API to accept Ibis backend + params

Right now, the adapter’s option bundle can’t carry what the DataFusion bridge needs.

**File:** `arrowdsl/plan/runner.py`

### Add fields to `AdapterRunOptions`

```python
from collections.abc import Mapping
from ibis.backends import BaseBackend
from ibis.expr.types import Value as IbisValue

@dataclass(frozen=True)
class AdapterRunOptions:
    adapter_mode: AdapterMode | None = None
    prefer_reader: bool = False
    metadata_spec: SchemaMetadataSpec | None = None
    attach_ordering_metadata: bool = True

    # PR5 additions:
    ibis_backend: BaseBackend | None = None
    ibis_params: Mapping[IbisValue, object] | None = None
```

This keeps everything backward compatible: call sites that don’t know about Ibis/DataFusion keep working.

---

# 4) Implement `AdapterMode.use_datafusion_bridge` inside `_run_ibis_plan()`

You already have the “smart execution” logic in `ibis_engine.runner`:

* `materialize_plan(plan, execution=...)`
* `stream_plan(plan, execution=...)`

Those can use:

* pure Ibis execution (existing behavior)
* *or* forced DataFusion bridge execution (Ibis → SQLGlot → DF), especially when `capture_explain` is enabled

### Modify `_run_ibis_plan`

**File:** `arrowdsl/plan/runner.py`

```python
from ibis_engine.runner import (
    DataFusionExecutionOptions,
    IbisPlanExecutionOptions,
    materialize_plan as ibis_materialize_plan,
    stream_plan as ibis_stream_plan,
)
from ibis_engine.registry import datafusion_context
from sqlglot_tools.bridge import IbisCompilerBackend  # protocol

def _run_ibis_plan(..., options: AdapterRunOptions, ...) -> PlanRunResult:
    adapter_mode = options.adapter_mode or AdapterMode()

    execution: IbisPlanExecutionOptions | None = None
    if (
        adapter_mode.use_datafusion_bridge
        and options.ibis_backend is not None
        and ctx.runtime.datafusion is not None
    ):
        df_ctx = datafusion_context(options.ibis_backend) or ctx.runtime.datafusion.session_context()
        execution = IbisPlanExecutionOptions(
            params=options.ibis_params,
            datafusion=DataFusionExecutionOptions(
                backend=cast(IbisCompilerBackend, options.ibis_backend),
                ctx=df_ctx,
                runtime_profile=ctx.runtime.datafusion,
                # options left as None -> runner will derive from runtime_profile.compile_options(...)
                options=None,
                allow_fallback=True,
            ),
        )

    if prefer_reader and ctx.determinism != DeterminismTier.CANONICAL:
        reader = (
            ibis_stream_plan(plan, batch_size=None, execution=execution)
            if execution is not None
            else plan.to_reader()
        )
        ...
    else:
        table = (
            ibis_materialize_plan(plan, execution=execution)
            if execution is not None
            else plan.to_table(params=options.ibis_params)
        )
        table, canonical_keys = apply_canonical_sort(...)
        ...
```

### Why this is the right integration point

* It’s the **single choke point** for all IbisPlan execution through your adapter layer.
* It activates explain/fallback capture automatically whenever:

  * `AdapterMode.use_datafusion_bridge=True`, and
  * `ctx.runtime.datafusion.capture_explain=True` (already set via debug mode plumbing)

### Deprecate in PR5

* The *direct* execution path in the adapter layer as “preferred”

  * Not removing it, but re-framing it as fallback/legacy:

    * “Direct `IbisPlan.to_table()` in adapter execution is deprecated; use DataFusion bridge mode.”

---

# 5) Fix `finalize_plan_result_adapter()` to stop bypassing the adapter

Right now, schema-alignment for IbisPlan does:

```python
aligned = align_table_to_schema(plan.to_table(), schema=...)
```

That completely bypasses the new bridge execution.

### Change it to materialize through `run_plan_adapter` first

**File:** `arrowdsl/plan_helpers.py`

* Extend `FinalizePlanAdapterOptions`:

```python
@dataclass(frozen=True)
class FinalizePlanAdapterOptions:
    adapter_mode: AdapterMode | None = None
    prefer_reader: bool = False
    schema: SchemaLike | None = None
    keep_extra_columns: bool = False

    # PR5 additions:
    ibis_backend: BaseBackend | None = None
    ibis_params: Mapping[IbisValue, object] | None = None
```

* Update `finalize_plan_result_adapter()` for IbisPlan:

```python
if isinstance(plan, IbisPlan):
    run_opts = AdapterRunOptions(
        adapter_mode=options.adapter_mode,
        prefer_reader=False if options.schema is not None else options.prefer_reader,
        ibis_backend=options.ibis_backend,
        ibis_params=options.ibis_params,
    )
    result = run_plan_adapter(plan, ctx=ctx, options=run_opts)

    if options.schema is not None:
        table = cast("TableLike", result.value)
        aligned = align_table_to_schema(
            table,
            schema=options.schema,
            safe_cast=ctx.safe_cast,
            keep_extra_columns=options.keep_extra_columns,
            on_error="unsafe" if ctx.safe_cast else "raise",
        )
        return PlanRunResult(value=aligned, kind="table")

    return result
```

This ensures:

* explain/fallback capture is preserved even when schema alignment is requested
* streaming is disabled only when required (schema alignment or CANONICAL determinism)

---

# 6) Update pipeline call sites to pass `ibis_backend` and `adapter_mode`

This is what flips the “bridge execution” switch in real runs.

### A) Relationship outputs execution (most important)

Relationship tables are currently executed via `CompiledOutput.execute(...)`, which uses `IbisPlan.to_table(params=...)` internally.

#### Add an optional `plan_executor` hook

**File:** `relspec/compiler.py`

```python
from collections.abc import Callable
PlanExecutor = Callable[[IbisPlan, ExecutionContext, Mapping[IbisValue, object] | None], pa.Table]

def execute(..., params=None, plan_executor: PlanExecutor | None = None) -> FinalizeResult:
    ...
    table_parts = [
        compiled.execute(..., params=params, plan_executor=plan_executor)
        for compiled in self.contributors
    ]
```

And in `CompiledRule.execute`:

```python
if self.rel_plan is not None:
    plan = compiler.compile(self.rel_plan, ctx=ctx, resolver=resolver)
    if plan_executor is not None:
        table = plan_executor(plan, ctx, params)
    else:
        table = plan.to_table(params=params)
```

#### Wire it up in the pipeline node

**File:** `hamilton_pipeline/modules/cpg_build.py` (`relationship_tables`)

Add inputs:

* `ibis_backend: BaseBackend`
* `adapter_mode: AdapterMode`

Then define executor:

```python
from arrowdsl.plan.runner import run_plan_adapter, AdapterRunOptions

def _executor(plan: IbisPlan, ctx: ExecutionContext, params):
    res = run_plan_adapter(
        plan,
        ctx=ctx,
        options=AdapterRunOptions(
            adapter_mode=adapter_mode,
            prefer_reader=False,
            ibis_backend=ibis_backend,
            ibis_params=params,
        ),
    )
    return cast("TableLike", res.value)
```

Pass it into `compiled.execute(...)`.

This makes relationship execution:

* use SQLGlot bridge when enabled
* capture explain/fallback into `ctx.runtime.datafusion`
* continue supporting Ibis params (`relspec_param_bindings`)

### B) Normalization runner

`normalize/runner.py` calls `run_plan_adapter(...)` with no adapter_mode/backend.

Update `run_normalize(...)` signature to accept (optional) `adapter_mode` + `ibis_backend` and pass them into AdapterRunOptions.

### C) CPG edges finalize path

`cpg/build_edges.py` already calls `finalize_plan_adapter(...)` with `adapter_mode`; extend to pass `ibis_backend` through `FinalizePlanAdapterOptions`.

(Once you do this, edges become fully observable in DataFusion explain/fallback too.)

---

# 7) Turn on bridge mode in the default architecture path

Right now, `AdapterMode()` defaults to:

* `use_ibis_bridge=True`
* `use_datafusion_bridge=False`

For PR5, I recommend:

* keep default **False** to avoid surprising behavior changes
* but enable it automatically in debug or targeted profiles

Two safe options:

1. Enable in debug mode only (recommended first)

   * e.g., in `hamilton_pipeline/modules/inputs.py.adapter_mode()`:

   ```python
   mode = AdapterMode()
   if ctx.debug:
       mode = replace(mode, use_datafusion_bridge=True)
   return mode
   ```
2. Or enable via env var:

   * `CODEANATOMY_USE_DATAFUSION_BRIDGE=1`

---

## Deprecations to include in PR5 (explicit list)

These are the ones I’d actually mark (docstrings + warnings where appropriate):

1. **Deprecated execution path:**

   * “Direct `IbisPlan.to_table()` / `to_reader()` in adapter execution paths”
   * Meaning: in `arrowdsl.plan.runner` + `arrowdsl.plan_helpers`, the preferred path becomes the adapter + DataFusion bridge when enabled.

2. **Deprecated helper:**

   * `ibis_engine.registry._datafusion_context` → replaced by `datafusion_context`

3. **Deprecated pipeline wiring:**

   * `hamilton_pipeline/modules/inputs.ibis_backend_config()` that does *not* depend on `ctx`
   * Rationale: it silently creates an independent DataFusion SessionContext which breaks “single session contract”.

---

## Validation / test plan (what to add in PR5)

Even if you don’t have a big test suite yet, PR5 is important enough to add 2–3 “smoke-level” tests.

### 1) Adapter execution populates explain collector

* Build ctx with `ctx.runtime.datafusion.capture_explain=True`
* Create an IbisPlan (`backend.table(...).select(...)`)
* Run via `run_plan_adapter(..., AdapterRunOptions(use_datafusion_bridge=True, ibis_backend=backend))`
* Assert: `ctx.runtime.datafusion.explain_entries()` is non-empty

### 2) Fallback collection works

* Create an Ibis expression that forces df_builder fallback (something you know isn’t translated)
* Assert `ctx.runtime.datafusion.fallback_entries()` has at least one event

### 3) Relationship params still work

* Create rule with scalar param usage
* Ensure `relspec_param_bindings` is passed and result changes as expected
* Execute using the new `plan_executor` path (bridge enabled)

---

## What PR5 sets up next (PR6+)

Once PR5 is merged and stable, you’ll have:

* a single DataFusion session,
* explain/fallback telemetry for real pipeline execution,
* param binding correctness in the bridged execution path.

That’s the moment to do the big architectural simplification:

**PR6 (suggested): migrate CPG nodes + props off ArrowDSL plan-lane → IbisPlan**

* You already did it for edges (`emit_edges_ibis.py` exists)
* Nodes/props are the remaining major Plan-lane holdouts

But PR6 is much easier/safer once PR5 is in, because you’ll have the visibility to debug the DataFusion-side query plans when anything diverges.

---


