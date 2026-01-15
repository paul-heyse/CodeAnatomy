I unzipped and walked through the codebase in **`src/`** (DataFusion runtime + registry bridge, SQLGlot → DataFusion translation, Ibis plan lane, relspec compiler/diagnostics, Hamilton pipeline wiring, run bundle/manifest output).

You’ve already landed a lot of the “PR1–PR4” architectural pieces (runtime profile, dataset registry bridge, diagnostics tables/substrait payloads, hybrid/kernel lane support). The next PRs I’d prioritize are the ones that **close the remaining “wiring gaps”** between those building blocks and the actual pipeline execution paths—so you’re truly executing against the shared DataFusion runtime, capturing explain/fallbacks, and steadily retiring the ArrowDSL plan lane.

Below is a concrete PR roadmap (each PR is intentionally scoped so it can land independently).

---

## PR5 — Make `ExecutionContext.runtime.datafusion` the single source of truth (fix the split-session issue)

### Why this should be next

Right now your **Hamilton `ctx`** and your **Ibis backend** are effectively operating with *different* DataFusion runtime instances:

* `hamilton_pipeline/modules/inputs.py: ctx()` builds an `ExecutionContext` whose `runtime.datafusion` is configured (and in debug mode it flips `capture_explain=True` inside `execution_context_factory`).
* But `hamilton_pipeline/modules/inputs.py: ibis_backend_config()` returns **`IbisBackendConfig()` with no `datafusion_profile` injected**, so `ibis_engine.backend.build_backend()` creates (or defaults) to a separate DataFusion profile/context.

That means:

* **DataFusion catalog snapshots / cached dataset lists / explain collector / fallback collector** in `hamilton_pipeline/modules/outputs.py::_datafusion_notes()` are looking at `ctx.runtime.datafusion.session_context()`, which may not be the same context the backend actually registered tables into.
* You’ll see “empty” or misleading DataFusion diagnostics even when the pipeline ran correctly.

### Scope / changes

1. **Inject the context profile into the backend config**

   * Update `hamilton_pipeline/modules/inputs.py` so `ibis_backend_config` depends on `ctx`:

     * `def ibis_backend_config(ctx: ExecutionContext) -> IbisBackendConfig:`
     * return `IbisBackendConfig(datafusion_profile=ctx.runtime.datafusion)`
2. (Optional but clean) Add a single node that exposes the shared `SessionContext` for anything that needs it:

   * `def datafusion_ctx(ctx: ExecutionContext) -> SessionContext: return ctx.runtime.datafusion.session_context()`

### Acceptance criteria

* `outputs._datafusion_notes()` shows:

  * `datafusion_catalog` populated
  * `datafusion_cached_datasets` populated when caching is enabled/used
  * `datafusion_explain` / `datafusion_fallbacks` populated once you start routing execution through the bridge (next PRs)

### Minimal tests

* A small integration test that:

  * builds `ctx`
  * builds `ibis_backend`
  * registers a table via the pipeline registry path
  * asserts the table appears in `ctx.runtime.datafusion.catalog_snapshot(...)`

---

## PR6 - Extend `AdapterMode.use_datafusion_bridge` beyond the plan runner (it is partial today)

### Why

`AdapterMode.use_datafusion_bridge` is already wired in `arrowdsl/plan/runner.py`, but the
adapter only covers IbisPlan execution when that runner is used. Several materialization
paths still call `plan.to_table()` or `plan.to_reader()` directly:

* normalize materialize_outputs path
* `extract/helpers.py: ast_def_nodes_plan`
* `ibis_engine/io_bridge.py` materializers and write helpers
* debug normalized-input writer in `hamilton_pipeline/modules/outputs.py`
* `cpg/relationship_plans.py: CatalogPlanResolver`

Those bypass the shared DataFusion SessionContext and skip explain/fallback capture, so
diagnostics are inconsistent and regressions are harder to spot.

### What I'd implement

1. Keep the plan runner path as the canonical gate for DataFusion-aware execution.
2. Introduce a single adapterized execution helper (see PR11) and route all remaining
   IbisPlan materializations through it.
3. Add a small test that asserts `datafusion_fallbacks` and `datafusion_explain` appear
   when `AdapterMode.use_datafusion_bridge` is enabled.

### Acceptance criteria

* Turning on `AdapterMode(use_datafusion_bridge=True)` causes:

  * `outputs._datafusion_notes()` to include non-empty `datafusion_fallbacks` when translation falls back to SQL
  * `datafusion_explain` to fill when `capture_explain=True`
* No behavior change when the flag is false.

---

## PR7 — Route **relspec relationship rule execution** through the unified execution adapter (stop calling `plan.to_table()` directly)

### Why

This is a major remaining “architecture gap”:

In `relspec/compiler.py`:

* `CompiledRule.execute()` compiles a `RelPlan` → `IbisPlan`
* then calls **`plan.to_table(params=params)` directly**

That bypasses:

* your adapter gating (`AdapterMode`)
* your DataFusion runtime bridge + collectors
* any consistent “execution policy” you want enforced centrally

This is especially important because relationship rules are the heart of the “target architecture” (relational compiler → deterministic graph tables).

### Scope / changes

* Update `relspec/compiler.py::CompiledRule.execute()` so that when it has an `IbisPlan`, it executes via a runner that:

  * has access to:

    * `ctx` (for determinism / canonical sort)
    * `params`
    * the shared DataFusion session/runtime profile
    * the Ibis backend/compiler

Concretely:

* If you adopt PR6 option (thread backend through options), then:

  * add `backend` to the `PlanResolver` protocol (or at least standardize that `resolver.backend` is present)
  * use the same “execute Ibis plan via adapter” helper here

### Acceptance criteria

* Relationship outputs still match (schema + row semantics)
* Now you can actually observe:

  * which rules caused SQL fallback
  * what EXPLAIN plans were produced (in debug mode)
  * DataFusion catalog snapshot includes the expected registered input datasets/param tables

### Minimal tests

* A tiny rule (1 join + projection) executed two ways:

  * old direct `plan.to_table(params=...)`
  * new adapter-executed path
    Assert identical results.

---

## PR8 — Flip normalize to Ibis-first (you already have `normalize/ibis_plan_builders.py`, now make it the default path)

### Why

Normalize is still heavily ArrowDSL-plan-lane-oriented (`normalize/runner.py` and many `normalize/*_plans.py`).

But you already created a strong Ibis plan builder suite:

* `normalize/ibis_plan_builders.py` has Ibis equivalents for key normalize datasets
* `normalize/runner.py` already supports `Plan | IbisPlan` in types

The next step toward the target architecture is to:

* make normalize compilation produce **IbisPlan by default**
* keep ArrowDSL plan lane as fallback only (temporary)

### Scope / changes

1. Update normalize compilation so that (when `adapter_mode.use_ibis_bridge` is true):

   * use `PLAN_BUILDERS_IBIS` and build `NormalizeIbisPlanOptions(...)`
2. Ensure normalize execution uses the same adapter runner from PR6:

   * so the DataFusion bridge/telemetry becomes consistent across normalize + relspec rules

### Acceptance criteria

* Normalize outputs produced by IbisPlan builders match existing outputs
* Determinism/canonical sort rules still hold (you already enforce ordering in finalize gates)

---

## PR9 — ArrowDSL plan lane decommission PRs (break into two cleanup PRs)

You already have a checklist in `config.py`:

> “All plan builders have Ibis bridge coverage… Ibis bridge enabled across pipeline entry points… DataFusion registry bridge validated… Kernel fallbacks verified… Legacy ArrowDSL plan helpers removed…”

I would split the “decommission” effort into two PRs so it’s not a mega-PR:

### PR9a — “No new ArrowDSL plans” enforcement + coverage audit

* Add a small audit utility that:

  * enumerates plan builders / outputs
  * reports which are still Plan-only vs Ibis-capable
* Make the pipeline default to Ibis where coverage exists
* Add a CI guardrail: if a new normalize/extract builder is added, it must provide an Ibis builder too (or explicitly justify Plan-only)

### PR9b — Remove / deprecate legacy helpers once coverage is complete

* Start with the least risky removals:

  * duplicated query builders / bridge utilities
  * Plan-only wrappers that now have Ibis equivalents

---

## PR10 — Unify runtime profile definitions 

You currently have **two runtime-profile systems**:

* `arrowdsl/core/context.py` defines `runtime_profile_factory(...)` and sets `capture_explain` in debug
* `config.py` also defines runtime profiles (`RUNTIME_PROFILES`, scan profiles, etc.) but that is *not* what `execution_context_factory` uses

This will drift over time.

### Scope

* Pick one source of truth `arrowdsl/core/context.py` since it’s already what the pipeline uses
* Remove or re-home the unused profile map in `config.py`

### Benefit

* Makes runtime configuration “obvious” and avoids silent mismatches.

---

## PR11 - Adapterize all IbisPlan materialization paths (make the adapter the only gate)

### Narrative

Right now, "use_datafusion_bridge" only takes effect when the plan runner is used.
Several other code paths still materialize IbisPlan directly, which means:

* the shared DataFusion SessionContext is not always used
* explain/fallback collectors are bypassed
* cross-run diagnostics are inconsistent depending on which path touched the plan

This PR makes IbisPlan execution uniform: if we have a context and adapter options,
IbisPlan materialization must go through the adapter or a single helper that wraps it.
Direct calls to `plan.to_table()` and `plan.to_reader()` remain valid only when an
explicit context is not available (for example, isolated utility usage).

### Code pattern (single adapterized helper)

```python
from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from ibis.backends import BaseBackend
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.plan.runner import AdapterRunOptions, run_plan_adapter
from config import AdapterMode
from ibis_engine.plan import IbisPlan


def materialize_ibis_plan_adapter(
    plan: IbisPlan,
    *,
    ctx: ExecutionContext,
    adapter_mode: AdapterMode | None,
    ibis_backend: BaseBackend | None,
    params: Mapping[IbisValue, object] | None = None,
) -> TableLike:
    result = run_plan_adapter(
        plan,
        ctx=ctx,
        options=AdapterRunOptions(
            adapter_mode=adapter_mode,
            prefer_reader=False,
            ibis_backend=ibis_backend,
            ibis_params=params,
        ),
    )
    return cast("TableLike", result.value)


def stream_ibis_plan_adapter(
    plan: IbisPlan,
    *,
    ctx: ExecutionContext,
    adapter_mode: AdapterMode | None,
    ibis_backend: BaseBackend | None,
    params: Mapping[IbisValue, object] | None = None,
) -> RecordBatchReaderLike:
    result = run_plan_adapter(
        plan,
        ctx=ctx,
        options=AdapterRunOptions(
            adapter_mode=adapter_mode,
            prefer_reader=True,
            ibis_backend=ibis_backend,
            ibis_params=params,
        ),
    )
    return cast("RecordBatchReaderLike", result.value)
```

### Target files

* `src/ibis_engine/io_bridge.py`
* `src/normalize/runner.py`
* `src/extract/helpers.py`
* `src/hamilton_pipeline/modules/outputs.py`
* `src/cpg/relationship_plans.py`
* `src/relspec/compiler_graph.py`

### Implementation checklist

* [ ] Add adapterized helper(s) in a single module (prefer `ibis_engine/execution.py`)
* [ ] Replace IbisPlan `to_table()` / `to_reader()` usages in the target files with the helper
* [ ] Thread `ExecutionContext`, `AdapterMode`, and `BaseBackend` into call sites that need them
* [ ] Keep a narrow fallback path only where no context is available (explicit, documented)
* [ ] Update debug writers to use the adapter so diagnostics are captured uniformly

### Acceptance criteria

* All IbisPlan materialization paths with a context run through the adapter
* `outputs._datafusion_notes()` shows explain/fallback entries for normalized-input writes
* No change in output schemas or determinism behavior

### Minimal tests

* Integration test that writes normalized inputs with `use_datafusion_bridge=True`
  and asserts non-empty `datafusion_fallbacks` or `datafusion_explain` in notes

---

## PR12 - Schema access without executing IbisPlan

### Narrative

Some code paths materialize IbisPlan simply to read a schema. That is unnecessary work,
and it can trigger DataFusion execution in places that should be "compile-time only."
Schema access should be purely logical: use the Ibis expression schema or a helper
that selects the right schema path based on Plan vs IbisPlan.

This PR is a small but high-leverage cleanup because it removes hidden execution
points and makes runtime behavior more predictable. It also reduces chances that
diagnostics capture queries that were never intended to execute.

### Code pattern (schema helper)

```python
from __future__ import annotations

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.plan.plan import Plan
from ibis_engine.plan import IbisPlan


def plan_schema(plan: Plan | IbisPlan, *, ctx: ExecutionContext) -> pa.Schema:
    if isinstance(plan, IbisPlan):
        return plan.expr.schema().to_pyarrow()
    return plan.schema(ctx=ctx)
```

### Target files

* `src/normalize/runner.py`
* `src/hamilton_pipeline/modules/outputs.py`
* `src/extract/helpers.py`

### Implementation checklist

* [ ] Introduce a small schema helper in a shared module (or local to the caller)
* [ ] Replace IbisPlan `to_table().schema` usages with logical schema access
* [ ] Ensure any schema-derived metadata stays identical to current outputs

### Acceptance criteria

* No IbisPlan execution occurs for schema-only inspection
* Canonical sort behavior remains unchanged

### Minimal tests

* Unit test that verifies schema helper output matches materialized schema

---

## PR13 - Execution policy and strictness for DataFusion fallback

### Narrative

Today, fallback to SQL is allowed and only recorded. That is good for dev, but it is
not sufficient for production-grade correctness. We should be able to declare a policy
that says "no fallback is allowed" in strict mode, and fail fast when it occurs.

This PR introduces an explicit execution policy that controls fallback behavior and
explain capture. It makes execution semantics a conscious choice rather than an
incidental side effect of runtime defaults.

### Code pattern (policy hook)

```python
from __future__ import annotations

from dataclasses import dataclass

from datafusion_engine.compile_options import DataFusionFallbackEvent
from datafusion_engine.runtime import DataFusionFallbackCollector


@dataclass(frozen=True)
class AdapterExecutionPolicy:
    allow_fallback: bool = True
    fail_on_fallback: bool = False


def policy_fallback_hook(
    policy: AdapterExecutionPolicy,
    collector: DataFusionFallbackCollector | None,
):
    def _hook(event: DataFusionFallbackEvent) -> None:
        if policy.fail_on_fallback:
            msg = f"DataFusion fallback blocked: {event.expression_type}"
            raise ValueError(msg)
        if collector is not None:
            collector.hook(event)
    return _hook
```

### Target files

* `src/config.py`
* `src/hamilton_pipeline/modules/inputs.py`
* `src/datafusion_engine/runtime.py`
* `src/ibis_engine/runner.py`
* `src/arrowdsl/plan/runner.py`

### Implementation checklist

* [ ] Add an execution policy model (config-level)
* [ ] Thread the policy into DataFusion compile options or adapter runner options
* [ ] Wrap fallback hooks to enforce policy and still collect diagnostics
* [ ] Expose a strict mode toggle (env var or config)
* [ ] Document policy semantics in the plan and in config docstrings

### Acceptance criteria

* In strict mode, any fallback raises a clear error with rule context
* In default mode, fallback continues to be recorded but not fatal

### Minimal tests

* Test that a known-unsupported operation fails in strict mode and passes in default mode

---

## PR14 - Rule-scoped diagnostics and lineage artifacts

### Narrative

Fallback and explain collectors are global today, which makes it hard to map diagnostics
back to the rule or dataset that triggered them. We want to understand "which rule fell
back and why" and to store that as part of the run bundle.

This PR adds rule-scoped labeling for explain and fallback events and records canonical
SQLGlot or SQL strings in the manifest. The result is a traceable, deterministic
diagnostics trail that can be correlated with output datasets.

### Code pattern (labeled hooks)

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from datafusion_engine.compile_options import DataFusionFallbackEvent


@dataclass(frozen=True)
class ExecutionLabel:
    rule_name: str
    output_dataset: str


def labeled_fallback_hook(label: ExecutionLabel, sink: list[dict[str, object]]):
    def _hook(event: DataFusionFallbackEvent) -> None:
        sink.append(
            {
                "rule": label.rule_name,
                "output": label.output_dataset,
                "reason": event.reason,
                "expression_type": event.expression_type,
                "sql": event.sql,
                "dialect": event.dialect,
            }
        )
    return _hook


def labeled_explain_hook(label: ExecutionLabel, sink: list[dict[str, object]]):
    def _hook(sql: str, rows: Sequence[Mapping[str, object]]) -> None:
        sink.append(
            {
                "rule": label.rule_name,
                "output": label.output_dataset,
                "sql": sql,
                "rows": [dict(row) for row in rows],
            }
        )
    return _hook
```

### Target files

* `src/datafusion_engine/runtime.py`
* `src/ibis_engine/runner.py`
* `src/relspec/compiler.py`
* `src/normalize/runner.py`
* `src/hamilton_pipeline/modules/outputs.py`
* `src/obs/manifest.py`

### Implementation checklist

* [ ] Add an execution label model for rule/output metadata
* [ ] Wrap explain/fallback hooks with labels during rule execution
* [ ] Store labeled diagnostics in run bundle or manifest notes
* [ ] Add SQLGlot canonical SQL or SQL strings to diagnostics payloads
* [ ] Ensure payloads are JSON-safe and stable across runs

### Acceptance criteria

* Manifest includes per-rule explain and fallback entries
* Diagnostics can be traced to output datasets deterministically

### Minimal tests

* Unit test that labeled hooks attach rule/output fields to diagnostics entries

---

## PR15 - Output parity harness (DataFusion bridge on/off)

### Narrative

This codebase is actively transitioning from ArrowDSL plan lane to DataFusion-backed
execution. The fastest way to protect correctness is an automated parity harness that
runs a small fixture through both modes and compares outputs.

The goal is not to prove perfect identity for every dataset, but to detect "wrong"
changes early. A schema match plus stable row hash (after canonical sort) is a strong
signal that the execution modes are equivalent.

### Code pattern (row hash helper)

```python
from __future__ import annotations

import hashlib

import pyarrow as pa
import pyarrow.compute as pc


def table_digest(table: pa.Table) -> str:
    sorted_table = table.sort_by([(name, "ascending") for name in table.schema.names])
    batches = sorted_table.to_batches()
    hasher = hashlib.sha256()
    for batch in batches:
        for col in batch.columns:
            data = col.to_pylist()
            hasher.update(str(data).encode("utf-8"))
    return hasher.hexdigest()
```

### Target files

* `tests/integration/test_datafusion_parity.py`
* `tests/fixtures/repos/mini_repo/` (or equivalent small fixture)
* `tests/utils.py` (shared hash helper, if preferred)

### Implementation checklist

* [ ] Add a minimal fixture repo with stable outputs
* [ ] Run the pipeline twice: adapter off and adapter on
* [ ] Compare output schemas and row hashes for key outputs
* [ ] Mark any expected differences explicitly (and document them)

### Acceptance criteria

* Parity test passes for key outputs (nodes, edges, props)
* Any expected deltas are documented and stable

### Minimal tests

* One integration test that runs both modes and compares hashes

---
