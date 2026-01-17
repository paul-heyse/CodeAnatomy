Below is the “best‑in‑class” architecture push I’d target **based on what’s actually in your `src/` right now** (especially `relspec/`, `datafusion_engine/`, `ibis_engine/`, `cpg/`, `arrowdsl/`, `schema_spec/`, and the Hamilton modules).

I’m going to do two things:

1. **Call out the concrete architectural friction points I see in your current layout (file/function specific).**
2. **Propose a clean target architecture + a PR roadmap** that keeps your current pipeline stable, but drives you toward:

   * *maximally rule‑driven*
   * *DataFusion/Ibis/Arrow first*
   * *minimal Python loops/materialization*
   * *stronger modular boundaries*
   * *richer diagnostics/telemetry*

---

## What you already have that is very close to “best architecture”

### 1) The `relspec/` stack is the right nucleus

Your `relspec` pipeline is already shaped like a serious production system:

* **Spec → Plan IR → CompiledRule → Execution**

  * `relspec/model.py` (rule kinds, join configs, etc.)
  * `relspec/plan.py` (RelPlan IR)
  * `relspec/compiler.py` (compile + kernel lane hooks)
  * `relspec/engine.py` (execute compiled plans against a registry)

* **Kernel-lane postprocessing exists and is explicit**
  `relspec/compiler.py` already does the “execute Ibis then apply kernels” pattern in a clean, composable way.

* **Per-rule execution events collector exists**
  `relspec/rules/exec_events.py` provides a strong foundation for observability as a table, not print logs.

This is exactly where the architecture should be anchored.

### 2) The DataFusion bridging and policy plumbing is solid

`datafusion_engine/runtime.py`, `datafusion_engine/bridge.py`, and `datafusion_engine/compile_options.py` already give you:

* A central runtime profile object (`DataFusionRuntimeProfile`)
* Compilation fallbacks (SQLGlot translation vs SQL fallback)
* Explain/analyze capture hooks
* Placeholders for “real” (non-Python) function factory installation

This is the right direction.

---

## The big architectural friction points (and why they matter)

### A) You’re still carrying two “plan worlds” for core CPG emission

Edges already have an Ibis path (`cpg/emit_edges_ibis.py` and the `use_ibis` branch in `cpg/build_edges.py`), but:

* **Nodes** are plan-lane only

  * `cpg/build_nodes.py`
  * `cpg/emit_nodes.py`

* **Props** are plan-lane only

  * `cpg/build_props.py`
  * `cpg/emit_props.py`

This prevents you from having one unified “rules → ibis → datafusion” world. It also forces you to keep `PlanCatalog` (ArrowDSL plan catalog) as a CPG build dependency, while `relspec` is living in a different universe.

**Result:** more code paths, more debug surface, more accidental materialization, and harder “best-in-class” performance tuning.

### B) Performance hotspot: Python loops inside “core primitives”

A few things are still implemented via Python iteration over Arrow values (which is exactly what you’re trying to avoid at scale):

* `datafusion_engine/udf_registry.py`: stable hashing + other UDF helpers do Python-level iteration
* `arrowdsl/core/ids.py`: hash fallback path iterates
* `cpg/build_nodes.py`: `_collect_symbols()` uses Python `set(...)` over `iter_array_values(...)`
* `arrowdsl/compute/filters.py`: JSON stringify UDF iterates to build lists

For small tables this is fine; for “ubiquitous repo-scale tool used by agents” it’s the wrong place to be doing Python loops.

### C) There’s a “missing center”: one object that represents the execution substrate

Right now you have:

* `ExecutionContext` (`arrowdsl/core/context.py`) for policy knobs
* `DataFusionRuntimeProfile` (`datafusion_engine/runtime.py`) for DF session config
* Ibis backend builder (`ibis_engine/backend.py`)
* Dataset registry (`ibis_engine/registry.py`)
* PlanRunner adapter (`arrowdsl/plan/runner.py`)
* plus ad-hoc wiring in Hamilton modules

…but no single “session/workspace” object that holds:

* the DF `SessionContext`
* the Ibis backend bound to that DF context
* the dataset registry
* rule execution observers / diagnostics sinks
* compile caches / plan replay caches
* concurrency controls

You’re *close* — but a “best architecture” wants a **single, explicit execution substrate object**.

### D) “Rule-driven CPG” is true for edges, but not for nodes/props

Edges are rule-driven via `relspec`. Nodes/props are “spec-table-driven” (`cpg/spec_tables.py`, `cpg/specs.py`) but not **the same rule execution framework**.

To be maximally rule-driven, your “emit nodes” and “emit props” should be treated as **rule outputs** too (even if they are simple filter/project rules).

---

## The best-in-class target architecture

Think of it as 5 layers, with one direction of dependency:

### 1) Specs layer

* `schema_spec/` defines datasets + contracts + scan policies.
* `relspec/` defines rule specs (edges, and eventually nodes/props too).
* `cpg/specs.py` either:

  * gets absorbed into relspec rules, or
  * becomes a thin “CPG spec adapter” that compiles into relspec rules.

### 2) Compilation layer

Everything compiles into **one canonical IR family**:

* “Relational plans”: **Ibis expressions** (IbisPlan)
* “Kernel plans”: either:

  * DataFusion table/scalar UDFs (preferred), or
  * Arrow kernels (fallback only)

### 3) Execution substrate

A single session object owns:

* DF SessionContext
* Ibis backend
* Dataset registry
* compile options / policy
* rule execution observer & diagnostics sink
* plan caching (substrait replay is the big win)

### 4) Storage + incremental

Dataset partition-upsert remains, but becomes a *first-class service*:

* `DatasetWriter` / `PartitionUpserter`
* Manifest + dataset fingerprinting
* per-rule output manifests

### 5) Observability

Everything important is a table:

* per-rule execution events (you already have this)
* compile fallback events
* plan explain/analyze summaries
* IO stats (rows, bytes, fragments)
* incremental closure sets (changed files / impacted modules / impacted symbols)

---

## Actionable architecture upgrades: the core moves I would make

I’m going to frame this as “PRs”, because that’s how you’ve been executing.

### PR-A: Introduce a single execution substrate object

**Goal:** stop passing around `ctx`, `backend`, `registry`, and ad-hoc policy bundles independently.

Create:

```python
# src/engine/session.py
from dataclasses import dataclass

from arrowdsl.core.context import ExecutionContext
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis.backends import BaseBackend
from ibis_engine.registry import IbisDatasetRegistry
from obs.diagnostics_tables import DiagnosticsCollector  # (or whatever sink you standardize)

@dataclass
class EngineSession:
    ctx: ExecutionContext
    df_profile: DataFusionRuntimeProfile | None
    ibis_backend: BaseBackend
    datasets: IbisDatasetRegistry
    diagnostics: DiagnosticsCollector | None = None

    @property
    def df_ctx(self):
        # convenience accessor
        if self.df_profile is None:
            return None
        return self.df_profile.session_context()
```

Then add a factory:

```python
# src/engine/session_factory.py
from ibis_engine.backend import build_ibis_backend
from ibis_engine.registry import IbisDatasetRegistry

def build_engine_session(*, ctx: ExecutionContext, diagnostics=None) -> EngineSession:
    df_profile = ctx.runtime.datafusion
    backend = build_ibis_backend(config=IbisBackendConfig(datafusion_profile=df_profile))
    datasets = IbisDatasetRegistry(backend=backend, scan_default_catalog="datafusion")
    return EngineSession(ctx=ctx, df_profile=df_profile, ibis_backend=backend, datasets=datasets, diagnostics=diagnostics)
```

**Deprecate (soft):**

* Threading `backend`, `compile_options`, `registry` separately through Hamilton nodes.
* Any “one-off” `build_ibis_backend()` calls scattered in pipeline modules.

**Why this matters:** it lets you cleanly plug in:

* plan caches
* substrait replay caches
* diagnostics sinks
* concurrency policy (“don’t oversubscribe DF threads + Python threads”)

---

### PR-B: Finish the Ibis migration for CPG nodes

**Goal:** remove plan-lane CPG nodes as the default path.

Add:

* `src/cpg/emit_nodes_ibis.py`
* update `cpg/build_nodes.py` to support an Ibis mode and make it default when DataFusion is enabled.

Sketch:

```python
# src/cpg/emit_nodes_ibis.py
import ibis
import pyarrow as pa
from ibis.expr.types import Table

from arrowdsl.core.context import Ordering
from cpg.schemas import CPG_NODES_SCHEMA
from cpg.specs import NodeEmitSpec
from ibis_engine.plan import IbisPlan
from ibis_engine.schema_utils import align_table_to_schema, coalesce_columns, ibis_null_literal

def emit_nodes_ibis(rel: IbisPlan | Table, *, spec: NodeEmitSpec) -> IbisPlan:
    t = rel.expr if isinstance(rel, IbisPlan) else rel

    node_id = coalesce_columns(t, spec.id_cols, default=ibis_null_literal(pa.string()))
    path    = coalesce_columns(t, spec.path_cols, default=ibis_null_literal(pa.string()))
    bstart  = coalesce_columns(t, spec.bstart_cols, default=ibis_null_literal(pa.int64()))
    bend    = coalesce_columns(t, spec.bend_cols, default=ibis_null_literal(pa.int64()))
    file_id = coalesce_columns(t, spec.file_id_cols, default=ibis_null_literal(pa.string()))

    out = t.select(
        node_id=node_id,
        node_kind=ibis.literal(spec.node_kind.value),
        path=path,
        bstart=bstart,
        bend=bend,
        file_id=file_id,
    )
    out = align_table_to_schema(out, schema=CPG_NODES_SCHEMA)
    return IbisPlan(expr=out, ordering=Ordering.unordered())
```

Then in `cpg/build_nodes.py`:

* Replace `_symbol_nodes_table()` python-set logic with an Ibis union/distinct plan:

  * union `scip_symbol_information.select(symbol)` and `scip_occurrences.select(symbol)`
  * `.distinct()`
  * optional `.order_by("symbol")` (only if you want deterministic output at this stage)

**Deprecate:**

* `cpg/emit_nodes.py` as “legacy plan-lane emission”
* `_collect_symbols()` path in `cpg/build_nodes.py` (python set)

---

### PR-C: Add Ibis-based props emission (and split heavy JSON props into a separate optional phase)

Props are the trickiest place because of JSON/stringify transforms.

To stay “best architecture” while keeping functionality:

#### C1) Add `emit_props_ibis.py` for non-JSON and light transforms

You can implement:

* literals
* simple casts
* `skip_if_none`
* bool/int/float/string types
* **leave `value_json` empty** for now

This immediately lets most props stay in the DataFusion lane.

#### C2) Split heavy JSON props into a separate dataset / optional stage

You already have `include_heavy_json_props` as an option in `PropsBuildOptions`. Make it architecturally explicit:

* `cpg_props_v1` = fast lane (no JSON serialization, or only JSON for already-string columns)
* `cpg_props_json_v1` = heavy lane (optional), computed using:

  * DataFusion `to_json` (if available), or
  * a Rust UDF, or
  * as last resort your current Arrow compute python UDF (but isolated and optional)

Then materialize:

* `cpg_props_v1` always (fast)
* `cpg_props_json_v1` only when asked

and optionally provide a view:

* `cpg_props_full_v1 = union_all(fast, json)`

**Deprecate:**

* The default path where `emit_props.py` does Python JSON stringify across large arrays in “mainline” output.
* Make that an opt-in “extra artifact”.

---

### PR-D: Make “Ibis everywhere” the default in `cpg/build_edges.py` too

You already have the split path; now make the architecture choice explicit:

* If DataFusion/Ibis is configured → always use `compile_relation_plans_ibis()` + `emit_edges_ibis()`
* Keep plan-lane edges only as fallback for “no DataFusion” mode

**Deprecate:**

* `compile_relation_plans()` (the version that executes and materializes relations before building plans) for the default pipeline.
* Any “materialize at compile time” rule path unless it’s explicitly a kernel boundary.

---

### PR-E: Replace Python-implemented “core primitives” with a real function factory

This is *the* performance PR if you want best-in-class.

Right now you already have the architecture hooks:

* `datafusion_engine/function_factory.py` has `install_function_factory(ctx)` and looks for `datafusion_ext`
* `rust/datafusion_ext/` exists but is placeholder

**Make this real.**

Minimum scope for “big payoff”:

* `stable_hash64`
* `stable_hash128` (or stable UUID)
* `col_to_byte`
* `position_encoding_norm`
* ideally JSON stringify for structs/lists as `to_json_stable(...)`

Once these are implemented in the DataFusion function factory, you can:

* keep the Ibis builtin UDF signatures in `ibis_engine/builtin_udfs.py`
* remove (or downgrade) the Python UDF path in `datafusion_engine/udf_registry.py`

**Deprecate (strong):**

* `datafusion_engine/udf_registry.register_datafusion_udfs()` as a default path
* any stable-hash implementation that iterates in Python (`iter_array_values` loops)

This is the “you are no longer a Python pipeline” milestone.

---

### PR-F: Make nodes/edges/props “rule outputs” (unify the spec frameworks)

This is the unification PR that makes the architecture truly “maximally rule-driven”.

You have two spec ecosystems today:

* `relspec` rules (for relationships/edges)
* `cpg/spec_tables` (for nodes/props emission)

I would unify by introducing *two new rule handlers* under `relspec/rules/handlers/`:

* `relspec.rules.handlers.cpg_nodes.NodeEmitRuleHandler`
* `relspec.rules.handlers.cpg_props.PropEmitRuleHandler`

Each rule would compile into:

* an `IbisQuerySpec` (filter/project)
* plus optional kernel lane steps (for anything not doable in DF)

Then:

* CPG build becomes “execute rule families”:

  * `RULESET_NODES`
  * `RULESET_EDGES`
  * `RULESET_PROPS`

The current `cpg/build_*` modules become thin compatibility wrappers or are deprecated.

**Deprecate:**

* “CPG build” as a separate architectural subsystem
* instead, it becomes just another `relspec` ruleset execution

This is the point where you have a single engine story.

---

## Diagnostics/observability improvements that make the system “agent-grade”

You already have great hooks; the architecture move is to make these artifacts always emitted.

### 1) Standardize “run manifest” + dataset manifests

For each run write:

* `obs_run_manifest_v1` (one row)
* `obs_rule_exec_v1` (already basically exists)
* `obs_df_fallback_v1`
* `obs_df_explain_v1`
* `obs_dataset_io_v1` (rows/bytes written per dataset + partition counts)
* `obs_incremental_closure_v1` (changed files/modules/symbols and impacted rule outputs)

### 2) Add “plan identity” for every rule output

Store:

* `rule_name`
* `plan_hash` (stable across runs)
* input dataset fingerprints
* output dataset fingerprint

This gives you:

* correctness debugging (“why did this change?”)
* caching keys (“can I skip execution?”)
* agent-friendly introspection (“what is the minimal impacted set?”)

### 3) Capture DataFusion physical plan metrics when available

If DF exposes metrics, persist:

* `elapsed_compute_ms`, `spills`, `bytes_read`, `bytes_written`, `partitions`, etc.

Even if you can’t do it for every plan initially, do it for:

* big rulesets (calls, import edges, qname dims)
* big props emitters

---

## “Best architecture” PR roadmap I’d actually execute

If I were driving this to an end-state without destabilizing your functioning pipeline:

1. **PR-A**: `EngineSession` substrate (low risk, big clarity)
2. **PR-B**: Ibis nodes emission (removes Python loops + plan-lane reliance)
3. **PR-D**: Make Ibis edges the default everywhere (stop materializing relations pre-build)
4. **PR-C**: Ibis props emission (fast lane) + isolate heavy JSON as optional artifact
5. **PR-E**: Real DataFusion function factory (Rust) for stable hash / key primitives
6. **PR-F**: Unify nodes/edges/props as relspec rule outputs (one rule engine)
7. Observability consolidation: make the “agent-grade diagnostics tables” always present

---

## The short list of “things I would deprecate” to reach the target architecture

### Deprecate as defaults (keep as fallback)

* Plan-lane CPG emission:

  * `cpg/emit_nodes.py`
  * `cpg/build_nodes.py` (plan-only path)
  * `cpg/emit_props.py`
  * `cpg/build_props.py` (plan-only path)
  * plan-lane branch in `cpg/build_edges.py`
* Materializing relation outputs at compile time:

  * `cpg/relationship_plans.compile_relation_plans()` (keep only for debugging / no-DF mode)

### Deprecate strongly once PR‑E is done

* Python-loop UDFs for stable hash & similar primitives:

  * `datafusion_engine/udf_registry.py` default registration path
  * `arrowdsl/core/ids.py` python hashing fallback
  * JSON stringify python UDFs as “mainline” (keep as optional artifact step)

---

## If you want, I can go one step deeper: “exact file-by-file refactor map”

If you want the same level of “diff-map / commit slices” as your earlier PR planning, tell me whether you prefer:

* **Path 1 (most incremental):** keep `cpg/build_*` public API, but internally route to Ibis + relspec progressively
  or
* **Path 2 (cleanest end state):** make `relspec` the only compilation/execution surface for nodes/edges/props, and turn `cpg/build_*` into compatibility wrappers

I can then produce a PR-by-PR checklist in the same style as before (commit slices, file checklist, exact signatures), but the architecture direction above is the core of what I’d target given your current code layout.



Below is a **Path 2 (cleanest end state)** refactor map + PR-by-PR checklist that’s aligned to your current `src/` tree and your stated goal:

* **`relspec` becomes the only compilation/execution surface** for producing **CPG nodes/edges/props** (and, by extension, the only “build API” you want LLM agents to learn).
* **`cpg/build_*` becomes compatibility wrappers** (same signatures, same return types), issuing **DeprecationWarnings** and delegating to `relspec.cpg.*`.
* The “rules-driven” nature remains: spec tables + rule tables are still the declarative source of truth, but **the public surface and orchestration** moves under `relspec`.

I’m going to assume you want this to be **reviewable + low-risk**, so the PRs are structured as **move/copy → rewire imports → flip wrappers**, with tight slices and tests after each.

---

## Target end state

### What callers should do (canonical)

```python
from relspec.cpg import (
    build_cpg_nodes,
    build_cpg_edges,
    build_cpg_props,
    CpgRegistry,
)
```

### What legacy callers can still do (compat)

```python
from cpg.build_nodes import build_cpg_nodes      # works, warns
from cpg.build_edges import build_cpg_edges      # works, warns
from cpg.build_props import build_cpg_props      # works, warns
```

### Architectural invariant at the end

* `relspec/cpg/*` contains **all real implementation** of:

  * spec decoding
  * plan compilation
  * execution + finalize
  * edge/node/prop emission
* `cpg/build_nodes.py`, `cpg/build_edges.py`, `cpg/build_props.py` contain **no real logic**, only wrappers.

---

## Deprecation policy you should implement immediately

You want deprecations to be **mechanically enforced**, but not noisy on import.

Use **call-time** warnings, not import-time warnings.

```python
# in cpg/build_nodes.py
import warnings
from relspec.cpg.build_nodes import build_cpg_nodes as _impl_build_cpg_nodes

def build_cpg_nodes(*, ctx, inputs=None, options=None, node_spec_table=None, registry=None):
    warnings.warn(
        "cpg.build_nodes.build_cpg_nodes is deprecated; use relspec.cpg.build_nodes.build_cpg_nodes",
        DeprecationWarning,
        stacklevel=2,
    )
    return _impl_build_cpg_nodes(
        ctx=ctx,
        inputs=inputs,
        options=options,
        node_spec_table=node_spec_table,
        registry=registry,
    )
```

Do the same for:

* `build_cpg_nodes_raw`
* `build_cpg_edges`, `build_cpg_edges_raw`
* `build_cpg_props`, `build_cpg_props_raw`

---

# PR-by-PR checklist

## PR21 — Create `relspec.cpg` public surface (no behavior change)

**Goal:** Add the new import surface and confirm it can be imported *without* pulling in `cpg/build_*` yet.

### Commits / review slices

1. **Add package skeleton + re-export API**
2. **Add a tiny import test**

### File checklist

* **NEW** `src/relspec/cpg/__init__.py`

  * Re-export the canonical build functions + key types (initially you can import from existing `cpg.*` to keep this PR no-op).
* **NEW** `tests/unit/test_relspec_cpg_imports.py`

  * Simple: `import relspec.cpg` and check attributes exist.

### Notes

* This PR is intentionally boring. It establishes the new public “learnable” surface for agents.

---

## PR22 — Move “CPG build core primitives” into `relspec.cpg` (constants + plan helpers)

**Goal:** Start migrating the shared implementation pieces that `build_*` modules depend on.

### Commits / review slices

1. Copy core modules into `relspec/cpg/*` (no rewires)
2. Rewire internal imports in copied modules (prefer relative imports within `relspec.cpg`)
3. Keep `cpg.*` versions as-is for now

### File checklist

Create copies (or move, if you prefer git mv + wrapper immediately):

* **NEW** `src/relspec/cpg/constants.py` (from `src/cpg/constants.py`)
* **NEW** `src/relspec/cpg/plan_specs.py` (from `src/cpg/plan_specs.py`)
* **NEW** `src/relspec/cpg/catalog.py` (from `src/cpg/catalog.py`)

**Rewire imports inside these new files** from `cpg.*` → `relspec.cpg.*` where applicable.

### Deprecations

None yet.

### Acceptance tests

* `tests/unit/test_plan_coverage.py`
* `tests/e2e/test_full_pipeline_repo.py`

---

## PR23 — Move spec codecs + spec tables into `relspec.cpg` (the declarative spine)

**Goal:** Make **node/edge/prop spec compilation** live under relspec.

This is the crux for “rules-driven graph build”: you want the decoding + canonical table schema to live in relspec.

### Commits / review slices

1. Move/copy spec model + spec-table codecs
2. Update relspec rule schema import (EDGE_EMIT_STRUCT dependency)
3. Add a roundtrip test for spec table decode

### File checklist

* **NEW** `src/relspec/cpg/specs.py` (from `src/cpg/specs.py`)
* **NEW** `src/relspec/cpg/spec_tables.py` (from `src/cpg/spec_tables.py`)
* **NEW** `src/relspec/cpg/edge_specs.py` (from `src/cpg/edge_specs.py`)

Then fix the cross-module dependency you currently have:

* **MOD** `src/relspec/rules/spec_tables.py`

  * Change:

    * `from cpg.spec_tables import EDGE_EMIT_STRUCT`
  * To:

    * `from relspec.cpg.spec_tables import EDGE_EMIT_STRUCT`
  * (In PR27 you’ll add a compat re-export from `cpg.spec_tables`.)

### Deprecations

None yet.

### Acceptance tests

* `tests/integration/test_rule_semantics.py`
* `tests/unit/test_plan_coverage.py`

---

## PR24 — Move CPG registry + schemas to `relspec.cpg` (CPG dataset ownership)

**Goal:** The canonical dataset specs + schema registry for CPG tables should be owned by `relspec`.

### Commits / review slices

1. Copy/move schemas + registry facade
2. Update incremental modules to import from new location (optional in this PR)
3. Keep `cpg.schemas` intact for now

### File checklist

* **NEW** `src/relspec/cpg/registry.py` (from `src/cpg/registry.py`)
* **NEW** `src/relspec/cpg/schemas.py` (from `src/cpg/schemas.py`)

Optionally (if you want CPG dataset spec rows to be “relspec-owned” too):

* **NEW** `src/relspec/cpg/registry_rows.py` (from `src/cpg/registry_rows.py`)
* **NEW** `src/relspec/cpg/registry_specs.py` (from `src/cpg/registry_specs.py`)
* **NEW** `src/relspec/cpg/registry_builders.py` (from `src/cpg/registry_builders.py`)
* plus whatever `registry_builders.py` needs (`registry_fields.py`, `registry_bundles.py`, etc.)

If you *don’t* want that much movement yet: keep those in `cpg/` and have `relspec.cpg.schemas` import them.

### Deprecations

None yet.

### Acceptance tests

* `tests/integration/test_partition_upsert.py` (schemas are used for upserts)
* `tests/e2e/test_incremental_parity.py`

---

## PR25 — Move node build to `relspec.cpg` and make `cpg/build_nodes.py` a wrapper

**Goal:** First “real” public surface swap: nodes.

### Commits / review slices

1. Copy/move implementation: `emit_nodes` + `build_nodes`
2. Update internal imports to `relspec.cpg.*`
3. Replace `cpg/build_nodes.py` with wrapper
4. Update `relspec.cpg.__init__` exports

### File checklist

* **NEW** `src/relspec/cpg/emit_nodes.py` (from `src/cpg/emit_nodes.py`)

* **NEW** `src/relspec/cpg/build_nodes.py` (from `src/cpg/build_nodes.py`)

* **MOD** `src/cpg/build_nodes.py` becomes wrapper

  * Must re-export:

    * `NodeBuildOptions`
    * `NodeInputTables`
    * `build_cpg_nodes`, `build_cpg_nodes_raw`
  * Must issue DeprecationWarning on call

* **MOD** `src/cpg/__init__.py`

  * `_EXPORT_MAP` entries for node build should point to:

    * `relspec.cpg.build_nodes`

### “Wrapper exactness” requirement

Keep the function signatures byte-for-byte identical so no downstream breakages:

* `build_cpg_nodes(ctx, inputs=None, options=None, node_spec_table=None, registry=None)`

### Acceptance tests

* `tests/e2e/test_full_pipeline_repo.py`
* Ensure incremental still runs (nodes_upsert path)

---

## PR26 — Move props build to `relspec.cpg` and make `cpg/build_props.py` a wrapper

Same structure as PR25.

### File checklist

* **NEW** `src/relspec/cpg/emit_props.py` (from `src/cpg/emit_props.py`)

* **NEW** `src/relspec/cpg/build_props.py` (from `src/cpg/build_props.py`)

* **MOD** `src/cpg/build_props.py` → wrapper

* **MOD** `src/cpg/__init__.py` export map updated

### Acceptance tests

* `tests/e2e/test_full_pipeline_repo.py`
* `tests/e2e/test_incremental_parity.py`

---

## PR27 — Move edges build to `relspec.cpg` and make `cpg/build_edges.py` a wrapper

Edges is the biggest one because it crosses:

* relationship rule outputs
* Ibis bridge execution
* telemetry/coverage

### Commits / review slices

1. Copy/move emitters + builder
2. Update internal imports to `relspec.cpg.*`
3. Replace `cpg/build_edges.py` with wrapper
4. Update `cpg.__init__` export map

### File checklist

* **NEW** `src/relspec/cpg/emit_edges.py` (from `src/cpg/emit_edges.py`)

* **NEW** `src/relspec/cpg/emit_edges_ibis.py` (from `src/cpg/emit_edges_ibis.py`)

* **NEW** `src/relspec/cpg/build_edges.py` (from `src/cpg/build_edges.py`)

* **MOD** `src/cpg/build_edges.py` → wrapper

* **MOD** `src/cpg/__init__.py` export map updated

### Deprecations introduced here

* `cpg.build_edges.*` (warnings)
* (do **not** deprecate `cpg.emit_edges*` yet unless you move them too; deprecating too early is noisy)

### Acceptance tests

* `tests/integration/test_datafusion_parity.py`
* `tests/e2e/test_full_pipeline_repo.py`

---

## PR28 — Cleanest end-state edge build: stop recompiling relationship rules inside edge build

This is the **architectural “win” PR** for Path 2, because right now:

* Relationship outputs are already compiled/executed in `hamilton_pipeline/modules/cpg_build.py` (`compiled_relationship_outputs` + `relationship_tables`),
* but `cpg/build_edges.py` still calls `compile_relation_plans*()` and essentially re-enters relationship rule compilation again.

### Goal

Make `relspec.cpg.build_edges` consume **executed relationship outputs** (or their Ibis plans) rather than recompiling relationship rules.

### Minimal diff approach (recommended)

Add a new internal constructor in edge build:

```python
# relspec/cpg/build_edges.py
def _relation_bundle_from_relationship_outputs(
    rel_outputs: Mapping[str, TableLike | DatasetSource],
    *,
    ctx: ExecutionContext,
    use_ibis: bool,
    backend: BaseBackend | None,
) -> RelationPlanBundle:
    ...
```

Then in `_edge_relation_context`:

* If `inputs.relationship_outputs` is provided:

  * build bundle directly from those outputs
  * **skip** `compile_relation_plans*()`
* Else:

  * fall back to legacy compilation (keeps old behavior working)

### Pipeline wiring change (so you actually benefit)

In `hamilton_pipeline/modules/cpg_build.py`:

* Change `cpg_edge_inputs` to pass the full mapping from `relationship_tables` (not just `RelationshipOutputTables.as_dict()` which only includes 5 keys).

Concretely, update:

```python
# today
relationship_outputs=relationship_output_tables.as_dict()
```

to:

```python
# end state
relationship_outputs=relationship_tables   # the dict returned by relationship_tables()
```

You may need a new Hamilton node output to expose the dict if current wiring doesn’t allow it cleanly.

### Deprecate

Once this is merged and you confirm parity:

* Deprecate `src/cpg/relationship_plans.py` and `src/relspec/cpg/relationship_plans.py` (if you copied it)

  * or demote it to “dev-only / benchmarking” utilities.

### Acceptance tests

* `tests/e2e/test_incremental_parity.py` (must be identical graph outputs)
* Add a perf regression test if you can (even a coarse “should not compile relationships twice” via telemetry counters)

---

# File-by-file refactor map (Path 2 end state)

This is the “where everything lands” map, focused on **nodes/edges/props build execution**.

## `src/relspec/` additions

**NEW package:**

* `src/relspec/cpg/__init__.py` — canonical public surface

**Implementation moved from `cpg/`:**

* `src/relspec/cpg/build_nodes.py`  ← from `src/cpg/build_nodes.py`

* `src/relspec/cpg/emit_nodes.py`   ← from `src/cpg/emit_nodes.py`

* `src/relspec/cpg/build_props.py`  ← from `src/cpg/build_props.py`

* `src/relspec/cpg/emit_props.py`   ← from `src/cpg/emit_props.py`

* `src/relspec/cpg/build_edges.py`  ← from `src/cpg/build_edges.py`

* `src/relspec/cpg/emit_edges.py`   ← from `src/cpg/emit_edges.py`

* `src/relspec/cpg/emit_edges_ibis.py` ← from `src/cpg/emit_edges_ibis.py`

**Support modules moved under relspec (recommended):**

* `src/relspec/cpg/specs.py`        ← from `src/cpg/specs.py`
* `src/relspec/cpg/spec_tables.py`  ← from `src/cpg/spec_tables.py`
* `src/relspec/cpg/edge_specs.py`   ← from `src/cpg/edge_specs.py`
* `src/relspec/cpg/catalog.py`      ← from `src/cpg/catalog.py`
* `src/relspec/cpg/plan_specs.py`   ← from `src/cpg/plan_specs.py`
* `src/relspec/cpg/constants.py`    ← from `src/cpg/constants.py`
* `src/relspec/cpg/registry.py`     ← from `src/cpg/registry.py`
* `src/relspec/cpg/schemas.py`      ← from `src/cpg/schemas.py`

**Relspec internal rewires:**

* `src/relspec/rules/spec_tables.py`

  * import `EDGE_EMIT_STRUCT` from `relspec.cpg.spec_tables` (not `cpg.spec_tables`)

## `src/cpg/` becomes compatibility wrappers

**These become wrappers (hard requirement):**

* `src/cpg/build_nodes.py` → wrapper to `relspec.cpg.build_nodes`
* `src/cpg/build_edges.py` → wrapper to `relspec.cpg.build_edges`
* `src/cpg/build_props.py` → wrapper to `relspec.cpg.build_props`

**These become wrappers too (recommended for consistency):**

* `src/cpg/specs.py` → wrapper to `relspec.cpg.specs`
* `src/cpg/spec_tables.py` → wrapper to `relspec.cpg.spec_tables`
* `src/cpg/edge_specs.py` → wrapper to `relspec.cpg.edge_specs`
* `src/cpg/emit_nodes.py` / `emit_edges.py` / `emit_edges_ibis.py` / `emit_props.py` → wrappers
* `src/cpg/catalog.py` / `plan_specs.py` / `constants.py` → wrappers
* `src/cpg/registry.py` / `schemas.py` → wrappers

**`src/cpg/__init__.py`**

* Update `_EXPORT_MAP` to point *directly* at `relspec.cpg.*` for:

  * build functions
  * schemas
  * registry
  * spec tables

This makes `import cpg; cpg.build_cpg_edges(...)` transparently call relspec.

## Pipeline wiring changes (to actually realize the clean end-state)

* `src/hamilton_pipeline/modules/cpg_build.py`

  * **Switch imports** to `from relspec.cpg...` (after wrappers exist)
  * **PR28:** feed full `relationship_tables` mapping into edge build to avoid recompiling relationships inside edge build.

* `src/hamilton_pipeline/pipeline_types.py`

  * Optionally replace `RelationshipOutputTables` (fixed 5-table bundle) with:

    * `RelationshipOutputs = Mapping[str, TableLike]`
  * Or keep the dataclass for “impact closure” and add a second mapping for edge emission.

---

## The “why this is Path 2 cleanest”

Once you finish PR28, the architecture becomes:

* **relspec rules compile once**
* relationship outputs are either:

  * executed once and reused by edge emission, or
  * kept as Ibis plans and only executed as part of final edge query (even better, if you push it)
* nodes/props/edges are all built through one obvious surface: `relspec.cpg.*`

This is exactly what makes it “ubiquitous for LLM agents”: one stable API and one conceptual model.

---

If you want, the next refinement after this Path 2 migration is a **single “graph product build” entrypoint**:

```python
from relspec.cpg import build_graph

artifacts = build_graph(ctx=ctx, inputs=..., options=..., incremental=...)
# returns nodes/edges/props plus rule_exec_events + telemetry tables
```

…but the PR plan above gets you the clean end state you asked for without forcing a mega-PR rewrite.

Below is the “next refinement” after Path 2: a **single, canonical “graph product build” entrypoint** that becomes the only API an LLM agent (or any caller) needs in order to build the CPG product artifacts (full or incremental), while the Hamilton DAG + relspec execution remain implementation details.

This is intentionally structured as **reviewable PR slices** with **file-by-file diffs** and **exact signatures** you can drop in.

---

## Target end-state

### What callers do (and only this)

```python
from graph import GraphProductBuildRequest, build_graph_product
from incremental.types import IncrementalConfig

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root=".",
        output_dir="build/products/cpg",         # stable base for incremental upserts
        work_dir="build/work",                   # optional
        incremental_config=IncrementalConfig(enabled=True),
        incremental_impact_strategy="symbol_closure",
    )
)

print("nodes:", result.cpg_nodes.paths.data)
print("edges:", result.cpg_edges.paths.data)
print("props:", result.cpg_props.paths.data)
print("manifest:", result.manifest_path)
print("bundle:", result.run_bundle_dir)
```

### What you deprecate (public surface)

* “Public usage” of `hamilton_pipeline.execute_pipeline(...)` (keep it, but treat it as **internal / low-level**).
* “Magic output node name” usage (`write_cpg_nodes_parquet`, etc.) in downstream code.
* Over time: direct calling of `cpg.build_*` as a primary API (in Path 2 they become compatibility wrappers; this entrypoint is the replacement).

---

## Why this refinement matters architecturally

Even after Path 2 (relspec-only compile/execution for nodes/edges/props), your *user-facing* execution still looks like:

* “know Hamilton”
* “know node names”
* “know what outputs to request”
* “interpret a dict of heterogeneous metadata”

That’s not agent-friendly.

This refinement makes your pipeline usable as a tool:

* **One function** to build a versioned product (`product="cpg"` today; extensible later).
* **Typed request/response**: stable fields with paths + counts.
* **Explicit knobs**: incremental, strategy, diagnostics, manifest/bundle.
* **No graph-internal naming leakage**: you hide Hamilton output node strings.

---

## PR21: Add the graph product build entrypoint (wrapper + typed result)

### PR21 goals

* Add `graph.build_graph_product()` as canonical entrypoint.
* It calls the existing Hamilton pipeline under the hood (so minimal risk).
* It returns a strongly typed result object (paths + row counts + optional artifacts).
* It selects Hamilton outputs automatically based on request flags.

### PR21.1 — New package: `src/graph/`

#### Files added

1. **`src/graph/__init__.py`** (new)

```python
"""Graph product build API (public surface)."""

from __future__ import annotations

from graph.product_build import (
    GraphProductBuildRequest,
    GraphProductBuildResult,
    build_graph_product,
)

__all__ = [
    "GraphProductBuildRequest",
    "GraphProductBuildResult",
    "build_graph_product",
]
```

2. **`src/graph/product_build.py`** (new)

This is the main implementation + types.

```python
"""Canonical graph product build entrypoints.

This is the public API that LLM agents should use. It intentionally hides Hamilton output node
names and returns a typed result with stable fields.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, cast

from arrowdsl.core.context import ExecutionContext
from config import AdapterMode
from core_types import JsonDict, JsonValue, PathLike, ensure_path
from hamilton_pipeline import PipelineExecutionOptions, execute_pipeline
from hamilton_pipeline.execution import ImpactStrategy
from hamilton_pipeline.pipeline_types import ScipIdentityOverrides, ScipIndexConfig
from incremental.types import IncrementalConfig

GraphProduct = Literal["cpg"]


# ----------------------------
# Typed outputs
# ----------------------------

@dataclass(frozen=True)
class FinalizeParquetPaths:
    """Paths returned by write_finalize_result_parquet."""
    data: Path
    errors: Path
    stats: Path
    alignment: Path


@dataclass(frozen=True)
class FinalizeParquetReport:
    """Finalize parquet output + counts."""
    paths: FinalizeParquetPaths
    rows: int
    error_rows: int


@dataclass(frozen=True)
class TableParquetReport:
    """Single table parquet output + count."""
    path: Path
    rows: int


@dataclass(frozen=True)
class GraphProductBuildResult:
    """Stable result for a graph product build."""
    product: GraphProduct
    repo_root: Path
    output_dir: Path

    cpg_nodes: FinalizeParquetReport
    cpg_edges: FinalizeParquetReport
    cpg_props: FinalizeParquetReport

    cpg_nodes_quality: TableParquetReport | None = None
    cpg_props_quality: TableParquetReport | None = None

    extract_error_artifacts: JsonDict | None = None
    manifest_path: Path | None = None
    run_bundle_dir: Path | None = None

    # Always return the raw pipeline output mapping for debugging/inspection.
    pipeline_outputs: Mapping[str, JsonDict | None] = field(default_factory=dict)


# ----------------------------
# Request model
# ----------------------------

@dataclass(frozen=True)
class GraphProductBuildRequest:
    repo_root: PathLike
    product: GraphProduct = "cpg"

    output_dir: PathLike | None = None
    work_dir: PathLike | None = None

    scip_index_config: ScipIndexConfig | None = None
    scip_identity_overrides: ScipIdentityOverrides | None = None

    adapter_mode: AdapterMode | None = None
    ctx: ExecutionContext | None = None

    incremental_config: IncrementalConfig | None = None
    incremental_impact_strategy: ImpactStrategy | None = None

    include_quality: bool = True
    include_extract_errors: bool = True
    include_manifest: bool = True
    include_run_bundle: bool = True

    # advanced escape hatches (keep, but most callers won’t touch)
    config: Mapping[str, JsonValue] = field(default_factory=dict)
    overrides: Mapping[str, object] | None = None


def build_graph_product(request: GraphProductBuildRequest) -> GraphProductBuildResult:
    """Build the requested graph product and return stable typed outputs."""
    repo_root_path = ensure_path(request.repo_root).resolve()

    outputs = _outputs_for_request(request)
    options = PipelineExecutionOptions(
        output_dir=request.output_dir,
        work_dir=request.work_dir,
        scip_index_config=request.scip_index_config,
        scip_identity_overrides=request.scip_identity_overrides,
        adapter_mode=request.adapter_mode,
        ctx=request.ctx,
        incremental_config=request.incremental_config,
        incremental_impact_strategy=request.incremental_impact_strategy,
        outputs=outputs,
        config=request.config,
        overrides=request.overrides,
    )

    raw = execute_pipeline(repo_root=repo_root_path, options=options)
    return _parse_result(
        request=request,
        repo_root=repo_root_path,
        pipeline_outputs=raw,
    )


# ----------------------------
# Internals: outputs selection + parsing
# ----------------------------

def _outputs_for_request(request: GraphProductBuildRequest) -> Sequence[str]:
    # Always produce the core product artifacts.
    outputs: list[str] = [
        "write_cpg_nodes_parquet",
        "write_cpg_edges_parquet",
        "write_cpg_props_parquet",
    ]
    if request.include_quality:
        outputs.extend(
            [
                "write_cpg_nodes_quality_parquet",
                "write_cpg_props_quality_parquet",
            ]
        )
    if request.include_extract_errors:
        outputs.append("write_extract_error_artifacts_parquet")
    if request.include_manifest:
        outputs.append("write_run_manifest_json")
    if request.include_run_bundle:
        outputs.append("write_run_bundle_dir")
    return outputs


def _require(outputs: Mapping[str, JsonDict | None], key: str) -> JsonDict:
    value = outputs.get(key)
    if value is None:
        raise ValueError(f"Missing required pipeline output {key!r}.")
    return cast("JsonDict", value)


def _optional(outputs: Mapping[str, JsonDict | None], key: str) -> JsonDict | None:
    return cast("JsonDict | None", outputs.get(key))


def _parse_finalize(report: JsonDict) -> FinalizeParquetReport:
    paths = cast("dict[str, str]", report.get("paths") or {})
    return FinalizeParquetReport(
        paths=FinalizeParquetPaths(
            data=Path(paths["data"]),
            errors=Path(paths["errors"]),
            stats=Path(paths["stats"]),
            alignment=Path(paths["alignment"]),
        ),
        rows=int(report.get("rows") or 0),
        error_rows=int(report.get("error_rows") or 0),
    )


def _parse_table(report: JsonDict) -> TableParquetReport:
    return TableParquetReport(
        path=Path(cast("str", report["path"])),
        rows=int(report.get("rows") or 0),
    )


def _parse_result(
    *,
    request: GraphProductBuildRequest,
    repo_root: Path,
    pipeline_outputs: Mapping[str, JsonDict | None],
) -> GraphProductBuildResult:
    # Compute output_dir as the directory containing the cpg_nodes data path.
    # (We do this because PipelineExecutionOptions resolves default output_dir internally.)
    nodes_report = _parse_finalize(_require(pipeline_outputs, "write_cpg_nodes_parquet"))
    output_dir = nodes_report.paths.data.parent

    edges_report = _parse_finalize(_require(pipeline_outputs, "write_cpg_edges_parquet"))
    props_report = _parse_finalize(_require(pipeline_outputs, "write_cpg_props_parquet"))

    nodes_quality = None
    if request.include_quality:
        q = _optional(pipeline_outputs, "write_cpg_nodes_quality_parquet")
        if q is not None:
            nodes_quality = _parse_table(q)

    props_quality = None
    if request.include_quality:
        q = _optional(pipeline_outputs, "write_cpg_props_quality_parquet")
        if q is not None:
            props_quality = _parse_table(q)

    manifest_path = None
    if request.include_manifest:
        m = _optional(pipeline_outputs, "write_run_manifest_json")
        if m is not None and m.get("path"):
            manifest_path = Path(cast("str", m["path"]))

    run_bundle_dir = None
    if request.include_run_bundle:
        b = _optional(pipeline_outputs, "write_run_bundle_dir")
        if b is not None and b.get("bundle_dir"):
            run_bundle_dir = Path(cast("str", b["bundle_dir"]))

    extract_errors = None
    if request.include_extract_errors:
        extract_errors = _optional(pipeline_outputs, "write_extract_error_artifacts_parquet")

    return GraphProductBuildResult(
        product=request.product,
        repo_root=repo_root,
        output_dir=output_dir,
        cpg_nodes=nodes_report,
        cpg_edges=edges_report,
        cpg_props=props_report,
        cpg_nodes_quality=nodes_quality,
        cpg_props_quality=props_quality,
        extract_error_artifacts=extract_errors,
        manifest_path=manifest_path,
        run_bundle_dir=run_bundle_dir,
        pipeline_outputs=pipeline_outputs,
    )


__all__ = [
    "FinalizeParquetPaths",
    "FinalizeParquetReport",
    "GraphProductBuildRequest",
    "GraphProductBuildResult",
    "TableParquetReport",
    "build_graph_product",
]
```

#### Why this shape is correct for your codebase

* Uses **existing** `PipelineExecutionOptions` and `execute_pipeline` (so it respects your incremental, multi-threading, and execution policy settings).
* Doesn’t duplicate “how the pipeline works”; it just standardizes the **external API**.
* Makes the output schema stable and agent-friendly.

---

### PR21.2 — Tests for the new entrypoint (e2e)

Add:

**`tests/e2e/test_graph_product_entrypoint.py`** (new)

```python
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from graph import GraphProductBuildRequest, build_graph_product
from hamilton_pipeline.pipeline_types import ScipIndexConfig


@pytest.mark.e2e
@pytest.mark.serial
def test_graph_product_build_entrypoint() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    output_dir = repo_root / "build" / "e2e_graph_product"
    if output_dir.exists():
        shutil.rmtree(output_dir)

    result = build_graph_product(
        GraphProductBuildRequest(
            repo_root=repo_root,
            output_dir=output_dir,
            scip_index_config=ScipIndexConfig(output_dir="build/scip"),
        )
    )

    assert result.cpg_nodes.paths.data.exists()
    assert result.cpg_edges.paths.data.exists()
    assert result.cpg_props.paths.data.exists()
    assert (output_dir / "cpg_nodes.parquet").exists()
    assert (output_dir / "cpg_edges.parquet").exists()
    assert (output_dir / "cpg_props.parquet").exists()
```

---

### PR21.3 — Optional: a tiny doc entry in README

Add a short “Recommended API” section that uses `graph.build_graph_product`.

---

## PR22: Convert existing CLI + compatibility wrappers to the new entrypoint

### PR22 goals

* Make the CLI call the new entrypoint (without breaking existing flags).
* Keep `scripts/run_full_pipeline.py` but treat it as a compatibility wrapper.
* Start deprecating “pipeline-level” usage in docs.

### PR22.1 — Update CLI script

Modify **`scripts/run_full_pipeline.py`**:

* Replace direct `execute_pipeline(...)` call with `build_graph_product(...)`.
* Keep flags identical.

Sketch:

```python
from graph import GraphProductBuildRequest, build_graph_product
# keep ScipIndexConfig, AdapterMode, etc.

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root=args.repo_root,
        output_dir=args.output_dir,
        work_dir=args.work_dir,
        scip_index_config=scip_config,
        adapter_mode=adapter_mode,
        incremental_impact_strategy=args.incremental_impact_strategy,
        include_quality=True,
        include_manifest=True,
        include_run_bundle=True,
        include_extract_errors=True,
    )
)
logger.info("Build complete. Output dir=%s bundle=%s", result.output_dir, result.run_bundle_dir)
```

This makes the CLI automatically benefit from any improvements to the product build API.

### PR22.2 — Deprecation messaging (lightweight, non-breaking)

I recommend **not** hard-deprecating `execute_pipeline()` yet (it’s heavily used in tests and internal hooks), but you can:

* Update docstrings and comments to say:

  * “Use `graph.build_graph_product` unless you are composing Hamilton directly.”
* Optionally add a **single** DeprecationWarning in `scripts/run_full_pipeline.py` help text instead of runtime warnings.

---

## “Diff map” exactly as I’d structure it in GitHub

### PR21: Graph product build entrypoint (core)

**Commit 1 — Add graph package + public API**

* ✅ `src/graph/__init__.py` (new)
* ✅ `src/graph/product_build.py` (new)

**Commit 2 — Tests**

* ✅ `tests/e2e/test_graph_product_entrypoint.py` (new)

**Commit 3 — Docs (optional, but small)**

* ✅ `README.md` (small addition: recommended API)

### PR22: Migrate CLI and start compatibility posture

**Commit 1 — CLI now uses product entrypoint**

* 🔁 `scripts/run_full_pipeline.py` (modified only)

**Commit 2 — Optional docs note**

* 🔁 `README.md` (explain that execute_pipeline is “advanced/low-level”)

---

## What functionality to deprecate (explicit list)

### Deprecate as “primary usage” (keep as internal/compat)

1. **Direct callers using `execute_pipeline(... outputs=[...])`**

* Replace with `build_graph_product(...)`.
* You’re deprecating the “knowledge of output node strings”, not necessarily the function itself.

2. **Downstream callers hard-coding the file layout**

* Replace with `GraphProductBuildResult` paths.

3. **Any public docs/examples that call into `hamilton_pipeline`**

* Replace with the new entrypoint.

### Keep (do not deprecate yet)

* Hamilton pipeline internals and output node functions
* Any internal DAG composition workflows (still valuable for development/testing)

---

## A small but important “best-in-class” upgrade: product versioning

Do this in PR21 (or PR23 if you want it separated):

Add to `GraphProductBuildResult`:

* `product_version: str` (e.g., `"cpg_ultimate_v1"`)
* `engine_versions: dict[str, str]` (pyarrow, datafusion, ibis)
* `run_id: str` (copied from run bundle name if present)

This makes the build result safe to cache and reason about for agents.

---

If you want, the very next refinement after *this* is to let `GraphProductBuildRequest` support a second execution mode:

* `mode="materialize"` (default): writes parquet outputs (as above)
* `mode="in_memory"`: returns `pa.Table` handles for nodes/edges/props **without** writing parquet (useful for interactive agents doing quick checks)

…but I’d implement the entrypoint wrapper + typed paths first, because it immediately improves usability without touching core execution.
