Here are the **highest‑leverage “next actions” beyond PR‑5 … PR‑10** to push the codebase toward the end‑state you described:

* **Rules are the only “program”** (relationship compilation is a compiler pipeline; everything else is infrastructure).
* Execution stays **inside DataFusion / Acero / PyArrow compute** (almost no Python loops/scalars).
* The system becomes **more modular + robust** (hard boundaries, contracts, determinism).
* You get **much richer diagnostics** (run bundles you can diff, and “why did this edge exist?” answers).

I’m anchoring the recommendations to the “relational compiler” mental model and determinism/ambiguity requirements in your guide. 

---

## 1) Add “RuleSpec is the only way” governance (hard guardrails, not conventions)

You already have a strong rule model + compiler surface. The missing piece (in most systems) is **enforcement**: “no new logic lands unless it is expressed as a rule (or a declared kernel capability).”

### Action

Introduce a **Strict Columnar Execution Policy** that:

1. forbids Python row loops / scalarization on “large tables”
2. forbids “external / table” execution modes unless explicitly whitelisted
3. requires every relationship output to carry the ambiguity + explainability columns when the rule kind can generate multiple candidates (confidence/score/ambiguity_group_id/origin/rule_name metadata). 

### Code shape

```python
# relspec/policies.py (or relspec/engine_policy.py)
from dataclasses import dataclass
from typing import FrozenSet

@dataclass(frozen=True)
class StrictColumnarPolicy:
    forbid_execution_modes: FrozenSet[str] = frozenset({"external", "table"})
    allowlisted_external_rules: FrozenSet[str] = frozenset()

    forbid_scalarization: bool = True  # blocks to_pylist/as_py on “non-diagnostics”
    max_rows_for_scalarization: int = 10_000  # diagnostics tables can be small

def assert_policy(rule, policy: StrictColumnarPolicy) -> None:
    if rule.execution_mode in policy.forbid_execution_modes and rule.name not in policy.allowlisted_external_rules:
        raise ValueError(f"Rule {rule.name} must compile to DataFusion/Acero/Arrow kernels (mode={rule.execution_mode}).")
```

### Deprecate

* Any “shortcut” code paths that materialize and then do Python list/dict operations for relationship compilation.
* Any “temporary” rules that exist only as Python functions without a spec row/table entry.

---

## 2) Make backend lane selection capability‑driven and *auditable*

Your kernel bridge direction is correct; the next step is to make lane selection:

* **repeatable**
* **visible in diagnostics**
* **fail‑closed** when “strict mode” is enabled

This aligns with the requirement that relationship compilation treats filters/projections as symbolic expression trees (not eager Python computation). 

### Action

Add a single “lane router” that decides, per rule:

* DataFusion plan lane
* Acero plan lane
* Arrow compute lane (fallback)

…and emits a diagnostic record *even when things succeed*.

### Code shape

```python
# relspec/lane_router.py
from dataclasses import dataclass
from enum import Enum

class ExecLane(str, Enum):
    DATAFUSION = "datafusion"
    ACERO = "acero"
    ARROW = "arrow"

@dataclass(frozen=True)
class LaneDecision:
    lane: ExecLane
    reason: str
    requires_ordering: bool = False

def choose_lane(rule, capabilities) -> LaneDecision:
    # pseudo-logic
    if capabilities.supports_datafusion(rule):
        return LaneDecision(ExecLane.DATAFUSION, "supported_by_datafusion")
    if capabilities.supports_acero(rule):
        return LaneDecision(ExecLane.ACERO, "supported_by_acero", requires_ordering=rule.kind in {"winner_select", "interval_align"})
    return LaneDecision(ExecLane.ARROW, "fallback_arrow_compute", requires_ordering=True)
```

### Diagnostics requirement

Persist the lane decision per rule. (Your diagnostics layer already has a schema for this style of reporting; extend it to always include the lane decision.)

---

## 3) Treat determinism as a first‑class contract, not a best effort

Your guide calls out the core hazard: **Acero does not guarantee row order** (hash join has no predictable output order), therefore “winner selection must always establish explicit sort keys, then keep‑first/top‑1 deterministically.” 

### Action

Make “sort‑before‑winner” an invariant:

* compiler refuses to compile a winner_select unless explicit ordering keys exist
* in “canonical determinism tier,” always canonical-sort at defined boundaries

Also: treat `to_table()` as an explicit pipeline breaker (“accumulate everything”), and make it a boundary you only use intentionally (finalize/caching), not casually. 

### Code shape (engine‑agnostic policy, backend‑specific implementation)

```python
# relspec/winner_policy.py
from dataclasses import dataclass
from typing import Tuple

@dataclass(frozen=True)
class WinnerPolicy:
    group_keys: Tuple[str, ...]
    order_by: Tuple[str, ...]     # ("score DESC", "occ_bstart ASC", "symbol ASC")
    keep: str = "first"           # or "top1"

def assert_winner_policy(policy: WinnerPolicy) -> None:
    if not policy.group_keys or not policy.order_by:
        raise ValueError("Winner selection requires explicit group_keys + order_by for determinism.")
```

### Deprecate

* Any “implicit keep-first” logic that relies on engine output order.
* Any stable ID computation that depends on row order (even indirectly).

---

## 4) Unify schema metadata as the “control plane” for contracts + ordering + evolution

You’ll get a big robustness win by leaning harder on **Arrow schema metadata** as the portable contract substrate:

* contract name/version
* ordering keys
* provenance flags (“produced_by_rule”, “evidence_rank”)
* determinism tier

Arrow makes this ergonomic (`schema.with_metadata(...)`), and you can unify schemas across partitions/runs via `pa.unify_schemas(...)`.

### Action

Adopt a single helper that stamps metadata on every produced relationship dataset and on final edges/nodes.

### Code shape

```python
import pyarrow as pa

def stamp_contract(schema: pa.Schema, *, contract: str, version: str, ordering_keys: str | None) -> pa.Schema:
    md = dict(schema.metadata or {})
    md[b"contract"] = contract.encode("utf-8")
    md[b"contract_version"] = version.encode("utf-8")
    if ordering_keys:
        md[b"ordering_keys"] = ordering_keys.encode("utf-8")
    return schema.with_metadata(md)

def unify_partition_schemas(schemas: list[pa.Schema]) -> pa.Schema:
    # makes schema evolution explicit + consistent
    return pa.unify_schemas(schemas)
```

### Deprecate

* Ad hoc “schema drift” handling scattered around emit/build code.
* Any code that relies on positional column ordering without aligning to a known schema.

---

## 5) Make scanning + runtime profiles *single‑sourced* and always recorded

Your Acero guidance emphasizes centralizing scan policies and recording scan telemetry (“every scan uses a QuerySpec; record fragment-selection telemetry”). 
Also: prefer streaming surfaces and only materialize at explicit boundaries. 

### Action

Do two things everywhere (DataFusion + Acero):

1. **Single scan builder** (already trending in your `RuntimeProfile/ScanProfile` direction).
2. **Always persist scan telemetry** as a dataset artifact (Parquet) not just embedded JSON in a manifest.

### Deprecate

* Any direct dataset scan calls that bypass the centralized scan policy.
* Any “silent scan” that doesn’t record fragment/rowgroup telemetry.

---

## 6) Push optimizer inputs + caching from “nice-to-have” into your execution contract

DataFusion performance stability depends heavily on statistics/caching policies:

* `collect_statistics` feeds the cost model and optimizer decisions 
* caching (e.g., `DataFrame.cache()`) is high ROI when many rules reuse the same normalized inputs 

### Action

Make a “RulePack Execution Profile” include:

* a cache plan (“hot inputs” cached after projection+filter)
* a stats policy (“collect_statistics on, meta_fetch_concurrency tuned”)
* a memory/spill policy

…and record these settings into run artifacts (you already snapshot settings; extend to cache/stats *behavior*).

### Practical addition

Add a “hot inputs registry” per rule pack (you already have `_HOT_DATAFUSION_INPUTS` in your Hamilton layer). Promote that to a rule‑pack spec so it’s not hidden in pipeline code.

---

## 7) Add incremental invalidation + diagnostics nodes as first‑class outputs

Your guide explicitly calls out:

* extracting tree-sitter error nodes (`is_error/is_missing/has_error`) and storing their spans 
* optional incremental invalidation via `Tree.edit` + `changed_ranges`  (backed by tree-sitter APIs)

### Action

Two outputs to add (even if incremental mode is feature-flagged):

1. `ts_parse_errors` table (file_id/path + byte span + node kind + severity)
2. `ts_changed_ranges` table (run-to-run comparisons in watch mode)

These become:

* diagnostic edges (graph layer)
* join anchors for “what spans are unsafe/ambiguous?”
* a “recompute only impacted rules” mechanism

### Deprecate

* treating parse errors as log lines instead of graph/queryable artifacts.

---

## 8) Turn diagnostics into *queryable datasets*, not just JSON blobs

Right now you have a strong manifest concept. The next step for “richer diagnostic outputs” is: everything important becomes a **small Arrow/Parquet dataset** so you can query it with DataFusion.

### Action

Write these as Parquet under `run_dir/diagnostics/`:

* `rule_compile_artifacts`
  Columns: `rule_name`, `plan_signature`, `lane`, `sqlglot_ast_json`, `substrait_b64`, `required_contract`, `produced_schema_fingerprint`, `policy_flags`, …
* `rule_exec_metrics`
  Columns: `rule_name`, `rows_out`, `wall_time_ms`, `cpu_time_ms`, `peak_mem_est`, …
* `engine_explain` (DataFusion EXPLAIN rows per statement)
* `engine_fallbacks` (why something fell back from DF → Arrow lane)

This lines up with the “debugging and change evaluation: run bundles” intent in the guide’s table of contents and overall design goal. 

### Deprecate

* “diagnostics only exist in logs” patterns.
* “EXPLAIN only when debugging locally” patterns.

---

## 9) Add semantic regression gates: plan diffs + output invariants (fast, deterministic CI)

To keep the architecture from drifting back toward Python logic, you need gates that fail loudly when:

* a rule’s plan signature changes unexpectedly
* a rule begins emitting a new ambiguity shape (missing `ambiguity_group_id`, etc.)
* output row counts explode
* ordering/determinism contract violated

Your guide’s stance (“evaluate whether a code change preserves intended semantics”) is exactly what these gates enforce. 

### Action

Add CI jobs:

1. **Plan signature snapshots** per “golden repo fixture”
2. **Schema/contract invariants** per relationship output
3. **Determinism test**: run twice with different thread counts; compare stable IDs + top‑K outputs

### Deprecate

* relying on end-to-end tests that only fail deep in graph emission.

---

## 10) The “last mile” deprecations that usually unlock big simplification

If your PR‑5..10 work moved a lot of logic toward Ibis/DataFusion, the next simplification wave is usually:

### Deprecate / retire

* Any remaining “bespoke intermediate table shaping” utilities that duplicate compiler responsibilities.
* Any code that creates relationship outputs without going through:
  **(rule spec → compile → contract validate → execute → emit diagnostics)**.
* Any per-module “scan settings” (batch sizes, readahead, threading) that bypass the unified runtime profile.

### Replace with

* a single compiler/executor surface that always returns:

  1. output table(s)
  2. artifacts (plans, signatures, lane decisions)
  3. metrics (rows, timing, fallbacks)
  4. diagnostics tables written as Parquet

---

# If you want a concrete “do-this-next” ordering

If I had to pick the **next 5 actions** to start immediately (high ROI, low churn):

1. **StrictColumnarPolicy + CI lint** that blocks Python scalarization/row loops for relationship compilation.
2. **Make lane selection auditable** (persist per-rule lane decision + reason).
3. **Determinism hardening**: require explicit `order_by` for any winner_select / keep-first semantics. 
4. **Diagnostics as datasets** (rule_compile_artifacts + rule_exec_metrics written to Parquet).
5. **Tree-sitter error nodes table** + optional changed_ranges artifact for incremental mode. 

If you’d like, I can map these directly onto your current module layout (e.g., exactly where I’d put the policy object, the diagnostics table schemas, and the parquet writers in `obs/` + `relspec/`), but the above is the “additional beyond PR‑5..10” set that most strongly drives the end-state you described.


Below is the “make-it-real” follow‑up: concrete, end‑to‑end actionable changes mapped onto your *current* repo layout. I’m focusing on the three anchors you called out (policy object, diagnostics table schemas, parquet writers) **and** the smallest extra plumbing needed so these become *actually used* by relspec execution + run bundles.

---

## 1) Policy object: make rule execution + evidence/confidence/ambiguity centrally controlled (and configurable)

### What you have today

* **Global policy registries** embedded in helpers:

  * `src/relspec/policies.py` uses `_POLICY_REGISTRY = PolicyRegistry()`
  * `src/normalize/policies.py` does the same pattern
* A legacy/unused registry file:

  * `src/relspec/policy_registry.py` defines `CONFIDENCE_POLICIES` / `AMBIGUITY_POLICIES`, but (currently) nothing else imports it.

This is functional, but it’s *not*:

* centrally configurable per run,
* enforceable (“disallow ARROW_FALLBACK kernels”, “raise on missing evidence cols/types”),
* nor easy to snapshot/export as part of the run bundle in a queryable way.

### Add this file (new): `src/relspec/pipeline_policy.py`

This becomes the single “policy object” you pass around.

```python
# src/relspec/pipeline_policy.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from arrowdsl.kernel.registry import KernelLane
from datafusion_engine.compile_options import DataFusionSqlPolicy
from ibis_engine.param_tables import ParamTablePolicy
from relspec.list_filter_gate import ListFilterGatePolicy
from relspec.rules.policies import PolicyRegistry


@dataclass(frozen=True)
class KernelLanePolicy:
    """
    Governs which kernel lanes are permitted for a run.
    - DF_UDF: DataFusion UDF lane
    - BUILTIN: Arrow compute / Acero lane
    - ARROW_FALLBACK: Arrow fallback lane (still high-perf, but not DF-native)
    """
    allowed: tuple[KernelLane, ...] = (
        KernelLane.DF_UDF,
        KernelLane.BUILTIN,
        KernelLane.ARROW_FALLBACK,
    )
    on_violation: Literal["error", "warn", "record_only"] = "warn"


@dataclass(frozen=True)
class DiagnosticsPolicy:
    # Runtime engine diagnostics
    capture_datafusion_metrics: bool = True
    capture_datafusion_traces: bool = True
    capture_datafusion_fallbacks: bool = True
    capture_datafusion_explains: bool = False  # keep off by default

    # Rule-time diagnostics
    emit_kernel_lane_diagnostics: bool = True


@dataclass(frozen=True)
class PipelinePolicy:
    """
    Single object to drive rule-spec behavior + enforcement + diagnostics.
    """
    policy_registry: PolicyRegistry = field(default_factory=PolicyRegistry)

    # Param + list filter gating (already used in relspec validation)
    param_table_policy: ParamTablePolicy = field(default_factory=ParamTablePolicy)
    list_filter_gate_policy: ListFilterGatePolicy = field(default_factory=ListFilterGatePolicy)

    # DataFusion SQL allowances (fallback path)
    datafusion_sql_policy: DataFusionSqlPolicy = field(default_factory=DataFusionSqlPolicy)

    # Kernel lane enforcement
    kernel_lanes: KernelLanePolicy = field(default_factory=KernelLanePolicy)

    # Diagnostics policy
    diagnostics: DiagnosticsPolicy = field(default_factory=DiagnosticsPolicy)
```

### Thread it into the pipeline (minimal touches)

You want this object to actually *govern* execution.

#### A) Add a Hamilton input node (new function) in `src/hamilton_pipeline/modules/inputs.py`

```python
from relspec.pipeline_policy import PipelinePolicy

@tag(layer="inputs", kind="object")
def pipeline_policy() -> PipelinePolicy:
    # later: allow overrides via config/env if desired
    return PipelinePolicy()
```

#### B) Use it when building the `RuleRegistry` in `src/hamilton_pipeline/modules/cpg_build.py`

Right now, you build `RuleRegistry(..., param_table_policy=param_table_policy)`.

Change signature + call site so the policy object is the source of truth:

```python
# src/hamilton_pipeline/modules/cpg_build.py

@tag(layer="relspec", artifact="rule_registry", kind="registry")
def rule_registry(
    param_table_specs: tuple[ParamTableSpec, ...],
    pipeline_policy: PipelinePolicy,
) -> RuleRegistry:
    return RuleRegistry(
        adapters=(CpgRuleAdapter(), RelspecRuleAdapter(), NormalizeRuleAdapter(), ExtractRuleAdapter()),
        param_table_specs=param_table_specs,
        param_table_policy=pipeline_policy.param_table_policy,
        list_filter_gate_policy=pipeline_policy.list_filter_gate_policy,
    )
```

(You already support `list_filter_gate_policy` on `RuleRegistry`; this just starts using it.)

#### C) Use it for confidence/ambiguity/evidence defaults in relationship rule resolution

In `src/hamilton_pipeline/modules/cpg_build.py`, `_resolve_relationship_rules()` calls:

* `apply_policy_defaults(rule, schema)`
* `evidence_spec_from_schema(schema)`
* `validate_policy_requirements(updated, schema)`

Update those helpers to accept an explicit registry (see next section).

---

## 2) Deprecate implicit globals in policy helpers (and make policy explicit)

### Modify: `src/relspec/policies.py`

**Goal:** no more hidden `_POLICY_REGISTRY` as the only option.

#### Change function signatures

```python
# src/relspec/policies.py
from warnings import warn

_DEFAULT_POLICY_REGISTRY = PolicyRegistry()

def confidence_policy_from_schema(
    schema: SchemaLike,
    *,
    registry: PolicyRegistry | None = None,
) -> ConfidencePolicy | None:
    if registry is None:
        warn("confidence_policy_from_schema() default registry is deprecated; pass registry=...", DeprecationWarning)
        registry = _DEFAULT_POLICY_REGISTRY
    return _confidence_policy_from_metadata(schema.metadata or {}, registry=registry)

def ambiguity_policy_from_schema(
    schema: SchemaLike,
    *,
    registry: PolicyRegistry | None = None,
) -> AmbiguityPolicy | None:
    if registry is None:
        warn("ambiguity_policy_from_schema() default registry is deprecated; pass registry=...", DeprecationWarning)
        registry = _DEFAULT_POLICY_REGISTRY
    return _ambiguity_policy_from_metadata(schema.metadata or {}, registry=registry)
```

#### Update internals to use provided registry

```python
def _confidence_policy_from_metadata(
    meta: Mapping[bytes, bytes],
    *,
    registry: PolicyRegistry,
) -> ConfidencePolicy | None:
    name = _meta_str(meta, CONFIDENCE_POLICY_META)
    if name:
        return registry.resolve_confidence("cpg", name)
    ...
```

…and similarly for `_ambiguity_policy_from_metadata`.

### Modify: `src/normalize/policies.py`

Same pattern: accept `registry: PolicyRegistry | None = None`, deprecate implicit global.

### Deprecation: `src/relspec/policy_registry.py`

This file appears unused in current code. Make it an explicit “legacy compat” module:

* Mark its exports as deprecated (docstring + `DeprecationWarning` in resolvers).
* Plan deletion once downstream call sites are migrated.

---

## 3) Diagnostics table schemas: make runtime + rule diagnostics queryable (Arrow schemas + versioning)

You already emit:

* `relspec/rule_diagnostics.arrow`
* `relspec/template_diagnostics.arrow`
* `relspec/datafusion_metrics.json`
* `relspec/datafusion_traces.json`

To make diagnostics richer and queryable, add **parquet-backed** diagnostics tables with explicit schemas.

### Add this file (new): `src/obs/diagnostics_schemas.py`

```python
# src/obs/diagnostics_schemas.py
from __future__ import annotations
import pyarrow as pa

DATAFUSION_FALLBACKS_V1 = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64()),
        pa.field("reason", pa.string()),
        pa.field("error", pa.string()),
        pa.field("expression_type", pa.string()),
        pa.field("sql", pa.string()),
        pa.field("dialect", pa.string()),
        pa.field("policy_violations", pa.list_(pa.string())),
    ],
    metadata={b"schema_name": b"datafusion_fallbacks_v1"},
)

DATAFUSION_EXPLAINS_V1 = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64()),
        pa.field("sql", pa.string()),
        pa.field("explain_rows_json", pa.string()),  # keep simple; can normalize later
        pa.field("explain_analyze", pa.bool_()),
    ],
    metadata={b"schema_name": b"datafusion_explains_v1"},
)

# Optional (but very useful): a compact per-rule execution rollup
RELSPEC_RULE_EXEC_V1 = pa.schema(
    [
        pa.field("event_time_unix_ms", pa.int64()),
        pa.field("output_dataset", pa.string()),
        pa.field("rule_name", pa.string()),
        pa.field("rule_kind", pa.string()),
        pa.field("rows_out", pa.int64()),
        pa.field("duration_ms", pa.int64()),
        pa.field("used_datafusion", pa.bool_()),
    ],
    metadata={b"schema_name": b"relspec_rule_exec_v1"},
)
```

### Add this file (new): `src/obs/diagnostics_tables.py`

Builders that convert collectors → `pa.Table`.

```python
# src/obs/diagnostics_tables.py
from __future__ import annotations
import json
import time
from collections.abc import Mapping, Sequence

import pyarrow as pa

from obs.diagnostics_schemas import DATAFUSION_EXPLAINS_V1, DATAFUSION_FALLBACKS_V1

def _now_ms() -> int:
    return int(time.time() * 1000)

def datafusion_fallbacks_table(events: Sequence[Mapping[str, object]]) -> pa.Table:
    rows = []
    now = _now_ms()
    for e in events:
        rows.append(
            {
                "event_time_unix_ms": int(e.get("event_time_unix_ms") or now),
                "reason": str(e.get("reason") or ""),
                "error": str(e.get("error") or ""),
                "expression_type": str(e.get("expression_type") or ""),
                "sql": str(e.get("sql") or ""),
                "dialect": str(e.get("dialect") or ""),
                "policy_violations": list(e.get("policy_violations") or []),
            }
        )
    return pa.Table.from_pylist(rows, schema=DATAFUSION_FALLBACKS_V1)

def datafusion_explains_table(explains: Sequence[Mapping[str, object]]) -> pa.Table:
    rows = []
    now = _now_ms()
    for ex in explains:
        rows.append(
            {
                "event_time_unix_ms": int(ex.get("event_time_unix_ms") or now),
                "sql": str(ex.get("sql") or ""),
                "explain_rows_json": json.dumps(ex.get("rows") or [], ensure_ascii=False),
                "explain_analyze": bool(ex.get("explain_analyze") or False),
            }
        )
    return pa.Table.from_pylist(rows, schema=DATAFUSION_EXPLAINS_V1)
```

---

## 4) Parquet writers: centralize in `obs/` and eliminate ad-hoc `pq.write_table` writes

### Add this file (new): `src/obs/parquet_writers.py`

Use your existing ArrowDSL parquet utilities (already in repo) to get:

* dataset directories,
* metadata sidecars,
* consistent compression/options.

```python
# src/obs/parquet_writers.py
from __future__ import annotations

from pathlib import Path
import pyarrow as pa

from arrowdsl.io.parquet import DatasetWriteConfig, ParquetWriteOptions, write_dataset_parquet

def write_obs_dataset(
    base_dir: str | Path,
    *,
    name: str,
    table: pa.Table,
    overwrite: bool = True,
    opts: ParquetWriteOptions | None = None,
) -> str:
    """
    Writes:
      <base_dir>/<name>/part-*.parquet (+ _metadata/_common_metadata)
    """
    base = Path(base_dir)
    ds_dir = base / name
    config = DatasetWriteConfig(opts=opts or ParquetWriteOptions(), overwrite=overwrite)
    return write_dataset_parquet(table, ds_dir, config=config)
```

### Replace ad-hoc parquet writes (deprecations + migrations)

#### A) Modify: `src/hamilton_pipeline/modules/params.py`

**Deprecate:** direct `pq.write_table(...)` in `write_param_tables_parquet`.

Replace with dataset writer:

```python
from arrowdsl.io.parquet import write_dataset_parquet, DatasetWriteConfig, ParquetWriteOptions

# inside write_param_tables_parquet loop:
write_dataset_parquet(
    artifact.table,
    target_dir,  # directory
    config=DatasetWriteConfig(opts=ParquetWriteOptions(), overwrite=True),
)
output[logical_name] = str(target_dir)
```

**Deprecate:** param signature computed from `to_pylist()` (see next section).

#### B) Modify: `src/obs/repro.py`

You currently write param tables with `pq.write_table(...)` inside the run bundle writer (search for the param artifact writer in this file).

Change it to call `obs.parquet_writers.write_obs_dataset(...)`.

---

## 5) Hook diagnostics into run bundles (so they actually ship)

### Extend DataFusion runtime profile to expose explain/fallback events

Modify: `src/datafusion_engine/runtime.py`

Add tiny accessors:

```python
# in DataFusionRuntimeProfile
def collect_fallback_events(self) -> list[dict[str, object]] | None:
    if not self.capture_fallbacks or self.fallback_collector is None:
        return None
    # DataFusionFallbackCollector already stores entries
    return list(self.fallback_collector.entries)

def collect_explain_entries(self) -> list[dict[str, object]] | None:
    if not self.capture_explain or self.explain_collector is None:
        return None
    return list(self.explain_collector.entries)
```

### Extend RunBundleContext to carry these as tables

Modify: `src/obs/repro.py`

Add fields:

```python
datafusion_fallbacks: pa.Table | None = None
datafusion_explains: pa.Table | None = None
```

### Populate them in `src/hamilton_pipeline/modules/outputs.py`

Update `_datafusion_runtime_artifacts(...)` or add a sibling helper:

```python
from obs.diagnostics_tables import datafusion_explains_table, datafusion_fallbacks_table

def _datafusion_runtime_diag_tables(ctx: ExecutionContext) -> tuple[pa.Table | None, pa.Table | None]:
    profile = ctx.runtime.datafusion
    if profile is None:
        return None, None
    fallbacks = profile.collect_fallback_events()
    explains = profile.collect_explain_entries()
    fb_table = datafusion_fallbacks_table(fallbacks) if fallbacks else None
    ex_table = datafusion_explains_table(explains) if explains else None
    return fb_table, ex_table
```

Then in `run_bundle_context(...)`, set:

* `datafusion_fallbacks=fb_table`
* `datafusion_explains=ex_table`

### Write them in the run bundle

Modify: `src/obs/repro.py::_write_runtime_artifacts(...)`

Add:

```python
from obs.parquet_writers import write_obs_dataset

if context.datafusion_fallbacks is not None:
    ds_path = write_obs_dataset(relspec_dir, name="datafusion_fallbacks", table=context.datafusion_fallbacks, overwrite=True)
    files_written.append(ds_path)

if context.datafusion_explains is not None:
    ds_path = write_obs_dataset(relspec_dir, name="datafusion_explains", table=context.datafusion_explains, overwrite=True)
    files_written.append(ds_path)
```

**Resulting layout:**

* `run_bundles/<run>/relspec/datafusion_fallbacks/part-*.parquet`
* `run_bundles/<run>/relspec/datafusion_explains/part-*.parquet`

That’s immediately queryable via DataFusion/Arrow dataset scans.

---

## 6) Performance + “minimal Python ops”: remove row-wise signatures (`to_pylist`) from param hashing

### What to deprecate

* `ibis_engine.param_tables.param_signature(...)` currently:

  * converts values to `str`,
  * sorts in Python,
  * hashes JSON.
* `_artifact_from_parquet(...)` in `hamilton_pipeline/modules/params.py` uses `to_pylist()`.

This is exactly the kind of “standard python operations and calculations” you said you want minimized.

### Add Arrow-native signature hashing (new function)

Add in `src/ibis_engine/param_tables.py` (or a new `param_signatures.py` module).

```python
import hashlib
import pyarrow as pa
from pyarrow import compute as pc

def param_signature_from_array(*, logical_name: str, values: pa.Array | pa.ChunkedArray) -> str:
    """
    Stable-ish signature without Python per-row processing.
    - Cast to string for canonicalization
    - Sort in Arrow
    - Hash in Arrow (uint64)
    - SHA256 over the raw uint64 buffer (+ logical_name)
    """
    arr = values
    arr = pc.cast(arr, pa.large_string(), safe=False)
    idx = pc.sort_indices(arr)              # Arrow sort
    arr_sorted = pc.take(arr, idx)          # Arrow take
    h64 = pc.hash(arr_sorted)               # Arrow hash -> uint64 array

    # Concatenate buffers without iterating Python-side
    buf = h64.buffers()[1]  # data buffer (skip null bitmap)
    m = hashlib.sha256()
    m.update(logical_name.encode("utf-8"))
    m.update(buf.to_pybytes() if buf is not None else b"")
    return m.hexdigest()
```

### Use it in `_artifact_from_parquet(...)`

Modify: `src/hamilton_pipeline/modules/params.py`

Replace:

```python
values = table[spec.key_col].to_pylist()
signature = param_signature(logical_name=spec.logical_name, values=values)
```

With:

```python
from ibis_engine.param_tables import param_signature_from_array
signature = param_signature_from_array(logical_name=spec.logical_name, values=table[spec.key_col])
```

### Deprecate old signature function (don’t delete immediately)

In `param_signature(...)`, emit `DeprecationWarning` and call the new implementation when possible (or keep the old as a strict fallback for non-scalar types).

---

## 7) “GitHub PR structure”: how I’d slice this into reviewable PRs (file-by-file)

### PR-A: Introduce PipelinePolicy + stop relying on global policy registries

**New**

* `src/relspec/pipeline_policy.py`

**Modified**

* `src/hamilton_pipeline/modules/inputs.py` (add `pipeline_policy()`)
* `src/hamilton_pipeline/modules/cpg_build.py` (use policy for rule registry + rule defaulting)
* `src/relspec/policies.py` (explicit `registry=...`, deprecate implicit global)
* `src/normalize/policies.py` (same)

**Deprecate**

* implicit `_POLICY_REGISTRY` usage paths (warnings, not removal)
* `src/relspec/policy_registry.py` marked legacy

---

### PR-B: Add diagnostics schemas + parquet writers (obs layer)

**New**

* `src/obs/diagnostics_schemas.py`
* `src/obs/diagnostics_tables.py`
* `src/obs/parquet_writers.py`

No behavioral change yet; just adds infra.

---

### PR-C: Wire DataFusion fallbacks/explains into run bundles as parquet datasets

**Modified**

* `src/datafusion_engine/runtime.py` (expose `collect_fallback_events`, `collect_explain_entries`)
* `src/obs/repro.py` (RunBundleContext fields + write parquet datasets)
* `src/hamilton_pipeline/modules/outputs.py` (populate new RunBundleContext fields)

**Deprecate**

* leaving only JSON for these artifacts (keep JSON for now; parquet is additive)

---

### PR-D: Remove row-wise Python signatures for param tables + unify param parquet writes

**Modified**

* `src/ibis_engine/param_tables.py` (new `param_signature_from_array`, deprecate old)
* `src/hamilton_pipeline/modules/params.py`

  * write param tables via dataset writer
  * compute signatures Arrow-side
* `src/obs/repro.py` (param artifacts written using `obs.parquet_writers`, not raw `pq.write_table`)

**Deprecate**

* direct `pq.write_table` usage in these two locations

---

## Summary of “what to deprecate” (explicit list)

1. **Implicit global PolicyRegistry usage**

   * `src/relspec/policies.py::_POLICY_REGISTRY`
   * `src/normalize/policies.py::_POLICY_REGISTRY`
   * Replace with explicit `registry=` sourced from `PipelinePolicy`.

2. **Legacy unused policy module**

   * `src/relspec/policy_registry.py` (treat as legacy compatibility; remove later)

3. **Ad-hoc parquet writes**

   * `pyarrow.parquet.write_table(...)` in:

     * `src/hamilton_pipeline/modules/params.py::write_param_tables_parquet`
     * `src/obs/repro.py` param-table artifact writing
   * Replace with ArrowDSL dataset parquet writers (metadata sidecars, consistent options).

4. **Row-wise Python param signatures**

   * `to_pylist()` + `param_signature(values: Sequence[object])`
   * Replace with Arrow-native hash + buffered sha256.

---

If you want to push the “maximally rule-driven” angle even harder after the above: the next natural follow-on is adding a **per-rule execution events** collector (schema already sketched as `RELSPEC_RULE_EXEC_V1`) by instrumenting `relspec.compiler.CompiledRule.execute()` / `CompiledOutput.execute()` with an optional observer. But the steps above are the cleanest “foundation PRs” that make policy + diagnostics + parquet artifacts first-class in your current architecture without destabilizing core execution.

Below is the “push it even harder” follow‑on I suggested: **a per‑rule execution events collector** (**`RELSPEC_RULE_EXEC_V1`**) wired by adding an **optional observer** to:

* `relspec.compiler.CompiledRule.execute()`
* `relspec.compiler.CompiledOutput.execute()`

…and then threading a collector through the Hamilton pipeline so you can **materialize** and/or **bundle** the events as a first‑class artifact.

I’m going to write this as a **PR-style implementation plan** with the **exact code shapes**, **file placements**, **commit slices**, and **precise (minimal) deprecations**.

---

## PR-11: RELSPEC_RULE_EXEC_V1 — per-rule execution events (observer + table artifact)

### What you get (end state)

1. **One Arrow table per run** capturing rule execution:

   * start/end timestamps
   * duration + stage timings (plan compile, plan execution, rule-meta kernel, each post-kernel)
   * output row/col counts + schema fingerprint
   * status + error payload (when failed)
   * **DataFusion explain/fallback correlation ranges** (so you can tie each rule to the global explain/fallback collector snapshots without duplicating huge blobs)
2. Zero behavioral change when observer is not provided.
3. A single place to add richer instrumentation later (per-output finalize, per-stage DataFusion metrics, etc.).

---

# 1) Add schema + event model + collector

### New file: `src/relspec/rules/exec_events.py`

This keeps the observability model close to rule execution while avoiding `obs/*` → `relspec/*` circular imports (the compiler lives in `relspec/`).

```python
# src/relspec/rules/exec_events.py
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol

import pyarrow as pa

from arrowdsl.spec.io import table_from_rows

RuleExecScope = Literal["rule", "output"]
RuleExecStatus = Literal["ok", "error"]
RuleExecLane = Literal["execute_fn", "rel_plan"]

RELSPEC_RULE_EXEC_V1 = pa.schema(
    [
        pa.field("output_dataset", pa.string(), nullable=False),

        # scope = "rule" rows have rule_name populated; scope="output" rows have it null
        pa.field("scope", pa.string(), nullable=False),
        pa.field("rule_name", pa.string(), nullable=True),

        pa.field("rule_kind", pa.string(), nullable=True),
        pa.field("lane", pa.string(), nullable=True),
        pa.field("plan_signature", pa.string(), nullable=True),

        # execution context snapshot (small + queryable)
        pa.field("ctx_mode", pa.string(), nullable=False),
        pa.field("determinism", pa.string(), nullable=False),
        pa.field("provenance", pa.bool_(), nullable=False),
        pa.field("debug", pa.bool_(), nullable=False),

        # timestamps + duration
        pa.field("started_at_unix_ns", pa.int64(), nullable=False),
        pa.field("ended_at_unix_ns", pa.int64(), nullable=False),
        pa.field("duration_ms", pa.float64(), nullable=False),

        # stage timings (ms)
        # Keys: "plan_compile_ms", "plan_execute_ms", "emit_rule_meta_ms",
        #       "post_kernels_total_ms", "post_kernel.<kind>_ms", "finalize_ms", etc.
        pa.field("timing_ms", pa.map_(pa.string(), pa.float64()), nullable=True),

        # output stats (for scope="rule": rule result; for scope="output": finalized result)
        pa.field("rows_out", pa.int64(), nullable=True),
        pa.field("cols_out", pa.int32(), nullable=True),
        pa.field("schema_fingerprint", pa.string(), nullable=True),

        # correlate to global DataFusion collectors without copying payload
        pa.field("df_explain_start", pa.int64(), nullable=True),
        pa.field("df_explain_end", pa.int64(), nullable=True),
        pa.field("df_fallback_start", pa.int64(), nullable=True),
        pa.field("df_fallback_end", pa.int64(), nullable=True),

        # error payload
        pa.field("status", pa.string(), nullable=False),
        pa.field("error_type", pa.string(), nullable=True),
        pa.field("error_message", pa.string(), nullable=True),
        pa.field("error_trace", pa.string(), nullable=True),

        # small extensibility hook (string->string only)
        pa.field("metadata", pa.map_(pa.string(), pa.string()), nullable=True),
    ],
    metadata={b"spec_kind": b"relspec_rule_exec_v1"},
)


@dataclass(frozen=True)
class RuleExecEvent:
    output_dataset: str
    scope: RuleExecScope
    rule_name: str | None

    rule_kind: str | None = None
    lane: str | None = None
    plan_signature: str | None = None

    ctx_mode: str = "tolerant"
    determinism: str = "unordered"
    provenance: bool = False
    debug: bool = False

    started_at_unix_ns: int = 0
    ended_at_unix_ns: int = 0
    duration_ms: float = 0.0

    timing_ms: Mapping[str, float] = field(default_factory=dict)

    rows_out: int | None = None
    cols_out: int | None = None
    schema_fingerprint: str | None = None

    df_explain_start: int | None = None
    df_explain_end: int | None = None
    df_fallback_start: int | None = None
    df_fallback_end: int | None = None

    status: RuleExecStatus = "ok"
    error_type: str | None = None
    error_message: str | None = None
    error_trace: str | None = None

    metadata: Mapping[str, str] = field(default_factory=dict)

    def to_row(self) -> dict[str, object]:
        return {
            "output_dataset": self.output_dataset,
            "scope": self.scope,
            "rule_name": self.rule_name,
            "rule_kind": self.rule_kind,
            "lane": self.lane,
            "plan_signature": self.plan_signature,
            "ctx_mode": self.ctx_mode,
            "determinism": self.determinism,
            "provenance": bool(self.provenance),
            "debug": bool(self.debug),
            "started_at_unix_ns": int(self.started_at_unix_ns),
            "ended_at_unix_ns": int(self.ended_at_unix_ns),
            "duration_ms": float(self.duration_ms),
            "timing_ms": dict(self.timing_ms) or None,
            "rows_out": self.rows_out,
            "cols_out": self.cols_out,
            "schema_fingerprint": self.schema_fingerprint,
            "df_explain_start": self.df_explain_start,
            "df_explain_end": self.df_explain_end,
            "df_fallback_start": self.df_fallback_start,
            "df_fallback_end": self.df_fallback_end,
            "status": self.status,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "error_trace": self.error_trace,
            "metadata": dict(self.metadata) or None,
        }


class RuleExecutionObserver(Protocol):
    def record(self, event: RuleExecEvent) -> None: ...


@dataclass
class RuleExecCollector(RuleExecutionObserver):
    events: list[RuleExecEvent] = field(default_factory=list)

    def record(self, event: RuleExecEvent) -> None:
        self.events.append(event)

    def to_table(self) -> pa.Table:
        return table_from_rows(RELSPEC_RULE_EXEC_V1, [e.to_row() for e in self.events])


def empty_rule_exec_table() -> pa.Table:
    # convenient helper for pipelines that want a stable output
    return table_from_rows(RELSPEC_RULE_EXEC_V1, [])
```

---

# 2) Instrument `CompiledRule.execute()` (core of the feature)

### File: `src/relspec/compiler.py`

#### Signature change (backwards compatible)

Add one optional param:

```python
def execute(
    self,
    *,
    ctx: ExecutionContext,
    resolver: PlanResolver[IbisPlan],
    compiler: RelPlanCompiler[IbisPlan],
    params: Mapping[IbisValue, object] | None = None,
    observer: RuleExecutionObserver | None = None,  # NEW
) -> TableLike:
```

#### Implementation shape (key parts)

```python
import time
import traceback

from arrowdsl.plan.metrics import table_summary
from relspec.plan import rel_plan_signature
from relspec.rules.exec_events import RuleExecEvent, RuleExecutionObserver
```

Then inside `CompiledRule.execute()`:

```python
def execute(..., observer: RuleExecutionObserver | None = None) -> TableLike:
    if observer is None:
        # fast path: identical behavior as today
        if self.execute_fn is not None:
            table = self.execute_fn(ctx, resolver)
        elif self.rel_plan is not None:
            plan = compiler.compile(self.rel_plan, ctx=ctx, resolver=resolver)
            table = plan.to_table(params=params)
        else:
            raise RuntimeError("CompiledRule has neither rel_plan nor execute_fn.")
        if self.emit_rule_meta:
            table = _build_rule_meta_kernel(self.rule)(table, ctx)
        for fn in self.post_kernels:
            table = fn(table, ctx)
        return _apply_rule_metadata(table, rule=self.rule)

    # --- observed path ---
    started_unix_ns = time.time_ns()
    t0 = time.perf_counter_ns()

    df_explain_start = df_explain_end = None
    df_fallback_start = df_fallback_end = None
    runtime = ctx.runtime.datafusion
    if runtime is not None:
        if runtime.explain_collector is not None:
            df_explain_start = len(runtime.explain_collector.entries)
        if runtime.fallback_collector is not None:
            df_fallback_start = len(runtime.fallback_collector.entries)

    timing: dict[str, float] = {}
    lane = "execute_fn" if self.execute_fn is not None else ("rel_plan" if self.rel_plan else None)
    plan_sig = rel_plan_signature(self.rel_plan) if self.rel_plan is not None else None

    try:
        # main execution
        if self.execute_fn is not None:
            t_exec = time.perf_counter_ns()
            table = self.execute_fn(ctx, resolver)
            timing["execute_fn_ms"] = (time.perf_counter_ns() - t_exec) / 1e6

        elif self.rel_plan is not None:
            t_compile = time.perf_counter_ns()
            plan = compiler.compile(self.rel_plan, ctx=ctx, resolver=resolver)
            timing["plan_compile_ms"] = (time.perf_counter_ns() - t_compile) / 1e6

            t_exec = time.perf_counter_ns()
            table = plan.to_table(params=params)
            timing["plan_execute_ms"] = (time.perf_counter_ns() - t_exec) / 1e6
        else:
            raise RuntimeError("CompiledRule has neither rel_plan nor execute_fn.")

        # rule meta kernel
        if self.emit_rule_meta:
            t_meta = time.perf_counter_ns()
            table = _build_rule_meta_kernel(self.rule)(table, ctx)
            timing["emit_rule_meta_ms"] = (time.perf_counter_ns() - t_meta) / 1e6

        # post kernels (label by spec.kind)
        post_total_ns = 0
        for fn, spec in zip(self.post_kernels, self.rule.post_kernels, strict=False):
            t_k = time.perf_counter_ns()
            table = fn(table, ctx)
            dt = time.perf_counter_ns() - t_k
            post_total_ns += dt
            kind = getattr(spec, "kind", "unknown")
            timing[f"post_kernel.{kind}_ms"] = dt / 1e6
        if post_total_ns:
            timing["post_kernels_total_ms"] = post_total_ns / 1e6

        table = _apply_rule_metadata(table, rule=self.rule)

        ended_unix_ns = time.time_ns()
        duration_ms = (time.perf_counter_ns() - t0) / 1e6

        if runtime is not None:
            if runtime.explain_collector is not None:
                df_explain_end = len(runtime.explain_collector.entries)
            if runtime.fallback_collector is not None:
                df_fallback_end = len(runtime.fallback_collector.entries)

        summ = table_summary(table)
        observer.record(
            RuleExecEvent(
                output_dataset=self.rule.output_dataset,
                scope="rule",
                rule_name=self.rule.name,
                rule_kind=str(self.rule.kind),
                lane=lane,
                plan_signature=plan_sig,
                ctx_mode=ctx.mode,
                determinism=ctx.determinism.value,
                provenance=ctx.provenance,
                debug=ctx.debug,
                started_at_unix_ns=started_unix_ns,
                ended_at_unix_ns=ended_unix_ns,
                duration_ms=duration_ms,
                timing_ms=timing,
                rows_out=summ["rows"],
                cols_out=summ["columns"],
                schema_fingerprint=summ["schema_fingerprint"],
                df_explain_start=df_explain_start,
                df_explain_end=df_explain_end,
                df_fallback_start=df_fallback_start,
                df_fallback_end=df_fallback_end,
                status="ok",
            )
        )
        return table

    except Exception as e:
        ended_unix_ns = time.time_ns()
        duration_ms = (time.perf_counter_ns() - t0) / 1e6
        trace = traceback.format_exc()
        if runtime is not None:
            if runtime.explain_collector is not None:
                df_explain_end = len(runtime.explain_collector.entries)
            if runtime.fallback_collector is not None:
                df_fallback_end = len(runtime.fallback_collector.entries)

        # keep trace bounded
        if len(trace) > 16_000:
            trace = trace[:16_000] + "\n...<truncated>..."

        observer.record(
            RuleExecEvent(
                output_dataset=self.rule.output_dataset,
                scope="rule",
                rule_name=self.rule.name,
                rule_kind=str(self.rule.kind),
                lane=lane,
                plan_signature=plan_sig,
                ctx_mode=ctx.mode,
                determinism=ctx.determinism.value,
                provenance=ctx.provenance,
                debug=ctx.debug,
                started_at_unix_ns=started_unix_ns,
                ended_at_unix_ns=ended_unix_ns,
                duration_ms=duration_ms,
                timing_ms=timing,
                df_explain_start=df_explain_start,
                df_explain_end=df_explain_end,
                df_fallback_start=df_fallback_start,
                df_fallback_end=df_fallback_end,
                status="error",
                error_type=type(e).__name__,
                error_message=str(e),
                error_trace=trace,
            )
        )
        raise
```

Why this shape works well in your architecture:

* The **observer is the only switch** between “zero overhead” and “full instrumentation.”
* **Stage timings** are captured without introducing additional runtime dependencies.
* The **DataFusion explain/fallback** correlation is done with **index ranges**, not massive duplicated blobs.

---

# 3) Instrument `CompiledOutput.execute()` (thread observer + optional output-level event)

### File: `src/relspec/compiler.py`

Change signature:

```python
def execute(
    self,
    *,
    ctx: ExecutionContext,
    resolver: PlanResolver[IbisPlan],
    compiler: RelPlanCompiler[IbisPlan] | None = None,
    contracts: ContractCatalog,
    params: Mapping[IbisValue, object] | None = None,
    observer: RuleExecutionObserver | None = None,  # NEW
) -> FinalizeResult:
```

Then:

* pass `observer=observer` into each `compiled.execute(...)`
* optionally (recommended) emit a **scope="output"** event around finalization

Sketch:

```python
import time
from arrowdsl.plan.metrics import table_summary
from relspec.rules.exec_events import RuleExecEvent

def execute(..., observer: RuleExecutionObserver | None = None) -> FinalizeResult:
    # existing ctx_exec logic unchanged...

    started_unix_ns = time.time_ns()
    t0 = time.perf_counter_ns()
    timing: dict[str, float] = {}

    plan_compiler = compiler or IbisRelPlanCompiler()

    t_rules = time.perf_counter_ns()
    table_parts = [
        compiled.execute(
            ctx=ctx_exec,
            resolver=resolver,
            compiler=plan_compiler,
            params=params,
            observer=observer,               # NEW
        )
        for compiled in self.contributors
    ]
    timing["contributors_total_ms"] = (time.perf_counter_ns() - t_rules) / 1e6

    t_fin = time.perf_counter_ns()
    result = _finalize_output_tables(
        output_dataset=self.output_dataset,
        contract_name=self.contract_name,
        table_parts=table_parts,
        ctx=ctx_exec,
        contracts=contracts,
    )
    timing["finalize_ms"] = (time.perf_counter_ns() - t_fin) / 1e6

    if observer is not None:
        ended_unix_ns = time.time_ns()
        duration_ms = (time.perf_counter_ns() - t0) / 1e6
        summ = table_summary(result.good)
        observer.record(
            RuleExecEvent(
                output_dataset=self.output_dataset,
                scope="output",
                rule_name=None,
                ctx_mode=ctx_exec.mode,
                determinism=ctx_exec.determinism.value,
                provenance=ctx_exec.provenance,
                debug=ctx_exec.debug,
                started_at_unix_ns=started_unix_ns,
                ended_at_unix_ns=ended_unix_ns,
                duration_ms=duration_ms,
                timing_ms=timing,
                rows_out=summ["rows"],
                cols_out=summ["columns"],
                schema_fingerprint=summ["schema_fingerprint"],
                status="ok",
                metadata={"contributors": str(len(self.contributors))},
            )
        )
    return result
```

That gives you **rule** rows *and* a single **output** row, which is extremely useful when “finalize” dominates runtime.

---

# 4) Wire it into the Hamilton pipeline (single execution, no double-running)

## 4a) Create the collector and pass it during relationship execution

### File: `src/hamilton_pipeline/modules/cpg_build.py`

In `relationship_tables(...)`:

* create collector
* pass observer to each `compiled.execute(...)`
* stash the resulting table under a reserved key in the returned dict

```python
from relspec.rules.exec_events import RuleExecCollector

_RULE_EXEC_KEY = "__relspec_rule_exec_v1__"

def relationship_tables(...):
    out: dict[str, TableLike] = {}
    collector = RuleExecCollector()

    for key, compiled in compiled_relationship_outputs.items():
        res = compiled.execute(
            ctx=ctx,
            resolver=relspec_resolver,
            contracts=relationship_contracts,
            params=relspec_param_bindings,
            observer=collector,   # NEW
        )
        out[key] = res.good

    out[_RULE_EXEC_KEY] = collector.to_table()  # NEW
    ...
    return out
```

## 4b) Expose it as its own Hamilton node (so run bundles / materializers can depend on it)

### File: `src/hamilton_pipeline/modules/cpg_build.py` (same module is fine)

```python
import pyarrow as pa
from arrowdsl.schema.build import empty_table
from relspec.rules.exec_events import RELSPEC_RULE_EXEC_V1

@tag(layer="relspec", artifact="relspec_rule_exec_events", kind="table")
def relspec_rule_exec_events(relationship_tables: dict[str, TableLike]) -> pa.Table:
    value = relationship_tables.get("__relspec_rule_exec_v1__")
    if value is None:
        return empty_table(RELSPEC_RULE_EXEC_V1)
    if isinstance(value, pa.Table):
        return value
    # keep it strict: this artifact should always be a materialized table
    return value.read_all()  # if RecordBatchReader-like ever appears
```

---

# 5) Persist it as a run bundle artifact

## 5a) Extend run bundle context + writer

### File: `src/obs/repro.py`

#### Add field to `RunBundleContext`

```python
rule_exec_events: pa.Table | None = None
```

#### Write it in `_write_relspec_snapshots(...)`

Add at the end of the relspec snapshot section:

```python
import pyarrow.parquet as pq  # already imported

if context.rule_exec_events is not None:
    target = relspec_dir / "rule_exec_events.parquet"
    pq.write_table(context.rule_exec_events, target)
    files_written.append(str(target))
```

(Parquet here is ideal: tiny, columnar, instantly analyzable.)

## 5b) Pass it into the RunBundleContext

### File: `src/hamilton_pipeline/modules/outputs.py`

Update `run_bundle_context(...)` signature to accept the new artifact:

```python
def run_bundle_context(
    run_bundle_inputs: RunBundleInputs,
    output_config: OutputConfig,
    relspec_rule_exec_events: pa.Table | None = None,  # NEW
    ctx: ExecutionContext | None = None,
) -> RunBundleContext | None:
    ...
    return RunBundleContext(
        ...
        rule_exec_events=relspec_rule_exec_events,  # NEW
        ...
    )
```

Now every run bundle gets:

* `relspec/rule_exec_events.parquet`

---

# 6) Optional: add a standalone materializer (debug folder)

If you want a quick on-disk artifact even without run bundles:

### File: `src/hamilton_pipeline/modules/outputs.py`

```python
@tag(layer="materialize", artifact="relspec_rule_exec_events_parquet", kind="side_effect")
def write_relspec_rule_exec_events_parquet(
    relspec_rule_exec_events: pa.Table,
    output_dir: str | None,
    work_dir: str | None,
) -> JsonDict | None:
    base = _default_debug_dir(output_dir, work_dir)
    if not base:
        return None
    path = base / "relspec_rule_exec_events.parquet"
    write_table_parquet(relspec_rule_exec_events, path, opts=ParquetWriteOptions(), overwrite=True)
    return {"path": str(path), "rows": int(relspec_rule_exec_events.num_rows)}
```

---

## Deprecations to introduce (minimal, precise)

This PR is fundamentally **additive**, so deprecations are intentionally light:

1. **Deprecate “manual per-rule timing via prints/logging”** (if anyone adds it later).

   * Add a short note in `relspec/compiler.py` docstring or `obs/manifest` docs:
     “Per-rule timings must be emitted via `RuleExecutionObserver` / `RELSPEC_RULE_EXEC_V1`.”

2. **Soft deprecate consuming DataFusion explain/fallback payloads without rule correlation.**

   * You currently capture explain/fallback aggregates in `outputs._datafusion_notes(...)`.
   * Keep them, but update any internal docs to say:
     “Use `rule_exec_events.df_explain_start/end` and `df_fallback_start/end` to attribute explains/fallbacks to a rule.”

No hard removals required.

---

# PR11 diff map (file-by-file checklist) + commit slices

### Commit 1 — Schema + collector (pure add)

* ✅ `src/relspec/rules/exec_events.py` **(new)**
* ✅ `src/relspec/rules/__init__.py` export if you want:

  * `from .exec_events import RELSPEC_RULE_EXEC_V1, RuleExecCollector, RuleExecEvent`

### Commit 2 — Compiler instrumentation (observer threading)

* ✅ `src/relspec/compiler.py`

  * add `observer: RuleExecutionObserver | None = None` to:

    * `CompiledRule.execute`
    * `CompiledOutput.execute`
  * add “fast path if observer is None”
  * record `RuleExecEvent` rows on ok/error
  * include DataFusion explain/fallback index ranges

### Commit 3 — Hamilton wiring (single execution; no reruns)

* ✅ `src/hamilton_pipeline/modules/cpg_build.py`

  * instantiate `RuleExecCollector` in `relationship_tables`
  * pass `observer=collector` into `compiled.execute`
  * add reserved key `__relspec_rule_exec_v1__` → table
  * new node `relspec_rule_exec_events(...) -> pa.Table`

### Commit 4 — Run bundle persistence

* ✅ `src/obs/repro.py`

  * add `RunBundleContext.rule_exec_events`
  * write `relspec/rule_exec_events.parquet`
* ✅ `src/hamilton_pipeline/modules/outputs.py`

  * accept `relspec_rule_exec_events` in `run_bundle_context`
  * pass through into `RunBundleContext`

### Commit 5 — Optional debug materializer (nice-to-have)

* ✅ `src/hamilton_pipeline/modules/outputs.py`

  * add `write_relspec_rule_exec_events_parquet`

---

## How you’ll use the output immediately

Once you have `relspec/rule_exec_events.parquet` in a run bundle:

* Sort by `duration_ms` to find slowest rules
* Filter `status == "error"` to get failing rules + full trace
* Join rule events to:

  * your rule definition table (by `rule_name`)
  * your plan diagnostics (by `plan_signature`)
  * DataFusion explain/fallback collectors (via `df_explain_*`/`df_fallback_*` ranges)

This creates the “closed loop” you want:
**rule spec → plan signature → execution telemetry → explain/fallback → actionable tuning.**

---


