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

