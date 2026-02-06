# DataFusion Planning & Optimization: Detailed Implementation Plan v2

**Date:** 2026-02-06
**Supersedes:** `datafusion_planning_optimization_best_in_class_plan_v1.md`
**Scope:** Validated gap analysis with code snippets, target files, implementation checklists, and deprecation lists

---

## Executive Summary

After comprehensive cq-based codebase review and live DataFusion API verification, the v1 gap
analysis is significantly revised. Of the original 14 gaps:

- **6 gaps are already implemented** (Gaps 1, 2, 6, 10, 11, 12) — the codebase already
  captures session settings, fingerprints plan-shape config, enables all Parquet pruning
  knobs (including `pushdown_filters=true` in all policy presets), supports parameterized
  queries via `SqlBindings`, has `JoinPolicy` in `DataFusionRuntimeProfile`, and pins
  catalog namespaces via `CatalogConfig`.
- **1 gap is partially addressed** (Gap 13) — `ScanUnit` already tracks
  `total_files`/`candidate_file_count`/`pruned_file_count` but lacks pruning ratio diagnostics.
- **7 gaps remain as true implementation targets** (Gaps 3, 4, 5, 7, 8, 9, 14).

This v2 plan provides detailed implementation guidance for each remaining gap, plus targeted
improvements for the partially-addressed gap.

---

## Partially Addressed — Targeted Improvement

### Gap 13: Delta File Pruning Observability ⚠️ PARTIALLY ADDRESSED

**Current state:** `ScanUnit` already tracks `total_files`, `candidate_file_count`, and
`pruned_file_count`. The `pushed_filters` are also captured. The Rust control plane has
`DeltaAddActionPayload` with stats and partition values.

**What remains:** Pruning predicates (the actual expressions used for file pruning) are not
stored as structured data. No diagnostic alert when pruning ratio is poor.

See **Scope Item 13** below for the targeted improvement.

---

## Scope Items: True Implementation Gaps

### Scope Item 3: Enable EXPLAIN Capture and Rewrite Profiler

**Status:** The profiler module (`plan/profiler.py`) is **dead code**. The env var
`CODEANATOMY_DISABLE_DF_EXPLAIN` defaults to `"1"` (disabled), making `capture_explain()`
always return `None`. The existing approach uses stdout capture (`contextlib.redirect_stdout`)
which is fragile. DataFusion's `ctx.sql("EXPLAIN ...")` returns structured `RecordBatch` data
with `plan_type` and `plan` columns — this is the correct approach.

**API verification (confirmed via live testing):**
```python
# Standard EXPLAIN returns RecordBatch with plan_type + plan columns
result = ctx.sql("EXPLAIN " + sql).collect()
# result[0]["plan_type"] = ["logical_plan", "physical_plan"]
# result[0]["plan"] = ["<plan_text>", "<plan_text>"]

# EXPLAIN VERBOSE returns rule-by-rule entries
result = ctx.sql("EXPLAIN VERBOSE " + sql).collect()
# result[0]["plan_type"] = [
#   "initial_logical_plan",
#   "logical_plan after inline_table_scan",
#   "logical_plan after type_coercion",
#   ...
#   "initial_physical_plan",
#   "physical_plan after aggregate_statistics",
#   ...
# ]

# EXPLAIN with show_statistics
ctx.sql("SET datafusion.explain.show_statistics = true")
result = ctx.sql("EXPLAIN " + sql).collect()
# physical plan text includes: statistics=[Rows=Inexact(3), Bytes=Inexact(624)]
```

#### Representative Code Snippet

```python
# New: src/datafusion_engine/plan/profiler.py (rewritten)

@dataclass(frozen=True)
class ExplainRow:
    """Single row from EXPLAIN output."""
    plan_type: str
    plan: str

@dataclass(frozen=True)
class ExplainCapture:
    """Structured EXPLAIN capture from DataFusion SQL interface."""
    rows: tuple[ExplainRow, ...]
    mode: str  # "standard", "verbose", "analyze"

    @property
    def logical_plan(self) -> str | None:
        for row in self.rows:
            if row.plan_type == "logical_plan":
                return row.plan
        return None

    @property
    def physical_plan(self) -> str | None:
        for row in self.rows:
            if row.plan_type == "physical_plan":
                return row.plan
        return None


def capture_explain_sql(
    ctx: SessionContext,
    sql: str,
    *,
    verbose: bool = False,
    analyze: bool = False,
) -> ExplainCapture | None:
    """Capture EXPLAIN output via SQL interface (structured RecordBatch).

    Parameters
    ----------
    ctx
        DataFusion session context.
    sql
        The SQL statement to explain.
    verbose
        Use EXPLAIN VERBOSE for rule-by-rule trace.
    analyze
        Use EXPLAIN ANALYZE (triggers execution).
    """
    prefix = "EXPLAIN"
    mode = "standard"
    if analyze:
        prefix = "EXPLAIN ANALYZE"
        mode = "analyze"
    elif verbose:
        prefix = "EXPLAIN VERBOSE"
        mode = "verbose"

    try:
        batches = ctx.sql(f"{prefix} {sql}").collect()
    except Exception:
        return None

    rows: list[ExplainRow] = []
    for batch in batches:
        plan_types = batch.column("plan_type").to_pylist()
        plans = batch.column("plan").to_pylist()
        for pt, p in zip(plan_types, plans, strict=False):
            rows.append(ExplainRow(plan_type=str(pt), plan=str(p)))

    if not rows:
        return None
    return ExplainCapture(rows=tuple(rows), mode=mode)
```

#### Target Files

| File | Action |
|------|--------|
| `src/datafusion_engine/plan/profiler.py` | **Rewrite** — replace stdout-capture approach with SQL-based structured capture |
| `src/datafusion_engine/plan/bundle.py` | **Edit** — wire new `capture_explain_sql()` into `_capture_explain_artifacts()` |
| `src/serde_artifacts.py` | **No change** — `explain_tree_rows` and `explain_verbose_rows` fields already exist and are structured |

#### Implementation Checklist

- [ ] Rewrite `capture_explain()` → `capture_explain_sql()` using `ctx.sql("EXPLAIN ...")` SQL interface
- [ ] Replace `ExplainCapture` dataclass with structured `ExplainRow` + `ExplainCapture`
- [ ] Remove `CODEANATOMY_DISABLE_DF_EXPLAIN` env var gate (or flip default to `"0"`)
- [ ] Remove `_run_explain_text()` stdout-capture helper
- [ ] Remove `_plan_has_partitions()` guard (no longer needed with SQL approach)
- [ ] Update `_capture_explain_artifacts()` in `bundle.py` to pass `ctx` and SQL string
- [ ] Populate `PlanArtifacts.explain_tree_rows` from standard EXPLAIN RecordBatch rows
- [ ] Populate `PlanArtifacts.explain_verbose_rows` from VERBOSE EXPLAIN rows
- [ ] Respect `DiagnosticsConfig.capture_explain` / `explain_verbose` / `explain_analyze` flags
- [ ] Add unit tests for the new profiler module
- [ ] Verify `PlanArtifactRow.explain_tree_rows_msgpack` / `explain_verbose_rows_msgpack` serialize correctly

#### Deprecation / Deletion

| Target | Action | Rationale |
|--------|--------|-----------|
| `_run_explain_text()` in `profiler.py` | **Delete** | Stdout capture replaced by SQL interface |
| `_plan_has_partitions()` in `profiler.py` | **Delete** | Partition check not needed for SQL-based explain |
| `_parse_duration_ms()` in `profiler.py` | **Delete** | Only relevant to EXPLAIN ANALYZE text; superseded by structured access |
| `_parse_output_rows()` in `profiler.py` | **Delete** | Same as above |
| `_DURATION_RE` / `_OUTPUT_ROWS_RE` regexes in `profiler.py` | **Delete** | No longer needed |
| `CODEANATOMY_DISABLE_DF_EXPLAIN` env var convention | **Delete** | Feature is now enabled by default, gated by `DiagnosticsConfig` |

---

### Scope Item 4: Rule-by-Rule Optimizer Tracing

**Status:** No code parses `EXPLAIN VERBOSE` output into structured per-rule trace data.
This depends on Scope Item 3 (structured explain capture).

**API verification (confirmed):** `EXPLAIN VERBOSE` returns `plan_type` entries like:
- `"initial_logical_plan"`
- `"logical_plan after inline_table_scan"`
- `"logical_plan after type_coercion"`
- `"logical_plan after simplify_expressions"`
- `"initial_physical_plan"`
- `"initial_physical_plan_with_schema"`
- `"physical_plan after aggregate_statistics"`
- `"physical_plan_with_schema"`

#### Representative Code Snippet

```python
# New: src/datafusion_engine/plan/optimizer_trace.py

import re
from dataclasses import dataclass

_RULE_PATTERN = re.compile(
    r"^(?P<phase>logical_plan|physical_plan) after (?P<rule>.+)$"
)

@dataclass(frozen=True)
class OptimizerStage:
    """Single optimizer rule application."""
    phase: str         # "logical" or "physical"
    rule_name: str     # e.g. "inline_table_scan", "aggregate_statistics"
    plan_text: str     # Plan text after this rule applied
    is_initial: bool   # True for initial_logical_plan / initial_physical_plan


@dataclass(frozen=True)
class OptimizerTrace:
    """Structured trace of optimizer rule applications."""
    stages: tuple[OptimizerStage, ...]

    @property
    def logical_stages(self) -> tuple[OptimizerStage, ...]:
        return tuple(s for s in self.stages if s.phase == "logical")

    @property
    def physical_stages(self) -> tuple[OptimizerStage, ...]:
        return tuple(s for s in self.stages if s.phase == "physical")

    @property
    def rule_names(self) -> tuple[str, ...]:
        return tuple(s.rule_name for s in self.stages if not s.is_initial)


def parse_optimizer_trace(explain_capture: ExplainCapture) -> OptimizerTrace:
    """Parse EXPLAIN VERBOSE rows into structured optimizer trace.

    Parameters
    ----------
    explain_capture
        Structured EXPLAIN VERBOSE capture from ``capture_explain_sql()``.

    Returns
    -------
    OptimizerTrace
        Structured trace of optimizer stages.
    """
    stages: list[OptimizerStage] = []
    for row in explain_capture.rows:
        plan_type = row.plan_type
        # Initial plans
        if plan_type.startswith("initial_logical_plan"):
            stages.append(OptimizerStage(
                phase="logical", rule_name="initial",
                plan_text=row.plan, is_initial=True,
            ))
            continue
        if plan_type.startswith("initial_physical_plan"):
            stages.append(OptimizerStage(
                phase="physical", rule_name="initial",
                plan_text=row.plan, is_initial=True,
            ))
            continue
        # Rule applications
        match = _RULE_PATTERN.match(plan_type)
        if match is not None:
            phase_raw = match.group("phase")
            phase = "logical" if phase_raw == "logical_plan" else "physical"
            stages.append(OptimizerStage(
                phase=phase, rule_name=match.group("rule"),
                plan_text=row.plan, is_initial=False,
            ))
    return OptimizerTrace(stages=tuple(stages))
```

#### Target Files

| File | Action |
|------|--------|
| `src/datafusion_engine/plan/optimizer_trace.py` | **Create** — new module for `OptimizerStage`, `OptimizerTrace`, `parse_optimizer_trace()` |
| `src/datafusion_engine/plan/bundle.py` | **Edit** — add `optimizer_trace: OptimizerTrace | None` to `DataFusionPlanBundle.plan_details` when verbose explain is captured |
| `src/datafusion_engine/plan/diagnostics.py` | **Edit** — add trace rule count and rule names to `PlanBundleDiagnostics` |

#### Implementation Checklist

- [ ] Create `optimizer_trace.py` with `OptimizerStage`, `OptimizerTrace`, `parse_optimizer_trace()`
- [ ] Parse `plan_type` column from EXPLAIN VERBOSE into structured stages
- [ ] Distinguish `initial_*` entries from `* after <rule>` entries
- [ ] Wire into `_capture_explain_artifacts()` when `DiagnosticsConfig.explain_verbose` is True
- [ ] Store trace in `plan_details` mapping (JSON-serializable via `to_builtins`)
- [ ] Add diagnostic attributes: `optimizer.logical_rule_count`, `optimizer.physical_rule_count`
- [ ] Add unit tests with representative EXPLAIN VERBOSE output
- [ ] Handle graceful degradation: if parsing fails, emit warning and continue with `None`

#### Deprecation / Deletion

None — this is a new module.

---

### Scope Item 5: Statistics Quality Assessment

**Status:** No verification that statistics exist in the physical plan. The Python
`ExecutionPlan` wrapper exposes only `children`, `display`, `display_indent`,
`partition_count` (confirmed via live API inspection — NO `.statistics()` method).
Must use `EXPLAIN` with `show_statistics=true` and parse the text output.

**API verification (confirmed):**
```python
ctx.sql("SET datafusion.explain.show_statistics = true")
result = ctx.sql("EXPLAIN " + sql).collect()
# physical plan text includes:
# "statistics=[Rows=Inexact(3), Bytes=Inexact(624), [(Col[0]: ...)]]"
# or "statistics=[Rows=Absent, Bytes=Absent]" when missing
```

#### Representative Code Snippet

```python
# New addition to: src/datafusion_engine/plan/profiler.py

import re
from enum import Enum

class StatsQuality(Enum):
    """Quality assessment for plan statistics availability."""
    EXACT = "exact"
    INEXACT = "inexact"
    ABSENT = "absent"
    MIXED = "mixed"
    UNKNOWN = "unknown"

_STATS_PATTERN = re.compile(r"statistics=\[Rows=(\w+)")

def assess_stats_quality(physical_plan_text: str) -> StatsQuality:
    """Assess statistics quality from physical plan EXPLAIN text.

    Parameters
    ----------
    physical_plan_text
        Physical plan text from ``EXPLAIN`` with ``show_statistics=true``.

    Returns
    -------
    StatsQuality
        Overall statistics quality across all plan operators.
    """
    matches = _STATS_PATTERN.findall(physical_plan_text)
    if not matches:
        return StatsQuality.UNKNOWN

    qualities: set[str] = set()
    for match_text in matches:
        if match_text.startswith("Exact"):
            qualities.add("exact")
        elif match_text.startswith("Inexact"):
            qualities.add("inexact")
        elif match_text == "Absent":
            qualities.add("absent")

    if len(qualities) == 1:
        q = next(iter(qualities))
        if q == "exact":
            return StatsQuality.EXACT
        if q == "inexact":
            return StatsQuality.INEXACT
        return StatsQuality.ABSENT
    if "absent" in qualities:
        return StatsQuality.MIXED
    return StatsQuality.INEXACT


def capture_explain_with_stats(
    ctx: SessionContext,
    sql: str,
) -> tuple[ExplainCapture | None, StatsQuality]:
    """Capture EXPLAIN with statistics and assess quality.

    Parameters
    ----------
    ctx
        DataFusion session context.
    sql
        SQL statement to explain.

    Returns
    -------
    tuple[ExplainCapture | None, StatsQuality]
        Explain capture and assessed statistics quality.
    """
    try:
        ctx.sql("SET datafusion.explain.show_statistics = true")
    except Exception:
        return None, StatsQuality.UNKNOWN

    capture = capture_explain_sql(ctx, sql, verbose=False, analyze=False)
    if capture is None:
        return None, StatsQuality.UNKNOWN

    physical_text = capture.physical_plan
    if physical_text is None:
        return capture, StatsQuality.UNKNOWN

    quality = assess_stats_quality(physical_text)
    return capture, quality
```

#### Target Files

| File | Action |
|------|--------|
| `src/datafusion_engine/plan/profiler.py` | **Edit** — add `StatsQuality`, `assess_stats_quality()`, `capture_explain_with_stats()` |
| `src/datafusion_engine/plan/bundle.py` | **Edit** — call stats assessment after explain capture; store quality in `plan_details` |
| `src/datafusion_engine/plan/diagnostics.py` | **Edit** — add `stats_quality: str | None` to `PlanBundleDiagnostics` for OTel span |

#### Implementation Checklist

- [ ] Add `StatsQuality` enum to profiler module
- [ ] Add `assess_stats_quality()` that parses `statistics=[Rows=...]` from explain text
- [ ] Add `capture_explain_with_stats()` that temporarily sets `show_statistics=true`
- [ ] Ensure `show_statistics` setting is scoped (set before, restore after, or use dedicated config)
- [ ] Wire into bundle building: assess quality and store `"stats_quality"` in `plan_details`
- [ ] Log warning when `StatsQuality.ABSENT` or `StatsQuality.MIXED` for plans with joins
- [ ] Add diagnostic attribute `plan.stats_quality` to OTel spans
- [ ] Add unit tests with representative explain output containing statistics annotations
- [ ] Handle graceful degradation: if SET command fails, return `UNKNOWN`

#### Deprecation / Deletion

None — additive feature on top of the rewritten profiler.

---

### Scope Item 7: Physical Plan Property Extraction

**Status:** The `execution_plan` object is captured but no structured properties are
extracted. Python's `ExecutionPlan` exposes only: `children()`, `display()`,
`display_indent()`, `partition_count()`, `from_proto()`, `to_proto()`. No structured
access to operator type, distribution, or ordering. Must parse `display_indent()` text.

**API verification (confirmed):**
```python
plan = df.execution_plan()
plan.partition_count()  # int
plan.display_indent()   # multi-line text with operator names and indent structure
# Example output:
# "CoalescePartitionsExec\n  SortExec: ...\n    ProjectionExec: ...\n      ParquetExec: ..."
```

#### Representative Code Snippet

```python
# New: src/datafusion_engine/plan/physical_analysis.py

import re
from dataclasses import dataclass

_OPERATOR_PATTERN = re.compile(r"^\s*(\w+Exec\w*)")
_INDENT_PATTERN = re.compile(r"^(\s*)")

@dataclass(frozen=True)
class PhysicalPlanProperties:
    """Structured properties extracted from a physical plan."""
    partition_count: int
    operator_counts: dict[str, int]
    sort_exec_count: int
    repartition_exec_count: int
    coalesce_exec_count: int
    hash_join_exec_count: int
    scan_exec_count: int
    total_operator_count: int
    max_depth: int


def extract_physical_properties(plan: object) -> PhysicalPlanProperties | None:
    """Extract structured properties from an ExecutionPlan.

    Parameters
    ----------
    plan
        DataFusion ExecutionPlan object.

    Returns
    -------
    PhysicalPlanProperties | None
        Structured properties, or None if extraction fails.
    """
    partition_count_fn = getattr(plan, "partition_count", None)
    if not callable(partition_count_fn):
        return None
    display_indent_fn = getattr(plan, "display_indent", None)
    if not callable(display_indent_fn):
        return None

    try:
        partition_count = partition_count_fn()
    except (RuntimeError, TypeError, ValueError):
        partition_count = 0

    try:
        indent_text = str(display_indent_fn())
    except (RuntimeError, TypeError, ValueError):
        return None

    return _parse_physical_properties(indent_text, partition_count=partition_count)


def _parse_physical_properties(
    indent_text: str,
    *,
    partition_count: int,
) -> PhysicalPlanProperties:
    operator_counts: dict[str, int] = {}
    max_depth = 0

    for line in indent_text.splitlines():
        if not line.strip():
            continue
        indent_match = _INDENT_PATTERN.match(line)
        if indent_match:
            depth = len(indent_match.group(1)) // 2
            max_depth = max(max_depth, depth)
        op_match = _OPERATOR_PATTERN.match(line.strip())
        if op_match:
            op_name = op_match.group(1)
            operator_counts[op_name] = operator_counts.get(op_name, 0) + 1

    return PhysicalPlanProperties(
        partition_count=partition_count,
        operator_counts=operator_counts,
        sort_exec_count=sum(v for k, v in operator_counts.items() if "Sort" in k),
        repartition_exec_count=sum(
            v for k, v in operator_counts.items() if "Repartition" in k
        ),
        coalesce_exec_count=sum(
            v for k, v in operator_counts.items() if "Coalesce" in k
        ),
        hash_join_exec_count=sum(
            v for k, v in operator_counts.items() if "HashJoin" in k
        ),
        scan_exec_count=sum(
            v for k, v in operator_counts.items()
            if k.endswith("ScanExec") or "Parquet" in k
        ),
        total_operator_count=sum(operator_counts.values()),
        max_depth=max_depth,
    )
```

#### Target Files

| File | Action |
|------|--------|
| `src/datafusion_engine/plan/physical_analysis.py` | **Create** — new module for `PhysicalPlanProperties` and `extract_physical_properties()` |
| `src/datafusion_engine/plan/bundle.py` | **Edit** — call `extract_physical_properties()` on execution plan, store in `plan_details` |
| `src/datafusion_engine/plan/diagnostics.py` | **Edit** — add physical property counts to OTel diagnostic spans |

#### Implementation Checklist

- [ ] Create `physical_analysis.py` with `PhysicalPlanProperties` dataclass
- [ ] Implement `extract_physical_properties()` using `display_indent()` text parsing
- [ ] Parse operator names from indent text (pattern: `<OperatorNameExec>`)
- [ ] Count sort, repartition, coalesce, hash join, scan operators
- [ ] Compute max tree depth from indentation
- [ ] Wire into bundle building after execution plan construction
- [ ] Store properties as `"physical_properties"` in `plan_details` mapping
- [ ] Add OTel span attributes: `plan.partition_count`, `plan.sort_count`, `plan.scan_count`
- [ ] Add unit tests with representative `display_indent()` output
- [ ] Handle graceful degradation: return `None` if `display_indent()` fails

#### Deprecation / Deletion

None — new module.

---

### Scope Item 8: Physical Plan Regression Detection

**Status:** Plan determinism validation (`artifact_store.py::validate_plan_determinism()`)
checks logical plan fingerprints but not physical plan topology. A change in DataFusion
version or session config can silently alter the physical plan.

**Depends on:** Scope Item 7 (physical properties extraction).

#### Representative Code Snippet

```python
# Addition to: src/datafusion_engine/plan/physical_analysis.py

def physical_topology_hash(properties: PhysicalPlanProperties) -> str:
    """Compute a stable hash of physical plan topology.

    The topology hash captures operator type sequence and counts.
    Changes in this hash without logical plan changes indicate a
    configuration or engine version regression.

    Parameters
    ----------
    properties
        Physical plan properties from ``extract_physical_properties()``.

    Returns
    -------
    str
        Hex digest of the topology hash.
    """
    # Canonical representation: sorted operator counts + partition count
    payload = (
        ("partition_count", properties.partition_count),
        ("operators", tuple(sorted(properties.operator_counts.items()))),
        ("max_depth", properties.max_depth),
    )
    return hash_msgpack_canonical(payload)
```

#### Target Files

| File | Action |
|------|--------|
| `src/datafusion_engine/plan/physical_analysis.py` | **Edit** — add `physical_topology_hash()` |
| `src/datafusion_engine/plan/bundle.py` | **Edit** — compute and store topology hash in `plan_details` |
| `src/datafusion_engine/plan/artifact_store.py` | **Edit** — add optional topology hash comparison in `validate_plan_determinism()` |
| `src/serde_artifacts.py` | **Edit** — add `physical_topology_hash: str | None` field to `PlanArtifactRow` |

#### Implementation Checklist

- [ ] Add `physical_topology_hash()` to `physical_analysis.py`
- [ ] Hash operator counts + partition count as canonical topology signature
- [ ] Store `"physical_topology_hash"` in `plan_details` and `PlanArtifactRow`
- [ ] Extend `validate_plan_determinism()` to optionally compare topology hashes
- [ ] When topology hash diverges but logical plan hash is unchanged, emit a warning diagnostic
- [ ] Add unit tests verifying topology hash stability
- [ ] Handle graceful degradation: topology hash is `None` when physical analysis unavailable

#### Deprecation / Deletion

None — additive to existing determinism validation.

---

### Scope Item 9: Scan Pushdown Verification

**Status:** `ScanLineage` captures `pushed_filters` from `TableScan.filters` in the
optimized logical plan, and `LineageReport.filters` captures filter expressions. However,
there is no comparison between scan-level pushed filters and residual `Filter` nodes to
detect pushdown failures.

**Key existing code (from `lineage/datafusion.py`):**
- `_extract_scan_lineage()` extracts `TableScan.filters` → `ScanLineage.pushed_filters`
- `_filters_from_exprs()` collects all `Filter` predicates into `LineageReport.filters`
- The `walk_logical_complete()` walker visits all plan nodes including `Filter` and `TableScan`

#### Representative Code Snippet

```python
# Addition to: src/datafusion_engine/lineage/datafusion.py

@dataclass(frozen=True)
class PushdownAnalysis:
    """Per-scan pushdown analysis comparing scan filters vs residual filters."""
    dataset_name: str
    pushed_filter_count: int
    residual_filter_count: int
    fully_pushed: bool


def analyze_pushdown(lineage: LineageReport) -> tuple[PushdownAnalysis, ...]:
    """Compare scan-level pushed filters with residual filter expressions.

    Parameters
    ----------
    lineage
        Lineage report from ``extract_lineage()``.

    Returns
    -------
    tuple[PushdownAnalysis, ...]
        Per-scan pushdown analysis.
    """
    # Count filter expressions that reference each dataset
    residual_filters_per_dataset: dict[str, int] = {}
    for expr in lineage.exprs:
        if expr.kind != "Filter":
            continue
        # Each referenced column maps to a dataset
        for table, _col in expr.referenced_columns:
            residual_filters_per_dataset[table] = (
                residual_filters_per_dataset.get(table, 0) + 1
            )

    analyses: list[PushdownAnalysis] = []
    for scan in lineage.scans:
        pushed = len(scan.pushed_filters)
        residual = residual_filters_per_dataset.get(scan.dataset_name, 0)
        analyses.append(PushdownAnalysis(
            dataset_name=scan.dataset_name,
            pushed_filter_count=pushed,
            residual_filter_count=residual,
            fully_pushed=residual == 0,
        ))
    return tuple(analyses)
```

#### Target Files

| File | Action |
|------|--------|
| `src/datafusion_engine/lineage/datafusion.py` | **Edit** — add `PushdownAnalysis` and `analyze_pushdown()` |
| `src/datafusion_engine/plan/bundle.py` | **Edit** — call `analyze_pushdown()` after lineage extraction, store in `plan_details` |
| `src/datafusion_engine/plan/diagnostics.py` | **Edit** — log diagnostic when `residual_filter_count > 0` |

#### Implementation Checklist

- [ ] Add `PushdownAnalysis` dataclass to `lineage/datafusion.py`
- [ ] Implement `analyze_pushdown()` comparing pushed vs residual filters per scan
- [ ] Track filter expression references to datasets via `expr.referenced_columns`
- [ ] Wire into bundle building: call after `extract_lineage()`, store in `plan_details`
- [ ] Add diagnostic when any scan has `fully_pushed=False` (residual filters remain)
- [ ] Add OTel span attribute `plan.pushdown.fully_pushed` as boolean
- [ ] Add unit tests with plans containing both pushed and residual filters
- [ ] Handle graceful degradation: empty analysis when lineage has no filters

#### Deprecation / Deletion

None — additive to existing lineage extraction.

---

### Scope Item 13 (Targeted): Pruning Predicate Observability

**What remains:** `ScanUnit` tracks file counts but not the actual pruning predicates.
No diagnostic alert when pruning ratio is poor (>50% of files scanned).

#### Representative Code Snippet

```python
# Targeted addition to diagnostics, not ScanUnit schema change:

def _log_pruning_diagnostics(scan_units: tuple[ScanUnit, ...]) -> None:
    """Emit diagnostic when pruning ratio is poor."""
    for unit in scan_units:
        if unit.total_files == 0:
            continue
        ratio = unit.candidate_file_count / unit.total_files
        if ratio > 0.5 and unit.total_files > 10:
            logger.warning(
                "Poor file pruning for %s: %d/%d files selected (%.0f%%)",
                unit.dataset_name,
                unit.candidate_file_count,
                unit.total_files,
                ratio * 100,
            )
```

#### Target Files

| File | Action |
|------|--------|
| `src/datafusion_engine/plan/bundle.py` | **Edit** — add pruning ratio diagnostic after scan unit construction |
| `src/datafusion_engine/plan/diagnostics.py` | **Edit** — add `scan.pruning_ratio` OTel attribute per scan unit |

#### Implementation Checklist

- [ ] Add pruning ratio check after scan units are computed in `build_plan_bundle()`
- [ ] Emit warning diagnostic when `candidate_file_count / total_files > 0.5` and `total_files > 10`
- [ ] Add OTel span attributes: `scan.<dataset>.pruning_ratio`, `scan.<dataset>.total_files`
- [ ] No schema changes to `ScanUnit` (file counts already present)

#### Deprecation / Deletion

None.

---

### Scope Item 14 (Meta-Gap): Production Plan Bundle Completeness

This is resolved by implementing Scope Items 3, 4, 5, 7, 8, 9. After implementation, a
production plan bundle will contain:

| Component | Source | Status |
|-----------|--------|--------|
| P0 (logical plan) | `DataFusionPlanBundle.logical_plan` | ✅ Exists |
| P1 (optimized logical) | `DataFusionPlanBundle.optimized_logical_plan` | ✅ Exists |
| P2 (physical plan) | `DataFusionPlanBundle.execution_plan` | ✅ Exists |
| Substrait bytes | `DataFusionPlanBundle.substrait_bytes` | ✅ Exists |
| Plan fingerprint | `DataFusionPlanBundle.plan_fingerprint` | ✅ Exists |
| Session settings | `PlanArtifacts.df_settings` | ✅ Exists |
| Planning env hash | `PlanArtifacts.planning_env_hash` | ✅ Exists |
| Rulepack snapshot | `PlanArtifacts.rulepack_snapshot` | ✅ Exists |
| EXPLAIN rows | `PlanArtifacts.explain_tree_rows` | 🔨 Scope Item 3 |
| EXPLAIN VERBOSE rows | `PlanArtifacts.explain_verbose_rows` | 🔨 Scope Item 3 |
| Optimizer trace | `plan_details["optimizer_trace"]` | 🔨 Scope Item 4 |
| Stats quality | `plan_details["stats_quality"]` | 🔨 Scope Item 5 |
| Physical properties | `plan_details["physical_properties"]` | 🔨 Scope Item 7 |
| Topology hash | `plan_details["physical_topology_hash"]` | 🔨 Scope Item 8 |
| Pushdown analysis | `plan_details["pushdown_analysis"]` | 🔨 Scope Item 9 |
| Delta file pruning | `ScanUnit.total_files/candidate_file_count` | ✅ Exists |

---

## Implementation Priority and Dependencies

```
Phase 1: Scope Item 3 (Enable explain capture / rewrite profiler)
  No dependencies. Foundation for all subsequent items.
  HIGHEST PRIORITY — currently dead code producing zero value.

Phase 2: Scope Items 4, 5 (Optimizer trace + Stats quality)
  Depends on Phase 1 (structured explain capture).
  Can be implemented in parallel with each other.

Phase 3: Scope Items 7, 8 (Physical properties + Topology regression)
  Item 8 depends on Item 7.
  Independent of Phase 2.

Phase 4: Scope Item 9 (Pushdown verification)
  Independent — uses existing lineage, not explain.

Phase 5: Scope Item 13 (Pruning diagnostics)
  Independent — small targeted improvement.
```

### Suggested Implementation Order

| Order | Item | Effort | Impact | Description |
|-------|------|--------|--------|-------------|
| 1 | **3** | Medium | **Critical** | Rewrite profiler; enable EXPLAIN capture |
| 2 | **4** | Small | High | Parse EXPLAIN VERBOSE into optimizer trace |
| 3 | **5** | Small | High | Statistics quality assessment |
| 4 | **7** | Medium | Medium | Physical plan property extraction |
| 5 | **8** | Small | Medium | Physical topology regression detection |
| 6 | **9** | Medium | High | Pushdown verification in lineage |
| 7 | **13** | Small | Low | Pruning ratio diagnostics |

---

## New Files Created

| File | Scope Item | Purpose |
|------|------------|---------|
| `src/datafusion_engine/plan/optimizer_trace.py` | 4 | Optimizer rule trace parsing |
| `src/datafusion_engine/plan/physical_analysis.py` | 7, 8 | Physical plan property extraction and topology hashing |

## Files Modified

| File | Scope Items | Changes |
|------|-------------|---------|
| `src/datafusion_engine/plan/profiler.py` | 3, 5 | Complete rewrite: SQL-based explain + stats quality |
| `src/datafusion_engine/plan/bundle.py` | 3, 4, 5, 7, 8, 9, 13 | Wire new capabilities into bundle building |
| `src/datafusion_engine/plan/diagnostics.py` | 4, 5, 7, 9, 13 | Add diagnostic attributes to OTel spans |
| `src/datafusion_engine/lineage/datafusion.py` | 9 | Add pushdown analysis |
| `src/datafusion_engine/plan/artifact_store.py` | 8 | Optional topology hash comparison |
| `src/serde_artifacts.py` | 8 | Add `physical_topology_hash` to `PlanArtifactRow` |

## Complete Deprecation / Deletion List

| Target | File | Scope Item | Rationale |
|--------|------|------------|-----------|
| `_run_explain_text()` | `profiler.py` | 3 | Stdout capture replaced by SQL interface |
| `_plan_has_partitions()` | `profiler.py` | 3 | Partition guard not needed for SQL approach |
| `_parse_duration_ms()` | `profiler.py` | 3 | Text parsing replaced by structured access |
| `_parse_output_rows()` | `profiler.py` | 3 | Text parsing replaced by structured access |
| `_DURATION_RE` regex | `profiler.py` | 3 | No longer needed |
| `_OUTPUT_ROWS_RE` regex | `profiler.py` | 3 | No longer needed |
| `CODEANATOMY_DISABLE_DF_EXPLAIN` env var | `profiler.py` | 3 | Feature gated by `DiagnosticsConfig` instead |

---

## Architectural Principles (Unchanged from v1)

1. **Additive, not disruptive:** All new fields are optional with sensible defaults.
2. **Inference-driven:** New capabilities produce signals for the scheduling pipeline.
3. **Fingerprint-inclusive:** New planning inputs affecting shape are in the fingerprint.
4. **Observable:** New capabilities produce artifacts persistable to Delta.
5. **Graceful degradation:** Failures produce `None`/`UNKNOWN`, not exceptions.
6. **msgspec contracts:** Cross-module contracts use `msgspec.Struct`.
7. **No custom optimizer rules (yet):** Focus on observing and configuring DataFusion's
   built-in optimizer, not writing custom rules.
