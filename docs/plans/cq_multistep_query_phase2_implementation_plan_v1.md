# cq Multi-Step Query (Run) Phase 2 Implementation Plan v1

This plan introduces a first‑class **multi-step CQ execution** command (`cq run`) that can orchestrate:
- Smart Search (`cq search` pipeline)
- Declarative queries (`cq q ...` entity + pattern queries)
- Existing analysis macros (`calls`, `impact`, `imports`, `exceptions`, `sig-impact`, `side-effects`, `scopes`, `bytecode-surface`)

The design target is the **high‑performance Phase 2** variant: multiple `cq q` steps should share **one repo scan** (and one file tabulation) instead of re-scanning per query.

This plan additionally integrates Cyclopts features that directly improve **LLM agent ergonomics** and **CLI robustness**:
- **User classes + JSON dict/list parsing** for step specifications (`--step`/`--steps` accept a single JSON token)
- **Group validators** for mutual exclusion and “at least one input” contracts
- A Cyclopts-style **command chaining** frontend (`cq chain`) that compiles to the same multi-step engine
- **Lazy-loaded commands** for faster cold start in agent loops

Each scope item includes representative snippets, target files, deprecations/removals, and an implementation checklist. A comprehensive test scope is included at the end.

---

## Status Summary (2026-02-05)

- Scope 1: Pending
- Scope 2: Pending
- Scope 3: Pending
- Scope 4: Pending
- Scope 5: Pending
- Scope 6: Pending
- Scope 7: Pending
- Scope 8: Pending
- Scope 9: Pending
- Scope 10: Pending

---

## Scope 1: Define a Typed `RunPlan` + `RunStep` Spec (msgspec‑first)

**Goal**  
Create a stable, typed plan format for multi-step execution that supports both CLI `--step` usage and `--plan` file loading.

**Approach**
- Add a `RunPlan` + tagged-union `RunStep` model layer.
- Use msgspec structs (fast conversion/validation; frozen/kw-only; omit defaults).
- Keep the spec intentionally narrow: only fields we will actually execute.
- Add `id` as optional in input; if missing, generate deterministic IDs (`{type}_{index}`).

**Representative snippet**

```python
# tools/cq/run/spec.py
from __future__ import annotations

import msgspec


class RunPlan(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    version: int = 1
    steps: tuple["RunStep", ...] = ()
    # Optional global scan restriction applied to *all* steps (safe default: None = repo root)
    in_dir: str | None = None
    exclude: tuple[str, ...] = ()


class RunStepBase(
    msgspec.Struct,
    kw_only=True,
    frozen=True,
    omit_defaults=True,
    # Tagged unions: msgspec uses the tag field for dispatch, but does not
    # materialize it as a Python attribute on the resulting object.
    tag=True,
    tag_field="type",
):
    id: str | None = None


class QStep(RunStepBase, tag="q"):
    query: str


class SearchStep(RunStepBase, tag="search"):
    query: str
    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    in_dir: str | None = None


class CallsStep(RunStepBase, tag="calls"):
    function: str


RunStep = QStep | SearchStep | CallsStep  # + remaining macro steps + per-step filters
```

**Target files**
- `tools/cq/run/spec.py` (new)
- Optional: `tools/cq/run/__init__.py` (new)

**Deprecate/Delete**
- None.

**Implementation checklist**
1. Add `RunPlan` + `RunStep` tagged union with only the fields we execute.
2. Add a helper to fill in missing step IDs deterministically.
3. Add `msgspec.convert(..., strict=True)` conversion entrypoints for dict/TOML inputs.
4. Add `__all__` exports for the spec types.

---

## Scope 2: Add `cq run` CLI Command + Inputs (`--step`, `--plan`)

**Goal**  
Expose multi-step execution directly via the CQ CLI (Cyclopts meta-app), without requiring external scripting.

**Approach**
- Add a new command module `tools/cq/cli_app/commands/run.py`.
- Add `RunParams` in `tools/cq/cli_app/params.py` and a corresponding options struct in `tools/cq/cli_app/options.py`.
- Support:
  - agent-first JSON steps via Cyclopts “user classes”:
    - repeated `--step '{"type":"q",...}'` (JSON object per occurrence)
    - `--steps '[{"type":"q",...}, {"type":"calls",...}]'` (JSON array in one token)
  - `--plan path/to/plan.toml` (team/repeatable)
- `--plan` and `--step` can be combined; steps concatenate in order.
- Error handling: treat plan parse errors as CQ results (like `q` fallback behavior does).
- Cyclopts validators:
  - enforce mutual exclusion or N-of-K constraints (e.g., `--regex` vs `--literal`)
  - enforce “at least one input” for run inputs (`--plan`/`--step`/`--steps`)

**Representative snippet**

```python
# tools/cq/cli_app/step_types.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class QStepCli:
    type: Literal["q"] = "q"
    id: str | None = None
    query: str = ""


@dataclass(frozen=True)
class CallsStepCli:
    type: Literal["calls"] = "calls"
    id: str | None = None
    function: str = ""


@dataclass(frozen=True)
class SearchStepCli:
    type: Literal["search"] = "search"
    id: str | None = None
    query: str = ""
    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    in_dir: str | None = None


RunStepCli = QStepCli | CallsStepCli | SearchStepCli
```

```python
# tools/cq/cli_app/params.py
from pathlib import Path

from cyclopts import Group, Parameter, validators

run_input = Group(
    "Run Input",
    validator=validators.LimitedChoice(min=1, max=3),  # require at least one input
)

@dataclass(kw_only=True)
class RunParams(FilterParams):
    plan: Annotated[
        Path | None,
        Parameter(
            name="--plan",
            group=run_input,
            validator=validators.Path(exists=True, file_okay=True, dir_okay=False),
            help="Path to a run plan TOML file",
        ),
    ] = None

    step: Annotated[
        list[RunStepCli],
        Parameter(
            name="--step",
            group=run_input,
            n_tokens=1,          # one JSON object per occurrence
            accepts_keys=False,  # keep --step as a single-token opaque JSON object (no dotted keys)
            help='Repeatable JSON step object (e.g., \'{"type":"q","query":"..."}\')',
        ),
    ] = field(default_factory=list)

    steps: Annotated[
        list[RunStepCli],
        Parameter(
            name="--steps",
            group=run_input,
            n_tokens=1,          # one JSON array token
            accepts_keys=False,  # keep --steps as a single-token opaque JSON array (no dotted keys)
            help='JSON array of steps (e.g., \'[{"type":"q",...},{"type":"calls",...}]\')',
        ),
    ] = field(default_factory=list)
```

```python
# tools/cq/cli_app/commands/run.py
from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.options import RunOptions, options_from_params
from tools.cq.cli_app.params import RunParams


def run(
    *,
    opts: Annotated[RunParams, Parameter(name="*")],
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    from tools.cq.run.loader import load_run_plan
    from tools.cq.run.runner import execute_run_plan

    if ctx is None:
        raise RuntimeError("Context not injected")

    options = options_from_params(opts, type_=RunOptions)
    plan = load_run_plan(options)
    result = execute_run_plan(plan, ctx)
    return CliResult(result=result, context=ctx, filters=options)
```

**Target files**
- `tools/cq/cli_app/commands/run.py` (new)
- `tools/cq/cli_app/app.py` (register `run`)
- `tools/cq/cli_app/params.py` (add `RunParams`)
- `tools/cq/cli_app/options.py` (add `RunOptions`)
- `tools/cq/cli_app/step_types.py` (new; Cyclopts dataclass union for JSON parsing)
- `tools/cq/run/loader.py` (updated; convert Cyclopts step objects to internal msgspec `RunStep`)
- `tools/cq/cli_app/commands/search.py` (update validators for `--regex` vs `--literal`)

**Deprecate/Delete**
- Avoid expanding dotted subkeys for step inputs in help (by using `accepts_keys=False`).
- (Optional) Deprecate any ad-hoc “`type:value`” step mini-grammar once JSON steps exist.

**Implementation checklist**
1. Implement Cyclopts signature for `run` with `--step` (repeatable JSON objects), `--steps` (JSON array), and `--plan`.
2. Register the command in `tools/cq/cli_app/app.py`.
3. Ensure global options still work (`--root`, `--format`, `--artifact-dir`, etc.).
4. Add help text and examples for `cq run`.
5. Add Cyclopts group validators:
   - `run` input contract: at least one of `--plan`/`--step`/`--steps`
   - `search`: mutual exclusion for `--regex` vs `--literal`

---

## Scope 3: Shared Merge Utility for Multi-Step Results (Unify with Bundles)

**Goal**  
Merge multiple step results into a single `CqResult` in a consistent, high-signal way, while preserving provenance.

**Approach**
- Introduce a shared merge helper used by both:
  - `tools/cq/core/bundles.py` (existing)
  - `cq run` (new)
- Standardize provenance injection in `Finding.details.data`:
  - `source_step`: the step id (e.g., `imports_typing`)
  - `source_macro`: the originating macro (`q`, `search`, `calls`, etc.)
- Prefix section titles with `"{source_step}: {section.title}"` to avoid ambiguity.

**Representative snippet**

```python
# tools/cq/core/merge.py
from __future__ import annotations

from tools.cq.core.schema import CqResult, DetailPayload, Finding, Section


def _clone_with_provenance(f: Finding, *, step_id: str, source_macro: str) -> Finding:
    data = dict(f.details.data)
    data["source_step"] = step_id
    data["source_macro"] = source_macro
    details = DetailPayload(kind=f.details.kind, score=f.details.score, data=data)
    return Finding(
        category=f.category,
        message=f.message,
        anchor=f.anchor,
        severity=f.severity,
        details=details,
    )


def merge_step_results(merged: CqResult, step_id: str, step_result: CqResult) -> None:
    source_macro = step_result.run.macro
    merged.summary.setdefault("steps", []).append(step_id)
    merged.summary.setdefault("step_summaries", {})[step_id] = step_result.summary

    merged.key_findings.extend(
        _clone_with_provenance(f, step_id=step_id, source_macro=source_macro)
        for f in step_result.key_findings
    )
    merged.evidence.extend(
        _clone_with_provenance(f, step_id=step_id, source_macro=source_macro)
        for f in step_result.evidence
    )
    merged.sections.extend(
        Section(
            title=f"{step_id}: {section.title}",
            findings=[
                _clone_with_provenance(f, step_id=step_id, source_macro=source_macro)
                for f in section.findings
            ],
            collapsed=section.collapsed,
        )
        for section in step_result.sections
    )
    merged.artifacts.extend(step_result.artifacts)
```

**Target files**
- `tools/cq/core/merge.py` (new)
- `tools/cq/core/bundles.py` (refactor to use `merge_step_results`)
- `tools/cq/run/runner.py` (new; uses `merge_step_results`)

**Deprecate/Delete**
- Deprecate direct use of `merge_bundle_results` internals once `core/merge.py` exists.
- Delete/inline redundant clone helpers in `tools/cq/core/bundles.py` (after migration).

**Implementation checklist**
1. Introduce `tools/cq/core/merge.py` with `merge_step_results`.
2. Migrate `tools/cq/core/bundles.py:merge_bundle_results` to call shared merge.
3. Ensure `source_step`/`source_macro` are always present on merged findings.
4. Add unit tests for merge behavior (ordering, provenance fields, section prefixing).

---

## Scope 4: Batch Execution Engine for Multiple `cq q` Steps (Single Scan)

**Goal**  
Execute multiple `cq q` steps with **one**:
- file tabulation
- ast-grep scan (facts record collection)
- `ScanContext` build

**Approach**
- Introduce a `BatchEntityQuerySession` that owns:
  - `files`: tabulated `.py` paths
  - `records`: the union `SgRecord` list
  - `scan_ctx`: `ScanContext` (interval index, callers assignment, etc.)
  - `candidates`: `EntityCandidates`
- The session is built once from the union of all `ToolPlan.sg_record_types` and union of scan paths.
- Each `Query` is then executed against the shared session (no rescans).
- Reuse a single `SymtableEnricher` instance for all scope filtering during the batch.

**Representative snippet**

```python
# tools/cq/query/batch.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tools.cq.query.enrichment import SymtableEnricher
from tools.cq.query.executor import _build_scan_context, _build_entity_candidates, _apply_entity_handlers
from tools.cq.query.planner import ToolPlan, scope_to_paths
from tools.cq.query.sg_parser import sg_scan


@dataclass(frozen=True)
class BatchEntityQuerySession:
    root: Path
    files: list[Path]
    records: list[SgRecord]
    scan: ScanContext
    candidates: EntityCandidates
    symtable: SymtableEnricher


def build_batch_session(
    *,
    root: Path,
    plans: list[ToolPlan],
    tc: Toolchain,
) -> BatchEntityQuerySession:
    # Union record types + union scan roots (correctness-first: avoid per-query excludes at scan-time)
    record_types: set[str] = set()
    paths: list[Path] = []
    for plan in plans:
        record_types |= set(plan.sg_record_types)
        paths.extend(scope_to_paths(plan.scope, root))

    records = sg_scan(paths=list({p.resolve() for p in paths}), record_types=record_types, root=root)
    scan = _build_scan_context(records)
    candidates = _build_entity_candidates(scan, records)
    return BatchEntityQuerySession(
        root=root,
        files=[],  # optional: store tabulated files when we thread it through (Scope 5)
        records=records,
        scan=scan,
        candidates=candidates,
        symtable=SymtableEnricher(root),
    )
```

**Target files**
- `tools/cq/query/batch.py` (new)
- `tools/cq/query/executor.py` (extract a public “execute with pre-scanned session” entrypoint)
- `tools/cq/run/runner.py` (execute `q` steps via batch session)

**Deprecate/Delete**
- None immediately.
- After migration: deprecate any ad-hoc “multi q” loops in scripts in favor of `cq run`.

**Implementation checklist**
1. Add `BatchEntityQuerySession` model + `build_batch_session`.
2. Add an executor function that can run a single `Query` against a provided session (no scan).
3. Ensure per-query scope constraints (`in=...`, `exclude=...`, globs) are still honored via record filtering.
4. Reuse one `SymtableEnricher` for scope filtering across all batch queries.
5. Add tests ensuring 2+ queries cause only **one** fact scan.

---

## Scope 5: Batch Relational Constraints (`inside=`, `has=`, metavars) Without Re-Tabulation/Re-Parse

**Goal**  
Preserve correctness and performance when batch-running queries that include relational constraints (which currently trigger additional scans for match-span collection).

**Approach**
- Thread the shared **tabulated file list** into relational matching (avoid repeating `tabulate_files` per query).
- Batch span collection:
  - Parse each file once for span-rule matching across *all queries that need it*.
  - Collect `match_spans_by_query[query_idx][file] -> spans`.
  - When executing each query, filter candidate records by the query’s precomputed spans.
- Prefer correctness-first:
  - Do not apply per-query excludes at scan time.
  - Apply per-query scope/exclude after scan + after span filtering.

**Representative snippet**

```python
# tools/cq/query/batch_spans.py
def collect_span_filters(
    *,
    root: Path,
    files: list[Path],
    queries: list[Query],
    plans: list[ToolPlan],
) -> list[dict[str, list[tuple[int, int]]]]:
    per_query: list[dict[str, list[tuple[int, int]]]] = [dict() for _ in queries]
    rule_sets = [
        plan.sg_rules if plan.sg_rules else ()
        for plan in plans
    ]

    for file_path in files:
        src = file_path.read_text(encoding="utf-8")
        node = SgRoot(src, "python").root()
        rel_path = _normalize_match_file(str(file_path), root)
        for q_idx, rules in enumerate(rule_sets):
            if not rules:
                continue
            for rule in rules:
                for match in _iter_rule_matches_for_spans(node, rule):
                    span = (match.range().start.line + 1, match.range().end.line + 1)
                    per_query[q_idx].setdefault(rel_path, []).append(span)

    return per_query
```

**Target files**
- `tools/cq/query/batch_spans.py` (new) or fold into `tools/cq/query/batch.py`
- `tools/cq/query/executor.py` (refactor to accept precomputed span filters for `_apply_rule_spans`)
- `tools/cq/run/runner.py`

**Deprecate/Delete**
- Deprecate per-query repeated `tabulate_files` inside `_collect_match_spans` for batch runs.

**Implementation checklist**
1. Add a span-filter collector that accepts the already-tabulated `files`.
2. Integrate span-filter usage into the batch session’s per-query execution path.
3. Ensure metavariable filters continue to work (apply them when building span filters).
4. Add tests for batch `inside=`/`has=` constraints that:
   - match results identical to single-query execution
   - avoid repeated tabulation and avoid repeated file parsing where practical

---

## Scope 6: Multi-Step Runner Orchestration (`execute_run_plan`)

**Goal**  
Execute a full mixed plan (search + q + macros) deterministically and produce a single merged output.

**Approach**
- Parse/normalize the `RunPlan`.
- Partition steps:
  - `q` steps execute via shared batch session (Scopes 4-5).
  - other steps execute sequentially (calls, search, etc.).
- Merge step results using `tools/cq/core/merge.py`.
- Provide robust failure behavior:
  - Default: continue on step error, embed error summary as a step finding.
  - Optional: `--stop-on-error`.

**Representative snippet**

```python
# tools/cq/run/runner.py
def execute_run_plan(plan: RunPlan, ctx: CliContext) -> CqResult:
    run_ctx = RunContext.from_parts(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain, started_ms=ms())
    merged = mk_result(run_ctx.to_runmeta("run"))
    merged.summary["plan_version"] = plan.version

    steps = normalize_step_ids(plan.steps)
    q_steps = [s for s in steps if isinstance(s, QStep)]
    other_steps = [s for s in steps if not isinstance(s, QStep)]

    q_results = _execute_q_steps_batch(q_steps, ctx)  # 1 scan
    for step_id, step_result in q_results:
        merge_step_results(merged, step_id, step_result)

    for step in other_steps:
        step_id = step.id or "<unknown_step>"
        step_result = _execute_non_q_step(step, ctx)
        merge_step_results(merged, step_id, step_result)

    return merged
```

**Target files**
- `tools/cq/run/runner.py` (new)
- `tools/cq/run/loader.py` (new; plan file parsing + CLI step parsing)
- `tools/cq/cli_app/commands/run.py` (new; glue)
- `tools/cq/core/merge.py` (new; Scope 3)

**Deprecate/Delete**
- None.

**Implementation checklist**
1. Implement plan loading: TOML via `tomllib`; also support inline JSON `--step` objects and `--steps` arrays.
2. Normalize/validate steps: type-specific required fields, deterministic IDs.
3. Execute batch `q` steps via shared session.
4. Execute non-`q` steps via existing macro entrypoints (`smart_search`, `cmd_calls`, etc.).
5. Merge into one `CqResult` with provenance.
6. Add `--stop-on-error` behavior.

---

## Scope 7: Documentation + Skill Updates

**Goal**  
Make `cq run` discoverable and easy to use for developers/agents.

**Approach**
- Update the CQ skill (`.claude/skills/cq/SKILL.md`) with:
  - `cq run` usage
  - examples for mixed steps
  - plan file example
- Add a short section to `tools/cq/README.md` describing multi-step execution and the performance benefits.

**Representative snippet**

```bash
# Agent-first multi-step via JSON steps
/cq run --steps '[{"type":"search","query":"build_graph_product"}, {"type":"q","query":"entity=function name=build_graph_product expand=callers(depth=2)"}, {"type":"calls","function":"build_graph_product"}]'

# Plan file
/cq run --plan docs/plans/cq_run_example.toml

# Command chaining frontend (segments separated by AND)
/cq chain q "entity=function name=build_graph_product" AND calls build_graph_product AND search build_graph_product
```

**Target files**
- `.claude/skills/cq/SKILL.md`
- `tools/cq/README.md`
- Optional: `docs/plans/cq_run_example.toml` (new example plan)

**Deprecate/Delete**
- None.

**Implementation checklist**
1. Document CLI usage and plan file format.
2. Add examples covering: smart search + q + calls.
3. Document “shared scan” behavior and its constraints.
4. Document JSON step contract for agents (recommended) and the `cq chain` frontend.

---

## Scope 8: Test Scope (Single Query + Multi Query Assurance)

This scope is intentionally comprehensive: `cq run` must not regress existing single-step behavior and must provide strong confidence in correctness + performance properties.

### 8.1 Unit Tests (Core)

**Plan parsing**
- Parse Cyclopts JSON step objects/lists into typed `RunStepCli` variants, then convert to internal msgspec `RunStep`.
- Load TOML plan files into `RunPlan` and validate strict conversion errors are surfaced cleanly.

**Batch scan behavior**
- Monkeypatch the scan primitive (`tools.cq.query.sg_parser.sg_scan` or `tools.cq.astgrep.sgpy_scanner.scan_files`) to count calls.
- Assert:
  - N `q` steps -> **1** fact scan call.
  - With relational constraints in multiple queries -> still **1** tabulation; span scan runs once per file (not per query).

**Representative snippet**

```python
# tests/unit/cq/test_run_batch_scan.py
def test_batch_q_steps_scan_once(monkeypatch, tmp_path: Path) -> None:
    calls: int = 0
    real = tools.cq.query.sg_parser.sg_scan

    def wrapped(*args, **kwargs):
        nonlocal calls
        calls += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(tools.cq.query.sg_parser, "sg_scan", wrapped)

    # Build a tiny repo and run cq run with multiple q steps...
    result = execute_run_plan(plan, ctx)
    assert calls == 1
    assert result.summary["steps"] == ["q_0", "q_1"]
```

**Correctness equivalence**
- For a temporary mini-repo:
  - Run each `q` query individually via `execute_plan`.
  - Run the same queries via batch session.
  - Compare the set of `(message, anchor.file, anchor.line)` for key findings (order can differ).

**Merge/provenance**
- Assert merged findings carry:
  - `details.data["source_step"]`
  - `details.data["source_macro"]`
- Assert section titles are prefixed with step id.

**Target test files**
- `tests/unit/cq/test_run_plan_parsing.py` (new)
- `tests/unit/cq/test_run_batch_scan.py` (new)
- `tests/unit/cq/test_run_merge.py` (new)
- `tests/unit/cq/test_run_validators.py` (new; `--regex` vs `--literal`, run input N-of-K)

### 8.2 CLI Parsing/Registration Tests

- Ensure the Cyclopts app parses `run` tokens and binds options correctly.
- Ensure `--format` and global options still work with `run`.

**Target test files**
- `tests/unit/cq/test_cli_meta_app.py` (extend)
- `tests/unit/cq/test_cli_parsing.py` (extend)

### 8.3 Golden Snapshots (CQ CLI)

Add/extend CQ golden tests to keep help and summary stable:
- `cq run` help output snapshot (similar to `tests/cli_golden/test_search_golden.py::test_search_help`)
- Optional: a minimal `RunMeta` + merged `CqResult` rendered via `render_markdown` to snapshot summary keys (`steps`, `step_summaries`, `plan_version`).

**Target test files**
- `tests/cli_golden/test_run_golden.py` (new)
- `tests/cli_golden/fixtures/` (if adding plan fixtures)

### 8.4 Regression Coverage for Existing Single-Step Behavior

No new tests are needed here beyond ensuring the existing suite stays green, but we should explicitly run:
- `tests/unit/cq/` (covers single `q`, `search`, macros, renderers)
- CQ golden snapshots (`tests/cli_golden/test_search_golden.py` plus the new `run` snapshots)

**Deprecate/Delete**
- None.

**Implementation checklist**
1. Add unit tests for plan parsing, batch scan call counts, and correctness equivalence.
2. Extend CQ CLI parsing tests to include `run`.
3. Add CQ golden snapshots for `cq run --help` and (optionally) a minimal merged summary render.
4. Ensure existing CQ unit tests continue to pass (single-step behavior preserved).

---

## Suggested Implementation Order

1. Scope 1 (spec) + Scope 2 (CLI command skeleton)
2. Scope 3 (merge utility) and refactor bundles to use it
3. Scope 4 (batch fact scan) + basic `run` runner wiring
4. Scope 5 (relational span batching) + correctness tests
5. Scope 9 (command chaining frontend)
6. Scope 10 (lazy-loaded command registration)
7. Scope 7 (docs/skill update)
8. Scope 8 (complete test suite + golden snapshots)

---

## Scope 9: Cyclopts-Style Command Chaining Frontend (`cq chain`)

**Goal**  
Add a delimiter-based “command chaining” frontend that is ergonomic for agents that already know `cq` subcommands, while executing through the same high-performance multi-step engine.

**Approach**
- Implement `cq chain` as a regular CQ command (not a meta launcher behavior change).
- `cq chain` accepts raw tokens, splits on a delimiter token (default: `AND`), and compiles each segment into a `RunStep`.
- Compile strategy:
  - Use Cyclopts parsing (`app.parse_args`) on each segment **without executing**.
  - Convert the parsed command + bound args into a `RunStep` (e.g., `q` → `QStep`, `calls` → `CallsStep`, `search` → `SearchStep`).
- Execute the resulting plan through `execute_run_plan` so `q` steps share one scan.

**Representative snippet**

```python
# tools/cq/cli_app/commands/chain.py
from __future__ import annotations

import itertools
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.run.chain import compile_chain_segments
from tools.cq.run.runner import execute_run_plan


def chain(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    delimiter: Annotated[str, Parameter(name="--delimiter")] = "AND",
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    if ctx is None:
        raise RuntimeError("Context not injected")

    groups = [
        list(g)
        for is_delim, g in itertools.groupby(tokens, lambda t: t == delimiter)
        if not is_delim
    ]
    plan = compile_chain_segments(groups, ctx)
    result = execute_run_plan(plan, ctx)
    return CliResult(result=result, context=ctx)
```

**Target files**
- `tools/cq/cli_app/commands/chain.py` (new)
- `tools/cq/run/chain.py` (new; segment → `RunStep` compilation)
- `tools/cq/cli_app/app.py` (register `chain`)
- `tests/unit/cq/test_chain_compilation.py` (new)

**Deprecate/Delete**
- None.

**Implementation checklist**
1. Define the chain delimiter contract and error handling for empty segments.
2. Implement “segment → RunStep” compilation for `q`, `search`, and `calls` first.
3. Add compilation support for additional CQ macros as step types are added.
4. Add tests asserting:
   - chaining produces the expected steps
   - chaining executes via the shared-scan path for multiple `q` segments

---

## Scope 10: Lazy-Loaded CQ Command Registration (Cyclopts)

**Goal**  
Reduce cold-start time for `cq` invocations in agent loops by lazily importing command modules.

**Approach**
- Register commands via import-path strings instead of importing all command modules at `tools/cq/cli_app/app.py` import time.
- Ensure lazy-loaded commands still work with the meta-app launcher (`ctx` injection) and config chain.
- Define shared `Group(...)` objects in the non-lazy root module (`tools/cq/cli_app/app.py`) so group defaults/validators are applied deterministically.
- Add a unit test that forces command resolution (`app[\"calls\"]`, `app[\"q\"]`, etc.) to catch broken import paths early.

**Representative snippet**

```python
# tools/cq/cli_app/app.py
app.command(\"tools.cq.cli_app.commands.analysis:calls\", group=analysis_group)
app.command(\"tools.cq.cli_app.commands.query:q\", group=analysis_group)
app.command(\"tools.cq.cli_app.commands.search:search\", group=analysis_group)
app.command(\"tools.cq.cli_app.commands.run:run\", group=analysis_group)
app.command(\"tools.cq.cli_app.commands.chain:chain\", group=analysis_group)
```

**Target files**
- `tools/cq/cli_app/app.py` (refactor registration to import-path strings)
- `tests/unit/cq/test_cli_lazy_loading.py` (new)

**Deprecate/Delete**
- Remove eager imports at the bottom of `tools/cq/cli_app/app.py` once lazy registration lands.

**Implementation checklist**
1. Convert each `app.command(...)` registration to import-path string form.
2. Add a smoke test that resolves each command node via `app[\"...\"]`.
3. Add a help-smoke test (optional) if root help needs to render without importing everything.
