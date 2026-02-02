# CQ Agent Reports Implementation Plan v1

## Executive Summary

This plan implements the full set of cq improvements described in the assessment, with a focus on:

- Correctness fixes (relational constraints, expanders, cross-file attribution)
- Better ast-grep integration (scope excludes/globs, single-pass scans)
- Caching and performance (IndexCache + QueryCache)
- Agent-ready report bundles that combine multiple cq macros into a single, rich report
- Higher precision using symtable and dis for call resolution and hazard context

Each scope item below includes representative code snippets, target files, deprecations, and an implementation checklist.

---

## Scope Item 1: Correctness Fixes for Relational Constraints, Expanders, and Cross-File Attribution

### Goals

- Ensure relational constraints on entity queries are executed (not ignored).
- Make expanders actually modify output (callers, callees, raises, imports, scope, bytecode surface).
- Prevent cross-file attribution errors in caller/hazard sections.

### Representative Code Snippets

```python
# tools/cq/query/executor.py

@dataclass(frozen=True)
class FileIntervalIndex:
    """Per-file interval index to avoid cross-file attribution."""

    by_file: dict[str, IntervalIndex]

    @classmethod
    def from_records(cls, records: list[SgRecord]) -> FileIntervalIndex:
        grouped = group_records_by_file(records)
        return cls(
            by_file={path: IntervalIndex.from_records(recs) for path, recs in grouped.items()}
        )

    def find_containing(self, record: SgRecord) -> SgRecord | None:
        index = self.by_file.get(record.file)
        if index is None:
            return None
        return index.find_containing(record.start_line)
```

```python
# tools/cq/query/executor.py

if plan.sg_rules:
    # Execute relational rules and intersect with scanned records
    rule_findings, rule_records, _ = _execute_ast_grep_rules(plan.sg_rules, paths, root, query)
    records = _intersect_records(records, rule_records)
```

```python
# tools/cq/query/executor.py

if "callers" in query.expand:
    result.sections.append(_build_callers_section(...))
if "raises" in query.expand:
    result.sections.append(_build_raises_section(...))
```

### Target Files

- `tools/cq/query/executor.py`
- `tools/cq/query/planner.py`
- `tools/cq/query/ir.py`
- `tools/cq/query/parser.py`
- `tools/cq/query/sg_parser.py`
- `tools/cq/macros/*.py`

### Deprecations / Deletes

- Deprecate single global `IntervalIndex` usage in `_build_callers_section` and `_build_hazards_section`.
- Remove unused expander scaffolding that only adjusted record types without producing output.

### Implementation Checklist

- [x] Add per-file interval index and use it for callers/hazards attribution.
- [x] Execute `plan.sg_rules` for entity queries and intersect results.
- [x] Implement expander output sections for `callers`, `callees`, `raises`, `imports`, `scope`, `bytecode_surface`.
- [x] Add tests to validate relational constraints on entity queries.
- [x] Add regression tests for cross-file caller attribution.

---

## Scope Item 2: Scope Excludes and ast-grep Globs Alignment

### Goals

- Ensure `scope.exclude` is honored for all query types.
- Prefer ast-grep `--globs` for include/exclude matching and ignore handling.
- Keep `rg` prefilter consistent with query scope.

### Representative Code Snippets

```python
# tools/cq/query/sg_parser.py

def sg_scan(
    paths: list[Path],
    record_types: set[str] | None = None,
    config_path: Path | None = None,
    root: Path | None = None,
    globs: list[str] | None = None,
) -> list[SgRecord]:
    cmd = ["ast-grep", "scan", "-c", str(config_path), "--json=stream"]
    if globs:
        for glob in globs:
            cmd.extend(["--globs", glob])
    cmd.extend(str(p) for p in paths)
    ...
```

```python
# tools/cq/query/planner.py

def scope_to_globs(scope: Scope) -> list[str]:
    globs = []
    globs.extend(scope.globs)
    globs.extend([f"!{pattern}" for pattern in scope.exclude])
    return globs
```

### Target Files

- `tools/cq/query/sg_parser.py`
- `tools/cq/query/planner.py`
- `tools/cq/query/executor.py`

### Deprecations / Deletes

- Deprecate direct `Path.glob` expansion for `scope.globs` in `scope_to_paths`.

### Implementation Checklist

- [x] Add `globs` parameter to `sg_scan`.
- [x] Convert `scope.exclude` to ast-grep `!` globs.
- [x] Apply the same excludes to `rg` prefilter (`--glob !...`).
- [x] Add tests for `exclude=tests` and `globs=src/**/*.py`.

---

## Scope Item 3: Single-Pass Pattern Query Execution

### Goals

- Avoid N scans for N rules in pattern queries.
- Use a single inline ruleset and split results by `ruleId`.

### Representative Code Snippets

```python
# tools/cq/query/executor.py

def _execute_ast_grep_rules(
    rules: tuple[AstGrepRule, ...],
    paths: list[Path],
    root: Path,
    query: Query | None = None,
) -> tuple[list[Finding], list[SgRecord], list[dict]]:
    rule_doc = _build_inline_rules_doc(rules)
    rule_file = _write_temp_rule_file(rule_doc)
    ...
```

```python
# tools/cq/query/executor.py

def _build_inline_rules_doc(rules: tuple[AstGrepRule, ...]) -> dict:
    return {
        "rules": [
            {
                "id": f"pattern_{i}",
                "language": "python",
                "rule": rule.to_yaml_dict(),
                "message": "Pattern match",
            }
            for i, rule in enumerate(rules)
        ]
    }
```

### Target Files

- `tools/cq/query/executor.py`

### Deprecations / Deletes

- Remove the per-rule scan loop inside `_execute_pattern_query`.

### Implementation Checklist

- [x] Create a multi-rule inline YAML document.
- [x] Execute one ast-grep scan and fan-out by rule id.
- [x] Preserve metavar filtering and match metadata.
- [x] Add unit tests for multi-rule pattern queries.

---

## Scope Item 4: Query and Index Cache Integration

### Goals

- Use `IndexCache` to reuse ast-grep scan output.
- Use `QueryCache` to store `CqResult` for repeated queries.
- Add cache visibility in report metadata.

### Representative Code Snippets

```python
# tools/cq/query/executor.py

@dataclass(frozen=True)
class QueryCacheKey:
    query: str
    root: str
    toolchain: dict[str, object]
    plan_signature: str


def _execute_with_cache(..., cache: QueryCache | None) -> CqResult:
    if cache is not None:
        cached = cache.get(key)
        if cached is not None:
            return cached
    result = _execute_uncached(...)
    if cache is not None:
        cache.set(key, result)
    return result
```

```python
# tools/cq/query/executor.py

records = sg_scan(..., cache=index_cache)
```

### Target Files

- `tools/cq/query/executor.py`
- `tools/cq/index/sqlite_cache.py`
- `tools/cq/index/query_cache.py`
- `tools/cq/cli.py`

### Deprecations / Deletes

- Deprecate direct `sg_scan` calls that ignore the index cache.

### Implementation Checklist

- [x] Add `--cache/--no-cache` CLI flags for `q`.
- [x] Implement `QueryCacheKey` with stable plan signature.
- [x] Route all `q` executions through cache-aware executor.
- [x] Surface cache hits in `result.summary`.
- [x] Add tests verifying cache hit behavior.

---

## Scope Item 5: Agent-Ready Report Bundles

### Goals

- Add a top-level `cq report <preset>` command.
- Provide single-command, rich outputs for common agent tasks.
- Aggregate multiple macros into one structured report.

### Report Presets

1. **Refactor Impact Report**: `calls + sig-impact + imports + exceptions + side-effects`
2. **Safety & Reliability Report**: `async-hazards + hazards patterns + exceptions + side-effects`
3. **Change Propagation Report**: `impact + calls + bytecode-surface + scopes`
4. **Dependency Health Report**: `imports --cycles + side-effects + scopes`

### Representative Code Snippets

```python
# tools/cq/core/bundles.py

@dataclass(frozen=True)
class ReportBundle:
    name: str
    description: str
    steps: tuple[BundleStep, ...]


def run_bundle(bundle: ReportBundle, ctx: BundleContext) -> CqResult:
    results = [step.execute(ctx) for step in bundle.steps]
    return merge_results(results, bundle)
```

```python
# tools/cq/cli.py

report_parser = subparsers.add_parser("report")
report_parser.add_argument("preset", choices=[...])
report_parser.set_defaults(func=_cmd_report_cli)
```

### Target Files

- `tools/cq/cli.py`
- `tools/cq/core/report.py`
- `tools/cq/core/schema.py`
- `tools/cq/core/bundles.py` (new)
- `tools/cq/macros/*.py`

### Deprecations / Deletes

- No removals required; report bundles are additive.

### Implementation Checklist

- [x] Define bundle schema and runner.
- [x] Implement `cq report <preset>` CLI entrypoint.
- [x] Merge bundle results into a single report with subsections per macro.
- [x] Add JSON artifact outputs per bundle run.
- [x] Add docs and examples in `tools/cq/README.md`.

---

## Scope Item 6: Precision Improvements Using symtable + dis + ast-grep Relational Rules

### Goals

- Tighten call resolution and reduce false positives.
- Improve hazard detection using structural context.
- Use bytecode evidence to backfill callsite details.

### Representative Code Snippets

```python
# tools/cq/query/enrichment.py

def enrich_call_targets(
    record: SgRecord,
    scope_info: SymtableInfo,
    bytecode: BytecodeInfo,
) -> dict[str, object]:
    return {
        "resolved_globals": scope_info.globals_used,
        "bytecode_calls": bytecode.call_functions,
    }
```

```python
# tools/cq/query/hazards.py

HAZARD_RULES = (
    AstGrepRule(pattern="subprocess.$M($$$, shell=True)"),
    AstGrepRule(pattern="eval($X)", inside="def $F($$$): $$$"),
)
```

### Target Files

- `tools/cq/query/enrichment.py`
- `tools/cq/query/executor.py`
- `tools/cq/query/hazards.py`
- `tools/cq/macros/async_hazards.py`

### Deprecations / Deletes

- Deprecate heuristic-only callsite matching where dis/symtable evidence is available.

### Implementation Checklist

- [x] Add symtable + dis evidence to call findings when available.
- [x] Use relational ast-grep rules for hazard context (inside async, inside try, etc.).
- [x] Add tests validating improved precision for method name collisions.

---

## Global Acceptance Criteria

- All relational constraints are honored for entity queries.
- Expanders produce actual report sections.
- No cross-file attribution errors in callers/hazards.
- Pattern queries execute in a single ast-grep scan.
- Scope excludes are respected by ast-grep and rg prefilter.
- Cache integration reduces repeated scan costs.
- Report bundles produce single-command, agent-usable outputs.

## Status Notes (Post-Implementation)

- Cache key stability is implemented via `_build_query_cache_key` (function-based) rather than a `QueryCacheKey` dataclass; behavior matches the intent.
- Scope include/exclude tests use CQ fixtures paths rather than `src/**/*.py` literals; semantics covered.
- Multi-rule pattern execution is covered by E2E inline-rule tests rather than a unit-only test.
- Hazard rules now include async and try-context hazards using relational constraints.

---

## Implementation Notes

- Keep all new code aligned with ruff/pyrefly/pyright strict rules.
- Use `uv run` for any Python tool invocation.
- Prefer ast-grep `--globs` over manual file expansion for accuracy and consistency.
