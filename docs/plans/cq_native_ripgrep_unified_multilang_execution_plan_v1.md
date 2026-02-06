# CQ Native Ripgrep + Unified Python/Rust Query Execution Plan v1

## Summary

This plan defines the implementation scopes to migrate `tools/cq` from `rpygrep` to native `rg` process execution, unify Python and Rust search/query flows, preserve Python-first behavior, and deliver simultaneous cross-language output with explicit diagnostics when Python-oriented requests only match Rust.

Primary outcomes:
- CQ search/query execution uses native `rg` only (no `rpygrep` runtime dependency).
- One unified search path can execute Python and Rust together.
- Python remains the default and highest-priority language in ranking and UX.
- Output includes language-partitioned evidence and cross-language diagnostics.
- CQ remains independent of `src/` and all work stays under `tools/cq` + CQ tests/docs.

---

## Locked Design Decisions

1. Python-first policy stays explicit:
   - Default execution includes Python first.
   - Ranking and follow-up suggestions prioritize Python findings.
2. Unified language mode is the target default:
   - Search and query execution support `lang=auto` (Python + Rust in one run).
3. CQ codebase remains self-contained:
   - No imports from `src/`.
4. Native ripgrep process layer is the only search backend:
   - Remove `rpygrep` usage and dependency after parity.
5. Tree-sitter direct usage is optional enrichment only:
   - Core correctness remains from `rg` + ast-grep pipeline.

---

## Scope 0 - Python Regression Firewall and Baseline Capture

### Goal
Create migration safety rails so Python behavior does not regress while backend architecture changes.

### Representative code pattern

```python
# tests/e2e/cq/test_query_regression.py

def test_python_default_behavior_stable(run_query) -> None:
    before = run_query("entity=function name=build_graph")
    after = run_query("entity=function name=build_graph")
    assert normalize_summary(before.summary) == normalize_summary(after.summary)
    assert normalize_findings(before.key_findings) == normalize_findings(after.key_findings)
```

### Target files
- `tests/unit/cq/search/test_smart_search.py`
- `tests/unit/cq/test_query_parser.py`
- `tests/e2e/cq/test_query_regression.py`
- `tests/cli_golden/test_search_golden.py`

### Deprecate/delete after completion
- None (this scope adds guardrails).

### Implementation checklist
- [ ] Add Python default-path regression tests for `search`, `q`, and `run`.
- [ ] Add snapshot-normalization helpers to avoid brittle timestamp/toolchain failures.
- [ ] Add explicit assertion that omitted language preserves Python semantics.

---

## Scope 1 - Native `rg` Process Core (Backend Foundation)

### Goal
Implement a CQ-native ripgrep execution module that handles command construction, process execution, JSON line decoding, limits, and error normalization.

### Representative code pattern

```python
# tools/cq/search/rg_native.py

def run_rg_json(
    *,
    root: Path,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str],
    exclude_globs: list[str],
    limits: SearchLimits,
) -> RgProcessResult:
    cmd = ["rg", "--json", "--line-number", "--column", "--max-count", str(limits.max_matches_per_file)]
    cmd += ["--max-depth", str(limits.max_depth), "--max-filesize", str(limits.max_file_size_bytes)]
    for rg_type in lang_types:
        cmd += ["--type", rg_type]
    for glob in include_globs:
        cmd += ["-g", glob]
    for glob in exclude_globs:
        cmd += ["-g", f"!{glob}"]
    if mode is QueryMode.LITERAL:
        cmd += ["-F"]
    cmd += ["-e", pattern, "."]
    return stream_rg_events(cmd=cmd, cwd=root, timeout_seconds=limits.timeout_seconds)
```

### Target files
- New `tools/cq/search/rg_native.py`
- `tools/cq/search/rg_events.py`
- `tools/cq/search/collector.py`
- `tools/cq/search/timeout.py`

### Deprecate/delete after completion
- `rpygrep` import usage patterns in:
  - `tools/cq/search/smart_search.py`
  - `tools/cq/search/adapter.py`

### Implementation checklist
- [ ] Add deterministic `rg` command builder with `-e` pattern safety.
- [ ] Implement process runner with timeout and structured stderr handling.
- [ ] Reuse existing JSON event decoder and collector where possible.
- [ ] Add capability probe helpers (`rg --version`, `rg --type-list`).
- [ ] Add unit tests for command generation and event parsing, including non-UTF8 event payloads.

---

## Scope 2 - Search Adapter and Smart Search Migration to Native `rg`

### Goal
Refactor all search candidate generation and adapter helpers to use `rg_native`, with no `RipGrepSearch` objects.

### Representative code pattern

```python
# tools/cq/search/smart_search.py

def _run_candidate_phase(ctx: SmartSearchContext) -> tuple[list[RawMatch], SearchStats, str]:
    rg_types = resolve_rg_types(ctx.lang_scope)  # ("py",), ("rust",), or ("py", "rust")
    pattern = compile_search_pattern(ctx.query, ctx.mode)
    proc = run_rg_json(
        root=ctx.root,
        pattern=pattern,
        mode=ctx.mode,
        lang_types=rg_types,
        include_globs=ctx.include_globs or [],
        exclude_globs=ctx.exclude_globs or [],
        limits=ctx.limits,
    )
    collector = collect_matches_from_events(proc.events, limits=ctx.limits, match_factory=RawMatch)
    return collector.matches, stats_from_collector(collector, timed_out=proc.timed_out), pattern
```

### Target files
- `tools/cq/search/smart_search.py`
- `tools/cq/search/adapter.py`
- `tools/cq/query/executor.py`
- `tools/cq/macros/calls.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/impact.py`

### Deprecate/delete after completion
- `RipGrepSearch` protocols and wrappers in `tools/cq/search/adapter.py`.
- Legacy code comments/docstrings referencing `rpygrep`.

### Implementation checklist
- [ ] Replace all `searcher.run()`/`run_direct()` flows with `rg_native`.
- [ ] Keep collector limit behavior parity (`max_files`, `max_total_matches`, truncation flags).
- [ ] Preserve output evidence anchoring (`SourceSpan`, line/column semantics).
- [ ] Add regression tests for adapter helper behavior parity.

---

## Scope 3 - Unified Language Scope Model (`python`, `rust`, `auto`)

### Goal
Move from single-language execution to language-scope execution that supports simultaneous Python + Rust runs in one command.

### Representative code pattern

```python
# tools/cq/query/language.py

QueryLanguageScope = Literal["python", "rust", "auto"]
DEFAULT_QUERY_LANGUAGE_SCOPE: QueryLanguageScope = "auto"

def expand_scope(scope: QueryLanguageScope) -> tuple[QueryLanguage, ...]:
    if scope == "auto":
        return ("python", "rust")
    return (scope,)
```

```python
# tools/cq/search/models.py

class SearchConfig(CqStruct, frozen=True):
    root: Path
    query: str
    mode: QueryMode
    limits: SearchLimits
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
```

### Target files
- `tools/cq/query/language.py`
- `tools/cq/query/ir.py`
- `tools/cq/query/parser.py`
- `tools/cq/run/spec.py`
- `tools/cq/search/models.py`
- `tools/cq/cli_app/types.py`
- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/commands/search.py`

### Deprecate/delete after completion
- `QueryLanguage`-only public CLI assumptions where a scope is required.
- Single-value language summary assumptions for search output.

### Implementation checklist
- [ ] Add `auto` language scope token to CLI + parser.
- [ ] Keep explicit `python`/`rust` overrides available for focused searches.
- [ ] Ensure `q`, `search`, and `run` all resolve scope consistently.
- [ ] Add parser tests for valid/invalid language scopes.

---

## Scope 4 - Unified Dual-Language Execution Orchestration

### Goal
Execute Python and Rust passes in one request, merge results deterministically, and keep Python-first ranking and section ordering.

### Representative code pattern

```python
# tools/cq/search/smart_search.py

def smart_search(root: Path, query: str, **kwargs: Unpack[SearchKwargs]) -> CqResult:
    ctx = _build_search_context(root, query, kwargs)
    langs = expand_scope(ctx.lang_scope)
    per_lang = [run_single_language_search(ctx, lang=lang) for lang in langs]
    merged = merge_multilang_search_results(per_lang, python_first=True)
    return merged
```

```python
# tools/cq/core/merge.py (or new tools/cq/search/multilang_merge.py)

def merge_multilang_search_results(
    results: list[LanguageSearchResult],
    *,
    python_first: bool,
) -> CqResult:
    order = ["python", "rust"] if python_first else sorted({r.lang for r in results})
    # stable merge by language priority, relevance score, file, line, col
    ...
```

### Target files
- `tools/cq/search/smart_search.py`
- New `tools/cq/search/multilang_merge.py`
- `tools/cq/run/runner.py`
- `tools/cq/query/executor.py`
- `tools/cq/core/merge.py` (if reused)

### Deprecate/delete after completion
- Language-partition execution loops that assume one language per command path.
- Rust-only gate errors for unified auto mode (replace with degradations + diagnostics).

### Implementation checklist
- [ ] Implement per-language execution units and deterministic merge.
- [ ] Keep Python-first ranking behavior explicit and testable.
- [ ] Ensure `run` step execution supports mixed-language query/search steps.
- [ ] Add integration tests validating stable merge order.

---

## Scope 5 - Multi-Language Output Model and Cross-Language Diagnostics

### Goal
Upgrade result summaries/sections to expose both language outputs in one result and emit explicit Python-intent vs Rust-only diagnostic findings.

### Representative code pattern

```python
# tools/cq/search/smart_search.py

def _build_summary(inputs: SearchSummaryInputs) -> dict[str, object]:
    return {
        "query": inputs.query,
        "lang_scope": inputs.lang_scope,
        "languages": {
            "python": inputs.python_stats.to_dict(),
            "rust": inputs.rust_stats.to_dict(),
        },
        "returned_matches": inputs.total_matches,
        "scan_method": "hybrid_multilang",
    }
```

```python
# tools/cq/search/multilang_diagnostics.py

def build_cross_language_diagnostics(
    *,
    query: str,
    python_matches: int,
    rust_matches: int,
    request_shape: RequestShape,
) -> list[Finding]:
    findings: list[Finding] = []
    if request_shape.python_oriented and python_matches == 0 and rust_matches > 0:
        findings.append(
            Finding(
                category="cross_language_hint",
                message=(
                    "Search query requested Python-oriented attributes but found no Python matches; "
                    "Rust matches were found."
                ),
                severity="warning",
            )
        )
    return findings
```

### Target files
- `tools/cq/search/smart_search.py`
- New `tools/cq/search/multilang_diagnostics.py`
- `tools/cq/core/schema.py` (summary field additions only)
- `tools/cq/core/report.py` (renderer updates)

### Deprecate/delete after completion
- Single-language summary fields used as sole source (`summary["lang"]` only).

### Implementation checklist
- [ ] Add `lang_scope` and per-language summary structures.
- [ ] Add cross-language diagnostic finding generation.
- [ ] Update markdown/json renderers for language-grouped reporting.
- [ ] Add CLI golden snapshots for unified output.

---

## Scope 6 - Toolchain and Runtime Capability Migration (`rpygrep` -> `rg`)

### Goal
Replace package-based ripgrep metadata with binary-based toolchain metadata and update runtime diagnostics accordingly.

### Representative code pattern

```python
# tools/cq/core/toolchain.py

def _detect_rg() -> tuple[bool, str | None]:
    proc = subprocess.run(["rg", "--version"], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        return False, None
    first = proc.stdout.splitlines()[0] if proc.stdout else ""
    return True, first.strip() or None

class Toolchain(CqStruct, frozen=True):
    rg_available: bool
    rg_version: str | None
    sgpy_available: bool
    sgpy_version: str | None
    ...
```

### Target files
- `tools/cq/core/toolchain.py`
- `tools/cq/core/run_context.py`
- `tools/cq/core/schema.py` (toolchain payload compatibility)
- `tests/unit/cq/test_runmeta_artifacts.py`
- `tests/integration/cq/test_search_migration.py` (renamed/reworked)

### Deprecate/delete after completion
- `has_rpygrep`, `require_rpygrep`, `rpygrep_available`, `rpygrep_version`.
- Any user-facing instructions to install `rpygrep`.

### Implementation checklist
- [ ] Add `rg` binary detection and robust fallback messaging.
- [ ] Update run metadata serialization tests for new toolchain keys.
- [ ] Keep one release window of backward-compatible metadata decoding if needed.

---

## Scope 7 - Optional Tree-sitter Rust Enrichment (Additive, Non-Critical Path)

### Goal
Add opt-in tree-sitter Rust enrichment for scoped context precision while keeping core correctness independent of enrichment success.

### Representative code pattern

```python
# tools/cq/search/tree_sitter_rust.py

def enrich_rust_context(file_path: Path, line: int, col: int) -> RustContext | None:
    try:
        tree = get_cached_rust_tree(file_path)
        node = locate_node(tree, line=line, col=col)
        return RustContext(scope=extract_scope_name(node), container=extract_container(node))
    except Exception:
        return None
```

### Target files
- New `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/classifier.py`
- `tools/cq/search/smart_search.py`

### Deprecate/delete after completion
- None (additive scope).

### Implementation checklist
- [ ] Add bounded, best-effort enrichment API (never fail hard).
- [ ] Gate enrichment by language and availability.
- [ ] Add unit tests for fallback behavior when parser/query fails.

---

## Scope 8 - CQ Macro Alignment on Native `rg`

### Goal
Update macro workflows (`calls`, `impact`, `sig-impact`) to consume unified native `rg` helpers and cleanly name fallback/scan methods.

### Representative code pattern

```python
# tools/cq/macros/calls.py

def _find_candidates(function_name: str, root: Path, *, limits: SearchLimits) -> list[tuple[Path, int]]:
    pattern = rf"\\b{function_name.rsplit('.', maxsplit=1)[-1]}\\s*\\("
    return find_call_candidates_native_rg(root=root, pattern=pattern, limits=limits, lang_scope="python")
```

### Target files
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/query/executor.py`

### Deprecate/delete after completion
- Function names and summary labels explicitly tied to `rpygrep` implementation.
- Legacy fallback labels (`scan_method: rpygrep`).

### Implementation checklist
- [ ] Rename helper methods to backend-neutral naming.
- [ ] Preserve Python-only macro scope where required.
- [ ] Update macro summaries and diagnostics to use native `rg` terminology.

---

## Scope 9 - Docs, Goldens, and Test Matrix Completion

### Goal
Complete test and documentation updates for the new backend and unified language model.

### Representative code pattern

```python
# tests/e2e/cq/test_multilang_search.py

def test_auto_scope_returns_python_and_rust(run_search) -> None:
    result = run_search("build_graph", lang="auto")
    assert result.summary["lang_scope"] == "auto"
    assert "python" in result.summary["languages"]
    assert "rust" in result.summary["languages"]
```

### Target files
- `tests/unit/cq/search/test_adapter.py`
- `tests/unit/cq/search/test_smart_search.py`
- `tests/integration/cq/test_search_migration.py` (rename to native rg)
- `tests/e2e/cq/test_rust_query_support.py`
- New `tests/e2e/cq/test_multilang_search.py`
- `tests/cli_golden/test_search_golden.py`
- `tests/cli_golden/fixtures/search_help.txt`
- `tools/cq/README.md`
- `AGENTS.md`
- `CLAUDE.md`
- `.claude/skills/cq/SKILL.md`

### Deprecate/delete after completion
- `rpygrep` wording in CQ docs and test descriptions.
- Obsolete golden fixtures containing stale toolchain keys/messages.

### Implementation checklist
- [ ] Add unit coverage for `lang_scope` parsing, merge, and diagnostics.
- [ ] Add integration/E2E coverage for simultaneous Python+Rust results.
- [ ] Add CLI goldens for unified output and cross-language warnings.
- [ ] Update all CQ documentation references to native `rg`.

---

## Scope 10 - Dependency and Codebase Cleanup (Finalize Migration)

### Goal
Remove `rpygrep` dependency and delete dead code paths after all prior scopes pass.

### Representative code pattern

```toml
# pyproject.toml
[project]
dependencies = [
  # remove: "rpygrep>=0.2.1",
]
```

### Target files
- `pyproject.toml`
- `uv.lock`
- `tools/cq/search/adapter.py` (if split/legacy wrappers removed)
- `tools/cq/core/toolchain.py`
- `tests/integration/cq/test_search_migration.py` (final renamed semantics)

### Deprecate/delete after completion
- `rpygrep` dependency entry and lockfile graph.
- Any compatibility shims only needed during transition.

### Implementation checklist
- [ ] Remove `rpygrep` from dependency manifests.
- [ ] Remove dead wrappers and transitional aliases.
- [ ] Confirm all CQ tests pass without `rpygrep` installed.
- [ ] Confirm docs and toolchain metadata no longer reference `rpygrep`.

---

## CQ-Focused Validation Matrix

### Required command sequence per major scope completion

```bash
uv run ruff format && uv run ruff check --fix
uv run pyrefly check
uv run pyright
uv run pytest -q tests/unit/cq tests/integration/cq tests/e2e/cq tests/cli_golden
```

### Additional migration-specific validation

```bash
# Verify rg capability and type support
rg --version
rg --type-list | rg '^(py|rust):'

# Verify unified output behavior
./cq search build_graph --lang auto --format json --no-save-artifact
./cq q "entity=function name=build_graph lang=auto in=tools/cq/" --format json --no-save-artifact
```

---

## Completion Criteria

1. CQ has no runtime dependency on `rpygrep`.
2. Native `rg` is the only search backend in `tools/cq`.
3. `search`, `q`, and `run` support unified language execution (`auto`) with Python-first ranking.
4. Output includes per-language details and cross-language diagnostics.
5. Python behavior remains stable and regression-tested.
6. CQ docs and golden outputs reflect the new architecture.
