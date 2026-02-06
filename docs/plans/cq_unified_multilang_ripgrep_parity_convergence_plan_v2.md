# CQ Unified Multilang Ripgrep + Parity Convergence Plan v2  
## Corrected Post-v1 Delta Plan (`tools/cq` only)

## Summary

This document is a corrected and optimized v2 plan based on the codebase state **after v1 migration work**.  
It focuses only on remaining convergence work, not already-completed v1 migration items.

Primary outcomes:
- One stable multilang result contract across `search`, `q`, and `run`.
- Shared multilang orchestration/merge logic (no per-command drift).
- Ast-grep-first Rust parity expansion for shared semantic surfaces.
- Capability-aware cross-language diagnostics across command families.
- Optional tree-sitter-rust enrichment as additive context only.

Scope remains limited to:
- `tools/cq`
- CQ tests (`tests/unit/cq`, `tests/integration/cq`, `tests/e2e/cq`, `tests/cli_golden`)
- CQ docs

---

## Post-v1 Baseline (Validated Against Current Code)

### Implemented

1. Language scope model exists and defaults to `auto`:
   - `tools/cq/query/language.py`
   - `tools/cq/query/ir.py`
   - `tools/cq/query/parser.py`
2. Native ripgrep process backend exists and is wired:
   - `tools/cq/search/rg_native.py`
   - `tools/cq/search/adapter.py`
   - `tools/cq/search/smart_search.py`
3. Toolchain metadata has `rg` capability fields:
   - `tools/cq/core/toolchain.py`
4. Rust ast-grep support exists for core entities (initial depth):
   - `tools/cq/astgrep/rules_rust.py`
   - `tools/cq/query/planner.py`

### Partial / Not Yet Converged

1. Result summary contract differs between `search` and `q` auto-scope merge paths:
   - `search` uses normalized `languages` stats partition.
   - `q` auto merge stores full per-language nested summaries.
2. Cross-language diagnostics are implemented in `search` only:
   - `tools/cq/search/smart_search.py`
3. `run` `q` step handling for `lang_scope=auto` bypasses grouped batching:
   - `tools/cq/run/runner.py` (`_prepare_q_step`)
4. No dedicated shared multilang diagnostics module yet.
5. No `tree_sitter_rust` enrichment module yet.

### Residual Cleanup (Not Blocking v2 Functional Work)

1. `rpygrep` dependency cleanup can remain separate:
   - `pyproject.toml`, `uv.lock` (if still present)

---

## Corrections Applied vs Prior v2 Draft

1. Removed assumptions that v1 fundamentals were still pending (`auto` scope and native `rg` are already implemented).
2. Reframed v2 as a **delta plan** targeting convergence gaps only.
3. Prioritized real bottlenecks discovered in code:
   - summary contract drift,
   - command-family orchestration drift,
   - run auto-scope batching inefficiency,
   - diagnostics centralization gap.
4. Updated target files and tests to match existing module/test layout.

---

## Locked Decisions

1. `auto` remains default language scope for `search`, `q`, and `run`.
2. Python-first ranking remains explicit and test-locked.
3. Ast-grep remains primary semantic extraction engine for both languages.
4. tree-sitter-rust is optional enrichment only; never a hard dependency.
5. Breaking changes are acceptable if they reduce long-term architectural drift.

---

## Scope 0 - Contract Normalization (Blocking)

### Goal
Define one canonical multilang summary schema and use it in `search`, `q`, and `run`.

### Key implementation moves
1. Introduce a shared summary builder:
   - New `tools/cq/core/multilang_summary.py`
2. Move summary assembly out of per-command ad hoc branches.
3. Normalize required keys:
   - `lang_scope`
   - `language_order`
   - `languages` (stats partitions only)
   - `cross_language_diagnostics`

### Target files
- New `tools/cq/core/multilang_summary.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/query/executor.py`
- `tools/cq/run/runner.py`
- `tools/cq/core/schema.py`
- `tools/cq/core/report.py`

### Representative code pattern
```python
def build_multilang_summary(*, scope, order, partitions, diagnostics, common):
    return {
        **common,
        "lang_scope": scope,
        "language_order": list(order),
        "languages": partitions,
        "cross_language_diagnostics": diagnostics,
    }
```

### Test scope
- `tests/unit/cq/search/test_smart_search.py`
- `tests/unit/cq/test_run_merge.py`
- `tests/unit/cq/test_executor_sections.py`
- `tests/e2e/cq/test_query_regression.py`
- `tests/cli_golden/test_search_golden.py`

### Exit gate
`uv run pytest -q tests/unit/cq/search/test_smart_search.py tests/unit/cq/test_run_merge.py tests/e2e/cq/test_query_regression.py tests/cli_golden/test_search_golden.py`

---

## Scope 1 - Shared Multilang Orchestrator for `search` / `q` / `run`

### Goal
Eliminate duplicated per-command language loops and merge behavior.

### Key implementation moves
1. Add a reusable orchestrator helper:
   - New `tools/cq/core/multilang_orchestrator.py`
2. Standardize merge ordering:
   - language priority (Python first),
   - relevance score,
   - file,
   - line,
   - column.
3. Make `search`, `q`, and `run` call the same orchestrator contract.

### Target files
- New `tools/cq/core/multilang_orchestrator.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/query/executor.py`
- `tools/cq/run/runner.py`

### Representative code pattern
```python
results = execute_by_scope(scope="auto", languages=("python", "rust"), run_one=run_for_language)
merged = merge_multilang(results, python_first=True)
```

### Test scope
- `tests/unit/cq/test_run_merge.py`
- `tests/unit/cq/search/test_smart_search.py`
- `tests/integration/cq/test_search_migration.py`
- `tests/e2e/cq/test_query_regression.py`

### Exit gate
`uv run pytest -q tests/unit/cq/test_run_merge.py tests/unit/cq/search/test_smart_search.py tests/integration/cq/test_search_migration.py`

---

## Scope 2 - `run` Auto-Scope Batch Optimization

### Goal
Remove immediate execution shortcut for `q` steps in `auto` scope and preserve batch efficiencies.

### Key implementation moves
1. Refactor `_prepare_q_step` in `tools/cq/run/runner.py`:
   - stop immediate `execute_plan(...)` for `lang_scope == "auto"`.
2. Expand `auto` into concrete language tasks and feed grouped entity/pattern batch paths.
3. Preserve deterministic merged output contract from Scope 0/1.

### Target files
- `tools/cq/run/runner.py`
- `tools/cq/query/batch.py`
- `tools/cq/query/batch_spans.py`
- `tools/cq/query/execution_requests.py`

### Representative code pattern
```python
if query.lang_scope == "auto":
    for lang in expand_language_scope("auto"):
        enqueue_scoped_q_step(step, lang=lang)
else:
    enqueue_scoped_q_step(step, lang=query.primary_language)
```

### Test scope
- `tests/unit/cq/test_run_batch_scan.py`
- `tests/unit/cq/test_run_relational_batching.py`
- `tests/unit/cq/test_run_merge.py`
- `tests/e2e/cq/test_query_regression.py`

### Exit gate
`uv run pytest -q tests/unit/cq/test_run_batch_scan.py tests/unit/cq/test_run_relational_batching.py tests/unit/cq/test_run_merge.py`

---

## Scope 3 - Shared Cross-Language Diagnostics

### Goal
Promote diagnostics from search-only heuristics to shared deterministic diagnostics across `search`, `q`, and `run`.

### Key implementation moves
1. Create dedicated diagnostics module:
   - New `tools/cq/search/multilang_diagnostics.py`
2. Encode capability matrix for:
   - shared features,
   - Python-only features (symtable/bytecode/decorator semantics),
   - Rust-only hints where relevant.
3. Reuse one diagnostic builder from all command families.

### Target files
- New `tools/cq/search/multilang_diagnostics.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/query/executor.py`
- `tools/cq/run/runner.py`
- `tools/cq/core/schema.py`

### Representative code pattern
```python
if request.python_oriented and stats.python_hits == 0 and stats.rust_hits > 0:
    add_warning("Python-oriented query had no Python matches; Rust matches exist.")
```

### Test scope
- `tests/unit/cq/search/test_smart_search.py`
- `tests/unit/cq/test_query_parser.py`
- `tests/e2e/cq/test_query_regression.py`
- `tests/cli_golden/test_search_golden.py`

### Exit gate
`uv run pytest -q tests/unit/cq/search/test_smart_search.py tests/e2e/cq/test_query_regression.py tests/cli_golden/test_search_golden.py`

---

## Scope 4 - Rust Parity Expansion (Ast-grep-First)

### Goal
Expand Rust rule coverage toward parity for shared semantic entities and relational operations.

### Key implementation moves
1. Extend Rust rule packs:
   - modules, impl-associated methods, macro invocations, attributes.
2. Strengthen planner mapping for Rust entity/query semantics.
3. Use `pattern.context + selector + strictness` for fragment-safe Rust matching.

### Target files
- `tools/cq/astgrep/rules_rust.py`
- `tools/cq/astgrep/rules.py`
- `tools/cq/query/planner.py`
- `tools/cq/query/sg_parser.py`
- `tools/cq/query/executor.py`

### Representative code pattern
```python
RS_DEF_MODULE = RuleSpec(..., config={"rule": {"kind": "mod_item"}})
RS_CALL_MACRO = RuleSpec(..., config={"rule": {"kind": "macro_invocation"}})
```

### Test scope
- `tests/unit/cq/test_astgrep_rules.py`
- `tests/unit/cq/test_relational_queries.py`
- `tests/e2e/cq/test_rust_query_support.py`
- `tests/e2e/cq/test_relational_operators.py`
- `tests/e2e/cq/test_pattern_context.py`

### Exit gate
`uv run pytest -q tests/unit/cq/test_astgrep_rules.py tests/unit/cq/test_relational_queries.py tests/e2e/cq/test_rust_query_support.py tests/e2e/cq/test_relational_operators.py`

---

## Scope 5 - Macro Surface Convergence Under Unified Scope

### Goal
Preserve single command surface while making macro behavior capability-aware in multilang contexts.

### Key implementation moves
1. Keep macro entrypoints unified (`calls`, `impact`, `sig-impact`, `scopes`).
2. Add scope-aware capability sections for language-limited analyses.
3. Avoid hard failures when partial language capability is expected.

### Target files
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/query/enrichment.py`

### Test scope
- `tests/unit/cq/macros/test_calls.py`
- `tests/integration/cq/test_calls_integration.py`
- `tests/e2e/cq/test_query_callers.py`
- `tests/e2e/cq/test_query_regression.py`

### Exit gate
`uv run pytest -q tests/unit/cq/macros/test_calls.py tests/integration/cq/test_calls_integration.py tests/e2e/cq/test_query_callers.py`

---

## Scope 6 - Optional tree-sitter-rust Enrichment

### Goal
Add bounded, non-blocking Rust context enrichment where ast-grep alone is insufficient.

### Key implementation moves
1. Add optional module:
   - New `tools/cq/search/tree_sitter_rust.py`
2. Enforce bounded execution:
   - byte-range scoping,
   - match limits,
   - graceful cancellation/degradation.
3. Keep core findings pipeline independent of enrichment success.

### Target files
- New `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/classifier.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/query/executor.py`

### Test scope
- `tests/unit/cq/search/test_classifier.py`
- `tests/unit/cq/search/test_smart_search.py`
- `tests/e2e/cq/test_rust_query_support.py`

### Exit gate
`uv run pytest -q tests/unit/cq/search/test_classifier.py tests/unit/cq/search/test_smart_search.py tests/e2e/cq/test_rust_query_support.py`

---

## Scope 7 - Goldens and Documentation Finalization

### Goal
Finalize output/help/docs contract for unified multilang behavior.

### Key implementation moves
1. Update renderer and goldens for stabilized schema.
2. Document diagnostics semantics and parity boundaries.
3. Remove stale split-language wording.

### Target files
- `tools/cq/core/report.py`
- `tools/cq/README.md`
- `.claude/skills/cq/SKILL.md`
- `AGENTS.md`
- `CLAUDE.md`
- `tests/cli_golden/fixtures/search_help.txt`
- `tests/cli_golden/fixtures/search_summary.txt`
- `tests/cli_golden/test_search_golden.py`

### Test scope
- `tests/cli_golden`
- `tests/e2e/cq/test_query_golden.py`

### Exit gate
`uv run pytest -q tests/cli_golden tests/e2e/cq/test_query_golden.py`

---

## Optimized Execution Sequence

1. Scope 0 (contract normalization) - blocking.
2. Scope 1 (shared orchestrator) - depends on scope 0.
3. Scope 2 (run batching optimization) - depends on scope 1.
4. Scope 3 (shared diagnostics) - depends on scopes 0-2 stable schema.
5. Scope 4 (Rust parity expansion) - can start after scope 1, but merge after scope 3.
6. Scope 5 (macro convergence) - after scope 3 diagnostics contract exists.
7. Scope 6 (tree-sitter enrichment) - after scope 4 baseline parity.
8. Scope 7 (goldens/docs) - last.

---

## Required Test Scenarios

1. Default `auto` scope returns unified schema from `search`, `q`, and `run`.
2. Explicit `python`/`rust` narrowing keeps schema shape unchanged.
3. Python-first tie-breaking is deterministic across repeated runs.
4. Rust parity covers shared entities and relational filters.
5. Cross-language warning appears for Python-oriented intent with Rust-only hits.
6. `run` `q` steps in `auto` scope use grouped batching and do not regress output.
7. Optional enrichment failures do not suppress base findings or crash.

---

## Program Acceptance Criteria

1. One canonical multilang summary schema is emitted by `search`, `q`, and `run`.
2. One shared multilang orchestration/merge path is used by command families.
3. Python-first behavior remains explicit and test-locked.
4. Rust shared-semantic parity is substantially expanded using ast-grep-first extraction.
5. Cross-language diagnostics are structured and deterministic across command families.
6. CQ remains isolated from `src/`.
7. CQ-only gate passes:
   - `uv run pytest -q tests/unit/cq tests/integration/cq tests/e2e/cq tests/cli_golden`
8. Full quality gate passes:
   - `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`

---

## Assumptions

1. v1 implementation is merged as the baseline for this v2 delta plan.
2. Breaking changes are acceptable in this migration window.
3. `auto` default scope is retained.
4. Residual package-lock cleanup (for legacy `rpygrep` entries) may be handled separately.
