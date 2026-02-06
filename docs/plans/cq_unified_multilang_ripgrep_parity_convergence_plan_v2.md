# CQ Unified Multilang Ripgrep + Parity Convergence Plan v2
## Status-Tracked Post-v1 Delta (`tools/cq` only)

## Summary

This document reflects an in-depth audit of the current codebase state after v1 and subsequent v2 work-in-progress.  
It marks each v2 scope as `completed`, `partial`, or `pending`, and re-sequences only the remaining implementation work.

Audit evidence was gathered from:
- `tools/cq` implementation review
- CQ-focused test suites
- current CLI/golden/doc outputs

Current CQ gate status (audit run):
- `uv run pytest -q tests/unit/cq tests/integration/cq tests/e2e/cq tests/cli_golden`
- Result: **643 passed**

Scope remains limited to:
- `tools/cq`
- CQ tests (`tests/unit/cq`, `tests/integration/cq`, `tests/e2e/cq`, `tests/cli_golden`)
- CQ docs

---

## Locked Decisions

1. `auto` remains default language scope for `search`, `q`, and `run`.
2. Python-first ranking remains explicit and test-locked.
3. Ast-grep remains primary semantic extraction engine for both languages.
4. tree-sitter-rust remains optional enrichment; core correctness must not depend on it.
5. Breaking changes are acceptable where they simplify architecture and remove drift.

---

## Scope Status Matrix

| Scope | Title | Status | Notes |
|---|---|---|---|
| 0 | Contract normalization | completed | Canonical multilang summary helpers exist and are asserted. |
| 1 | Shared multilang orchestrator (`search`/`q`/`run`) | completed | Shared orchestration and deterministic merge helpers are in use. |
| 2 | `run` auto-scope batch optimization | completed | Auto-scope q-steps are expanded and batch-executed per language. |
| 3 | Shared cross-language diagnostics | partial | Shared module exists and is wired, but capability matrix remains minimal. |
| 4 | Rust parity expansion (ast-grep-first) | partial | Core entities work; advanced parity targets remain. |
| 5 | Macro surface convergence under unified scope | pending | Macro commands remain effectively Python-only and not scope-aware. |
| 6 | Optional tree-sitter-rust enrichment hardening | partial | Module + integration exist; bounded-control model is not complete. |
| 7 | Goldens/docs finalization | partial | Unified scope docs/goldens exist; parity boundaries/capability semantics need final lock. |

---

## Completed Scopes (Evidence)

### Scope 0 - Contract normalization (`completed`)
- Implemented canonical summary helper:
  - `tools/cq/core/multilang_summary.py`
- Required keys validated:
  - `lang_scope`, `language_order`, `languages`, `cross_language_diagnostics`
- Integrated in search/query merge paths:
  - `tools/cq/search/smart_search.py`
  - `tools/cq/core/multilang_orchestrator.py`
- Tests:
  - `tests/unit/cq/search/test_smart_search.py`
  - `tests/unit/cq/test_executor_sections.py`
  - `tests/e2e/cq/test_query_regression.py`

### Scope 1 - Shared orchestrator (`completed`)
- Implemented:
  - `tools/cq/core/multilang_orchestrator.py`
- Used by:
  - `tools/cq/search/smart_search.py` (`execute_by_language_scope`, `merge_partitioned_items`)
  - `tools/cq/query/executor.py` (`execute_by_language_scope`, `merge_language_cq_results`)
  - `tools/cq/run/runner.py` (`merge_language_cq_results`)

### Scope 2 - `run` auto-scope batching (`completed`)
- `run` expands `lang_scope=auto` q-steps to concrete language partitions and batches execution:
  - `tools/cq/run/runner.py` (`_expand_q_step_by_scope`, grouped batch runners)
- Verified by tests:
  - `tests/unit/cq/test_run_batch_scan.py`
  - `tests/unit/cq/test_run_relational_batching.py`
  - `tests/unit/cq/test_run_merge.py`

---

## Remaining Scopes

## Scope 3 - Shared cross-language diagnostics (`partial`)

### Implemented now
1. Shared module exists:
   - `tools/cq/search/multilang_diagnostics.py`
2. Shared diagnostics are wired into:
   - `tools/cq/search/smart_search.py`
   - `tools/cq/query/executor.py`
   - `tools/cq/run/runner.py`
3. Existing behavior is deterministic for Python-oriented + Rust-only hit cases.

### Remaining work
1. Expand from intent-heuristic diagnostics to explicit capability diagnostics:
   - Python-only semantic requests in Rust scope (`decorator`, bytecode/symtable-style intents).
   - partial-language capability reporting instead of only match-count mismatch warnings.
2. Introduce capability matrix structure and reusable mappings (shared, python-only, rust-limited).
3. Standardize diagnostic counters/labels in summary payload for all command families.

### Target files
- `tools/cq/search/multilang_diagnostics.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/query/executor.py`
- `tools/cq/run/runner.py`
- `tools/cq/core/schema.py`

### Test additions
- `tests/unit/cq/search/test_smart_search.py` (matrix cases)
- `tests/e2e/cq/test_query_regression.py` (capability warnings by feature intent)
- `tests/cli_golden/test_search_golden.py` (diagnostic rendering stability)

---

## Scope 4 - Rust parity expansion (ast-grep-first) (`partial`)

### Implemented now
1. Core Rust support exists in rules/planner:
   - `tools/cq/astgrep/rules_rust.py`
   - `tools/cq/query/planner.py`
2. Core entity queries currently supported end-to-end:
   - function/class/import/callsite (with current constraints)

### Remaining work
1. Expand Rust rule coverage for parity-critical shared surfaces:
   - `mod_item` (module entity parity)
   - `macro_invocation`
   - attribute-oriented evidence where mappable
2. Strengthen method semantics:
   - ensure Rust `method` behavior is impl-scoped and not indistinguishable from free functions.
3. Expand relational and pattern-context parity coverage for Rust-specific fragments.
4. Tighten classifier/category mappings for new Rust node kinds with confidence bucket consistency.

### Target files
- `tools/cq/astgrep/rules_rust.py`
- `tools/cq/astgrep/rules.py`
- `tools/cq/query/planner.py`
- `tools/cq/query/sg_parser.py`
- `tools/cq/query/executor.py`
- `tools/cq/search/classifier.py`

### Test additions
- `tests/unit/cq/test_astgrep_rules.py` (new Rust rule families)
- `tests/unit/cq/test_relational_queries.py` (Rust relational semantics)
- `tests/e2e/cq/test_rust_query_support.py` (module/method/macro invocation parity cases)
- `tests/e2e/cq/test_relational_operators.py` (Rust runtime relational behavior)
- `tests/e2e/cq/test_pattern_context.py` (Rust context/selector cases)

---

## Scope 5 - Macro surface convergence under unified scope (`pending`)

### Current gap
Macro command surfaces remain Python-centric and do not expose unified scope/capability-aware behavior:
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/scopes.py`

### Required implementation
1. Add explicit scope handling (`auto|python|rust`) for macro entrypoints.
2. Preserve Python enrichments while returning structured capability diagnostics for non-equivalent Rust semantics.
3. Standardize macro output summary/sections with the multilang reporting conventions.
4. Remove hard-fail UX where partial capability warnings are the correct behavior.

### Test additions
- `tests/unit/cq/macros/test_calls.py` (scope + diagnostic payload behavior)
- `tests/integration/cq/test_calls_integration.py` (auto/python/rust behavior)
- `tests/e2e/cq/test_query_callers.py` (end-to-end compatibility and warnings)

---

## Scope 6 - Optional tree-sitter-rust enrichment hardening (`partial`)

### Implemented now
1. Optional enrichment module exists:
   - `tools/cq/search/tree_sitter_rust.py`
2. Smart search attaches best-effort Rust tree-sitter context:
   - `tools/cq/search/smart_search.py`
3. Fail-open behavior is tested:
   - `tests/unit/cq/search/test_tree_sitter_rust.py`

### Remaining work
1. Add bounded execution controls (currently minimal):
   - explicit match/range constraints
   - deterministic guardrails for deep/large parse paths
   - optional cancellation/progress hooks if we standardize those interfaces
2. Define strict separation between enrichment-only fields and core ranking/count semantics.
3. Extend coverage for macro-heavy and complex Rust contexts where ast-grep context is sparse.

### Target files
- `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/classifier.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/query/executor.py` (only if enrichment is surfaced for q-path details)

### Test additions
- `tests/unit/cq/search/test_classifier.py`
- `tests/unit/cq/search/test_smart_search.py`
- `tests/e2e/cq/test_rust_query_support.py`

---

## Scope 7 - Goldens/docs finalization (`partial`)

### Implemented now
1. Unified scope model is already documented in CQ docs:
   - `tools/cq/README.md`
   - `.claude/skills/cq/SKILL.md`
   - `AGENTS.md`
2. Search goldens include multilang summary keys:
   - `tests/cli_golden/fixtures/search_help.txt`
   - `tests/cli_golden/fixtures/search_summary.txt`

### Remaining work
1. Finalize parity matrix and explicit non-equivalence docs after scopes 3-6 land.
2. Ensure documentation examples reflect final diagnostics and macro scope behavior.
3. Regenerate and lock CLI/e2e goldens after final parity/capability changes.

### Target files
- `tools/cq/README.md`
- `.claude/skills/cq/SKILL.md`
- `AGENTS.md`
- `CLAUDE.md`
- `tests/cli_golden/fixtures/search_help.txt`
- `tests/cli_golden/fixtures/search_summary.txt`
- `tests/cli_golden/test_search_golden.py`

---

## Revised Execution Sequence (Remaining Work Only)

1. Scope 3 (diagnostics matrix hardening) - baseline for parity/macro UX.
2. Scope 4 (Rust parity expansion) - entity/rule/classifier/runtime parity.
3. Scope 5 (macro convergence) - depends on shared diagnostics contract.
4. Scope 6 (tree-sitter hardening) - parallelizable late, after Rust parity baseline is stable.
5. Scope 7 (docs/goldens lock) - final step.

---

## Revised Exit Gates

1. Core CQ gate after each scope:
   - `uv run pytest -q tests/unit/cq tests/integration/cq tests/e2e/cq tests/cli_golden`
2. Full quality gate at final lock:
   - `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`

---

## Acceptance Criteria (Remaining Delta)

1. Diagnostics are capability-aware and consistent across `search`, `q`, and `run`.
2. Rust parity materially expands beyond current core entities (module/macro/method semantics and relational depth).
3. Macro commands operate on unified scope semantics with structured capability diagnostics.
4. tree-sitter-rust enrichment is bounded, optional, and never alters core result correctness.
5. Docs and goldens fully align with the final unified multilang behavior.

---

## Assumptions

1. v1 baseline is merged and stable.
2. Breaking changes remain acceptable in this migration window.
3. `auto` stays the default scope.
4. CQ remains isolated from `src/`.
5. Residual dependency-lock cleanup can be tracked separately if needed.
