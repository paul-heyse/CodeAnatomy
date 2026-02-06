# CQ Unified Multi-Language Ripgrep + Rust Parity Convergence Plan v2 (Post-v1 Baseline)

## Summary

This plan assumes `docs/plans/cq_native_ripgrep_unified_multilang_execution_plan_v1.md` is fully implemented.

From that baseline, this v2 plan closes remaining parity and interface gaps so CQ presents a single, language-agnostic command surface where Python and Rust execute together by default, with Python-first prioritization and explicit cross-language diagnostics.

Primary outcomes:
- Native `rg` remains the only candidate-generation backend.
- `q`, `search`, and `run` execute unified Python+Rust by default (no language-specific command split).
- Rust semantic coverage approaches Python parity for shared concepts via ast-grep-first rules.
- Python-only features remain supported but are surfaced as capability-aware diagnostics rather than separate command families.
- Tree-sitter Rust remains additive enrichment, not required for correctness.

---

## Assumed Baseline (Completed Before v2)

1. `rpygrep` migration is complete and removed.
2. Unified language scope exists (`auto` default) and runs Python+Rust in one invocation.
3. Smart search and query pipelines are already native-`rg` + ast-grep based.
4. CQ remains fully isolated from `src/` and changes are contained to `tools/cq` + CQ tests/docs.

---

## Findings (Assuming v1 Complete)

1. Interface unification still needs contract hardening so language is a filter, not a command-mode distinction.
2. Rust parity is typically still shallow in rule depth (core entities work, advanced semantics lag Python).
3. Multi-language output shape usually exists but needs strict schema consistency across `search`, `q`, and `run`.
4. Python-only analysis capabilities (symtable/bytecode/decorators) need explicit capability messaging when Rust is in-scope.
5. Tree-sitter Rust enrichments can improve macro-heavy and context-sensitive Rust classification without replacing ast-grep.

---

## Locked Decisions

1. Unified command surface only:
   - No distinct Python-vs-Rust command names.
   - `--lang` and `lang=` remain optional scope filters only.
2. Python-first behavior is preserved:
   - Ranking, follow-ups, and tie-breaking prioritize Python evidence.
3. Ast-grep-first for semantic extraction:
   - Prefer ast-grep-py whenever the same information can be obtained.
4. Tree-sitter Rust is additive:
   - Used for precision/context enrichment where ast-grep is insufficient.
5. Capability-aware output over hard failures:
   - Unsupported language-feature combinations return structured diagnostics.

---

## Scope 0 - Contract Freeze and Baseline Validation

### Goal
Freeze a compatibility contract for unified outputs and verify baseline stability for Python-first behavior.

### Representative code pattern

```python
# tests/e2e/cq/test_multilang_contract.py

def test_default_scope_is_unified_but_python_first(run_search) -> None:
    result = run_search("build_graph")
    assert result.summary["lang_scope"] == "auto"
    assert result.summary["language_order"] == ["python", "rust"]
```

### Target files
- `tests/unit/cq/search/test_smart_search.py`
- `tests/unit/cq/test_query_parser.py`
- `tests/e2e/cq/test_query_regression.py`
- `tests/e2e/cq/test_multilang_contract.py` (new)
- `tests/cli_golden/test_search_golden.py`

### Deprecate/delete after completion
- Ad-hoc output assertions that do not validate per-language contract fields.

### Implementation checklist
- [ ] Add schema assertions for unified summary keys (`lang_scope`, `languages`, `language_order`).
- [ ] Add Python-first deterministic ordering tests for tied results.
- [ ] Add non-regression snapshots for Python-only query behavior under default scope.

---

## Scope 1 - Unified CLI/DSL Interface Hardening

### Goal
Ensure `q`, `search`, and `run` expose one language-agnostic interface and consistent language-scope semantics.

### Representative code pattern

```python
# tools/cq/query/language.py

QueryLanguageScope = Literal["auto", "python", "rust"]
DEFAULT_QUERY_LANGUAGE_SCOPE: QueryLanguageScope = "auto"


def parse_language_scope(raw: str | None) -> QueryLanguageScope:
    return "auto" if raw is None else normalize_scope(raw)
```

### Target files
- `tools/cq/query/language.py`
- `tools/cq/query/ir.py`
- `tools/cq/query/parser.py`
- `tools/cq/run/spec.py`
- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/options.py`
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/search.py`
- `tools/cq/cli_app/commands/run.py`

### Deprecate/delete after completion
- Any code paths that interpret language as command-mode branching.

### Implementation checklist
- [ ] Normalize scope parsing across CLI flags and query DSL tokens.
- [ ] Ensure omitted language always resolves to `auto` at command boundary.
- [ ] Ensure explicit `python|rust` remains a narrowing filter only.
- [ ] Add validation that mixed-language runs cannot diverge across command families.

---

## Scope 2 - Shared Semantic Taxonomy Across Python and Rust

### Goal
Unify category and evidence semantics so merged results are comparable and rankable without language-specific render branching.

### Representative code pattern

```python
# tools/cq/search/classifier.py

SEMANTIC_KIND_MAP: dict[str, tuple[MatchCategory, float]] = {
    "function_definition": ("definition", 0.95),
    "function_item": ("definition", 0.95),
    "import_statement": ("import", 0.95),
    "use_declaration": ("import", 0.95),
    "call": ("callsite", 0.95),
    "call_expression": ("callsite", 0.95),
    "macro_invocation": ("callsite", 0.90),
}
```

### Target files
- `tools/cq/search/classifier.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/core/scoring.py`
- `tools/cq/core/schema.py`

### Deprecate/delete after completion
- Language-special-case category mapping duplicated in multiple modules.

### Implementation checklist
- [ ] Centralize node-kind to category mapping by language namespace and shared category output.
- [ ] Align confidence/evidence buckets for cross-language comparability.
- [ ] Ensure merged ranking uses stable tie-breakers independent of language-specific internals.

---

## Scope 3 - Rust Parity Expansion via ast-grep Rule Packs (Primary)

### Goal
Expand Rust coverage for shared semantic surfaces using ast-grep as the primary engine.

### Representative code pattern

```python
# tools/cq/astgrep/rules_rust.py

RS_DEF_MOD = RuleSpec(
    rule_id="rs_def_mod",
    record_type="def",
    kind="mod",
    config={"rule": {"kind": "mod_item"}},
)

RS_CALL_MACRO = RuleSpec(
    rule_id="rs_call_macro",
    record_type="call",
    kind="macro_invocation",
    config={"rule": {"kind": "macro_invocation"}},
)
```

### Target files
- `tools/cq/astgrep/rules_rust.py`
- `tools/cq/astgrep/rules.py`
- `tools/cq/query/planner.py`
- `tools/cq/query/executor.py`
- `tools/cq/query/sg_parser.py`

### Deprecate/delete after completion
- Narrow Rust rule assumptions that only support function/struct/enum/trait + call_expression/use.

### Implementation checklist
- [ ] Add Rust rules for modules, impl-associated methods, macros, attributes, and broader type items.
- [ ] Use `pattern.context + selector + strictness` for fragment-safe Rust matching.
- [ ] Expand relational query support coverage with Rust-safe selectors and fields.
- [ ] Add parity tests for each shared entity surface.

---

## Scope 4 - Capability Model and Cross-Language Diagnostics

### Goal
When queries imply language-specific semantics, emit deterministic diagnostics while still returning valid cross-language findings.

### Representative code pattern

```python
# tools/cq/search/multilang_diagnostics.py

def build_capability_diagnostics(req: QueryIntent, stats: LanguageStats) -> list[Finding]:
    findings: list[Finding] = []
    if req.python_specific and stats.python_matches == 0 and stats.rust_matches > 0:
        findings.append(
            Finding(
                category="cross_language_hint",
                message=(
                    "Python-specific attributes requested returned no Python matches; "
                    "Rust matches were found for the same query context."
                ),
                severity="warning",
            )
        )
    return findings
```

### Target files
- New `tools/cq/search/multilang_diagnostics.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/query/executor.py`
- `tools/cq/run/runner.py`
- `tools/cq/core/schema.py`

### Deprecate/delete after completion
- Hard failure paths for unsupported mixed-language feature usage where graceful diagnostics are sufficient.

### Implementation checklist
- [ ] Define capability matrix for Python-only, Rust-only, and shared features.
- [ ] Emit warnings (not crashes) for unsupported feature-language combinations.
- [ ] Add structured diagnostic categories for renderer and automation consumption.

---

## Scope 5 - Macro and Analysis Command Convergence (No Command Split)

### Goal
Keep one command surface while exposing language-aware behavior for macro/analysis commands.

### Representative code pattern

```python
# tools/cq/macros/calls.py

def calls(function: str, *, lang_scope: QueryLanguageScope = "auto") -> CqResult:
    # shared entrypoint
    # python: full symtable/bytecode enrichment
    # rust: ast-grep call + macro invocation coverage
    # unsupported extras -> diagnostics section
    ...
```

### Target files
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/query/enrichment.py`
- `tools/cq/cli_app/commands/analysis.py`

### Deprecate/delete after completion
- Implicit Python-only assumptions in command entrypoints without diagnostics.

### Implementation checklist
- [ ] Route macro commands through shared language-scope handling.
- [ ] Preserve Python enrichments; add Rust equivalents where semantically valid.
- [ ] Emit capability warnings for Python-only analyses requested under Rust scope.
- [ ] Keep output schema identical across commands.

---

## Scope 6 - Optional tree-sitter-rust Enrichment (Secondary)

### Goal
Integrate tree-sitter-rust where it adds value beyond ast-grep, especially around macro token trees and range-scoped precision.

### Representative code pattern

```python
# tools/cq/search/tree_sitter_rust.py

def enrich_rust_match_context(file_path: Path, byte_range: tuple[int, int]) -> RustTsContext | None:
    query = compile_query_pack("tags")
    cursor = QueryCursor(query)
    cursor.set_containing_byte_range(*byte_range)
    return extract_best_context(cursor.matches(parsed_tree.root_node))
```

### Target files
- New `tools/cq/search/tree_sitter_rust.py`
- `tools/cq/search/classifier.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/query/executor.py`

### Deprecate/delete after completion
- None (additive path).

### Implementation checklist
- [ ] Use rooted/local query-pack linting before runtime use.
- [ ] Enforce bounded query execution (`byte_range`, `match_limit`, cancellation hooks).
- [ ] Use enrichment only as optional augmentation; never block base findings.
- [ ] Add failure-path tests proving graceful degradation.

---

## Scope 7 - Output, Renderers, and Golden Contracts

### Goal
Finalize stable multi-language reporting contract across markdown/json and CLI goldens.

### Representative code pattern

```python
# tools/cq/search/smart_search.py

summary = {
    "lang_scope": "auto",
    "language_order": ["python", "rust"],
    "languages": {
        "python": python_stats,
        "rust": rust_stats,
    },
    "cross_language_diagnostics": diag_counts,
}
```

### Target files
- `tools/cq/core/report.py`
- `tools/cq/core/schema.py`
- `tools/cq/search/smart_search.py`
- `tests/cli_golden/test_search_golden.py`
- `tests/cli_golden/fixtures/`

### Deprecate/delete after completion
- Legacy single-language-only summary rendering branches.

### Implementation checklist
- [ ] Standardize summary fields across `search`, `q`, and `run`.
- [ ] Update markdown renderer section ordering for unified outputs.
- [ ] Regenerate and review CLI goldens for multi-language formatting stability.

---

## Scope 8 - Documentation and Final Cleanup

### Goal
Align docs to a single unified CQ interface and remove stale migration-era references.

### Representative code pattern

```md
# tools/cq/README.md

## Language behavior
- Default: unified (`auto`) executes Python and Rust together.
- `--lang python|rust` narrows scope, it does not change command family.
```

### Target files
- `tools/cq/README.md`
- `AGENTS.md`
- `CLAUDE.md`
- `.claude/skills/cq/SKILL.md`
- `docs/plans/` (linkage updates)

### Deprecate/delete after completion
- Language-split wording that implies separate Python/Rust command paths.
- Legacy migration notes that assume `rpygrep` still exists.

### Implementation checklist
- [ ] Update command docs to emphasize one interface, optional scope narrowing.
- [ ] Add explicit parity matrix (shared vs language-specific outputs).
- [ ] Add examples for default unified execution and diagnostic interpretation.

---

## Acceptance Criteria

1. CQ has one command surface with unified default execution (`auto`) across Python and Rust.
2. `q`, `search`, and `run` produce one merged result with consistent per-language summaries.
3. Rust semantic coverage reaches parity for shared concepts using ast-grep-first extraction.
4. Python-only analyses remain available with structured capability diagnostics under Rust scope.
5. No runtime dependency or docs references remain for `rpygrep`.
6. CQ remains independent of `src/`.

---

## CQ-Focused Validation Matrix

### Core quality gate

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q
```

### CQ-targeted validation subset

```bash
uv run pytest -q tests/unit/cq tests/integration/cq tests/e2e/cq tests/cli_golden
```

### Unified behavior spot checks

```bash
./cq search build_graph --format json --no-save-artifact
./cq search build_graph --lang rust --format json --no-save-artifact
./cq q "entity=function name=build_graph" --format json --no-save-artifact
./cq run --step '{"type":"search","query":"build_graph"}' --format json --no-save-artifact
```

