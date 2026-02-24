# CQ Rust Query Deficiency Remediation Implementation Plan v1 (2026-02-24)

## Scope Summary

This plan remediates all seven high-confidence deficiencies documented in `docs/reviews/cq_rust_query_audit_2026-02-24.md`: Rust query-pack compile failures, `q` summary/section inconsistencies, `run`/`chain` scope leakage, grouped-import undercount, Rust string-literal misclassification, unsupported-macro contract noise, and Rust neighborhood symbol ambiguity. Design stance: incremental, test-first hardening with no compatibility shims for output contracts; one targeted input compatibility normalization is retained for `run --step/--steps` aliases (`lang`/`in`).

## Design Principles

1. Grammar ABI first: treat Rust node kinds/fields as runtime ABI (`Language.node_kind_*`, `field_name_for_id`) and reject non-existent query references.
2. Manifest-driven query packs: keep Rust query-pack behavior aligned with the grammar bundle surface (`tree-sitter.json` + `queries/*.scm`) instead of ad-hoc assumptions.
3. Named-node structural matching over text heuristics: for Rust imports and signatures, prefer AST-stable extraction paths consistent with ast-grep/tree-sitter guidance.
4. Summary contract determinism: `matches`, `total_matches`, language partitions, and section payloads must agree for every execution mode.
5. Scope integrity in orchestration: `lang_scope` and `in_dir` must propagate identically across direct, `run`, and `chain` execution surfaces.
6. Clean fail-open for unsupported macros: unsupported Rust paths emit capability diagnostics only, never pseudo-analytical results.
7. Deterministic target resolution: symbol-only neighborhood resolution must bias toward canonical definitions (`fn`) and surface ambiguity explicitly.

## Current Baseline

- `tools/cq/search/queries/rust/50_modules_imports.scm` references `visibility:` as a field; Rust runtime fields do not include `visibility`, causing query compile failure.
- `tools/cq/search/queries/rust/85_locals.scm` references node type `closure_parameter`; Rust runtime emits `parameter` in closure parameter lists.
- `tools/cq/orchestration/multilang_summary.py` can emit `languages.<lang>.matches > 0` while `languages.<lang>.total_matches == 0` and top-level `summary.total_matches == 0`.
- `tools/cq/run/spec.py` uses `SearchStep.lang_scope`/`SearchStep.in_dir`; JSON keys `lang`/`in` are silently dropped during strict decode because unknown fields are currently tolerated.
- `tools/cq/run/chain.py` `_build_search_step()` forwards `in_dir` but does not forward `--lang` into `SearchStep.lang_scope`.
- `tools/cq/query/import_utils.py` `extract_rust_use_import_name()` returns one terminal symbol, so grouped Rust imports (`use foo::{A, B};`) under-match name-filtered `entity=import` queries.
- `tools/cq/search/pipeline/classifier.py` maps `string`/`concatenated_string` but misses Rust string node kinds (`string_literal`, raw/byte variants), allowing misclassification as `callsite` via parent fallback.
- `tools/cq/macros/rust_fallback_policy.py` and `tools/cq/macros/multilang_fallback.py` apply fallback search even when Rust capability is `none`, producing noisy pseudo-results and regex parse noise (`exceptions`).
- `tools/cq/core/summary_types.py` maps `sig-impact` and `macro:sig-impact` to `impact`, obscuring command-level variant identity.
- `tools/cq/neighborhood/target_resolution.py` symbol scoring can choose impl-adjacent Rust lines over canonical `fn` definitions for symbol-only targets.

## S1. Rust Query-Pack ABI Conformance and Lint Hardening

### Goal

Fix Rust query-pack compile failures and make lint diagnostics precise enough to catch node/field drift immediately. Rust `search` must run with `query_pack_lint.status == "ok"` under the default environment.

### Representative Code Snippets

```scheme
; tools/cq/search/queries/rust/50_modules_imports.scm
; Remove invalid `visibility:` field usage and match visibility modifier as a child node.
((use_declaration
   (visibility_modifier) @import.visibility
   argument: (scoped_identifier) @import.path) @import.declaration
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_public")
 (#set! cq.anchor "import.path"))
```

```scheme
; tools/cq/search/queries/rust/85_locals.scm
; Rust closure parameters use `parameter`, not `closure_parameter`.
((closure_expression
   parameters: (closure_parameters
                 (parameter pattern: (identifier) @local.definition))) @local.binding
 (#set! cq.emit "locals")
 (#set! cq.kind "local_definition")
 (#set! cq.anchor "local.definition"))
```

```python
# tools/cq/search/tree_sitter/contracts/query_models.py
# Preserve compile failure detail so lint output is actionable.
try:
    query = compile_query(source)
except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
    issues.append(
        QueryPackLintIssueV1(
            language=language,
            pack_name=pack_name,
            code="compile_error",
            message=str(exc) or type(exc).__name__,
        )
    )
    return tuple(issues)
```

### Files to Edit

- `tools/cq/search/queries/rust/50_modules_imports.scm`
- `tools/cq/search/queries/rust/85_locals.scm`
- `tools/cq/search/tree_sitter/contracts/query_models.py`
- `tools/cq/search/tree_sitter/query/lint.py`
- `tools/cq/search/pipeline/smart_search_summary.py`

### New Files to Create

- `tests/unit/cq/search/test_rust_query_pack_lint.py`
- `tests/e2e/cq/test_rust_query_pack_lint_e2e.py`

### Legacy Decommission/Delete Scope

- Delete invalid query fragments in `50_modules_imports.scm` that use `visibility:` field selectors.
- Delete invalid `closure_parameter` references in `85_locals.scm`.
- Delete fixture expectations that require `"Query pack lint failed: 2 errors"` for normal Rust search flows.

---

## S2. Rust Import Coverage for Grouped `use` Forms

### Goal

Close Rust `entity=import` false negatives for grouped imports and aliases, and align Rust import extraction across `q` and search evidence payloads.

### Representative Code Snippets

```python
# tools/cq/query/import_utils.py
def extract_rust_use_import_names(text: str) -> tuple[str, ...]:
    """Return all locally-bound names from a Rust `use` declaration."""
    cleaned = text.split("//", maxsplit=1)[0].strip()
    match = re.match(r"use\s+([^;]+);?", cleaned)
    if not match:
        return ()
    target = match.group(1).strip()
    if "{" not in target:
        leaf = target.rsplit("::", maxsplit=1)[-1].strip()
        alias = leaf.rsplit(" as ", maxsplit=1)[-1].strip() if " as " in leaf else leaf
        return (alias.strip("{} "),) if alias else ()

    prefix, grouped = target.split("{", maxsplit=1)
    grouped = grouped.rsplit("}", maxsplit=1)[0]
    out: list[str] = []
    for spec in grouped.split(","):
        token = spec.strip()
        if not token:
            continue
        alias = token.rsplit(" as ", maxsplit=1)[-1].strip() if " as " in token else token
        out.append(alias)
    return tuple(name for name in out if name)
```

```python
# tools/cq/query/executor_definitions.py
if record.record == "import" and record.kind == "use_declaration":
    extracted_names = extract_rust_use_import_names(record.text)
    if name.startswith("~"):
        pattern = name[1:]
        return any(re.search(pattern, candidate) for candidate in extracted_names)
    return name in extracted_names
```

```python
# tools/cq/search/tree_sitter/rust_lane/fact_extraction.py
# Build full grouped import paths when pack captures base + leaf.
base = row.captures.get("import.base")
leaf = row.captures.get("import.leaf")
if isinstance(base, str) and isinstance(leaf, str):
    target_path = f"{base}::{leaf}"
```

### Files to Edit

- `tools/cq/query/import_utils.py`
- `tools/cq/query/executor_definitions.py`
- `tools/cq/query/executor_definitions_imports.py`
- `tools/cq/search/queries/rust/50_modules_imports.scm`
- `tools/cq/search/tree_sitter/rust_lane/fact_extraction.py`

### New Files to Create

- `tests/e2e/cq/_fixtures/rust_grouped_imports.rs`

### Legacy Decommission/Delete Scope

- Delete one-name-only Rust import matching assumptions in `matches_name()` for `use_declaration` records.
- Delete single-target dependence on `extract_rust_use_import_name()` in name-filtered Rust import paths.

---

## S3. `q` Summary and Section Contract Consistency

### Goal

Guarantee that Rust `q` outputs have consistent summary counters and deterministic section surfaces whenever findings exist.

### Representative Code Snippets

```python
# tools/cq/orchestration/multilang_summary.py
matches = _coerce_int(summary_map.get("matches"), fallback_matches)
if matches == 0:
    matches = _coerce_int(summary_map.get("total_matches"), fallback_matches)
raw_total = _coerce_int(summary_map.get("total_matches"), 0)
total_matches = raw_total if raw_total > 0 else matches
...
payload = LanguagePartitionStats(
    matches=matches,
    total_matches=total_matches,
    ...
)
```

```python
# tools/cq/query/section_fallbacks.py
def ensure_query_sections(result: CqResult, *, title: str) -> CqResult:
    if result.sections or not result.key_findings:
        return result
    return append_result_section(
        result,
        Section(title=title, findings=tuple(result.key_findings)),
    )
```

```python
# tools/cq/query/executor_runtime_entity.py / executor_runtime_pattern.py
summary = apply_summary_mapping(
    summary,
    {
        "matches": len(key_findings),
        "total_matches": len(key_findings),
        "files_scanned": len({r.file for r in records}),
    },
)
result = ensure_query_sections(result, title="Findings")
```

### Files to Edit

- `tools/cq/orchestration/multilang_summary.py`
- `tools/cq/query/executor_runtime_entity.py`
- `tools/cq/query/executor_runtime_pattern.py`
- `tools/cq/query/executor_definitions.py`
- `tools/cq/core/summary_update_contracts.py`

### New Files to Create

- `tools/cq/query/section_fallbacks.py`
- `tests/unit/cq/test_query_section_fallbacks.py`

### Legacy Decommission/Delete Scope

- Delete zero-default `total_matches` propagation behavior when positive `matches` exists.
- Delete empty-section success paths for non-empty `q` result sets.

---

## S4. `run`/`chain` Rust Scope Propagation and Step Decoding

### Goal

Make `run` and `chain` behavior equivalent to direct commands for Rust scope controls, including compatibility handling for step payload aliases.

### Representative Code Snippets

```python
# tools/cq/run/step_payload_normalization.py
_SEARCH_ALIASES = {
    "lang": "lang_scope",
    "in": "in_dir",
}

def normalize_step_payload(payload: dict[str, object]) -> dict[str, object]:
    if payload.get("type") != "search":
        return payload
    out = dict(payload)
    for src, dst in _SEARCH_ALIASES.items():
        if src in out and dst not in out:
            out[dst] = out.pop(src)
    return out
```

```python
# tools/cq/run/step_decode.py
raw = decode_json_strict(raw_json, type_=dict[str, object])
normalized = normalize_step_payload(raw)
return convert_strict(normalized, type_=RunStep)
```

```python
# tools/cq/run/chain.py
return SearchStep(
    query=_require_str_arg(args, 0, "query"),
    mode=mode,
    include_strings=getattr(opts, "include_strings", False),
    in_dir=getattr(opts, "in_dir", None),
    lang_scope=parse_query_language_scope(str(getattr(opts, "lang", "auto"))),
)
```

### Files to Edit

- `tools/cq/run/spec.py`
- `tools/cq/run/step_decode.py`
- `tools/cq/run/loader.py`
- `tools/cq/run/chain.py`
- `tests/unit/cq/test_run_plan_parsing.py`
- `tests/unit/cq/test_chain_compilation.py`
- `tests/e2e/cq/test_run_command_e2e.py`
- `tests/e2e/cq/test_chain_command_e2e.py`

### New Files to Create

- `tools/cq/run/step_payload_normalization.py`
- `tests/unit/cq/test_run_step_payload_normalization.py`
- `tests/e2e/cq/test_rust_run_chain_scope_e2e.py`

### Legacy Decommission/Delete Scope

- Delete permissive unknown-key behavior for run-step decoding without normalization.
- Delete chain search-step construction that omits `lang_scope` propagation.

---

## S5. Rust String Literal Classification and `--include-strings` Semantics

### Goal

Correct Rust literal classification so non-code filtering works deterministically, and ensure `--include-strings` visibly changes output for Rust literal probes.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/classifier.py
NODE_KIND_MAP.update(
    {
        "string_literal": ("string_match", 0.95),
        "raw_string_literal": ("string_match", 0.95),
        "byte_string_literal": ("string_match", 0.95),
        "raw_byte_string_literal": ("string_match", 0.95),
    }
)
```

```python
# tests/e2e/cq/test_rust_literal_string_classification_e2e.py
result_code_only = run_cq_result([
    "search",
    "Async UDFs require the async-udf feature",
    "--literal",
    "--lang",
    "rust",
    "--in",
    "rust",
    "--format",
    "json",
    "--no-save-artifact",
])
assert all(f.category != "callsite" for f in result_code_only.key_findings)
```

### Files to Edit

- `tools/cq/search/pipeline/classifier.py`
- `tools/cq/search/pipeline/smart_search_sections.py`
- `tests/unit/cq/search/test_classifier.py`
- `tests/e2e/cq/test_search_command_e2e.py`

### New Files to Create

- `tests/e2e/cq/test_rust_literal_string_classification_e2e.py`
- `tests/e2e/cq/_fixtures/rust_string_literals.rs`

### Legacy Decommission/Delete Scope

- Delete Rust string-to-callsite fallback behavior caused by unmapped Rust string node kinds.

---

## S6. Unsupported Rust Macro Contract Normalization and `sig-impact` Variant Routing

### Goal

Ensure unsupported Rust macro surfaces return clean capability diagnostics without pseudo-analysis data, and assign `sig-impact` a correct summary variant identity.

### Representative Code Snippets

```python
# tools/cq/core/summary_types.py
type SummaryVariantName = Literal[
    "search", "calls", "impact", "sig-impact", "run", "neighborhood"
]

class SigImpactSummaryV1(SummaryEnvelopeV1, frozen=True):
    summary_variant: SummaryVariantName = "sig-impact"
```

```python
# tools/cq/macros/rust_fallback_policy.py
levels = CAPABILITY_MATRIX.get(f"macro:{policy.macro_name}", {})
rust_level = levels.get("rust", "none")
if rust_level == "none":
    return apply_unsupported_macro_contract(
        result=result,
        macro_name=policy.macro_name,
        root=root,
    )
```

```python
# tools/cq/macros/_rust_fallback.py
def rust_fallback_search(..., mode: QueryMode = QueryMode.LITERAL) -> ...:
    matches = search_content(..., pattern=pattern, lang_scope="rust", mode=mode)
```

### Files to Edit

- `tools/cq/core/summary_types.py`
- `tools/cq/search/semantic/diagnostics.py`
- `tools/cq/macros/rust_fallback_policy.py`
- `tools/cq/macros/multilang_fallback.py`
- `tools/cq/macros/_rust_fallback.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/macros/imports.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/macros/bytecode.py`
- `tests/unit/cq/macros/test_rust_fallback_policy.py`
- `tests/unit/cq/test_summary_variants.py`

### New Files to Create

- `tests/e2e/cq/test_rust_macro_contracts_e2e.py`

### Legacy Decommission/Delete Scope

- Delete summary-mode mapping that forces `sig-impact` into `impact` variant.
- Delete unsupported-path fallback searches that generate pseudo Rust evidence for macros marked `rust: none`.
- Delete regex-pattern fallback usage in `exceptions` unsupported Rust path.

---

## S7. Rust Neighborhood Symbol Resolution Determinism

### Goal

Make Rust symbol-only neighborhood resolution prefer canonical function definitions and report ambiguity deterministically.

### Representative Code Snippets

```python
# tools/cq/neighborhood/target_resolution.py
if language == "rust":
    if re.search(rf"^\s*(pub(\([^)]*\))?\s+)?(async\s+)?fn\s+{name}\b", line_text):
        score += 600
    elif re.search(rf"^\s*impl\b", line_text):
        score -= 120
```

```python
# tools/cq/neighborhood/target_resolution.py
if len(tied_candidates) > 1:
    degrades.append(
        DegradeEventV1(
            stage="target_resolution",
            severity="info",
            category="ambiguous_symbol",
            message=f"Multiple Rust symbol candidates for {symbol_name}; chose best-ranked",
        )
    )
```

### Files to Edit

- `tools/cq/neighborhood/target_resolution.py`
- `tools/cq/neighborhood/executor.py`
- `tests/unit/cq/neighborhood/test_executor.py`
- `tests/e2e/cq/test_neighborhood_command_e2e.py`

### New Files to Create

- `tests/unit/cq/neighborhood/test_target_resolution_rust.py`

### Legacy Decommission/Delete Scope

- Delete Rust scoring tie behavior that allows impl-item matches to outrank direct `fn` definitions for same symbol target.

---

## S8. Rust CQ Regression Matrix and Review Artifact Refresh

### Goal

Encode the audited Rust query matrix as executable regression coverage and refresh review artifacts to track remediation state and residual risk.

### Representative Code Snippets

```python
# tests/e2e/cq/test_rust_query_regression_matrix_e2e.py
@pytest.mark.e2e
@pytest.mark.requires_ast_grep
@pytest.mark.requires_tree_sitter
@pytest.mark.parametrize("case", MATRIX_CASES)
def test_rust_matrix_cases(run_cq_result: Callable[..., CqResult], case: MatrixCase) -> None:
    result = run_cq_result(case.argv)
    assert case.assertions(result)
```

```markdown
# docs/reviews/cq_rust_query_audit_2026-02-24.md
## Remediation Status
- S1: complete / pending
- S2: complete / pending
...
```

### Files to Edit

- `docs/reviews/cq_rust_query_audit_2026-02-24.md`
- `tests/e2e/cq/test_rust_query_support.py`
- `tests/e2e/cq/test_q_command_e2e.py`
- `tests/e2e/cq/test_run_command_e2e.py`
- `tests/e2e/cq/test_chain_command_e2e.py`
- `tests/e2e/cq/test_search_command_e2e.py`
- `tests/e2e/cq/fixtures/*.json` (golden snapshots affected by S1-S7)

### New Files to Create

- `tests/e2e/cq/test_rust_query_regression_matrix_e2e.py`
- `tests/e2e/cq/fixtures/rust_query_regression_matrix_cases.json`

### Legacy Decommission/Delete Scope

- Delete stale golden expectations that assert lint-failure diagnostics (`"Query pack lint failed: 2 errors"`) as normal behavior.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S5)

- Delete all fixture assertions and snapshot lines that encode Rust query-pack lint failure as expected output.
- Delete temporary Rust-string misclassification allowances in classifier/search tests.

### Batch D2 (after S2, S3)

- Delete one-name Rust `use` extraction assumption across import query matching and finding rendering.
- Delete import-query empty-section behavior when findings are present.

### Batch D3 (after S4)

- Delete non-normalized run-step search payload acceptance paths that silently ignore `lang`/`in` keys.
- Delete chain search-step builder behavior that defaults to `lang_scope="auto"` despite explicit `--lang`.

### Batch D4 (after S6)

- Delete unsupported-macro pseudo-evidence paths for Rust-`none` capability macros.
- Delete `sig-impact -> impact` summary-variant compatibility mapping.

### Batch D5 (after S7, S8)

- Delete ambiguity-blind neighborhood symbol resolution scoring fallback for Rust.
- Delete ad-hoc manual audit-only commands from review narrative once matrix tests are authoritative.

## Implementation Sequence

1. S1. Rust query-pack ABI conformance and lint hardening. This removes systemic compile failures and stabilizes search diagnostics.
2. S2. Rust import coverage for grouped `use` forms. Import undercount should be corrected before recalibrating summary/section surfaces.
3. S3. `q` summary and section contract consistency. Once match extraction is correct, summary and presentation consistency can be fixed without masking extraction defects.
4. S4. `run`/`chain` scope propagation and step decoding. Scope correctness should be fixed before e2e matrix expansion.
5. S5. Rust string classification and `--include-strings` semantics. This is independent after S1 and can proceed in parallel with S4 if needed.
6. S6. Unsupported macro contract normalization and variant routing. Contract-level behavior changes are high-impact and should follow core query/search correctness.
7. S7. Rust neighborhood symbol resolution determinism. Targeting is isolated and can land after output contracts stabilize.
8. S8. Regression matrix and review artifact refresh. Finalize with full coverage and updated review status once behavior is stable.
9. D1. Execute cross-scope deletions after S1 and S5.
10. D2. Execute cross-scope deletions after S2 and S3.
11. D3. Execute cross-scope deletions after S4.
12. D4. Execute cross-scope deletions after S6.
13. D5. Execute cross-scope deletions after S7 and S8.

## Implementation Checklist

- [x] S1. Rust query-pack ABI conformance and lint hardening
- [x] S2. Rust import coverage for grouped `use` forms
- [x] S3. `q` summary and section contract consistency
- [x] S4. `run`/`chain` Rust scope propagation and step decoding
- [x] S5. Rust string literal classification and `--include-strings` semantics
- [x] S6. Unsupported Rust macro contract normalization and `sig-impact` variant routing
- [x] S7. Rust neighborhood symbol resolution determinism
- [x] S8. Rust CQ regression matrix and review artifact refresh
- [x] D1. Cross-scope deletion batch
- [x] D2. Cross-scope deletion batch
- [x] D3. Cross-scope deletion batch
- [x] D4. Cross-scope deletion batch
- [x] D5. Cross-scope deletion batch

## Post-Implementation Audit (2026-02-24)

Audit method:
- Verified planned files and new artifacts exist in the repository.
- Verified code-path behavior for each remediation area (`S1`-`S8`) using source inspection.
- Ran focused behavioral confirmation tests:
  - `uv run pytest -q tests/unit/cq/search/test_rust_query_pack_lint.py tests/e2e/cq/test_rust_query_pack_lint_e2e.py tests/unit/cq/test_query_section_fallbacks.py tests/unit/cq/test_run_step_payload_normalization.py tests/e2e/cq/test_rust_run_chain_scope_e2e.py tests/e2e/cq/test_rust_literal_string_classification_e2e.py tests/e2e/cq/test_rust_macro_contracts_e2e.py tests/unit/cq/neighborhood/test_target_resolution_rust.py tests/e2e/cq/test_rust_query_regression_matrix_e2e.py`
  - Result: `22 passed`.

Scope status:
- `S1`: complete. Rust query packs compile cleanly and lint contract is covered by unit/e2e tests.
- `S2`: complete. Grouped Rust `use` name matching is handled in query execution and covered by e2e matrix/query support tests.
- `S3`: complete. Rust `q` summary `matches`/`total_matches` and fallback section behavior are now deterministic and covered by unit tests.
- `S4`: complete. `run`/`chain` now normalize `lang`/`in` aliases and preserve Rust scope propagation; covered by unit and e2e tests.
- `S5`: complete. Rust string literal node kinds are classified as `string_match` and `--include-strings` behavior is verified by e2e.
- `S6`: complete. Rust-unsupported macro paths emit capability diagnostics only, and `sig-impact` maps to its own summary variant.
- `S7`: complete. Rust neighborhood symbol resolution now prefers canonical `fn` definitions and emits deterministic ambiguity degrade events.
- `S8`: complete. Regression matrix coverage and review remediation status updates are present and passing.

Cross-scope deletion batch status:
- `D1`: complete.
- `D2`: complete.
- `D3`: complete.
- `D4`: complete.
- `D5`: complete.

Plan-to-implementation deviations (non-blocking, scope-complete):
- Some file-level edits listed in the original plan were not required after centralized fixes landed (for example, `tools/cq/run/spec.py`, `tools/cq/search/tree_sitter/query/lint.py`, `tools/cq/search/pipeline/smart_search_summary.py`, `tools/cq/core/summary_update_contracts.py`, and select macro entrypoint modules). Behavior targeted by those items is now covered via shared normalization/fallback paths and passing tests.

Remaining scope to implement:
- None for `S1`-`S8` and `D1`-`D5`.
