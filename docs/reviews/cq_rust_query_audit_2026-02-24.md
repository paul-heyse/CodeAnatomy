# CQ Rust Query Audit - 2026-02-24

## Scope

Audit objective:
- Run environment setup (`scripts/bootstrap_codex.sh`, `uv sync`).
- Exercise a broad set of CQ query types against Rust code.
- Independently verify CQ outputs with `rg` and `ast-grep`.
- Identify Rust CQ configuration and behavior deficiencies.

Repository: `CodeAnatomy`  
Date: `2026-02-24`

## Environment Setup

Commands run:

```bash
bash scripts/bootstrap_codex.sh
uv sync
```

Observed:
- Bootstrap completed successfully.
- Python pinned at `3.13.12`.
- `uv sync` completed successfully.

## Query Matrix (CQ vs Independent Confirmation)

| Query Type | CQ Command | CQ Result | Independent Confirmation | Assessment |
|---|---|---|---|---|
| `search` identifier | `./cq search register_udf --lang rust --in rust --format json` | `total_matches=7`, rust scope only, but `query_pack_lint failed` | `rg -n "\\bregister_udf\\b" rust --glob '*.rs'` => `7` | Recall correct; query-pack lint defect present |
| `search` regex | `./cq search "register_.*udf" --regex --lang rust --in rust --format json` | `total_matches=20`, but `query_pack_lint failed` | `rg -n "register_.*udf" rust --glob '*.rs'` => `20` | Recall correct; query-pack lint defect present |
| `search` literal + include-strings off | `./cq search "Async UDFs require the async-udf feature" --literal --lang rust --in rust --format json` | `total_matches=4`, classified as `callsite: 4` | `rg` shows 4 occurrences, all string literals | Classification defect (strings treated as callsites) |
| `search` literal + include-strings on | same + `--include-strings` | also `total_matches=4`, same classification | same | `--include-strings` has no observable effect for this Rust literal case |
| `q` entity function | `./cq q "entity=function name=register_udf lang=rust in=rust" --format json` | `languages.rust.matches=1`, but `summary.total_matches=0`, only Neighborhood Preview section | `rg -n "\\bfn\\s+register_udf\\s*\\(" rust --glob '*.rs'` => `1` | Match found but summary/section rendering inconsistent |
| `q` entity import | `./cq q "entity=import name=SessionContext lang=rust in=rust" --format json` | `53` key findings, `summary.total_matches=0`, no sections | `rg -n "^\\s*use\\s+.*SessionContext" rust --glob '*.rs'` => `67`; `14` missing from CQ | Import undercount + summary/section inconsistency |
| `q` pattern (concrete) | `./cq q "pattern='ctx.register_udf(udf)' lang=rust in=rust" --format json` | `rust_matches=2`, `key_findings=2`, `summary.total_matches=0`, no sections | `ast-grep run -p 'ctx.register_udf(udf)' -l rust --json=stream rust` => `2` | Match correctness good; reporting pipeline inconsistent |
| `q` pattern (metavars) | `./cq q "pattern='$OBJ.register_udf($X)' lang=rust in=rust" --format json` | `rust_matches=6`, `key_findings=6`, `summary.total_matches=0`, no sections | `ast-grep run -p '$OBJ.register_udf($X)' -l rust --json=stream rust` => `6` | Match correctness good; reporting pipeline inconsistent |
| `q` pattern (function signature) | `./cq q "pattern='pub fn $F($ARGS) -> $RET { $$$BODY }' lang=rust in=rust" --format json` | `rust_matches=394`, `summary.total_matches=0`, no sections | `ast-grep` same pattern => `394` | Pattern engine good; reporting pipeline inconsistent |
| `q` broad function census | `./cq q "entity=function lang=rust in=rust" --format json` | `rust_matches=3455`, `summary.total_matches=0`, only Neighborhood Preview section | `rg -n "^\\s*(pub(\\([^)]*\\))?\\s+)?(async\\s+)?fn\\s+" rust --glob '*.rs'` => `3479` | Near-consistent corpus coverage; reporting pipeline inconsistent |
| `calls` macro | `./cq calls register_udf --include 'rust/**/*.rs' --format json` | key finding says "Found 2 calls", no call-site section, capability says partial support | direct call occurrences for `register_udf(` are `7` total lexical occurrences in Rust | Under-reporting and incomplete presentation for Rust |
| `impact` macro | `./cq impact register_udf --param udf --include 'rust/**/*.rs' --format json` | says taint reaches `2` sites + capability says unsupported; emits unrelated SyntaxWarnings | Rust call references are >2 for `register_udf` | Internally inconsistent behavior for unsupported macro |
| `sig-impact` macro | `./cq sig-impact ... --include 'rust/**/*.rs' --format json` | reports unsupported, but `summary_variant` returned as `impact` | N/A | Wrong summary variant / routing bug |
| `imports` macro | `./cq imports --include 'rust/**/*.rs' --format json` | capability limitation: unsupported for rust | N/A | Expected unsupported, but reporting path returns generic `search` variant |
| `scopes` macro | `./cq scopes rust/datafusion_ext/src/udf/primitives.rs --format json` | unsupported for rust | N/A | Expected unsupported |
| `exceptions` macro | `./cq exceptions --include 'rust/**/*.rs' --format json` | unsupported for rust, plus ripgrep regex parse error emitted | N/A | Error-noise bug in unsupported path |
| `bytecode-surface` macro | `./cq bytecode-surface rust/datafusion_ext/src/udf/primitives.rs --format json` | unsupported for rust | N/A | Expected unsupported |
| `neighborhood` symbol target | `./cq neighborhood register_udf --lang rust --format json` | resolves to `impl PySessionContext` (impl item), not directly to function | file-target variant resolves correctly to function | Symbol resolution ambiguity defect |
| `neighborhood` file target | `./cq neighborhood rust/datafusion_python/src/context.rs:834:12 --lang rust --format json` | correctly targets `register_udf` function and returns structural slices | manual source inspection aligns | Works as expected |
| `run` search step | `./cq run --steps '[{"type":"search","query":"register_udf","lang":"rust","in":"rust"}]' --format json` | step runs with `lang_scope=auto`, includes Python+Rust anyway | direct `search --lang rust --in rust` stays Rust-only | Run-step option propagation defect (`lang`, `in` ignored) |
| `run` q step | `./cq run --steps '[{"type":"q","query":"entity=function name=register_udf lang=rust in=rust"}]' --include 'rust/**/*.rs' --format json` | reproduces same `q` summary mismatch | same as direct `q` | Consistent with `q` reporting defect |
| `chain` search+q | `./cq chain search register_udf --lang rust --in rust AND q "entity=function ..."` | search step still `lang_scope=auto`, includes Python | direct search with same args is Rust-only | Chain parser/forwarding defect for search-step options |

## High-Confidence Deficiencies

1. Rust query-pack lint is failing on every Rust `search`.
- Evidence: repeated `query_pack_lint failed` with
  - `rust:50_modules_imports.scm:compile_error:QueryError`
  - `rust:85_locals.scm:compile_error:QueryError`
- Impact: Rust enrichment/classification planes are partially broken by default.

2. `q` Rust result reporting is internally inconsistent.
- `key_findings` and `languages.rust.matches` carry real match counts.
- `summary.total_matches` is `0` across many Rust `q` runs.
- Section rendering is often empty or reduced to Neighborhood Preview.
- Impact: downstream automation and human interpretation are unreliable unless consumers parse `key_findings`.

3. `run` and `chain` do not reliably preserve Rust scoping for `search` steps.
- `lang=rust` and `in=rust` passed in step/segment still yielded `lang_scope=auto`.
- Python results leaked into supposedly Rust-only runs.
- Impact: mixed-language contamination in scripted workflows.

4. Rust import entity extraction misses grouped-import forms.
- For `SessionContext`, CQ returned `53`, while independent Rust import lines were `67`.
- No false positives found (`extra_in_cq=0`), only false negatives (`14` missing).
- Missing examples include grouped imports such as:
  - `rust/codeanatomy_engine/src/compiler/compile_phases.rs:5`
  - `rust/codeanatomy_engine/src/compiler/pushdown_probe_extract.rs:12`
  - `rust/datafusion_python/src/catalog.rs:38`
- Impact: refactor and dependency analysis on Rust imports is incomplete.

5. String-literal handling in Rust `search` is misclassified and flag-insensitive.
- Literal-only query yielded `callsite` classifications.
- `--include-strings` did not change output.
- Impact: category-level analytics and "code-only vs non-code" filtering are not trustworthy.

6. Unsupported Rust macros return noisy/inconsistent behavior instead of clean fail-open.
- `impact`: simultaneously reports unsupported + pseudo-results.
- `sig-impact`: returns unsupported but mislabeled `summary_variant=impact`.
- `exceptions`: emits regex parse errors despite unsupported status.
- Impact: automation and users cannot rely on contract-level behavior for unsupported macros.

7. Symbol-based Rust neighborhood target resolution is ambiguous.
- `neighborhood register_udf --lang rust` resolved to an impl item.
- File-position target resolved correctly.
- Impact: symbol-only workflows are less deterministic in Rust.

## Notable Strengths

- Rust `search` recall was accurate for tested identifier and regex probes.
- Rust `q` pattern matching itself appears strong when using explicit metavariable patterns (validated exactly against `ast-grep`).
- Rust file-target neighborhood assembly works and returns stable structural slices.

## Conclusions

Rust CQ support has a solid matching core for `search` and `q` pattern engines, but current Rust configuration/reporting has several high-impact defects for production automation:

- Broken Rust query-pack lint in default search enrichment.
- Incorrect/empty summary and section surfaces for Rust `q` outputs.
- Run/chain scope leakage from Rust into Python for search steps.
- Import coverage blind spots for grouped `use` declarations.
- Inconsistent unsupported-macro behavior and error noise.

Primary remediation order:
1. Fix Rust query-pack compile errors (`50_modules_imports.scm`, `85_locals.scm`).
2. Correct Rust `q` result wiring (`summary.total_matches`, section population).
3. Fix `run`/`chain` propagation of Rust search scope options.
4. Patch Rust import extraction for grouped imports.
5. Normalize unsupported macro behavior to a clean, deterministic contract.

## Remediation Status (2026-02-24)

- S1. Rust query-pack ABI conformance and lint hardening: complete.
- S2. Rust grouped-import coverage for `entity=import`: complete.
- S3. `q` summary/section consistency for Rust: complete.
- S4. `run`/`chain` Rust scope propagation and step alias decoding: complete.
- S5. Rust string-literal classification and `--include-strings` semantics: complete.
- S6. Unsupported Rust macro contract normalization and `sig-impact` routing: complete.
- S7. Rust neighborhood symbol-resolution determinism: complete.
- S8. Rust regression matrix + artifact refresh: complete.

Cross-scope deletion batches:
- D1: complete.
- D2: complete.
- D3: complete.
- D4: complete.
- D5: complete.

Authoritative regression coverage now includes:
- `tests/e2e/cq/test_rust_query_pack_lint_e2e.py`
- `tests/e2e/cq/test_rust_literal_string_classification_e2e.py`
- `tests/e2e/cq/test_rust_run_chain_scope_e2e.py`
- `tests/e2e/cq/test_rust_macro_contracts_e2e.py`
- `tests/e2e/cq/test_rust_query_regression_matrix_e2e.py`
