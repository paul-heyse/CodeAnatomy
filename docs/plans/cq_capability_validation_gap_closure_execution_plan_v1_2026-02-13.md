# CQ Capability Validation Gap-Closure Execution Plan v1 (Prioritized Reliability + Contract Alignment)

**Status:** Proposed  
**Date:** 2026-02-13  
**Scope:** `tools/cq` runtime + CQ CLI command layer + CQ unit/e2e/golden coverage  
**Input Validation Artifacts:** `build/cq_capability_validation/summary.tsv`, `build/cq_capability_validation/logs/*`

## 1. Executive Summary

This plan closes the highest-value gaps identified from representative production command runs across CQ capability surface area (`search`, `q`, `calls`, `impact`, `sig-impact`, `scopes`, `imports`, `exceptions`, `side-effects`, `bytecode-surface`, `neighborhood`, `run`, `chain`, `report`, `ldmd`, `schema`, admin commands).

Baseline from the validation sweep:

1. 37 representative command cases executed.
2. 31 succeeded, 6 failed in the initial matrix.
3. After command normalization (quoted globs, valid LDMD section IDs), remaining material product gaps are:
   - schema export failures (`schema --kind result|components`);
   - include/exclude filters effectively ignored in `imports`, `exceptions`, `side-effects`;
   - contract/documentation mismatch for top-level composite `q` (`any/all/not`) and LDMD `root` navigation ergonomics;
   - unstable Python LSP enrichment availability (high `request_failed` rate, fail-open but low enrichment utility);
   - output-contract consistency issues for some format paths (`--format json` not always yielding JSON envelopes).

Hard-cutover posture for this plan:

1. No compatibility shims for broken schema paths or legacy output behavior.
2. Command contracts should match documented/skill examples, or docs are updated in the same scope when behavior is intentionally changed.
3. Fail-open behavior remains for enrichment, but degradation semantics must be deterministic and explicit.

---

## 2. Priority Matrix

## P0 (Blocker)

1. Fix schema export crashes for `cq schema --kind result` and `--kind components`.
2. Make `--include/--exclude` authoritative for `imports`, `exceptions`, `side-effects`.

## P1 (High)

1. Align `q` composite semantics with documented behavior (`any/all/not` without `entity=`).
2. Add LDMD `root` section ergonomics and consistent `--format json` envelope behavior.
3. Harden Python LSP availability/retry/status semantics to reduce `request_failed` degradation.

## P2 (Medium)

1. CLI UX consistency for macro options and warning hygiene.
2. Documentation/skill alignment for Rust pattern query examples and composite query usage.

---

## 3. Locked Decisions

1. Keep fail-open semantics for LSP and enrichment.
2. Keep no-compatibility cutover approach; do not preserve broken schema or format paths.
3. Enforce explicit msgspec boundary typing for public schema-exportable models.
4. Use shared scope/file filtering utilities across macro commands (no per-command filtering drift).
5. Run full CQ quality gate only after all implementation workstreams are complete.

---

## 4. Workstreams

## Workstream 1 (P0): Schema Export Recovery and msgspec Boundary Cleanup

### Scope

Restore `cq schema --kind result` and `cq schema --kind components` by eliminating unsupported `object`-typed fields from schema-exported contract paths, or providing deterministic schema hooks where dynamic payloads are required.

### Representative Code Snippets

```python
# tools/cq/core/json_types.py
from __future__ import annotations
from typing import TypeAlias

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
```

```python
# tools/cq/core/schema_export.py
def _schema_hook(type_: object) -> dict[str, object] | None:
    if type_ is object:
        return {
            "anyOf": [
                {"type": "string"},
                {"type": "number"},
                {"type": "integer"},
                {"type": "boolean"},
                {"type": "null"},
                {"type": "array"},
                {"type": "object"},
            ]
        }
    return None


def cq_result_schema() -> dict[str, object]:
    return msgspec.json.schema(CqResult, schema_hook=_schema_hook)
```

### Target File Edits

1. `tools/cq/core/schema.py`
2. `tools/cq/core/contracts.py`
3. `tools/cq/core/schema_export.py`
4. `tools/cq/core/public_serialization.py`

### New Files

1. `tools/cq/core/json_types.py`
2. `tests/unit/cq/core/test_schema_export.py`

### Decommission and Deletes

1. Remove raw `dict[str, object]` usage from schema-exported struct fields where typed JSON payload alias is viable.
2. Remove duplicated ad hoc schema generation branches in admin command handling.

### Implementation Checklist

- [ ] Introduce shared `JsonValue` type alias for schema-safe dynamic payloads.
- [ ] Replace schema-export-visible `object` fields with typed alternatives where feasible.
- [ ] Add schema hook fallback for unavoidable dynamic object fields.
- [ ] Validate `cq schema --kind result|query|components` all succeed.

---

## Workstream 2 (P0): Enforce Include/Exclude Scope in Macro Commands

### Scope

Make `--include/--exclude` fully effective in `imports`, `exceptions`, and `side-effects` execution paths.

### Representative Code Snippets

```python
# tools/cq/macros/scope_filters.py
def resolve_macro_files(
    *,
    root: Path,
    include: list[str] | None,
    exclude: list[str] | None,
    extensions: tuple[str, ...],
) -> list[Path]:
    repo_ctx = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_ctx)
    globs: list[str] = []
    if include:
        globs.extend(include)
    if exclude:
        globs.extend([f"!{pattern}" for pattern in exclude])
    return tabulate_files(repo_index, [root], globs, extensions=extensions).files
```

```python
# tools/cq/macros/imports.py
files = resolve_macro_files(
    root=root,
    include=filters.include,
    exclude=filters.exclude,
    extensions=(".py", ".pyi", ".rs"),
)
```

### Target File Edits

1. `tools/cq/macros/imports.py`
2. `tools/cq/macros/exceptions.py`
3. `tools/cq/macros/side_effects.py`
4. `tools/cq/cli_app/commands/analysis.py`
5. `tools/cq/cli_app/options.py`

### New Files

1. `tools/cq/macros/scope_filters.py`
2. `tests/unit/cq/macros/test_scope_filtering.py`

### Decommission and Deletes

1. Delete command-local file enumeration branches that bypass include/exclude filters.
2. Delete duplicated extension/scope filtering helpers across macro modules.

### Implementation Checklist

- [ ] Centralize macro file filtering in one shared helper.
- [ ] Wire helper into `imports`, `exceptions`, `side-effects`.
- [ ] Add summary diagnostics (`scope_file_count`, `scope_filter_applied`) for transparency.
- [ ] Add regressions asserting include/exclude materially changes scan set.

---

## Workstream 3 (P1): Composite Query Contract Alignment (`any/all/not`)

### Scope

Align parser/execution behavior with documented composite-query usage where `any/all/not` can be top-level without explicit `entity=...`.

### Representative Code Snippets

```python
# tools/cq/query/parser.py
def parse_query(query_string: str) -> Query:
    tokens = _tokenize(query_string)
    if "pattern" in tokens or "pattern.context" in tokens:
        return _parse_pattern_query(tokens)
    if {"any", "all", "not"} & set(tokens) and "entity" not in tokens:
        return _parse_composite_pattern_query(tokens)
    return _parse_entity_query(tokens)
```

```python
# tools/cq/query/parser.py
def _parse_composite_pattern_query(tokens: dict[str, str]) -> Query:
    # Desugar into a pattern query envelope with composite rule attached.
    state = _PatternQueryState(pattern_spec=PatternSpec(pattern="$X"))
    state.scope = _parse_scope(tokens)
    state.lang_scope = _parse_query_language_scope(tokens.get("lang"))
    state.composite = _parse_composite_rule(tokens)
    return state.build()
```

### Target File Edits

1. `tools/cq/query/parser.py`
2. `tools/cq/query/planner.py`
3. `tools/cq/query/executor.py`
4. `tools/cq/query/ir.py`

### New Files

1. `tests/unit/cq/query/test_composite_queries.py`
2. `tests/e2e/cq/test_q_composite_e2e.py`

### Decommission and Deletes

1. Remove hard fail path that rejects top-level composite queries without `entity`.
2. Remove stale docs/examples asserting unsupported syntax.

### Implementation Checklist

- [ ] Add parser support for top-level `any/all/not`.
- [ ] Ensure planner/executor process composite pattern envelopes correctly.
- [ ] Ensure output summary mode is deterministic (`pattern_composite` or equivalent).
- [ ] Add e2e coverage for documented composite examples.

---

## Workstream 4 (P1): LDMD Root Ergonomics + JSON Envelope Consistency

### Scope

Support `root` alias for LDMD navigation and make `--format json` contract-consistent for LDMD get/neighbor responses.

### Representative Code Snippets

```python
# tools/cq/ldmd/format.py
def _resolve_section_id(index: LdmdIndex, section_id: str) -> str:
    if section_id == "root":
        if not index.sections:
            raise LdmdParseError("Document has no sections")
        return index.sections[0].id
    return section_id
```

```python
# tools/cq/cli_app/commands/ldmd.py
if ctx.output_format == "json":
    return {
        "section_id": resolved_id,
        "mode": mode,
        "depth": depth,
        "content": extracted.decode("utf-8", errors="replace"),
    }
```

### Target File Edits

1. `tools/cq/ldmd/format.py`
2. `tools/cq/cli_app/commands/ldmd.py`
3. `tools/cq/cli_app/result.py`

### New Files

1. `tests/unit/cq/ldmd/test_root_alias.py`
2. `tests/unit/cq/ldmd/test_ldmd_json_contract.py`

### Decommission and Deletes

1. Remove duplicate section-not-found handling branches that do not account for alias resolution.
2. Remove inconsistent raw-string response path when `--format json` is selected.

### Implementation Checklist

- [ ] Add `root -> first section` alias resolution for `get` and `neighbors`.
- [ ] Return structured JSON envelope for LDMD commands under JSON format.
- [ ] Keep markdown behavior unchanged.
- [ ] Add unit/e2e tests for alias and envelope behavior.

---

## Workstream 5 (P1): Python LSP Reliability and Deterministic Degradation Semantics

### Scope

Reduce `request_failed` prevalence in Python LSP enrichment and ensure degradation fields distinguish unavailable vs attempted-failed vs skipped states consistently.

### Representative Code Snippets

```python
# tools/cq/search/pyrefly_lsp.py
for attempt in range(max_attempts):
    try:
        return request_pyrefly(...)
    except TimeoutError:
        record_timeout()
        continue
    except (OSError, RuntimeError, ValueError, TypeError) as exc:
        record_failure(type(exc).__name__)
        if not is_session_alive():
            restart_session()
        continue
return None
```

```python
# tools/cq/core/front_door_insight.py
insight = msgspec.structs.replace(
    insight,
    degradation=msgspec.structs.replace(
        insight.degradation,
        lsp=derive_lsp_status(...),
        notes=dedupe_notes((*insight.degradation.notes, *lsp_reason_notes)),
    ),
)
```

### Target File Edits

1. `tools/cq/search/pyrefly_lsp.py`
2. `tools/cq/search/diagnostics_pull.py`
3. `tools/cq/core/front_door_insight.py`
4. `tools/cq/search/smart_search.py`
5. `tools/cq/macros/calls.py`
6. `tools/cq/query/entity_front_door.py`

### New Files

1. `tests/unit/cq/search/test_pyrefly_recovery.py`
2. `tests/unit/cq/core/test_lsp_degradation_states.py`

### Decommission and Deletes

1. Remove command-local ad hoc LSP status branches after centralized derivation is fully wired.
2. Remove repeated "request_failed" note assembly in multiple modules.

### Implementation Checklist

- [ ] Implement bounded retry and session recovery for pyrefly request path.
- [ ] Emit stable status set (`unavailable`, `skipped`, `failed`, `partial`, `ok`).
- [ ] Ensure front-door degradation notes are deduplicated and causal.
- [ ] Add regressions for simulated unavailable/failure/partial/ok scenarios.

---

## Workstream 6 (P2): Command UX and Output Contract Consistency

### Scope

Close non-blocking UX and contract inconsistencies discovered during matrix runs.

### Representative Code Snippets

```python
# tools/cq/cli_app/commands/admin.py
if ctx.output_format == "json":
    return {"deprecated": True, "message": "Cache management has been removed."}
return "Cache management has been removed. Caching is no longer used."
```

```python
# tools/cq/macros/impact.py
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=SyntaxWarning, module="nodejs_wheel")
    # execute analysis
```

### Target File Edits

1. `tools/cq/cli_app/commands/admin.py`
2. `tools/cq/macros/impact.py`
3. `tools/cq/cli_app/commands/analysis.py` (option-surface consistency review)
4. `tools/cq/README.md`
5. `.claude/skills/cq/reference/cq_reference.md`

### New Files

1. `tests/unit/cq/cli/test_admin_output_contracts.py`

### Decommission and Deletes

1. Remove plain-text-only deprecated command responses when JSON output is requested.
2. Remove stale docs/examples that rely on unsupported query syntaxes.

### Implementation Checklist

- [ ] Normalize deprecated command responses by output format.
- [ ] Reduce non-actionable warning noise in stderr paths.
- [ ] Align docs/skills with implemented query semantics and quoting guidance.
- [ ] Add targeted tests for output-format consistency.

---

## Workstream 7: Regression Tests, Goldens, and Capability Matrix Guardrails

### Scope

Add regression coverage directly tied to failures/discrepancies discovered in the capability sweep.

### Unit Tests

1. `tests/unit/cq/core/test_schema_export.py`  
   - Assert `schema --kind result|query|components` equivalent functions succeed.
2. `tests/unit/cq/macros/test_scope_filtering.py`  
   - Assert include/exclude materially changes macro scan sets.
3. `tests/unit/cq/query/test_composite_queries.py`  
   - Assert top-level `any/all/not` parse and execute.
4. `tests/unit/cq/ldmd/test_root_alias.py`  
   - Assert `root` alias resolves and navigates.
5. `tests/unit/cq/ldmd/test_ldmd_json_contract.py`  
   - Assert `--format json` returns JSON envelopes.
6. `tests/unit/cq/search/test_pyrefly_recovery.py`  
   - Assert retry and degradation state semantics.

### E2E/Golden

1. Expand `tests/e2e/cq/_support/projections.py` for:
   - schema command success projections,
   - macro filter-sensitive fields,
   - composite query summary mode fields.
2. Update/add e2e tests:
   - `tests/e2e/cq/test_q_command_e2e.py`
   - `tests/e2e/cq/test_ldmd_command_e2e.py`
   - `tests/e2e/cq/test_report_command_e2e.py`
3. Refresh CQ goldens only after code changes are complete.

### Capability Matrix Script

Add a repeatable guard script mirroring this validation run:

1. `scripts/cq_capability_matrix.sh` (or `tools/cq/perf/capability_matrix.py`)
2. Outputs:
   - `build/cq_capability_validation/summary.tsv`
   - `build/cq_capability_validation/logs/*`

---

## 5. Execution Sequence

1. Workstream 1 (schema recovery)  
2. Workstream 2 (macro include/exclude enforcement)  
3. Workstream 3 (composite query contract alignment)  
4. Workstream 4 (LDMD root alias + JSON envelopes)  
5. Workstream 5 (LSP reliability/degradation hardening)  
6. Workstream 6 (UX/output polish + docs alignment)  
7. Workstream 7 (tests, goldens, capability-matrix guardrail)  
8. Final CQ-only quality gate and representative command replay

---

## 6. Acceptance Criteria

1. `./cq schema --kind result --format json` succeeds.
2. `./cq schema --kind components --format json` succeeds.
3. `imports/exceptions/side-effects` materially respect `--include/--exclude`.
4. `./cq q "any='...'"` (without `entity`) executes per documented contract.
5. `./cq ldmd get <file> --id root --format json` succeeds with structured JSON output.
6. Python LSP degradation is deterministic and no longer dominated by repeated opaque `request_failed` states under normal operation.
7. Deprecated admin commands honor JSON output contract when `--format json` is selected.
8. CQ unit + e2e + golden suites covering touched scope pass.

---

## 7. Assumptions and Defaults

1. Hard-cutover internal refactors are acceptable in this design-phase branch.
2. Behavior/documentation mismatches are resolved in code where feasible; otherwise docs are updated in the same change set.
3. LSP hardening remains fail-open for command completion semantics.
4. Capability matrix replay remains CQ-only (`tests/unit/cq`, `tests/e2e/cq`, `tests/cli_golden` CQ-relevant assertions).
