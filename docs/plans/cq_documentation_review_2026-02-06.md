# CQ Documentation Review (2026-02-06)

## Scope
Reviewed documentation updates in:
- `.claude/skills/cq/SKILL.md`
- `.claude/skills/cq/reference/cq_reference.md`
- `AGENTS.md`
- `CLAUDE.md`
- `tools/cq/README.md`

Validation method:
- Line-by-line doc review with line references.
- Runtime validation against live CLI (`./cq ... --help` and representative commands).
- Lightweight implementation cross-check in `tools/cq/query/*` and `tools/cq/cli_app/*` for disputed features.

## Findings (Ordered by Severity)

### 1) Critical: Contradictory pinned Python version across top-level guidance
- Evidence:
  - `AGENTS.md:13` states `Python 3.13.12`.
  - `CLAUDE.md:309` states `Python 3.13.11`.
- Impact:
  - Conflicting setup/runtime assumptions for agents and humans.
- Recommendation:
  - Standardize on one pinned version across both files (current runtime/toolchain reports 3.13.12).

### 2) High: Skill name mismatch for DataFusion skill
- Evidence:
  - `AGENTS.md:54` and `CLAUDE.md:30` reference `/dfdl_ref`.
  - Available skill in current environment is `datafusion-stack`.
- Impact:
  - Command discoverability failure and broken guidance.
- Recommendation:
  - Replace `/dfdl_ref` with `/datafusion-stack` in both docs.

### 3) High: Multiple `cq search --in <dir>/` examples are broken due trailing slash
- Evidence (docs):
  - `AGENTS.md:33`, `AGENTS.md:126`
  - `.claude/skills/cq/SKILL.md:18`, `.claude/skills/cq/SKILL.md:104`, `.claude/skills/cq/SKILL.md:127`
  - `.claude/skills/cq/reference/cq_reference.md:306`
- Runtime repro:
  - `./cq search CqResult --in tools/cq/core/ --format summary` -> `No files were searched`.
  - `./cq search CqResult --in tools/cq/core --format summary` -> findings returned.
  - `CQ_ENABLE_RUST_QUERY=1 ./cq search ScalarUDF --lang rust --in rust/ --format summary` -> `No files were searched`.
  - `CQ_ENABLE_RUST_QUERY=1 ./cq search ScalarUDF --lang rust --in rust --format summary` -> findings returned.
- Impact:
  - Users copy/pasting examples get false negatives.
- Recommendation:
  - Remove trailing slashes in all `search --in` examples.
  - Add one explicit note: use `--in path` (no trailing slash).

### 4) High: Rust `run --steps` example in skill doc is not valid for `search` steps
- Evidence:
  - `.claude/skills/cq/SKILL.md:107` uses `{"type":"search", ..., "lang":"rust"}`.
- Runtime repro:
  - `CQ_ENABLE_RUST_QUERY=1 ./cq run --steps '[{"type":"search","query":"register_udf","lang":"rust"}]'` fails with invalid step conversion.
- Implementation cross-check:
  - `tools/cq/cli_app/step_types.py:19` (`SearchStepCli`) has no `lang` field.
  - `tools/cq/run/spec.py:48` (`SearchStep`) does include `lang` for plan-file execution.
- Impact:
  - Docs advertise a JSON-steps shape that the CLI does not accept.
- Recommendation:
  - Update docs to one of:
    - Use `--plan` TOML for `search` step language, or
    - Use `q` step with `lang=rust` in query string when using `--steps`.

### 5) High: Decorator query documentation is inaccurate in semantics and examples
- Evidence (docs):
  - `.claude/skills/cq/SKILL.md:800` says `entity=decorator` finds decorator definitions.
  - `.claude/skills/cq/SKILL.md:807`, `.claude/skills/cq/SKILL.md:813` show `entity=function/class decorated_by=...`.
  - `.claude/skills/cq/reference/cq_reference.md:1606`, `:1621`, `:1624` repeat same guidance.
- Runtime repro:
  - `./cq q "entity=function in=tests/" --format summary` -> 2113 findings.
  - `./cq q "entity=function decorated_by=totally_nonexistent_decorator in=tests/" --format summary` -> also 2113 findings (no effective filter).
  - `./cq q "entity=decorator in=tests/" --limit 1 --format json` returns decorated functions/classes, not decorator definitions.
- Implementation cross-check:
  - Decorator filter execution is in `_process_decorator_query`, only dispatched for `entity=="decorator"`: `tools/cq/query/executor.py:386-387`, `tools/cq/query/executor.py:1185`.
- Impact:
  - Users will trust filters that currently behave as no-ops for `entity=function/class`.
- Recommendation:
  - Document current behavior explicitly:
    - `entity=decorator` = decorated definitions.
    - `decorated_by`/`decorator_count_*` currently effective in decorator query path.
  - Remove or mark `entity=function/class decorated_by=...` as unsupported until implemented.

### 6) High: Join query section documents features that are parsed but not executed
- Evidence (docs):
  - `.claude/skills/cq/SKILL.md:822-836`
  - `.claude/skills/cq/reference/cq_reference.md:1669-1699`
- Runtime repro:
  - `./cq q "entity=function in=tools/cq/" --format summary` -> 906 findings.
  - `./cq q "entity=function used_by=function:totally_nonexistent_target in=tools/cq/" --format summary` -> also 906 findings.
- Implementation cross-check:
  - Joins parsed in `tools/cq/query/parser.py:578`.
  - No `query.joins` execution usage in `tools/cq/query/executor.py` (no runtime application path).
- Impact:
  - Significant false confidence in cross-entity filtering.
- Recommendation:
  - Mark joins as "planned/not yet enforced" or remove from user-facing docs until implemented.

### 7) High: `fields=hazards` is documented for `q` but rejected by parser
- Evidence (docs):
  - `.claude/skills/cq/reference/cq_reference.md:893`, `:916`, `:1948`, `:1951`, `:1959`, `:2105`
- Runtime repro:
  - `./cq q "entity=function fields=hazards" --format md` -> `Invalid field: 'hazards'. Valid fields: def, loc, callers, callees, evidence, imports, decorators, decorated_functions`.
- Implementation cross-check:
  - Valid fields in `tools/cq/query/ir.py:28-37` (no `hazards`).
- Impact:
  - Multiple advanced examples are non-runnable as written.
- Recommendation:
  - Remove `fields=hazards` from `q` docs or clearly re-scope hazards to macros where actually supported.

### 8) Medium-High: Scope filtering docs list unsupported/ambiguous scope values
- Evidence (docs):
  - `.claude/skills/cq/reference/cq_reference.md:1544-1547`, `:1560-1561`, `:1581` advertise `module`, `function`, `class` scope values.
- Runtime/implementation cross-check:
  - Scope matching code has explicit handling for `closure`, `nested`, `toplevel`; unknown values fall through without restriction: `tools/cq/query/enrichment.py:512-518`.
  - Example: `scope=module` in `tools/cq/` produced same count as baseline in local run.
- Impact:
  - Filters appear to work while silently not narrowing results.
- Recommendation:
  - Document currently supported values only (`closure`, `nested`, `toplevel`) and remove unsupported scope examples.

### 9) Medium: Entity semantics for `method` and `module` are not documented accurately
- Evidence:
  - Docs present `method`/`module` capabilities in places (for example `CLAUDE.md` q help text context and CQ reference sections).
- Implementation cross-check:
  - `module` is hardcoded non-match: `tools/cq/query/executor.py:1510`.
  - `method` currently matches generic function kinds without class-context validation: `tools/cq/query/executor.py:1507-1508`.
- Impact:
  - Overstated precision for method/module queries.
- Recommendation:
  - Add "current semantics" notes for `method` and `module`, or hide these entities from user docs until behavior is tightened.

### 10) Medium: `q` limit behavior is inconsistently documented
- Evidence (docs):
  - Query-string syntax advertises `limit=N`: `.claude/skills/cq/SKILL.md:493`, `.claude/skills/cq/reference/cq_reference.md:823-825`.
- Implementation cross-check:
  - Limit slicing exists in pattern execution paths: `tools/cq/query/executor.py:570-571`, `:630-631`.
  - No equivalent cap in `_execute_entity_query`: `tools/cq/query/executor.py:477-535`.
- Impact:
  - Users expect entity-query limit semantics that are not consistently enforced.
- Recommendation:
  - Either document this as pattern-only until fixed, or implement entity-path limit application and keep docs as-is.

## File-Specific Quality Notes

### `tools/cq/README.md`
- Overall status: mostly accurate and much safer than the large skill reference.
- Improvement opportunities:
  - Clarify Rust support in `run`: `--steps` JSON cannot currently set `search.lang`; TOML plan can.
  - Add a short "copy-safe examples" note for `search --in` path formatting (no trailing slash).

### `AGENTS.md` and `CLAUDE.md`
- Highest-priority fixes are consistency:
  - Python version alignment.
  - Skill naming alignment.
- Secondary improvement:
  - Reduce duplicated CQ command detail and link to one canonical CQ reference to reduce drift.

### `.claude/skills/cq/*`
- Main issue is over-claiming advanced DSL surfaces without matching runtime behavior.
- Recommendation:
  - Introduce a clear capability legend per feature: `Implemented`, `Partial`, `Planned`.

## Suggested Remediation Plan (Docs-Only)
1. Fix hard contradictions first (Python version, skill name).
2. Replace all broken `search --in <dir>/` examples with `search --in <dir>`.
3. Split advanced CQ reference into:
   - "Available now" (runtime-validated), and
   - "Planned/experimental" (explicitly non-authoritative for production use).
4. Add a tiny doc-validation script for representative examples:
   - Run commands in `--format summary` mode.
   - Fail if command errors or if output contradicts documented expectation.
5. Keep `tools/cq/README.md` as canonical CLI contract; make skill docs link out instead of duplicating deep semantics.

## Assumptions
- Review emphasizes current runtime truth of `tools/cq` in this repo state.
- Recommendations are docs-focused; implementation fixes are intentionally out of scope for this review artifact.
