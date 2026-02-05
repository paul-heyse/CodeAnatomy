# cq Ruff Complexity Refactor Plan v1

This plan captures the agreed architecture changes to resolve Ruff complexity/arg-count errors in `tools/cq` by restructuring CLI options, orchestration pipelines, and renderers into smaller, typed components. It aligns with the Smart Search design objectives while improving testability, performance, and robustness.

The plan now integrates a **msgspec‑first internal model layer** and **pydantic‑only boundary validation** strategy for CQ tooling.

---

## Status Summary (2026-02-05)

- Scope 1: Complete
- Scope 2: Complete
- Scope 3: Complete
- Scope 4: Partial
- Scope 5: Complete
- Scope 6: Complete
- Scope 7: Complete
- Scope 8: Complete
- Scope 9: Complete
- Scope 10: Complete
- Scope 11: Complete
- Scope 12: Complete
- Scope 13: Complete
- Scope 14: Complete

---

## Scope 1: Shared msgspec Base (`CqStruct`) + Constraints

**Status**: Complete

**Goal**  
Standardize CQ internal models on msgspec for performance and immutable semantics, with constrained fields where helpful.

**Representative snippet**

```python
# tools/cq/core/structs.py
import msgspec

class CqStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    """Base struct for CQ internal data models."""
    pass
```

```python
# tools/cq/search/smart_search.py
from typing import Annotated

Line = Annotated[int, msgspec.Meta(ge=1)]
Col = Annotated[int, msgspec.Meta(ge=0)]
```

**Target files**
- `tools/cq/core/structs.py`
- `tools/cq/cli_app/options.py`
- `tools/cq/search/context.py`
- `tools/cq/query/execution_context.py`
- `tools/cq/search/smart_search.py`

**Deprecate/Delete**
- Ad‑hoc dataclass options objects once msgspec structs replace them.

**Implementation checklist**
1. Add `CqStruct` base class with `kw_only=True`, `frozen=True`, `omit_defaults=True`. 
2. Migrate option/context models to inherit from `CqStruct`. 
3. Introduce `Annotated[..., msgspec.Meta(...)]` constraints for line/col and other obvious invariants.

---

## Scope 2: Boundary Validation via msgspec + Pydantic

**Status**: Complete

**Goal**  
Use msgspec for internal validation and pydantic only for stringy external inputs (env/CLI/config).

**Representative snippet**

```python
# tools/cq/cli_app/config_models.py
from pydantic import BaseModel, ConfigDict

class CqConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    include: list[str] = []
    exclude: list[str] = []
    limit: int | None = None
```

```python
# tools/cq/cli_app/commands/query.py
opts = msgspec.convert(raw_opts, type=QueryOptions, strict=True)
```

**Target files**
- `tools/cq/cli_app/config_models.py` (new)
- `tools/cq/cli_app/context.py`
- `tools/cq/cli_app/commands/*.py`

**Deprecate/Delete**
- Manual string coercion logic in CLI/context helpers.

**Implementation checklist**
1. Add `CqConfigModel` for external config/env inputs. 
2. Convert validated config into msgspec options via `msgspec.convert(..., strict=True)`. 
3. Ensure pydantic models do not cross internal pipeline boundaries.

---

## Scope 3: CLI Options Dataclasses + Command Helpers

**Status**: Complete

**Goal**  
Replace wide command signatures with structured option objects (msgspec structs) and helper functions.

**Representative snippet**

```python
# tools/cq/cli_app/options.py
from tools.cq.core.structs import CqStruct

class CommonFilters(CqStruct):
    include: list[str]
    exclude: list[str]
    impact: list[str]
    confidence: list[str]
    severity: list[str]
    limit: int | None

class QueryOptions(CommonFilters):
    explain_files: bool = False
```

```python
# tools/cq/cli_app/commands/query.py
from tools.cq.cli_app.options import QueryOptions

def q(query_string: str, *, opts: QueryOptions, ctx: CliContext | None = None) -> CliResult:
    return _run_query(query_string, opts=opts, ctx=ctx)
```

**Target files**
- `tools/cq/cli_app/options.py`
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/search.py`
- `tools/cq/cli_app/commands/report.py`
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/cli_app/context.py`

**Deprecate/Delete**
- Inline filter-building helpers duplicated across command modules.

**Implementation checklist**
1. Add `CommonFilters` and per-command option structs. 
2. Convert command entrypoints to accept `opts` and delegate to `_run_*` helpers. 
3. Migrate filter parsing into a shared helper in `options.py`. 
4. Update CLI parsing glue to construct `opts` (Cyclopts binding or manual). 
5. Update unit tests for new command signatures.

---

## Scope 4: Smart Search Context + Collector Refactor

**Status**: Partial

**Goal**  
Split candidate collection and enrichment into smaller, testable stages.

**Representative snippet**

```python
# tools/cq/search/context.py
from tools.cq.core.structs import CqStruct

class SmartSearchContext(CqStruct):
    root: Path
    query: str
    limits: SearchLimits
    include_globs: list[str] | None
    exclude_globs: list[str] | None
    mode: QueryMode
```

```python
# tools/cq/search/collector.py
@dataclass
class RgCollector:
    limits: SearchLimits
    matches: list[RawMatch] = field(default_factory=list)
    seen_files: set[str] = field(default_factory=set)

    def handle_match(self, payload: dict[str, object]) -> None:
        ...

    def handle_summary(self, payload: dict[str, object]) -> None:
        ...
```

**Target files**
- `tools/cq/search/context.py`
- `tools/cq/search/collector.py`
- `tools/cq/search/smart_search.py`

**Deprecate/Delete**
- `_collect_candidates_raw` monolith in `smart_search.py`.

**Implementation checklist**
1. Introduce `SmartSearchContext` and migrate orchestration to stage functions. 
2. Implement `RgCollector` with `handle_match`, `handle_summary`, `finalize`. 
3. Replace `_collect_candidates_raw` with collector usage. 
4. Update tests to exercise collector directly.

**Remaining**
- Add `RgCollector.finalize()` (or explicitly document why a finalize hook is unnecessary). 
- Remove or clearly deprecate `_collect_candidates_raw` now that collector usage is primary. 
- Add unit coverage that calls `RgCollector` directly (match + summary handling).

---

## Scope 5: Query Execution Context + Staged Executor

**Status**: Complete

**Goal**  
Break `_execute_entity_query` and `_execute_ast_grep_rules` into staged steps.

**Representative snippet**

```python
# tools/cq/query/execution_context.py
class QueryExecutionContext(CqStruct):
    query: Query
    plan: ToolPlan
    tc: Toolchain
    root: Path
    argv: list[str] | None
    started_ms: float
```

```python
# tools/cq/query/executor.py

def _execute_entity_query(ctx: QueryExecutionContext) -> CqResult:
    scan = _scan_entities(ctx)
    matches = _filter_matches(scan, ctx)
    return _build_result(scan, matches, ctx)
```

**Target files**
- `tools/cq/query/execution_context.py`
- `tools/cq/query/executor.py`

**Deprecate/Delete**
- Direct argument-heavy `_execute_entity_query` and `_execute_ast_grep_rules` signatures.

**Implementation checklist**
1. Introduce `QueryExecutionContext`. 
2. Split scanning, filtering, scoring, and section building into helpers. 
3. Update call sites to pass context rather than many params. 
4. Preserve existing summary metadata fields.

---

## Scope 6: Macro Pipelines With Context Objects

**Status**: Complete

**Goal**  
Reduce local-variable counts in macros by moving data into context objects.

**Representative snippet**

```python
# tools/cq/macros/calls.py
@dataclass
class CallsContext:
    root: Path
    target: str
    limits: SearchLimits
    records: list[SgRecord] = field(default_factory=list)


def build_calls_result(ctx: CallsContext) -> CqResult:
    _collect_records(ctx)
    _score_records(ctx)
    return _render_calls(ctx)
```

**Target files**
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/imports.py`

**Deprecate/Delete**
- Inlined multi-step pipelines with 15+ locals.

**Implementation checklist**
1. Introduce per-macro context dataclasses. 
2. Extract collection/scoring/section building into helpers. 
3. Update macro entrypoints to use context flow.

---

## Scope 7: Renderer Builders (Dot + Mermaid)

**Status**: Complete

**Goal**  
Split rendering into data-collection and formatting steps.

**Representative snippet**

```python
# tools/cq/core/renderers/dot.py
@dataclass
class DotRenderBuilder:
    nodes: list[str] = field(default_factory=list)
    edges: list[str] = field(default_factory=list)

    def add_node(self, node_id: str, label: str) -> None:
        self.nodes.append(...)

    def render(self) -> str:
        return "\n".join(["digraph {...}", *self.nodes, *self.edges, "}"])
```

**Target files**
- `tools/cq/core/renderers/dot.py`
- `tools/cq/core/renderers/mermaid.py`

**Deprecate/Delete**
- Large inline render functions with many branches/locals.

**Implementation checklist**
1. Add builder classes with `add_node`, `add_edge`, `render`. 
2. Refactor render entrypoints to use builders. 
3. Update tests or snapshots if output ordering changes.

---

## Scope 8: Parser/Planner Dispatch Tables

**Status**: Complete

**Goal**  
Reduce branching in parser/planner by using token handler maps.

**Representative snippet**

```python
# tools/cq/query/parser.py
TOKEN_HANDLERS: dict[str, Callable[[dict[str, str], QueryBuilder], None]] = {
    "scope": _parse_scope,
    "fields": _parse_fields,
    "expand": _parse_expanders,
}

for key, value in tokens.items():
    handler = TOKEN_HANDLERS.get(key)
    if handler:
        handler(tokens, builder)
```

**Target files**
- `tools/cq/query/parser.py`
- `tools/cq/query/planner.py`
- `tools/cq/query/symbol_resolver.py`

**Deprecate/Delete**
- Deeply nested `if/elif` chains in parsing logic.

**Implementation checklist**
1. Introduce handler maps for parser and planner phases. 
2. Extract repeated token validation into helpers. 
3. Update tests for parser error messages if needed.

---

## Scope 9: Classifier Return Fan‑Out Cleanup

**Status**: Complete

**Goal**  
Reduce return count and simplify flow in `classify_from_node`.

**Representative snippet**

```python
# tools/cq/search/classifier.py
result: NodeClassification | None = None
if node is not None:
    result = _classify_node(...)
return result
```

**Target files**
- `tools/cq/search/classifier.py`

**Deprecate/Delete**
- Multiple early return branches in classification logic.

**Implementation checklist**
1. Extract predicates into helpers. 
2. Convert to single-return flow. 
3. Update tests if behavior changes.

---

## Scope 10: Findings Table Constructor Simplification

**Status**: Complete

**Goal**  
Reduce parameter counts in findings table helpers.

**Representative snippet**

```python
# tools/cq/core/findings_table.py
class FindingsTableOptions(CqStruct):
    limit: int | None
    sort_key: str
    include_details: bool


def build_findings_table(findings: list[Finding], opts: FindingsTableOptions) -> str:
    ...
```

**Target files**
- `tools/cq/core/findings_table.py`
- `tools/cq/core/tests/test_findings_table.py`

**Deprecate/Delete**
- Constructors with 6+ positional arguments.

**Implementation checklist**
1. Introduce `FindingsTableOptions`. 
2. Update call sites/tests to pass `opts`. 
3. Ensure rendering output stays stable.

---

## Scope 11: CLI Launcher Refactor

**Status**: Complete

**Goal**  
Reduce complexity in `cli_app/app.py` `launcher`.

**Representative snippet**

```python
# tools/cq/cli_app/app.py
@dataclass
class LaunchContext:
    argv: list[str]
    output_format: OutputFormat
    filters: FilterConfig


def launcher(argv: list[str]) -> int:
    ctx = _build_launch_context(argv)
    return _run_command(ctx)
```

**Target files**
- `tools/cq/cli_app/app.py`

**Deprecate/Delete**
- Large inlined branching logic in launcher.

**Implementation checklist**
1. Add `LaunchContext` struct. 
2. Split parsing, dispatch, rendering into helpers. 
3. Update CLI tests to ensure behavior unchanged.

---

## Scope 12: JSON Codec Reuse + Deterministic Output

**Status**: Complete

**Goal**  
Use shared msgspec encoders/decoders for performance and stable output.

**Representative snippet**

```python
# tools/cq/core/codec.py
JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
JSON_DECODER = msgspec.json.Decoder(type=CqResult, strict=True)
```

**Target files**
- `tools/cq/core/codec.py`
- `tools/cq/core/bundles.py`
- `tools/cq/core/report.py`

**Deprecate/Delete**
- Ad‑hoc JSON dumps/loads for CQ artifacts.

**Implementation checklist**
1. Add shared encoders/decoders. 
2. Replace local JSON serialization with codec helpers. 
3. Ensure golden tests remain stable.

---

## Scope 13: Schema Evolution Guidelines For CQ Artifacts

**Status**: Complete

**Goal**  
Align CQ artifact evolution with msgspec schema evolution rules.

**Representative snippet**

```python
# docs (or code comments)
# - New fields must have defaults.
# - For array_like structs, append only; never reorder.
# - Do not change existing field types.
```

**Target files**
- `tools/cq/core/schema.py`
- `tools/cq/core/bundles.py`
- `docs/` (add explicit schema evolution notes if needed)

**Deprecate/Delete**
- None (policy/discipline scope).

**Implementation checklist**
1. Audit CQ artifact structs for defaults on new fields. 
2. Add inline comments or doc notes for evolution rules. 
3. Ensure any `array_like` usage follows append-only discipline.

---

## Scope 14: CLI/Env String Coercion Policy

**Status**: Complete

**Goal**  
Make string‑based coercion explicit and isolated to boundary parsing.

**Representative snippet**

```python
# tools/cq/cli_app/context.py
config = CqConfigModel.model_validate_strings(env_map)
opts = msgspec.convert(config.model_dump(), type=CommonFilters, strict=True)
```

**Target files**
- `tools/cq/cli_app/context.py`
- `tools/cq/cli_app/commands/*.py`

**Deprecate/Delete**
- Implicit coercions scattered throughout CLI helpers.

**Implementation checklist**
1. Centralize env/CLI coercion in `context.py` or `config_models.py`. 
2. Ensure internal code always receives typed msgspec structs. 
3. Update docs/tests for the new coercion boundary.

---

## Quality Gate (After Implementation)

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q
```

For local iteration on this scope, run:

```bash
uv run ruff check tools/cq
uv run pytest tests/unit/cq/ tests/cli_golden/ -m "not e2e"
```

---

## Notes

- Use `/cq calls <function>` before modifying any function body in `tools/cq`.
- Prefer msgspec structs for internal models and pydantic only at trust boundaries.
- Keep Smart Search output schema stable while refactoring.
