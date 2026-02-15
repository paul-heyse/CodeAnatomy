# Ripgrep & ast-grep-py Improvements Implementation Plan v1 (2026-02-15)

## Scope Summary

This plan covers 23 scope items organized into four groups that elevate CQ's ripgrep and ast-grep-py utilization to best-in-class while fixing confirmed execution-path correctness gaps:

- **Group A (10 items):** Ripgrep improvements — unified operation modes + scoped paths, multi-pattern OR, boundary mode standardization, context lines, count probes, file enumeration, deterministic sort, multiline mode, JSON fidelity, capability negotiation.
- **Group B (7 items):** ast-grep-py improvements — variadic captures, dynamic metavariable extraction, YAML rule packs, shared utility rules, constraint pushdown, refinement predicates, pattern-object execution.
- **Group C (4 items):** Combined/new capabilities — ripgrep prefiltering for ast-grep scans, begin/end file events, neighborhood ripgrep-lane consolidation, macro symbol-target resolution via shared rg lane.
- **Group D (2 items):** Query correctness hardening — planner/executor threading fixes and pattern runtime parity for composite/nthChild/pattern-object semantics.

**Design stance:** Hard additions — no compatibility shims. New fields use defaults so existing callers are unaffected. No tree-sitter scope overlap: ripgrep handles text-level candidate generation, ast-grep handles structural classification, tree-sitter owns semantic enrichment.

**Priority order:** D1, D2, A0, B7, B4, B6, B1, A3, C3, A9, A8, A2, A1, A4, A5, C2, C1, C4, B5, A6, B2, B3, A7.

## Design Principles

1. **No tree-sitter duplication** — Ripgrep and ast-grep improvements operate strictly in their existing layers (text candidate generation and structural classification). Tree-sitter retains exclusive ownership of incremental parsing, query-pack semantic extraction, byte-window queries, structural neighborhoods, and gap-fill enrichment.
2. **Additive defaults** — All new fields on `SearchLimits`, `RgRunRequest`, `RgRunSettingsV1`, and `Toolchain` use default values that preserve current behavior. No existing callers break.
3. **msgspec contracts at boundaries** — All new serializable types use `msgspec.Struct` base classes (`CqStruct`, `CqSettingsStruct`, `CqOutputStruct`). Runtime-only handles stay as `dataclass` or plain objects.
4. **Test parity** — Every new module or function gets a corresponding test file. Existing test files gain parametrized cases for new behavior.
5. **Minimal Python post-processing** — Push filtering, matching, and ordering into ripgrep/ast-grep native engines wherever the API supports it.
6. **Execution-path correctness first** — Any scope item that changes behavior already represented in `Query`/`ToolPlan` must land before performance-oriented items. Specifically, planner/executor pattern threading and metavariable capture fidelity are blocking prerequisites for prefilter and profile optimization work.
7. **Single ripgrep execution plane** — New ripgrep capabilities must route through `tools/cq/search/rg/runner.py` + `tools/cq/search/rg/contracts.py` + `tools/cq/search/_shared/core.py`. No new ad-hoc `subprocess.run(["rg", ...])` lanes are added in helper modules.

## Current Baseline

- **`tools/cq/search/rg/runner.py`** (236 LOC): `build_rg_command()` emits 11 flags (`--json`, `--line-number`, `--column`, `--max-count`, `--max-depth`, `--max-filesize`, `--type`, `-g`, `-F`, `-e`). Single pattern only: `command.extend(["-e", pattern, "."])` at line 98. No `-w`, `-U`, `-P`, `-C/-A/-B`, `--count`, `--files`, `--sort`.
- **`tools/cq/search/rg/codec.py`** (222 LOC): Typed decoder handles `RgMatchEvent` and `RgSummaryEvent` only. `RgEvent` fallback captures unknown types but discards them. No `context`, `begin`, or `end` event types, and typed payloads omit `absolute_offset` and richer end-event stats.
- **`tools/cq/search/rg/collector.py`** (211 LOC): `RgCollector.handle_event()` dispatches on `type == "match"` and `type == "summary"`, ignores all others. No context line storage, no begin/end file accounting, and no typed `absolute_offset` usage.
- **`tools/cq/search/rg/adapter.py`** (244 LOC): `find_call_candidates()` manually builds `rf"\b{symbol}\s*\("`; smart-search has a separate identifier-boundary construction path, so boundary semantics are duplicated across callsites. `find_def_lines()` reads files with Python instead of ripgrep.
- **`tools/cq/search/rg/contracts.py`** (65 LOC): `RgRunSettingsV1` has `pattern: str` (single), `mode: str`, `lang_types`, globs. No multi-pattern or context fields.
- **`tools/cq/search/_shared/core.py`** (384 LOC): `RgRunRequest` has `pattern: str` (single). No multi-pattern, context, sort, or multiline fields.
- **`tools/cq/search/rg/runner.py`**: Command assembly hardcodes search root to `"."`; there is no operation-mode contract for `--count`, `--files`, or `--files-with-matches` and no path-scoped search input.
- **`tools/cq/search/pipeline/profiles.py`** (67 LOC): `SearchLimits` has 6 fields with 4 presets (`DEFAULT`, `INTERACTIVE`, `AUDIT`, `LITERAL`). No `context_before`, `context_after`, `sort_by_path`, or `multiline` fields.
- **`tools/cq/search/pipeline/classifier.py`**: `QueryMode` enum has 3 values: `IDENTIFIER`, `REGEX`, `LITERAL`. Multiline behavior is correctly modeled as a search option rather than a query-mode dimension.
- **`tools/cq/astgrep/sgpy_scanner.py`** (533 LOC): `scan_with_pattern()` accepts only `pattern: str` and is only exercised by scanner unit tests. Query execution does not call this function. `_extract_metavars()` iterates hardcoded names calling `get_match()` only — never calls `get_multiple_matches()`.
- **`tools/cq/astgrep/rules_py.py`**: 23 Python rules as `RuleSpec` objects.
- **`tools/cq/astgrep/rules_rust.py`**: 8 Rust rules as `RuleSpec` objects.
- **`tools/cq/core/toolchain.py`** (148 LOC): `Toolchain` has no PCRE2 detection field. `_detect_rg()` only runs `rg --version`.
- **`tools/cq/query/ir.py`**: `PatternSpec` already has `context: str | None` and `selector: str | None` fields.
- **`tools/cq/query/parser.py`**: `_parse_pattern_object()` parses `pattern.context` and `pattern.selector` from tokens into `PatternSpec`. Parser also captures `composite` and `nth_child` for pattern queries.
- **`tools/cq/query/planner.py`**: `_compile_pattern_query()` threads `pattern/context/selector/strictness` into `AstGrepRule` but currently drops `query.composite` and `query.nth_child`.
- **`tools/cq/query/executor.py`**: `_iter_rule_matches()` uses only `find_all(pattern=...)` / `find_all(kind=...)` and does not use inline rule execution for pattern findings. `_iter_rule_matches_for_spans()` has inline-rule support, so span filtering and finding generation currently use different rule execution semantics.
- **`tools/cq/query/batch.py`**: Batch entity sessions run ast-grep scans via `scan_files(...)`; prefilter adoption must include this path, not just `query/sg_parser.py`.
- **`tools/cq/query/executor.py`**: Metavariable extraction is hardcoded to `_COMMON_METAVAR_NAMES`; captures for valid names outside that list (e.g., `$FOO123`) are dropped.
- **`tools/cq/query/metavar.py`**: Contains richer metavariable parsing helpers (`parse_metavariables`) but these are not currently integrated into query execution.
- **`tools/cq/search/pipeline/classifier_runtime.py`**: `_is_docstring_context()` walks parent chain in Python — candidate for `node.inside()` replacement.
- **`tools/cq/search/python/extractors.py`**: 25+ `.parent()`/`.kind()` chains for context classification — candidates for refinement predicate replacement.
- **`tools/cq/macros/shared.py`**: `resolve_target_files()` currently scans file text in Python (`"def {target}"` / `"class {target}"`) for symbol fallback candidate discovery.
- **`tools/cq/neighborhood/target_resolution.py`**: Uses direct `subprocess.run(["rg", ...])` rather than the shared rg runner/contracts path.
- **`tools/cq/search/rg/runner.py`**: `detect_rg_types()` exists but is not consumed by runtime capability gating.
- **`tools/cq/astgrep/rules/python_facts/*.yml`**: 23 existing CLI-mode YAML rule files already externalize the Python rules for ast-grep's CLI runner. These define rules using ast-grep's native YAML schema (`id`, `language`, `severity`, `rule`, `metadata`). The `metadata.record` and `metadata.kind` fields map to `RuleSpec.record_type` and `RuleSpec.kind`. Example: `py_def_function.yml` uses `rule.kind: function_definition` with `metadata: {record: def, kind: function}`.
- **`tools/cq/astgrep/sgconfig.yml`**: ast-grep CLI config already declares `utilDirs: [utils]` — the shared utility rule directory mechanism is pre-configured. `ruleDirs` points to `rules/python_facts`.
- **`tools/cq/astgrep/rules_py.py`**: 23 Python `RuleSpec` objects are hand-translated equivalents of the CLI-mode YAML rules — NOT auto-generated. Comment says "Converts all YAML rules" but this is descriptive, not automated.
- **`tools/cq/astgrep/rules_rust.py`**: 8 Rust `RuleSpec` objects with **no** corresponding CLI-mode YAML files.
- **`tools/cq/core/typed_boundary.py`**: Contains `decode_yaml_strict()` — the canonical CQ pattern for YAML boundary decoding using `msgspec.yaml.decode(raw, type=type_, strict=True)` with `BoundaryDecodeError` wrapping. Already used in `tools/cq/search/tree_sitter/contracts/query_models.py:load_pack_rules()` for loading YAML-defined query pack contracts.
- **YAML tooling**: CQ uses `msgspec.yaml` (which wraps PyYAML SafeLoader internally) for all YAML operations. No raw `pyyaml` API usage exists. No custom loader subclassing, tag constructors, or representers are needed.

---

## S0. A0 — Unified Ripgrep Operation Modes + Scoped Path Execution Contracts

### Goal

Introduce one shared contract for all ripgrep operation modes (`json`, `count`, `files`, `files_with_matches`) and scoped path inputs. This is a prerequisite for S1/S7/S10 so those scopes do not create new ad-hoc `subprocess.run(["rg", ...])` lanes.

### Representative Code Snippets

```python
# tools/cq/search/_shared/core.py — RgRunRequest extension
class RgRunRequest(CqStruct, frozen=True):
    root: Path
    pattern: str
    mode: QueryMode
    lang_types: tuple[str, ...]
    limits: SearchLimits
    include_globs: list[str] = msgspec.field(default_factory=list)
    exclude_globs: list[str] = msgspec.field(default_factory=list)
    operation: str = "json"  # json|count|files|files_with_matches
    paths: tuple[str, ...] = (".",)
    extra_patterns: tuple[str, ...] = ()
```

```python
# tools/cq/search/rg/contracts.py — serialized settings parity
class RgRunSettingsV1(CqSettingsStruct, frozen=True):
    pattern: str
    mode: str
    lang_types: tuple[str, ...]
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    operation: str = "json"
    paths: tuple[str, ...] = (".",)
    extra_patterns: tuple[str, ...] = ()


class RgProcessResultV1(CqOutputStruct, frozen=True):
    command: tuple[str, ...]
    timed_out: bool
    returncode: int
    stderr: str
    events: tuple[dict[str, object], ...] = ()
    stdout_lines: tuple[str, ...] = ()  # required by count/files operation modes
```

```python
# tools/cq/search/rg/runner.py — build_rg_command() mode/path switch
def build_rg_command(...) -> list[str]:
    command = ["rg"]
    if operation == "json":
        command.extend(["--json", "--line-number", "--column"])
    elif operation == "count":
        command.extend(["--count-matches", "--no-heading"])
    elif operation == "files":
        command.append("--files")
    elif operation == "files_with_matches":
        command.extend(["--files-with-matches", "--no-messages"])
    else:
        msg = f"Unsupported rg operation: {operation}"
        raise ValueError(msg)

    # Shared type/glob/mode flags...
    # Shared pattern flags for search operations...

    search_paths = list(paths) or ["."]
    if operation == "files":
        command.extend(search_paths)
    else:
        command.extend(["-e", pattern])
        for extra in extra_patterns:
            command.extend(["-e", extra])
        command.extend(search_paths)
    return command
```

### Files to Edit

- `tools/cq/search/_shared/core.py` — Extend `RgRunRequest` with `operation`, `paths`, and `extra_patterns`.
- `tools/cq/search/rg/contracts.py` — Extend `RgRunSettingsV1` with matching fields and request/settings conversion.
- `tools/cq/search/rg/runner.py` — Route command construction through operation-mode and path-aware logic.
- `tools/cq/search/rg/adapter.py` — Use operation-mode settings for file-list/count helpers instead of direct command composition.

### New Files to Create

- None — expand existing `tests/unit/cq/search/rg/test_runner.py` and `tests/unit/cq/search/rg/test_contracts.py` with operation-mode and path-scoping coverage.

### Legacy Decommission/Delete Scope

- Delete operation-specific ad-hoc rg command assembly in helper modules introduced by S1/S7/S10 drafts; all such behavior is superseded by `RgRunRequest`/`RgRunSettingsV1` + shared runner routing.

---

## S1. C1 — Ripgrep-Accelerated File Prefiltering for ast-grep Batch Scans

### Goal

Eliminate unnecessary file reads and AST parses in multi-file ast-grep scans by using ripgrep `--files-with-matches` (`-l`) as a text prefilter before structural matching. Apply this in the real execution path (`query/sg_parser.py` + query batch flows), not only in test helper paths.

### Representative Code Snippets

```python
# tools/cq/search/rg/prefilter.py
"""Ripgrep prefilter helpers for ast-grep batch scans."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.astgrep.sgpy_scanner import RuleSpec
from tools.cq.core.pathing import normalize_repo_relative_path
from tools.cq.query.language import ripgrep_types_for_scope
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import INTERACTIVE, SearchLimits
from tools.cq.search.rg.runner import run_rg_files_with_matches

if TYPE_CHECKING:
    from tools.cq.query.language import QueryLanguageScope


_METAVAR_TOKEN_RE = re.compile(r"\${1,3}_?[A-Z][A-Z0-9_]*")


def extract_literal_fragments(pattern: str) -> list[str]:
    """Return non-metavariable literal fragments sorted by selectivity."""
    fragments = _METAVAR_TOKEN_RE.split(pattern)
    cleaned = [f.strip() for f in fragments if f.strip()]
    return sorted(cleaned, key=len, reverse=True)


def collect_prefilter_fragments(rules: tuple[RuleSpec, ...]) -> tuple[str, ...]:
    """Collect candidate literals from rule pattern strings."""
    fragments: list[str] = []
    for rule in rules:
        pattern = rule.to_config().get("pattern")
        if isinstance(pattern, str):
            fragments.extend(extract_literal_fragments(pattern)[:2])
    # De-duplicate while preserving high-selectivity order.
    unique = sorted({f for f in fragments if len(f) >= 3}, key=len, reverse=True)
    return tuple(unique[:8])


def rg_prefilter_files(
    root: Path,
    *,
    files: list[Path],
    literals: tuple[str, ...],
    lang_scope: QueryLanguageScope,
    limits: SearchLimits | None = None,
) -> list[Path]:
    """Return subset of files that contain at least one literal."""
    if not literals:
        return files
    effective_limits = limits or INTERACTIVE
    candidate_paths = tuple(
        normalize_repo_relative_path(path.resolve(), root=root.resolve()) for path in files
    )
    matched_rel = run_rg_files_with_matches(
        root=root,
        patterns=literals,
        mode=QueryMode.LITERAL,
        lang_types=tuple(ripgrep_types_for_scope(lang_scope)),
        include_globs=[],
        exclude_globs=[],
        paths=candidate_paths,
        limits=effective_limits,
    )
    if matched_rel is None:
        return files  # Fail open
    return sorted((root / rel).resolve() for rel in matched_rel)
```

```python
# tools/cq/astgrep/sgpy_scanner.py — integrate prefilter into production scan path
from tools.cq.search.rg.prefilter import collect_prefilter_fragments, rg_prefilter_files

def scan_files(
    files: list[Path],
    rules: tuple[RuleSpec, ...],
    root: Path,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
    *,
    prefilter: bool = True,
) -> list[SgRecord]:
    candidate_files = files
    if prefilter and len(files) > 1 and rules:
        literals = collect_prefilter_fragments(rules)
        candidate_files = rg_prefilter_files(
            root,
            files=files,
            literals=literals,
            lang_scope="python" if lang == "python" else "rust",
        )

    for file_path in candidate_files:
        ...
```

```python
# tools/cq/query/sg_parser.py — enable prefilter where it matters
records = scan_files(files, rules, root, lang=lang, prefilter=True)
```

```python
# tools/cq/query/batch.py — same prefilter behavior for shared entity sessions
records = scan_files(files, rules, root, lang=lang, prefilter=True)
```

```python
# tools/cq/search/pipeline/classifier_runtime.py — keep single-file classification direct
records = scan_files([file_path], rules, root, lang=lang, prefilter=False)
```

```python
# tools/cq/query/executor.py — optional prefilter in pattern runtime paths
# Apply only when file_count > 1 and literals are available.
for file_path in maybe_prefilter_astgrep_files(files, rules, root, lang=lang):
    ...
```

### Files to Edit

- `tools/cq/astgrep/sgpy_scanner.py` — Add `prefilter` parameter to `scan_files()` and apply prefilter prior to parse loop.
- `tools/cq/query/sg_parser.py` — Enable prefilter for batch scans used in query execution.
- `tools/cq/query/batch.py` — Enable prefilter for shared batch entity scan sessions.
- `tools/cq/search/pipeline/classifier_runtime.py` — Explicitly disable prefilter for single-file classification lane.
- `tools/cq/query/executor.py` — Optionally apply prefilter to pattern runtime file loops when multi-file.
- `tests/unit/cq/test_sg_parser.py` — Add coverage asserting prefilter integration in parser scan path.
- `tests/unit/cq/test_run_batch_scan.py` — Add coverage asserting prefilter integration in query batch sessions.

### New Files to Create

- `tools/cq/search/rg/prefilter.py` — Ripgrep prefilter helper module.
- `tests/unit/cq/search/rg/test_prefilter.py` — Literal extraction + prefilter behavior tests.

### Legacy Decommission/Delete Scope

- None — additive optimization with fail-open behavior.

---

## S2. A2 — Word-Boundary Mode (`-w/--word-regexp`) for Identifier Searches

### Goal

Standardize identifier boundary behavior on ripgrep `-w` everywhere identifier mode is used. Remove duplicated manual `\b` wrapping in adapter and smart-search candidate generation so behavior is consistent and centralized.

### Representative Code Snippets

```python
# tools/cq/search/rg/runner.py — build_rg_command() addition
if mode.value == "literal":
    command.append("-F")
elif mode.value == "identifier":
    command.append("-w")
command.extend(["-e", pattern, "."])
```

```python
# tools/cq/search/rg/adapter.py — remove manual \b wrapping
import re

symbol = function_name.rsplit(".", maxsplit=1)[-1]
pattern = rf"{re.escape(symbol)}\s*\("
request = RgRunRequest(
    root=root,
    pattern=pattern,
    mode=QueryMode.IDENTIFIER,
    ...
)
```

```python
# tools/cq/search/pipeline/smart_search.py — centralize identifier pattern escaping
def _identifier_pattern(query: str) -> str:
    return re.escape(query)

def _run_candidate_phase(...):
    pattern = _identifier_pattern(ctx.query) if mode == QueryMode.IDENTIFIER else ctx.query
    ...
```

### Files to Edit

- `tools/cq/search/rg/runner.py` — Emit `-w` for identifier mode.
- `tools/cq/search/rg/adapter.py` — Remove manual boundary wrapping and keep identifier mode selection.
- `tools/cq/search/pipeline/smart_search.py` — Replace manual `\b...\b` construction with escaped identifier pattern and rely on `-w`.

### New Files to Create

- None — update existing runner/adapter/smart-search tests.

### Legacy Decommission/Delete Scope

- `tools/cq/search/rg/adapter.py` — Delete `rf"\b{symbol}\s*\("` pattern construction.
- `tools/cq/search/pipeline/smart_search.py` — Delete manual `rf"\b{re.escape(query)}\b"` wrappers in candidate command generation.

---

## S3. A1 — Multi-Pattern OR Search (`-e P1 -e P2`)

### Goal

Allow batching multiple patterns into a single ripgrep invocation using multiple `-e` flags for OR semantics. This reduces subprocess overhead when searching for multiple symbols (e.g., a function plus its aliases, or multiple import names).

### Representative Code Snippets

```python
# tools/cq/search/_shared/core.py — RgRunRequest extension
class RgRunRequest(CqStruct, frozen=True):
    """Input contract for native ripgrep JSON execution."""

    root: Path
    pattern: str
    mode: QueryMode
    lang_types: tuple[str, ...]
    limits: SearchLimits
    include_globs: list[str] = msgspec.field(default_factory=list)
    exclude_globs: list[str] = msgspec.field(default_factory=list)
    extra_patterns: tuple[str, ...] = ()  # Additional OR patterns
```

```python
# tools/cq/search/rg/runner.py — build_rg_command() multi-pattern
def build_rg_command(
    *,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str],
    exclude_globs: list[str],
    limits: SearchLimits,
    extra_patterns: tuple[str, ...] = (),
) -> list[str]:
    # ... existing command construction ...
    if mode.value == "literal":
        command.append("-F")
    elif mode.value == "identifier":
        command.append("-w")

    command.extend(["-e", pattern])
    for extra in extra_patterns:
        command.extend(["-e", extra])
    command.append(".")
    return command
```

```python
# tools/cq/search/rg/contracts.py — RgRunSettingsV1 extension
class RgRunSettingsV1(CqSettingsStruct, frozen=True):
    """Serializable settings payload for native ripgrep execution."""

    pattern: str
    mode: str
    lang_types: tuple[str, ...]
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    extra_patterns: tuple[str, ...] = ()
```

### Files to Edit

- `tools/cq/search/_shared/core.py` — Add `extra_patterns: tuple[str, ...] = ()` field to `RgRunRequest`, update `to_settings()`.
- `tools/cq/search/rg/runner.py` — Update `build_rg_command()` signature and body to emit multiple `-e` flags; update `run_rg_json()` to pass `extra_patterns` through; update `build_command_from_settings()`.
- `tools/cq/search/rg/contracts.py` — Add `extra_patterns` field to `RgRunSettingsV1`; update `settings_from_request()`.

### New Files to Create

- None — existing tests gain parametrized multi-pattern cases.

### Legacy Decommission/Delete Scope

- None — additive change with backward-compatible defaults.

---

## S4. B1 — `get_multiple_matches()` for Variadic Captures (`$$$ARGS`)

### Goal

Capture variadic metavariables as first-class structured data instead of dropping them. Extend match extraction to call `get_multiple_matches()` whenever a metavariable is known to be variadic, and preserve both per-node captures and aggregate text.

### Representative Code Snippets

```python
# tools/cq/astgrep/sgpy_scanner.py — structured single+multi capture extraction
def _extract_metavars(
    match: SgNode,
    *,
    metavar_names: tuple[str, ...],
    variadic_names: frozenset[str],
) -> dict[str, dict[str, object]]:
    captures: dict[str, dict[str, object]] = {}
    for name in metavar_names:
        single = match.get_match(name)
        if single is not None:
            captures[name] = _node_payload(single)
            captures[f"${name}"] = captures[name]

        if name in variadic_names:
            multi = match.get_multiple_matches(name)
            if multi:
                captures[f"$$${name}"] = {
                    "kind": "multi",
                    "nodes": [_node_payload(node) for node in multi],
                }
    return captures
```

```python
# tools/cq/query/executor.py — include variadic captures in pattern findings
captures = _extract_match_metavars(
    match,
    metavar_names=rule_ctx.metavar_names,
    include_multi=True,
)
finding.details["metavar_captures"] = captures
```

### Files to Edit

- `tools/cq/astgrep/sgpy_scanner.py` — Add structured multi-capture extraction and thread known metavariable names.
- `tools/cq/query/executor.py` — Include multi-capture extraction in pattern-finding detail payloads.

### New Files to Create

- None — extend existing scanner/query executor tests with variadic capture cases.

### Legacy Decommission/Delete Scope

- Remove single-only metavariable extraction paths that call only `get_match()` for variadic names.

---

## S5. A3 — Context Lines (`-C`/`-A`/`-B`) for Richer Match Payloads

### Goal

Handle ripgrep's `type: "context"` JSON events to store surrounding lines per match. Add optional `context_before` and `context_after` fields to `SearchLimits` and pass them as `--before-context` / `--after-context` to the ripgrep command builder. Useful for showing function signatures near callsites and improving heuristic classification.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/profiles.py — SearchLimits extension
class SearchLimits(CqSettingsStruct, frozen=True):
    # ... existing fields ...
    max_file_size_bytes: PositiveInt = 2 * 1024 * 1024
    context_before: int = 0
    context_after: int = 0
```

```python
# tools/cq/search/rg/runner.py — build_rg_command() context flags
    # After existing limit flags, before type flags:
    if limits.context_before > 0:
        command.extend(["--before-context", str(limits.context_before)])
    if limits.context_after > 0:
        command.extend(["--after-context", str(limits.context_after)])
```

```python
# tools/cq/search/rg/codec.py — New context event type
class RgContextData(msgspec.Struct, omit_defaults=True):
    """Typed ripgrep context line data payload."""

    path: RgPath | None = None
    lines: RgText | None = None
    line_number: int | None = None
    submatches: list[RgSubmatch] = msgspec.field(default_factory=list)


class RgContextEvent(msgspec.Struct, frozen=True, tag_field="type", tag="context"):
    """Typed ripgrep context event."""

    data: RgContextData

    @property
    def type(self) -> Literal["context"]:
        """Return tagged event type."""
        return "context"

# Update union types:
type RgTypedEvent = RgMatchEvent | RgSummaryEvent | RgContextEvent
type RgAnyEvent = RgMatchEvent | RgSummaryEvent | RgContextEvent | RgEvent
```

```python
# tools/cq/search/rg/collector.py — Context line handling in RgCollector
@dataclass
class RgCollector:
    # ... existing fields ...
    context_lines: dict[str, dict[int, str]] = field(default_factory=dict)
    # Maps file_path -> {line_number: line_text}

    def handle_event(self, event: RgAnyEvent) -> None:
        if event.type == "context":
            self._handle_context(event)
            return
        # ... existing dispatch ...

    def _handle_context(self, event: RgAnyEvent) -> None:
        """Store context lines keyed by file and line number."""
        if not isinstance(event, RgContextEvent):
            return
        data = event.data
        file_path = data.path.text if data.path else None
        if file_path and data.line_number is not None and data.lines:
            if file_path not in self.context_lines:
                self.context_lines[file_path] = {}
            text = data.lines.text or data.lines.bytes or ""
            self.context_lines[file_path][data.line_number] = text
```

### Files to Edit

- `tools/cq/search/pipeline/profiles.py` — Add `context_before: int = 0` and `context_after: int = 0` fields to `SearchLimits`.
- `tools/cq/search/rg/runner.py` — Add `--before-context` / `--after-context` flags in `build_rg_command()` when values > 0.
- `tools/cq/search/rg/codec.py` — Add `RgContextData`, `RgContextEvent` types; update `RgTypedEvent` and `RgAnyEvent` unions.
- `tools/cq/search/rg/collector.py` — Add `context_lines` dict field; add `_handle_context()` method; update `handle_event()` dispatch.

### New Files to Create

- None — existing test files gain context event cases.

### Legacy Decommission/Delete Scope

- None — additive with backward-compatible defaults.

---

## S6. B5 — `node.matches()` / `node.inside()` / `node.has()` for Post-Match Refinement

### Goal

Replace Python-side parent-chain walking in a phased, high-confidence sequence instead of a single large sweep.
Phase 1 migrates the highest-value hotspots (`classifier_runtime.py` docstring/scope checks and a bounded set of extractor helpers).
Phase 2 expands to remaining extractor parent-chain sites only after Phase 1 equivalence tests pass.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/classifier_runtime.py — Replace _is_docstring_context()
# Before:
def _is_docstring_context(node: SgNode) -> bool:
    parent = node.parent()
    if parent is None:
        return False
    if parent.kind() == "expression_statement":
        gp = parent.parent()
        if gp is not None and gp.kind() in (
            "function_definition", "class_definition", "module",
        ):
            return True
    return False

# After:
def _is_docstring_context(node: SgNode) -> bool:
    return node.inside(kind="expression_statement") and (
        node.inside(kind="function_definition")
        or node.inside(kind="class_definition")
        or node.inside(kind="module")
    )
```

```python
# tools/cq/search/python/extractors.py — Phase 1: replace bounded parent-chain helpers
# Before (representative):
def _is_method_def(node: SgNode) -> bool:
    parent = node.parent()
    if parent is None:
        return False
    if parent.kind() == "block":
        gp = parent.parent()
        if gp is not None and gp.kind() == "class_definition":
            return True
    return False

# After:
def _is_method_def(node: SgNode) -> bool:
    return node.inside(kind="class_definition")
```

```python
# tools/cq/search/pipeline/classifier_runtime.py — Phase 1: use has() for child checks
# Before:
def _has_decorator(node: SgNode) -> bool:
    for child in node.children():
        if child.kind() == "decorator":
            return True
    return False

# After:
def _has_decorator(node: SgNode) -> bool:
    return node.has(kind="decorator")
```

### Files to Edit

- `tools/cq/search/pipeline/classifier_runtime.py` — Phase 1 migration for `_is_docstring_context()` and bounded child-check helpers.
- `tools/cq/search/python/extractors.py` — Phase 1 migration for a named subset of helpers (method/class/function context predicates) with behavior-parity assertions before any wider conversion.

### New Files to Create

- None — extend existing `tests/unit/cq/search/pipeline/test_classification.py` and `tests/unit/cq/search/python/test_extractors.py` with explicit parity cases for each migrated helper.

### Legacy Decommission/Delete Scope

- `tools/cq/search/pipeline/classifier_runtime.py` — Remove only the specific parent-chain helpers replaced in Phase 1.
- `tools/cq/search/python/extractors.py` — Remove only superseded helper implementations named in the Phase 1 file manifest; defer broader deletion to a follow-on phase after measured parity.

---

## S7. A4 — `--count` / `--count-matches` for Fast Cardinality Probes

### Goal

Add a lightweight `run_rg_count()` function routed through the shared rg lane from S0. This uses operation-mode `count` output for fast cardinality probes without full JSON event decode and enables adaptive profile selection in smart-search pre-flight.

### Representative Code Snippets

```python
# tools/cq/search/rg/runner.py — New count function via shared runner contracts

def run_rg_count(
    *,
    root: Path,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    paths: tuple[str, ...] = (".",),
    limits: SearchLimits | None = None,
) -> dict[str, int]:
    settings = RgRunSettingsV1(
        pattern=pattern,
        mode=mode.value,
        lang_types=lang_types,
        include_globs=tuple(include_globs or ()),
        exclude_globs=tuple(exclude_globs or ()),
        operation="count",
        paths=paths,
    )
    result = run_with_settings(root=root, limits=limits or INTERACTIVE, settings=settings)
    counts: dict[str, int] = {}
    if result.returncode in (0, 1):
        for line in result.stdout_lines:
            parts = line.rsplit(":", maxsplit=1)
            if len(parts) == 2:
                path, count_str = parts
                try:
                    counts[path.strip()] = int(count_str.strip())
                except ValueError:
                    continue
    return counts
```

### Files to Edit

- `tools/cq/search/rg/runner.py` — Add `run_rg_count()` function using S0 operation-mode routing.
- `tools/cq/search/rg/contracts.py` — Include `stdout_lines` payload for non-JSON operation modes.
- `tools/cq/search/pipeline/smart_search.py` — Use `run_rg_count()` in pre-flight to select adaptive profile.

### New Files to Create

- None — existing test file `tests/unit/cq/search/rg/test_runner.py` gains count-mode cases.

### Legacy Decommission/Delete Scope

- None — additive function.

---

## S8. B4 — Constraint-Driven Metavariable Filtering for Pattern Queries

### Goal

Push eligible metavariable filters into ast-grep `constraints` in the production pattern-query runtime (`query/executor.py`). This removes avoidable Python-side post-filtering for positive regex filters while preserving Python fallback for unsupported forms (for example negated filters).

### Representative Code Snippets

```python
# tools/cq/query/metavar.py — partition filters into pushdown + residual
def partition_metavar_filters(
    filters: tuple[MetaVarFilter, ...],
) -> tuple[dict[str, dict[str, str]], tuple[MetaVarFilter, ...]]:
    constraints: dict[str, dict[str, str]] = {}
    residual: list[MetaVarFilter] = []
    for item in filters:
        if item.negate:
            residual.append(item)
            continue
        constraints[item.name] = {"regex": item.pattern}
    return constraints, tuple(residual)
```

```python
# tools/cq/query/executor.py — apply constraints in ast-grep config path
constraints, residual_filters = partition_metavar_filters(ctx.query.metavar_filters)
config: Config = {"rule": cast("Rule", rule.to_yaml_dict())}
if constraints:
    config["constraints"] = constraints
matches = list(node.find_all(config=config))

# Keep Python fallback for residual filters only.
if residual_filters:
    captures = _parse_sgpy_metavariables(match, metavar_names=rule_ctx.metavar_names)
    if not apply_metavar_filters(captures, residual_filters):
        continue
```

### Files to Edit

- `tools/cq/query/metavar.py` — Add filter partitioning helper for ast-grep constraint pushdown.
- `tools/cq/query/executor.py` — Thread pushdown constraints into inline rule execution, preserve residual Python filtering path.

### New Files to Create

- None — extend existing pattern query tests with pushdown/residual mixed cases.

### Legacy Decommission/Delete Scope

- Remove Python-only filtering passes that are superseded by ast-grep `constraints` for positive regex metavariable filters.

---

## S9. A8 — Multiline Ripgrep (`-U`) for Cross-Line Pattern Matching

### Goal

Enable multiline text search mode using ripgrep's `-U --multiline-dotall` flags. This serves the text search use case for patterns spanning lines (decorators + function defs, multi-line imports, try/except blocks) without duplicating tree-sitter's structural parsing.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/profiles.py — SearchLimits extension
class SearchLimits(CqSettingsStruct, frozen=True):
    # ... existing fields ...
    context_before: int = 0  # (from S5)
    context_after: int = 0   # (from S5)
    multiline: bool = False
```

```python
# tools/cq/search/rg/runner.py — build_rg_command() multiline support
    # After mode-specific flags:
    if multiline:
        command.extend(["-U", "--multiline-dotall"])
```

```python
# tools/cq/search/rg/runner.py — build_rg_command() updated signature
def build_rg_command(
    *,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str],
    exclude_globs: list[str],
    limits: SearchLimits,
    extra_patterns: tuple[str, ...] = (),
) -> list[str]:
    # ... existing command construction ...
    if limits.multiline:
        command.extend(["-U", "--multiline-dotall"])
    # ... rest of command ...
```

### Files to Edit

- `tools/cq/search/pipeline/profiles.py` — Add `multiline: bool = False` field to `SearchLimits`.
- `tools/cq/search/rg/runner.py` — Add `-U --multiline-dotall` flags when `limits.multiline` is True.
- `tools/cq/search/rg/collector.py` — Handle multiline match spans (matches may span multiple lines) in `_append_submatch()`.
- `tools/cq/search/_shared/core.py` — Document multiline behavior in `RgRunRequest`.

### New Files to Create

- None — existing tests gain multiline mode cases.

### Legacy Decommission/Delete Scope

- None — additive with backward-compatible default.

---

## S10. A5 — `--files` Mode for File-Set Enumeration

### Goal

Add a `list_candidate_files()` function using shared rg operation mode `files` (from S0) to enumerate exactly which files would be searched given current ignore/glob/type configuration. Useful for debugging "why didn't rg find X?" and validating scope configuration without creating a new subprocess lane.

### Representative Code Snippets

```python
# tools/cq/search/rg/adapter.py — New file enumeration function

def list_candidate_files(
    root: Path,
    *,
    lang_types: tuple[str, ...] = (),
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    paths: tuple[str, ...] = (".",),
    limits: SearchLimits | None = None,
) -> list[Path]:
    """List files that ripgrep would search given current configuration.

    settings = RgRunSettingsV1(
        pattern="",
        mode="regex",
        lang_types=lang_types,
        include_globs=tuple(include_globs or ()),
        exclude_globs=tuple(exclude_globs or ()),
        operation="files",
        paths=paths,
    )
    result = run_with_settings(root=root, limits=limits or INTERACTIVE, settings=settings)
    if result.returncode not in (0, 1):
        return []
    return sorted((root / rel.strip()).resolve() for rel in result.stdout_lines if rel.strip())
```

### Files to Edit

- `tools/cq/search/rg/adapter.py` — Add `list_candidate_files()` function using S0 operation-mode settings.
- `tools/cq/search/rg/contracts.py` — Carry `stdout_lines` for `operation="files"` responses.

### New Files to Create

- None — expand existing `tests/unit/cq/search/rg/test_runner.py` and `tests/unit/cq/search/rg/test_contracts.py` with `operation="files"` coverage plus adapter integration assertions.

### Legacy Decommission/Delete Scope

- None — additive function.

---
## S11. B6 — Pattern Object Syntax (`context` + `selector`) Threading

### Goal

Execute `pattern.context`, `pattern.selector`, and non-default strictness in the production pattern-query runtime. Parser and IR already preserve these fields; this scope ensures they are used for matching instead of only appearing in summary metadata.

### Representative Code Snippets

```python
# tools/cq/query/executor.py — execute pattern-object rules via Config path
def _matches_for_rule(
    node: SgNode,
    rule: AstGrepRule,
    *,
    constraints: dict[str, dict[str, str]] | None = None,
) -> list[SgNode]:
    if rule.requires_inline_rule() or constraints:
        config: Config = {"rule": cast("Rule", rule.to_yaml_dict())}
        if constraints:
            config["constraints"] = constraints
        return list(node.find_all(config=config))
    if rule.kind and rule.pattern in {"$FUNC", "$METHOD", "$CLASS"}:
        return list(node.find_all(kind=rule.kind))
    return list(node.find_all(pattern=rule.pattern))
```

```python
# tools/cq/query/planner.py — keep context/selector/strictness threaded
rule = AstGrepRule(
    pattern=query.pattern_spec.pattern,
    context=query.pattern_spec.context,
    selector=query.pattern_spec.selector,
    strictness=query.pattern_spec.strictness,
)
```

```python
# tools/cq/query/executor.py — use same matcher in findings + span collection
for match in _matches_for_rule(node, rule, constraints=constraints):
    ...
```

### Files to Edit

- `tools/cq/query/executor.py` — Add shared rule-matching helper and route pattern-object execution through Config.
- `tools/cq/query/planner.py` — Keep compile-time threading explicit for context/selector/strictness fields.

### New Files to Create

- None — extend existing planner/executor tests with pattern-object runtime cases.

### Legacy Decommission/Delete Scope

- Remove pattern-object metadata-only execution behavior in `tools/cq/query/executor.py`.

---

## S12. C3 — Ripgrep `begin`/`end` Events for File-Level Metadata

### Goal

Handle ripgrep `type: "begin"` and `type: "end"` JSON events so file accounting is event-driven and accurate. This provides reliable scanned-file counts, binary-file detection, and per-file lifecycle metadata without extra syscalls.

### Representative Code Snippets

```python
# tools/cq/search/rg/codec.py — typed begin/end payloads
class RgBeginData(msgspec.Struct, omit_defaults=True):
    path: RgPath | None = None


class RgEndData(msgspec.Struct, omit_defaults=True):
    path: RgPath | None = None
    binary_offset: int | None = None
    stats: RgSummaryStats | None = None


class RgBeginEvent(msgspec.Struct, frozen=True, tag_field="type", tag="begin"):
    data: RgBeginData


class RgEndEvent(msgspec.Struct, frozen=True, tag_field="type", tag="end"):
    data: RgEndData


type RgTypedEvent = (
    RgMatchEvent | RgSummaryEvent | RgContextEvent | RgBeginEvent | RgEndEvent
)
```

```python
# tools/cq/search/rg/collector.py — file lifecycle tracking
@dataclass
class RgCollector:
    files_started: set[str] = field(default_factory=set)
    files_completed: set[str] = field(default_factory=set)
    binary_files: set[str] = field(default_factory=set)

    def handle_event(self, event: RgAnyEvent) -> None:
        if event.type == "begin":
            self._handle_begin(event)
            return
        if event.type == "end":
            self._handle_end(event)
            return
        ...
```

```python
# tools/cq/search/pipeline/smart_search.py — prefer event-driven file counts
scanned_files = len(collector.files_completed or collector.files_started)
```

### Files to Edit

- `tools/cq/search/rg/codec.py` — Add begin/end event payload types and union support.
- `tools/cq/search/rg/collector.py` — Track started/completed files and binary-file markers from begin/end events.
- `tools/cq/search/pipeline/smart_search.py` — Use begin/end-driven file stats for summary fields.

### New Files to Create

- None — extend codec/collector/smart-search tests with begin/end fixtures.

### Legacy Decommission/Delete Scope

- Remove fallback-only scanned-file inference where begin/end metadata is available.

---

## S13. A6 — `--sort path` for Deterministic Output Ordering

### Goal

Add an optional `sort_by_path: bool` field to `SearchLimits` and pass `--sort path` to ripgrep when enabled. This guarantees file-order determinism from the source, replacing post-collection Python sorting. Trade-off: disables ripgrep parallelism, so only enable in test/CI profiles.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/profiles.py — SearchLimits extension
class SearchLimits(CqSettingsStruct, frozen=True):
    # ... existing fields ...
    sort_by_path: bool = False
```

```python
# tools/cq/search/rg/runner.py — build_rg_command() sort flag
    if limits.sort_by_path:
        command.extend(["--sort", "path"])
```

```python
# tools/cq/search/pipeline/profiles.py — CI profile
CI = SearchLimits(
    max_files=50000,
    max_total_matches=100000,
    timeout_seconds=300.0,
    max_depth=50,
    max_file_size_bytes=10 * 1024 * 1024,
    sort_by_path=True,
)
```

### Files to Edit

- `tools/cq/search/pipeline/profiles.py` — Add `sort_by_path: bool = False` field to `SearchLimits`.
- `tools/cq/search/rg/runner.py` — Add `--sort path` flag when `limits.sort_by_path` is True.

### New Files to Create

- None — existing tests gain sort mode cases.

### Legacy Decommission/Delete Scope

- Python-side post-collection sorting in callers can be removed when `sort_by_path=True` is the active profile.

---

## S14. B2 — Externalized YAML/JSON Rule Packs

### Goal

Unify the dual rule systems into a YAML-authoritative loader with staged migration. Stage 1 adds loader + parity validation while keeping `rules_py.py`/`rules_rust.py` as compatibility providers. Stage 2 migrates imports/callsites and tests to loader-backed APIs. Stage 3 deletes compatibility modules only after all direct imports are removed and parity tests pass.

**Key design decision:** Use `decode_yaml_strict()` from `tools/cq/core/typed_boundary.py` as the canonical YAML decode path, consistent with `load_pack_rules()` in `query_models.py`. Do NOT create standalone `msgspec.yaml.Decoder` instances — all YAML decoding in CQ goes through the typed boundary layer for uniform `BoundaryDecodeError` handling.

### Representative Code Snippets

```yaml
# tools/cq/astgrep/rules/python_facts/py_def_function.yml — EXISTING (no changes needed)
# These 23 files already define rules in ast-grep's native YAML schema.
# The loader bridges this schema to RuleSpec objects.
id: py_def_function
language: python
severity: hint
rule:
  kind: function_definition
  regex: "^def "
  not:
    has:
      kind: type_parameter
metadata:
  record: def
  kind: function
```

```yaml
# tools/cq/astgrep/rules/rust_facts/rs_def_function.yml — NEW (8 Rust rules)
# Only Rust rules need new YAML files; Python rules already exist.
id: rs_def_function
language: rust
severity: hint
rule:
  kind: function_item
metadata:
  record: def
  kind: function
```

```python
# tools/cq/astgrep/rulepack_loader.py
"""YAML rule pack loader for ast-grep-py rules.

Bridges the existing CLI-mode YAML rule files (ast-grep native schema)
into RuleSpec tuples for the Python-side scanner runtime. Uses
decode_yaml_strict() from typed_boundary for all YAML decoding.
"""

from __future__ import annotations

from pathlib import Path

import msgspec

from tools.cq.astgrep.sgpy_scanner import RuleSpec
from tools.cq.core.structs import CqStruct
from tools.cq.core.typed_boundary import BoundaryDecodeError, decode_yaml_strict


class CliRuleMetadata(CqStruct, frozen=True):
    """Metadata section from ast-grep CLI YAML rule format."""

    record: str = ""
    kind: str = ""


class CliRuleFile(CqStruct, frozen=True):
    """Single ast-grep CLI YAML rule file schema.

    Maps the native ast-grep CLI format (id, language, severity, rule, metadata)
    to a structure that can be converted to RuleSpec.
    """

    id: str
    language: str = "python"
    severity: str = "hint"
    rule: dict[str, object] = msgspec.field(default_factory=dict)
    metadata: CliRuleMetadata = CliRuleMetadata()


def load_cli_rule_file(path: Path) -> RuleSpec | None:
    """Load a single ast-grep CLI YAML rule file and convert to RuleSpec.

    Parameters
    ----------
    path
        Path to a CLI-mode YAML rule file.

    Returns
    -------
    RuleSpec | None
        Converted RuleSpec, or None if decoding fails.
    """
    try:
        parsed = decode_yaml_strict(path.read_bytes(), type_=CliRuleFile)
    except (OSError, BoundaryDecodeError):
        return None
    return RuleSpec(
        rule_id=parsed.id,
        record_type=parsed.metadata.record,
        kind=parsed.metadata.kind,
        config={"rule": dict(parsed.rule)},
    )


def load_rules_from_directory(rule_dir: Path) -> tuple[RuleSpec, ...]:
    """Load all YAML rule files from a directory into a RuleSpec tuple.

    Parameters
    ----------
    rule_dir
        Directory containing ``*.yml`` rule files.

    Returns
    -------
    tuple[RuleSpec, ...]
        Sorted tuple of loaded RuleSpec objects.
    """
    if not rule_dir.is_dir():
        return ()
    specs: list[RuleSpec] = []
    for yaml_file in sorted(rule_dir.glob("*.yml")):
        spec = load_cli_rule_file(yaml_file)
        if spec is not None:
            specs.append(spec)
    return tuple(specs)


def load_default_rulepacks() -> dict[str, tuple[RuleSpec, ...]]:
    """Load all built-in rule packs from the rules directory.

    Scans ``rules/python_facts/`` (23 existing files) and
    ``rules/rust_facts/`` (8 new files) relative to the astgrep
    package directory.

    Returns
    -------
    dict[str, tuple[RuleSpec, ...]]
        Mapping of language name to rule tuple.
    """
    base = Path(__file__).parent / "rules"
    packs: dict[str, tuple[RuleSpec, ...]] = {}
    for facts_dir in sorted(base.glob("*_facts")):
        lang = facts_dir.name.removesuffix("_facts")
        rules = load_rules_from_directory(facts_dir)
        if rules:
            packs[lang] = rules
    return packs
```

```python
# tests/unit/cq/test_rulepack_loader.py — Round-trip parity test
"""Validate YAML rules produce identical RuleSpec configs to Python rules."""

from __future__ import annotations

import pytest

from tools.cq.astgrep.rulepack_loader import load_default_rulepacks
from tools.cq.astgrep.rules_py import PYTHON_FACT_RULES


def test_yaml_python_rule_parity() -> None:
    """Every YAML-loaded Python rule must match the hand-coded Python RuleSpec."""
    packs = load_default_rulepacks()
    yaml_rules = packs.get("python", ())
    yaml_by_id = {r.rule_id: r for r in yaml_rules}
    python_by_id = {r.rule_id: r for r in PYTHON_FACT_RULES}

    # Same rule IDs
    assert set(yaml_by_id.keys()) == set(python_by_id.keys()), (
        f"Rule ID mismatch: YAML-only={set(yaml_by_id) - set(python_by_id)}, "
        f"Python-only={set(python_by_id) - set(yaml_by_id)}"
    )

    # Same record_type and kind per rule
    for rule_id in yaml_by_id:
        yaml_r = yaml_by_id[rule_id]
        py_r = python_by_id[rule_id]
        assert yaml_r.record_type == py_r.record_type, (
            f"{rule_id}: record_type mismatch: YAML={yaml_r.record_type}, Python={py_r.record_type}"
        )
        assert yaml_r.kind == py_r.kind, (
            f"{rule_id}: kind mismatch: YAML={yaml_r.kind}, Python={py_r.kind}"
        )
```

### Files to Edit

- `tools/cq/astgrep/sgpy_scanner.py` — Add optional `rulepack_path` parameter to `scan_files()`, fallback to Python rules when YAML not available.
- `tools/cq/astgrep/rules.py` — Update language-aware rule dispatcher to prefer YAML-loaded rules when available, falling back to Python rule modules.
- `tests/unit/cq/test_rules_py.py` — Migrate direct imports to loader-backed interfaces before decommission.
- `tests/unit/cq/test_astgrep_rules.py` — Update rust-rule assertions to run through loader-backed dispatch.
- `tests/unit/cq/test_sgpy_scanner.py` — Replace direct `rules_py` imports with fixture-based loader rule retrieval.

### New Files to Create

- `tools/cq/astgrep/rulepack_loader.py` — YAML rule pack loader using `decode_yaml_strict()` to bridge CLI-mode YAML schema to `RuleSpec`.
- `tools/cq/astgrep/rules/rust_facts/` — New directory with 8 YAML rule files for Rust (the 23 Python YAML rules already exist at `rules/python_facts/`).
- `tests/unit/cq/test_rulepack_loader.py` — Round-trip parity tests validating YAML rules produce identical `RuleSpec` configs to existing Python rules, plus Rust rule loading tests.

### Legacy Decommission/Delete Scope

- `tools/cq/astgrep/rules_py.py` — Mark as compatibility-only in Stage 1; delete only after Stage 2 import migration is complete across runtime callsites and tests.
- `tools/cq/astgrep/rules_rust.py` — Same staged deletion policy; no removal until YAML rust packs and migration tests are green.

---

## S15. B3 — Shared Utility Rules via `utils` + `matches` in Config

### Goal

Create reusable utility-rule packs for both Python and Rust and consume them through `matches`. The `utilDirs: [utils]` wiring already exists in `sgconfig.yml`; this scope item makes utility rules first-class in the Python runtime loader path and de-duplicates repeated rule fragments.

### Representative Code Snippets

```yaml
# tools/cq/astgrep/utils/is-python-literal.yml
id: is-python-literal
language: python
rule:
  any:
    - kind: string
    - kind: integer
    - kind: float
    - kind: true
    - kind: false
    - kind: none
```

```yaml
# tools/cq/astgrep/utils/is-rust-test-attr.yml
id: is-rust-test-attr
language: rust
rule:
  kind: attribute_item
  regex: "^#\\[(test|tokio::test)\\]"
```

```yaml
# tools/cq/astgrep/rules/rust_facts/rs_def_test_function.yml
id: rs_def_test_function
language: rust
severity: hint
rule:
  all:
    - kind: function_item
    - has:
        matches: is-rust-test-attr
metadata:
  record: def
  kind: test_function
```

```python
# tools/cq/astgrep/rulepack_loader.py — inject utils into Python runtime configs
def load_utils(utils_dir: Path) -> dict[str, dict[str, object]]:
    ...

def build_runtime_config(rule: CliRuleFile, utils: dict[str, dict[str, object]]) -> Config:
    config: Config = {"rule": dict(rule.rule)}
    if utils:
        config["utils"] = utils
    return config
```

### Files to Edit

- `tools/cq/astgrep/rulepack_loader.py` — Load `utils/` files and inject utility maps into runtime Config objects.
- `tools/cq/astgrep/sgpy_scanner.py` — Ensure Config-based scan path preserves `utils` sections.
- `tools/cq/astgrep/rules/python_facts/*.yml` — Replace duplicated inline sub-rules with `matches` references where applicable.
- `tools/cq/astgrep/rules/rust_facts/*.yml` — Adopt utility rules for reusable Rust attribute/call-site predicates.

### New Files to Create

- `tools/cq/astgrep/utils/is-python-literal.yml` — Python literal utility matcher.
- `tools/cq/astgrep/utils/is-python-builtin-call.yml` — Python builtin-call utility matcher.
- `tools/cq/astgrep/utils/is-rust-test-attr.yml` — Rust test attribute utility matcher.
- `tests/unit/cq/test_rulepack_utils.py` — Utility loading + `matches` resolution tests.

### Legacy Decommission/Delete Scope

- Remove duplicated per-rule fragments that become utility-rule references (especially repeated literal and attribute matcher blocks).

---

## S16. A7 — PCRE2 Capability Detection and Conditional Lookaround Patterns

### Goal

Detect and publish ripgrep PCRE2 capability once at bootstrap (`rg --pcre2-version`) and use that signal for safe conditional advanced-regex execution. This is the capability-negotiation scope; JSON fidelity improvements land in S21.

### Representative Code Snippets

```python
# tools/cq/core/toolchain.py — detect and store PCRE2 capability
def _detect_pcre2() -> tuple[bool, str | None]:
    proc = subprocess.run(
        ["rg", "--pcre2-version"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return False, None
    line = proc.stdout.splitlines()[0].strip() if proc.stdout else ""
    return True, line or None


class Toolchain(CqStruct, frozen=True):
    rg_available: bool
    rg_version: str | None
    rg_pcre2_available: bool = False
    rg_pcre2_version: str | None = None
    ...
```

```python
# tools/cq/search/rg/runner.py — optional lookaround-aware fallback
_LOOKAROUND_RE = re.compile(r"\(\?[<!=]")

if pcre2_available and _LOOKAROUND_RE.search(pattern):
    command.append("-P")
```

### Files to Edit

- `tools/cq/core/toolchain.py` — Add PCRE2 detection and capability/version fields.
- `tools/cq/search/rg/runner.py` — Accept capability input and guard conditional `-P` usage.
- `tools/cq/search/pipeline/smart_search.py` — Surface PCRE2 capability in summary/telemetry.

### New Files to Create

- None — add capability detection and runner behavior tests in existing files.

### Legacy Decommission/Delete Scope

- None — additive capability.

---

## S17. B7 — Dynamic Metavariable Discovery and Unified Capture Model

### Goal

Replace hardcoded metavariable name lists with parser-driven discovery from pattern/rule text. Use one capture model across scanner and query executor for single (`$X`), unnamed (`$$OP`), and variadic (`$$$ARGS`) metavariables.

### Representative Code Snippets

```python
# tools/cq/query/metavar.py — extract names from pattern/rule strings
_METAVAR_TOKEN_RE = re.compile(r"\${1,3}([A-Z][A-Z0-9_]*)")

def extract_metavar_names(text: str) -> tuple[str, ...]:
    return tuple(sorted({m.group(1) for m in _METAVAR_TOKEN_RE.finditer(text)}))

def extract_rule_metavars(rule: AstGrepRule) -> tuple[str, ...]:
    parts = [rule.pattern]
    for item in (rule.context, rule.inside, rule.has, rule.precedes, rule.follows):
        if item:
            parts.append(item)
    if rule.composite:
        parts.extend(rule.composite.patterns)
    return tuple(sorted({name for part in parts for name in extract_metavar_names(part)}))
```

```python
# tools/cq/query/executor.py — remove _COMMON_METAVAR_NAMES
metavar_names = extract_rule_metavars(rule_ctx.rule)
captures = _extract_match_metavars(
    match,
    metavar_names=metavar_names,
    include_multi=True,
)
```

### Files to Edit

- `tools/cq/query/metavar.py` — Add dynamic metavariable name extraction helpers.
- `tools/cq/query/executor.py` — Replace hardcoded metavariable constants with dynamic extraction.
- `tools/cq/astgrep/sgpy_scanner.py` — Reuse dynamic metavariable names in scanner match serialization.

### New Files to Create

- None — extend existing metavariable parsing and pattern-query tests.

### Legacy Decommission/Delete Scope

- Delete `_COMMON_METAVAR_NAMES` in `tools/cq/query/executor.py`.
- Delete hardcoded `common_names` lists in `tools/cq/astgrep/sgpy_scanner.py`.

---

## S18. D1 — Pattern Planner Completeness (`composite`, `nthChild`)

### Goal

Thread parser-supported `Query.composite` and `Query.nth_child` fields into planner-generated `AstGrepRule` so pattern queries preserve full semantics through execution planning.

### Representative Code Snippets

```python
# tools/cq/query/planner.py — _compile_pattern_query()
rule = AstGrepRule(
    pattern=query.pattern_spec.pattern,
    context=query.pattern_spec.context,
    selector=query.pattern_spec.selector,
    strictness=query.pattern_spec.strictness,
    composite=query.composite,
    nth_child=query.nth_child,
)
```

```python
# tests/unit/cq/test_pattern_queries.py
def test_compile_pattern_query_threads_composite_and_nth_child() -> None:
    ...
    plan = compile_query(query)
    assert plan.sg_rules[0].composite is not None
    assert plan.sg_rules[0].nth_child is not None
```

### Files to Edit

- `tools/cq/query/planner.py` — Add composite/nthChild threading in `_compile_pattern_query()`.
- `tests/unit/cq/test_pattern_queries.py` — Add compile-time parity coverage for composite/nthChild.

### New Files to Create

- None.

### Legacy Decommission/Delete Scope

- Remove planner behavior that silently drops `query.composite` and `query.nth_child`.

---

## S19. D2 — Unified Pattern Runtime Semantics (Findings + Span Filtering)

### Goal

Unify pattern match execution so findings generation and span filtering use the exact same rule execution path. This removes current semantic drift between `_iter_rule_matches()` and `_iter_rule_matches_for_spans()`.

### Representative Code Snippets

```python
# tools/cq/query/executor.py — single execution helper
def _execute_rule_matches(
    node: SgNode,
    rule: AstGrepRule,
    *,
    constraints: dict[str, dict[str, str]] | None = None,
) -> list[SgNode]:
    if rule.requires_inline_rule() or constraints:
        config: Config = {"rule": cast("Rule", rule.to_yaml_dict())}
        if constraints:
            config["constraints"] = constraints
        return list(node.find_all(config=config))
    if rule.kind and rule.pattern in {"$FUNC", "$METHOD", "$CLASS"}:
        return list(node.find_all(kind=rule.kind))
    return list(node.find_all(pattern=rule.pattern))
```

```python
# tools/cq/query/executor.py — reuse in both flows
for match in _execute_rule_matches(rule_ctx.node, rule_ctx.rule, constraints=rule_ctx.constraints):
    ...

for match in _execute_rule_matches(node, rule, constraints=constraints):
    spans.append(_to_match_span(match, rel_path))
```

### Files to Edit

- `tools/cq/query/executor.py` — Replace split match iterators with one shared execution function.
- `tests/unit/cq/test_pattern_queries.py` — Add parity tests ensuring findings and spans match identical rule semantics.

### New Files to Create

- `tests/unit/cq/test_pattern_runtime_parity.py` — Focused regression file for findings-vs-span runtime semantic parity.

### Legacy Decommission/Delete Scope

- Delete `_iter_rule_matches_for_spans()` and duplicate/misaligned execution logic in `tools/cq/query/executor.py`.

---

## S20. C2 — Neighborhood Ripgrep-Lane Consolidation

### Goal

Replace direct subprocess ripgrep calls in neighborhood target resolution with the shared ripgrep runner/contracts path and canonical path helpers from `tools/cq/core/pathing.py`. This consolidates timeout/error handling, language filtering, and path normalization across CQ lanes.

### Representative Code Snippets

```python
# tools/cq/search/rg/adapter.py — reusable symbol probe helper
def find_symbol_candidates(
    root: Path,
    symbol_name: str,
    *,
    lang_scope: QueryLanguageScope,
    limits: SearchLimits,
) -> list[tuple[str, int, str]]:
    ...
```

```python
# tools/cq/neighborhood/target_resolution.py — consume shared rg lane + canonical pathing
from tools.cq.core.pathing import normalize_repo_relative_path

rows = find_symbol_candidates(
    root=root,
    symbol_name=symbol_name,
    lang_scope="rust" if language == "rust" else "python",
    limits=INTERACTIVE,
)

rel_path = normalize_repo_relative_path(root / rows[0][0], root=root)
```

### Files to Edit

- `tools/cq/neighborhood/target_resolution.py` — Remove direct `subprocess.run(["rg", ...])` usage and call shared adapter helper.
- `tools/cq/search/rg/adapter.py` — Add symbol-candidate helper built on shared runner/contracts.
- `tools/cq/core/pathing.py` — Reuse existing normalization helpers; no new neighborhood-specific path normalization utilities.

### New Files to Create

- `tests/unit/cq/neighborhood/test_target_resolution_rg_lane.py` — Coverage for neighborhood symbol fallback through shared rg path.

### Legacy Decommission/Delete Scope

- Delete direct ripgrep command assembly and subprocess execution in `tools/cq/neighborhood/target_resolution.py`.
- Delete neighborhood-local `_normalize_file_path()` logic superseded by shared `core/pathing.py` helpers.

---

## S21. A9 — Ripgrep JSON Fidelity (`absolute_offset`, bytes/text unions, richer stats)

### Goal

Capture and use ripgrep JSON fidelity fields currently dropped in typed decoding (`absolute_offset`, richer summary stats, bytes/text unions). This improves byte-accurate anchoring and summary diagnostics without new scanning passes.

### Representative Code Snippets

```python
# tools/cq/search/rg/codec.py — preserve richer match/summary payload fields
class RgMatchData(msgspec.Struct, omit_defaults=True):
    path: RgPath | None = None
    lines: RgText | None = None
    line_number: int | None = None
    absolute_offset: int | None = None
    submatches: list[RgSubmatch] = msgspec.field(default_factory=list)


class RgSummaryStats(msgspec.Struct, omit_defaults=True):
    searches: int | None = None
    searches_with_match: int | None = None
    matches: int | None = None
    matched_lines: int | None = None
    bytes_searched: int | None = None
    bytes_printed: int | None = None
```

```python
# tools/cq/search/rg/collector.py — use absolute byte offsets when available
absolute_base = data.absolute_offset if isinstance(data.absolute_offset, int) else 0
match_abs_start = absolute_base + start
match_abs_end = absolute_base + end
```

```python
# tools/cq/search/pipeline/smart_search.py — propagate absolute offsets in RawMatch
class RawMatch(CqStruct, frozen=True):
    ...
    match_abs_byte_start: int | None = None
    match_abs_byte_end: int | None = None
```

```python
# tools/cq/search/pipeline/smart_search.py — expose richer stats
summary["rg_stats"] = {
    "matches": stats.total_matches,
    "matched_lines": collector.summary_stats.get("matched_lines", 0),
    "bytes_searched": collector.summary_stats.get("bytes_searched", 0),
    "bytes_printed": collector.summary_stats.get("bytes_printed", 0),
}
```

### Files to Edit

- `tools/cq/search/rg/codec.py` — Add `absolute_offset` and richer summary-stat fields.
- `tools/cq/search/rg/collector.py` — Track and propagate absolute byte offsets where available.
- `tools/cq/search/pipeline/smart_search.py` — Surface richer ripgrep stats in summary output and propagate absolute-byte offsets through `RawMatch`.

### New Files to Create

- None — add JSON fidelity fixtures to existing codec/collector test modules.

### Legacy Decommission/Delete Scope

- Remove assumptions that all byte offsets are line-relative when `absolute_offset` is available.

---

## S22. C4 — Macro Symbol-Target Resolution via Shared Ripgrep Lane

### Goal

Replace Python text scanning in `tools/cq/macros/shared.py:resolve_target_files()` with shared rg-lane symbol discovery for macro target resolution. This improves speed and consistency for macro commands (`scopes`, `bytecode-surface`, related target-based flows) without overlapping tree-sitter semantics.

### Representative Code Snippets

```python
# tools/cq/search/rg/adapter.py — macro-focused symbol definition probe
def find_symbol_definition_files(
    root: Path,
    symbol_name: str,
    *,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    limits: SearchLimits | None = None,
) -> list[Path]:
    escaped = re.escape(symbol_name)
    pattern = rf"(def|class)\s+{escaped}\b"
    rows = find_files_with_pattern(
        root=root,
        pattern=pattern,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        limits=limits,
        lang_scope="python",
    )
    return rows
```

```python
# tools/cq/macros/shared.py — resolve_target_files() integration
files = find_symbol_definition_files(
    root=root,
    symbol_name=target,
    include_globs=include,
    exclude_globs=exclude,
    limits=INTERACTIVE,
)
if files:
    return files[: max(0, int(max_files))]

# Fail-open fallback retained only during migration window.
return _legacy_text_scan_target_files(...)
```

### Files to Edit

- `tools/cq/macros/shared.py` — Replace `read_text()`-based target symbol scanning with shared rg helper + temporary fail-open fallback.
- `tools/cq/search/rg/adapter.py` — Add macro-focused symbol definition probe helper.
- `tools/cq/macros/scopes.py` — Verify behavior parity for `cmd_scopes()` target resolution path.
- `tools/cq/macros/bytecode.py` — Verify behavior parity for `cmd_bytecode_surface()` target resolution path.

### New Files to Create

- None — extend existing `tests/unit/cq/macros/test_shared.py`, `tests/unit/cq/macros/test_target_resolution.py`, and `tests/unit/cq/macros/test_scope_filtering.py` for symbol-resolution parity.

### Legacy Decommission/Delete Scope

- `tools/cq/macros/shared.py` — Delete legacy per-file text scanning loop (`"def {target}"` / `"class {target}"`) after rg-backed path proves parity in macro tests.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S2/A2, S3/A1)

- Delete all manual `\b` wrapping for identifier search patterns in `tools/cq/search/rg/adapter.py` and `tools/cq/search/pipeline/smart_search.py` — superseded by centralized `-w` handling.

### Batch D2 (after S14/B2, S15/B3)

- Remove direct imports of `tools/cq/astgrep/rules_py.py` and `tools/cq/astgrep/rules_rust.py` from runtime callsites and tests (`tests/unit/cq/test_rules_py.py`, `tests/unit/cq/test_astgrep_rules.py`, `tests/unit/cq/test_sgpy_scanner.py`), switching to loader-backed dispatch paths.
- Delete `tools/cq/astgrep/rules_py.py` and `tools/cq/astgrep/rules_rust.py` only after the import-migration step above and parity tests are both complete.

### Batch D3 (after S6/B5, S8/B4, S11/B6, S19/D2)

- Remove only the migrated Phase 1 parent-chain helpers and metadata-only pattern filtering paths superseded by refinement predicates, constraint pushdown, and unified inline-rule execution. Defer wider extractor cleanup until follow-on parity validation lands.

### Batch D4 (after S17/B7, S19/D2)

- Delete hardcoded metavariable-name constants in scanner/executor once dynamic metavariable extraction is the only path.

### Batch D5 (after S20/C2, S21/A9)

- Remove neighborhood-specific ripgrep subprocess wrappers and ad-hoc output parsing now replaced by shared rg lane contracts.
- Remove neighborhood-local `_normalize_file_path()` in favor of `tools/cq/core/pathing.py` helpers.

### Batch D6 (after S22/C4)

- Delete macro-local per-file text scanning loop in `tools/cq/macros/shared.py:resolve_target_files()` once rg-backed symbol target resolution parity is confirmed.

---

## Implementation Sequence

1. **S18/D1 — Planner completeness (`composite`, `nthChild`)** (Correctness prerequisite for downstream runtime work)
2. **S19/D2 — Unified pattern runtime semantics** (Eliminates semantic drift between findings and span filtering)
3. **S17/B7 — Dynamic metavariable discovery** (Unblocks robust capture + filter parity)
4. **S8/B4 — Constraint pushdown** (Moves positive metavariable filtering into native engine)
5. **S11/B6 — Pattern object runtime threading** (Ensures context/selector/strictness are executed)
6. **S4/B1 — Variadic captures** (Completes multi-capture fidelity after dynamic metavar threading)
7. **S0/A0 — Unified rg operation modes + scoped paths** (Required foundation for S1/S7/S10)
8. **S5/A3 — Context line support** (Foundation for richer ripgrep event handling)
9. **S12/C3 — Begin/end event handling** (Accurate file accounting and binary detection)
10. **S21/A9 — JSON fidelity fields** (Absolute offsets + richer stats + `RawMatch` propagation)
11. **S9/A8 — Multiline mode** (Cross-line text search mode)
12. **S2/A2 — Word-boundary standardization** (Centralized identifier semantics)
13. **S3/A1 — Multi-pattern OR** (Contract extension for batched probes)
14. **S7/A4 — Count probes** (Adaptive preflight optimization through shared rg lane)
15. **S10/A5 — File-set enumeration** (Debug and diagnostics utility through shared rg lane)
16. **S20/C2 — Neighborhood rg-lane consolidation** (Shared execution stack + canonical pathing)
17. **S1/C1 — Ripgrep prefiltering for ast-grep scans** (Apply after rg lane unification and multi-pattern support)
18. **S22/C4 — Macro symbol-target resolution** (Reuse shared rg lane for macro target discovery)
19. **S6/B5 — Refinement predicate migration** (Phased cleanup/perf improvement path)
20. **S13/A6 — Deterministic sort mode** (CI determinism toggle)
21. **S14/B2 — YAML rule pack loader** (Schema bridge + migration staging)
22. **S15/B3 — Shared utility rule packs** (Deduplicate rule fragments after loader landing)
23. **S16/A7 — PCRE2 capability detection** (Capability telemetry and conditional advanced regex support)

**Rationale:** correctness-path scopes (D/B execution fidelity) land first, then rg-lane contract foundations, then event/model fidelity and optimization scopes, then rulepack migration and utility-rule consolidation with staged decommissioning.

---

## Implementation Checklist

- [ ] S0/A0 — Unified ripgrep operation modes + scoped path execution contracts
- [ ] S1/C1 — Ripgrep-accelerated prefiltering for ast-grep batch scans
- [ ] S2/A2 — Word-boundary mode (`-w`) standardization
- [ ] S3/A1 — Multi-pattern OR search (`-e P1 -e P2`)
- [ ] S4/B1 — Variadic metavariable captures via `get_multiple_matches()`
- [ ] S5/A3 — Context lines (`-C`/`-A`/`-B`) in rg lane
- [ ] S6/B5 — Refinement predicate migration (`matches`/`inside`/`has`)
- [ ] S7/A4 — `--count` / `--count-matches` preflight probes
- [ ] S8/B4 — Constraint pushdown for metavar filtering
- [ ] S9/A8 — Multiline ripgrep mode (`-U --multiline-dotall`)
- [ ] S10/A5 — `--files` candidate-set enumeration
- [ ] S11/B6 — Pattern object runtime threading (`context` + `selector`)
- [ ] S12/C3 — Ripgrep `begin`/`end` event handling
- [ ] S13/A6 — `--sort path` deterministic ordering
- [ ] S14/B2 — YAML-authoritative rule pack loading
- [ ] S15/B3 — Shared utility rules (`utils` + `matches`)
- [ ] S16/A7 — PCRE2 capability detection and conditional lookaround support
- [ ] S17/B7 — Dynamic metavariable discovery + unified capture model
- [ ] S18/D1 — Planner threading parity for `composite`/`nthChild`
- [ ] S19/D2 — Unified pattern runtime semantics across findings/spans
- [ ] S20/C2 — Neighborhood ripgrep-lane consolidation
- [ ] S21/A9 — Ripgrep JSON fidelity (`absolute_offset`, bytes/text unions, richer stats)
- [ ] S22/C4 — Macro symbol-target resolution via shared ripgrep lane
- [ ] D1 — Decommission manual identifier `\b` wrapping (after S2, S3)
- [ ] D2 — Migrate imports off Python rule modules and then decommission compatibility modules (after S14, S15)
- [ ] D3 — Decommission Python-side parent-chain/metadata filtering (after S6, S8, S11, S19)
- [ ] D4 — Decommission hardcoded metavariable-name lists (after S17, S19)
- [ ] D5 — Decommission neighborhood-specific rg subprocess wrappers (after S20, S21)
- [ ] D6 — Decommission macro-local text scanning target resolution (after S22)
