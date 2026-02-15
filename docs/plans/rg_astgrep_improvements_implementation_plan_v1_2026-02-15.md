# Ripgrep & ast-grep-py Improvements Implementation Plan v1 (2026-02-15)

## Scope Summary

This plan covers 16 scope items organized into three groups that elevate CQ's ripgrep and ast-grep-py utilization to best-in-class:

- **Group A (7 items):** Ripgrep improvements — multi-pattern OR, word-boundary mode, context lines, count probes, file enumeration, deterministic sort, PCRE2 detection.
- **Group B (6 items):** ast-grep-py improvements — variadic captures, YAML rule packs, shared utility rules, constraint-driven filtering, refinement predicates, pattern objects.
- **Group C (3 items):** Combined/new capabilities — ripgrep-accelerated prefiltering for ast-grep, multiline text search, begin/end file events.

**Design stance:** Hard additions — no compatibility shims. New fields use defaults so existing callers are unaffected. No tree-sitter scope overlap: ripgrep handles text-level candidate generation, ast-grep handles structural classification, tree-sitter owns semantic enrichment.

**Priority order:** C1, A2, A1, B1, A3, B5, A4, B4, C2, A5, B6, C3, A6, B2, B3, A7.

## Design Principles

1. **No tree-sitter duplication** — Ripgrep and ast-grep improvements operate strictly in their existing layers (text candidate generation and structural classification). Tree-sitter retains exclusive ownership of incremental parsing, query-pack semantic extraction, byte-window queries, structural neighborhoods, and gap-fill enrichment.
2. **Additive defaults** — All new fields on `SearchLimits`, `RgRunRequest`, `RgRunSettingsV1`, and `Toolchain` use default values that preserve current behavior. No existing callers break.
3. **msgspec contracts at boundaries** — All new serializable types use `msgspec.Struct` base classes (`CqStruct`, `CqSettingsStruct`, `CqOutputStruct`). Runtime-only handles stay as `dataclass` or plain objects.
4. **Test parity** — Every new module or function gets a corresponding test file. Existing test files gain parametrized cases for new behavior.
5. **Minimal Python post-processing** — Push filtering, matching, and ordering into ripgrep/ast-grep native engines wherever the API supports it.

## Current Baseline

- **`tools/cq/search/rg/runner.py`** (236 LOC): `build_rg_command()` emits 11 flags (`--json`, `--line-number`, `--column`, `--max-count`, `--max-depth`, `--max-filesize`, `--type`, `-g`, `-F`, `-e`). Single pattern only: `command.extend(["-e", pattern, "."])` at line 98. No `-w`, `-U`, `-P`, `-C/-A/-B`, `--count`, `--files`, `--sort`.
- **`tools/cq/search/rg/codec.py`** (222 LOC): Typed decoder handles `RgMatchEvent` and `RgSummaryEvent` only. `RgEvent` fallback captures unknown types but discards them. No `context`, `begin`, or `end` event types.
- **`tools/cq/search/rg/collector.py`** (211 LOC): `RgCollector.handle_event()` dispatches on `type == "match"` and `type == "summary"`, ignores all others. No context line storage.
- **`tools/cq/search/rg/adapter.py`** (244 LOC): `find_call_candidates()` manually builds `rf"\b{symbol}\s*\("` — should use `-w` instead. `find_def_lines()` reads files with Python instead of ripgrep.
- **`tools/cq/search/rg/contracts.py`** (65 LOC): `RgRunSettingsV1` has `pattern: str` (single), `mode: str`, `lang_types`, globs. No multi-pattern or context fields.
- **`tools/cq/search/_shared/core.py`** (384 LOC): `RgRunRequest` has `pattern: str` (single). No multi-pattern, context, sort, or multiline fields.
- **`tools/cq/search/pipeline/profiles.py`** (67 LOC): `SearchLimits` has 6 fields with 4 presets (`DEFAULT`, `INTERACTIVE`, `AUDIT`, `LITERAL`). No `context_before`, `context_after`, `sort_by_path`, or `multiline` fields.
- **`tools/cq/search/pipeline/classifier.py`**: `QueryMode` enum has 3 values: `IDENTIFIER`, `REGEX`, `LITERAL`. No `MULTILINE` mode.
- **`tools/cq/astgrep/sgpy_scanner.py`** (533 LOC): `scan_with_pattern()` accepts only `pattern: str` — no pattern objects, no constraints. `_extract_metavars()` iterates 21 hardcoded names calling `get_match()` only — never calls `get_multiple_matches()`.
- **`tools/cq/astgrep/rules_py.py`**: 23 Python rules as `RuleSpec` objects.
- **`tools/cq/astgrep/rules_rust.py`**: 8 Rust rules as `RuleSpec` objects.
- **`tools/cq/core/toolchain.py`** (148 LOC): `Toolchain` has no PCRE2 detection field. `_detect_rg()` only runs `rg --version`.
- **`tools/cq/query/ir.py`**: `PatternSpec` already has `context: str | None` and `selector: str | None` fields.
- **`tools/cq/query/parser.py`**: `_parse_pattern_object()` parses `pattern.context` and `pattern.selector` from tokens into `PatternSpec`. Context/selector are stored in metadata but NOT passed to ast-grep-py Config execution.
- **`tools/cq/search/pipeline/classifier_runtime.py`**: `_is_docstring_context()` walks parent chain in Python — candidate for `node.inside()` replacement.
- **`tools/cq/search/python/extractors.py`**: 25+ `.parent()`/`.kind()` chains for context classification — candidates for refinement predicate replacement.

---

## S1. C1 — Ripgrep-Accelerated File Prefiltering for ast-grep Batch Scans

### Goal

Eliminate 90%+ of unnecessary file reads and AST parses in `scan_files()` by using ripgrep `--files-with-matches` (`-l`) as a fast text-level prefilter before ast-grep structural scanning. Extract literal substrings from ast-grep patterns to construct the ripgrep prefilter query automatically.

### Representative Code Snippets

```python
# tools/cq/search/rg/prefilter.py
"""Ripgrep-accelerated file prefiltering for ast-grep batch scans."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.query.language import QueryLanguage, QueryLanguageScope


# Metavariable pattern: $X, $$X, $$$X
_METAVAR_RE = re.compile(r"\${1,3}[A-Z_][A-Z0-9_]*")


def extract_literal_fragments(pattern: str) -> list[str]:
    """Extract longest non-metavariable substrings from an ast-grep pattern.

    Parameters
    ----------
    pattern
        ast-grep pattern string (e.g., ``eval($X)`` or ``def $F($$$ARGS): $$$``).

    Returns
    -------
    list[str]
        Non-empty literal fragments sorted by length descending.
    """
    fragments = _METAVAR_RE.split(pattern)
    cleaned = [f.strip() for f in fragments if f.strip()]
    return sorted(cleaned, key=len, reverse=True)


def rg_prefilter(
    root: Path,
    pattern: str,
    *,
    files: list[Path],
    lang_types: tuple[str, ...] = (),
    timeout_seconds: float = 10.0,
) -> list[Path]:
    """Return only files containing literal fragments from an ast-grep pattern.

    Parameters
    ----------
    root
        Repository root.
    pattern
        ast-grep pattern from which literal fragments are extracted.
    files
        Candidate file list to filter.
    lang_types
        Ripgrep type filters.
    timeout_seconds
        Subprocess timeout.

    Returns
    -------
    list[Path]
        Subset of ``files`` containing at least one literal fragment.
    """
    fragments = extract_literal_fragments(pattern)
    if not fragments:
        return files  # No literal content — cannot prefilter

    # Use longest fragment for best selectivity
    literal = fragments[0]
    if len(literal) < 3:
        return files  # Too short to be selective

    command = ["rg", "--files-with-matches", "-F", "-l", "--no-heading"]
    for lt in lang_types:
        command.extend(["--type", lt])
    command.extend(["-e", literal, "."])

    proc = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds,
    )
    if proc.returncode not in (0, 1):
        return files  # Fail open

    matching_files = {
        (root / line.strip()).resolve()
        for line in proc.stdout.splitlines()
        if line.strip()
    }
    if not matching_files:
        return []

    file_set = {f.resolve() for f in files}
    return sorted(file_set & matching_files)
```

```python
# tools/cq/astgrep/sgpy_scanner.py — Integration point in scan_files()
# After file list construction, before the scan loop:

from tools.cq.search.rg.prefilter import rg_prefilter

def scan_files(
    files: list[Path],
    rules: tuple[RuleSpec, ...],
    root: Path,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
    *,
    prefilter: bool = True,
) -> list[SgRecord]:
    if prefilter and rules:
        # Extract representative pattern from first rule with a pattern key
        representative = _extract_representative_pattern(rules)
        if representative:
            files = rg_prefilter(root, representative, files=files)
    # ... existing scan loop ...
```

### Files to Edit

- `tools/cq/astgrep/sgpy_scanner.py` — Add `prefilter` parameter to `scan_files()`, call `rg_prefilter()` before scanning.
- `tools/cq/search/pipeline/smart_search.py` — Pass prefilter option through to ast-grep scan calls.

### New Files to Create

- `tools/cq/search/rg/prefilter.py` — `extract_literal_fragments()`, `rg_prefilter()`.
- `tests/unit/cq/search/rg/test_prefilter.py` — Tests for literal extraction and prefilter logic.

### Legacy Decommission/Delete Scope

- None — this is purely additive.

---

## S2. A2 — Word-Boundary Mode (`-w/--word-regexp`) for Identifier Searches

### Goal

When `QueryMode.IDENTIFIER` is active, pass ripgrep's native `-w` flag instead of relying on manual `\b` wrapping in callers. This produces more correct word-boundary matching that handles Unicode, punctuation, and edge cases better than manual regex construction.

### Representative Code Snippets

```python
# tools/cq/search/rg/runner.py — build_rg_command() addition
# After the literal-mode check and before the pattern extension:

    if mode.value == "literal":
        command.append("-F")
    elif mode.value == "identifier":
        command.append("-w")
    command.extend(["-e", pattern, "."])
```

```python
# tools/cq/search/rg/adapter.py — find_call_candidates() simplification
# Before (line 130):
#     symbol = function_name.rsplit(".", maxsplit=1)[-1]
#     pattern = rf"\b{symbol}\s*\("
# After:
    symbol = function_name.rsplit(".", maxsplit=1)[-1]
    pattern = rf"{symbol}\s*\("
    # Word boundary is handled by -w flag via QueryMode.IDENTIFIER
```

### Files to Edit

- `tools/cq/search/rg/runner.py` — Add `-w` flag when `mode == IDENTIFIER` in `build_rg_command()`.
- `tools/cq/search/rg/adapter.py` — Remove manual `\b` wrapping from `find_call_candidates()` pattern construction; use `QueryMode.IDENTIFIER` to let `-w` handle it.

### New Files to Create

- None — existing test files gain new parametrized cases.

### Legacy Decommission/Delete Scope

- `tools/cq/search/rg/adapter.py:130` — Remove manual `\b` prefix from pattern `rf"\b{symbol}\s*\("`. The `\b` becomes redundant with `-w`.

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

Extend `_extract_metavars()` in `sgpy_scanner.py` to call `get_multiple_matches()` for `$$$`-prefixed variadic metavariable names, enabling individual argument-level captures. Currently, patterns like `print($$$ARGS)` lose individual argument captures because only `get_match()` is called.

### Representative Code Snippets

```python
# tools/cq/astgrep/sgpy_scanner.py — _extract_metavars() extension

# Known variadic metavariable names ($$$ prefix captures)
_VARIADIC_NAMES = frozenset({"ARGS", "KWARGS", "PARAMS", "BODY", "ITEMS"})


def _extract_metavars(match: SgNode) -> dict[str, dict[str, Any]]:
    """Extract metavariable captures from a match."""
    metavars: dict[str, dict[str, Any]] = {}

    for name in common_names:
        # Try single capture first
        captured = match.get_match(name)
        if captured is not None:
            range_obj = captured.range()
            payload = {
                "text": captured.text(),
                "start": {"line": range_obj.start.line, "column": range_obj.start.column},
                "end": {"line": range_obj.end.line, "column": range_obj.end.column},
            }
            metavars[name] = payload
            metavars[f"${name}"] = payload

        # Try variadic multi-capture for known variadic names
        if name in _VARIADIC_NAMES:
            multi = match.get_multiple_matches(name)
            if multi:
                items = []
                for node in multi:
                    r = node.range()
                    items.append({
                        "text": node.text(),
                        "start": {"line": r.start.line, "column": r.start.column},
                        "end": {"line": r.end.line, "column": r.end.column},
                    })
                metavars[f"${name}_items"] = {"items": items, "count": len(items)}

    return metavars
```

### Files to Edit

- `tools/cq/astgrep/sgpy_scanner.py` — Extend `_extract_metavars()` to call `get_multiple_matches()` for variadic names. Add `_VARIADIC_NAMES` constant.

### New Files to Create

- None — existing test file `tests/unit/cq/test_sgpy_scanner.py` gains new cases.

### Legacy Decommission/Delete Scope

- None — additive extension to existing function.

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

Replace Python-side parent-chain walking (`.parent()`, `.kind() ==` patterns) in classifier and extractor code with ast-grep-py's native refinement predicates (`matches()`, `inside()`, `has()`). These run in Rust and are more efficient than Python-side filtering.

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
# tools/cq/search/python/extractors.py — Replace parent-chain context checks
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
# tools/cq/search/pipeline/classifier_runtime.py — Use has() for child checks
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

- `tools/cq/search/pipeline/classifier_runtime.py` — Replace `_is_docstring_context()` and similar parent-chain checks with `inside()` predicates. Replace child iteration checks with `has()`.
- `tools/cq/search/python/extractors.py` — Replace `.parent()`/`.kind()` context classification chains with refinement predicates.

### New Files to Create

- None — tests for existing files cover the refactored logic.

### Legacy Decommission/Delete Scope

- `tools/cq/search/pipeline/classifier_runtime.py` — Remove manual parent-chain walking code in `_is_docstring_context()` and similar helpers.
- `tools/cq/search/python/extractors.py` — Remove explicit `.parent()` / `.kind()` chain checks for context classification (approximately 25+ patterns across the file).

---

## S7. A4 — `--count` / `--count-matches` for Fast Cardinality Probes

### Goal

Add a lightweight `run_rg_count()` function that uses `--count` mode for fast cardinality probes without paying full JSON decode cost. Enables adaptive profile selection in smart search pre-flight: if a count query returns many results, switch to a more restrictive profile before the full search.

### Representative Code Snippets

```python
# tools/cq/search/rg/runner.py — New count function

def run_rg_count(
    *,
    root: Path,
    pattern: str,
    mode: QueryMode,
    lang_types: tuple[str, ...],
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    timeout_seconds: float = 5.0,
) -> dict[str, int]:
    """Run ``rg --count`` and return per-file match counts.

    Parameters
    ----------
    root
        Search root directory.
    pattern
        Search pattern.
    mode
        Query mode for flag selection.
    lang_types
        Ripgrep type filters.
    include_globs
        Include glob patterns.
    exclude_globs
        Exclude glob patterns.
    timeout_seconds
        Subprocess timeout.

    Returns
    -------
    dict[str, int]
        Mapping of relative file path to match count.
    """
    command = ["rg", "--count", "--no-heading"]
    for lt in lang_types:
        command.extend(["--type", lt])
    for g in (include_globs or []):
        command.extend(["-g", g])
    for g in (exclude_globs or []):
        normalized = g[1:] if g.startswith("!") else g
        command.extend(["-g", f"!{normalized}"])
    if mode.value == "literal":
        command.append("-F")
    elif mode.value == "identifier":
        command.append("-w")
    command.extend(["-e", pattern, "."])

    proc = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds,
    )
    counts: dict[str, int] = {}
    if proc.returncode in (0, 1):
        for line in proc.stdout.splitlines():
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

- `tools/cq/search/rg/runner.py` — Add `run_rg_count()` function; update `__all__`.
- `tools/cq/search/pipeline/smart_search.py` — Use `run_rg_count()` in pre-flight to select adaptive profile.

### New Files to Create

- None — existing test file `tests/unit/cq/search/rg/test_runner.py` gains count-mode cases.

### Legacy Decommission/Delete Scope

- None — additive function.

---

## S8. B4 — Constraint-Driven Metavariable Filtering for Pattern Queries

### Goal

Extend `scan_with_pattern()` to accept optional constraint dicts and pass them to ast-grep-py's Config-based search. This pushes filtering into the native Rust engine instead of Python post-processing. Currently, constraints are used in exactly one place (`find_import_aliases()`).

### Representative Code Snippets

```python
# tools/cq/astgrep/sgpy_scanner.py — scan_with_pattern() extension

def scan_with_pattern(
    files: list[Path],
    pattern: str,
    root: Path,
    rule_id: str = "pattern_query",
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
    *,
    constraints: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Scan files with a pattern and optional metavariable constraints.

    Parameters
    ----------
    constraints
        Optional metavariable constraints, e.g.,
        ``{"TYPE": {"regex": "^(int|str|float)$"}}`` constrains ``$TYPE``
        to match only those type names.
    """
    matches: list[dict[str, Any]] = []

    for file_path in files:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        sg_root = SgRoot(src, lang)
        node = sg_root.root()

        if constraints:
            # Build Config with constraints for native Rust filtering
            config: Config = {
                "rule": {"pattern": pattern},
                "constraints": constraints,
            }
            file_matches = node.find_all(config=config)
        else:
            file_matches = node.find_all(pattern=pattern)

        for match in file_matches:
            match_dict = _node_to_match_dict(match, file_path, root, rule_id)
            matches.append(match_dict)

    return matches
```

### Files to Edit

- `tools/cq/astgrep/sgpy_scanner.py` — Add `constraints` parameter to `scan_with_pattern()`, build Config with constraints when provided.
- `tools/cq/query/executor.py` — Thread constraint dicts from query IR through to `scan_with_pattern()` calls.

### New Files to Create

- None — existing test file gains constraint parametrized cases.

### Legacy Decommission/Delete Scope

- None — existing Python-side post-filtering for constraints in callers can be removed as callers migrate.

---

## S9. C2 — Multiline Ripgrep (`-U`) for Cross-Line Pattern Matching

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

Add a `list_candidate_files()` function using ripgrep's `--files` mode to enumerate exactly which files would be searched given current ignore/glob/type configuration. Useful for debugging "why didn't rg find X?" and validating scope configuration.

### Representative Code Snippets

```python
# tools/cq/search/rg/adapter.py — New file enumeration function

def list_candidate_files(
    root: Path,
    *,
    lang_types: tuple[str, ...] = (),
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    timeout_seconds: float = 10.0,
) -> list[Path]:
    """List files that ripgrep would search given current configuration.

    Uses ``rg --files`` mode for exact file-set enumeration matching
    ripgrep's ignore, glob, and type filtering.

    Parameters
    ----------
    root
        Search root directory.
    lang_types
        Ripgrep type filters.
    include_globs
        Include glob patterns.
    exclude_globs
        Exclude glob patterns.
    timeout_seconds
        Subprocess timeout.

    Returns
    -------
    list[Path]
        Sorted list of absolute file paths.
    """
    command = ["rg", "--files"]
    for lt in lang_types:
        command.extend(["--type", lt])
    for g in (include_globs or []):
        command.extend(["-g", g])
    for g in (exclude_globs or []):
        normalized = g[1:] if g.startswith("!") else g
        command.extend(["-g", f"!{normalized}"])
    command.append(".")

    proc = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds,
    )
    if proc.returncode not in (0, 1):
        return []
    return sorted(
        (root / line.strip()).resolve()
        for line in proc.stdout.splitlines()
        if line.strip()
    )
```

### Files to Edit

- `tools/cq/search/rg/adapter.py` — Add `list_candidate_files()` function; update `__all__`.

### New Files to Create

- None — existing test file `tests/unit/cq/search/rg/test_runner.py` or a new `test_adapter.py` gains file enumeration cases.

### Legacy Decommission/Delete Scope

- None — additive function.

---

## S11. B6 — Pattern Object Syntax (`context` + `selector`) Threading

### Goal

Thread the already-parsed `pattern.context` and `pattern.selector` from `PatternSpec` through to ast-grep-py's Config-based search execution. Currently, the query parser creates `PatternSpec` with these fields populated, but the executor only stores them as metadata — they are never passed to ast-grep-py.

### Representative Code Snippets

```python
# tools/cq/astgrep/sgpy_scanner.py — scan_with_pattern() pattern object support

def scan_with_pattern(
    files: list[Path],
    pattern: str,
    root: Path,
    rule_id: str = "pattern_query",
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
    *,
    constraints: dict[str, dict[str, str]] | None = None,
    context: str | None = None,
    selector: str | None = None,
) -> list[dict[str, Any]]:
    """Scan files with a pattern, optional constraints, and pattern object fields.

    Parameters
    ----------
    context
        Pattern context for disambiguation (e.g., ``class Foo:\\n  $X``).
    selector
        Node kind selector for disambiguation (e.g., ``expression_statement``).
    """
    matches: list[dict[str, Any]] = []

    for file_path in files:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        sg_root = SgRoot(src, lang)
        node = sg_root.root()

        if context or selector:
            # Build pattern object for Config-based search
            pattern_obj: dict[str, str] = {}
            if context:
                pattern_obj["context"] = context
                pattern_obj["selector"] = selector or pattern
            else:
                pattern_obj["selector"] = selector
                pattern_obj["context"] = pattern
            config: Config = {"rule": {"pattern": pattern_obj}}
            if constraints:
                config["constraints"] = constraints
            file_matches = node.find_all(config=config)
        elif constraints:
            config = {"rule": {"pattern": pattern}, "constraints": constraints}
            file_matches = node.find_all(config=config)
        else:
            file_matches = node.find_all(pattern=pattern)

        for match in file_matches:
            match_dict = _node_to_match_dict(match, file_path, root, rule_id)
            matches.append(match_dict)

    return matches
```

```python
# tools/cq/query/executor.py — Thread PatternSpec fields to scan_with_pattern()
# Where scan_with_pattern is called with pattern_spec:
    results = scan_with_pattern(
        files=candidate_files,
        pattern=query.pattern_spec.pattern,
        root=root,
        rule_id=f"pq_{query.pattern_spec.pattern[:40]}",
        lang=lang,
        context=query.pattern_spec.context,
        selector=query.pattern_spec.selector,
    )
```

### Files to Edit

- `tools/cq/astgrep/sgpy_scanner.py` — Add `context` and `selector` parameters to `scan_with_pattern()`, construct pattern objects for Config-based search.
- `tools/cq/query/executor.py` — Pass `pattern_spec.context` and `pattern_spec.selector` to `scan_with_pattern()`.

### New Files to Create

- None — existing tests gain pattern object cases.

### Legacy Decommission/Delete Scope

- `tools/cq/query/executor.py` — Remove metadata-only storage of `pattern_spec.context`/`pattern_spec.selector` (lines that store them in common metadata dict without threading to ast-grep).

---

## S12. C3 — Ripgrep `begin`/`end` Events for File-Level Metadata

### Goal

Handle ripgrep's `type: "begin"` and `type: "end"` JSON events to collect file-level metadata without additional syscalls. Provides accurate file count even when no matches are found, early binary file detection, and per-file timing.

### Representative Code Snippets

```python
# tools/cq/search/rg/codec.py — New begin/end event types

class RgBeginData(msgspec.Struct, omit_defaults=True):
    """Typed ripgrep begin event data payload."""

    path: RgPath | None = None


class RgEndData(msgspec.Struct, omit_defaults=True):
    """Typed ripgrep end event data payload."""

    path: RgPath | None = None
    binary_offset: int | None = None
    stats: RgSummaryStats | None = None


class RgBeginEvent(msgspec.Struct, frozen=True, tag_field="type", tag="begin"):
    """Typed ripgrep begin event."""

    data: RgBeginData

    @property
    def type(self) -> Literal["begin"]:
        """Return tagged event type."""
        return "begin"


class RgEndEvent(msgspec.Struct, frozen=True, tag_field="type", tag="end"):
    """Typed ripgrep end event."""

    data: RgEndData

    @property
    def type(self) -> Literal["end"]:
        """Return tagged event type."""
        return "end"


# Updated unions:
type RgTypedEvent = RgMatchEvent | RgSummaryEvent | RgContextEvent | RgBeginEvent | RgEndEvent
type RgAnyEvent = RgMatchEvent | RgSummaryEvent | RgContextEvent | RgBeginEvent | RgEndEvent | RgEvent
```

```python
# tools/cq/search/rg/collector.py — File tracking in RgCollector
@dataclass
class RgCollector:
    # ... existing fields ...
    file_count: int = 0
    binary_files: list[str] = field(default_factory=list)

    def handle_event(self, event: RgAnyEvent) -> None:
        if event.type == "begin":
            self._handle_begin(event)
            return
        if event.type == "end":
            self._handle_end(event)
            return
        # ... existing dispatch ...

    def _handle_begin(self, event: RgAnyEvent) -> None:
        if isinstance(event, RgBeginEvent) and event.data.path:
            self.file_count += 1

    def _handle_end(self, event: RgAnyEvent) -> None:
        if isinstance(event, RgEndEvent) and event.data.binary_offset is not None:
            path = event.data.path.text if event.data.path else None
            if path:
                self.binary_files.append(path)
```

### Files to Edit

- `tools/cq/search/rg/codec.py` — Add `RgBeginData`, `RgEndData`, `RgBeginEvent`, `RgEndEvent` types; update union types and decoder.
- `tools/cq/search/rg/collector.py` — Add `file_count`, `binary_files` fields; add `_handle_begin()`, `_handle_end()` methods; update `handle_event()`.

### New Files to Create

- None — existing test files gain begin/end event cases.

### Legacy Decommission/Delete Scope

- None — additive with backward-compatible defaults.

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

Externalize the 23 Python rules and 8 Rust rules from hardcoded `RuleSpec` objects in Python files to version-controlled YAML files. Enables non-developer authoring, swappable rule packs, and clean evolution. Migration path: generate YAML from existing Python rules, validate round-trip, then switch loader.

### Representative Code Snippets

```yaml
# tools/cq/astgrep/rulepacks/python.yaml
version: "1"
defaults:
  language: python
  overlap_policy: first
rules:
  - id: py_def_function
    record_type: def
    kind: function
    config:
      rule:
        kind: function_definition

  - id: py_def_class
    record_type: def
    kind: class
    config:
      rule:
        kind: class_definition

  - id: py_call_name
    record_type: call
    kind: name_call
    config:
      rule:
        pattern: "$F($$$)"
        kind: call
```

```python
# tools/cq/astgrep/rulepack_loader.py
"""YAML rule pack loader for ast-grep-py rules."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.astgrep.sgpy_scanner import RuleSpec
from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from collections.abc import Sequence


class RulePackEntry(CqStruct, frozen=True):
    """Single rule entry in a YAML rule pack."""

    id: str
    record_type: str
    kind: str
    config: dict[str, object] = msgspec.field(default_factory=dict)


class RulePack(CqStruct, frozen=True):
    """Parsed YAML rule pack."""

    version: str
    defaults: dict[str, str] = msgspec.field(default_factory=dict)
    rules: tuple[RulePackEntry, ...] = ()


_YAML_DECODER = msgspec.yaml.Decoder(type=RulePack)


def load_rulepack(path: Path) -> tuple[RuleSpec, ...]:
    """Load a YAML rule pack file and convert to RuleSpec tuple.

    Parameters
    ----------
    path
        Path to the YAML rule pack file.

    Returns
    -------
    tuple[RuleSpec, ...]
        Tuple of RuleSpec objects loaded from the YAML pack.

    Raises
    ------
    FileNotFoundError
        If the rule pack file does not exist.
    """
    raw = path.read_bytes()
    pack = _YAML_DECODER.decode(raw)
    return tuple(
        RuleSpec(
            rule_id=entry.id,
            record_type=entry.record_type,
            kind=entry.kind,
            config=dict(entry.config),
        )
        for entry in pack.rules
    )


def load_default_rulepacks() -> dict[str, tuple[RuleSpec, ...]]:
    """Load all built-in rule packs from the rulepacks directory.

    Returns
    -------
    dict[str, tuple[RuleSpec, ...]]
        Mapping of language name to rule tuple.
    """
    pack_dir = Path(__file__).parent / "rulepacks"
    packs: dict[str, tuple[RuleSpec, ...]] = {}
    if pack_dir.is_dir():
        for yaml_file in sorted(pack_dir.glob("*.yaml")):
            lang = yaml_file.stem
            packs[lang] = load_rulepack(yaml_file)
    return packs
```

### Files to Edit

- `tools/cq/astgrep/sgpy_scanner.py` — Add optional `rulepack_path` parameter to `scan_files()`, fallback to Python rules when YAML not available.

### New Files to Create

- `tools/cq/astgrep/rulepack_loader.py` — YAML rule pack loader.
- `tools/cq/astgrep/rulepacks/python.yaml` — Externalized Python rules (23 rules).
- `tools/cq/astgrep/rulepacks/rust.yaml` — Externalized Rust rules (8 rules).
- `tests/unit/cq/astgrep/test_rulepack_loader.py` — Round-trip tests, validation tests.

### Legacy Decommission/Delete Scope

- `tools/cq/astgrep/rules_py.py` — Entire file becomes legacy once YAML loader is validated and active. Keep as fallback during migration, delete after validation.
- `tools/cq/astgrep/rules_rust.py` — Same as above.

---

## S15. B3 — Shared Utility Rules via `utils` + `matches` in Config

### Goal

Define reusable sub-matcher rules (e.g., `is-literal`, `is-builtin-call`, `is-dunder-method`) in the `utils` section of ast-grep-py Config and reference them via `matches` in complex rules. This reduces duplication across rules and improves classification consistency.

### Representative Code Snippets

```yaml
# tools/cq/astgrep/rulepacks/python.yaml — Shared utils section
version: "1"
defaults:
  language: python
utils:
  is-literal:
    any:
      - kind: string
      - kind: integer
      - kind: float
      - kind: true
      - kind: false
      - kind: none

  is-builtin-call:
    pattern: "$F($$$)"
    constraints:
      F:
        regex: "^(print|len|type|isinstance|hasattr|getattr|setattr|range|enumerate|zip|map|filter|sorted|reversed|list|dict|set|tuple|str|int|float|bool)$"

  is-dunder-method:
    pattern: "def $M($$$):"
    constraints:
      M:
        regex: "^__[a-z]+__$"

  is-decorator:
    kind: decorator

rules:
  - id: py_call_builtin
    record_type: call
    kind: builtin_call
    config:
      rule:
        matches: is-builtin-call

  - id: py_literal_assignment
    record_type: assign_ctor
    kind: literal_assign
    config:
      rule:
        kind: assignment
        has:
          matches: is-literal
```

```python
# tools/cq/astgrep/rulepack_loader.py — Utils handling
# The existing loader passes the full YAML config (including utils)
# through to ast-grep-py Config, which handles utils natively.
# No special Python-side handling needed — ast-grep resolves
# `matches` references against the `utils` section internally.
```

### Files to Edit

- `tools/cq/astgrep/rulepack_loader.py` — Ensure `utils` section from YAML is passed through to Config objects.
- `tools/cq/astgrep/rulepacks/python.yaml` — Add `utils` section with shared matchers; update rules to use `matches`.

### New Files to Create

- None — integrated into B2 infrastructure.

### Legacy Decommission/Delete Scope

- Duplicated pattern checks across individual Python rules that are superseded by shared utils (e.g., multiple rules checking "is this a literal?" independently).

---

## S16. A7 — PCRE2 Capability Detection and Conditional Lookaround Patterns

### Goal

Detect PCRE2 availability at bootstrap via `rg --pcre2-version`, store the capability in `Toolchain`, and conditionally use `-P` for patterns containing lookaround syntax. This enables more powerful regex patterns when the environment supports them.

### Representative Code Snippets

```python
# tools/cq/core/toolchain.py — PCRE2 detection

def _detect_pcre2() -> bool:
    """Detect whether ripgrep was compiled with PCRE2 support.

    Returns
    -------
    bool
        True if PCRE2 is available.
    """
    proc = subprocess.run(
        ["rg", "--pcre2-version"],
        check=False,
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0 and bool(proc.stdout.strip())


class Toolchain(CqStruct, frozen=True):
    """Available tools and versions."""

    rg_available: bool
    rg_version: str | None
    rg_pcre2_available: bool = False  # New field
    sgpy_available: bool
    sgpy_version: str | None
    # ... remaining fields ...

    @staticmethod
    def detect() -> Toolchain:
        rg_available, rg_version = _detect_rg()
        rg_pcre2 = _detect_pcre2() if rg_available else False
        # ... rest of detection ...
        return Toolchain(
            rg_available=rg_available,
            rg_version=rg_version,
            rg_pcre2_available=rg_pcre2,
            # ... rest ...
        )
```

```python
# tools/cq/search/rg/runner.py — Conditional PCRE2 flag
import re

_LOOKAROUND_RE = re.compile(r"\(\?[<!=]")


def build_rg_command(
    *,
    pattern: str,
    mode: QueryMode,
    # ...
    pcre2_available: bool = False,
) -> list[str]:
    # ... existing construction ...
    if pcre2_available and _LOOKAROUND_RE.search(pattern):
        command.append("-P")
    # ...
```

### Files to Edit

- `tools/cq/core/toolchain.py` — Add `_detect_pcre2()` function; add `rg_pcre2_available: bool = False` field to `Toolchain`; update `detect()`.
- `tools/cq/search/rg/runner.py` — Add `pcre2_available` parameter to `build_rg_command()`; add lookaround detection regex; conditionally emit `-P` flag.

### New Files to Create

- None — existing test files gain PCRE2 detection cases.

### Legacy Decommission/Delete Scope

- None — additive capability.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S2/A2, S3/A1)

- Delete manual `\b` prefix from `find_call_candidates()` pattern construction in `tools/cq/search/rg/adapter.py:130` — superseded by `-w` flag from A2. This deletion is safe only after both A2 (word-boundary mode) and A1 (multi-pattern which changes the `-e` line) are landed.

### Batch D2 (after S14/B2, S15/B3)

- Delete `tools/cq/astgrep/rules_py.py` (entire file) — superseded by `tools/cq/astgrep/rulepacks/python.yaml`. Only safe after YAML round-trip validation passes.
- Delete `tools/cq/astgrep/rules_rust.py` (entire file) — superseded by `tools/cq/astgrep/rulepacks/rust.yaml`. Only safe after YAML round-trip validation passes.

### Batch D3 (after S6/B5, S8/B4, S11/B6)

- Remove Python-side post-filtering code in callers that manually walk parent chains or check context — superseded by native refinement predicates (B5), constraint filtering (B4), and pattern objects (B6). This spans multiple files:
  - `tools/cq/search/pipeline/classifier_runtime.py` — Manual parent-chain walking in `_is_docstring_context()` and related helpers.
  - `tools/cq/search/python/extractors.py` — 25+ `.parent()`/`.kind()` context classification patterns.
  - `tools/cq/query/executor.py` — Metadata-only storage of context/selector fields (lines that store in dict without threading to ast-grep).

---

## Implementation Sequence

1. **S1/C1 — Ripgrep prefiltering** (Highest priority, independent, greatest performance impact)
2. **S2/A2 — Word-boundary mode** (Simple, unlocks cleaner patterns in adapter)
3. **S3/A1 — Multi-pattern OR** (Contract-level change, needed by many callers)
4. **S4/B1 — Variadic captures** (Independent, unblocks argument-level analysis)
5. **S5/A3 — Context lines** (Codec + collector change, needed before C3)
6. **S6/B5 — Refinement predicates** (Independent, replaces Python parent-chain walking)
7. **S7/A4 — Count probes** (Independent, enables adaptive profiles)
8. **S8/B4 — Constraint filtering** (Depends on scan_with_pattern being extensible)
9. **S9/C2 — Multiline search** (Depends on S5 for SearchLimits extension pattern)
10. **S10/A5 — File enumeration** (Independent, diagnostic utility)
11. **S11/B6 — Pattern objects** (Depends on S8 for scan_with_pattern extension pattern, threads PatternSpec)
12. **S12/C3 — Begin/end events** (Depends on S5 codec pattern for new event types)
13. **S13/A6 — Sort mode** (Independent, small SearchLimits addition)
14. **S14/B2 — YAML rule packs** (Large scope, independent, but informs B3)
15. **S15/B3 — Shared utility rules** (Depends on S14 for YAML infrastructure)
16. **S16/A7 — PCRE2 detection** (Independent, lowest priority, environment-dependent)

**Rationale:** C1 first because it delivers the largest performance gain. A2 and A1 next because they are small contract-level changes that many later items build upon. B-group items are interleaved to provide immediate value from refinement predicates (B5) while the larger YAML migration (B2/B3) is saved for later.

---

## Implementation Checklist

- [ ] S1/C1 — Ripgrep-accelerated file prefiltering for ast-grep batch scans
- [ ] S2/A2 — Word-boundary mode (`-w`) for identifier searches
- [ ] S3/A1 — Multi-pattern OR search (`-e P1 -e P2`)
- [ ] S4/B1 — `get_multiple_matches()` for variadic captures
- [ ] S5/A3 — Context lines (`-C`/`-A`/`-B`) for richer match payloads
- [ ] S6/B5 — Refinement predicates (`matches`/`inside`/`has`)
- [ ] S7/A4 — `--count` / `--count-matches` for fast cardinality probes
- [ ] S8/B4 — Constraint-driven metavariable filtering
- [ ] S9/C2 — Multiline ripgrep (`-U`) for cross-line patterns
- [ ] S10/A5 — `--files` mode for file-set enumeration
- [ ] S11/B6 — Pattern object syntax (`context` + `selector`) threading
- [ ] S12/C3 — Ripgrep `begin`/`end` events for file-level metadata
- [ ] S13/A6 — `--sort path` for deterministic output ordering
- [ ] S14/B2 — Externalized YAML/JSON rule packs
- [ ] S15/B3 — Shared utility rules via `utils` + `matches`
- [ ] S16/A7 — PCRE2 capability detection and conditional lookaround
- [ ] D1 — Decommission manual `\b` wrapping (after S2, S3)
- [ ] D2 — Decommission Python rule files (after S14, S15)
- [ ] D3 — Decommission Python-side parent-chain filtering (after S6, S8, S11)
