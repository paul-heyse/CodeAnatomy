# CQ Tool: Complete Pivot from ripgrep to rpygrep

> **Status:** Approved for Implementation
> **Author:** Claude
> **Created:** 2026-02-02
> **Context:** Replace all direct ripgrep subprocess calls with typed rpygrep wrapper

---

## Executive Summary

This document describes the **complete replacement** of all direct `rg` (ripgrep) subprocess calls in the cq tool with **rpygrep v0.2.1**, a strongly-typed Python wrapper. This pivot achieves:

1. **100% feature coverage** with minor workarounds for 2 edge cases
2. **Safety by default** - Built-in caps for file size, match count, traversal depth
3. **Elimination of fragile parsing** - Typed Pydantic models replace manual stdout splitting
4. **~200 lines of code reduction** - Builder pattern replaces manual command construction
5. **Async support** - Enable streaming results for large codebases

---

## Current State: Direct ripgrep Usage

### Files Requiring Migration

| File | Function | Current Pattern | Lines |
|------|----------|-----------------|-------|
| `query/executor.py` | `rg_files_with_matches()` | `rg --files-with-matches` | 1093-1143 |
| `macros/calls.py` | `_rg_find_candidates()` | `rg --line-number --no-heading` | 232-279 |
| `macros/impact.py` | `_rg_find_callers()` | `rg --line-number --no-heading` | 483-527 |
| `macros/sig_impact.py` | Uses `_rg_find_candidates` | (imports from calls.py) | 221-222 |
| `core/toolchain.py` | `Toolchain.rg_path` | `shutil.which("rg")` | 52-93 |

### Current ripgrep Features Used

| Feature | Usage | Purpose |
|---------|-------|---------|
| `--files-with-matches` | executor.py | Find files containing pattern (no content) |
| `--type py` | All locations | Filter to Python files only |
| `--line-number` | calls.py, impact.py | Include line numbers in output |
| `--no-heading` | calls.py, impact.py | Flat output format |
| `-e PATTERN` | executor.py | Explicit regex pattern |
| `-g GLOB` / `-g !GLOB` | executor.py | Include/exclude globs |
| Word boundary regex | All macros | `\b{name}\s*\(` patterns |
| Return code 0/1/2+ | All | Success/no-match/error handling |
| 30s timeout | macros | Subprocess timeout |

### Current Safety Gaps

| Gap | Current State | Risk |
|-----|---------------|------|
| Max file size | None | Large files slow scans |
| Max matches/file | None | Pathological patterns return 10K+ results |
| Max traversal depth | None | Deep node_modules trees explode |
| Excluded types | gitignore only | May scan binaries, parquet, sqlite |
| Error details | Silent failures | Return code 2+ returns empty list |

---

## rpygrep Feature Mapping

### Complete Coverage Analysis

| cq Feature | rpygrep Equivalent | Status |
|------------|-------------------|--------|
| `--files-with-matches` | `RipGrepSearch` + extract paths + `max_count(1)` | ⚠️ Workaround |
| `--type py` | `include_type("py")` | ✅ Direct |
| `--line-number` | `match.data.line_number` (JSON mode) | ✅ Direct |
| `--no-heading` | N/A (structured JSON output) | ✅ N/A |
| `-e PATTERN` | `add_pattern(pattern)` | ✅ Direct |
| `-g GLOB` | `include_glob(glob)` | ✅ Direct |
| `-g !GLOB` | `exclude_glob(glob)` | ✅ Direct |
| Regex patterns | Full regex + `patterns_are_not_regex()` | ✅ Direct |
| Return codes | Internal handling, yields empty on no match | ✅ Direct |
| Timeout | External async wrapper needed | ⚠️ Workaround |

### New Capabilities Gained

| Feature | rpygrep Method | Benefit |
|---------|---------------|---------|
| Max file size | `max_file_size(bytes)` | Skip minified bundles, generated files |
| Max matches/file | `max_count(n)` | Bound output volume |
| Max traversal depth | `max_depth(n)` | Bound directory recursion |
| Excluded binary types | `exclude_types(DEFAULT_EXCLUDED_TYPES)` | Skip parquet, sqlite, pickle, images |
| Context lines | `before_context(n)`, `after_context(n)` | Snippet rendering |
| Submatch offsets | `match.data.submatches[].start/end` | Precise highlighting |
| Async iteration | `arun()` | Non-blocking for large repos |
| Filesystem boundary | `one_file_system()` | Don't cross mounts |
| Deterministic order | `sort(by="path")` | Reproducible results |
| PCRE2 fallback | `auto_hybrid_regex()` | Lookbehind, backrefs |
| Escape hatch | `add_extra_options([...])` | Any rg flag if needed |

---

## Workaround Details

### Workaround 1: `--files-with-matches` Replacement

**Problem:** rpygrep uses `--json` mode internally. Ripgrep forbids combining `--json` with `--files-with-matches`.

**Solution:** Use `RipGrepSearch` with `max_count(1)` and collect unique paths:

```python
def find_files_with_pattern(root: Path, pattern: str, ...) -> Iterator[Path]:
    """Equivalent to: rg --files-with-matches -e pattern --type py"""
    rg = (
        RipGrepSearch(working_directory=root)
        .add_pattern(pattern)
        .include_type("py")
        .max_count(1)  # Stop after first match per file
    )
    seen: set[Path] = set()
    for result in rg.run():
        if result.path not in seen:
            seen.add(result.path)
            yield result.path
```

**Performance Impact:** Minimal. JSON parsing overhead is negligible vs I/O. The `max_count(1)` optimization ensures we stop reading each file after first match.

### Workaround 2: Timeout Control

**Problem:** rpygrep has no built-in timeout parameter.

**Solution A (Async - Preferred):**
```python
import asyncio

async def search_with_timeout(
    rg: RipGrepSearch,
    timeout_seconds: float = 30.0,
) -> list[RipGrepSearchResult]:
    results = []
    try:
        async with asyncio.timeout(timeout_seconds):
            async for result in rg.arun():
                results.append(result)
    except asyncio.TimeoutError:
        pass  # Return partial results
    return results
```

**Solution B (Sync with thread):**
```python
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

def search_with_timeout_sync(
    rg: RipGrepSearch,
    timeout_seconds: float = 30.0,
) -> list[RipGrepSearchResult]:
    def _run():
        return list(rg.run())

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_run)
        try:
            return future.result(timeout=timeout_seconds)
        except FuturesTimeout:
            return []
```

---

## New Module: `tools/cq/search/`

### Directory Structure

```
tools/cq/search/
├── __init__.py
├── adapter.py          # Core rpygrep wrapper
├── profiles.py         # Safety limit presets
└── timeout.py          # Timeout utilities
```

### `tools/cq/search/profiles.py`

```python
"""Search safety profiles for agent-grade defaults."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SearchLimits:
    """Configurable safety limits for ripgrep operations."""
    max_depth: int = 50
    max_file_size: int = 5 * 1024 * 1024  # 5 MiB
    max_count: int = 500  # matches per file
    timeout_seconds: float = 30.0


# Pre-built profiles
INTERACTIVE = SearchLimits(
    max_depth=20,
    max_file_size=2 * 1024 * 1024,  # 2 MiB
    max_count=50,
    timeout_seconds=10.0,
)

AUDIT = SearchLimits(
    max_depth=100,
    max_file_size=50 * 1024 * 1024,  # 50 MiB
    max_count=10_000,
    timeout_seconds=120.0,
)

LITERAL = SearchLimits(
    max_depth=30,
    max_file_size=5 * 1024 * 1024,  # 5 MiB
    max_count=200,
    timeout_seconds=30.0,
)

DEFAULT = SearchLimits()  # Balanced defaults
```

### `tools/cq/search/timeout.py`

```python
"""Timeout utilities for rpygrep operations."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from rpygrep import RipGrepSearch
    from rpygrep.types import RipGrepSearchResult

T = TypeVar("T")


async def search_async_with_timeout(
    rg: RipGrepSearch,
    timeout_seconds: float,
) -> list[RipGrepSearchResult]:
    """Run async search with timeout, returning partial results on timeout."""
    results: list[RipGrepSearchResult] = []
    try:
        async with asyncio.timeout(timeout_seconds):
            async for result in rg.arun():
                results.append(result)
    except asyncio.TimeoutError:
        pass
    return results


def search_sync_with_timeout(
    rg: RipGrepSearch,
    timeout_seconds: float,
) -> list[RipGrepSearchResult]:
    """Run sync search with timeout using thread pool."""
    def _collect() -> list[RipGrepSearchResult]:
        return list(rg.run())

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_collect)
        try:
            return future.result(timeout=timeout_seconds)
        except FuturesTimeout:
            return []
```

### `tools/cq/search/adapter.py`

```python
"""
Typed ripgrep integration for cq.

Provides complete replacement for all direct rg subprocess calls with:
- Agent-safe search profiles (bounded depth, file size, match count)
- Consistent error handling
- Integration with cq's Scope and ToolPlan
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Sequence

from rpygrep import DEFAULT_EXCLUDED_TYPES, RipGrepFind, RipGrepSearch

from .profiles import DEFAULT, SearchLimits
from .timeout import search_sync_with_timeout

if TYPE_CHECKING:
    from rpygrep.types import RipGrepSearchResult


# Standard excludes for code search (beyond DEFAULT_EXCLUDED_TYPES)
REPO_SINK_GLOBS: tuple[str, ...] = (
    "**/.git/**",
    "**/node_modules/**",
    "**/.venv/**",
    "**/venv/**",
    "**/__pycache__/**",
    "**/dist/**",
    "**/build/**",
    "**/.tox/**",
    "**/.mypy_cache/**",
    "**/.pytest_cache/**",
    "**/target/**",
    "**/*.egg-info/**",
    "**/.ruff_cache/**",
)


def _apply_common_options(
    rg: RipGrepFind | RipGrepSearch,
    *,
    include_globs: Sequence[str],
    exclude_globs: Sequence[str],
    limits: SearchLimits,
) -> None:
    """Apply common filtering options to Find or Search builder."""
    # Type filter: Python only
    rg.include_type("py")

    # Excludes: DEFAULT_EXCLUDED_TYPES + repo sinks + custom
    rg.exclude_types(list(DEFAULT_EXCLUDED_TYPES))
    for glob in REPO_SINK_GLOBS:
        rg.exclude_glob(glob)
    for glob in exclude_globs:
        rg.exclude_glob(glob)

    # Includes
    for glob in include_globs:
        rg.include_glob(glob)

    # Depth limit
    rg.max_depth(limits.max_depth)


# -----------------------------------------------------------------------------
# File Discovery (replaces: rg --files with filters)
# -----------------------------------------------------------------------------


def find_python_files(
    root: Path,
    *,
    include_globs: Sequence[str] = (),
    exclude_globs: Sequence[str] = (),
    limits: SearchLimits = DEFAULT,
) -> Iterator[Path]:
    """
    Find Python files under root with agent-safe defaults.

    Replaces: RipGrepFind for pure file enumeration.
    """
    rg = RipGrepFind(working_directory=root)
    _apply_common_options(
        rg,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        limits=limits,
    )
    yield from rg.run()


# -----------------------------------------------------------------------------
# File Matching (replaces: rg --files-with-matches)
# -----------------------------------------------------------------------------


def find_files_with_pattern(
    root: Path,
    pattern: str,
    *,
    include_globs: Sequence[str] = (),
    exclude_globs: Sequence[str] = (),
    limits: SearchLimits = DEFAULT,
    literal: bool = False,
    case_sensitive: bool = True,
) -> list[Path]:
    """
    Find files containing a pattern.

    Replaces: rg_files_with_matches() in executor.py

    Note: Uses RipGrepSearch with max_count(1) since --json mode
    cannot combine with --files-with-matches.
    """
    rg = RipGrepSearch(working_directory=root)

    # Pattern
    rg.add_pattern(pattern)
    if literal:
        rg.patterns_are_not_regex()
    rg.case_sensitive(case_sensitive)

    # Common options
    _apply_common_options(
        rg,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        limits=limits,
    )

    # Safety limits
    rg.max_file_size(limits.max_file_size)
    rg.max_count(1)  # Only need to know if file matches

    # Execute with timeout
    results = search_sync_with_timeout(rg, limits.timeout_seconds)

    # Dedupe and return paths
    seen: set[Path] = set()
    paths: list[Path] = []
    for result in results:
        if result.path not in seen:
            seen.add(result.path)
            paths.append(result.path)
    return paths


# -----------------------------------------------------------------------------
# Content Search (replaces: rg --line-number --no-heading)
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class ContentMatch:
    """Structured content match result."""
    path: Path
    line_number: int
    line_text: str
    submatches: tuple[tuple[int, int], ...]  # (start, end) byte offsets


def search_content(
    root: Path,
    pattern: str,
    *,
    include_globs: Sequence[str] = (),
    exclude_globs: Sequence[str] = (),
    limits: SearchLimits = DEFAULT,
    literal: bool = False,
    case_sensitive: bool = True,
    context_before: int = 0,
    context_after: int = 0,
) -> list[ContentMatch]:
    """
    Search file contents and return structured matches.

    Replaces:
    - _rg_find_candidates() in calls.py
    - _rg_find_callers() in impact.py
    """
    rg = RipGrepSearch(working_directory=root)

    # Pattern
    rg.add_pattern(pattern)
    if literal:
        rg.patterns_are_not_regex()
    rg.case_sensitive(case_sensitive)

    # Common options
    _apply_common_options(
        rg,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        limits=limits,
    )

    # Safety limits
    rg.max_file_size(limits.max_file_size)
    rg.max_count(limits.max_count)

    # Context
    if context_before > 0:
        rg.before_context(context_before)
    if context_after > 0:
        rg.after_context(context_after)

    # Execute with timeout
    results = search_sync_with_timeout(rg, limits.timeout_seconds)

    # Convert to ContentMatch objects
    matches: list[ContentMatch] = []
    for result in results:
        for match in result.matches or []:
            data = match.data
            line_text = data.lines.text if data.lines and data.lines.text else "<binary>"

            submatches = tuple(
                (sm.start, sm.end)
                for sm in (data.submatches or [])
                if sm.start is not None and sm.end is not None
            )

            matches.append(ContentMatch(
                path=result.path,
                line_number=data.line_number or 0,
                line_text=line_text,
                submatches=submatches,
            ))

    return matches


def find_call_candidates(
    root: Path,
    function_name: str,
    *,
    limits: SearchLimits = DEFAULT,
) -> list[tuple[Path, int]]:
    """
    Find candidate call sites for a function.

    Replaces: _rg_find_candidates() in calls.py

    Returns list of (path, line_number) tuples.
    """
    # Extract simple name from qualified name (e.g., "Class.method" -> "method")
    search_name = function_name.rsplit(".", maxsplit=1)[-1]
    pattern = rf"\b{search_name}\s*\("

    matches = search_content(root, pattern, limits=limits)
    return [(m.path, m.line_number) for m in matches]


def find_callers(
    root: Path,
    function_name: str,
    *,
    limits: SearchLimits = DEFAULT,
) -> list[tuple[Path, int]]:
    """
    Find potential callers of a function.

    Replaces: _rg_find_callers() in impact.py

    Returns list of (path, line_number) tuples.
    """
    # Same logic as find_call_candidates
    return find_call_candidates(root, function_name, limits=limits)
```

### `tools/cq/search/__init__.py`

```python
"""Search module providing typed ripgrep integration."""
from __future__ import annotations

from .adapter import (
    REPO_SINK_GLOBS,
    ContentMatch,
    find_call_candidates,
    find_callers,
    find_files_with_pattern,
    find_python_files,
    search_content,
)
from .profiles import (
    AUDIT,
    DEFAULT,
    INTERACTIVE,
    LITERAL,
    SearchLimits,
)

__all__ = [
    # Profiles
    "SearchLimits",
    "DEFAULT",
    "INTERACTIVE",
    "AUDIT",
    "LITERAL",
    # Constants
    "REPO_SINK_GLOBS",
    # Types
    "ContentMatch",
    # Functions
    "find_python_files",
    "find_files_with_pattern",
    "search_content",
    "find_call_candidates",
    "find_callers",
]
```

---

## Migration: File-by-File Changes

### 1. `query/executor.py`

**Remove:**
```python
def rg_files_with_matches(
    root: Path,
    pattern: str,
    scope: Scope,
) -> list[Path]:
    cmd = ["rg", "--files-with-matches", "-e", pattern, "--type", "py"]
    # ... 50 lines of subprocess handling ...
```

**Replace with:**
```python
from tools.cq.search import find_files_with_pattern, SearchLimits

def rg_files_with_matches(
    root: Path,
    pattern: str,
    scope: Scope,
) -> list[Path]:
    """Use rpygrep to find files matching pattern."""
    search_root = root / scope.in_dir if scope.in_dir else root
    return find_files_with_pattern(
        search_root,
        pattern,
        include_globs=tuple(scope.globs) if scope.globs else (),
        exclude_globs=tuple(scope.exclude) if scope.exclude else (),
        limits=SearchLimits(max_depth=scope.max_depth or 50),
    )
```

### 2. `macros/calls.py`

**Remove:**
```python
def _rg_find_candidates(
    rg_path: str,
    function_name: str,
    root: Path,
) -> list[tuple[str, int]]:
    candidates: list[tuple[str, int]] = []
    search_name = function_name.rsplit(".", maxsplit=1)[-1]
    try:
        result = subprocess.run(
            [rg_path, "--type", "py", "--line-number", "--no-heading",
             rf"\b{search_name}\s*\(", str(root)],
            # ... 30 lines of parsing ...
```

**Replace with:**
```python
from tools.cq.search import find_call_candidates, INTERACTIVE

def _rg_find_candidates(
    function_name: str,
    root: Path,
) -> list[tuple[Path, int]]:
    """Use rpygrep to find candidate call sites."""
    return find_call_candidates(root, function_name, limits=INTERACTIVE)
```

**Update call site:**
```python
# Before:
rg_path = tc.require_rg()
candidates = _rg_find_candidates(rg_path, function_name, root)

# After:
candidates = _rg_find_candidates(function_name, root)
```

### 3. `macros/impact.py`

**Remove:**
```python
def _rg_find_callers(
    rg_path: str,
    function_name: str,
    root: Path,
) -> list[tuple[str, int]]:
    # ... 45 lines of subprocess + parsing ...
```

**Replace with:**
```python
from tools.cq.search import find_callers, INTERACTIVE

def _rg_find_callers(
    function_name: str,
    root: Path,
) -> list[tuple[Path, int]]:
    """Use rpygrep to find potential callers."""
    return find_callers(root, function_name, limits=INTERACTIVE)
```

**Update call site:**
```python
# Before:
rg_path = request.tc.rg_path
caller_sites: list[tuple[str, int]] = []
if rg_path:
    caller_sites = _rg_find_callers(rg_path, request.function_name, request.root)

# After:
caller_sites = _rg_find_callers(request.function_name, request.root)
```

### 4. `macros/sig_impact.py`

**Update import:**
```python
# Before:
from tools.cq.macros.calls import _rg_find_candidates, _group_candidates, _collect_call_sites

# After: (no change needed if calls.py is updated, just uses new signature)
```

### 5. `core/toolchain.py`

**Update to detect rpygrep instead of rg path:**

```python
@dataclass(frozen=True)
class Toolchain:
    """External tool availability."""

    rpygrep_available: bool
    rpygrep_version: str | None
    rg_version: str | None  # Still detect rg for version info
    sg_path: str | None
    sg_version: str | None
    py_version: str

    @classmethod
    def detect(cls) -> Toolchain:
        """Detect available tools."""
        import shutil
        import subprocess
        import sys

        # Detect rpygrep
        rpygrep_available = False
        rpygrep_version = None
        try:
            import importlib.metadata
            rpygrep_version = importlib.metadata.version("rpygrep")
            rpygrep_available = True
        except Exception:
            pass

        # Detect ripgrep version (for reporting, rpygrep handles invocation)
        rg_path = shutil.which("rg")
        rg_version = None
        if rg_path:
            try:
                result = subprocess.run(
                    [rg_path, "--version"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0 and result.stdout:
                    rg_version = result.stdout.strip().split()[-1]
            except Exception:
                pass

        # Detect ast-grep
        sg_path = shutil.which("sg") or shutil.which("ast-grep")
        sg_version = None
        if sg_path:
            try:
                result = subprocess.run(
                    [sg_path, "--version"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0 and result.stdout:
                    sg_version = result.stdout.strip().split()[-1]
            except Exception:
                pass

        py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

        return Toolchain(
            rpygrep_available=rpygrep_available,
            rpygrep_version=rpygrep_version,
            rg_version=rg_version,
            sg_path=sg_path,
            sg_version=sg_version,
            py_version=py_version,
        )

    def versions(self) -> dict[str, str | None]:
        """Get tool version information."""
        return {
            "rpygrep": self.rpygrep_version,
            "rg": self.rg_version,
            "sg": self.sg_version,
            "python": self.py_version,
        }

    def require_rpygrep(self) -> None:
        """Ensure rpygrep is available."""
        if not self.rpygrep_available:
            msg = (
                "rpygrep is required but not installed. "
                "Install with: uv add rpygrep"
            )
            raise RuntimeError(msg)

    def require_sg(self) -> str:
        """Get ast-grep path, raising if not available."""
        if not self.sg_path:
            msg = (
                "ast-grep (sg) is required but not found. "
                "Install with: pip install ast-grep-py"
            )
            raise RuntimeError(msg)
        return self.sg_path

    @property
    def has_rpygrep(self) -> bool:
        """Check if rpygrep is available."""
        return self.rpygrep_available

    # Deprecated: kept for backwards compatibility during migration
    @property
    def rg_path(self) -> str | None:
        """Deprecated: use has_rpygrep instead."""
        import shutil
        return shutil.which("rg")

    def require_rg(self) -> str:
        """Deprecated: rpygrep handles rg invocation."""
        self.require_rpygrep()
        rg = shutil.which("rg")
        if not rg:
            raise RuntimeError("ripgrep (rg) not found")
        return rg
```

---

## Dependency Changes

### `pyproject.toml`

```toml
[project.dependencies]
# ... existing deps ...
rpygrep = ">=0.2.1,<0.3"
```

### Runtime Requirement

- `rg` (ripgrep) must still be installed (rpygrep shells out to it)
- rpygrep validates rg availability at runtime

---

## Configuration Integration

### New CLI Global Options

Add to `cli_app/types.py` or similar:

```python
from tools.cq.search import SearchLimits, INTERACTIVE, AUDIT, LITERAL

PROFILE_MAP = {
    "interactive": INTERACTIVE,
    "audit": AUDIT,
    "literal": LITERAL,
}
```

Add to global options:
```
--search-profile <interactive|audit|literal>  # Default: interactive
--max-search-depth N                          # Override profile
--max-file-size BYTES                         # Override profile
--max-matches-per-file N                      # Override profile
```

### Environment Variables

```bash
CQ_SEARCH_PROFILE=interactive    # Profile preset
CQ_MAX_SEARCH_DEPTH=50           # Override max_depth
CQ_MAX_FILE_SIZE=5242880         # Override max_file_size (bytes)
CQ_MAX_MATCHES_PER_FILE=500      # Override max_count
```

### Config File (`.cq.toml`)

```toml
[cq.search]
profile = "interactive"
max_depth = 50
max_file_size = 5242880
max_matches_per_file = 500
```

---

## Testing Strategy

### Unit Tests

Create `tests/unit/cq/search/`:

```python
# tests/unit/cq/search/test_adapter.py

import pytest
from pathlib import Path
from tools.cq.search import (
    find_python_files,
    find_files_with_pattern,
    search_content,
    find_call_candidates,
    SearchLimits,
)


@pytest.fixture
def sample_repo(tmp_path: Path) -> Path:
    """Create a minimal test repository."""
    (tmp_path / "main.py").write_text("def hello():\n    print('world')\n")
    (tmp_path / "test.py").write_text("from main import hello\nhello()\n")
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules/pkg.py").write_text("# should be excluded\n")
    return tmp_path


def test_find_python_files_excludes_node_modules(sample_repo: Path) -> None:
    """Verify node_modules is excluded by default."""
    files = list(find_python_files(sample_repo))
    names = {f.name for f in files}
    assert "main.py" in names
    assert "test.py" in names
    assert "pkg.py" not in names  # Excluded


def test_find_files_with_pattern(sample_repo: Path) -> None:
    """Verify pattern matching works."""
    files = find_files_with_pattern(sample_repo, "hello")
    names = {f.name for f in files}
    assert "main.py" in names
    assert "test.py" in names


def test_search_content_returns_line_numbers(sample_repo: Path) -> None:
    """Verify line numbers are correct."""
    matches = search_content(sample_repo, r"def hello")
    assert len(matches) == 1
    assert matches[0].path.name == "main.py"
    assert matches[0].line_number == 1


def test_find_call_candidates(sample_repo: Path) -> None:
    """Verify call site detection."""
    candidates = find_call_candidates(sample_repo, "hello")
    assert len(candidates) >= 1
    paths = {c[0].name for c in candidates}
    assert "test.py" in paths


def test_max_file_size_excludes_large_files(tmp_path: Path) -> None:
    """Verify large files are skipped."""
    small = tmp_path / "small.py"
    small.write_text("hello = 1\n")

    large = tmp_path / "large.py"
    large.write_text("hello = 1\n" + "x" * 1_000_000)  # ~1MB

    limits = SearchLimits(max_file_size=10_000)  # 10KB limit
    files = find_files_with_pattern(tmp_path, "hello", limits=limits)
    names = {f.name for f in files}

    assert "small.py" in names
    assert "large.py" not in names  # Excluded by size
```

### Integration Tests

```python
# tests/integration/cq/test_search_migration.py

def test_rg_files_with_matches_equivalence(sample_repo: Path) -> None:
    """Verify new implementation matches old behavior."""
    # This test compares old subprocess approach vs new rpygrep approach
    # to ensure migration doesn't change results
    ...
```

### Contract Tests

Consider adding the rpygrep contract pack from the library reference to detect drift across rg/rpygrep upgrades.

---

## Migration Phases

### Phase 1: Add New Module (Days 1-2)

1. Add `rpygrep>=0.2.1` to `pyproject.toml`
2. Create `tools/cq/search/` module with all files
3. Add unit tests for new module
4. No changes to existing code

**Risk:** None (purely additive)

### Phase 2: Parallel Implementation (Days 3-4)

1. Add feature flag: `CQ_USE_RPYGREP=1`
2. Update `executor.py` to use new module when flag set
3. Run tests with both paths, compare results
4. Fix any discrepancies

**Risk:** Low (opt-in only)

### Phase 3: Macro Migration (Days 5-6)

1. Update `calls.py` to use `find_call_candidates()`
2. Update `impact.py` to use `find_callers()`
3. Update `sig_impact.py` (minimal changes)
4. Update `toolchain.py` to detect rpygrep
5. Run full test suite

**Risk:** Medium (behavioral changes)

### Phase 4: Cleanup (Days 7-8)

1. Remove feature flag, make rpygrep default
2. Remove deprecated `rg_path` property from Toolchain
3. Remove old subprocess code
4. Update documentation
5. Add search profile CLI options

**Risk:** Low (cleanup only)

---

## Rollback Plan

If issues arise post-migration:

1. The old subprocess code can be restored from git
2. Feature flag allows instant rollback during Phase 2-3
3. Toolchain still detects raw `rg` binary for fallback

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Code reduction | ~200 lines removed |
| Safety coverage | 100% of searches have limits |
| Test coverage | >90% on new search module |
| No regressions | All existing tests pass |
| Performance | <5% slowdown (acceptable for safety gains) |

---

---

## Detailed Implementation Specifications

This section provides per-scope-item implementation details including:
- **Code Patterns**: Key architectural elements and function implementations
- **Deprecation Targets**: Files, functions, and code to remove after migration
- **Implementation Checklist**: Step-by-step tasks for the migration

---

### Scope Item 1: `query/executor.py` - `rg_files_with_matches()`

#### Current Code (Lines 1093-1143)

```python
# CURRENT: Direct subprocess call with fragile parsing
def rg_files_with_matches(
    root: Path,
    pattern: str,
    scope: Scope,
) -> list[Path]:
    cmd = ["rg", "--files-with-matches", "-e", pattern, "--type", "py"]

    if scope.in_dir:
        cmd.append(scope.in_dir)
    else:
        cmd.append(".")

    if scope.globs:
        for glob in scope.globs:
            cmd.extend(["-g", glob])
    for exclude in scope.exclude:
        cmd.extend(["-g", f"!{exclude}"])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=root,
    )

    if result.returncode not in (0, 1):  # 1 means no matches
        return []

    files: list[Path] = []
    for line in result.stdout.strip().split("\n"):
        if line:
            files.append(root / line)

    return files
```

#### New Code Pattern

```python
# NEW: Type-safe rpygrep with configurable safety limits
from tools.cq.search import find_files_with_pattern, SearchLimits

def rg_files_with_matches(
    root: Path,
    pattern: str,
    scope: Scope,
    *,
    limits: SearchLimits | None = None,
) -> list[Path]:
    """Use rpygrep to find files matching pattern.

    Parameters
    ----------
    root
        Repository root.
    pattern
        Regex pattern to search.
    scope
        Scope constraints (in_dir, globs, exclude).
    limits
        Optional search safety limits. Uses scope.max_depth if not provided.

    Returns
    -------
    list[Path]
        Files containing pattern matches.
    """
    # Determine search root from scope
    search_root = root / scope.in_dir if scope.in_dir else root

    # Build limits from scope or defaults
    effective_limits = limits or SearchLimits(
        max_depth=scope.max_depth or 50,
    )

    return find_files_with_pattern(
        search_root,
        pattern,
        include_globs=tuple(scope.globs) if scope.globs else (),
        exclude_globs=tuple(scope.exclude) if scope.exclude else (),
        limits=effective_limits,
    )
```

#### Key Architectural Elements

| Element | Description |
|---------|-------------|
| `Scope` integration | Map `scope.in_dir`, `scope.globs`, `scope.exclude` to rpygrep options |
| Safety limits | Derive `SearchLimits` from `scope.max_depth` |
| Return type | Unchanged `list[Path]` for backward compatibility |
| Error handling | rpygrep handles internally; no return code checking needed |

#### Deprecation Targets

| Target | Type | Lines | Action |
|--------|------|-------|--------|
| `subprocess` import | import | (module-level) | Remove if unused elsewhere |
| `rg_files_with_matches` body | function body | 1114-1143 | Replace with rpygrep call |
| Return code handling | logic | 1135-1136 | Remove (rpygrep handles internally) |
| Manual stdout parsing | logic | 1138-1142 | Remove (rpygrep returns typed paths) |

#### Implementation Checklist

- [ ] Add `from tools.cq.search import find_files_with_pattern, SearchLimits` import
- [ ] Update function signature to accept optional `limits` parameter
- [ ] Replace function body with rpygrep adapter call
- [ ] Update docstring to document new `limits` parameter
- [ ] Remove unused `subprocess` import if not used elsewhere in module
- [ ] Add unit test for `rg_files_with_matches` with mocked `find_files_with_pattern`
- [ ] Add integration test comparing old vs new implementation output
- [ ] Verify all callers of `rg_files_with_matches` still work

---

### Scope Item 2: `macros/calls.py` - `_rg_find_candidates()`

#### Current Code (Lines 232-279)

```python
# CURRENT: Subprocess with fragile colon-split parsing
_MIN_RG_PARTS = 2  # Minimum parts in rg output line

def _rg_find_candidates(
    rg_path: str,
    function_name: str,
    root: Path,
) -> list[tuple[str, int]]:
    candidates: list[tuple[str, int]] = []
    search_name = function_name.rsplit(".", maxsplit=1)[-1]

    try:
        result = subprocess.run(
            [
                rg_path,
                "--type", "py",
                "--line-number",
                "--no-heading",
                rf"\b{search_name}\s*\(",
                str(root),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if not line:
                    continue
                parts = line.split(":", _MIN_RG_PARTS)
                if len(parts) >= _MIN_RG_PARTS:
                    try:
                        filepath = parts[0]
                        lineno = int(parts[1])
                        rel = Path(filepath).relative_to(root)
                        candidates.append((str(rel), lineno))
                    except (ValueError, TypeError):
                        pass
    except (subprocess.TimeoutExpired, OSError):
        pass

    return candidates
```

#### New Code Pattern

```python
# NEW: Typed results with structured parsing
from tools.cq.search import find_call_candidates, INTERACTIVE, SearchLimits

def _rg_find_candidates(
    function_name: str,
    root: Path,
    *,
    limits: SearchLimits = INTERACTIVE,
) -> list[tuple[Path, int]]:
    """Use rpygrep to find candidate call sites.

    Parameters
    ----------
    function_name
        Function name (simple or qualified like "Class.method").
    root
        Repository root.
    limits
        Search safety limits.

    Returns
    -------
    list[tuple[Path, int]]
        Candidate (path, line_number) pairs.

    Note
    ----
    Signature change: no longer requires rg_path parameter.
    Return type changed from tuple[str, int] to tuple[Path, int].
    """
    return find_call_candidates(root, function_name, limits=limits)
```

#### Key Architectural Elements

| Element | Description |
|---------|-------------|
| Signature simplification | Remove `rg_path` parameter (rpygrep handles internally) |
| Return type upgrade | `tuple[str, int]` → `tuple[Path, int]` for type safety |
| Timeout handling | Moved to `SearchLimits.timeout_seconds` |
| Pattern construction | Moved to `find_call_candidates` (same `\b{name}\s*\(` regex) |
| Error recovery | rpygrep adapter returns empty list on timeout/error |

#### Deprecation Targets

| Target | Type | Lines | Action |
|--------|------|-------|--------|
| `_MIN_RG_PARTS` constant | constant | (near line 15) | Delete |
| `rg_path` parameter | parameter | 233 | Remove from signature |
| `subprocess.run` call | code block | 248-262 | Delete |
| Colon-split parsing | code block | 264-275 | Delete |
| `subprocess.TimeoutExpired` handling | exception | 276 | Delete |
| Manual return code check | logic | 263 | Delete |

#### Call Site Updates Required

```python
# BEFORE (calls.py:run_calls_macro or similar)
rg_path = tc.require_rg()
candidates = _rg_find_candidates(rg_path, function_name, root)

# AFTER
candidates = _rg_find_candidates(function_name, root)
# Or with custom limits:
candidates = _rg_find_candidates(function_name, root, limits=AUDIT)
```

#### Implementation Checklist

- [ ] Add `from tools.cq.search import find_call_candidates, INTERACTIVE, SearchLimits` import
- [ ] Remove `_MIN_RG_PARTS` constant if only used here
- [ ] Update `_rg_find_candidates` signature (remove `rg_path`, add optional `limits`)
- [ ] Replace function body with rpygrep adapter call
- [ ] Update return type annotation from `list[tuple[str, int]]` to `list[tuple[Path, int]]`
- [ ] Update docstring
- [ ] Update all call sites to remove `rg_path` argument
- [ ] Update `_group_candidates` to accept `Path` keys if needed
- [ ] Update `_collect_call_sites` to handle `Path` inputs
- [ ] Remove `tc.require_rg()` calls at call sites
- [ ] Add unit tests for new signature
- [ ] Verify no other code depends on string-typed file paths in results

---

### Scope Item 3: `macros/impact.py` - `_rg_find_callers()`

#### Current Code (Lines 483-527)

```python
# CURRENT: Nearly identical pattern to calls.py
_RG_SPLIT_PARTS = 2

def _rg_find_callers(
    rg_path: str,
    function_name: str,
    root: Path,
) -> list[tuple[str, int]]:
    callers: list[tuple[str, int]] = []

    try:
        result = subprocess.run(
            [
                rg_path,
                "--type", "py",
                "--line-number",
                "--no-heading",
                rf"\b{function_name}\s*\(",
                str(root),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if not line:
                    continue
                parts = line.split(":", _RG_SPLIT_PARTS)
                if len(parts) >= _RG_SPLIT_PARTS:
                    filepath = parts[0]
                    with suppress(ValueError, TypeError):
                        lineno = int(parts[1])
                        rel = Path(filepath).relative_to(root)
                        callers.append((str(rel), lineno))
    except (subprocess.TimeoutExpired, OSError):
        pass

    return callers
```

#### New Code Pattern

```python
# NEW: Delegate to shared adapter function
from tools.cq.search import find_callers, INTERACTIVE, SearchLimits

def _rg_find_callers(
    function_name: str,
    root: Path,
    *,
    limits: SearchLimits = INTERACTIVE,
) -> list[tuple[Path, int]]:
    """Use rpygrep to find potential callers of a function.

    Parameters
    ----------
    function_name
        Function name to search for.
    root
        Repository root.
    limits
        Search safety limits.

    Returns
    -------
    list[tuple[Path, int]]
        Candidate caller (path, line_number) pairs.

    Note
    ----
    Signature change: no longer requires rg_path parameter.
    Return type changed from tuple[str, int] to tuple[Path, int].
    """
    return find_callers(root, function_name, limits=limits)
```

#### Key Architectural Elements

| Element | Description |
|---------|-------------|
| Shared implementation | `find_callers` delegates to same logic as `find_call_candidates` |
| Signature parity | Same change pattern as `calls.py` |
| Contextlib removal | `suppress(ValueError, TypeError)` no longer needed |

#### Deprecation Targets

| Target | Type | Lines | Action |
|--------|------|-------|--------|
| `_RG_SPLIT_PARTS` constant | constant | (near module top) | Delete |
| `rg_path` parameter | parameter | 484 | Remove from signature |
| `subprocess.run` call | code block | 498-512 | Delete |
| `suppress` usage | code block | 521 | Delete |
| Manual path parsing | code block | 514-524 | Delete |
| `contextlib.suppress` import | import | (module-level) | Remove if unused |

#### Call Site Updates Required

```python
# BEFORE (impact.py:run_impact_macro or similar)
rg_path = request.tc.rg_path
caller_sites: list[tuple[str, int]] = []
if rg_path:
    caller_sites = _rg_find_callers(rg_path, request.function_name, request.root)

# AFTER
caller_sites = _rg_find_callers(request.function_name, request.root)
```

#### Implementation Checklist

- [ ] Add `from tools.cq.search import find_callers, INTERACTIVE, SearchLimits` import
- [ ] Remove `_RG_SPLIT_PARTS` constant if only used here
- [ ] Update `_rg_find_callers` signature (remove `rg_path`, add optional `limits`)
- [ ] Replace function body with rpygrep adapter call
- [ ] Update return type annotation from `list[tuple[str, int]]` to `list[tuple[Path, int]]`
- [ ] Update docstring
- [ ] Update all call sites to remove `rg_path` argument
- [ ] Remove conditional `if rg_path:` checks at call sites (rpygrep always available)
- [ ] Remove `contextlib.suppress` import if no longer used
- [ ] Add unit tests for new signature
- [ ] Verify downstream code handles `Path` type in results

---

### Scope Item 4: `macros/sig_impact.py` - Import Updates

#### Current Code (Lines 30, 221-224)

```python
# CURRENT: Imports from calls.py
from tools.cq.macros.calls import _collect_call_sites, _group_candidates, _rg_find_candidates

# Usage in _gather_call_sites (line 221-224):
def _gather_call_sites(
    tc: Toolchain,
    root: Path,
    symbol: str,
) -> list[CallSite]:
    rg_path = tc.require_rg()
    candidates = _rg_find_candidates(rg_path, symbol, root)
    by_file = _group_candidates(candidates)
    return _collect_call_sites(root, by_file, symbol)
```

#### New Code Pattern

```python
# NEW: Updated imports and call pattern
from tools.cq.macros.calls import _collect_call_sites, _group_candidates, _rg_find_candidates
from tools.cq.search import INTERACTIVE, SearchLimits

def _gather_call_sites(
    root: Path,
    symbol: str,
    *,
    limits: SearchLimits = INTERACTIVE,
) -> list[CallSite]:
    """Gather all call sites for a symbol.

    Parameters
    ----------
    root
        Repository root.
    symbol
        Symbol to search for.
    limits
        Search safety limits.

    Returns
    -------
    list[CallSite]
        Detected call sites.

    Note
    ----
    Signature change: removed tc parameter (no longer needed).
    """
    candidates = _rg_find_candidates(symbol, root, limits=limits)
    by_file = _group_candidates(candidates)
    return _collect_call_sites(root, by_file, symbol)
```

#### Key Architectural Elements

| Element | Description |
|---------|-------------|
| `Toolchain` removal | No longer needed in `_gather_call_sites` signature |
| Transitive dependency | Changes cascade from `calls.py` updates |
| Optional `limits` | Allows callers to customize search behavior |

#### Deprecation Targets

| Target | Type | Lines | Action |
|--------|------|-------|--------|
| `tc` parameter | parameter | 217 | Remove from signature |
| `tc.require_rg()` call | line | 221 | Delete |
| `rg_path` variable | variable | 221 | Delete |
| `Toolchain` TYPE_CHECKING import | import | 33 | Remove if unused |

#### Call Site Updates Required

```python
# BEFORE (somewhere in sig_impact.py run function)
sites = _gather_call_sites(tc, root, symbol)

# AFTER
sites = _gather_call_sites(root, symbol)
# Or with custom limits:
sites = _gather_call_sites(root, symbol, limits=AUDIT)
```

#### Implementation Checklist

- [ ] Wait for calls.py migration to complete (dependency)
- [ ] Add `from tools.cq.search import INTERACTIVE, SearchLimits` import
- [ ] Update `_gather_call_sites` signature (remove `tc`, add optional `limits`)
- [ ] Update function body to call new `_rg_find_candidates` signature
- [ ] Update docstring
- [ ] Update all call sites to remove `tc` argument
- [ ] Remove `Toolchain` import if no longer used in module
- [ ] Verify `_group_candidates` handles `Path` keys from new return type
- [ ] Add unit tests for updated signature

---

### Scope Item 5: `core/toolchain.py` - Toolchain Class

#### Current Code (Lines 14-160)

```python
# CURRENT: Focused on binary path detection
@dataclass
class Toolchain:
    rg_path: str | None
    rg_version: str | None
    sg_path: str | None
    sg_version: str | None
    py_path: str
    py_version: str

    @staticmethod
    def detect() -> Toolchain:
        # Detect rg via shutil.which
        rg_path = shutil.which("rg")
        rg_version = None
        if rg_path:
            # subprocess.run to get version
            ...

    def require_rg(self) -> str:
        if not self.rg_path:
            raise RuntimeError("ripgrep not found")
        return self.rg_path
```

#### New Code Pattern

```python
# NEW: rpygrep availability + backwards compat shim
from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass


@dataclass
class Toolchain:
    """Available tools and their versions.

    Parameters
    ----------
    rpygrep_available : bool
        Whether rpygrep package is installed.
    rpygrep_version : str | None
        rpygrep package version.
    rg_version : str | None
        ripgrep CLI version (for diagnostics).
    sg_path : str | None
        Path to ast-grep binary.
    sg_version : str | None
        ast-grep version string.
    py_path : str
        Path to Python interpreter.
    py_version : str
        Python version string.
    """

    rpygrep_available: bool
    rpygrep_version: str | None
    rg_version: str | None
    sg_path: str | None
    sg_version: str | None
    py_path: str
    py_version: str

    @staticmethod
    def detect() -> Toolchain:
        """Detect available tools and versions."""
        import sys

        # Detect rpygrep package
        rpygrep_available = False
        rpygrep_version = None
        try:
            import importlib.metadata
            rpygrep_version = importlib.metadata.version("rpygrep")
            rpygrep_available = True
        except Exception:
            pass

        # Detect ripgrep version (rpygrep invokes rg internally)
        rg_version = None
        rg_path = shutil.which("rg")
        if rg_path:
            try:
                result = subprocess.run(
                    [rg_path, "--version"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    first_line = result.stdout.strip().split("\n")[0]
                    rg_version = first_line.split()[-1] if first_line else None
            except (subprocess.TimeoutExpired, OSError):
                pass

        # Detect ast-grep
        sg_path = shutil.which("sg") or shutil.which("ast-grep")
        sg_version = None
        if sg_path:
            try:
                result = subprocess.run(
                    [sg_path, "--version"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    sg_version = result.stdout.strip().split()[-1]
            except (subprocess.TimeoutExpired, OSError):
                pass

        py_path = sys.executable
        py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

        return Toolchain(
            rpygrep_available=rpygrep_available,
            rpygrep_version=rpygrep_version,
            rg_version=rg_version,
            sg_path=sg_path,
            sg_version=sg_version,
            py_path=py_path,
            py_version=py_version,
        )

    def to_dict(self) -> dict[str, str | None]:
        """Convert to dict for RunMeta."""
        return {
            "rpygrep": self.rpygrep_version,
            "rg": self.rg_version,
            "sg": self.sg_version,
            "python": self.py_version,
        }

    def require_rpygrep(self) -> None:
        """Ensure rpygrep is available.

        Raises
        ------
        RuntimeError
            If rpygrep package is not installed.
        """
        if not self.rpygrep_available:
            msg = (
                "rpygrep package is required but not installed. "
                "Install with: uv add rpygrep"
            )
            raise RuntimeError(msg)

    def require_sg(self) -> str:
        """Get ast-grep path, raising if not available."""
        if not self.sg_path:
            msg = (
                "ast-grep (sg) is required but not found. "
                "Install with: pip install ast-grep-py (Python) or cargo install ast-grep (Rust)"
            )
            raise RuntimeError(msg)
        return self.sg_path

    @property
    def has_rpygrep(self) -> bool:
        """Check if rpygrep is available."""
        return self.rpygrep_available

    @property
    def has_sg(self) -> bool:
        """Check if ast-grep is available."""
        return self.sg_path is not None

    # -------------------------------------------------------------------------
    # Deprecated: Backwards compatibility shims (remove after full migration)
    # -------------------------------------------------------------------------

    @property
    def rg_path(self) -> str | None:
        """Deprecated: use has_rpygrep instead.

        Returns path to rg binary for compatibility during migration.
        """
        return shutil.which("rg")

    def require_rg(self) -> str:
        """Deprecated: rpygrep handles rg invocation.

        Provided for backwards compatibility during migration.
        """
        self.require_rpygrep()
        rg = shutil.which("rg")
        if not rg:
            raise RuntimeError("ripgrep (rg) not found on PATH")
        return rg
```

#### Key Architectural Elements

| Element | Description |
|---------|-------------|
| `rpygrep_available` field | Primary indicator for search capability |
| `rpygrep_version` field | Package version for diagnostics |
| `rg_version` field | Still detected for version reporting (rg CLI) |
| `rg_path` field removal | No longer stored; computed on-demand in deprecated shim |
| `require_rpygrep()` method | New primary validation method |
| Backwards compat shims | `rg_path` property and `require_rg()` kept temporarily |

#### Deprecation Targets

| Target | Type | Lines | Action |
|--------|------|-------|--------|
| `rg_path` field | dataclass field | 33 | Remove (replaced with property shim) |
| `require_rg()` | method | 114-133 | Mark deprecated, remove after migration |
| `rg_path` property | property | - | Remove after all callers migrated |

#### Implementation Checklist

- [ ] Add `rpygrep_available: bool` and `rpygrep_version: str | None` fields
- [ ] Remove `rg_path` as stored field (becomes computed property)
- [ ] Update `detect()` to check `importlib.metadata.version("rpygrep")`
- [ ] Add `require_rpygrep()` method
- [ ] Add `has_rpygrep` property
- [ ] Update `to_dict()` to include `rpygrep` version
- [ ] Keep `rg_path` as deprecated property for backwards compatibility
- [ ] Keep `require_rg()` as deprecated method that calls `require_rpygrep()` first
- [ ] Add deprecation docstrings to backwards compat shims
- [ ] Update unit tests for new fields and methods
- [ ] Update any code that accesses `tc.rg_path` directly to use `tc.has_rpygrep`

---

## Post-Migration Cleanup Checklist

After all scope items are complete and verified:

### Files to Modify

| File | Cleanup Action |
|------|----------------|
| `tools/cq/query/executor.py` | Remove `subprocess` import if unused |
| `tools/cq/macros/calls.py` | Remove `_MIN_RG_PARTS`, `subprocess` import |
| `tools/cq/macros/impact.py` | Remove `_RG_SPLIT_PARTS`, `subprocess` import, `suppress` import |
| `tools/cq/macros/sig_impact.py` | Remove `Toolchain` TYPE_CHECKING import if unused |
| `tools/cq/core/toolchain.py` | Remove deprecated `rg_path` property and `require_rg()` method |

### Dependencies to Update

| File | Change |
|------|--------|
| `pyproject.toml` | Add `rpygrep = ">=0.2.1,<0.3"` to dependencies |

### Tests to Add

| Test File | Purpose |
|-----------|---------|
| `tests/unit/cq/search/test_profiles.py` | Test `SearchLimits` dataclass and presets |
| `tests/unit/cq/search/test_timeout.py` | Test timeout wrappers |
| `tests/unit/cq/search/test_adapter.py` | Test adapter functions |
| `tests/integration/cq/test_search_migration.py` | Verify equivalence with old implementation |

### Documentation to Update

| File | Change |
|------|--------|
| `tools/cq/README.md` | Add search profile documentation |
| `.claude/skills/cq/SKILL.md` | Document new search limits options |
| `AGENTS.md` | Update cq skill table if needed |

---

## Verification Commands

After implementation, run these commands to verify:

```bash
# 1. Verify rpygrep is installed
uv run python -c "import rpygrep; print(rpygrep.__version__)"

# 2. Run unit tests for new search module
uv run pytest tests/unit/cq/search/ -v

# 3. Run integration tests
uv run pytest tests/integration/cq/ -v

# 4. Run full cq test suite
uv run pytest tests/unit/cq/ tests/integration/cq/ -v

# 5. Manual smoke test
uv run python -c "from tools.cq.cli_app import main; main()" calls build_graph_product

# 6. Verify no subprocess calls remain (should find 0 for rg)
uv run python -c "from tools.cq.search import find_files_with_pattern; from pathlib import Path; print(find_files_with_pattern(Path('.'), 'subprocess.run.*rg'))"
```

---

## References

- [rpygrep PyPI](https://pypi.org/project/rpygrep/0.2.1/)
- [ripgrep manpage](https://manpages.debian.org/testing/ripgrep/rg.1.en.html)
- [rpygrep library reference](/home/paul/CodeAnatomy/docs/python_library_reference/rpygrep.md)
- [cq architecture](/home/paul/CodeAnatomy/.claude/skills/cq/reference/cq_reference.md)
