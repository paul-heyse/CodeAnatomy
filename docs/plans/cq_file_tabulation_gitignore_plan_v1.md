# CQ File Tabulation + Gitignore Integration Plan (v1)

## Goal

Unify CQ file tabulation and path normalization using pygit2 for repository discovery and pathspec (GitIgnoreSpec) for Git-faithful ignore semantics. The resulting pipeline must include untracked files that are *likely to be tracked* (i.e., not ignored by Git rules), and must **not** introduce a tracked-only mode.

## Non-Goals

- No tracked-only option at the file level.
- No behavioral changes outside CQ file tabulation, path normalization, and reporting/diagnostics of filters.
- No dependency on `git` CLI in production codepaths (pygit2 and pathspec only).

## Scope Item 1: Repository Root + Workdir Resolution (pygit2)

**Intent:** Make CQ’s root/workdir discovery authoritative and Git-faithful, so all downstream paths are normalized against the true repository root.

**Key changes**
- Introduce a single repo discovery utility that:
  - resolves the Git repository root and workdir via pygit2,
  - distinguishes bare vs non-bare repos,
  - provides a normalized `repo_root: Path` used by CQ.

**Representative snippet**

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pygit2


@dataclass(frozen=True)
class RepoContext:
    """Resolved git repository context."""

    repo_root: Path
    workdir: Path
    is_bare: bool


def resolve_repo_context(start: Path) -> RepoContext:
    """Resolve repo root/workdir using pygit2."""
    discovered = pygit2.discover_repository(str(start))
    repo = pygit2.Repository(discovered)
    workdir = Path(repo.workdir) if repo.workdir else Path(repo.path).parent
    repo_root = workdir.resolve()
    return RepoContext(
        repo_root=repo_root,
        workdir=workdir.resolve(),
        is_bare=repo.is_bare,
    )
```

**Target files**
- `tools/cq/index/` (new module for repo discovery)
- `tools/cq/cli.py` (wire root resolution into CLI)
- `tools/cq/query/executor.py` (consume normalized root)

**Deprecate / delete after completion**
- Any ad-hoc repo root inference in CQ (if present in `tools/cq/cli.py` or helpers).

**Implementation checklist**
- [ ] Add `RepoContext` and `resolve_repo_context(...)`.
- [ ] Replace implicit `Path.cwd()` repo root usage with resolved root.
- [ ] Ensure all CQ entrypoints use the resolved root consistently.

---

## Scope Item 2: Git-Faithful Ignore Compilation (pathspec GitIgnoreSpec)

**Intent:** Use GitIgnoreSpec to match Git’s actual ignore behavior, ensuring “likely to be tracked” untracked files are included.

**Key changes**
- Parse ignore patterns from:
  - `.gitignore` files (root and nested),
  - `.git/info/exclude`,
  - global excludes (from git config, if accessible via pygit2 config).
- Compile into `GitIgnoreSpec` and apply to candidate file lists.

**Representative snippet**

```python
from __future__ import annotations

from pathlib import Path

from pathspec import GitIgnoreSpec


def load_gitignore_spec(repo_root: Path) -> GitIgnoreSpec:
    """Build a GitIgnoreSpec from gitignore sources."""
    patterns: list[str] = []
    for ignore_path in _iter_gitignore_files(repo_root):
        patterns.extend(ignore_path.read_text(encoding="utf-8").splitlines())
    patterns.extend(_load_git_info_exclude(repo_root))
    patterns.extend(_load_global_excludes(repo_root))
    return GitIgnoreSpec.from_lines(patterns)
```

**Target files**
- `tools/cq/index/gitignore.py` (new)
- `tools/cq/index/files.py` (new)

**Deprecate / delete after completion**
- Any CQ-specific ignore filters that do not use GitIgnoreSpec.

**Implementation checklist**
- [ ] Implement `_iter_gitignore_files(...)` walking repo tree with caching.
- [ ] Implement `load_gitignore_spec(...)` with GitIgnoreSpec.
- [ ] Implement `.git/info/exclude` support.
- [ ] Implement global excludes via pygit2 config (if available).

---

## Scope Item 3: Unified File Tabulation Pipeline (Tracked + Likely-to-be-Tracked Untracked)

**Intent:** Enumerate tracked files **and** untracked-but-not-ignored files to represent the next `git add -A` surface.

**Key changes**
- Combine:
  - tracked files from pygit2 index,
  - untracked candidates from filesystem walk,
  - filter untracked by GitIgnoreSpec.
- Ensure the merged set is deterministic and repo-relative.

**Representative snippet**

```python
from __future__ import annotations

from pathlib import Path

import pygit2


def enumerate_repo_files(repo_root: Path, spec: GitIgnoreSpec) -> list[str]:
    """Return repo-relative POSIX paths for tracked + likely-to-be-tracked files."""
    repo = pygit2.Repository(str(repo_root / ".git"))
    tracked = {entry.path for entry in repo.index}

    untracked = set()
    for path in repo_root.rglob("*.py"):
        rel = path.relative_to(repo_root).as_posix()
        if rel in tracked:
            continue
        if spec.match_file(rel):
            continue
        untracked.add(rel)

    return sorted(tracked | untracked)
```

**Target files**
- `tools/cq/index/files.py` (new, main enumeration API)
- `tools/cq/query/sg_parser.py` (consume file list for scans)
- `tools/cq/query/executor.py` (propagate file list into scan contexts)

**Deprecate / delete after completion**
- Direct filesystem `rglob("*.py")` usage in CQ where it ignores gitignore.

**Implementation checklist**
- [ ] Build `enumerate_repo_files(...)`.
- [ ] Ensure file list is deterministic and repo-relative.
- [ ] Replace ad-hoc file list generation in CQ query execution with the unified API.

---

## Scope Item 4: Path Normalization + Anchor Hygiene

**Intent:** Normalize **all** file paths to repo-relative POSIX strings at ingestion, so anchors and cache keys are consistent and goldens are stable.

**Key changes**
- Normalize all match paths from:
  - ast-grep scan output,
  - inline rules output,
  - cached records.
- Ensure anchors use repo-relative POSIX paths.

**Representative snippet**

```python
from __future__ import annotations

from pathlib import Path


def normalize_repo_path(file_path: str, repo_root: Path) -> str:
    """Convert absolute paths to repo-relative POSIX paths."""
    path = Path(file_path)
    if path.is_absolute():
        return path.relative_to(repo_root).as_posix()
    return path.as_posix()
```

**Target files**
- `tools/cq/query/sg_parser.py`
- `tools/cq/query/executor.py`
- `tools/cq/index/sqlite_cache.py` (record serialization)

**Deprecate / delete after completion**
- Any path normalization scattered across CQ modules that diverges from repo-relative POSIX form.

**Implementation checklist**
- [ ] Normalize all `SgRecord.file` values at parse/cache boundaries.
- [ ] Normalize inline rule match file paths.
- [ ] Ensure anchor file paths are repo-relative for all findings.

---

## Scope Item 5: CQ Scope + Globs Integration with Git Ignore

**Intent:** Make CQ scope filters and globs apply **after** Git-ignore filtering, avoiding false excludes and making queries consistent with Git behavior.

**Key changes**
- Apply CQ globs to the post-gitignore file list.
- Keep exclude semantics intact and deterministic.

**Representative snippet**

```python
def filter_with_globs(files: list[str], globs: list[str]) -> list[str]:
    """Apply CQ include/exclude globs to repo-relative file paths."""
    return [f for f in files if _matches_globs(f, globs)]
```

**Target files**
- `tools/cq/index/files.py`
- `tools/cq/query/sg_parser.py`

**Deprecate / delete after completion**
- Independent glob filtering implemented outside the unified file pipeline.

**Implementation checklist**
- [ ] Route all CQ file selection through the unified pipeline.
- [ ] Ensure `globs=` works consistently across query modes.

---

## Scope Item 6: Explainable Filtering Diagnostics

**Intent:** Provide a diagnostics path for “why a file was excluded,” using pathspec `check_*` results.

**Key changes**
- Add optional debug output to show:
  - ignore rule index,
  - include/exclude decision,
  - CQ glob effect.

**Representative snippet**

```python
from pathspec import GitIgnoreSpec


def explain_exclusion(spec: GitIgnoreSpec, rel_path: str) -> dict[str, object]:
    """Return structured ignore decision for diagnostics."""
    result = spec.check_file(rel_path)
    return {
        "file": rel_path,
        "ignored": bool(result.include is False),
        "rule_index": result.index,
    }
```

**Target files**
- `tools/cq/index/files.py`
- `tools/cq/cli.py` (optional debug flag wiring)

**Deprecate / delete after completion**
- N/A (new diagnostics only).

**Implementation checklist**
- [ ] Add diagnostics helper (non-default).
- [ ] Add CLI flag to emit diagnostics.

---

## Scope Item 7: Cache Key + Index Cache Alignment

**Intent:** Align cache keys with normalized paths and updated file tabulation to prevent stale path mismatches.

**Key changes**
- Ensure cache keys are built from normalized file lists and repo-relative paths.
- Enforce no caching for deleted/nonexistent files.

**Representative snippet**

```python
def cache_key_files(files: list[str]) -> str:
    """Stable cache key input for file lists."""
    return "\n".join(sorted(files))
```

**Target files**
- `tools/cq/index/sqlite_cache.py`
- `tools/cq/index/query_cache.py`

**Deprecate / delete after completion**
- Any cache key inputs derived from absolute paths.

**Implementation checklist**
- [ ] Normalize cache records to repo-relative paths.
- [ ] Guard cache writes for missing files.

---

## Validation Plan (post-implementation)

- Run CQ test suite:
  - `uv run pytest tests/unit/cq tests/e2e/cq`
- Validate anchors are repo-relative in JSON output.
- Validate untracked-but-not-ignored files appear in CQ results.
- Ensure globs and `in=` scope still filter correctly on normalized paths.

## Target Outcome

CQ file tabulation becomes Git-faithful, deterministic, and consistent with “next git add -A” semantics, while maintaining repo-relative path references across all outputs and caches. No tracked‑only mode is introduced. 
