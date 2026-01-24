# pygit2 Integration Plan

> **Goal**: Replace GitHub CLI and git CLI subprocess usage with in‑process `pygit2` (libgit2) and expand Git‑aware capabilities (identity, status, diffs, blame, submodules, worktrees) while preserving safe fallbacks for non‑Git directories.

---

## 0) Target Architecture Snapshot

### Git context + identity resolution
```python
# extract/git_context.py
import pygit2

repo_root = pygit2.discover_repository(str(candidate_path))
repo = pygit2.Repository(repo_root)
head_commit = repo.head.peel(pygit2.Commit)
head_sha = str(head_commit.id)
origin_url = repo.remotes["origin"].url if "origin" in repo.remotes else None
```

### Git‑accurate file listing (tracked + untracked, ignore‑aware)
```python
# extract/repo_scan_pygit2.py
status = repo.status()  # honors gitignore
paths = set(status)
paths.update(entry.path for entry in repo.index)
```

### Stable blob reading at a ref
```python
# extract/repo_blobs_git.py
commit = repo.revparse_single(ref).peel(pygit2.Commit)
entry = commit.tree.get(path_posix)
if entry is not None:
    blob = repo[entry.id]
    data = blob.data
```

---

## Scope 1 — Git context, repo discovery, and identity (replace `gh`)

### Why
Centralize repository discovery and identity in pygit2 to remove the GitHub CLI dependency and enable offline / deterministic behavior.

### Representative Code Snippet
```python
# extract/git_context.py
from dataclasses import dataclass
import pygit2

@dataclass(frozen=True)
class GitContext:
    repo: pygit2.Repository
    repo_root: str
    head_sha: str
    origin_url: str | None


def open_git_context(path: str) -> GitContext | None:
    repo_path = pygit2.discover_repository(path)
    if repo_path is None:
        return None
    repo = pygit2.Repository(repo_path)
    head = repo.head.peel(pygit2.Commit)
    origin = repo.remotes["origin"].url if "origin" in repo.remotes else None
    return GitContext(repo=repo, repo_root=repo.workdir or repo.path, head_sha=str(head.id), origin_url=origin)
```

### Target Files
- `src/extract/scip_identity.py` (replace GH CLI with pygit2 identity resolution)
- `src/extract/ast_extract.py` (repo root resolution via pygit2 when available)
- `src/extract/repo_scan.py` (use new git context entrypoint)
- `src/extract/helpers.py` (if shared git context utilities are surfaced)
- **New:** `src/extract/git_context.py`

### Implementation Checklist
- [x] Add `GitContext` helper to centralize repo discovery + HEAD resolution.
- [x] Replace `gh_repo_name_with_owner` / `gh_repo_head_sha` with pygit2 logic.
- [x] Parse `origin_url` to derive `nameWithOwner` when host is github.com; fallback to repo path otherwise.
- [x] Keep explicit overrides (`project_name_override`, `project_version_override`) intact.
- [x] Add feature gating (`pygit2.features`) where remote auth is required (see `extract/git_remotes.py`).

---

## Scope 2 — Git‑accurate file listing (replace `git ls-files`)

### Why
Avoid subprocess usage and get consistent ignore handling via libgit2 status/index.

### Representative Code Snippet
```python
# extract/repo_scan_pygit2.py
import pygit2

status = repo.status()  # includes untracked + respects .gitignore
paths = set(status)
paths.update(entry.path for entry in repo.index)
```

### Target Files
- `src/extract/repo_scan_git.py` (decommission)
- `src/extract/repo_scan.py` (pivot to pygit2 listing, keep FS fallback)
- **New:** `src/extract/repo_scan_pygit2.py`

### Implementation Checklist
- [x] Implement pygit2‑based listing (tracked + untracked) with ignore semantics.
- [x] Preserve existing include/exclude glob filtering and symlink rules.
- [x] Keep filesystem fallback for non‑git directories.
- [x] Add tests for parity vs current `git ls-files` behavior.

---

## Scope 3 — Snapshot‑at‑ref support for blob extraction

### Why
Reading blobs from the object database enables deterministic analysis at a commit (or tag), supports bare repos, and avoids working tree drift.

### Representative Code Snippet
```python
# extract/repo_blobs_git.py
commit = repo.revparse_single(ref).peel(pygit2.Commit)
entry = commit.tree.get(path_posix)
blob = repo[entry.id]
text = blob.data.decode("utf-8", errors="replace")
```

### Target Files
- `src/extract/repo_blobs.py` (add optional `ref` path)
- **New:** `src/extract/repo_blobs_git.py`

### Implementation Checklist
- [x] Add `ref` option (or `read_from_git`) to control blob source.
- [x] Use tree/blob lookup for HEAD or provided ref.
- [x] Preserve current working‑tree path for default behavior.
- [x] Validate size limits and encoding detection remain consistent.

---

## Scope 4 — Incremental diff / commit‑walker integration

### Why
Allow change‑scoped extraction by commit range using libgit2 walkers and diffs; aligns with incremental pipelines.

### Representative Code Snippet
```python
# extract/git_delta.py
walker = repo.walk(new_oid, pygit2.GIT_SORT_TOPOLOGICAL)
walker.hide(old_oid)
for commit in walker:
    diff = repo.diff(commit.parents[0], commit)
```

### Target Files
- `src/incremental/*` (wire commit range detection)
- `src/extract/repo_scan.py` (optional “changed files only” mode)
- **New:** `src/extract/git_delta.py`

### Implementation Checklist
- [x] Add commit‑range inputs to extract path (old/new ref).
- [x] Build changed file list via `repo.diff` or `Tree.diff_to_tree`.
- [x] Plug into repo scanning as an optional mode.
- [x] Wire incremental pipeline settings to supply diff refs automatically.

---

## Scope 5 — Submodules + worktrees awareness

### Why
Avoid missing code when repos use submodules or multiple worktrees.

### Representative Code Snippet
```python
# extract/git_context.py
submodules = repo.listall_submodules()
worktrees = repo.list_worktrees() if hasattr(repo, "list_worktrees") else []
```

### Target Files
- `src/extract/repo_scan.py`
- `src/extract/repo_scan_fs.py` (fallback rules should remain)
- **New:** `src/extract/git_submodules.py`

### Implementation Checklist
- [x] Optionally include submodule working directories in scans.
- [x] Honor `.gitmodules` update semantics via pygit2 submodule API.
- [x] Detect worktrees and avoid double‑scanning.

---

## Scope 6 — Mailmap + blame for author‑aware diagnostics

### Why
If you ever surface author metadata, pygit2 can normalize identities and provide blame data without shelling out.

### Representative Code Snippet
```python
# extract/git_authorship.py
mailmap = pygit2.Mailmap.from_repository(repo)
blame = repo.blame(path_posix)
for hunk in blame:
    author = mailmap.resolve_signature(hunk.final_signature)
```

### Target Files
- **New:** `src/extract/git_authorship.py`
- Optional downstream usage in diagnostics/telemetry emitters

### Implementation Checklist
- [x] Add mailmap resolution utilities.
- [x] Provide optional blame extraction paths (guarded by feature flags).
- [x] Keep this optional to avoid runtime cost when not needed.

---

## Scope 7 — Remote access, callbacks, and feature gating

### Why
If any workflows fetch/clone/push, use pygit2 remotes with explicit callbacks and transport gating.

### Representative Code Snippet
```python
# extract/git_remotes.py
if pygit2.features & pygit2.GIT_FEATURE_SSH:
    remote.fetch(callbacks=cb, prune=pygit2.enums.FetchPrune.PRUNE, depth=1)
```

### Target Files
- **New:** `src/extract/git_remotes.py`
- `src/extract/scip_identity.py` (if remote probing is required)

### Implementation Checklist
- [x] Add credential and certificate callback helpers.
- [x] Gate transport support by `pygit2.features`.
- [x] Keep remote operations optional and explicit.

---

## Scope 8 — Global settings and safety controls

### Why
Centralize ownership validation, cache sizing, and network timeouts at process level.

### Representative Code Snippet
```python
# extract/git_settings.py
import pygit2

settings = pygit2.Settings()
settings.owner_validation = False
settings.server_timeout = 30_000
```

### Target Files
- **New:** `src/extract/git_settings.py`
- `src/engine/runtime_profile.py` (if runtime config is exposed)

### Implementation Checklist
- [x] Add a runtime‑configurable git settings hook.
- [x] Allow safe defaults and explicit overrides.
- [x] Document any security implications of `owner_validation`.

---

## Scope 9 — Pathspec-backed filtering & ignore semantics

### Why
Pathspec gives Git‑faithful ignore behavior and richer include/exclude diagnostics for both pygit2 listings and filesystem fallbacks.

### Representative Code Snippet
```python
# extract/pathspec_filters.py
from pathspec import GitIgnoreSpec, PathSpec

ignore_spec = GitIgnoreSpec.from_lines(gitignore_lines)
include_spec = PathSpec.from_lines("gitwildmatch", include_globs)
exclude_spec = PathSpec.from_lines("gitwildmatch", exclude_globs)

included = include_spec.match_file(path) if include_globs else True
excluded = exclude_spec.match_file(path) if exclude_globs else False
ignored = ignore_spec.match_file(path)
```

### Target Files
- `src/extract/repo_scan.py` (apply pathspec filters to pygit2 paths)
- `src/extract/repo_scan_fs.py` (replace fnmatch filtering with pathspec)
- **New:** `src/extract/pathspec_filters.py`

### Implementation Checklist
- [x] Build a GitIgnoreSpec from `.gitignore`/`.git/info/exclude` when present.
- [x] Apply PathSpec include/exclude filters on pygit2‑derived paths.
- [x] Replace filesystem fallback filtering with `match_tree_entries` / `check_file`.
- [x] Add optional diagnostics for why a path was excluded (CheckResult index).

---

## Decommission / Delete List

### Files to remove
- `src/extract/repo_scan_git.py` (git CLI wrapper)

### Functions to remove
- `_run_gh`, `gh_repo_name_with_owner`, `gh_repo_head_sha` in `src/extract/scip_identity.py`
- `_matches_any_glob` and `fnmatch`-based filtering helpers in `src/extract/repo_scan_fs.py`

### External dependency removal
- GitHub CLI reliance in SCIP identity resolution

---

## Execution Order (Recommended)
1) Scope 1 (GitContext + SCIP identity via pygit2).
2) Scope 2 (repo file listing via pygit2; delete git CLI path).
3) Scope 9 (pathspec filters for pygit2 + filesystem fallback).
4) Scope 3 (blob reads from ODB at ref).
5) Scope 4 (commit walker + diff integration for incremental).
6) Scope 5 (submodule + worktree handling).
7) Scope 6 (mailmap + blame support).
8) Scope 7 (remote access + feature gating).
9) Scope 8 (settings control plane).

---

## Completion Criteria
- No GitHub CLI or git CLI subprocess calls remain in extraction.
- Repo scanning and identity resolution are pygit2‑first with safe fallbacks.
- Pathspec provides Git‑faithful ignore handling for non‑git fallbacks and additional include/exclude policy filters.
- Deterministic “analysis at ref” is supported for blob extraction.
- Incremental diff‑based scanning is available as an opt‑in path.
- Submodules/worktrees are handled without double‑scanning.
- Mailmap/blame and remote features are available behind explicit flags.
- Global pygit2 settings are centrally configured and documented.
