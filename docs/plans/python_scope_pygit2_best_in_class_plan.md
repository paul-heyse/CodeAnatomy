# Python Scope via pygit2 Best-in-Class Plan

> **Goal**: Make the extraction basis *always* the Python file scope of the target git directory, computed on a single canonical basis using `pygit2`, while extending scope to cover external Python interfaces (imports outside the repo) with zero gaps. This is a design-phase, breaking-change target state.

---

## 0) Target Architecture Snapshot

### Canonical repo scope resolution (pygit2-only)
```python
# extract/repo_scope.py
from dataclasses import dataclass
from pathlib import Path
import pygit2

@dataclass(frozen=True)
class RepoScope:
    repo: pygit2.Repository
    repo_root: Path
    python_extensions: frozenset[str]
    include_untracked: bool
    include_submodules: bool
    include_worktrees: bool


def resolve_repo_scope(path: Path, options: "RepoScopeOptions") -> RepoScope:
    git_ctx = open_git_context(path)
    if git_ctx is None or git_ctx.repo.workdir is None:
        msg = "Path is not inside a git workdir; repo scope is required."
        raise ValueError(msg)
    python_exts = resolve_python_extensions(git_ctx.repo_root, options.python_scope)
    return RepoScope(
        repo=git_ctx.repo,
        repo_root=git_ctx.repo_root.resolve(),
        python_extensions=python_exts,
        include_untracked=options.include_untracked,
        include_submodules=options.include_submodules,
        include_worktrees=options.include_worktrees,
    )
```

### Pathspec scope rules (include/exclude + diagnostics)
```python
# extract/scope_rules.py
from dataclasses import dataclass
from pathspec import PathSpec
from pathspec.util import detailed_match_files, normalize_file

@dataclass(frozen=True)
class ScopeRuleSet:
    include_spec: PathSpec
    exclude_spec: PathSpec


def build_scope_rules(*, include_lines: list[str], exclude_lines: list[str]) -> ScopeRuleSet:
    include_spec = PathSpec.from_lines("gitwildmatch", include_lines)
    exclude_spec = PathSpec.from_lines("gitwildmatch", exclude_lines)
    return ScopeRuleSet(include_spec=include_spec, exclude_spec=exclude_spec)


def check_scope_path(rel_path: str, rules: ScopeRuleSet) -> tuple[bool, int | None, int | None]:
    normalized = normalize_file(rel_path)
    include_result = rules.include_spec.check_file(normalized)
    exclude_result = rules.exclude_spec.check_file(normalized)
    include = include_result.include is True and exclude_result.include is not True
    return include, include_result.index, exclude_result.index


def explain_scope_paths(paths: list[str], rules: ScopeRuleSet) -> dict[str, object]:
    include_detail = detailed_match_files(rules.include_spec.patterns, paths)
    exclude_detail = detailed_match_files(rules.exclude_spec.patterns, paths)
    return {"include": include_detail, "exclude": exclude_detail}
```

### Git-accurate Python file listing (tracked + untracked, ignore-aware)
```python
# extract/repo_scan_pygit2.py
from pathlib import Path
import pygit2


def iter_repo_python_paths(scope: RepoScope, rules: ScopeRuleSet) -> list[Path]:
    flags = pygit2.GIT_STATUS_OPT_DISABLE_PATHSPEC_MATCH
    if scope.include_untracked:
        flags |= pygit2.GIT_STATUS_OPT_INCLUDE_UNTRACKED
        flags |= pygit2.GIT_STATUS_OPT_RECURSE_UNTRACKED_DIRS
    status = scope.repo.status(
        options=pygit2.StatusOptions(
            show=pygit2.GIT_STATUS_SHOW_INDEX_AND_WORKDIR,
            flags=flags,
        )
    )
    paths = set(status)
    paths.update(entry.path for entry in scope.repo.index)
    results: list[Path] = []
    for rel_posix in paths:
        if scope.repo.path_is_ignored(rel_posix):
            continue
        include, _include_idx, _exclude_idx = check_scope_path(rel_posix, rules)
        if not include:
            continue
        results.append(Path(rel_posix))
    return results
```

### External interface scope (imports outside repo)
```python
# extract/python_external_scope.py
def resolve_external_interfaces(
    imports: Iterable[str],
    *,
    repo_root: Path,
    env: PythonEnvProfile,
) -> Iterable[ExternalInterface]:
    for name in imports:
        spec = importlib.util.find_spec(name)
        if spec is None or spec.origin is None:
            yield ExternalInterface(name=name, status="unresolved")
            continue
        origin = Path(spec.origin)
        if _is_within_repo(origin, repo_root):
            continue
        yield ExternalInterface(
            name=name,
            status="resolved",
            origin=origin,
            dist=env.distribution_for_path(origin),
            is_stdlib=env.is_stdlib(origin),
        )
```

---

## Scope 1 — Canonical Git Repo Scope (pygit2-only, no filesystem fallback)

### Why
We must ensure scope is derived from a single, deterministic git basis and always reflects what `git add -A` would stage (tracked + untracked, ignore-aware). This removes mismatches between pygit2 and filesystem walks and eliminates repo root ambiguity.

### Representative Code Snippet
```python
# extract/repo_scan.py
scope = resolve_repo_scope(repo_root_path, options.scope)
paths = iter_repo_python_paths(scope, rules)
for rel in sorted(paths, key=lambda p: p.as_posix()):
    row = _build_repo_file_row(rel=rel, repo_root=scope.repo_root, options=options)
    if row is not None:
        yield row
```

### Target Files to Modify
- `src/extract/repo_scan.py`
- `src/extract/repo_scan_pygit2.py`
- `src/extract/git_context.py`
- `src/extract/git_submodules.py`
- **New**: `src/extract/repo_scope.py`

### Code/Modules to Delete (in this scope)
- None in this scope (deletions deferred until final scope item).

### Implementation Checklist
- [ ] Add `RepoScopeOptions` + `RepoScope` in `src/extract/repo_scope.py`.
- [ ] Make repo discovery mandatory via `pygit2.discover_repository`.
- [ ] Replace `iter_repo_files_fs` fallback with pygit2-only listing.
- [ ] Ensure untracked inclusion uses `pygit2.StatusOptions` (include untracked, recurse).
- [ ] Enforce `repo.path_is_ignored` as the ignore decision (no custom gitignore parsing).
- [ ] Normalize `repo_root` to pygit2 workdir and reject bare repos unless explicitly supported.

---

## Scope 2 — Pathspec Scope RuleSet + Diagnostics

### Why
We need a deterministic, composable rule system for include/exclude behavior and a way to explain *why* each path is in or out of scope. Pathspec provides check results, match indices, and full pattern traceability.

### Representative Code Snippet
```python
# extract/scope_rules.py
include_lines = globs_for_extensions(scope.python_extensions)
exclude_lines = list(scope_policy.exclude_globs)
rule_set = build_scope_rules(include_lines=include_lines, exclude_lines=exclude_lines)

include, include_index, exclude_index = check_scope_path(rel_posix, rule_set)
if include:
    manifest.add(
        path=rel_posix,
        include_index=include_index,
        exclude_index=exclude_index,
    )
```

### Target Files to Modify
- `src/extract/repo_scan.py`
- `src/extract/pathspec_filters.py` (refactor into rule-set layer or supersede)
- `src/extract/scope_manifest.py`
- **New**: `src/extract/scope_rules.py`

### Code/Modules to Delete (in this scope)
- Deprecated helpers in `src/extract/pathspec_filters.py` once superseded by `scope_rules.py`.

### Implementation Checklist
- [ ] Introduce `ScopeRuleSet` built from `PathSpec.from_lines("gitwildmatch", ...)`.
- [ ] Use `check_file()` for include/exclude decisions and store indices in the manifest.
- [ ] Add rule-set composition order (defaults → repo config → user overrides).
- [ ] Add optional `detailed_match_files()` diagnostics for telemetry/debug output.
- [ ] Normalize all paths with `normalize_file()` before matching.

---

## Scope 3 — Python Extension Registry (dynamic, repo-aware)

### Why
We must include *all* Python file extensions used by the project and treat them as part of scope. Hard-coded extensions create gaps, and extension lists must be shared across extractors.

### Representative Code Snippet
```python
# extract/python_scope.py
DEFAULT_PY_EXTS = {".py", ".pyi", ".pyx", ".pxd", ".pxi", ".pyw"}


def resolve_python_extensions(repo_root: Path, policy: PythonScopePolicy) -> frozenset[str]:
    configured = _extensions_from_config(repo_root)
    discovered = _extensions_from_repo(repo_root)
    return frozenset(DEFAULT_PY_EXTS | configured | discovered | set(policy.extra_extensions))


def globs_for_extensions(exts: Iterable[str]) -> list[str]:
    return [f"**/*{ext}" for ext in sorted(exts)]
```

### Target Files to Modify
- `src/extract/repo_scan.py`
- `src/extract/tree_sitter_extract.py`
- `src/datafusion_engine/extract_templates.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/dataloaders.py`
- **New**: `src/extract/python_scope.py`

### Code/Modules to Delete (in this scope)
- Hard-coded Python extension lists in:
  - `src/extract/tree_sitter_extract.py`
  - `src/datafusion_engine/extract_templates.py`

### Implementation Checklist
- [ ] Add `PythonScopePolicy` to allow explicit extension overrides.
- [ ] Resolve extension list from config (`pyproject.toml`, `setup.cfg`, `mypy.ini`) and repo contents.
- [ ] Build pathspec include rules from `globs_for_extensions()`.
- [ ] Store the resolved extension list in a new manifest dataset (`python_extensions_v1`).
- [ ] Ensure extractors consume the same extension list (repo scan, tree-sitter, CST filters).

---

## Scope 4 — External Interface Scope (imports outside repo)

### Why
Imports into the project must have zero gaps, even when they resolve outside the git repository. External interfaces must be captured as a first-class scope, without restricting SCIP documents.

### Representative Code Snippet
```python
# extract/python_external_scope.py
@dataclass(frozen=True)
class ExternalInterface:
    name: str
    status: str
    origin: Path | None = None
    dist_name: str | None = None
    dist_version: str | None = None
    is_stdlib: bool = False


def build_external_interface_manifest(imports: Iterable[str], env: PythonEnvProfile) -> list[ExternalInterface]:
    items: list[ExternalInterface] = []
    for name in imports:
        spec = importlib.util.find_spec(name)
        if spec is None or spec.origin is None:
            items.append(ExternalInterface(name=name, status="unresolved"))
            continue
        origin = Path(spec.origin)
        dist = env.distribution_for_path(origin)
        items.append(
            ExternalInterface(
                name=name,
                status="resolved",
                origin=origin,
                dist_name=dist.name if dist else None,
                dist_version=dist.version if dist else None,
                is_stdlib=env.is_stdlib(origin),
            )
        )
    return items
```

### Target Files to Modify
- `src/extract/ast_extract.py` (standardize import rows)
- `src/extract/cst_extract.py` (standardize import rows)
- `src/extract/tree_sitter_extract.py` (standardize import rows)
- `src/datafusion_engine/extract_extractors.py` (register new external interface extractor)
- **New**: `src/extract/python_external_scope.py`
- **New**: `src/extract/python_env_profile.py`

### Code/Modules to Delete (in this scope)
- None in this scope.

### Implementation Checklist
- [ ] Create a unified import table (`python_imports_v1`) fed by AST/CST/tree-sitter.
- [ ] Add `python_external_interfaces_v1` derived from imports + environment resolution.
- [ ] Explicitly mark external interfaces (stdlib vs site-packages vs path-based modules).
- [ ] Keep SCIP extraction independent and unfiltered by repo scope.
- [ ] Add policy flags for external interface extraction depth (metadata-only vs full parse).

---

## Scope 5 — Scope Manifest + Worklist Enforcement

### Why
Worklist generation and `file_contexts` must not bypass the canonical scope. A central scope manifest becomes the single source of truth for in-scope files and reasons.

### Representative Code Snippet
```python
# extract/worklists.py
if file_contexts is not None:
    contexts = list(file_contexts)
else:
    contexts = list(_worklist_stream(...))

if scope_manifest is not None:
    allowed = scope_manifest.allowed_paths()
    contexts = [ctx for ctx in contexts if ctx.path in allowed]

yield from contexts
```

### Target Files to Modify
- `src/extract/worklists.py`
- `src/extract/helpers.py`
- `src/extract/repo_scan.py`
- `src/datafusion_engine/extract_extractors.py`
- `src/extract/scope_manifest.py`

### Code/Modules to Delete (in this scope)
- None in this scope.

### Implementation Checklist
- [ ] Introduce `scope_manifest_v1` with per-path scope metadata and reasons.
- [ ] Store `include_index` / `exclude_index` from pathspec in the manifest.
- [ ] Intersect `file_contexts` with the scope manifest in `iter_worklist_contexts`.
- [ ] Ensure all extractors accept a `scope_manifest` via `ExtractExecutionContext`.
- [ ] Add explicit `scope_kind` (repo/external) for downstream consumers.
- [ ] Confirm SCIP extraction ignores the scope manifest entirely.

---

## Scope 6 — Deterministic Caching + Scope Telemetry

### Why
Scope must be cacheable and deterministic, including untracked files. We also need visibility into scope changes (added/removed files, ignore behavior, external interface resolution rate).

### Representative Code Snippet
```python
# extract/repo_scan.py
scope_hash = stable_cache_key(
    "repo_scope",
    {
        "repo_root": str(scope.repo_root),
        "python_extensions": sorted(scope.python_extensions),
        "rule_set": scope_rules.signature(),
        "paths": [rel.as_posix() for rel in sorted(paths)],
    },
)
```

### Target Files to Modify
- `src/extract/repo_scan.py`
- `src/extract/cache_utils.py`
- `src/extract/evidence_plan.py`
- `src/obs/scan_telemetry.py`
- `src/extract/scope_rules.py`

### Code/Modules to Delete (in this scope)
- None in this scope.

### Implementation Checklist
- [ ] Add scope hash computation based on paths + extension set + rule-set signature.
- [ ] Include scope hash in cache keys and scan telemetry artifacts.
- [ ] Emit `repo_scope_stats_v1` (counts, ignored, untracked, external).
- [ ] Add diagnostics for unresolved external interfaces.
- [ ] Store pathspec match counts and top patterns in telemetry.

---

## Scope 7 — Breaking API + Config Reshape

### Why
To make scope canonical and extensible, configuration must pivot to explicit scope policies instead of ad-hoc include/exclude globs.

### Representative Code Snippet
```python
# hamilton_pipeline/pipeline_types.py
@dataclass(frozen=True)
class RepoScopeConfig:
    repo_root: str
    include_untracked: bool = True
    python_extensions: tuple[str, ...] = ()
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    include_submodules: bool = False
    include_worktrees: bool = False
    external_interface_depth: str = "metadata"
```

### Target Files to Modify
- `src/hamilton_pipeline/pipeline_types.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/dataloaders.py`
- `src/datafusion_engine/extract_templates.py`
- `src/extract/repo_scan.py`
- `src/extract/scope_rules.py`

### Code/Modules to Delete (in this scope)
- Old config fields that directly set include/exclude globs once `RepoScopeConfig` is adopted.

### Implementation Checklist
- [ ] Introduce `RepoScopeConfig` and migrate callers.
- [ ] Deprecate direct `include_globs` overrides in pipeline inputs.
- [ ] Add explicit policy ordering (defaults → repo config → user overrides).
- [ ] Add clear validation errors for non-git paths or bare repos.
- [ ] Ensure all defaults are expressed via the new policy objects.

---

## Scope 8 — Deferred Deletions (post-completion cleanup)

### Why
Some legacy files and helpers should be removed only after the new scope system is fully adopted and validated.

### Representative Code Snippet
```python
# final cleanup: remove filesystem fallback and manual gitignore parsing
```

### Target Files to Modify
- `src/extract/repo_scan.py`
- `src/extract/pathspec_filters.py`

### Code/Modules to Delete (after all prior scopes complete)
- `src/extract/repo_scan_fs.py`
- Legacy helpers in `src/extract/pathspec_filters.py` that parse `.gitignore`
- Any remaining hard-coded include_globs defaults (superseded by `python_scope.py`)
- Any direct worklist bypass that skips scope manifest enforcement

### Implementation Checklist
- [ ] Remove filesystem scanning fallback completely.
- [ ] Delete manual gitignore parsing (rely on libgit2 ignore rules only).
- [ ] Replace pathspec_filters with scope_rules (or delete if fully superseded).
- [ ] Purge legacy include_globs defaults and stale config wiring.
- [ ] Audit extractors for any remaining scope bypass paths.

---

## Notes and Explicit Constraints
- Scope must include untracked files that would be staged by `git add -A`.
- Python extension coverage must be dynamic and complete.
- Pathspec rule sets provide include/exclude diagnostics; git ignore truth stays in pygit2.
- External interfaces must be captured even when outside repo; SCIP must remain independent.
- This plan assumes breaking changes are acceptable (design-phase target state).
