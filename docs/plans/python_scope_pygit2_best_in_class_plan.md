# Python Scope via pygit2 Best-in-Class Plan

> **Goal**: Make the extraction basis *always* the Python file scope of the target git directory, computed on a single canonical basis using `pygit2`, while extending scope to cover external Python interfaces (imports outside the repo) with zero gaps. This is a design-phase, breaking-change target state.
>
> **Status (as of January 29, 2026)**: Implemented. All scope checklists below are now complete; deviations from the original sketch are called out inline where relevant.

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
    extension_catalog: PythonExtensionCatalog
    include_untracked: bool
    include_submodules: bool
    include_worktrees: bool
    follow_symlinks: bool


def resolve_repo_scope(path: Path, options: "RepoScopeOptions") -> RepoScope:
    git_ctx = open_git_context(path)
    if git_ctx is None or git_ctx.repo.workdir is None:
        msg = "Path is not inside a git workdir; repo scope is required."
        raise ValueError(msg)
    catalog = resolve_python_extension_catalog(
        git_ctx.repo_root,
        options.python_scope,
        repo=git_ctx.repo,
    )
    return RepoScope(
        repo=git_ctx.repo,
        repo_root=git_ctx.repo_root.resolve(),
        python_extensions=catalog.extensions,
        extension_catalog=catalog,
        include_untracked=options.include_untracked,
        include_submodules=options.include_submodules,
        include_worktrees=options.include_worktrees,
        follow_symlinks=options.follow_symlinks,
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
    include_spec: PathSpec | None
    exclude_spec: PathSpec | None
    include_lines: tuple[str, ...]
    exclude_lines: tuple[str, ...]


def build_scope_rules(*, include_lines: list[str], exclude_lines: list[str]) -> ScopeRuleSet:
    include_spec = PathSpec.from_lines("gitwildmatch", include_lines) if include_lines else None
    exclude_spec = PathSpec.from_lines("gitwildmatch", exclude_lines) if exclude_lines else None
    return ScopeRuleSet(
        include_spec=include_spec,
        exclude_spec=exclude_spec,
        include_lines=tuple(include_lines),
        exclude_lines=tuple(exclude_lines),
    )


def check_scope_path(rel_path: str, rules: ScopeRuleSet) -> ScopeRuleDecision:
    normalized = normalize_file(rel_path)
    include = True
    include_index = None
    exclude_index = None
    if rules.include_spec is not None:
        include_result = rules.include_spec.check_file(normalized)
        include_index = include_result.index
        include = include_result.include is True
    if include and rules.exclude_spec is not None:
        exclude_result = rules.exclude_spec.check_file(normalized)
        exclude_index = exclude_result.index
        if exclude_result.include is True:
            include = False
    return ScopeRuleDecision(
        include=include,
        include_index=include_index,
        exclude_index=exclude_index,
        normalized_path=normalized,
    )


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
    status_flags = repo_status_paths(scope)
    paths = set(iter_repo_candidate_paths(scope))
    results: list[Path] = []
    for rel_posix in paths:
        if scope.repo.path_is_ignored(rel_posix):
            continue
        decision = check_scope_path(rel_posix, rules)
        if not decision.include:
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

**Status**: Complete.

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
- [x] Add `RepoScopeOptions` + `RepoScope` in `src/extract/repo_scope.py`.
- [x] Make repo discovery mandatory via `pygit2.discover_repository`.
- [x] Replace `iter_repo_files_fs` fallback with pygit2-only listing.
- [x] Ensure untracked inclusion uses `pygit2.StatusOptions` (include untracked, recurse).
- [x] Enforce `repo.path_is_ignored` as the ignore decision (no custom gitignore parsing).
- [x] Normalize `repo_root` to pygit2 workdir and reject bare repos unless explicitly supported.

---

## Scope 2 — Pathspec Scope RuleSet + Diagnostics

**Status**: Complete.

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
- **Deleted**: `src/extract/pathspec_filters.py` (fully superseded by `scope_rules.py`).

### Implementation Checklist
- [x] Introduce `ScopeRuleSet` built from `PathSpec.from_lines("gitwildmatch", ...)`.
- [x] Use `check_file()` for include/exclude decisions and store indices in the manifest.
- [x] Add rule-set composition order (defaults → repo config → user overrides).
- [x] Add optional `detailed_match_files()` diagnostics for telemetry/debug output.
- [x] Normalize all paths with `normalize_file()` before matching.

---

## Scope 3 — Python Extension Registry (dynamic, repo-aware)

**Status**: Complete.

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
- [x] Add `PythonScopePolicy` to allow explicit extension overrides.
- [x] Resolve extension list from config (`pyproject.toml`, `setup.cfg`, `mypy.ini`) and repo contents.
- [x] Build pathspec include rules from `globs_for_extensions()`.
- [x] Store the resolved extension list in a new manifest dataset (`python_extensions_v1`).
- [x] Ensure extractors consume the same extension list (repo scan, tree-sitter, CST filters).

---

## Scope 4 — External Interface Scope (imports outside repo)

**Status**: Complete.

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
- [x] Create a unified import *view* (`python_imports`) fed by AST/CST/tree-sitter import views.
- [x] Add `python_external_interfaces_v1` derived from imports + environment resolution.
- [x] Explicitly mark external interfaces (stdlib vs site-packages vs path-based modules).
- [x] Keep SCIP extraction independent and unfiltered by repo scope.
- [x] Add policy flags for external interface extraction depth (metadata-only vs full parse).

---

## Scope 5 — Scope Manifest + Worklist Enforcement

**Status**: Complete.

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
- [x] Introduce `scope_manifest_v1` with per-path scope metadata and reasons.
- [x] Store `include_index` / `exclude_index` from pathspec in the manifest.
- [x] Intersect `file_contexts` with the scope manifest in `iter_worklist_contexts`.
- [x] Ensure all extractors accept a `scope_manifest` via `ExtractExecutionContext`.
- [x] Add explicit `scope_kind` (repo/external) for downstream consumers.
- [x] Confirm SCIP extraction ignores the scope manifest entirely.

---

## Scope 6 — Deterministic Caching + Scope Telemetry

**Status**: Complete.

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
- `src/extract/scope_rules.py`
- `src/extract/python_external_scope.py`

### Code/Modules to Delete (in this scope)
- None in this scope.

### Implementation Checklist
- [x] Add scope hash computation based on paths + extension set + rule-set signature.
- [x] Include scope hash in cache keys and scan telemetry artifacts.
- [x] Emit `repo_scope_stats_v1` (counts, ignored, untracked).
- [x] Add diagnostics for unresolved external interfaces via `python_external_stats_v1`.
- [x] Store pathspec match counts and top patterns in telemetry.

---

## Scope 7 — Breaking API + Config Reshape

**Status**: Complete.

### Why
To make scope canonical and extensible, configuration must pivot to explicit scope policies instead of ad-hoc include/exclude globs.

### Representative Code Snippet
```python
# hamilton_pipeline/pipeline_types.py
@dataclass(frozen=True)
class RepoScopeConfig:
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
- [x] Introduce `RepoScopeConfig` and migrate callers.
- [x] Deprecate direct `include_globs` overrides in pipeline inputs.
- [x] Add explicit policy ordering (defaults → repo config → user overrides).
- [x] Add clear validation errors for non-git paths or bare repos.
- [x] Ensure all defaults are expressed via the new policy objects.

---

## Scope 8 — Deferred Deletions (post-completion cleanup)

**Status**: Complete.

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
- [x] Remove filesystem scanning fallback completely.
- [x] Delete manual gitignore parsing (rely on libgit2 ignore rules only).
- [x] Replace pathspec_filters with scope_rules (or delete if fully superseded).
- [x] Purge legacy include_globs defaults and stale config wiring.
- [x] Audit extractors for any remaining scope bypass paths.

---

## Notes and Explicit Constraints
- Scope must include untracked files that would be staged by `git add -A`.
- Python extension coverage must be dynamic and complete.
- Pathspec rule sets provide include/exclude diagnostics; git ignore truth stays in pygit2.
- External interfaces must be captured even when outside repo; SCIP must remain independent.
- This plan assumes breaking changes are acceptable (design-phase target state).

---

## Scope 9 — Materialize `python_imports_v1` (physical dataset)

### Why
The `python_imports` view is convenient but recomputed and ephemeral. A physical dataset provides a stable, cacheable import graph for reuse across runs, better diffability, and simpler downstream consumption.

### Representative Code Snippet
```python
# extract/python_imports_extract.py
@dataclass(frozen=True)
class PythonImportsExtractResult:
    """Result for python import extraction."""

    python_imports: TableLike


def extract_python_imports(
    ast_imports: TableLike | None = None,
    cst_imports: TableLike | None = None,
    ts_imports: TableLike | None = None,
) -> PythonImportsExtractResult:
    frames = [frame for frame in (ast_imports, cst_imports, ts_imports) if frame is not None]
    if not frames:
        return PythonImportsExtractResult(python_imports=_empty_imports_table())
    unified = _union_import_tables(frames)
    return PythonImportsExtractResult(python_imports=unified)
```

### Target Files to Modify
- **New**: `src/extract/python_imports_extract.py`
- `src/datafusion_engine/extract_extractors.py`
- `src/datafusion_engine/extract_templates.py`
- `src/datafusion_engine/extract_template_specs.py`
- `src/datafusion_engine/view_registry.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/extract_metadata.py` (via template expansion)

### Code/Modules to Delete (in this scope)
- None (retain `python_imports` view as a thin wrapper over `python_imports_v1` for compatibility).

### Implementation Checklist
- [ ] Introduce `extract_python_imports` and materialize `python_imports_v1`.
- [ ] Add schema + metadata entries for `python_imports_v1` with stable fields.
- [ ] Update `python_external` to depend on `python_imports_v1` by default.
- [ ] Keep `python_imports` view as alias to the physical dataset (or select from it).
- [ ] Add cache key updates where needed to include import graph when materialized.

---

## Scope 10 — Submodule + Worktree Traversal (explicit, repo-aware)

### Why
`include_submodules` / `include_worktrees` are currently configuration knobs without traversal logic. In multi-repo codebases, this leaves scope gaps and makes the resulting scope inaccurate versus a developer’s actual edit surface.

### Representative Code Snippet
```python
# extract/repo_scan_pygit2.py
def iter_scoped_repo_roots(scope: RepoScope) -> list[Path]:
    roots = [scope.repo_root]
    if scope.include_submodules:
        roots.extend(discover_submodule_roots(scope.repo_root))
    if scope.include_worktrees:
        roots.extend(discover_worktree_roots(scope.repo_root))
    return roots
```

### Target Files to Modify
- `src/extract/repo_scan.py`
- `src/extract/repo_scan_pygit2.py`
- `src/extract/repo_scope.py`
- `src/extract/git_submodules.py`
- `src/extract/git_context.py`

### Code/Modules to Delete (in this scope)
- None (existing submodule helpers are reused and extended).

### Implementation Checklist
- [ ] Implement `discover_submodule_roots()` based on `.gitmodules` and pygit2.
- [ ] Implement `discover_worktree_roots()` using pygit2 worktree metadata.
- [ ] Build per-root `RepoScope` and merge into a multi-root scan bundle.
- [ ] Prefix paths in the scope manifest with the submodule/worktree-relative root.
- [ ] Ensure `repo.path_is_ignored` uses the correct repo for each root.
- [ ] Emit per-root scope stats, preserving root identity in telemetry.

---

## Scope 11 — Pathspec Trace Telemetry (detailed_match_files payloads)

### Why
We already track pathspec match counts. Detailed trace payloads make it possible to debug why a given path was included/excluded, enabling richer diagnostics and targeted rule fixes.

### Representative Code Snippet
```python
# extract/repo_scan.py
trace_payload = explain_scope_paths(sample_paths, rules)
record_artifact(profile, "repo_scope_trace_v1", {
    "repo_root": str(repo_root),
    "scope_hash": bundle.scope_hash,
    "trace": trace_payload,
})
```

### Target Files to Modify
- `src/extract/repo_scan.py`
- `src/extract/scope_rules.py`
- `src/obs/diagnostics.py` (if a new artifact type registry is required)

### Code/Modules to Delete (in this scope)
- None.

### Implementation Checklist
- [ ] Add optional trace capture (with sampling) using `detailed_match_files`.
- [ ] Emit `repo_scope_trace_v1` with include/exclude pattern traces.
- [ ] Keep trace payload size bounded (sample limits + top-N patterns).
- [ ] Add config flags for enabling trace diagnostics.
