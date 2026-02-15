"""File tabulation for CQ with gitignore semantics."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pygit2
from pathspec import GitIgnoreSpec

from tools.cq.core.structs import CqStruct
from tools.cq.index.gitignore import load_gitignore_spec
from tools.cq.index.repo import RepoContext, open_repo


class FileFilterDecision(CqStruct, frozen=True):
    """Decision details for file filtering diagnostics."""

    file: str
    ignored: bool
    ignore_rule_index: int | None
    glob_excluded: bool
    scope_excluded: bool


class FileTabulationResult(CqStruct, frozen=True):
    """Result of file tabulation."""

    files: list[Path]
    decisions: list[FileFilterDecision]


@dataclass(frozen=True)
class RepoFileIndex:
    """Indexed repository file data for CQ."""

    repo_root: Path
    tracked: set[str]
    ignore_spec: GitIgnoreSpec


@dataclass(frozen=True)
class UntrackedScanConfig:
    """Inputs for scanning untracked files."""

    repo_root: Path
    scope_root: Path
    tracked: set[str]
    ignore_spec: GitIgnoreSpec
    extensions: frozenset[str]
    explain: bool


def build_repo_file_index(repo_context: RepoContext) -> RepoFileIndex:
    """Build repo file index with tracked set and ignore spec.

    Returns:
    -------
    RepoFileIndex
        Indexed repository metadata for filtering and tabulation.
    """
    repo = open_repo(repo_context)
    tracked = _collect_tracked_paths(repo)
    ignore_spec = load_gitignore_spec(repo_context.repo_root, repo_context.git_dir, repo)
    return RepoFileIndex(
        repo_root=repo_context.repo_root,
        tracked=tracked,
        ignore_spec=ignore_spec,
    )


def tabulate_files(
    repo_index: RepoFileIndex,
    scope_paths: Sequence[Path],
    globs: Sequence[str] | None,
    *,
    extensions: Sequence[str],
    explain: bool = False,
) -> FileTabulationResult:
    """Tabulate repo files including tracked and likely-to-be-tracked untracked files.

    Returns:
    -------
    FileTabulationResult
        Tabulated files and optional filter decisions.
    """
    if not scope_paths:
        return FileTabulationResult(files=[], decisions=[])

    resolved_repo_root = repo_index.repo_root.resolve()
    scope_roots = _resolve_scope_roots(resolved_repo_root, scope_paths)
    scope_prefixes = _scope_prefixes_for_repo(resolved_repo_root, scope_roots)
    tracked = _filter_tracked_to_scope(
        repo_index.tracked,
        scope_prefixes,
    )
    extension_set = frozenset(extensions)
    tracked_files = {
        (resolved_repo_root / rel).resolve()
        for rel in tracked
        if _is_candidate_file(resolved_repo_root / rel, extension_set)
    }

    untracked_files, ignored_decisions = _scan_untracked_scope_files(
        repo_root=resolved_repo_root,
        scope_roots=scope_roots,
        tracked=repo_index.tracked,
        ignore_spec=repo_index.ignore_spec,
        extensions=extension_set,
        explain=explain,
    )

    all_files = list(tracked_files | untracked_files)
    decisions: list[FileFilterDecision] = []

    filtered: list[Path] = []
    for path in all_files:
        rel_path = _normalize_relative_path(path=path, repo_root=resolved_repo_root)
        scope_excluded = False
        glob_excluded = False
        if rel_path is None or not _is_within_scope(rel_path, scope_prefixes):
            scope_excluded = True
        if globs and (rel_path is None or not _matches_globs(rel_path, globs)):
            glob_excluded = True
        if scope_excluded or glob_excluded:
            if explain:
                decisions.append(
                    FileFilterDecision(
                        file=rel_path or path.as_posix(),
                        ignored=False,
                        ignore_rule_index=None,
                        glob_excluded=glob_excluded,
                        scope_excluded=scope_excluded,
                    )
                )
            continue
        filtered.append(path)

    if explain:
        decisions.extend(ignored_decisions)

    return FileTabulationResult(
        files=sorted(filtered, key=lambda path: path.as_posix()),
        decisions=decisions,
    )


def _scan_untracked_scope_files(
    *,
    repo_root: Path,
    scope_roots: Sequence[Path],
    tracked: set[str],
    ignore_spec: GitIgnoreSpec,
    extensions: frozenset[str],
    explain: bool,
) -> tuple[set[Path], list[FileFilterDecision]]:
    files: set[Path] = set()
    decisions: list[FileFilterDecision] = []
    for scope_root in scope_roots:
        config = UntrackedScanConfig(
            repo_root=repo_root,
            scope_root=scope_root,
            tracked=tracked,
            ignore_spec=ignore_spec,
            extensions=extensions,
            explain=explain,
        )
        collected_files, collected_decisions = _collect_untracked_files(config)
        files.update(collected_files)
        decisions.extend(collected_decisions)
    return files, decisions


def _collect_tracked_paths(repo: pygit2.Repository | None) -> set[str]:
    if repo is None:
        return set()
    return {entry.path for entry in repo.index}


def _resolve_scope_roots(repo_root: Path, scope_paths: Sequence[Path]) -> list[Path]:
    roots: list[Path] = []
    for path in scope_paths:
        resolved = path if path.is_absolute() else repo_root / path
        if resolved.exists():
            roots.append(resolved.resolve())
    return roots if roots else []


def _filter_tracked_to_scope(
    tracked: set[str],
    scope_prefixes: tuple[str, ...],
) -> set[str]:
    if not scope_prefixes:
        return set()
    filtered: set[str] = set()
    for rel_path in tracked:
        if _is_within_scope(rel_path, scope_prefixes):
            filtered.add(rel_path)
    return filtered


def _path_is_under(rel_path: str, scope_prefix: str) -> bool:
    normalized_rel = rel_path.strip("/")
    normalized_scope = scope_prefix.strip("/")
    if not normalized_scope or normalized_scope == ".":
        return True
    return normalized_rel == normalized_scope or normalized_rel.startswith(f"{normalized_scope}/")


def _collect_untracked_files(
    config: UntrackedScanConfig,
) -> tuple[set[Path], list[FileFilterDecision]]:
    files: set[Path] = set()
    decisions: list[FileFilterDecision] = []
    if not _is_relative_to(config.scope_root, config.repo_root):
        return files, decisions
    if config.scope_root.is_file():
        return _collect_untracked_file(config)
    return _collect_untracked_tree(config)


def _collect_untracked_file(
    config: UntrackedScanConfig,
) -> tuple[set[Path], list[FileFilterDecision]]:
    files: set[Path] = set()
    decisions: list[FileFilterDecision] = []
    if _is_candidate_file(config.scope_root, config.extensions):
        rel = config.scope_root.relative_to(config.repo_root).as_posix()
        if rel not in config.tracked and not config.ignore_spec.match_file(rel):
            files.add(config.scope_root.resolve())
        elif config.explain and rel not in config.tracked:
            _record_ignore_decision(decisions, rel, config.ignore_spec)
    return files, decisions


def _collect_untracked_tree(
    config: UntrackedScanConfig,
) -> tuple[set[Path], list[FileFilterDecision]]:
    files: set[Path] = set()
    decisions: list[FileFilterDecision] = []
    for path in config.scope_root.rglob("*"):
        if ".git" in path.parts:
            continue
        if not _is_candidate_file(path, config.extensions):
            continue
        rel = path.relative_to(config.repo_root).as_posix()
        if rel in config.tracked:
            continue
        if config.ignore_spec.match_file(rel):
            if config.explain:
                _record_ignore_decision(decisions, rel, config.ignore_spec)
            continue
        files.add(path.resolve())
    return files, decisions


def _record_ignore_decision(
    decisions: list[FileFilterDecision],
    rel: str,
    ignore_spec: GitIgnoreSpec,
) -> None:
    result = ignore_spec.check_file(rel)
    decisions.append(
        FileFilterDecision(
            file=rel,
            ignored=result.include is False,
            ignore_rule_index=result.index,
            glob_excluded=False,
            scope_excluded=False,
        )
    )


def _is_candidate_file(path: Path, extensions: frozenset[str]) -> bool:
    if not path.is_file():
        return False
    return path.suffix in extensions


def _matches_globs(rel_path: str, globs: Sequence[str]) -> bool:
    if not globs:
        return True
    has_includes = any(not glob.startswith("!") for glob in globs)
    include = not has_includes
    for glob in globs:
        negated = glob.startswith("!")
        pattern = glob[1:] if negated else glob
        if Path(rel_path).match(pattern):
            include = not negated
    return include


def _is_within_scope(rel_path: str, scope_prefixes: tuple[str, ...]) -> bool:
    return any(_path_is_under(rel_path, prefix) for prefix in scope_prefixes)


def _scope_prefixes_for_repo(repo_root: Path, scope_roots: Sequence[Path]) -> tuple[str, ...]:
    prefixes: list[str] = []
    for scope_root in scope_roots:
        try:
            rel = scope_root.relative_to(repo_root).as_posix().strip("/")
        except ValueError:
            continue
        if rel in {"", "."}:
            rel = ""
        prefixes.append(rel)
    if not prefixes:
        return ()
    return tuple(dict.fromkeys(prefixes))


def _normalize_relative_path(path: Path, repo_root: Path) -> str | None:
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        try:
            return path.resolve().relative_to(repo_root).as_posix()
        except ValueError:
            return None


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    else:
        return True
