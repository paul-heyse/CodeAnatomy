"""Repository scope resolution for Python extraction."""

from __future__ import annotations

import configparser
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import pygit2

from core_types import PathLike, ensure_path
from extract.git.context import open_git_context
from extract.python.scope import (
    PythonExtensionCatalog,
    PythonScopePolicy,
    globs_for_extensions,
    resolve_python_extension_catalog,
)
from utils.file_io import read_pyproject_toml

DEFAULT_EXCLUDE_GLOBS: tuple[str, ...] = (
    "**/.git/**",
    "**/__pycache__/**",
    "**/.venv/**",
    "**/venv/**",
    "**/node_modules/**",
    "**/dist/**",
    "**/build/**",
    "**/.mypy_cache/**",
    "**/.pytest_cache/**",
    "**/.ruff_cache/**",
)


@dataclass(frozen=True)
class RepoScopeOptions:
    """Options controlling repository scope resolution."""

    python_scope: PythonScopePolicy = field(default_factory=PythonScopePolicy)
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    include_untracked: bool = True
    include_submodules: bool = False
    include_worktrees: bool = False
    follow_symlinks: bool = False


@dataclass(frozen=True)
class RepoScope:
    """Resolved repository scope context."""

    repo: pygit2.Repository
    repo_root: Path
    python_extensions: frozenset[str]
    extension_catalog: PythonExtensionCatalog
    include_untracked: bool
    include_submodules: bool
    include_worktrees: bool
    follow_symlinks: bool


def default_repo_scope_options() -> RepoScopeOptions:
    """Return default RepoScopeOptions.

    Returns:
    -------
    RepoScopeOptions
        Default scope options.
    """
    return RepoScopeOptions()


def resolve_repo_scope(path: PathLike, options: RepoScopeOptions) -> RepoScope:
    """Resolve a RepoScope for the given path.

    Args:
        path: Path inside the target repository.
        options: Repo scope resolution options.

    Returns:
        RepoScope: Result.

    Raises:
        ValueError: If the path is not inside a git workdir.
    """
    git_ctx = open_git_context(ensure_path(path))
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


def scope_rule_lines(scope: RepoScope, options: RepoScopeOptions) -> tuple[list[str], list[str]]:
    """Return include/exclude rule lines for a scope.

    Returns:
    -------
    tuple[list[str], list[str]]
        Include and exclude rule lines.
    """
    include_lines: list[str] = []
    include_lines.extend(globs_for_extensions(scope.python_extensions))
    config_include, config_exclude = _scope_globs_from_config(scope.repo_root)
    include_lines.extend(config_include)
    include_lines.extend(list(options.include_globs))
    exclude_lines: list[str] = []
    exclude_lines.extend(DEFAULT_EXCLUDE_GLOBS)
    exclude_lines.extend(config_exclude)
    exclude_lines.extend(list(options.exclude_globs))
    return include_lines, exclude_lines


def _scope_globs_from_config(repo_root: Path) -> tuple[list[str], list[str]]:
    include: list[str] = []
    exclude: list[str] = []
    pyproject_include, pyproject_exclude = _scope_globs_from_pyproject(repo_root)
    include.extend(pyproject_include)
    exclude.extend(pyproject_exclude)
    setup_include, setup_exclude = _scope_globs_from_setup_cfg(repo_root)
    include.extend(setup_include)
    exclude.extend(setup_exclude)
    return include, exclude


def _scope_globs_from_pyproject(repo_root: Path) -> tuple[list[str], list[str]]:
    path = repo_root / "pyproject.toml"
    if not path.is_file():
        return [], []
    try:
        data = read_pyproject_toml(path)
    except (OSError, ValueError):
        return [], []
    tool = data.get("tool")
    if not isinstance(tool, dict):
        return [], []
    codeanatomy = tool.get("codeanatomy")
    if not isinstance(codeanatomy, dict):
        return [], []
    include: list[str] = []
    exclude: list[str] = []
    scope = codeanatomy.get("scope")
    if isinstance(scope, dict):
        include.extend(_normalize_glob_list(scope.get("include_globs")))
        exclude.extend(_normalize_glob_list(scope.get("exclude_globs")))
    include.extend(_normalize_glob_list(codeanatomy.get("include_globs")))
    exclude.extend(_normalize_glob_list(codeanatomy.get("exclude_globs")))
    return include, exclude


def _scope_globs_from_setup_cfg(repo_root: Path) -> tuple[list[str], list[str]]:
    path = repo_root / "setup.cfg"
    if not path.is_file():
        return [], []
    parser = configparser.ConfigParser()
    try:
        parser.read(path, encoding="utf-8")
    except (OSError, configparser.Error):
        return [], []
    include: list[str] = []
    exclude: list[str] = []
    for section in ("codeanatomy", "tool:codeanatomy"):
        if not parser.has_section(section):
            continue
        include.extend(_normalize_glob_list(parser.get(section, "include_globs", fallback="")))
        exclude.extend(_normalize_glob_list(parser.get(section, "exclude_globs", fallback="")))
    return include, exclude


def _normalize_glob_list(value: object | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw = [item.strip() for item in value.replace(",", " ").split() if item.strip()]
    elif isinstance(value, Iterable):
        raw = [str(item).strip() for item in value if str(item).strip()]
    else:
        return []
    return [item for item in raw if item]


__all__ = [
    "DEFAULT_EXCLUDE_GLOBS",
    "RepoScope",
    "RepoScopeOptions",
    "default_repo_scope_options",
    "resolve_repo_scope",
    "scope_rule_lines",
]
