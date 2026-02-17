"""AST extractor option and parse setup helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from extract.coordination.context import FileContext
from extract.git.context import discover_repo_root_from_paths
from extract.infrastructure.options import ParallelOptions, RepoOptions, WorklistQueueOptions
from utils.file_io import read_pyproject_toml


@dataclass(frozen=True)
class AstExtractOptions(RepoOptions, WorklistQueueOptions, ParallelOptions):
    """Define AST extraction options."""

    type_comments: bool = True
    feature_version: tuple[int, int] | int | None = None
    mode: Literal["exec", "eval", "single", "func_type"] = "exec"
    optimize: Literal[-1, 0, 1, 2] | None = None
    allow_top_level_await: bool = False
    dont_inherit: bool = True
    max_bytes: int | None = 50_000_000
    max_nodes: int | None = 1_000_000
    cache_by_sha: bool = True


_PYTHON_VERSION_RE = re.compile(r"(\\d+)\\.(\\d+)")


def _parse_requires_python(spec: str) -> tuple[int, int] | None:
    match = _PYTHON_VERSION_RE.search(spec)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


def _feature_version_from_pyproject(repo_root: Path) -> tuple[int, int] | None:
    pyproject = repo_root / "pyproject.toml"
    if not pyproject.exists():
        return None
    try:
        data = read_pyproject_toml(pyproject)
    except (OSError, ValueError):
        return None
    project = data.get("project")
    if isinstance(project, Mapping):
        requires = project.get("requires-python")
        if isinstance(requires, str):
            parsed = _parse_requires_python(requires)
            if parsed is not None:
                return parsed
    tool = data.get("tool")
    if isinstance(tool, Mapping):
        poetry = tool.get("poetry")
        if isinstance(poetry, Mapping):
            dependencies = poetry.get("dependencies")
            if isinstance(dependencies, Mapping):
                requires = dependencies.get("python")
                if isinstance(requires, str):
                    return _parse_requires_python(requires)
    return None


def _infer_repo_root(contexts: Sequence[FileContext]) -> Path | None:
    candidate_paths = [ctx.abs_path for ctx in contexts if ctx.abs_path]
    if candidate_paths:
        git_root = discover_repo_root_from_paths(candidate_paths)
        if git_root is not None:
            return git_root
    for ctx in contexts:
        if not ctx.abs_path or not ctx.path:
            continue
        rel_path = Path(ctx.path)
        if rel_path.is_absolute():
            continue
        abs_path = Path(ctx.abs_path).resolve()
        rel_parts = rel_path.parts
        if len(rel_parts) == 0:
            continue
        if abs_path.parts[-len(rel_parts) :] != rel_parts:
            continue
        return abs_path.parents[len(rel_parts) - 1]
    return None


def _resolve_feature_version(
    options: AstExtractOptions,
    contexts: Sequence[FileContext],
) -> AstExtractOptions:
    if options.feature_version is not None:
        return options
    repo_root = _infer_repo_root(contexts)
    if repo_root is None:
        return options
    feature_version = _feature_version_from_pyproject(repo_root)
    if feature_version is None:
        return options
    resolved = replace(options, feature_version=feature_version)
    if resolved.dont_inherit:
        resolved = replace(resolved, dont_inherit=False)
    if resolved.allow_top_level_await:
        resolved = replace(resolved, allow_top_level_await=False)
    return resolved


def _format_feature_version(value: tuple[int, int] | int | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, tuple):
        return f"{value[0]}.{value[1]}"
    return str(value)


__all__ = [
    "AstExtractOptions",
]
