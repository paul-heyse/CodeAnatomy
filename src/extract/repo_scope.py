"""Backward-compatible re-exports for repository scope helpers."""

from __future__ import annotations

from extract.scanning.repo_scope import (
    DEFAULT_EXCLUDE_GLOBS,
    RepoScope,
    RepoScopeOptions,
    default_repo_scope_options,
    resolve_repo_scope,
    scope_rule_lines,
)

__all__ = [
    "DEFAULT_EXCLUDE_GLOBS",
    "RepoScope",
    "RepoScopeOptions",
    "default_repo_scope_options",
    "resolve_repo_scope",
    "scope_rule_lines",
]
