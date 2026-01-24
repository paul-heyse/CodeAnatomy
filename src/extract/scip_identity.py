"""Resolve SCIP project identity via pygit2."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from extract.git_context import (
    default_project_name,
    github_name_with_owner,
    open_git_context,
)

DEFAULT_ORG_PREFIX = "github.com/org"
DEFAULT_PROJECT_VERSION = "unversioned"


@dataclass(frozen=True)
class ScipIdentity:
    """Stable identity values for SCIP symbol namespaces."""

    project_name: str
    project_version: str
    project_namespace: str | None


def resolve_scip_identity(
    repo_root: Path,
    *,
    project_name_override: str | None,
    project_version_override: str | None,
    project_namespace_override: str | None,
) -> ScipIdentity:
    """Resolve SCIP identity values from pygit2 or overrides.

    Parameters
    ----------
    repo_root:
        Repository root path.
    project_name_override:
        Optional project name override.
    project_version_override:
        Optional project version override.
    project_namespace_override:
        Optional project namespace override.

    Returns
    -------
    ScipIdentity
        Resolved SCIP identity values.
    """
    git_ctx = open_git_context(repo_root)
    repo_slug = github_name_with_owner(git_ctx.origin_url) if git_ctx is not None else None
    project_name = project_name_override or repo_slug or default_project_name(repo_root)
    head_sha = git_ctx.head_sha if git_ctx is not None else None
    project_version = project_version_override or head_sha or DEFAULT_PROJECT_VERSION
    namespace = project_namespace_override or DEFAULT_ORG_PREFIX
    return ScipIdentity(
        project_name=project_name,
        project_version=project_version,
        project_namespace=namespace,
    )
