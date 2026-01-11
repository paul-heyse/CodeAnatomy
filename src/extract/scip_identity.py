"""Resolve SCIP project identity via the GitHub CLI."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ORG_PREFIX = "github.com/org"


@dataclass(frozen=True)
class ScipIdentity:
    """Stable identity values for SCIP symbol namespaces."""

    project_name: str
    project_version: str
    project_namespace: str | None


def _run_gh(repo_root: Path, args: list[str]) -> str:
    """Run a GitHub CLI command and return stdout.

    Parameters
    ----------
    repo_root:
        Repository root used as the working directory.
    args:
        GitHub CLI arguments.

    Returns
    -------
    str
        Standard output from the command.

    Raises
    ------
    RuntimeError
        Raised when the command exits non-zero.
    """
    try:
        proc = subprocess.run(
            ["gh", *args],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        msg = (
            "GitHub CLI command failed.\n"
            f"cmd={' '.join(['gh', *args])}\n"
            f"stdout:\n{exc.stdout}\n"
            f"stderr:\n{exc.stderr}\n"
        )
        raise RuntimeError(msg) from exc
    return proc.stdout.strip()


def gh_repo_name_with_owner(repo_root: Path) -> str:
    """Return the GitHub nameWithOwner for the repo.

    Parameters
    ----------
    repo_root:
        Repository root path.

    Returns
    -------
    str
        Repository slug in nameWithOwner form.
    """
    return _run_gh(repo_root, ["repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"])


def gh_repo_head_sha(repo_root: Path, name_with_owner: str) -> str:
    """Return the HEAD commit SHA for the repo via the GitHub API.

    Parameters
    ----------
    repo_root:
        Repository root path.
    name_with_owner:
        Repository slug in nameWithOwner form.

    Returns
    -------
    str
        HEAD commit SHA.
    """
    return _run_gh(
        repo_root,
        ["api", f"repos/{name_with_owner}/commits/HEAD", "--jq", ".sha"],
    )


def resolve_scip_identity(
    repo_root: Path,
    *,
    project_name_override: str | None,
    project_version_override: str | None,
    project_namespace_override: str | None,
) -> ScipIdentity:
    """Resolve SCIP identity values from GitHub CLI or overrides.

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
    repo = project_name_override or gh_repo_name_with_owner(repo_root)
    sha = project_version_override or gh_repo_head_sha(repo_root, repo)
    namespace = project_namespace_override or DEFAULT_ORG_PREFIX
    return ScipIdentity(
        project_name=repo,
        project_version=sha,
        project_namespace=namespace,
    )
