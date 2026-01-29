"""Scope manifest helpers for repo and external interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pygit2

from extract.scope_rules import ScopeRuleSet, check_scope_path

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


@dataclass(frozen=True)
class ScopeManifestEntry:
    """Manifest entry for a scoped path."""

    path: str
    included: bool
    ignored_by_git: bool
    include_index: int | None
    exclude_index: int | None
    scope_kind: str
    repo_root: str | None
    is_untracked: bool

    def to_row(self) -> dict[str, str | None]:
        """Return a row payload for persistence.

        Returns
        -------
        dict[str, str | None]
            Manifest row payload.
        """
        return {
            "path": self.path,
            "included": str(self.included),
            "ignored_by_git": str(self.ignored_by_git),
            "include_index": str(self.include_index) if self.include_index is not None else None,
            "exclude_index": str(self.exclude_index) if self.exclude_index is not None else None,
            "scope_kind": self.scope_kind,
            "repo_root": self.repo_root,
            "is_untracked": str(self.is_untracked),
        }


@dataclass(frozen=True)
class ScopeManifest:
    """Scope manifest container."""

    entries: tuple[ScopeManifestEntry, ...]

    def allowed_paths(self) -> frozenset[str]:
        """Return the set of included paths.

        Returns
        -------
        frozenset[str]
            Paths included in scope.
        """
        return frozenset(entry.path for entry in self.entries if entry.included)

    def rows(self) -> list[dict[str, str | None]]:
        """Return row payloads for manifest entries.

        Returns
        -------
        list[dict[str, str | None]]
            Manifest rows.
        """
        return [entry.to_row() for entry in self.entries]


@dataclass(frozen=True)
class ScopeManifestOptions:
    """Options for building scope manifests."""

    repo: pygit2.Repository
    rules: ScopeRuleSet
    scope_kind: str
    prefix: Path | None = None
    repo_root: Path | None = None
    status_flags: Mapping[str, int] | None = None


def build_scope_manifest(
    paths: Iterable[str],
    *,
    options: ScopeManifestOptions,
) -> ScopeManifest:
    """Build a scope manifest from candidate paths.

    Returns
    -------
    ScopeManifest
        Scope manifest entries for the candidate paths.
    """
    entries: list[ScopeManifestEntry] = []
    prefix_value = options.prefix or Path()
    repo_root_value = str(options.repo_root) if options.repo_root is not None else None
    for rel_posix in paths:
        rel_path = Path(rel_posix)
        prefixed = rel_path
        if prefix_value.parts:
            prefixed = prefix_value / rel_path
        prefixed_posix = prefixed.as_posix()
        ignored_by_git = False
        try:
            ignored_by_git = options.repo.path_is_ignored(rel_posix)
        except pygit2.GitError:
            ignored_by_git = False
        decision = check_scope_path(rel_posix, options.rules)
        is_untracked = False
        if options.status_flags is not None:
            flags = options.status_flags.get(rel_posix, 0)
            is_untracked = bool(flags & pygit2.GIT_STATUS_WT_NEW)
        included = decision.include and not ignored_by_git
        entries.append(
            ScopeManifestEntry(
                path=prefixed_posix,
                included=included,
                ignored_by_git=ignored_by_git,
                include_index=decision.include_index,
                exclude_index=decision.exclude_index,
                scope_kind=options.scope_kind,
                repo_root=repo_root_value,
                is_untracked=is_untracked,
            )
        )
    return ScopeManifest(entries=tuple(entries))


__all__ = [
    "ScopeManifest",
    "ScopeManifestEntry",
    "ScopeManifestOptions",
    "build_scope_manifest",
]
