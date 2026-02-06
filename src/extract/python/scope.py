"""Python extension discovery and scope helpers."""

from __future__ import annotations

import configparser
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pygit2

from core.config_base import FingerprintableConfig, config_fingerprint
from extract.git.context import open_git_context
from utils.file_io import read_pyproject_toml

DEFAULT_PYTHON_EXTENSIONS: frozenset[str] = frozenset(
    {".py", ".pyi", ".pyx", ".pxd", ".pxi", ".pyw"}
)


@dataclass(frozen=True)
class PythonScopePolicy(FingerprintableConfig):
    """Configuration for Python extension discovery."""

    extra_extensions: tuple[str, ...] = ()
    discover_extensions: bool = True
    discover_shebang: bool = True
    max_discovery_files: int | None = 50_000

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the Python scope policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing Python scope policy settings.
        """
        return {
            "extra_extensions": self.extra_extensions,
            "discover_extensions": self.discover_extensions,
            "discover_shebang": self.discover_shebang,
            "max_discovery_files": self.max_discovery_files,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the Python scope policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class PythonExtensionCatalog:
    """Resolved Python extensions with source annotations."""

    extensions: frozenset[str]
    sources: Mapping[str, tuple[str, ...]]

    def rows(self) -> list[dict[str, str]]:
        """Return rows for python_extensions_v1 datasets.

        Returns:
        -------
        list[dict[str, str]]
            Row payloads for extension/source pairs.
        """
        rows: list[dict[str, str]] = []
        for ext in sorted(self.extensions):
            sources = self.sources.get(ext, ())
            if not sources:
                rows.append({"extension": ext, "source": "unknown"})
                continue
            rows.extend([{"extension": ext, "source": source} for source in sources])
        return rows


def normalize_extension(value: str) -> str | None:
    """Normalize an extension into a dotted lowercase suffix.

    Returns:
    -------
    str | None
        Normalized extension or None when empty.
    """
    cleaned = value.strip().lower()
    if not cleaned:
        return None
    if not cleaned.startswith("."):
        cleaned = f".{cleaned}"
    if cleaned == ".":
        return None
    return cleaned


def globs_for_extensions(extensions: Iterable[str]) -> list[str]:
    """Return glob patterns for the provided extensions.

    Returns:
    -------
    list[str]
        Glob patterns for use in pathspec rules.
    """
    return [f"**/*{ext}" for ext in sorted({ext for ext in extensions if ext})]


def resolve_python_extensions(
    repo_root: Path,
    policy: PythonScopePolicy,
    *,
    repo: pygit2.Repository | None = None,
) -> frozenset[str]:
    """Resolve Python extensions for a repository.

    Returns:
    -------
    frozenset[str]
        Discovered Python extensions.
    """
    return resolve_python_extension_catalog(repo_root, policy, repo=repo).extensions


def resolve_python_extension_catalog(
    repo_root: Path,
    policy: PythonScopePolicy,
    *,
    repo: pygit2.Repository | None = None,
) -> PythonExtensionCatalog:
    """Return the resolved Python extensions and source annotations.

    Returns:
    -------
    PythonExtensionCatalog
        Extension catalog with source labels.
    """
    sources: dict[str, set[str]] = {}
    extensions = set(DEFAULT_PYTHON_EXTENSIONS)
    for ext in extensions:
        sources.setdefault(ext, set()).add("default")
    config_exts = _extensions_from_config(repo_root)
    for ext in config_exts:
        extensions.add(ext)
        sources.setdefault(ext, set()).add("config")
    for raw_ext in policy.extra_extensions:
        ext = normalize_extension(raw_ext)
        if ext is None:
            continue
        extensions.add(ext)
        sources.setdefault(ext, set()).add("extra")
    if policy.discover_extensions:
        discovered = _discover_extensions(repo_root, policy, repo=repo)
        for ext in discovered:
            extensions.add(ext)
            sources.setdefault(ext, set()).add("discovered")
    catalog_sources = {ext: tuple(sorted(vals)) for ext, vals in sources.items()}
    return PythonExtensionCatalog(
        extensions=frozenset(sorted(extensions)),
        sources=catalog_sources,
    )


def _extensions_from_config(repo_root: Path) -> set[str]:
    extensions: set[str] = set()
    extensions.update(_extensions_from_pyproject(repo_root))
    extensions.update(_extensions_from_setup_cfg(repo_root))
    extensions.update(_extensions_from_mypy_ini(repo_root))
    return extensions


def _extensions_from_pyproject(repo_root: Path) -> set[str]:
    pyproject = repo_root / "pyproject.toml"
    if not pyproject.is_file():
        return set()
    try:
        data = read_pyproject_toml(pyproject)
    except (OSError, ValueError):
        return set()
    tool = data.get("tool")
    if not isinstance(tool, dict):
        return set()
    codeanatomy = tool.get("codeanatomy")
    if not isinstance(codeanatomy, dict):
        return set()
    scope = codeanatomy.get("python_scope")
    extensions = set()
    if isinstance(scope, dict):
        value = scope.get("extensions")
        extensions.update(_normalize_extension_list(value))
    value = codeanatomy.get("python_extensions")
    extensions.update(_normalize_extension_list(value))
    return extensions


def _extensions_from_setup_cfg(repo_root: Path) -> set[str]:
    path = repo_root / "setup.cfg"
    if not path.is_file():
        return set()
    parser = configparser.ConfigParser()
    try:
        parser.read(path, encoding="utf-8")
    except (OSError, configparser.Error):
        return set()
    extensions = set()
    for section in ("codeanatomy", "tool:codeanatomy"):
        if not parser.has_section(section):
            continue
        value = parser.get(section, "python_extensions", fallback="")
        extensions.update(_normalize_extension_list(value))
    return extensions


def _extensions_from_mypy_ini(repo_root: Path) -> set[str]:
    path = repo_root / "mypy.ini"
    if not path.is_file():
        return set()
    parser = configparser.ConfigParser()
    try:
        parser.read(path, encoding="utf-8")
    except (OSError, configparser.Error):
        return set()
    if not parser.has_section("mypy"):
        return set()
    value = parser.get("mypy", "python_extensions", fallback="")
    return _normalize_extension_list(value)


def _normalize_extension_list(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        raw = [item.strip() for item in value.replace(",", " ").split() if item.strip()]
    elif isinstance(value, Iterable):
        raw = [str(item).strip() for item in value if str(item).strip()]
    else:
        return set()
    normalized: set[str] = set()
    for item in raw:
        ext = normalize_extension(item)
        if ext is not None:
            normalized.add(ext)
    return normalized


def _discover_extensions(
    repo_root: Path,
    policy: PythonScopePolicy,
    *,
    repo: pygit2.Repository | None,
) -> set[str]:
    resolved_repo = _resolve_discovery_repo(repo_root, repo=repo)
    if resolved_repo is None:
        return set()
    discovered: set[str] = set()
    for rel_path in _iter_discovery_paths(resolved_repo, policy=policy):
        ext = normalize_extension(rel_path.suffix)
        if ext is None:
            continue
        if ext in DEFAULT_PYTHON_EXTENSIONS:
            discovered.add(ext)
            continue
        if policy.discover_shebang and _has_python_shebang(Path(resolved_repo.workdir) / rel_path):
            discovered.add(ext)
    return discovered


def _resolve_discovery_repo(
    repo_root: Path,
    *,
    repo: pygit2.Repository | None,
) -> pygit2.Repository | None:
    resolved_repo = repo
    if resolved_repo is None:
        git_ctx = open_git_context(repo_root)
        resolved_repo = git_ctx.repo if git_ctx is not None else None
    if resolved_repo is None or resolved_repo.workdir is None:
        return None
    return resolved_repo


def _iter_discovery_paths(
    repo: pygit2.Repository,
    *,
    policy: PythonScopePolicy,
) -> Iterator[Path]:
    max_files = policy.max_discovery_files
    for index, rel_path in enumerate(_iter_repo_paths(repo, include_untracked=True)):
        if max_files is not None and index >= max_files:
            break
        if _is_ignored_path(repo, rel_path):
            continue
        yield rel_path


def _is_ignored_path(repo: pygit2.Repository, rel_path: Path) -> bool:
    try:
        return repo.path_is_ignored(rel_path.as_posix())
    except pygit2.GitError:
        return True


def _iter_repo_paths(
    repo: pygit2.Repository,
    *,
    include_untracked: bool,
) -> Iterator[Path]:
    flags = int(getattr(pygit2, "GIT_STATUS_OPT_DISABLE_PATHSPEC_MATCH", 0))
    if include_untracked:
        flags |= int(getattr(pygit2, "GIT_STATUS_OPT_INCLUDE_UNTRACKED", 0))
        flags |= int(getattr(pygit2, "GIT_STATUS_OPT_RECURSE_UNTRACKED_DIRS", 0))
    status_options_type = getattr(pygit2, "StatusOptions", None)
    options = None
    if status_options_type is not None:
        show_flag = int(getattr(pygit2, "GIT_STATUS_SHOW_INDEX_AND_WORKDIR", 0))
        options = status_options_type(show=show_flag, flags=flags)
    try:
        status_fn = cast("Callable[..., Mapping[str, object]]", repo.status)
        status = status_fn(options) if options is not None else status_fn()
    except pygit2.GitError:
        return
    paths: set[str] = set(status)
    paths.update(entry.path for entry in repo.index)
    for rel_posix in sorted(paths):
        yield Path(rel_posix)


def _has_python_shebang(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.is_symlink():
        return False
    try:
        with path.open("rb") as handle:
            line = handle.readline(256)
    except OSError:
        return False
    if not line.startswith(b"#!"):
        return False
    return b"python" in line.lower()


__all__ = [
    "DEFAULT_PYTHON_EXTENSIONS",
    "PythonExtensionCatalog",
    "PythonScopePolicy",
    "globs_for_extensions",
    "normalize_extension",
    "resolve_python_extension_catalog",
    "resolve_python_extensions",
]
