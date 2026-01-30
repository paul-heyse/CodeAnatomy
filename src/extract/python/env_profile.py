"""Python environment profiling for external interface resolution."""

from __future__ import annotations

import sysconfig
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True)
class PythonEnvProfile:
    """Resolved Python environment paths and distribution lookup."""

    stdlib_paths: tuple[Path, ...]
    site_paths: tuple[Path, ...]
    package_dists: Mapping[str, Sequence[str]]

    def is_stdlib(self, path: Path) -> bool:
        """Return True when the path is under the stdlib.

        Returns
        -------
        bool
            True for stdlib paths.
        """
        resolved = path.resolve()
        return any(_is_within(resolved, base) for base in self.stdlib_paths)

    def distribution_for_path(self, path: Path) -> metadata.Distribution | None:
        """Return the distribution that owns the given path when possible.

        Returns
        -------
        importlib.metadata.Distribution | None
            Distribution metadata when resolvable.
        """
        resolved = path.resolve()
        for site_path in self.site_paths:
            if not _is_within(resolved, site_path):
                continue
            rel = resolved.relative_to(site_path)
            if not rel.parts:
                continue
            top = rel.parts[0]
            dist_name = _dist_name_from_top_level(top, self.package_dists)
            if dist_name is None:
                continue
            try:
                return metadata.distribution(dist_name)
            except metadata.PackageNotFoundError:
                return None
        return None


def resolve_python_env_profile() -> PythonEnvProfile:
    """Resolve the active Python environment profile.

    Returns
    -------
    PythonEnvProfile
        Environment profile with stdlib/site paths.
    """
    stdlib_paths = _stdlib_paths()
    site_paths = _site_paths()
    package_dists = metadata.packages_distributions()
    return PythonEnvProfile(
        stdlib_paths=stdlib_paths,
        site_paths=site_paths,
        package_dists=package_dists,
    )


def _stdlib_paths() -> tuple[Path, ...]:
    paths = sysconfig.get_paths()
    stdlib: list[Path] = []
    for key in ("stdlib", "platstdlib"):
        value = paths.get(key)
        if value:
            stdlib.append(Path(value).resolve())
    return tuple(dict.fromkeys(stdlib))


def _site_paths() -> tuple[Path, ...]:
    paths = sysconfig.get_paths()
    site: list[Path] = []
    for key in ("purelib", "platlib"):
        value = paths.get(key)
        if value:
            site.append(Path(value).resolve())
    return tuple(dict.fromkeys(site))


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _dist_name_from_top_level(
    top_level: str,
    package_dists: Mapping[str, Sequence[str]],
) -> str | None:
    cleaned = top_level
    if cleaned.endswith(".py"):
        cleaned = cleaned[: -len(".py")]
    elif cleaned.endswith(".pyc"):
        cleaned = cleaned[: -len(".pyc")]
    elif "." in cleaned and cleaned.endswith((".so", ".pyd")):
        cleaned = cleaned.split(".", maxsplit=1)[0]
    dist_names = package_dists.get(cleaned)
    if not dist_names:
        return None
    return str(dist_names[0])


__all__ = ["PythonEnvProfile", "resolve_python_env_profile"]
