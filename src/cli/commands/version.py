"""Version reporting for CodeAnatomy CLI."""

from __future__ import annotations

import json
import platform
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version


def get_version() -> str:
    """Get the CodeAnatomy package version string.

    Returns:
    -------
    str
        Version string, or "0.0.0-dev" if not installed.
    """
    return _package_version("codeanatomy") or "0.0.0-dev"


def get_version_info() -> dict[str, object]:
    """Get detailed version information.

    Returns:
    -------
    dict[str, object]
        Structured version payload.
    """
    return {
        "codeanatomy": get_version(),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "dependencies": {
            "cyclopts": _package_version("cyclopts"),
            "datafusion": _package_version("datafusion"),
            "datafusion_ext": _package_version("datafusion_ext"),
        },
    }


def version_command() -> int:
    """Show version and engine information.

    Returns:
    -------
    int
        Exit status code.
    """
    payload = json.dumps(get_version_info(), indent=2, sort_keys=True)
    sys.stdout.write(payload + "\n")
    return 0


def _package_version(name: str) -> str | None:
    try:
        return pkg_version(name)
    except PackageNotFoundError:
        return None


__all__ = ["get_version", "get_version_info", "version_command"]
