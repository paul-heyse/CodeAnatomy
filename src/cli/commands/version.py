"""Version reporting for CodeAnatomy CLI."""

from __future__ import annotations

import json
import sys
from importlib.metadata import PackageNotFoundError, version


def print_version_info() -> None:
    """Print version and engine information."""
    payload = {
        "codeanatomy": _package_version("codeanatomy") or "unknown",
        "python": sys.version.split()[0],
        "dependencies": {
            "cyclopts": _package_version("cyclopts"),
            "datafusion": _package_version("datafusion"),
            "datafusion_ext": _package_version("datafusion_ext"),
            "rustworkx": _package_version("rustworkx"),
        },
    }
    encoded = json.dumps(payload, indent=2, sort_keys=True)
    sys.stdout.write(encoded + "\n")


def _package_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


__all__ = ["print_version_info"]
