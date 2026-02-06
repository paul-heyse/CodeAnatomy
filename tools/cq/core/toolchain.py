"""Toolchain detection for CQ."""

from __future__ import annotations

import subprocess

from tools.cq.core.structs import CqStruct


def _package_version(name: str) -> str | None:
    """Return installed package version when available.

    Returns:
    -------
    str | None
        Package version, otherwise ``None``.
    """
    try:
        import importlib.metadata
    except ImportError:
        return None
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _detect_rg() -> tuple[bool, str | None]:
    """Detect ripgrep availability.

    Returns:
    -------
    tuple[bool, str | None]
        Availability flag and version banner.
    """
    proc = subprocess.run(
        ["rg", "--version"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return False, None
    line = proc.stdout.splitlines()[0].strip() if proc.stdout else ""
    return True, line or None


class Toolchain(CqStruct, frozen=True):
    """Available tools and versions."""

    rg_available: bool
    rg_version: str | None
    sgpy_available: bool
    sgpy_version: str | None
    py_path: str
    py_version: str

    @staticmethod
    def detect() -> Toolchain:
        """Detect available tools and versions.

        Returns:
        -------
        Toolchain
            Detected toolchain snapshot.
        """
        import sys

        rg_available, rg_version = _detect_rg()
        sgpy_version = _package_version("ast-grep-py")
        sgpy_available = sgpy_version is not None
        py_path = sys.executable
        py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        return Toolchain(
            rg_available=rg_available,
            rg_version=rg_version,
            sgpy_available=sgpy_available,
            sgpy_version=sgpy_version,
            py_path=py_path,
            py_version=py_version,
        )

    def to_dict(self) -> dict[str, str | None]:
        """Convert to run metadata mapping.

        Returns:
        -------
        dict[str, str | None]
            Tool version mapping for run metadata.
        """
        return {
            "rg": self.rg_version,
            "sgpy": self.sgpy_version,
            "python": self.py_version,
        }

    def require_rg(self) -> None:
        """Verify ripgrep is available.

        Raises:
            RuntimeError: If the operation cannot be completed.
        """
        if not self.rg_available:
            msg = "ripgrep (rg) is required but was not found on PATH."
            raise RuntimeError(msg)

    def require_sgpy(self) -> None:
        """Verify ast-grep-py package is available.

        Raises:
            RuntimeError: If the operation cannot be completed.
        """
        if not self.sgpy_available:
            msg = (
                "ast-grep-py Python package is required but not found. "
                "Install with: uv add ast-grep-py"
            )
            raise RuntimeError(msg)

    @property
    def has_sgpy(self) -> bool:
        """Check whether ast-grep-py is available."""
        return self.sgpy_available

    @property
    def has_rg(self) -> bool:
        """Check whether ripgrep is available."""
        return self.rg_available


__all__ = [
    "Toolchain",
]
