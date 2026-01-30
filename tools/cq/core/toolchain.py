"""Toolchain detection for cq.

Detects availability and versions of external tools (rg, ast-grep, python).
"""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass


@dataclass
class Toolchain:
    """Available tools and their versions.

    Parameters
    ----------
    rg_path : str | None
        Path to ripgrep binary.
    rg_version : str | None
        Ripgrep version string.
    sg_path : str | None
        Path to ast-grep binary.
    sg_version : str | None
        ast-grep version string.
    py_path : str
        Path to Python interpreter.
    py_version : str
        Python version string.
    """

    rg_path: str | None
    rg_version: str | None
    sg_path: str | None
    sg_version: str | None
    py_path: str
    py_version: str

    @staticmethod
    def detect() -> Toolchain:
        """Detect available tools and versions.

        Returns
        -------
        Toolchain
            Detected tool information.
        """
        import sys

        # Detect ripgrep
        rg_path = shutil.which("rg")
        rg_version = None
        if rg_path:
            try:
                result = subprocess.run(
                    [rg_path, "--version"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    # First line: "ripgrep X.Y.Z"
                    first_line = result.stdout.strip().split("\n")[0]
                    rg_version = first_line.split()[-1] if first_line else None
            except (subprocess.TimeoutExpired, OSError):
                pass

        # Detect ast-grep
        sg_path = shutil.which("sg") or shutil.which("ast-grep")
        sg_version = None
        if sg_path:
            try:
                result = subprocess.run(
                    [sg_path, "--version"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    sg_version = result.stdout.strip().split()[-1]
            except (subprocess.TimeoutExpired, OSError):
                pass

        # Python is always available
        py_path = sys.executable
        py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

        return Toolchain(
            rg_path=rg_path,
            rg_version=rg_version,
            sg_path=sg_path,
            sg_version=sg_version,
            py_path=py_path,
            py_version=py_version,
        )

    def to_dict(self) -> dict[str, str | None]:
        """Convert to dict for RunMeta.

        Returns
        -------
        dict[str, str | None]
            Tool version information.
        """
        return {
            "rg": self.rg_version,
            "sg": self.sg_version,
            "python": self.py_version,
        }

    def require_rg(self) -> str:
        """Get ripgrep path, raising if not available.

        Returns
        -------
        str
            Path to rg binary.

        Raises
        ------
        RuntimeError
            If ripgrep is not installed.
        """
        if not self.rg_path:
            msg = (
                "ripgrep (rg) is required but not found. "
                "Install with: brew install ripgrep (macOS) or apt install ripgrep (Linux)"
            )
            raise RuntimeError(msg)
        return self.rg_path

    @property
    def has_sg(self) -> bool:
        """Check if ast-grep is available."""
        return self.sg_path is not None
