"""Toolchain detection for cq.

Detects availability and versions of external tools (ast-grep-py, python, rpygrep).
"""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class Toolchain(CqStruct, frozen=True):
    """Available tools and their versions.

    Parameters
    ----------
    rpygrep_available : bool
        Whether rpygrep Python package is available.
    rpygrep_version : str | None
        rpygrep package version string.
    sgpy_available : bool
        Whether ast-grep-py Python package is available.
    sgpy_version : str | None
        ast-grep-py package version string.
    py_path : str
        Path to Python interpreter.
    py_version : str
        Python version string.
    """

    rpygrep_available: bool
    rpygrep_version: str | None
    sgpy_available: bool
    sgpy_version: str | None
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

        # Detect rpygrep Python package
        rpygrep_available = False
        rpygrep_version = None
        try:
            import importlib.metadata
        except ImportError:
            # importlib.metadata not available (Python < 3.8)
            pass
        else:
            try:
                rpygrep_version = importlib.metadata.version("rpygrep")
                rpygrep_available = True
            except importlib.metadata.PackageNotFoundError:
                # rpygrep package not installed
                pass

        # Detect ast-grep-py Python package
        sgpy_available = False
        sgpy_version = None
        try:
            import importlib.metadata
        except ImportError:
            pass
        else:
            try:
                sgpy_version = importlib.metadata.version("ast-grep-py")
                sgpy_available = True
            except importlib.metadata.PackageNotFoundError:
                # ast-grep-py package not installed
                pass

        # Python is always available
        py_path = sys.executable
        py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

        return Toolchain(
            rpygrep_available=rpygrep_available,
            rpygrep_version=rpygrep_version,
            sgpy_available=sgpy_available,
            sgpy_version=sgpy_version,
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
            "rpygrep": self.rpygrep_version,
            "sgpy": self.sgpy_version,
            "python": self.py_version,
        }

    def require_rpygrep(self) -> None:
        """Verify rpygrep package is available, raising if not.

        Raises
        ------
        RuntimeError
            If rpygrep is not installed.
        """
        if not self.rpygrep_available:
            msg = (
                "rpygrep Python package is required but not found. "
                "Install with: pip install rpygrep or uv add rpygrep"
            )
            raise RuntimeError(msg)

    def require_sgpy(self) -> None:
        """Verify ast-grep-py package is available, raising if not.

        Raises
        ------
        RuntimeError
            If ast-grep-py is not installed.
        """
        if not self.sgpy_available:
            msg = (
                "ast-grep-py Python package is required but not found. "
                "Install with: pip install ast-grep-py or uv add ast-grep-py"
            )
            raise RuntimeError(msg)

    @property
    def has_sgpy(self) -> bool:
        """Check if ast-grep-py is available.

        Returns
        -------
        bool
            True if ast-grep-py Python package is installed.
        """
        return self.sgpy_available

    @property
    def has_rpygrep(self) -> bool:
        """Check if rpygrep is available.

        Returns
        -------
        bool
            True if rpygrep Python package is installed.
        """
        return self.rpygrep_available
