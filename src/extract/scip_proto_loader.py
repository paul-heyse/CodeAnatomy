"""Load scip_pb2 bindings from build/scip."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType


class ScipProtoLoadError(RuntimeError):
    """Raised when scip_pb2 module spec cannot be loaded."""


def load_scip_pb2_from_build(build_dir: Path) -> ModuleType:
    """Load the scip_pb2 module from the build/scip directory.

    Parameters
    ----------
    build_dir:
        Path to the build/scip directory.

    Returns
    -------
    types.ModuleType
        Loaded scip_pb2 module.

    Raises
    ------
    FileNotFoundError
        Raised when scip_pb2.py is missing.
    ScipProtoLoadError
        Raised when the module spec cannot be loaded.
    """
    module_path = build_dir / "scip_pb2.py"
    if not module_path.exists():
        raise FileNotFoundError(module_path)

    spec = importlib.util.spec_from_file_location("scip_pb2", module_path)
    if spec is None or spec.loader is None:
        raise ScipProtoLoadError

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def ensure_scip_pb2(repo_root: Path, build_dir: Path) -> Path:
    """Ensure scip_pb2.py exists under build/scip, generating it if missing.

    Parameters
    ----------
    repo_root:
        Repository root path.
    build_dir:
        Build/scip directory path.

    Returns
    -------
    pathlib.Path
        Path to the scip_pb2.py file.

    Raises
    ------
    FileNotFoundError
        Raised when the codegen script or generated module is missing.
    """
    module_path = build_dir / "scip_pb2.py"
    if module_path.exists():
        return module_path

    script = repo_root / "scripts" / "scip_proto_codegen.py"
    if not script.exists():
        raise FileNotFoundError(script)

    subprocess.run(
        [
            sys.executable,
            str(script),
            "--repo-root",
            str(repo_root),
            "--build-subdir",
            str(build_dir),
        ],
        check=True,
    )
    if not module_path.exists():
        raise FileNotFoundError(module_path)
    return module_path
