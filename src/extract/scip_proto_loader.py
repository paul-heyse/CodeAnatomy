"""Load scip_pb2 bindings from build/scip."""

from __future__ import annotations

import importlib.util
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
