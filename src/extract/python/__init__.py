"""Python-specific scope and environment utilities.

This subpackage provides:
- Python extension discovery and scope helpers (scope)
- Python environment profiling (env_profile)
"""

from __future__ import annotations

from extract.python.env_profile import (
    PythonEnvProfile,
    resolve_python_env_profile,
)
from extract.python.scope import (
    PythonExtensionCatalog,
    PythonScopePolicy,
    globs_for_extensions,
    resolve_python_extension_catalog,
)

__all__ = [
    # env_profile
    "PythonEnvProfile",
    # scope
    "PythonExtensionCatalog",
    "PythonScopePolicy",
    "globs_for_extensions",
    "resolve_python_env_profile",
    "resolve_python_extension_catalog",
]
