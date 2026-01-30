"""SCIP (Source Code Intelligence Protocol) extraction subsystem.

This subpackage provides SCIP index extraction and parsing utilities:
- extract.py: Main SCIP extraction logic
- setup.py: SCIP protobuf loading and indexer configuration
- identity.py: SCIP project identity resolution
"""

from __future__ import annotations

from extract.extractors.scip.identity import (
    DEFAULT_ORG_PREFIX,
    DEFAULT_PROJECT_VERSION,
    ScipIdentity,
    resolve_scip_identity,
)
from extract.extractors.scip.setup import (
    BUILD_SUBDIR,
    ScipIndexPaths,
    ScipProtoLoadError,
    build_scip_index_options,
    ensure_scip_build_dir,
    ensure_scip_pb2,
    load_scip_pb2_from_build,
    resolve_scip_paths,
    scip_environment_payload,
    write_scip_environment_json,
)

__all__ = [
    "BUILD_SUBDIR",
    "DEFAULT_ORG_PREFIX",
    "DEFAULT_PROJECT_VERSION",
    "ScipIdentity",
    "ScipIndexPaths",
    "ScipProtoLoadError",
    "build_scip_index_options",
    "ensure_scip_build_dir",
    "ensure_scip_pb2",
    "load_scip_pb2_from_build",
    "resolve_scip_identity",
    "resolve_scip_paths",
    "scip_environment_payload",
    "write_scip_environment_json",
]
