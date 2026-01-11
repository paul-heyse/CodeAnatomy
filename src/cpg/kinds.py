"""Compatibility shim for CPG kinds and roles."""

from __future__ import annotations

from enum import StrEnum

from cpg.kinds_ultimate import (
    EdgeKind,
    NodeKind,
    validate_derivation_extractors,
    validate_registry_completeness,
)


class EntityKind(StrEnum):
    """Define entity kinds for properties tables."""

    NODE = "node"
    EDGE = "edge"


SCIP_ROLE_DEFINITION = 1
SCIP_ROLE_IMPORT = 2
SCIP_ROLE_WRITE = 4
SCIP_ROLE_READ = 8
SCIP_ROLE_GENERATED = 16
SCIP_ROLE_TEST = 32
SCIP_ROLE_FORWARD_DEFINITION = 64

__all__ = [
    "SCIP_ROLE_DEFINITION",
    "SCIP_ROLE_FORWARD_DEFINITION",
    "SCIP_ROLE_GENERATED",
    "SCIP_ROLE_IMPORT",
    "SCIP_ROLE_READ",
    "SCIP_ROLE_TEST",
    "SCIP_ROLE_WRITE",
    "EdgeKind",
    "EntityKind",
    "NodeKind",
    "validate_derivation_extractors",
    "validate_registry_completeness",
]
