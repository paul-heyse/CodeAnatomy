"""Shared SCIP role flag metadata for CPG builders."""

from __future__ import annotations

from cpg.kinds_ultimate import SCIP_ROLE_FORWARD_DEFINITION, SCIP_ROLE_GENERATED, SCIP_ROLE_TEST

ROLE_FLAG_SPECS: tuple[tuple[str, int, str], ...] = (
    ("generated", SCIP_ROLE_GENERATED, "scip_role_generated"),
    ("test", SCIP_ROLE_TEST, "scip_role_test"),
    ("forward_definition", SCIP_ROLE_FORWARD_DEFINITION, "scip_role_forward_definition"),
)

__all__ = ["ROLE_FLAG_SPECS"]
