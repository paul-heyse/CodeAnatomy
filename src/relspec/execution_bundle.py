"""Execution bundle and plan signature helpers for relspec."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from arrowdsl.io.ipc import payload_hash
from datafusion_engine.schema_introspection import schema_map_fingerprint_from_mapping
from sqlglot_tools.lineage import LineagePayload
from sqlglot_tools.optimizer import (
    SqlGlotPolicy,
    SqlGlotPolicySnapshot,
    canonical_ast_fingerprint,
    plan_fingerprint,
    resolve_sqlglot_policy,
    sqlglot_policy_snapshot_for,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlglot_tools.compat import Expression
    from sqlglot_tools.optimizer import SchemaMapping

_SIGNATURE_VERSION: int = 1
_SIGNATURE_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("policy_hash", pa.string()),
        pa.field("policy_rules_hash", pa.string()),
        pa.field("schema_map_hash", pa.string()),
        pa.field("ast_hash", pa.string()),
        pa.field("plan_fingerprint", pa.string()),
    ]
)


@dataclass(frozen=True)
class ExecutionBundle:
    """Bundle of compilation artifacts for rule execution."""

    sqlglot_expr: Expression
    sqlglot_policy: SqlGlotPolicySnapshot
    schema_map_hash: str | None
    plan_fingerprint: str
    ast_fingerprint: str
    lineage: LineagePayload | None
    substrait_bytes: bytes | None
    df_logical_plan: str | None
    df_physical_plan: str | None
    artifacts: Mapping[str, object]

    def signature(self) -> str:
        """Return a stable signature for the bundle.

        Returns
        -------
        str
            Stable signature for this execution bundle.
        """
        return execution_bundle_signature(self)


def schema_map_fingerprint(schema_map: SchemaMapping | None) -> str | None:
    """Return a fingerprint for a SQLGlot schema map.

    Returns
    -------
    str | None
        Schema map fingerprint when available.
    """
    if schema_map is None:
        return None
    return schema_map_fingerprint_from_mapping(schema_map)


def sqlglot_plan_signature(
    expr: Expression,
    *,
    policy: SqlGlotPolicy | None = None,
    schema_map_hash: str | None = None,
) -> str:
    """Return a stable plan signature for a SQLGlot expression.

    Parameters
    ----------
    expr
        SQLGlot expression to fingerprint.
    policy
        SQLGlot policy used for normalization and generation.
    schema_map_hash
        Optional schema map fingerprint to include in the signature.

    Returns
    -------
    str
        Stable signature hash for the plan.
    """
    resolved_policy = policy or resolve_sqlglot_policy()
    policy_snapshot = sqlglot_policy_snapshot_for(resolved_policy)
    payload = {
        "version": _SIGNATURE_VERSION,
        "policy_hash": policy_snapshot.policy_hash,
        "policy_rules_hash": policy_snapshot.rules_hash,
        "schema_map_hash": schema_map_hash or "",
        "ast_hash": canonical_ast_fingerprint(expr),
        "plan_fingerprint": plan_fingerprint(expr, dialect=policy_snapshot.write_dialect),
    }
    return payload_hash(payload, _SIGNATURE_SCHEMA)


def execution_bundle_signature(bundle: ExecutionBundle) -> str:
    """Return a stable signature for an execution bundle.

    Returns
    -------
    str
        Stable signature hash for the bundle.
    """
    payload = {
        "version": _SIGNATURE_VERSION,
        "policy_hash": bundle.sqlglot_policy.policy_hash,
        "policy_rules_hash": bundle.sqlglot_policy.rules_hash,
        "schema_map_hash": bundle.schema_map_hash or "",
        "ast_hash": bundle.ast_fingerprint,
        "plan_fingerprint": bundle.plan_fingerprint,
    }
    return payload_hash(payload, _SIGNATURE_SCHEMA)


__all__ = [
    "ExecutionBundle",
    "execution_bundle_signature",
    "schema_map_fingerprint",
    "sqlglot_plan_signature",
]
