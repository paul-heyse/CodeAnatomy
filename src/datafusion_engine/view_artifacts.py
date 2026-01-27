"""View artifact registry payloads derived from canonical SQLGlot ASTs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import sqlglot.expressions as exp
from sqlglot.schema import MappingSchema

from arrowdsl.schema.abi import schema_fingerprint, schema_to_dict, schema_to_msgpack
from datafusion_engine.schema_introspection import schema_map_for_sqlglot
from datafusion_engine.sql_policy_engine import CompilationArtifacts, SQLPolicyProfile
from serde_msgspec import dumps_msgpack
from sqlglot_tools.lineage import referenced_udf_calls
from sqlglot_tools.optimizer import (
    ast_policy_fingerprint,
    sqlglot_policy_snapshot_for,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.schema_introspection import SchemaIntrospector

# Runtime import for DataFusionPlanBundle (used in function signatures)
from datafusion_engine.plan_bundle import DataFusionPlanBundle


@dataclass(frozen=True)
class ViewArtifact:
    """Canonical artifact bundle for a registered view.

    DEPRECATED: Use DataFusionViewArtifact for new code.
    """

    name: str
    ast: exp.Expression
    serde_payload: list[dict[str, object]]
    ast_fingerprint: str
    policy_hash: str
    plan_fingerprint: str
    schema: pa.Schema
    lineage: dict[str, set[tuple[str, str]]]
    required_udfs: tuple[str, ...]
    sql: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for diagnostics and persistence.

        Returns
        -------
        dict[str, object]
            JSON-serializable payload for diagnostics and storage.
        """
        return {
            "name": self.name,
            "plan_fingerprint": self.plan_fingerprint,
            "ast_fingerprint": self.ast_fingerprint,
            "policy_hash": self.policy_hash,
            "schema": schema_to_dict(self.schema),
            "required_udfs": list(self.required_udfs),
            "lineage": _lineage_payload(self.lineage),
            "sql": self.sql,
            "serde_payload": list(self.serde_payload),
        }

    def diagnostics_payload(self, *, event_time_unix_ms: int) -> dict[str, object]:
        """Return a stable diagnostics payload.

        Parameters
        ----------
        event_time_unix_ms:
            Event timestamp in milliseconds.

        Returns
        -------
        dict[str, object]
            Diagnostics-ready payload with serialized schema and AST artifacts.
        """
        lineage_payload = _lineage_payload(self.lineage)
        return {
            "event_time_unix_ms": event_time_unix_ms,
            "name": self.name,
            "plan_fingerprint": self.plan_fingerprint,
            "ast_fingerprint": self.ast_fingerprint,
            "policy_hash": self.policy_hash,
            "schema_fingerprint": schema_fingerprint(self.schema),
            "schema_msgpack": schema_to_msgpack(self.schema),
            "required_udfs": list(self.required_udfs),
            "lineage_msgpack": dumps_msgpack(lineage_payload),
            "sql": self.sql,
            "serde_payload_msgpack": dumps_msgpack(list(self.serde_payload)),
        }


@dataclass(frozen=True)
class DataFusionViewArtifact:
    """DataFusion-native view artifact using plan_bundle instead of AST.

    This artifact uses DataFusion's plan fingerprint directly without requiring
    SQLGlot AST or policy_hash, making it suitable for DataFusion-native views.

    Attributes
    ----------
    name : str
        View name.
    plan_fingerprint : str
        DataFusion plan fingerprint from plan_bundle.
    schema : pa.Schema
        View output schema.
    required_udfs : tuple[str, ...]
        UDF names required by this view.
    referenced_tables : tuple[str, ...]
        Table names referenced by this view.
    """

    name: str
    plan_fingerprint: str
    schema: pa.Schema
    required_udfs: tuple[str, ...]
    referenced_tables: tuple[str, ...]

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for diagnostics and persistence.

        Returns
        -------
        dict[str, object]
            JSON-serializable payload for diagnostics and storage.
        """
        return {
            "name": self.name,
            "plan_fingerprint": self.plan_fingerprint,
            "schema": schema_to_dict(self.schema),
            "required_udfs": list(self.required_udfs),
            "referenced_tables": list(self.referenced_tables),
        }

    def diagnostics_payload(self, *, event_time_unix_ms: int) -> dict[str, object]:
        """Return a stable diagnostics payload.

        Parameters
        ----------
        event_time_unix_ms
            Event timestamp in milliseconds.

        Returns
        -------
        dict[str, object]
            Diagnostics-ready payload with serialized schema.
        """
        return {
            "event_time_unix_ms": event_time_unix_ms,
            "name": self.name,
            "plan_fingerprint": self.plan_fingerprint,
            "schema_fingerprint": schema_fingerprint(self.schema),
            "schema_msgpack": schema_to_msgpack(self.schema),
            "required_udfs": list(self.required_udfs),
            "referenced_tables": list(self.referenced_tables),
        }


@dataclass(frozen=True)
class ViewArtifactInputs:
    """Inputs for building a view artifact."""

    ctx: SessionContext
    name: str
    ast: exp.Expression
    schema: pa.Schema
    required_udfs: Sequence[str] | None = None
    policy_profile: SQLPolicyProfile | None = None
    sql: str | None = None


def build_view_artifact(inputs: ViewArtifactInputs) -> ViewArtifact:
    """Build a ViewArtifact from a canonical SQLGlot AST.

    DEPRECATED: Use build_view_artifact_from_bundle for DataFusion-native views.
    This function uses SQLGlot policy snapshots which are deprecated.

    Returns
    -------
    ViewArtifact
        View artifact bundle for diagnostics and caching.
    """
    profile = inputs.policy_profile or SQLPolicyProfile()
    policy = profile.to_sqlglot_policy()
    # DEPRECATED: Use plan_fingerprint from DataFusionPlanBundle instead
    policy_hash = sqlglot_policy_snapshot_for(policy).policy_hash
    schema_map = schema_map_for_sqlglot(_schema_introspector(inputs.ctx))
    mapping = MappingSchema(dict(schema_map))
    artifacts = CompilationArtifacts.from_ast(inputs.ast, mapping)
    # DEPRECATED: ast_policy_fingerprint combines ast_fingerprint + policy_hash
    # Use plan_fingerprint from DataFusionPlanBundle instead
    plan_fingerprint = ast_policy_fingerprint(
        ast_fingerprint=artifacts.ast_fingerprint,
        policy_hash=policy_hash,
    )
    required_udfs = (
        tuple(inputs.required_udfs)
        if inputs.required_udfs is not None
        else tuple(sorted(referenced_udf_calls(inputs.ast)))
    )
    return ViewArtifact(
        name=inputs.name,
        ast=inputs.ast,
        serde_payload=artifacts.serde_payload,
        ast_fingerprint=artifacts.ast_fingerprint,  # DEPRECATED
        policy_hash=policy_hash,  # DEPRECATED
        plan_fingerprint=plan_fingerprint,  # DEPRECATED: Use bundle.plan_fingerprint
        schema=inputs.schema,
        lineage=artifacts.lineage_by_column,
        required_udfs=required_udfs,
        sql=inputs.sql,
    )


def build_view_artifact_from_bundle(
    bundle: DataFusionPlanBundle,
    *,
    name: str,
    schema: pa.Schema,
    required_udfs: tuple[str, ...],
    referenced_tables: tuple[str, ...],
) -> DataFusionViewArtifact:
    """Build a DataFusionViewArtifact from a DataFusion plan bundle.

    Parameters
    ----------
    bundle
        DataFusion plan bundle containing optimized logical plan.
    name
        View name.
    schema
        View output schema.
    required_udfs
        Required UDF names.
    referenced_tables
        Referenced table names.

    Returns
    -------
    DataFusionViewArtifact
        DataFusion-native view artifact.
    """
    return DataFusionViewArtifact(
        name=name,
        plan_fingerprint=bundle.plan_fingerprint,
        schema=schema,
        required_udfs=required_udfs,
        referenced_tables=referenced_tables,
    )


def _schema_introspector(ctx: SessionContext) -> SchemaIntrospector:
    from datafusion_engine.schema_introspection import SchemaIntrospector

    return SchemaIntrospector(ctx)


def _lineage_payload(
    lineage: Mapping[str, set[tuple[str, str]]],
) -> dict[str, list[dict[str, str]]]:
    payload: dict[str, list[dict[str, str]]] = {}
    for target, sources in lineage.items():
        payload[target] = [{"table": table, "column": column} for table, column in sorted(sources)]
    return payload


VIEW_ARTIFACT_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("plan_fingerprint", pa.string(), nullable=False),
        pa.field("ast_fingerprint", pa.string(), nullable=False),
        pa.field("policy_hash", pa.string(), nullable=False),
        pa.field("schema_msgpack", pa.binary(), nullable=False),
        pa.field("required_udfs", pa.list_(pa.string()), nullable=True),
        pa.field("lineage_msgpack", pa.binary(), nullable=False),
        pa.field("sql", pa.string(), nullable=True),
        pa.field("serde_payload_msgpack", pa.binary(), nullable=False),
    ]
)


def view_artifact_payload_table(rows: Sequence[Mapping[str, object]]) -> pa.Table:
    """Build a deterministic Arrow table for view artifact payloads.

    Parameters
    ----------
    rows
        View artifact payloads as dictionaries.

    Returns
    -------
    pyarrow.Table
        Arrow table with the canonical view artifact schema.

    Raises
    ------
    ValueError
        Raised when a payload is missing required fields.
    """
    normalized: list[dict[str, object]] = []
    for row in rows:
        name = row.get("name")
        plan_fingerprint = row.get("plan_fingerprint")
        ast_fingerprint = row.get("ast_fingerprint")
        policy_hash = row.get("policy_hash")
        if (
            name is None
            or plan_fingerprint is None
            or ast_fingerprint is None
            or policy_hash is None
        ):
            msg = "View artifact payload is missing required fields."
            raise ValueError(msg)
        schema_payload_raw = row.get("schema")
        lineage_payload_raw = row.get("lineage")
        serde_payload_raw = row.get("serde_payload")
        schema_payload: Mapping[str, object] = {}
        lineage_payload: Mapping[str, object] = {}
        serde_payload: Sequence[object] = ()
        if isinstance(schema_payload_raw, Mapping):
            schema_payload = schema_payload_raw
        if isinstance(lineage_payload_raw, Mapping):
            lineage_payload = lineage_payload_raw
        if isinstance(serde_payload_raw, Sequence) and not isinstance(
            serde_payload_raw, (str, bytes)
        ):
            serde_payload = serde_payload_raw
        required_udfs = row.get("required_udfs")
        normalized.append(
            {
                "name": str(name),
                "plan_fingerprint": str(plan_fingerprint),
                "ast_fingerprint": str(ast_fingerprint),
                "policy_hash": str(policy_hash),
                "schema_msgpack": dumps_msgpack(schema_payload),
                "required_udfs": (
                    [str(value) for value in required_udfs]
                    if isinstance(required_udfs, Sequence)
                    and not isinstance(required_udfs, (str, bytes))
                    else None
                ),
                "lineage_msgpack": dumps_msgpack(lineage_payload),
                "sql": str(row["sql"]) if row.get("sql") is not None else None,
                "serde_payload_msgpack": dumps_msgpack(serde_payload),
            }
        )
    return pa.Table.from_pylist(normalized, schema=VIEW_ARTIFACT_PAYLOAD_SCHEMA)


__all__ = [
    "VIEW_ARTIFACT_PAYLOAD_SCHEMA",
    "DataFusionViewArtifact",
    "ViewArtifact",
    "ViewArtifactInputs",
    "build_view_artifact",
    "build_view_artifact_from_bundle",
    "view_artifact_payload_table",
]
