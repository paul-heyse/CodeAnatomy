"""View artifact registry payloads derived from DataFusion plan bundles."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from arrowdsl.schema.abi import schema_fingerprint, schema_to_dict, schema_to_msgpack
from serde_msgspec import dumps_msgpack

if TYPE_CHECKING:
    from datafusion_engine.plan_bundle import DataFusionPlanBundle


@dataclass(frozen=True)
class DataFusionViewArtifact:
    """DataFusion-native view artifact using plan bundles.

    Attributes
    ----------
    name : str
        View name.
    plan_fingerprint : str
        DataFusion plan fingerprint from the plan bundle.
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


VIEW_ARTIFACT_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("plan_fingerprint", pa.string(), nullable=False),
        pa.field("schema_msgpack", pa.binary(), nullable=False),
        pa.field("required_udfs", pa.list_(pa.string()), nullable=True),
        pa.field("referenced_tables", pa.list_(pa.string()), nullable=True),
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
        if name is None or plan_fingerprint is None:
            msg = "View artifact payload is missing required fields."
            raise ValueError(msg)
        schema_payload_raw = row.get("schema")
        schema_payload: Mapping[str, object] = {}
        if isinstance(schema_payload_raw, Mapping):
            schema_payload = schema_payload_raw
        required_udfs = row.get("required_udfs")
        referenced_tables = row.get("referenced_tables")
        normalized.append(
            {
                "name": str(name),
                "plan_fingerprint": str(plan_fingerprint),
                "schema_msgpack": dumps_msgpack(schema_payload),
                "required_udfs": (
                    [str(value) for value in required_udfs]
                    if isinstance(required_udfs, Sequence)
                    and not isinstance(required_udfs, (str, bytes))
                    else None
                ),
                "referenced_tables": (
                    [str(value) for value in referenced_tables]
                    if isinstance(referenced_tables, Sequence)
                    and not isinstance(referenced_tables, (str, bytes))
                    else None
                ),
            }
        )
    return pa.Table.from_pylist(normalized, schema=VIEW_ARTIFACT_PAYLOAD_SCHEMA)


__all__ = [
    "VIEW_ARTIFACT_PAYLOAD_SCHEMA",
    "DataFusionViewArtifact",
    "build_view_artifact_from_bundle",
    "view_artifact_payload_table",
]
