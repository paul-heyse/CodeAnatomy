"""View artifact registry payloads derived from DataFusion plan bundles."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from arrow_utils.schema.abi import schema_fingerprint, schema_to_dict, schema_to_msgpack
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
    plan_task_signature : str
        Runtime-aware task signature derived from the plan bundle.
    schema : pa.Schema
        View output schema.
    required_udfs : tuple[str, ...]
        UDF names required by this view.
    referenced_tables : tuple[str, ...]
        Table names referenced by this view.
    """

    name: str
    plan_fingerprint: str
    plan_task_signature: str
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
            "plan_task_signature": self.plan_task_signature,
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
            "plan_task_signature": self.plan_task_signature,
            "schema_fingerprint": schema_fingerprint(self.schema),
            "schema_msgpack": schema_to_msgpack(self.schema),
            "required_udfs": list(self.required_udfs),
            "referenced_tables": list(self.referenced_tables),
        }


_PLAN_TASK_SIGNATURE_VERSION = 1


def _hash_payload(payload: object) -> str:
    return hashlib.sha256(dumps_msgpack(payload)).hexdigest()


def _df_settings_hash(df_settings: Mapping[str, str]) -> str:
    entries = tuple(sorted((str(key), str(value)) for key, value in df_settings.items()))
    return _hash_payload(entries)


def _delta_inputs_payload(
    bundle: DataFusionPlanBundle,
) -> tuple[tuple[str, int | None, str | None], ...]:
    payload: list[tuple[str, int | None, str | None]] = []
    for item in bundle.delta_inputs:
        dataset_name = item.dataset_name
        if not dataset_name:
            continue
        payload.append((dataset_name, item.version, item.timestamp))
    return tuple(sorted(payload, key=lambda entry: entry[0]))


def _plan_task_signature(bundle: DataFusionPlanBundle, *, runtime_hash: str | None) -> str:
    artifacts = bundle.artifacts
    df_settings_hash = (
        _df_settings_hash(artifacts.df_settings)
        if isinstance(artifacts.df_settings, Mapping)
        else ""
    )
    payload = (
        ("version", _PLAN_TASK_SIGNATURE_VERSION),
        ("runtime_hash", runtime_hash or ""),
        ("plan_fingerprint", bundle.plan_fingerprint),
        ("function_registry_hash", artifacts.function_registry_hash),
        ("udf_snapshot_hash", artifacts.udf_snapshot_hash),
        ("rewrite_tags", tuple(sorted(artifacts.rewrite_tags))),
        ("domain_planner_names", tuple(sorted(artifacts.domain_planner_names))),
        ("df_settings_hash", df_settings_hash),
        ("required_udfs", tuple(sorted(bundle.required_udfs))),
        ("required_rewrite_tags", tuple(sorted(bundle.required_rewrite_tags))),
        ("delta_inputs", _delta_inputs_payload(bundle)),
    )
    return _hash_payload(payload)


@dataclass(frozen=True)
class ViewArtifactLineage:
    """Lineage inputs for view artifact construction."""

    required_udfs: tuple[str, ...]
    referenced_tables: tuple[str, ...]


@dataclass(frozen=True)
class ViewArtifactRequest:
    """Inputs required to build a view artifact."""

    name: str
    schema: pa.Schema
    lineage: ViewArtifactLineage
    runtime_hash: str | None = None


def build_view_artifact_from_bundle(
    bundle: DataFusionPlanBundle,
    *,
    request: ViewArtifactRequest,
) -> DataFusionViewArtifact:
    """Build a DataFusionViewArtifact from a DataFusion plan bundle.

    Parameters
    ----------
    bundle
        DataFusion plan bundle containing optimized logical plan.
    request
        View artifact request payload.

    Returns
    -------
    DataFusionViewArtifact
        DataFusion-native view artifact.
    """
    plan_task_signature = _plan_task_signature(bundle, runtime_hash=request.runtime_hash)
    return DataFusionViewArtifact(
        name=request.name,
        plan_fingerprint=bundle.plan_fingerprint,
        plan_task_signature=plan_task_signature,
        schema=request.schema,
        required_udfs=request.lineage.required_udfs,
        referenced_tables=request.lineage.referenced_tables,
    )


VIEW_ARTIFACT_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("plan_fingerprint", pa.string(), nullable=False),
        pa.field("plan_task_signature", pa.string(), nullable=False),
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
        plan_task_signature = row.get("plan_task_signature") or plan_fingerprint
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
                "plan_task_signature": str(plan_task_signature),
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
    "ViewArtifactLineage",
    "ViewArtifactRequest",
    "build_view_artifact_from_bundle",
    "view_artifact_payload_table",
]
