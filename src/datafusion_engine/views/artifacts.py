"""View artifact registry payloads derived from DataFusion plan bundles."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Literal, cast

import msgspec
import pyarrow as pa

from datafusion_engine.arrow.abi import schema_to_dict, schema_to_msgpack
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.schema.contracts import SCHEMA_ABI_FINGERPRINT_META
from serde_artifacts import ViewArtifactPayload
from serde_msgspec import (
    StructBaseCompat,
    StructBaseStrict,
    convert,
    dumps_msgpack,
    to_builtins,
    validation_error_payload,
)
from utils.hashing import hash_msgpack_canonical, hash_settings

CachePolicy = Literal["none", "delta_staging", "delta_output"]

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact


def _coerce_payload_list(value: object, *, field: str) -> list[object]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    msg = f"Expected {field} to be a list."
    raise TypeError(msg)


class DataFusionViewArtifact(StructBaseCompat, frozen=True):
    """DataFusion-native view artifact using plan bundles.

    Attributes:
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
    cache_policy : CachePolicy
        Semantic cache policy for the view.
    """

    name: str
    plan_fingerprint: str
    plan_task_signature: str
    schema: pa.Schema
    required_udfs: tuple[str, ...]
    referenced_tables: tuple[str, ...]
    cache_policy: CachePolicy = "none"
    schema_describe: tuple[Mapping[str, object], ...] = ()
    schema_provenance: Mapping[str, object] | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for diagnostics and persistence.

        Returns:
        -------
        dict[str, object]
            JSON-serializable payload for diagnostics and storage.
        """
        payload = ViewArtifactPayload(
            name=self.name,
            plan_fingerprint=self.plan_fingerprint,
            plan_task_signature=self.plan_task_signature,
            schema=schema_to_dict(self.schema),
            schema_describe=tuple(dict(row) for row in self.schema_describe),
            schema_provenance=(
                dict(self.schema_provenance) if self.schema_provenance is not None else {}
            ),
            required_udfs=tuple(self.required_udfs),
            referenced_tables=tuple(self.referenced_tables),
        )
        payload_dict = cast("dict[str, object]", to_builtins(payload, str_keys=True))
        payload_dict["schema_describe"] = _coerce_payload_list(
            payload_dict.get("schema_describe"),
            field="schema_describe",
        )
        payload_dict["required_udfs"] = _coerce_payload_list(
            payload_dict.get("required_udfs"),
            field="required_udfs",
        )
        payload_dict["referenced_tables"] = _coerce_payload_list(
            payload_dict.get("referenced_tables"),
            field="referenced_tables",
        )
        return payload_dict

    def diagnostics_payload(self, *, event_time_unix_ms: int) -> dict[str, object]:
        """Return a stable diagnostics payload.

        Parameters
        ----------
        event_time_unix_ms
            Event timestamp in milliseconds.

        Returns:
        -------
        dict[str, object]
            Diagnostics-ready payload with serialized schema.
        """
        return {
            "event_time_unix_ms": event_time_unix_ms,
            "name": self.name,
            "plan_fingerprint": self.plan_fingerprint,
            "plan_task_signature": self.plan_task_signature,
            "schema_identity_hash": schema_identity_hash(self.schema),
            "schema_msgpack": schema_to_msgpack(self.schema),
            "schema_describe_msgpack": dumps_msgpack([dict(row) for row in self.schema_describe]),
            "schema_provenance_msgpack": dumps_msgpack(
                dict(self.schema_provenance) if self.schema_provenance is not None else {}
            ),
            "required_udfs": list(self.required_udfs),
            "referenced_tables": list(self.referenced_tables),
        }


_PLAN_TASK_SIGNATURE_VERSION = 2


def _delta_inputs_payload(
    bundle: DataFusionPlanArtifact,
) -> tuple[tuple[str, int | None, str | None], ...]:
    payload: list[tuple[str, int | None, str | None]] = []
    for item in bundle.delta_inputs:
        dataset_name = item.dataset_name
        if not dataset_name:
            continue
        payload.append((dataset_name, item.version, item.timestamp))
    return tuple(sorted(payload, key=lambda entry: entry[0]))


def _plan_task_signature(bundle: DataFusionPlanArtifact, *, runtime_hash: str | None) -> str:
    artifacts = bundle.artifacts
    df_settings_hash = (
        hash_settings(artifacts.df_settings) if isinstance(artifacts.df_settings, Mapping) else ""
    )
    payload = (
        ("version", _PLAN_TASK_SIGNATURE_VERSION),
        ("runtime_hash", runtime_hash or ""),
        ("plan_fingerprint", bundle.plan_fingerprint),
        ("function_registry_hash", artifacts.function_registry_hash),
        ("udf_snapshot_hash", artifacts.udf_snapshot_hash),
        ("planning_env_hash", artifacts.planning_env_hash),
        ("rulepack_hash", artifacts.rulepack_hash or ""),
        ("information_schema_hash", artifacts.information_schema_hash),
        ("rewrite_tags", tuple(sorted(artifacts.rewrite_tags))),
        ("domain_planner_names", tuple(sorted(artifacts.domain_planner_names))),
        ("df_settings_hash", df_settings_hash),
        ("required_udfs", tuple(sorted(bundle.required_udfs))),
        ("required_rewrite_tags", tuple(sorted(bundle.required_rewrite_tags))),
        ("delta_inputs", _delta_inputs_payload(bundle)),
    )
    return hash_msgpack_canonical(payload)


class ViewArtifactLineage(StructBaseStrict, frozen=True):
    """Lineage inputs for view artifact construction."""

    required_udfs: tuple[str, ...]
    referenced_tables: tuple[str, ...]


class ViewArtifactRequest(StructBaseStrict, frozen=True):
    """Inputs required to build a view artifact."""

    name: str
    schema: pa.Schema
    lineage: ViewArtifactLineage
    runtime_hash: str | None = None
    cache_policy: CachePolicy = "none"


def build_view_artifact_from_bundle(
    bundle: DataFusionPlanArtifact,
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

    Returns:
    -------
    DataFusionViewArtifact
        DataFusion-native view artifact.
    """
    plan_task_signature = _plan_task_signature(bundle, runtime_hash=request.runtime_hash)
    schema_describe = _schema_describe_payload(request.schema)
    schema_provenance = _schema_provenance_payload(request.schema)
    return DataFusionViewArtifact(
        name=request.name,
        plan_fingerprint=bundle.plan_fingerprint,
        plan_task_signature=plan_task_signature,
        schema=request.schema,
        required_udfs=request.lineage.required_udfs,
        referenced_tables=request.lineage.referenced_tables,
        cache_policy=request.cache_policy,
        schema_describe=schema_describe,
        schema_provenance=schema_provenance,
    )


VIEW_ARTIFACT_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("plan_fingerprint", pa.string(), nullable=False),
        pa.field("plan_task_signature", pa.string(), nullable=False),
        pa.field("schema_msgpack", pa.binary(), nullable=False),
        pa.field("schema_describe_msgpack", pa.binary(), nullable=True),
        pa.field("schema_provenance_msgpack", pa.binary(), nullable=True),
        pa.field("required_udfs", pa.list_(pa.string()), nullable=True),
        pa.field("referenced_tables", pa.list_(pa.string()), nullable=True),
    ]
)


def view_artifact_payload_table(rows: Sequence[Mapping[str, object]]) -> pa.Table:
    """Build a deterministic Arrow table for view artifact payloads.

    Args:
        rows: Description.

    Returns:
        pa.Table: Result.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    normalized: list[dict[str, object]] = []
    for row in rows:
        payload_raw = dict(row)
        if not payload_raw.get("plan_task_signature") and payload_raw.get("plan_fingerprint"):
            payload_raw["plan_task_signature"] = payload_raw["plan_fingerprint"]
        try:
            payload = convert(payload_raw, target_type=ViewArtifactPayload, strict=True)
        except msgspec.ValidationError as exc:
            details = validation_error_payload(exc)
            msg = f"View artifact payload validation failed: {details}"
            raise ValueError(msg) from exc
        normalized.append(
            {
                "name": payload.name,
                "plan_fingerprint": payload.plan_fingerprint,
                "plan_task_signature": payload.plan_task_signature,
                "schema_msgpack": dumps_msgpack(payload.schema),
                "schema_describe_msgpack": dumps_msgpack(payload.schema_describe),
                "schema_provenance_msgpack": dumps_msgpack(payload.schema_provenance),
                "required_udfs": list(payload.required_udfs) or None,
                "referenced_tables": list(payload.referenced_tables) or None,
            }
        )
    return pa.Table.from_pylist(normalized, schema=VIEW_ARTIFACT_PAYLOAD_SCHEMA)


def _schema_metadata_payload(schema: pa.Schema) -> dict[str, str]:
    metadata = schema.metadata or {}
    items = sorted(metadata.items(), key=lambda item: str(item[0]))
    return {
        (key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)): (
            value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
        )
        for key, value in items
    }


def _schema_describe_payload(schema: pa.Schema) -> tuple[Mapping[str, object], ...]:
    return tuple(
        {
            "column_name": field.name,
            "data_type": str(field.type),
            "nullable": field.nullable,
            "source": "arrow_schema",
        }
        for field in schema
    )


def _schema_provenance_payload(schema: pa.Schema) -> Mapping[str, object]:
    metadata_payload = _schema_metadata_payload(schema)
    abi_key = (
        SCHEMA_ABI_FINGERPRINT_META.decode("utf-8")
        if isinstance(SCHEMA_ABI_FINGERPRINT_META, bytes)
        else str(SCHEMA_ABI_FINGERPRINT_META)
    )
    abi_value = metadata_payload.get(abi_key)
    return {
        "source": "arrow_schema",
        "schema_identity_hash": schema_identity_hash(schema),
        "schema_metadata": metadata_payload,
        "explicit_schema": abi_value is not None,
        "schema_abi_fingerprint": abi_value,
    }


__all__ = [
    "VIEW_ARTIFACT_PAYLOAD_SCHEMA",
    "CachePolicy",
    "DataFusionViewArtifact",
    "ViewArtifactLineage",
    "ViewArtifactRequest",
    "build_view_artifact_from_bundle",
    "view_artifact_payload_table",
]
