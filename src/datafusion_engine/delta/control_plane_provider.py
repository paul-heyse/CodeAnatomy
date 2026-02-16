"""Provider construction helpers for the Delta control plane."""

from __future__ import annotations

from typing import cast

from datafusion import SessionContext

from datafusion_engine.delta.control_plane_core import (
    DeltaCdfProviderBundle,
    DeltaProviderBundle,
    DeltaProviderRequest,
    _internal_ctx,
    _parse_add_actions,
    _raise_engine_error,
    _require_internal_entrypoint,
    _scan_effective_payload,
)
from datafusion_engine.delta.object_store import register_delta_object_store
from datafusion_engine.delta.payload import schema_ipc_payload
from datafusion_engine.delta.protocol import delta_feature_gate_rust_payload
from datafusion_engine.delta.protocols import DeltaProviderHandle
from datafusion_engine.delta.store_policy import resolve_storage_profile
from datafusion_engine.errors import ErrorKind
from utils.validation import ensure_mapping


def delta_provider_from_session(
    ctx: SessionContext,
    *,
    request: DeltaProviderRequest,
) -> DeltaProviderBundle:
    """Resolve a Delta provider bundle from a live session context.

    Returns:
    -------
    DeltaProviderBundle
        Provider capsule and associated control-plane metadata.
    """
    storage_profile = resolve_storage_profile(
        table_uri=request.table_uri,
        policy=None,
        storage_options=request.storage_options,
        log_storage_options=None,
    )
    register_delta_object_store(
        ctx,
        table_uri=storage_profile.canonical_uri,
        storage_options=storage_profile.to_datafusion_object_store_options(),
        storage_profile=storage_profile,
    )
    provider_factory = _require_internal_entrypoint("delta_table_provider_from_session")
    schema_ipc = schema_ipc_payload(request.delta_scan.schema) if request.delta_scan else None
    storage_payload = (
        list(storage_profile.to_deltalake_options().items())
        if storage_profile.storage_options
        else None
    )
    response = provider_factory(
        _internal_ctx(
            ctx,
            entrypoint="delta_table_provider_from_session",
        ),
        storage_profile.canonical_uri,
        storage_payload,
        request.version,
        request.timestamp,
        request.predicate,
        request.delta_scan.file_column_name if request.delta_scan else None,
        request.delta_scan.enable_parquet_pushdown if request.delta_scan else None,
        request.delta_scan.schema_force_view_types if request.delta_scan else None,
        request.delta_scan.wrap_partition_values if request.delta_scan else None,
        schema_ipc,
        *delta_feature_gate_rust_payload(request.gate),
    )
    payload = ensure_mapping(response, label="delta_table_provider_from_session")
    snapshot = ensure_mapping(payload.get("snapshot"), label="snapshot")
    scan_config = ensure_mapping(payload.get("scan_config"), label="scan_config")
    provider = payload.get("provider")
    if provider is None:
        msg = "Delta control-plane response missing provider capsule."
        _raise_engine_error(msg, kind=ErrorKind.DELTA)
    return DeltaProviderBundle(
        provider=cast("DeltaProviderHandle", provider),
        snapshot=snapshot,
        scan_config=scan_config,
        scan_effective=_scan_effective_payload(scan_config),
        add_actions=_parse_add_actions(payload.get("add_actions")),
        predicate_error=cast("str | None", payload.get("predicate_error")),
    )


__all__ = [
    "DeltaCdfProviderBundle",
    "DeltaProviderBundle",
    "DeltaProviderRequest",
    "delta_provider_from_session",
]
