"""Table-provider artifacts and capability helpers for dataset registration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.delta.provider_artifacts import delta_scan_snapshot_payload
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.lineage.diagnostics import record_artifact
from datafusion_engine.tables.metadata import (
    record_table_provider_metadata,
    table_provider_metadata,
)
from obs.otel import get_run_id

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class TableProviderArtifact:
    """Artifact payload describing one registered table provider."""

    name: str
    provider: object | None
    provider_kind: str
    source: object | None = None
    details: Mapping[str, object] | None = None


@dataclass(frozen=True)
class _ProviderModeContext:
    name: str
    provider_kind: str
    provider: object | None
    capsule_id: str | None
    details: Mapping[str, object] | None


def table_provider_capsule(provider: object) -> object | None:
    """Return a DataFusion table-provider capsule when the provider exposes one."""
    attr = getattr(provider, "__datafusion_table_provider__", None)
    if callable(attr):
        return attr()
    attr = getattr(provider, "datafusion_table_provider", None)
    if callable(attr):
        return attr()
    return None


def provider_capsule_id(provider: object) -> str:
    """Return a stable identifier for a DataFusion table provider capsule.

    Raises:
        AttributeError: If ``provider`` does not expose a table-provider capsule.
    """
    capsule = table_provider_capsule(provider)
    if capsule is None:
        msg = "Provider does not expose a DataFusion table provider capsule."
        raise AttributeError(msg)
    return repr(capsule)


def _provider_capsule_id(
    provider: object | None,
    *,
    source: object | None,
) -> str | None:
    if source is not None:
        try:
            return provider_capsule_id(source)
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return None
    if provider is None:
        return None
    return repr(provider)


def _provider_pushdown_value(provider: object, *, names: Sequence[str]) -> str | bool | None:
    for name in names:
        attr = getattr(provider, name, None)
        if isinstance(attr, bool):
            return attr
        if callable(attr):
            try:
                value = attr()
            except TypeError:
                continue
            if isinstance(value, bool):
                return value
            if value is None:
                return None
            return str(value)
    return None


def _provider_pushdown_hints(provider: object) -> dict[str, object]:
    return {
        "projection_pushdown": _provider_pushdown_value(
            provider,
            names=("supports_projection_pushdown",),
        ),
        "predicate_pushdown": _provider_pushdown_value(
            provider,
            names=(
                "supports_filter_pushdown",
                "supports_filters_pushdown",
                "supports_predicate_pushdown",
            ),
        ),
        "limit_pushdown": _provider_pushdown_value(
            provider,
            names=("supports_limit_pushdown",),
        ),
    }


def _provider_mode_severity(
    format_name: str | None,
    *,
    provider_is_native: bool,
) -> str:
    if format_name == "delta" and not provider_is_native:
        return "warn"
    return "info"


def _record_provider_mode_diagnostics(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    context: _ProviderModeContext,
) -> None:
    if runtime_profile is None:
        return
    format_name = None
    if context.details is not None:
        format_value = context.details.get("format")
        if isinstance(format_value, str):
            format_name = format_value
    provider_class = type(context.provider).__name__ if context.provider is not None else None
    ffi_table_provider = context.capsule_id is not None
    severity = _provider_mode_severity(
        format_name,
        provider_is_native=ffi_table_provider,
    )
    strict_native_provider_enabled = runtime_profile.features.enforce_delta_ffi_provider
    expected_native_provider = format_name == "delta"
    provider_is_native = ffi_table_provider
    strict_native_provider_violation = (
        strict_native_provider_enabled and expected_native_provider and not provider_is_native
    )
    payload = {
        "dataset": context.name,
        "provider_mode": context.provider_kind,
        "provider_class": provider_class,
        "ffi_table_provider": ffi_table_provider,
        "capsule_id": context.capsule_id,
        "strict_native_provider_enabled": strict_native_provider_enabled,
        "expected_native_provider": expected_native_provider,
        "provider_is_native": provider_is_native,
        "strict_native_provider_violation": strict_native_provider_violation,
        "run_id": get_run_id(),
        "diagnostic.severity": severity,
        "diagnostic.category": "datafusion_provider",
    }
    from serde_artifact_specs import DATASET_PROVIDER_MODE_SPEC

    record_artifact(runtime_profile, DATASET_PROVIDER_MODE_SPEC, payload)


def record_table_provider_artifact(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    artifact: TableProviderArtifact,
) -> None:
    """Record table-provider artifact and diagnostics payloads."""
    capsule_id = _provider_capsule_id(artifact.provider, source=artifact.source)
    _record_provider_mode_diagnostics(
        runtime_profile=runtime_profile,
        context=_ProviderModeContext(
            name=artifact.name,
            provider_kind=artifact.provider_kind,
            provider=artifact.provider,
            capsule_id=capsule_id,
            details=artifact.details,
        ),
    )
    payload: dict[str, object] = {
        "name": artifact.name,
        "provider": artifact.provider_kind,
        "provider_type": type(artifact.provider).__name__
        if artifact.provider is not None
        else None,
        "capsule_id": capsule_id,
    }
    if artifact.provider is not None:
        payload.update(_provider_pushdown_hints(artifact.provider))
    if artifact.details:
        payload.update(artifact.details)
    from serde_artifact_specs import DATAFUSION_TABLE_PROVIDERS_SPEC

    record_artifact(runtime_profile, DATAFUSION_TABLE_PROVIDERS_SPEC, payload)


def update_table_provider_capabilities(
    ctx: SessionContext,
    *,
    name: str,
    supports_insert: bool | None = None,
    supports_cdf: bool | None = None,
) -> None:
    """Update capability metadata for a registered table provider."""
    metadata = table_provider_metadata(ctx, table_name=name)
    if metadata is None:
        return
    updated = replace(
        metadata,
        supports_insert=supports_insert,
        supports_cdf=supports_cdf,
    )
    record_table_provider_metadata(ctx, metadata=updated)


def update_table_provider_scan_config(
    ctx: SessionContext,
    *,
    name: str,
    delta_scan_snapshot: object | None,
    delta_scan_identity_hash: str | None,
    delta_scan_effective: Mapping[str, object] | None,
) -> None:
    """Update Delta scan-configuration metadata for a registered table provider."""
    metadata = table_provider_metadata(ctx, table_name=name)
    if metadata is None:
        return
    updated = replace(
        metadata,
        delta_scan_config=delta_scan_snapshot_payload(delta_scan_snapshot),
        delta_scan_identity_hash=delta_scan_identity_hash,
        delta_scan_effective=dict(delta_scan_effective) if delta_scan_effective else None,
    )
    record_table_provider_metadata(ctx, metadata=updated)


def _schema_identity_hash(schema: pa.Schema | None) -> str | None:
    if schema is None:
        return None
    return schema_identity_hash(schema)


def update_table_provider_fingerprints(
    ctx: SessionContext,
    *,
    name: str,
    schema: pa.Schema | None,
) -> tuple[str | None, str | None, dict[str, object]]:
    """Update schema/DDL fingerprints and return resolved fingerprint details.

    Returns:
        tuple[str | None, str | None, dict[str, object]]: Schema hash, DDL hash,
        and details payload for persisted artifact metadata.
    """
    metadata = table_provider_metadata(ctx, table_name=name)
    if metadata is None:
        return None, None, {}
    schema_identity_hash_value = _schema_identity_hash(schema)
    ddl_fingerprint = metadata.ddl_fingerprint
    if ddl_fingerprint is None and metadata.ddl is not None:
        from schema_spec.dataset_spec import ddl_fingerprint_from_definition

        ddl_fingerprint = ddl_fingerprint_from_definition(metadata.ddl)
    updated = replace(
        metadata,
        schema_identity_hash=schema_identity_hash_value,
        ddl_fingerprint=ddl_fingerprint,
    )
    record_table_provider_metadata(ctx, metadata=updated)
    details: dict[str, object] = {}
    if schema_identity_hash_value is not None:
        details["schema_identity_hash"] = schema_identity_hash_value
    if ddl_fingerprint is not None:
        details["ddl_fingerprint"] = ddl_fingerprint
    return schema_identity_hash_value, ddl_fingerprint, details


__all__ = [
    "TableProviderArtifact",
    "provider_capsule_id",
    "record_table_provider_artifact",
    "table_provider_capsule",
    "update_table_provider_capabilities",
    "update_table_provider_fingerprints",
    "update_table_provider_scan_config",
]
