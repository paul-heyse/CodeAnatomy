"""Delta object-store registration helpers for DataFusion."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from datafusion_engine.io.adapter import DataFusionIOAdapter
from utils.value_coercion import coerce_bool

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class DeltaObjectStoreSpec:
    """Resolved object-store specification for DataFusion registration."""

    scheme: str
    host: str | None
    store: object
    store_kind: str


def _normalize_options(options: Mapping[str, str] | None) -> dict[str, str]:
    if not options:
        return {}
    normalized: dict[str, str] = {}
    for key, value in options.items():
        lowered = str(key).lower()
        text = str(value)
        normalized[lowered] = text
        normalized[lowered.replace("-", "_")] = text
    return normalized


def _option(options: Mapping[str, str], *keys: str) -> str | None:
    for key in keys:
        value = options.get(key)
        if value is not None:
            return value
    return None


def _bool_option(options: Mapping[str, str], *keys: str) -> bool | None:
    value = _option(options, *keys)
    if value is None:
        return None
    return coerce_bool(value)


def _build_s3_store(
    host: str,
    options: Mapping[str, str],
) -> DeltaObjectStoreSpec:
    from datafusion import object_store

    kwargs: dict[str, object] = {
        "region": _option(options, "region", "aws_region"),
        "access_key_id": _option(options, "access_key_id", "aws_access_key_id"),
        "secret_access_key": _option(options, "secret_access_key", "aws_secret_access_key"),
        "session_token": _option(options, "session_token", "aws_session_token"),
        "endpoint": _option(options, "endpoint", "aws_endpoint", "aws_s3_endpoint"),
        "allow_http": _bool_option(options, "allow_http", "aws_allow_http"),
        "imdsv1_fallback": _bool_option(
            options,
            "imdsv1_fallback",
            "aws_imdsv1_fallback",
        ),
    }
    kwargs = {key: value for key, value in kwargs.items() if value is not None}
    return DeltaObjectStoreSpec(
        scheme="s3",
        host=host,
        store=object_store.AmazonS3(host, **kwargs),
        store_kind="s3",
    )


def _build_gcs_store(
    host: str,
    options: Mapping[str, str],
) -> DeltaObjectStoreSpec:
    from datafusion import object_store

    kwargs = {
        "service_account_path": _option(
            options,
            "service_account_path",
            "google_service_account_path",
        )
    }
    kwargs = {key: value for key, value in kwargs.items() if value is not None}
    return DeltaObjectStoreSpec(
        scheme="gs",
        host=host,
        store=object_store.GoogleCloud(host, **kwargs),
        store_kind="gcs",
    )


def _build_azure_store(
    host: str,
    options: Mapping[str, str],
) -> DeltaObjectStoreSpec:
    from datafusion import object_store

    kwargs: dict[str, object] = {
        "account": _option(options, "account", "account_name", "azure_storage_account_name"),
        "access_key": _option(options, "access_key", "azure_storage_access_key"),
        "bearer_token": _option(options, "bearer_token", "azure_storage_bearer_token"),
        "client_id": _option(options, "client_id", "azure_storage_client_id"),
        "client_secret": _option(options, "client_secret", "azure_storage_client_secret"),
        "tenant_id": _option(options, "tenant_id", "azure_storage_tenant_id"),
        "sas_query_pairs": _option(options, "sas_query_pairs", "azure_storage_sas_token"),
        "use_emulator": _bool_option(options, "use_emulator", "azure_storage_use_emulator"),
        "allow_http": _bool_option(options, "allow_http", "azure_allow_http"),
    }
    kwargs = {key: value for key, value in kwargs.items() if value is not None}
    return DeltaObjectStoreSpec(
        scheme="az",
        host=host,
        store=object_store.MicrosoftAzure(host, **kwargs),
        store_kind="azure",
    )


def _build_http_store(base_url: str) -> DeltaObjectStoreSpec:
    from datafusion import object_store

    return DeltaObjectStoreSpec(
        scheme="http",
        host=None,
        store=object_store.Http(base_url),
        store_kind="http",
    )


def _resolve_builder(
    scheme: str,
) -> tuple[str, Callable[[str, Mapping[str, str]], DeltaObjectStoreSpec]] | None:
    if scheme == "s3":
        return "s3", _build_s3_store
    if scheme in {"gs", "gcs"}:
        return "gs", _build_gcs_store
    if scheme in {"az", "azure", "abfs", "abfss"}:
        return "az", _build_azure_store
    return None


def _resolve_store_spec(
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
) -> DeltaObjectStoreSpec | None:
    parsed = urlparse(table_uri)
    if not parsed.scheme:
        return None
    scheme = parsed.scheme.lower()
    if scheme in {"s3a", "s3n"}:
        msg = "Delta table URIs must use canonical s3:// scheme (not s3a:// or s3n://)."
        raise ValueError(msg)
    if scheme in {"file"}:
        return None
    host = parsed.netloc or None
    options = _normalize_options(storage_options)
    if scheme in {"http", "https"}:
        base_url = f"{scheme}://{parsed.netloc}" if parsed.netloc else table_uri
        return _build_http_store(base_url)
    resolved = _resolve_builder(scheme)
    if resolved is None or host is None:
        return None
    _, builder = resolved
    return builder(host, options)


def register_delta_object_store(
    ctx: SessionContext,
    *,
    table_uri: str,
    storage_options: Mapping[str, str] | None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> DeltaObjectStoreSpec | None:
    """Register an object store for a Delta table URI when supported.

    Returns
    -------
    DeltaObjectStoreSpec | None
        Registered object store spec or ``None`` when registration is skipped.
    """
    spec = _resolve_store_spec(table_uri=table_uri, storage_options=storage_options)
    if spec is None:
        return None
    adapter = DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)
    adapter.register_object_store(
        scheme=f"{spec.scheme}://",
        store=spec.store,
        host=spec.host,
    )
    return spec


__all__ = ["DeltaObjectStoreSpec", "register_delta_object_store"]
