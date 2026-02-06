"""Resource construction helpers for OpenTelemetry."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from opentelemetry.sdk.resources import Resource

from obs.otel.constants import ResourceAttribute
from utils.env_utils import env_value

_DEFAULT_SERVICE_NAME = "codeanatomy"
_DEFAULT_ENVIRONMENT = "unknown"


@dataclass(frozen=True)
class ResourceOptions:
    """Optional resource attributes for OpenTelemetry resources."""

    service_version: str | None = None
    service_namespace: str | None = None
    environment: str | None = None
    instance_id: str | None = None
    attributes: Mapping[str, str] | None = None


def resolve_service_name(default: str = _DEFAULT_SERVICE_NAME) -> str:
    """Resolve the service name using env overrides.

    Parameters
    ----------
    default
        Default service name.

    Returns:
    -------
    str
        Resolved service name.
    """
    return env_value("OTEL_SERVICE_NAME") or default


def _resolve_environment_value(value: str | None) -> str:
    if isinstance(value, str) and value.strip():
        return value
    env_current = env_value("CODEANATOMY_ENVIRONMENT")
    if env_current:
        return env_current
    env_fallback = env_value("DEPLOYMENT_ENVIRONMENT")
    if env_fallback:
        return env_fallback
    return _DEFAULT_ENVIRONMENT


def build_resource(service_name: str, options: ResourceOptions | None = None) -> Resource:
    """Build a Resource with standard service identity attributes.

    Parameters
    ----------
    service_name
        Service name to record.
    options
        Optional resource settings, including version, namespace, environment,
        instance ID, and additional attributes.

    Returns:
    -------
    Resource
        OpenTelemetry resource with merged attributes.
    """
    resolved = options or ResourceOptions()
    payload: dict[str, str] = {ResourceAttribute.SERVICE_NAME.value: service_name}
    if resolved.service_version:
        payload[ResourceAttribute.SERVICE_VERSION.value] = resolved.service_version
    if resolved.service_namespace:
        payload[ResourceAttribute.SERVICE_NAMESPACE.value] = resolved.service_namespace
    payload[ResourceAttribute.DEPLOYMENT_ENVIRONMENT.value] = _resolve_environment_value(
        resolved.environment
    )
    if resolved.instance_id:
        payload[ResourceAttribute.SERVICE_INSTANCE_ID.value] = resolved.instance_id
    if resolved.attributes:
        payload.update({str(key): str(value) for key, value in resolved.attributes.items()})
    return Resource.create(payload)


__all__ = ["ResourceOptions", "build_resource", "resolve_service_name"]
