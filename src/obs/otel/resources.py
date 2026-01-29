"""Resource construction helpers for OpenTelemetry."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import opentelemetry.semconv._incubating.attributes.service_attributes as inc_service_attributes
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.attributes import service_attributes

from utils.env_utils import env_value

_DEFAULT_SERVICE_NAME = "codeanatomy"
_DEPLOYMENT_ENVIRONMENT_NAME = "deployment.environment.name"


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

    Returns
    -------
    str
        Resolved service name.
    """
    return env_value("OTEL_SERVICE_NAME") or default


def build_resource(service_name: str, options: ResourceOptions | None = None) -> Resource:
    """Build a Resource with standard service identity attributes.

    Parameters
    ----------
    service_name
        Service name to record.
    options
        Optional resource settings, including version, namespace, environment,
        instance ID, and additional attributes.

    Returns
    -------
    Resource
        OpenTelemetry resource with merged attributes.
    """
    resolved = options or ResourceOptions()
    payload: dict[str, str] = {service_attributes.SERVICE_NAME: service_name}
    if resolved.service_version:
        payload[service_attributes.SERVICE_VERSION] = resolved.service_version
    if resolved.service_namespace:
        payload[inc_service_attributes.SERVICE_NAMESPACE] = resolved.service_namespace
    if resolved.environment:
        payload[_DEPLOYMENT_ENVIRONMENT_NAME] = resolved.environment
    if resolved.instance_id:
        payload[inc_service_attributes.SERVICE_INSTANCE_ID] = resolved.instance_id
    if resolved.attributes:
        payload.update({str(key): str(value) for key, value in resolved.attributes.items()})
    return Resource.create(payload)


__all__ = ["ResourceOptions", "build_resource", "resolve_service_name"]
