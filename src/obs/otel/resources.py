"""Resource construction helpers for OpenTelemetry."""

from __future__ import annotations

import os
from collections.abc import Mapping

import opentelemetry.semconv._incubating.attributes.deployment_attributes as inc_deployment_attributes
import opentelemetry.semconv._incubating.attributes.service_attributes as inc_service_attributes
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.attributes import service_attributes

_DEFAULT_SERVICE_NAME = "codeanatomy"


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


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
    return _env_value("OTEL_SERVICE_NAME") or default


def build_resource(
    *,
    service_name: str,
    service_version: str | None = None,
    service_namespace: str | None = None,
    environment: str | None = None,
    attributes: Mapping[str, str] | None = None,
) -> Resource:
    """Build a Resource with standard service identity attributes.

    Parameters
    ----------
    service_name
        Service name to record.
    service_version
        Optional service version.
    service_namespace
        Optional service namespace.
    environment
        Optional deployment environment name.
    attributes
        Extra resource attributes to merge.

    Returns
    -------
    Resource
        OpenTelemetry resource with merged attributes.
    """
    payload: dict[str, str] = {service_attributes.SERVICE_NAME: service_name}
    if service_version:
        payload[service_attributes.SERVICE_VERSION] = service_version
    if service_namespace:
        payload[inc_service_attributes.SERVICE_NAMESPACE] = service_namespace
    if environment:
        payload[inc_deployment_attributes.DEPLOYMENT_ENVIRONMENT] = environment
    if attributes:
        payload.update({str(key): str(value) for key, value in attributes.items()})
    return Resource.create(payload)


__all__ = ["build_resource", "resolve_service_name"]
