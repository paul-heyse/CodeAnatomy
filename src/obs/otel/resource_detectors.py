"""Resource detector helpers for OpenTelemetry bootstrap."""

from __future__ import annotations

import functools
import logging
from collections.abc import Iterable, Mapping
from importlib.metadata import EntryPoint, entry_points
from typing import cast

from opentelemetry.sdk.resources import Resource, ResourceDetector, get_aggregated_resources

from utils.env_utils import env_text
from utils.uuid_factory import secure_token_hex

_LOGGER = logging.getLogger(__name__)
_DEFAULT_DETECTORS = ("process", "os", "host", "container", "k8s")


def _resource_detectors_explicit() -> bool:
    return bool(env_text("OTEL_EXPERIMENTAL_RESOURCE_DETECTORS"))


@functools.lru_cache(maxsize=1)
def resolve_service_instance_id() -> str:
    """Return a stable service.instance.id for the current process.

    Returns
    -------
    str
        Stable instance identifier for this process.
    """
    env_value = env_text("OTEL_SERVICE_INSTANCE_ID") or env_text("CODEANATOMY_SERVICE_INSTANCE_ID")
    if env_value:
        return env_value
    return secure_token_hex()


def resolve_detector_names() -> tuple[str, ...]:
    """Resolve the detector names to run based on env configuration.

    Returns
    -------
    tuple[str, ...]
        Detector names to enable.
    """
    raw = env_text("OTEL_EXPERIMENTAL_RESOURCE_DETECTORS")
    explicit = _resource_detectors_explicit()
    if raw:
        requested = [part.strip() for part in raw.split(",") if part.strip()]
    else:
        requested = list(_DEFAULT_DETECTORS)
    available = _available_detectors()
    resolved = [name for name in requested if name in available]
    missing = sorted(set(requested) - available)
    if missing:
        message = "Resource detectors unavailable: %s"
        if explicit:
            _LOGGER.warning(message, ", ".join(missing))
        else:
            _LOGGER.info(message, ", ".join(missing))
    return tuple(resolved)


def build_detected_resource(resource: Resource) -> Resource:
    """Apply configured resource detectors to a base resource.

    Parameters
    ----------
    resource
        Base resource to enrich with detectors.

    Returns
    -------
    Resource
        Resource enriched with detector data.
    """
    detectors = _resolve_detectors()
    if not detectors:
        return resource
    return get_aggregated_resources(detectors, initial_resource=resource)


def merge_resource_overrides(resource: Resource, overrides: Mapping[str, str] | None) -> Resource:
    """Merge resource overrides after detectors to ensure override precedence.

    Parameters
    ----------
    resource
        Base resource.
    overrides
        Override attributes to apply last.

    Returns
    -------
    Resource
        Resource with overrides merged.
    """
    if not overrides:
        return resource
    normalized = {str(key): str(value) for key, value in overrides.items()}
    return resource.merge(Resource.create(normalized))


def _entry_points(group: str) -> list[EntryPoint]:
    eps = entry_points()
    if hasattr(eps, "select"):
        return list(eps.select(group=group))
    getter = getattr(eps, "get", None)
    if callable(getter):
        return list(cast("Iterable[EntryPoint]", getter(group, ())))
    return []


def _available_detectors() -> set[str]:
    return {entry.name for entry in _entry_points("opentelemetry_resource_detector")}


def _resolve_detectors() -> list[ResourceDetector]:
    detectors: list[ResourceDetector] = []
    explicit = _resource_detectors_explicit()
    configured = set(resolve_detector_names())
    for entry in _entry_points("opentelemetry_resource_detector"):
        if entry.name not in configured:
            continue
        try:
            detector = entry.load()
        except (AttributeError, ImportError, RuntimeError) as exc:
            message = "Failed to load resource detector %s: %s"
            if explicit:
                _LOGGER.warning(message, entry.name, exc)
            else:
                _LOGGER.info(message, entry.name, exc)
            continue
        if isinstance(detector, type):
            try:
                detector = detector()
            except (RuntimeError, TypeError, ValueError) as exc:
                _LOGGER.warning(
                    "Failed to instantiate resource detector %s: %s",
                    entry.name,
                    exc,
                )
                continue
        if isinstance(detector, ResourceDetector):
            detectors.append(detector)
        else:
            _LOGGER.warning("Resource detector %s has incompatible type.", entry.name)
    return detectors


__all__ = [
    "build_detected_resource",
    "merge_resource_overrides",
    "resolve_detector_names",
    "resolve_service_instance_id",
]
