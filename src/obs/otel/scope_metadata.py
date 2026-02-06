"""Instrumentation scope metadata resolution for OpenTelemetry."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from utils.env_utils import env_text

_SCHEMA_URL_ENV = "CODEANATOMY_OTEL_SCHEMA_URL"
_ALT_SCHEMA_URL_ENV = "OTEL_SCHEMA_URL"


def _resolve_schema_url() -> str | None:
    value = env_text(_SCHEMA_URL_ENV)
    if value is None:
        value = env_text(_ALT_SCHEMA_URL_ENV)
    return value


def _resolve_instrumentation_version() -> str | None:
    env_version = env_text("CODEANATOMY_SERVICE_VERSION")
    if env_version:
        return env_version
    for package in ("codeanatomy", "codeanatomy-engine"):
        try:
            return version(package)
        except PackageNotFoundError:
            continue
    return None


_INSTRUMENTATION_VERSION = _resolve_instrumentation_version()
_SCHEMA_URL = _resolve_schema_url()


def instrumentation_version() -> str | None:
    """Return the resolved instrumentation version, if available.

    Returns:
    -------
    str | None
        Instrumentation version, if detected.
    """
    return _INSTRUMENTATION_VERSION


def instrumentation_schema_url() -> str | None:
    """Return the resolved schema URL, if configured.

    Returns:
    -------
    str | None
        Schema URL if configured.
    """
    return _SCHEMA_URL


__all__ = ["instrumentation_schema_url", "instrumentation_version"]
