"""Instrumentation scope metadata resolution for OpenTelemetry."""

from __future__ import annotations

import os
from importlib.metadata import PackageNotFoundError, version

_SCHEMA_URL_ENV = "CODEANATOMY_OTEL_SCHEMA_URL"
_ALT_SCHEMA_URL_ENV = "OTEL_SCHEMA_URL"


def _resolve_schema_url() -> str | None:
    raw = os.environ.get(_SCHEMA_URL_ENV) or os.environ.get(_ALT_SCHEMA_URL_ENV)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _resolve_instrumentation_version() -> str | None:
    env_version = os.environ.get("CODEANATOMY_SERVICE_VERSION")
    if env_version:
        value = env_version.strip()
        if value:
            return value
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

    Returns
    -------
    str | None
        Instrumentation version, if detected.
    """
    return _INSTRUMENTATION_VERSION


def instrumentation_schema_url() -> str | None:
    """Return the resolved schema URL, if configured.

    Returns
    -------
    str | None
        Schema URL if configured.
    """
    return _SCHEMA_URL


__all__ = ["instrumentation_schema_url", "instrumentation_version"]
