"""Environment-aware fixtures for conformance test matrices."""

from __future__ import annotations

import os
from typing import cast

import pytest

from tests.harness.profiles import (
    ConformanceBackendConfig,
    ConformanceBackendKind,
    resolve_conformance_backend_config,
)

_SUPPORTED_BACKENDS = {"fs", "minio", "localstack"}


@pytest.fixture(scope="session")
def conformance_backend() -> str:
    """Return the active conformance backend and skip when unavailable.

    Raises:
        ValueError: If the configured backend is not one of the supported values.
    """
    backend = os.environ.get("CODEANATOMY_CONFORMANCE_BACKEND", "fs").strip().lower()
    if backend not in _SUPPORTED_BACKENDS:
        msg = (
            "Unsupported CODEANATOMY_CONFORMANCE_BACKEND value: "
            f"{backend!r}. Expected one of {sorted(_SUPPORTED_BACKENDS)!r}."
        )
        raise ValueError(msg)
    if backend == "minio" and not os.environ.get("CODEANATOMY_MINIO_ENDPOINT"):
        pytest.skip("MinIO conformance backend selected but CODEANATOMY_MINIO_ENDPOINT is unset")
    if backend == "localstack" and not os.environ.get("CODEANATOMY_LOCALSTACK_ENDPOINT"):
        pytest.skip(
            "LocalStack conformance backend selected but CODEANATOMY_LOCALSTACK_ENDPOINT is unset"
        )
    return backend


@pytest.fixture(scope="session")
def conformance_backend_config(conformance_backend: str) -> ConformanceBackendConfig:
    """Return canonical backend-resolved URI/storage configuration."""
    return resolve_conformance_backend_config(cast("ConformanceBackendKind", conformance_backend))


@pytest.fixture(scope="session")
def conformance_storage_options(
    conformance_backend_config: ConformanceBackendConfig,
) -> dict[str, str]:
    """Return storage options for the selected conformance backend."""
    return dict(conformance_backend_config.storage_options)
