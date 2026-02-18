"""Environment-aware fixtures for conformance test matrices."""

from __future__ import annotations

import os

import pytest

_SUPPORTED_BACKENDS = {"fs", "minio", "localstack"}


@pytest.fixture(scope="session")
def conformance_backend() -> str:
    """Return the active conformance backend and skip when unavailable."""
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
def conformance_storage_options(conformance_backend: str) -> dict[str, str]:
    """Return storage options for the selected conformance backend."""
    if conformance_backend == "fs":
        return {}
    if conformance_backend == "minio":
        endpoint = os.environ.get("CODEANATOMY_MINIO_ENDPOINT", "")
        return {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ALLOW_HTTP": "true",
            "AWS_REGION": os.environ.get("CODEANATOMY_MINIO_REGION", "us-east-1"),
        }
    endpoint = os.environ.get("CODEANATOMY_LOCALSTACK_ENDPOINT", "")
    return {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": os.environ.get("CODEANATOMY_LOCALSTACK_REGION", "us-east-1"),
    }
