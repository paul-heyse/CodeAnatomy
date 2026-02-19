"""Runtime profile helpers for Delta/DataFusion conformance tests."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.capabilities import is_delta_extension_compatible
from datafusion_engine.delta.service import DeltaService
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.session.runtime_ops import bind_delta_service
from datafusion_engine.session.runtime_profile_config import (
    DataSourceConfig,
    DiagnosticsConfig,
    ExtractOutputConfig,
    FeatureGatesConfig,
    PolicyBundleConfig,
)
from obs.diagnostics import DiagnosticsCollector

ConformanceBackendKind = Literal["fs", "minio", "localstack"]


@dataclass(frozen=True)
class ConformanceBackendConfig:
    """Backend-resolved URI and storage-option contract for conformance lanes."""

    kind: ConformanceBackendKind
    storage_options: Mapping[str, str]
    log_storage_options: Mapping[str, str]
    bucket: str | None = None
    root_prefix: str = "codeanatomy/conformance"

    def table_uri(self, *, table_name: str, root_dir: Path) -> str:
        """Build backend-specific Delta URI for a test table name.

        Returns:
            str: Backend-specific Delta table URI.
        """
        if self.kind == "fs":
            return str(root_dir / table_name)
        bucket = self.bucket or "codeanatomy-conformance"
        prefix_parts = [
            self.root_prefix.strip("/"),
            root_dir.name.strip("/"),
            table_name.strip("/"),
        ]
        object_key = "/".join(part for part in prefix_parts if part)
        return f"s3://{bucket}/{object_key}"

    def artifact_context(self, *, table_name: str, root_dir: Path) -> dict[str, object]:
        """Return deterministic backend context for artifact payloads."""
        return {
            "backend": self.kind,
            "table_uri": self.table_uri(table_name=table_name, root_dir=root_dir),
            "storage_options": dict(self.storage_options),
            "log_storage_options": dict(self.log_storage_options),
        }


def resolve_conformance_backend_config(
    backend: ConformanceBackendKind,
) -> ConformanceBackendConfig:
    """Resolve backend-specific URI/storage contracts for conformance tests.

    Returns:
        ConformanceBackendConfig: Resolved backend configuration contract.
    """
    if backend == "fs":
        return ConformanceBackendConfig(kind="fs", storage_options={}, log_storage_options={})

    if backend == "minio":
        resolved_endpoint = os.environ.get("CODEANATOMY_MINIO_ENDPOINT", "").strip()
        bucket = os.environ.get("CODEANATOMY_MINIO_BUCKET", "codeanatomy").strip()
        options = {
            "AWS_ENDPOINT_URL": resolved_endpoint,
            "AWS_ALLOW_HTTP": "true",
            "AWS_REGION": os.environ.get("CODEANATOMY_MINIO_REGION", "us-east-1"),
            "AWS_ACCESS_KEY_ID": os.environ.get("CODEANATOMY_MINIO_ACCESS_KEY", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get(
                "CODEANATOMY_MINIO_SECRET_KEY",
                "minioadmin",
            ),
            "AWS_S3_FORCE_PATH_STYLE": "true",
        }
        return ConformanceBackendConfig(
            kind="minio",
            storage_options=options,
            log_storage_options=options,
            bucket=bucket or "codeanatomy",
        )

    resolved_endpoint = os.environ.get("CODEANATOMY_LOCALSTACK_ENDPOINT", "").strip()
    bucket = os.environ.get("CODEANATOMY_LOCALSTACK_BUCKET", "codeanatomy").strip()
    options = {
        "AWS_ENDPOINT_URL": resolved_endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": os.environ.get("CODEANATOMY_LOCALSTACK_REGION", "us-east-1"),
        "AWS_ACCESS_KEY_ID": os.environ.get("CODEANATOMY_LOCALSTACK_ACCESS_KEY", "test"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get(
            "CODEANATOMY_LOCALSTACK_SECRET_KEY",
            "test",
        ),
        "AWS_S3_FORCE_PATH_STYLE": "true",
    }
    return ConformanceBackendConfig(
        kind="localstack",
        storage_options=options,
        log_storage_options=options,
        bucket=bucket or "codeanatomy",
    )


def clone_profile_with_delta_service(
    profile: DataFusionRuntimeProfile,
    *,
    diagnostics: DiagnosticsCollector | None = None,
) -> DataFusionRuntimeProfile:
    """Clone a runtime profile and bind a fresh Delta service.

    Returns:
        DataFusionRuntimeProfile: Cloned profile with bound Delta service.
    """
    diagnostics_config = (
        DiagnosticsConfig(diagnostics_sink=diagnostics)
        if diagnostics is not None
        else profile.diagnostics
    )
    cloned = DataFusionRuntimeProfile(
        architecture_version=profile.architecture_version,
        execution=profile.execution,
        catalog=profile.catalog,
        data_sources=profile.data_sources,
        zero_row_bootstrap=profile.zero_row_bootstrap,
        features=profile.features,
        diagnostics=diagnostics_config,
        policies=profile.policies,
        view_registry=profile.view_registry,
    )
    bind_delta_service(cloned, service=DeltaService(profile=cloned))
    return cloned


def conformance_profile(
    *,
    diagnostics: DiagnosticsCollector | None = None,
    dataset_locations: Mapping[str, DatasetLocation] | None = None,
    enforce_native_provider: bool | None = None,
    plan_artifacts_root: str | None = None,
) -> DataFusionRuntimeProfile:
    """Build a deterministic runtime profile for conformance tests.

    Returns:
    -------
    DataFusionRuntimeProfile
        Runtime profile configured for conformance scenarios.
    """
    if enforce_native_provider is None:
        enforce_native_provider = strict_native_provider_supported()
    extract_output = ExtractOutputConfig(dataset_locations=dict(dataset_locations or {}))
    policies = (
        PolicyBundleConfig(plan_artifacts_root=plan_artifacts_root)
        if plan_artifacts_root is not None
        else PolicyBundleConfig()
    )
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(extract_output=extract_output),
        diagnostics=DiagnosticsConfig(diagnostics_sink=diagnostics),
        policies=policies,
        features=FeatureGatesConfig(
            enable_schema_registry=False,
            enable_schema_evolution_adapter=False,
            enable_udfs=False,
            enforce_delta_ffi_provider=enforce_native_provider,
        ),
    )
    bind_delta_service(profile, service=DeltaService(profile=profile))
    return profile


def conformance_profile_with_sink(
    *,
    dataset_locations: Mapping[str, DatasetLocation] | None = None,
    enforce_native_provider: bool | None = None,
    plan_artifacts_root: str | None = None,
) -> tuple[DataFusionRuntimeProfile, DiagnosticsCollector]:
    """Build a conformance profile with an attached diagnostics collector.

    Returns:
    -------
    tuple[DataFusionRuntimeProfile, DiagnosticsCollector]
        The configured runtime profile and its diagnostics sink.
    """
    sink = DiagnosticsCollector()
    profile = conformance_profile(
        diagnostics=sink,
        dataset_locations=dataset_locations,
        enforce_native_provider=enforce_native_provider,
        plan_artifacts_root=plan_artifacts_root,
    )
    return profile, sink


@lru_cache(maxsize=1)
def strict_native_provider_supported() -> bool:
    """Return whether strict non-fallback Delta provider probing is compatible.

    Returns:
    -------
    bool
        `True` when strict native provider probing is both available and compatible.
    """
    profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(
            enable_schema_registry=False,
            enable_schema_evolution_adapter=False,
            enable_udfs=False,
            enforce_delta_ffi_provider=True,
        ),
    )
    compatibility = is_delta_extension_compatible(
        profile.session_context(),
        entrypoint="delta_table_provider_from_session",
        require_non_fallback=True,
    )
    return compatibility.available and compatibility.compatible
