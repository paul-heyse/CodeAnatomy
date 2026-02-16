"""Delta service boundary for DataFusion integration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow.interop import RecordBatchReaderLike
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.dataset.resolution import (
    DatasetResolution,
    DatasetResolutionRequest,
    resolve_dataset_provider,
)
from datafusion_engine.delta.capabilities import (
    DeltaExtensionCompatibility,
    is_delta_extension_compatible,
)
from datafusion_engine.delta.provider_artifacts import (
    ServiceProviderArtifactInput,
    build_delta_provider_build_result,
    provider_build_request_from_service_context,
)
from datafusion_engine.delta.store_policy import (
    apply_delta_store_policy,
    resolve_delta_store_policy,
)
from datafusion_engine.lineage.diagnostics import record_artifact
from obs.otel import get_run_id
from serde_msgspec import StructBaseStrict
from storage.deltalake.delta_maintenance import (
    cleanup_delta_log,
    create_delta_checkpoint,
    vacuum_delta,
)
from storage.deltalake.delta_metadata import delta_table_schema
from storage.deltalake.delta_read import (
    DeltaCdfOptions,
    DeltaDeleteWhereRequest,
    DeltaFeatureMutationOptions,
    DeltaMergeArrowRequest,
    DeltaReadRequest,
    DeltaSchemaRequest,
    DeltaVacuumOptions,
    StorageOptions,
    delta_add_constraints,
    delta_cdf_enabled,
    delta_delete_where,
    delta_history_snapshot,
    delta_merge_arrow,
    delta_protocol_snapshot,
    delta_table_version,
    enable_delta_change_data_feed,
    enable_delta_check_constraints,
    enable_delta_column_mapping,
    enable_delta_deletion_vectors,
    enable_delta_features,
    enable_delta_in_commit_timestamps,
    enable_delta_row_tracking,
    enable_delta_v2_checkpoints,
    read_delta_cdf,
    read_delta_cdf_eager,
    read_delta_table,
    read_delta_table_eager,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from datafusion import SessionContext

    from datafusion_engine.delta.protocol import DeltaFeatureGate, DeltaProtocolSnapshot
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class DeltaMutationRequest:
    """Unified request for Delta mutations."""

    merge: DeltaMergeArrowRequest | None = None
    delete: DeltaDeleteWhereRequest | None = None

    def validate(self) -> None:
        """Validate that exactly one mutation is specified.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.merge is None and self.delete is None:
            msg = "DeltaMutationRequest requires merge or delete payloads."
            raise ValueError(msg)
        if self.merge is not None and self.delete is not None:
            msg = "DeltaMutationRequest must not include both merge and delete payloads."
            raise ValueError(msg)


class DeltaFeatureMutationRequest(StructBaseStrict, frozen=True):
    """Request payload for Delta feature mutation options."""

    path: str
    storage_options: StorageOptions | None = None
    log_storage_options: StorageOptions | None = None
    version: int | None = None
    timestamp: str | None = None
    gate: DeltaFeatureGate | None = None
    commit_metadata: Mapping[str, str] | None = None
    dataset_name: str | None = None


@dataclass(frozen=True)
class _ProviderArtifactRecordRequest:
    """Provider artifact capture request."""

    ctx: SessionContext
    resolution: DatasetResolution
    location: DatasetLocation
    name: str | None
    predicate: str | None
    scan_files: Sequence[str] | None


def _canonical_provider_mode(resolution: DatasetResolution) -> str:
    if resolution.provider_kind == "delta_cdf":
        return "cdf_table_provider"
    return "delta_table_provider"


@dataclass(frozen=True)
class DeltaFeatureOps:
    """Feature mutation helpers bound to a Delta service."""

    service: DeltaService

    def feature_mutation_options(
        self,
        request: DeltaFeatureMutationRequest,
    ) -> DeltaFeatureMutationOptions:
        """Build resolved feature mutation options using runtime defaults.

        Returns:
        -------
        DeltaFeatureMutationOptions
            Resolved mutation options with runtime defaults applied.
        """
        resolved_storage, resolved_log = self.service.resolve_store_options(
            path=request.path,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
        )
        return DeltaFeatureMutationOptions(
            path=request.path,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
            version=request.version,
            timestamp=request.timestamp,
            gate=request.gate,
            commit_metadata=request.commit_metadata,
            runtime_profile=self.service.profile,
            dataset_name=request.dataset_name,
        )

    def enable_features(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        features: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """Enable Delta table feature properties using runtime defaults.

        Returns:
        -------
        dict[str, str]
            Properties applied to the Delta table.
        """
        resolved = self.service.resolve_feature_options(options)
        return enable_delta_features(
            resolved,
            features=features,
        )

    def enable_change_data_feed(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable Delta change data feed using runtime defaults.

        Returns:
        -------
        Mapping[str, object]
            Control-plane mutation report payload.
        """
        return enable_delta_change_data_feed(
            self.service.resolve_feature_options(options),
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )

    def enable_deletion_vectors(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable Delta deletion vectors using runtime defaults.

        Returns:
        -------
        Mapping[str, object]
            Control-plane mutation report payload.
        """
        return enable_delta_deletion_vectors(
            self.service.resolve_feature_options(options),
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )

    def enable_row_tracking(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable Delta row tracking using runtime defaults.

        Returns:
        -------
        Mapping[str, object]
            Control-plane mutation report payload.
        """
        return enable_delta_row_tracking(
            self.service.resolve_feature_options(options),
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )

    def enable_in_commit_timestamps(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        enablement_version: int | None = None,
        enablement_timestamp: str | None = None,
    ) -> Mapping[str, object]:
        """Enable in-commit timestamps using runtime defaults.

        Returns:
        -------
        Mapping[str, object]
            Control-plane mutation report payload.
        """
        return enable_delta_in_commit_timestamps(
            self.service.resolve_feature_options(options),
            enablement_version=enablement_version,
            enablement_timestamp=enablement_timestamp,
        )

    def enable_column_mapping(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        mode: str = "name",
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable Delta column mapping using runtime defaults.

        Returns:
        -------
        Mapping[str, object]
            Control-plane mutation report payload.
        """
        return enable_delta_column_mapping(
            self.service.resolve_feature_options(options),
            mode=mode,
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )

    def enable_v2_checkpoints(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable Delta v2 checkpoints using runtime defaults.

        Returns:
        -------
        Mapping[str, object]
            Control-plane mutation report payload.
        """
        return enable_delta_v2_checkpoints(
            self.service.resolve_feature_options(options),
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )

    def enable_check_constraints(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        allow_protocol_versions_increase: bool = True,
    ) -> Mapping[str, object]:
        """Enable Delta check constraints using runtime defaults.

        Returns:
        -------
        Mapping[str, object]
            Control-plane mutation report payload.
        """
        return enable_delta_check_constraints(
            self.service.resolve_feature_options(options),
            allow_protocol_versions_increase=allow_protocol_versions_increase,
        )

    def add_constraints(
        self,
        options: DeltaFeatureMutationOptions,
        *,
        constraints: Mapping[str, str],
    ) -> Mapping[str, object]:
        """Add Delta check constraints using runtime defaults.

        Returns:
        -------
        Mapping[str, object]
            Control-plane mutation report payload.
        """
        return delta_add_constraints(
            self.service.resolve_feature_options(options),
            constraints=constraints,
        )


@dataclass(frozen=True)
class DeltaService:
    """Unified Delta access service for runtime profiles."""

    profile: DataFusionRuntimeProfile

    @property
    def features(self) -> DeltaFeatureOps:
        """Return feature mutation helpers bound to this service."""
        return DeltaFeatureOps(self)

    def _resolve_store_options(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None,
        log_storage_options: StorageOptions | None,
    ) -> tuple[dict[str, str], dict[str, str]]:
        return resolve_delta_store_policy(
            table_uri=path,
            policy=self.profile.policies.delta_store_policy,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )

    def resolve_store_options(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None,
        log_storage_options: StorageOptions | None,
    ) -> tuple[dict[str, str], dict[str, str]]:
        """Resolve storage options using the profile's Delta store policy.

        Returns:
        -------
        tuple[dict[str, str], dict[str, str]]
            Resolved storage and log storage options.
        """
        return self._resolve_store_options(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )

    def _apply_store_policy(self, location: DatasetLocation) -> DatasetLocation:
        return apply_delta_store_policy(
            location,
            policy=self.profile.policies.delta_store_policy,
        )

    def provider(
        self,
        *,
        location: DatasetLocation,
        name: str | None = None,
        predicate: str | None = None,
        scan_files: Sequence[str] | None = None,
    ) -> DatasetResolution:
        """Resolve a Delta provider using the runtime profile.

        Returns:
        -------
        DatasetResolution
            Resolved dataset provider with scan metadata.
        """
        ctx = self.profile.session_context()
        resolved_location = self._apply_store_policy(location)
        resolution = resolve_dataset_provider(
            DatasetResolutionRequest(
                ctx=ctx,
                location=resolved_location,
                runtime_profile=self.profile,
                name=name,
                predicate=predicate,
                scan_files=scan_files,
            )
        )
        self._record_provider_artifact(
            _ProviderArtifactRecordRequest(
                ctx=ctx,
                resolution=resolution,
                location=resolved_location,
                name=name,
                predicate=predicate,
                scan_files=scan_files,
            )
        )
        return resolution

    def _record_provider_artifact(
        self,
        request: _ProviderArtifactRecordRequest,
    ) -> None:
        if self.profile.diagnostics.diagnostics_sink is None:
            return
        from serde_artifact_specs import DELTA_SERVICE_PROVIDER_SPEC

        compatibility = is_delta_extension_compatible(
            request.ctx,
            entrypoint="delta_provider_from_session",
            require_non_fallback=self.profile.features.enforce_delta_ffi_provider,
        )
        payload = self._provider_artifact_payload(
            request=request,
            compatibility=compatibility,
        )
        record_artifact(self.profile, DELTA_SERVICE_PROVIDER_SPEC, payload)

    def _provider_artifact_payload(
        self,
        *,
        request: _ProviderArtifactRecordRequest,
        compatibility: DeltaExtensionCompatibility,
    ) -> dict[str, object]:
        resolution = request.resolution
        snapshot = resolution.delta_snapshot
        snapshot_mapping = snapshot if isinstance(snapshot, Mapping) else None
        provider_mode = _canonical_provider_mode(resolution)
        strict_enabled = self.profile.features.enforce_delta_ffi_provider
        ffi_table_provider = (
            snapshot_mapping.get("ffi_table_provider") if snapshot_mapping is not None else None
        )
        provider_is_native = ffi_table_provider if isinstance(ffi_table_provider, bool) else True
        strict_violation = strict_enabled and not provider_is_native
        payload_request = provider_build_request_from_service_context(
            ServiceProviderArtifactInput(
                request=request,
                compatibility=compatibility,
                provider_mode=provider_mode,
                strict_native_provider_enabled=strict_enabled,
                strict_native_provider_violation=strict_violation,
                include_event_metadata=True,
                run_id=get_run_id(),
            )
        )
        return build_delta_provider_build_result(payload_request).as_payload()

    def table_version(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        gate: DeltaFeatureGate | None = None,
    ) -> int | None:
        """Return the latest Delta table version when the table exists.

        Returns:
        -------
        int | None
            Latest Delta table version, or ``None`` when unavailable.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        return delta_table_version(
            path,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
            gate=gate,
        )

    def table_schema(self, request: DeltaSchemaRequest) -> pa.Schema | None:
        """Return a Delta table schema using runtime defaults.

        Returns:
        -------
        pyarrow.Schema | None
            Schema for the table or ``None`` when unavailable.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=request.path,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
        )
        resolved_request = replace(
            request,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
        )
        return delta_table_schema(resolved_request)

    def read_table(self, request: DeltaReadRequest) -> RecordBatchReaderLike:
        """Read a Delta table using the runtime profile (streaming).

        Returns:
        -------
        RecordBatchReaderLike
            Streaming reader containing the requested Delta snapshot.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=request.path,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
        )
        resolved = replace(
            request,
            runtime_profile=self.profile,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
        )
        return read_delta_table(resolved)

    def read_table_eager(self, request: DeltaReadRequest) -> pa.Table:
        """Read a Delta table and materialize to an Arrow table.

        Returns:
        -------
        pyarrow.Table
            Materialized table containing the requested Delta snapshot.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=request.path,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
        )
        resolved = replace(
            request,
            runtime_profile=self.profile,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
        )
        return read_delta_table_eager(resolved)

    def read_cdf(
        self,
        *,
        table_path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> RecordBatchReaderLike:
        """Read Delta change data feed table (streaming).

        Returns:
        -------
        RecordBatchReaderLike
            Streaming reader for the requested change data feed range.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=table_path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        return read_delta_cdf(
            table_path,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
            cdf_options=cdf_options,
        )

    def read_cdf_eager(
        self,
        *,
        table_path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> pa.Table:
        """Read Delta change data feed and materialize to a table.

        Returns:
        -------
        pyarrow.Table
            Materialized Arrow table with CDF changes.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=table_path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        return read_delta_cdf_eager(
            table_path,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
            cdf_options=cdf_options,
        )

    def cdf_enabled(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> bool:
        """Return whether CDF is enabled for a Delta table.

        Returns:
        -------
        bool
            True when CDF is enabled for the table.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        return delta_cdf_enabled(
            path,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
        )

    def history_snapshot(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        limit: int = 1,
        gate: DeltaFeatureGate | None = None,
    ) -> Mapping[str, object] | None:
        """Return the latest Delta history snapshot using runtime defaults.

        Returns:
        -------
        Mapping[str, object] | None
            History snapshot payload or ``None`` when unavailable.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        return delta_history_snapshot(
            path,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
            limit=limit,
            gate=gate,
        )

    def protocol_snapshot(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        gate: DeltaFeatureGate | None = None,
    ) -> DeltaProtocolSnapshot | None:
        """Return the Delta protocol snapshot using runtime defaults.

        Returns:
        -------
        DeltaProtocolSnapshot | None
            Protocol snapshot or ``None`` when unavailable.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        return delta_protocol_snapshot(
            path,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
            gate=gate,
        )

    def vacuum(
        self,
        *,
        path: str,
        options: DeltaVacuumOptions | None = None,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> list[str]:
        """Run Delta vacuum using runtime store-policy defaults.

        Returns:
        -------
        list[str]
            Removed or eligible file paths.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        return vacuum_delta(
            path,
            options=options,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
        )

    def create_checkpoint(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        dataset_name: str | None = None,
    ) -> Mapping[str, object]:
        """Create a Delta checkpoint using runtime store-policy defaults.

        Returns:
        -------
        Mapping[str, object]
            Checkpoint report payload.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        return create_delta_checkpoint(
            path,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
            runtime_profile=self.profile,
            dataset_name=dataset_name,
        )

    def cleanup_log(
        self,
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        dataset_name: str | None = None,
    ) -> Mapping[str, object]:
        """Clean Delta log metadata using runtime store-policy defaults.

        Returns:
        -------
        Mapping[str, object]
            Cleanup report payload.
        """
        resolved_storage, resolved_log = self._resolve_store_options(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )
        return cleanup_delta_log(
            path,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
            runtime_profile=self.profile,
            dataset_name=dataset_name,
        )

    def _resolve_feature_options(
        self,
        options: DeltaFeatureMutationOptions,
    ) -> DeltaFeatureMutationOptions:
        resolved_storage, resolved_log = self._resolve_store_options(
            path=options.path,
            storage_options=options.storage_options,
            log_storage_options=options.log_storage_options,
        )
        return replace(
            options,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
            runtime_profile=self.profile,
        )

    def resolve_feature_options(
        self,
        options: DeltaFeatureMutationOptions,
    ) -> DeltaFeatureMutationOptions:
        """Resolve feature mutation options using runtime defaults.

        Returns:
        -------
        DeltaFeatureMutationOptions
            Resolved feature mutation options with runtime defaults applied.
        """
        return self._resolve_feature_options(options)

    def delete_where(
        self,
        request: DeltaDeleteWhereRequest,
        *,
        ctx: SessionContext | None = None,
    ) -> Mapping[str, object]:
        """Delete rows from a Delta table.

        Returns:
        -------
        Mapping[str, object]
            Delete operation report payload.
        """
        resolved_ctx = ctx or self.profile.delta_ops.delta_runtime_ctx()
        resolved_storage, resolved_log = self._resolve_store_options(
            path=request.path,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
        )
        resolved_request = replace(
            request,
            runtime_profile=self.profile,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
        )
        return delta_delete_where(resolved_ctx, request=resolved_request)

    def merge_arrow(
        self,
        request: DeltaMergeArrowRequest,
        *,
        ctx: SessionContext | None = None,
    ) -> Mapping[str, object]:
        """Merge Arrow data into a Delta table.

        Returns:
        -------
        Mapping[str, object]
            Merge operation report payload.
        """
        resolved_ctx = ctx or self.profile.delta_ops.delta_runtime_ctx()
        resolved_storage, resolved_log = self._resolve_store_options(
            path=request.path,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
        )
        resolved_request = replace(
            request,
            runtime_profile=self.profile,
            storage_options=resolved_storage or None,
            log_storage_options=resolved_log or None,
        )
        return delta_merge_arrow(resolved_ctx, request=resolved_request)

    def mutate(
        self,
        request: DeltaMutationRequest,
        *,
        ctx: SessionContext | None = None,
    ) -> Mapping[str, object]:
        """Execute a Delta mutation request.

        Args:
            request: Mutation request payload.
            ctx: Optional DataFusion session context.

        Returns:
            Mapping[str, object]: Result.

        Raises:
            ValueError: If the request contains neither merge nor delete payload.
        """
        request.validate()
        if request.merge is not None:
            return self.merge_arrow(request.merge, ctx=ctx)
        if request.delete is not None:
            return self.delete_where(request.delete, ctx=ctx)
        msg = "DeltaMutationRequest did not include a merge or delete payload."
        raise ValueError(msg)


__all__ = [
    "DeltaFeatureMutationRequest",
    "DeltaMutationRequest",
    "DeltaService",
]
