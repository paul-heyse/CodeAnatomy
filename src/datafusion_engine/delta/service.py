"""Delta service boundary for DataFusion integration."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.dataset.resolution import (
    DatasetResolution,
    DatasetResolutionRequest,
    resolve_dataset_provider,
)
from storage.deltalake.delta import (
    DeltaCdfOptions,
    DeltaDeleteWhereRequest,
    DeltaMergeArrowRequest,
    DeltaReadRequest,
    DeltaSchemaRequest,
    StorageOptions,
    delta_delete_where,
    delta_merge_arrow,
    delta_table_schema,
    delta_table_version,
    read_delta_cdf,
    read_delta_table,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class DeltaMutationRequest:
    """Unified request for Delta mutations."""

    merge: DeltaMergeArrowRequest | None = None
    delete: DeltaDeleteWhereRequest | None = None

    def validate(self) -> None:
        """Validate that exactly one mutation is specified.

        Raises
        ------
        ValueError
            Raised when the request has zero or multiple mutations.
        """
        if self.merge is None and self.delete is None:
            msg = "DeltaMutationRequest requires merge or delete payloads."
            raise ValueError(msg)
        if self.merge is not None and self.delete is not None:
            msg = "DeltaMutationRequest must not include both merge and delete payloads."
            raise ValueError(msg)


@dataclass(frozen=True)
class DeltaService:
    """Unified Delta access service for runtime profiles."""

    profile: DataFusionRuntimeProfile

    def provider(
        self,
        *,
        location: DatasetLocation,
        name: str | None = None,
        predicate: str | None = None,
        scan_files: Sequence[str] | None = None,
    ) -> DatasetResolution:
        """Resolve a Delta provider using the runtime profile.

        Returns
        -------
        DatasetResolution
            Resolved dataset provider with scan metadata.
        """
        ctx = self.profile.session_context()
        return resolve_dataset_provider(
            DatasetResolutionRequest(
                ctx=ctx,
                location=location,
                runtime_profile=self.profile,
                name=name,
                predicate=predicate,
                scan_files=scan_files,
            )
        )

    @staticmethod
    def table_version(
        *,
        path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> int | None:
        """Return the latest Delta table version when the table exists.

        Returns
        -------
        int | None
            Latest Delta table version, or ``None`` when unavailable.
        """
        return delta_table_version(
            path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )

    @staticmethod
    def table_schema(request: DeltaSchemaRequest) -> pa.Schema | None:
        """Return a Delta table schema using runtime defaults.

        Returns
        -------
        pyarrow.Schema | None
            Schema for the table or ``None`` when unavailable.
        """
        return delta_table_schema(request)

    def read_table(self, request: DeltaReadRequest) -> pa.Table:
        """Read a Delta table using the runtime profile.

        Returns
        -------
        pyarrow.Table
            Table containing the requested Delta snapshot.
        """
        resolved = replace(request, runtime_profile=self.profile)
        return read_delta_table(resolved)

    @staticmethod
    def read_cdf(
        *,
        table_path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> pa.Table:
        """Read Delta change data feed table.

        Returns
        -------
        pyarrow.Table
            Change data feed table for the requested range.
        """
        return read_delta_cdf(
            table_path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            cdf_options=cdf_options,
        )

    def delete_where(
        self,
        request: DeltaDeleteWhereRequest,
        *,
        ctx: SessionContext | None = None,
    ) -> Mapping[str, object]:
        """Delete rows from a Delta table.

        Returns
        -------
        Mapping[str, object]
            Delete operation report payload.
        """
        resolved_ctx = ctx or self.profile.delta_runtime_ctx()
        resolved_request = replace(request, runtime_profile=self.profile)
        return delta_delete_where(resolved_ctx, request=resolved_request)

    def merge_arrow(
        self,
        request: DeltaMergeArrowRequest,
        *,
        ctx: SessionContext | None = None,
    ) -> Mapping[str, object]:
        """Merge Arrow data into a Delta table.

        Returns
        -------
        Mapping[str, object]
            Merge operation report payload.
        """
        resolved_ctx = ctx or self.profile.delta_runtime_ctx()
        resolved_request = replace(request, runtime_profile=self.profile)
        return delta_merge_arrow(resolved_ctx, request=resolved_request)

    def mutate(
        self,
        request: DeltaMutationRequest,
        *,
        ctx: SessionContext | None = None,
    ) -> Mapping[str, object]:
        """Execute a Delta mutation request.

        Returns
        -------
        Mapping[str, object]
            Mutation operation report payload.

        Raises
        ------
        ValueError
            Raised when the mutation request is invalid.
        """
        request.validate()
        if request.merge is not None:
            return self.merge_arrow(request.merge, ctx=ctx)
        if request.delete is not None:
            return self.delete_where(request.delete, ctx=ctx)
        msg = "DeltaMutationRequest did not include a merge or delete payload."
        raise ValueError(msg)


__all__ = ["DeltaMutationRequest", "DeltaService"]
