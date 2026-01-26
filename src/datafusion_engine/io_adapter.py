"""Unified IO adapter for all DataFusion access paths.

This module consolidates object store registration, dataset registration,
and dataset metadata capture into a single IO adapter used by all DataFusion
execution paths.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext, SQLOptions
from datafusion.dataframe import DataFrame

from datafusion_engine.diagnostics import record_artifact, recorder_for_profile
from datafusion_engine.introspection import invalidate_introspection_cache
from datafusion_engine.sql_safety import execution_policy_for_profile, validate_sql_safety

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from ibis_engine.registry import DatasetLocation


@dataclass(frozen=True)
class ListingTableRegistration:
    """Listing table registration details."""

    name: str
    location: str
    table_partition_cols: Sequence[tuple[str, object]] | None = None
    file_extension: str = ".parquet"
    schema: pa.Schema | None = None
    file_sort_order: Sequence[tuple[str, str]] | None = None
    overwrite: bool = False


@dataclass(frozen=True)
class DataFusionIOAdapter:
    """Unified IO adapter for all DataFusion access paths.

    Consolidates object store registration, in-memory Arrow table registration,
    and PyArrow Dataset registration into a single adapter. All registration
    operations flow through this adapter for consistency and diagnostics capture.

    Parameters
    ----------
    ctx
        DataFusion session context for registration operations.
    profile
        Runtime profile containing execution policy and diagnostics sink.
        If None, uses default execution policy.

    Examples
    --------
    >>> from datafusion import SessionContext
    >>> ctx = SessionContext()
    >>> adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    >>> adapter.register_arrow_table("my_table", pa.table({"col": [1, 2, 3]}))
    """

    ctx: SessionContext
    profile: DataFusionRuntimeProfile | None

    def register_object_store(
        self,
        *,
        scheme: str,
        store: object,
        host: str | None = None,
    ) -> None:
        """Register object store with diagnostics capture.

        Registers an object store with the DataFusion session context,
        enabling access to external data sources (S3, GCS, Azure, etc.).

        Parameters
        ----------
        scheme
            URI scheme for the object store (e.g., "s3", "gs", "az").
        store
            Object store instance to register.
        host
            Optional host identifier for the object store.

        Notes
        -----
        This method records a diagnostics artifact when a diagnostics
        sink is configured in the runtime profile.
        """
        self.ctx.register_object_store(scheme, store, host)
        self._record_registration(
            name=scheme,
            registration_type="object_store",
            location=host,
        )
        self._record_artifact(
            "object_store_registered",
            {
                "scheme": scheme,
                "host": host,
            },
        )

    def register_arrow_table(
        self,
        name: str,
        table: pa.Table,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register in-memory Arrow table.

        Registers a PyArrow Table as a named table in the DataFusion
        catalog for use in SQL queries.

        Parameters
        ----------
        name
            Table name for registration.
        table
            PyArrow Table to register.
        overwrite
            If True, deregister existing table with the same name before
            registering. Defaults to False.

        Notes
        -----
        If a table with the same name already exists and overwrite is
        False, DataFusion will raise an error during registration.
        """
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        self.ctx.register_table(name, table)
        invalidate_introspection_cache(self.ctx)

    def register_record_batches(
        self,
        name: str,
        batches: list[list[pa.RecordBatch]],
        *,
        overwrite: bool = False,
    ) -> None:
        """Register record batches as a DataFusion table.

        Parameters
        ----------
        name
            Table name for registration.
        batches
            Nested list of record batches (partitioned batches).
        overwrite
            If True, deregister existing table with the same name before
            registering. Defaults to False.
        """
        register = getattr(self.ctx, "register_record_batches", None)
        if not callable(register):
            msg = "SessionContext does not support register_record_batches."
            raise NotImplementedError(msg)
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        register(name, batches)
        invalidate_introspection_cache(self.ctx)
        self._record_registration(
            name=name,
            registration_type="table",
        )

    def register_table_provider(
        self,
        name: str,
        provider: object,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a table provider with diagnostics capture.

        Parameters
        ----------
        name
            Table name for registration.
        provider
            Table provider instance to register.
        overwrite
            If True, deregister existing table with the same name before
            registering. Defaults to False.
        """
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        self.ctx.register_table(name, provider)
        invalidate_introspection_cache(self.ctx)
        self._record_registration(
            name=name,
            registration_type="table",
        )
        self._record_artifact(
            "table_provider_registered",
            {"name": name, "provider_type": type(provider).__name__},
        )

    def register_catalog_provider(
        self,
        name: str,
        provider: object,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a catalog provider with diagnostics capture.

        Parameters
        ----------
        name
            Catalog name for registration.
        provider
            Catalog provider instance to register.
        overwrite
            If True, allow replacing an existing catalog registration.
        """
        if overwrite:
            deregister = getattr(self.ctx, "deregister_catalog", None)
            if callable(deregister):
                with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                    deregister(name)
        from typing import cast

        from datafusion.catalog import Catalog, CatalogProvider

        typed_provider = cast(
            "Catalog | CatalogProvider",
            provider,
        )
        self.ctx.register_catalog_provider(name, typed_provider)
        self._record_registration(
            name=name,
            registration_type="catalog",
        )
        self._record_artifact(
            "catalog_provider_registered",
            {"name": name, "provider_type": type(provider).__name__},
        )

    def register_view(
        self,
        name: str,
        df: DataFrame,
        *,
        overwrite: bool = False,
        temporary: bool = False,
    ) -> None:
        """Register a DataFusion view from a DataFrame.

        Parameters
        ----------
        name
            View name for registration.
        df
            DataFrame to convert into a view.
        overwrite
            If True, deregister existing table with the same name before
            registering. Defaults to False.
        temporary
            If True, register as a temporary view.
        """
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        view = df.into_view(temporary=temporary)
        self.ctx.register_table(name, view)
        invalidate_introspection_cache(self.ctx)
        self._record_registration(
            name=name,
            registration_type="view",
        )
        self._record_artifact(
            "view_registered",
            {"name": name, "temporary": temporary},
        )

    def register_dataset(
        self,
        name: str,
        dataset: ds.Dataset,
    ) -> None:
        """Register PyArrow Dataset as table provider.

        Registers a PyArrow Dataset as a named table in the DataFusion
        catalog. Datasets support partitioned data and predicate pushdown.

        Parameters
        ----------
        name
            Table name for registration.
        dataset
            PyArrow Dataset to register.

        Notes
        -----
        PyArrow Datasets provide efficient access to partitioned data
        with predicate and projection pushdown capabilities.
        """
        self.ctx.register_dataset(name, dataset)
        invalidate_introspection_cache(self.ctx)

    def register_parquet(
        self,
        name: str,
        path: str | Sequence[str],
        *,
        overwrite: bool = False,
        options: Mapping[str, object] | None = None,
    ) -> None:
        """Register a Parquet dataset via DataFusion registration."""
        register = getattr(self.ctx, "register_parquet", None)
        if not callable(register):
            msg = "SessionContext does not support register_parquet."
            raise NotImplementedError(msg)
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        register(name, path, **dict(options or {}))
        invalidate_introspection_cache(self.ctx)
        self._record_registration(name=name, registration_type="table", location=str(path))

    def register_csv(
        self,
        name: str,
        path: str | Sequence[str],
        *,
        overwrite: bool = False,
        options: Mapping[str, object] | None = None,
    ) -> None:
        """Register a CSV dataset via DataFusion registration."""
        register = getattr(self.ctx, "register_csv", None)
        if not callable(register):
            msg = "SessionContext does not support register_csv."
            raise NotImplementedError(msg)
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        register(name, path, **dict(options or {}))
        invalidate_introspection_cache(self.ctx)
        self._record_registration(name=name, registration_type="table", location=str(path))

    def register_json(
        self,
        name: str,
        path: str | Sequence[str],
        *,
        overwrite: bool = False,
        options: Mapping[str, object] | None = None,
    ) -> None:
        """Register a JSON dataset via DataFusion registration."""
        register = getattr(self.ctx, "register_json", None)
        if not callable(register):
            msg = "SessionContext does not support register_json."
            raise NotImplementedError(msg)
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        register(name, path, **dict(options or {}))
        invalidate_introspection_cache(self.ctx)
        self._record_registration(name=name, registration_type="table", location=str(path))

    def register_avro(
        self,
        name: str,
        path: str | Sequence[str],
        *,
        overwrite: bool = False,
        options: Mapping[str, object] | None = None,
    ) -> None:
        """Register an Avro dataset via DataFusion registration."""
        register = getattr(self.ctx, "register_avro", None)
        if not callable(register):
            msg = "SessionContext does not support register_avro."
            raise NotImplementedError(msg)
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        register(name, path, **dict(options or {}))
        invalidate_introspection_cache(self.ctx)
        self._record_registration(name=name, registration_type="table", location=str(path))

    def register_listing_table(self, spec: ListingTableRegistration) -> None:
        """Register a listing table when supported by the backend.

        Parameters
        ----------
        spec
            Registration details including name, location, partition columns,
            file extension, optional schema override, sort order, and overwrite.
        """
        register = getattr(self.ctx, "register_listing_table", None)
        if not callable(register):
            msg = "SessionContext does not support register_listing_table."
            raise NotImplementedError(msg)
        if spec.overwrite and self.ctx.table_exist(spec.name):
            self._deregister_table(spec.name)
        register(
            name=spec.name,
            path=spec.location,
            table_partition_cols=(
                list(spec.table_partition_cols) if spec.table_partition_cols else None
            ),
            file_extension=spec.file_extension,
            schema=spec.schema,
            file_sort_order=spec.file_sort_order,
        )
        invalidate_introspection_cache(self.ctx)
        self._record_registration(
            name=spec.name,
            registration_type="table",
            location=spec.location,
        )
        self._record_artifact(
            "listing_table_registered",
            {"name": spec.name, "location": spec.location},
        )

    def execute_statement(self, sql: str, *, allow_statements: bool = True) -> DataFrame:
        """Execute a SQL statement with safety validation.

        Parameters
        ----------
        sql
            SQL statement to execute.
        allow_statements
            Whether statement execution should be allowed by policy.

        Returns
        -------
        datafusion.dataframe.DataFrame
            DataFrame returned by the DataFusion execution.
        """
        self._validate_sql(sql, allow_statements=allow_statements)
        df = self.ctx.sql_with_options(sql, self._statement_options())
        _ = df.collect()
        invalidate_introspection_cache(self.ctx)
        return df

    def deregister_table(self, name: str) -> None:
        """Deregister a table from the DataFusion catalog.

        Parameters
        ----------
        name
            Table name to deregister.
        """
        self._deregister_table(name)

    def _deregister_table(self, name: str) -> None:
        """Centralized table deregistration.

        Removes a table from the DataFusion catalog schema.

        Parameters
        ----------
        name
            Table name to deregister.

        Notes
        -----
        This is an internal helper method used by overwrite operations.
        """
        catalog = self.ctx.catalog()
        schema = catalog.schema()
        schema.deregister_table(name)
        invalidate_introspection_cache(self.ctx)

    def _statement_options(self) -> SQLOptions:
        """Build SQL options from runtime profile.

        Constructs DataFusion SQLOptions based on the execution policy
        specified in the runtime profile.

        Returns
        -------
        SQLOptions
            DataFusion SQLOptions configured with policy settings.

        Notes
        -----
        Uses the `statement_sql_options_for_profile` function from
        `datafusion_engine.sql_safety` to generate SQL options.
        """
        from datafusion_engine.sql_options import statement_sql_options_for_profile

        return statement_sql_options_for_profile(self.profile)

    def _validate_sql(self, sql: str, *, allow_statements: bool = False) -> None:
        """Validate SQL against the runtime execution policy.

        Parameters
        ----------
        sql
            SQL text to validate.
        allow_statements
            Whether to allow statement execution in the policy.

        Raises
        ------
        ValueError
            Raised when SQL violates the execution policy.
        """
        policy = execution_policy_for_profile(self.profile, allow_statements=allow_statements)
        violations = validate_sql_safety(sql, policy, dialect="datafusion")
        if violations:
            msg = f"SQL policy violations: {', '.join(violations)}."
            raise ValueError(msg)

    def _record_registration(
        self,
        *,
        name: str,
        registration_type: str,
        location: str | None = None,
    ) -> None:
        """Record a standardized registration diagnostic."""
        if self.profile is None:
            return
        recorder = recorder_for_profile(self.profile, operation_id=f"register_{name}")
        if recorder is None:
            return
        recorder.record_registration(
            name=name,
            registration_type=registration_type,
            location=location,
        )

    def _record_artifact(self, name: str, payload: dict[str, object]) -> None:
        """Record diagnostics artifact.

        Records a diagnostics artifact if a diagnostics sink is
        configured in the runtime profile.

        Parameters
        ----------
        name
            Artifact name for diagnostics recording.
        payload
            Artifact payload containing metadata to record.

        Notes
        -----
        This method is a no-op if no diagnostics sink is configured.
        """
        record_artifact(self.profile, name, payload)


def _location_payload(location: DatasetLocation | str) -> str:
    if isinstance(location, str):
        return location
    return str(location.path)
