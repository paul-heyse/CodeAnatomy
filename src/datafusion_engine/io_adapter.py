"""Unified IO adapter for all DataFusion access paths.

This module consolidates object store registration, DDL-based external table
registration, and dataset metadata capture into a single IO adapter used by
all DataFusion execution paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext, SQLOptions

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from ibis_engine.registry import DatasetLocation


@dataclass(frozen=True)
class DataFusionIOAdapter:
    """Unified IO adapter for all DataFusion access paths.

    Consolidates object store registration, external table DDL generation
    and registration, in-memory Arrow table registration, and PyArrow
    Dataset registration into a single adapter. All registration operations
    flow through this adapter for consistency and diagnostics capture.

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
        self._record_artifact(
            "object_store_registered",
            {
                "scheme": scheme,
                "host": host,
            },
        )

    def external_table_ddl(  # noqa: PLR6301
        self,
        *,
        name: str,
        location: DatasetLocation,
    ) -> str:
        """Generate DDL via SQLGlot AST builder.

        Builds a CREATE EXTERNAL TABLE DDL statement using the SQLGlot
        AST builder for the specified dataset location.

        Parameters
        ----------
        name
            Table name for registration.
        location
            Dataset location metadata including path and storage options.

        Returns
        -------
        str
            Rendered SQL DDL statement.

        Notes
        -----
        Uses the `build_external_table_ddl` function from
        `sqlglot_tools.ddl_builders` to generate the DDL statement.
        Instance method for consistency with adapter pattern and future
        extension via profile-based customization.
        """
        from sqlglot_tools.ddl_builders import build_external_table_ddl

        return build_external_table_ddl(
            name=name,
            location=location,
        )

    def register_external_table(
        self,
        *,
        name: str,
        location: DatasetLocation,
    ) -> None:
        """Register external table with full diagnostics.

        Creates an external table in the DataFusion catalog using
        DDL-based registration. The DDL is generated via SQLGlot AST
        builders and executed with policy-based SQL options.

        Parameters
        ----------
        name
            Table name for registration.
        location
            Dataset location metadata including path and storage options.

        Notes
        -----
        This method records a diagnostics artifact containing the
        generated DDL and location information when a diagnostics
        sink is configured in the runtime profile.
        """
        ddl = self.external_table_ddl(name=name, location=location)
        options = self._statement_options()
        self.ctx.sql_with_options(ddl, options).collect()
        self._record_artifact(
            "external_table_registered",
            {
                "name": name,
                "location": str(location),
                "ddl": ddl,
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
        from datafusion_engine.sql_safety import statement_sql_options_for_profile

        return statement_sql_options_for_profile(self.profile)

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
        if self.profile and self.profile.diagnostics_sink:
            self.profile.diagnostics_sink.record_artifact(name, payload)
