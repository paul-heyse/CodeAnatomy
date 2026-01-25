"""Unified IO adapter for all DataFusion access paths.

This module consolidates object store registration, DDL-based external table
registration, and dataset metadata capture into a single IO adapter used by
all DataFusion execution paths.
"""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext, SQLOptions
from datafusion.dataframe import DataFrame

from datafusion_engine.diagnostics import recorder_for_profile
from datafusion_engine.introspection import invalidate_introspection_cache
from datafusion_engine.sql_safety import (
    execution_policy_for_profile,
    validate_sql_safety,
)
from sqlglot_tools.compat import exp
from sqlglot_tools.ddl_builders import ExternalTableDDLConfig, build_external_table_ddl

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from ibis_engine.registry import DatasetLocation
    from schema_spec.specs import TableSchemaSpec
    from schema_spec.system import DataFusionScanOptions


@dataclass(frozen=True)
class ListingTableRegistration:
    """Listing table registration details."""

    name: str
    location: str
    table_partition_cols: Sequence[tuple[str, object]] | None = None
    file_extension: str = ".parquet"
    schema: pa.Schema | None = None
    file_sort_order: Sequence[Sequence[object]] | None = None
    overwrite: bool = False


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

    def external_table_ddl(
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
        config = _external_table_ddl_config(
            location=location,
            profile=self.profile,
        )
        return build_external_table_ddl(
            name=name,
            location=location,
            config=config,
        )

    def register_external_table(
        self,
        *,
        name: str,
        location: DatasetLocation | str,
        ddl: str | None = None,
        sql_options: SQLOptions | None = None,
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
        ddl
            Optional pre-rendered DDL statement for registration.
        sql_options
            Optional SQL options override for the registration statement.

        Notes
        -----
        This method records a diagnostics artifact containing the
        generated DDL and location information when a diagnostics
        sink is configured in the runtime profile.

        Raises
        ------
        ValueError
            If DDL is not provided and location is not a DatasetLocation.
        """
        resolved_ddl = ddl
        if resolved_ddl is None:
            if isinstance(location, str):
                msg = "DatasetLocation is required when DDL is not provided."
                raise ValueError(msg)
            resolved_ddl = self.external_table_ddl(name=name, location=location)
        self._validate_sql(resolved_ddl)
        options = sql_options or self._statement_options()
        self.ctx.sql_with_options(resolved_ddl, options).collect()
        self._record_registration(
            name=name,
            registration_type="table",
            location=_location_payload(location),
        )
        self._record_artifact(
            "external_table_registered",
            {
                "name": name,
                "location": _location_payload(location),
                "ddl": resolved_ddl,
            },
        )
        invalidate_introspection_cache(self.ctx)

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
        if self.profile and self.profile.diagnostics_sink:
            self.profile.diagnostics_sink.record_artifact(name, payload)


def _location_payload(location: DatasetLocation | str) -> str:
    if isinstance(location, str):
        return location
    return str(location.path)


def _external_table_ddl_config(
    *,
    location: DatasetLocation,
    profile: DataFusionRuntimeProfile | None,
) -> ExternalTableDDLConfig:
    from ibis_engine.registry import (
        resolve_datafusion_scan_options,
    )

    _ensure_delta_ddl_support(location)
    scan = resolve_datafusion_scan_options(location)
    partitioned_by = (
        tuple(col for col, _ in scan.partition_cols) if scan and scan.partition_cols else None
    )
    file_sort_order = scan.file_sort_order if scan else None
    unbounded = scan.unbounded if scan else False
    options = _external_table_options(
        location=location,
        scan=scan,
        profile=profile,
    )
    compression = None
    if options is not None:
        compression = options.pop("compression", None)
    dialect = _external_table_dialect(location=location, profile=profile)
    schema_expressions = _schema_expressions_for_location(
        location=location,
        dialect=dialect,
    )
    return ExternalTableDDLConfig(
        schema=None,
        schema_expressions=schema_expressions,
        file_format=location.format or "PARQUET",
        options=options,
        compression=compression,
        partitioned_by=partitioned_by,
        file_sort_order=file_sort_order,
        unbounded=unbounded,
        dialect=dialect,
    )


def _external_table_dialect(
    *,
    location: DatasetLocation,
    profile: DataFusionRuntimeProfile | None,
) -> str:
    if (
        profile is not None
        and profile.enable_delta_session_defaults
        and location.format == "delta"
    ):
        return "datafusion_ext"
    return "datafusion"


def _schema_expressions_for_location(
    *,
    location: DatasetLocation,
    dialect: str,
) -> list[exp.Expression] | None:
    table_spec = _resolve_table_spec(location)
    if table_spec is None:
        return None
    column_defs = table_spec.to_sqlglot_column_defs(dialect=dialect)
    expressions: list[exp.Expression] = list(column_defs)
    if table_spec.key_fields:
        expressions.append(
            exp.PrimaryKey(
                expressions=[exp.to_identifier(field) for field in table_spec.key_fields]
            )
        )
    return expressions


def _resolve_table_spec(location: DatasetLocation) -> TableSchemaSpec | None:
    if location.dataset_spec is not None:
        return location.dataset_spec.table_spec
    return location.table_spec


def _external_table_options(
    *,
    location: DatasetLocation,
    scan: DataFusionScanOptions | None,
    profile: DataFusionRuntimeProfile | None,
) -> dict[str, str] | None:
    from ibis_engine.registry import resolve_delta_log_storage_options

    options: dict[str, object] = {}
    if profile is not None and profile.external_table_options:
        options.update(profile.external_table_options)
    if location.format == "delta":
        log_storage = resolve_delta_log_storage_options(location)
        if log_storage:
            options.update(log_storage)
    elif location.storage_options:
        options.update(location.storage_options)
    if location.read_options:
        options.update(location.read_options)
    options.update(_scan_external_table_options(location=location, scan=scan))
    return _normalize_external_table_options(options)


def _scan_external_table_options(
    *,
    location: DatasetLocation,
    scan: DataFusionScanOptions | None,
) -> dict[str, object]:
    if scan is None:
        return {}
    options: dict[str, object] = {}
    if getattr(scan, "file_extension", None) and location.format != "delta":
        options["file_extension"] = scan.file_extension
    if location.format == "parquet":
        for key in (
            "skip_metadata",
            "schema_force_view_types",
            "binary_as_string",
            "skip_arrow_metadata",
        ):
            value = getattr(scan, key, None)
            if value is not None:
                options[key] = value
        parquet_column_options = getattr(scan, "parquet_column_options", None)
        if parquet_column_options is not None:
            options.update(parquet_column_options.external_table_options())
    return options


def _normalize_external_table_options(
    options: dict[str, object],
) -> dict[str, str] | None:
    normalized: dict[str, str] = {}
    for key, value in options.items():
        rendered = _option_literal_value(value)
        if rendered is None:
            continue
        normalized[str(key)] = rendered
    return normalized or None


def _option_literal_value(value: object) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return None


def _ensure_delta_ddl_support(location: DatasetLocation) -> None:
    if location.format != "delta":
        return
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError as exc:
        msg = "Delta DDL registration requires datafusion_ext.install_delta_table_factory."
        raise RuntimeError(msg) from exc
    installer = getattr(module, "install_delta_table_factory", None)
    if not callable(installer):
        msg = "Delta DDL registration requires datafusion_ext.install_delta_table_factory."
        raise TypeError(msg)
