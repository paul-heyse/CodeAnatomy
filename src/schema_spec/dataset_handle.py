"""Dataset handle helpers for schema-driven registration."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from arrowdsl.core.interop import SchemaLike
from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.schema_registry import is_nested_dataset
from ibis_engine.registry import DatasetLocation, resolve_datafusion_scan_options
from schema_spec.specs import ExternalTableConfigOverrides

if TYPE_CHECKING:
    from schema_spec.system import DatasetSpec
    from schema_spec.view_specs import ViewSpec


def _schema_version_from_name(name: str) -> int | None:
    """Extract a version suffix from a schema name.

    Returns
    -------
    int | None
        Parsed version suffix when available.
    """
    _, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return int(suffix)
    return None


@dataclass(frozen=True)
class DatasetHandle:
    """Object-oriented dataset handle with schema + lifecycle."""

    spec: DatasetSpec

    def __post_init__(self) -> None:
        """Validate dataset handle invariants.

        Raises
        ------
        ValueError
            Raised when the dataset naming or versioning is invalid.
        """
        name = self.spec.name
        if not name:
            msg = "DatasetHandle requires a non-empty dataset name."
            raise ValueError(msg)
        name_version = _schema_version_from_name(name)
        spec_version = self.spec.table_spec.version
        if spec_version is not None and name_version is not None and spec_version != name_version:
            msg = (
                "DatasetHandle version mismatch: "
                f"name {name!r} implies v{name_version} but spec has v{spec_version}."
            )
            raise ValueError(msg)
        if is_nested_dataset(name):
            return
        if spec_version is None and name_version is None:
            msg = (
                "DatasetHandle requires a versioned name or explicit table_spec.version "
                f"for {name!r}."
            )
            raise ValueError(msg)

    def schema(self) -> SchemaLike:
        """Return the dataset schema.

        Returns
        -------
        SchemaLike
            Arrow schema for the dataset.
        """
        return self.spec.schema()

    def ddl(
        self,
        *,
        location: str,
        file_format: str,
        overrides: ExternalTableConfigOverrides | None = None,
    ) -> str:
        """Return a CREATE EXTERNAL TABLE statement for the dataset.

        Parameters
        ----------
        location:
            Dataset location for the external table.
        file_format:
            Storage format for the external table.
        overrides:
            Optional overrides for table options and formatting.

        Returns
        -------
        str
            CREATE EXTERNAL TABLE statement derived from the spec.
        """
        config = self.spec.table_spec.external_table_config(
            location=location,
            file_format=file_format,
            overrides=overrides,
        )
        return self.spec.external_table_sql(config)

    def ddl_for_location(
        self,
        location: DatasetLocation,
        *,
        table_name: str | None = None,
        dialect: str | None = None,
    ) -> str:
        """Return a CREATE EXTERNAL TABLE statement for a DatasetLocation.

        Parameters
        ----------
        location:
            Dataset location describing storage format and path.
        table_name:
            Optional override for the table name.
        dialect:
            Optional SQL dialect for the DDL statement.

        Returns
        -------
        str
            CREATE EXTERNAL TABLE statement using the location metadata.
        """
        scan = resolve_datafusion_scan_options(location)
        partitioned_by = None
        file_sort_order = None
        unbounded = None
        if scan is not None:
            partitioned_by = tuple(col for col, _ in scan.partition_cols) or None
            file_sort_order = scan.file_sort_order or None
            unbounded = scan.unbounded
        overrides = ExternalTableConfigOverrides(
            table_name=table_name,
            dialect=dialect,
            options=location.read_options,
            partitioned_by=partitioned_by,
            file_sort_order=file_sort_order,
            unbounded=unbounded,
        )
        return self.ddl(
            location=str(location.path),
            file_format=location.format,
            overrides=overrides,
        )

    def register(
        self,
        ctx: SessionContext,
        *,
        location: DatasetLocation,
    ) -> DataFrame:
        """Register the dataset in DataFusion and return a DataFrame.

        Parameters
        ----------
        ctx:
            DataFusion session context used for registration.
        location:
            Dataset location to register.

        Returns
        -------
        datafusion.dataframe.DataFrame
            Registered DataFrame for the dataset location.
        """
        return register_dataset_df(ctx, name=self.spec.name, location=location)

    def register_views(
        self,
        ctx: SessionContext,
        *,
        record_view: Callable[[str, str | None], None] | None = None,
        validate: bool = True,
    ) -> None:
        """Register associated view specs into DataFusion.

        Parameters
        ----------
        ctx:
            DataFusion session context used for registration.
        record_view:
            Optional callback to record view definitions.
        validate:
            Whether to validate the view schemas after registration.
        """
        for view in self.spec.resolved_view_specs():
            view.register(ctx, record_view=record_view, validate=validate)

    def view_specs(self) -> tuple[ViewSpec, ...]:
        """Return the view specs associated with the dataset.

        Returns
        -------
        tuple[ViewSpec, ...]
            View specifications for the dataset.
        """
        return self.spec.resolved_view_specs()


__all__ = ["DatasetHandle"]
