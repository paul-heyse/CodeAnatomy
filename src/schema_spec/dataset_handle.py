"""Dataset handle helpers for schema-driven registration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion.dataframe import DataFrame

from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.schema.registry import is_extract_nested_dataset
from schema_spec.dataset_spec_ops import (
    dataset_spec_name,
    dataset_spec_resolved_view_specs,
    dataset_spec_schema,
)

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.session.runtime import SessionRuntime
    from schema_spec.system import DatasetSpec
    from schema_spec.view_specs import ViewSpec


def _schema_version_from_name(name: str) -> int | None:
    """Extract a version suffix from a schema name.

    Returns:
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

        Raises:
            ValueError: If the operation cannot be completed.
        """
        name = dataset_spec_name(self.spec)
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
        if is_extract_nested_dataset(name):
            return
        if spec_version is None and name_version is None:
            msg = (
                "DatasetHandle requires a versioned name or explicit table_spec.version "
                f"for {name!r}."
            )
            raise ValueError(msg)

    def schema(self) -> SchemaLike:
        """Return the dataset schema.

        Returns:
        -------
        SchemaLike
            Arrow schema for the dataset.
        """
        return dataset_spec_schema(self.spec)

    def register(
        self,
        session_runtime: SessionRuntime,
        *,
        location: DatasetLocation,
    ) -> DataFrame:
        """Register the dataset in DataFusion and return a DataFrame.

        Parameters
        ----------
        session_runtime:
            DataFusion SessionRuntime used for registration.
        location:
            Dataset location to register.

        Returns:
        -------
        datafusion.dataframe.DataFrame
            Registered DataFrame for the dataset location.
        """
        from datafusion_engine.dataset.registration import (
            DatasetRegistrationOptions,
            register_dataset_df,
        )

        return register_dataset_df(
            session_runtime.ctx,
            name=dataset_spec_name(self.spec),
            location=location,
            options=DatasetRegistrationOptions(runtime_profile=session_runtime.profile),
        )

    @staticmethod
    def register_views(
        session_runtime: SessionRuntime,
        *,
        validate: bool = True,
    ) -> None:
        """Register associated view specs into DataFusion (removed).

        Args:
            session_runtime: Description.
            validate: Description.

        Raises:
            RuntimeError: If the operation cannot be completed.
        """
        _ = session_runtime
        _ = validate
        msg = "DatasetHandle.register_views is removed; use ensure_view_graph."
        raise RuntimeError(msg)

    def view_specs(self) -> tuple[ViewSpec, ...]:
        """Return the view specs associated with the dataset.

        Returns:
        -------
        tuple[ViewSpec, ...]
            View specifications for the dataset.
        """
        return dataset_spec_resolved_view_specs(self.spec)


__all__ = ["DatasetHandle"]
