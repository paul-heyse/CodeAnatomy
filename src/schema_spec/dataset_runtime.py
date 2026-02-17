"""Dataset-spec introspection/discovery runtime helpers."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from functools import lru_cache
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.dataset as ds

from core_types import PathLike
from datafusion_engine.arrow.encoding import EncodingPolicy
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.delta.service_protocol import DeltaServicePort
from datafusion_engine.schema.alignment import SchemaEvolutionSpec
from datafusion_engine.schema.introspection_core import SchemaIntrospector
from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from schema_spec.dataset_spec import DatasetOpenSpec, DatasetSpec


def dataset_spec_encoding_policy(spec: DatasetSpec) -> EncodingPolicy | None:
    """Return encoding policy derived from the dataset schema spec."""
    from datafusion_engine.arrow.metadata import encoding_policy_from_spec

    policy = encoding_policy_from_spec(spec.table_spec)
    if not policy.specs:
        return None
    return policy


def ddl_fingerprint_from_definition(ddl: str) -> str:
    """Return a stable fingerprint for a CREATE TABLE statement."""
    return hash_sha256_hex(ddl.encode("utf-8"))


def dataset_table_ddl_fingerprint(
    name: str,
    *,
    introspector: SchemaIntrospector,
) -> str | None:
    """Return the DDL fingerprint for a DataFusion-registered table."""
    ddl = dataset_table_definition(name, introspector=introspector)
    if ddl is None:
        return None
    return ddl_fingerprint_from_definition(ddl)


def dataset_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> DatasetSpec:
    """Create a DatasetSpec from an Arrow schema.

    Returns:
        DatasetSpec: Derived dataset specification.
    """
    from schema_spec.discovery import dataset_spec_from_schema as _dataset_spec_from_schema

    return _dataset_spec_from_schema(name=name, schema=schema, version=version)


def dataset_table_definition(name: str, *, introspector: SchemaIntrospector) -> str | None:
    """Return the DataFusion CREATE TABLE definition for a dataset."""
    return introspector.table_definition(name)


def dataset_table_constraints(name: str, *, introspector: SchemaIntrospector) -> tuple[str, ...]:
    """Return DataFusion constraint metadata for a dataset."""
    return introspector.table_constraints(name)


def dataset_table_column_defaults(
    name: str, *, introspector: SchemaIntrospector
) -> dict[str, object]:
    """Return DataFusion column default metadata for a dataset."""
    return introspector.table_column_defaults(name)


def dataset_table_logical_plan(name: str, *, introspector: SchemaIntrospector) -> str | None:
    """Return DataFusion logical plan text for a dataset."""
    return introspector.table_logical_plan(name)


def schema_from_datafusion_catalog(name: str, *, introspector: SchemaIntrospector) -> pa.Schema:
    """Return Arrow schema from DataFusion catalog for a registered table."""
    return introspector.table_schema(name)


def dataset_spec_from_dataset(
    name: str,
    dataset: ds.Dataset,
    *,
    version: int | None = None,
) -> DatasetSpec:
    """Create a DatasetSpec from a dataset schema.

    Returns:
        DatasetSpec: Derived dataset specification.
    """
    from schema_spec.discovery import dataset_spec_from_dataset as _dataset_spec_from_dataset

    return _dataset_spec_from_dataset(name=name, dataset=dataset, version=version)


def dataset_spec_from_path(
    name: str,
    path: PathLike,
    *,
    options: DatasetOpenSpec | None = None,
    version: int | None = None,
    delta_service: DeltaServicePort | None = None,
) -> DatasetSpec:
    """Create a DatasetSpec from a dataset path.

    Returns:
        DatasetSpec: Derived dataset specification.
    """
    from schema_spec.discovery import dataset_spec_from_path as _dataset_spec_from_path

    return _dataset_spec_from_path(
        name=name,
        path=path,
        options=options,
        version=version,
        delta_service=delta_service,
    )


@lru_cache(maxsize=1)
def _schema_evolution_presets() -> Mapping[str, SchemaEvolutionSpec]:
    from datafusion_engine.schema.nested_views import extract_nested_dataset_names

    return {
        name: SchemaEvolutionSpec(
            allow_missing=True,
            allow_extra=True,
            allow_casts=True,
        )
        for name in (*extract_nested_dataset_names(), "symtable_files_v1")
    }


class _SchemaEvolutionPresetsProxy(Mapping[str, SchemaEvolutionSpec]):
    """Lazy mapping proxy for schema-evolution defaults."""

    def __getitem__(self, key: str) -> SchemaEvolutionSpec:
        return _schema_evolution_presets()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(_schema_evolution_presets())

    def __len__(self) -> int:
        return len(_schema_evolution_presets())


SCHEMA_EVOLUTION_PRESETS: Mapping[str, SchemaEvolutionSpec] = _SchemaEvolutionPresetsProxy()


def resolve_schema_evolution_spec(name: str) -> SchemaEvolutionSpec:
    """Return a canonical schema evolution spec for a dataset name."""
    return _schema_evolution_presets().get(name, SchemaEvolutionSpec())


__all__ = [
    "SCHEMA_EVOLUTION_PRESETS",
    "dataset_spec_encoding_policy",
    "dataset_spec_from_dataset",
    "dataset_spec_from_path",
    "dataset_spec_from_schema",
    "dataset_table_column_defaults",
    "dataset_table_constraints",
    "dataset_table_ddl_fingerprint",
    "dataset_table_definition",
    "dataset_table_logical_plan",
    "ddl_fingerprint_from_definition",
    "resolve_schema_evolution_spec",
    "schema_from_datafusion_catalog",
]
