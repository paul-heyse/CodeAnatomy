"""Runtime-backed registry accessors for normalize datasets."""

from __future__ import annotations

import contextlib
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import SessionContext

import normalize.dataset_specs as static_dataset_specs
from core_types import PathLike, ensure_path
from datafusion_engine.arrow_interop import SchemaLike
from datafusion_engine.arrow_schema.metadata import SchemaMetadataSpec
from datafusion_engine.dataset_registration import register_dataset_df
from datafusion_engine.dataset_registry import DatasetLocation
from datafusion_engine.introspection import introspection_cache_for_ctx
from datafusion_engine.query_spec import QuerySpec
from datafusion_engine.schema_contracts import (
    SchemaContract,
    SchemaViolation,
    schema_contract_from_dataset_spec,
)
from datafusion_engine.schema_introspection import table_names_snapshot
from datafusion_engine.schema_policy import SchemaPolicy, SchemaPolicyOptions, schema_policy_factory
from datafusion_engine.sql_options import sql_options_for_profile
from datafusion_engine.table_provider_metadata import TableProviderMetadata, table_provider_metadata
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import ContractSpec, DatasetSpec

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile

NORMALIZE_STAGE_META = "normalize_stage"
NORMALIZE_ALIAS_META = "normalize_alias"


def normalize_output_root(output_dir: PathLike) -> Path:
    """Return the normalize output root directory.

    Parameters
    ----------
    output_dir : PathLike
        Base output directory.

    Returns
    -------
    pathlib.Path
        Normalize output root directory.
    """
    resolved = ensure_path(output_dir)
    return resolved / "normalize"


def normalize_output_locations(
    output_dir: PathLike,
    *,
    datasets: Sequence[str] | None = None,
) -> Mapping[str, str]:
    """Return output paths for normalize datasets.

    Parameters
    ----------
    output_dir : PathLike
        Base output directory.
    datasets : Sequence[str] | None
        Optional dataset names or aliases to resolve.

    Returns
    -------
    Mapping[str, str]
        Mapping of dataset names to output locations.
    """
    root = normalize_output_root(output_dir)
    names = _normalize_output_names(datasets)
    return {name: str(root / name) for name in names}


def register_normalize_output_tables(
    ctx: SessionContext,
    output_dir: PathLike,
    *,
    datasets: Sequence[str] | None = None,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> Mapping[str, str]:
    """Register normalize outputs in DataFusion using non-DDL APIs.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session context for DDL registration.
    output_dir : PathLike
        Base output directory.
    datasets : Sequence[str] | None
        Optional dataset names or aliases to register.
    runtime_profile : DataFusionRuntimeProfile | None
        Optional runtime profile used for SQL options.

    Returns
    -------
    Mapping[str, str]
        Mapping of dataset names to registered output locations.
    """
    locations = normalize_output_locations(output_dir, datasets=datasets)
    for name, location in locations.items():
        spec = static_dataset_specs.dataset_spec(name)
        register_dataset_df(
            ctx,
            name=name,
            location=DatasetLocation(
                path=location,
                format="delta",
                dataset_spec=spec,
            ),
            runtime_profile=runtime_profile,
        )
    return locations


def dataset_names(ctx: SessionContext | None = None) -> tuple[str, ...]:
    """Return normalize dataset names from runtime and static registries.

    Parameters
    ----------
    ctx : SessionContext | None
        Optional DataFusion session context to query for registered tables.

    Returns
    -------
    tuple[str, ...]
        Sorted tuple of normalize dataset names.
    """
    session = _resolve_session_context(ctx)
    names = table_names_snapshot(session, sql_options=sql_options_for_profile(None))
    normalize_names = {name for name in names if _is_normalize_dataset(session, name)}
    static_names = set(static_dataset_specs.dataset_names())
    return tuple(sorted(normalize_names | static_names))


def dataset_alias(name: str, *, ctx: SessionContext | None = None) -> str:
    """Return the canonical alias for a normalize dataset.

    Parameters
    ----------
    name : str
        Dataset name to resolve.
    ctx : SessionContext | None
        Optional DataFusion session context for metadata lookup. When omitted,
        returns the static schema without table metadata enrichment.

    Returns
    -------
    str
        Canonical dataset alias.
    """
    session = _resolve_session_context(ctx)
    meta = _metadata_for_table(session, name)
    alias = meta.get(NORMALIZE_ALIAS_META)
    if alias:
        return alias
    with contextlib.suppress(KeyError):
        return static_dataset_specs.dataset_alias(name)
    return _strip_version(name)


def dataset_name_from_alias(alias: str, *, ctx: SessionContext | None = None) -> str:
    """Return the dataset name for a normalize alias.

    Parameters
    ----------
    alias : str
        Alias to resolve.
    ctx : SessionContext | None
        Optional DataFusion session context for metadata lookup.

    Returns
    -------
    str
        Dataset name for the alias.

    Raises
    ------
    ValueError
        Raised when multiple datasets map to the same alias.
    KeyError
        Raised when the alias is not recognized.
    """
    session = _resolve_session_context(ctx)
    names = dataset_names(session)
    alias_map: dict[str, str] = {}
    for name in names:
        resolved = dataset_alias(name, ctx=session)
        existing = alias_map.get(resolved)
        if existing is not None and existing != name:
            msg = f"Duplicate normalize alias {resolved!r} for {existing!r} and {name!r}."
            raise ValueError(msg)
        alias_map[resolved] = name
    if alias in alias_map:
        return alias_map[alias]
    if alias in names:
        return alias
    msg = f"Unknown normalize dataset alias: {alias!r}."
    raise KeyError(msg)


def dataset_schema(name: str, *, ctx: SessionContext | None = None) -> SchemaLike:
    """Return the dataset schema from the DataFusion context.

    Parameters
    ----------
    name : str
        Dataset name to resolve.
    ctx : SessionContext | None
        Optional DataFusion session context for metadata lookup.

    Returns
    -------
    SchemaLike
        Dataset schema enriched with table metadata when available.

    Raises
    ------
    KeyError
        Raised when the dataset is not registered.
    """
    try:
        schema = static_dataset_specs.dataset_schema(name)
    except KeyError as exc:
        msg = f"Unknown DataFusion schema: {name!r}."
        raise KeyError(msg) from exc
    if ctx is None:
        return schema
    session = _resolve_session_context(ctx)
    metadata = _metadata_for_table(session, name)
    return _schema_with_table_metadata(schema, metadata=metadata)


def dataset_spec(name: str, *, ctx: SessionContext | None = None) -> DatasetSpec:
    """Return a DatasetSpec for the dataset name.

    Parameters
    ----------
    name : str
        Dataset name to resolve.
    ctx : SessionContext | None
        DataFusion session context used for information_schema validation.

    Returns
    -------
    DatasetSpec
        Dataset specification for the given name.
    """
    if ctx is not None:
        _resolve_session_context(ctx)
    return static_dataset_specs.dataset_spec(name)


def dataset_contract(name: str, *, ctx: SessionContext | None = None) -> ContractSpec:
    """Return the ContractSpec for the dataset name.

    Parameters
    ----------
    name : str
        Dataset name to resolve.
    ctx : SessionContext | None
        Optional DataFusion session context.

    Returns
    -------
    ContractSpec
        Contract specification for the dataset.
    """
    return dataset_spec(name, ctx=ctx).contract_spec_or_default()


def dataset_schema_contract(name: str, *, ctx: SessionContext | None = None) -> SchemaContract:
    """Return the SchemaContract for the dataset name.

    Parameters
    ----------
    name : str
        Dataset name to resolve.
    ctx : SessionContext | None
        Optional DataFusion session context.

    Returns
    -------
    SchemaContract
        Schema contract derived from the dataset spec.
    """
    spec = dataset_spec(name, ctx=ctx)
    return schema_contract_from_dataset_spec(name=spec.name, spec=spec)


def dataset_contract_violations(
    name: str,
    *,
    ctx: SessionContext | None = None,
) -> tuple[SchemaViolation, ...]:
    """Return schema contract violations for the dataset name.

    Parameters
    ----------
    name : str
        Dataset name to resolve.
    ctx : SessionContext | None
        Optional DataFusion session context.

    Returns
    -------
    tuple[SchemaViolation, ...]
        Contract violations for the current information_schema snapshot.
    """
    session = _resolve_session_context(ctx)
    contract = dataset_schema_contract(name, ctx=session)
    snapshot = introspection_cache_for_ctx(session).snapshot
    return tuple(contract.validate_against_introspection(snapshot))


def dataset_schema_policy(name: str) -> SchemaPolicy:
    """Return a schema policy derived from the dataset spec.

    Parameters
    ----------
    name : str
        Dataset name to resolve.

    Returns
    -------
    SchemaPolicy
        Policy configured for schema validation and encoding.
    """
    spec = dataset_spec(name)
    contract = spec.contract()
    options = SchemaPolicyOptions(
        schema=contract.with_versioned_schema(),
        encoding=spec.encoding_policy(),
        metadata=spec.metadata_spec,
        validation=contract.validation,
    )
    return schema_policy_factory(spec.table_spec, options=options)


def dataset_metadata_spec(name: str, *, ctx: SessionContext | None = None) -> SchemaMetadataSpec:
    """Return the metadata spec for the dataset name.

    Parameters
    ----------
    name : str
        Dataset name to resolve.
    ctx : SessionContext | None
        Optional DataFusion session context.

    Returns
    -------
    SchemaMetadataSpec
        Metadata specification for the dataset.
    """
    return dataset_spec(name, ctx=ctx).metadata_spec


def dataset_input_schema(name: str) -> SchemaLike:
    """Return the input schema for a normalize dataset.

    Parameters
    ----------
    name : str
        Dataset name to resolve.

    Returns
    -------
    SchemaLike
        Input schema for the dataset.
    """
    return static_dataset_specs.dataset_input_schema(name)


def dataset_input_columns(name: str) -> tuple[str, ...]:
    """Return input column names for a normalize dataset.

    Parameters
    ----------
    name : str
        Dataset name to resolve.

    Returns
    -------
    tuple[str, ...]
        Input column names for the dataset.
    """
    return static_dataset_specs.dataset_input_columns(name)


def dataset_query(name: str, *, ctx: SessionContext | None = None) -> QuerySpec:
    """Return the QuerySpec for the dataset name.

    Parameters
    ----------
    name : str
        Dataset name to resolve.
    ctx : SessionContext | None
        Optional DataFusion session context.

    Returns
    -------
    QuerySpec
        Query specification for the dataset.
    """
    return dataset_spec(name, ctx=ctx).query()


def dataset_specs() -> Iterable[DatasetSpec]:
    """Return static normalize dataset specs.

    Returns
    -------
    Iterable[DatasetSpec]
        Static dataset specifications for normalize datasets.
    """
    return static_dataset_specs.dataset_specs()


def dataset_table_spec(name: str, *, ctx: SessionContext | None = None) -> TableSchemaSpec:
    """Return the TableSchemaSpec for the dataset name.

    Parameters
    ----------
    name : str
        Dataset name to resolve.
    ctx : SessionContext | None
        Optional DataFusion session context.

    Returns
    -------
    TableSchemaSpec
        Table schema specification for the dataset.
    """
    return dataset_spec(name, ctx=ctx).table_spec


def _resolve_session_context(ctx: SessionContext | None) -> SessionContext:
    if ctx is not None:
        return ctx
    msg = "DataFusion SessionContext is required; pass an explicit context."
    raise ValueError(msg)


def _metadata_for_table(ctx: SessionContext, name: str) -> dict[str, str]:
    metadata: dict[str, str] = {}
    provider = table_provider_metadata(id(ctx), table_name=name)
    if isinstance(provider, TableProviderMetadata) and provider.metadata:
        metadata.update(provider.metadata)
    return metadata


def _is_normalize_dataset(ctx: SessionContext, name: str) -> bool:
    return _metadata_for_table(ctx, name).get(NORMALIZE_STAGE_META) == "normalize"


def _normalize_output_names(names: Sequence[str] | None) -> tuple[str, ...]:
    if names is None:
        return tuple(static_dataset_specs.dataset_names())
    resolved: list[str] = []
    for raw_name in names:
        resolved_name = raw_name
        with contextlib.suppress(KeyError):
            resolved_name = static_dataset_specs.dataset_name_from_alias(raw_name)
        resolved.append(resolved_name)
    return tuple(dict.fromkeys(resolved))


def _strip_version(name: str) -> str:
    base, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return base
    return name


def _decode_metadata(metadata: Mapping[bytes, bytes]) -> dict[str, str]:
    decoded: dict[str, str] = {}
    for raw_key, raw_value in metadata.items():
        key = _decode_meta_value(raw_key)
        value = _decode_meta_value(raw_value)
        if key is None or value is None:
            continue
        decoded[key] = value
    return decoded


def _decode_meta_value(value: object) -> str | None:
    if isinstance(value, bytes):
        with contextlib.suppress(UnicodeDecodeError):
            return value.decode("utf-8")
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    return None


def _schema_with_table_metadata(
    schema: SchemaLike,
    *,
    metadata: Mapping[str, str],
) -> SchemaLike:
    if not metadata:
        return schema
    if isinstance(schema, pa.Schema):
        merged = dict(schema.metadata or {})
        for key, value in metadata.items():
            merged.setdefault(key.encode("utf-8"), str(value).encode("utf-8"))
        return schema.with_metadata(merged)
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            resolved_schema = cast("pa.Schema", resolved)
            merged = dict(resolved_schema.metadata or {})
            for key, value in metadata.items():
                merged.setdefault(key.encode("utf-8"), str(value).encode("utf-8"))
            return resolved_schema.with_metadata(merged)
    return schema


__all__ = [
    "NORMALIZE_ALIAS_META",
    "NORMALIZE_STAGE_META",
    "dataset_alias",
    "dataset_contract",
    "dataset_contract_violations",
    "dataset_input_columns",
    "dataset_input_schema",
    "dataset_metadata_spec",
    "dataset_name_from_alias",
    "dataset_names",
    "dataset_query",
    "dataset_schema",
    "dataset_schema_contract",
    "dataset_schema_policy",
    "dataset_spec",
    "dataset_specs",
    "dataset_table_spec",
    "normalize_output_locations",
    "normalize_output_root",
    "register_normalize_output_tables",
]
