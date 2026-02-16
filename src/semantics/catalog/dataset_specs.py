"""Static accessors for semantic dataset specifications."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.arrow.interop import SchemaLike
    from schema_spec.dataset_spec import ContractSpec, DatasetSpec
    from semantics.catalog.dataset_rows import SemanticDatasetRow


@dataclass
class _DatasetSpecCache:
    dataset_rows: dict[str, SemanticDatasetRow] | None = None
    input_schemas: dict[str, SchemaLike] | None = None
    dataset_specs: dict[str, DatasetSpec] | None = None


_CACHE = _DatasetSpecCache()


def _get_all_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Import and return all dataset rows to avoid circular import.

    Returns:
    -------
    tuple[SemanticDatasetRow, ...]
        All semantic dataset rows.
    """
    from semantics.catalog.dataset_rows import get_all_dataset_rows

    return get_all_dataset_rows()


def _build_spec(row: SemanticDatasetRow) -> DatasetSpec:
    """Import and build dataset spec to avoid circular import.

    Parameters
    ----------
    row
        Semantic dataset row.

    Returns:
    -------
    DatasetSpec
        Built dataset spec.
    """
    from semantics.catalog.spec_builder import build_dataset_spec

    return build_dataset_spec(row)


def _build_input(row: SemanticDatasetRow) -> SchemaLike:
    """Import and build input schema to avoid circular import.

    Parameters
    ----------
    row
        Semantic dataset row.

    Returns:
    -------
    SchemaLike
        Built input schema.
    """
    from semantics.catalog.spec_builder import build_input_schema

    return build_input_schema(row)


def _get_dataset_rows_map() -> dict[str, SemanticDatasetRow]:
    """Return the lazily-initialized dataset rows mapping.

    Returns:
    -------
    dict[str, SemanticDatasetRow]
        Mapping from dataset name to row.
    """
    dataset_rows = _CACHE.dataset_rows
    if dataset_rows is None:
        dataset_rows = {row.name: row for row in _get_all_dataset_rows()}
        _CACHE.dataset_rows = dataset_rows
    return dataset_rows


def _get_input_schemas() -> dict[str, SchemaLike]:
    """Return the lazily-initialized input schemas mapping.

    Returns:
    -------
    dict[str, SchemaLike]
        Mapping from dataset name to input schema.
    """
    input_schemas = _CACHE.input_schemas
    if input_schemas is None:
        input_schemas = {row.name: _build_input(row) for row in _get_all_dataset_rows()}
        _CACHE.input_schemas = input_schemas
    return input_schemas


def _get_dataset_specs() -> dict[str, DatasetSpec]:
    """Return the lazily-initialized dataset specs mapping.

    Returns:
    -------
    dict[str, DatasetSpec]
        Mapping from dataset name to DatasetSpec.
    """
    dataset_specs = _CACHE.dataset_specs
    if dataset_specs is None:
        dataset_specs = {row.name: _build_spec(row) for row in _get_all_dataset_rows()}
        _CACHE.dataset_specs = dataset_specs
    return dataset_specs


def dataset_spec(name: str) -> DatasetSpec:
    """Return a DatasetSpec by name.

    Parameters
    ----------
    name
        Dataset name.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    specs = _get_dataset_specs()
    if name not in specs:
        msg = f"Unknown semantic dataset: {name!r}."
        raise KeyError(msg)
    return specs[name]


def maybe_dataset_spec(name: str) -> DatasetSpec | None:
    """Return a DatasetSpec by name when available.

    Returns:
    -------
    DatasetSpec | None
        Registered dataset spec when present, otherwise ``None``.
    """
    try:
        return dataset_spec(name)
    except KeyError:
        return None


def dataset_specs() -> Iterable[DatasetSpec]:
    """Return all semantic dataset specs.

    Returns:
    -------
    Iterable[DatasetSpec]
        Dataset specifications in registry order.
    """
    return (spec for spec in _get_dataset_specs().values())


def dataset_schema(name: str) -> SchemaLike:
    """Return the schema for a semantic dataset with metadata applied.

    Parameters
    ----------
    name
        Dataset name to retrieve.

    Returns:
    -------
    SchemaLike
        Dataset schema with metadata.
    """
    from schema_spec.dataset_spec import dataset_spec_schema

    spec = dataset_spec(name)
    return dataset_spec_schema(spec)


def dataset_input_schema(name: str) -> SchemaLike:
    """Return the input schema for a semantic dataset.

    Args:
        name: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    schemas = _get_input_schemas()
    if name not in schemas:
        msg = f"Unknown semantic dataset: {name!r}."
        raise KeyError(msg)
    return schemas[name]


def dataset_names() -> tuple[str, ...]:
    """Return semantic dataset names in registry order.

    Returns:
    -------
    tuple[str, ...]
        Dataset names.
    """
    return tuple(row.name for row in _get_all_dataset_rows())


def dataset_name_from_alias(alias: str) -> str:
    """Return the canonical dataset name for an alias (identity mapping)."""
    return alias


def dataset_alias(name: str) -> str:
    """Return the canonical alias for a dataset name (identity mapping)."""
    return name


def dataset_contract(
    name: str,
) -> ContractSpec:
    """Return the ContractSpec for a semantic dataset.

    Parameters
    ----------
    name
        Dataset name to retrieve.

    Returns:
    -------
    ContractSpec
        Contract specification for the dataset.
    """
    from schema_spec.dataset_spec import dataset_spec_contract_spec_or_default

    spec = dataset_spec(name)
    return dataset_spec_contract_spec_or_default(spec)


def _reset_cache() -> None:
    """Reset module-level dataset spec caches. For testing only."""
    _CACHE.dataset_rows = None
    _CACHE.input_schemas = None
    _CACHE.dataset_specs = None


def dataset_contract_schema(name: str) -> SchemaLike:
    """Return the contract schema for a semantic dataset.

    Parameters
    ----------
    name
        Dataset name to retrieve.

    Returns:
    -------
    SchemaLike
        Arrow schema defined by the dataset contract.
    """
    contract = dataset_contract(name)
    return contract.to_contract().schema


def dataset_merge_keys(name: str) -> tuple[str, ...]:
    """Return merge keys for a semantic dataset.

    Args:
        name: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    rows = _get_dataset_rows_map()
    if name not in rows:
        msg = f"Unknown semantic dataset: {name!r}."
        raise KeyError(msg)
    row = rows[name]
    if row.merge_keys is None:
        return ()
    return row.merge_keys


def supports_incremental(name: str) -> bool:
    """Return whether a semantic dataset supports incremental processing.

    Args:
        name: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    rows = _get_dataset_rows_map()
    if name not in rows:
        msg = f"Unknown semantic dataset: {name!r}."
        raise KeyError(msg)
    row = rows[name]
    return row.supports_cdf and row.merge_keys is not None


__all__ = [
    "_reset_cache",
    "dataset_alias",
    "dataset_contract",
    "dataset_contract_schema",
    "dataset_input_schema",
    "dataset_merge_keys",
    "dataset_name_from_alias",
    "dataset_names",
    "dataset_schema",
    "dataset_spec",
    "dataset_specs",
    "supports_incremental",
]
