"""Static accessors for semantic dataset specifications.

This module provides compatibility functions matching the normalize layer
dataset_specs API. Functions accept an optional SessionContext parameter
for runtime context integration.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.arrow.interop import SchemaLike
    from schema_spec.system import ContractSpec, DatasetSpec
    from semantics.catalog.dataset_rows import SemanticDatasetRow
    from semantics.migrations import MigrationFn


def _strip_version(name: str) -> str:
    """Strip version suffix from a dataset name.

    Parameters
    ----------
    name
        Dataset name potentially containing a version suffix.

    Returns
    -------
    str
        Dataset name without version suffix.
    """
    base, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return base
    return name


def _parse_version(name: str) -> tuple[str, int | None]:
    """Parse a dataset name into (base, version).

    Parameters
    ----------
    name
        Dataset name potentially containing a version suffix.

    Returns
    -------
    tuple[str, int | None]
        Base name and version number when present, otherwise None.
    """
    base, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return base, int(suffix)
    return name, None


@dataclass
class _DatasetSpecCache:
    dataset_rows: dict[str, SemanticDatasetRow] | None = None
    input_schemas: dict[str, SchemaLike] | None = None
    dataset_specs: dict[str, DatasetSpec] | None = None
    dataset_aliases: dict[str, str] | None = None
    aliases_to_name: dict[str, str] | None = None


_CACHE = _DatasetSpecCache()


def _get_all_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Import and return all dataset rows to avoid circular import.

    Returns
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

    Returns
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

    Returns
    -------
    SchemaLike
        Built input schema.
    """
    from semantics.catalog.spec_builder import build_input_schema

    return build_input_schema(row)


def _get_dataset_rows_map() -> dict[str, SemanticDatasetRow]:
    """Return the lazily-initialized dataset rows mapping.

    Returns
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

    Returns
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

    Returns
    -------
    dict[str, DatasetSpec]
        Mapping from dataset name to DatasetSpec.
    """
    dataset_specs = _CACHE.dataset_specs
    if dataset_specs is None:
        dataset_specs = {row.name: _build_spec(row) for row in _get_all_dataset_rows()}
        _CACHE.dataset_specs = dataset_specs
    return dataset_specs


def _get_alias_maps() -> tuple[dict[str, str], dict[str, str]]:
    """Return the lazily-initialized alias mappings.

    Returns
    -------
    tuple[dict[str, str], dict[str, str]]
        Tuple of (name_to_alias, alias_to_name) mappings.

    Raises
    ------
    ValueError
        Raised when duplicate aliases are detected in the registry.
    """
    dataset_aliases = _CACHE.dataset_aliases
    aliases_to_name = _CACHE.aliases_to_name
    if dataset_aliases is None or aliases_to_name is None:
        from schema_spec.dataset_spec_ops import dataset_spec_contract_spec_or_default
        from semantics.migrations import migration_for, migration_skeleton
        from semantics.schema_diff import diff_contract_specs

        aliases: dict[str, str] = {}
        reverse: dict[str, str] = {}
        grouped: dict[str, list[tuple[str, int | None]]] = {}
        for row in _get_all_dataset_rows():
            alias = _strip_version(row.name)
            _, version = _parse_version(row.name)
            grouped.setdefault(alias, []).append((row.name, version))

        for alias, entries in grouped.items():
            if len(entries) == 1:
                name, _version = entries[0]
                aliases[name] = alias
                reverse[alias] = name
                continue

            if any(version is None for _name, version in entries):
                msg = f"Duplicate semantic dataset alias with unversioned name: {alias!r}."
                raise ValueError(msg)

            latest_name, _latest_version = max(entries, key=lambda item: item[1] or 0)
            latest_contract = dataset_spec_contract_spec_or_default(dataset_spec(latest_name))
            for name, _version in entries:
                aliases[name] = alias
                if name == latest_name:
                    continue
                if migration_for(name, latest_name) is not None:
                    continue
                diff = diff_contract_specs(
                    dataset_spec_contract_spec_or_default(dataset_spec(name)),
                    latest_contract,
                )
                if not diff.is_breaking:
                    continue
                diff_lines = diff.summary_lines() or ("no schema changes detected",)
                diff_summary = "\n".join(f"- {line}" for line in diff_lines)
                skeleton = migration_skeleton(name, latest_name, diff)
                msg = (
                    f"Missing migration from {name!r} to {latest_name!r} "
                    f"for dataset alias {alias!r}.\n"
                    f"Schema diff:\n{diff_summary}\n\n"
                    f"Suggested skeleton:\n{skeleton}"
                )
                raise ValueError(msg)
            reverse[alias] = latest_name

        dataset_aliases = aliases
        aliases_to_name = reverse
        _CACHE.dataset_aliases = dataset_aliases
        _CACHE.aliases_to_name = aliases_to_name
    return dataset_aliases, aliases_to_name


def dataset_spec(name: str, ctx: SessionContext | None = None) -> DatasetSpec:
    """Return a DatasetSpec by name.

    Parameters
    ----------
    name
        Dataset name to retrieve.
    ctx
        Optional DataFusion session context for runtime enrichment.

    Returns
    -------
    DatasetSpec
        Registered dataset spec.

    Raises
    ------
    KeyError
        Raised when the dataset name is not found.
    """
    # ctx reserved for future runtime enrichment
    _ = ctx
    specs = _get_dataset_specs()
    if name not in specs:
        msg = f"Unknown semantic dataset: {name!r}."
        raise KeyError(msg)
    return specs[name]


def maybe_dataset_spec(name: str, ctx: SessionContext | None = None) -> DatasetSpec | None:
    """Return a DatasetSpec by name when available.

    Returns
    -------
    DatasetSpec | None
        Registered dataset spec when present, otherwise ``None``.
    """
    try:
        return dataset_spec(name, ctx=ctx)
    except KeyError:
        return None


def dataset_specs() -> Iterable[DatasetSpec]:
    """Return all semantic dataset specs.

    Returns
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

    Returns
    -------
    SchemaLike
        Dataset schema with metadata.
    """
    from schema_spec.dataset_spec_ops import dataset_spec_schema

    spec = dataset_spec(name)
    return dataset_spec_schema(spec)


def dataset_input_schema(name: str) -> SchemaLike:
    """Return the input schema for a semantic dataset.

    The input schema represents the expected schema from upstream sources
    before transformations are applied.

    Parameters
    ----------
    name
        Dataset name to retrieve.

    Returns
    -------
    SchemaLike
        Input schema for plan sources.

    Raises
    ------
    KeyError
        Raised when the dataset name is not found.
    """
    schemas = _get_input_schemas()
    if name not in schemas:
        msg = f"Unknown semantic dataset: {name!r}."
        raise KeyError(msg)
    return schemas[name]


def dataset_names() -> tuple[str, ...]:
    """Return semantic dataset names in registry order.

    Returns
    -------
    tuple[str, ...]
        Dataset names.
    """
    return tuple(row.name for row in _get_all_dataset_rows())


def dataset_name_from_alias(alias: str) -> str:
    """Return the dataset name for a canonical alias.

    Parameters
    ----------
    alias
        Alias to resolve.

    Returns
    -------
    str
        Versioned dataset name.

    Raises
    ------
    KeyError
        Raised when the dataset alias is unknown.
    """
    name_to_alias, alias_to_name = _get_alias_maps()
    name = alias_to_name.get(alias)
    if name is not None:
        return name
    # Check if alias is actually a name
    if alias in name_to_alias:
        return alias
    msg = f"Unknown semantic dataset alias: {alias!r}."
    raise KeyError(msg)


def dataset_alias(name: str) -> str:
    """Return the canonical alias for a dataset name.

    Parameters
    ----------
    name
        Dataset name to resolve.

    Returns
    -------
    str
        Dataset alias used in pipeline wiring.

    Raises
    ------
    KeyError
        Raised when the dataset name is unknown.
    """
    name_to_alias, alias_to_name = _get_alias_maps()
    alias = name_to_alias.get(name)
    if alias is not None:
        return alias
    # Check if name is actually an alias
    if name in alias_to_name:
        return name
    msg = f"Unknown semantic dataset: {name!r}."
    raise KeyError(msg)


def dataset_migration(source: str, target: str) -> MigrationFn | None:
    """Return a migration function for the given dataset pair.

    Returns
    -------
    MigrationFn | None
        Migration function when registered, otherwise ``None``.
    """
    from semantics.migrations import migration_for

    return migration_for(source, target)


def dataset_contract(
    name: str,
    ctx: SessionContext | None = None,
) -> ContractSpec:
    """Return the ContractSpec for a semantic dataset.

    Parameters
    ----------
    name
        Dataset name to retrieve.
    ctx
        Optional DataFusion session context for runtime enrichment.

    Returns
    -------
    ContractSpec
        Contract specification for the dataset.
    """
    from schema_spec.dataset_spec_ops import dataset_spec_contract_spec_or_default

    spec = dataset_spec(name, ctx=ctx)
    return dataset_spec_contract_spec_or_default(spec)


def dataset_contract_schema(name: str) -> SchemaLike:
    """Return the contract schema for a semantic dataset.

    Parameters
    ----------
    name
        Dataset name to retrieve.

    Returns
    -------
    SchemaLike
        Arrow schema defined by the dataset contract.
    """
    contract = dataset_contract(name)
    return contract.to_contract().schema


def dataset_merge_keys(name: str) -> tuple[str, ...]:
    """Return merge keys for a semantic dataset.

    Merge keys are used for Delta Lake merge operations during
    incremental processing.

    Parameters
    ----------
    name
        Dataset name to retrieve.

    Returns
    -------
    tuple[str, ...]
        Merge key column names, or empty tuple if merge is not supported.

    Raises
    ------
    KeyError
        Raised when the dataset name is not found.
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

    A dataset supports incremental processing when it has CDF enabled
    and merge keys defined.

    Parameters
    ----------
    name
        Dataset name to check.

    Returns
    -------
    bool
        True if the dataset supports incremental processing.

    Raises
    ------
    KeyError
        Raised when the dataset name is not found.
    """
    rows = _get_dataset_rows_map()
    if name not in rows:
        msg = f"Unknown semantic dataset: {name!r}."
        raise KeyError(msg)
    row = rows[name]
    return row.supports_cdf and row.merge_keys is not None


__all__ = [
    "dataset_alias",
    "dataset_contract",
    "dataset_contract_schema",
    "dataset_input_schema",
    "dataset_merge_keys",
    "dataset_migration",
    "dataset_name_from_alias",
    "dataset_names",
    "dataset_schema",
    "dataset_spec",
    "dataset_specs",
    "supports_incremental",
]
