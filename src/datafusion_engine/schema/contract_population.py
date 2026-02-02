"""Contract auto-population from catalog schemas.

This module provides utilities for automatically deriving ContractSpec instances
from PyArrow schemas and DataFusion registered tables. Contract population
reduces manual contract definition by inferring constraints from schema metadata
and field nullability.

Schema constraints are inferred from:
- Field nullability: non-nullable fields become required_non_null
- Arrow metadata: KEY_FIELDS_META and REQUIRED_NON_NULL_META
- Optional heuristics for key field inference from naming patterns

Example
-------
>>> from datafusion import SessionContext
>>> from datafusion_engine.schema.contract_population import (
...     populate_contract_from_table,
...     ContractPopulationOptions,
... )
>>> ctx = SessionContext()
>>> ctx.register_parquet("my_table", "data.parquet")
>>> contract = populate_contract_from_table(
...     ctx,
...     "my_table",
...     options=ContractPopulationOptions(infer_key_fields=True),
... )
"""

from __future__ import annotations

import contextlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.arrow.metadata import (
    schema_constraints_from_metadata,
    schema_identity_from_metadata,
)
from datafusion_engine.schema.introspection import schema_from_table
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import (
    ContractCatalogSpec,
    ContractSpec,
    DedupeSpecSpec,
    SortKeySpec,
    make_contract_spec,
)

if TYPE_CHECKING:
    from datafusion import SessionContext


# Common key field name patterns for heuristic inference
_KEY_FIELD_PATTERNS: tuple[str, ...] = (
    "id",
    "pk",
    "key",
    "uuid",
    "guid",
)

_KEY_FIELD_SUFFIXES: tuple[str, ...] = (
    "_id",
    "_pk",
    "_key",
    "_uuid",
    "_guid",
)


def _decode_field_metadata(metadata: dict[bytes, bytes] | None) -> dict[str, str]:
    """Decode Arrow field metadata into a string-keyed mapping.

    Returns
    -------
    dict[str, str]
        Decoded metadata mapping.
    """
    if not metadata:
        return {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in metadata.items()
    }


def _field_spec_from_arrow_field(arrow_field: pa.Field) -> FieldSpec:
    """Build a FieldSpec from an Arrow field.

    Parameters
    ----------
    arrow_field
        Arrow field to convert.

    Returns
    -------
    FieldSpec
        Field specification derived from the Arrow field.
    """
    metadata = _decode_field_metadata(arrow_field.metadata)
    encoding_value = metadata.get("encoding")
    encoding = "dictionary" if encoding_value == "dictionary" else None
    return FieldSpec(
        name=arrow_field.name,
        dtype=arrow_field.type,
        nullable=arrow_field.nullable,
        metadata=metadata,
        default_value=metadata.get("default_value"),
        encoding=encoding,
    )


def _infer_required_non_null_from_fields(
    schema: pa.Schema,
    *,
    exclude_key_fields: Sequence[str] = (),
) -> tuple[str, ...]:
    """Infer required non-null fields from non-nullable schema fields.

    Parameters
    ----------
    schema
        Arrow schema to analyze.
    exclude_key_fields
        Key fields to exclude from required_non_null (they are implicitly required).

    Returns
    -------
    tuple[str, ...]
        Non-nullable field names excluding key fields.
    """
    exclude_set = set(exclude_key_fields)
    return tuple(f.name for f in schema if not f.nullable and f.name not in exclude_set)


def _merge_field_names(*sources: Sequence[str]) -> tuple[str, ...]:
    """Merge field name sequences preserving order and removing duplicates.

    Returns
    -------
    tuple[str, ...]
        Merged field names without duplicates.
    """
    seen: set[str] = set()
    merged: list[str] = []
    for source in sources:
        for name in source:
            if name not in seen:
                merged.append(name)
                seen.add(name)
    return tuple(merged)


def _infer_key_fields(
    field_names: Sequence[str],
    patterns: tuple[str, ...],
    suffixes: tuple[str, ...],
) -> tuple[str, ...]:
    """Infer key fields based on naming patterns and suffixes.

    Parameters
    ----------
    field_names
        Field names to analyze.
    patterns
        Exact match patterns for key field names.
    suffixes
        Suffix patterns for key field names.

    Returns
    -------
    tuple[str, ...]
        Inferred key field names.
    """
    candidates: list[str] = []
    for field_name in field_names:
        lower_name = field_name.lower()
        if lower_name in patterns:
            candidates.append(field_name)
            continue
        if any(lower_name.endswith(suffix) for suffix in suffixes):
            candidates.append(field_name)
    return tuple(candidates)


def _is_semantic_output_name(table_name: str) -> bool:
    try:
        from semantics.registry import SEMANTIC_MODEL
    except (ImportError, RuntimeError, TypeError, ValueError):
        return False
    output_names = {spec.name for spec in SEMANTIC_MODEL.outputs}
    if table_name in output_names:
        return True
    try:
        from semantics.catalog.dataset_specs import dataset_name_from_alias
    except (ImportError, RuntimeError, TypeError, ValueError):
        return False
    with contextlib.suppress(KeyError):
        return dataset_name_from_alias(table_name) in output_names
    return False


def _resolve_version(identity: dict[str, str], default: int) -> int:
    """Resolve version from identity metadata or use default.

    Parameters
    ----------
    identity
        Schema identity metadata.
    default
        Default version to use.

    Returns
    -------
    int
        Resolved version number.
    """
    if "version" not in identity:
        return default
    try:
        return int(identity["version"])
    except (ValueError, TypeError):
        return default


def _build_contract_extras(
    key_fields: tuple[str, ...],
    options: ContractPopulationOptions,
) -> tuple[DedupeSpecSpec | None, tuple[SortKeySpec, ...]]:
    """Build optional dedupe spec and canonical sort from key fields.

    Parameters
    ----------
    key_fields
        Resolved key field names.
    options
        Population options.

    Returns
    -------
    tuple[DedupeSpecSpec | None, tuple[SortKeySpec, ...]]
        Dedupe spec and canonical sort tuple.
    """
    dedupe: DedupeSpecSpec | None = None
    if options.infer_dedupe and key_fields:
        dedupe = DedupeSpecSpec(keys=key_fields)

    canonical_sort: tuple[SortKeySpec, ...] = ()
    if options.include_canonical_sort and key_fields:
        canonical_sort = tuple(SortKeySpec(column=col, order="ascending") for col in key_fields)
    return dedupe, canonical_sort


def _resolve_key_fields(
    field_names: Sequence[str],
    metadata_key_fields: tuple[str, ...],
    options: ContractPopulationOptions,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Resolve key fields from metadata and/or inference.

    Parameters
    ----------
    field_names
        Schema field names.
    metadata_key_fields
        Key fields from schema metadata.
    options
        Population options.

    Returns
    -------
    tuple[tuple[str, ...], tuple[str, ...]]
        Tuple of (merged key fields, inferred key fields).
    """
    inferred: tuple[str, ...] = ()
    if options.infer_key_fields and not metadata_key_fields:
        all_patterns = _KEY_FIELD_PATTERNS + options.key_field_patterns
        all_suffixes = _KEY_FIELD_SUFFIXES + options.key_field_suffixes
        inferred = _infer_key_fields(field_names, all_patterns, all_suffixes)
    merged = _merge_field_names(metadata_key_fields, inferred)
    return merged, inferred


def _resolve_required_non_null(
    schema: pa.Schema,
    metadata_required: tuple[str, ...],
    key_fields: tuple[str, ...],
    options: ContractPopulationOptions,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Resolve required non-null fields from metadata and/or inference.

    Parameters
    ----------
    schema
        Arrow schema.
    metadata_required
        Required fields from schema metadata.
    key_fields
        Key fields (excluded from inference).
    options
        Population options.

    Returns
    -------
    tuple[tuple[str, ...], tuple[str, ...]]
        Tuple of (merged required, inferred required).
    """
    inferred: tuple[str, ...] = ()
    if options.infer_required_non_null:
        inferred = _infer_required_non_null_from_fields(schema, exclude_key_fields=key_fields)
    merged = _merge_field_names(metadata_required, inferred)
    return merged, inferred


@dataclass(frozen=True)
class ContractPopulationOptions:
    """Options controlling contract auto-population behavior.

    Attributes
    ----------
    infer_required_non_null
        Infer required_non_null from non-nullable fields. Default True.
    infer_key_fields
        Infer key_fields from naming patterns when not in metadata. Default True.
    infer_dedupe
        Infer dedupe spec from key fields. Default False.
    default_version
        Default contract version when not in schema metadata. Default 1.
    include_canonical_sort
        Generate canonical_sort from key fields. Default False.
    key_field_patterns
        Additional patterns to match for key field inference.
    key_field_suffixes
        Additional suffixes to match for key field inference.
    """

    infer_required_non_null: bool = True
    infer_key_fields: bool = True
    infer_dedupe: bool = False
    default_version: int = 1
    include_canonical_sort: bool = False
    key_field_patterns: tuple[str, ...] = ()
    key_field_suffixes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ContractPopulationResult:
    """Result of contract population with diagnostic metadata.

    Attributes
    ----------
    contract
        Populated ContractSpec instance.
    inferred_key_fields
        Key fields that were inferred rather than from metadata.
    inferred_required
        Required fields that were inferred from nullability.
    metadata_key_fields
        Key fields sourced from schema metadata.
    metadata_required
        Required fields sourced from schema metadata.
    """

    contract: ContractSpec
    inferred_key_fields: tuple[str, ...] = ()
    inferred_required: tuple[str, ...] = ()
    metadata_key_fields: tuple[str, ...] = ()
    metadata_required: tuple[str, ...] = ()


def populate_contract_from_schema(
    name: str,
    schema: pa.Schema,
    *,
    options: ContractPopulationOptions | None = None,
) -> ContractSpec:
    """Populate a ContractSpec from a PyArrow schema.

    Extract constraints from schema metadata and optionally infer additional
    constraints from field nullability and naming patterns.

    Parameters
    ----------
    name
        Contract/table name for the generated spec.
    schema
        PyArrow schema to derive the contract from.
    options
        Population options controlling inference behavior.

    Returns
    -------
    ContractSpec
        Contract specification derived from the schema.
    """
    result = populate_contract_from_schema_detailed(name, schema, options=options)
    return result.contract


def populate_contract_from_schema_detailed(
    name: str,
    schema: pa.Schema,
    *,
    options: ContractPopulationOptions | None = None,
) -> ContractPopulationResult:
    """Populate a ContractSpec from a schema with detailed provenance.

    Parameters
    ----------
    name
        Contract/table name for the generated spec.
    schema
        PyArrow schema to derive the contract from.
    options
        Population options controlling inference behavior.

    Returns
    -------
    ContractPopulationResult
        Result containing contract and inference metadata.
    """
    if options is None:
        options = ContractPopulationOptions()

    # Extract constraints from Arrow metadata
    metadata_required, metadata_key_fields = schema_constraints_from_metadata(schema.metadata)
    identity = schema_identity_from_metadata(schema.metadata)

    # Determine version and name
    version = _resolve_version(identity, options.default_version)
    resolved_name = identity.get("name", name) or name

    # Build field specs from schema
    fields = [_field_spec_from_arrow_field(f) for f in schema]

    # Resolve key fields
    key_fields, inferred_key_fields = _resolve_key_fields(
        [f.name for f in fields], metadata_key_fields, options
    )

    # Resolve required non-null
    required_non_null, inferred_required = _resolve_required_non_null(
        schema, metadata_required, key_fields, options
    )

    # Build TableSchemaSpec and contract
    table_spec = TableSchemaSpec(
        name=resolved_name,
        version=version,
        fields=fields,
        required_non_null=required_non_null,
        key_fields=key_fields,
    )
    dedupe, canonical_sort = _build_contract_extras(key_fields, options)
    contract = make_contract_spec(
        table_spec=table_spec,
        dedupe=dedupe,
        canonical_sort=canonical_sort,
        version=version,
    )

    return ContractPopulationResult(
        contract=contract,
        inferred_key_fields=inferred_key_fields,
        inferred_required=inferred_required,
        metadata_key_fields=metadata_key_fields,
        metadata_required=metadata_required,
    )


def populate_contract_from_table(
    ctx: SessionContext,
    table_name: str,
    *,
    options: ContractPopulationOptions | None = None,
) -> ContractSpec:
    """Populate a ContractSpec from a DataFusion registered table.

    Retrieve the schema from the DataFusion catalog and derive a contract
    specification with constraints inferred from schema metadata and
    field nullability.

    Parameters
    ----------
    ctx
        DataFusion session context with the table registered.
    table_name
        Name of the registered table.
    options
        Population options controlling inference behavior.

    Returns
    -------
    ContractSpec
        Contract specification derived from the table schema.

    Raises
    ------
    ValueError
        Raised when attempting to populate contracts for semantic outputs.
    """
    if _is_semantic_output_name(table_name):
        msg = (
            "Contract population from catalog schemas is disabled for semantic outputs; "
            "use IR-driven schema specs instead."
        )
        raise ValueError(msg)
    schema = schema_from_table(ctx, table_name)
    return populate_contract_from_schema(table_name, schema, options=options)


def populate_contract_from_table_detailed(
    ctx: SessionContext,
    table_name: str,
    *,
    options: ContractPopulationOptions | None = None,
) -> ContractPopulationResult:
    """Populate a ContractSpec from a table with detailed provenance.

    Parameters
    ----------
    ctx
        DataFusion session context with the table registered.
    table_name
        Name of the registered table.
    options
        Population options controlling inference behavior.

    Returns
    -------
    ContractPopulationResult
        Result containing contract and inference metadata.

    Raises
    ------
    ValueError
        Raised when attempting to populate contracts for semantic outputs.
    """
    if _is_semantic_output_name(table_name):
        msg = (
            "Contract population from catalog schemas is disabled for semantic outputs; "
            "use IR-driven schema specs instead."
        )
        raise ValueError(msg)
    schema = schema_from_table(ctx, table_name)
    return populate_contract_from_schema_detailed(table_name, schema, options=options)


@dataclass
class ContractCatalogPopulator:
    """Batch populator for contract catalogs from DataFusion tables.

    Use this class to generate ContractCatalogSpec instances from multiple
    registered tables with consistent population options.

    Parameters
    ----------
    ctx
        DataFusion session context.
    options
        Default population options for all tables.
    table_options
        Per-table option overrides keyed by table name.

    Example
    -------
    >>> populator = ContractCatalogPopulator(ctx, options)
    >>> catalog = populator.populate_for_tables(["table_a", "table_b"])
    """

    ctx: SessionContext
    options: ContractPopulationOptions = field(default_factory=ContractPopulationOptions)
    table_options: dict[str, ContractPopulationOptions] = field(default_factory=dict)
    _results: dict[str, ContractPopulationResult] = field(default_factory=dict, repr=False)

    def populate_for_table(self, table_name: str) -> ContractSpec:
        """Populate a contract for a single table.

        Parameters
        ----------
        table_name
            Name of the registered table.

        Returns
        -------
        ContractSpec
            Contract specification for the table.
        """
        opts = self.table_options.get(table_name, self.options)
        result = populate_contract_from_table_detailed(self.ctx, table_name, options=opts)
        self._results[table_name] = result
        return result.contract

    def populate_for_tables(self, table_names: Sequence[str]) -> ContractCatalogSpec:
        """Populate contracts for multiple tables.

        Parameters
        ----------
        table_names
            Names of registered tables to populate contracts for.

        Returns
        -------
        ContractCatalogSpec
            Catalog of contract specifications keyed by table name.
        """
        contracts: dict[str, ContractSpec] = {}
        for table_name in table_names:
            contracts[table_name] = self.populate_for_table(table_name)
        return ContractCatalogSpec(contracts=contracts)

    def populate_all_tables(self) -> ContractCatalogSpec:
        """Populate contracts for all tables in the session context.

        Queries the DataFusion catalog for registered table names and
        populates contracts for each.

        Returns
        -------
        ContractCatalogSpec
            Catalog of contract specifications for all tables.
        """
        from datafusion_engine.schema.introspection import table_names_snapshot

        table_names = table_names_snapshot(self.ctx)
        # Filter out information_schema tables
        filtered = [
            name for name in sorted(table_names) if not name.startswith("information_schema.")
        ]
        return self.populate_for_tables(filtered)

    def results(self) -> dict[str, ContractPopulationResult]:
        """Return population results with inference metadata.

        Returns
        -------
        dict[str, ContractPopulationResult]
            Mapping of table names to detailed population results.
        """
        return dict(self._results)

    def clear_results(self) -> None:
        """Clear cached population results."""
        self._results.clear()


__all__ = [
    "ContractCatalogPopulator",
    "ContractPopulationOptions",
    "ContractPopulationResult",
    "populate_contract_from_schema",
    "populate_contract_from_schema_detailed",
    "populate_contract_from_table",
    "populate_contract_from_table_detailed",
]
