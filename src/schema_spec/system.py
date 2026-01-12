"""Schema system registry, factories, and Arrow validation integration."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, TypedDict, Unpack

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.finalize.finalize import Contract, FinalizeContext
from arrowdsl.plan.ops import DedupeSpec, SortKey
from arrowdsl.plan.query import ProjectionSpec, QuerySpec, ScanContext
from arrowdsl.schema.schema import (
    CastErrorPolicy,
    SchemaEvolutionSpec,
    SchemaMetadataSpec,
    SchemaTransform,
)
from arrowdsl.schema.validation import ArrowValidationOptions, validate_table
from schema_spec.specs import ArrowFieldSpec, FieldBundle, TableSchemaSpec

if TYPE_CHECKING:
    import pyarrow.dataset as ds


def validate_arrow_table(
    table: TableLike,
    *,
    spec: TableSchemaSpec,
    options: ArrowValidationOptions | None = None,
) -> TableLike:
    """Validate an Arrow table with Arrow-native validation.

    Returns
    -------
    TableLike
        Validated table.

    Raises
    ------
    ValueError
        Raised when strict validation fails.
    """
    options = options or ArrowValidationOptions()
    report = validate_table(table, spec=spec, options=options)
    if options.strict is True and not report.valid:
        msg = f"Arrow validation failed for {spec.name!r}."
        raise ValueError(msg)
    return report.validated


class SortKeySpec(BaseModel):
    """Sort key specification."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    column: str
    order: Literal["ascending", "descending"] = "ascending"

    def to_sort_key(self) -> SortKey:
        """Convert to a SortKey instance.

        Returns
        -------
        SortKey
            Sort key instance.
        """
        return SortKey(column=self.column, order=self.order)


class DedupeSpecSpec(BaseModel):
    """Dedupe specification."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    keys: tuple[str, ...]
    tie_breakers: tuple[SortKeySpec, ...] = ()
    strategy: Literal[
        "KEEP_FIRST_AFTER_SORT",
        "KEEP_BEST_BY_SCORE",
        "COLLAPSE_LIST",
        "KEEP_ARBITRARY",
    ] = "KEEP_FIRST_AFTER_SORT"

    def to_dedupe_spec(self) -> DedupeSpec:
        """Convert to a DedupeSpec instance.

        Returns
        -------
        DedupeSpec
            Dedupe spec instance.
        """
        return DedupeSpec(
            keys=self.keys,
            tie_breakers=tuple(tb.to_sort_key() for tb in self.tie_breakers),
            strategy=self.strategy,
        )


class ContractSpec(BaseModel):
    """Output contract specification."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    name: str
    table_schema: TableSchemaSpec

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    version: int | None = None

    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None

    @field_validator("virtual_field_docs")
    @classmethod
    def _docs_match_virtual_fields(
        cls, value: dict[str, str] | None, info: ValidationInfo
    ) -> dict[str, str] | None:
        if value is None:
            return None
        virtual_fields: tuple[str, ...] = info.data.get("virtual_fields", ())
        missing = [key for key in value if key not in virtual_fields]
        if missing:
            msg = f"virtual_field_docs keys missing in virtual_fields: {missing}"
            raise ValueError(msg)
        return value

    def to_contract(self) -> Contract:
        """Convert to a runtime Contract.

        Returns
        -------
        Contract
            Runtime contract instance.
        """
        version = self.version if self.version is not None else self.table_schema.version
        return Contract(
            name=self.name,
            schema=self.table_schema.to_arrow_schema(),
            schema_spec=self.table_schema,
            key_fields=self.table_schema.key_fields,
            required_non_null=self.table_schema.required_non_null,
            dedupe=self.dedupe.to_dedupe_spec() if self.dedupe is not None else None,
            canonical_sort=tuple(sk.to_sort_key() for sk in self.canonical_sort),
            version=version,
            virtual_fields=self.virtual_fields,
            virtual_field_docs=self.virtual_field_docs,
            validation=self.validation,
        )


@dataclass(frozen=True)
class DatasetSpec:
    """Unified dataset spec with schema, contract, and query behavior."""

    table_spec: TableSchemaSpec
    contract_spec: ContractSpec | None = None
    query_spec: QuerySpec | None = None
    evolution_spec: SchemaEvolutionSpec = field(default_factory=SchemaEvolutionSpec)
    metadata_spec: SchemaMetadataSpec = field(default_factory=SchemaMetadataSpec)
    validation: ArrowValidationOptions | None = None

    @property
    def name(self) -> str:
        """Return the dataset name.

        Returns
        -------
        str
            Dataset name.
        """
        return self.table_spec.name

    def schema(self) -> SchemaLike:
        """Return the Arrow schema with dataset metadata applied.

        Returns
        -------
        SchemaLike
            Arrow schema with metadata.
        """
        return self.metadata_spec.apply(self.table_spec.to_arrow_schema())

    def query(self) -> QuerySpec:
        """Return the query spec, deriving it from the table spec if needed.

        Returns
        -------
        QuerySpec
            Query spec for scanning.
        """
        if self.query_spec is not None:
            return self.query_spec
        cols = tuple(field.name for field in self.table_spec.fields)
        return QuerySpec(projection=ProjectionSpec(base=cols))

    def contract_spec_or_default(self) -> ContractSpec:
        """Return the contract spec, deriving a default when missing.

        Returns
        -------
        ContractSpec
            Contract spec with validation applied.
        """
        if self.contract_spec is not None:
            if self.validation is None or self.contract_spec.validation is not None:
                return self.contract_spec
            return self.contract_spec.model_copy(update={"validation": self.validation})
        return ContractSpec(
            name=self.table_spec.name,
            table_schema=self.table_spec,
            version=self.table_spec.version,
            validation=self.validation,
        )

    def contract(self) -> Contract:
        """Return a runtime contract derived from the contract spec.

        Returns
        -------
        Contract
            Runtime contract instance.
        """
        return self.contract_spec_or_default().to_contract()

    def scan_context(self, dataset: ds.Dataset, ctx: ExecutionContext) -> ScanContext:
        """Return a ScanContext for dataset scanning.

        Returns
        -------
        ScanContext
            Scan context configured for this dataset.
        """
        return ScanContext(dataset=dataset, spec=self.query(), ctx=ctx)

    def finalize_context(self, ctx: ExecutionContext) -> FinalizeContext:
        """Return a FinalizeContext for this dataset spec.

        Returns
        -------
        FinalizeContext
            Finalize context configured for this dataset.
        """
        transform = SchemaTransform(
            schema=self.schema(),
            safe_cast=ctx.safe_cast,
            keep_extra_columns=ctx.provenance,
            on_error="unsafe" if ctx.safe_cast else "raise",
        )
        return FinalizeContext(contract=self.contract(), transform=transform)

    def unify_tables(
        self,
        tables: Sequence[TableLike],
        ctx: ExecutionContext | None = None,
    ) -> TableLike:
        """Unify table schemas using the evolution spec and execution context.

        Returns
        -------
        TableLike
            Unified table with aligned schema.
        """
        safe_cast = True
        keep_extra_columns = False
        on_error: CastErrorPolicy = "unsafe"
        if ctx is not None:
            safe_cast = ctx.safe_cast
            keep_extra_columns = ctx.provenance
            on_error = "unsafe" if ctx.safe_cast else "raise"
        return self.evolution_spec.unify_and_cast(
            tables,
            safe_cast=safe_cast,
            on_error=on_error,
            keep_extra_columns=keep_extra_columns,
        )


@dataclass(frozen=True)
class TableSpecConstraints:
    """Required and key fields for a table schema."""

    required_non_null: Iterable[str] = ()
    key_fields: Iterable[str] = ()


@dataclass(frozen=True)
class VirtualFieldSpec:
    """Virtual field names and documentation for a contract."""

    fields: tuple[str, ...] = ()
    docs: Mapping[str, str] | None = None


class ContractSpecKwargs(TypedDict, total=False):
    """Keyword arguments supported by make_contract_spec."""

    dedupe: DedupeSpecSpec | None
    canonical_sort: Iterable[SortKeySpec]
    virtual: VirtualFieldSpec | None
    version: int | None
    validation: ArrowValidationOptions | None


class DatasetSpecKwargs(TypedDict, total=False):
    """Keyword arguments supported by make_dataset_spec."""

    contract_spec: ContractSpec | None
    query_spec: QuerySpec | None
    evolution_spec: SchemaEvolutionSpec | None
    metadata_spec: SchemaMetadataSpec | None
    validation: ArrowValidationOptions | None


def _merge_names(*parts: Iterable[str]) -> tuple[str, ...]:
    merged: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for name in part:
            if name in seen:
                continue
            merged.append(name)
            seen.add(name)
    return tuple(merged)


def _merge_fields(
    bundles: Iterable[FieldBundle],
    fields: Iterable[ArrowFieldSpec],
) -> list[ArrowFieldSpec]:
    merged: list[ArrowFieldSpec] = []
    for bundle in bundles:
        merged.extend(bundle.fields)
    merged.extend(fields)
    return merged


def make_table_spec(
    name: str,
    *,
    version: int | None,
    bundles: Iterable[FieldBundle],
    fields: Iterable[ArrowFieldSpec],
    constraints: TableSpecConstraints | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from field bundles and explicit fields.

    Returns
    -------
    TableSchemaSpec
        Table schema specification.
    """
    if constraints is None:
        constraints = TableSpecConstraints()
    bundle_required = (bundle.required_non_null for bundle in bundles)
    bundle_keys = (bundle.key_fields for bundle in bundles)
    return TableSchemaSpec(
        name=name,
        version=version,
        fields=_merge_fields(bundles, fields),
        required_non_null=_merge_names(*bundle_required, constraints.required_non_null),
        key_fields=_merge_names(*bundle_keys, constraints.key_fields),
    )


def make_contract_spec(
    *,
    table_spec: TableSchemaSpec,
    **kwargs: Unpack[ContractSpecKwargs],
) -> ContractSpec:
    """Create a ContractSpec from a TableSchemaSpec.

    Returns
    -------
    ContractSpec
        Contract specification.
    """
    dedupe = kwargs.get("dedupe")
    canonical_sort = kwargs.get("canonical_sort", ())
    virtual = kwargs.get("virtual")
    version = kwargs.get("version")
    validation = kwargs.get("validation")
    virtual_fields = virtual.fields if virtual is not None else ()
    virtual_docs = dict(virtual.docs) if virtual is not None and virtual.docs is not None else None
    return ContractSpec(
        name=table_spec.name,
        table_schema=table_spec,
        dedupe=dedupe,
        canonical_sort=tuple(canonical_sort),
        version=version if version is not None else table_spec.version,
        virtual_fields=virtual_fields,
        virtual_field_docs=virtual_docs,
        validation=validation,
    )


def make_dataset_spec(
    *,
    table_spec: TableSchemaSpec,
    **kwargs: Unpack[DatasetSpecKwargs],
) -> DatasetSpec:
    """Create a DatasetSpec from schema, contract, and query settings.

    Returns
    -------
    DatasetSpec
        Dataset specification bundling schema, contract, and query behavior.
    """
    contract_spec = kwargs.get("contract_spec")
    query_spec = kwargs.get("query_spec")
    evolution_spec = kwargs.get("evolution_spec")
    metadata_spec = kwargs.get("metadata_spec")
    validation = kwargs.get("validation")
    evolution = evolution_spec or SchemaEvolutionSpec()
    metadata = metadata_spec or SchemaMetadataSpec()
    return DatasetSpec(
        table_spec=table_spec,
        contract_spec=contract_spec,
        query_spec=query_spec,
        evolution_spec=evolution,
        metadata_spec=metadata,
        validation=validation,
    )


def _sort_key_specs(keys: Iterable[SortKey]) -> tuple[SortKeySpec, ...]:
    """Convert SortKey objects into SortKeySpec values.

    Returns
    -------
    tuple[SortKeySpec, ...]
        SortKeySpec values for the keys.
    """
    return tuple(SortKeySpec(column=key.column, order=key.order) for key in keys)


def _dedupe_spec_spec(dedupe: DedupeSpec | None) -> DedupeSpecSpec | None:
    """Convert a DedupeSpec into a DedupeSpecSpec.

    Returns
    -------
    DedupeSpecSpec | None
        Dedupe spec model or ``None``.
    """
    if dedupe is None:
        return None
    return DedupeSpecSpec(
        keys=dedupe.keys,
        tie_breakers=_sort_key_specs(dedupe.tie_breakers),
        strategy=dedupe.strategy,
    )


def _table_spec_from_contract(contract: Contract) -> TableSchemaSpec:
    """Build a TableSchemaSpec from a runtime contract.

    Returns
    -------
    TableSchemaSpec
        Table schema specification derived from the contract.
    """
    if contract.schema_spec is not None:
        return contract.schema_spec
    table_spec = table_spec_from_schema(contract.name, contract.schema, version=contract.version)
    if not contract.required_non_null and not contract.key_fields:
        return table_spec
    return table_spec.model_copy(
        update={
            "required_non_null": contract.required_non_null,
            "key_fields": contract.key_fields,
        }
    )


def dataset_spec_from_contract(contract: Contract) -> DatasetSpec:
    """Create a DatasetSpec aligned to a runtime Contract.

    Returns
    -------
    DatasetSpec
        Dataset spec derived from the contract.
    """
    table_spec = _table_spec_from_contract(contract)
    contract_spec = ContractSpec(
        name=contract.name,
        table_schema=table_spec,
        dedupe=_dedupe_spec_spec(contract.dedupe),
        canonical_sort=_sort_key_specs(contract.canonical_sort),
        version=contract.version,
        virtual_fields=contract.virtual_fields,
        virtual_field_docs=contract.virtual_field_docs,
        validation=contract.validation,
    )
    return make_dataset_spec(table_spec=table_spec, contract_spec=contract_spec)


def _decode_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    if not metadata:
        return {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in metadata.items()
    }


def table_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from a pyarrow.Schema.

    Returns
    -------
    TableSchemaSpec
        Table schema specification.
    """
    fields = [
        ArrowFieldSpec(
            name=field.name,
            dtype=field.type,
            nullable=field.nullable,
            metadata=_decode_metadata(field.metadata),
        )
        for field in schema
    ]
    return TableSchemaSpec(name=name, version=version, fields=fields)


def dataset_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> DatasetSpec:
    """Create a DatasetSpec from an Arrow schema.

    Returns
    -------
    DatasetSpec
        Dataset spec derived from the schema.
    """
    table_spec = table_spec_from_schema(name, schema, version=version)
    return make_dataset_spec(table_spec=table_spec)


@dataclass(frozen=True)
class SchemaRegistry:
    """Registry for dataset specs."""

    dataset_specs: dict[str, DatasetSpec] = field(default_factory=dict)

    def register_dataset(self, spec: DatasetSpec) -> DatasetSpec:
        """Register or return an existing dataset spec.

        Returns
        -------
        DatasetSpec
            Registered dataset spec.
        """
        existing = self.dataset_specs.get(spec.name)
        if existing is not None:
            return existing
        self.dataset_specs[spec.name] = spec
        return spec


GLOBAL_SCHEMA_REGISTRY = SchemaRegistry()


class ContractCatalogSpec(BaseModel):
    """Collection of contract specifications keyed by name."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    contracts: dict[str, ContractSpec]

    @field_validator("contracts")
    @classmethod
    def _names_match(cls, contracts: dict[str, ContractSpec]) -> dict[str, ContractSpec]:
        mismatched = [name for name, spec in contracts.items() if name != spec.name]
        if mismatched:
            msg = f"contract key mismatch: {mismatched}"
            raise ValueError(msg)
        return contracts

    def to_contracts(self) -> dict[str, ContractSpec]:
        """Return a name->spec mapping.

        Returns
        -------
        dict[str, ContractSpec]
            Mapping of contract names to specs.
        """
        return dict(self.contracts)

    def register_into(self, registry: SchemaRegistry) -> SchemaRegistry:
        """Register catalog entries into a schema registry.

        Returns
        -------
        SchemaRegistry
            Updated registry with catalog entries.
        """
        for contract in self.contracts.values():
            registry.register_dataset(
                make_dataset_spec(table_spec=contract.table_schema, contract_spec=contract)
            )
        return registry


__all__ = [
    "GLOBAL_SCHEMA_REGISTRY",
    "ArrowValidationOptions",
    "ContractCatalogSpec",
    "ContractSpec",
    "ContractSpecKwargs",
    "DatasetSpec",
    "DatasetSpecKwargs",
    "DedupeSpecSpec",
    "SchemaRegistry",
    "SortKeySpec",
    "TableSpecConstraints",
    "VirtualFieldSpec",
    "dataset_spec_from_contract",
    "dataset_spec_from_schema",
    "make_contract_spec",
    "make_dataset_spec",
    "make_table_spec",
    "table_spec_from_schema",
    "validate_arrow_table",
]
