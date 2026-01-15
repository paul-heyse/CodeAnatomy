"""Schema system registry, factories, and Arrow validation integration."""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal, Protocol, TypedDict, Unpack, cast

import ibis
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.types as patypes
from ibis.expr.types import Table

from arrowdsl.compute.expr_core import ExprSpec
from arrowdsl.core.context import ExecutionContext, OrderingLevel
from arrowdsl.core.interop import DataTypeLike, SchemaLike, TableLike
from arrowdsl.finalize.finalize import Contract, FinalizeContext
from arrowdsl.plan.ops import DedupeSpec, SortKey
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import PathLike, ProjectionSpec, QuerySpec, open_dataset
from arrowdsl.plan.runner_types import PlanRunResultProto, plan_runner_module
from arrowdsl.schema.metadata import (
    encoding_policy_from_spec,
    merge_metadata_specs,
    metadata_spec_from_schema,
    ordering_metadata_spec,
    schema_constraints_from_metadata,
    schema_identity_from_metadata,
)
from arrowdsl.schema.policy import SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import (
    CastErrorPolicy,
    EncodingPolicy,
    SchemaEvolutionSpec,
    SchemaMetadataSpec,
)
from arrowdsl.schema.validation import (
    ArrowValidationOptions,
    duplicate_key_rows_plan,
    invalid_rows_plan,
    validate_table,
)
from arrowdsl.spec.io import read_spec_table
from schema_spec.specs import (
    ENCODING_DICTIONARY,
    ENCODING_META,
    ArrowFieldSpec,
    DerivedFieldSpec,
    ExternalTableConfig,
    FieldBundle,
    TableSchemaSpec,
)

if TYPE_CHECKING:
    from arrowdsl.plan.scan_io import DatasetSource, PlanSource
    from arrowdsl.spec.tables.schema import SchemaSpecTables


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


@dataclass(frozen=True)
class SortKeySpec:
    """Sort key specification."""

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


@dataclass(frozen=True)
class DedupeSpecSpec:
    """Dedupe specification."""

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


@dataclass(frozen=True)
class DataFusionScanOptions:
    """DataFusion-specific scan configuration."""

    partition_cols: tuple[tuple[str, pa.DataType], ...] = ()
    file_sort_order: tuple[str, ...] = ()
    parquet_pruning: bool = True
    skip_metadata: bool = False
    file_extension: str | None = None
    cache: bool = False


def _ordering_metadata_spec(
    contract_spec: ContractSpec | None,
    table_spec: TableSchemaSpec,
) -> SchemaMetadataSpec | None:
    if contract_spec is not None and contract_spec.canonical_sort:
        keys = tuple((key.column, key.order) for key in contract_spec.canonical_sort)
        return ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=keys)
    if table_spec.key_fields:
        keys = tuple((name, "ascending") for name in table_spec.key_fields)
        return ordering_metadata_spec(OrderingLevel.IMPLICIT, keys=keys)
    return None


@dataclass(frozen=True)
class ContractSpec:
    """Output contract specification."""

    name: str
    table_schema: TableSchemaSpec

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    version: int | None = None

    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None

    def __post_init__(self) -> None:
        """Validate contract spec invariants.

        Raises
        ------
        ValueError
            Raised when virtual field docs reference undefined virtual fields.
        """
        if self.virtual_field_docs is None:
            return
        missing = [key for key in self.virtual_field_docs if key not in self.virtual_fields]
        if missing:
            msg = f"virtual_field_docs keys missing in virtual_fields: {missing}"
            raise ValueError(msg)

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
    datafusion_scan: DataFusionScanOptions | None = None
    derived_fields: tuple[DerivedFieldSpec, ...] = ()
    predicate: ExprSpec | None = None
    pushdown_predicate: ExprSpec | None = None
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
        ordering = _ordering_metadata_spec(self.contract_spec, self.table_spec)
        merged = merge_metadata_specs(self.metadata_spec, ordering)
        return merged.apply(self.table_spec.to_arrow_schema())

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
        derived = {spec.name: spec.expr for spec in self.derived_fields}
        return QuerySpec(
            projection=ProjectionSpec(base=cols, derived=derived),
            predicate=self.predicate,
            pushdown_predicate=self.pushdown_predicate,
        )

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
            return replace(self.contract_spec, validation=self.validation)
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

    def external_table_sql(self, config: ExternalTableConfig) -> str:
        """Return a CREATE EXTERNAL TABLE statement for this dataset.

        Returns
        -------
        str
            CREATE EXTERNAL TABLE statement for this dataset schema.
        """
        return self.table_spec.to_create_external_table_sql(config)

    def _plan_for_validation(self, source: PlanSource, *, ctx: ExecutionContext) -> Plan:
        if isinstance(source, ds.Dataset):
            return _plan_from_dataset(source, spec=self, ctx=ctx)
        if isinstance(source, _dataset_source_class()):
            return _plan_from_dataset(source.dataset, spec=self, ctx=ctx)
        return _plan_from_source(source, ctx=ctx, label=f"{self.name}_validate")

    def validation_plans(self, source: PlanSource, *, ctx: ExecutionContext) -> ValidationPlans:
        """Return plan-lane validation pipelines for invalid rows and duplicate keys.

        Returns
        -------
        ValidationPlans
            Plans for invalid rows and duplicate keys.
        """
        plan = self._plan_for_validation(source, ctx=ctx)
        invalid = invalid_rows_plan(plan, spec=self.table_spec, ctx=ctx)
        dupes = duplicate_key_rows_plan(plan, keys=self.table_spec.key_fields, ctx=ctx)
        return ValidationPlans(invalid_rows=invalid, duplicate_keys=dupes)

    def finalize_context(self, ctx: ExecutionContext) -> FinalizeContext:
        """Return a FinalizeContext for this dataset spec.

        Returns
        -------
        FinalizeContext
            Finalize context configured for this dataset.
        """
        contract = self.contract()
        ordering = _ordering_metadata_spec(self.contract_spec, self.table_spec)
        metadata = merge_metadata_specs(self.metadata_spec, ordering)
        policy = schema_policy_factory(
            self.table_spec,
            ctx=ctx,
            options=SchemaPolicyOptions(
                schema=contract.with_versioned_schema(),
                encoding=self.encoding_policy(),
                metadata=metadata,
                validation=contract.validation,
            ),
        )
        return FinalizeContext(contract=contract, schema_policy=policy)

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

    def encoding_policy(self) -> EncodingPolicy | None:
        """Return an encoding policy derived from the schema spec.

        Returns
        -------
        EncodingPolicy | None
            Encoding policy when encoding hints are present.
        """
        policy = encoding_policy_from_spec(self.table_spec)
        if not policy.specs:
            return None
        return policy


@dataclass(frozen=True)
class ValidationPlans:
    """Plan-lane validation outputs for a dataset."""

    invalid_rows: Plan
    duplicate_keys: Plan

    def to_tables(self, *, ctx: ExecutionContext) -> tuple[TableLike, TableLike]:
        """Materialize validation plans into Arrow tables.

        Returns
        -------
        tuple[TableLike, TableLike]
            Invalid rows and duplicate key tables.
        """
        invalid = _run_plan(
            self.invalid_rows,
            ctx=ctx,
            prefer_reader=False,
            attach_ordering_metadata=True,
        ).value
        dupes = _run_plan(
            self.duplicate_keys,
            ctx=ctx,
            prefer_reader=False,
            attach_ordering_metadata=True,
        ).value
        return cast("TableLike", invalid), cast("TableLike", dupes)


def _run_plan(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    prefer_reader: bool,
    attach_ordering_metadata: bool,
) -> PlanRunResultProto:
    module = plan_runner_module()
    return module.run_plan(
        plan,
        ctx=ctx,
        prefer_reader=prefer_reader,
        attach_ordering_metadata=attach_ordering_metadata,
    )


class _ScanIOModule(Protocol):
    DatasetSource: type[DatasetSource]

    def plan_from_dataset(
        self,
        dataset: ds.Dataset,
        *,
        spec: DatasetSpec,
        ctx: ExecutionContext,
    ) -> Plan: ...

    def plan_from_source(
        self,
        source: PlanSource,
        *,
        ctx: ExecutionContext,
        label: str,
    ) -> Plan: ...


def _scan_io_module() -> _ScanIOModule:
    return cast("_ScanIOModule", importlib.import_module("arrowdsl.plan.scan_io"))


def _dataset_source_class() -> type[DatasetSource]:
    return _scan_io_module().DatasetSource


def _plan_from_dataset(
    dataset: ds.Dataset,
    *,
    spec: DatasetSpec,
    ctx: ExecutionContext,
) -> Plan:
    return _scan_io_module().plan_from_dataset(dataset, spec=spec, ctx=ctx)


def _plan_from_source(
    source: PlanSource,
    *,
    ctx: ExecutionContext,
    label: str,
) -> Plan:
    return _scan_io_module().plan_from_source(source, ctx=ctx, label=label)


class _SpecTablesModule(Protocol):
    SchemaSpecTables: type[SchemaSpecTables]

    def table_specs_from_tables(
        self,
        field_table: pa.Table,
        *,
        constraints_table: pa.Table | None = None,
    ) -> dict[str, TableSchemaSpec]: ...

    def contract_specs_from_table(
        self,
        contract_table: pa.Table,
        table_specs: dict[str, TableSchemaSpec],
    ) -> dict[str, ContractSpec]: ...

    def dataset_specs_from_tables(
        self,
        field_table: pa.Table,
        *,
        constraints_table: pa.Table | None = None,
        contract_table: pa.Table | None = None,
    ) -> dict[str, DatasetSpec]: ...


def _spec_tables_module() -> _SpecTablesModule:
    return cast("_SpecTablesModule", importlib.import_module("arrowdsl.spec.tables.schema"))


def _schema_spec_tables_class() -> type[SchemaSpecTables]:
    return _spec_tables_module().SchemaSpecTables


def _table_specs_from_tables(
    field_table: pa.Table,
    *,
    constraints_table: pa.Table | None = None,
) -> dict[str, TableSchemaSpec]:
    return _spec_tables_module().table_specs_from_tables(
        field_table,
        constraints_table=constraints_table,
    )


def _contract_specs_from_table(
    contract_table: pa.Table,
    table_specs: dict[str, TableSchemaSpec],
) -> dict[str, ContractSpec]:
    return _spec_tables_module().contract_specs_from_table(contract_table, table_specs)


def _dataset_specs_from_tables(
    field_table: pa.Table,
    *,
    constraints_table: pa.Table | None = None,
    contract_table: pa.Table | None = None,
) -> dict[str, DatasetSpec]:
    return _spec_tables_module().dataset_specs_from_tables(
        field_table,
        constraints_table=constraints_table,
        contract_table=contract_table,
    )


class _ReadDatasetParams(Protocol):
    def __init__(self, **kwargs: object) -> None: ...


class _IbisRegistryModule(Protocol):
    ReadDatasetParams: type[_ReadDatasetParams]

    def read_dataset(
        self,
        backend: ibis.backends.BaseBackend,
        *,
        params: _ReadDatasetParams,
    ) -> Table: ...


def _ibis_registry_module() -> _IbisRegistryModule:
    return cast("_IbisRegistryModule", importlib.import_module("ibis_engine.registry"))


@dataclass(frozen=True)
class DatasetOpenSpec:
    """Dataset open parameters for schema discovery."""

    dataset_format: str = "parquet"
    filesystem: pafs.FileSystem | None = None
    partitioning: str | None = "hive"
    schema: SchemaLike | None = None
    read_options: Mapping[str, object] = field(default_factory=dict)

    def open(self, path: PathLike) -> ds.Dataset:
        """Open a dataset using the stored options.

        Returns
        -------
        ds.Dataset
            Opened dataset instance.
        """
        return open_dataset(
            path,
            dataset_format=self.dataset_format,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            schema=self.schema,
        )

    def read_ibis_table(
        self,
        path: PathLike,
        *,
        backend: ibis.backends.BaseBackend,
        table_name: str | None = None,
    ) -> Table:
        """Read an Ibis table using the stored options.

        Returns
        -------
        ibis.expr.types.Table
            Ibis table expression for the dataset.
        """
        module = _ibis_registry_module()
        params = module.ReadDatasetParams(
            path=path,
            dataset_format=self.dataset_format,
            read_options=self.read_options,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            table_name=table_name,
        )
        return module.read_dataset(backend, params=params)


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
    datafusion_scan: DataFusionScanOptions | None
    derived_fields: Sequence[DerivedFieldSpec]
    predicate: ExprSpec | None
    pushdown_predicate: ExprSpec | None
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
    datafusion_scan = kwargs.get("datafusion_scan")
    derived_fields = tuple(kwargs["derived_fields"]) if "derived_fields" in kwargs else ()
    predicate = kwargs.get("predicate")
    pushdown_predicate = kwargs.get("pushdown_predicate")
    evolution_spec = kwargs.get("evolution_spec")
    metadata_spec = kwargs.get("metadata_spec")
    validation = kwargs.get("validation")
    evolution = evolution_spec or SchemaEvolutionSpec()
    metadata = metadata_spec or SchemaMetadataSpec()
    return DatasetSpec(
        table_spec=table_spec,
        contract_spec=contract_spec,
        query_spec=query_spec,
        datafusion_scan=datafusion_scan,
        derived_fields=derived_fields,
        predicate=predicate,
        pushdown_predicate=pushdown_predicate,
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
    return replace(
        table_spec,
        required_non_null=contract.required_non_null,
        key_fields=contract.key_fields,
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


def _encoding_hint_from_field(
    field_meta: Mapping[str, str],
    *,
    dtype: DataTypeLike,
) -> Literal["dictionary"] | None:
    hint = field_meta.get(ENCODING_META)
    if hint == ENCODING_DICTIONARY:
        return ENCODING_DICTIONARY
    if patypes.is_dictionary(dtype):
        return ENCODING_DICTIONARY
    return None


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
    fields = []
    for schema_field in schema:
        meta = _decode_metadata(schema_field.metadata)
        encoding = _encoding_hint_from_field(meta, dtype=schema_field.type)
        fields.append(
            ArrowFieldSpec(
                name=schema_field.name,
                dtype=schema_field.type,
                nullable=schema_field.nullable,
                metadata=meta,
                encoding=encoding,
            )
        )
    meta_name, meta_version = schema_identity_from_metadata(schema.metadata)
    required_non_null, key_fields = schema_constraints_from_metadata(schema.metadata)
    resolved_name = meta_name or name
    resolved_version = version if version is not None else meta_version
    return TableSchemaSpec(
        name=resolved_name,
        version=resolved_version,
        fields=fields,
        required_non_null=required_non_null,
        key_fields=key_fields,
    )


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
    metadata_spec = metadata_spec_from_schema(schema)
    return make_dataset_spec(table_spec=table_spec, metadata_spec=metadata_spec)


def dataset_spec_from_dataset(
    name: str,
    dataset: ds.Dataset,
    *,
    version: int | None = None,
) -> DatasetSpec:
    """Create a DatasetSpec from a dataset schema.

    Returns
    -------
    DatasetSpec
        Dataset spec derived from the dataset schema.
    """
    return dataset_spec_from_schema(name, dataset.schema, version=version)


def dataset_spec_from_path(
    name: str,
    path: PathLike,
    *,
    options: DatasetOpenSpec | None = None,
    version: int | None = None,
) -> DatasetSpec:
    """Create a DatasetSpec from a dataset path.

    Returns
    -------
    DatasetSpec
        Dataset spec derived from the dataset path.
    """
    options = options or DatasetOpenSpec()
    dataset = options.open(path)
    return dataset_spec_from_dataset(name, dataset, version=version)


def contract_catalog_spec_from_tables(
    *,
    field_table: pa.Table,
    constraints_table: pa.Table | None = None,
    contract_table: pa.Table | None = None,
) -> ContractCatalogSpec:
    """Build a ContractCatalogSpec from schema spec tables.

    Returns
    -------
    ContractCatalogSpec
        Catalog spec derived from the provided tables.
    """
    table_specs = _table_specs_from_tables(field_table, constraints_table=constraints_table)
    if contract_table is None:
        return ContractCatalogSpec(contracts={})
    return ContractCatalogSpec(
        contracts=_contract_specs_from_table(contract_table, table_specs),
    )


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

    def register_dataset_from_dataset(
        self,
        name: str,
        dataset: ds.Dataset,
        *,
        version: int | None = None,
    ) -> DatasetSpec:
        """Register a dataset spec derived from a dataset schema.

        Returns
        -------
        DatasetSpec
            Registered dataset spec.
        """
        return self.register_dataset(dataset_spec_from_dataset(name, dataset, version=version))

    def register_dataset_from_path(
        self,
        name: str,
        path: PathLike,
        *,
        options: DatasetOpenSpec | None = None,
        version: int | None = None,
    ) -> DatasetSpec:
        """Register a dataset spec derived from a dataset path.

        Returns
        -------
        DatasetSpec
            Registered dataset spec.
        """
        options = options or DatasetOpenSpec()
        dataset = options.open(path)
        return self.register_dataset(dataset_spec_from_dataset(name, dataset, version=version))

    def register_from_tables(self, tables: SchemaSpecTables) -> dict[str, DatasetSpec]:
        """Register dataset specs from schema spec tables.

        Returns
        -------
        dict[str, DatasetSpec]
            Mapping of dataset names to registered specs.
        """
        dataset_specs = _dataset_specs_from_tables(
            tables.field_table,
            constraints_table=tables.constraints_table,
            contract_table=tables.contract_table,
        )
        for spec in dataset_specs.values():
            self.register_dataset(spec)
        return dataset_specs

    def register_from_paths(
        self,
        *,
        field_table: PathLike,
        constraints_table: PathLike | None = None,
        contract_table: PathLike | None = None,
    ) -> dict[str, DatasetSpec]:
        """Register dataset specs from on-disk spec tables.

        Returns
        -------
        dict[str, DatasetSpec]
            Mapping of dataset names to registered specs.
        """
        tables = _schema_spec_tables_class()(
            field_table=read_spec_table(field_table),
            constraints_table=read_spec_table(constraints_table)
            if constraints_table is not None
            else None,
            contract_table=read_spec_table(contract_table) if contract_table is not None else None,
        )
        return self.register_from_tables(tables)


GLOBAL_SCHEMA_REGISTRY = SchemaRegistry()


def register_dataset_spec(
    spec: DatasetSpec,
    *,
    registry: SchemaRegistry | None = None,
) -> DatasetSpec:
    """Register a dataset spec into the provided registry.

    Returns
    -------
    DatasetSpec
        Registered dataset specification.
    """
    target = registry or GLOBAL_SCHEMA_REGISTRY
    return target.register_dataset(spec)


@dataclass(frozen=True)
class ContractCatalogSpec:
    """Collection of contract specifications keyed by name."""

    contracts: dict[str, ContractSpec]

    def __post_init__(self) -> None:
        """Validate that contract names match mapping keys.

        Raises
        ------
        ValueError
            Raised when contract keys do not match their spec names.
        """
        mismatched = [name for name, spec in self.contracts.items() if name != spec.name]
        if mismatched:
            msg = f"contract key mismatch: {mismatched}"
            raise ValueError(msg)

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
    "DataFusionScanOptions",
    "DatasetOpenSpec",
    "DatasetSpec",
    "DatasetSpecKwargs",
    "DedupeSpecSpec",
    "SchemaRegistry",
    "SortKeySpec",
    "TableSpecConstraints",
    "ValidationPlans",
    "VirtualFieldSpec",
    "contract_catalog_spec_from_tables",
    "dataset_spec_from_contract",
    "dataset_spec_from_dataset",
    "dataset_spec_from_path",
    "dataset_spec_from_schema",
    "make_contract_spec",
    "make_dataset_spec",
    "make_table_spec",
    "register_dataset_spec",
    "table_spec_from_schema",
    "validate_arrow_table",
]
