"""Schema system registry, factories, and Arrow validation integration."""

from __future__ import annotations

import hashlib
import importlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal, Protocol, TypedDict, Unpack, cast

import ibis
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from ibis.backends import BaseBackend
from ibis.expr.types import BooleanValue, Table

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import DataTypeLike, SchemaLike, TableLike
from arrowdsl.core.ordering import Ordering, OrderingLevel
from arrowdsl.core.plan_ops import DedupeSpec, SortKey
from arrowdsl.finalize.finalize import Contract, FinalizeContext
from arrowdsl.schema.metadata import (
    encoding_policy_from_spec,
    merge_metadata_specs,
    metadata_spec_from_schema,
    ordering_metadata_spec,
)
from arrowdsl.schema.policy import SchemaPolicyOptions, schema_policy_factory
from arrowdsl.schema.schema import (
    CastErrorPolicy,
    EncodingPolicy,
    SchemaEvolutionSpec,
    SchemaMetadataSpec,
    missing_key_fields,
    register_schema_extensions,
    required_field_names,
)
from arrowdsl.schema.validation import ArrowValidationOptions, validate_table
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.schema_registry import (
    is_nested_dataset,
    nested_dataset_names,
    nested_view_spec,
)
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisProjectionSpec, IbisQuerySpec
from ibis_engine.scan_io import DatasetSource, PlanSource, plan_from_dataset, plan_from_source
from schema_spec.dataset_handle import DatasetHandle
from schema_spec.specs import (
    ArrowFieldSpec,
    DerivedFieldSpec,
    ExternalTableConfig,
    FieldBundle,
    TableSchemaSpec,
)
from storage.dataset_sources import (
    DatasetDiscoveryOptions,
    DatasetSourceOptions,
    PathLike,
    normalize_dataset_source,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy

if TYPE_CHECKING:
    from arrowdsl.spec.expr_ir import ExprIR
    from arrowdsl.spec.tables.schema import SchemaSpecTables
    from ibis_engine.execution import IbisExecutionContext
    from schema_spec.view_specs import ViewSpec


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
class TableSchemaContract:
    """Combine file and partition schema into a TableSchema contract."""

    file_schema: pa.Schema
    partition_cols: tuple[tuple[str, pa.DataType], ...] = ()

    def partition_schema(self) -> pa.Schema | None:
        """Return the partition schema when partition columns are present.

        Returns
        -------
        pyarrow.Schema | None
            Partition schema when partition columns are defined.
        """
        if not self.partition_cols:
            return None
        fields = [pa.field(name, dtype, nullable=False) for name, dtype in self.partition_cols]
        return pa.schema(fields)


@dataclass(frozen=True)
class ParquetColumnOptions:
    """Per-column Parquet scan options."""

    statistics_enabled: tuple[str, ...] = ()
    bloom_filter_enabled: tuple[str, ...] = ()
    dictionary_enabled: tuple[str, ...] = ()

    def external_table_options(self) -> dict[str, str]:
        """Return DataFusion external table options for column settings.

        Returns
        -------
        dict[str, str]
            External table options keyed by option name.
        """
        options: dict[str, str] = {}
        if self.statistics_enabled:
            options["statistics_enabled"] = ",".join(self.statistics_enabled)
        if self.bloom_filter_enabled:
            options["bloom_filter_enabled"] = ",".join(self.bloom_filter_enabled)
        if self.dictionary_enabled:
            options["dictionary_enabled"] = ",".join(self.dictionary_enabled)
        return options


@dataclass(frozen=True)
class DataFusionScanOptions:
    """DataFusion-specific scan configuration."""

    partition_cols: tuple[tuple[str, pa.DataType], ...] = ()
    file_sort_order: tuple[str, ...] = ()
    parquet_pruning: bool = True
    skip_metadata: bool = True
    skip_arrow_metadata: bool | None = None
    binary_as_string: bool | None = None
    schema_force_view_types: bool | None = None
    listing_table_factory_infer_partitions: bool | None = None
    listing_table_ignore_subdirectory: bool | None = None
    file_extension: str | None = None
    cache: bool = False
    collect_statistics: bool | None = None
    meta_fetch_concurrency: int | None = None
    list_files_cache_ttl: str | None = None
    list_files_cache_limit: str | None = None
    projection_exprs: tuple[str, ...] = ()
    parquet_column_options: ParquetColumnOptions | None = None
    listing_mutable: bool = False
    unbounded: bool = False
    table_schema_contract: TableSchemaContract | None = None
    expr_adapter_factory: object | None = None


@dataclass(frozen=True)
class DeltaScanOptions:
    """Delta-specific scan configuration."""

    file_column_name: str | None = None
    enable_parquet_pushdown: bool = True
    schema_force_view_types: bool = False
    schema: pa.Schema | None = None


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


def _validate_view_specs(view_specs: Sequence[ViewSpec], *, label: str) -> None:
    """Validate view spec invariants.

    Raises
    ------
    ValueError
        Raised when view specs are duplicated or unnamed.
    """
    names = [view.name for view in view_specs]
    if not names:
        return
    if "" in names:
        msg = f"{label} view_specs contain empty view names."
        raise ValueError(msg)
    duplicates = {name for name in names if names.count(name) > 1}
    if duplicates:
        msg = f"{label} view_specs contain duplicate names: {sorted(duplicates)}"
        raise ValueError(msg)


def _can_cast_type(actual: DataTypeLike, target: DataTypeLike) -> bool:
    try:
        pc.cast(pa.scalar(None, type=actual), target, safe=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
        return False
    return True


def schema_evolution_compatible(
    *,
    expected: TableSchemaSpec,
    actual: SchemaLike,
    evolution_spec: SchemaEvolutionSpec,
) -> bool:
    """Return whether an actual schema is compatible with the expected spec.

    Parameters
    ----------
    expected:
        Expected table schema specification.
    actual:
        Actual Arrow schema discovered from the dataset.
    evolution_spec:
        Evolution rules to apply when comparing schemas.

    Returns
    -------
    bool
        True when the schemas are compatible under the evolution rules.
    """
    mapped_fields: list[pa.Field] = []
    seen: set[str] = set()
    for actual_field in actual:
        name = evolution_spec.resolve_name(actual_field.name)
        if name in seen:
            return False
        mapped_fields.append(
            pa.field(
                name,
                actual_field.type,
                actual_field.nullable,
                metadata=actual_field.metadata,
            )
        )
        seen.add(name)
    mapped_schema = pa.schema(mapped_fields, metadata=actual.metadata)
    actual_fields = {field.name: field for field in mapped_schema}
    expected_names = [field.name for field in expected.fields]
    if not evolution_spec.allow_extra:
        extras = [name for name in actual_fields if name not in expected_names]
        if extras:
            return False
    for field_spec in expected.fields:
        actual_field = actual_fields.get(field_spec.name)
        if actual_field is None:
            if evolution_spec.allow_missing:
                continue
            return False
        if actual_field.type == field_spec.dtype:
            continue
        if evolution_spec.allow_casts and _can_cast_type(actual_field.type, field_spec.dtype):
            continue
        return False
    return True


@dataclass(frozen=True)
class ContractSpec:
    """Output contract specification."""

    name: str
    table_schema: TableSchemaSpec

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    constraints: tuple[str, ...] = ()
    version: int | None = None

    virtual_fields: tuple[str, ...] = ()
    virtual_field_docs: dict[str, str] | None = None
    validation: ArrowValidationOptions | None = None
    view_specs: tuple[ViewSpec, ...] = ()

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
        _validate_view_specs(self.view_specs, label="contract")

    def to_contract(self) -> Contract:
        """Convert to a runtime Contract.

        Returns
        -------
        Contract
            Runtime contract instance.
        """
        version = self.version if self.version is not None else self.table_schema.version
        contract_kwargs: ContractKwargs = {
            "name": self.name,
            "schema": self.table_schema.to_arrow_schema(),
            "schema_spec": self.table_schema,
            "key_fields": self.table_schema.key_fields,
            "required_non_null": self.table_schema.required_non_null,
            "dedupe": self.dedupe.to_dedupe_spec() if self.dedupe is not None else None,
            "canonical_sort": tuple(sk.to_sort_key() for sk in self.canonical_sort),
            "constraints": self.constraints,
            "version": version,
            "virtual_fields": self.virtual_fields,
            "virtual_field_docs": self.virtual_field_docs,
            "validation": self.validation,
        }
        return Contract(**contract_kwargs)


@dataclass(frozen=True)
class DatasetSpec:
    """Unified dataset spec with schema, contract, and query behavior."""

    table_spec: TableSchemaSpec
    contract_spec: ContractSpec | None = None
    query_spec: IbisQuerySpec | None = None
    view_specs: tuple[ViewSpec, ...] = ()
    datafusion_scan: DataFusionScanOptions | None = None
    delta_scan: DeltaScanOptions | None = None
    delta_write_policy: DeltaWritePolicy | None = None
    delta_schema_policy: DeltaSchemaPolicy | None = None
    delta_constraints: tuple[str, ...] = ()
    derived_fields: tuple[DerivedFieldSpec, ...] = ()
    predicate: ExprIR | None = None
    pushdown_predicate: ExprIR | None = None
    evolution_spec: SchemaEvolutionSpec = field(default_factory=SchemaEvolutionSpec)
    metadata_spec: SchemaMetadataSpec = field(default_factory=SchemaMetadataSpec)
    validation: ArrowValidationOptions | None = None

    def __post_init__(self) -> None:
        """Validate dataset spec invariants."""
        _validate_view_specs(self.view_specs, label="dataset")

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

    def ordering(self) -> Ordering:
        """Return ordering metadata derived from the dataset spec.

        Returns
        -------
        Ordering
            Ordering metadata inferred from contract and schema settings.
        """
        if self.contract_spec is not None and self.contract_spec.canonical_sort:
            keys = tuple((key.column, key.order) for key in self.contract_spec.canonical_sort)
            return Ordering.explicit(keys)
        if self.table_spec.key_fields:
            return Ordering.implicit()
        return Ordering.unordered()

    def query(self) -> IbisQuerySpec:
        """Return the query spec, deriving it from the table spec if needed.

        Returns
        -------
        IbisQuerySpec
            Query spec for scanning.
        """
        if self.query_spec is not None:
            return self.query_spec
        cols = tuple(field.name for field in self.table_spec.fields)
        derived = {spec.name: spec.expr for spec in self.derived_fields}
        return IbisQuerySpec(
            projection=IbisProjectionSpec(base=cols, derived=derived),
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

    def to_handle(self) -> DatasetHandle:
        """Return a DatasetHandle for this dataset spec.

        Returns
        -------
        DatasetHandle
            Dataset handle bound to this spec.
        """
        return DatasetHandle(spec=self)

    def resolved_view_specs(self) -> tuple[ViewSpec, ...]:
        """Return the merged view specs for this dataset.

        Returns
        -------
        tuple[ViewSpec, ...]
            View specs from both the dataset and its contract.
        """
        specs: list[ViewSpec] = []
        seen: set[str] = set()
        if self.contract_spec is not None:
            for view in self.contract_spec.view_specs:
                if view.name in seen:
                    continue
                specs.append(view)
                seen.add(view.name)
        for view in self.view_specs:
            if view.name in seen:
                continue
            specs.append(view)
            seen.add(view.name)
        if not specs and is_nested_dataset(self.name):
            return (nested_view_spec(self.name),)
        return tuple(specs)

    def external_table_sql(self, config: ExternalTableConfig) -> str:
        """Return a CREATE EXTERNAL TABLE statement for this dataset.

        Returns
        -------
        str
            CREATE EXTERNAL TABLE statement for this dataset schema.
        """
        return self.table_spec.to_create_external_table_sql(config)

    def _plan_for_validation(
        self,
        source: PlanSource,
        *,
        ctx: ExecutionContext,
        backend: BaseBackend,
    ) -> IbisPlan:
        if isinstance(source, DatasetSource):
            return plan_from_dataset(
                source.dataset,
                spec=self,
                ctx=ctx,
                backend=backend,
                name=self.name,
            )
        return plan_from_source(
            source,
            ctx=ctx,
            backend=backend,
            name=f"{self.name}_validate",
        )

    def validation_plans(self, source: PlanSource, *, ctx: ExecutionContext) -> ValidationPlans:
        """Return plan-lane validation pipelines for invalid rows and duplicate keys.

        Returns
        -------
        ValidationPlans
            Plans for invalid rows and duplicate keys.
        """
        backend = _ibis_backend_for_ctx(ctx)
        plan = self._plan_for_validation(source, ctx=ctx, backend=backend)
        invalid = _invalid_rows_plan(plan, spec=self.table_spec)
        dupes = _duplicate_key_rows_plan(plan, keys=self.table_spec.key_fields)
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

    invalid_rows: IbisPlan
    duplicate_keys: IbisPlan

    def to_tables(self, *, ctx: ExecutionContext) -> tuple[TableLike, TableLike]:
        """Materialize validation plans into Arrow tables.

        Returns
        -------
        tuple[TableLike, TableLike]
            Invalid rows and duplicate key tables.
        """
        execution = _ibis_execution_for_ctx(ctx)
        module = importlib.import_module("ibis_engine.execution")
        materialize = module.materialize_ibis_plan
        invalid = materialize(self.invalid_rows, execution=execution)
        dupes = materialize(self.duplicate_keys, execution=execution)
        return invalid, dupes


def _ibis_backend_for_ctx(ctx: ExecutionContext) -> BaseBackend:
    return build_backend(IbisBackendConfig(datafusion_profile=ctx.runtime.datafusion))


def _ibis_execution_for_ctx(ctx: ExecutionContext) -> IbisExecutionContext:
    module = importlib.import_module("ibis_engine.execution")
    execution_cls = cast("type[IbisExecutionContext]", module.IbisExecutionContext)
    return execution_cls(
        ctx=ctx,
        ibis_backend=_ibis_backend_for_ctx(ctx),
    )


def _empty_validation_plan() -> IbisPlan:
    empty = ibis.memtable(pa.table({}))
    return IbisPlan(expr=empty, ordering=Ordering.unordered())


def _invalid_rows_plan(plan: IbisPlan, *, spec: TableSchemaSpec) -> IbisPlan:
    required = required_field_names(spec)
    available = set(plan.expr.columns)
    predicates = [plan.expr[name].isnull() for name in required if name in available]
    if not predicates:
        filtered = plan.expr.filter(cast("BooleanValue", ibis.literal(value=False)))
        return IbisPlan(expr=filtered, ordering=Ordering.unordered())
    predicate = predicates[0]
    for part in predicates[1:]:
        predicate |= part
    filtered = plan.expr.filter(predicate)
    return IbisPlan(expr=filtered, ordering=Ordering.unordered())


def _duplicate_key_rows_plan(plan: IbisPlan, *, keys: Sequence[str]) -> IbisPlan:
    if not keys:
        return _empty_validation_plan()
    available = set(plan.expr.columns)
    missing = missing_key_fields(keys, missing_cols=[key for key in keys if key not in available])
    if missing:
        return _empty_validation_plan()
    count_col = f"{keys[0]}_count"
    grouped = plan.expr.group_by([plan.expr[key] for key in keys]).aggregate(
        **{count_col: plan.expr[keys[0]].count()}
    )
    filtered = grouped.filter(grouped[count_col] > ibis.literal(1))
    return IbisPlan(expr=filtered, ordering=Ordering.unordered())


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

    dataset_format: str = "delta"
    filesystem: object | None = None
    files: tuple[str, ...] | None = None
    partitioning: str | None = "hive"
    schema: SchemaLike | None = None
    parquet_read_options: ds.ParquetReadOptions | None = None
    read_options: Mapping[str, object] = field(default_factory=dict)
    storage_options: Mapping[str, str] = field(default_factory=dict)
    delta_version: int | None = None
    delta_timestamp: str | None = None
    discovery: DatasetDiscoveryOptions | None = field(default_factory=DatasetDiscoveryOptions)

    def open(self, path: PathLike) -> ds.Dataset:
        """Open a dataset using the stored options.

        Returns
        -------
        ds.Dataset
            Opened dataset instance.
        """
        if self.schema is not None:
            register_schema_extensions(self.schema)
        return normalize_dataset_source(
            path,
            options=DatasetSourceOptions(
                dataset_format=self.dataset_format,
                filesystem=self.filesystem,
                files=self.files,
                partitioning=self.partitioning,
                schema=self.schema,
                parquet_read_options=self.parquet_read_options,
                storage_options=self.storage_options,
                delta_version=self.delta_version,
                delta_timestamp=self.delta_timestamp,
                discovery=self.discovery,
            ),
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
            storage_options=self.storage_options,
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
    constraints: Iterable[str]
    virtual: VirtualFieldSpec | None
    version: int | None
    validation: ArrowValidationOptions | None
    view_specs: Sequence[ViewSpec]


class ContractKwargs(TypedDict):
    """Keyword arguments for building runtime contracts."""

    name: str
    schema: SchemaLike
    schema_spec: TableSchemaSpec | None
    key_fields: tuple[str, ...]
    required_non_null: tuple[str, ...]
    dedupe: DedupeSpec | None
    canonical_sort: tuple[SortKey, ...]
    constraints: tuple[str, ...]
    version: int | None
    virtual_fields: tuple[str, ...]
    virtual_field_docs: dict[str, str] | None
    validation: ArrowValidationOptions | None


class DatasetSpecKwargs(TypedDict, total=False):
    """Keyword arguments supported by make_dataset_spec."""

    contract_spec: ContractSpec | None
    query_spec: IbisQuerySpec | None
    view_specs: Sequence[ViewSpec]
    datafusion_scan: DataFusionScanOptions | None
    delta_scan: DeltaScanOptions | None
    delta_write_policy: DeltaWritePolicy | None
    delta_schema_policy: DeltaSchemaPolicy | None
    delta_constraints: Sequence[str]
    derived_fields: Sequence[DerivedFieldSpec]
    predicate: ExprIR | None
    pushdown_predicate: ExprIR | None
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


def _merge_constraints(*parts: Iterable[str]) -> tuple[str, ...]:
    """Return unique constraint expressions in stable order.

    Returns
    -------
    tuple[str, ...]
        Normalized constraint expressions.
    """
    merged: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for constraint in part:
            normalized = constraint.strip()
            if not normalized or normalized in seen:
                continue
            merged.append(normalized)
            seen.add(normalized)
    return tuple(merged)


def _delta_constraints_from_table_spec(table_spec: TableSchemaSpec) -> tuple[str, ...]:
    """Return default Delta constraints derived from schema constraints.

    Returns
    -------
    tuple[str, ...]
        Constraint expressions derived from required and key fields.
    """
    required = _merge_names(table_spec.required_non_null, table_spec.key_fields)
    if not required:
        return ()
    required_set = set(required)
    ordered = [field.name for field in table_spec.fields if field.name in required_set]
    return tuple(f"{name} IS NOT NULL" for name in ordered)


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


def table_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from an Arrow schema.

    Returns
    -------
    TableSchemaSpec
        Table schema specification derived from the Arrow schema.
    """
    return TableSchemaSpec.from_schema(name, schema, version=version)


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
    constraints = kwargs.get("constraints", ())
    virtual = kwargs.get("virtual")
    version = kwargs.get("version")
    validation = kwargs.get("validation")
    view_specs = tuple(cast("Sequence[ViewSpec]", kwargs.get("view_specs", ())))
    virtual_fields = virtual.fields if virtual is not None else ()
    virtual_docs = dict(virtual.docs) if virtual is not None and virtual.docs is not None else None
    return ContractSpec(
        name=table_spec.name,
        table_schema=table_spec,
        dedupe=dedupe,
        canonical_sort=tuple(canonical_sort),
        constraints=tuple(constraints),
        version=version if version is not None else table_spec.version,
        virtual_fields=virtual_fields,
        virtual_field_docs=virtual_docs,
        validation=validation,
        view_specs=view_specs,
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
    return DatasetSpec(
        table_spec=table_spec,
        contract_spec=kwargs.get("contract_spec"),
        query_spec=kwargs.get("query_spec"),
        view_specs=tuple(kwargs.get("view_specs", ())),
        datafusion_scan=kwargs.get("datafusion_scan"),
        delta_scan=kwargs.get("delta_scan"),
        delta_write_policy=kwargs.get("delta_write_policy"),
        delta_schema_policy=kwargs.get("delta_schema_policy"),
        delta_constraints=tuple(kwargs.get("delta_constraints", ())),
        derived_fields=tuple(kwargs.get("derived_fields", ())),
        predicate=kwargs.get("predicate"),
        pushdown_predicate=kwargs.get("pushdown_predicate"),
        evolution_spec=kwargs.get("evolution_spec") or SchemaEvolutionSpec(),
        metadata_spec=kwargs.get("metadata_spec") or SchemaMetadataSpec(),
        validation=kwargs.get("validation"),
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
    table_spec = TableSchemaSpec.from_schema(
        contract.name,
        contract.schema,
        version=contract.version,
    )
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
    constraints = contract.constraints if hasattr(contract, "constraints") else ()
    delta_constraints = _merge_constraints(
        constraints,
        _delta_constraints_from_table_spec(table_spec),
    )
    contract_spec = ContractSpec(
        name=contract.name,
        table_schema=table_spec,
        dedupe=_dedupe_spec_spec(contract.dedupe),
        canonical_sort=_sort_key_specs(contract.canonical_sort),
        constraints=constraints,
        version=contract.version,
        virtual_fields=contract.virtual_fields,
        virtual_field_docs=contract.virtual_field_docs,
        validation=contract.validation,
    )
    return make_dataset_spec(
        table_spec=table_spec,
        contract_spec=contract_spec,
        delta_constraints=delta_constraints,
    )


def ddl_fingerprint_from_definition(ddl: str) -> str:
    """Return a stable fingerprint for a CREATE TABLE statement.

    Parameters
    ----------
    ddl
        CREATE TABLE statement text.

    Returns
    -------
    str
        Stable fingerprint of the DDL string.
    """
    return hashlib.sha256(ddl.encode("utf-8")).hexdigest()


def dataset_table_ddl_fingerprint(name: str) -> str | None:
    """Return the DDL fingerprint for a DataFusion-registered table.

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns
    -------
    str | None
        Stable DDL fingerprint when available.
    """
    ddl = dataset_table_definition(name)
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

    Returns
    -------
    DatasetSpec
        Dataset spec derived from the schema.
    """
    table_spec = TableSchemaSpec.from_schema(name, schema, version=version)
    metadata_spec = metadata_spec_from_schema(schema)
    evolution_spec = resolve_schema_evolution_spec(name)
    return make_dataset_spec(
        table_spec=table_spec,
        metadata_spec=metadata_spec,
        evolution_spec=evolution_spec,
        delta_constraints=_delta_constraints_from_table_spec(table_spec),
    )


def dataset_table_definition(name: str) -> str | None:
    """Return the DataFusion CREATE TABLE definition for a dataset.

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns
    -------
    str | None
        CREATE TABLE statement when available.
    """
    ctx = DataFusionRuntimeProfile().session_context()
    return SchemaIntrospector(ctx).table_definition(name)


def dataset_table_constraints(name: str) -> tuple[str, ...]:
    """Return DataFusion constraint metadata for a dataset.

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns
    -------
    tuple[str, ...]
        Constraint expressions or identifiers, when available.
    """
    ctx = DataFusionRuntimeProfile().session_context()
    return SchemaIntrospector(ctx).table_constraints(name)


def dataset_table_column_defaults(name: str) -> dict[str, object]:
    """Return DataFusion column default metadata for a dataset.

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns
    -------
    dict[str, object]
        Mapping of column names to default expressions.
    """
    ctx = DataFusionRuntimeProfile().session_context()
    return SchemaIntrospector(ctx).table_column_defaults(name)


def dataset_table_logical_plan(name: str) -> str | None:
    """Return DataFusion logical plan text for a dataset.

    Parameters
    ----------
    name
        Dataset name registered in DataFusion.

    Returns
    -------
    str | None
        Logical plan text when available.
    """
    ctx = DataFusionRuntimeProfile().session_context()
    return SchemaIntrospector(ctx).table_logical_plan(name)


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


SCHEMA_EVOLUTION_PRESETS: Mapping[str, SchemaEvolutionSpec] = {
    name: SchemaEvolutionSpec(
        allow_missing=True,
        allow_extra=True,
        allow_casts=True,
    )
    for name in (*nested_dataset_names(), "symtable_files_v1")
}


def resolve_schema_evolution_spec(name: str) -> SchemaEvolutionSpec:
    """Return a canonical schema evolution spec for a dataset name.

    Returns
    -------
    SchemaEvolutionSpec
        Evolution rules for the dataset name.
    """
    return SCHEMA_EVOLUTION_PRESETS.get(name, SchemaEvolutionSpec())


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


__all__ = [
    "SCHEMA_EVOLUTION_PRESETS",
    "ArrowValidationOptions",
    "ContractCatalogSpec",
    "ContractKwargs",
    "ContractSpec",
    "ContractSpecKwargs",
    "DataFusionScanOptions",
    "DatasetOpenSpec",
    "DatasetSpec",
    "DatasetSpecKwargs",
    "DedupeSpecSpec",
    "DeltaScanOptions",
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "ParquetColumnOptions",
    "SortKeySpec",
    "TableSchemaContract",
    "TableSpecConstraints",
    "ValidationPlans",
    "VirtualFieldSpec",
    "contract_catalog_spec_from_tables",
    "dataset_spec_from_contract",
    "dataset_spec_from_dataset",
    "dataset_spec_from_path",
    "dataset_spec_from_schema",
    "dataset_table_column_defaults",
    "dataset_table_constraints",
    "dataset_table_ddl_fingerprint",
    "dataset_table_definition",
    "dataset_table_logical_plan",
    "ddl_fingerprint_from_definition",
    "make_contract_spec",
    "make_dataset_spec",
    "make_table_spec",
    "resolve_schema_evolution_spec",
    "schema_evolution_compatible",
    "table_spec_from_schema",
    "validate_arrow_table",
]
