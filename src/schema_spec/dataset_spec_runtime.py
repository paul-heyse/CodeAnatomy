"""Runtime and accessor extensions extracted from dataset_spec."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Literal, TypedDict, Unpack, cast

import msgspec
import pyarrow as pa
import pyarrow.dataset as ds

from arrow_utils.core.ordering import Ordering, OrderingLevel
from core.config_base import config_fingerprint
from core_types import IdentifierStr, NonNegativeInt
from datafusion_engine.arrow.build import register_schema_extensions
from datafusion_engine.arrow.interop import SchemaLike
from datafusion_engine.arrow.metadata import (
    SchemaMetadataSpec,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from datafusion_engine.delta.protocol import DeltaFeatureGate
from datafusion_engine.expr.query_spec import ProjectionSpec, QuerySpec
from datafusion_engine.expr.spec import ExprSpec
from datafusion_engine.kernels import DedupeSpec, SortKey
from datafusion_engine.schema.alignment import SchemaEvolutionSpec
from datafusion_engine.schema.finalize import Contract
from datafusion_engine.schema.validation import ArrowValidationOptions
from schema_spec.dataset_spec import (
    ContractRow,
    DataFusionScanOptions,
    DedupeSpecSpec,
    DeltaScanPolicyDefaults,
    ParquetColumnOptions,
    ScanPolicyConfig,
    ScanPolicyDefaults,
    SortKeySpec,
    TableSchemaContract,
    apply_delta_scan_policy,
    apply_scan_policy,
    validate_arrow_table,
)
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import DerivedFieldSpec, FieldBundle, TableSchemaSpec
from schema_spec.view_specs import ViewSpec
from serde_msgspec import StructBaseStrict
from storage.dataset_sources import (
    DatasetDiscoveryOptions,
    DatasetSourceOptions,
    PathLike,
    normalize_dataset_source,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy
from utils.validation import validate_required_items


class DeltaScanOptions(StructBaseStrict, frozen=True):
    """Delta-specific scan configuration.

    Delta table registration uses DataFusion's native Delta TableProvider.
    All IO contracts for Delta tables are specified through DataFusion
    registration and catalog metadata.

    Notes:
    -----
    Delta CDF configuration is handled via DatasetLocation.delta_cdf_options
    to keep change-data-feed registration separate from standard scans.

    Attributes:
    ----------
    file_column_name : str | None
        Column name for source file metadata in Delta scans.
    enable_parquet_pushdown : bool
        Enable predicate pushdown to underlying Parquet files.
    schema_force_view_types : bool | None
        Force schema types to match view definitions.
    wrap_partition_values : bool
        Wrap partition values in structs for nested access.
    schema : pa.Schema | None
        Optional schema override for Delta table registration.
    """

    file_column_name: str | None = None
    enable_parquet_pushdown: bool = True
    schema_force_view_types: bool | None = None
    wrap_partition_values: bool = False
    schema: pa.Schema | None = None


class DeltaCdfPolicy(StructBaseStrict, frozen=True):
    """Policy describing Delta CDF requirements."""

    required: bool = False
    allow_out_of_range: bool = False

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the Delta CDF policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing Delta CDF policy settings.
        """
        return {
            "required": self.required,
            "allow_out_of_range": self.allow_out_of_range,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the Delta CDF policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


class DeltaMaintenancePolicy(StructBaseStrict, frozen=True):
    """Policy describing Delta maintenance expectations."""

    optimize_on_write: bool = False
    optimize_target_size: int | None = None
    z_order_cols: tuple[str, ...] = ()
    z_order_when: Literal["never", "periodic", "after_partition_complete"] = "never"
    vacuum_on_write: bool = False
    vacuum_retention_hours: int | None = None
    vacuum_dry_run: bool = False
    enforce_retention_duration: bool = True
    checkpoint_on_write: bool = False
    enable_deletion_vectors: bool = False
    enable_v2_checkpoints: bool = False
    enable_log_compaction: bool = False

    # Outcome-based maintenance thresholds (Wave 4A)
    optimize_file_threshold: int | None = None
    total_file_threshold: int | None = None
    vacuum_version_threshold: int | None = None
    checkpoint_version_interval: int | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the Delta maintenance policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing Delta maintenance policy settings.
        """
        return {
            "optimize_on_write": self.optimize_on_write,
            "optimize_target_size": self.optimize_target_size,
            "z_order_cols": self.z_order_cols,
            "z_order_when": self.z_order_when,
            "vacuum_on_write": self.vacuum_on_write,
            "vacuum_retention_hours": self.vacuum_retention_hours,
            "vacuum_dry_run": self.vacuum_dry_run,
            "enforce_retention_duration": self.enforce_retention_duration,
            "checkpoint_on_write": self.checkpoint_on_write,
            "enable_deletion_vectors": self.enable_deletion_vectors,
            "enable_v2_checkpoints": self.enable_v2_checkpoints,
            "enable_log_compaction": self.enable_log_compaction,
            "optimize_file_threshold": self.optimize_file_threshold,
            "total_file_threshold": self.total_file_threshold,
            "vacuum_version_threshold": self.vacuum_version_threshold,
            "checkpoint_version_interval": self.checkpoint_version_interval,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the Delta maintenance policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


type DatasetKind = Literal["primary", "delta_cdf"]


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

    Args:
        view_specs: Description.
        label: Description.

    Raises:
        ValueError: If the operation cannot be completed.
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


class ContractSpec(StructBaseStrict, frozen=True):
    """Output contract specification."""

    name: IdentifierStr
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
        """Validate contract spec invariants."""
        if self.virtual_field_docs is None:
            return
        validate_required_items(
            self.virtual_field_docs,
            self.virtual_fields,
            item_label="virtual_field_docs keys in virtual_fields",
            error_type=ValueError,
        )
        _validate_view_specs(self.view_specs, label="contract")

    def to_contract(self) -> Contract:
        """Convert to a runtime Contract.

        Returns:
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


class DeltaPolicyBundle(StructBaseStrict, frozen=True):
    """Delta-specific policy bundle for dataset specs."""

    scan: DeltaScanOptions | None = None
    cdf_policy: DeltaCdfPolicy | None = None
    maintenance_policy: DeltaMaintenancePolicy | None = None
    write_policy: DeltaWritePolicy | None = None
    schema_policy: DeltaSchemaPolicy | None = None
    feature_gate: DeltaFeatureGate | None = None
    constraints: tuple[str, ...] = ()


class ValidationPolicySpec(StructBaseStrict, frozen=True):
    """Runtime DataFrame validation policy."""

    enabled: bool = True
    lazy: bool = True
    sample: NonNegativeInt | None = None
    head: NonNegativeInt | None = None
    tail: NonNegativeInt | None = None
    strict: bool | Literal["filter"] | None = None
    coerce: bool | None = None


def validation_policy_to_arrow_options(
    policy: ValidationPolicySpec | None,
) -> ArrowValidationOptions | None:
    """Map dataframe validation policy to Arrow validation options.

    Returns:
    -------
    ArrowValidationOptions | None
        Arrow validation options derived from the dataframe policy, or
        ``None`` when validation is disabled.
    """
    from schema_spec.validation import (
        validation_policy_to_arrow_options as _validation_policy_to_arrow_options,
    )

    return _validation_policy_to_arrow_options(policy)


def validation_policy_payload(policy: ValidationPolicySpec | None) -> Mapping[str, object] | None:
    """Return a JSON-serializable payload for the dataframe validation policy.

    Returns:
    -------
    Mapping[str, object] | None
        Serializable policy payload when defined.
    """
    if policy is None:
        return None
    return {
        "enabled": policy.enabled,
        "lazy": policy.lazy,
        "sample": policy.sample,
        "head": policy.head,
        "tail": policy.tail,
        "strict": policy.strict,
        "coerce": policy.coerce,
    }


class DatasetPolicies(StructBaseStrict, frozen=True):
    """Policy bundle for dataset specs."""

    datafusion_scan: DataFusionScanOptions | None = None
    delta: DeltaPolicyBundle | None = None
    validation: ArrowValidationOptions | None = None
    dataframe_validation: ValidationPolicySpec | None = None
    strict_schema_validation: bool | None = None


class DatasetSpec(StructBaseStrict, frozen=True):
    """Unified dataset spec with schema, contract, and query behavior."""

    table_spec: TableSchemaSpec
    dataset_kind: DatasetKind = "primary"
    contract_spec: ContractSpec | None = None
    query_spec: QuerySpec | None = None
    view_specs: tuple[ViewSpec, ...] = ()
    policies: DatasetPolicies = msgspec.field(default_factory=DatasetPolicies)
    derived_fields: tuple[DerivedFieldSpec, ...] = ()
    predicate: ExprSpec | None = None
    pushdown_predicate: ExprSpec | None = None
    evolution_spec: SchemaEvolutionSpec = msgspec.field(default_factory=SchemaEvolutionSpec)
    metadata_spec: SchemaMetadataSpec = msgspec.field(default_factory=SchemaMetadataSpec)

    def __post_init__(self) -> None:
        """Validate dataset spec invariants."""
        _validate_view_specs(self.view_specs, label="dataset")


class DatasetOpenSpec(StructBaseStrict, frozen=True):
    """Dataset open parameters for schema discovery."""

    dataset_format: str = "delta"
    filesystem: object | None = None
    files: tuple[str, ...] | None = None
    partitioning: str | None = "hive"
    schema: SchemaLike | None = None
    read_options: Mapping[str, object] = msgspec.field(default_factory=dict)
    storage_options: Mapping[str, str] = msgspec.field(default_factory=dict)
    delta_log_storage_options: Mapping[str, str] = msgspec.field(default_factory=dict)
    delta_version: int | None = None
    delta_timestamp: str | None = None
    discovery: DatasetDiscoveryOptions | None = msgspec.field(
        default_factory=DatasetDiscoveryOptions
    )

    def open(self, path: PathLike) -> ds.Dataset:
        """Open a dataset using the stored options.

        Args:
            path: Description.

        Returns:
            ds.Dataset: Result.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.dataset_format == "delta":
            msg = "Delta dataset discovery requires DataFusion catalog introspection."
            raise ValueError(msg)
        if self.dataset_format == "parquet":
            msg = "Parquet dataset discovery is disabled for delta-only IO."
            raise ValueError(msg)
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
                storage_options=self.storage_options,
                delta_version=self.delta_version,
                delta_timestamp=self.delta_timestamp,
                discovery=self.discovery,
            ),
        )


class TableSpecConstraints(StructBaseStrict, frozen=True):
    """Required and key fields for a table schema."""

    required_non_null: Iterable[str] = ()
    key_fields: Iterable[str] = ()


class VirtualFieldSpec(StructBaseStrict, frozen=True):
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

    dataset_kind: DatasetKind
    contract_spec: ContractSpec | None
    query_spec: QuerySpec | None
    view_specs: Sequence[ViewSpec]
    policies: DatasetPolicies | None
    datafusion_scan: DataFusionScanOptions | None
    delta: DeltaPolicyBundle | None
    delta_scan: DeltaScanOptions | None
    delta_cdf_policy: DeltaCdfPolicy | None
    delta_maintenance_policy: DeltaMaintenancePolicy | None
    delta_write_policy: DeltaWritePolicy | None
    delta_schema_policy: DeltaSchemaPolicy | None
    delta_feature_gate: DeltaFeatureGate | None
    delta_constraints: Sequence[str]
    derived_fields: Sequence[DerivedFieldSpec]
    predicate: ExprSpec | None
    pushdown_predicate: ExprSpec | None
    evolution_spec: SchemaEvolutionSpec | None
    metadata_spec: SchemaMetadataSpec | None
    validation: ArrowValidationOptions | None
    dataframe_validation: ValidationPolicySpec | None
    strict_schema_validation: bool | None


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

    Returns:
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

    Returns:
    -------
    tuple[str, ...]
        Constraint expressions derived from required and key fields.
    """
    from schema_spec.table_spec import delta_constraints_from_table_spec

    return delta_constraints_from_table_spec(table_spec)


def _merge_fields(
    bundles: Iterable[FieldBundle],
    fields: Iterable[FieldSpec],
) -> list[FieldSpec]:
    merged: list[FieldSpec] = []
    for bundle in bundles:
        merged.extend(bundle.fields)
    merged.extend(fields)
    return merged


def make_table_spec(
    name: str,
    *,
    version: int | None,
    bundles: Iterable[FieldBundle],
    fields: Iterable[FieldSpec],
    constraints: TableSpecConstraints | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from field bundles and explicit fields.

    Returns:
    -------
    TableSchemaSpec
        Table schema specification.
    """
    from schema_spec.table_spec import make_table_spec as _make_table_spec

    return _make_table_spec(
        name,
        version=version,
        bundles=bundles,
        fields=fields,
        constraints=constraints,
    )


def table_spec_from_schema(
    name: str,
    schema: SchemaLike,
    *,
    version: int | None = None,
) -> TableSchemaSpec:
    """Create a TableSchemaSpec from an Arrow schema.

    Returns:
    -------
    TableSchemaSpec
        Table schema specification derived from the Arrow schema.
    """
    from schema_spec.table_spec import table_spec_from_schema as _table_spec_from_schema

    return _table_spec_from_schema(name, schema, version=version)


def make_contract_spec(
    *,
    table_spec: TableSchemaSpec,
    **kwargs: Unpack[ContractSpecKwargs],
) -> ContractSpec:
    """Create a ContractSpec from a TableSchemaSpec.

    Returns:
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

    Returns:
    -------
    DatasetSpec
        Dataset specification bundling schema, contract, and query behavior.
    """
    policies = kwargs.get("policies")
    if policies is None:
        delta_bundle = kwargs.get("delta")
        if delta_bundle is None:
            delta_bundle = DeltaPolicyBundle(
                scan=kwargs.get("delta_scan"),
                cdf_policy=kwargs.get("delta_cdf_policy"),
                maintenance_policy=kwargs.get("delta_maintenance_policy"),
                write_policy=kwargs.get("delta_write_policy"),
                schema_policy=kwargs.get("delta_schema_policy"),
                feature_gate=kwargs.get("delta_feature_gate"),
                constraints=tuple(kwargs.get("delta_constraints", ())),
            )
        policies = DatasetPolicies(
            datafusion_scan=kwargs.get("datafusion_scan"),
            delta=delta_bundle,
            validation=kwargs.get("validation"),
            dataframe_validation=kwargs.get("dataframe_validation"),
            strict_schema_validation=kwargs.get("strict_schema_validation"),
        )
    return DatasetSpec(
        table_spec=table_spec,
        dataset_kind=kwargs.get("dataset_kind", "primary"),
        contract_spec=kwargs.get("contract_spec"),
        query_spec=kwargs.get("query_spec"),
        view_specs=tuple(kwargs.get("view_specs", ())),
        policies=policies,
        derived_fields=tuple(kwargs.get("derived_fields", ())),
        predicate=kwargs.get("predicate"),
        pushdown_predicate=kwargs.get("pushdown_predicate"),
        evolution_spec=kwargs.get("evolution_spec") or SchemaEvolutionSpec(),
        metadata_spec=kwargs.get("metadata_spec") or SchemaMetadataSpec(),
    )


def _sort_key_specs(keys: Iterable[SortKey]) -> tuple[SortKeySpec, ...]:
    """Convert SortKey objects into SortKeySpec values.

    Returns:
    -------
    tuple[SortKeySpec, ...]
        SortKeySpec values for the keys.
    """
    return tuple(SortKeySpec(column=key.column, order=key.order) for key in keys)


def _dedupe_spec_spec(dedupe: DedupeSpec | None) -> DedupeSpecSpec | None:
    """Convert a DedupeSpec into a DedupeSpecSpec.

    Returns:
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

    Returns:
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
    return msgspec.structs.replace(
        table_spec,
        required_non_null=contract.required_non_null,
        key_fields=contract.key_fields,
    )


def dataset_spec_from_contract(contract: Contract) -> DatasetSpec:
    """Create a DatasetSpec aligned to a runtime Contract.

    Returns:
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


def dataset_spec_name(spec: DatasetSpec) -> str:
    """Return the dataset name."""
    return spec.table_spec.name


def dataset_spec_datafusion_scan(spec: DatasetSpec) -> DataFusionScanOptions | None:
    """Return DataFusion scan options from the policy bundle."""
    return spec.policies.datafusion_scan


def dataset_spec_delta_scan(spec: DatasetSpec) -> DeltaScanOptions | None:
    """Return Delta scan options from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.scan


def dataset_spec_delta_cdf_policy(spec: DatasetSpec) -> DeltaCdfPolicy | None:
    """Return Delta CDF policy from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.cdf_policy


def dataset_spec_delta_maintenance_policy(spec: DatasetSpec) -> DeltaMaintenancePolicy | None:
    """Return Delta maintenance policy from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.maintenance_policy


def dataset_spec_with_delta_maintenance(
    spec: DatasetSpec,
    maintenance_policy: DeltaMaintenancePolicy | None,
) -> DatasetSpec:
    """Return a dataset spec with updated Delta maintenance policy."""
    if maintenance_policy is None:
        return spec
    delta = spec.policies.delta
    if delta is None:
        delta = DeltaPolicyBundle(maintenance_policy=maintenance_policy)
    else:
        delta = msgspec.structs.replace(delta, maintenance_policy=maintenance_policy)
    policies = msgspec.structs.replace(spec.policies, delta=delta)
    return msgspec.structs.replace(spec, policies=policies)


def dataset_spec_delta_write_policy(spec: DatasetSpec) -> DeltaWritePolicy | None:
    """Return Delta write policy from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.write_policy


def dataset_spec_delta_schema_policy(spec: DatasetSpec) -> DeltaSchemaPolicy | None:
    """Return Delta schema policy from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.schema_policy


def dataset_spec_delta_feature_gate(spec: DatasetSpec) -> DeltaFeatureGate | None:
    """Return Delta feature gate from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return None
    return delta.feature_gate


def dataset_spec_delta_constraints(spec: DatasetSpec) -> tuple[str, ...]:
    """Return declared Delta constraints from the policy bundle."""
    delta = spec.policies.delta
    if delta is None:
        return ()
    return delta.constraints


def dataset_spec_strict_schema_validation(spec: DatasetSpec) -> bool | None:
    """Return strict schema validation preference from the policy bundle."""
    return spec.policies.strict_schema_validation


def dataset_spec_schema(spec: DatasetSpec) -> SchemaLike:
    """Return the Arrow schema with dataset metadata and ordering applied."""
    ordering = _ordering_metadata_spec(spec.contract_spec, spec.table_spec)
    delta_policy = spec.policies.delta
    if (
        ordering is None
        and delta_policy is not None
        and delta_policy.write_policy is not None
        and delta_policy.write_policy.zorder_by
    ):
        keys = tuple((name, "ascending") for name in delta_policy.write_policy.zorder_by)
        ordering = ordering_metadata_spec(OrderingLevel.EXPLICIT, keys=keys)
    merged = merge_metadata_specs(spec.metadata_spec, ordering)
    return merged.apply(spec.table_spec.to_arrow_schema())


def dataset_spec_ordering(spec: DatasetSpec) -> Ordering:
    """Return ordering metadata derived from contract and key fields."""
    if spec.contract_spec is not None and spec.contract_spec.canonical_sort:
        keys = tuple((key.column, key.order) for key in spec.contract_spec.canonical_sort)
        return Ordering.explicit(keys)
    if spec.table_spec.key_fields:
        return Ordering.implicit()
    return Ordering.unordered()


def dataset_spec_query(spec: DatasetSpec) -> QuerySpec:
    """Return query spec, deriving projection from schema fields when absent."""
    if spec.query_spec is not None:
        return spec.query_spec
    cols = tuple(field.name for field in spec.table_spec.fields)
    derived = {field_spec.name: field_spec.expr for field_spec in spec.derived_fields}
    return QuerySpec(
        projection=ProjectionSpec(base=cols, derived=derived),
        predicate=spec.predicate,
        pushdown_predicate=spec.pushdown_predicate,
    )


def dataset_spec_contract_spec_or_default(spec: DatasetSpec) -> ContractSpec:
    """Return contract spec with policy validation defaulted when missing."""
    if spec.contract_spec is not None:
        validation = spec.policies.validation
        if validation is None or spec.contract_spec.validation is not None:
            return spec.contract_spec
        return msgspec.structs.replace(spec.contract_spec, validation=validation)
    return ContractSpec(
        name=spec.table_spec.name,
        table_schema=spec.table_spec,
        version=spec.table_spec.version,
        validation=spec.policies.validation,
    )


def dataset_spec_contract(spec: DatasetSpec) -> Contract:
    """Return runtime Contract derived from DatasetSpec."""
    return dataset_spec_contract_spec_or_default(spec).to_contract()


def dataset_spec_resolved_view_specs(spec: DatasetSpec) -> tuple[ViewSpec, ...]:
    """Return merged view specs with contract-level views taking precedence."""
    specs: list[ViewSpec] = []
    seen: set[str] = set()
    if spec.contract_spec is not None:
        for view in spec.contract_spec.view_specs:
            if view.name in seen:
                continue
            specs.append(view)
            seen.add(view.name)
    for view in spec.view_specs:
        if view.name in seen:
            continue
        specs.append(view)
        seen.add(view.name)
    return tuple(specs)


from schema_spec.dataset_runtime import (
    SCHEMA_EVOLUTION_PRESETS,
    dataset_spec_encoding_policy,
    dataset_spec_from_dataset,
    dataset_spec_from_path,
    dataset_spec_from_schema,
    dataset_table_column_defaults,
    dataset_table_constraints,
    dataset_table_ddl_fingerprint,
    dataset_table_definition,
    dataset_table_logical_plan,
    ddl_fingerprint_from_definition,
    resolve_schema_evolution_spec,
    schema_from_datafusion_catalog,
)


class ContractCatalogSpec(StructBaseStrict, frozen=True):
    """Collection of contract specifications keyed by name."""

    contracts: dict[str, ContractSpec]

    def __post_init__(self) -> None:
        """Validate that contract names match mapping keys.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        mismatched = [name for name, spec in self.contracts.items() if name != spec.name]
        if mismatched:
            msg = f"contract key mismatch: {mismatched}"
            raise ValueError(msg)

    def to_contracts(self) -> dict[str, ContractSpec]:
        """Return a name->spec mapping.

        Returns:
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
    "ContractRow",
    "ContractSpec",
    "ContractSpecKwargs",
    "DataFusionScanOptions",
    "DatasetKind",
    "DatasetOpenSpec",
    "DatasetPolicies",
    "DatasetSpec",
    "DatasetSpecKwargs",
    "DedupeSpecSpec",
    "DeltaCdfPolicy",
    "DeltaMaintenancePolicy",
    "DeltaPolicyBundle",
    "DeltaScanOptions",
    "DeltaScanPolicyDefaults",
    "DeltaSchemaPolicy",
    "DeltaWritePolicy",
    "ParquetColumnOptions",
    "ScanPolicyConfig",
    "ScanPolicyDefaults",
    "SortKeySpec",
    "TableSchemaContract",
    "TableSpecConstraints",
    "ValidationPolicySpec",
    "VirtualFieldSpec",
    "apply_delta_scan_policy",
    "apply_scan_policy",
    "dataset_spec_contract",
    "dataset_spec_contract_spec_or_default",
    "dataset_spec_datafusion_scan",
    "dataset_spec_delta_cdf_policy",
    "dataset_spec_delta_constraints",
    "dataset_spec_delta_feature_gate",
    "dataset_spec_delta_maintenance_policy",
    "dataset_spec_delta_scan",
    "dataset_spec_delta_schema_policy",
    "dataset_spec_delta_write_policy",
    "dataset_spec_encoding_policy",
    "dataset_spec_from_contract",
    "dataset_spec_from_dataset",
    "dataset_spec_from_path",
    "dataset_spec_from_schema",
    "dataset_spec_name",
    "dataset_spec_ordering",
    "dataset_spec_query",
    "dataset_spec_resolved_view_specs",
    "dataset_spec_schema",
    "dataset_spec_strict_schema_validation",
    "dataset_spec_with_delta_maintenance",
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
    "schema_from_datafusion_catalog",
    "table_spec_from_schema",
    "validate_arrow_table",
    "validation_policy_payload",
    "validation_policy_to_arrow_options",
]
