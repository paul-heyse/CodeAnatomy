"""
Schema contract validation for DataFusion tables.

This module provides declarative schema contracts that can be validated
against introspection snapshots and used to generate DDL. Contracts enable
compile-time detection of schema drift and provide the foundation for
schema evolution policies.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.schema_introspection import schema_from_table
from schema_spec.field_spec import FieldSpec
from schema_spec.system import ContractSpec, DatasetSpec, TableSchemaContract
from utils.registry_protocol import MutableRegistry, Registry

SCHEMA_ABI_FINGERPRINT_META: bytes = b"schema_abi_fingerprint"

if TYPE_CHECKING:
    from datafusion_engine.introspection import IntrospectionSnapshot


class SchemaViolationType(Enum):
    """Types of schema violations."""

    MISSING_COLUMN = auto()
    EXTRA_COLUMN = auto()
    TYPE_MISMATCH = auto()
    NULLABILITY_MISMATCH = auto()
    MISSING_TABLE = auto()
    METADATA_MISMATCH = auto()


@dataclass(frozen=True)
class SchemaViolation:
    """
    A single schema contract violation.

    Attributes
    ----------
    violation_type : SchemaViolationType
        Category of violation
    table_name : str
        Name of table with violation
    column_name : str | None
        Column name if violation is column-specific
    expected : str | None
        Expected value (type, nullability, etc.)
    actual : str | None
        Actual value found in catalog
    """

    violation_type: SchemaViolationType
    table_name: str
    column_name: str | None
    expected: str | None
    actual: str | None

    def __str__(self) -> str:
        """
        Format violation as human-readable message.

        Returns
        -------
        str
            Formatted violation description
        """
        message = f"Unknown violation: {self.violation_type}"
        if self.violation_type == SchemaViolationType.MISSING_TABLE:
            message = f"Table '{self.table_name}' not found"
        elif self.violation_type == SchemaViolationType.METADATA_MISMATCH:
            message = (
                f"Metadata mismatch for '{self.table_name}': "
                f"key={self.column_name!r} expected={self.expected!r} actual={self.actual!r}"
            )
        elif self.violation_type == SchemaViolationType.MISSING_COLUMN:
            message = f"Column '{self.table_name}.{self.column_name}' not found"
        elif self.violation_type == SchemaViolationType.EXTRA_COLUMN:
            message = f"Unexpected column '{self.table_name}.{self.column_name}'"
        elif self.violation_type == SchemaViolationType.TYPE_MISMATCH:
            message = (
                f"Type mismatch for '{self.table_name}.{self.column_name}': "
                f"expected {self.expected}, got {self.actual}"
            )
        elif self.violation_type == SchemaViolationType.NULLABILITY_MISMATCH:
            message = (
                f"Nullability mismatch for '{self.table_name}.{self.column_name}': "
                f"expected {self.expected}, got {self.actual}"
            )
        return message


class EvolutionPolicy(Enum):
    """Schema evolution policies."""

    STRICT = auto()  # No changes allowed
    ADDITIVE = auto()  # New columns allowed, no removals
    RELAXED = auto()  # Any compatible change allowed


def _decode_field_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    if not metadata:
        return {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in metadata.items()
    }


def _field_spec_from_arrow_field(field: pa.Field) -> FieldSpec:
    metadata = _decode_field_metadata(field.metadata)
    encoding_value = metadata.get("encoding")
    encoding = "dictionary" if encoding_value == "dictionary" else None
    return FieldSpec(
        name=field.name,
        dtype=field.type,
        nullable=field.nullable,
        metadata=metadata,
        default_value=metadata.get("default_value"),
        encoding=encoding,
    )


@dataclass(frozen=True)
class SchemaContract:
    """
    Declared schema contract for a dataset.

    Use this to define expected schemas and validate against
    actual catalog state. Contracts can be validated at compile time
    and used to generate DDL for table creation.

    Attributes
    ----------
    table_name : str
        Name of table this contract applies to
    columns : tuple[FieldSpec, ...]
        Column definitions
    partition_cols : tuple[str, ...]
        Partitioning column names
    ordering : tuple[str, ...]
        Ordering column names
    evolution_policy : EvolutionPolicy
        Schema evolution rules
    """

    table_name: str
    columns: tuple[FieldSpec, ...]
    partition_cols: tuple[str, ...] = ()
    ordering: tuple[str, ...] = ()
    evolution_policy: EvolutionPolicy = EvolutionPolicy.STRICT
    schema_metadata: dict[bytes, bytes] = field(default_factory=dict)
    enforce_columns: bool = True

    @classmethod
    def from_arrow_schema(
        cls,
        table_name: str,
        schema: pa.Schema,
        **kwargs: Any,
    ) -> SchemaContract:
        """
        Create from PyArrow schema.

        Parameters
        ----------
        table_name : str
            Name for table
        schema : pa.Schema
            PyArrow schema defining columns
        **kwargs
            Additional contract attributes (partition_cols, ordering, etc.)

        Returns
        -------
        SchemaContract
            Contract derived from schema
        """
        columns = tuple(_field_spec_from_arrow_field(field) for field in schema)
        metadata_override = kwargs.pop("schema_metadata", None)
        metadata = dict(schema.metadata or {})
        metadata.setdefault(
            SCHEMA_ABI_FINGERPRINT_META,
            schema_identity_hash(schema).encode("utf-8"),
        )
        if metadata_override:
            metadata.update(metadata_override)
        return cls(
            table_name=table_name,
            columns=columns,
            schema_metadata=metadata,
            **kwargs,
        )

    def to_arrow_schema(self) -> pa.Schema:
        """
        Convert to PyArrow schema.

        Returns
        -------
        pa.Schema
            PyArrow schema representation
        """
        metadata = self.schema_metadata or None
        return pa.schema([col.to_arrow_field() for col in self.columns], metadata=metadata)

    def validate_against_introspection(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> list[SchemaViolation]:
        """
        Validate contract against actual catalog state.

        Checks for missing/extra columns, type mismatches, and applies
        evolution policy rules.

        Parameters
        ----------
        snapshot : IntrospectionSnapshot
            Point-in-time catalog snapshot to validate against

        Returns
        -------
        list[SchemaViolation]
            List of violations (empty if valid)
        """
        violations: list[SchemaViolation] = []

        # Check table exists
        if not snapshot.table_exists(self.table_name):
            violations.append(
                SchemaViolation(
                    violation_type=SchemaViolationType.MISSING_TABLE,
                    table_name=self.table_name,
                    column_name=None,
                    expected=None,
                    actual=None,
                )
            )
            return violations

        if not self.enforce_columns:
            return violations

        # Get actual columns
        actual_cols = dict(snapshot.get_table_columns(self.table_name))

        expected_cols = {col.name: col for col in self.columns}

        # Check for missing columns
        for col_name, contract in expected_cols.items():
            if col_name not in actual_cols:
                violations.append(
                    SchemaViolation(
                        violation_type=SchemaViolationType.MISSING_COLUMN,
                        table_name=self.table_name,
                        column_name=col_name,
                        expected=str(contract.dtype),
                        actual=None,
                    )
                )
            else:
                # Check type compatibility
                actual_type = actual_cols[col_name]
                expected_type = self._arrow_type_to_sql(contract.dtype)
                if not self._types_compatible(expected_type, actual_type):
                    violations.append(
                        SchemaViolation(
                            violation_type=SchemaViolationType.TYPE_MISMATCH,
                            table_name=self.table_name,
                            column_name=col_name,
                            expected=expected_type,
                            actual=actual_type,
                        )
                    )

        # Check for extra columns (if strict policy)
        if self.evolution_policy == EvolutionPolicy.STRICT:
            for col_name, col_type in actual_cols.items():
                if col_name not in expected_cols:
                    violations.append(
                        SchemaViolation(
                            violation_type=SchemaViolationType.EXTRA_COLUMN,
                            table_name=self.table_name,
                            column_name=col_name,
                            expected=None,
                            actual=col_type,
                        )
                    )

        return violations

    def schema_from_catalog(self, ctx: SessionContext) -> pa.Schema:
        """
        Get Arrow schema from DataFusion catalog.

        Parameters
        ----------
        ctx : SessionContext
            DataFusion session context

        Returns
        -------
        pa.Schema
            Arrow schema resolved from catalog
        """
        return schema_from_table(ctx, self.table_name)

    @staticmethod
    def _arrow_type_to_sql(arrow_type: pa.DataType) -> str:
        """
        Convert Arrow type to SQL type string.

        Parameters
        ----------
        arrow_type : pa.DataType
            Arrow data type

        Returns
        -------
        str
            SQL type string
        """
        # Simplified mapping - extend as needed
        type_map = {
            pa.int64(): "Int64",
            pa.int32(): "Int32",
            pa.string(): "Utf8",
            pa.float64(): "Float64",
            pa.bool_(): "Boolean",
        }
        return type_map.get(arrow_type, str(arrow_type))

    @staticmethod
    def _types_compatible(expected: str, actual: str) -> bool:
        """
        Check if types are compatible.

        Parameters
        ----------
        expected : str
            Expected type string
        actual : str
            Actual type string from catalog

        Returns
        -------
        bool
            True if types are compatible
        """
        # Normalize for comparison
        expected_norm = expected.lower().replace(" ", "")
        actual_norm = actual.lower().replace(" ", "")
        return expected_norm == actual_norm


@dataclass
class ContractRegistry(Registry[str, SchemaContract]):
    """
    Registry of schema contracts for validation.

    Maintains a collection of schema contracts and provides
    batch validation against introspection snapshots.
    """

    registry: MutableRegistry[str, SchemaContract] = field(default_factory=MutableRegistry)

    def register(self, key: str, value: SchemaContract) -> None:
        """Register a schema contract by table name."""
        self.registry.register(key, value)

    def get(self, key: str) -> SchemaContract | None:
        """Return a schema contract when present.

        Returns
        -------
        SchemaContract | None
            Matching schema contract or None.
        """
        return self.registry.get(key)

    def __contains__(self, key: str) -> bool:
        """Return True when a contract is registered.

        Returns
        -------
        bool
            True when the key is registered.
        """
        return key in self.registry

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered contract names.

        Returns
        -------
        Iterator[str]
            Iterator over registered keys.
        """
        return iter(self.registry)

    def __len__(self) -> int:
        """Return the number of registered contracts.

        Returns
        -------
        int
            Number of registered contracts.
        """
        return len(self.registry)

    def items(self) -> Iterator[tuple[str, SchemaContract]]:
        """Iterate over contract entries.

        Returns
        -------
        Iterator[tuple[str, SchemaContract]]
            Iterator of registry items.
        """
        return self.registry.items()

    def snapshot(self) -> Mapping[str, SchemaContract]:
        """Return a snapshot of registered contracts.

        Returns
        -------
        Mapping[str, SchemaContract]
            Snapshot of current registry contents.
        """
        return self.registry.snapshot()

    def register_contract(self, contract: SchemaContract) -> None:
        """Register a schema contract by table name."""
        self.registry.register(contract.table_name, contract)

    def validate_all(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> dict[str, list[SchemaViolation]]:
        """Validate all registered contracts.

        Returns
        -------
        dict[str, list[SchemaViolation]]
            Mapping from table name to violations.
        """
        violations_dict: dict[str, list[SchemaViolation]] = {}
        for name, contract in self.registry.items():
            violations_dict[name] = contract.validate_against_introspection(snapshot)
        return violations_dict

    def get_violations(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> list[SchemaViolation]:
        """Get all violations across all contracts.

        Returns
        -------
        list[SchemaViolation]
            Flattened list of schema violations.
        """
        return [
            violation
            for _, contract in self.registry.items()
            for violation in contract.validate_against_introspection(snapshot)
        ]


def schema_contract_from_table_schema_contract(
    *,
    table_name: str,
    contract: TableSchemaContract,
    evolution_policy: EvolutionPolicy = EvolutionPolicy.STRICT,
    enforce_columns: bool = True,
) -> SchemaContract:
    """Build a SchemaContract from a TableSchemaContract.

    Parameters
    ----------
    table_name
        Name of the table the contract applies to.
    contract
        Table schema contract containing file schema and partition columns.
    evolution_policy
        Evolution policy for the contract.
    enforce_columns
        Whether to enforce column-level schema matching.

    Returns
    -------
    SchemaContract
        Schema contract constructed from the table schema contract.
    """
    columns = tuple(_field_spec_from_arrow_field(field) for field in contract.file_schema)
    partition_cols = tuple(name for name, _dtype in contract.partition_cols)
    if contract.partition_cols:
        partition_fields = tuple(
            FieldSpec(
                name=name,
                dtype=dtype,
                nullable=False,
            )
            for name, dtype in contract.partition_cols
        )
        columns = (*columns, *partition_fields)
    schema_metadata = dict(contract.file_schema.metadata or {})
    return SchemaContract(
        table_name=table_name,
        columns=columns,
        partition_cols=partition_cols,
        evolution_policy=evolution_policy,
        schema_metadata=schema_metadata,
        enforce_columns=enforce_columns,
    )


def _should_enforce_columns(spec: DatasetSpec) -> bool:
    if spec.view_specs:
        return False
    return spec.query_spec is None


def schema_contract_from_dataset_spec(
    *,
    name: str,
    spec: DatasetSpec,
    evolution_policy: EvolutionPolicy = EvolutionPolicy.STRICT,
    enforce_columns: bool | None = None,
) -> SchemaContract:
    """Build a SchemaContract from a DatasetSpec.

    Parameters
    ----------
    name
        Dataset name to bind into the contract.
    spec
        DatasetSpec providing schema and scan settings.
    evolution_policy
        Evolution policy for the contract.
    enforce_columns
        Override column enforcement; defaults to view/query-aware behavior.

    Returns
    -------
    SchemaContract
        Schema contract derived from the dataset specification.
    """
    table_schema = cast("pa.Schema", spec.schema())
    partition_cols = ()
    if spec.datafusion_scan is not None:
        partition_cols = spec.datafusion_scan.partition_cols
    table_contract = TableSchemaContract(
        file_schema=table_schema,
        partition_cols=partition_cols,
    )
    resolved_enforce = _should_enforce_columns(spec) if enforce_columns is None else enforce_columns
    return schema_contract_from_table_schema_contract(
        table_name=name,
        contract=table_contract,
        evolution_policy=evolution_policy,
        enforce_columns=resolved_enforce,
    )


def schema_contract_from_contract_spec(
    *,
    name: str,
    spec: ContractSpec,
    evolution_policy: EvolutionPolicy = EvolutionPolicy.STRICT,
    enforce_columns: bool = True,
) -> SchemaContract:
    """Build a SchemaContract from a ContractSpec.

    Parameters
    ----------
    name
        Dataset name to bind into the contract.
    spec
        ContractSpec providing a table schema specification.
    evolution_policy
        Evolution policy for the contract.
    enforce_columns
        Whether to enforce column-level schema matching.

    Returns
    -------
    SchemaContract
        Schema contract derived from the contract specification.
    """
    table_schema = spec.table_schema.to_arrow_schema()
    table_contract = TableSchemaContract(file_schema=table_schema, partition_cols=())
    return schema_contract_from_table_schema_contract(
        table_name=name,
        contract=table_contract,
        evolution_policy=evolution_policy,
        enforce_columns=enforce_columns,
    )


__all__ = [
    "ContractRegistry",
    "EvolutionPolicy",
    "SchemaContract",
    "SchemaViolation",
    "SchemaViolationType",
    "schema_contract_from_contract_spec",
    "schema_contract_from_dataset_spec",
    "schema_contract_from_table_schema_contract",
]
