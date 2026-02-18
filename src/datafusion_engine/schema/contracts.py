"""Schema contract validation for DataFusion tables.

This module provides declarative schema contracts that can be validated
against introspection snapshots and used to generate DDL. Contracts enable
compile-time detection of schema drift and provide the foundation for
schema evolution policies.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from datafusion import SessionContext

from core.config_base import config_fingerprint
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.schema.constraints import (
    ConstraintSpec,
    ConstraintType,
    TableConstraints,
    constraint_key_fields,
    delta_check_constraints,
    delta_constraints_for_location,
    merge_constraint_expressions,
    normalize_column_names,
    table_constraint_definitions,
    table_constraints_from_location,
    table_constraints_from_spec,
)
from datafusion_engine.schema.contract_builders import (
    field_spec_from_arrow_field as _field_spec_from_arrow_field,
)
from datafusion_engine.schema.contract_builders import (
    schema_contract_from_contract_spec,
    schema_contract_from_dataset_spec,
    schema_contract_from_table_schema_contract,
)
from datafusion_engine.schema.divergence import (
    SchemaDivergence,
    compute_schema_divergence,
)
from datafusion_engine.schema.introspection_core import schema_from_table
from datafusion_engine.schema.type_normalization import (
    normalize_type_string,
)
from datafusion_engine.schema.type_resolution import arrow_type_to_sql as _arrow_type_to_sql
from schema_spec.arrow_types import arrow_type_from_pyarrow
from schema_spec.field_spec import FieldSpec
from utils.registry_protocol import MutableRegistry, Registry
from validation.violations import ValidationViolation, ViolationType

SCHEMA_ABI_FINGERPRINT_META: bytes = b"schema_abi_fingerprint"

if TYPE_CHECKING:
    from datafusion_engine.catalog.introspection import IntrospectionSnapshot
    from semantics.types import AnnotatedSchema


class EvolutionPolicy(Enum):
    """Schema evolution policies."""

    STRICT = auto()  # No changes allowed
    ADDITIVE = auto()  # New columns allowed, no removals
    RELAXED = auto()  # Any compatible change allowed

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the evolution policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing the evolution policy.
        """
        return {"policy": self.name}

    def fingerprint(self) -> str:
        """Return fingerprint for the evolution policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())


@dataclass(frozen=True)
class SchemaContract:
    """Declared schema contract for a dataset.

    Use this to define expected schemas and validate against
    actual catalog state. Contracts can be validated at compile time
    and used to generate DDL for table creation.

    Attributes:
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
    annotated_schema: AnnotatedSchema | None = None
    enforce_semantic_types: bool = False

    @classmethod
    def from_arrow_schema(
        cls,
        table_name: str,
        schema: pa.Schema,
        **kwargs: Any,
    ) -> SchemaContract:
        """Create from PyArrow schema.

        Parameters
        ----------
        table_name : str
            Name for table
        schema : pa.Schema
            PyArrow schema defining columns
        **kwargs
            Additional contract attributes (partition_cols, ordering, etc.)

        Returns:
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

    @classmethod
    def from_annotated_schema(
        cls,
        table_name: str,
        annotated: AnnotatedSchema,
        *,
        enforce_columns: bool = False,
        enforce_semantic_types: bool = True,
    ) -> SchemaContract:
        """Create from AnnotatedSchema with semantic type enforcement.

        Parameters
        ----------
        table_name
            Name for table.
        annotated
            AnnotatedSchema providing columns with semantic types.
        enforce_columns
            Whether to enforce column presence and types.
        enforce_semantic_types
            Whether to enforce semantic type matching.

        Returns:
        -------
        SchemaContract
            Contract with annotated schema for semantic validation.
        """
        columns = tuple(
            FieldSpec(
                name=col.name,
                dtype=arrow_type_from_pyarrow(col.arrow_type),
                nullable=col.is_nullable,
            )
            for col in annotated
        )
        schema = annotated.to_arrow()
        metadata = dict(schema.metadata or {})
        metadata.setdefault(
            SCHEMA_ABI_FINGERPRINT_META,
            schema_identity_hash(schema).encode("utf-8"),
        )
        return cls(
            table_name=table_name,
            columns=columns,
            schema_metadata=metadata,
            enforce_columns=enforce_columns,
            annotated_schema=annotated,
            enforce_semantic_types=enforce_semantic_types,
        )

    def to_arrow_schema(self) -> pa.Schema:
        """Convert to PyArrow schema.

        Returns:
        -------
        pa.Schema
            PyArrow schema representation
        """
        metadata = self.schema_metadata or None
        return pa.schema([col.to_arrow_field() for col in self.columns], metadata=metadata)

    def validate_against_introspection(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> list[ValidationViolation]:
        """Validate contract against actual catalog state.

        Checks for missing/extra columns, type mismatches, and applies
        evolution policy rules.

        Parameters
        ----------
        snapshot : IntrospectionSnapshot
            Point-in-time catalog snapshot to validate against

        Returns:
        -------
        list[ValidationViolation]
            List of violations (empty if valid)
        """
        violations: list[ValidationViolation] = []

        # Check table exists
        if not snapshot.table_exists(self.table_name):
            violations.append(
                ValidationViolation(
                    violation_type=ViolationType.MISSING_TABLE,
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
                    ValidationViolation(
                        violation_type=ViolationType.MISSING_COLUMN,
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
                        ValidationViolation(
                            violation_type=ViolationType.TYPE_MISMATCH,
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
                        ValidationViolation(
                            violation_type=ViolationType.EXTRA_COLUMN,
                            table_name=self.table_name,
                            column_name=col_name,
                            expected=None,
                            actual=col_type,
                        )
                    )

        return violations

    def validate_against_schema(self, schema: pa.Schema) -> list[ValidationViolation]:
        """Validate the contract against an Arrow schema.

        Parameters
        ----------
        schema : pa.Schema
            Arrow schema to validate.

        Returns:
        -------
        list[ValidationViolation]
            List of violations (empty if valid).
        """
        if not self.enforce_columns:
            return []
        actual_cols, expected_cols = self._column_maps(schema)
        violations = self._column_violations(actual_cols, expected_cols)
        if self.evolution_policy == EvolutionPolicy.STRICT:
            violations.extend(self._extra_column_violations(actual_cols, expected_cols))
        violations.extend(self._metadata_violations(schema))
        violations.extend(self._semantic_type_violations(schema))
        return violations

    def _column_maps(self, schema: pa.Schema) -> tuple[dict[str, pa.Field], dict[str, FieldSpec]]:
        actual_cols = {field.name: field for field in schema}
        expected_cols = {col.name: col for col in self.columns}
        return actual_cols, expected_cols

    def _column_violations(
        self,
        actual_cols: Mapping[str, pa.Field],
        expected_cols: Mapping[str, FieldSpec],
    ) -> list[ValidationViolation]:
        violations: list[ValidationViolation] = []
        for col_name, contract in expected_cols.items():
            actual_field = actual_cols.get(col_name)
            if actual_field is None:
                violations.append(
                    ValidationViolation(
                        violation_type=ViolationType.MISSING_COLUMN,
                        table_name=self.table_name,
                        column_name=col_name,
                        expected=str(contract.dtype),
                        actual=None,
                    )
                )
                continue
            expected_type = self._arrow_type_to_sql(contract.dtype)
            actual_type = self._arrow_type_to_sql(actual_field.type)
            if not self._types_compatible(expected_type, actual_type):
                violations.append(
                    ValidationViolation(
                        violation_type=ViolationType.TYPE_MISMATCH,
                        table_name=self.table_name,
                        column_name=col_name,
                        expected=expected_type,
                        actual=actual_type,
                    )
                )
            if contract.nullable is False and actual_field.nullable is True:
                violations.append(
                    ValidationViolation(
                        violation_type=ViolationType.NULLABILITY_MISMATCH,
                        table_name=self.table_name,
                        column_name=col_name,
                        expected="non-nullable",
                        actual="nullable",
                    )
                )
        return violations

    def _extra_column_violations(
        self,
        actual_cols: Mapping[str, pa.Field],
        expected_cols: Mapping[str, FieldSpec],
    ) -> list[ValidationViolation]:
        violations: list[ValidationViolation] = []
        for col_name, actual_field in actual_cols.items():
            if col_name in expected_cols:
                continue
            violations.append(
                ValidationViolation(
                    violation_type=ViolationType.EXTRA_COLUMN,
                    table_name=self.table_name,
                    column_name=col_name,
                    expected=None,
                    actual=self._arrow_type_to_sql(actual_field.type),
                )
            )
        return violations

    def _metadata_violations(self, schema: pa.Schema) -> list[ValidationViolation]:
        expected_meta = self.schema_metadata
        if not expected_meta:
            return []
        actual_meta = schema.metadata or {}
        violations: list[ValidationViolation] = []
        for key, expected_value in expected_meta.items():
            if key not in actual_meta:
                violations.append(
                    ValidationViolation(
                        violation_type=ViolationType.METADATA_MISMATCH,
                        table_name=self.table_name,
                        column_name=key.decode("utf-8", errors="replace"),
                        expected=expected_value.decode("utf-8", errors="replace"),
                        actual=None,
                    )
                )
                continue
            actual_value = actual_meta.get(key)
            if actual_value == expected_value:
                continue
            actual_text = (
                actual_value.decode("utf-8", errors="replace")
                if isinstance(actual_value, (bytes, bytearray))
                else None
            )
            violations.append(
                ValidationViolation(
                    violation_type=ViolationType.METADATA_MISMATCH,
                    table_name=self.table_name,
                    column_name=key.decode("utf-8", errors="replace"),
                    expected=expected_value.decode("utf-8", errors="replace"),
                    actual=actual_text,
                )
            )
        return violations

    def _semantic_type_violations(self, schema: pa.Schema) -> list[ValidationViolation]:
        if not self.enforce_semantic_types or self.annotated_schema is None:
            return []
        from semantics.types import AnnotatedSchema, SemanticType

        expected = self.annotated_schema
        actual = AnnotatedSchema.from_arrow_schema(schema)
        violations: list[ValidationViolation] = []
        for column in expected:
            actual_col = actual.get(column.name)
            if actual_col is None:
                continue
            if column.semantic_type == SemanticType.UNKNOWN:
                continue
            if actual_col.semantic_type != column.semantic_type:
                violations.append(
                    ValidationViolation(
                        violation_type=ViolationType.SEMANTIC_TYPE_MISMATCH,
                        table_name=self.table_name,
                        column_name=column.name,
                        expected=column.semantic_type.value,
                        actual=actual_col.semantic_type.value,
                    )
                )
        return violations

    def schema_from_catalog(self, ctx: SessionContext) -> pa.Schema:
        """Get Arrow schema from DataFusion catalog.

        Parameters
        ----------
        ctx : SessionContext
            DataFusion session context

        Returns:
        -------
        pa.Schema
            Arrow schema resolved from catalog
        """
        return schema_from_table(ctx, self.table_name)

    @staticmethod
    def _arrow_type_to_sql(arrow_type: pa.DataType) -> str:
        """Convert Arrow type to SQL type string.

        Parameters
        ----------
        arrow_type : pa.DataType
            Arrow data type

        Returns:
        -------
        str
            SQL type string
        """
        return _arrow_type_to_sql(arrow_type)

    @staticmethod
    def _normalize_type_string(value: str) -> str:
        return normalize_type_string(value)

    @staticmethod
    def _types_compatible(expected: str, actual: str) -> bool:
        """Check if types are compatible.

        Parameters
        ----------
        expected : str
            Expected type string
        actual : str
            Actual type string from catalog

        Returns:
        -------
        bool
            True if types are compatible
        """
        expected_norm = SchemaContract._normalize_type_string(expected)
        actual_norm = SchemaContract._normalize_type_string(actual)
        return expected_norm == actual_norm


@dataclass
class ContractRegistry(Registry[str, SchemaContract]):
    """Registry of schema contracts for validation.

    Maintains a collection of schema contracts and provides
    batch validation against introspection snapshots.
    """

    registry: MutableRegistry[str, SchemaContract] = field(default_factory=MutableRegistry)

    def register(self, key: str, value: SchemaContract) -> None:
        """Register a schema contract by table name."""
        self.registry.register(key, value)

    def get(self, key: str) -> SchemaContract | None:
        """Return a schema contract when present.

        Returns:
        -------
        SchemaContract | None
            Matching schema contract or None.
        """
        return self.registry.get(key)

    def __contains__(self, key: str) -> bool:
        """Return True when a contract is registered.

        Returns:
        -------
        bool
            True when the key is registered.
        """
        return key in self.registry

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered contract names.

        Returns:
        -------
        Iterator[str]
            Iterator over registered keys.
        """
        return iter(self.registry)

    def __len__(self) -> int:
        """Return the number of registered contracts.

        Returns:
        -------
        int
            Number of registered contracts.
        """
        return len(self.registry)

    def items(self) -> Iterator[tuple[str, SchemaContract]]:
        """Iterate over contract entries.

        Returns:
        -------
        Iterator[tuple[str, SchemaContract]]
            Iterator of registry items.
        """
        return self.registry.items()

    def snapshot(self) -> Mapping[str, SchemaContract]:
        """Return a snapshot of registered contracts.

        Returns:
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
    ) -> dict[str, list[ValidationViolation]]:
        """Validate all registered contracts.

        Returns:
        -------
        dict[str, list[ValidationViolation]]
            Mapping from table name to violations.
        """
        violations_dict: dict[str, list[ValidationViolation]] = {}
        for name, contract in self.registry.items():
            violations_dict[name] = contract.validate_against_introspection(snapshot)
        return violations_dict

    def get_violations(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> list[ValidationViolation]:
        """Get all violations across all contracts.

        Returns:
        -------
        list[ValidationViolation]
            Flattened list of schema violations.
        """
        return [
            violation
            for _, contract in self.registry.items()
            for violation in contract.validate_against_introspection(snapshot)
        ]


__all__ = [
    "ConstraintSpec",
    "ConstraintType",
    "ContractRegistry",
    "EvolutionPolicy",
    "SchemaContract",
    "SchemaDivergence",
    "TableConstraints",
    "ValidationViolation",
    "ViolationType",
    "compute_schema_divergence",
    "constraint_key_fields",
    "delta_check_constraints",
    "delta_constraints_for_location",
    "merge_constraint_expressions",
    "normalize_column_names",
    "schema_contract_from_contract_spec",
    "schema_contract_from_dataset_spec",
    "schema_contract_from_table_schema_contract",
    "table_constraint_definitions",
    "table_constraints_from_location",
    "table_constraints_from_spec",
]
