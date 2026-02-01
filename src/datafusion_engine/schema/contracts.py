"""
Schema contract validation for DataFusion tables.

This module provides declarative schema contracts that can be validated
against introspection snapshots and used to generate DDL. Contracts enable
compile-time detection of schema drift and provide the foundation for
schema evolution policies.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Literal, cast

import msgspec
import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.schema.introspection import schema_from_table
from schema_spec.field_spec import FieldSpec
from schema_spec.specs import TableSchemaSpec
from schema_spec.system import ContractSpec, DatasetSpec, TableSchemaContract
from utils.registry_protocol import MutableRegistry, Registry
from validation.violations import ValidationViolation, ViolationType

SCHEMA_ABI_FINGERPRINT_META: bytes = b"schema_abi_fingerprint"

if TYPE_CHECKING:
    from datafusion_engine.catalog.introspection import IntrospectionSnapshot
    from datafusion_engine.dataset.registry import DatasetLocation
    from semantics.types import AnnotatedSchema


ConstraintType = Literal["pk", "not_null", "check", "unique"]


def normalize_column_names(values: Iterable[str] | None) -> tuple[str, ...]:
    """Return normalized column names in stable order.

    Returns
    -------
    tuple[str, ...]
        Normalized column names without duplicates.
    """
    if values is None:
        return ()
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        name = str(value).strip()
        if not name or name in seen:
            continue
        ordered.append(name)
        seen.add(name)
    return tuple(ordered)


def merge_constraint_expressions(*parts: Iterable[str]) -> tuple[str, ...]:
    """Return normalized constraint expressions from multiple sources.

    Returns
    -------
    tuple[str, ...]
        Normalized constraint expressions without duplicates.
    """
    merged: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for entry in part:
            normalized = str(entry).strip()
            if not normalized or normalized in seen:
                continue
            merged.append(normalized)
            seen.add(normalized)
    return tuple(merged)


class ConstraintSpec(msgspec.Struct, frozen=True):
    """Constraint specification for governance policies.

    Attributes
    ----------
    constraint_type : Literal["pk", "not_null", "check", "unique"]
        Constraint type identifier.
    column : str | None
        Single-column constraint target, when applicable.
    columns : tuple[str, ...] | None
        Multi-column constraint targets, when applicable.
    expression : str | None
        Optional SQL expression for CHECK constraints.
    """

    constraint_type: ConstraintType
    column: str | None = None
    columns: tuple[str, ...] | None = None
    expression: str | None = None

    def normalized_columns(self) -> tuple[str, ...]:
        """Return normalized column names from the constraint spec.

        Returns
        -------
        tuple[str, ...]
            Normalized column names for the constraint.
        """
        if self.columns:
            return normalize_column_names(self.columns)
        if self.column:
            return normalize_column_names((self.column,))
        return ()

    def normalized_expression(self) -> str | None:
        """Return a normalized expression string, if present.

        Returns
        -------
        str | None
            Normalized expression, or None if unavailable.
        """
        if self.expression is None:
            return None
        normalized = self.expression.strip()
        return normalized or None


class TableConstraints(msgspec.Struct, frozen=True):
    """Governance constraints for a table.

    Attributes
    ----------
    primary_key : tuple[str, ...] | None
        Ordered primary key column names.
    not_null : tuple[str, ...] | None
        Column names that must be non-null.
    checks : tuple[ConstraintSpec, ...] | None
        Structured CHECK constraints.
    unique : tuple[tuple[str, ...], ...] | None
        Unique constraint column groups.
    """

    primary_key: tuple[str, ...] | None = None
    not_null: tuple[str, ...] | None = None
    checks: tuple[ConstraintSpec, ...] | None = None
    unique: tuple[tuple[str, ...], ...] | None = None

    def required_non_null(self) -> tuple[str, ...]:
        """Return normalized non-null column names.

        Returns
        -------
        tuple[str, ...]
            Columns required to be non-null.
        """
        return normalize_column_names((*(self.not_null or ()), *(self.primary_key or ())))

    def check_expressions(self) -> tuple[str, ...]:
        """Return normalized check expressions.

        Returns
        -------
        tuple[str, ...]
            Check expressions for the constraint bundle.
        """
        expressions: list[str] = []
        for check in self.checks or ():
            if check.constraint_type != "check":
                continue
            normalized = check.normalized_expression()
            if normalized:
                expressions.append(normalized)
        return merge_constraint_expressions(expressions)


def table_constraints_from_spec(
    spec: TableSchemaSpec,
    *,
    checks: Iterable[str] = (),
    unique: Iterable[Iterable[str]] = (),
) -> TableConstraints:
    """Build a TableConstraints payload from a schema spec.

    Parameters
    ----------
    spec : TableSchemaSpec
        Table schema specification to translate.
    checks : Iterable[str]
        CHECK constraint expressions to include.
    unique : Iterable[Iterable[str]]
        Unique constraint column groups.

    Returns
    -------
    TableConstraints
        Constraint bundle derived from the schema spec.
    """
    primary_key = normalize_column_names(spec.key_fields)
    not_null = normalize_column_names(spec.required_non_null)
    check_specs = tuple(
        ConstraintSpec(constraint_type="check", expression=expression)
        for expression in merge_constraint_expressions(checks)
    )
    unique_specs: list[tuple[str, ...]] = []
    for entry in unique:
        normalized = normalize_column_names(entry)
        if normalized:
            unique_specs.append(normalized)
    return TableConstraints(
        primary_key=primary_key or None,
        not_null=not_null or None,
        checks=check_specs or None,
        unique=tuple(unique_specs) or None,
    )


def constraint_key_fields(constraints: TableConstraints) -> tuple[str, ...]:
    """Return key fields derived from constraint metadata.

    Parameters
    ----------
    constraints : TableConstraints
        Constraints bundle to inspect.

    Returns
    -------
    tuple[str, ...]
        Key fields resolved from primary or unique constraints.
    """
    if constraints.primary_key:
        return constraints.primary_key
    if constraints.unique:
        return constraints.unique[0]
    return ()


def delta_check_constraints(constraints: TableConstraints) -> tuple[str, ...]:
    """Return Delta CHECK constraint expressions for a constraint bundle.

    Parameters
    ----------
    constraints : TableConstraints
        Constraints bundle to translate.

    Returns
    -------
    tuple[str, ...]
        Delta CHECK constraint expressions.
    """
    not_null_exprs = tuple(f"{name} IS NOT NULL" for name in constraints.required_non_null())
    return merge_constraint_expressions(not_null_exprs, constraints.check_expressions())


def table_constraint_definitions(constraints: TableConstraints) -> tuple[str, ...]:
    """Return DDL-style constraint definitions for metadata snapshots.

    Parameters
    ----------
    constraints : TableConstraints
        Constraints bundle to translate.

    Returns
    -------
    tuple[str, ...]
        Constraint definitions for information_schema metadata.
    """
    definitions: list[str] = []
    primary_key = normalize_column_names(constraints.primary_key)
    if primary_key:
        definitions.append(f"PRIMARY KEY ({', '.join(primary_key)})")
    for unique_cols in constraints.unique or ():
        normalized = normalize_column_names(unique_cols)
        if normalized:
            definitions.append(f"UNIQUE ({', '.join(normalized)})")
    definitions.extend(delta_check_constraints(constraints))
    return merge_constraint_expressions(definitions)


def table_constraints_from_location(
    location: DatasetLocation | None,
    *,
    extra_checks: Iterable[str] = (),
) -> TableConstraints:
    """Resolve TableConstraints for a dataset location.

    Parameters
    ----------
    location : DatasetLocation | None
        Dataset location to inspect.
    extra_checks : Iterable[str]
        Additional CHECK constraint expressions to merge.

    Returns
    -------
    TableConstraints
        Resolved constraints for the dataset.
    """
    if location is None:
        return TableConstraints(
            checks=tuple(
                ConstraintSpec(constraint_type="check", expression=expression)
                for expression in merge_constraint_expressions(extra_checks)
            )
            or None,
        )
    dataset_spec = location.dataset_spec
    table_spec = dataset_spec.table_spec if dataset_spec is not None else location.table_spec
    resolved_checks: tuple[str, ...]
    if location.delta_constraints:
        resolved_checks = merge_constraint_expressions(location.delta_constraints, extra_checks)
    elif dataset_spec is not None:
        resolved_checks = merge_constraint_expressions(dataset_spec.delta_constraints, extra_checks)
    else:
        resolved_checks = merge_constraint_expressions(extra_checks)
    if table_spec is None:
        return TableConstraints(
            checks=tuple(
                ConstraintSpec(constraint_type="check", expression=expression)
                for expression in resolved_checks
            )
            or None,
        )
    return table_constraints_from_spec(table_spec, checks=resolved_checks)


def delta_constraints_for_location(
    location: DatasetLocation | None,
    *,
    extra_checks: Iterable[str] = (),
) -> tuple[str, ...]:
    """Return Delta CHECK constraints resolved from a dataset location.

    Parameters
    ----------
    location : DatasetLocation | None
        Dataset location to inspect.
    extra_checks : Iterable[str]
        Additional CHECK constraint expressions to merge.

    Returns
    -------
    tuple[str, ...]
        Delta CHECK constraint expressions.
    """
    return delta_check_constraints(
        table_constraints_from_location(location, extra_checks=extra_checks)
    )


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
    annotated_schema: AnnotatedSchema | None = None
    enforce_semantic_types: bool = False

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

        Returns
        -------
        SchemaContract
            Contract with annotated schema for semantic validation.
        """
        columns = tuple(
            FieldSpec(
                name=col.name,
                dtype=col.arrow_type,
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
    ) -> list[ValidationViolation]:
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

        Returns
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
    ) -> dict[str, list[ValidationViolation]]:
        """Validate all registered contracts.

        Returns
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

        Returns
        -------
        list[ValidationViolation]
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
    "ConstraintSpec",
    "ConstraintType",
    "ContractRegistry",
    "EvolutionPolicy",
    "SchemaContract",
    "TableConstraints",
    "ValidationViolation",
    "ViolationType",
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
