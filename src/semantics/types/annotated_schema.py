"""Annotated schema with semantic type information.

Provides schema annotations that overlay semantic information onto
Arrow or DataFusion schemas. This enables programmatic join inference
and validation based on semantic types rather than column names.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from semantics.types.core import (
    STANDARD_COLUMNS,
    CompatibilityGroup,
    SemanticType,
    get_compatibility_groups,
    infer_semantic_type,
)

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import DataFrame


@dataclass(frozen=True)
class AnnotatedColumn:
    """A column with semantic type annotation.

    Attributes:
    ----------
    name
        Column name.
    arrow_type
        PyArrow data type.
    semantic_type
        Inferred or explicit semantic type.
    compatibility_groups
        Groups this column can participate in for joins.
    is_nullable
        Whether the column allows null values.
    """

    name: str
    arrow_type: pa.DataType
    semantic_type: SemanticType
    compatibility_groups: tuple[CompatibilityGroup, ...] = ()
    is_nullable: bool = True


@dataclass
class AnnotatedSchema:
    """Schema with semantic type annotations.

    Wraps an Arrow schema with semantic type information for each column.
    Provides methods for querying columns by semantic type or compatibility
    group, enabling programmatic join inference.

    Attributes:
    ----------
    columns
        Tuple of annotated columns.
    """

    columns: tuple[AnnotatedColumn, ...]
    _by_name: dict[str, AnnotatedColumn] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        """Build the name lookup index."""
        object.__setattr__(self, "_by_name", {c.name: c for c in self.columns})

    def __getitem__(self, name: str) -> AnnotatedColumn:
        """Get column by name.

        Parameters
        ----------
        name
            Column name.

        Returns:
        -------
        AnnotatedColumn
            Annotated column.
        """
        return self._by_name[name]

    def __contains__(self, name: str) -> bool:
        """Check if column exists.

        Parameters
        ----------
        name
            Column name.

        Returns:
        -------
        bool
            True if column exists.
        """
        return name in self._by_name

    def __len__(self) -> int:
        """Return number of columns.

        Returns:
        -------
        int
            Number of columns in the schema.
        """
        return len(self.columns)

    def __iter__(self) -> Iterator[AnnotatedColumn]:
        """Iterate over columns.

        Returns:
        -------
        Iterator[AnnotatedColumn]
            Iterator over AnnotatedColumn instances.
        """
        return iter(self.columns)

    @property
    def column_names(self) -> tuple[str, ...]:
        """Get all column names.

        Returns:
        -------
        tuple[str, ...]
            Column names in order.
        """
        return tuple(c.name for c in self.columns)

    def get(self, name: str, default: AnnotatedColumn | None = None) -> AnnotatedColumn | None:
        """Get column by name with default.

        Parameters
        ----------
        name
            Column name.
        default
            Default value if not found.

        Returns:
        -------
        AnnotatedColumn | None
            Annotated column or default.
        """
        return self._by_name.get(name, default)

    def columns_by_semantic_type(self, sem_type: SemanticType) -> tuple[AnnotatedColumn, ...]:
        """Get columns matching a semantic type.

        Parameters
        ----------
        sem_type
            Semantic type to filter by.

        Returns:
        -------
        tuple[AnnotatedColumn, ...]
            Columns with the specified semantic type.
        """
        return tuple(c for c in self.columns if c.semantic_type == sem_type)

    def columns_by_compatibility_group(
        self, group: CompatibilityGroup
    ) -> tuple[AnnotatedColumn, ...]:
        """Get columns in a compatibility group.

        Parameters
        ----------
        group
            Compatibility group to filter by.

        Returns:
        -------
        tuple[AnnotatedColumn, ...]
            Columns in the specified group.
        """
        return tuple(c for c in self.columns if group in c.compatibility_groups)

    def join_key_columns(self) -> tuple[AnnotatedColumn, ...]:
        """Get columns that are join keys.

        Returns:
        -------
        tuple[AnnotatedColumn, ...]
            Columns marked as join keys in standard columns.
        """
        result = []
        for col in self.columns:
            if col.name in STANDARD_COLUMNS:
                spec = STANDARD_COLUMNS[col.name]
                if spec.is_join_key:
                    result.append(col)
        return tuple(result)

    def partition_key_columns(self) -> tuple[AnnotatedColumn, ...]:
        """Get columns that are partition keys.

        Returns:
        -------
        tuple[AnnotatedColumn, ...]
            Columns marked as partition keys in standard columns.
        """
        result = []
        for col in self.columns:
            if col.name in STANDARD_COLUMNS:
                spec = STANDARD_COLUMNS[col.name]
                if spec.is_partition_key:
                    result.append(col)
        return tuple(result)

    def has_semantic_type(self, sem_type: SemanticType) -> bool:
        """Check if schema has any column with the semantic type.

        Parameters
        ----------
        sem_type
            Semantic type to check for.

        Returns:
        -------
        bool
            True if any column has the semantic type.
        """
        return any(c.semantic_type == sem_type for c in self.columns)

    def has_compatibility_group(self, group: CompatibilityGroup) -> bool:
        """Check if schema has any column in the compatibility group.

        Parameters
        ----------
        group
            Compatibility group to check for.

        Returns:
        -------
        bool
            True if any column is in the group.
        """
        return any(group in c.compatibility_groups for c in self.columns)

    def common_compatibility_groups(self, other: AnnotatedSchema) -> set[CompatibilityGroup]:
        """Find compatibility groups shared between schemas.

        Parameters
        ----------
        other
            Another annotated schema.

        Returns:
        -------
        set[CompatibilityGroup]
            Groups present in both schemas.
        """
        self_groups: set[CompatibilityGroup] = set()
        for col in self.columns:
            self_groups.update(col.compatibility_groups)

        other_groups: set[CompatibilityGroup] = set()
        for col in other.columns:
            other_groups.update(col.compatibility_groups)

        return self_groups & other_groups

    def infer_join_keys(self, other: AnnotatedSchema) -> list[tuple[str, str]]:
        """Infer potential join keys between two schemas.

        Uses compatibility groups to find columns that can be joined.

        Parameters
        ----------
        other
            Another annotated schema.

        Returns:
        -------
        list[tuple[str, str]]
            List of (self_column, other_column) pairs that can be joined.
        """
        common_groups = self.common_compatibility_groups(other)
        if not common_groups:
            return []

        join_pairs: list[tuple[str, str]] = []
        for group in common_groups:
            self_cols = self.columns_by_compatibility_group(group)
            other_cols = other.columns_by_compatibility_group(group)
            for self_col in self_cols:
                for other_col in other_cols:
                    # Prefer exact name matches
                    if self_col.name == other_col.name:
                        join_pairs.insert(0, (self_col.name, other_col.name))
                    else:
                        join_pairs.append((self_col.name, other_col.name))
        return join_pairs

    @classmethod
    def from_arrow_schema(cls, schema: pa.Schema) -> AnnotatedSchema:
        """Build annotated schema from Arrow schema.

        Parameters
        ----------
        schema
            PyArrow schema.

        Returns:
        -------
        AnnotatedSchema
            Annotated schema with inferred semantic types.
        """
        columns = []
        for arrow_field in schema:
            sem_type = infer_semantic_type(arrow_field.name)
            compat_groups = get_compatibility_groups(arrow_field.name)
            columns.append(
                AnnotatedColumn(
                    name=arrow_field.name,
                    arrow_type=arrow_field.type,
                    semantic_type=sem_type,
                    compatibility_groups=compat_groups,
                    is_nullable=arrow_field.nullable,
                )
            )
        return cls(columns=tuple(columns))

    @classmethod
    def from_dataframe(cls, df: DataFrame) -> AnnotatedSchema:
        """Build annotated schema from DataFusion DataFrame.

        Parameters
        ----------
        df
            DataFusion DataFrame.

        Returns:
        -------
        AnnotatedSchema
            Annotated schema with inferred semantic types.
        """
        return cls.from_arrow_schema(df.schema())

    def to_arrow(self) -> pa.Schema:
        """Return the PyArrow schema representation.

        Returns:
        -------
        pa.Schema
            Arrow schema derived from annotated columns.
        """
        import pyarrow as pa

        return pa.schema(
            [
                pa.field(
                    column.name,
                    column.arrow_type,
                    nullable=column.is_nullable,
                )
                for column in self.columns
            ]
        )


__all__ = [
    "AnnotatedColumn",
    "AnnotatedSchema",
]
