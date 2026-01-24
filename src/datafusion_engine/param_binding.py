"""Parameter binding resolution and validation for DataFusion execution.

This module provides a security-focused parameter binding system that:
- Separates scalar parameters from table-like parameters
- Validates parameter names against an allowlist pattern
- Registers table parameters into SessionContext before execution
- Supports PyArrow tables, DataFrames, and scalar values
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext
    from ibis.expr.types import Value


@dataclass(frozen=True)
class DataFusionParamBindings:
    """Resolved parameter bindings for SQL execution.

    Parameters
    ----------
    param_values
        Scalar parameter values (int, str, float, etc.) for SQL substitution.
    named_tables
        Table-like parameters (DataFrame, PyArrow Table) to register in context.
    """

    param_values: dict[str, Any] = field(default_factory=dict)
    named_tables: dict[str, DataFrame | pa.Table] = field(default_factory=dict)

    def merge(self, other: DataFusionParamBindings) -> DataFusionParamBindings:
        """Merge two binding sets, raising on conflicts.

        Parameters
        ----------
        other
            Another binding set to merge with this one.

        Returns
        -------
        DataFusionParamBindings
            New bindings with combined parameters and tables.

        Raises
        ------
        ValueError
            If there are overlapping parameter names in either scalar or table params.
        """
        overlapping = set(self.param_values) & set(other.param_values)
        if overlapping:
            msg = f"Conflicting scalar params: {overlapping}"
            raise ValueError(msg)

        overlapping_tables = set(self.named_tables) & set(other.named_tables)
        if overlapping_tables:
            msg = f"Conflicting table params: {overlapping_tables}"
            raise ValueError(msg)

        return DataFusionParamBindings(
            param_values={**self.param_values, **other.param_values},
            named_tables={**self.named_tables, **other.named_tables},
        )


# Allowlist for parameter names (security boundary)
ALLOWED_PARAM_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_param_name(name: str) -> None:
    """Validate parameter name against allowlist.

    Parameters
    ----------
    name
        Parameter name to validate.

    Raises
    ------
    ValueError
        If parameter name does not match the allowlist pattern [a-zA-Z_][a-zA-Z0-9_]*.
    """
    if not ALLOWED_PARAM_PATTERN.match(name):
        msg = f"Invalid parameter name '{name}': must match [a-zA-Z_][a-zA-Z0-9_]*"
        raise ValueError(msg)


def resolve_param_bindings(
    values: Mapping[str, Any] | Mapping[Value, Any] | None,
    *,
    validate_names: bool = True,
    allowlist: Sequence[str] | None = None,
) -> DataFusionParamBindings:
    """Resolve parameter bindings into scalar vs table-like lanes.

    This function separates scalar parameters (for SQL substitution) from
    table-like parameters (for context registration). It supports both
    string keys and Ibis scalar parameter expressions.

    Parameters
    ----------
    values
        Mapping of parameter names/expressions to values.
        - Scalar values (int, str, float, etc.) → param_values
        - Table-like values (DataFrame, Table) → named_tables
    validate_names
        Whether to validate parameter names against allowlist.

    Returns
    -------
    DataFusionParamBindings
        Resolved bindings ready for execution.
    """
    if values is None:
        return DataFusionParamBindings()

    param_values: dict[str, Any] = {}
    named_tables: dict[str, Any] = {}

    for key, value in values.items():
        # Extract name from Ibis scalar param or use string key
        name = (
            key.get_name()  # type: ignore[union-attr]
            if hasattr(key, "get_name")
            else str(key)
        )

        if validate_names:
            validate_param_name(name)
        if allowlist is not None:
            allowed = set(allowlist)
            if name not in allowed:
                msg = f"Parameter name '{name}' is not allowlisted."
                raise ValueError(msg)

        # Route based on value type
        if isinstance(value, (pa.Table, pa.RecordBatch)):
            named_tables[name] = value
        elif hasattr(value, "to_pyarrow"):
            # DataFrame-like with Arrow export
            named_tables[name] = value
        else:
            # Scalar value
            param_values[name] = value

    return DataFusionParamBindings(
        param_values=param_values,
        named_tables=named_tables,
    )


def apply_bindings_to_context(
    ctx: SessionContext,
    bindings: DataFusionParamBindings,
) -> None:
    """Register table-like bindings into context before execution.

    This function takes all table-like parameters and registers them
    as named tables in the DataFusion SessionContext, making them
    available for SQL queries.

    Parameters
    ----------
    ctx
        DataFusion SessionContext to register tables into.
    bindings
        Resolved parameter bindings containing named tables.
    """
    for name, table in bindings.named_tables.items():
        if isinstance(table, pa.Table):
            ctx.register_table(name, table)
        elif hasattr(table, "to_pyarrow"):
            # Type guard: table has to_pyarrow method
            ctx.register_table(name, table.to_pyarrow())  # type: ignore[union-attr]
        else:
            ctx.register_table(name, table)
