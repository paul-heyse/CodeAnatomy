"""Parameter binding resolution and validation for DataFusion execution.

This module provides a security-focused parameter binding system that:
- Separates scalar parameters from table-like parameters
- Validates parameter names against an allowlist pattern
- Registers table parameters into SessionContext before execution
- Supports PyArrow tables, DataFrames, and scalar values
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from datafusion_engine.io_adapter import DataFusionIOAdapter

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext


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
    values: Mapping[str, Any] | Mapping[object, Any] | None,
    *,
    validate_names: bool = True,
    allowlist: Sequence[str] | None = None,
) -> DataFusionParamBindings:
    """Resolve parameter bindings into scalar vs table-like lanes.

    This function separates scalar parameters (for SQL substitution) from
    table-like parameters (for context registration).

    Parameters
    ----------
    values
        Mapping of parameter names to values.
        - Scalar values (int, str, float, etc.) → param_values
        - Table-like values (DataFrame, Table) → named_tables
    validate_names
        Whether to validate parameter names against allowlist.
    allowlist
        Optional allowlist of parameter names permitted for binding.

    Returns
    -------
    DataFusionParamBindings
        Resolved bindings ready for execution.

    Raises
    ------
    ValueError
        If a parameter name fails validation or is not allowlisted.
    """
    if values is None:
        return DataFusionParamBindings()

    param_values: dict[str, Any] = {}
    named_tables: dict[str, Any] = {}

    for key, value in values.items():
        name = str(key)
        get_name = getattr(key, "get_name", None)
        if callable(get_name):
            candidate = get_name()
            if isinstance(candidate, str):
                name = candidate

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
        _register_table_like(ctx, name, table)


@contextlib.contextmanager
def register_table_params(
    ctx: SessionContext,
    bindings: DataFusionParamBindings,
) -> Iterator[None]:
    """Register table-like bindings into context with automatic cleanup.

    Parameters
    ----------
    ctx
        DataFusion SessionContext to register tables into.
    bindings
        Resolved parameter bindings containing named tables.
    """
    if not bindings.named_tables:
        yield
        return
    registered: list[str] = []
    try:
        for name, table in bindings.named_tables.items():
            _ensure_table_slot(ctx, name)
            _register_table_like(ctx, name, table)
            registered.append(name)
        yield
    finally:
        from datafusion_engine.io_adapter import DataFusionIOAdapter

        adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
        for name in registered:
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                adapter.deregister_table(name)


def _ensure_table_slot(ctx: SessionContext, name: str) -> None:
    try:
        ctx.table(name)
    except (KeyError, RuntimeError, TypeError, ValueError):
        return
    msg = f"Named parameter {name!r} collides with an existing table."
    raise ValueError(msg)


def _register_table_like(ctx: SessionContext, name: str, table: object) -> None:
    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    if isinstance(table, pa.Table):
        adapter.register_arrow_table(name, table)
        return
    to_pyarrow = getattr(table, "to_pyarrow", None)
    if callable(to_pyarrow):
        adapter.register_arrow_table(name, to_pyarrow())
        return
    adapter.register_table_provider(name, table)
