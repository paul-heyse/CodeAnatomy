"""Parameter bridge helpers for Ibis execution."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

import ibis
import ibis.expr.datatypes as dt
from ibis.expr.types import BooleanValue, Table, Value

from ibis_engine.param_tables import scalar_param_signature

type JoinHow = Literal[
    "anti",
    "any_inner",
    "any_left",
    "asof",
    "cross",
    "inner",
    "left",
    "outer",
    "positional",
    "right",
    "semi",
]


@dataclass(frozen=True)
class ScalarParamSpec:
    """Scalar parameter specification for rule execution."""

    name: str
    dtype: str
    default: object | None = None
    required: bool = True

    def sql_type(self) -> str:
        """Return the DataFusion SQL type for the parameter.

        Returns
        -------
        str
            SQL type string suitable for PREPARE statements.
        """
        return _sql_type_from_dtype(ibis.dtype(self.dtype))


@dataclass
class IbisParamRegistry:
    """Registry for mapping param specs to Ibis parameter expressions."""

    params: dict[str, Value] = field(default_factory=dict)
    specs: dict[str, ScalarParamSpec] = field(default_factory=dict)

    def register(self, spec: ScalarParamSpec) -> Value:
        """Return (and register) the Ibis parameter for a spec.

        Returns
        -------
        Scalar
            Ibis parameter expression.

        Raises
        ------
        ValueError
            Raised when the spec name or dtype is empty.
        """
        if not spec.name:
            msg = "ParamSpec.name must be non-empty."
            raise ValueError(msg)
        if not spec.dtype:
            msg = f"ParamSpec {spec.name!r} requires a dtype."
            raise ValueError(msg)
        existing = self.params.get(spec.name)
        if existing is not None:
            return existing
        dtype = _normalize_param_dtype(spec)
        param = ibis.param(ibis.dtype(dtype)).name(spec.name)
        self.params[spec.name] = param
        self.specs[spec.name] = spec
        return param

    def bindings(self, values: Mapping[str, object]) -> dict[Value, object]:
        """Return Ibis bindings for provided parameter values.

        Returns
        -------
        dict[Scalar, object]
            Parameter bindings for Ibis execution.

        Raises
        ------
        ValueError
            Raised when required parameter values are missing.
        """
        resolved: dict[str, object] = dict(values)
        missing: list[str] = []
        for name, spec in self.specs.items():
            if name in resolved:
                continue
            if spec.default is not None:
                resolved[name] = spec.default
                continue
            if spec.required:
                missing.append(name)
        if missing:
            msg = f"Missing parameter values: {sorted(missing)}."
            raise ValueError(msg)
        return {self.params[name]: resolved[name] for name in self.params}


def specs_from_rel_ops(ops: Sequence[object]) -> tuple[ScalarParamSpec, ...]:
    """Extract parameter specs from a sequence of relational ops.

    Returns
    -------
    tuple[ParamSpec, ...]
        Parameter specs in encounter order.
    """
    specs: list[ScalarParamSpec] = []
    for op in ops:
        name = getattr(op, "name", None)
        dtype = getattr(op, "dtype", None)
        if not isinstance(name, str) or not isinstance(dtype, str):
            continue
        if _param_kind(dtype) != "scalar":
            continue
        specs.append(ScalarParamSpec(name=name, dtype=dtype))
    return tuple(specs)


def list_param_names_from_rel_ops(ops: Sequence[object]) -> tuple[str, ...]:
    """Return list-param names declared via ParamOp entries.

    Returns
    -------
    tuple[str, ...]
        List parameter names in encounter order.
    """
    names: list[str] = []
    for op in ops:
        name = getattr(op, "name", None)
        dtype = getattr(op, "dtype", None)
        if not isinstance(name, str) or not isinstance(dtype, str):
            continue
        if _param_kind(dtype) != "array":
            continue
        if name:
            names.append(name)
    return tuple(dict.fromkeys(names))


def _param_kind(dtype: str) -> Literal["scalar", "array"]:
    normalized = dtype.strip().lower()
    if normalized.startswith(("array<", "array[", "list<", "list[")):
        return "array"
    return "scalar"


def _normalize_param_dtype(spec: ScalarParamSpec) -> str:
    return spec.dtype


def registry_from_specs(specs: Iterable[ScalarParamSpec]) -> IbisParamRegistry:
    """Build an Ibis parameter registry from parameter specs.

    Returns
    -------
    IbisParamRegistry
        Registry populated with parameter specs.
    """
    registry = IbisParamRegistry()
    for spec in specs:
        registry.register(spec)
    return registry


def registry_from_ops(ops: Sequence[object]) -> IbisParamRegistry:
    """Build an Ibis parameter registry from relational ops.

    Returns
    -------
    IbisParamRegistry
        Registry populated with parameter specs.
    """
    return registry_from_specs(specs_from_rel_ops(ops))


def param_sql_types(specs: Iterable[ScalarParamSpec]) -> dict[str, str]:
    """Return SQL type mappings for parameter specs.

    Returns
    -------
    dict[str, str]
        Mapping of parameter name to SQL type string.
    """
    return {spec.name: spec.sql_type() for spec in specs}


def param_types_from_bindings(
    values: Mapping[str, object] | Mapping[Value, object],
) -> dict[str, str]:
    """Return SQL types inferred from parameter bindings.

    Returns
    -------
    dict[str, str]
        Mapping of parameter name to SQL type string.

    Raises
    ------
    ValueError
        Raised when a parameter name cannot be resolved.
    """
    resolved: dict[str, str] = {}
    for key, value in values.items():
        if isinstance(key, str):
            resolved[_normalize_sql_param_name(key)] = _sql_type_from_value(value)
            continue
        name = _param_name(key)
        if not name:
            msg = "Parameter expression is missing a stable name."
            raise ValueError(msg)
        resolved[name] = _sql_type_from_dtype(key.type())
    return resolved


def datafusion_param_bindings(
    values: Mapping[str, object] | Mapping[Value, object],
) -> dict[str, object]:
    """Return DataFusion parameter bindings for execution.

    Returns
    -------
    dict[str, object]
        DataFusion parameter bindings.

    Raises
    ------
    ValueError
        Raised when a parameter expression lacks a stable name.
    """
    bindings: dict[str, object] = {}
    for key, value in values.items():
        if isinstance(key, str):
            bindings[_normalize_sql_param_name(key)] = value
            continue
        name = _param_name(key)
        if not name:
            msg = "Parameter expression is missing a stable name."
            raise ValueError(msg)
        bindings[name] = value
    return bindings


def param_binding_signature(
    values: Mapping[str, object] | Mapping[Value, object] | None,
) -> str | None:
    """Return a signature for parameter bindings when available.

    Parameters
    ----------
    values:
        Parameter bindings keyed by name or Ibis values.

    Returns
    -------
    str | None
        Hex-encoded signature for parameter values, or ``None`` when missing.
    """
    if not values:
        return None
    try:
        bindings = datafusion_param_bindings(values)
    except ValueError:
        return None
    if not bindings:
        return None
    return scalar_param_signature(bindings)


def param_binding_mode(values: Mapping[str, object] | Mapping[Value, object] | None) -> str:
    """Return the parameter binding mode for diagnostics.

    Parameters
    ----------
    values:
        Parameter bindings keyed by name or Ibis values.

    Returns
    -------
    str
        ``"none"`` when no parameters are provided, otherwise ``"named"``,
        ``"typed"``, or ``"mixed"`` based on the binding keys.
    """
    if not values:
        return "none"
    keys = list(values.keys())
    if all(isinstance(key, str) for key in keys):
        return "named"
    if all(isinstance(key, Value) for key in keys):
        return "typed"
    return "mixed"


def list_param_join(
    table: Table,
    *,
    param_table: Table,
    left_col: str,
    right_col: str | None = None,
    options: JoinOptions | None = None,
) -> Table:
    """Return a semi-join against a list-parameter table.

    Returns
    -------
    ibis.expr.types.Table
        Filtered table constrained by param table membership.
    """
    key = right_col or left_col
    resolved = options or JoinOptions(how="semi")
    return stable_join(
        table,
        param_table,
        [table[left_col] == param_table[key]],
        options=resolved,
    )


@dataclass(frozen=True)
class JoinOptions:
    """Configuration for stable Ibis joins."""

    how: JoinHow = "inner"
    lname: str = ""
    rname: str = "{name}_r"


def stable_join(
    left: Table,
    right: Table,
    predicates: Sequence[BooleanValue],
    *,
    options: JoinOptions | None = None,
) -> Table:
    """Return a join with standardized collision handling.

    Returns
    -------
    ibis.expr.types.Table
        Joined table with collision-safe suffixes applied.
    """
    resolved = options or JoinOptions()
    return left.join(
        right,
        predicates,
        how=resolved.how,
        lname=resolved.lname,
        rname=resolved.rname,
    )


def _param_name(param: Value) -> str:
    getter = getattr(param, "get_name", None)
    if callable(getter):
        result = getter()
        return result if isinstance(result, str) else ""
    return ""


def _normalize_sql_param_name(name: str) -> str:
    return name.lstrip(":$")


def _sql_type_from_dtype(dtype: dt.DataType) -> str:
    mapping: list[tuple[Callable[[], bool], str]] = [
        (dtype.is_boolean, "BOOLEAN"),
        (dtype.is_int8, "TINYINT"),
        (dtype.is_int16, "SMALLINT"),
        (dtype.is_int32, "INT"),
        (dtype.is_int64, "BIGINT"),
        (dtype.is_uint8, "TINYINT UNSIGNED"),
        (dtype.is_uint16, "SMALLINT UNSIGNED"),
        (dtype.is_uint32, "INT UNSIGNED"),
        (dtype.is_uint64, "BIGINT UNSIGNED"),
        (dtype.is_float32, "FLOAT"),
        (dtype.is_float64, "DOUBLE"),
        (dtype.is_string, "VARCHAR"),
        (dtype.is_timestamp, "TIMESTAMP"),
        (dtype.is_date, "DATE"),
        (dtype.is_time, "TIME"),
    ]
    for predicate, sql_type in mapping:
        if predicate():
            return sql_type
    if isinstance(dtype, dt.Decimal):
        precision = dtype.precision or 38
        scale = dtype.scale or 0
        return f"DECIMAL({precision},{scale})"
    return str(dtype).upper()


def _sql_type_from_value(value: object) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, str):
        return "VARCHAR"
    if value is None:
        return "VARCHAR"
    return "VARCHAR"


ParamSpec = ScalarParamSpec


__all__ = [
    "IbisParamRegistry",
    "JoinOptions",
    "ParamSpec",
    "ScalarParamSpec",
    "datafusion_param_bindings",
    "list_param_join",
    "list_param_names_from_rel_ops",
    "param_binding_mode",
    "param_binding_signature",
    "param_sql_types",
    "param_types_from_bindings",
    "registry_from_ops",
    "registry_from_specs",
    "specs_from_rel_ops",
    "stable_join",
]
