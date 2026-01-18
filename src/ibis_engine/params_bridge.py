"""Parameter bridge helpers for Ibis execution."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

import ibis
from ibis.expr.types import BooleanValue, Table, Value

from relspec.rules.rel_ops import ParamOp, RelOpT

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


def specs_from_rel_ops(ops: Sequence[RelOpT]) -> tuple[ScalarParamSpec, ...]:
    """Extract parameter specs from a sequence of relational ops.

    Returns
    -------
    tuple[ParamSpec, ...]
        Parameter specs in encounter order.
    """
    specs: list[ScalarParamSpec] = []
    for op in ops:
        if not isinstance(op, ParamOp):
            continue
        if _param_kind(op.dtype) != "scalar":
            continue
        specs.append(ScalarParamSpec(name=op.name, dtype=op.dtype))
    return tuple(specs)


def list_param_names_from_rel_ops(ops: Sequence[RelOpT]) -> tuple[str, ...]:
    """Return list-param names declared via ParamOp entries.

    Returns
    -------
    tuple[str, ...]
        List parameter names in encounter order.
    """
    names: list[str] = []
    for op in ops:
        if not isinstance(op, ParamOp):
            continue
        if _param_kind(op.dtype) != "array":
            continue
        if op.name:
            names.append(op.name)
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


def registry_from_ops(ops: Sequence[RelOpT]) -> IbisParamRegistry:
    """Build an Ibis parameter registry from relational ops.

    Returns
    -------
    IbisParamRegistry
        Registry populated with parameter specs.
    """
    return registry_from_specs(specs_from_rel_ops(ops))


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
            bindings[key] = value
            continue
        name = _param_name(key)
        if not name:
            msg = "Parameter expression is missing a stable name."
            raise ValueError(msg)
        bindings[name] = value
    return bindings


def list_param_join(
    table: Table,
    *,
    param_table: Table,
    left_col: str,
    right_col: str | None = None,
) -> Table:
    """Return a semi-join against a list-parameter table.

    Returns
    -------
    ibis.expr.types.Table
        Filtered table constrained by param table membership.
    """
    key = right_col or left_col
    return stable_join(
        table,
        param_table,
        [table[left_col] == param_table[key]],
        options=JoinOptions(how="semi"),
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
    right_view = right.view()
    return left.join(
        right_view,
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


ParamSpec = ScalarParamSpec


__all__ = [
    "IbisParamRegistry",
    "JoinOptions",
    "ParamSpec",
    "ScalarParamSpec",
    "datafusion_param_bindings",
    "list_param_join",
    "list_param_names_from_rel_ops",
    "registry_from_ops",
    "registry_from_specs",
    "specs_from_rel_ops",
    "stable_join",
]
