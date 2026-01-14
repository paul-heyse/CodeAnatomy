"""Parameter bridge helpers for Ibis execution."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

import ibis
from ibis.expr.types import Value

from relspec.rules.rel_ops import ParamOp, RelOpT


@dataclass(frozen=True)
class ParamSpec:
    """Parameter specification for rule execution."""

    name: str
    dtype: str
    kind: Literal["scalar", "array"] = "scalar"


@dataclass
class IbisParamRegistry:
    """Registry for mapping param specs to Ibis parameter expressions."""

    params: dict[str, Value] = field(default_factory=dict)

    def register(self, spec: ParamSpec) -> Value:
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
        missing = [name for name in self.params if name not in values]
        if missing:
            msg = f"Missing parameter values: {sorted(missing)}."
            raise ValueError(msg)
        return {self.params[name]: values[name] for name in self.params}


def specs_from_rel_ops(ops: Sequence[RelOpT]) -> tuple[ParamSpec, ...]:
    """Extract parameter specs from a sequence of relational ops.

    Returns
    -------
    tuple[ParamSpec, ...]
        Parameter specs in encounter order.
    """
    return tuple(
        ParamSpec(name=op.name, dtype=op.dtype, kind=_param_kind(op.dtype))
        for op in ops
        if isinstance(op, ParamOp)
    )


def _param_kind(dtype: str) -> Literal["scalar", "array"]:
    normalized = dtype.strip().lower()
    if normalized.startswith(("array<", "array[", "list<", "list[")):
        return "array"
    return "scalar"


def _normalize_param_dtype(spec: ParamSpec) -> str:
    if spec.kind != "array":
        return spec.dtype
    normalized = spec.dtype.strip()
    lowered = normalized.lower()
    if lowered.startswith(("array<", "array[", "list<", "list[")):
        return normalized
    return f"array<{normalized}>"


def registry_from_specs(specs: Iterable[ParamSpec]) -> IbisParamRegistry:
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


def datafusion_param_bindings(values: Mapping[str, object]) -> dict[str, object]:
    """Return DataFusion parameter bindings for execution.

    Returns
    -------
    dict[str, object]
        DataFusion parameter bindings.
    """
    return dict(values)


__all__ = [
    "IbisParamRegistry",
    "ParamSpec",
    "datafusion_param_bindings",
    "registry_from_ops",
    "registry_from_specs",
    "specs_from_rel_ops",
]
