"""Compute UDF registry and capability helpers for ArrowDSL."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import DataTypeLike, pc
from arrowdsl.ops.catalog import Lane

type UdfFn = Callable[..., object]
type Volatility = Literal["immutable", "stable", "volatile"]


@dataclass(frozen=True)
class UdfSpec:
    """Specification for a scalar UDF registration."""

    name: str
    inputs: Mapping[str, DataTypeLike]
    output: DataTypeLike
    fn: UdfFn
    summary: str | None = None
    description: str | None = None

    def metadata(self) -> dict[str, str]:
        """Return metadata for UDF registration.

        Returns
        -------
        dict[str, str]
            Metadata mapping for the UDF.
        """
        meta: dict[str, str] = {}
        if self.summary:
            meta["summary"] = self.summary
        if self.description:
            meta["description"] = self.description
        return meta


@dataclass
class ComputeRegistry:
    """Registry for cached compute UDF registrations."""

    registered: set[str] = field(default_factory=set)

    def ensure(self, spec: UdfSpec) -> str:
        """Ensure a UDF is registered and return its name.

        Returns
        -------
        str
            Registered UDF name.
        """
        if spec.name in self.registered:
            return spec.name
        try:
            pc.get_function(spec.name)
        except KeyError:
            pc.register_scalar_function(
                spec.fn,
                spec.name,
                spec.metadata(),
                spec.inputs,
                spec.output,
            )
        self.registered.add(spec.name)
        return spec.name


@dataclass(frozen=True)
class FunctionCapability:
    """Capability metadata for compute functions."""

    name: str
    lane: Lane
    volatility: Volatility = "stable"


_DEFAULT_REGISTRY = ComputeRegistry()


def default_registry() -> ComputeRegistry:
    """Return the default compute registry.

    Returns
    -------
    ComputeRegistry
        Shared compute registry instance.
    """
    return _DEFAULT_REGISTRY


def ensure_udf(spec: UdfSpec) -> str:
    """Ensure a UDF is registered in the default registry.

    Returns
    -------
    str
        Registered UDF name.
    """
    return default_registry().ensure(spec)


def ensure_udfs(specs: Sequence[UdfSpec]) -> tuple[str, ...]:
    """Ensure a sequence of UDFs are registered in the default registry.

    Returns
    -------
    tuple[str, ...]
        Registered UDF names in input order.
    """
    return tuple(ensure_udf(spec) for spec in specs)


def resolve_function_capability(
    name: str,
    *,
    udf_spec: UdfSpec | None,
    ctx: ExecutionContext,
) -> FunctionCapability:
    """Resolve compute function capability for the runtime.

    Returns
    -------
    FunctionCapability
        Capability metadata describing the execution lane.
    """
    if ctx.runtime.datafusion is not None and udf_spec is None:
        return FunctionCapability(name=name, lane="datafusion")
    return FunctionCapability(name=name, lane="kernel")


__all__ = [
    "ComputeRegistry",
    "FunctionCapability",
    "UdfSpec",
    "default_registry",
    "ensure_udf",
    "ensure_udfs",
    "resolve_function_capability",
]
