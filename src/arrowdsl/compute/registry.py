"""Compute UDF registry and capability helpers for ArrowDSL."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from arrowdsl.core.interop import DataTypeLike, pc

if TYPE_CHECKING:
    from arrowdsl.core.context import ExecutionContext

type UdfFn = Callable[..., object]
type KernelFn = Callable[..., object]
type Volatility = Literal["immutable", "stable", "volatile"]
type Lane = Literal["datafusion", "acero", "kernel"]


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


@dataclass(frozen=True)
class KernelSpec:
    """Specification for registering Arrow compute kernels."""

    name: str
    inputs: Mapping[str, DataTypeLike]
    output: DataTypeLike
    fn: KernelFn
    summary: str | None = None
    description: str | None = None

    def metadata(self) -> dict[str, str]:
        """Return metadata for kernel registration.

        Returns
        -------
        dict[str, str]
            Metadata mapping for the kernel.
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


@dataclass
class KernelRegistry:
    """Registry for Arrow compute kernel registrations."""

    registered: set[str] = field(default_factory=set)

    def ensure(self, spec: KernelSpec) -> str:
        """Ensure a kernel is registered and return its name.

        Returns
        -------
        str
            Registered kernel name.
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
_DEFAULT_KERNEL_REGISTRY = KernelRegistry()


def default_registry() -> ComputeRegistry:
    """Return the default compute registry.

    Returns
    -------
    ComputeRegistry
        Shared compute registry instance.
    """
    return _DEFAULT_REGISTRY


def default_kernel_registry() -> KernelRegistry:
    """Return the default kernel registry.

    Returns
    -------
    KernelRegistry
        Shared kernel registry instance.
    """
    return _DEFAULT_KERNEL_REGISTRY


def ensure_udf(spec: UdfSpec) -> str:
    """Ensure a UDF is registered in the default registry.

    Returns
    -------
    str
        Registered UDF name.
    """
    return default_registry().ensure(spec)


def ensure_kernel(spec: KernelSpec) -> str:
    """Ensure a kernel is registered in the default registry.

    Returns
    -------
    str
        Registered kernel name.
    """
    return default_kernel_registry().ensure(spec)


def ensure_udfs(specs: Sequence[UdfSpec]) -> tuple[str, ...]:
    """Ensure a sequence of UDFs are registered in the default registry.

    Returns
    -------
    tuple[str, ...]
        Registered UDF names in input order.
    """
    return tuple(ensure_udf(spec) for spec in specs)


def ensure_kernels(specs: Sequence[KernelSpec]) -> tuple[str, ...]:
    """Ensure a sequence of kernels are registered in the default registry.

    Returns
    -------
    tuple[str, ...]
        Registered kernel names in input order.
    """
    return tuple(ensure_kernel(spec) for spec in specs)


def resolve_function_capability(
    name: str,
    *,
    udf_spec: UdfSpec | None,
    kernel_spec: KernelSpec | None = None,
    ctx: ExecutionContext,
) -> FunctionCapability:
    """Resolve compute function capability for the runtime.

    Returns
    -------
    FunctionCapability
        Capability metadata describing the execution lane.
    """
    if kernel_spec is not None:
        return FunctionCapability(name=name, lane="kernel")
    try:
        pc.get_function(name)
        return FunctionCapability(name=name, lane="kernel")
    except KeyError:
        pass
    if ctx.runtime.datafusion is not None and udf_spec is None:
        return FunctionCapability(name=name, lane="datafusion")
    return FunctionCapability(name=name, lane="kernel")


__all__ = [
    "ComputeRegistry",
    "FunctionCapability",
    "KernelRegistry",
    "KernelSpec",
    "UdfSpec",
    "default_kernel_registry",
    "default_registry",
    "ensure_kernel",
    "ensure_kernels",
    "ensure_udf",
    "ensure_udfs",
    "resolve_function_capability",
]
