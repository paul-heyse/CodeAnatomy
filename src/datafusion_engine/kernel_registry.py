"""Kernel selection helpers for DataFusion-native execution."""

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from datafusion_engine.kernels import datafusion_kernel_registry

KernelFn = Callable[..., TableLike]


class KernelLane(StrEnum):
    """Execution lane for a kernel."""

    BUILTIN = "builtin"


@dataclass(frozen=True)
class KernelDef:
    """Kernel definition metadata."""

    name: str
    lane: KernelLane
    volatility: Literal["stable", "volatile", "immutable"] = "stable"
    requires_ordering: bool = False
    impl_name: str = ""

    def __post_init__(self) -> None:
        """Normalize the implementation name when unspecified."""
        if not self.impl_name:
            object.__setattr__(self, "impl_name", self.name)


KERNEL_REGISTRY: dict[str, KernelDef] = {
    "interval_align": KernelDef(
        name="interval_align",
        lane=KernelLane.BUILTIN,
        volatility="stable",
        requires_ordering=True,
    ),
    "explode_list": KernelDef(
        name="explode_list",
        lane=KernelLane.BUILTIN,
        volatility="stable",
        requires_ordering=False,
    ),
    "dedupe": KernelDef(
        name="dedupe",
        lane=KernelLane.BUILTIN,
        volatility="stable",
        requires_ordering=True,
    ),
    "winner_select": KernelDef(
        name="winner_select",
        lane=KernelLane.BUILTIN,
        volatility="stable",
        requires_ordering=True,
    ),
}


def kernel_def(name: str) -> KernelDef:
    """Return the kernel definition for the given name.

    Returns
    -------
    KernelDef
        Kernel definition metadata.

    Raises
    ------
    KeyError
        Raised when the kernel name is unknown.
    """
    try:
        return KERNEL_REGISTRY[name]
    except KeyError as exc:
        msg = f"Unknown kernel definition: {name!r}."
        raise KeyError(msg) from exc


@dataclass(frozen=True)
class KernelCapability:
    """Capability metadata for kernel selection."""

    name: str
    lane: KernelLane
    volatility: Literal["stable", "volatile", "immutable"] = "stable"
    requires_ordering: bool = False
    available: bool = True


def kernel_capability(name: str, *, ctx: ExecutionContext) -> KernelCapability:
    """Return kernel capability metadata for a runtime context.

    Returns
    -------
    KernelCapability
        Capability metadata describing lane and availability.
    """
    definition = kernel_def(name)
    registry = datafusion_kernel_registry()
    available = ctx.runtime.datafusion is not None and name in registry
    return KernelCapability(
        name=definition.name,
        lane=definition.lane,
        volatility=definition.volatility,
        requires_ordering=definition.requires_ordering,
        available=available,
    )


def resolve_kernel(name: str, *, ctx: ExecutionContext) -> KernelFn:
    """Return the DataFusion kernel implementation for the runtime.

    Returns
    -------
    KernelFn
        Kernel implementation for the requested name.

    Raises
    ------
    ValueError
        Raised when DataFusion is unavailable.
    KeyError
        Raised when the kernel name is unknown.
    """
    if ctx.runtime.datafusion is None:
        msg = "DataFusion runtime is required for kernel execution."
        raise ValueError(msg)
    registry = datafusion_kernel_registry()
    if name not in registry:
        msg = f"Unknown kernel name: {name!r}."
        raise KeyError(msg)
    return functools.partial(registry[name], _ctx=ctx)


__all__ = [
    "KERNEL_REGISTRY",
    "KernelCapability",
    "KernelDef",
    "KernelFn",
    "KernelLane",
    "kernel_capability",
    "kernel_def",
    "resolve_kernel",
]
