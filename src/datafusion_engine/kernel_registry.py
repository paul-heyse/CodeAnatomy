"""Kernel selection helpers for DataFusion-native execution."""

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.kernel.registry import KernelLane, kernel_def
from datafusion_engine.kernels import datafusion_kernel_registry

KernelFn = Callable[..., TableLike]


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
    "KernelCapability",
    "KernelFn",
    "kernel_capability",
    "resolve_kernel",
]
