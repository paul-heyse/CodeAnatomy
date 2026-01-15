"""Kernel registry metadata for ArrowDSL."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


class KernelLane(StrEnum):
    """Execution lane for a kernel."""

    BUILTIN = "builtin"
    DF_UDF = "df_udf"
    ARROW_FALLBACK = "arrow_fallback"


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
        lane=KernelLane.DF_UDF,
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
        lane=KernelLane.ARROW_FALLBACK,
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


__all__ = ["KERNEL_REGISTRY", "KernelDef", "KernelLane", "kernel_def"]
