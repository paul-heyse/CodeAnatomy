"""Compute registry for Arrow UDFs and transform helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from arrowdsl.core.interop import DataTypeLike, pc

type UdfFn = Callable[..., object]


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


def resolve_kernel(
    name: str,
    *,
    fallbacks: Sequence[str] = (),
    required: bool = False,
) -> str | None:
    """Resolve a compute kernel name from candidates.

    Returns
    -------
    str | None
        Resolved kernel name or ``None`` when unavailable and not required.

    Raises
    ------
    KeyError
        Raised when no candidate kernels exist and ``required=True``.
    """
    for candidate in (name, *fallbacks):
        try:
            pc.get_function(candidate)
        except KeyError:
            continue
        return candidate
    if required:
        msg = f"Missing compute kernel: {name!r}."
        raise KeyError(msg)
    return None


__all__ = [
    "ComputeRegistry",
    "UdfSpec",
    "default_registry",
    "ensure_udf",
    "resolve_kernel",
]
