"""Typed protocol surface for Rust Delta FFI handles."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class InternalSessionContext(Protocol):
    """Opaque internal DataFusion context passed to Rust entrypoints."""


@runtime_checkable
class RustDeltaEntrypoint(Protocol):
    """Callable Rust entrypoint exposed by the extension module."""

    def __call__(self, *args: object, **kwargs: object) -> object:
        """Invoke the Rust entrypoint with positional/keyword arguments."""
        ...


@runtime_checkable
class RustDeltaExtensionModule(Protocol):
    """Rust extension module exposing Delta control-plane entrypoints."""

    __name__: str

    def __getattr__(self, name: str) -> object:
        """Return a named attribute exported by the extension module."""
        ...


@runtime_checkable
class RustCdfOptionsHandle(Protocol):
    """Mutable CDF options handle allocated by the Rust extension."""

    starting_version: int | None
    ending_version: int | None
    starting_timestamp: str | None
    ending_timestamp: str | None
    allow_out_of_range: bool


@runtime_checkable
class DeltaCdfExtensionModule(Protocol):
    """Extension protocol exposing the Delta CDF provider factory."""

    def delta_cdf_table_provider(self, *args: object, **kwargs: object) -> object:
        """Build a CDF table provider using Rust extension entrypoints."""
        ...


@runtime_checkable
class DeltaTableHandle(Protocol):
    """Opaque Rust Delta table handle protocol."""

    def version(self) -> int:
        """Return Delta table version."""
        ...

    def table_uri(self) -> str:
        """Return Delta table URI."""
        ...

    def schema(self) -> object:
        """Return table schema payload."""
        ...

    def files(self) -> list[str]:
        """Return active data-file list."""
        ...


@runtime_checkable
class DeltaProviderCapsuleHandle(Protocol):
    """TableProvider capsule surface accepted by DataFusion Python."""

    def datafusion_table_provider(self) -> object:
        """Return the Python table-provider capsule."""
        ...


@runtime_checkable
class DeltaSnapshotPayload(Protocol):
    """Typed shape for Delta snapshot payload mappings."""

    def get(self, key: str, default: object = None) -> object:
        """Return snapshot payload value for ``key``."""
        ...


type DeltaProviderHandle = DeltaProviderCapsuleHandle | DeltaTableHandle


__all__ = [
    "DeltaCdfExtensionModule",
    "DeltaProviderCapsuleHandle",
    "DeltaProviderHandle",
    "DeltaSnapshotPayload",
    "DeltaTableHandle",
    "InternalSessionContext",
    "RustCdfOptionsHandle",
    "RustDeltaEntrypoint",
    "RustDeltaExtensionModule",
]
