"""Tests for runtime-checkable delta protocol contracts."""

from __future__ import annotations

from datafusion_engine.delta.protocols import (
    DeltaProviderCapsuleHandle,
    DeltaTableHandle,
    RustCdfOptionsHandle,
    RustDeltaEntrypoint,
)


class _Entrypoint:
    def __call__(self, *args: object, **kwargs: object) -> object:
        return {"args": args, "kwargs": kwargs}


class _CdfOptions:
    starting_version: int | None = None
    ending_version: int | None = None
    starting_timestamp: str | None = None
    ending_timestamp: str | None = None
    allow_out_of_range: bool = False


class _TableHandle:
    @staticmethod
    def version() -> int:
        return 1

    @staticmethod
    def table_uri() -> str:
        return "memory://table"

    @staticmethod
    def schema() -> object:
        return {"schema": "ok"}

    @staticmethod
    def files() -> list[str]:
        return ["part-000.parquet"]


class _ProviderCapsule:
    @staticmethod
    def datafusion_table_provider() -> object:
        return object()


def test_rust_entrypoint_protocol_is_runtime_checkable() -> None:
    """Entrypoint test double satisfies runtime-checkable protocol."""
    assert isinstance(_Entrypoint(), RustDeltaEntrypoint)


def test_cdf_options_protocol_is_runtime_checkable() -> None:
    """CDF options test double satisfies runtime-checkable protocol."""
    assert isinstance(_CdfOptions(), RustCdfOptionsHandle)


def test_delta_table_handle_protocol_is_runtime_checkable() -> None:
    """Table-handle test double satisfies runtime-checkable protocol."""
    assert isinstance(_TableHandle(), DeltaTableHandle)


def test_provider_capsule_protocol_is_runtime_checkable() -> None:
    """Provider-capsule double satisfies runtime-checkable protocol."""
    assert isinstance(_ProviderCapsule(), DeltaProviderCapsuleHandle)
