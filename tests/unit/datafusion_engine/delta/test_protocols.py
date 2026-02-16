# ruff: noqa: D100, D103, PLR6301
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
    def version(self) -> int:
        return 1

    def table_uri(self) -> str:
        return "memory://table"

    def schema(self) -> object:
        return {"schema": "ok"}

    def files(self) -> list[str]:
        return ["part-000.parquet"]


class _ProviderCapsule:
    def datafusion_table_provider(self) -> object:
        return object()


def test_rust_entrypoint_protocol_is_runtime_checkable() -> None:
    assert isinstance(_Entrypoint(), RustDeltaEntrypoint)


def test_cdf_options_protocol_is_runtime_checkable() -> None:
    assert isinstance(_CdfOptions(), RustCdfOptionsHandle)


def test_delta_table_handle_protocol_is_runtime_checkable() -> None:
    assert isinstance(_TableHandle(), DeltaTableHandle)


def test_provider_capsule_protocol_is_runtime_checkable() -> None:
    assert isinstance(_ProviderCapsule(), DeltaProviderCapsuleHandle)
