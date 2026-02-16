"""Unit tests for strict dataset resolution provider behavior."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.dataset import resolution
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.contracts import DeltaCdfContract
from datafusion_engine.delta.control_plane_core import DeltaProviderRequest
from datafusion_engine.errors import DataFusionEngineError, ErrorKind

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class _FakeCdfOptions:
    starting_version: int | None = 0
    ending_version: int | None = None
    starting_timestamp: str | None = None
    ending_timestamp: str | None = None
    columns: tuple[str, ...] | None = None
    predicate: str | None = None
    allow_out_of_range: bool = False


@dataclass(frozen=True)
class _FakeCdfRequest:
    options: _FakeCdfOptions | None


@dataclass(frozen=True)
class _FakeCdfContract:
    table_uri: str
    storage_options: dict[str, str] | None
    version: int | None
    timestamp: str | None
    options: _FakeCdfOptions | None

    def to_request(self) -> _FakeCdfRequest:
        return _FakeCdfRequest(options=self.options)


def _provider_request() -> DeltaProviderRequest:
    return DeltaProviderRequest(
        table_uri="memory://delta/events",
        storage_options={"region": "us-east-1"},
        version=7,
        timestamp=None,
        delta_scan=None,
    )


def _ctx_stub() -> SessionContext:
    return cast("SessionContext", object())


def test_delta_provider_bundle_fails_fast_when_control_plane_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider creation failures must fail fast with no degraded fallback."""

    def _raise_provider(*_args: object, **_kwargs: object) -> object:
        msg = "ffi entrypoint failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(resolution, "delta_provider_from_session", _raise_provider)
    provider_bundle = resolution._delta_provider_bundle  # noqa: SLF001

    with pytest.raises(DataFusionEngineError) as excinfo:
        _ = provider_bundle(
            _ctx_stub(),
            request=_provider_request(),
            scan_files=None,
        )

    assert excinfo.value.kind == ErrorKind.PLUGIN
    assert "degraded Python fallback paths have been removed" in str(excinfo.value)


def test_resolve_delta_cdf_fails_fast_when_control_plane_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CDF provider failures must fail fast with no degraded fallback."""

    def _raise_cdf_provider(*_args: object, **_kwargs: object) -> object:
        msg = "ffi cdf entrypoint failed"
        raise RuntimeError(msg)

    contract = _FakeCdfContract(
        table_uri="memory://delta/events",
        storage_options={"region": "us-east-1"},
        version=11,
        timestamp=None,
        options=_FakeCdfOptions(starting_version=3),
    )
    monkeypatch.setattr(
        resolution,
        "build_delta_cdf_contract",
        lambda _location: cast("DeltaCdfContract", contract),
    )
    monkeypatch.setattr(resolution, "delta_cdf_provider", _raise_cdf_provider)
    resolve_delta_cdf = resolution._resolve_delta_cdf  # noqa: SLF001
    location = DatasetLocation(
        path="memory://delta/events",
        format="delta",
        datafusion_provider="delta_cdf",
    )

    with pytest.raises(DataFusionEngineError) as excinfo:
        _ = resolve_delta_cdf(location=location, name="events")

    assert excinfo.value.kind == ErrorKind.PLUGIN
    assert "degraded Python fallback paths have been removed" in str(excinfo.value)
