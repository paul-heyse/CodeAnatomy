"""Unit tests for dataset resolution strict/degraded provider modes."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.dataset import resolution
from datafusion_engine.delta.contracts import DeltaCdfContract
from datafusion_engine.delta.control_plane import (
    DeltaProviderBundle,
    DeltaProviderRequest,
)
from datafusion_engine.errors import DataFusionEngineError, ErrorKind

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class _FeatureFlags:
    enforce_delta_ffi_provider: bool


@dataclass(frozen=True)
class _RuntimeProfileStub:
    features: _FeatureFlags


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


def _runtime_profile_stub(*, enforce_delta_ffi_provider: bool) -> DataFusionRuntimeProfile:
    profile = _RuntimeProfileStub(
        features=_FeatureFlags(enforce_delta_ffi_provider=enforce_delta_ffi_provider)
    )
    return cast("DataFusionRuntimeProfile", profile)


def _cdf_contract_stub(contract: _FakeCdfContract) -> DeltaCdfContract:
    return cast("DeltaCdfContract", contract)


def test_delta_provider_bundle_strict_mode_raises_plugin_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strict mode must fail fast when control-plane provider creation fails."""

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
            runtime_profile=None,
        )

    assert excinfo.value.kind == ErrorKind.PLUGIN
    assert "strict native-provider mode" in str(excinfo.value)


def test_delta_provider_bundle_uses_degraded_mode_when_strict_opt_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strictness opt-out should route provider failures through degraded mode."""
    expected_bundle = DeltaProviderBundle(
        provider={"kind": "dataset"},
        snapshot={"provider_mode": "pyarrow_dataset_degraded"},
        scan_config={},
        scan_effective={},
    )

    def _raise_provider(*_args: object, **_kwargs: object) -> object:
        msg = "ffi entrypoint failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(resolution, "delta_provider_from_session", _raise_provider)
    monkeypatch.setattr(
        resolution,
        "_degraded_delta_provider_bundle",
        lambda **_kwargs: expected_bundle,
    )
    provider_bundle = resolution._delta_provider_bundle  # noqa: SLF001
    profile = _runtime_profile_stub(enforce_delta_ffi_provider=False)

    resolved = provider_bundle(
        _ctx_stub(),
        request=_provider_request(),
        scan_files=None,
        runtime_profile=profile,
    )

    assert resolved is expected_bundle


def test_degraded_delta_provider_bundle_emits_degraded_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Degraded provider bundle should expose deterministic fallback metadata."""

    class _FakeDeltaTable:
        def __init__(
            self,
            table_uri: str,
            *,
            version: int | None,
            storage_options: dict[str, str] | None,
        ) -> None:
            self.table_uri = table_uri
            self._version = 13 if version is None else version
            self.storage_options = storage_options

        def load_as_version(self, _timestamp: str) -> None:
            return None

        def to_pyarrow_dataset(self) -> object:
            return {"provider": "dataset"}

        def version(self) -> int:
            return self._version

    fake_module = ModuleType("deltalake")
    fake_module.__dict__["DeltaTable"] = _FakeDeltaTable
    monkeypatch.setitem(sys.modules, "deltalake", fake_module)
    degraded_bundle = resolution._degraded_delta_provider_bundle  # noqa: SLF001

    bundle = degraded_bundle(
        request=_provider_request(),
        scan_files=("part-000.parquet",),
        error=RuntimeError("ffi unavailable"),
    )

    assert bundle.snapshot["provider_mode"] == "pyarrow_dataset_degraded"
    assert bundle.scan_effective["fallback"] is True
    assert bundle.scan_effective["scan_files_requested"] is True
    assert bundle.snapshot["scan_files_applied"] is False


def test_degraded_delta_cdf_bundle_emits_degraded_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Degraded CDF bundle should expose deterministic fallback metadata."""

    class _FakeCdfReader:
        def read_all(self) -> object:
            return {"provider": "cdf"}

    class _FakeDeltaTable:
        def __init__(
            self,
            table_uri: str,
            *,
            version: int | None,
            storage_options: dict[str, str] | None,
        ) -> None:
            self.table_uri = table_uri
            self._version = 17 if version is None else version
            self.storage_options = storage_options

        def load_as_version(self, _timestamp: str) -> None:
            return None

        def load_cdf(self, **_kwargs: object) -> _FakeCdfReader:
            return _FakeCdfReader()

        def version(self) -> int:
            return self._version

    fake_module = ModuleType("deltalake")
    fake_module.__dict__["DeltaTable"] = _FakeDeltaTable
    monkeypatch.setitem(sys.modules, "deltalake", fake_module)
    degraded_cdf_bundle = resolution._degraded_delta_cdf_bundle  # noqa: SLF001
    contract = _FakeCdfContract(
        table_uri="memory://delta/events",
        storage_options={"region": "us-east-1"},
        version=11,
        timestamp=None,
        options=_FakeCdfOptions(starting_version=3),
    )

    bundle = degraded_cdf_bundle(
        contract=_cdf_contract_stub(contract),
        error=RuntimeError("ffi unavailable"),
    )

    assert bundle.snapshot["provider_mode"] == "pyarrow_cdf_degraded"
    assert bundle.snapshot["version"] == 11
    assert bundle.cdf_options is not None
    assert bundle.cdf_options.starting_version == 3
