"""Tests for scan policy-aware scan unit overrides."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from datafusion_engine.dataset import resolution as resolution_module
from datafusion_engine.dataset.registry import DatasetLocation
from schema_spec.dataset_spec import DeltaScanPolicyDefaults, ScanPolicyConfig, ScanPolicyDefaults

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.lineage.scheduling import ScanUnit
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


@dataclass(frozen=True)
class _CapturedOverride:
    location: DatasetLocation


def _policy_override() -> ScanPolicyConfig:
    return ScanPolicyConfig(
        listing=ScanPolicyDefaults(collect_statistics=False),
        delta_listing=ScanPolicyDefaults(collect_statistics=False),
        delta_scan=DeltaScanPolicyDefaults(enable_parquet_pushdown=True),
    )


def test_apply_scan_unit_overrides_applies_dataset_policy_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dataset-specific scan policy should be applied before override registration."""
    captured: list[_CapturedOverride] = []
    location = DatasetLocation(path="/tmp/dataset_a", format="delta")
    scan_unit = cast(
        "ScanUnit",
        SimpleNamespace(
            dataset_name="dataset_a",
            key="scan_key_1",
            candidate_files=("/tmp/dataset_a/part-000.parquet",),
        ),
    )

    monkeypatch.setattr(
        resolution_module,
        "_resolve_dataset_location",
        lambda *_args, **_kwargs: location,
    )
    monkeypatch.setattr(
        resolution_module,
        "_scan_files_for_units",
        lambda *_args, **_kwargs: ("part-000.parquet",),
    )

    def _register_override(
        _ctx: object,
        *,
        adapter: object,
        spec: Any,
    ) -> None:
        _ = adapter
        captured.append(_CapturedOverride(location=spec.location))

    monkeypatch.setattr(resolution_module, "_register_delta_override", _register_override)
    monkeypatch.setattr(
        resolution_module,
        "_record_override_artifact",
        lambda *_args, **_kwargs: None,
    )

    resolution_module.apply_scan_unit_overrides(
        cast("SessionContext", object()),
        scan_units=(scan_unit,),
        runtime_profile=cast("DataFusionRuntimeProfile", object()),
        dataset_resolver=None,
        scan_policy_overrides_by_dataset={"dataset_a": _policy_override()},
    )

    assert len(captured) == 1
    resolved = captured[0].location
    assert resolved.overrides is not None
    assert resolved.overrides.datafusion_scan is not None
    assert resolved.overrides.datafusion_scan.collect_statistics is False
    assert resolved.overrides.delta is not None
    assert resolved.overrides.delta.scan is not None
    assert resolved.overrides.delta.scan.enable_parquet_pushdown is True


def test_apply_scan_unit_overrides_preserves_location_without_policy_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without policy overrides, scan unit override registration should keep location unchanged."""
    captured: list[_CapturedOverride] = []
    location = DatasetLocation(path="/tmp/dataset_a", format="delta")
    scan_unit = cast(
        "ScanUnit",
        SimpleNamespace(
            dataset_name="dataset_a",
            key="scan_key_1",
            candidate_files=("/tmp/dataset_a/part-000.parquet",),
        ),
    )

    monkeypatch.setattr(
        resolution_module,
        "_resolve_dataset_location",
        lambda *_args, **_kwargs: location,
    )
    monkeypatch.setattr(
        resolution_module,
        "_scan_files_for_units",
        lambda *_args, **_kwargs: ("part-000.parquet",),
    )

    def _register_override(
        _ctx: object,
        *,
        adapter: object,
        spec: Any,
    ) -> None:
        _ = adapter
        captured.append(_CapturedOverride(location=spec.location))

    monkeypatch.setattr(resolution_module, "_register_delta_override", _register_override)
    monkeypatch.setattr(
        resolution_module,
        "_record_override_artifact",
        lambda *_args, **_kwargs: None,
    )

    resolution_module.apply_scan_unit_overrides(
        cast("SessionContext", object()),
        scan_units=(scan_unit,),
        runtime_profile=cast("DataFusionRuntimeProfile", object()),
        dataset_resolver=None,
    )

    assert len(captured) == 1
    assert captured[0].location.overrides is None


def test_apply_scan_unit_overrides_records_resolver_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scan override boundary should always record resolver identity when provided."""
    calls: list[tuple[object, str]] = []
    resolver = object()
    monkeypatch.setattr(
        "semantics.resolver_identity.record_resolver_if_tracking",
        lambda tracked_resolver, *, label="unknown": calls.append((tracked_resolver, label)),
    )

    resolution_module.apply_scan_unit_overrides(
        cast("SessionContext", object()),
        scan_units=(),
        runtime_profile=None,
        dataset_resolver=cast("Any", resolver),
    )

    assert calls == [(resolver, "scan_override")]
