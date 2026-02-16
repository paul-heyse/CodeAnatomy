# ruff: noqa: D100, D103, ANN001
from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from datafusion_engine.delta.service import DeltaFeatureOps, DeltaService
from storage.deltalake.delta_read import DeltaFeatureMutationOptions


@dataclass
class _ServiceStub:
    resolved: DeltaFeatureMutationOptions

    def resolve_feature_options(
        self, _options: DeltaFeatureMutationOptions
    ) -> DeltaFeatureMutationOptions:
        return self.resolved


def test_feature_ops_prefers_storage_builtin_enable_change_data_feed(monkeypatch) -> None:
    called: list[tuple[object, bool]] = []

    def _fake_enable(
        options: object,
        *,
        allow_protocol_versions_increase: bool,
    ) -> dict[str, object]:
        called.append((options, allow_protocol_versions_increase))
        return {"ok": True}

    monkeypatch.setattr(
        "datafusion_engine.delta.service.enable_delta_change_data_feed", _fake_enable
    )

    options = DeltaFeatureMutationOptions(path="s3://bucket/table")
    service = _ServiceStub(resolved=options)
    result = DeltaFeatureOps(service=cast("DeltaService", service)).enable_change_data_feed(
        options=options,
        allow_protocol_versions_increase=False,
    )

    assert result == {"ok": True}
    assert called == [(options, False)]
