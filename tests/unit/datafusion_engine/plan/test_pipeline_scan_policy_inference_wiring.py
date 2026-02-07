# ruff: noqa: INP001
"""Tests for scan policy inference wiring in planning pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

from datafusion_engine.delta.scan_policy_inference import ScanPolicyOverride
from datafusion_engine.plan import pipeline
from datafusion_engine.plan.signals import PlanSignals
from schema_spec.system import DeltaScanPolicyDefaults, ScanPolicyConfig, ScanPolicyDefaults

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.graph import ViewNode
    from semantics.compile_context import SemanticExecutionContext


@dataclass(frozen=True)
class _RuntimeProfile:
    policies: object
    features: object | None = None

    def session_runtime(self) -> object:
        return SimpleNamespace(udf_snapshot={})

    def settings_hash(self) -> str:
        return "settings-hash"


def _policy(
    *,
    collect_statistics: bool | None,
    enable_parquet_pushdown: bool | None,
) -> ScanPolicyConfig:
    return ScanPolicyConfig(
        listing=ScanPolicyDefaults(collect_statistics=collect_statistics),
        delta_listing=ScanPolicyDefaults(collect_statistics=collect_statistics),
        delta_scan=DeltaScanPolicyDefaults(enable_parquet_pushdown=enable_parquet_pushdown),
    )


def test_plan_with_delta_pins_wires_scan_policy_inference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Planning should derive, merge, record, and apply inferred scan policies."""
    base_policy = _policy(
        collect_statistics=True,
        enable_parquet_pushdown=False,
    )
    runtime_profile = _RuntimeProfile(
        policies=SimpleNamespace(scan_policy=base_policy),
    )
    semantic_context = SimpleNamespace(dataset_resolver=object(), manifest=object())
    input_nodes = (SimpleNamespace(name="input_a"), SimpleNamespace(name="input_b"))
    baseline_nodes = (
        SimpleNamespace(name="view_a", plan_bundle=SimpleNamespace(plan_fingerprint="fp-a")),
        SimpleNamespace(name="view_b", plan_bundle=SimpleNamespace(plan_fingerprint="fp-b")),
    )
    scan_unit = SimpleNamespace(key="scan_key_1", dataset_name="dataset_a", candidate_files=())
    apply_calls: list[dict[str, object]] = []
    record_calls: list[tuple[ScanPolicyOverride, ...]] = []
    derive_calls: list[PlanSignals] = []
    capability_calls: list[object] = []
    plan_calls = {"count": 0}
    capability_snapshot: dict[str, object] = {
        "execution_metrics": {"status": "available"},
        "plan_capabilities": {"has_execution_plan_statistics": True},
    }

    class _Facade:
        def __init__(self, **_kwargs: object) -> None:
            return None

        def ensure_view_graph(self, **_kwargs: object) -> None:
            return None

    def _plan_view_nodes(*_args: object, **_kwargs: object) -> tuple[SimpleNamespace, ...]:
        plan_calls["count"] += 1
        if plan_calls["count"] == 1:
            return baseline_nodes
        return baseline_nodes

    def _derive(
        signals: PlanSignals,
        *,
        base_policy: ScanPolicyConfig,
        capability_snapshot: object,
    ) -> tuple[ScanPolicyOverride, ...]:
        _ = base_policy
        derive_calls.append(signals)
        capability_calls.append(capability_snapshot)
        if len(derive_calls) == 1:
            return (
                ScanPolicyOverride(
                    dataset_name="dataset_a",
                    policy=_policy(
                        collect_statistics=False,
                        enable_parquet_pushdown=False,
                    ),
                    reasons=("small_table",),
                ),
            )
        return (
            ScanPolicyOverride(
                dataset_name="dataset_a",
                policy=_policy(
                    collect_statistics=True,
                    enable_parquet_pushdown=True,
                ),
                reasons=("has_pushed_filters",),
            ),
        )

    def _scan_planning_result(*_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(
            scan_units=(scan_unit,),
            scan_keys_by_task={},
            scan_task_name_by_key={"scan_key_1": "scan_unit_1"},
            scan_task_units_by_name={"scan_unit_1": scan_unit},
            scan_task_names_by_task={},
        )

    monkeypatch.setattr(pipeline, "DataFusionExecutionFacade", _Facade)
    monkeypatch.setattr(pipeline, "_plan_view_nodes", _plan_view_nodes)
    monkeypatch.setattr(pipeline, "infer_deps_from_view_nodes", lambda *_args, **_kwargs: ())
    monkeypatch.setattr(pipeline, "_scan_planning", _scan_planning_result)
    monkeypatch.setattr(pipeline, "_lineage_by_view", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pipeline, "_scan_inferred_deps", lambda *_args, **_kwargs: ())
    monkeypatch.setattr(
        pipeline,
        "apply_scan_unit_overrides",
        lambda *_args, **kwargs: apply_calls.append(kwargs),
    )
    monkeypatch.setattr(
        pipeline,
        "record_scan_policy_decisions",
        lambda _profile, *, overrides: record_calls.append(tuple(overrides)),
    )
    monkeypatch.setattr(
        pipeline,
        "extract_plan_signals",
        lambda bundle: PlanSignals(plan_fingerprint=getattr(bundle, "plan_fingerprint", None)),
    )
    monkeypatch.setattr(
        "datafusion_engine.extensions.runtime_capabilities.build_runtime_capabilities_snapshot",
        lambda *_args, **_kwargs: capability_snapshot,
    )
    monkeypatch.setattr(pipeline, "derive_scan_policy_overrides", _derive)

    pipeline.plan_with_delta_pins(
        cast("SessionContext", object()),
        view_nodes=cast("Sequence[ViewNode]", input_nodes),
        runtime_profile=cast("DataFusionRuntimeProfile", runtime_profile),
        snapshot={},
        semantic_context=cast("SemanticExecutionContext", semantic_context),
    )

    assert len(derive_calls) == 2
    assert capability_calls == [capability_snapshot, capability_snapshot]
    assert len(record_calls) == 1
    assert len(record_calls[0]) == 1
    merged = record_calls[0][0]
    assert merged.dataset_name == "dataset_a"
    assert merged.reasons == ("has_pushed_filters", "small_table")
    assert merged.policy.listing.collect_statistics is False
    assert merged.policy.delta_scan.enable_parquet_pushdown is True

    assert len(apply_calls) == 1
    policy_map = apply_calls[0]["scan_policy_overrides_by_dataset"]
    assert isinstance(policy_map, dict)
    dataset_policy = policy_map["dataset_a"]
    assert dataset_policy.listing.collect_statistics is False
    assert dataset_policy.delta_scan.enable_parquet_pushdown is True
