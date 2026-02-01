"""Tests for semantic output materialization policies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.delta.schema_guard import SchemaEvolutionPolicy
from datafusion_engine.io.write import WriteViewRequest
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.catalog.dataset_specs import dataset_spec
from semantics.pipeline import SemanticOutputWriteContext, _write_semantic_output
from semantics.runtime import SemanticRuntimeConfig

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.io.write import WritePipeline


@dataclass
class _CapturedWrite:
    request: object | None = None


class _FakePipeline:
    def __init__(self, captured: _CapturedWrite) -> None:
        self._captured = captured

    def write_view(self, request: object) -> None:
        self._captured.request = request


class _FakeContext:
    def __init__(self, view_name: str) -> None:
        self._view_name = view_name

    def table(self, name: str) -> object:
        if name != self._view_name:
            msg = f"Unexpected view name: {name!r}."
            raise ValueError(msg)
        return object()


def _dataset_location(view_name: str) -> DatasetLocation:
    spec = dataset_spec(view_name)
    return DatasetLocation(
        path=f"/tmp/{view_name}",
        format="delta",
        dataset_spec=spec,
        delta_write_policy=spec.delta_write_policy,
        delta_schema_policy=spec.delta_schema_policy,
        delta_maintenance_policy=spec.delta_maintenance_policy,
        delta_feature_gate=spec.delta_feature_gate,
    )


def test_write_semantic_output_applies_write_policy_and_schema_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view_name = "py_bc_blocks_norm_v1"
    location = _dataset_location(view_name)
    spec = dataset_spec(view_name)
    assert spec.delta_write_policy is not None

    captured = _CapturedWrite()
    pipeline = _FakePipeline(captured)
    ctx = _FakeContext(view_name)
    profile = DataFusionRuntimeProfile()
    runtime_config = SemanticRuntimeConfig(schema_evolution_enabled=True)

    def _apply_delta_store_policy(
        loc: DatasetLocation, *, policy: object | None
    ) -> DatasetLocation:
        _ = policy
        return loc

    def _arrow_schema_from_df(_: object) -> pa.Schema:
        return pa.schema([("path", pa.string())])

    captured_policy: list[SchemaEvolutionPolicy] = []

    def _enforce_schema_policy(
        *,
        expected_schema: pa.Schema,
        dataset_location: DatasetLocation,
        policy: SchemaEvolutionPolicy,
    ) -> str:
        _ = expected_schema
        _ = dataset_location
        captured_policy.append(policy)
        return "schema_hash"

    from datafusion_engine.delta import schema_guard, store_policy
    from datafusion_engine.views import bundle_extraction

    monkeypatch.setattr(store_policy, "apply_delta_store_policy", _apply_delta_store_policy)
    monkeypatch.setattr(bundle_extraction, "arrow_schema_from_df", _arrow_schema_from_df)
    monkeypatch.setattr(schema_guard, "enforce_schema_policy", _enforce_schema_policy)

    write_context = SemanticOutputWriteContext(
        ctx=cast("SessionContext", ctx),
        pipeline=cast("WritePipeline", pipeline),
        runtime_profile=profile,
        runtime_config=runtime_config,
        schema_policy=SchemaEvolutionPolicy(),
    )

    _write_semantic_output(
        view_name=view_name,
        output_location=location,
        write_context=write_context,
    )

    assert captured.request is not None
    request = captured.request
    assert isinstance(request, WriteViewRequest)
    assert tuple(spec.delta_write_policy.partition_by) == request.partition_by
    assert request.format_options is not None
    assert request.format_options["delta_write_policy"] is spec.delta_write_policy
    assert request.format_options["delta_schema_policy"] is spec.delta_schema_policy
    assert request.format_options["delta_maintenance_policy"] is spec.delta_maintenance_policy
    assert captured_policy[0].mode == "additive"


def test_write_semantic_output_disables_schema_evolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view_name = "py_bc_blocks_norm_v1"
    location = _dataset_location(view_name)

    captured = _CapturedWrite()
    pipeline = _FakePipeline(captured)
    ctx = _FakeContext(view_name)
    profile = DataFusionRuntimeProfile()
    runtime_config = SemanticRuntimeConfig(schema_evolution_enabled=False)

    def _apply_delta_store_policy(
        loc: DatasetLocation, *, policy: object | None
    ) -> DatasetLocation:
        _ = policy
        return loc

    def _arrow_schema_from_df(_: object) -> pa.Schema:
        return pa.schema([("path", pa.string())])

    captured_policy: list[SchemaEvolutionPolicy] = []

    def _enforce_schema_policy(
        *,
        expected_schema: pa.Schema,
        dataset_location: DatasetLocation,
        policy: SchemaEvolutionPolicy,
    ) -> str:
        _ = expected_schema
        _ = dataset_location
        captured_policy.append(policy)
        return "schema_hash"

    from datafusion_engine.delta import schema_guard, store_policy
    from datafusion_engine.views import bundle_extraction

    monkeypatch.setattr(store_policy, "apply_delta_store_policy", _apply_delta_store_policy)
    monkeypatch.setattr(bundle_extraction, "arrow_schema_from_df", _arrow_schema_from_df)
    monkeypatch.setattr(schema_guard, "enforce_schema_policy", _enforce_schema_policy)

    write_context = SemanticOutputWriteContext(
        ctx=cast("SessionContext", ctx),
        pipeline=cast("WritePipeline", pipeline),
        runtime_profile=profile,
        runtime_config=runtime_config,
        schema_policy=SchemaEvolutionPolicy(),
    )

    _write_semantic_output(
        view_name=view_name,
        output_location=location,
        write_context=write_context,
    )

    assert captured.request is not None
    assert captured_policy[0].mode == "strict"
