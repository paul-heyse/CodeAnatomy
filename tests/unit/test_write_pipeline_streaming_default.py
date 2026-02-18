"""Tests for streaming-first write-pipeline boundaries."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pyarrow as pa
import pytest
from datafusion import SessionContext

from datafusion_engine.io import write_pipeline as write_pipeline_module
from datafusion_engine.io.write_pipeline import WritePipeline
from schema_spec.dataset_spec import DatasetSpec


def test_validate_dataframe_uses_record_batch_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validation path should pass a streaming reader to schema validation."""
    ctx = SessionContext()
    table = pa.table({"id": [1, 2, 3]})
    df = ctx.from_arrow(table, name="events")
    pipeline = WritePipeline(ctx)

    captured: dict[str, object] = {}

    def _fake_validate(
        value: object,
        *,
        spec: object,
        options: object,
        runtime_profile: object | None,
    ) -> object:
        captured["is_reader"] = isinstance(value, pa.RecordBatchReader)
        captured["spec"] = spec
        _ = options
        _ = runtime_profile
        return value

    monkeypatch.setattr(write_pipeline_module, "validate_arrow_table", _fake_validate)
    monkeypatch.setattr(
        WritePipeline,
        "_resolve_validation_options",
        staticmethod(lambda **_kwargs: object()),
    )

    dataset_spec = SimpleNamespace(
        table_spec=object(),
        policies=SimpleNamespace(validation=None, dataframe_validation=None),
    )
    validate_dataframe = vars(type(pipeline))["_validate_dataframe"]
    validate_dataframe(
        pipeline,
        df,
        dataset_spec=cast("DatasetSpec", dataset_spec),
        overrides=None,
    )

    assert captured["is_reader"] is True
    assert captured["spec"] is dataset_spec.table_spec
