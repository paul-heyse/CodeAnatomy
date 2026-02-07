from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa

from datafusion_engine.arrow.interop import RecordBatchReaderLike
from datafusion_engine.io.write import WriteMethod, WriteMode, WriteRequest, WriteResult
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.compile_context import dataset_bindings_for_profile
from semantics.incremental.runtime import IncrementalRuntime, IncrementalRuntimeBuildRequest
from semantics.incremental.write_helpers import (
    IncrementalDeltaWriteRequest,
    write_delta_table_via_pipeline,
)


def test_write_delta_table_streaming_reader(monkeypatch: Any, tmp_path: Path) -> None:
    profile = DataFusionRuntimeProfile()
    runtime = IncrementalRuntime.build(
        IncrementalRuntimeBuildRequest(
            profile=profile,
            dataset_resolver=dataset_bindings_for_profile(profile),
        )
    )
    schema = pa.schema([("value", pa.int64())])
    batch = pa.record_batch([pa.array([1, 2])], schema=schema)
    reader = pa.RecordBatchReader.from_batches(schema, [batch])

    seen: dict[str, object] = {}

    def fake_datafusion_from_arrow(
        _ctx: object, *, name: str, value: object, **_kwargs: object
    ) -> object:
        _ = name
        assert isinstance(value, RecordBatchReaderLike)
        seen["value"] = value
        return object()

    class DummyPipeline:
        def __init__(
            self, _ctx: object, *, sql_options: object, recorder: object, runtime_profile: object
        ) -> None:
            _ = (sql_options, recorder, runtime_profile)

        def write(self, request: WriteRequest) -> WriteResult:
            seen["request"] = request
            return WriteResult(
                request=request,
                method=WriteMethod.STREAMING,
                sql=None,
            )

    monkeypatch.setattr(
        "semantics.incremental.write_helpers.datafusion_from_arrow",
        fake_datafusion_from_arrow,
    )
    monkeypatch.setattr(
        "semantics.incremental.write_helpers.WritePipeline",
        DummyPipeline,
    )

    result = write_delta_table_via_pipeline(
        runtime=runtime,
        table=reader,
        request=IncrementalDeltaWriteRequest(
            destination=str(tmp_path / "delta"),
            mode=WriteMode.APPEND,
        ),
    )
    assert isinstance(result, WriteResult)
    assert "value" in seen
