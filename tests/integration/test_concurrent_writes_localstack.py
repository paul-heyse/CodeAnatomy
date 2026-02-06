"""Concurrent-write conformance checks for localstack-class test lanes."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable

from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from tests.harness.profiles import conformance_profile
from tests.test_helpers.arrow_seed import register_arrow_table
from tests.test_helpers.optional_deps import (
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)

require_datafusion_udfs()
require_deltalake()
require_delta_extension()


def _conflict_family(exc: BaseException) -> str:
    message = str(exc).lower()
    if any(
        token in message
        for token in (
            "concurrent",
            "version already exists",
            "transaction conflict",
            "commit failed",
        )
    ):
        return "conflict"
    return "other"


def _write_batch(*, destination: str, table_name: str, ids: list[int]) -> None:
    profile = conformance_profile()
    ctx = profile.session_context()
    source = register_arrow_table(
        ctx,
        name=table_name,
        value=pa.table({"id": ids}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=source,
            destination=destination,
            format=WriteFormat.DELTA,
            mode=WriteMode.APPEND,
        )
    )


@pytest.mark.integration
@pytest.mark.localstack
def test_concurrent_delta_writes_have_classified_outcomes(tmp_path: Path) -> None:
    """Run concurrent appends and classify outcomes into conflict families."""
    destination = str(tmp_path / "events_delta")
    profile = conformance_profile()
    ctx = profile.session_context()
    seed = register_arrow_table(
        ctx,
        name="events_seed",
        value=pa.table({"id": [1]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    pipeline.write(
        WriteRequest(
            source=seed,
            destination=destination,
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
        )
    )
    outcomes: list[str] = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                _write_batch,
                destination=destination,
                table_name=f"events_append_{index}",
                ids=[index + 2],
            )
            for index in range(2)
        ]
        for future in futures:
            exc = future.exception()
            if exc is not None:  # pragma: no cover - conflict timing is nondeterministic
                outcomes.append(_conflict_family(exc))
                continue
            outcomes.append("ok")
    assert set(outcomes).issubset({"ok", "conflict"})
    table = DeltaTable(destination)
    assert table.to_pyarrow_table().num_rows >= 2
