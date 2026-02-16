# ruff: noqa: D100, D103, ANN001, INP001
from __future__ import annotations

from typing import cast

from datafusion_engine.arrow import metadata_read
from datafusion_engine.arrow.interop import SchemaLike


def test_read_metadata_payload_delegates(monkeypatch) -> None:
    monkeypatch.setattr(metadata_read, "metadata_payload", lambda _schema: {"a": 1})
    assert metadata_read.read_metadata_payload(cast("SchemaLike", object())) == {"a": 1}
