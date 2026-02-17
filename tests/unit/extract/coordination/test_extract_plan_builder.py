"""Tests for extract plan builder helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from extract.coordination import extract_plan_builder

if TYPE_CHECKING:
    from extract.session import ExtractSession


def test_build_plan_from_row_batches_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Extract plan builder should delegate row-batch conversion and planning."""
    monkeypatch.setattr(
        extract_plan_builder,
        "record_batch_reader_from_row_batches",
        lambda *_args, **_kwargs: "reader",
    )
    monkeypatch.setattr(
        extract_plan_builder,
        "datafusion_plan_from_reader",
        lambda *_args, **_kwargs: "plan",
    )
    assert (
        extract_plan_builder.build_plan_from_row_batches(
            "dataset",
            [[{"a": 1}]],
            session=cast("ExtractSession", object()),
        )
        == "plan"
    )
