"""Tests for external index provider contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from datafusion import SessionContext

from storage.external_index import (
    ExternalIndexRequest,
    ExternalIndexSelection,
    select_candidates_with_external_indexes,
)


class _NoopProvider:
    provider_name = "noop"

    @staticmethod
    def supports(_request: ExternalIndexRequest) -> bool:
        return True

    @staticmethod
    def select_candidates(
        _ctx: SessionContext,
        *,
        _request: ExternalIndexRequest,
    ) -> None:
        return None


class _SelectionProvider:
    provider_name = "selection"

    @staticmethod
    def supports(_request: ExternalIndexRequest) -> bool:
        return True

    @staticmethod
    def select_candidates(
        _ctx: SessionContext,
        *,
        _request: ExternalIndexRequest,
    ) -> ExternalIndexSelection:
        return ExternalIndexSelection(
            candidate_files=(Path("a.parquet"),),
            total_files=10,
            candidate_file_count=1,
            pruned_file_count=9,
        )


class _ExplodingProvider:
    provider_name = "exploding"

    @staticmethod
    def supports(_request: ExternalIndexRequest) -> bool:
        return True

    @staticmethod
    def select_candidates(
        _ctx: SessionContext,
        *,
        _request: ExternalIndexRequest,
    ) -> None:
        message = "boom"
        raise RuntimeError(message)


def _request() -> ExternalIndexRequest:
    location = cast("Any", type("L", (), {"format": "delta"})())
    return ExternalIndexRequest(
        dataset_name="dataset",
        location=location,
        lineage=cast("Any", object()),
    )


def test_selects_first_provider_that_returns_candidates() -> None:
    """Test selects first provider that returns candidates."""
    selection, provider = select_candidates_with_external_indexes(
        cast("SessionContext", object()),
        request=_request(),
        providers=(_NoopProvider(), _SelectionProvider()),
    )
    assert provider == "selection"
    assert selection is not None
    assert selection.candidate_file_count == 1


def test_provider_errors_are_fail_open() -> None:
    """Test provider errors are fail open."""
    selection, provider = select_candidates_with_external_indexes(
        cast("SessionContext", object()),
        request=_request(),
        providers=(_ExplodingProvider(), _SelectionProvider()),
    )
    assert provider == "selection"
    assert selection is not None
