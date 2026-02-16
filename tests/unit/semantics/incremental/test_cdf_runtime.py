"""Unit tests for incremental CDF runtime helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore
from semantics.incremental.cdf_runtime import (
    CdfReadInputs,
    _prepare_cdf_read_state,
    _resolve_cdf_inputs,
)
from semantics.incremental.cdf_types import CdfFilterPolicy

EXPECTED_START_VERSION = 4
EXPECTED_CURRENT_VERSION = 8


class _ResolverStub:
    @staticmethod
    def location(dataset_name: str) -> object | None:
        _ = dataset_name


class _StorageStub:
    storage_options: ClassVar[dict[str, str]] = {}
    log_storage_options: ClassVar[dict[str, str]] = {}


class _DeltaServiceStub:
    def __init__(self) -> None:
        self.paths: list[str] = []

    def table_version(
        self,
        *,
        path: str,
        storage_options: Any,
        log_storage_options: Any,
    ) -> int:
        _ = storage_options, log_storage_options
        self.paths.append(path)
        return EXPECTED_CURRENT_VERSION


class _RuntimeStub:
    def __init__(self) -> None:
        self._delta_service = _DeltaServiceStub()

    @staticmethod
    def scan_policy() -> object:
        return object()

    def delta_service(self) -> _DeltaServiceStub:
        return self._delta_service


class _ContextStub:
    def __init__(self) -> None:
        self.runtime = _RuntimeStub()
        self.dataset_resolver = _ResolverStub()

    @staticmethod
    def resolve_storage(*, table_uri: str) -> _StorageStub:
        _ = table_uri
        return _StorageStub()


def test_resolve_cdf_inputs_accepts_object_store_uri() -> None:
    """CDF input resolution does not require local filesystem paths."""
    context = _ContextStub()

    inputs = _resolve_cdf_inputs(
        context,
        dataset_path="s3://bucket/table",
        dataset_name="dataset",
    )

    assert inputs is not None
    assert inputs.path == "s3://bucket/table"
    assert context.runtime.delta_service().paths == ["s3://bucket/table"]


def test_prepare_cdf_state_pushes_filter_policy(tmp_path: Path) -> None:
    """Prepared CDF state carries SQL predicate from filter policy."""
    cursor_store = CdfCursorStore(cursors_path=tmp_path / "cursors")
    cursor_store.save_cursor(CdfCursor(dataset_name="dataset", last_version=3))

    inputs = CdfReadInputs(
        path="s3://bucket/table",
        current_version=8,
        storage_options={},
        log_storage_options={},
        scan_options=None,
        cdf_policy=None,
    )

    state = _prepare_cdf_read_state(
        _ContextStub(),
        dataset_name="dataset",
        inputs=inputs,
        cursor_store=cursor_store,
        filter_policy=CdfFilterPolicy.inserts_and_updates_only(),
    )

    assert state is not None
    assert state.cdf_options.starting_version == EXPECTED_START_VERSION
    assert state.cdf_options.ending_version == EXPECTED_CURRENT_VERSION
    assert state.cdf_options.predicate is not None
    assert "_change_type" in state.cdf_options.predicate
