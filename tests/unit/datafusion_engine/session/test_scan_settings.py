"""Tests for shared scan-setting helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from datafusion_engine.session.scan_settings import apply_scan_settings

if TYPE_CHECKING:
    from datafusion import SessionContext, SQLOptions

    from schema_spec.scan_options import DataFusionScanOptions


class _FakeDataFrame:
    def collect(self) -> list[object]:
        _ = self
        return []


class _FakeSQLOptions:
    def __init__(self) -> None:
        self.allow_statements = False

    def with_allow_statements(self, *, allow: bool) -> _FakeSQLOptions:
        self.allow_statements = allow
        return self


class _FakeSessionContext:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    def sql_with_options(self, sql: str, _options: _FakeSQLOptions) -> _FakeDataFrame:
        self.sql_calls.append(sql)
        return _FakeDataFrame()


class _ScanOptions:
    collect_statistics = True
    meta_fetch_concurrency = 8
    list_files_cache_limit = 123
    list_files_cache_ttl = "5m"
    listing_table_factory_infer_partitions = False
    listing_table_ignore_subdirectory = True


def test_apply_scan_settings_sets_expected_runtime_options() -> None:
    """Scan settings should translate into corresponding DataFusion SET commands."""
    ctx = _FakeSessionContext()
    options = _FakeSQLOptions()

    apply_scan_settings(
        cast("SessionContext", ctx),
        scan=cast("DataFusionScanOptions", _ScanOptions()),
        sql_options=cast("SQLOptions", options),
    )

    assert any("datafusion.execution.collect_statistics" in sql for sql in ctx.sql_calls)
    assert any("datafusion.execution.meta_fetch_concurrency" in sql for sql in ctx.sql_calls)
    assert any("datafusion.runtime.list_files_cache_limit" in sql for sql in ctx.sql_calls)
    assert any("datafusion.runtime.list_files_cache_ttl" in sql for sql in ctx.sql_calls)


def test_apply_scan_settings_noop_when_scan_is_none() -> None:
    """No scan options should result in no runtime-setting SQL execution."""
    ctx = _FakeSessionContext()
    options = _FakeSQLOptions()

    apply_scan_settings(
        cast("SessionContext", ctx),
        scan=None,
        sql_options=cast("SQLOptions", options),
    )

    assert ctx.sql_calls == []
