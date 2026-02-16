# ruff: noqa: D100, D103, INP001, RUF012
from __future__ import annotations

from datafusion_engine.lineage.protocols import LineageQuery


class _Lineage:
    required_udfs = ("f",)
    required_rewrite_tags = ("tag",)
    scans = ()
    joins = ()
    exprs = ()
    required_columns_by_dataset = {"dataset": ("column",)}


def test_lineage_query_protocol_runtime_checkable() -> None:
    assert isinstance(_Lineage(), LineageQuery)
