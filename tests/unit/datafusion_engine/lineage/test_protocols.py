"""Tests for lineage protocol runtime compatibility."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from datafusion_engine.lineage.protocols import (
    LineageExpr,
    LineageJoin,
    LineageQuery,
    LineageScan,
)


@dataclass
class _Scan:
    dataset_name: str = "dataset"
    projected_columns: Sequence[str] = ()
    pushed_filters: Sequence[str] = ()


@dataclass
class _Join:
    join_type: str = "inner"
    left_keys: Sequence[str] = ()
    right_keys: Sequence[str] = ()


@dataclass
class _Expr:
    kind: str = "column"
    referenced_columns: Sequence[Sequence[str]] = ()
    referenced_udfs: Sequence[str] = ()
    text: str | None = None


class _Lineage:
    @property
    def required_udfs(self) -> Sequence[str]:
        return ("f",)

    @property
    def required_rewrite_tags(self) -> Sequence[str]:
        return ("tag",)

    @property
    def scans(self) -> Sequence[LineageScan]:
        return (_Scan(),)

    @property
    def joins(self) -> Sequence[LineageJoin]:
        return (_Join(),)

    @property
    def exprs(self) -> Sequence[LineageExpr]:
        return (_Expr(),)

    @property
    def required_columns_by_dataset(self) -> dict[str, Sequence[str]]:
        return {"dataset": ("column",)}


def test_lineage_query_protocol_runtime_checkable() -> None:
    """Lineage query test double satisfies runtime-checkable protocol."""
    assert isinstance(_Lineage(), LineageQuery)
