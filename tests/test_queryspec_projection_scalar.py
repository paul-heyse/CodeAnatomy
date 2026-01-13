"""Tests for QuerySpec projection scalar validation."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from arrowdsl.core.interop import ArrayLike, ComputeExpression, TableLike, pc
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.schema.build import FieldExpr


@dataclass(frozen=True)
class _NonScalarExpr:
    """Column expression that is not safe for scan projection."""

    name: str

    def to_expression(self) -> ComputeExpression:
        return pc.field(self.name)

    def materialize(self, table: TableLike) -> ArrayLike:
        return table[self.name]

    def is_scalar(self) -> bool:
        return self is None


def test_scan_columns_allows_scalar_derived() -> None:
    """Allow scalar-safe derived expressions in scan projections."""
    spec = QuerySpec(
        projection=ProjectionSpec(
            base=("path",),
            derived={"alias_path": FieldExpr("path")},
        )
    )
    cols = spec.scan_columns(provenance=False)
    assert isinstance(cols, dict)
    assert set(cols) == {"path", "alias_path"}


def test_scan_columns_rejects_non_scalar_derived() -> None:
    """Reject non-scalar derived expressions in scan projections."""
    spec = QuerySpec(
        projection=ProjectionSpec(
            base=("path",),
            derived={"bad_expr": _NonScalarExpr("path")},
        )
    )
    with pytest.raises(ValueError, match="not scalar-safe"):
        spec.scan_columns(provenance=False)
