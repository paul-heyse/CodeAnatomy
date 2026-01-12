"""Declarative query specs for dataset scans and projections."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from arrowdsl.column_ops import ColumnExpr, FieldExpr
from arrowdsl.predicates import FilterSpec, PredicateSpec
from arrowdsl.pyarrow_protocols import ComputeExpression

type ColumnsSpec = Sequence[str] | Mapping[str, ComputeExpression]


@dataclass(frozen=True)
class ProjectionSpec:
    """Defines the scan projection.

    Parameters
    ----------
    base:
        Base dataset columns (read as-is).
    derived:
        Derived columns computed at scan time, when supported.
    """

    base: tuple[str, ...]
    derived: Mapping[str, ColumnExpr] = field(default_factory=dict)


@dataclass(frozen=True)
class QuerySpec:
    """Declarative scan spec for a dataset."""

    projection: ProjectionSpec
    predicate: PredicateSpec | None = None
    pushdown_predicate: PredicateSpec | None = None

    def scan_columns(self, *, provenance: bool) -> ColumnsSpec:
        """Return the scan column spec for Arrow scanners.

        Parameters
        ----------
        provenance:
            When ``True``, include provenance columns.

        Returns
        -------
        ColumnsSpec
            Column spec for scanners or scan nodes.
        """
        if not provenance and not self.projection.derived:
            return list(self.projection.base)

        cols: dict[str, ComputeExpression] = {
            col: FieldExpr(col).to_expression() for col in self.projection.base
        }
        cols.update({name: expr.to_expression() for name, expr in self.projection.derived.items()})

        if provenance:
            cols.update(
                {
                    "prov_filename": FieldExpr("__filename").to_expression(),
                    "prov_fragment_index": FieldExpr("__fragment_index").to_expression(),
                    "prov_batch_index": FieldExpr("__batch_index").to_expression(),
                    "prov_last_in_fragment": FieldExpr("__last_in_fragment").to_expression(),
                }
            )
        return cols

    def predicate_expression(self) -> ComputeExpression | None:
        """Return the plan-lane predicate expression, if any.

        Returns
        -------
        ComputeExpression | None
            Predicate expression or ``None`` when not defined.
        """
        filt = self.filter_spec()
        if filt is None:
            return None
        return filt.to_expression()

    def pushdown_expression(self) -> ComputeExpression | None:
        """Return the scan pushdown predicate expression, if any.

        Returns
        -------
        ComputeExpression | None
            Pushdown predicate expression or ``None`` when not defined.
        """
        filt = self.pushdown_filter_spec()
        if filt is None:
            return None
        return filt.to_expression()

    def filter_spec(self) -> FilterSpec | None:
        """Return the filter spec for plan-lane filtering, if any.

        Returns
        -------
        FilterSpec | None
            Filter specification or ``None`` when not defined.
        """
        if self.predicate is None:
            return None
        return FilterSpec(self.predicate)

    def pushdown_filter_spec(self) -> FilterSpec | None:
        """Return the filter spec for scan pushdown, if any.

        Returns
        -------
        FilterSpec | None
            Filter specification or ``None`` when not defined.
        """
        if self.pushdown_predicate is None:
            return None
        return FilterSpec(self.pushdown_predicate)

    @staticmethod
    def simple(
        *cols: str,
        predicate: PredicateSpec | None = None,
        pushdown_predicate: PredicateSpec | None = None,
    ) -> QuerySpec:
        """Build a simple QuerySpec from column names.

        Parameters
        ----------
        *cols:
            Base column names.
        predicate:
            Optional in-plan predicate.
        pushdown_predicate:
            Optional pushdown predicate for scanning.

        Returns
        -------
        QuerySpec
            Query specification instance.
        """
        return QuerySpec(
            projection=ProjectionSpec(base=tuple(cols)),
            predicate=predicate,
            pushdown_predicate=pushdown_predicate,
        )
