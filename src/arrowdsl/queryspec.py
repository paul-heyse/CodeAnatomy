"""Declarative query specs for dataset scans and projections."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from arrowdsl.compute import pc
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
    derived: Mapping[str, ComputeExpression] = field(default_factory=dict)


@dataclass(frozen=True)
class QuerySpec:
    """Declarative scan spec for a dataset."""

    projection: ProjectionSpec
    predicate: ComputeExpression | None = None
    pushdown_predicate: ComputeExpression | None = None

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

        cols: dict[str, ComputeExpression] = {col: pc.field(col) for col in self.projection.base}
        cols.update(dict(self.projection.derived))

        if provenance:
            cols.update(
                {
                    "prov_filename": pc.field("__filename"),
                    "prov_fragment_index": pc.field("__fragment_index"),
                    "prov_batch_index": pc.field("__batch_index"),
                    "prov_last_in_fragment": pc.field("__last_in_fragment"),
                }
            )
        return cols

    @staticmethod
    def simple(
        *cols: str,
        predicate: ComputeExpression | None = None,
        pushdown_predicate: ComputeExpression | None = None,
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
