from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow.compute as pc


ColumnsSpec = Union[Sequence[str], Mapping[str, "pc.Expression"]]


@dataclass(frozen=True)
class ProjectionSpec:
    """Defines the scan projection.

    base:
      Base dataset columns (read as-is).

    derived:
      Derived columns computed at scan time (when supported) and repeated in the
      plan-time Project node for semantic consistency.
    """

    base: Tuple[str, ...]
    derived: Mapping[str, "pc.Expression"] = field(default_factory=dict)


@dataclass(frozen=True)
class QuerySpec:
    """Declarative scan spec for a dataset."""

    projection: ProjectionSpec
    predicate: Optional["pc.Expression"] = None
    pushdown_predicate: Optional["pc.Expression"] = None

    def scan_columns(self, *, provenance: bool) -> ColumnsSpec:
        """Return columns spec for ds.Scanner/ScanNodeOptions.

        If provenance or derived columns are requested, returns a dict mapping output
        names to Expressions; otherwise returns a simple list of column names.
        """
        import pyarrow.compute as pc

        if not provenance and not self.projection.derived:
            return list(self.projection.base)

        cols: Dict[str, "pc.Expression"] = {c: pc.field(c) for c in self.projection.base}
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
        predicate: Optional["pc.Expression"] = None,
        pushdown_predicate: Optional["pc.Expression"] = None,
    ) -> "QuerySpec":
        return QuerySpec(
            projection=ProjectionSpec(base=tuple(cols)),
            predicate=predicate,
            pushdown_predicate=pushdown_predicate,
        )
