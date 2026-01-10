from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .runtime import ExecutionContext, Ordering, OrderingLevel

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa
    import pyarrow.compute as pc
    from pyarrow import acero


ReaderThunk = Callable[[], "pa.RecordBatchReader"]
TableThunk = Callable[[], "pa.Table"]


@dataclass(frozen=True)
class Plan:
    """A small wrapper around an Acero Declaration or an eager source."""

    decl: acero.Declaration | None = None
    reader_thunk: ReaderThunk | None = None
    table_thunk: TableThunk | None = None

    label: str = ""
    ordering: Ordering = Ordering.unordered()

    def __post_init__(self) -> None:
        sources = [
            self.decl is not None,
            self.reader_thunk is not None,
            self.table_thunk is not None,
        ]
        if sum(sources) != 1:
            raise ValueError("Plan must have exactly one of: decl, reader_thunk, table_thunk")

    # ----------------------
    # Execution surfaces
    # ----------------------

    def to_reader(self, *, ctx: ExecutionContext) -> pa.RecordBatchReader:
        """Preferred for streaming consumption."""
        import pyarrow as pa  # noqa: F401

        if self.reader_thunk is not None:
            return self.reader_thunk()
        if self.decl is not None:
            return self.decl.to_reader(use_threads=ctx.use_threads)
        assert self.table_thunk is not None
        return self.table_thunk().to_reader()  # type: ignore[no-any-return]

    def to_table(self, *, ctx: ExecutionContext) -> pa.Table:
        """Materialize (a deliberate boundary)."""
        import pyarrow as pa  # noqa: F401

        if self.table_thunk is not None:
            return self.table_thunk()
        if self.decl is not None:
            return self.decl.to_table(use_threads=ctx.use_threads)
        assert self.reader_thunk is not None
        return self.reader_thunk().read_all()

    # ----------------------
    # Constructors
    # ----------------------

    @staticmethod
    def table_source(table: pa.Table, *, label: str = "") -> Plan:
        from pyarrow import acero

        decl = acero.Declaration("table_source", acero.TableSourceNodeOptions(table))
        return Plan(decl=decl, label=label, ordering=Ordering.implicit())

    @staticmethod
    def from_table(table: pa.Table, *, label: str = "") -> Plan:
        return Plan(table_thunk=lambda: table, label=label, ordering=Ordering.implicit())

    @staticmethod
    def from_reader(reader: pa.RecordBatchReader, *, label: str = "") -> Plan:
        return Plan(reader_thunk=lambda: reader, label=label, ordering=Ordering.implicit())

    # ----------------------
    # Plan-lane operators
    # ----------------------

    def filter(self, predicate: pc.Expression, *, label: str = "") -> Plan:
        from pyarrow import acero

        if self.decl is None:
            raise TypeError("filter() requires an Acero-backed Plan (decl != None)")
        decl = acero.Declaration("filter", acero.FilterNodeOptions(predicate), inputs=[self.decl])
        return Plan(decl=decl, label=label or self.label, ordering=self.ordering)

    def project(
        self, expressions: Sequence[pc.Expression], names: Sequence[str], *, label: str = ""
    ) -> Plan:
        from pyarrow import acero

        if self.decl is None:
            raise TypeError("project() requires an Acero-backed Plan (decl != None)")
        decl = acero.Declaration(
            "project", acero.ProjectNodeOptions(list(expressions), list(names)), inputs=[self.decl]
        )
        return Plan(decl=decl, label=label or self.label, ordering=self.ordering)

    def order_by(self, sort_keys: Sequence[tuple[str, str]], *, label: str = "") -> Plan:
        """Establish explicit order.

        sort_keys: list of (column, "ascending"|"descending")
        """
        from pyarrow import acero

        if self.decl is None:
            raise TypeError("order_by() requires an Acero-backed Plan (decl != None)")
        decl = acero.Declaration(
            "order_by", acero.OrderByNodeOptions(sort_keys=list(sort_keys)), inputs=[self.decl]
        )
        return Plan(
            decl=decl, label=label or self.label, ordering=Ordering.explicit(tuple(sort_keys))
        )

    def mark_unordered(self, *, label: str = "") -> Plan:
        """Force ordering metadata to UNORDERED (e.g., after joins/aggs)."""
        if self.decl is not None:
            return Plan(decl=self.decl, label=label or self.label, ordering=Ordering.unordered())
        if self.table_thunk is not None:
            return Plan(
                table_thunk=self.table_thunk,
                label=label or self.label,
                ordering=Ordering.unordered(),
            )
        assert self.reader_thunk is not None
        return Plan(
            reader_thunk=self.reader_thunk, label=label or self.label, ordering=Ordering.unordered()
        )

    # ----------------------
    # Ordering helpers
    # ----------------------

    def is_ordered(self) -> bool:
        return self.ordering.level in (OrderingLevel.IMPLICIT, OrderingLevel.EXPLICIT)
