from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence, TYPE_CHECKING, Union

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow.compute as pc


@dataclass(frozen=True)
class E:
    """Expression macros for QuerySpec predicates and Acero filter/project nodes."""

    @staticmethod
    def field(name: str) -> "pc.Expression":
        import pyarrow.compute as pc

        return pc.field(name)

    @staticmethod
    def scalar(value: Any) -> "pc.Expression":
        import pyarrow.compute as pc

        return pc.scalar(value)

    # -------------
    # Predicates
    # -------------

    @staticmethod
    def eq(col: str, value: Any) -> "pc.Expression":
        return E.field(col) == E.scalar(value)

    @staticmethod
    def ne(col: str, value: Any) -> "pc.Expression":
        return E.field(col) != E.scalar(value)

    @staticmethod
    def lt(col: str, value: Any) -> "pc.Expression":
        return E.field(col) < E.scalar(value)

    @staticmethod
    def le(col: str, value: Any) -> "pc.Expression":
        return E.field(col) <= E.scalar(value)

    @staticmethod
    def gt(col: str, value: Any) -> "pc.Expression":
        return E.field(col) > E.scalar(value)

    @staticmethod
    def ge(col: str, value: Any) -> "pc.Expression":
        return E.field(col) >= E.scalar(value)

    @staticmethod
    def between(col: str, lo: Any, hi: Any) -> "pc.Expression":
        return (E.field(col) >= E.scalar(lo)) & (E.field(col) <= E.scalar(hi))

    @staticmethod
    def in_(col: str, values: Sequence[Any]) -> "pc.Expression":
        return E.field(col).isin(list(values))

    @staticmethod
    def is_null(col: str) -> "pc.Expression":
        return E.field(col).is_null()

    @staticmethod
    def is_valid(col: str) -> "pc.Expression":
        return E.field(col).is_valid()

    # -------------
    # Boolean ops
    # -------------

    @staticmethod
    def and_(*exprs: "pc.Expression") -> "pc.Expression":
        out = exprs[0]
        for e in exprs[1:]:
            out = out & e
        return out

    @staticmethod
    def or_(*exprs: "pc.Expression") -> "pc.Expression":
        out = exprs[0]
        for e in exprs[1:]:
            out = out | e
        return out

    @staticmethod
    def not_(expr: "pc.Expression") -> "pc.Expression":
        return ~expr

    # -------------
    # Projection helpers
    # -------------

    @staticmethod
    def cast(expr: Union[str, "pc.Expression"], target_type: Any, *, safe: bool = True) -> "pc.Expression":
        import pyarrow.compute as pc

        e = E.field(expr) if isinstance(expr, str) else expr
        return pc.cast(e, target_type, safe=safe)

    @staticmethod
    def coalesce(*exprs: Union[str, "pc.Expression"]) -> "pc.Expression":
        import pyarrow.compute as pc

        es = [E.field(e) if isinstance(e, str) else e for e in exprs]
        return pc.coalesce(*es)
