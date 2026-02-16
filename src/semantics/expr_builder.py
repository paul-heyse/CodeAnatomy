"""Expression-builder helpers for semantic schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion.expr import Expr

    from semantics.schema import SemanticSchema


class SemanticExprBuilder:
    """Build DataFusion expressions from semantic-schema column roles."""

    def __init__(self, schema: SemanticSchema) -> None:
        """Initialize expression builder for a resolved semantic schema."""
        self._schema = schema

    def path_col(self) -> Expr:
        """Return path-column expression."""
        from datafusion import col

        return col(self._schema.path_name())

    def span_start_col(self) -> Expr:
        """Return span-start expression."""
        return _expr_for_path(self._schema.span_start_name())

    def span_end_col(self) -> Expr:
        """Return span-end expression."""
        end_name = self._schema.span_end_name()
        if end_name.endswith("byte_len"):
            return _expr_for_path(self._schema.span_start_name()) + _expr_for_path(end_name)
        return _expr_for_path(end_name)

    def entity_id_col(self) -> Expr:
        """Return primary entity-id expression."""
        from datafusion import col

        return col(self._schema.entity_id_name())

    def symbol_col(self) -> Expr:
        """Return primary symbol expression."""
        from datafusion import col

        return col(self._schema.symbol_name())

    def span_expr(self) -> Expr:
        """Return span struct expression via ``span_make`` UDF."""
        from datafusion_engine.udf.expr import udf_expr

        return udf_expr("span_make", self.span_start_col(), self.span_end_col())

    def entity_id_expr(self, prefix: str) -> Expr:
        """Return stable entity-id expression via ``stable_id`` UDF."""
        from datafusion import functions as f

        from datafusion_engine.udf.expr import udf_expr

        joined = f.concat_ws(
            "\x1f",
            self.path_col().cast(pa.string()),
            self.span_start_col().cast(pa.string()),
            self.span_end_col().cast(pa.string()),
        )
        return udf_expr("stable_id", prefix, joined)


def _expr_for_path(path: str) -> Expr:
    from datafusion import col

    parts = path.split(".")
    expr = col(parts[0])
    for part in parts[1:]:
        expr = expr[part]
    return expr


__all__ = ["SemanticExprBuilder"]
