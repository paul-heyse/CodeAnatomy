"""Semantic schema discovery - column discovery by type, not name.

This is the core abstraction: given a DataFrame, find the semantic columns
and generate expressions using them. The schema is analyzed once, then
expressions are generated on demand.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from semantics.column_types import ColumnType, TableType, infer_column_type, infer_table_type
from semantics.config import SemanticConfig

if TYPE_CHECKING:
    from datafusion import DataFrame
    from datafusion.expr import Expr


class SemanticSchemaError(ValueError):
    """Raised when a semantic schema requirement is not met."""


@dataclass(frozen=True)
class SemanticSchema:
    """Schema with semantic column discovery.

    Discovers columns by semantic type, not explicit name. This means
    when column names change (bstart -> byte_start), the schema patterns
    update in one place and all downstream code adapts.

    Attributes
    ----------
    column_types
        Mapping of column name to semantic type.
    table_type
        Derived table type.
    _path
        Discovered path column name.
    _span_start
        Discovered span start column name.
    _span_end
        Discovered span end column name.
    _span_unit
        Span unit inferred from column names or specs.
    _entity_ids
        Discovered entity ID column names.
    _symbols
        Discovered symbol column names.
    _texts
        Discovered text column names.
    """

    column_types: dict[str, ColumnType] = field(default_factory=dict)
    table_type: TableType = TableType.RAW
    _path: str | None = None
    _span_start: str | None = None
    _span_end: str | None = None
    _span_unit: str | None = None
    _entity_ids: tuple[str, ...] = ()
    _symbols: tuple[str, ...] = ()
    _texts: tuple[str, ...] = ()

    @classmethod
    def from_df(
        cls,
        df: DataFrame,
        *,
        table_name: str | None = None,
        config: SemanticConfig | None = None,
    ) -> SemanticSchema:
        """Analyze a DataFrame and discover semantic columns.

        Parameters
        ----------
        df
            DataFrame to analyze.
        table_name
            Optional table name used for overrides and error context.
        config
            Optional semantic configuration overrides.

        Returns
        -------
        SemanticSchema
            Schema with discovered columns.
        """
        schema = df.schema()
        resolved_config = config or SemanticConfig()
        overrides = resolved_config.overrides_for(table_name)
        column_types: dict[str, ColumnType] = {}
        candidates: dict[ColumnType, list[str]] = {
            ColumnType.PATH: [],
            ColumnType.SPAN_START: [],
            ColumnType.SPAN_END: [],
            ColumnType.ENTITY_ID: [],
            ColumnType.SYMBOL: [],
            ColumnType.TEXT: [],
        }

        for fld in schema:
            col_name = fld.name
            col_type = infer_column_type(col_name, patterns=resolved_config.type_patterns)
            if col_type == ColumnType.ENTITY_ID and _matches_any(
                col_name,
                resolved_config.disallow_entity_id_patterns,
            ):
                col_type = ColumnType.OTHER
            column_types[col_name] = col_type

            # Collect candidates for semantic type resolution
            if col_type in candidates:
                candidates[col_type].append(col_name)

        path = candidates[ColumnType.PATH][0] if candidates[ColumnType.PATH] else None
        span_start = _pick_primary(
            candidates[ColumnType.SPAN_START],
            prefer=("bstart", "byte_start"),
        )
        span_end = _pick_primary(
            candidates[ColumnType.SPAN_END],
            prefer=("bend", "byte_end"),
        )
        entity_ids = candidates[ColumnType.ENTITY_ID]
        symbols = candidates[ColumnType.SYMBOL]
        texts = candidates[ColumnType.TEXT]

        path = _apply_override(
            overrides,
            column_types=column_types,
            column_type=ColumnType.PATH,
            current=path,
            table=table_name,
        )
        span_start = _apply_override(
            overrides,
            column_types=column_types,
            column_type=ColumnType.SPAN_START,
            current=span_start,
            table=table_name,
        )
        span_end = _apply_override(
            overrides,
            column_types=column_types,
            column_type=ColumnType.SPAN_END,
            current=span_end,
            table=table_name,
        )
        entity_ids = _apply_list_override(
            overrides,
            column_types=column_types,
            column_type=ColumnType.ENTITY_ID,
            current=entity_ids,
            table=table_name,
        )
        if ColumnType.ENTITY_ID not in overrides:
            entity_ids = _prefer_entity_id(entity_ids)
        symbols = _apply_list_override(
            overrides,
            column_types=column_types,
            column_type=ColumnType.SYMBOL,
            current=symbols,
            table=table_name,
        )
        texts = _apply_list_override(
            overrides,
            column_types=column_types,
            column_type=ColumnType.TEXT,
            current=texts,
            table=table_name,
        )

        table_type = infer_table_type(set(column_types.values()))
        span_unit = _infer_span_unit(span_start)

        return cls(
            column_types=column_types,
            table_type=table_type,
            _path=path,
            _span_start=span_start,
            _span_end=span_end,
            _span_unit=span_unit,
            _entity_ids=tuple(entity_ids),
            _symbols=tuple(symbols),
            _texts=tuple(texts),
        )

    # -------------------------------------------------------------------------
    # Predicates: What does this schema have?
    # -------------------------------------------------------------------------

    def _has_path(self) -> bool:
        """Check if schema has a path column.

        Returns
        -------
        bool
            ``True`` when a path column is present.
        """
        return self._path is not None

    def _has_span(self) -> bool:
        """Check if schema has complete span columns.

        Returns
        -------
        bool
            ``True`` when both span start and end columns are present.
        """
        return self._span_start is not None and self._span_end is not None

    def _has_entity_id(self) -> bool:
        """Check if schema has entity ID columns.

        Returns
        -------
        bool
            ``True`` when entity ID columns are present.
        """
        return len(self._entity_ids) > 0

    def _has_span_unit(self) -> bool:
        """Check if schema has span unit metadata.

        Returns
        -------
        bool
            ``True`` when span unit metadata is present.
        """
        return self._span_unit is not None

    def _has_symbol(self) -> bool:
        """Check if schema has symbol columns.

        Returns
        -------
        bool
            ``True`` when symbol columns are present.
        """
        return len(self._symbols) > 0

    def require_evidence(self, *, table: str) -> None:
        """Ensure the schema satisfies evidence requirements.

        Parameters
        ----------
        table
            Table name for error context.

        Raises
        ------
        SemanticSchemaError
            Raised when the schema is not evidence-compatible.
        """
        if not self._is_evidence():
            msg = f"Table {table!r} is not an evidence table."
            raise SemanticSchemaError(msg)

    def require_entity(self, *, table: str) -> None:
        """Ensure the schema satisfies entity requirements.

        Parameters
        ----------
        table
            Table name for error context.

        Raises
        ------
        SemanticSchemaError
            Raised when the schema is not entity-compatible.
        """
        if not self._is_entity():
            msg = f"Table {table!r} is not an entity table."
            raise SemanticSchemaError(msg)

    def require_symbol_source(self, *, table: str) -> None:
        """Ensure the schema satisfies symbol source requirements.

        Parameters
        ----------
        table
            Table name for error context.

        Raises
        ------
        SemanticSchemaError
            Raised when the schema is not symbol-source compatible.
        """
        if not self._has_symbol():
            msg = f"Table {table!r} is not a symbol source table."
            raise SemanticSchemaError(msg)

    def require_span_unit(self, *, table: str) -> str:
        """Ensure span unit metadata is available.

        Parameters
        ----------
        table
            Table name for error context.

        Returns
        -------
        str
            Span unit string.

        Raises
        ------
        SemanticSchemaError
            Raised when span unit metadata is unavailable.
        """
        if not self._has_span_unit():
            msg = f"Table {table!r} does not declare a span unit."
            raise SemanticSchemaError(msg)
        span_unit = self._span_unit
        if span_unit is None:
            msg = f"Table {table!r} does not declare a span unit."
            raise SemanticSchemaError(msg)
        return span_unit

    def _is_evidence(self) -> bool:
        """Check if schema qualifies as evidence table.

        Returns
        -------
        bool
            ``True`` when path and span columns are present.
        """
        return self._has_path() and self._has_span()

    def _is_entity(self) -> bool:
        """Check if schema qualifies as entity table.

        Returns
        -------
        bool
            ``True`` when evidence columns and entity IDs are present.
        """
        return self._is_evidence() and self._has_entity_id()

    # -------------------------------------------------------------------------
    # Column accessors: Get the discovered column names
    # -------------------------------------------------------------------------

    def path_name(self) -> str:
        """Get the path column name.

        Returns
        -------
        str
            Path column name.

        Raises
        ------
        ValueError
            Raised when no path column is present.
        """
        if self._path is None:
            msg = "No path column found"
            raise ValueError(msg)
        return self._path

    def span_start_name(self) -> str:
        """Get the span start column name.

        Returns
        -------
        str
            Span start column name.

        Raises
        ------
        ValueError
            Raised when no span start column is present.
        """
        if self._span_start is None:
            msg = "No span start column found"
            raise ValueError(msg)
        return self._span_start

    def span_end_name(self) -> str:
        """Get the span end column name.

        Returns
        -------
        str
            Span end column name.

        Raises
        ------
        ValueError
            Raised when no span end column is present.
        """
        if self._span_end is None:
            msg = "No span end column found"
            raise ValueError(msg)
        return self._span_end

    def entity_id_name(self) -> str:
        """Get the first entity ID column name.

        Returns
        -------
        str
            Entity ID column name.

        Raises
        ------
        ValueError
            Raised when no entity ID columns are present.
        """
        if not self._entity_ids:
            msg = "No entity ID column found"
            raise ValueError(msg)
        return self._entity_ids[0]

    def symbol_name(self) -> str:
        """Get the first symbol column name.

        Returns
        -------
        str
            Symbol column name.

        Raises
        ------
        ValueError
            Raised when no symbol columns are present.
        """
        if not self._symbols:
            msg = "No symbol column found"
            raise ValueError(msg)
        return self._symbols[0]

    def text_names(self) -> tuple[str, ...]:
        """Get all text column names.

        Returns
        -------
        tuple[str, ...]
            Text column names.
        """
        return self._texts

    def _span_start_candidates(self) -> tuple[str, ...]:
        """Return candidate span start columns in schema order.

        Returns
        -------
        tuple[str, ...]
            Span start column candidates.
        """
        return tuple(
            name
            for name, col_type in self.column_types.items()
            if col_type == ColumnType.SPAN_START
        )

    def _span_end_candidates(self) -> tuple[str, ...]:
        """Return candidate span end columns in schema order.

        Returns
        -------
        tuple[str, ...]
            Span end column candidates.
        """
        return tuple(
            name for name, col_type in self.column_types.items() if col_type == ColumnType.SPAN_END
        )

    def _has_ambiguous_span(self) -> bool:
        """Return True when multiple span candidates exist.

        Returns
        -------
        bool
            ``True`` when multiple span candidates exist.
        """
        return len(self._span_start_candidates()) > 1 or len(self._span_end_candidates()) > 1

    def prefixed(self, prefix: str) -> SemanticSchema:
        """Return a SemanticSchema with prefixed column names.

        Parameters
        ----------
        prefix
            Prefix to apply to all column names.

        Returns
        -------
        SemanticSchema
            Prefixed semantic schema.
        """

        def _apply(name: str | None) -> str | None:
            if name is None:
                return None
            return f"{prefix}{name}"

        return SemanticSchema(
            column_types={
                f"{prefix}{name}": col_type for name, col_type in self.column_types.items()
            },
            table_type=self.table_type,
            _path=_apply(self._path),
            _span_start=_apply(self._span_start),
            _span_end=_apply(self._span_end),
            _span_unit=self._span_unit,
            _entity_ids=tuple(f"{prefix}{name}" for name in self._entity_ids),
            _symbols=tuple(f"{prefix}{name}" for name in self._symbols),
            _texts=tuple(f"{prefix}{name}" for name in self._texts),
        )

    # -------------------------------------------------------------------------
    # Expression generators: Build DataFusion expressions
    # -------------------------------------------------------------------------

    def path_col(self) -> Expr:
        """Get path column expression.

        Returns
        -------
        Expr
            DataFusion column expression.
        """
        from datafusion import col

        return col(self.path_name())

    def span_start_col(self) -> Expr:
        """Get span start column expression.

        Returns
        -------
        Expr
            DataFusion column expression.
        """
        from datafusion import col

        return col(self.span_start_name())

    def span_end_col(self) -> Expr:
        """Get span end column expression.

        Returns
        -------
        Expr
            DataFusion column expression.
        """
        from datafusion import col

        return col(self.span_end_name())

    def entity_id_col(self) -> Expr:
        """Get first entity ID column expression.

        Returns
        -------
        Expr
            DataFusion column expression.
        """
        from datafusion import col

        return col(self.entity_id_name())

    def symbol_col(self) -> Expr:
        """Get first symbol column expression.

        Returns
        -------
        Expr
            DataFusion column expression.
        """
        from datafusion import col

        return col(self.symbol_name())

    def span_expr(self) -> Expr:
        """Generate span struct expression using Rust UDF.

        Returns struct<start: i64, end: i64, unit: str>.

        Returns
        -------
        Expr
            Span struct expression.
        """
        from datafusion_engine.udf.shims import span_make

        # span_make(bstart, bend) -> struct
        return span_make(self.span_start_col(), self.span_end_col())

    def entity_id_expr(self, prefix: str) -> Expr:
        """Generate stable entity ID using Rust UDF.

        Parameters
        ----------
        prefix
            Entity type prefix (e.g., "ref", "def", "call").

        Returns
        -------
        Expr
            stable_id(prefix, path, bstart, bend) expression.
        """
        from datafusion_engine.udf.shims import stable_id_parts

        return stable_id_parts(
            prefix,
            self.path_col(),
            self.span_start_col(),
            self.span_end_col(),
        )


def _matches_any(name: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    return any(pattern.search(name) for pattern in patterns)


def _apply_override(
    overrides: Mapping[ColumnType, str],
    *,
    column_types: Mapping[str, ColumnType],
    column_type: ColumnType,
    current: str | None,
    table: str | None,
) -> str | None:
    override = overrides.get(column_type)
    if override is None:
        return current
    if override not in column_types:
        msg = f"Override column {override!r} not found for table {table!r}."
        raise SemanticSchemaError(msg)
    return override


def _apply_list_override(
    overrides: Mapping[ColumnType, str],
    *,
    column_types: Mapping[str, ColumnType],
    column_type: ColumnType,
    current: list[str],
    table: str | None,
) -> list[str]:
    override = overrides.get(column_type)
    if override is None:
        return current
    if override not in column_types:
        msg = f"Override column {override!r} not found for table {table!r}."
        raise SemanticSchemaError(msg)
    if override in current:
        return [override, *[name for name in current if name != override]]
    return [override, *current]


def _pick_primary(candidates: list[str], *, prefer: tuple[str, ...]) -> str | None:
    for name in prefer:
        if name in candidates:
            return name
    if candidates:
        return candidates[0]
    return None


def _prefer_entity_id(entity_ids: list[str]) -> list[str]:
    if "entity_id" not in entity_ids:
        return entity_ids
    return ["entity_id", *[name for name in entity_ids if name != "entity_id"]]


def _infer_span_unit(span_start: str | None) -> str | None:
    if span_start is None:
        return None
    if span_start == "bstart" or span_start.endswith("_bstart"):
        return "byte"
    if span_start == "byte_start" or span_start.endswith("_byte_start"):
        return "byte"
    return None


__all__ = ["SemanticSchema", "SemanticSchemaError"]
