# Pipeline Semantics

## Core Insight

The entire pipeline is governed by a small set of semantic rules. Each rule says:
- **IF** a table has certain column types
- **THEN** certain operations become available / certain properties emerge

DataFusion, rustworkx, and Hamilton handle the execution. We just define the semantics.

---

## The Semantic Types

```
COLUMN TYPES
============
PATH          := path | file_path | document_path
FILE_ID       := file_id
SPAN_START    := bstart | *_bstart | byte_start | *_byte_start
SPAN_END      := bend | *_bend | byte_end | *_byte_end
ENTITY_ID     := *_id (excluding file_id)
SYMBOL        := symbol | name | qname | qualified_name
TEXT          := *_text | *_name (content to normalize)
EVIDENCE      := confidence | score | origin | resolution_method

TABLE TYPES (derived from column presence)
==========================================
EVIDENCE_TABLE := has(PATH) ∧ has(SPAN_START) ∧ has(SPAN_END)
ENTITY_TABLE   := EVIDENCE_TABLE ∧ has(ENTITY_ID)
SYMBOL_TABLE   := has(SYMBOL)
RELATION_TABLE := has(ENTITY_ID) ∧ has(SYMBOL)
```

---

## The Rules

### Rule 1: Entity ID Derivation

```
IF   table has (PATH, SPAN_START, SPAN_END)
THEN can derive ENTITY_ID := stable_id(prefix, PATH, SPAN_START, SPAN_END)

Params: prefix (string)
Result: new column of type ENTITY_ID
```

### Rule 2: Span Struct Derivation

```
IF   table has (SPAN_START, SPAN_END)
THEN can derive SPAN := span_make(SPAN_START, SPAN_END, "byte")

Params: none
Result: new column of type STRUCT<start, end, byte_span>
```

### Rule 3: Text Normalization

```
IF   table has TEXT columns
THEN can derive NORMALIZED_TEXT := utf8_normalize(TEXT, "NFC")

Params: normalization form (default "NFC")
Result: new column with normalized text
```

### Rule 4: Path Join

```
IF   table_A has PATH ∧ table_B has PATH
THEN can join: table_A ⋈[PATH = PATH] table_B

Params: join type (inner, left, right)
Result: joined table with columns from both
```

### Rule 5: Span Overlap Join

```
IF   table_A is EVIDENCE_TABLE ∧ table_B is EVIDENCE_TABLE ∧ can_path_join(A, B)
THEN can join: table_A ⋈[PATH ∧ span_overlaps(A.SPAN, B.SPAN)] table_B

Params: none
Result: joined table where spans overlap
```

### Rule 6: Span Contains Join

```
IF   table_A is EVIDENCE_TABLE ∧ table_B is EVIDENCE_TABLE ∧ can_path_join(A, B)
THEN can join: table_A ⋈[PATH ∧ span_contains(A.SPAN, B.SPAN)] table_B

Params: which contains which (A contains B, or B contains A)
Result: joined table where one span contains the other
```

### Rule 7: Relationship Projection

```
IF   joined_table has (ENTITY_ID from A) ∧ (SYMBOL from B)
THEN can project to RELATION_TABLE:
     - entity_id := ENTITY_ID
     - symbol := SYMBOL
     - path := PATH
     - bstart := SPAN_START
     - bend := SPAN_END
     - origin := literal(origin_name)

Params: origin_name (string identifying the relationship source)
Result: RELATION_TABLE
```

### Rule 8: Union with Discriminator

```
IF   table_1, table_2, ... table_n have compatible schemas
THEN can union: UNION(table_1, ..., table_n) with discriminator column

Params: discriminator_column_name, value_per_table
Result: unioned table with discriminator
```

### Rule 9: Array Aggregation

```
IF   table has (GROUP_KEY, VALUE_COLUMNS)
THEN can aggregate: GROUP BY GROUP_KEY, array_agg(VALUE_COLUMNS)

Params: group_key columns, value columns, order_by (optional)
Result: table with list<struct> columns
```

### Rule 10: Deduplication

```
IF   table has (KEY_COLUMNS, SCORE_COLUMN)
THEN can dedupe: keep row with best SCORE per KEY

Params: key columns, score column, ascending/descending
Result: deduplicated table
```

---

## Composition: How Rules Chain

```
EXTRACTION TABLE (cst_refs)
    │
    ├─── Rule 1 (Entity ID) ───► ENTITY_TABLE (cst_refs_norm)
    │       └─ Rule 2 (Span Struct)
    │
    ├─── Rule 5 (Span Overlap) with SYMBOL_TABLE (scip_occurrences)
    │       │
    │       └─── Rule 7 (Relationship Projection) ───► RELATION_TABLE (rel_name_symbol)
    │
    └─── ... other relationship chains ...

RELATION_TABLES
    │
    └─── Rule 8 (Union) ───► CPG_EDGES

ENTITY_TABLES
    │
    └─── Rule 8 (Union) ───► CPG_NODES
```

---

## The Semantic Registry

```python
# src/semantics/types.py
"""The type system for pipeline semantics."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Callable
import re


class ColumnType(StrEnum):
    """Semantic column types."""
    PATH = auto()
    FILE_ID = auto()
    SPAN_START = auto()
    SPAN_END = auto()
    ENTITY_ID = auto()
    SYMBOL = auto()
    TEXT = auto()
    EVIDENCE = auto()
    NESTED = auto()
    OTHER = auto()


# Pattern-based type inference
TYPE_PATTERNS: list[tuple[re.Pattern, ColumnType]] = [
    (re.compile(r'^(path|file_path|document_path)$'), ColumnType.PATH),
    (re.compile(r'^file_id$'), ColumnType.FILE_ID),
    (re.compile(r'^bstart$|_bstart$|^byte_start$|_byte_start$'), ColumnType.SPAN_START),
    (re.compile(r'^bend$|_bend$|^byte_end$|_byte_end$'), ColumnType.SPAN_END),
    (re.compile(r'_id$'), ColumnType.ENTITY_ID),  # Must come after file_id
    (re.compile(r'^(symbol|qname|qualified_name)$'), ColumnType.SYMBOL),
    (re.compile(r'^name$|_name$|_text$'), ColumnType.TEXT),
    (re.compile(r'^(confidence|score|origin|resolution_method)$'), ColumnType.EVIDENCE),
]


class TableType(StrEnum):
    """Derived table types based on column presence."""
    RAW = auto()           # No semantic columns recognized
    EVIDENCE = auto()      # Has PATH + SPAN
    ENTITY = auto()        # EVIDENCE + ENTITY_ID
    SYMBOL_SOURCE = auto() # Has SYMBOL
    RELATION = auto()      # Has ENTITY_ID + SYMBOL
    CPG_NODE = auto()      # Final node output
    CPG_EDGE = auto()      # Final edge output


def infer_table_type(column_types: set[ColumnType]) -> TableType:
    """Infer table type from column types present."""
    has_path = ColumnType.PATH in column_types
    has_span = ColumnType.SPAN_START in column_types and ColumnType.SPAN_END in column_types
    has_entity_id = ColumnType.ENTITY_ID in column_types
    has_symbol = ColumnType.SYMBOL in column_types

    if has_entity_id and has_symbol:
        return TableType.RELATION
    if has_entity_id and has_path and has_span:
        return TableType.ENTITY
    if has_path and has_span:
        return TableType.EVIDENCE
    if has_symbol:
        return TableType.SYMBOL_SOURCE
    return TableType.RAW
```

---

## The Rule Engine

```python
# src/semantics/rules.py
"""Rules that define what operations are available."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from semantics.types import ColumnType, TableType

if TYPE_CHECKING:
    from datafusion import DataFrame
    from datafusion.expr import Expr


class Rule(Protocol):
    """A rule defines: condition → available operation."""

    name: str

    def applies(self, table_type: TableType, column_types: set[ColumnType]) -> bool:
        """Does this rule apply to this table?"""
        ...

    def apply(self, df: DataFrame, **params) -> DataFrame:
        """Apply the rule, returning transformed DataFrame."""
        ...


@dataclass(frozen=True)
class DeriveEntityId:
    """Rule 1: Derive entity ID from path + span."""

    name: str = "derive_entity_id"

    def applies(self, table_type: TableType, column_types: set[ColumnType]) -> bool:
        return table_type in (TableType.EVIDENCE, TableType.RAW) and \
               ColumnType.PATH in column_types and \
               ColumnType.SPAN_START in column_types and \
               ColumnType.SPAN_END in column_types

    def apply(self, df: DataFrame, *, prefix: str, sem: SemanticSchema) -> DataFrame:
        from datafusion import lit
        return df.with_column(
            f"{prefix}_id",
            sem.entity_id_expr(prefix)
        )


@dataclass(frozen=True)
class DeriveSpan:
    """Rule 2: Derive span struct from span columns."""

    name: str = "derive_span"

    def applies(self, table_type: TableType, column_types: set[ColumnType]) -> bool:
        return ColumnType.SPAN_START in column_types and ColumnType.SPAN_END in column_types

    def apply(self, df: DataFrame, *, sem: SemanticSchema) -> DataFrame:
        return df.with_column("span", sem.span_expr())


@dataclass(frozen=True)
class JoinBySpanOverlap:
    """Rule 5: Join tables where spans overlap."""

    name: str = "join_span_overlap"

    def applies_to_pair(
        self,
        left_type: TableType, left_cols: set[ColumnType],
        right_type: TableType, right_cols: set[ColumnType],
    ) -> bool:
        # Both must be evidence tables (have path + span)
        left_ok = left_type in (TableType.EVIDENCE, TableType.ENTITY) or \
                  (ColumnType.PATH in left_cols and ColumnType.SPAN_START in left_cols)
        right_ok = right_type in (TableType.EVIDENCE, TableType.ENTITY, TableType.SYMBOL_SOURCE) or \
                   (ColumnType.PATH in right_cols and ColumnType.SPAN_START in right_cols)
        return left_ok and right_ok

    def apply(
        self,
        left: DataFrame, right: DataFrame,
        left_sem: SemanticSchema, right_sem: SemanticSchema,
    ) -> DataFrame:
        from compiler.join_helpers import join_by_span_overlap
        return join_by_span_overlap(left, right, left_sem, right_sem)


@dataclass(frozen=True)
class ProjectToRelation:
    """Rule 7: Project joined table to relationship schema."""

    name: str = "project_relation"

    def applies(self, table_type: TableType, column_types: set[ColumnType]) -> bool:
        # After a join, we should have entity_id and symbol available
        return ColumnType.ENTITY_ID in column_types and ColumnType.SYMBOL in column_types

    def apply(
        self,
        df: DataFrame,
        *,
        entity_col: str,
        symbol_col: str,
        origin: str,
        sem: SemanticSchema,
    ) -> DataFrame:
        from datafusion import col, lit
        return df.select(
            col(entity_col).alias("entity_id"),
            col(symbol_col).alias("symbol"),
            sem.path_col().alias("path"),
            sem.span_start_col().alias("bstart"),
            sem.span_end_col().alias("bend"),
            lit(origin).alias("origin"),
        ).distinct()


# All rules
RULES: list[Rule] = [
    DeriveEntityId(),
    DeriveSpan(),
    JoinBySpanOverlap(),
    ProjectToRelation(),
    # ... more rules
]
```

---

## The Semantic Compiler

```python
# src/semantics/compiler.py
"""Compiles semantic operations to DataFusion plans."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from semantics.types import ColumnType, TableType, infer_table_type, TYPE_PATTERNS
from semantics.rules import RULES

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext


@dataclass
class TableInfo:
    """Semantic information about a table."""
    name: str
    df: DataFrame
    column_types: dict[str, ColumnType] = field(default_factory=dict)
    table_type: TableType = TableType.RAW

    @classmethod
    def analyze(cls, name: str, df: DataFrame) -> TableInfo:
        """Analyze a DataFrame and infer semantic types."""
        schema = df.schema()
        column_types = {}

        for field in schema:
            col_name = field.name
            col_type = ColumnType.OTHER

            for pattern, ctype in TYPE_PATTERNS:
                if pattern.match(col_name):
                    col_type = ctype
                    break

            column_types[col_name] = col_type

        table_type = infer_table_type(set(column_types.values()))

        return cls(name=name, df=df, column_types=column_types, table_type=table_type)

    def available_rules(self) -> list[Rule]:
        """Which rules can be applied to this table?"""
        col_types = set(self.column_types.values())
        return [r for r in RULES if hasattr(r, 'applies') and r.applies(self.table_type, col_types)]


class SemanticCompiler:
    """Compiles high-level operations using semantic rules."""

    def __init__(self, ctx: SessionContext):
        self.ctx = ctx
        self._tables: dict[str, TableInfo] = {}

    def register(self, name: str) -> TableInfo:
        """Register and analyze a table."""
        df = self.ctx.table(name)
        info = TableInfo.analyze(name, df)
        self._tables[name] = info
        return info

    def normalize(self, table_name: str, *, prefix: str) -> DataFrame:
        """Apply normalization rules to an evidence table.

        IF table is EVIDENCE_TABLE
        THEN derive entity_id and span
        """
        info = self._tables.get(table_name) or self.register(table_name)

        if info.table_type not in (TableType.EVIDENCE, TableType.RAW):
            raise ValueError(f"{table_name} is not an evidence table")

        df = info.df
        sem = SemanticSchema.from_df(df)

        # Apply Rule 1: derive entity ID
        df = df.with_column(f"{prefix}_id", sem.entity_id_expr(prefix))

        # Apply Rule 2: derive span
        df = df.with_column("span", sem.span_expr())

        return df

    def relate(
        self,
        left_table: str,
        right_table: str,
        *,
        join_type: str = "overlap",  # or "contains"
        filter_sql: str | None = None,
        origin: str,
    ) -> DataFrame:
        """Build a relationship between two tables.

        IF left is ENTITY_TABLE and right is SYMBOL_SOURCE
        THEN join by span relationship and project to RELATION schema
        """
        left_info = self._tables.get(left_table) or self.register(left_table)
        right_info = self._tables.get(right_table) or self.register(right_table)

        left_sem = SemanticSchema.from_df(left_info.df)
        right_sem = SemanticSchema.from_df(right_info.df)

        # Apply Rule 5 or 6: join by span
        if join_type == "overlap":
            from compiler.join_helpers import join_by_span_overlap
            joined = join_by_span_overlap(left_info.df, right_info.df, left_sem, right_sem)
        else:
            from compiler.join_helpers import join_by_span_contains
            joined = join_by_span_contains(left_info.df, right_info.df, left_sem, right_sem)

        # Apply filter if provided
        if filter_sql:
            joined = joined.filter(filter_sql)

        # Apply Rule 7: project to relation
        # Find entity_id and symbol columns
        entity_col = next(
            (c for c, t in left_info.column_types.items() if t == ColumnType.ENTITY_ID),
            None
        )
        symbol_col = next(
            (c for c, t in right_info.column_types.items() if t == ColumnType.SYMBOL),
            None
        )

        if not entity_col or not symbol_col:
            raise ValueError("Cannot build relation: missing entity_id or symbol")

        from datafusion import col, lit
        return joined.select(
            col(entity_col).alias("entity_id"),
            col(symbol_col).alias("symbol"),
            left_sem.path_col().alias("path"),
            left_sem.span_start_col().alias("bstart"),
            left_sem.span_end_col().alias("bend"),
            lit(origin).alias("origin"),
        ).distinct()

    def union_with_discriminator(
        self,
        table_names: list[str],
        *,
        discriminator: str = "kind",
    ) -> DataFrame:
        """Union tables with a discriminator column.

        IF tables have compatible schemas
        THEN union with discriminator
        """
        dfs = []
        for name in table_names:
            info = self._tables.get(name) or self.register(name)
            from datafusion import lit
            dfs.append(info.df.with_column(discriminator, lit(name)))

        result = dfs[0]
        for df in dfs[1:]:
            result = result.union(df)

        return result
```

---

## Usage: The Entire Pipeline

```python
# src/pipeline.py
"""The CPG pipeline using semantic operations."""
from semantics.compiler import SemanticCompiler


def build_cpg(ctx: SessionContext):
    compiler = SemanticCompiler(ctx)

    # Normalize extraction tables (Rule 1 + Rule 2)
    ctx.register_view("cst_refs_norm", compiler.normalize("cst_refs", prefix="ref"))
    ctx.register_view("cst_defs_norm", compiler.normalize("cst_defs", prefix="def"))
    ctx.register_view("cst_imports_norm", compiler.normalize("cst_imports", prefix="import"))
    ctx.register_view("cst_calls_norm", compiler.normalize("cst_callsites", prefix="call"))

    # Register normalized tables for relationship building
    for name in ["cst_refs_norm", "cst_defs_norm", "cst_imports_norm", "cst_calls_norm"]:
        compiler.register(name)
    compiler.register("scip_occurrences")

    # Build relationships (Rule 5 + Rule 7)
    ctx.register_view("rel_name_symbol", compiler.relate(
        "cst_refs_norm", "scip_occurrences",
        join_type="overlap",
        filter_sql="is_read = true",
        origin="cst_ref",
    ))

    ctx.register_view("rel_def_symbol", compiler.relate(
        "cst_defs_norm", "scip_occurrences",
        join_type="contains",
        filter_sql="is_definition = true",
        origin="cst_def",
    ))

    ctx.register_view("rel_import_symbol", compiler.relate(
        "cst_imports_norm", "scip_occurrences",
        join_type="overlap",
        filter_sql="is_import = true",
        origin="cst_import",
    ))

    ctx.register_view("rel_callsite_symbol", compiler.relate(
        "cst_calls_norm", "scip_occurrences",
        join_type="overlap",
        origin="cst_call",
    ))

    # Build CPG outputs (Rule 8)
    ctx.register_view("cpg_edges", compiler.union_with_discriminator(
        ["rel_name_symbol", "rel_def_symbol", "rel_import_symbol", "rel_callsite_symbol"],
        discriminator="edge_kind",
    ))

    ctx.register_view("cpg_nodes", compiler.union_with_discriminator(
        ["cst_refs_norm", "cst_defs_norm", "cst_imports_norm", "cst_calls_norm"],
        discriminator="node_kind",
    ))
```

---

## Summary: The 10 Rules

| # | Name | Condition | Operation |
|---|------|-----------|-----------|
| 1 | Derive Entity ID | PATH + SPAN | `stable_id(prefix, path, bstart, bend)` |
| 2 | Derive Span | SPAN_START + SPAN_END | `span_make(bstart, bend, "byte")` |
| 3 | Normalize Text | TEXT columns | `utf8_normalize(text, "NFC")` |
| 4 | Path Join | both have PATH | equijoin on path |
| 5 | Span Overlap | both EVIDENCE + path join | filter `span_overlaps(a, b)` |
| 6 | Span Contains | both EVIDENCE + path join | filter `span_contains(a, b)` |
| 7 | Relation Project | ENTITY_ID + SYMBOL | select to relation schema |
| 8 | Union | compatible schemas | union with discriminator |
| 9 | Aggregate | GROUP_KEY + VALUES | `array_agg(values)` |
| 10 | Dedupe | KEY + SCORE | keep best per key |

**These 10 rules generate the entire pipeline.**

---

## What This Enables

```python
# Instead of 3,000 lines of explicit expressions:
ctx.register_view("rel_name_symbol", compiler.relate(
    "cst_refs_norm", "scip_occurrences",
    join_type="overlap",
    filter_sql="is_read = true",
    origin="cst_ref",
))

# The compiler:
# 1. Analyzes schemas → infers column types
# 2. Checks rules → determines available operations
# 3. Applies rules → generates DataFusion expressions
# 4. Registers view → DataFusion handles the rest
```

**The semantics are explicit. The implementation is derived.**
