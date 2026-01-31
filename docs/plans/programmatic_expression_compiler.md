# Programmatic Expression Compiler

## Core Insight

**DataFusion's LogicalPlanBuilder IS the framework.** We don't build a separate "activity" layer - we build expression helpers that plug into DataFusion's native plan building.

The only abstraction we add:
1. **Semantic column discovery** - Find columns by type, not name
2. **Expression templates** - Build expressions using discovered columns
3. **Join inference** - Match tables by compatible semantic types

Everything else is just DataFusion.

---

## The Minimal Layer

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         EXTRACTION TABLES                                │
│            (registered as TableProviders in SessionContext)             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      SEMANTIC COLUMN DISCOVERY                           │
│                                                                          │
│  ctx.table("cst_refs").schema() → classify each column:                 │
│    - file_id → FILE_IDENTITY                                            │
│    - path → PATH                                                        │
│    - bstart, bend → SPAN_START, SPAN_END                                │
│    - ref_text → TEXT                                                    │
│                                                                          │
│  Result: SemanticSchema with find_span(), find_path(), find_text()      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     EXPRESSION TEMPLATES (Rust UDFs)                     │
│                                                                          │
│  Templates that use semantic discovery:                                  │
│                                                                          │
│  entity_id(sem, prefix) = stable_id(prefix, sem.find_path(), sem.span())│
│  span_struct(sem) = span_make(sem.find_span_start(), sem.find_span_end())│
│  overlap_filter(left_sem, right_sem) = span_overlaps(left.span, right.span)│
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATAFUSION LOGICALPLANBUILDER                         │
│                                                                          │
│  ctx.table("cst_refs")                                                  │
│      .join(ctx.table("scip"), left_on=[path], right_on=[path])         │
│      .filter(overlap_filter(left_sem, right_sem))                       │
│      .select(entity_id(left_sem, "ref"), col("symbol"), span_struct())  │
│                                                                          │
│  → DataFusion handles schema propagation, optimization, execution       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## The Actual Code (Minimal)

---

## Part 0: What DataFusion Already Gives Us

DataFusion's `LogicalPlanBuilder` handles:
- **Arbitrary combinations of inputs** via `join`, `join_on`, `union`, etc.
- **Schema propagation** - output schema derived from inputs automatically
- **Expression composition** - `col()`, `lit()`, function calls, nested types
- **Optimization** - push down filters, prune columns, reorder joins

We don't need to reinvent this. We just need helpers that:
1. Discover columns by semantic type (not hardcoded names)
2. Generate expressions that adapt to the discovered columns

---

## The Mental Model

```
Extraction Tables  →  Schema Introspection  →  Semantic Graph  →  Expression Synthesis
                             ↓
                    Column classifications:
                    - span_cols: (bstart, bend), (*_bstart, *_bend)
                    - path_cols: path, file_path, document_path
                    - id_cols: *_id, file_id, node_id
                    - symbol_cols: symbol, name, qname
                    - evidence_cols: confidence, score, origin
```

---

## Part 1: Semantic Schema (The Only Abstraction)

```python
# src/compiler/semantic_schema.py
"""Semantic column discovery - the only abstraction we add."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
import re

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import DataFrame
    from datafusion.expr import Expr


@dataclass(frozen=True)
class SemanticSchema:
    """Schema with semantic column discovery.

    This is the ONLY abstraction we add on top of DataFusion.
    It answers: "given this schema, where are the spans/paths/ids?"
    """

    df: DataFrame  # Keep reference for schema access

    # Discovered column groups (populated lazily)
    _path: str | None = None
    _span_start: str | None = None
    _span_end: str | None = None
    _text_cols: tuple[str, ...] = ()
    _id_cols: tuple[str, ...] = ()

    @classmethod
    def from_df(cls, df: DataFrame) -> SemanticSchema:
        """Analyze a DataFrame and discover semantic columns."""
        schema = df.schema()
        names = schema.names

        # Find path column
        path = next((n for n in names if n in ("path", "document_path", "file_path")), None)

        # Find span columns (match pairs)
        span_start = next((n for n in names if n == "bstart" or n.endswith("_bstart")), None)
        span_end = None
        if span_start:
            prefix = span_start.replace("bstart", "").replace("_bstart", "")
            span_end = next((n for n in names if n in (f"{prefix}bend", f"{prefix}_bend", "bend")), None)

        # Find text columns
        text_cols = tuple(n for n in names if "text" in n.lower() or n in ("name", "symbol"))

        # Find ID columns
        id_cols = tuple(n for n in names if n.endswith("_id") and n != "file_id")

        return cls(
            df=df,
            _path=path,
            _span_start=span_start,
            _span_end=span_end,
            _text_cols=text_cols,
            _id_cols=id_cols,
        )

    # Expression generators (these return DataFusion Exprs)
    def path_col(self) -> Expr:
        """Get path column expression."""
        from datafusion import col
        if self._path is None:
            raise ValueError("No path column found")
        return col(self._path)

    def span_start_col(self) -> Expr:
        """Get span start column expression."""
        from datafusion import col
        if self._span_start is None:
            raise ValueError("No span start column found")
        return col(self._span_start)

    def span_end_col(self) -> Expr:
        """Get span end column expression."""
        from datafusion import col
        if self._span_end is None:
            raise ValueError("No span end column found")
        return col(self._span_end)

    def span_expr(self) -> Expr:
        """Generate span struct expression using discovered columns."""
        from datafusion import lit
        from datafusion_ext import span_make
        return span_make(self.span_start_col(), self.span_end_col(), lit("byte"))

    def entity_id_expr(self, prefix: str) -> Expr:
        """Generate stable entity ID using discovered columns."""
        from datafusion import lit
        from datafusion_ext import stable_id
        return stable_id(lit(prefix), self.path_col(), self.span_start_col(), self.span_end_col())

    def has_span(self) -> bool:
        return self._span_start is not None and self._span_end is not None

    def has_path(self) -> bool:
        return self._path is not None
```

---

## Part 2: Join Helpers (Just Functions)

```python
# src/compiler/join_helpers.py
"""Helper functions for common join patterns. Not a framework - just functions."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import DataFrame
    from compiler.semantic_schema import SemanticSchema


def join_by_path(left: DataFrame, right: DataFrame, left_sem: SemanticSchema, right_sem: SemanticSchema) -> DataFrame:
    """Join two DataFrames on their path columns."""
    return left.join(
        right,
        left_on=[left_sem._path],
        right_on=[right_sem._path],
        how="inner",
    )


def join_by_span_overlap(
    left: DataFrame,
    right: DataFrame,
    left_sem: SemanticSchema,
    right_sem: SemanticSchema,
) -> DataFrame:
    """Join on path, then filter by span overlap."""
    from datafusion_ext import span_overlaps

    joined = join_by_path(left, right, left_sem, right_sem)
    return joined.filter(span_overlaps(left_sem.span_expr(), right_sem.span_expr()))


def join_by_span_contains(
    left: DataFrame,
    right: DataFrame,
    left_sem: SemanticSchema,
    right_sem: SemanticSchema,
) -> DataFrame:
    """Join on path, then filter by span containment (left contains right)."""
    from datafusion_ext import span_contains

    joined = join_by_path(left, right, left_sem, right_sem)
    return joined.filter(span_contains(left_sem.span_expr(), right_sem.span_expr()))
```

That's it. No "JoinKind" enum, no "InferredJoin" dataclass. Just functions you call.

---

## Part 3: That's All The Abstraction We Need

The `SemanticSchema` plus a few join helpers IS the entire abstraction. Everything else is just DataFusion.

---

## Part 4: Usage Pattern (Just DataFusion + Helpers)

```python
# Example: Building a relationship view
# No custom compiler - just DataFusion with semantic helpers

from datafusion import SessionContext, col, lit
from compiler.semantic_schema import SemanticSchema
from compiler.join_helpers import join_by_span_overlap

ctx = SessionContext()
# ... register tables ...

# Load tables and analyze semantics
cst_refs = ctx.table("cst_refs")
scip_occs = ctx.table("scip_occurrences")

cst_sem = SemanticSchema.from_df(cst_refs)
scip_sem = SemanticSchema.from_df(scip_occs)

# Build the relationship view using DataFusion's native API
# with semantic helpers for column discovery
rel_name_symbol = (
    join_by_span_overlap(cst_refs, scip_occs, cst_sem, scip_sem)
    .filter("is_read = true")
    .select(
        cst_sem.entity_id_expr("ref").alias("entity_id"),
        col("symbol"),
        cst_sem.path_col().alias("path"),
        cst_sem.span_start_col().alias("bstart"),
        cst_sem.span_end_col().alias("bend"),
        lit("cst_ref").alias("origin"),
    )
    .distinct()
)

ctx.register_view("rel_name_symbol", rel_name_symbol)
```

That's it. No "ExpressionCompiler" class, no "RelationshipDef" specs. Just:
1. Load table → analyze semantics
2. Use DataFusion's native API
3. Use semantic helpers for column discovery

---

## Part 5: The Full Pipeline (Still Just Functions)

```python
# src/pipeline/cpg.py
"""The CPG pipeline - functions that build DataFrames."""
from __future__ import annotations

from datafusion import SessionContext, col, lit
from compiler.semantic_schema import SemanticSchema
from compiler.join_helpers import join_by_span_overlap, join_by_span_contains


def build_normalized_refs(ctx: SessionContext) -> None:
    """Normalize CST refs with stable IDs and spans."""
    df = ctx.table("cst_refs")
    sem = SemanticSchema.from_df(df)

    normalized = df.select(
        sem.entity_id_expr("ref").alias("ref_id"),
        col("file_id"),
        sem.path_col().alias("path"),
        col("ref_text"),
        sem.span_expr().alias("span"),
        sem.span_start_col().alias("bstart"),
        sem.span_end_col().alias("bend"),
    )
    ctx.register_view("cst_refs_norm", normalized)


def build_rel_name_symbol(ctx: SessionContext) -> None:
    """Build name→symbol relationship."""
    cst = ctx.table("cst_refs_norm")
    scip = ctx.table("scip_occurrences")
    cst_sem = SemanticSchema.from_df(cst)
    scip_sem = SemanticSchema.from_df(scip)

    rel = (
        join_by_span_overlap(cst, scip, cst_sem, scip_sem)
        .filter("is_read = true")
        .select(
            col("ref_id").alias("entity_id"),
            col("symbol"),
            cst_sem.path_col().alias("path"),
            cst_sem.span_start_col().alias("bstart"),
            cst_sem.span_end_col().alias("bend"),
            lit("cst_ref").alias("origin"),
        )
        .distinct()
    )
    ctx.register_view("rel_name_symbol", rel)


def build_rel_def_symbol(ctx: SessionContext) -> None:
    """Build def→symbol relationship (def contains occurrence)."""
    cst = ctx.table("cst_defs_norm")
    scip = ctx.table("scip_occurrences")
    cst_sem = SemanticSchema.from_df(cst)
    scip_sem = SemanticSchema.from_df(scip)

    rel = (
        join_by_span_contains(cst, scip, cst_sem, scip_sem)
        .filter("is_definition = true")
        .select(
            col("def_id").alias("entity_id"),
            col("symbol"),
            cst_sem.path_col().alias("path"),
            cst_sem.span_start_col().alias("bstart"),
            cst_sem.span_end_col().alias("bend"),
            lit("cst_def").alias("origin"),
        )
        .distinct()
    )
    ctx.register_view("rel_def_symbol", rel)


def build_cpg_edges(ctx: SessionContext) -> None:
    """Union all relationship tables into CPG edges."""
    rel_tables = ["rel_name_symbol", "rel_def_symbol", "rel_import_symbol", "rel_callsite_symbol"]

    # Load and tag each table
    dfs = []
    for name in rel_tables:
        df = ctx.table(name).with_column("edge_kind", lit(name.replace("rel_", "")))
        dfs.append(df)

    # Union
    result = dfs[0]
    for df in dfs[1:]:
        result = result.union(df)

    ctx.register_view("cpg_edges", result)


def build_pipeline(ctx: SessionContext) -> None:
    """Execute the full pipeline."""
    # Order matters - DataFusion handles the rest
    build_normalized_refs(ctx)
    build_normalized_defs(ctx)
    build_normalized_imports(ctx)
    build_normalized_callsites(ctx)

    build_rel_name_symbol(ctx)
    build_rel_def_symbol(ctx)
    build_rel_import_symbol(ctx)
    build_rel_callsite_symbol(ctx)

    build_cpg_nodes(ctx)
    build_cpg_edges(ctx)
```

**Key insight**: These are just functions. DataFusion handles:
- Schema propagation
- Query optimization
- Execution planning
- Arbitrary input combinations

---

## Why This Works

### 1. Schema Changes → Update SemanticSchema Patterns

When `bstart` becomes `byte_start`:
- Add pattern to SemanticSchema: `r'^byte_start$|_byte_start$'`
- All pipeline functions automatically find the new column

### 2. Rustworkx Gets Dependencies From Plans

```python
# After building views, extract deps for scheduling
def get_deps_for_rustworkx(ctx: SessionContext, view_names: list[str]) -> list[InferredDeps]:
    deps = []
    for name in view_names:
        df = ctx.table(name)
        plan = df.optimized_logical_plan()
        lineage = extract_lineage(plan)

        deps.append(InferredDeps(
            task_name=name,
            output=name,
            inputs=frozenset(lineage.referenced_tables),
            required_columns=dict(lineage.required_columns_by_dataset),
            plan_fingerprint=hash_plan(plan),
        ))
    return deps
```

### 3. What We Delete

| Current | Lines | After |
|---------|-------|-------|
| `_VIEW_SELECT_EXPRS` dict | 3,000 | Pipeline functions (~500 lines) |
| Schema registries | 1,500 | `SemanticSchema.from_df()` |
| Custom spec classes | 2,000 | Nothing - use DataFusion directly |
| Join inference code | 500 | Join helper functions (~50 lines) |

**Total: ~7,000 lines → ~550 lines**

---

## Summary

```
┌────────────────────────────────────────────────────────────────────────┐
│                    THE MINIMAL ARCHITECTURE                             │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. Extraction tables (registered in SessionContext)                   │
│                                                                         │
│  2. SemanticSchema - discovers columns by type                         │
│     • find path, span, id columns                                      │
│     • generate expressions (entity_id, span_struct)                    │
│                                                                         │
│  3. Join helpers - functions, not framework                            │
│     • join_by_path()                                                   │
│     • join_by_span_overlap()                                           │
│     • join_by_span_contains()                                          │
│                                                                         │
│  4. Pipeline functions - build DataFrames using DataFusion API         │
│     • build_normalized_refs(ctx)                                       │
│     • build_rel_name_symbol(ctx)                                       │
│     • build_cpg_edges(ctx)                                             │
│                                                                         │
│  5. Plan introspection → InferredDeps → rustworkx                      │
│                                                                         │
└────────────────────────────────────────────────────────────────────────┘

DataFusion handles everything else:
• Schema propagation
• Query optimization
• Arbitrary input combinations
• Execution planning
```

**No custom compiler framework. Just functions + DataFusion.**
