# DataFrame Expression Minimalist Architecture

## Executive Summary

This document defines a **minimalist pipeline architecture** where:
1. **Extraction schemas are constants** (frozen PyArrow schemas)
2. **DataFrame expressions ARE the compiler** (no intermediate spec layers)
3. **47 Rust UDFs provide calculation primitives** (hashing, spans, normalization, aggregation)
4. **Delta Lake writes are the only output** (no intermediate materialization)

**Core principle:** If DataFusion expressions can express it, don't build abstractions over it.

---

## Critical Gap Analysis: Extraction → Rustworkx

### The User's Core Question

> a) What do we get from extraction?
> b) What do we need going into rustworkx?
> The difference between those we need to formalize and redesign.

### Answer: The Gap Is the Expression Compiler

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    EXTRACTION OUTPUT (What We Get)                       │
├─────────────────────────────────────────────────────────────────────────┤
│  • PyArrow Tables with frozen schemas                                    │
│  • Nested structures: LIST<STRUCT> for nodes, edges, imports, etc.       │
│  • File identity fields: file_id, path, file_sha256                     │
│  • Byte spans: bstart, bend                                             │
│  • No dependency info, no task metadata, no plan fingerprints           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│            THE GAP: DataFrame Expression Compiler                        │
├─────────────────────────────────────────────────────────────────────────┤
│  ViewDef(name, build_fn) → df.optimized_logical_plan() → LineageReport  │
│                                                                          │
│  What the compiler provides:                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │ • output_schema       ← df.schema()                                 ││
│  │ • input_tables        ← lineage.referenced_tables                   ││
│  │ • required_columns    ← lineage.required_columns_by_dataset         ││
│  │ • required_udfs       ← expression analysis                         ││
│  │ • plan_fingerprint    ← hash(optimized_logical_plan)                ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    RUSTWORKX INPUT (What We Need)                        │
├─────────────────────────────────────────────────────────────────────────┤
│  InferredDeps {                                                          │
│      task_name: str,                    # ← ViewDef.name                │
│      output: str,                       # ← ViewDef.name                │
│      inputs: frozenset[str],            # ← lineage.referenced_tables   │
│      required_columns: dict[str, tuple],# ← lineage.required_columns_*  │
│      required_udfs: frozenset[str],     # ← expression analysis         │
│      plan_fingerprint: str,             # ← hash(plan)                  │
│  }                                                                       │
└─────────────────────────────────────────────────────────────────────────┘
```

### The Key Insight

**Everything rustworkx needs can be derived from DataFusion plan introspection.**

There is no need for:
- Manual `inputs=` declarations on tasks
- Separate schema registries
- Contract specification files
- Dependency configuration

The expression compiler IS the bridge. Here's how:

```python
# The minimal interface between expression compiler and rustworkx
def compile_views_for_scheduling(
    ctx: SessionContext,
    view_defs: list[ViewDef],
) -> list[InferredDeps]:
    """Bridge from view definitions to rustworkx input."""
    results = []

    for view in view_defs:
        df = view.build(ctx)
        plan = df.optimized_logical_plan()
        lineage = extract_lineage(plan)

        results.append(InferredDeps(
            task_name=view.name,
            output=view.name,
            inputs=frozenset(lineage.referenced_tables),
            required_columns=dict(lineage.required_columns_by_dataset),
            required_udfs=frozenset(lineage.required_udfs),
            plan_fingerprint=hash_plan(plan),
        ))

    return results
```

### What This Eliminates

| Eliminated | Lines | Was Doing | Now Done By |
|------------|-------|-----------|-------------|
| `_VIEW_SELECT_EXPRS` dict | 3,000 | Manual column expressions | View builder functions |
| Schema registries | 1,500 | Duplicate schema defs | Extraction schema constants |
| `src/schema_spec/*.py` specs | 3,000+ | Relationship/node specs | Data-driven ViewDef |
| Task declarations | 500 | Manual inputs= | Plan introspection |
| Contract validation | 500 | Post-hoc schema checks | df.schema() at compile time |

### The Minimal Add-Ons (Part of Compiler, Not Separate)

These are expression patterns, not abstraction layers:

1. **Deduplication** → `row_number().over(partition).filter(__rn == 1)`
2. **Span operations** → `span_make(bstart, bend)`, `span_overlaps(a, b)`
3. **ID generation** → `stable_id("prefix", col1, col2)`
4. **Normalization** → `utf8_normalize(text)`, `qname_normalize(name)`

All of these are **Rust UDFs called from expressions**, not separate modules.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        EXTRACTION (Constants)                        │
│  ┌─────────┐ ┌─────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────┐  │
│  │   AST   │ │   CST   │ │ Bytecode │ │ Symtable │ │ SCIP/TS/Git │  │
│  └────┬────┘ └────┬────┘ └────┬─────┘ └────┬─────┘ └──────┬──────┘  │
│       │           │           │            │              │          │
│       └───────────┴───────────┴────────────┴──────────────┘          │
│                              │                                        │
│                    Arrow Tables (frozen schemas)                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                     DATAFUSION SESSION                               │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ ctx.register_table("ast_nodes", ast_table)                     │ │
│  │ ctx.register_table("cst_refs", cst_refs_table)                 │ │
│  │ ctx.register_table("scip_occurrences", scip_table)             │ │
│  │ ...                                                            │ │
│  └────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ install_all_udfs(ctx)  # 47 Rust UDFs                          │ │
│  │   - Hashing: stable_hash64, stable_id, prefixed_hash_parts     │ │
│  │   - Spans: span_make, span_len, span_overlaps, span_contains   │ │
│  │   - Text: utf8_normalize, qname_normalize, null_if_blank       │ │
│  │   - Aggregates: collect_set, count_if, any_value_det, arg_max  │ │
│  │   - Windows: row_number, lag, lead                             │ │
│  └────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                   VIEW DEFINITIONS (Pure Expressions)                │
│                                                                      │
│  views = [                                                           │
│      ViewDef("cst_refs_norm", deps=["cst_refs"], expr=lambda ctx:   │
│          ctx.table("cst_refs").select(                              │
│              stable_id("ref", col("path"), col("bstart")).alias(    │
│                  "ref_id"),                                         │
│              col("path"),                                           │
│              utf8_normalize(col("ref_text")).alias("text"),         │
│              span_make(col("bstart"), col("bend")).alias("span"),   │
│          )                                                          │
│      ),                                                             │
│      ViewDef("rel_name_symbol", deps=["cst_refs_norm", "scip"], ...│
│      ...                                                            │
│  ]                                                                  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                      EXECUTION (Simple Loop)                         │
│                                                                      │
│  for view in topological_sort(views):                               │
│      df = view.build(ctx)                                           │
│      ctx.register_view(view.name, df)                               │
│      if view.name in OUTPUTS:                                       │
│          write_delta(df, f"tables/{view.name}")                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Part 1: Extraction Schemas (Constants)

### 1.1 Frozen Schema Definitions

All extraction schemas are **compile-time constants**. No schema generation, inference, or derivation at runtime.

```python
# src/extract/schemas.py
"""Frozen extraction schemas. Do not modify at runtime."""
from __future__ import annotations

import pyarrow as pa

# ─────────────────────────────────────────────────────────────────────
# File Identity Bundle (shared by all extractions)
# ─────────────────────────────────────────────────────────────────────
FILE_ID = pa.field("file_id", pa.string(), nullable=False)
PATH = pa.field("path", pa.string(), nullable=False)
FILE_SHA256 = pa.field("file_sha256", pa.string(), nullable=True)

FILE_IDENTITY_FIELDS = (FILE_ID, PATH, FILE_SHA256)

# ─────────────────────────────────────────────────────────────────────
# Byte Span Bundle (shared by span-aware extractions)
# ─────────────────────────────────────────────────────────────────────
BSTART = pa.field("bstart", pa.int64(), nullable=False)
BEND = pa.field("bend", pa.int64(), nullable=False)

SPAN_FIELDS = (BSTART, BEND)

# ─────────────────────────────────────────────────────────────────────
# CST Extraction Schemas
# ─────────────────────────────────────────────────────────────────────
CST_REFS_SCHEMA = pa.schema([
    *FILE_IDENTITY_FIELDS,
    pa.field("ref_kind", pa.string()),
    pa.field("ref_text", pa.string()),
    pa.field("expr_ctx", pa.string()),
    pa.field("scope_type", pa.string()),
    pa.field("scope_name", pa.string()),
    pa.field("parent_kind", pa.string()),
    *SPAN_FIELDS,
])

CST_DEFS_SCHEMA = pa.schema([
    *FILE_IDENTITY_FIELDS,
    pa.field("kind", pa.string()),
    pa.field("name", pa.string()),
    pa.field("def_bstart", pa.int64()),
    pa.field("def_bend", pa.int64()),
    pa.field("name_bstart", pa.int64()),
    pa.field("name_bend", pa.int64()),
    pa.field("docstring", pa.string(), nullable=True),
    pa.field("decorator_count", pa.int32()),
    pa.field("qnames", pa.list_(pa.struct([
        ("name", pa.string()),
        ("source", pa.string()),
    ]))),
])

CST_IMPORTS_SCHEMA = pa.schema([
    *FILE_IDENTITY_FIELDS,
    pa.field("kind", pa.string()),
    pa.field("module", pa.string(), nullable=True),
    pa.field("relative_level", pa.int32()),
    pa.field("name", pa.string()),
    pa.field("asname", pa.string(), nullable=True),
    pa.field("is_star", pa.bool_()),
    pa.field("stmt_bstart", pa.int64()),
    pa.field("stmt_bend", pa.int64()),
    pa.field("alias_bstart", pa.int64()),
    pa.field("alias_bend", pa.int64()),
])

CST_CALLSITES_SCHEMA = pa.schema([
    *FILE_IDENTITY_FIELDS,
    pa.field("call_bstart", pa.int64()),
    pa.field("call_bend", pa.int64()),
    pa.field("callee_bstart", pa.int64()),
    pa.field("callee_bend", pa.int64()),
    pa.field("callee_shape", pa.string()),
    pa.field("callee_text", pa.string()),
    pa.field("arg_count", pa.int32()),
    pa.field("callee_dotted", pa.string(), nullable=True),
    pa.field("callee_qnames", pa.list_(pa.struct([
        ("name", pa.string()),
        ("source", pa.string()),
    ]))),
])

# ─────────────────────────────────────────────────────────────────────
# SCIP Extraction Schemas
# ─────────────────────────────────────────────────────────────────────
SCIP_OCCURRENCES_SCHEMA = pa.schema([
    pa.field("document_id", pa.string()),
    pa.field("path", pa.string()),
    pa.field("symbol", pa.string()),
    pa.field("symbol_roles", pa.int32()),
    pa.field("start_line", pa.int32()),
    pa.field("start_char", pa.int32()),
    pa.field("end_line", pa.int32()),
    pa.field("end_char", pa.int32()),
    pa.field("is_definition", pa.bool_()),
    pa.field("is_import", pa.bool_()),
    pa.field("is_write", pa.bool_()),
    pa.field("is_read", pa.bool_()),
])

SCIP_SYMBOLS_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()),
    pa.field("display_name", pa.string(), nullable=True),
    pa.field("kind", pa.int32()),
    pa.field("kind_name", pa.string()),
    pa.field("enclosing_symbol", pa.string(), nullable=True),
    pa.field("documentation", pa.list_(pa.string())),
])

# ─────────────────────────────────────────────────────────────────────
# Schema Registry (frozen dict)
# ─────────────────────────────────────────────────────────────────────
EXTRACTION_SCHEMAS: dict[str, pa.Schema] = {
    "cst_refs": CST_REFS_SCHEMA,
    "cst_defs": CST_DEFS_SCHEMA,
    "cst_imports": CST_IMPORTS_SCHEMA,
    "cst_callsites": CST_CALLSITES_SCHEMA,
    "scip_occurrences": SCIP_OCCURRENCES_SCHEMA,
    "scip_symbols": SCIP_SYMBOLS_SCHEMA,
}
```

### 1.2 What We Delete

Remove these schema generation/derivation systems:

| File | Lines | Reason |
|------|-------|--------|
| `src/extract/schema_derivation.py` | ~300 | Schema derivation is unnecessary when schemas are constants |
| `src/schema_spec/nested_types.py` | ~200 | Builder pattern unnecessary for static schemas |
| `src/datafusion_engine/schema/inference.py` | ~250 | No runtime inference needed |
| `src/schema_spec/evidence_metadata.py` | ~100 | Evidence fields can be inline where needed |
| `src/schema_spec/span_fields.py` | ~80 | Span fields defined directly in schemas |

**Estimated reduction: 900+ lines**

---

## Part 2: The Expression Compiler (ViewDef System)

### 2.1 View Definition Structure

```python
# src/views/core.py
"""Minimalist view definition system."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

@dataclass(frozen=True)
class ViewDef:
    """A view is a named DataFrame expression builder.

    This is the ONLY abstraction over DataFusion expressions.
    Everything else is just DataFusion API calls.
    """
    name: str
    depends_on: tuple[str, ...]
    build: Callable[[SessionContext], DataFrame]
    is_output: bool = False  # Whether to write to Delta


@dataclass(frozen=True)
class ViewCatalog:
    """Immutable collection of view definitions."""
    views: tuple[ViewDef, ...]

    def get(self, name: str) -> ViewDef | None:
        return next((v for v in self.views if v.name == name), None)

    def outputs(self) -> tuple[ViewDef, ...]:
        return tuple(v for v in self.views if v.is_output)

    def topological_order(self) -> tuple[ViewDef, ...]:
        """Return views in dependency order."""
        # Simple Kahn's algorithm - no external libraries needed
        in_degree: dict[str, int] = {v.name: 0 for v in self.views}
        dependents: dict[str, list[str]] = {v.name: [] for v in self.views}

        for view in self.views:
            for dep in view.depends_on:
                if dep in in_degree:
                    in_degree[view.name] += 1
                    dependents[dep].append(view.name)

        queue = [name for name, deg in in_degree.items() if deg == 0]
        result = []

        while queue:
            name = queue.pop(0)
            result.append(name)
            for dependent in dependents[name]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return tuple(self.get(name) for name in result if self.get(name))
```

### 2.2 View Definitions (Pure Expressions)

```python
# src/views/definitions.py
"""All view definitions as pure DataFrame expressions."""
from __future__ import annotations

from datafusion import col, lit
from datafusion import functions as f

from views.core import ViewDef

# ─────────────────────────────────────────────────────────────────────
# UDF Imports (from Rust)
# ─────────────────────────────────────────────────────────────────────
from datafusion_ext import (
    stable_id,
    stable_id_parts,
    stable_hash64,
    span_make,
    span_overlaps,
    span_contains,
    utf8_normalize,
    qname_normalize,
    collect_set,
    count_if,
    any_value_det,
    arg_max,
    row_number,
)

# ─────────────────────────────────────────────────────────────────────
# Normalization Views
# ─────────────────────────────────────────────────────────────────────

cst_refs_normalized = ViewDef(
    name="cst_refs_normalized",
    depends_on=("cst_refs",),
    build=lambda ctx: ctx.table("cst_refs").select(
        stable_id("ref", col("path"), col("bstart"), col("bend")).alias("ref_id"),
        col("file_id"),
        col("path"),
        col("ref_kind"),
        utf8_normalize(col("ref_text")).alias("ref_text"),
        col("expr_ctx"),
        col("scope_type"),
        col("scope_name"),
        span_make(col("bstart"), col("bend")).alias("span"),
        col("bstart"),
        col("bend"),
    ),
)

cst_defs_normalized = ViewDef(
    name="cst_defs_normalized",
    depends_on=("cst_defs",),
    build=lambda ctx: ctx.table("cst_defs").select(
        stable_id("def", col("path"), col("def_bstart"), col("def_bend")).alias("def_id"),
        col("file_id"),
        col("path"),
        col("kind"),
        col("name"),
        span_make(col("def_bstart"), col("def_bend")).alias("def_span"),
        span_make(col("name_bstart"), col("name_bend")).alias("name_span"),
        col("def_bstart"),
        col("def_bend"),
        col("docstring"),
        col("qnames"),
    ),
)

cst_imports_normalized = ViewDef(
    name="cst_imports_normalized",
    depends_on=("cst_imports",),
    build=lambda ctx: ctx.table("cst_imports").select(
        stable_id("import", col("path"), col("alias_bstart"), col("alias_bend")).alias("import_id"),
        col("file_id"),
        col("path"),
        col("kind"),
        col("module"),
        col("name"),
        col("asname"),
        col("is_star"),
        span_make(col("stmt_bstart"), col("stmt_bend")).alias("stmt_span"),
        span_make(col("alias_bstart"), col("alias_bend")).alias("alias_span"),
        col("alias_bstart").alias("bstart"),
        col("alias_bend").alias("bend"),
    ),
)

cst_callsites_normalized = ViewDef(
    name="cst_callsites_normalized",
    depends_on=("cst_callsites",),
    build=lambda ctx: ctx.table("cst_callsites").select(
        stable_id("call", col("path"), col("call_bstart"), col("call_bend")).alias("call_id"),
        col("file_id"),
        col("path"),
        span_make(col("call_bstart"), col("call_bend")).alias("call_span"),
        span_make(col("callee_bstart"), col("callee_bend")).alias("callee_span"),
        col("callee_shape"),
        utf8_normalize(col("callee_text")).alias("callee_text"),
        col("arg_count"),
        col("callee_dotted"),
        col("callee_qnames"),
        col("call_bstart").alias("bstart"),
        col("call_bend").alias("bend"),
    ),
)

scip_occurrences_with_bytes = ViewDef(
    name="scip_occurrences_with_bytes",
    depends_on=("scip_occurrences", "file_lines"),
    build=lambda ctx: ctx.table("scip_occurrences")
        .join(ctx.table("file_lines"), on="path")
        .select(
            col("document_id"),
            col("path"),
            col("symbol"),
            col("symbol_roles"),
            # Convert line/col to byte offsets using file_lines
            # (simplified - actual impl uses col_to_byte UDF)
            col("start_line"),
            col("start_char"),
            col("end_line"),
            col("end_char"),
            col("is_definition"),
            col("is_import"),
            col("is_write"),
            col("is_read"),
        ),
)

# ─────────────────────────────────────────────────────────────────────
# Relationship Views
# ─────────────────────────────────────────────────────────────────────

rel_name_symbol = ViewDef(
    name="rel_name_symbol",
    depends_on=("cst_refs_normalized", "scip_occurrences_with_bytes"),
    is_output=True,
    build=lambda ctx: (
        ctx.table("cst_refs_normalized")
        .join(
            ctx.table("scip_occurrences_with_bytes"),
            on="path",
            how="inner",
        )
        .filter(
            span_overlaps(col("span"), col("scip_span"))
            & col("is_read")  # Only references, not definitions
        )
        .select(
            col("ref_id").alias("entity_id"),
            col("symbol"),
            col("path"),
            col("bstart"),
            col("bend"),
            lit("ref").alias("entity_kind"),
        )
        .distinct()
    ),
)

rel_def_symbol = ViewDef(
    name="rel_def_symbol",
    depends_on=("cst_defs_normalized", "scip_occurrences_with_bytes"),
    is_output=True,
    build=lambda ctx: (
        ctx.table("cst_defs_normalized")
        .join(
            ctx.table("scip_occurrences_with_bytes"),
            on="path",
            how="inner",
        )
        .filter(
            span_contains(col("def_span"), col("scip_span"))
            & col("is_definition")
        )
        .select(
            col("def_id").alias("entity_id"),
            col("symbol"),
            col("path"),
            col("def_bstart").alias("bstart"),
            col("def_bend").alias("bend"),
            lit("def").alias("entity_kind"),
        )
        .distinct()
    ),
)

rel_import_symbol = ViewDef(
    name="rel_import_symbol",
    depends_on=("cst_imports_normalized", "scip_occurrences_with_bytes"),
    is_output=True,
    build=lambda ctx: (
        ctx.table("cst_imports_normalized")
        .join(
            ctx.table("scip_occurrences_with_bytes"),
            on="path",
            how="inner",
        )
        .filter(
            span_overlaps(col("alias_span"), col("scip_span"))
            & col("is_import")
        )
        .select(
            col("import_id").alias("entity_id"),
            col("symbol"),
            col("path"),
            col("bstart"),
            col("bend"),
            lit("import").alias("entity_kind"),
        )
        .distinct()
    ),
)

rel_callsite_symbol = ViewDef(
    name="rel_callsite_symbol",
    depends_on=("cst_callsites_normalized", "scip_occurrences_with_bytes"),
    is_output=True,
    build=lambda ctx: (
        ctx.table("cst_callsites_normalized")
        .join(
            ctx.table("scip_occurrences_with_bytes"),
            on="path",
            how="inner",
        )
        .filter(
            span_overlaps(col("callee_span"), col("scip_span"))
        )
        .select(
            col("call_id").alias("entity_id"),
            col("symbol"),
            col("path"),
            col("bstart"),
            col("bend"),
            lit("callsite").alias("entity_kind"),
        )
        .distinct()
    ),
)

# ─────────────────────────────────────────────────────────────────────
# CPG Output Views
# ─────────────────────────────────────────────────────────────────────

cpg_nodes = ViewDef(
    name="cpg_nodes",
    depends_on=(
        "cst_refs_normalized",
        "cst_defs_normalized",
        "cst_imports_normalized",
        "cst_callsites_normalized",
    ),
    is_output=True,
    build=lambda ctx: (
        # Union all entity types into CPG nodes
        ctx.table("cst_refs_normalized")
        .select(
            col("ref_id").alias("node_id"),
            lit("Reference").alias("node_kind"),
            col("path"),
            col("bstart"),
            col("bend"),
        )
        .union(
            ctx.table("cst_defs_normalized").select(
                col("def_id").alias("node_id"),
                lit("Definition").alias("node_kind"),
                col("path"),
                col("def_bstart").alias("bstart"),
                col("def_bend").alias("bend"),
            )
        )
        .union(
            ctx.table("cst_imports_normalized").select(
                col("import_id").alias("node_id"),
                lit("Import").alias("node_kind"),
                col("path"),
                col("bstart"),
                col("bend"),
            )
        )
        .union(
            ctx.table("cst_callsites_normalized").select(
                col("call_id").alias("node_id"),
                lit("CallSite").alias("node_kind"),
                col("path"),
                col("bstart"),
                col("bend"),
            )
        )
    ),
)

cpg_edges = ViewDef(
    name="cpg_edges",
    depends_on=(
        "rel_name_symbol",
        "rel_def_symbol",
        "rel_import_symbol",
        "rel_callsite_symbol",
    ),
    is_output=True,
    build=lambda ctx: (
        # Create edges from relationships
        ctx.table("rel_name_symbol")
        .select(
            stable_id_parts("edge", col("entity_id"), col("symbol")).alias("edge_id"),
            col("entity_id").alias("src_node_id"),
            stable_id("symbol", col("symbol")).alias("dst_node_id"),
            lit("REFERENCES").alias("edge_kind"),
        )
        .union(
            ctx.table("rel_def_symbol").select(
                stable_id_parts("edge", col("entity_id"), col("symbol")).alias("edge_id"),
                col("entity_id").alias("src_node_id"),
                stable_id("symbol", col("symbol")).alias("dst_node_id"),
                lit("DEFINES").alias("edge_kind"),
            )
        )
        .union(
            ctx.table("rel_import_symbol").select(
                stable_id_parts("edge", col("entity_id"), col("symbol")).alias("edge_id"),
                col("entity_id").alias("src_node_id"),
                stable_id("symbol", col("symbol")).alias("dst_node_id"),
                lit("IMPORTS").alias("edge_kind"),
            )
        )
        .union(
            ctx.table("rel_callsite_symbol").select(
                stable_id_parts("edge", col("entity_id"), col("symbol")).alias("edge_id"),
                col("entity_id").alias("src_node_id"),
                stable_id("symbol", col("symbol")).alias("dst_node_id"),
                lit("CALLS").alias("edge_kind"),
            )
        )
    ),
)

# ─────────────────────────────────────────────────────────────────────
# View Catalog
# ─────────────────────────────────────────────────────────────────────

ALL_VIEWS = ViewCatalog(views=(
    # Normalization
    cst_refs_normalized,
    cst_defs_normalized,
    cst_imports_normalized,
    cst_callsites_normalized,
    scip_occurrences_with_bytes,
    # Relationships
    rel_name_symbol,
    rel_def_symbol,
    rel_import_symbol,
    rel_callsite_symbol,
    # CPG
    cpg_nodes,
    cpg_edges,
))
```

### 2.3 What We Delete

Remove these view abstraction layers:

| File/Directory | Lines | Reason |
|----------------|-------|--------|
| `src/datafusion_engine/views/view_spec.py` | ~400 | ViewDef replaces ViewProjectionSpec |
| `src/datafusion_engine/views/view_specs_catalog.py` | ~300 | Catalog is inline with definitions |
| `src/datafusion_engine/views/dsl.py` | ~200 | DSL replaced by direct expressions |
| `src/datafusion_engine/views/dsl_views.py` | ~500 | Views are now pure expressions |
| `src/cpg/spec_registry.py` | ~500 | Specs replaced by ViewDef |
| `src/cpg/relationship_specs.py` | ~300 | Relationships are views |
| `src/cpg/relationship_builder.py` | ~400 | Building is in expressions |
| `src/schema_spec/relationship_specs.py` | ~400 | Duplicate of above |

**Estimated reduction: 3,000+ lines**

---

## Part 3: Execution (Simple Loop)

### 3.1 Pipeline Runner

```python
# src/pipeline/runner.py
"""Minimalist pipeline execution."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext
    import pyarrow as pa

from views.core import ViewCatalog
from extract.schemas import EXTRACTION_SCHEMAS


def create_session() -> SessionContext:
    """Create DataFusion session with all UDFs registered."""
    from datafusion import SessionContext
    from datafusion_ext import register_all_udfs

    ctx = SessionContext()
    register_all_udfs(ctx)
    return ctx


def register_extraction_tables(
    ctx: SessionContext,
    tables: dict[str, pa.Table],
) -> None:
    """Register extraction tables in the session."""
    for name, table in tables.items():
        expected_schema = EXTRACTION_SCHEMAS.get(name)
        if expected_schema is not None:
            # Validate schema matches constant
            if table.schema != expected_schema:
                raise ValueError(
                    f"Table {name} schema mismatch: "
                    f"got {table.schema}, expected {expected_schema}"
                )
        ctx.register_table(name, table)


def run_pipeline(
    ctx: SessionContext,
    catalog: ViewCatalog,
    output_dir: Path,
) -> dict[str, Path]:
    """Execute pipeline and write outputs to Delta.

    Returns mapping of output name to Delta table path.
    """
    from deltalake import write_deltalake

    outputs: dict[str, Path] = {}

    # Execute views in topological order
    for view in catalog.topological_order():
        # Build DataFrame from expression
        df = view.build(ctx)

        # Register as view for downstream dependencies
        ctx.register_view(view.name, df)

        # Write outputs to Delta
        if view.is_output:
            table_path = output_dir / view.name
            arrow_table = df.collect()  # Materialize
            write_deltalake(str(table_path), arrow_table, mode="overwrite")
            outputs[view.name] = table_path

    return outputs


def build_cpg(
    extraction_tables: dict[str, pa.Table],
    output_dir: Path,
) -> dict[str, Path]:
    """Main entry point: extraction tables → CPG outputs."""
    from views.definitions import ALL_VIEWS

    ctx = create_session()
    register_extraction_tables(ctx, extraction_tables)
    return run_pipeline(ctx, ALL_VIEWS, output_dir)
```

### 3.2 Two Options: Minimal vs Keep Rustworkx+Hamilton

**Option A: Minimal (Simple Loop)**

For simple pipelines, ViewCatalog's topological_order() is sufficient:

| File/Directory | Lines | Status |
|----------------|-------|--------|
| `src/relspec/rustworkx_graph.py` | ~800 | Replace with ViewCatalog.topological_order() |
| `src/relspec/rustworkx_schedule.py` | ~400 | Not needed for simple execution |
| `src/relspec/execution_plan.py` | ~600 | ViewCatalog + run_pipeline() |
| `src/hamilton_pipeline/*` | ~3000 | Not needed |

**Estimated reduction: 4,800+ lines**

**Option B: Keep Rustworkx + Hamilton (Full Scheduling)**

For complex pipelines needing parallelism, costs, and critical path analysis:

```python
# src/compiler/schedule.py - Compiler feeds rustworkx directly
def compile_for_scheduling(ctx: SessionContext, views: ViewCatalog) -> list[InferredDeps]:
    """The ONLY interface to rustworkx - plan introspection."""
    deps = []
    for view in views.views:
        df = view.build(ctx)
        plan = df.optimized_logical_plan()
        lineage = extract_lineage(plan)

        deps.append(InferredDeps(
            task_name=view.name,
            output=view.name,
            inputs=frozenset(lineage.referenced_tables),
            required_columns=dict(lineage.required_columns_by_dataset),
            required_udfs=frozenset(lineage.required_udfs),
            plan_fingerprint=hash_plan(plan),
        ))
    return deps


def build_schedule(ctx: SessionContext, views: ViewCatalog, evidence: EvidenceCatalog):
    """Build rustworkx graph from compiled views."""
    deps = compile_for_scheduling(ctx, views)
    task_graph = build_task_graph_from_inferred_deps(deps)
    schedule = schedule_tasks(task_graph, evidence=evidence)
    return schedule
```

With Option B, we KEEP:
- `src/relspec/rustworkx_graph.py` - Graph building (but only from InferredDeps)
- `src/relspec/rustworkx_schedule.py` - Scheduling
- `src/relspec/inferred_deps.py` - InferredDeps dataclass
- `src/hamilton_pipeline/driver_factory.py` - DAG execution

But we REMOVE:
- Manual task declarations
- Separate contract registries
- Schema validation code (derived from plans)
- Task/Plan catalog classes (views ARE the catalog)

**Key insight for Option B:** The compiler's `compile_for_scheduling()` is the SINGLE entry point to rustworkx. All metadata comes from plan introspection, not declarations.

---

## Part 4: Add-Ons (Thin Extensions to Expression Compiler)

### 4.1 Deduplication Pattern

```python
# src/views/patterns.py
"""Reusable expression patterns for view definitions."""
from __future__ import annotations

from datafusion import col, Expr
from datafusion import functions as f
from datafusion_ext import row_number


def dedupe_keep_best(
    df: DataFrame,
    partition_keys: list[str],
    order_by: Expr,
    descending: bool = True,
) -> DataFrame:
    """Deduplicate keeping row with best score.

    This is an expression pattern, not an abstraction layer.
    """
    order_expr = order_by.desc() if descending else order_by.asc()

    return (
        df.with_column(
            "__rn",
            row_number().over(
                partition_by=[col(k) for k in partition_keys],
                order_by=[order_expr],
            ),
        )
        .filter(col("__rn") == 1)
        .drop("__rn")
    )


# Usage in view definition:
# dedupe_keep_best(df, ["entity_id", "symbol"], col("confidence"))
```

### 4.2 Output Validation (Boundary Only)

```python
# src/views/validation.py
"""Output validation at Delta write boundaries."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass(frozen=True)
class OutputContract:
    """Minimal contract for output validation."""
    name: str
    required_columns: tuple[str, ...]
    key_columns: tuple[str, ...]


# Define contracts for outputs only
OUTPUT_CONTRACTS = {
    "cpg_nodes": OutputContract(
        name="cpg_nodes",
        required_columns=("node_id", "node_kind", "path", "bstart", "bend"),
        key_columns=("node_id",),
    ),
    "cpg_edges": OutputContract(
        name="cpg_edges",
        required_columns=("edge_id", "src_node_id", "dst_node_id", "edge_kind"),
        key_columns=("edge_id",),
    ),
}


def validate_output(name: str, table: pa.Table) -> None:
    """Validate output table before Delta write."""
    contract = OUTPUT_CONTRACTS.get(name)
    if contract is None:
        return  # No contract = no validation

    schema_names = set(table.schema.names)
    missing = set(contract.required_columns) - schema_names
    if missing:
        raise ValueError(f"Output {name} missing required columns: {missing}")

    # Check key columns have no nulls
    for key in contract.key_columns:
        if key in schema_names:
            col = table.column(key)
            if col.null_count > 0:
                raise ValueError(f"Output {name} has nulls in key column: {key}")
```

### 4.3 Incremental Processing (CDF Add-On)

```python
# src/views/incremental.py
"""Optional CDF-aware incremental processing."""
from __future__ import annotations

from datafusion import col
from datafusion_ext import cdf_is_upsert, cdf_is_delete


def apply_cdf_changes(
    base_df: DataFrame,
    changes_df: DataFrame,
    key_columns: list[str],
) -> DataFrame:
    """Apply CDF changes to base DataFrame.

    This is an optional add-on for incremental processing.
    """
    # Filter to upserts only
    upserts = changes_df.filter(cdf_is_upsert(col("_change_type")))

    # Left anti-join to remove deleted keys
    deletes = changes_df.filter(cdf_is_delete(col("_change_type")))
    base_without_deletes = base_df.join(
        deletes.select(*[col(k) for k in key_columns]),
        on=key_columns,
        how="left_anti",
    )

    # Union with upserts
    return base_without_deletes.union(upserts.drop("_change_type"))
```

---

## Part 5: Migration Path

### 5.1 Phase 1: Freeze Schemas (Week 1)

1. Create `src/extract/schemas.py` with frozen PyArrow schemas
2. Update extractors to validate against frozen schemas
3. Delete schema derivation/inference modules

### 5.2 Phase 2: Convert Views (Week 2)

1. Create `src/views/core.py` with ViewDef
2. Convert existing view specs to pure expressions in `src/views/definitions.py`
3. Delete ViewProjectionSpec and DSL modules

### 5.3 Phase 3: Simplify Execution (Week 3)

1. Create `src/pipeline/runner.py` with simple loop
2. Remove rustworkx graph analysis
3. Remove Hamilton DAG generation
4. Remove task/plan catalog systems

### 5.4 Phase 4: Cleanup (Week 4)

1. Remove all deleted modules from imports
2. Update tests to use new structure
3. Delete empty directories
4. Update documentation

---

## Summary: Before vs After

| Metric | Before | After | Reduction |
|--------|--------|-------|-----------|
| **Lines of code** | ~15,000 | ~5,000 | 67% |
| **Modules** | 80+ | 20 | 75% |
| **Abstraction layers** | 5 | 1 | 80% |
| **Schema systems** | 4 | 1 | 75% |
| **View spec systems** | 3 | 1 | 67% |
| **Execution systems** | 3 | 1 | 67% |

**The result:** DataFrame expressions ARE the pipeline. No intermediate abstractions.

---

## Appendix A: Rust UDF Integration

### Available Rust UDFs (47 Total)

The Rust codebase provides all calculation primitives needed for the pipeline:

#### Hashing/ID Generation (10 UDFs)
| UDF | Signature | Purpose |
|-----|-----------|---------|
| `stable_hash64` | `(value: str) → i64` | Blake2b 64-bit hash |
| `stable_hash128` | `(value: str) → str` | Blake2b 128-bit hash (hex) |
| `stable_id` | `(prefix: str, value: str) → str` | Prefixed 128-bit ID |
| `stable_id_parts` | `(prefix: str, ...parts: str) → str` | Multi-part ID |
| `prefixed_hash64` | `(prefix: str, value: str) → str` | Prefixed 64-bit hash |
| `prefixed_hash_parts64` | `(prefix: str, ...parts: str) → str` | Multi-part prefixed hash |
| `stable_hash_any` | `(value: any) → str` | Hash any value |
| `arrow_metadata` | `(struct: struct, key: str) → str` | Extract field metadata |
| `semantic_tag` | `(value: str, tag: str) → str` | Tagged value |
| `cpg_score` | `(confidence: f64, priority: i32) → f64` | CPG scoring |

#### Span Operations (6 UDFs)
| UDF | Signature | Purpose |
|-----|-----------|---------|
| `span_make` | `(bstart: i64, bend: i64, unit: str) → struct` | Create span struct |
| `span_len` | `(span: struct) → i64` | Span length in bytes |
| `span_overlaps` | `(a: struct, b: struct) → bool` | Test span overlap |
| `span_contains` | `(outer: struct, inner: struct) → bool` | Test span containment |
| `span_id` | `(span: struct) → str` | Stable ID from span |
| `interval_align_score` | `(a: struct, b: struct) → f64` | Alignment score |

#### Text/Normalization (4 UDFs)
| UDF | Signature | Purpose |
|-----|-----------|---------|
| `utf8_normalize` | `(text: str, form: str) → str` | Unicode normalization (NFC/NFD/NFKC/NFKD) |
| `utf8_null_if_blank` | `(text: str) → str?` | Null if whitespace-only |
| `qname_normalize` | `(qname: str) → str` | Qualified name normalization |
| `col_to_byte` | `(line: i32, col: i32, text: str) → i64` | Column to byte offset |

#### Collection Operations (4 UDFs)
| UDF | Signature | Purpose |
|-----|-----------|---------|
| `list_compact` | `(list: list<T>) → list<T>` | Remove nulls |
| `list_unique_sorted` | `(list: list<T>) → list<T>` | Dedupe and sort |
| `map_get_default` | `(map: map, key: str, default: str) → str` | Get with default |
| `map_normalize` | `(map: map) → map` | Normalize map keys |
| `struct_pick` | `(struct: struct, ...fields: str) → struct` | Project struct fields |

#### Aggregate UDFs (11)
| UDF | Signature | Purpose |
|-----|-----------|---------|
| `list_unique` | `(col: T) → list<T>` | Collect unique values |
| `collect_set` | `(col: T) → list<T>` | Distinct collection |
| `count_distinct_agg` | `(col: T) → i64` | Count distinct |
| `count_if` | `(condition: bool) → i64` | Conditional count |
| `any_value_det` | `(col: T) → T` | Deterministic any value |
| `arg_max` | `(col: T, by: V) → T` | Value at max |
| `arg_min` | `(col: T, by: V) → T` | Value at min |
| `asof_select` | `(col: T, ts: i64) → T` | ASOF selection |
| `first_value` | `(col: T) → T` | First value in group |
| `last_value` | `(col: T) → T` | Last value in group |
| `string_agg` | `(col: str, sep: str) → str` | String concatenation |

#### Window UDFs (3)
| UDF | Signature | Purpose |
|-----|-----------|---------|
| `row_number` | `() → i64` | Row number in partition |
| `lag` | `(col: T, offset: i64) → T` | Previous row value |
| `lead` | `(col: T, offset: i64) → T` | Next row value |

#### Delta CDF UDFs (3)
| UDF | Signature | Purpose |
|-----|-----------|---------|
| `cdf_change_rank` | `(change_type: str) → i32` | Change priority |
| `cdf_is_upsert` | `(change_type: str) → bool` | Is insert/update |
| `cdf_is_delete` | `(change_type: str) → bool` | Is delete |

### Usage Pattern in View Expressions

```python
from datafusion import col, lit
from datafusion_ext import stable_id, span_make, utf8_normalize, row_number

def build_cst_refs_view(ctx):
    return (
        ctx.table("cst_refs")
        .select(
            # ID generation
            stable_id("ref", col("path"), col("bstart"), col("bend")).alias("ref_id"),

            # Normalization
            utf8_normalize(col("ref_text"), lit("NFC")).alias("ref_text"),

            # Span creation
            span_make(col("bstart"), col("bend"), lit("byte")).alias("span"),

            # Passthrough
            col("file_id"),
            col("path"),
        )
        # Deduplication using window
        .with_column("__rn", row_number().over(
            partition_by=[col("ref_id")],
            order_by=[col("confidence").desc()]
        ))
        .filter(col("__rn") == lit(1))
        .drop("__rn")
    )
```

---

## Appendix B: Direct Answer to User's Questions

### Q: What do we get from extraction?

**Answer:** PyArrow Tables with these characteristics:

1. **Frozen schemas** - Known at compile time, never change during pipeline run
2. **Nested structures** - `LIST<STRUCT>` for nodes, edges, imports per file
3. **Canonical fields** - file_id, path, bstart, bend on all fact tables
4. **No metadata** - No dependency info, no task specs, no plan fingerprints

Example extraction table:
```
cst_refs: {file_id, path, file_sha256, ref_kind, ref_text, expr_ctx, scope_type, scope_name, parent_kind, bstart, bend}
```

### Q: What do we need going into rustworkx?

**Answer:** `InferredDeps` objects with:

```python
@dataclass(frozen=True)
class InferredDeps:
    task_name: str                           # View/task identifier
    output: str                              # Output dataset name
    inputs: frozenset[str]                   # Table dependencies
    required_columns: dict[str, tuple]       # Per-table column requirements
    required_udfs: frozenset[str]            # UDF dependencies
    plan_fingerprint: str                    # Cache invalidation key
```

### Q: What's the difference (the gap)?

**Gap = Metadata that extraction doesn't produce but rustworkx needs:**

| Needed by Rustworkx | Source |
|---------------------|--------|
| `task_name` | ViewDef.name |
| `output` | ViewDef.name |
| `inputs` | `df.optimized_logical_plan()` → lineage extraction |
| `required_columns` | Plan walking → `required_columns_by_dataset` |
| `required_udfs` | Expression analysis |
| `plan_fingerprint` | `hash(optimized_logical_plan)` |

### Q: How do we formalize and redesign?

**The Expression Compiler IS the formalization:**

```python
# This is the ENTIRE bridge between extraction and rustworkx
def compile_views_to_deps(ctx: SessionContext, views: list[ViewDef]) -> list[InferredDeps]:
    return [
        InferredDeps(
            task_name=v.name,
            output=v.name,
            inputs=frozenset(extract_lineage(v.build(ctx).optimized_logical_plan()).referenced_tables),
            required_columns=dict(extract_lineage(v.build(ctx).optimized_logical_plan()).required_columns_by_dataset),
            required_udfs=frozenset(extract_lineage(v.build(ctx).optimized_logical_plan()).required_udfs),
            plan_fingerprint=hash_plan(v.build(ctx).optimized_logical_plan()),
        )
        for v in views
    ]
```

**What becomes unnecessary:**
- Manual `inputs=` declarations on tasks
- Separate schema registries (use extraction schemas directly)
- Contract specification files (derive from `df.schema()`)
- Task/Plan catalog classes (ViewDef IS the task)
- Normalization modules (express as view select expressions)
