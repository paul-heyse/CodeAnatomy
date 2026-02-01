# Semantics Quality & Reliability Plan

This document proposes a best-in-class reliability and quality approach for the semantic compiler, integrating insights from `semantic_approach_review.md` with the rich extraction data available in our pipeline.

---

## Executive Summary

The semantic compiler produces CPG relationships by joining evidence from multiple extraction sources. This plan establishes a **three-tier signal model** that maximizes join quality while maintaining robustness:

1. **Hard Keys** - Identity constraints that must match for candidate generation
2. **Soft Evidence** - Features that rank candidates within ambiguity groups
3. **Quality Signals** - Provider health metrics that modulate confidence

The approach leverages all available extraction metadata: CST parse errors, SCIP diagnostics, tree-sitter stats, and coordinate encoding metadata to produce relationships with explicit `confidence`, `score`, and `ambiguity_group_id` fields.

**Implementation Policy**: Avoid raw SQL strings. Use native DataFusion DataFrame/Expr APIs for joins, filters, projections, aggregates, and window functions. If a SQL string is ever unavoidable, isolate it in a single well-documented function and treat it as a temporary exception.

---

## Part 1: Available Quality Signals from Extraction

### 1.1 CST Extraction Quality Data

**Parse Manifest** (`cst_parse_manifest`):
- `encoding`: File encoding detected
- `parser_backend`: Parser used ("native" vs "lib2to3")
- `libcst_version`: LibCST version for reproducibility
- `parsed_python_version`: Target Python version

**Parse Errors** (`cst_parse_errors`):
- `error_type`: Classification of parse failure
- `message`: Error description
- `line`, `column`, `length`: Error location
- Coordinate metadata: `line_base`, `col_unit`, `end_exclusive`

**Usage**: Files with parse errors should have CST-derived relationships penalized.

### 1.2 SCIP Extraction Quality Data

**Index Stats** (`scip_index_stats`):
- `diagnostic_count`: Number of indexing diagnostics
- `missing_position_encoding_count`: Coordinate issues

**Occurrence Flags**:
- `is_generated`: Marks generated code
- `is_test`: Marks test code

**Document Metadata** (`scip_documents`):
- `position_encoding`: "UnspecifiedPositionEncoding", "UTF8", "UTF16", "UTF32"

**Diagnostics** (`scip_diagnostics`):
- `severity`: Error severity level
- `message`: Diagnostic description

**Usage**: High diagnostic counts or missing position encoding should penalize SCIP-derived relationships.

### 1.3 Tree-sitter Extraction Quality Data

**Stats** (`ts_stats`):
- `error_count`: Syntax errors found
- `missing_count`: Missing node count
- `parse_timed_out`: Boolean timeout flag
- `match_limit_exceeded`: Boolean limit flag

**Usage**: Timed-out or error-heavy files should have tree-sitter relationships penalized.

### 1.4 File Metadata Quality Data

**File Index** (`file_index`):
- `file_id`: Stable identifier
- `file_sha256`: Content hash for validity checking
- Coordinate metadata: `line_base`, `col_unit`, `end_exclusive`

**Usage**: `file_sha256` mismatches between sources indicate stale data.

---

## Part 2: Three-Tier Signal Model

### 2.1 Hard Keys (Identity Constraints)

Hard keys define what **must** match for a join to produce valid candidates. These are used in JOIN ON clauses and post-join filters.

| Join Type | Hard Keys |
|-----------|-----------|
| File identity | `file_id` or `(repo, path)` |
| Owner relationship | `owner_def_id = def_id` (when non-null) |
| Span overlap | `bstart < other_bend AND other_bstart < bend` |
| Span containment | `other_bstart >= bstart AND other_bend <= bend` |
| Exact span | `bstart = other_bstart AND bend = other_bend` |
| Symbol identity | `symbol` (when SCIP-aligned) |

**Principle**: Keep hard keys minimal and selective. More constraints reduce recall.

### 2.2 Soft Evidence (Ranking Features)

Soft evidence helps rank candidates **within** an ambiguity group. These produce `score` contributions.

| Feature | Weight | Expression |
|---------|--------|------------|
| Owner ID exact match | +100 | `CASE WHEN owner_def_id = def_id THEN 1 ELSE 0 END` |
| Span contained | +50 | `CASE WHEN inner_bstart >= outer_bstart AND inner_bend <= outer_bend THEN 1 ELSE 0 END` |
| Minimal span delta | -0.01 | `ABS((bend - bstart) - (other_bend - other_bstart))` |
| Context match (Load) | +10 | `CASE WHEN expr_ctx = 'Load' AND is_read THEN 1 ELSE 0 END` |
| Context match (Store) | +10 | `CASE WHEN expr_ctx = 'Store' AND is_write THEN 1 ELSE 0 END` |
| Kind match | +5 | `CASE WHEN owner_kind = def_kind THEN 1 ELSE 0 END` |
| Syntax kind match | +5 | `CASE WHEN syntax_kind = expected_kind THEN 1 ELSE 0 END` |

**Principle**: Features should be discriminative within ambiguity groups. Avoid features that are constant across all candidates.

### 2.3 Quality Signals (Confidence Modifiers)

Quality signals reflect **provider reliability** and penalize relationships from unreliable sources.

| Signal | Weight | Expression |
|--------|--------|------------|
| CST parse errors | -200 | `has_cst_parse_errors` |
| Tree-sitter timeout | -300 | `ts_timed_out` |
| Tree-sitter errors | -50 | `ts_error_count` |
| Match limit exceeded | -150 | `ts_match_limit_exceeded` |
| SCIP diagnostics | -100 | `has_scip_diagnostics` |
| Missing coord encoding | -100 | `CASE WHEN col_unit IS NULL OR line_base IS NULL THEN 1 ELSE 0 END` |
| SHA256 mismatch | -500 | `CASE WHEN left_sha != right_sha THEN 1 ELSE 0 END` |
| Generated code | -50 | `is_generated` |

**Principle**: Quality signals should not filter out candidates entirely (unless catastrophic), but should reduce confidence scores.

---

## Part 3: Evidence Tier Scoring

Different evidence sources have inherent reliability differences. The `base_score` encodes this precedence:

| Evidence Tier | Base Score | Base Confidence | Rationale |
|---------------|------------|-----------------|-----------|
| SCIP (type-resolved) | 2000 | 0.95 | Full semantic analysis |
| Symtable (scope-aware) | 1500 | 0.85 | Python's own symbol table |
| Bytecode (runtime) | 1200 | 0.75 | Actual execution instructions |
| CST (structural) | 1000 | 0.70 | Syntactic structure only |
| Tree-sitter (fast parse) | 800 | 0.65 | Approximate syntax |
| Heuristics (name match) | 500 | 0.50 | Pattern-based guessing |

When multiple rules produce candidates for the same entity, the tier ordering ensures SCIP-based edges win over heuristic edges.

### 3.1 Expression Policy (No SQL)

- All relationship logic must be expressed using DataFusion `Expr` builders (e.g., `col`, `lit`, `when`) and DataFrame APIs.
- `RelationshipSpec` expressions are represented as `ExprSpec` callables and evaluated via a typed `ExprContext`.
- Add a validation pass that checks referenced columns exist in the joined schema and that expressions are deterministic.

---

## Part 4: Implementation Specifications

### 4.1 Core Dataclasses

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Literal, Protocol, Sequence

from datafusion import Expr


class ExprContext(Protocol):
    """Expression context for building DataFusion Expr objects."""

    def col(self, name: str) -> Expr: ...
    def lit(self, value: object) -> Expr: ...


ExprSpec = Callable[[ExprContext], Expr]


@dataclass(frozen=True)
class SelectExpr:
    """Projected expression with explicit alias."""
    expr: ExprSpec
    alias: str


@dataclass(frozen=True)
class OrderSpec:
    """Order by expression with explicit direction."""
    expr: ExprSpec
    direction: Literal["asc", "desc"] = "desc"


@dataclass(frozen=True)
class HardPredicate:
    """Boolean predicate that must be true for valid candidates."""
    expr: ExprSpec


@dataclass(frozen=True)
class Feature:
    """Numeric feature for scoring candidates."""
    name: str
    expr: ExprSpec
    weight: float
    kind: Literal["evidence", "quality"] = "evidence"


@dataclass(frozen=True)
class SignalsSpec:
    """Three-tier signal specification for a relationship."""
    hard: Sequence[HardPredicate] = ()
    features: Sequence[Feature] = ()
    base_score: float = 0.0
    base_confidence: float = 0.5


@dataclass(frozen=True)
class RankSpec:
    """Deterministic ranking and ambiguity grouping."""
    ambiguity_key_expr: ExprSpec
    ambiguity_group_id_expr: ExprSpec | None = None
    order_by: Sequence[OrderSpec] = (OrderSpec(lambda e: e.col("score")),)
    keep: Literal["all", "best"] = "all"
    top_k: int = 1


@dataclass(frozen=True)
class RelationshipSpec:
    """Complete relationship specification with quality features."""
    name: str
    left_view: str
    right_view: str

    # Equi-join keys (fast, selective)
    left_on: Sequence[str] = ()
    right_on: Sequence[str] = ()
    how: Literal["inner", "left", "right", "full"] = "inner"

    # Three-tier signals
    signals: SignalsSpec = field(default_factory=SignalsSpec)

    # Contract fields
    origin: str = "semantic_compiler"
    provider: str = "unknown_provider"
    rule_name: str | None = None

    # Ranking
    rank: RankSpec | None = None

    # Output columns
    select_exprs: Sequence[SelectExpr] = ()
```

`ExprContextImpl` should resolve columns with left/right aliases and provide helper accessors for literals and common DataFusion functions. This keeps all relationship logic in native DataFusion expressions and avoids SQL strings.

### 4.2 File Quality View

Create a reusable `file_quality` view aggregating all provider health signals:

```python
from datafusion.functions import col, coalesce, count, lit, when

def build_file_quality_view(ctx: SessionContext) -> DataFrame:
    """Build aggregated file quality signals from all extraction sources."""
    cst_err = (
        ctx.table("cst_parse_errors")
        .aggregate(
            ["file_id"],
            [
                (count(lit(1)) > lit(0)).cast("int").alias("has_cst_parse_errors"),
                count(lit(1)).alias("cst_error_count"),
            ],
        )
    )
    ts = (
        ctx.table("ts_stats")
        .select(
            col("file_id"),
            col("parse_timed_out").cast("int").alias("ts_timed_out"),
            col("error_count").alias("ts_error_count"),
            col("missing_count").alias("ts_missing_count"),
            col("match_limit_exceeded").cast("int").alias("ts_match_limit_exceeded"),
        )
    )
    scip_diag = (
        ctx.table("scip_diagnostics")
        .aggregate(
            ["document_id"],
            [
                (count(lit(1)) > lit(0)).cast("int").alias("has_scip_diagnostics"),
                count(lit(1)).alias("scip_diagnostic_count"),
            ],
        )
        .select(
            col("document_id").alias("file_id"),
            col("has_scip_diagnostics"),
            col("scip_diagnostic_count"),
        )
    )
    scip_meta = (
        ctx.table("scip_documents")
        .select(
            col("document_id").alias("file_id"),
            when(col("position_encoding") == lit("UnspecifiedPositionEncoding"), lit(1))
            .otherwise(lit(0))
            .alias("scip_encoding_unspecified"),
        )
    )
    base = (
        ctx.table("file_index")
        .join(cst_err, join_keys=(["file_id"], ["file_id"]), how="left")
        .join(ts, join_keys=(["file_id"], ["file_id"]), how="left")
        .join(scip_diag, join_keys=(["file_id"], ["file_id"]), how="left")
        .join(scip_meta, join_keys=(["file_id"], ["file_id"]), how="left")
    )
    return base.select(
        col("file_id"),
        col("file_sha256"),
        coalesce(col("has_cst_parse_errors"), lit(0)).alias("has_cst_parse_errors"),
        coalesce(col("cst_error_count"), lit(0)).alias("cst_error_count"),
        coalesce(col("ts_timed_out"), lit(0)).alias("ts_timed_out"),
        coalesce(col("ts_error_count"), lit(0)).alias("ts_error_count"),
        coalesce(col("ts_missing_count"), lit(0)).alias("ts_missing_count"),
        coalesce(col("ts_match_limit_exceeded"), lit(0)).alias("ts_match_limit_exceeded"),
        coalesce(col("has_scip_diagnostics"), lit(0)).alias("has_scip_diagnostics"),
        coalesce(col("scip_diagnostic_count"), lit(0)).alias("scip_diagnostic_count"),
        coalesce(col("scip_encoding_unspecified"), lit(0)).alias(
            "scip_encoding_unspecified"
        ),
        (
            lit(1000)
            - coalesce(col("has_cst_parse_errors"), lit(0)) * lit(200)
            - coalesce(col("ts_timed_out"), lit(0)) * lit(300)
            - coalesce(col("ts_error_count"), lit(0)) * lit(10)
            - coalesce(col("ts_match_limit_exceeded"), lit(0)) * lit(150)
            - coalesce(col("has_scip_diagnostics"), lit(0)) * lit(100)
            - coalesce(col("scip_encoding_unspecified"), lit(0)) * lit(50)
        ).alias("file_quality_score"),
    )
```

### 4.3 Relationship Compiler with Quality Integration

```python
from datafusion.functions import col, coalesce, lit, row_number, window

from semantics.exprs import ExprContextImpl, clamp, stable_hash64

def compile_relationship_with_quality(
    ctx: SessionContext,
    spec: RelationshipSpec,
    file_quality_df: DataFrame,
) -> DataFrame:
    """Compile relationship spec to DataFusion DataFrame with quality signals."""

    l = ctx.table(spec.left_view).alias("l")
    r = ctx.table(spec.right_view).alias("r")
    expr_ctx = ExprContextImpl(left_alias="l", right_alias="r")

    # 1) Equi-join on hard keys
    if spec.left_on and spec.right_on:
        df = l.join(r, join_keys=(list(spec.left_on), list(spec.right_on)), how=spec.how)
    else:
        df = l.cross_join(r)

    # 2) Join file quality signals
    df = df.join(
        file_quality_df.select(
            "file_id",
            "file_quality_score",
            "has_cst_parse_errors",
            "ts_timed_out",
            "has_scip_diagnostics",
        ),
        join_keys=(["file_id"], ["file_id"]),
        how="left",
    )

    # 3) Apply hard predicates
    for hp in spec.signals.hard:
        df = df.filter(hp.expr(expr_ctx))

    # 4) Add feature columns
    feature_cols: list[str] = []
    for f in spec.signals.features:
        feat_name = f"feat__{f.kind}__{f.name}"
        df = df.with_column(feat_name, f.expr(expr_ctx))
        feature_cols.append(feat_name)

    # 5) Compute score = base_score + weighted_features + file_quality_adjustment
    score_expr = lit(spec.signals.base_score)
    for f in spec.signals.features:
        score_expr = score_expr + lit(f.weight) * col(f"feat__{f.kind}__{f.name}")
    score_expr = score_expr + coalesce(col("file_quality_score"), lit(1000)) - lit(1000)
    df = df.with_column("score", score_expr)

    # 6) Compute confidence = clamp(base + scaled_score)
    conf_expr = clamp(
        lit(spec.signals.base_confidence) + (col("score") / lit(10000.0)),
        min_value=lit(0.0),
        max_value=lit(1.0),
    )
    df = df.with_column("confidence", conf_expr)

    rule_name = spec.rule_name or spec.name
    df = df.select(
        *[sel.expr(expr_ctx).alias(sel.alias) for sel in spec.select_exprs],
        col("score"),
        col("confidence"),
        lit(spec.origin).alias("origin"),
        lit(spec.provider).alias("provider"),
        lit(rule_name).alias("rule_name"),
    )

    # 7) Ambiguity grouping + deterministic winner selection
    if spec.rank:
        group_key = spec.rank.ambiguity_key_expr(expr_ctx)
        group_id = (
            spec.rank.ambiguity_group_id_expr(expr_ctx)
            if spec.rank.ambiguity_group_id_expr
            else stable_hash64(group_key)
        )
        order_by = [order.expr(expr_ctx).sort(order.direction) for order in spec.rank.order_by]
        df = df.with_column("ambiguity_group_id", group_id)
        df = df.with_column("_rn", row_number().over(window(partition_by=[group_key], order_by=order_by)))
        if spec.rank.keep == "best":
            df = df.filter(col("_rn") <= lit(spec.rank.top_k)).drop_columns("_rn")

    return df
```

`stable_hash64` should be a registered DataFusion UDF returning deterministic 64-bit hashes. `clamp` should be implemented via DataFusion expressions (e.g., `least(greatest(value, min), max)`), not SQL strings.

---

## Part 5: Concrete Relationship Specifications

### 5.1 Expression Helpers (DataFusion-native)

```python
from datafusion.functions import abs as abs_, lit, when


def c(name: str) -> ExprSpec:
    return lambda e: e.col(name)


def v(value: object) -> ExprSpec:
    return lambda e: e.lit(value)


def eq(left: str, right: str) -> ExprSpec:
    return lambda e: e.col(left) == e.col(right)


def gt(left: str, right: str) -> ExprSpec:
    return lambda e: e.col(left) > e.col(right)


def between_overlap(start_a: str, end_a: str, start_b: str, end_b: str) -> ExprSpec:
    return lambda e: (e.col(start_a) < e.col(end_b)) & (e.col(start_b) < e.col(end_a))


def case_eq(left: str, right: str) -> ExprSpec:
    return lambda e: when(e.col(left) == e.col(right), e.lit(1)).otherwise(e.lit(0))
```

### 5.2 CST Docstring → Owner Definition (ID-based, high confidence)

```python
REL_CST_DOCSTRING_OWNER_BY_ID = RelationshipSpec(
    name="rel_cst_docstring_owner_by_id",
    left_view="cst_docstrings_norm",
    right_view="cst_defs_norm",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="libcst",
    origin="semantic_compiler",
    signals=SignalsSpec(
        base_score=1000,
        base_confidence=0.98,
        hard=[
            HardPredicate(lambda e: e.col("owner_def_id").is_not_null()),
            HardPredicate(eq("owner_def_id", "def_id")),
        ],
        features=[
            Feature("owner_kind_matches", case_eq("owner_kind", "def_kind"), weight=5.0),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("ds_entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(c("def_bstart"), direction="asc"),
            OrderSpec(c("def_bend"), direction="asc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("ds_entity_id"), "src"),
        SelectExpr(c("def_entity_id"), "dst"),
        SelectExpr(v("has_docstring"), "kind"),
        SelectExpr(c("docstring_text"), "payload"),
    ],
)
```

### 5.3 CST Docstring → Owner Definition (Span fallback, lower confidence)

```python
REL_CST_DOCSTRING_OWNER_BY_SPAN = RelationshipSpec(
    name="rel_cst_docstring_owner_by_span",
    left_view="cst_docstrings_norm",
    right_view="cst_defs_norm",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="libcst",
    origin="semantic_compiler",
    signals=SignalsSpec(
        base_score=500,
        base_confidence=0.75,
        hard=[
            HardPredicate(lambda e: e.col("owner_def_id").is_null()),
            HardPredicate(eq("owner_def_bstart", "def_bstart")),
            HardPredicate(eq("owner_def_bend", "def_bend")),
        ],
        features=[
            Feature("owner_kind_matches", case_eq("owner_kind", "def_kind"), weight=5.0),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("ds_entity_id"),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(c("def_bstart"), direction="asc"),
            OrderSpec(c("def_bend"), direction="asc"),
        ],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("ds_entity_id"), "src"),
        SelectExpr(c("def_entity_id"), "dst"),
        SelectExpr(v("has_docstring"), "kind"),
        SelectExpr(c("docstring_text"), "payload"),
    ],
)
```

### 5.4 CST Name Ref → SCIP Symbol (Span-aligned, high confidence)

```python
REL_CST_REF_TO_SCIP_SYMBOL = RelationshipSpec(
    name="rel_cst_ref_to_scip_symbol",
    left_view="cst_refs_norm",
    right_view="scip_occurrences_norm",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="scip",
    origin="semantic_compiler",
    signals=SignalsSpec(
        base_score=2000,
        base_confidence=0.95,
        hard=[
            HardPredicate(between_overlap("cst_bstart", "cst_bend", "scip_bstart", "scip_bend")),
        ],
        features=[
            Feature(
                "contained",
                lambda e: when(
                    (e.col("scip_bstart") >= e.col("cst_bstart"))
                    & (e.col("scip_bend") <= e.col("cst_bend")),
                    e.lit(1),
                ).otherwise(e.lit(0)),
                weight=50.0,
            ),
            Feature(
                "span_len_delta",
                lambda e: abs_(
                    (e.col("scip_bend") - e.col("scip_bstart"))
                    - (e.col("cst_bend") - e.col("cst_bstart"))
                ),
                weight=-0.01,
            ),
            Feature(
                "ctx_matches_read",
                lambda e: when(
                    (e.col("cst_expr_ctx") == e.lit("Load")) & e.col("scip_is_read"),
                    e.lit(1),
                ).otherwise(e.lit(0)),
                weight=10.0,
            ),
            Feature(
                "ctx_matches_write",
                lambda e: when(
                    (e.col("cst_expr_ctx") == e.lit("Store")) & e.col("scip_is_write"),
                    e.lit(1),
                ).otherwise(e.lit(0)),
                weight=10.0,
            ),
            Feature(
                "coord_encoding_penalty",
                lambda e: when(
                    e.col("scip_position_encoding") == e.lit("UnspecifiedPositionEncoding"),
                    e.lit(1),
                ).otherwise(e.lit(0)),
                weight=-100.0,
                kind="quality",
            ),
            Feature(
                "generated_penalty",
                lambda e: when(e.col("scip_is_generated"), e.lit(1)).otherwise(e.lit(0)),
                weight=-50.0,
                kind="quality",
            ),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("cst_ref_id"),
        ambiguity_group_id_expr=lambda e: stable_hash64(e.col("cst_ref_id")),
        order_by=[
            OrderSpec(c("score"), direction="desc"),
            OrderSpec(c("scip_bstart"), direction="asc"),
            OrderSpec(c("scip_bend"), direction="asc"),
            OrderSpec(c("scip_symbol"), direction="asc"),
        ],
        keep="all",
    ),
    select_exprs=[
        SelectExpr(c("cst_ref_id"), "src"),
        SelectExpr(lambda e: stable_hash64(e.col("scip_symbol")), "dst"),
        SelectExpr(v("refers_to"), "kind"),
        SelectExpr(c("scip_symbol"), "symbol"),
        SelectExpr(c("scip_symbol_roles"), "roles"),
    ],
)
```

### 5.5 Call Site → Definition (Multi-tier resolution)

```python
REL_CALL_TO_DEF_SCIP = RelationshipSpec(
    name="rel_call_to_def_scip",
    left_view="cst_callsites_norm",
    right_view="scip_definitions_norm",
    left_on=["file_id", "callee_symbol"],
    right_on=["file_id", "symbol"],
    how="inner",
    provider="scip",
    origin="semantic_compiler",
    signals=SignalsSpec(
        base_score=2000,
        base_confidence=0.95,
        hard=[],
        features=[
            Feature(
                "signature_arg_match",
                case_eq("def_arg_count", "call_arg_count"),
                weight=20.0,
            ),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("callsite_id"),
        order_by=[OrderSpec(c("score"), direction="desc"), OrderSpec(c("def_bstart"), direction="asc")],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("callsite_id"), "src"),
        SelectExpr(c("def_entity_id"), "dst"),
        SelectExpr(v("calls"), "kind"),
    ],
)

REL_CALL_TO_DEF_NAME = RelationshipSpec(
    name="rel_call_to_def_name",
    left_view="cst_callsites_norm",
    right_view="cst_defs_norm",
    left_on=["file_id"],
    right_on=["file_id"],
    how="inner",
    provider="libcst",
    origin="semantic_compiler",
    signals=SignalsSpec(
        base_score=500,
        base_confidence=0.50,
        hard=[
            HardPredicate(eq("callee_name", "def_name")),
            HardPredicate(gt("callsite_bstart", "def_bstart")),
        ],
        features=[
            Feature("qname_match", case_eq("callee_qname", "def_qname"), weight=30.0),
            Feature(
                "scope_proximity",
                lambda e: lit(1.0)
                / (lit(1.0) + abs_(e.col("callsite_bstart") - e.col("def_bend"))),
                weight=10.0,
            ),
        ],
    ),
    rank=RankSpec(
        ambiguity_key_expr=c("callsite_id"),
        order_by=[OrderSpec(c("score"), direction="desc"), OrderSpec(c("def_bstart"), direction="desc")],
        keep="best",
        top_k=1,
    ),
    select_exprs=[
        SelectExpr(c("callsite_id"), "src"),
        SelectExpr(c("def_entity_id"), "dst"),
        SelectExpr(v("calls"), "kind"),
    ],
)
```

---

## Part 6: Diagnostics and Coverage Reporting

### 6.1 Relationship Quality Metrics View

```python
from datafusion.functions import (
    avg,
    col,
    count,
    count_distinct,
    lit,
    max as max_,
    min as min_,
    sum as sum_,
    when,
)

def build_relationship_quality_metrics(
    ctx: SessionContext,
    relationship_name: str,
) -> DataFrame:
    """Build quality metrics for a compiled relationship."""
    df = ctx.table(relationship_name)
    return (
        df.aggregate(
            [],
            [
                count(lit(1)).alias("total_edges"),
                count_distinct(col("src")).alias("distinct_sources"),
                count_distinct(col("dst")).alias("distinct_targets"),
                avg(col("confidence")).alias("avg_confidence"),
                min_(col("confidence")).alias("min_confidence"),
                max_(col("confidence")).alias("max_confidence"),
                avg(col("score")).alias("avg_score"),
                count_distinct(col("ambiguity_group_id")).alias("ambiguity_groups"),
                sum_(
                    when(col("_rn") > lit(1), lit(1)).otherwise(lit(0))
                ).alias("ambiguous_edges"),
                sum_(
                    when(col("confidence") < lit(0.5), lit(1)).otherwise(lit(0))
                ).alias("low_confidence_edges"),
            ],
        )
        .with_column("relationship_name", lit(relationship_name))
    )
```

### 6.2 File Coverage Report

```python
from datafusion.functions import col, coalesce, count, lit, when

def build_file_coverage_report(ctx: SessionContext) -> DataFrame:
    """Report extraction coverage and quality per file."""
    cst_def_count = (
        ctx.table("cst_defs")
        .aggregate(["file_id"], [count(lit(1)).alias("cnt")])
        .select(col("file_id"), col("cnt").alias("cst_def_count"))
    )
    cst_ref_count = (
        ctx.table("cst_refs")
        .aggregate(["file_id"], [count(lit(1)).alias("cnt")])
        .select(col("file_id"), col("cnt").alias("cst_ref_count"))
    )
    scip_occ_count = (
        ctx.table("scip_occurrences")
        .aggregate(["document_id"], [count(lit(1)).alias("cnt")])
        .select(col("document_id").alias("file_id"), col("cnt").alias("scip_occurrence_count"))
    )
    base = (
        ctx.table("file_index")
        .join(ctx.table("file_quality"), join_keys=(["file_id"], ["file_id"]), how="left")
        .join(cst_def_count, join_keys=(["file_id"], ["file_id"]), how="left")
        .join(cst_ref_count, join_keys=(["file_id"], ["file_id"]), how="left")
        .join(scip_occ_count, join_keys=(["file_id"], ["file_id"]), how="left")
    )
    quality_tier = (
        when(col("file_quality_score") >= lit(800), lit("high"))
        .when(col("file_quality_score") >= lit(500), lit("medium"))
        .otherwise(lit("low"))
    )
    return (
        base.select(
            col("file_id"),
            col("path"),
            col("file_quality_score"),
            col("has_cst_parse_errors"),
            col("ts_error_count"),
            col("has_scip_diagnostics"),
            coalesce(col("cst_def_count"), lit(0)).alias("cst_def_count"),
            coalesce(col("cst_ref_count"), lit(0)).alias("cst_ref_count"),
            coalesce(col("scip_occurrence_count"), lit(0)).alias("scip_occurrence_count"),
            quality_tier.alias("quality_tier"),
        )
        .sort(col("file_quality_score").sort("asc"))
    )
```

### 6.3 Ambiguity Analysis View

```python
from datafusion.functions import (
    array_agg_distinct,
    col,
    count,
    lit,
    max as max_,
    min as min_,
)

def build_ambiguity_analysis(
    ctx: SessionContext,
    relationship_name: str,
) -> DataFrame:
    """Analyze ambiguity patterns in a relationship."""
    df = ctx.table(relationship_name)
    metrics = df.aggregate(
        ["ambiguity_group_id"],
        [
            count(lit(1)).alias("candidate_count"),
            (max_(col("score")) - min_(col("score"))).alias("score_spread"),
            max_(col("confidence")).alias("best_confidence"),
            array_agg_distinct(col("provider")).alias("providers"),
            array_agg_distinct(col("rule_name")).alias("rules"),
        ],
    )
    return (
        metrics.filter(col("candidate_count") > lit(1))
        .sort(col("candidate_count").sort("desc"))
        .limit(100)
    )
```

---

## Part 7: Integration with Existing Semantics Module

### 7.1 Module Structure

```
src/semantics/
├── __init__.py
├── schema.py              # Existing: SemanticSchema discovery
├── specs.py               # Existing: SemanticTableSpec, SpanBinding
├── exprs.py               # NEW: DataFusion Expr helpers, clamp, stable_hash64, ExprContextImpl
├── quality.py             # NEW: SignalsSpec, Feature, RankSpec, RelationshipSpec
├── signals.py             # NEW: File quality view builder
├── compiler.py            # MODIFY: Add quality-aware compilation
├── relationship_specs.py  # NEW: Concrete RelationshipSpec instances
├── diagnostics.py         # NEW: Quality metrics and coverage reports
└── pipeline.py            # MODIFY: Integrate quality views
```

### 7.2 Pipeline Integration Points

1. **Early in pipeline**: Register `file_quality` view before relationship compilation
2. **During compilation**: Pass `file_quality_df` to relationship compiler (DataFusion-native APIs only)
3. **After compilation**: Generate quality metrics for each relationship using DataFrame aggregates
4. **Final output**: Include quality metadata in CPG edges

### 7.3 SemanticCompiler Modifications

```python
class SemanticCompiler:
    """Extended compiler with quality signal integration."""

    def __init__(
        self,
        ctx: SessionContext,
        file_quality_df: DataFrame | None = None,
    ):
        self.ctx = ctx
        self.file_quality_df = file_quality_df or self._build_default_quality()

    def _build_default_quality(self) -> DataFrame:
        """Build file quality view if not provided."""
        return build_file_quality_view(self.ctx)

    def compile_relationship(self, spec: RelationshipSpec) -> DataFrame:
        """Compile relationship with quality signals integrated (DataFusion-native)."""
        return compile_relationship_with_quality(
            self.ctx, spec, self.file_quality_df
        )

    def compile_all_relationships(
        self,
        specs: Sequence[RelationshipSpec],
    ) -> dict[str, DataFrame]:
        """Compile all relationships and collect diagnostics."""
        results = {}
        for spec in specs:
            df = self.compile_relationship(spec)
            results[spec.name] = df
        return results
```

---

## Part 8: Validation and Testing Strategy

### 8.1 Unit Tests for Signal Computation

```python
def test_score_computation_deterministic():
    """Score computation produces same result for same inputs."""
    # Create minimal test data
    # Verify score = base_score + sum(weight * feature)

def test_confidence_bounds():
    """Confidence is always in [0, 1]."""
    # Test edge cases with extreme scores

def test_ambiguity_grouping_stable():
    """Same inputs produce same ambiguity_group_id."""
    # Verify stable_hash64 consistency

def test_expression_validation_rejects_unknown_columns():
    """ExprSpec validation rejects missing columns before execution."""
    # Build spec with a non-existent column and assert validation error

def test_no_sql_strings_in_specs():
    """RelationshipSpec expressions are DataFusion Expr builders only."""
    # Ensure spec helpers produce Expr objects, not SQL strings
```

### 8.2 Integration Tests for Quality Signals

```python
def test_parse_error_penalizes_confidence():
    """Files with parse errors have lower confidence edges."""
    # Create file with parse errors
    # Verify edges have confidence < base_confidence

def test_scip_tier_beats_heuristic():
    """SCIP-based edges outrank heuristic edges."""
    # Create overlapping edges from SCIP and heuristics
    # Verify SCIP edge selected when keep="best"
```

### 8.3 Property-Based Tests

```python
@given(base_score=st.floats(0, 5000), features=st.lists(st.floats(-100, 100)))
def test_score_monotonic_in_base(base_score, features):
    """Higher base_score always produces higher score."""
    # Property: score increases monotonically with base_score
```

---

## Part 9: Rollout Plan

### Phase 1: Foundation (Week 1)
- [x] Implement `exprs.py` helpers (`ExprContextImpl`, `clamp`, `stable_hash64`, DSL helpers)
- [x] Implement `SignalsSpec`, `Feature`, `RankSpec`, `RelationshipSpec` dataclasses
- [x] Implement `build_file_quality_view()` function
- [x] Implement expression validation for `ExprSpec` usage
- [x] Unit tests for core dataclasses

**Status (2026-02-01):** Foundation work is complete, including ExprSpec validation in `semantics/exprs.py` and unit coverage in `tests/unit/semantics`.

### Phase 2: Compiler Integration (Week 2)
- [x] Modify `SemanticCompiler` to accept quality signals
- [x] Implement `compile_relationship_with_quality()`
- [x] Integration tests for quality-aware compilation

**Status (2026-02-01):** Quality-aware compilation is implemented in `semantics/compiler.py` with integration coverage in `tests/unit/semantics/test_quality_compile.py`.

### Phase 3: Relationship Specs (Week 3)
- [x] Define concrete quality specs for core relationships
- [x] Migrate existing semantic rules to new spec format
- [x] Validate output schema compatibility

**Status (2026-02-01):** Core CPG relationship specs now live in `semantics/quality_specs.py` and are used by `semantics/spec_registry.py`. Dataset rows align with projected output schemas.

### Phase 4: Diagnostics (Week 4)
- [x] Implement quality metrics views
- [x] Implement coverage and ambiguity reports
- [ ] Dashboard integration for monitoring

**Status (2026-02-01):** Implemented in `semantics/diagnostics.py` and `semantics/signals.py`, and registered via semantic catalog builders. Dashboard integration remains.

### Phase 5: Production (Week 5+)
- [x] Enable quality signals in production pipeline
- [ ] Monitor confidence distributions
- [ ] Tune weights based on observed quality

**Status (2026-02-01):** Quality signals are enabled in the semantic pipeline; monitoring and tuning remain.

---

## Part 10: Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Average edge confidence | > 0.75 | `AVG(confidence)` across all edges |
| Ambiguity rate | < 5% | Edges with `_rn > 1` / total edges |
| SCIP coverage | > 80% | Files with SCIP data / total Python files |
| Quality degradation detection | < 1 hour | Time to alert on quality drop |
| False positive rate | < 2% | Manual audit sample |

---

## Appendix A: Weight Tuning Guidelines

Initial weights are based on domain knowledge. Tune based on:

1. **Precision analysis**: Sample ambiguous groups, verify winner is correct
2. **Coverage analysis**: Check if valid edges are being filtered
3. **Confidence calibration**: Verify confidence correlates with actual correctness

Tuning loop:
```
For each relationship:
  1. Sample 100 ambiguous groups
  2. Manually label correct winner
  3. Compute accuracy = correct_winners / total_groups
  4. If accuracy < 0.90:
     - Analyze misses
     - Adjust feature weights or add features
     - Re-run
```

---

## Appendix B: YAML Schema for Relationship Specs

For external configuration:

```yaml
relationships:
  - name: rel_cst_docstring_owner
    left_view: cst_docstrings_norm
    right_view: cst_defs_norm
    how: inner
    left_on: [file_id]
    right_on: [file_id]
    provider: libcst
    origin: semantic_compiler

    signals:
      base_score: 1000
      base_confidence: 0.98
      hard:
        - op: and
          args:
            - op: is_not_null
              col: owner_def_id
            - op: eq
              left: owner_def_id
              right: def_id
      features:
        - name: owner_kind_matches
          kind: evidence
          weight: 5
          expr:
            op: case
            when:
              - if:
                  op: eq
                  left: owner_kind
                  right: def_kind
                then: 1

    rank:
      ambiguity_key_expr:
        op: col
        name: ds_entity_id
      order_by:
        - expr:
            op: col
            name: score
          direction: desc
        - expr:
            op: col
            name: def_bstart
          direction: asc
      keep: best
      top_k: 1

    select_exprs:
      - expr:
          op: col
          name: ds_entity_id
        alias: src
      - expr:
          op: col
          name: def_entity_id
        alias: dst
      - expr:
          op: lit
          value: "has_docstring"
        alias: kind
```
