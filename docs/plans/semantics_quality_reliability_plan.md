# Semantics Quality & Reliability Plan

This document proposes a best-in-class reliability and quality approach for the semantic compiler, integrating insights from `semantic_approach_review.md` with the rich extraction data available in our pipeline.

---

## Executive Summary

The semantic compiler produces CPG relationships by joining evidence from multiple extraction sources. This plan establishes a **three-tier signal model** that maximizes join quality while maintaining robustness:

1. **Hard Keys** - Identity constraints that must match for candidate generation
2. **Soft Evidence** - Features that rank candidates within ambiguity groups
3. **Quality Signals** - Provider health metrics that modulate confidence

The approach leverages all available extraction metadata: CST parse errors, SCIP diagnostics, tree-sitter stats, and coordinate encoding metadata to produce relationships with explicit `confidence`, `score`, and `ambiguity_group_id` fields.

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

---

## Part 4: Implementation Specifications

### 4.1 Core Dataclasses

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Sequence

ExprStr = str  # SQL expression strings for DataFusion compilation


@dataclass(frozen=True)
class HardPredicate:
    """Boolean predicate that must be true for valid candidates."""
    expr: ExprStr


@dataclass(frozen=True)
class Feature:
    """Numeric feature for scoring candidates."""
    name: str
    expr: ExprStr
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
    ambiguity_key_expr: ExprStr
    ambiguity_group_id_expr: ExprStr | None = None
    order_by: Sequence[str] = ("score DESC",)
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
    select_exprs: Sequence[ExprStr] = ()
```

### 4.2 File Quality View

Create a reusable `file_quality` view aggregating all provider health signals:

```python
def build_file_quality_view(ctx: SessionContext) -> DataFrame:
    """Build aggregated file quality signals from all extraction sources."""
    return ctx.sql("""
        WITH
        cst_err AS (
            SELECT
                file_id,
                CAST(COUNT(*) > 0 AS INT) AS has_cst_parse_errors,
                COUNT(*) AS cst_error_count
            FROM cst_parse_errors
            GROUP BY file_id
        ),
        ts AS (
            SELECT
                file_id,
                CAST(parse_timed_out AS INT) AS ts_timed_out,
                error_count AS ts_error_count,
                missing_count AS ts_missing_count,
                CAST(match_limit_exceeded AS INT) AS ts_match_limit_exceeded
            FROM ts_stats
        ),
        scip_diag AS (
            SELECT
                document_id AS file_id,
                CAST(COUNT(*) > 0 AS INT) AS has_scip_diagnostics,
                COUNT(*) AS scip_diagnostic_count
            FROM scip_diagnostics
            GROUP BY document_id
        ),
        scip_meta AS (
            SELECT
                document_id AS file_id,
                CASE WHEN position_encoding = 'UnspecifiedPositionEncoding' THEN 1 ELSE 0 END
                    AS scip_encoding_unspecified
            FROM scip_documents
        )
        SELECT
            f.file_id,
            f.file_sha256,
            COALESCE(cst_err.has_cst_parse_errors, 0) AS has_cst_parse_errors,
            COALESCE(cst_err.cst_error_count, 0) AS cst_error_count,
            COALESCE(ts.ts_timed_out, 0) AS ts_timed_out,
            COALESCE(ts.ts_error_count, 0) AS ts_error_count,
            COALESCE(ts.ts_missing_count, 0) AS ts_missing_count,
            COALESCE(ts.ts_match_limit_exceeded, 0) AS ts_match_limit_exceeded,
            COALESCE(scip_diag.has_scip_diagnostics, 0) AS has_scip_diagnostics,
            COALESCE(scip_diag.scip_diagnostic_count, 0) AS scip_diagnostic_count,
            COALESCE(scip_meta.scip_encoding_unspecified, 0) AS scip_encoding_unspecified,
            -- Composite quality score (lower = worse)
            (1000
             - COALESCE(cst_err.has_cst_parse_errors, 0) * 200
             - COALESCE(ts.ts_timed_out, 0) * 300
             - COALESCE(ts.ts_error_count, 0) * 10
             - COALESCE(ts.ts_match_limit_exceeded, 0) * 150
             - COALESCE(scip_diag.has_scip_diagnostics, 0) * 100
             - COALESCE(scip_meta.scip_encoding_unspecified, 0) * 50
            ) AS file_quality_score
        FROM file_index f
        LEFT JOIN cst_err ON f.file_id = cst_err.file_id
        LEFT JOIN ts ON f.file_id = ts.file_id
        LEFT JOIN scip_diag ON f.file_id = scip_diag.file_id
        LEFT JOIN scip_meta ON f.file_id = scip_meta.file_id
    """)
```

### 4.3 Relationship Compiler with Quality Integration

```python
def compile_relationship_with_quality(
    ctx: SessionContext,
    spec: RelationshipSpec,
    file_quality_df: DataFrame,
) -> DataFrame:
    """Compile relationship spec to DataFusion DataFrame with quality signals."""

    l = ctx.table(spec.left_view)
    r = ctx.table(spec.right_view)

    # 1) Equi-join on hard keys
    if spec.left_on and spec.right_on:
        df = l.join(r, join_keys=(list(spec.left_on), list(spec.right_on)), how=spec.how)
    else:
        df = l.cross_join(r)

    # 2) Join file quality signals
    df = df.join(
        file_quality_df.select("file_id", "file_quality_score", "has_cst_parse_errors",
                               "ts_timed_out", "has_scip_diagnostics"),
        join_keys=(["file_id"], ["file_id"]),
        how="left"
    )

    # 3) Apply hard predicates
    for hp in spec.signals.hard:
        df = df.filter(hp.expr)

    # 4) Add feature columns
    feature_cols = []
    for f in spec.signals.features:
        feature_cols.append(f"{f.expr} AS feat__{f.kind}__{f.name}")
    if feature_cols:
        df = df.select_exprs("*", *feature_cols)

    # 5) Compute score = base_score + weighted_features + file_quality_adjustment
    score_terms = [str(spec.signals.base_score)]
    for f in spec.signals.features:
        score_terms.append(f"({f.weight}) * feat__{f.kind}__{f.name}")
    # Add file quality contribution
    score_terms.append("COALESCE(file_quality_score - 1000, 0)")  # Normalize around 0
    score_expr = " + ".join(score_terms) + " AS score"

    # 6) Compute confidence = clamp(base + scaled_score)
    conf_expr = (
        f"LEAST(1.0, GREATEST(0.0, "
        f"{spec.signals.base_confidence} + (score / 10000.0)"
        f")) AS confidence"
    )

    rule_name = spec.rule_name or spec.name
    df = df.select_exprs(
        *spec.select_exprs,
        score_expr,
        conf_expr,
        f"'{spec.origin}' AS origin",
        f"'{spec.provider}' AS provider",
        f"'{rule_name}' AS rule_name",
    )

    # 7) Ambiguity grouping + deterministic winner selection
    if spec.rank:
        group_key = spec.rank.ambiguity_key_expr
        group_id = spec.rank.ambiguity_group_id_expr or f"stable_hash64({group_key})"
        df = df.select_exprs(
            "*",
            f"{group_id} AS ambiguity_group_id",
            f"ROW_NUMBER() OVER ("
            f"PARTITION BY {group_key} "
            f"ORDER BY {', '.join(spec.rank.order_by)}"
            f") AS _rn",
        )
        if spec.rank.keep == "best":
            df = df.filter(f"_rn <= {spec.rank.top_k}").drop_columns("_rn")

    return df
```

---

## Part 5: Concrete Relationship Specifications

### 5.1 CST Docstring → Owner Definition (ID-based, high confidence)

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
            HardPredicate("owner_def_id IS NOT NULL"),
            HardPredicate("owner_def_id = def_id"),
        ],
        features=[
            Feature("owner_kind_matches",
                    "CASE WHEN owner_kind = def_kind THEN 1 ELSE 0 END",
                    weight=5.0, kind="evidence"),
        ],
    ),

    rank=RankSpec(
        ambiguity_key_expr="ds_entity_id",
        order_by=["score DESC", "def_bstart ASC", "def_bend ASC"],
        keep="best",
        top_k=1,
    ),

    select_exprs=[
        "ds_entity_id AS src",
        "def_entity_id AS dst",
        "'has_docstring' AS kind",
        "docstring_text AS payload",
    ],
)
```

### 5.2 CST Docstring → Owner Definition (Span fallback, lower confidence)

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
            HardPredicate("owner_def_id IS NULL"),
            HardPredicate("owner_def_bstart = def_bstart"),
            HardPredicate("owner_def_bend = def_bend"),
        ],
        features=[
            Feature("owner_kind_matches",
                    "CASE WHEN owner_kind = def_kind THEN 1 ELSE 0 END",
                    weight=5.0, kind="evidence"),
        ],
    ),

    rank=RankSpec(
        ambiguity_key_expr="ds_entity_id",
        order_by=["score DESC", "def_bstart ASC", "def_bend ASC"],
        keep="best",
        top_k=1,
    ),

    select_exprs=[
        "ds_entity_id AS src",
        "def_entity_id AS dst",
        "'has_docstring' AS kind",
        "docstring_text AS payload",
    ],
)
```

### 5.3 CST Name Ref → SCIP Symbol (Span-aligned, high confidence)

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
        base_score=2000,  # SCIP tier
        base_confidence=0.95,
        hard=[
            # Span overlap predicate
            HardPredicate("cst_bstart < scip_bend AND scip_bstart < cst_bend"),
        ],
        features=[
            # Prefer contained spans
            Feature("contained",
                    "CASE WHEN scip_bstart >= cst_bstart AND scip_bend <= cst_bend THEN 1 ELSE 0 END",
                    weight=50.0, kind="evidence"),
            # Penalize large span differences
            Feature("span_len_delta",
                    "ABS((scip_bend - scip_bstart) - (cst_bend - cst_bstart))",
                    weight=-0.01, kind="evidence"),
            # Context matching
            Feature("ctx_matches_read",
                    "CASE WHEN cst_expr_ctx = 'Load' AND scip_is_read THEN 1 ELSE 0 END",
                    weight=10.0, kind="evidence"),
            Feature("ctx_matches_write",
                    "CASE WHEN cst_expr_ctx = 'Store' AND scip_is_write THEN 1 ELSE 0 END",
                    weight=10.0, kind="evidence"),
            # Quality: penalize missing coordinate encoding
            Feature("coord_encoding_penalty",
                    "CASE WHEN scip_position_encoding = 'UnspecifiedPositionEncoding' THEN 1 ELSE 0 END",
                    weight=-100.0, kind="quality"),
            # Quality: penalize generated/test code
            Feature("generated_penalty",
                    "CASE WHEN scip_is_generated THEN 1 ELSE 0 END",
                    weight=-50.0, kind="quality"),
        ],
    ),

    rank=RankSpec(
        ambiguity_key_expr="cst_ref_id",
        ambiguity_group_id_expr="stable_hash64(cst_ref_id)",
        order_by=["score DESC", "scip_bstart ASC", "scip_bend ASC", "scip_symbol ASC"],
        keep="all",  # Keep all for analysis; set to "best" for production
    ),

    select_exprs=[
        "cst_ref_id AS src",
        "stable_hash64(scip_symbol) AS dst",
        "'refers_to' AS kind",
        "scip_symbol AS symbol",
        "scip_symbol_roles AS roles",
    ],
)
```

### 5.4 Call Site → Definition (Multi-tier resolution)

```python
# Tier 1: SCIP-resolved call target
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
        hard=[],  # Equi-join on symbol is sufficient
        features=[
            Feature("signature_arg_match",
                    "CASE WHEN def_arg_count = call_arg_count THEN 1 ELSE 0 END",
                    weight=20.0, kind="evidence"),
        ],
    ),

    rank=RankSpec(
        ambiguity_key_expr="callsite_id",
        order_by=["score DESC", "def_bstart ASC"],
        keep="best",
        top_k=1,
    ),

    select_exprs=[
        "callsite_id AS src",
        "def_entity_id AS dst",
        "'calls' AS kind",
    ],
)

# Tier 2: Name-based fallback (lower confidence)
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
        base_score=500,  # Heuristic tier
        base_confidence=0.50,
        hard=[
            HardPredicate("callee_name = def_name"),
            HardPredicate("callsite_bstart > def_bstart"),  # Call after def
        ],
        features=[
            Feature("qname_match",
                    "CASE WHEN callee_qname = def_qname THEN 1 ELSE 0 END",
                    weight=30.0, kind="evidence"),
            Feature("scope_proximity",
                    "1.0 / (1 + ABS(callsite_bstart - def_bend))",
                    weight=10.0, kind="evidence"),
        ],
    ),

    rank=RankSpec(
        ambiguity_key_expr="callsite_id",
        order_by=["score DESC", "def_bstart DESC"],  # Prefer nearest preceding def
        keep="best",
        top_k=1,
    ),

    select_exprs=[
        "callsite_id AS src",
        "def_entity_id AS dst",
        "'calls' AS kind",
    ],
)
```

---

## Part 6: Diagnostics and Coverage Reporting

### 6.1 Relationship Quality Metrics View

```python
def build_relationship_quality_metrics(
    ctx: SessionContext,
    relationship_name: str,
) -> DataFrame:
    """Build quality metrics for a compiled relationship."""
    return ctx.sql(f"""
        SELECT
            '{relationship_name}' AS relationship_name,
            COUNT(*) AS total_edges,
            COUNT(DISTINCT src) AS distinct_sources,
            COUNT(DISTINCT dst) AS distinct_targets,
            AVG(confidence) AS avg_confidence,
            MIN(confidence) AS min_confidence,
            MAX(confidence) AS max_confidence,
            AVG(score) AS avg_score,
            COUNT(DISTINCT ambiguity_group_id) AS ambiguity_groups,
            SUM(CASE WHEN _rn > 1 THEN 1 ELSE 0 END) AS ambiguous_edges,
            SUM(CASE WHEN confidence < 0.5 THEN 1 ELSE 0 END) AS low_confidence_edges
        FROM {relationship_name}
    """)
```

### 6.2 File Coverage Report

```python
def build_file_coverage_report(ctx: SessionContext) -> DataFrame:
    """Report extraction coverage and quality per file."""
    return ctx.sql("""
        SELECT
            f.file_id,
            f.path,
            fq.file_quality_score,
            fq.has_cst_parse_errors,
            fq.ts_error_count,
            fq.has_scip_diagnostics,
            COALESCE(cst_def_count.cnt, 0) AS cst_def_count,
            COALESCE(cst_ref_count.cnt, 0) AS cst_ref_count,
            COALESCE(scip_occ_count.cnt, 0) AS scip_occurrence_count,
            CASE
                WHEN fq.file_quality_score >= 800 THEN 'high'
                WHEN fq.file_quality_score >= 500 THEN 'medium'
                ELSE 'low'
            END AS quality_tier
        FROM file_index f
        LEFT JOIN file_quality fq ON f.file_id = fq.file_id
        LEFT JOIN (
            SELECT file_id, COUNT(*) AS cnt FROM cst_defs GROUP BY file_id
        ) cst_def_count ON f.file_id = cst_def_count.file_id
        LEFT JOIN (
            SELECT file_id, COUNT(*) AS cnt FROM cst_refs GROUP BY file_id
        ) cst_ref_count ON f.file_id = cst_ref_count.file_id
        LEFT JOIN (
            SELECT document_id AS file_id, COUNT(*) AS cnt FROM scip_occurrences GROUP BY document_id
        ) scip_occ_count ON f.file_id = scip_occ_count.file_id
        ORDER BY fq.file_quality_score ASC
    """)
```

### 6.3 Ambiguity Analysis View

```python
def build_ambiguity_analysis(
    ctx: SessionContext,
    relationship_name: str,
) -> DataFrame:
    """Analyze ambiguity patterns in a relationship."""
    return ctx.sql(f"""
        SELECT
            ambiguity_group_id,
            COUNT(*) AS candidate_count,
            MAX(score) - MIN(score) AS score_spread,
            MAX(confidence) AS best_confidence,
            ARRAY_AGG(DISTINCT provider) AS providers,
            ARRAY_AGG(DISTINCT rule_name) AS rules
        FROM {relationship_name}
        GROUP BY ambiguity_group_id
        HAVING COUNT(*) > 1
        ORDER BY candidate_count DESC
        LIMIT 100
    """)
```

---

## Part 7: Integration with Existing Semantics Module

### 7.1 Module Structure

```
src/semantics/
├── __init__.py
├── schema.py              # Existing: SemanticSchema discovery
├── specs.py               # Existing: SemanticTableSpec, SpanBinding
├── quality.py             # NEW: SignalsSpec, Feature, RankSpec, RelationshipSpec
├── signals.py             # NEW: File quality view builder
├── compiler.py            # MODIFY: Add quality-aware compilation
├── relationship_specs.py  # NEW: Concrete RelationshipSpec instances
├── diagnostics.py         # NEW: Quality metrics and coverage reports
└── pipeline.py            # MODIFY: Integrate quality views
```

### 7.2 Pipeline Integration Points

1. **Early in pipeline**: Register `file_quality` view before relationship compilation
2. **During compilation**: Pass `file_quality_df` to relationship compiler
3. **After compilation**: Generate quality metrics for each relationship
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
        """Compile relationship with quality signals integrated."""
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
- [ ] Implement `SignalsSpec`, `Feature`, `RankSpec`, `RelationshipSpec` dataclasses
- [ ] Implement `build_file_quality_view()` function
- [ ] Unit tests for core dataclasses

### Phase 2: Compiler Integration (Week 2)
- [ ] Modify `SemanticCompiler` to accept quality signals
- [ ] Implement `compile_relationship_with_quality()`
- [ ] Integration tests for quality-aware compilation

### Phase 3: Relationship Specs (Week 3)
- [ ] Define `relationship_specs.py` with all concrete specs
- [ ] Migrate existing semantic rules to new spec format
- [ ] Validate output schema compatibility

### Phase 4: Diagnostics (Week 4)
- [ ] Implement quality metrics views
- [ ] Implement coverage and ambiguity reports
- [ ] Dashboard integration for monitoring

### Phase 5: Production (Week 5+)
- [ ] Enable quality signals in production pipeline
- [ ] Monitor confidence distributions
- [ ] Tune weights based on observed quality

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
        - expr: "owner_def_id IS NOT NULL AND owner_def_id = def_id"
      features:
        - name: owner_kind_matches
          kind: evidence
          weight: 5
          expr: "CASE WHEN owner_kind = def_kind THEN 1 ELSE 0 END"

    rank:
      ambiguity_key_expr: "ds_entity_id"
      order_by: ["score DESC", "def_bstart ASC"]
      keep: best
      top_k: 1

    select_exprs:
      - "ds_entity_id AS src"
      - "def_entity_id AS dst"
      - "'has_docstring' AS kind"
```
