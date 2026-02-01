"""File quality view builder for quality-aware relationships.

This module builds aggregated file quality signals from multiple extraction
sources. The file_quality_v1 view provides per-file quality metrics that
can be joined during relationship compilation to adjust confidence scores.

Quality signals include:
- CST parse errors (from cst_parse_errors)
- Tree-sitter stats (from tree_sitter_files_v1.stats struct field)
- SCIP diagnostics (from scip_diagnostics)
- SCIP encoding quality (from scip_documents)

Usage
-----
>>> from semantics.signals import build_file_quality_view
>>> from datafusion import SessionContext

>>> ctx = SessionContext()
>>> # ... register extraction tables ...
>>> file_quality_df = build_file_quality_view(ctx)
>>> print(file_quality_df.schema())
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion import Expr, col, lit
from datafusion import functions as f

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame


# Default weights for quality penalty computation
# Higher weight = larger penalty for that issue
DEFAULT_QUALITY_WEIGHTS: dict[str, int] = {
    "has_cst_parse_errors": 200,
    "ts_timed_out": 300,
    "ts_error_count": 10,  # Per error
    "ts_match_limit_exceeded": 150,
    "has_scip_diagnostics": 100,
    "scip_encoding_unspecified": 50,
}

# Base quality score (penalties subtracted from this)
BASE_QUALITY_SCORE: int = 1000


def _table_exists(ctx: SessionContext, name: str) -> bool:
    """Check if a table exists in the session context.

    Returns
    -------
    bool
        True if table exists, False otherwise.
    """
    try:
        ctx.table(name)
    except Exception:  # noqa: BLE001
        return False
    return True


def _build_cst_parse_errors_signals(ctx: SessionContext) -> DataFrame | None:
    """Build CST parse error signals.

    Aggregates cst_parse_errors by file_id to produce:
    - has_cst_parse_errors: 1 if any errors, 0 otherwise
    - cst_error_count: count of parse errors

    Returns
    -------
    DataFrame | None
        CST parse error signals DataFrame, or None if table doesn't exist.
    """
    if not _table_exists(ctx, "cst_parse_errors"):
        return None

    return ctx.table("cst_parse_errors").aggregate(
        [col("file_id")],
        [
            (f.count(lit(1)) > lit(0)).cast("int32").alias("has_cst_parse_errors"),
            f.count(lit(1)).alias("cst_error_count"),
        ],
    )


def _build_tree_sitter_signals(ctx: SessionContext) -> DataFrame | None:
    """Build tree-sitter quality signals.

    Extracts stats struct fields from tree_sitter_files_v1:
    - ts_timed_out: 1 if parse timed out, 0 otherwise
    - ts_error_count: count of syntax errors
    - ts_missing_count: count of missing nodes
    - ts_match_limit_exceeded: 1 if query match limit exceeded, 0 otherwise

    Returns
    -------
    DataFrame | None
        Tree-sitter signals DataFrame, or None if table doesn't exist.
    """
    if not _table_exists(ctx, "tree_sitter_files_v1"):
        return None

    # Access nested struct fields via [] indexing syntax
    stats = col("stats")
    return ctx.table("tree_sitter_files_v1").select(
        col("file_id"),
        # Cast booleans to int for easier aggregation
        f.coalesce(
            stats["parse_timed_out"].cast("int32"),
            lit(0),
        ).alias("ts_timed_out"),
        f.coalesce(
            stats["error_count"],
            lit(0),
        ).alias("ts_error_count"),
        f.coalesce(
            stats["missing_count"],
            lit(0),
        ).alias("ts_missing_count"),
        f.coalesce(
            stats["match_limit_exceeded"].cast("int32"),
            lit(0),
        ).alias("ts_match_limit_exceeded"),
    )


def _build_scip_diagnostics_signals(ctx: SessionContext) -> DataFrame | None:
    """Build SCIP diagnostics signals.

    Aggregates scip_diagnostics by document_id (aliased to file_id):
    - has_scip_diagnostics: 1 if any diagnostics, 0 otherwise
    - scip_diagnostic_count: count of diagnostics

    Returns
    -------
    DataFrame | None
        SCIP diagnostics signals DataFrame, or None if table doesn't exist.
    """
    if not _table_exists(ctx, "scip_diagnostics"):
        return None

    return (
        ctx.table("scip_diagnostics")
        .aggregate(
            [col("document_id")],
            [
                (f.count(lit(1)) > lit(0)).cast("int32").alias("has_scip_diagnostics"),
                f.count(lit(1)).alias("scip_diagnostic_count"),
            ],
        )
        .select(
            col("document_id").alias("file_id"),
            col("has_scip_diagnostics"),
            col("scip_diagnostic_count"),
        )
    )


def _build_scip_encoding_signals(ctx: SessionContext) -> DataFrame | None:
    """Build SCIP position encoding signals.

    Extracts position encoding quality from scip_documents:
    - scip_encoding_unspecified: 1 if encoding is unspecified, 0 otherwise

    Unspecified encoding is a quality signal because it may indicate
    incomplete or low-quality SCIP data.

    Returns
    -------
    DataFrame | None
        SCIP encoding signals DataFrame, or None if table doesn't exist.
    """
    if not _table_exists(ctx, "scip_documents"):
        return None

    return ctx.table("scip_documents").select(
        col("document_id").alias("file_id"),
        f.when(
            col("position_encoding") == lit("UnspecifiedPositionEncoding"),
            lit(1),
        )
        .otherwise(lit(0))
        .alias("scip_encoding_unspecified"),
    )


def _join_if_present(base: DataFrame, signals: DataFrame | None) -> DataFrame:
    """Left join signals DataFrame to base if not None.

    Returns
    -------
    DataFrame
        Joined DataFrame (unchanged if signals is None).
    """
    if signals is None:
        return base
    return base.join(signals, left_on=["file_id"], right_on=["file_id"], how="left")


def _coalesce_col(name: str) -> Expr:
    """Return coalesced column expression with 0 default.

    Returns
    -------
    Expr
        Coalesced column expression.
    """
    return f.coalesce(col(name), lit(0))


def _compute_quality_score(weights: dict[str, int]) -> Expr:
    """Compute quality score expression from weights.

    Returns
    -------
    Expr
        Quality score expression.
    """
    return (
        lit(BASE_QUALITY_SCORE)
        - _coalesce_col("has_cst_parse_errors") * lit(weights.get("has_cst_parse_errors", 0))
        - _coalesce_col("ts_timed_out") * lit(weights.get("ts_timed_out", 0))
        - _coalesce_col("ts_error_count") * lit(weights.get("ts_error_count", 0))
        - _coalesce_col("ts_match_limit_exceeded") * lit(weights.get("ts_match_limit_exceeded", 0))
        - _coalesce_col("has_scip_diagnostics") * lit(weights.get("has_scip_diagnostics", 0))
        - _coalesce_col("scip_encoding_unspecified")
        * lit(weights.get("scip_encoding_unspecified", 0))
    )


def build_file_quality_view(
    ctx: SessionContext,
    *,
    base_table: str = "file_index",
    weights: dict[str, int] | None = None,
) -> DataFrame:
    """Build aggregated file quality signals from all extraction sources.

    Joins quality signals from multiple sources and computes a weighted
    quality score per file. Files with higher quality scores are preferred
    during relationship compilation.

    Parameters
    ----------
    ctx
        DataFusion session context with extraction tables registered.
    base_table
        Name of the base file table (default: "file_index").
        Must have file_id column.
    weights
        Quality penalty weights. Keys are signal names, values are
        penalty amounts subtracted from base score. Uses DEFAULT_QUALITY_WEIGHTS
        if not provided.

    Returns
    -------
    DataFrame
        File quality view with columns:
        - file_id: File identifier
        - file_sha256: File content hash (if available)
        - has_cst_parse_errors: 1 if CST parse errors exist
        - cst_error_count: Number of CST parse errors
        - ts_timed_out: 1 if tree-sitter parse timed out
        - ts_error_count: Number of tree-sitter syntax errors
        - ts_missing_count: Number of tree-sitter missing nodes
        - ts_match_limit_exceeded: 1 if tree-sitter match limit exceeded
        - has_scip_diagnostics: 1 if SCIP diagnostics exist
        - scip_diagnostic_count: Number of SCIP diagnostics
        - scip_encoding_unspecified: 1 if SCIP position encoding unspecified
        - file_quality_score: Computed quality score (higher = better)

    Raises
    ------
    ValueError
        If base_table doesn't exist in the context.

    Notes
    -----
    Uses LEFT JOINs to handle missing extraction tables gracefully.
    Missing signals are coalesced to 0 (no penalty).

    The file_quality_score is computed as:
        base_score - sum(signal_value * signal_weight)

    Where base_score is 1000 by default.
    """
    if not _table_exists(ctx, base_table):
        msg = f"Base table {base_table!r} not found in session context."
        raise ValueError(msg)

    resolved_weights = weights or DEFAULT_QUALITY_WEIGHTS

    # Start with base file table and check for file_sha256
    base = ctx.table(base_table)
    base_columns: list[str] = list(base.schema().names) if hasattr(base.schema(), "names") else []
    has_sha256 = "file_sha256" in base_columns

    # Build base selection
    base = (
        base.select(col("file_id"), col("file_sha256"))
        if has_sha256
        else base.select(col("file_id"), lit(None).cast("string").alias("file_sha256"))
    )

    # Join all signal sources
    base = _join_if_present(base, _build_cst_parse_errors_signals(ctx))
    base = _join_if_present(base, _build_tree_sitter_signals(ctx))
    base = _join_if_present(base, _build_scip_diagnostics_signals(ctx))
    base = _join_if_present(base, _build_scip_encoding_signals(ctx))

    # Build final selection with coalesced defaults and computed score
    return base.select(
        col("file_id"),
        col("file_sha256"),
        _coalesce_col("has_cst_parse_errors").alias("has_cst_parse_errors"),
        _coalesce_col("cst_error_count").alias("cst_error_count"),
        _coalesce_col("ts_timed_out").alias("ts_timed_out"),
        _coalesce_col("ts_error_count").alias("ts_error_count"),
        _coalesce_col("ts_missing_count").alias("ts_missing_count"),
        _coalesce_col("ts_match_limit_exceeded").alias("ts_match_limit_exceeded"),
        _coalesce_col("has_scip_diagnostics").alias("has_scip_diagnostics"),
        _coalesce_col("scip_diagnostic_count").alias("scip_diagnostic_count"),
        _coalesce_col("scip_encoding_unspecified").alias("scip_encoding_unspecified"),
        _compute_quality_score(resolved_weights).alias("file_quality_score"),
    )


__all__ = [
    "BASE_QUALITY_SCORE",
    "DEFAULT_QUALITY_WEIGHTS",
    "build_file_quality_view",
]
