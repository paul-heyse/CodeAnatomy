"""Dependency inference from Ibis/SQLGlot expression analysis.

This module provides calculation-driven dependency inference as an alternative
to declared-dependency scheduling. The dependency graph is derived from actual
Ibis/DataFusion expression analysis rather than bespoke rule specifications.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlglot.errors import SqlglotError

from sqlglot_tools.bridge import IbisCompilerBackend, ibis_to_sqlglot
from sqlglot_tools.lineage import referenced_tables, required_columns_by_table
from sqlglot_tools.optimizer import plan_fingerprint

if TYPE_CHECKING:
    from ibis_engine.plan import IbisPlan
    from sqlglot_tools.compat import Expression

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class InferredDeps:
    """Dependencies inferred from Ibis/SQLGlot expression analysis.

    Captures table and column-level dependencies by analyzing the actual
    query plan rather than relying on declared inputs.

    Attributes
    ----------
    rule_name : str
        Name of the rule these dependencies apply to.
    output : str
        Output dataset name produced by the rule.
    inputs : tuple[str, ...]
        Table names inferred from expression analysis.
    required_columns : Mapping[str, tuple[str, ...]]
        Per-table columns required by the expression.
    plan_fingerprint : str
        Stable hash for caching and comparison.
    declared_inputs : tuple[str, ...] | None
        Original declared inputs for comparison, if available.
    inputs_match : bool
        Whether inferred inputs match declared inputs.
    extra_inferred : tuple[str, ...]
        Tables inferred but not declared.
    missing_declared : tuple[str, ...]
        Tables declared but not inferred.
    """

    rule_name: str
    output: str
    inputs: tuple[str, ...]
    required_columns: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    plan_fingerprint: str = ""
    declared_inputs: tuple[str, ...] | None = None
    inputs_match: bool = True
    extra_inferred: tuple[str, ...] = ()
    missing_declared: tuple[str, ...] = ()


@dataclass(frozen=True)
class InferredDepsComparison:
    """Summary of comparison between declared and inferred dependencies.

    Attributes
    ----------
    total_rules : int
        Total number of rules compared.
    matched_rules : int
        Rules where declared and inferred inputs match.
    mismatched_rules : int
        Rules with discrepancies.
    extra_inferred_total : int
        Total count of extra inferred tables across all rules.
    missing_declared_total : int
        Total count of missing declared tables across all rules.
    mismatches : tuple[InferredDeps, ...]
        Detailed mismatch information for each discrepant rule.
    """

    total_rules: int
    matched_rules: int
    mismatched_rules: int
    extra_inferred_total: int
    missing_declared_total: int
    mismatches: tuple[InferredDeps, ...] = ()


@dataclass(frozen=True)
class InferredDepsRequest:
    """Inputs for dependency inference."""

    rule_name: str
    output: str
    declared_inputs: tuple[str, ...] | None = None
    dialect: str = "datafusion"


def infer_deps_from_ibis_plan(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    request: InferredDepsRequest,
    sqlglot_expr: Expression | None = None,
) -> InferredDeps:
    """Infer dependencies from an Ibis plan using SQLGlot lineage.

    Analyzes the Ibis expression to extract:
    - Referenced tables (input dependencies)
    - Required columns per table (column-level lineage)
    - Plan fingerprint for caching

    Parameters
    ----------
    plan : IbisPlan
        Compiled Ibis plan to analyze.
    backend : IbisCompilerBackend
        Ibis backend with SQLGlot compiler.
    request : InferredDepsRequest
        Request payload including rule name, output, and optional declared inputs.
    sqlglot_expr : Expression | None
        Optional precompiled SQLGlot expression for the plan.

    Returns
    -------
    InferredDeps
        Inferred dependencies with comparison metadata.
    """
    rule_name = request.rule_name
    output = request.output
    declared_inputs = request.declared_inputs
    dialect = request.dialect

    # Compile Ibis to SQLGlot for analysis
    sg_expr = sqlglot_expr or ibis_to_sqlglot(plan.expr, backend=backend, params=None)

    # Extract table references
    tables = referenced_tables(sg_expr)

    # Extract column-level lineage
    columns_by_table: dict[str, tuple[str, ...]] = {}
    try:
        columns_by_table = required_columns_by_table(
            plan.expr,
            backend=backend,
            dialect=dialect,
        )
    except (SqlglotError, TypeError, ValueError):
        _LOG.debug(
            "Column-level lineage extraction failed for rule %r, using table-level only",
            rule_name,
            exc_info=True,
        )

    # Compute plan fingerprint
    fingerprint = plan_fingerprint(sg_expr, dialect=dialect)

    # Compare with declared inputs if provided
    inputs_match = True
    extra_inferred: tuple[str, ...] = ()
    missing_declared: tuple[str, ...] = ()

    if declared_inputs is not None:
        inferred_set = set(tables)
        declared_set = set(declared_inputs)
        extra_inferred = tuple(sorted(inferred_set - declared_set))
        missing_declared = tuple(sorted(declared_set - inferred_set))
        inputs_match = not extra_inferred and not missing_declared

    return InferredDeps(
        rule_name=rule_name,
        output=output,
        inputs=tables,
        required_columns=columns_by_table,
        plan_fingerprint=fingerprint,
        declared_inputs=declared_inputs,
        inputs_match=inputs_match,
        extra_inferred=extra_inferred,
        missing_declared=missing_declared,
    )


def infer_deps_from_sqlglot_expr(
    expr: object,
    *,
    rule_name: str,
    output: str,
    declared_inputs: tuple[str, ...] | None = None,
    dialect: str = "datafusion",
) -> InferredDeps:
    """Infer dependencies from a raw SQLGlot expression.

    Lower-level variant that works directly with SQLGlot expressions
    when an Ibis plan is not available.

    Parameters
    ----------
    expr : Expression
        SQLGlot expression to analyze.
    rule_name : str
        Name of the rule being analyzed.
    output : str
        Output dataset name.
    declared_inputs : tuple[str, ...] | None
        Original declared inputs for comparison.
    dialect : str
        SQL dialect for fingerprinting.

    Returns
    -------
    InferredDeps
        Inferred dependencies.

    Raises
    ------
    TypeError
        Raised when expr is not a SQLGlot Expression.
    """
    from sqlglot_tools.compat import Expression

    if not isinstance(expr, Expression):
        msg = f"Expected SQLGlot Expression, got {type(expr).__name__}"
        raise TypeError(msg)

    # Extract table references
    tables = referenced_tables(expr)

    # Compute plan fingerprint
    fingerprint = plan_fingerprint(expr, dialect=dialect)

    # Compare with declared inputs if provided
    inputs_match = True
    extra_inferred: tuple[str, ...] = ()
    missing_declared: tuple[str, ...] = ()

    if declared_inputs is not None:
        inferred_set = set(tables)
        declared_set = set(declared_inputs)
        extra_inferred = tuple(sorted(inferred_set - declared_set))
        missing_declared = tuple(sorted(declared_set - inferred_set))
        inputs_match = not extra_inferred and not missing_declared

    return InferredDeps(
        rule_name=rule_name,
        output=output,
        inputs=tables,
        required_columns={},  # Cannot extract without Ibis schema context
        plan_fingerprint=fingerprint,
        declared_inputs=declared_inputs,
        inputs_match=inputs_match,
        extra_inferred=extra_inferred,
        missing_declared=missing_declared,
    )


def compare_deps(
    declared: tuple[str, ...],
    inferred: InferredDeps,
) -> InferredDeps:
    """Compare declared inputs against inferred dependencies.

    Updates the InferredDeps with comparison results.

    Parameters
    ----------
    declared : tuple[str, ...]
        Declared input dependencies.
    inferred : InferredDeps
        Previously inferred dependencies.

    Returns
    -------
    InferredDeps
        Updated with comparison metadata.
    """
    declared_set = set(declared)
    inferred_set = set(inferred.inputs)

    extra_inferred = tuple(sorted(inferred_set - declared_set))
    missing_declared = tuple(sorted(declared_set - inferred_set))
    inputs_match = not extra_inferred and not missing_declared

    return InferredDeps(
        rule_name=inferred.rule_name,
        output=inferred.output,
        inputs=inferred.inputs,
        required_columns=inferred.required_columns,
        plan_fingerprint=inferred.plan_fingerprint,
        declared_inputs=declared,
        inputs_match=inputs_match,
        extra_inferred=extra_inferred,
        missing_declared=missing_declared,
    )


def summarize_inferred_deps(
    deps: Sequence[InferredDeps],
) -> InferredDepsComparison:
    """Summarize comparison results across multiple rules.

    Parameters
    ----------
    deps : Sequence[InferredDeps]
        Inferred dependencies for multiple rules.

    Returns
    -------
    InferredDepsComparison
        Summary statistics and mismatches.
    """
    total = len(deps)
    matched = sum(1 for d in deps if d.inputs_match)
    mismatched = total - matched
    extra_total = sum(len(d.extra_inferred) for d in deps)
    missing_total = sum(len(d.missing_declared) for d in deps)
    mismatches = tuple(d for d in deps if not d.inputs_match)

    return InferredDepsComparison(
        total_rules=total,
        matched_rules=matched,
        mismatched_rules=mismatched,
        extra_inferred_total=extra_total,
        missing_declared_total=missing_total,
        mismatches=mismatches,
    )


def log_inferred_deps_comparison(
    comparison: InferredDepsComparison,
    *,
    logger: logging.Logger | None = None,
    level: int = logging.WARNING,
) -> None:
    """Log comparison results for observability.

    Parameters
    ----------
    comparison : InferredDepsComparison
        Comparison summary to log.
    logger : logging.Logger | None
        Logger to use. Defaults to module logger.
    level : int
        Log level for mismatches.
    """
    log = logger or _LOG

    if comparison.mismatched_rules == 0:
        log.info(
            "Dependency inference: %d/%d rules match declared inputs",
            comparison.matched_rules,
            comparison.total_rules,
        )
        return

    log.log(
        level,
        "Dependency inference: %d/%d rules have mismatches "
        "(extra_inferred=%d, missing_declared=%d)",
        comparison.mismatched_rules,
        comparison.total_rules,
        comparison.extra_inferred_total,
        comparison.missing_declared_total,
    )

    for mismatch in comparison.mismatches:
        log.log(
            level,
            "  Rule %r: extra=%s, missing=%s",
            mismatch.rule_name,
            list(mismatch.extra_inferred),
            list(mismatch.missing_declared),
        )


__all__ = [
    "InferredDeps",
    "InferredDepsComparison",
    "InferredDepsRequest",
    "compare_deps",
    "infer_deps_from_ibis_plan",
    "infer_deps_from_sqlglot_expr",
    "log_inferred_deps_comparison",
    "summarize_inferred_deps",
]
