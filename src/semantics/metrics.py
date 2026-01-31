"""Metrics collection for semantic pipeline operations.

This module provides dataclasses and utilities for collecting metrics from
semantic pipeline operations. Metrics include operation timing, row counts,
and plan fingerprints for caching.

Usage
-----
>>> from semantics.metrics import PipelineMetrics, collect_dataframe_metrics
>>>
>>> metrics = PipelineMetrics()
>>> op_metrics = collect_dataframe_metrics(
...     df,
...     operation="normalize",
...     input_table="cst_refs",
...     output_name="cst_refs_norm_v1",
... )
>>> metrics.add_operation(op_metrics)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import DataFrame


@dataclass
class SemanticOperationMetrics:
    """Metrics for a semantic operation.

    Captures information about a single semantic pipeline operation such as
    normalize, relate, or union. Row counts are optional since they require
    execution to compute.

    Attributes
    ----------
    operation
        Name of the operation (normalize, relate, union, aggregate, dedupe).
    input_table
        Primary input table name.
    output_name
        Output view name.
    input_row_count
        Row count of input (optional, requires execution).
    output_row_count
        Row count of output (optional, requires execution).
    elapsed_ms
        Operation duration in milliseconds.
    plan_fingerprint
        Hash of the logical plan for caching.
    """

    operation: str
    input_table: str
    output_name: str
    input_row_count: int | None = None
    output_row_count: int | None = None
    elapsed_ms: float | None = None
    plan_fingerprint: str | None = None

    def with_row_counts(
        self,
        *,
        input_count: int | None = None,
        output_count: int | None = None,
    ) -> SemanticOperationMetrics:
        """Return a copy with row counts populated.

        Parameters
        ----------
        input_count
            Input row count (from execution).
        output_count
            Output row count (from execution).

        Returns
        -------
        SemanticOperationMetrics
            New metrics instance with row counts.
        """
        return SemanticOperationMetrics(
            operation=self.operation,
            input_table=self.input_table,
            output_name=self.output_name,
            input_row_count=input_count if input_count is not None else self.input_row_count,
            output_row_count=output_count if output_count is not None else self.output_row_count,
            elapsed_ms=self.elapsed_ms,
            plan_fingerprint=self.plan_fingerprint,
        )

    def with_elapsed(self, elapsed_ms: float) -> SemanticOperationMetrics:
        """Return a copy with elapsed time populated.

        Parameters
        ----------
        elapsed_ms
            Elapsed time in milliseconds.

        Returns
        -------
        SemanticOperationMetrics
            New metrics instance with elapsed time.
        """
        return SemanticOperationMetrics(
            operation=self.operation,
            input_table=self.input_table,
            output_name=self.output_name,
            input_row_count=self.input_row_count,
            output_row_count=self.output_row_count,
            elapsed_ms=elapsed_ms,
            plan_fingerprint=self.plan_fingerprint,
        )

    def with_plan_fingerprint(self, fingerprint: str) -> SemanticOperationMetrics:
        """Return a copy with plan fingerprint populated.

        Parameters
        ----------
        fingerprint
            Plan fingerprint hash.

        Returns
        -------
        SemanticOperationMetrics
            New metrics instance with plan fingerprint.
        """
        return SemanticOperationMetrics(
            operation=self.operation,
            input_table=self.input_table,
            output_name=self.output_name,
            input_row_count=self.input_row_count,
            output_row_count=self.output_row_count,
            elapsed_ms=self.elapsed_ms,
            plan_fingerprint=fingerprint,
        )


@dataclass
class PipelineMetrics:
    """Aggregated metrics for a complete pipeline run.

    Collects metrics from multiple semantic operations and provides
    summary statistics for the entire pipeline execution.

    Attributes
    ----------
    operation_metrics
        List of per-operation metrics.
    total_elapsed_ms
        Total pipeline duration in milliseconds.
    views_created
        Number of views created during the pipeline run.
    """

    operation_metrics: list[SemanticOperationMetrics] = field(default_factory=list)
    total_elapsed_ms: float | None = None
    views_created: int = 0

    def add_operation(self, metrics: SemanticOperationMetrics) -> None:
        """Add operation metrics to the pipeline.

        Parameters
        ----------
        metrics
            Metrics from a single operation.
        """
        self.operation_metrics.append(metrics)
        self.views_created += 1

    def operations_by_type(self, operation: str) -> list[SemanticOperationMetrics]:
        """Filter operations by type.

        Parameters
        ----------
        operation
            Operation type to filter by.

        Returns
        -------
        list[SemanticOperationMetrics]
            Operations matching the specified type.
        """
        return [m for m in self.operation_metrics if m.operation == operation]

    def total_input_rows(self) -> int | None:
        """Sum of all input row counts.

        Returns
        -------
        int | None
            Total input rows, or None if any operation lacks row counts.
        """
        counts = [m.input_row_count for m in self.operation_metrics]
        if any(c is None for c in counts):
            return None
        return sum(c for c in counts if c is not None)

    def total_output_rows(self) -> int | None:
        """Sum of all output row counts.

        Returns
        -------
        int | None
            Total output rows, or None if any operation lacks row counts.
        """
        counts = [m.output_row_count for m in self.operation_metrics]
        if any(c is None for c in counts):
            return None
        return sum(c for c in counts if c is not None)

    def as_dict(self) -> dict[str, object]:
        """Convert to dictionary for serialization.

        Returns
        -------
        dict[str, object]
            Dictionary representation of pipeline metrics.
        """
        return {
            "views_created": self.views_created,
            "total_elapsed_ms": self.total_elapsed_ms,
            "total_input_rows": self.total_input_rows(),
            "total_output_rows": self.total_output_rows(),
            "operations": [
                {
                    "operation": m.operation,
                    "input_table": m.input_table,
                    "output_name": m.output_name,
                    "input_row_count": m.input_row_count,
                    "output_row_count": m.output_row_count,
                    "elapsed_ms": m.elapsed_ms,
                    "plan_fingerprint": m.plan_fingerprint,
                }
                for m in self.operation_metrics
            ],
        }


def collect_dataframe_metrics(
    df: DataFrame,
    *,
    operation: str,
    input_table: str,
    output_name: str,
) -> SemanticOperationMetrics:
    """Collect metrics from a DataFrame operation.

    Create initial metrics for a semantic operation. Row counts are not
    computed since they require execution. Use ``with_row_counts()`` after
    executing the DataFrame to populate row counts.

    Parameters
    ----------
    df
        The DataFrame to collect metrics from.
    operation
        Operation name (normalize, relate, union, aggregate, dedupe).
    input_table
        Primary input table name.
    output_name
        Output view name.

    Returns
    -------
    SemanticOperationMetrics
        Metrics without row counts (add them after execution if needed).

    Examples
    --------
    >>> metrics = collect_dataframe_metrics(
    ...     df,
    ...     operation="normalize",
    ...     input_table="cst_refs",
    ...     output_name="cst_refs_norm_v1",
    ... )
    >>> # After execution:
    >>> metrics = metrics.with_row_counts(input_count=1000, output_count=1000)
    """
    # DataFrame is provided for future extension (e.g., plan extraction)
    # Currently we only capture the metadata without executing
    _ = df  # Acknowledge parameter for future use
    return SemanticOperationMetrics(
        operation=operation,
        input_table=input_table,
        output_name=output_name,
    )


__all__ = [
    "PipelineMetrics",
    "SemanticOperationMetrics",
    "collect_dataframe_metrics",
]
