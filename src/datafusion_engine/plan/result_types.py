"""Result types for DataFusion plan execution.

This module contains the shared result types used across the plan execution
and session facade layers. By extracting these types into a dedicated module,
we break the circular dependency between facade.py and execution.py while
enabling clean unidirectional dependency flow.

Dependency hierarchy:
    result_types.py (pure types, minimal deps)
         ↑
    execution.py (plan execution helpers)
         ↑
    facade.py (high-level orchestration)
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import DataFrame

    from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
    from datafusion_engine.io.write import WriteResult
    from datafusion_engine.lineage.scan import ScanUnit
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import ManifestDatasetResolver
    from serde_artifacts import PlanArtifacts


# ---------------------------------------------------------------------------
# Execution Result Types (from facade.py)
# ---------------------------------------------------------------------------


class ExecutionResultKind(StrEnum):
    """Execution result kind discriminator."""

    DATAFRAME = "dataframe"
    TABLE = "table"
    READER = "reader"
    WRITE = "write"


@dataclass(frozen=True)
class ExecutionResult:
    """Unified execution result wrapper."""

    kind: ExecutionResultKind
    dataframe: DataFrame | None = None
    table: TableLike | None = None
    reader: RecordBatchReaderLike | None = None
    write_result: WriteResult | None = None
    plan_bundle: DataFusionPlanBundle | None = None

    @staticmethod
    def from_dataframe(
        df: DataFrame,
        *,
        plan_bundle: DataFusionPlanBundle | None = None,
    ) -> ExecutionResult:
        """Wrap a DataFusion DataFrame.

        Parameters
        ----------
        df
            DataFusion DataFrame to wrap.
        plan_bundle
            Optional plan bundle for lineage tracking.

        Returns:
        -------
        ExecutionResult
            Wrapped execution result.
        """
        return ExecutionResult(
            kind=ExecutionResultKind.DATAFRAME,
            dataframe=df,
            plan_bundle=plan_bundle,
        )

    @staticmethod
    def from_table(table: TableLike) -> ExecutionResult:
        """Wrap a materialized table-like object.

        Parameters
        ----------
        table
            Materialized table-like object.

        Returns:
        -------
        ExecutionResult
            Wrapped execution result.
        """
        return ExecutionResult(kind=ExecutionResultKind.TABLE, table=table)

    @staticmethod
    def from_reader(reader: RecordBatchReaderLike) -> ExecutionResult:
        """Wrap a record batch reader.

        Parameters
        ----------
        reader
            Record batch reader to wrap.

        Returns:
        -------
        ExecutionResult
            Wrapped execution result.
        """
        return ExecutionResult(kind=ExecutionResultKind.READER, reader=reader)

    @staticmethod
    def from_write(result: WriteResult) -> ExecutionResult:
        """Wrap a write result.

        Parameters
        ----------
        result
            Write result to wrap.

        Returns:
        -------
        ExecutionResult
            Wrapped execution result.
        """
        return ExecutionResult(kind=ExecutionResultKind.WRITE, write_result=result)

    def require_dataframe(self) -> DataFrame:
        """Return the DataFrame result or raise when missing.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.dataframe is None:
            msg = f"Execution result is not a dataframe: {self.kind}."
            raise ValueError(msg)
        return self.dataframe

    def require_table(self) -> TableLike:
        """Return the materialized table result or raise when missing.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.table is None:
            msg = f"Execution result is not a table: {self.kind}."
            raise ValueError(msg)
        return self.table

    def require_reader(self) -> RecordBatchReaderLike:
        """Return the record batch reader or raise when missing.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.reader is None:
            msg = f"Execution result is not a reader: {self.kind}."
            raise ValueError(msg)
        return self.reader

    def require_write(self) -> WriteResult:
        """Return the write result or raise when missing.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.write_result is None:
            msg = f"Execution result is not a write result: {self.kind}."
            raise ValueError(msg)
        return self.write_result


# ---------------------------------------------------------------------------
# Plan Execution Types (from execution.py)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PlanScanOverrides:
    """Scan override options for plan execution."""

    scan_units: Sequence[ScanUnit] = ()
    scan_keys: Sequence[str] | None = None
    apply_scan_overrides: bool = True


@dataclass(frozen=True)
class PlanEmissionOptions:
    """Emission options for plan execution artifacts and telemetry."""

    emit_artifacts: bool = True
    emit_telemetry: bool = True


@dataclass(frozen=True)
class PlanExecutionOptions:
    """Options that control plan bundle execution."""

    runtime_profile: DataFusionRuntimeProfile | None = None
    view_name: str | None = None
    scan: PlanScanOverrides = field(default_factory=PlanScanOverrides)
    emit: PlanEmissionOptions = field(default_factory=PlanEmissionOptions)
    dataset_resolver: ManifestDatasetResolver | None = None


@dataclass(frozen=True)
class PlanExecutionResult:
    """Result payload for plan-bundle execution."""

    plan_bundle: DataFusionPlanBundle
    execution_result: ExecutionResult
    output: DataFrame | None
    artifacts: PlanArtifacts | None
    telemetry: Mapping[str, float]
    scan_units: tuple[ScanUnit, ...] = ()
    scan_keys: tuple[str, ...] = ()


__all__ = [
    "ExecutionResult",
    "ExecutionResultKind",
    "PlanEmissionOptions",
    "PlanExecutionOptions",
    "PlanExecutionResult",
    "PlanScanOverrides",
]
