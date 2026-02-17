"""Extract plan building and evidence projection helpers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

import msgspec
import pyarrow as pa
from datafusion.dataframe import DataFrame

from datafusion_engine.arrow.build import (
    record_batch_reader_from_row_batches as schema_record_batch_reader_from_row_batches,
)
from datafusion_engine.arrow.build import (
    record_batch_reader_from_rows as schema_record_batch_reader_from_rows,
)
from datafusion_engine.arrow.interop import RecordBatchReaderLike
from datafusion_engine.expr.query_spec import apply_query_spec
from datafusion_engine.extract.registry import dataset_query, dataset_schema, extract_metadata
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.plan.bundle_artifact import (
    DataFusionPlanArtifact,
    PlanBundleOptions,
    build_plan_artifact,
)
from extract.coordination.evidence_plan import EvidencePlan
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.coordination.spec_helpers import (
    ExtractExecutionOptions,
    plan_requires_row,
    rule_execution_options,
)
from extract.session import ExtractSession
from serde_msgspec import to_builtins


@dataclass(frozen=True)
class ExtractPlanOptions:
    """Options for building extract plans."""

    normalize: ExtractNormalizeOptions | None = None
    evidence_plan: EvidencePlan | None = None
    repo_id: str | None = None

    def resolved_repo_id(self) -> str | None:
        """Return the effective repo id for query construction."""
        if self.repo_id is not None:
            return self.repo_id
        if self.normalize is None:
            return None
        return self.normalize.repo_id


@dataclass(frozen=True)
class _ExtractProjectionRequest:
    """Inputs required to apply query and evidence projection."""

    name: str
    table: DataFrame
    session: ExtractSession
    normalize: ExtractNormalizeOptions | None = None
    evidence_plan: EvidencePlan | None = None
    repo_id: str | None = None


def _build_plan_artifact_from_df(
    df: DataFrame,
    *,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    return build_plan_artifact(
        session.session_runtime.ctx,
        df,
        options=PlanBundleOptions(
            validate_udfs=True,
            session_runtime=session.session_runtime,
        ),
    )


def _empty_plan_from_table(
    table: DataFrame,
    *,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    return _build_plan_artifact_from_df(table.limit(0), session=session)


def record_batch_reader_from_row_batches(
    name: str,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
) -> pa.RecordBatchReader:
    """Return a RecordBatchReader aligned to the dataset schema."""
    schema = dataset_schema(name)
    return schema_record_batch_reader_from_row_batches(schema, row_batches)


def record_batch_reader_from_rows(
    name: str,
    rows: Iterable[Mapping[str, object]],
) -> pa.RecordBatchReader:
    """Return a RecordBatchReader aligned to the dataset schema."""
    schema = dataset_schema(name)
    return schema_record_batch_reader_from_rows(schema, rows)


def datafusion_plan_from_reader(
    name: str,
    reader: RecordBatchReaderLike,
    *,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    """Return a DataFusion plan bundle for a RecordBatchReader."""
    df = datafusion_from_arrow(session.session_runtime.ctx, name=name, value=reader)
    return _build_plan_artifact_from_df(df, session=session)


def extract_plan_from_reader(
    name: str,
    reader: RecordBatchReaderLike,
    *,
    session: ExtractSession,
    options: ExtractPlanOptions | None = None,
) -> DataFusionPlanArtifact:
    """Return an extract plan bundle for a RecordBatchReader."""
    resolved = options or ExtractPlanOptions()
    raw_plan = datafusion_plan_from_reader(name, reader, session=session)
    return apply_query_and_project(
        _ExtractProjectionRequest(
            name=name,
            table=raw_plan.df,
            session=session,
            normalize=resolved.normalize,
            evidence_plan=resolved.evidence_plan,
            repo_id=resolved.resolved_repo_id(),
        )
    )


def raw_plan_from_rows(
    name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    """Return a raw plan bundle for a row iterator."""
    reader = record_batch_reader_from_rows(name, rows)
    return datafusion_plan_from_reader(name, reader, session=session)


def extract_plan_from_rows(
    name: str,
    rows: Iterable[Mapping[str, object]],
    *,
    session: ExtractSession,
    options: ExtractPlanOptions | None = None,
) -> DataFusionPlanArtifact:
    """Return an extract plan bundle for a row iterator."""
    reader = record_batch_reader_from_rows(name, rows)
    return extract_plan_from_reader(
        name,
        reader,
        session=session,
        options=options,
    )


def extract_plan_from_row_batches(
    name: str,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
    *,
    session: ExtractSession,
    options: ExtractPlanOptions | None = None,
) -> DataFusionPlanArtifact:
    """Return an extract plan bundle for row batches."""
    reader = record_batch_reader_from_row_batches(name, row_batches)
    return extract_plan_from_reader(
        name,
        reader,
        session=session,
        options=options,
    )


def build_plan_from_row_batches(
    name: str,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
    *,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    """Build a raw DataFusion plan artifact from row-batch iterables.

    Returns:
        DataFusionPlanArtifact: Raw plan artifact built from row batches.
    """
    reader = record_batch_reader_from_row_batches(name, row_batches)
    return datafusion_plan_from_reader(name, reader, session=session)


def _options_overrides(options: object | None) -> Mapping[str, object]:
    if options is None:
        return {}
    if isinstance(options, Mapping):
        return dict(options)
    try:
        builtins = to_builtins(options)
    except (TypeError, msgspec.EncodeError):
        return {}
    if isinstance(builtins, Mapping):
        return dict(builtins)
    return {}


def _stage_enabled(condition: str, execution: ExtractExecutionOptions) -> bool:
    if not condition:
        return True
    if condition == "allowlist":
        return bool(execution.module_allowlist)
    value = execution.as_mapping().get(condition)
    if isinstance(value, bool):
        return value
    return bool(value)


def apply_query_and_project(request: _ExtractProjectionRequest) -> DataFusionPlanArtifact:
    """Apply registry query and evidence projection to a DataFusion table.

    Returns:
        DataFusionPlanArtifact: Projected and query-filtered plan artifact.
    """
    row = extract_metadata(request.name)
    if request.evidence_plan is not None and not plan_requires_row(request.evidence_plan, row):
        return _empty_plan_from_table(request.table, session=request.session)
    overrides = _options_overrides(request.normalize.options if request.normalize else None)
    execution = rule_execution_options(
        row.template or request.name,
        request.evidence_plan,
        overrides=overrides,
    )
    if row.enabled_when is not None and not _stage_enabled(row.enabled_when, execution):
        return _empty_plan_from_table(request.table, session=request.session)
    projection: tuple[str, ...] = ()
    if request.evidence_plan is not None:
        required = set(request.evidence_plan.required_columns_for(request.name))
        required.update(row.join_keys)
        required.update(spec.name for spec in row.derived)
        if row.evidence_required_columns:
            required.update(row.evidence_required_columns)
        if required:
            schema = dataset_schema(request.name)
            projection = tuple(field.name for field in schema if field.name in required)
    spec = dataset_query(
        request.name,
        repo_id=request.repo_id,
        projection=projection if projection else None,
    )
    df = apply_query_spec(request.table, spec=spec)
    return _build_plan_artifact_from_df(df, session=request.session)


__all__ = [
    "ExtractPlanOptions",
    "apply_query_and_project",
    "build_plan_from_row_batches",
    "datafusion_plan_from_reader",
    "extract_plan_from_reader",
    "extract_plan_from_row_batches",
    "extract_plan_from_rows",
    "raw_plan_from_rows",
    "record_batch_reader_from_row_batches",
    "record_batch_reader_from_rows",
]
