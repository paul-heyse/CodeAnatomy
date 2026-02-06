"""Shared extractor helpers and registries.

This module provides backward-compatible re-exports from the split context
and materialization modules, plus remaining utility functions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.arrow.interop import ScalarLike, TableLike
from datafusion_engine.extract.extractors import (
    ExtractorSpec,
    extractor_specs,
    outputs_for_template,
    select_extractors_for_outputs,
)
from datafusion_engine.plan.bundle import DataFusionPlanBundle
from extract.coordination.context import (
    ExtractExecutionContext,
    FileContext,
    RepoFileRow,
    SpanSpec,
    attrs_map,
    byte_span_dict,
    bytes_from_file_ctx,
    file_identity_row,
    iter_contexts,
    iter_file_contexts,
    pos_dict,
    span_dict,
    text_from_file_ctx,
)
from extract.coordination.evidence_plan import EvidencePlan
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    apply_query_and_project,
    datafusion_plan_from_reader,
    extract_dataset_location_or_raise,
    extract_plan_from_reader,
    extract_plan_from_row_batches,
    extract_plan_from_rows,
    materialize_extract_plan,
    materialize_extract_reader,
    raw_plan_from_rows,
    record_batch_reader_from_row_batches,
    record_batch_reader_from_rows,
)

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import SessionRuntime


def ast_def_nodes(nodes: TableLike) -> TableLike:
    """Return AST node rows that represent definitions.

    Returns:
    -------
    TableLike
        Table filtered to function/class definitions.
    """
    if nodes.num_rows == 0:
        return nodes
    allowed = {"FunctionDef", "AsyncFunctionDef", "ClassDef"}
    values = list(nodes["kind"])

    def _as_text(item: object | None) -> str | None:
        if item is None:
            return None
        if hasattr(item, "as_py"):
            return cast("str | None", cast("ScalarLike", item).as_py())
        return str(item)

    mask = pa.array([_as_text(item) in allowed for item in values])
    return nodes.filter(mask)


def requires_evidence(plan: EvidencePlan | None, name: str) -> bool:
    """Return whether an evidence plan requires a dataset.

    Returns:
    -------
    bool
        ``True`` when the dataset is required.
    """
    if plan is None:
        return True
    return plan.requires_dataset(name)


def requires_evidence_template(plan: EvidencePlan | None, template: str) -> bool:
    """Return whether an evidence plan requires a template.

    Returns:
    -------
    bool
        ``True`` when the template is required.
    """
    if plan is None:
        return True
    return plan.requires_template(template)


def required_extractors(plan: EvidencePlan | None) -> tuple[ExtractorSpec, ...]:
    """Return extractor specs required by an evidence plan.

    Returns:
    -------
    tuple[ExtractorSpec, ...]
        Extractor specs needed for the plan.
    """
    if plan is None:
        return extractor_specs()
    return select_extractors_for_outputs(plan.sources)


def template_outputs(plan: EvidencePlan | None, template: str) -> tuple[str, ...]:
    """Return output aliases for a template given an evidence plan.

    Returns:
    -------
    tuple[str, ...]
        Output aliases for the template, or empty when not required.
    """
    if plan is None:
        return outputs_for_template(template)
    if not plan.requires_template(template):
        return ()
    return outputs_for_template(template)


def ast_def_nodes_plan(
    plan: DataFusionPlanBundle,
    *,
    session_runtime: SessionRuntime,
) -> DataFusionPlanBundle:
    """Return a plan bundle filtered to AST definition nodes.

    Returns:
    -------
    DataFusionPlanBundle
        Plan bundle filtered to function/class definitions.
    """
    from datafusion_engine.plan.bundle import PlanBundleOptions, build_plan_bundle

    values = [
        "FunctionDef",
        "AsyncFunctionDef",
        "ClassDef",
    ]
    filtered = plan.df.filter(f.in_list(col("kind"), [lit(value) for value in values]))
    return build_plan_bundle(
        session_runtime.ctx,
        filtered,
        options=PlanBundleOptions(
            validate_udfs=True,
            session_runtime=session_runtime,
        ),
    )


# Re-export all symbols for backward compatibility
__all__ = [
    # From context
    "ExtractExecutionContext",
    # From materialization
    "ExtractMaterializeOptions",
    "ExtractPlanOptions",
    "FileContext",
    "RepoFileRow",
    "SpanSpec",
    "apply_query_and_project",
    # Local utilities
    "ast_def_nodes",
    "ast_def_nodes_plan",
    "attrs_map",
    "byte_span_dict",
    "bytes_from_file_ctx",
    "datafusion_plan_from_reader",
    "extract_dataset_location_or_raise",
    "extract_plan_from_reader",
    "extract_plan_from_row_batches",
    "extract_plan_from_rows",
    "file_identity_row",
    "iter_contexts",
    "iter_file_contexts",
    "materialize_extract_plan",
    "materialize_extract_reader",
    "pos_dict",
    "raw_plan_from_rows",
    "record_batch_reader_from_row_batches",
    "record_batch_reader_from_rows",
    "required_extractors",
    "requires_evidence",
    "requires_evidence_template",
    "span_dict",
    "template_outputs",
    "text_from_file_ctx",
]
