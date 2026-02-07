"""Coordination layer for extract execution context and materialization.

This subpackage provides:
- context.py: FileContext, RepoFileRow, SpanSpec and file context utilities
- materialization.py: Extract plan building and materialization functions
- evidence_plan.py: Evidence requirement planning
- spec_helpers.py: Extractor option handling
- schema_ops.py: Schema normalization and policy
"""

from __future__ import annotations

from extract.coordination.context import (
    ExtractExecutionContext,
    FileContext,
    RepoFileRow,
    SpanSpec,
    attrs_map,
    span_dict,
)
from extract.coordination.evidence_plan import (
    EvidencePlan,
    EvidenceRequirement,
    compile_evidence_plan,
)
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_row_batches,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import (
    ExtractNormalizeOptions,
    apply_pipeline_kernels,
    finalize_context_for_dataset,
    metadata_spec_for_dataset,
    metadata_specs_for_datasets,
    normalized_schema_policy_for_dataset,
    schema_policy_for_dataset,
    validate_extract_output,
)
from extract.coordination.spec_helpers import (
    ExtractExecutionOptions,
    extractor_option_values,
    plan_feature_flags,
    plan_requires_row,
    rule_execution_options,
)

__all__ = [
    # evidence_plan
    "EvidencePlan",
    "EvidenceRequirement",
    # context
    "ExtractExecutionContext",
    # spec_helpers
    "ExtractExecutionOptions",
    # materialization
    "ExtractMaterializeOptions",
    "ExtractNormalizeOptions",
    "ExtractPlanOptions",
    "FileContext",
    "RepoFileRow",
    "SpanSpec",
    # schema_ops
    "apply_pipeline_kernels",
    "attrs_map",
    "compile_evidence_plan",
    "extract_plan_from_row_batches",
    "extract_plan_from_rows",
    "extractor_option_values",
    "finalize_context_for_dataset",
    "materialize_extract_plan",
    "metadata_spec_for_dataset",
    "metadata_specs_for_datasets",
    "normalized_schema_policy_for_dataset",
    "plan_feature_flags",
    "plan_requires_row",
    "rule_execution_options",
    "schema_policy_for_dataset",
    "span_dict",
    "validate_extract_output",
]
