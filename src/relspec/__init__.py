"""Inference-first relationship specification helpers."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "CalibrationBounds": ("relspec.calibration_bounds", "CalibrationBounds"),
    "DEFAULT_CALIBRATION_BOUNDS": ("relspec.calibration_bounds", "DEFAULT_CALIBRATION_BOUNDS"),
    "validate_calibration_bounds": ("relspec.calibration_bounds", "validate_calibration_bounds"),
    "CompiledExecutionPolicy": ("relspec.compiled_policy", "CompiledExecutionPolicy"),
    "DecisionOutcome": ("relspec.decision_provenance", "DecisionOutcome"),
    "DecisionProvenanceGraph": ("relspec.decision_provenance", "DecisionProvenanceGraph"),
    "DecisionRecord": ("relspec.decision_provenance", "DecisionRecord"),
    "EvidenceRecord": ("relspec.decision_provenance", "EvidenceRecord"),
    "RelspecError": ("relspec.errors", "RelspecError"),
    "RelspecExecutionAuthorityError": ("relspec.errors", "RelspecExecutionAuthorityError"),
    "RelspecValidationError": ("relspec.errors", "RelspecValidationError"),
    "ExecutionPackageArtifact": ("relspec.execution_package", "ExecutionPackageArtifact"),
    "build_execution_package": ("relspec.execution_package", "build_execution_package"),
    "InferenceConfidence": ("relspec.inference_confidence", "InferenceConfidence"),
    "high_confidence": ("relspec.inference_confidence", "high_confidence"),
    "low_confidence": ("relspec.inference_confidence", "low_confidence"),
    "InferredDeps": ("relspec.inferred_deps", "InferredDeps"),
    "InferredDepsInputs": ("relspec.inferred_deps", "InferredDepsInputs"),
    "infer_deps_from_plan_bundle": ("relspec.inferred_deps", "infer_deps_from_plan_bundle"),
    "infer_deps_from_view_nodes": ("relspec.inferred_deps", "infer_deps_from_view_nodes"),
    "DiagnosticsPolicy": ("relspec.pipeline_policy", "DiagnosticsPolicy"),
    "PipelinePolicy": ("relspec.pipeline_policy", "PipelinePolicy"),
    "PolicyCalibrationResult": ("relspec.policy_calibrator", "PolicyCalibrationResult"),
    "CalibrationThresholds": ("relspec.policy_calibrator", "CalibrationThresholds"),
    "ExecutionMetricsSummary": ("relspec.policy_calibrator", "ExecutionMetricsSummary"),
    "calibrate_from_execution_metrics": (
        "relspec.policy_calibrator",
        "calibrate_from_execution_metrics",
    ),
    "TableSizeTier": ("relspec.table_size_tiers", "TableSizeTier"),
    "classify_table_size": ("relspec.table_size_tiers", "classify_table_size"),
    "RELATION_OUTPUT_ORDERING_KEYS": ("relspec.contracts", "RELATION_OUTPUT_ORDERING_KEYS"),
    "RELATION_OUTPUT_NAME": ("relspec.view_defs", "RELATION_OUTPUT_NAME"),
}

if TYPE_CHECKING:
    CalibrationBounds: Any
    DEFAULT_CALIBRATION_BOUNDS: Any
    validate_calibration_bounds: Any
    CompiledExecutionPolicy: Any
    DecisionOutcome: Any
    DecisionProvenanceGraph: Any
    DecisionRecord: Any
    EvidenceRecord: Any
    RelspecError: Any
    RelspecExecutionAuthorityError: Any
    RelspecValidationError: Any
    ExecutionPackageArtifact: Any
    build_execution_package: Any
    InferenceConfidence: Any
    high_confidence: Any
    low_confidence: Any
    InferredDeps: Any
    InferredDepsInputs: Any
    infer_deps_from_plan_bundle: Any
    infer_deps_from_view_nodes: Any
    DiagnosticsPolicy: Any
    PipelinePolicy: Any
    PolicyCalibrationResult: Any
    CalibrationThresholds: Any
    ExecutionMetricsSummary: Any
    calibrate_from_execution_metrics: Any
    TableSizeTier: Any
    classify_table_size: Any
    RELATION_OUTPUT_ORDERING_KEYS: Any
    RELATION_OUTPUT_NAME: Any


def __getattr__(name: str) -> object:
    target = _EXPORT_MAP.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_EXPORT_MAP))


__all__ = tuple(_EXPORT_MAP)
