"""Engine-native observability artifact structs.

Provide structured artifact types for engine plan summaries and
execution summaries, replacing Hamilton-era diagnostics artifacts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from engine.spec_builder import SemanticExecutionSpec


class EnginePlanSummaryArtifact(StructBaseStrict, frozen=True):
    """Plan summary artifact for the engine execution spec."""

    spec_hash: str
    view_count: int
    join_edge_count: int
    rule_intent_count: int
    rulepack_profile: str
    input_relation_count: int
    output_target_count: int


class EngineExecutionSummaryArtifact(StructBaseStrict, frozen=True):
    """Execution summary artifact from RunResult."""

    spec_hash: str
    envelope_hash: str
    output_rows: int
    output_batches: int
    elapsed_compute_nanos: int
    spill_file_count: int
    spilled_bytes: int
    operator_count: int
    tables_materialized: int
    total_rows_written: int
    warning_count: int
    warning_codes: tuple[str, ...]


def record_engine_plan_summary(spec: SemanticExecutionSpec) -> EnginePlanSummaryArtifact:
    """Build a plan summary artifact from a SemanticExecutionSpec.

    Parameters
    ----------
    spec
        The compiled semantic execution spec.

    Returns:
    -------
    EnginePlanSummaryArtifact
        Summary suitable for diagnostics recording.
    """
    join_edge_count = len(spec.join_graph.edges)
    if join_edge_count == 0 and spec.view_definitions:
        # Backward-compatible fallback for specs that don't populate join_graph edges.
        join_edge_count = sum(
            1
            for view_def in spec.view_definitions
            if view_def.view_kind == "relate"
            or view_def.transform.__class__.__name__ == "RelateTransform"
        )

    return EnginePlanSummaryArtifact(
        spec_hash=spec.spec_hash.hex() if spec.spec_hash else "",
        view_count=len(spec.view_definitions) if spec.view_definitions else 0,
        join_edge_count=join_edge_count,
        rule_intent_count=len(spec.rule_intents) if spec.rule_intents else 0,
        rulepack_profile=spec.rulepack_profile,
        input_relation_count=len(spec.input_relations) if spec.input_relations else 0,
        output_target_count=len(spec.output_targets) if spec.output_targets else 0,
    )


def _extract_warning_codes(run_result: dict[str, object]) -> tuple[str, ...]:
    """Extract warning codes from RunResult warnings list.

    Parameters
    ----------
    run_result
        The raw RunResult envelope.

    Returns:
    -------
    tuple[str, ...]
        Warning codes extracted from the warnings.
    """
    warnings = run_result.get("warnings", ())
    if not isinstance(warnings, (list, tuple)):
        return ()
    return tuple(
        str(w.get("code", "unknown") if isinstance(w, dict) else "unknown") for w in warnings
    )


def _extract_output_stats(run_result: dict[str, object]) -> tuple[int, int]:
    """Extract output table statistics from RunResult outputs.

    Parameters
    ----------
    run_result
        The raw RunResult envelope.

    Returns:
    -------
    tuple[int, int]
        Tables materialized and total rows written.
    """
    outputs = run_result.get("outputs", ())
    if not isinstance(outputs, (list, tuple)):
        return 0, 0

    tables_materialized = len(outputs)
    total_rows_written = 0
    for output in outputs:
        if isinstance(output, dict):
            rows = output.get("rows_written", 0)
            if isinstance(rows, (int, float)):
                total_rows_written += int(rows)

    return tables_materialized, total_rows_written


def _extract_trace_metrics(
    run_result: dict[str, object],
) -> tuple[int, int, int, int, int, int]:
    """Extract metrics from RunResult trace_metrics_summary.

    Parameters
    ----------
    run_result
        The raw RunResult envelope.

    Returns:
    -------
    tuple[int, int, int, int, int, int]
        Output rows, batches, elapsed compute nanos, spill file count,
        spilled bytes, operator count.
    """
    trace_summary = run_result.get("trace_metrics_summary")
    if not isinstance(trace_summary, dict):
        return 0, 0, 0, 0, 0, 0

    return (
        int(trace_summary.get("output_rows", 0)),
        int(trace_summary.get("output_batches", 0)),
        int(trace_summary.get("elapsed_compute_nanos", 0)),
        int(trace_summary.get("spill_file_count", 0)),
        int(trace_summary.get("spilled_bytes", 0)),
        int(trace_summary.get("operator_count", 0)),
    )


def _bytes_to_hex(run_result: dict[str, object], key: str) -> str:
    """Convert bytes field to hex string.

    Parameters
    ----------
    run_result
        The raw RunResult envelope.
    key
        Field name to extract.

    Returns:
    -------
    str
        Hex-encoded bytes or empty string.
    """
    val = run_result.get(key)
    if isinstance(val, bytes):
        return val.hex()
    if isinstance(val, (list, tuple)) and all(isinstance(x, int) for x in val):
        return bytes(val).hex()
    return ""


def record_engine_execution_summary(
    run_result: dict[str, object],
) -> EngineExecutionSummaryArtifact:
    """Build an execution summary artifact from a RunResult dict.

    Parameters
    ----------
    run_result
        The raw RunResult envelope from the Rust engine.

    Returns:
    -------
    EngineExecutionSummaryArtifact
        Execution summary suitable for diagnostics recording.
    """
    warning_codes = _extract_warning_codes(run_result)
    tables_materialized, total_rows_written = _extract_output_stats(run_result)
    (
        output_rows,
        output_batches,
        elapsed_compute_nanos,
        spill_file_count,
        spilled_bytes,
        operator_count,
    ) = _extract_trace_metrics(run_result)

    return EngineExecutionSummaryArtifact(
        spec_hash=_bytes_to_hex(run_result, "spec_hash"),
        envelope_hash=_bytes_to_hex(run_result, "envelope_hash"),
        output_rows=output_rows,
        output_batches=output_batches,
        elapsed_compute_nanos=elapsed_compute_nanos,
        spill_file_count=spill_file_count,
        spilled_bytes=spilled_bytes,
        operator_count=operator_count,
        tables_materialized=tables_materialized,
        total_rows_written=total_rows_written,
        warning_count=len(warning_codes),
        warning_codes=warning_codes,
    )


__all__ = [
    "EngineExecutionSummaryArtifact",
    "EnginePlanSummaryArtifact",
    "record_engine_execution_summary",
    "record_engine_plan_summary",
]
