"""Result rendering and action pipeline for cq CLI."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from tools.cq.core.artifacts import (
    save_artifact_json,
    save_diagnostics_artifact,
    save_neighborhood_overflow_artifact,
    save_search_artifact_bundle_cache,
)
from tools.cq.core.cache.contracts import SearchArtifactBundleV1
from tools.cq.core.findings_table import (
    FindingsTableOptions,
    apply_filters,
    build_frame,
    flatten_result,
    rehydrate_result,
)
from tools.cq.core.renderers import (
    render_dot,
    render_mermaid_class_diagram,
    render_mermaid_flowchart,
)
from tools.cq.core.report import render_markdown, render_summary
from tools.cq.core.serialization import dumps_json

if TYPE_CHECKING:
    from tools.cq.cli_app.context import CliResult, FilterConfig
    from tools.cq.cli_app.types import OutputFormat
    from tools.cq.core.schema import CqResult


def _attach_insight_artifact_refs(
    result: CqResult,
    *,
    diagnostics_ref: str | None = None,
    telemetry_ref: str | None = None,
    neighborhood_overflow_ref: str | None = None,
) -> None:
    from tools.cq.core.front_door_insight import (
        attach_artifact_refs,
        attach_neighborhood_overflow_ref,
        coerce_front_door_insight,
        to_public_front_door_insight_dict,
    )

    insight = coerce_front_door_insight(result.summary.get("front_door_insight"))
    if insight is None:
        return
    updated = attach_artifact_refs(
        insight,
        diagnostics=diagnostics_ref,
        telemetry=telemetry_ref,
        neighborhood_overflow=neighborhood_overflow_ref,
    )
    if neighborhood_overflow_ref:
        updated = attach_neighborhood_overflow_ref(updated, overflow_ref=neighborhood_overflow_ref)
    result.summary["front_door_insight"] = to_public_front_door_insight_dict(updated)


def apply_result_filters(result: CqResult, filters: FilterConfig) -> CqResult:
    """Apply CLI filter options to a result.

    Parameters
    ----------
    result
        Original analysis result.
    filters
        Filter configuration.

    Returns:
    -------
    CqResult
        Filtered result.
    """
    if not filters.has_filters:
        return result

    records = flatten_result(result)
    if not records:
        return result

    df = build_frame(records)
    options = FindingsTableOptions(
        include=filters.include if filters.include else None,
        exclude=filters.exclude if filters.exclude else None,
        impact=[str(b) for b in filters.impact] if filters.impact else None,
        confidence=[str(b) for b in filters.confidence] if filters.confidence else None,
        severity=[str(s) for s in filters.severity] if filters.severity else None,
        limit=filters.limit,
    )
    filtered_df = apply_filters(df, options)
    return rehydrate_result(result, filtered_df)


def render_result(
    result: CqResult,
    output_format: OutputFormat,
) -> str:
    """Render a result to string in the specified format.

    Parameters
    ----------
    result
        Analysis result.
    output_format
        Output format.

    Returns:
    -------
    str
        Rendered output.
    """
    format_value = str(output_format)

    if format_value == "both":
        md = render_markdown(result)
        js = dumps_json(result, indent=2)
        return f"{md}\n\n---\n\n{js}"

    renderers: dict[str, Callable[[CqResult], str]] = {
        "json": lambda payload: dumps_json(payload, indent=2),
        "md": render_markdown,
        "summary": render_summary,
        "mermaid": render_mermaid_flowchart,
        "mermaid-class": render_mermaid_class_diagram,
        "dot": render_dot,
    }

    # LDMD format uses lazy import (implemented in R6)
    if format_value == "ldmd":
        from tools.cq.ldmd.writer import render_ldmd_from_cq_result

        return render_ldmd_from_cq_result(result)

    renderer = renderers.get(format_value, render_markdown)
    return renderer(result)


def handle_result(cli_result: CliResult, filters: FilterConfig | None = None) -> int:
    """Handle a CLI result using context-based configuration.

    This function uses the output settings from the CliContext to render
    and save results, providing a unified output handling mechanism.

    Parameters
    ----------
    cli_result
        The CLI result containing the result data and context.
    filters
        Optional filter configuration. If not provided, uses filters
        from the CliResult or an empty FilterConfig.

    Returns:
    -------
    int
        Exit code (0 for success).
    """
    from tools.cq.cli_app.context import FilterConfig
    from tools.cq.cli_app.types import OutputFormat
    from tools.cq.search.smart_search import pop_search_object_view_for_run

    non_cq_exit = _handle_non_cq_result(cli_result)
    if non_cq_exit is not None:
        return non_cq_exit

    ctx = cli_result.context
    result = cli_result.result

    # Use context settings or defaults
    output_format = ctx.output_format if ctx.output_format else OutputFormat.md
    artifact_dir = str(ctx.artifact_dir) if ctx.artifact_dir else None
    no_save = not ctx.save_artifact

    filters = _resolve_filters(cli_result, filters, FilterConfig)

    # Apply filters
    result = apply_result_filters(result, filters)
    from tools.cq.core.schema import assign_result_finding_ids

    assign_result_finding_ids(result)

    _handle_artifact_persistence(
        result=result,
        artifact_dir=artifact_dir,
        no_save=no_save,
        pop_search_object_view_for_run=pop_search_object_view_for_run,
    )

    # Render and output
    output = render_result(result, output_format)
    _emit_output(output, output_format=output_format)

    return 0


def _emit_output(output: str, *, output_format: OutputFormat) -> None:
    """Emit rendered output without introducing wrapping artifacts.

    JSON payloads are consumed by downstream automation and tests, so they must
    be written verbatim instead of going through Rich wrapping.
    """
    from tools.cq.cli_app.app import console

    format_value = str(output_format)
    if format_value in {"json", "ldmd", "dot", "mermaid", "mermaid-class"}:
        stream = console.file
        stream.write(output)
        if not output.endswith("\n"):
            stream.write("\n")
        stream.flush()
        return
    console.print(output)


def _handle_non_cq_result(cli_result: CliResult) -> int | None:
    from tools.cq.cli_app.app import console
    from tools.cq.cli_app.context import CliTextResult

    if cli_result.is_cq_result:
        return None
    if isinstance(cli_result.result, CliTextResult):
        if cli_result.result.media_type == "application/json":
            stream = console.file
            stream.write(cli_result.result.text)
            if not cli_result.result.text.endswith("\n"):
                stream.write("\n")
            stream.flush()
        else:
            console.print(cli_result.result.text)
    return cli_result.get_exit_code()


def _resolve_filters(
    cli_result: CliResult,
    filters: FilterConfig | None,
    filter_type: type[FilterConfig],
) -> FilterConfig:
    if filters is not None:
        return filters
    return cli_result.filters if cli_result.filters else filter_type()


def _handle_artifact_persistence(
    *,
    result: CqResult,
    artifact_dir: str | None,
    no_save: bool,
    pop_search_object_view_for_run: Callable[[str], object | None],
) -> None:
    if no_save:
        run_id = result.run.run_id
        if result.run.macro == "search" and run_id is not None:
            _ = pop_search_object_view_for_run(run_id)
        return
    if result.run.macro == "search":
        _save_search_artifacts(
            result,
            pop_search_object_view_for_run=pop_search_object_view_for_run,
        )
        return
    _save_general_artifacts(result, artifact_dir)


def _save_search_artifacts(
    result: CqResult,
    *,
    pop_search_object_view_for_run: Callable[[str], object | None],
) -> None:
    run_id = result.run.run_id
    if run_id is None:
        return
    object_view = pop_search_object_view_for_run(run_id)
    if object_view is None:
        return
    search_artifact = save_search_artifact_bundle_cache(
        result,
        _build_search_artifact_bundle(result, object_view),
    )
    if search_artifact is None:
        return
    result.artifacts.append(search_artifact)
    _attach_insight_artifact_refs(
        result,
        diagnostics_ref=search_artifact.path,
        telemetry_ref=search_artifact.path,
        neighborhood_overflow_ref=None,
    )


def _save_general_artifacts(result: CqResult, artifact_dir: str | None) -> None:
    artifact = save_artifact_json(result, artifact_dir)
    result.artifacts.append(artifact)
    diagnostics_artifact = save_diagnostics_artifact(result, artifact_dir)
    if diagnostics_artifact is not None:
        result.artifacts.append(diagnostics_artifact)
    overflow_artifact = save_neighborhood_overflow_artifact(result, artifact_dir)
    if overflow_artifact is not None:
        result.artifacts.append(overflow_artifact)
    _attach_insight_artifact_refs(
        result,
        diagnostics_ref=diagnostics_artifact.path if diagnostics_artifact is not None else None,
        telemetry_ref=diagnostics_artifact.path if diagnostics_artifact is not None else None,
        neighborhood_overflow_ref=overflow_artifact.path if overflow_artifact is not None else None,
    )


def _build_search_artifact_bundle(
    result: CqResult,
    object_view: object,
) -> SearchArtifactBundleV1:
    import msgspec

    from tools.cq.search.object_resolution_contracts import SearchObjectResolvedViewV1

    resolved_view = msgspec.convert(object_view, type=SearchObjectResolvedViewV1)
    return SearchArtifactBundleV1(
        run_id=result.run.run_id or "no_run_id",
        query=_search_query(result.summary),
        macro=result.run.macro,
        summary=_search_artifact_summary(result.summary),
        object_summaries=list(resolved_view.summaries),
        occurrences=list(resolved_view.occurrences),
        diagnostics=_search_artifact_diagnostics(result.summary),
        snippets=dict(resolved_view.snippets),
        created_ms=(
            result.run.run_created_ms
            if isinstance(result.run.run_created_ms, int | float)
            else result.run.started_ms
        ),
    )


def _search_query(summary: dict[str, object]) -> str:
    raw = summary.get("query")
    if isinstance(raw, str) and raw:
        return raw
    return "<unknown>"


def _search_artifact_summary(summary: dict[str, object]) -> dict[str, object]:
    keys = (
        "query",
        "mode",
        "lang_scope",
        "returned_matches",
        "total_matches",
        "matched_files",
        "scanned_files",
        "resolved_objects",
        "resolved_occurrences",
    )
    payload: dict[str, object] = {}
    for key in keys:
        value = summary.get(key)
        if value is not None:
            payload[key] = value
    return payload


def _search_artifact_diagnostics(summary: dict[str, object]) -> dict[str, object]:
    keys = (
        "enrichment_telemetry",
        "python_semantic_overview",
        "python_semantic_telemetry",
        "python_semantic_diagnostics",
        "rust_semantic_telemetry",
        "cross_language_diagnostics",
        "semantic_planes",
        "language_capabilities",
        "dropped_by_scope",
    )
    payload: dict[str, object] = {}
    for key in keys:
        value = summary.get(key)
        if value is not None:
            payload[key] = value
    return payload
