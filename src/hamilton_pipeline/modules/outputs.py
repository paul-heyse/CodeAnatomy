"""Hamilton output nodes for inference-driven pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from hamilton_pipeline.io_contracts import (
    AdjacencyOutputs,
    OutputPlanArtifactsContext,
    OutputPlanContext,
    OutputRuntimeContext,
    PrimaryOutputs,
    _plan_identity_hashes_for_outputs,
)
from hamilton_pipeline.tag_policy import TagPolicy, apply_tag
from hamilton_pipeline.types import CacheRuntimeContext, OutputConfig
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from engine.runtime_profile import RuntimeProfileSpec
    from hamilton_pipeline.io_contracts import DataSaverDict
else:
    DataFusionPlanBundle = object
    RuntimeProfileSpec = object
    DataSaverDict = object


@apply_tag(TagPolicy(layer="inputs", kind="scalar", artifact="run_id"))
def run_id() -> str:
    """Return a run-scoped UUID for pipeline outputs.

    Returns
    -------
    str
        Unique run identifier.
    """
    return uuid7_str()


def output_runtime_context(
    runtime_profile_spec: RuntimeProfileSpec,
    output_config: OutputConfig,
    cache_context: CacheRuntimeContext,
) -> OutputRuntimeContext:
    """Build output runtime context.

    Parameters
    ----------
    runtime_profile_spec
        Runtime profile specification for this run.
    output_config
        Output configuration settings.
    cache_context
        Cache configuration snapshot for the run.

    Returns
    -------
    OutputRuntimeContext
        Runtime and output configuration context.
    """
    return OutputRuntimeContext(
        runtime_profile_spec=runtime_profile_spec,
        output_config=output_config,
        cache_context=cache_context,
    )


def output_plan_context(
    plan_signature: str,
    output_plan_artifacts_context: OutputPlanArtifactsContext,
    run_id: str,
    materialized_outputs: tuple[str, ...] | None = None,
) -> OutputPlanContext:
    """Build output plan context.

    Parameters
    ----------
    plan_signature
        Plan signature string for the run.
    output_plan_artifacts_context
        Plan artifact metadata for the execution run.
    run_id
        Run identifier for the pipeline execution.
    materialized_outputs
        Output node names targeted for materialization.

    Returns
    -------
    OutputPlanContext
        Plan metadata context.
    """
    artifact_ids = dict(output_plan_artifacts_context.plan_artifact_ids)
    artifact_ids.update(
        _plan_identity_hashes_for_outputs(output_plan_artifacts_context.plan_bundles_by_task)
    )
    outputs = tuple(str(name) for name in materialized_outputs or ())
    return OutputPlanContext(
        plan_signature=plan_signature,
        plan_fingerprints=output_plan_artifacts_context.plan_fingerprints,
        plan_bundles_by_task=output_plan_artifacts_context.plan_bundles_by_task,
        run_id=run_id,
        artifact_ids=artifact_ids,
        materialized_outputs=outputs or None,
    )


def output_plan_artifacts_context(
    plan_fingerprints: Mapping[str, str],
    plan_bundles_by_task: Mapping[str, DataFusionPlanBundle],
    plan_artifact_ids: Mapping[str, str],
) -> OutputPlanArtifactsContext:
    """Build plan artifacts metadata for output plan contexts.

    Parameters
    ----------
    plan_fingerprints
        Mapping of plan fingerprints by dataset.
    plan_bundles_by_task
        Plan bundles grouped by task name.
    plan_artifact_ids
        Mapping of plan artifact identifiers by dataset.

    Returns
    -------
    OutputPlanArtifactsContext
        Plan artifacts metadata container.
    """
    return OutputPlanArtifactsContext(
        plan_fingerprints=plan_fingerprints,
        plan_bundles_by_task=plan_bundles_by_task,
        plan_artifact_ids=plan_artifact_ids,
    )


def primary_outputs(
    write_cpg_nodes_delta: DataSaverDict,
    write_cpg_edges_delta: DataSaverDict,
    write_cpg_props_delta: DataSaverDict,
) -> PrimaryOutputs:
    """Bundle primary output artifacts.

    Parameters
    ----------
    write_cpg_nodes_delta
        CPG nodes output artifact metadata.
    write_cpg_edges_delta
        CPG edges output artifact metadata.
    write_cpg_props_delta
        CPG props output artifact metadata.

    Returns
    -------
    PrimaryOutputs
        Bundle of primary output artifacts.
    """
    return PrimaryOutputs(
        cpg_nodes=write_cpg_nodes_delta,
        cpg_edges=write_cpg_edges_delta,
        cpg_props=write_cpg_props_delta,
    )


def adjacency_outputs(
    write_cpg_props_map_delta: DataSaverDict,
    write_cpg_edges_by_src_delta: DataSaverDict,
    write_cpg_edges_by_dst_delta: DataSaverDict,
) -> AdjacencyOutputs:
    """Bundle adjacency output artifacts.

    Parameters
    ----------
    write_cpg_props_map_delta
        CPG props map output artifact metadata.
    write_cpg_edges_by_src_delta
        CPG edges-by-source output artifact metadata.
    write_cpg_edges_by_dst_delta
        CPG edges-by-destination output artifact metadata.

    Returns
    -------
    AdjacencyOutputs
        Bundle of adjacency output artifacts.
    """
    return AdjacencyOutputs(
        cpg_props_map=write_cpg_props_map_delta,
        cpg_edges_by_src=write_cpg_edges_by_src_delta,
        cpg_edges_by_dst=write_cpg_edges_by_dst_delta,
    )


__all__ = [
    "adjacency_outputs",
    "output_plan_artifacts_context",
    "output_plan_context",
    "output_runtime_context",
    "primary_outputs",
    "run_id",
]
