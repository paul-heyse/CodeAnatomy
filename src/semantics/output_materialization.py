"""Semantic output location resolution and Delta materialization helpers."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion_engine.delta.schema_guard import SchemaEvolutionPolicy
from datafusion_engine.io.write_core import WritePipeline
from semantics.naming import canonical_output_name
from semantics.output_names import RELATION_OUTPUT_NAME

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.program_manifest import SemanticProgramManifest
    from semantics.registry import SemanticModel


@dataclass(frozen=True)
class SemanticOutputWriteContext:
    """Inputs required to materialize semantic outputs."""

    ctx: SessionContext
    pipeline: WritePipeline
    runtime_profile: DataFusionRuntimeProfile
    schema_policy: SchemaEvolutionPolicy


def semantic_output_view_names(
    *,
    model: SemanticModel,
    requested_outputs: Collection[str] | None = None,
    manifest: SemanticProgramManifest,
) -> list[str]:
    """Return semantic output view names."""
    if requested_outputs is None:
        view_names = [spec.name for spec in model.outputs]
        if RELATION_OUTPUT_NAME not in view_names:
            view_names.append(RELATION_OUTPUT_NAME)
        return view_names

    resolved = {canonical_output_name(name, manifest=manifest) for name in requested_outputs}
    return [spec.name for spec in model.outputs if spec.name in resolved]


def ensure_canonical_output_locations(
    output_locations: Mapping[str, DatasetLocation],
    *,
    manifest: SemanticProgramManifest,
) -> None:
    """Validate that semantic output locations use canonical names.

    Raises:
        ValueError: If any output location key is not already canonical.
    """
    non_canonical = {
        name: canonical_output_name(name, manifest=manifest)
        for name in output_locations
        if canonical_output_name(name, manifest=manifest) != name
    }
    if non_canonical:
        msg = f"Semantic outputs must use canonical names: {non_canonical!r}."
        raise ValueError(msg)


def semantic_output_locations(
    view_names: Sequence[str],
    runtime_profile: DataFusionRuntimeProfile,
) -> dict[str, DatasetLocation]:
    """Resolve dataset locations for semantic outputs.

    Returns:
        dict[str, DatasetLocation]: Canonicalized semantic output dataset locations.
    """
    import msgspec

    from datafusion_engine.dataset.registry import DatasetLocationOverrides
    from datafusion_engine.session.runtime_dataset_io import semantic_output_locations_for_profile
    from schema_spec.dataset_spec import (
        DeltaPolicyBundle,
        dataset_spec_delta_feature_gate,
        dataset_spec_delta_maintenance_policy,
        dataset_spec_delta_schema_policy,
        dataset_spec_delta_write_policy,
    )
    from semantics.catalog.dataset_specs import dataset_spec

    base_locations = semantic_output_locations_for_profile(runtime_profile)
    output_locations: dict[str, DatasetLocation] = {}
    for name in view_names:
        base = base_locations.get(name)
        if base is None:
            continue
        spec = dataset_spec(name)
        delta_bundle = DeltaPolicyBundle(
            write_policy=dataset_spec_delta_write_policy(spec),
            schema_policy=dataset_spec_delta_schema_policy(spec),
            maintenance_policy=dataset_spec_delta_maintenance_policy(spec),
            feature_gate=dataset_spec_delta_feature_gate(spec),
        )
        existing_overrides = base.overrides
        if existing_overrides is not None:
            enriched_overrides = msgspec.structs.replace(existing_overrides, delta=delta_bundle)
        else:
            enriched_overrides = DatasetLocationOverrides(delta=delta_bundle)
        output_locations[name] = msgspec.structs.replace(
            base,
            dataset_spec=spec,
            overrides=enriched_overrides,
        )
    return output_locations


def ensure_semantic_output_locations(
    view_names: Sequence[str],
    output_locations: Mapping[str, DatasetLocation],
) -> None:
    """Ensure all semantic outputs have explicit dataset locations.

    Raises:
        ValueError: If any requested semantic output is missing a dataset location.
    """
    missing_outputs = [name for name in view_names if name not in output_locations]
    if missing_outputs:
        missing = ", ".join(sorted(missing_outputs))
        msg = (
            "Semantic outputs require explicit dataset locations. "
            f"Missing locations for: {missing}."
        )
        raise ValueError(msg)


def write_semantic_output(
    *,
    view_name: str,
    output_location: DatasetLocation,
    write_context: SemanticOutputWriteContext,
) -> None:
    """Materialize a single semantic output view.

    Raises:
        ValueError: If resolved output location is not a Delta dataset.
    """
    from datafusion_engine.delta.schema_guard import enforce_schema_policy
    from datafusion_engine.delta.store_policy import apply_delta_store_policy
    from datafusion_engine.io.write_core import WriteFormat, WriteMode, WriteViewRequest
    from datafusion_engine.views.bundle_extraction import arrow_schema_from_df
    from schema_spec.dataset_spec import (
        dataset_spec_delta_maintenance_policy,
        dataset_spec_delta_schema_policy,
        dataset_spec_delta_write_policy,
    )
    from semantics.catalog.dataset_specs import dataset_spec
    from semantics.diagnostics import SEMANTIC_DIAGNOSTIC_VIEW_NAMES

    spec = dataset_spec(view_name)
    location = apply_delta_store_policy(
        output_location,
        policy=write_context.runtime_profile.policies.delta_store_policy,
    )
    if location.format != "delta":
        msg = f"Semantic output {view_name!r} must be stored as Delta."
        raise ValueError(msg)
    schema = arrow_schema_from_df(write_context.ctx.table(view_name))
    resolved_schema_policy = write_context.schema_policy
    delta_schema_policy = dataset_spec_delta_schema_policy(spec)
    if delta_schema_policy is not None and delta_schema_policy.schema_mode == "merge":
        resolved_schema_policy = SchemaEvolutionPolicy(mode="additive")
    if not write_context.runtime_profile.features.enable_schema_evolution_adapter:
        resolved_schema_policy = SchemaEvolutionPolicy(mode="strict")
    schema_hash = enforce_schema_policy(
        expected_schema=schema,
        dataset_location=location,
        policy=resolved_schema_policy,
    )
    commit_metadata: dict[str, object] = {
        "semantic_schema_hash": schema_hash,
        "semantic_view": view_name,
    }
    if view_name in SEMANTIC_DIAGNOSTIC_VIEW_NAMES:
        commit_metadata["snapshot_kind"] = view_name
    delta_write_policy = dataset_spec_delta_write_policy(spec)
    format_options: dict[str, object] = {
        "delta_commit_metadata": commit_metadata,
        "delta_write_policy": delta_write_policy,
        "delta_schema_policy": delta_schema_policy,
        "delta_maintenance_policy": dataset_spec_delta_maintenance_policy(spec),
    }
    partition_by = tuple(delta_write_policy.partition_by) if delta_write_policy is not None else ()
    write_context.pipeline.write_view(
        WriteViewRequest(
            view_name=view_name,
            destination=str(location.path),
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            partition_by=partition_by,
            format_options=format_options,
        )
    )


def materialize_semantic_outputs(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    schema_policy: SchemaEvolutionPolicy,
    model: SemanticModel,
    requested_outputs: Collection[str] | None,
    manifest: SemanticProgramManifest,
) -> None:
    """Materialize semantic output views to Delta locations."""
    from datafusion_engine.session.runtime_dataset_io import semantic_output_locations_for_profile

    view_names = semantic_output_view_names(
        model=model,
        requested_outputs=requested_outputs,
        manifest=manifest,
    )
    base_locations = semantic_output_locations_for_profile(runtime_profile)
    ensure_canonical_output_locations(base_locations, manifest=manifest)
    output_locations = semantic_output_locations(view_names, runtime_profile)
    ensure_semantic_output_locations(view_names, output_locations)

    pipeline = WritePipeline(ctx, runtime_profile=runtime_profile)
    write_context = SemanticOutputWriteContext(
        ctx=ctx,
        pipeline=pipeline,
        runtime_profile=runtime_profile,
        schema_policy=schema_policy,
    )
    for view_name in view_names:
        write_semantic_output(
            view_name=view_name,
            output_location=output_locations[view_name],
            write_context=write_context,
        )


__all__ = [
    "SemanticOutputWriteContext",
    "ensure_canonical_output_locations",
    "ensure_semantic_output_locations",
    "materialize_semantic_outputs",
    "semantic_output_locations",
    "semantic_output_view_names",
    "write_semantic_output",
]
