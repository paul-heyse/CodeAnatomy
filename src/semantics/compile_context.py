"""Compile-context orchestration for semantic program manifests."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from semantics.naming import canonical_output_name, output_name_map_from_views
from semantics.program_manifest import (
    ManifestDatasetBindings,
    ManifestDatasetResolver,
    SemanticProgramManifest,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.facade import DataFusionExecutionFacade
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.ir import SemanticIR
    from semantics.registry import SemanticModel
    from semantics.validation.policy import SemanticInputValidationPolicy


def _resolved_outputs(outputs: Collection[str] | None) -> tuple[str, ...] | None:
    if outputs is None:
        return None
    return tuple(canonical_output_name(name) for name in outputs)


def semantic_ir_for_outputs(
    outputs: Collection[str] | None = None,
    *,
    model: SemanticModel | None = None,
) -> SemanticIR:
    """Return semantic IR for canonicalized output names."""
    from semantics.ir_pipeline import build_semantic_ir

    return build_semantic_ir(outputs=_resolved_outputs(outputs), model=model)


@dataclass(frozen=True)
class SemanticExecutionContext:
    """Shared execution context carrying compiled semantic authorities."""

    manifest: SemanticProgramManifest
    dataset_resolver: ManifestDatasetResolver
    runtime_profile: DataFusionRuntimeProfile
    ctx: SessionContext
    model: SemanticModel
    facade: DataFusionExecutionFacade | None = None


def build_semantic_execution_context(
    *,
    runtime_profile: DataFusionRuntimeProfile,
    outputs: Collection[str] | None = None,
    model: SemanticModel | None = None,
    policy: SemanticInputValidationPolicy = "schema_only",
    ctx: SessionContext | None = None,
    input_mapping: Mapping[str, str] | None = None,
) -> SemanticExecutionContext:
    """Compile semantic artifacts and return a shared execution context.

    Returns:
        SemanticExecutionContext: Compiled manifest, resolver, and execution context.
    """
    from datafusion_engine.dataset.registry import dataset_catalog_from_profile
    from semantics.compile_invariants import record_compile_if_tracking
    from semantics.registry import SEMANTIC_MODEL
    from semantics.validation.policy import (
        default_semantic_input_mapping,
        resolve_semantic_input_mapping,
        validate_semantic_inputs,
    )

    record_compile_if_tracking()
    active_ctx = ctx or runtime_profile.session_context()
    resolved_outputs = _resolved_outputs(outputs) or ()
    if input_mapping is not None:
        resolved_input_mapping = dict(input_mapping)
    else:
        try:
            resolved_input_mapping = resolve_semantic_input_mapping(active_ctx)
        except ValueError:
            resolved_input_mapping = default_semantic_input_mapping()
    resolved_model = SEMANTIC_MODEL if model is None else model
    semantic_ir = semantic_ir_for_outputs(outputs, model=resolved_model)
    catalog = dataset_catalog_from_profile(runtime_profile)
    dataset_bindings = ManifestDatasetBindings(
        locations={name: catalog.get(name) for name in catalog.names()}
    )
    manifest = SemanticProgramManifest(
        semantic_ir=semantic_ir,
        requested_outputs=resolved_outputs,
        input_mapping=resolved_input_mapping,
        validation_policy=policy,
        dataset_bindings=dataset_bindings,
        output_name_map=output_name_map_from_views(semantic_ir.views),
    )
    validation = validate_semantic_inputs(
        ctx=active_ctx,
        manifest=manifest,
        policy=policy,
    )
    compiled_manifest = replace(manifest, validation=validation).with_fingerprint()
    return SemanticExecutionContext(
        manifest=compiled_manifest,
        dataset_resolver=compiled_manifest.dataset_bindings,
        runtime_profile=runtime_profile,
        ctx=active_ctx,
        model=resolved_model,
        facade=None,
    )


__all__ = [
    "SemanticExecutionContext",
    "build_semantic_execution_context",
    "semantic_ir_for_outputs",
]
