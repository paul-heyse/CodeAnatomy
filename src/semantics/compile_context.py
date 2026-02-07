"""Compile-context orchestration for semantic program manifests."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field, replace
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
    from semantics.validation.policy import SemanticInputValidationPolicy


def _resolved_outputs(outputs: Collection[str] | None) -> tuple[str, ...] | None:
    if outputs is None:
        return None
    return tuple(canonical_output_name(name) for name in outputs)


def semantic_ir_for_outputs(outputs: Collection[str] | None = None) -> SemanticIR:
    """Return semantic IR for canonicalized output names."""
    from semantics.ir_pipeline import build_semantic_ir

    return build_semantic_ir(outputs=_resolved_outputs(outputs))


@dataclass
class CompileContext:
    """Memoized compile context for semantic program orchestration."""

    runtime_profile: DataFusionRuntimeProfile
    outputs: Collection[str] | None = None
    policy: SemanticInputValidationPolicy = "schema_only"
    input_mapping: Mapping[str, str] | None = None
    _semantic_ir_cache: SemanticIR | None = field(default=None, init=False, repr=False)
    _dataset_bindings_cache: ManifestDatasetBindings | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def semantic_ir(self) -> SemanticIR:
        """Return cached semantic IR for this compile context."""
        if self._semantic_ir_cache is not None:
            return self._semantic_ir_cache
        self._semantic_ir_cache = semantic_ir_for_outputs(self.outputs)
        return self._semantic_ir_cache

    def dataset_bindings(self) -> ManifestDatasetBindings:
        """Return cached dataset bindings from runtime catalog resolution."""
        if self._dataset_bindings_cache is not None:
            return self._dataset_bindings_cache
        from datafusion_engine.dataset.registry import dataset_catalog_from_profile

        catalog = dataset_catalog_from_profile(self.runtime_profile)
        self._dataset_bindings_cache = ManifestDatasetBindings(
            locations={name: catalog.get(name) for name in catalog.names()}
        )
        return self._dataset_bindings_cache

    def compile(self, *, ctx: SessionContext | None = None) -> SemanticProgramManifest:
        """Compile and validate a semantic program manifest.

        Returns:
        -------
        SemanticProgramManifest
            Manifest enriched with validation status and fingerprint.
        """
        from semantics.validation.policy import (
            resolve_semantic_input_mapping,
            validate_semantic_inputs,
        )

        active_ctx = ctx or self.runtime_profile.session_context()
        resolved_outputs = _resolved_outputs(self.outputs) or ()
        resolved_input_mapping = (
            dict(self.input_mapping)
            if self.input_mapping is not None
            else resolve_semantic_input_mapping(active_ctx)
        )
        ir = self.semantic_ir()
        manifest = SemanticProgramManifest(
            semantic_ir=ir,
            requested_outputs=resolved_outputs,
            input_mapping=resolved_input_mapping,
            validation_policy=self.policy,
            dataset_bindings=self.dataset_bindings(),
            output_name_map=output_name_map_from_views(ir.views),
        )
        validation = validate_semantic_inputs(
            ctx=active_ctx,
            manifest=manifest,
            policy=self.policy,
        )
        return replace(manifest, validation=validation).with_fingerprint()


@dataclass(frozen=True)
class SemanticExecutionContext:
    """Shared execution context carrying compiled semantic authorities."""

    manifest: SemanticProgramManifest
    dataset_resolver: ManifestDatasetResolver
    runtime_profile: DataFusionRuntimeProfile
    ctx: SessionContext
    facade: DataFusionExecutionFacade | None = None


def _dataset_bindings_for_profile(
    runtime_profile: DataFusionRuntimeProfile,
) -> ManifestDatasetBindings:
    """Resolve manifest dataset bindings from the compile boundary.

    This is the internal implementation. External callers should use
    ``SemanticExecutionContext.dataset_resolver`` instead.

    Returns:
        ManifestDatasetBindings: Manifest-backed dataset bindings for the profile.
    """
    return CompileContext(runtime_profile=runtime_profile).dataset_bindings()


def dataset_bindings_for_profile(
    runtime_profile: DataFusionRuntimeProfile,
) -> ManifestDatasetBindings:
    """Resolve manifest dataset bindings from the compile boundary.

    .. deprecated::
        Use ``SemanticExecutionContext.dataset_resolver`` instead.

    Returns:
        ManifestDatasetBindings: Manifest-backed dataset bindings for the profile.
    """
    import warnings

    warnings.warn(
        "dataset_bindings_for_profile() is deprecated; "
        "use SemanticExecutionContext.dataset_resolver instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return _dataset_bindings_for_profile(runtime_profile)


def compile_semantic_program(
    *,
    runtime_profile: DataFusionRuntimeProfile,
    outputs: Collection[str] | None = None,
    policy: SemanticInputValidationPolicy = "schema_only",
    ctx: SessionContext | None = None,
    input_mapping: Mapping[str, str] | None = None,
) -> SemanticProgramManifest:
    """Compile and validate a semantic program manifest.

    Returns:
    -------
    SemanticProgramManifest
        Manifest enriched with validation status and fingerprint.
    """
    from semantics.compile_invariants import record_compile_if_tracking

    record_compile_if_tracking()
    return CompileContext(
        runtime_profile=runtime_profile,
        outputs=outputs,
        policy=policy,
        input_mapping=input_mapping,
    ).compile(ctx=ctx)


def build_semantic_execution_context(
    *,
    runtime_profile: DataFusionRuntimeProfile,
    outputs: Collection[str] | None = None,
    policy: SemanticInputValidationPolicy = "schema_only",
    ctx: SessionContext | None = None,
    input_mapping: Mapping[str, str] | None = None,
    facade: DataFusionExecutionFacade | None = None,
) -> SemanticExecutionContext:
    """Compile semantic artifacts and return a shared execution context.

    Returns:
        SemanticExecutionContext: Compiled manifest, resolver, and execution context.
    """
    from semantics.compile_invariants import record_compile_if_tracking

    record_compile_if_tracking()
    compile_ctx = CompileContext(
        runtime_profile=runtime_profile,
        outputs=outputs,
        policy=policy,
        input_mapping=input_mapping,
    )
    active_ctx = ctx or runtime_profile.session_context()
    manifest = compile_ctx.compile(ctx=active_ctx)
    return SemanticExecutionContext(
        manifest=manifest,
        dataset_resolver=manifest.dataset_bindings,
        runtime_profile=runtime_profile,
        ctx=active_ctx,
        facade=facade,
    )


__all__ = [
    "CompileContext",
    "SemanticExecutionContext",
    "build_semantic_execution_context",
    "compile_semantic_program",
    "dataset_bindings_for_profile",
    "semantic_ir_for_outputs",
]
