"""Tests for semantic program manifest contracts."""

from __future__ import annotations

import msgspec
import pytest

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from semantics.compile_context import (
    CompileContext,
    SemanticExecutionContext,
    build_semantic_execution_context,
)
from semantics.ir import SemanticIR
from semantics.program_manifest import ManifestDatasetBindings, SemanticProgramManifest
from semantics.validation.catalog_validation import SemanticInputValidationResult


def _manifest(bindings: ManifestDatasetBindings) -> SemanticProgramManifest:
    return SemanticProgramManifest(
        semantic_ir=SemanticIR(views=()),
        requested_outputs=(),
        input_mapping={},
        validation_policy="schema_only",
        dataset_bindings=bindings,
        validation=SemanticInputValidationResult(
            valid=True,
            missing_tables=(),
            missing_columns={},
            resolved_tables={},
        ),
    )


def test_manifest_dataset_bindings_lookup_subset_and_payload() -> None:
    bindings = ManifestDatasetBindings(
        locations={
            "zeta": DatasetLocation(path="/tmp/zeta", format="delta"),
            "alpha": DatasetLocation(path="/tmp/alpha", format="delta"),
        }
    )

    assert bindings.has_location("alpha")
    assert not bindings.has_location("missing")
    assert bindings.location("alpha") is not None
    assert bindings.location("missing") is None
    assert set(bindings.names()) == {"alpha", "zeta"}

    subset = bindings.subset(("alpha", "missing"))
    assert set(subset.names()) == {"alpha"}
    assert list(bindings.payload().keys()) == ["alpha", "zeta"]


def test_manifest_dataset_bindings_require_location_raises_key_error() -> None:
    bindings = ManifestDatasetBindings(locations={})
    with pytest.raises(KeyError, match="Required dataset location not found"):
        _ = bindings.require_location("missing_dataset")


def test_semantic_program_manifest_payload_and_fingerprint_stability() -> None:
    bindings = ManifestDatasetBindings(
        locations={"alpha": DatasetLocation(path="/tmp/alpha", format="delta")}
    )
    manifest = _manifest(bindings)

    with_fingerprint = manifest.with_fingerprint()
    with_fingerprint_again = manifest.with_fingerprint()
    assert with_fingerprint.fingerprint is not None
    assert with_fingerprint.fingerprint == with_fingerprint_again.fingerprint
    assert with_fingerprint.manifest_version == 1


def test_dataset_bindings_helper_matches_compile_context() -> None:
    profile = DataFusionRuntimeProfile(
        data_sources=msgspec.structs.replace(
            DataFusionRuntimeProfile().data_sources,
            semantic_output=msgspec.structs.replace(
                DataFusionRuntimeProfile().data_sources.semantic_output,
                locations={
                    "semantic_nodes_union": DatasetLocation(
                        path="/tmp/semantic_nodes_union",
                        format="delta",
                    )
                },
            ),
        )
    )
    helper_bindings = build_semantic_execution_context(runtime_profile=profile).dataset_resolver
    compile_bindings = CompileContext(runtime_profile=profile).dataset_bindings()
    helper_names = sorted(helper_bindings.names())
    compile_names = sorted(compile_bindings.names())
    assert helper_names == compile_names
    for name in compile_names:
        helper_location = helper_bindings.location(name)
        compile_location = compile_bindings.location(name)
        assert helper_location is not None
        assert compile_location is not None
        assert helper_location.path == compile_location.path
        assert helper_location.format == compile_location.format


def test_semantic_execution_context_carries_manifest_resolver() -> None:
    profile = DataFusionRuntimeProfile()
    bindings = ManifestDatasetBindings(
        locations={"repo_snapshot": DatasetLocation(path="/tmp/repo_snapshot", format="delta")}
    )
    manifest = _manifest(bindings)
    ctx = profile.session_context()

    execution_context = SemanticExecutionContext(
        manifest=manifest,
        dataset_resolver=bindings,
        runtime_profile=profile,
        ctx=ctx,
    )
    assert execution_context.dataset_resolver.location("repo_snapshot") is not None
