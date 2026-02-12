"""Output contract constants for the engine execution pipeline.

Canonical names are engine-native (`cpg_*`), while legacy `write_*_delta`
aliases are retained for a compatibility window.
"""

from __future__ import annotations

# Canonical CPG outputs produced by the Rust engine
CANONICAL_CPG_OUTPUTS: tuple[str, ...] = (
    "cpg_nodes",
    "cpg_edges",
    "cpg_props",
    "cpg_props_map",
    "cpg_edges_by_src",
    "cpg_edges_by_dst",
)

# Backward-compatible aliases retained for one migration window.
LEGACY_CPG_OUTPUTS: tuple[str, ...] = (
    "write_cpg_nodes_delta",
    "write_cpg_edges_delta",
    "write_cpg_props_delta",
    "write_cpg_props_map_delta",
    "write_cpg_edges_by_src_delta",
    "write_cpg_edges_by_dst_delta",
)

CPG_OUTPUT_CANONICAL_TO_LEGACY: dict[str, str] = dict(
    zip(CANONICAL_CPG_OUTPUTS, LEGACY_CPG_OUTPUTS, strict=False)
)
CPG_OUTPUT_LEGACY_TO_CANONICAL: dict[str, str] = {
    legacy: canonical for canonical, legacy in CPG_OUTPUT_CANONICAL_TO_LEGACY.items()
}

# Canonical list used by all engine-native code paths.
ENGINE_CPG_OUTPUTS: tuple[str, ...] = CANONICAL_CPG_OUTPUTS

# Canonical auxiliary outputs produced by run_build contract artifacts.
PYTHON_AUXILIARY_OUTPUTS: tuple[str, ...] = (
    "normalize_outputs_delta",
    "extract_error_artifacts_delta",
    "run_manifest_delta",
)

AUXILIARY_OUTPUT_CANONICAL_TO_LEGACY: dict[str, str] = {
    "normalize_outputs_delta": "write_normalize_outputs_delta",
    "extract_error_artifacts_delta": "write_extract_error_artifacts_delta",
    "run_manifest_delta": "write_run_manifest_delta",
}

# Orchestrator-level outputs
ORCHESTRATOR_OUTPUTS: tuple[str, ...] = ("run_bundle_dir",)

ORCHESTRATOR_OUTPUT_CANONICAL_TO_LEGACY: dict[str, str] = {
    "run_bundle_dir": "write_run_bundle_dir",
}

# Full pipeline output set (union of all canonical sources)
FULL_PIPELINE_OUTPUTS: tuple[str, ...] = (
    *ENGINE_CPG_OUTPUTS,
    *PYTHON_AUXILIARY_OUTPUTS,
    *ORCHESTRATOR_OUTPUTS,
)

# Mapping from canonical output name to source
OUTPUT_SOURCE_MAP: dict[str, str] = {
    **dict.fromkeys(ENGINE_CPG_OUTPUTS, "rust_engine"),
    **dict.fromkeys(PYTHON_AUXILIARY_OUTPUTS, "python_auxiliary"),
    **dict.fromkeys(ORCHESTRATOR_OUTPUTS, "orchestrator"),
}

# Compatibility map that also accepts legacy CPG aliases.
OUTPUT_SOURCE_MAP_WITH_ALIASES: dict[str, str] = {
    **OUTPUT_SOURCE_MAP,
    **dict.fromkeys(LEGACY_CPG_OUTPUTS, "rust_engine"),
    **dict.fromkeys(AUXILIARY_OUTPUT_CANONICAL_TO_LEGACY.values(), "python_auxiliary"),
    **dict.fromkeys(ORCHESTRATOR_OUTPUT_CANONICAL_TO_LEGACY.values(), "orchestrator"),
}


def canonical_cpg_output_name(name: str) -> str:
    """Return canonical CPG output name for canonical or legacy input."""
    return CPG_OUTPUT_LEGACY_TO_CANONICAL.get(name, name)


def legacy_cpg_output_name(name: str) -> str:
    """Return legacy CPG alias for a canonical name when available."""
    return CPG_OUTPUT_CANONICAL_TO_LEGACY.get(name, name)


def output_aliases(name: str) -> tuple[str, ...]:
    """Return canonical/legacy aliases for a CPG output name.

    Non-CPG outputs are returned as a singleton tuple.
    """
    canonical_cpg = canonical_cpg_output_name(name)
    cpg_legacy = CPG_OUTPUT_CANONICAL_TO_LEGACY.get(canonical_cpg)
    if cpg_legacy is not None:
        return (canonical_cpg, cpg_legacy)
    for canonical, legacy in AUXILIARY_OUTPUT_CANONICAL_TO_LEGACY.items():
        if name in {canonical, legacy}:
            return (canonical, legacy)
    for canonical, legacy in ORCHESTRATOR_OUTPUT_CANONICAL_TO_LEGACY.items():
        if name in {canonical, legacy}:
            return (canonical, legacy)
    return (name,)


__all__ = [
    "AUXILIARY_OUTPUT_CANONICAL_TO_LEGACY",
    "CANONICAL_CPG_OUTPUTS",
    "CPG_OUTPUT_CANONICAL_TO_LEGACY",
    "CPG_OUTPUT_LEGACY_TO_CANONICAL",
    "ENGINE_CPG_OUTPUTS",
    "FULL_PIPELINE_OUTPUTS",
    "LEGACY_CPG_OUTPUTS",
    "ORCHESTRATOR_OUTPUTS",
    "ORCHESTRATOR_OUTPUT_CANONICAL_TO_LEGACY",
    "OUTPUT_SOURCE_MAP",
    "OUTPUT_SOURCE_MAP_WITH_ALIASES",
    "PYTHON_AUXILIARY_OUTPUTS",
    "canonical_cpg_output_name",
    "legacy_cpg_output_name",
    "output_aliases",
]
