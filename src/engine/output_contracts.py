"""Output contract constants for the engine execution pipeline.

Define the canonical output names and their sources (Rust engine vs Python-side).
These constants replace the Hamilton-era FULL_PIPELINE_OUTPUTS tuple.
"""

from __future__ import annotations

# CPG outputs produced by the Rust engine
ENGINE_CPG_OUTPUTS: tuple[str, ...] = (
    "write_cpg_nodes_delta",
    "write_cpg_edges_delta",
    "write_cpg_props_delta",
    "write_cpg_props_map_delta",
    "write_cpg_edges_by_src_delta",
    "write_cpg_edges_by_dst_delta",
)

# Auxiliary outputs handled Python-side
PYTHON_AUXILIARY_OUTPUTS: tuple[str, ...] = (
    "write_normalize_outputs_delta",
    "write_extract_error_artifacts_delta",
    "write_run_manifest_delta",
)

# Orchestrator-level outputs
ORCHESTRATOR_OUTPUTS: tuple[str, ...] = ("write_run_bundle_dir",)

# Full pipeline output set (union of all sources)
FULL_PIPELINE_OUTPUTS: tuple[str, ...] = (
    *ENGINE_CPG_OUTPUTS,
    *PYTHON_AUXILIARY_OUTPUTS,
    *ORCHESTRATOR_OUTPUTS,
)

# Mapping from output name to source
OUTPUT_SOURCE_MAP: dict[str, str] = {
    **dict.fromkeys(ENGINE_CPG_OUTPUTS, "rust_engine"),
    **dict.fromkeys(PYTHON_AUXILIARY_OUTPUTS, "python_auxiliary"),
    **dict.fromkeys(ORCHESTRATOR_OUTPUTS, "orchestrator"),
}


__all__ = [
    "ENGINE_CPG_OUTPUTS",
    "FULL_PIPELINE_OUTPUTS",
    "ORCHESTRATOR_OUTPUTS",
    "OUTPUT_SOURCE_MAP",
    "PYTHON_AUXILIARY_OUTPUTS",
]
