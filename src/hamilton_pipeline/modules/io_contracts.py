"""Hamilton IO contract nodes."""

from __future__ import annotations

from hamilton_pipeline import io_contracts as _io_contracts

repo_files = _io_contracts.repo_files
scip_tables = _io_contracts.scip_tables
write_cpg_delta_output = _io_contracts.write_cpg_delta_output
write_extract_error_artifacts_delta = _io_contracts.write_extract_error_artifacts_delta
write_normalize_outputs_delta = _io_contracts.write_normalize_outputs_delta
write_run_bundle_dir = _io_contracts.write_run_bundle_dir
write_run_manifest_delta = _io_contracts.write_run_manifest_delta

for _export in (
    repo_files,
    scip_tables,
    write_cpg_delta_output,
    write_extract_error_artifacts_delta,
    write_normalize_outputs_delta,
    write_run_bundle_dir,
    write_run_manifest_delta,
):
    _export.__module__ = __name__

__all__ = [
    "repo_files",
    "scip_tables",
    "write_cpg_delta_output",
    "write_extract_error_artifacts_delta",
    "write_normalize_outputs_delta",
    "write_run_bundle_dir",
    "write_run_manifest_delta",
]
