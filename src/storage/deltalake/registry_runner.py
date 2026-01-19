"""Registry runners for Delta Lake exports."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from core_types import PathLike, ensure_path
from storage.deltalake.cpg_registry import (
    write_registry_delta as write_cpg_registry_delta,
)
from storage.deltalake.registry_models import RegistryWriteOptions, RegistryWriteResult
from storage.deltalake.relspec_registry import (
    write_registry_delta as write_relspec_registry_delta,
)

RegistryTarget = Literal["cpg", "relspec"]


class RegistryWriter(Protocol):
    def __call__(
        self,
        base_dir: str,
        *,
        write_options: RegistryWriteOptions | None = None,
    ) -> RegistryWriteResult: ...


REGISTRY_WRITERS: Mapping[RegistryTarget, RegistryWriter] = {
    "cpg": write_cpg_registry_delta,
    "relspec": write_relspec_registry_delta,
}


@dataclass(frozen=True)
class RegistryRunResult:
    """Results for a registry export run."""

    results: Mapping[RegistryTarget, RegistryWriteResult]


def registry_output_dir(output_dir: PathLike, target: RegistryTarget) -> Path:
    """Resolve the registry export directory for a target.

    Returns
    -------
    pathlib.Path
        Registry export directory path.
    """
    resolved = ensure_path(output_dir)
    return resolved / "registry" / target


def run_registry_target(
    output_dir: PathLike,
    target: RegistryTarget,
    *,
    write_options: RegistryWriteOptions | None = None,
) -> RegistryWriteResult:
    """Write registry tables for a single target.

    Returns
    -------
    RegistryWriteResult
        Delta write results plus diagnostics table.
    """
    writer = REGISTRY_WRITERS[target]
    target_dir = registry_output_dir(output_dir, target)
    return writer(str(target_dir), write_options=write_options)


def run_cpg_registry(
    output_dir: PathLike,
    *,
    write_options: RegistryWriteOptions | None = None,
) -> RegistryWriteResult:
    """Write CPG registry tables to Delta Lake.

    Returns
    -------
    RegistryWriteResult
        Delta write results plus diagnostics table.
    """
    return run_registry_target(
        output_dir,
        "cpg",
        write_options=write_options,
    )


def run_relspec_registry(
    output_dir: PathLike,
    *,
    write_options: RegistryWriteOptions | None = None,
) -> RegistryWriteResult:
    """Write relspec registry tables to Delta Lake.

    Returns
    -------
    RegistryWriteResult
        Delta write results plus diagnostics table.
    """
    return run_registry_target(
        output_dir,
        "relspec",
        write_options=write_options,
    )


def run_registry_exports(
    output_dir: PathLike,
    *,
    targets: Sequence[RegistryTarget] | None = None,
    write_options: RegistryWriteOptions | None = None,
) -> RegistryRunResult:
    """Write registry tables for all targets.

    Returns
    -------
    RegistryRunResult
        Results keyed by registry target.
    """
    resolved_targets = targets or tuple(REGISTRY_WRITERS.keys())
    results: dict[RegistryTarget, RegistryWriteResult] = {}
    for target in resolved_targets:
        results[target] = run_registry_target(
            output_dir,
            target,
            write_options=write_options,
        )
    return RegistryRunResult(results=results)


__all__ = [
    "REGISTRY_WRITERS",
    "RegistryRunResult",
    "RegistryTarget",
    "registry_output_dir",
    "run_cpg_registry",
    "run_registry_exports",
    "run_registry_target",
    "run_relspec_registry",
]
