"""External Python interface extraction from unified import tables."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from importlib import machinery, metadata, util
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack

from arrow_utils.core.array_iter import iter_table_rows
from core_types import PathLike, ensure_path
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.plan.bundle import DataFusionPlanBundle
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.infrastructure.result_types import ExtractResult
from extract.python.env_profile import PythonEnvProfile, resolve_python_env_profile
from extract.session import ExtractSession
from utils.value_coercion import coerce_bool, coerce_int

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extract.coordination.evidence_plan import EvidencePlan


@dataclass(frozen=True)
class ExternalInterfaceExtractOptions:
    """Options for external interface extraction."""

    repo_id: str | None = None
    include_stdlib: bool = True
    include_unresolved: bool = True
    max_imports: int | None = None
    depth: Literal["metadata", "full"] = "metadata"


@dataclass(frozen=True)
class ExternalInterface:
    """Resolved external interface summary."""

    name: str
    status: str
    origin: str | None
    dist_name: str | None
    dist_version: str | None
    is_stdlib: bool

    def to_row(self) -> dict[str, str | None]:
        """Return a row payload for this interface.

        Returns
        -------
        dict[str, str | None]
            Row payload for the external interface dataset.
        """
        return {
            "name": self.name,
            "status": self.status,
            "origin": self.origin,
            "dist_name": self.dist_name,
            "dist_version": self.dist_version,
            "is_stdlib": str(self.is_stdlib),
        }


def extract_python_external(
    python_imports: TableLike,
    repo_root: PathLike,
    options: ExternalInterfaceExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract external Python interfaces from unified imports.

    Returns
    -------
    ExtractResult[TableLike]
        Extracted external interface table.
    """
    normalized_options = normalize_options(
        "python_external", options, ExternalInterfaceExtractOptions
    )
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    rows = _collect_external_interface_rows(
        python_imports,
        repo_root=repo_root,
        options=normalized_options,
    )
    _record_python_external_stats(
        rows,
        runtime_profile=runtime_profile,
        options=normalized_options,
        repo_root=repo_root,
    )
    plan = _build_external_interface_plan(
        rows,
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
        session=session,
    )
    table = materialize_extract_plan(
        "python_external_interfaces_v1",
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            apply_post_kernels=True,
        ),
    )
    return ExtractResult(table=table, extractor_name="python_external")


def _build_external_interface_plan(
    rows: list[dict[str, str | None]],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
    session: ExtractSession,
) -> DataFusionPlanBundle:
    return extract_plan_from_rows(
        "python_external_interfaces_v1",
        rows,
        session=session,
        options=ExtractPlanOptions(
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    )


def _collect_external_interface_rows(
    python_imports: TableLike,
    *,
    repo_root: PathLike,
    options: ExternalInterfaceExtractOptions,
) -> list[dict[str, str | None]]:
    env = resolve_python_env_profile()
    root_path = ensure_path(repo_root).resolve()
    rows: list[dict[str, str | None]] = []
    targets = _collect_import_targets(
        python_imports,
        depth=options.depth,
        limit=options.max_imports,
    )
    for target in targets:
        interface = _resolve_external_interface(
            target,
            repo_root=root_path,
            env=env,
            options=options,
        )
        if interface is None:
            continue
        rows.append(interface.to_row())
    return rows


def _collect_import_targets(
    python_imports: TableLike,
    *,
    depth: Literal["metadata", "full"],
    limit: int | None,
) -> list[str]:
    targets: list[str] = []
    seen: set[str] = set()
    for row in iter_table_rows(python_imports):
        for target in _import_targets_from_row(row, depth=depth):
            if target in seen:
                continue
            seen.add(target)
            targets.append(target)
            if limit is not None and len(targets) >= limit:
                return targets
    return targets


def _import_targets_from_row(
    row: Mapping[str, object],
    *,
    depth: Literal["metadata", "full"],
) -> list[str]:
    module = row.get("module")
    name = row.get("name")
    level_value = coerce_int(row.get("level"))
    if level_value is not None and level_value > 0:
        return []
    module_value = module if isinstance(module, str) and module else None
    name_value = name if isinstance(name, str) and name else None
    is_star_import = coerce_bool(row.get("is_star"))
    if is_star_import is None:
        is_star_import = name_value == "*"
    targets: list[str] = []
    if module_value is not None:
        targets.append(module_value)
        if depth == "full" and name_value and not is_star_import:
            targets.append(f"{module_value}.{name_value}")
        return targets
    if name_value is not None and name_value != "*":
        targets.append(name_value)
    return targets


def _resolve_external_interface(
    name: str,
    *,
    repo_root: Path,
    env: PythonEnvProfile,
    options: ExternalInterfaceExtractOptions,
) -> ExternalInterface | None:
    spec = util.find_spec(name)
    if spec is None:
        return _unresolved_interface(name, options)
    return _interface_from_spec(
        name,
        spec=spec,
        repo_root=repo_root,
        env=env,
        options=options,
    )


def _interface_from_spec(
    name: str,
    *,
    spec: machinery.ModuleSpec,
    repo_root: Path,
    env: PythonEnvProfile,
    options: ExternalInterfaceExtractOptions,
) -> ExternalInterface | None:
    origin = spec.origin
    if origin in {"built-in", "frozen"}:
        return _stdlib_interface(name, origin, options)
    origin_path = _spec_origin_path(spec)
    if origin_path is None:
        return _unresolved_interface(name, options)
    if _is_within_repo(origin_path, repo_root):
        return None
    is_stdlib = env.is_stdlib(origin_path)
    if is_stdlib and not options.include_stdlib:
        return None
    dist_name, dist_version = _distribution_info(
        env.distribution_for_path(origin_path),
    )
    return ExternalInterface(
        name=name,
        status="resolved",
        origin=str(origin_path),
        dist_name=dist_name,
        dist_version=dist_version,
        is_stdlib=is_stdlib,
    )


def _unresolved_interface(
    name: str,
    options: ExternalInterfaceExtractOptions,
) -> ExternalInterface | None:
    if not options.include_unresolved:
        return None
    return ExternalInterface(
        name=name,
        status="unresolved",
        origin=None,
        dist_name=None,
        dist_version=None,
        is_stdlib=False,
    )


def _stdlib_interface(
    name: str,
    origin: str,
    options: ExternalInterfaceExtractOptions,
) -> ExternalInterface | None:
    if not options.include_stdlib:
        return None
    return ExternalInterface(
        name=name,
        status="resolved",
        origin=origin,
        dist_name=None,
        dist_version=None,
        is_stdlib=True,
    )


def _distribution_info(
    dist: metadata.Distribution | None,
) -> tuple[str | None, str | None]:
    if dist is None:
        return None, None
    metadata = dist.metadata
    dist_name = metadata.get("Name") if metadata is not None else None
    return dist_name, dist.version


def _spec_origin_path(spec: machinery.ModuleSpec) -> Path | None:
    origin = spec.origin
    if origin is None and spec.submodule_search_locations is not None:
        first_location = next(iter(spec.submodule_search_locations), None)
        if first_location is None:
            return None
        return ensure_path(first_location)
    if isinstance(origin, str):
        return Path(origin)
    return None


def _is_within_repo(path: Path, repo_root: Path) -> bool:
    try:
        path.resolve().relative_to(repo_root)
    except ValueError:
        return False
    return True


def _record_python_external_stats(
    rows: list[dict[str, str | None]],
    *,
    runtime_profile: DataFusionRuntimeProfile | None,
    options: ExternalInterfaceExtractOptions,
    repo_root: PathLike,
) -> None:
    profile = runtime_profile
    if profile is None or profile.diagnostics.diagnostics_sink is None:
        return
    resolved = sum(1 for row in rows if row.get("status") == "resolved")
    unresolved = sum(1 for row in rows if row.get("status") == "unresolved")
    stdlib = sum(1 for row in rows if row.get("is_stdlib") == "True")
    payload = {
        "repo_root": str(ensure_path(repo_root)),
        "total": len(rows),
        "resolved": resolved,
        "unresolved": unresolved,
        "stdlib": stdlib,
        "include_stdlib": options.include_stdlib,
        "include_unresolved": options.include_unresolved,
        "max_imports": options.max_imports,
        "depth": options.depth,
        "repo_id": options.repo_id,
    }
    from datafusion_engine.lineage.diagnostics import record_artifact

    record_artifact(profile, "python_external_stats_v1", payload)


class _ExternalTablesKwargs(TypedDict, total=False):
    python_imports: Required[TableLike]
    repo_root: Required[PathLike]
    options: ExternalInterfaceExtractOptions | None
    evidence_plan: EvidencePlan | None
    session: ExtractSession | None
    profile: str
    prefer_reader: bool


def extract_python_external_tables(
    **kwargs: Unpack[_ExternalTablesKwargs],
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Extract external interface tables as a name-keyed bundle.

    Returns
    -------
    Mapping[str, TableLike | RecordBatchReaderLike]
        Output tables keyed by dataset name.
    """
    python_imports = kwargs["python_imports"]
    repo_root = kwargs["repo_root"]
    normalized_options = normalize_options(
        "python_external", kwargs.get("options"), ExternalInterfaceExtractOptions
    )
    exec_context = ExtractExecutionContext(
        evidence_plan=kwargs.get("evidence_plan"),
        session=kwargs.get("session"),
        profile=kwargs.get("profile", "default"),
    )
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    rows = _collect_external_interface_rows(
        python_imports,
        repo_root=repo_root,
        options=normalized_options,
    )
    _record_python_external_stats(
        rows,
        runtime_profile=runtime_profile,
        options=normalized_options,
        repo_root=repo_root,
    )
    plan = _build_external_interface_plan(
        rows,
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
        session=session,
    )
    table = materialize_extract_plan(
        "python_external_interfaces_v1",
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=kwargs.get("prefer_reader", False),
            apply_post_kernels=True,
        ),
    )
    return {"python_external_interfaces": table}


__all__ = [
    "ExternalInterface",
    "ExternalInterfaceExtractOptions",
    "extract_python_external",
    "extract_python_external_tables",
]
