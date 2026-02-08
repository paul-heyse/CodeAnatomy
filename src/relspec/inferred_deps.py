"""Dependency inference from DataFusion plan bundles."""

from __future__ import annotations

import contextlib
import importlib
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import msgspec

from datafusion_engine.lineage.datafusion import ScanLineage
from schema_spec.dataset_spec_ops import dataset_spec_name
from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.lineage.datafusion import ScanLineage
    from datafusion_engine.plan.bundle import DataFusionPlanBundle
    from datafusion_engine.schema.contracts import SchemaContract
    from datafusion_engine.views.graph import ViewNode
    from schema_spec.system import DatasetSpec


_LOGGER = logging.getLogger(__name__)


class InferredDeps(StructBaseStrict, frozen=True):
    """Dependencies inferred from DataFusion plan bundles.

    Captures table and column-level dependencies by analyzing the actual
    query plan rather than relying on declared inputs.

    Attributes:
    ----------
    task_name : str
        Name of the task these dependencies apply to.
    output : str
        Output dataset name produced by the task.
    inputs : tuple[str, ...]
        Table names inferred from expression analysis.
    required_columns : Mapping[str, tuple[str, ...]]
        Per-table columns required by the expression.
    required_types : Mapping[str, tuple[tuple[str, str], ...]]
        Per-table required column/type pairs.
    required_metadata : Mapping[str, tuple[tuple[bytes, bytes], ...]]
        Per-table required metadata entries.
    plan_fingerprint : str
        Stable hash for caching and comparison.
    required_udfs : tuple[str, ...]
        Required UDF names inferred from the expression.
    required_rewrite_tags : tuple[str, ...]
        Required rewrite tags derived from referenced UDFs.
    scans : tuple[ScanLineage, ...]
        Structured scan lineage entries for scan planning.
    """

    task_name: str
    output: str
    inputs: tuple[str, ...]
    required_columns: Mapping[str, tuple[str, ...]] = msgspec.field(default_factory=dict)
    required_types: Mapping[str, tuple[tuple[str, str], ...]] = msgspec.field(default_factory=dict)
    required_metadata: Mapping[str, tuple[tuple[bytes, bytes], ...]] = msgspec.field(
        default_factory=dict
    )
    plan_fingerprint: str = ""
    required_udfs: tuple[str, ...] = ()
    required_rewrite_tags: tuple[str, ...] = ()
    scans: tuple[ScanLineage, ...] = ()


@dataclass(frozen=True)
class InferredDepsInputs:
    """Inputs needed to infer dependencies from a DataFusion plan bundle.

    Attributes:
    ----------
    task_name : str
        Name of the task these dependencies apply to.
    output : str
        Output dataset name produced by the task.
    plan_bundle : DataFusionPlanBundle
        DataFusion plan bundle for native lineage extraction.
    ctx : SessionContext | None
        Optional DataFusion context for resolving dataset specs.
    snapshot : Mapping[str, object] | None
        Optional Rust UDF snapshot for validation.
    required_udfs : Sequence[str] | None
        Optional explicit UDF names.
    """

    task_name: str
    output: str
    plan_bundle: DataFusionPlanBundle
    ctx: SessionContext | None = None
    snapshot: Mapping[str, object] | None = None
    required_udfs: Sequence[str] | None = None


def infer_deps_from_plan_bundle(
    inputs: InferredDepsInputs,
) -> InferredDeps:
    """Infer dependencies from a DataFusion plan bundle.

    This is the preferred path for dependency inference, using DataFusion-native
    lineage extraction instead of SQLGlot parsing.

    Parameters
    ----------
    inputs : InferredDepsInputs
        Dependency inference inputs including plan bundle and task metadata.

    Returns:
    -------
    InferredDeps
        Inferred dependencies extracted from DataFusion plan.

    """
    from datafusion_engine.lineage.datafusion import extract_lineage

    plan_bundle = inputs.plan_bundle

    # Extract lineage from the optimized logical plan
    lineage_snapshot = inputs.snapshot or plan_bundle.artifacts.udf_snapshot
    lineage = extract_lineage(
        plan_bundle.optimized_logical_plan,
        udf_snapshot=lineage_snapshot,
    )

    # Map lineage to InferredDeps format
    tables = _expand_nested_inputs(lineage.referenced_tables)
    columns_by_table = dict(lineage.required_columns_by_dataset)

    # Compute required types and metadata from registry
    required_types = _required_types_from_registry(columns_by_table, ctx=inputs.ctx)
    required_metadata = _required_metadata_for_tables(columns_by_table, ctx=inputs.ctx)

    from datafusion_engine.views.bundle_extraction import resolve_required_udfs_from_bundle

    snapshot = inputs.snapshot or plan_bundle.artifacts.udf_snapshot
    resolved_snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    resolved_udfs = resolve_required_udfs_from_bundle(
        plan_bundle,
        snapshot=resolved_snapshot,
    )
    if inputs.required_udfs is not None:
        allowed = {name.lower() for name in resolved_udfs}
        extra = [
            name
            for name in inputs.required_udfs
            if isinstance(name, str) and name.lower() not in allowed
        ]
        if extra:
            _LOGGER.warning(
                "Ignoring explicit required_udfs not present in plan bundle: %s",
                sorted(set(extra)),
            )

    if resolved_udfs:
        from datafusion_engine.udf.runtime import validate_required_udfs

        validate_required_udfs(resolved_snapshot, required=resolved_udfs)

    # Use plan fingerprint from bundle
    fingerprint = plan_bundle.plan_fingerprint

    return InferredDeps(
        task_name=inputs.task_name,
        output=inputs.output,
        inputs=tables,
        required_columns=columns_by_table,
        required_types=required_types,
        required_metadata=required_metadata,
        plan_fingerprint=fingerprint,
        required_udfs=resolved_udfs,
        required_rewrite_tags=lineage.required_rewrite_tags,
        scans=lineage.scans,
    )


def _expand_nested_inputs(inputs: Sequence[str]) -> tuple[str, ...]:
    expanded: list[str] = []
    seen: set[str] = set()
    if not inputs:
        return ()
    from datafusion_engine.schema.registry import nested_path_for

    for name in inputs:
        if name not in seen:
            expanded.append(name)
            seen.add(name)
        try:
            root, _path = nested_path_for(name)
        except KeyError:
            continue
        if root not in seen:
            expanded.append(root)
            seen.add(root)
    return tuple(expanded)


def infer_deps_from_view_nodes(
    nodes: Sequence[ViewNode],
    *,
    ctx: SessionContext | None = None,
    snapshot: Mapping[str, object] | None = None,
) -> tuple[InferredDeps, ...]:
    """Infer dependencies for view nodes using DataFusion plan bundles.

    Args:
        nodes: View nodes to analyze.
        ctx: Optional DataFusion session context.
        snapshot: Optional UDF snapshot for lineage extraction.

    Returns:
        tuple[InferredDeps, ...]: Result.

    Raises:
        ValueError: If both `ctx` and node plan bundles are unavailable.
    """
    inferred: list[InferredDeps] = []
    for node in nodes:
        if node.plan_bundle is None:
            msg = (
                f"View node {node.name!r} is missing plan_bundle. "
                "Plan bundles are required for dependency inference."
            )
            raise ValueError(msg)
        inferred.append(
            infer_deps_from_plan_bundle(
                InferredDepsInputs(
                    task_name=node.name,
                    output=node.name,
                    ctx=ctx,
                    snapshot=snapshot,
                    required_udfs=tuple(node.required_udfs) if node.required_udfs else None,
                    plan_bundle=node.plan_bundle,
                )
            )
        )
    return tuple(inferred)


def _required_metadata_for_tables(
    columns_by_table: Mapping[str, tuple[str, ...]],
    *,
    ctx: SessionContext | None = None,
) -> dict[str, tuple[tuple[bytes, bytes], ...]]:
    required: dict[str, tuple[tuple[bytes, bytes], ...]] = {}
    for table_name in columns_by_table:
        spec = _dataset_spec_for_table(table_name, ctx=ctx)
        if spec is None:
            continue
        from schema_spec.dataset_spec_ops import dataset_spec_schema

        metadata = dataset_spec_schema(spec).metadata
        if metadata:
            required[table_name] = tuple(sorted(metadata.items(), key=lambda item: item[0]))
    return required


def _required_types_from_registry(
    columns_by_table: Mapping[str, tuple[str, ...]],
    *,
    ctx: SessionContext | None = None,
) -> dict[str, tuple[tuple[str, str], ...]]:
    required: dict[str, tuple[tuple[str, str], ...]] = {}
    for table_name, columns in columns_by_table.items():
        contract = _schema_contract_for_table(table_name, ctx=ctx)
        if contract is None:
            continue
        pairs = _types_from_contract(contract, columns)
        if pairs:
            required[table_name] = pairs
    return required


def _optional_module_attr(module: str, attr: str) -> object | None:
    try:
        loaded = importlib.import_module(module)
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    return getattr(loaded, attr, None)


def _extract_dataset_spec(name: str) -> DatasetSpec | None:
    extract_dataset_spec = _optional_module_attr(
        "datafusion_engine.extract_registry",
        "dataset_spec",
    )
    if not callable(extract_dataset_spec):
        return None
    extract_dataset_spec = cast("Callable[[str], DatasetSpec]", extract_dataset_spec)
    try:
        return extract_dataset_spec(name)
    except KeyError:
        return None


def _normalize_dataset_spec(name: str, *, ctx: SessionContext | None = None) -> DatasetSpec | None:
    normalize_dataset_spec = _optional_module_attr(
        "semantics.catalog.dataset_specs",
        "dataset_spec",
    )
    if not callable(normalize_dataset_spec):
        return None
    normalize_dataset_spec = cast("Callable[..., DatasetSpec]", normalize_dataset_spec)
    try:
        return normalize_dataset_spec(name, ctx=ctx)
    except KeyError:
        return None


def _relationship_dataset_spec(
    name: str,
    *,
    ctx: SessionContext | None = None,
) -> DatasetSpec | None:
    relationship_dataset_specs = _optional_module_attr(
        "schema_spec.relationship_specs",
        "relationship_dataset_specs",
    )
    if not callable(relationship_dataset_specs):
        return None
    relationship_dataset_specs = cast(
        "Callable[..., Sequence[DatasetSpec]]",
        relationship_dataset_specs,
    )
    with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
        for spec in relationship_dataset_specs(ctx=ctx):
            if dataset_spec_name(spec) == name:
                return spec
    return None


def _dataset_spec_for_table(name: str, *, ctx: SessionContext | None = None) -> DatasetSpec | None:
    resolvers = (
        _extract_dataset_spec,
        lambda table_name: _normalize_dataset_spec(table_name, ctx=ctx),
        lambda table_name: _relationship_dataset_spec(table_name, ctx=ctx),
    )
    for resolver in resolvers:
        spec = resolver(name)
        if spec is not None:
            return spec
    return None


def _schema_contract_for_table(
    name: str, *, ctx: SessionContext | None = None
) -> SchemaContract | None:
    spec = _dataset_spec_for_table(name, ctx=ctx)
    if spec is None:
        return None
    try:
        from datafusion_engine.schema.contracts import schema_contract_from_dataset_spec
    except (ImportError, RuntimeError, TypeError, ValueError):
        return None
    from schema_spec.dataset_spec_ops import dataset_spec_name

    return schema_contract_from_dataset_spec(name=dataset_spec_name(spec), spec=spec)


def _types_from_contract(
    contract: SchemaContract,
    columns: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    if not contract.enforce_columns:
        return ()
    by_name = {col.name: col for col in contract.columns}
    pairs: list[tuple[str, str]] = []
    for name in columns:
        column = by_name.get(name)
        if column is not None:
            pairs.append((name, str(column.dtype)))
    return tuple(pairs)


__all__ = [
    "InferredDeps",
    "InferredDepsInputs",
    "infer_deps_from_plan_bundle",
    "infer_deps_from_view_nodes",
]
