"""Reusable Delta/DataFusion smoke scenario harness."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from datafusion_engine.io.write_core import WriteFormat, WriteMode, WritePipeline, WriteRequest
from extraction.delta_tools import DeltaQueryRequest, delta_query
from tests.harness.plan_bundle import build_plan_manifest_for_sql, persist_plan_artifacts
from tests.harness.profiles import (
    ConformanceBackendConfig,
    conformance_profile_with_sink,
    resolve_conformance_backend_config,
)
from tests.test_helpers.arrow_seed import register_arrow_table

_MISSING_DELTA_VERSION_MSG = "Delta smoke round-trip did not produce a committed Delta version."


@dataclass(frozen=True)
class DeltaSmokeScenario:
    """Inputs for the Delta/DataFusion round-trip smoke scenario."""

    tmp_path: Path
    source_table_name: str = "events_source"
    query_table_name: str = "events"
    delta_dir_name: str = "events_delta"
    artifacts_dir_name: str = "conformance_artifacts"


@dataclass(frozen=True)
class DeltaSmokeResult:
    """Output bundle for Delta/DataFusion smoke assertions."""

    rows: tuple[dict[str, object], ...]
    delta_version: int
    manifest_path: Path
    details_path: Path
    runtime_capabilities: tuple[Mapping[str, object], ...]
    provider_artifacts: tuple[Mapping[str, object], ...]
    service_provider_artifacts: tuple[Mapping[str, object], ...]
    strict_native_provider_enabled: bool
    backend: str
    table_uri: str
    storage_options: Mapping[str, str]
    log_storage_options: Mapping[str, str]


def run_delta_smoke_round_trip(
    scenario: DeltaSmokeScenario,
    *,
    backend_config: ConformanceBackendConfig | None = None,
) -> DeltaSmokeResult:
    """Execute the canonical Delta/DataFusion smoke round-trip.

    Args:
        scenario: Description.

    Returns:
        DeltaSmokeResult: Result.

    Raises:
        RuntimeError: If the operation cannot be completed.
    """
    backend = backend_config or resolve_conformance_backend_config("fs")
    table_uri = backend.table_uri(table_name=scenario.delta_dir_name, root_dir=scenario.tmp_path)
    artifacts_root = scenario.tmp_path / scenario.artifacts_dir_name
    profile, sink = conformance_profile_with_sink(
        plan_artifacts_root=str(artifacts_root),
    )
    ctx = profile.session_context()
    source = register_arrow_table(
        ctx,
        name=scenario.source_table_name,
        value=pa.table({"id": [1, 2], "label": ["a", "b"]}),
    )
    pipeline = WritePipeline(ctx, runtime_profile=profile)
    write_result = pipeline.write(
        WriteRequest(
            source=source,
            destination=table_uri,
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
            format_options={
                "storage_options": dict(backend.storage_options),
                "log_storage_options": dict(backend.log_storage_options),
            },
        )
    )
    if write_result.delta_result is None or write_result.delta_result.version is None:
        raise RuntimeError(_MISSING_DELTA_VERSION_MSG)

    plan_manifest, plan_details = build_plan_manifest_for_sql(
        ctx=ctx,
        session_runtime=profile.session_runtime(),
        sql=f"SELECT id, label FROM {scenario.source_table_name} ORDER BY id",
    )
    manifest_path, details_path = persist_plan_artifacts(
        output_dir=artifacts_root,
        plan_manifest=plan_manifest,
        plan_details=plan_details,
    )
    sql = f"SELECT id, label FROM {scenario.query_table_name} ORDER BY id"
    rows = _rows_from_reader(
        delta_query(
            DeltaQueryRequest(
                path=table_uri,
                storage_options=dict(backend.storage_options),
                log_storage_options=dict(backend.log_storage_options),
                table_name=scenario.query_table_name,
                runtime_profile=profile,
                builder=lambda ctx, table_name: ctx.sql(
                    f"SELECT id, label FROM {table_name} ORDER BY id"
                ),
                query_label=sql,
            )
        )
    )
    artifacts = sink.artifacts_snapshot()
    return DeltaSmokeResult(
        rows=rows,
        delta_version=int(write_result.delta_result.version),
        manifest_path=manifest_path,
        details_path=details_path,
        runtime_capabilities=tuple(_artifact_rows(artifacts, "datafusion_runtime_capabilities_v1")),
        provider_artifacts=tuple(_artifact_rows(artifacts, "dataset_provider_mode_v1")),
        service_provider_artifacts=tuple(_artifact_rows(artifacts, "delta_service_provider_v1")),
        strict_native_provider_enabled=profile.features.enforce_delta_ffi_provider,
        backend=backend.kind,
        table_uri=table_uri,
        storage_options=dict(backend.storage_options),
        log_storage_options=dict(backend.log_storage_options),
    )


def _artifact_rows(
    artifacts: Mapping[str, object],
    key: str,
) -> tuple[Mapping[str, object], ...]:
    payload = artifacts.get(key, [])
    if not isinstance(payload, list):
        return ()
    return tuple(dict(entry) for entry in payload if isinstance(entry, Mapping))


def _rows_from_reader(reader: object) -> tuple[dict[str, object], ...]:
    read_all = getattr(reader, "read_all", None)
    if not callable(read_all):
        return ()
    table = read_all()
    to_pydict = getattr(table, "to_pydict", None)
    if not callable(to_pydict):
        return ()
    columns_obj = to_pydict()
    if not isinstance(columns_obj, Mapping):
        return ()
    columns: dict[str, list[object]] = {}
    for key, values in columns_obj.items():
        if not isinstance(key, str):
            continue
        if isinstance(values, Sequence) and not isinstance(values, str):
            columns[key] = list(values)
    column_names = tuple(columns)
    row_values = zip(*(columns[name] for name in column_names), strict=True)
    return tuple(dict(zip(column_names, values, strict=True)) for values in row_values)


__all__ = [
    "DeltaSmokeResult",
    "DeltaSmokeScenario",
    "run_delta_smoke_round_trip",
]
