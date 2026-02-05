"""Delta-backed snapshots of DataFusion metadata caches."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from datafusion_engine.dataset.registration import (
    DatasetRegistrationOptions,
    register_dataset_df,
)
from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.io.write import WriteFormat, WriteMode, WritePipeline, WriteRequest
from datafusion_engine.session.helpers import deregister_table

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


_CACHE_SNAPSHOT_QUERIES: Mapping[str, str] = {
    "df_metadata_cache": "metadata_cache",
    "df_statistics_cache": "statistics_cache",
    "df_list_files_cache": "list_files_cache",
}


@dataclass(frozen=True)
class CacheSnapshotEvent:
    """Diagnostics payload for cache snapshot writes."""

    snapshot_name: str
    cache_table: str
    cache_path: str | None
    snapshot_version: int | None
    error: str | None = None
    event_time_unix_ms: int | None = None

    def to_row(self) -> dict[str, object]:
        """Return a JSON-ready snapshot payload.

        Returns
        -------
        dict[str, object]
            Snapshot payload dictionary.
        """
        return {
            "event_time_unix_ms": self.event_time_unix_ms or int(time.time() * 1000),
            "snapshot_name": self.snapshot_name,
            "cache_table": self.cache_table,
            "cache_path": self.cache_path,
            "snapshot_version": self.snapshot_version,
            "error": self.error,
        }


def snapshot_datafusion_caches(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> list[dict[str, object]]:
    """Snapshot DataFusion cache tables into Delta and register results.

    Returns
    -------
    list[dict[str, object]]
        Diagnostics payloads for each snapshot attempt.
    """
    cache_root = Path(runtime_profile.io_ops.metadata_cache_snapshot_root())
    cache_root.mkdir(parents=True, exist_ok=True)
    pipeline = WritePipeline(ctx, runtime_profile=runtime_profile)
    events: list[dict[str, object]] = []
    for snapshot_name, table_name in _CACHE_SNAPSHOT_QUERIES.items():
        sql = f"SELECT * FROM {table_name}()"
        from datafusion_engine.cache.commit_metadata import (
            CacheCommitMetadataRequest,
            cache_commit_metadata,
        )
        from datafusion_engine.cache.ledger import (
            CacheSnapshotRegistryEntry,
            record_cache_snapshot_registry,
        )
        from obs.otel.cache import cache_span

        try:
            with cache_span(
                "cache.metadata.snapshot",
                cache_policy="metadata_snapshot",
                cache_scope="metadata",
                operation="snapshot",
                attributes={
                    "snapshot_name": snapshot_name,
                    "cache_table": table_name,
                },
            ) as (_span, set_result):
                df = ctx.sql(sql)
                path = cache_root / snapshot_name
                commit_metadata = cache_commit_metadata(
                    CacheCommitMetadataRequest(
                        operation="cache_snapshot",
                        cache_policy="metadata_snapshot",
                        cache_scope="metadata",
                        cache_key=snapshot_name,
                        extra={"cache_table": table_name},
                    )
                )
                result = pipeline.write(
                    WriteRequest(
                        source=df,
                        destination=str(path),
                        format=WriteFormat.DELTA,
                        mode=WriteMode.OVERWRITE,
                        format_options={"commit_metadata": commit_metadata},
                    )
                )
                set_result("write")
        except (RuntimeError, TypeError, ValueError) as exc:
            record_cache_snapshot_registry(
                runtime_profile,
                entry=CacheSnapshotRegistryEntry(
                    snapshot_name=snapshot_name,
                    cache_table=table_name,
                    cache_path=None,
                    snapshot_version=None,
                    error=str(exc),
                ),
                ctx=ctx,
            )
            events.append(
                CacheSnapshotEvent(
                    snapshot_name=snapshot_name,
                    cache_table=table_name,
                    cache_path=None,
                    snapshot_version=None,
                    error=str(exc),
                ).to_row()
            )
            continue
        path = cache_root / snapshot_name
        location = DatasetLocation(path=str(path), format="delta")
        deregister_table(ctx, snapshot_name)
        register_dataset_df(
            ctx,
            name=snapshot_name,
            location=location,
            options=DatasetRegistrationOptions(runtime_profile=runtime_profile),
        )
        snapshot_version = result.delta_result.version if result.delta_result else None
        record_cache_snapshot_registry(
            runtime_profile,
            entry=CacheSnapshotRegistryEntry(
                snapshot_name=snapshot_name,
                cache_table=table_name,
                cache_path=str(path),
                snapshot_version=snapshot_version,
                error=None,
            ),
            ctx=ctx,
        )
        events.append(
            CacheSnapshotEvent(
                snapshot_name=snapshot_name,
                cache_table=table_name,
                cache_path=str(path),
                snapshot_version=snapshot_version,
            ).to_row()
        )
    return events


__all__ = ["CacheSnapshotEvent", "snapshot_datafusion_caches"]
