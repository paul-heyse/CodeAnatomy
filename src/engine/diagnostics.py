"""Consolidated diagnostics recording for engine operations."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from datafusion_engine.session.facade import ExecutionResult
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.views.artifacts import DataFusionViewArtifact
    from serde_schema_registry import ArtifactSpec


@dataclass(frozen=True)
class PlanExecutionEvent:
    """Typed payload for plan execution diagnostics."""

    plan_id: str
    result_kind: str
    rows: int | None
    plan_fingerprint: str | None

    def to_payload(self) -> dict[str, object]:
        """Return payload suitable for diagnostics recording.

        Returns:
        -------
        dict[str, object]
            Diagnostics event payload.
        """
        payload: dict[str, object] = {
            "plan_id": self.plan_id,
            "result_kind": self.result_kind,
            "rows": self.rows,
        }
        if self.plan_fingerprint is not None:
            payload["plan_fingerprint"] = self.plan_fingerprint
        return payload


@dataclass(frozen=True)
class ExtractWriteEvent:
    """Typed payload for extract output writes."""

    dataset: str
    mode: str
    path: str
    file_format: str
    rows: int | None
    copy_sql: str | None
    copy_options: Mapping[str, object] | None
    delta_version: int | None

    def to_payload(self) -> dict[str, object]:
        """Return payload suitable for diagnostics recording.

        Returns:
        -------
        dict[str, object]
            Diagnostics event payload.
        """
        return {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": self.dataset,
            "mode": self.mode,
            "path": self.path,
            "format": self.file_format,
            "rows": self.rows,
            "copy_sql": self.copy_sql,
            "copy_options": dict(self.copy_options) if self.copy_options is not None else None,
            "delta_version": self.delta_version,
        }


@dataclass(frozen=True)
class ExtractQualityEvent:
    """Typed payload for extract quality diagnostics."""

    dataset: str
    stage: str
    status: str
    rows: int | None
    location_path: str | None
    location_format: str | None
    issue: str | None = None
    extractor: str | None = None
    plan_fingerprint: str | None = None
    plan_signature: str | None = None

    def to_payload(self) -> dict[str, object]:
        """Return payload suitable for diagnostics recording.

        Returns:
        -------
        dict[str, object]
            Diagnostics event payload.
        """
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": self.dataset,
            "stage": self.stage,
            "status": self.status,
            "rows": self.rows,
            "location_path": self.location_path,
            "location_format": self.location_format,
        }
        if self.issue is not None:
            payload["issue"] = self.issue
        if self.extractor is not None:
            payload["extractor"] = self.extractor
        if self.plan_fingerprint is not None:
            payload["plan_fingerprint"] = self.plan_fingerprint
        if self.plan_signature is not None:
            payload["plan_signature"] = self.plan_signature
        return payload


@dataclass
class EngineEventRecorder:
    """Unified diagnostics recorder for engine operations.

    Provides typed methods for recording common engine events to the
    diagnostics sink. Centralizes diagnostics formatting and recording
    logic that was previously scattered across engine modules.
    """

    runtime_profile: DataFusionRuntimeProfile | None

    @property
    def _sink(self) -> object | None:
        if self.runtime_profile is None:
            return None
        return self.runtime_profile.diagnostics.diagnostics_sink

    def record_artifact(self, name: ArtifactSpec | str, payload: Mapping[str, object]) -> None:
        """Record a diagnostics artifact.

        Parameters
        ----------
        name
            Artifact spec or string name for the diagnostics sink.
        payload
            Artifact payload to record.
        """
        if self._sink is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact

        record_artifact(self.runtime_profile, name, payload)

    def record_events(self, name: str, events: list[Mapping[str, object]]) -> None:
        """Record diagnostics events.

        Parameters
        ----------
        name
            Event stream name.
        events
            List of event payloads to record.
        """
        if self._sink is None:
            return
        from datafusion_engine.lineage.diagnostics import record_events

        record_events(self.runtime_profile, name, events)

    def record_plan_execution(
        self,
        *,
        plan_id: str,
        result: ExecutionResult,
        view_artifact: DataFusionViewArtifact | None = None,
    ) -> None:
        """Record a plan execution event.

        Parameters
        ----------
        plan_id
            Identifier for the executed plan.
        result
            Execution result from DataFusion.
        view_artifact
            Optional view artifact with plan fingerprint.
        """
        rows = result.table.num_rows if result.table is not None else None
        event = PlanExecutionEvent(
            plan_id=plan_id,
            result_kind=result.kind.value,
            rows=rows,
            plan_fingerprint=view_artifact.plan_fingerprint if view_artifact else None,
        )
        self.record_artifact("plan_execute_v1", event.to_payload())

    def record_extract_write(
        self,
        event: ExtractWriteEvent,
    ) -> None:
        """Record an extract output write event.

        Parameters
        ----------
        event
            Extract write event payload.
        """
        self.record_artifact("datafusion_extract_output_writes_v1", event.to_payload())

    def record_extract_quality_events(self, events: Sequence[ExtractQualityEvent]) -> None:
        """Record extract quality event rows.

        Parameters
        ----------
        events
            Extract quality event payloads.
        """
        if not events:
            return
        rows: list[Mapping[str, object]] = [event.to_payload() for event in events]
        self.record_events("extract_quality_v1", rows)

    def record_diskcache_stats(self) -> None:
        """Record diskcache stats for known cache kinds."""
        if self._sink is None or self.runtime_profile is None:
            return
        profile = self.runtime_profile.policies.diskcache_profile
        if profile is None:
            return
        from cache.diskcache_factory import DiskCacheKind, cache_for_kind, diskcache_stats_snapshot

        events: list[Mapping[str, object]] = []
        for kind in ("plan", "extract", "schema", "repo_scan", "runtime", "coordination"):
            cache = cache_for_kind(profile, cast("DiskCacheKind", kind))
            settings = profile.settings_for(cast("DiskCacheKind", kind))
            payload = diskcache_stats_snapshot(cache)
            payload.update(
                {
                    "kind": kind,
                    "profile_key": self.runtime_profile.context_cache_key(),
                    "size_limit_bytes": settings.size_limit_bytes,
                    "eviction_policy": settings.eviction_policy,
                    "cull_limit": settings.cull_limit,
                    "shards": settings.shards,
                    "statistics": settings.statistics,
                    "tag_index": settings.tag_index,
                    "disk_min_file_size": settings.disk_min_file_size,
                    "sqlite_journal_mode": settings.sqlite_journal_mode,
                    "sqlite_mmap_size": settings.sqlite_mmap_size,
                    "sqlite_synchronous": settings.sqlite_synchronous,
                }
            )
            events.append(payload)
        if events:
            self.record_events("diskcache_stats_v1", events)

    def record_delta_maintenance(
        self,
        *,
        dataset: str | None,
        path: str,
        operation: str,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Record a Delta maintenance operation event.

        Parameters
        ----------
        dataset
            Dataset name if known.
        path
            Delta table path.
        operation
            Maintenance operation type.
        extra
            Additional payload fields.
        """
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": dataset,
            "path": path,
            "operation": operation,
        }
        if extra:
            payload.update(extra)
        self.record_artifact("delta_maintenance_v1", payload)

    def record_delta_query(
        self,
        *,
        path: str,
        sql: str | None,
        table_name: str,
        engine: str,
    ) -> None:
        """Record a Delta query execution event.

        Parameters
        ----------
        path
            Delta table path.
        sql
            SQL query executed.
        table_name
            Table name used in query.
        engine
            Query engine used.
        """
        payload: dict[str, object] = {
            "event_time_unix_ms": int(time.time() * 1000),
            "path": path,
            "sql": sql,
            "table_name": table_name,
            "engine": engine,
        }
        self.record_artifact("delta_query_v1", payload)


__all__ = ["EngineEventRecorder", "ExtractQualityEvent", "PlanExecutionEvent"]
