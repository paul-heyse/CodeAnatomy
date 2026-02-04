"""Canonical OpenTelemetry constants for CodeAnatomy."""

from __future__ import annotations

from enum import StrEnum


class MetricName(StrEnum):
    """Canonical metric names."""

    STAGE_DURATION = "codeanatomy.stage.duration"
    TASK_DURATION = "codeanatomy.task.duration"
    DATAFUSION_DURATION = "codeanatomy.datafusion.execute.duration"
    WRITE_DURATION = "codeanatomy.datafusion.write.duration"
    ARTIFACT_COUNT = "codeanatomy.artifact.count"
    ERROR_COUNT = "codeanatomy.error.count"
    DATASET_ROWS = "codeanatomy.dataset.rows"
    DATASET_COLUMNS = "codeanatomy.dataset.columns"
    SCAN_ROW_GROUPS = "codeanatomy.scan.row_groups"
    SCAN_FRAGMENTS = "codeanatomy.scan.fragments"
    CACHE_OPERATION_COUNT = "codeanatomy.cache.operation.count"
    CACHE_OPERATION_DURATION = "codeanatomy.cache.operation.duration"
    STORAGE_OPERATION_COUNT = "codeanatomy.storage.operation.count"
    STORAGE_OPERATION_DURATION = "codeanatomy.storage.operation.duration"


class AttributeName(StrEnum):
    """Canonical attribute names."""

    RUN_ID = "codeanatomy.run_id"
    STAGE = "stage"
    STATUS = "status"
    TASK_KIND = "task_kind"
    PLAN_KIND = "plan_kind"
    DESTINATION = "destination"
    ARTIFACT_KIND = "artifact_kind"
    ERROR_TYPE = "error_type"
    STAGE_NAME = "codeanatomy.stage"
    DATASET = "dataset"
    CACHE_POLICY = "cache.policy"
    CACHE_SCOPE = "cache.scope"
    CACHE_OPERATION = "cache.operation"
    CACHE_RESULT = "cache.result"
    STORAGE_OPERATION = "storage.operation"


class ScopeName(StrEnum):
    """Canonical instrumentation scope names."""

    ROOT = "codeanatomy"
    PIPELINE = "codeanatomy.pipeline"
    EXTRACT = "codeanatomy.extract"
    NORMALIZE = "codeanatomy.normalize"
    PLANNING = "codeanatomy.planning"
    SCHEDULING = "codeanatomy.scheduling"
    DATAFUSION = "codeanatomy.datafusion"
    STORAGE = "codeanatomy.storage"
    HAMILTON = "codeanatomy.hamilton"
    CPG = "codeanatomy.cpg"
    OBS = "codeanatomy.obs"
    DIAGNOSTICS = "codeanatomy.diagnostics"
    SEMANTICS = "codeanatomy.semantics"


class ResourceAttribute(StrEnum):
    """Canonical resource attribute names."""

    SERVICE_NAME = "service.name"
    SERVICE_VERSION = "service.version"
    SERVICE_NAMESPACE = "service.namespace"
    SERVICE_INSTANCE_ID = "service.instance.id"
    DEPLOYMENT_ENVIRONMENT = "deployment.environment.name"


__all__ = [
    "AttributeName",
    "MetricName",
    "ResourceAttribute",
    "ScopeName",
]
