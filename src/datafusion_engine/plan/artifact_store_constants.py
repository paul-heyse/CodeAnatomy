"""Shared constants for plan-artifact storage."""

from __future__ import annotations

from pathlib import Path

PLAN_ARTIFACTS_TABLE_NAME = "datafusion_plan_artifacts_v10"
WRITE_ARTIFACTS_TABLE_NAME = "datafusion_write_artifacts_v2"
PIPELINE_EVENTS_TABLE_NAME = "datafusion_pipeline_events_v2"
_ARTIFACTS_DIRNAME = PLAN_ARTIFACTS_TABLE_NAME
_WRITE_ARTIFACTS_DIRNAME = WRITE_ARTIFACTS_TABLE_NAME
_PIPELINE_EVENTS_DIRNAME = PIPELINE_EVENTS_TABLE_NAME
_LOCAL_ARTIFACTS_DIRNAME = "artifacts"

try:
    _DEFAULT_ARTIFACTS_ROOT = Path(__file__).resolve().parents[2] / ".artifacts"
except IndexError:
    _DEFAULT_ARTIFACTS_ROOT = Path.cwd() / ".artifacts"

__all__ = [
    "PIPELINE_EVENTS_TABLE_NAME",
    "PLAN_ARTIFACTS_TABLE_NAME",
    "WRITE_ARTIFACTS_TABLE_NAME",
    "_ARTIFACTS_DIRNAME",
    "_DEFAULT_ARTIFACTS_ROOT",
    "_LOCAL_ARTIFACTS_DIRNAME",
    "_PIPELINE_EVENTS_DIRNAME",
    "_WRITE_ARTIFACTS_DIRNAME",
]
