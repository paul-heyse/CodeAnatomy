"""Boundary configuration models for cq CLI."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class CqConfigModel(BaseModel):
    """Boundary configuration model for CQ CLI inputs."""

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    root: str | None = None
    verbose: int | None = None
    output_format: str | None = Field(default=None, alias="format")
    artifact_dir: str | None = None
    save_artifact: bool | None = None
    no_save_artifact: bool | None = None
