"""Pydantic runtime model base configuration."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class RuntimeBase(BaseModel):
    """Base class for runtime validation models."""

    model_config = ConfigDict(
        extra="forbid",
        validate_default=True,
        frozen=True,
        revalidate_instances="always",
    )


__all__ = ["RuntimeBase"]
