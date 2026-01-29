"""MessagePack extension codes and wrappers for msgspec serialization."""

from __future__ import annotations

from dataclasses import dataclass

SCHEMA_EXT_CODE: int = 3
SUBSTRAIT_EXT_CODE: int = 10
LOGICAL_PLAN_PROTO_EXT_CODE: int = 11
OPTIMIZED_PLAN_PROTO_EXT_CODE: int = 12
EXECUTION_PLAN_PROTO_EXT_CODE: int = 13


@dataclass(frozen=True)
class SubstraitBytes:
    """Wrapper for Substrait payload bytes."""

    data: bytes


@dataclass(frozen=True)
class LogicalPlanProtoBytes:
    """Wrapper for logical plan proto bytes."""

    data: bytes


@dataclass(frozen=True)
class OptimizedPlanProtoBytes:
    """Wrapper for optimized plan proto bytes."""

    data: bytes


@dataclass(frozen=True)
class ExecutionPlanProtoBytes:
    """Wrapper for execution plan proto bytes."""

    data: bytes


__all__ = [
    "EXECUTION_PLAN_PROTO_EXT_CODE",
    "LOGICAL_PLAN_PROTO_EXT_CODE",
    "OPTIMIZED_PLAN_PROTO_EXT_CODE",
    "SCHEMA_EXT_CODE",
    "SUBSTRAIT_EXT_CODE",
    "ExecutionPlanProtoBytes",
    "LogicalPlanProtoBytes",
    "OptimizedPlanProtoBytes",
    "SubstraitBytes",
]
