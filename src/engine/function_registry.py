"""Unified function registry across execution lanes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Literal

from datafusion_engine.function_factory import DEFAULT_RULE_PRIMITIVES, RulePrimitive

ExecutionLane = Literal["kernel", "datafusion", "ibis", "acero"]


@dataclass(frozen=True)
class FunctionParamSpec:
    """Parameter specification for a registered function."""

    name: str
    dtype: str

    def payload(self) -> dict[str, str]:
        """Return a JSON-ready payload for the parameter.

        Returns
        -------
        dict[str, str]
            JSON-ready parameter payload.
        """
        return {"name": self.name, "dtype": self.dtype}


@dataclass(frozen=True)
class FunctionSpec:
    """Cross-lane function specification."""

    name: str
    params: tuple[FunctionParamSpec, ...]
    return_type: str
    volatility: str
    lanes: tuple[ExecutionLane, ...]
    rewrite_tags: tuple[str, ...] = ()
    catalog: str | None = None
    database: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-serializable payload for the spec.

        Returns
        -------
        dict[str, object]
            JSON-serializable spec payload.
        """
        return {
            "name": self.name,
            "params": [param.payload() for param in self.params],
            "return_type": self.return_type,
            "volatility": self.volatility,
            "lanes": list(self.lanes),
            "rewrite_tags": list(self.rewrite_tags),
            "catalog": self.catalog,
            "database": self.database,
        }


@dataclass(frozen=True)
class FunctionRegistry:
    """Registry of available function specs by name."""

    specs: dict[str, FunctionSpec] = field(default_factory=dict)
    lane_precedence: tuple[ExecutionLane, ...] = ("kernel", "datafusion", "ibis", "acero")

    def resolve_lane(self, name: str) -> ExecutionLane | None:
        """Return the preferred execution lane for a function name.

        Returns
        -------
        ExecutionLane | None
            Selected execution lane or ``None`` when not found.
        """
        spec = self.specs.get(name)
        if spec is None:
            return None
        for lane in self.lane_precedence:
            if lane in spec.lanes:
                return lane
        return None

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for the registry.

        Returns
        -------
        dict[str, object]
            JSON-ready registry payload.
        """
        return {
            "specs": {name: spec.payload() for name, spec in sorted(self.specs.items())},
            "lane_precedence": list(self.lane_precedence),
        }

    def fingerprint(self) -> str:
        """Return a stable fingerprint for the registry contents.

        Returns
        -------
        str
            SHA-256 fingerprint of the registry payload.
        """
        payload = self.payload()
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def build_function_registry(
    *,
    primitives: tuple[RulePrimitive, ...] = DEFAULT_RULE_PRIMITIVES,
    lane_precedence: tuple[ExecutionLane, ...] = ("kernel", "datafusion", "ibis", "acero"),
) -> FunctionRegistry:
    """Build the default function registry.

    Returns
    -------
    FunctionRegistry
        Registry configured from the provided primitives.
    """
    ibis_lane = {primitive.name for primitive in primitives}
    kernel_lane = {"stable_hash64", "stable_hash128", "position_encoding_norm", "col_to_byte"}
    specs: dict[str, FunctionSpec] = {}
    for primitive in primitives:
        lanes: list[ExecutionLane] = ["datafusion"]
        if primitive.name in ibis_lane:
            lanes.append("ibis")
        if primitive.name in kernel_lane:
            lanes.append("kernel")
        specs[primitive.name] = FunctionSpec(
            name=primitive.name,
            params=tuple(FunctionParamSpec(param.name, param.dtype) for param in primitive.params),
            return_type=primitive.return_type,
            volatility=primitive.volatility,
            lanes=tuple(dict.fromkeys(lanes)),
        )
    return FunctionRegistry(specs=specs, lane_precedence=lane_precedence)


def default_function_registry() -> FunctionRegistry:
    """Return the default cross-lane function registry.

    Returns
    -------
    FunctionRegistry
        Default cross-lane registry instance.
    """
    return build_function_registry()


__all__ = [
    "ExecutionLane",
    "FunctionParamSpec",
    "FunctionRegistry",
    "FunctionSpec",
    "build_function_registry",
    "default_function_registry",
]
