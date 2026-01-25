"""
Unified function registry for UDFs with performance ladder.

This module provides a single source of truth for backend-native UDFs and
explicitly disallows Python/PyArrow/Pandas UDF lanes in DataFusion execution.
The registry integrates with DataFusion introspection and Ibis builtin UDF
decorators.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, get_type_hints

import ibis

if TYPE_CHECKING:
    from ibis.backends.datafusion import Backend

    from datafusion_engine.introspection import IntrospectionSnapshot


logger = logging.getLogger(__name__)


class UDFLane(Enum):
    """
    Performance tier for UDF execution.

    Listed in order of preference (fastest to slowest).
    """

    BUILTIN = auto()  # Backend native - fastest
    PYARROW = auto()  # Arrow compute kernels - vectorized
    PANDAS = auto()  # Pandas vectorized - good
    PYTHON = auto()  # Row-by-row - slow (last resort)


@dataclass(frozen=True)
class UDFSignature:
    """
    Type signature for a UDF.

    Attributes
    ----------
    arg_types : tuple[str, ...]
        Ibis type strings for function arguments
    return_type : str
        Ibis type string for return value
    """

    arg_types: tuple[str, ...]
    return_type: str

    @classmethod
    def from_annotations(cls, func: Callable[..., object]) -> UDFSignature:
        """
        Extract signature from function annotations.

        Converts Python type hints to Ibis type strings using a standard
        mapping. Defaults to 'string' for unknown types.

        Parameters
        ----------
        func : Callable
            Function with type annotations

        Returns
        -------
        UDFSignature
            Extracted signature with Ibis type strings
        """
        hints = get_type_hints(func)
        return_hint = hints.pop("return", "string")
        arg_hints = tuple(hints.values())

        return cls(
            arg_types=tuple(cls._hint_to_ibis(h) for h in arg_hints),
            return_type=cls._hint_to_ibis(return_hint),
        )

    @staticmethod
    def _hint_to_ibis(hint: Any) -> str:
        """
        Convert Python type hint to Ibis type string.

        Parameters
        ----------
        hint : Any
            Python type hint

        Returns
        -------
        str
            Ibis type string ('int64', 'float64', 'string', 'boolean')
        """
        type_map = {
            int: "int64",
            float: "float64",
            str: "string",
            bool: "boolean",
        }
        return type_map.get(hint, "string")


@dataclass(frozen=True)
class UDFSpec:
    """
    Specification for a user-defined function.

    Includes implementation, lane selection, and metadata for registration
    with Ibis backend.

    Attributes
    ----------
    name : str
        Function name as seen in SQL
    lane : UDFLane
        Performance tier for execution
    implementation : Callable
        Python implementation (or placeholder for builtins)
    signature : UDFSignature
        Type signature
    description : str | None
        Human-readable description
    catalog : str | None
        Catalog scope for builtin functions
    database : str | None
        Database scope for builtin functions
    """

    name: str
    lane: UDFLane
    implementation: Callable[..., object]
    signature: UDFSignature
    description: str | None = None
    catalog: str | None = None
    database: str | None = None

    def register(self, backend: Backend) -> None:  # noqa: ARG002
        """
        Register UDF with Ibis backend using appropriate decorator.

        Uses lane-specific Ibis decorators to achieve optimal performance:
        - BUILTIN: Reference existing backend function
        - PYARROW/PANDAS/PYTHON: Disabled for Rust-only DataFusion execution

        Parameters
        ----------
        backend : Backend
            Ibis DataFusion backend to register with

        Raises
        ------
        RuntimeError
            Raised when a non-builtin UDF lane is attempted.
        """
        if self.lane != UDFLane.BUILTIN:
            msg = (
                "Python/PyArrow/Pandas UDF lanes are disabled for DataFusion. "
                "Convert this function to a Rust UDF."
            )
            raise RuntimeError(msg)
        match self.lane:
            case UDFLane.BUILTIN:
                # Reference existing backend function
                @ibis.udf.scalar.builtin(
                    name=self.name,
                    catalog=self.catalog,
                    database=self.database,
                )
                def _builtin() -> None: ...

            case UDFLane.PYARROW | UDFLane.PANDAS | UDFLane.PYTHON:
                msg = (
                    "Python/PyArrow/Pandas UDF lanes are disabled for DataFusion. "
                    "Convert this function to a Rust UDF."
                )
                raise RuntimeError(msg)


@dataclass
class FunctionRegistry:
    """
    Unified function registry for all UDFs.

    Provides single source of truth for function availability and lane selection.
    Automatically prefers faster lanes when multiple implementations exist.

    Attributes
    ----------
    _specs : dict[str, UDFSpec]
        Registered function specifications
    _registered : set[str]
        Names of functions already registered with backend
    """

    _specs: dict[str, UDFSpec] = field(default_factory=dict)
    _registered: set[str] = field(default_factory=set)

    def register_spec(self, spec: UDFSpec) -> None:
        """
        Register a UDF specification.

        If a function with the same name already exists, keeps the faster
        lane implementation (lower enum value = faster).

        Parameters
        ----------
        spec : UDFSpec
            Function specification to register
        """
        if spec.name in self._specs:
            existing = self._specs[spec.name]
            if existing.lane.value < spec.lane.value:
                logger.info(
                    "Keeping faster lane for '%s': %s over %s",
                    spec.name,
                    existing.lane.name,
                    spec.lane.name,
                )
                return

        self._specs[spec.name] = spec

    def register_builtin(
        self,
        name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> None:
        """
        Register reference to backend builtin function.

        Parameters
        ----------
        name : str
            Function name
        catalog : str | None
            Catalog scope (optional)
        database : str | None
            Database scope (optional)
        """
        self.register_spec(
            UDFSpec(
                name=name,
                lane=UDFLane.BUILTIN,
                implementation=lambda: None,  # Placeholder
                signature=UDFSignature((), "any"),
                catalog=catalog,
                database=database,
            )
        )

    @staticmethod
    def register_pyarrow(
        name: str,
        _func: Callable[..., object],
        _signature: UDFSignature | None = None,
    ) -> None:
        """
        Register PyArrow-based UDF (disabled in Rust-only DataFusion mode).

        Parameters
        ----------
        name : str
            Function name
        _func : Callable
            PyArrow-vectorized implementation (unused)
        _signature : UDFSignature | None
            Type signature (unused)

        Raises
        ------
        RuntimeError
            Raised when PyArrow UDFs are requested.
        """
        msg = (
            "PyArrow UDFs are disabled for DataFusion. "
            f"Convert '{name}' to a Rust UDF."
        )
        raise RuntimeError(msg)

    @staticmethod
    def register_pandas(
        name: str,
        _func: Callable[..., object],
        _signature: UDFSignature | None = None,
    ) -> None:
        """
        Register Pandas-based UDF (disabled in Rust-only DataFusion mode).

        Parameters
        ----------
        name : str
            Function name
        _func : Callable
            Pandas-vectorized implementation (unused)
        _signature : UDFSignature | None
            Type signature (unused)

        Raises
        ------
        RuntimeError
            Raised when Pandas UDFs are requested.
        """
        msg = (
            "Pandas UDFs are disabled for DataFusion. "
            f"Convert '{name}' to a Rust UDF."
        )
        raise RuntimeError(msg)

    @staticmethod
    def register_python(
        name: str,
        _func: Callable[..., object],
        _signature: UDFSignature | None = None,
    ) -> None:
        """
        Register Python row-by-row UDF (disabled in Rust-only DataFusion mode).

        Parameters
        ----------
        name : str
            Function name
        _func : Callable
            Row-by-row Python implementation (unused)
        _signature : UDFSignature | None
            Type signature (unused)

        Raises
        ------
        RuntimeError
            Raised when Python UDFs are requested.
        """
        msg = (
            "Python UDFs are disabled for DataFusion. "
            f"Convert '{name}' to a Rust UDF."
        )
        raise RuntimeError(msg)

    def apply_to_backend(self, backend: Backend) -> None:
        """
        Register all UDFs with the backend.

        Only registers functions that haven't been registered yet.
        This method is idempotent.

        Parameters
        ----------
        backend : Backend
            Ibis DataFusion backend to register with
        """
        for name, spec in self._specs.items():
            if name not in self._registered:
                spec.register(backend)
                self._registered.add(name)

    def merge_from_introspection(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> None:
        """
        Merge builtin functions from introspection.

        Populates the registry with backend-native functions that don't
        need custom implementations. Only adds functions that aren't
        already registered.

        Parameters
        ----------
        snapshot : IntrospectionSnapshot
            Captured catalog state with function metadata
        """
        for name in snapshot.function_signatures():
            if name not in self._specs:
                self.register_builtin(name)

    def get_lane_stats(self) -> dict[UDFLane, int]:
        """
        Get count of functions by lane.

        Returns
        -------
        dict[UDFLane, int]
            Mapping of lane to function count
        """
        stats = dict.fromkeys(UDFLane, 0)
        for spec in self._specs.values():
            stats[spec.lane] += 1
        return stats

    def list_slow_functions(self) -> list[str]:
        """
        List functions using slow Python lane.

        Useful for identifying optimization opportunities.

        Returns
        -------
        list[str]
            Names of functions using Python row-by-row execution
        """
        return [name for name, spec in self._specs.items() if spec.lane == UDFLane.PYTHON]
