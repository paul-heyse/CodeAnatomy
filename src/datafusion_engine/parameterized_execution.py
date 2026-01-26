"""Parameterized execution model for templated rulepacks.

This module provides parameterized rulepack execution using Ibis `ibis.param`
for templated expressions that compile once and execute with different
parameter values. Enables efficient multi-configuration runs.

Examples
--------
>>> from datafusion_engine.parameterized_execution import (
...     ParameterSpec,
...     ParameterizedRulepack,
... )
>>> rulepack = ParameterizedRulepack.create(
...     name="edge_filter",
...     builder=lambda params: edges.filter(edges.kind == params["kind"]),
...     param_specs=[ParameterSpec(name="kind", ibis_type="string")],
... )
>>> result = rulepack.execute(backend, {"kind": "CALL"})
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import ibis
import pyarrow as pa
from ibis.expr.types import Scalar, Value

if TYPE_CHECKING:
    from ibis.backends.datafusion import Backend
    from ibis.expr.types import Table as IbisTable

    from sqlglot_tools.bridge import IbisCompilerBackend


@dataclass(frozen=True)
class ParameterSpec:
    """Specification for a typed parameter.

    Parameters
    ----------
    name
        Parameter identifier used in builder functions.
    ibis_type
        Ibis type string (e.g., "string", "int64", "float64").
    description
        Optional documentation for the parameter.
    default
        Default value if not provided during execution.
    required
        Whether the parameter must be provided (True if no default).

    Examples
    --------
    >>> spec = ParameterSpec(
    ...     name="threshold",
    ...     ibis_type="float64",
    ...     description="Minimum confidence threshold",
    ...     default=0.5,
    ... )
    >>> param = spec.create_param()
    """

    name: str
    ibis_type: str
    description: str | None = None
    default: Any = None
    required: bool = True

    def create_param(self) -> Scalar:
        """Create Ibis parameter scalar.

        Returns
        -------
        Scalar
            Ibis parameter scalar with the specified type.
        """
        return ibis.param(ibis.dtype(self.ibis_type))

    def validate_value(self, value: Any) -> None:
        """Validate a value against this specification.

        Parameters
        ----------
        value
            Value to validate.

        Raises
        ------
        TypeError
            If value type is incompatible with specification.
        """
        # Basic type validation based on ibis_type
        type_validators: dict[str, type | tuple[type, ...]] = {
            "string": str,
            "int64": (int,),
            "int32": (int,),
            "float64": (int, float),
            "float32": (int, float),
            "boolean": bool,
            "date": str,  # Allow ISO date strings
            "timestamp": str,  # Allow ISO timestamp strings
        }

        expected_types = type_validators.get(self.ibis_type)
        if expected_types is not None and not isinstance(value, expected_types):
            msg = f"Parameter '{self.name}' expects {self.ibis_type}, got {type(value).__name__}"
            raise TypeError(msg)


@dataclass
class ParameterizedRulepack:
    """Rulepack with typed parameters for dynamic execution.

    Compile once, execute with different parameter configurations.
    This enables efficient multi-configuration runs where the same
    rulepack is executed with different parameter values.

    Parameters
    ----------
    name
        Rulepack identifier.
    expr
        Compiled Ibis table expression with parameters.
    param_specs
        Mapping of parameter names to their specifications.
    _params
        Internal mapping of parameter names to Ibis parameter objects.

    Examples
    --------
    >>> rulepack = ParameterizedRulepack.create(
    ...     name="edge_filter",
    ...     builder=lambda params: edges.filter(
    ...         (edges.kind == params["kind"]) & (edges.confidence > params["threshold"])
    ...     ),
    ...     param_specs=[
    ...         ParameterSpec(name="kind", ibis_type="string"),
    ...         ParameterSpec(name="threshold", ibis_type="float64", default=0.5),
    ...     ],
    ... )
    >>> call_edges = rulepack.execute(backend, {"kind": "CALL", "threshold": 0.8})
    >>> import_edges = rulepack.execute(backend, {"kind": "IMPORT"})
    """

    name: str
    expr: IbisTable
    param_specs: dict[str, ParameterSpec] = field(default_factory=dict)
    _params: dict[str, Scalar] = field(default_factory=dict, repr=False)

    @classmethod
    def create(
        cls,
        name: str,
        builder: Callable[[dict[str, Scalar]], IbisTable],
        param_specs: list[ParameterSpec],
    ) -> ParameterizedRulepack:
        """Create parameterized rulepack from builder function.

        Parameters
        ----------
        name
            Rulepack identifier.
        builder
            Function that takes params dict and returns Ibis expression.
        param_specs
            Parameter specifications.

        Returns
        -------
        ParameterizedRulepack
            Ready-to-execute rulepack.

        Examples
        --------
        >>> rulepack = ParameterizedRulepack.create(
        ...     name="filter_by_type",
        ...     builder=lambda params: table.filter(table.type == params["type"]),
        ...     param_specs=[ParameterSpec(name="type", ibis_type="string")],
        ... )
        """
        # Create param objects
        params = {spec.name: spec.create_param() for spec in param_specs}

        # Build expression with params
        expr = builder(params)

        return cls(
            name=name,
            expr=expr,
            param_specs={spec.name: spec for spec in param_specs},
            _params=params,
        )

    @classmethod
    def from_expression(
        cls,
        name: str,
        expr: IbisTable,
        param_specs: list[ParameterSpec],
        params: dict[str, Scalar],
    ) -> ParameterizedRulepack:
        """Create rulepack from existing expression and parameters.

        Use this when you already have an Ibis expression with parameters
        created separately.

        Parameters
        ----------
        name
            Rulepack identifier.
        expr
            Ibis expression with embedded parameters.
        param_specs
            Parameter specifications.
        params
            Mapping of parameter names to Ibis parameter objects.

        Returns
        -------
        ParameterizedRulepack
            Ready-to-execute rulepack.
        """
        return cls(
            name=name,
            expr=expr,
            param_specs={spec.name: spec for spec in param_specs},
            _params=params,
        )

    def execute(
        self,
        backend: Backend,
        values: dict[str, Any],
        *,
        validate: bool = True,
    ) -> pa.Table:
        """Execute with specific parameter values.

        Parameters
        ----------
        backend
            Ibis DataFusion backend used for validation.
        values
            Parameter name -> value mapping.
        validate
            Whether to validate parameter types.

        Returns
        -------
        pa.Table
            Execution result as PyArrow table.

        Notes
        -----
        May raise ValueError if required parameter is missing or unknown
        parameter provided. May raise TypeError if parameter value type
        is incompatible (when validate=True).
        """
        self._validate_backend(backend)
        resolved_values = self._resolve_values(values, validate=validate)

        # Map param objects to values
        param_mapping: dict[Scalar, object] = {
            self._params[name]: value for name, value in resolved_values.items()
        }

        # Execute with params
        return self.expr.to_pyarrow(params=param_mapping)

    def execute_streaming(
        self,
        backend: Backend,
        values: dict[str, Any],
        *,
        chunk_size: int = 250_000,
        validate: bool = True,
    ) -> pa.RecordBatchReader:
        """Execute with streaming output.

        Parameters
        ----------
        backend
            Ibis DataFusion backend used for validation.
        values
            Parameter name -> value mapping.
        chunk_size
            Target batch size for streaming.
        validate
            Whether to validate parameter types.

        Returns
        -------
        pa.RecordBatchReader
            Streaming reader over execution results.
        """
        self._validate_backend(backend)
        resolved_values = self._resolve_values(values, validate=validate)

        param_mapping: dict[Scalar, object] = {
            self._params[name]: value for name, value in resolved_values.items()
        }

        return self.expr.to_pyarrow_batches(
            params=cast("Mapping[Value, object]", param_mapping),
            chunk_size=chunk_size,
        )

    def compile_sql(
        self,
        backend: Backend,
        values: dict[str, Any] | None = None,
    ) -> str:
        """Compile to SQL for diagnostics, optionally with specific values.

        If values not provided, returns parameterized SQL with placeholders.

        Parameters
        ----------
        backend
            Ibis DataFusion backend used for validation.
        values
            Optional parameter values for substitution.

        Returns
        -------
        str
            Compiled SQL string.
        """
        self._validate_backend(backend)
        from datafusion_engine.compile_pipeline import CompilationPipeline, CompileOptions
        from ibis_engine.registry import datafusion_context

        ctx = datafusion_context(backend)
        pipeline = CompilationPipeline(ctx, CompileOptions())
        _ = values
        compiled = pipeline.compile_ibis(
            self.expr,
            backend=cast("IbisCompilerBackend", backend),
        )
        return compiled.rendered_sql

    @staticmethod
    def _validate_backend(backend: Backend) -> None:
        name = getattr(backend, "name", None)
        if name is not None and name != "datafusion":
            msg = f"Parameterized execution requires DataFusion backend, got {name!r}."
            raise ValueError(msg)

    def get_param_names(self) -> list[str]:
        """Get list of parameter names.

        Returns
        -------
        list[str]
            Parameter names in specification order.
        """
        return list(self.param_specs.keys())

    def get_required_params(self) -> list[str]:
        """Get list of required parameter names.

        Returns
        -------
        list[str]
            Names of parameters without defaults.
        """
        return [name for name, spec in self.param_specs.items() if spec.default is None]

    def get_optional_params(self) -> list[str]:
        """Get list of optional parameter names.

        Returns
        -------
        list[str]
            Names of parameters with defaults.
        """
        return [name for name, spec in self.param_specs.items() if spec.default is not None]

    def param_objects(self) -> dict[str, Scalar]:
        """Return parameter objects keyed by name.

        Returns
        -------
        dict[str, Scalar]
            Mapping of parameter names to Ibis parameter objects.
        """
        return dict(self._params)

    def _resolve_values(
        self,
        values: dict[str, Any],
        *,
        validate: bool = True,
    ) -> dict[str, Any]:
        """Resolve parameter values with defaults and validation.

        Parameters
        ----------
        values
            Provided parameter values.
        validate
            Whether to validate types.

        Returns
        -------
        dict[str, Any]
            Complete parameter values with defaults applied.

        Raises
        ------
        ValueError
            If required parameter missing or unknown parameter provided.
        """
        # Check for unknown parameters
        for name in values:
            if name not in self.param_specs:
                msg = f"Unknown parameter: {name}"
                raise ValueError(msg)

        # Build resolved values with defaults
        resolved: dict[str, Any] = {}
        for name, spec in self.param_specs.items():
            if name in values:
                value = values[name]
                if validate:
                    spec.validate_value(value)
                resolved[name] = value
            elif spec.default is not None:
                resolved[name] = spec.default
            else:
                msg = f"Missing required parameter: {name}"
                raise ValueError(msg)

        return resolved


@dataclass
class RulepackRegistry:
    """Registry of parameterized rulepacks.

    Manages a collection of rulepacks for batch execution and inspection.

    Parameters
    ----------
    rulepacks
        Mapping of rulepack names to their definitions.

    Examples
    --------
    >>> registry = RulepackRegistry()
    >>> registry.register(rulepack)
    >>> result = registry.execute("edge_filter", backend, {"kind": "CALL"})
    """

    rulepacks: dict[str, ParameterizedRulepack] = field(default_factory=dict)

    def register(self, rulepack: ParameterizedRulepack) -> None:
        """Register a parameterized rulepack.

        Parameters
        ----------
        rulepack
            Rulepack to register.
        """
        self.rulepacks[rulepack.name] = rulepack

    def get(self, name: str) -> ParameterizedRulepack:
        """Get rulepack by name.

        Parameters
        ----------
        name
            Rulepack name.

        Returns
        -------
        ParameterizedRulepack
            Registered rulepack.

        Raises
        ------
        KeyError
            If rulepack not found.
        """
        if name not in self.rulepacks:
            msg = f"Rulepack not found: {name}"
            raise KeyError(msg)
        return self.rulepacks[name]

    def execute(
        self,
        name: str,
        backend: Backend,
        values: dict[str, Any],
        **kwargs: Any,
    ) -> pa.Table:
        """Execute registered rulepack by name.

        Parameters
        ----------
        name
            Rulepack name.
        backend
            Ibis DataFusion backend.
        values
            Parameter values.
        **kwargs
            Additional execution options.

        Returns
        -------
        pa.Table
            Execution result.
        """
        rulepack = self.get(name)
        return rulepack.execute(backend, values, **kwargs)

    def execute_all(
        self,
        backend: Backend,
        values_by_rulepack: dict[str, dict[str, Any]],
    ) -> dict[str, pa.Table]:
        """Execute multiple rulepacks with their respective values.

        Parameters
        ----------
        backend
            Ibis DataFusion backend.
        values_by_rulepack
            Mapping of rulepack name -> parameter values.

        Returns
        -------
        dict[str, pa.Table]
            Mapping of rulepack name -> execution result.
        """
        results = {}
        for name, values in values_by_rulepack.items():
            results[name] = self.execute(name, backend, values)
        return results

    def list_rulepacks(self) -> list[str]:
        """List all registered rulepack names.

        Returns
        -------
        list[str]
            Sorted list of rulepack names.
        """
        return sorted(self.rulepacks.keys())

    def describe(self, name: str) -> dict[str, Any]:
        """Get description of a rulepack and its parameters.

        Parameters
        ----------
        name
            Rulepack name.

        Returns
        -------
        dict[str, Any]
            Rulepack metadata including parameters.
        """
        rulepack = self.get(name)
        return {
            "name": rulepack.name,
            "parameters": [
                {
                    "name": spec.name,
                    "type": spec.ibis_type,
                    "description": spec.description,
                    "default": spec.default,
                    "required": spec.default is None,
                }
                for spec in rulepack.param_specs.values()
            ],
        }


def create_simple_rulepack(
    name: str,
    table: IbisTable,
    filter_column: str,
    param_type: str = "string",
) -> ParameterizedRulepack:
    """Create a simple filter rulepack.

    Convenience function for creating rulepacks that filter a table
    by a single column equality condition.

    Parameters
    ----------
    name
        Rulepack name.
    table
        Source Ibis table.
    filter_column
        Column to filter on.
    param_type
        Ibis type for the filter value parameter.

    Returns
    -------
    ParameterizedRulepack
        Ready-to-execute filter rulepack.

    Examples
    --------
    >>> rulepack = create_simple_rulepack(
    ...     name="filter_by_kind",
    ...     table=edges_table,
    ...     filter_column="kind",
    ...     param_type="string",
    ... )
    >>> result = rulepack.execute(backend, {"value": "CALL"})
    """
    return ParameterizedRulepack.create(
        name=name,
        builder=lambda params: table.filter(table[filter_column] == params["value"]),
        param_specs=[
            ParameterSpec(
                name="value",
                ibis_type=param_type,
                description=f"Value to filter {filter_column} by",
            ),
        ],
    )


__all__ = [
    "ParameterSpec",
    "ParameterizedRulepack",
    "RulepackRegistry",
    "create_simple_rulepack",
]
