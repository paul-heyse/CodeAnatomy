"""Function-catalog snapshot helpers extracted from SchemaIntrospector."""

from __future__ import annotations

from typing import Protocol


class FunctionCatalogIntrospector(Protocol):
    """Protocol for function-catalog snapshot providers."""

    def routines_snapshot(self) -> list[dict[str, object]]:
        """Return function routine rows."""
        ...

    def parameters_snapshot(self) -> list[dict[str, object]]:
        """Return function parameter rows."""
        ...

    def function_catalog_snapshot(
        self, *, include_parameters: bool = True
    ) -> list[dict[str, object]]:
        """Return joined function-catalog rows."""
        ...


def routines_snapshot(introspector: FunctionCatalogIntrospector) -> list[dict[str, object]]:
    """Return routine metadata rows from the schema introspector."""
    return introspector.routines_snapshot()


def parameters_snapshot(introspector: FunctionCatalogIntrospector) -> list[dict[str, object]]:
    """Return routine-parameter metadata rows from the schema introspector."""
    return introspector.parameters_snapshot()


def function_catalog_snapshot(
    introspector: FunctionCatalogIntrospector,
    *,
    include_parameters: bool = True,
) -> list[dict[str, object]]:
    """Return function catalog rows from the schema introspector."""
    return introspector.function_catalog_snapshot(include_parameters=include_parameters)


__all__ = ["function_catalog_snapshot", "parameters_snapshot", "routines_snapshot"]
