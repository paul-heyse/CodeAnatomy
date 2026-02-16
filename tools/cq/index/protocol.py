"""Protocol for symbol index implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tools.cq.index.def_index import ClassDecl, FnDecl, ModuleInfo


class SymbolIndex(Protocol):
    """Protocol for symbol lookup operations.

    DefIndex is the canonical implementation. This protocol enables
    dependency injection for testing and alternative implementations.
    """

    def all_functions(self) -> Iterator[FnDecl]:
        """Iterate over all function declarations.

        Yields:
        ------
        FnDecl
            Function declarations.
        """
        ...

    def all_classes(self) -> Iterator[ClassDecl]:
        """Iterate over all class declarations.

        Yields:
        ------
        ClassDecl
            Class declarations.
        """
        ...

    def find_function_by_name(self, name: str) -> list[FnDecl]:
        """Find all functions with given name.

        Parameters
        ----------
        name : str
            Function name (not qualified).

        Returns:
        -------
        list[FnDecl]
            Matching declarations.
        """
        ...

    def find_function_by_qualified_name(self, qname: str) -> list[FnDecl]:
        """Find functions by qualified name (Class.method or function).

        Parameters
        ----------
        qname : str
            Qualified name.

        Returns:
        -------
        list[FnDecl]
            Matching declarations.
        """
        ...

    def find_function_keys(self, symbol: str) -> list[str]:
        """Find function keys matching symbol.

        Parameters
        ----------
        symbol : str
            Function or qualified name.

        Returns:
        -------
        list[str]
            Matching function keys.
        """
        ...

    def find_class_by_name(self, name: str) -> list[ClassDecl]:
        """Find all classes with given name.

        Parameters
        ----------
        name : str
            Class name.

        Returns:
        -------
        list[ClassDecl]
            Matching declarations.
        """
        ...

    def find_class_keys(self, name: str) -> list[str]:
        """Find class keys matching name.

        Parameters
        ----------
        name : str
            Class name.

        Returns:
        -------
        list[str]
            Matching class keys.
        """
        ...

    def get_module_for_file(self, file: str) -> ModuleInfo | None:
        """Get module info for file.

        Parameters
        ----------
        file : str
            Relative file path.

        Returns:
        -------
        ModuleInfo | None
            Module info if indexed.
        """
        ...

    def resolve_import_alias(self, file: str, name: str) -> tuple[str | None, str | None]:
        """Resolve import alias in context of a file.

        Parameters
        ----------
        file : str
            File where name is used.
        name : str
            Name or dotted name to resolve.

        Returns:
        -------
        tuple[str | None, str | None]
            (module, symbol) if resolved, else (None, None).
        """
        ...


__all__ = ["SymbolIndex"]
