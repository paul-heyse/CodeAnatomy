"""Import-analysis AST visitor."""

from __future__ import annotations

import ast
from collections.abc import Callable


class ImportVisitor[ImportInfoT](ast.NodeVisitor):
    """Extract import statements from a module AST."""

    def __init__(
        self,
        file: str,
        *,
        make_import_info: Callable[..., ImportInfoT],
        resolve_relative_import: Callable[[str, int, str | None], str | None],
    ) -> None:
        """Initialize visitor state and injected import resolution helpers."""
        self.file = file
        self.imports: list[ImportInfoT] = []
        self._make_import_info = make_import_info
        self._resolve_relative_import = resolve_relative_import

    def visit_Import(self, node: ast.Import) -> None:
        """Record `import ...` statements as import-info payloads."""
        for alias in node.names:
            self.imports.append(
                self._make_import_info(
                    file=self.file,
                    line=node.lineno,
                    module=alias.name,
                    is_from=False,
                    alias=alias.asname,
                )
            )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Record `from ... import ...` statements as import-info payloads."""
        module = node.module or ""
        is_relative = node.level > 0

        if is_relative:
            resolved = self._resolve_relative_import(self.file, node.level, node.module)
            if resolved:
                module = resolved

        names = tuple(alias.name for alias in node.names if alias.name != "*")
        self.imports.append(
            self._make_import_info(
                file=self.file,
                line=node.lineno,
                module=module,
                names=names,
                is_from=True,
                is_relative=is_relative,
                level=node.level,
            )
        )


__all__ = ["ImportVisitor"]
