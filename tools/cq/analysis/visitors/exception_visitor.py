"""Exception-analysis AST visitor."""

from __future__ import annotations

import ast
from collections.abc import Callable

_MAX_MESSAGE_LEN = 120
_MESSAGE_TRIM = 117


class ExceptionVisitor[RaiseSiteT, CatchSiteT](ast.NodeVisitor):
    """Collect raise/catch sites from an AST."""

    def __init__(
        self,
        file: str,
        *,
        make_raise_site: Callable[..., RaiseSiteT],
        make_catch_site: Callable[..., CatchSiteT],
        safe_unparse: Callable[..., str],
    ) -> None:
        """Initialize visitor state and injected site factory callables."""
        self.file = file
        self.raises: list[RaiseSiteT] = []
        self.catches: list[CatchSiteT] = []
        self._current_function = "<module>"
        self._current_class: str | None = None
        self._make_raise_site = make_raise_site
        self._make_catch_site = make_catch_site
        self._safe_unparse = safe_unparse

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Track function scope while visiting nested statements."""
        old_func = self._current_function
        self._current_function = node.name
        self.generic_visit(node)
        self._current_function = old_func

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Track async-function scope while visiting nested statements."""
        old_func = self._current_function
        self._current_function = node.name
        self.generic_visit(node)
        self._current_function = old_func

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Track class scope while visiting nested statements."""
        old_class = self._current_class
        self._current_class = node.name
        self.generic_visit(node)
        self._current_class = old_class

    def visit_Raise(self, node: ast.Raise) -> None:
        """Record raise-site evidence for explicit raises and reraises."""
        if node.exc is None:
            self.raises.append(
                self._make_raise_site(
                    file=self.file,
                    line=node.lineno,
                    exception_type="<reraise>",
                    in_function=self._current_function,
                    in_class=self._current_class,
                    is_reraise=True,
                )
            )
        else:
            self.raises.append(
                self._make_raise_site(
                    file=self.file,
                    line=node.lineno,
                    exception_type=self._extract_exception_type(node.exc),
                    in_function=self._current_function,
                    in_class=self._current_class,
                    message=self._extract_message(node.exc),
                )
            )
        self.generic_visit(node)

    def visit_Try(self, node: ast.Try) -> None:
        """Record catch-site evidence from try/except handlers."""
        for handler in node.handlers:
            exc_types: list[str] = []
            is_bare = False

            if handler.type is None:
                is_bare = True
                exc_types = ["<any>"]
            elif isinstance(handler.type, ast.Tuple):
                for elt in handler.type.elts:
                    exc_types.append(self._get_name(elt))
            else:
                exc_types.append(self._get_name(handler.type))

            has_handler = bool(handler.body)
            if len(handler.body) == 1 and isinstance(handler.body[0], ast.Pass):
                has_handler = False

            reraises = any(isinstance(stmt, ast.Raise) for stmt in handler.body)

            self.catches.append(
                self._make_catch_site(
                    file=self.file,
                    line=handler.lineno,
                    exception_types=tuple(exc_types),
                    in_function=self._current_function,
                    in_class=self._current_class,
                    has_handler=has_handler,
                    is_bare_except=is_bare,
                    reraises=reraises,
                )
            )

        self.generic_visit(node)

    def _extract_exception_type(self, exc: ast.expr) -> str:
        if isinstance(exc, ast.Name):
            return exc.id
        if isinstance(exc, ast.Call):
            return self._get_name(exc.func)
        if isinstance(exc, ast.Attribute):
            return self._safe_unparse(exc, default=exc.attr)
        return "<unknown>"

    @staticmethod
    def _extract_message(exc: ast.expr) -> str | None:
        if isinstance(exc, ast.Call) and exc.args:
            first_arg = exc.args[0]
            if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                msg = first_arg.value
                if len(msg) > _MAX_MESSAGE_LEN:
                    return f"{msg[:_MESSAGE_TRIM]}..."
                return msg
        return None

    def _get_name(self, node: ast.expr) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return self._safe_unparse(node, default=node.attr)
        return "<unknown>"


__all__ = ["ExceptionVisitor"]
