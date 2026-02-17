"""Side-effect-analysis AST visitor."""

from __future__ import annotations

import ast
from collections.abc import Callable, Mapping


class SideEffectVisitor[SideEffectT](ast.NodeVisitor):
    """Detect module-level side effects."""

    def __init__(
        self,
        file: str,
        *,
        make_side_effect: Callable[..., SideEffectT],
        safe_unparse: Callable[..., str],
        safe_top_level: set[str],
        ambient_patterns: Mapping[str, str],
        is_main_guard: Callable[[ast.If], bool],
    ) -> None:
        """Initialize visitor state and injected side-effect classification helpers."""
        self.file = file
        self.effects: list[SideEffectT] = []
        self._in_def = 0
        self._in_main_guard = False
        self._make_side_effect = make_side_effect
        self._safe_unparse = safe_unparse
        self._safe_top_level = safe_top_level
        self._ambient_patterns = dict(ambient_patterns)
        self._is_main_guard = is_main_guard

    def visit_If(self, node: ast.If) -> None:
        """Track top-level main-guard blocks while traversing conditional bodies."""
        if self._is_main_guard(node) and self._in_def == 0:
            self._in_main_guard = True
            self.generic_visit(node)
            self._in_main_guard = False
        else:
            self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Increment definition depth when entering a function definition."""
        self._in_def += 1
        self.generic_visit(node)
        self._in_def -= 1

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Increment definition depth when entering an async function definition."""
        self._in_def += 1
        self.generic_visit(node)
        self._in_def -= 1

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Increment definition depth when entering a class definition."""
        self._in_def += 1
        self.generic_visit(node)
        self._in_def -= 1

    def visit_Call(self, node: ast.Call) -> None:
        """Record top-level call expressions that are not explicitly safe."""
        if self._in_def == 0 and not self._in_main_guard:
            callee = self._format_expr(node.func)
            if callee not in self._safe_top_level:
                self.effects.append(
                    self._make_side_effect(
                        file=self.file,
                        line=node.lineno,
                        kind="top_level_call",
                        description=f"Import-time call: {callee}(...)",
                    )
                )
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        """Record top-level subscript assignment mutations."""
        if self._in_def == 0 and not self._in_main_guard:
            for target in node.targets:
                if isinstance(target, ast.Subscript):
                    target_str = self._format_expr(target)
                    self.effects.append(
                        self._make_side_effect(
                            file=self.file,
                            line=node.lineno,
                            kind="global_write",
                            description=f"Module-level mutation: {target_str} = ...",
                        )
                    )
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        """Record top-level augmented assignment mutations."""
        if self._in_def == 0 and not self._in_main_guard:
            target_str = self._format_expr(node.target)
            self.effects.append(
                self._make_side_effect(
                    file=self.file,
                    line=node.lineno,
                    kind="global_write",
                    description=f"Module-level augmented assign: {target_str} {ast.dump(node.op)} ...",
                )
            )
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        """Record configured ambient-read attribute access patterns."""
        if self._in_def == 0 and not self._in_main_guard:
            full = self._format_expr(node)
            for pattern, category in self._ambient_patterns.items():
                if full.endswith(pattern) or pattern in full:
                    self.effects.append(
                        self._make_side_effect(
                            file=self.file,
                            line=node.lineno,
                            kind="ambient_read",
                            description=f"Ambient read ({category}): {full}",
                        )
                    )
                    break
        self.generic_visit(node)

    def _format_expr(self, node: ast.AST) -> str:
        try:
            return self._safe_unparse(node, default="<unknown>")
        except TypeError:
            try:
                value = self._safe_unparse(node)
            except (RuntimeError, TypeError, ValueError):
                return "<unknown>"
            return value if isinstance(value, str) else "<unknown>"


__all__ = ["SideEffectVisitor"]
