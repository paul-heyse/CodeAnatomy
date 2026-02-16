"""Static guard for manifest-authoritative canonical output naming."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

_ALLOWLIST: set[tuple[str, str]] = {
    ("src/datafusion_engine/dataset/semantic_catalog.py", "_register_semantic_outputs"),
    ("src/semantics/catalog/semantic_singletons_registry.py", "<module>"),
    ("src/semantics/compile_context.py", "_resolved_outputs"),
    ("src/semantics/ir_pipeline.py", "_build_cpg_output_rows"),
    ("src/semantics/ir_pipeline.py", "build_semantic_ir"),
    ("src/semantics/ir_pipeline.py", "compile_semantics"),
    ("src/semantics/registry.py", "_build_output_specs"),
    ("src/semantics/registry.py", "SemanticNormalizationSpec.output_name"),
    ("src/semantics/registry.py", "build_semantic_model"),
    ("src/semantics/registry.py", "normalization_output_name"),
}


@dataclass(frozen=True)
class _Violation:
    path: str
    scope: str
    line: int
    col: int


class _CanonicalCallVisitor(ast.NodeVisitor):
    def __init__(self, rel_path: str) -> None:
        self._rel_path = rel_path
        self._scope: list[str] = []
        self.violations: list[_Violation] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()

    def visit_Call(self, node: ast.Call) -> None:
        if not self._is_canonical_call(node):
            self.generic_visit(node)
            return

        has_manifest_kw = any(
            isinstance(keyword, ast.keyword) and keyword.arg == "manifest"
            for keyword in node.keywords
        )
        scope = self._scope_name()
        if not has_manifest_kw and (self._rel_path, scope) not in _ALLOWLIST:
            self.violations.append(
                _Violation(
                    path=self._rel_path,
                    scope=scope,
                    line=node.lineno,
                    col=node.col_offset,
                )
            )
        self.generic_visit(node)

    def _scope_name(self) -> str:
        if not self._scope:
            return "<module>"
        if len(self._scope) == 1:
            return self._scope[0]
        return ".".join(self._scope)

    @staticmethod
    def _is_canonical_call(node: ast.Call) -> bool:
        if isinstance(node.func, ast.Name):
            return node.func.id == "canonical_output_name"
        if isinstance(node.func, ast.Attribute):
            return node.func.attr == "canonical_output_name"
        return False


def test_post_compile_canonical_output_name_calls_require_manifest() -> None:
    """Test post compile canonical output name calls require manifest."""
    repo_root = Path(__file__).resolve().parents[3]
    src_root = repo_root / "src"
    violations: list[_Violation] = []
    for path in sorted(src_root.rglob("*.py")):
        rel_path = path.relative_to(repo_root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        visitor = _CanonicalCallVisitor(rel_path)
        visitor.visit(tree)
        violations.extend(visitor.violations)

    assert not violations, (
        "canonical_output_name(...) calls without manifest= are disallowed outside "
        f"compile/bootstrap allowlist. Violations: {violations}"
    )
