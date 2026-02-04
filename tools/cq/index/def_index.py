"""Definition index for repo-wide symbol resolution.

Builds an index of all function and class definitions across the repository,
tracking import aliases for accurate call resolution.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

_SELF_CLS: set[str] = {"self", "cls"}


@dataclass
class ParamInfo:
    """Information about a function parameter.

    Parameters
    ----------
    name : str
        Parameter name.
    annotation : str | None
        Type annotation as string, if present.
    default : str | None
        Default value as string, if present.
    kind : str
        Parameter kind: POSITIONAL_ONLY, POSITIONAL_OR_KEYWORD, VAR_POSITIONAL,
        KEYWORD_ONLY, VAR_KEYWORD.
    """

    name: str
    annotation: str | None = None
    default: str | None = None
    kind: str = "POSITIONAL_OR_KEYWORD"


@dataclass
class FnDecl:
    """Function declaration information.

    Parameters
    ----------
    name : str
        Function name.
    file : str
        File path relative to repo root.
    line : int
        Line number of definition.
    params : list[ParamInfo]
        Parameter information.
    is_async : bool
        Whether function is async.
    is_method : bool
        Whether function is a method (has self/cls first param).
    class_name : str | None
        Containing class name, if method.
    decorators : list[str]
        Decorator names.
    """

    name: str
    file: str
    line: int
    params: list[ParamInfo] = field(default_factory=list)
    is_async: bool = False
    is_method: bool = False
    class_name: str | None = None
    decorators: list[str] = field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        """Return class.method or function name.

        Returns
        -------
        str
            Qualified function name.
        """
        if self.class_name:
            return f"{self.class_name}.{self.name}"
        return self.name

    @property
    def key(self) -> str:
        """Return unique key: file::qualified_name.

        Returns
        -------
        str
            Unique function key.
        """
        return f"{self.file}::{self.qualified_name}"


@dataclass
class ClassDecl:
    """Class declaration information.

    Parameters
    ----------
    name : str
        Class name.
    file : str
        File path relative to repo root.
    line : int
        Line number of definition.
    bases : list[str]
        Base class names.
    methods : list[FnDecl]
        Method declarations.
    decorators : list[str]
        Decorator names.
    """

    name: str
    file: str
    line: int
    bases: list[str] = field(default_factory=list)
    methods: list[FnDecl] = field(default_factory=list)
    decorators: list[str] = field(default_factory=list)

    @property
    def key(self) -> str:
        """Return unique key: file::class_name.

        Returns
        -------
        str
            Unique class key.
        """
        return f"{self.file}::{self.name}"


@dataclass
class ModuleInfo:
    """Information about a single module.

    Parameters
    ----------
    file : str
        File path relative to repo root.
    functions : list[FnDecl]
        Top-level function declarations.
    classes : list[ClassDecl]
        Class declarations.
    module_aliases : dict[str, str]
        Import aliases: alias -> module (e.g., {"np": "numpy"}).
    symbol_aliases : dict[str, tuple[str, str]]
        Symbol imports: alias -> (module, name).
    """

    file: str
    functions: list[FnDecl] = field(default_factory=list)
    classes: list[ClassDecl] = field(default_factory=list)
    module_aliases: dict[str, str] = field(default_factory=dict)
    symbol_aliases: dict[str, tuple[str, str]] = field(default_factory=dict)


def _safe_unparse(node: ast.AST | None) -> str | None:
    if node is None:
        return None
    with suppress(ValueError, TypeError):
        return ast.unparse(node)
    return None


def _extract_param_info(arg: ast.arg, default: ast.expr | None) -> ParamInfo:
    """Extract parameter info from AST arg node.

    Returns
    -------
    ParamInfo
        Parsed parameter information.
    """
    annotation = _safe_unparse(arg.annotation) if arg.annotation else None
    default_str = _safe_unparse(default) if default is not None else None

    return ParamInfo(
        name=arg.arg,
        annotation=annotation,
        default=default_str,
    )


def _extract_params(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ParamInfo]:
    """Extract all parameters from a function definition.

    Returns
    -------
    list[ParamInfo]
        Extracted parameter metadata.
    """
    params: list[ParamInfo] = []
    args = node.args

    # Calculate default offset for positional args
    num_pos = len(args.posonlyargs) + len(args.args)
    num_defaults = len(args.defaults)
    default_offset = num_pos - num_defaults

    # Positional-only parameters
    for i, arg in enumerate(args.posonlyargs):
        idx = i - default_offset
        default = args.defaults[idx] if idx >= 0 else None
        param = _extract_param_info(arg, default)
        param.kind = "POSITIONAL_ONLY"
        params.append(param)

    # Regular positional/keyword parameters
    for i, arg in enumerate(args.args):
        idx = len(args.posonlyargs) + i - default_offset
        default = args.defaults[idx] if idx >= 0 else None
        param = _extract_param_info(arg, default)
        param.kind = "POSITIONAL_OR_KEYWORD"
        params.append(param)

    # *args
    if args.vararg:
        param = _extract_param_info(args.vararg, None)
        param.kind = "VAR_POSITIONAL"
        params.append(param)

    # Keyword-only parameters
    for i, arg in enumerate(args.kwonlyargs):
        default = args.kw_defaults[i]  # May be None
        param = _extract_param_info(arg, default)
        param.kind = "KEYWORD_ONLY"
        params.append(param)

    # **kwargs
    if args.kwarg:
        param = _extract_param_info(args.kwarg, None)
        param.kind = "VAR_KEYWORD"
        params.append(param)

    return params


def _extract_decorator_names(decorators: list[ast.expr]) -> list[str]:
    """Extract decorator names from AST nodes.

    Returns
    -------
    list[str]
        Decorator names.
    """
    names: list[str] = []
    for dec in decorators:
        if isinstance(dec, ast.Name):
            names.append(dec.id)
        elif isinstance(dec, ast.Attribute):
            decorator = _safe_unparse(dec)
            if decorator is not None:
                names.append(decorator)
        elif isinstance(dec, ast.Call):
            if isinstance(dec.func, ast.Name):
                names.append(dec.func.id)
            elif isinstance(dec.func, ast.Attribute):
                decorator = _safe_unparse(dec.func)
                if decorator is not None:
                    names.append(decorator)
    return names


def _is_excluded(path: Path, exclude_patterns: list[str]) -> bool:
    return any(path.match(pattern) for pattern in exclude_patterns)


def _iter_source_files(
    root_path: Path,
    *,
    include_patterns: list[str],
    exclude_patterns: list[str],
    max_files: int,
) -> Iterator[Path]:
    files_processed = 0
    for pattern in include_patterns:
        for filepath in root_path.glob(pattern):
            if files_processed >= max_files:
                return
            if _is_excluded(filepath, exclude_patterns):
                continue
            if not filepath.is_file():
                continue
            files_processed += 1
            yield filepath


class DefIndexVisitor(ast.NodeVisitor):
    """AST visitor that extracts definitions from a module."""

    def __init__(self, file: str) -> None:
        self.file = file
        self.functions: list[FnDecl] = []
        self.classes: list[ClassDecl] = []
        self.module_aliases: dict[str, str] = {}
        self.symbol_aliases: dict[str, tuple[str, str]] = {}
        self._current_class: str | None = None

    def visit_Import(self, node: ast.Import) -> None:
        """Record module import aliases."""
        for alias in node.names:
            name = alias.asname or alias.name
            self.module_aliases[name] = alias.name

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Record symbol import aliases."""
        if node.module is None:
            return
        for alias in node.names:
            name = alias.asname or alias.name
            if alias.name == "*":
                continue
            self.symbol_aliases[name] = (node.module, alias.name)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Visit a function definition."""
        self._visit_func(node, is_async=False)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Visit an async function definition."""
        self._visit_func(node, is_async=True)

    def _visit_func(self, node: ast.FunctionDef | ast.AsyncFunctionDef, *, is_async: bool) -> None:
        """Index a function or method definition."""
        params = _extract_params(node)
        decorators = _extract_decorator_names(node.decorator_list)

        # Check if this is a method
        is_method = bool(params) and params[0].name in _SELF_CLS

        fn = FnDecl(
            name=node.name,
            file=self.file,
            line=node.lineno,
            params=params,
            is_async=is_async,
            is_method=is_method,
            class_name=self._current_class,
            decorators=decorators,
        )

        if self._current_class is None:
            self.functions.append(fn)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Visit a class definition."""
        bases: list[str] = []
        for base in node.bases:
            base_name = _safe_unparse(base)
            if base_name is not None:
                bases.append(base_name)

        decorators = _extract_decorator_names(node.decorator_list)

        cls = ClassDecl(
            name=node.name,
            file=self.file,
            line=node.lineno,
            bases=bases,
            decorators=decorators,
        )

        # Visit methods
        prev_class = self._current_class
        self._current_class = node.name

        for child in node.body:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                params = _extract_params(child)
                decorators = _extract_decorator_names(child.decorator_list)
                is_async = isinstance(child, ast.AsyncFunctionDef)
                is_method = bool(params) and params[0].name in _SELF_CLS

                fn = FnDecl(
                    name=child.name,
                    file=self.file,
                    line=child.lineno,
                    params=params,
                    is_async=is_async,
                    is_method=is_method,
                    class_name=node.name,
                    decorators=decorators,
                )
                cls.methods.append(fn)

        self._current_class = prev_class
        self.classes.append(cls)

    def to_module_info(self) -> ModuleInfo:
        """Convert visitor state to ModuleInfo.

        Returns
        -------
        ModuleInfo
            Collected module information.
        """
        return ModuleInfo(
            file=self.file,
            functions=self.functions,
            classes=self.classes,
            module_aliases=self.module_aliases,
            symbol_aliases=self.symbol_aliases,
        )


@dataclass
class DefIndex:
    """Repository-wide definition index.

    Parameters
    ----------
    root : str
        Repository root path.
    modules : dict[str, ModuleInfo]
        Module info by file path.
    """

    root: str
    modules: dict[str, ModuleInfo] = field(default_factory=dict)

    @staticmethod
    def build(
        root: str | Path,
        max_files: int = 10000,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> DefIndex:
        """Build definition index from repository.

        Parameters
        ----------
        root : str | Path
            Repository root.
        max_files : int
            Maximum files to scan.
        include_patterns : list[str] | None
            Glob patterns to include. Defaults to ["**/*.py"].
        exclude_patterns : list[str] | None
            Glob patterns to exclude.

        Returns
        -------
        DefIndex
            Built index.
        """
        root_path = Path(root).resolve()
        if include_patterns is None:
            include_patterns = ["**/*.py"]
        if exclude_patterns is None:
            exclude_patterns = [
                "**/.*",
                "**/__pycache__/**",
                "**/node_modules/**",
                "**/venv/**",
                "**/.venv/**",
                "**/build/**",
                "**/dist/**",
            ]

        index = DefIndex(root=str(root_path))
        for filepath in _iter_source_files(
            root_path,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            max_files=max_files,
        ):
            rel_path = str(filepath.relative_to(root_path))
            try:
                source = filepath.read_text(encoding="utf-8")
                tree = ast.parse(source, filename=str(filepath))
            except (SyntaxError, OSError, UnicodeDecodeError):
                continue
            visitor = DefIndexVisitor(rel_path)
            visitor.visit(tree)
            index.modules[rel_path] = visitor.to_module_info()

        return index

    @staticmethod
    def load_or_build(
        root: str | Path,
        max_files: int = 10000,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> DefIndex:
        """Build a fresh definition index.

        Build is always fresh; no caching is performed.
        """
        root_path = Path(root).resolve()
        if include_patterns is None:
            include_patterns = ["**/*.py"]
        if exclude_patterns is None:
            exclude_patterns = [
                "**/.*",
                "**/__pycache__/**",
                "**/node_modules/**",
                "**/venv/**",
                "**/.venv/**",
                "**/build/**",
                "**/dist/**",
            ]

        return DefIndex.build(
            root=root_path,
            max_files=max_files,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
        )

    def all_functions(self) -> Iterator[FnDecl]:
        """Iterate over all function declarations.

        Yields
        ------
        FnDecl
            Function declarations.
        """
        for mod in self.modules.values():
            yield from mod.functions
            for cls in mod.classes:
                yield from cls.methods

    def all_classes(self) -> Iterator[ClassDecl]:
        """Iterate over all class declarations.

        Yields
        ------
        ClassDecl
            Class declarations.
        """
        for mod in self.modules.values():
            yield from mod.classes

    def find_function_by_name(self, name: str) -> list[FnDecl]:
        """Find all functions with given name.

        Parameters
        ----------
        name : str
            Function name (not qualified).

        Returns
        -------
        list[FnDecl]
            Matching declarations.
        """
        return [fn for fn in self.all_functions() if fn.name == name]

    def find_function_by_qualified_name(self, qname: str) -> list[FnDecl]:
        """Find functions by qualified name (Class.method or function).

        Parameters
        ----------
        qname : str
            Qualified name.

        Returns
        -------
        list[FnDecl]
            Matching declarations.
        """
        return [fn for fn in self.all_functions() if fn.qualified_name == qname]

    def find_function_keys(self, symbol: str) -> list[str]:
        """Find function keys matching symbol.

        Parameters
        ----------
        symbol : str
            Function or qualified name.

        Returns
        -------
        list[str]
            Matching function keys.
        """
        # Try qualified name first
        results = self.find_function_by_qualified_name(symbol)
        if not results:
            results = self.find_function_by_name(symbol)
        return [fn.key for fn in results]

    def find_class_by_name(self, name: str) -> list[ClassDecl]:
        """Find all classes with given name.

        Parameters
        ----------
        name : str
            Class name.

        Returns
        -------
        list[ClassDecl]
            Matching declarations.
        """
        return [cls for cls in self.all_classes() if cls.name == name]

    def find_class_keys(self, name: str) -> list[str]:
        """Find class keys matching name.

        Parameters
        ----------
        name : str
            Class name.

        Returns
        -------
        list[str]
            Matching class keys.
        """
        return [cls.key for cls in self.find_class_by_name(name)]

    def get_module_for_file(self, file: str) -> ModuleInfo | None:
        """Get module info for file.

        Parameters
        ----------
        file : str
            Relative file path.

        Returns
        -------
        ModuleInfo | None
            Module info if indexed.
        """
        return self.modules.get(file)

    def resolve_import_alias(self, file: str, name: str) -> tuple[str | None, str | None]:
        """Resolve import alias in context of a file.

        Parameters
        ----------
        file : str
            File where name is used.
        name : str
            Name or dotted name to resolve.

        Returns
        -------
        tuple[str | None, str | None]
            (module, symbol) if resolved, else (None, None).
        """
        mod = self.modules.get(file)
        if not mod:
            return (None, None)

        # Check module aliases first
        parts = name.split(".")
        first = parts[0]

        if first in mod.module_aliases:
            resolved_mod = mod.module_aliases[first]
            if len(parts) > 1:
                return (resolved_mod, ".".join(parts[1:]))
            return (resolved_mod, None)

        if first in mod.symbol_aliases:
            from_mod, sym = mod.symbol_aliases[first]
            if len(parts) > 1:
                return (from_mod, f"{sym}.{'.'.join(parts[1:])}")
            return (from_mod, sym)

        return (None, None)
