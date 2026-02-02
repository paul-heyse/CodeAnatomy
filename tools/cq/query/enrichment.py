"""Enrichment module for Python code analysis.

Provides symtable and bytecode analysis for enriching query results.
"""

from __future__ import annotations

import ast
import dis
import logging
import re
import symtable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.introspection.symtable_extract import ScopeGraph, extract_scope_graph

if TYPE_CHECKING:
    from types import CodeType

    from tools.cq.core.schema import Finding
    from tools.cq.query.ir import ScopeFilter
    from tools.cq.query.sg_parser import SgRecord

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SymtableInfo:
    """Symbol table information for a definition."""

    locals: tuple[str, ...]
    globals_used: tuple[str, ...]
    free_vars: tuple[str, ...]  # Closure variables
    nested_scopes: int


@dataclass(frozen=True)
class BytecodeInfo:
    """Bytecode analysis results."""

    load_globals: tuple[str, ...]
    load_attrs: tuple[str, ...]
    call_functions: tuple[str, ...]


def analyze_symtable(source: str, filename: str) -> dict[str, SymtableInfo]:
    """Analyze source code with symtable module.

    Parameters
    ----------
    source
        Python source code to analyze
    filename
        Filename for error reporting

    Returns
    -------
    dict[str, SymtableInfo]
        Mapping from function/class names to their SymtableInfo

    Notes
    -----
    Gracefully handles syntax errors and returns empty dict on failure.
    """
    try:
        table = symtable.symtable(source, filename, "exec")
    except SyntaxError as exc:
        logger.debug("Syntax error in %s: %s", filename, exc)
        return {}
    except (ValueError, TypeError) as exc:
        logger.warning("Failed to build symtable for %s: %s", filename, exc)
        return {}

    result: dict[str, SymtableInfo] = {}

    def _process_symbol_table(st: symtable.SymbolTable) -> None:
        """Recursively process symbol table and children."""
        # Get symbol information for this scope
        if st.get_type() in {"function", "class"}:
            name = st.get_name()
            local_syms = tuple(sorted(sym.get_name() for sym in st.get_symbols() if sym.is_local()))
            global_syms = tuple(
                sorted(sym.get_name() for sym in st.get_symbols() if sym.is_global())
            )
            free_syms = tuple(sorted(sym.get_name() for sym in st.get_symbols() if sym.is_free()))

            # Count nested scopes
            nested_count = len(st.get_children())

            result[name] = SymtableInfo(
                locals=local_syms,
                globals_used=global_syms,
                free_vars=free_syms,
                nested_scopes=nested_count,
            )

        # Process children recursively
        for child in st.get_children():
            _process_symbol_table(child)

    _process_symbol_table(table)
    return result


def analyze_bytecode(code_object: CodeType) -> BytecodeInfo:
    """Analyze a code object's bytecode.

    Parameters
    ----------
    code_object
        Code object to analyze

    Returns
    -------
    BytecodeInfo
        Extracted LOAD_GLOBAL, LOAD_ATTR, and CALL_FUNCTION information

    Notes
    -----
    Handles both Python 3.11+ and earlier bytecode formats.
    """
    load_globals: set[str] = set()
    load_attrs: set[str] = set()
    call_functions: set[str] = set()

    try:
        instructions = list(dis.get_instructions(code_object))
    except (TypeError, AttributeError) as exc:
        logger.debug("Failed to disassemble code object: %s", exc)
        return BytecodeInfo(
            load_globals=(),
            load_attrs=(),
            call_functions=(),
        )

    for i, instr in enumerate(instructions):
        # Capture LOAD_GLOBAL operations
        if instr.opname in {"LOAD_GLOBAL", "LOAD_NAME"} and instr.argval:
            load_globals.add(str(instr.argval))

        # Capture LOAD_ATTR operations
        elif instr.opname == "LOAD_ATTR" and instr.argval:
            load_attrs.add(str(instr.argval))

        # Capture function calls
        # Python 3.11+ uses CALL, earlier versions use CALL_FUNCTION
        elif instr.opname in {"CALL", "CALL_FUNCTION", "CALL_METHOD"} and i > 0:
            # Try to identify the function being called by looking backwards
            # This is a heuristic approach
            prev_instr = instructions[i - 1]
            if prev_instr.opname in {"LOAD_ATTR", "LOAD_GLOBAL", "LOAD_NAME"} and prev_instr.argval:
                call_functions.add(str(prev_instr.argval))

    return BytecodeInfo(
        load_globals=tuple(sorted(load_globals)),
        load_attrs=tuple(sorted(load_attrs)),
        call_functions=tuple(sorted(call_functions)),
    )


def _extract_definition_name(matched_text: str, kind: str) -> str | None:
    """Extract function or class name from matched text.

    Parameters
    ----------
    matched_text
        The matched source code text
    kind
        Either 'function_definition' or 'class_definition'

    Returns
    -------
    str | None
        Extracted name or None if extraction fails
    """
    if kind == "function_definition" and "def " in matched_text:
        name_start = matched_text.find("def ") + 4
        name_end = matched_text.find("(", name_start)
        if name_end > name_start:
            return matched_text[name_start:name_end].strip()

    if kind == "class_definition" and "class " in matched_text:
        name_start = matched_text.find("class ") + 6
        # Find either '(' or ':'
        paren_pos = matched_text.find("(", name_start)
        colon_pos = matched_text.find(":", name_start)
        name_end = (
            min(p for p in [paren_pos, colon_pos] if p > name_start)
            if any(p > name_start for p in [paren_pos, colon_pos])
            else -1
        )
        if name_end > name_start:
            return matched_text[name_start:name_end].strip()

    return None


def _enrich_with_bytecode(
    tree: ast.Module,
    record: SgRecord,
    file_path: Path,
) -> BytecodeInfo | None:
    """Attempt to enrich record with bytecode analysis.

    Parameters
    ----------
    tree
        Parsed AST module
    record
        Record to enrich
    file_path
        Source file path

    Returns
    -------
    BytecodeInfo | None
        Bytecode info if analysis succeeds, None otherwise
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.lineno == record.start_line:
            try:
                # Compile the function to get code object
                func_source = ast.unparse(node)
                func_code = compile(func_source, str(file_path), "exec")
                # The code object for the function is in the constants
                for const in func_code.co_consts:
                    if hasattr(const, "co_name") and const.co_name == node.name:
                        return analyze_bytecode(const)
            except (ValueError, SyntaxError, TypeError):
                return None
            break
    return None


def _enrich_record(
    record: SgRecord,
    source: str,
    symtable_map: dict[str, SymtableInfo],
    tree: ast.Module | None,
    file_path: Path,
) -> dict:
    """Enrich a single record with symtable and bytecode data.

    Parameters
    ----------
    record
        Record to enrich
    source
        Source file content
    symtable_map
        Symbol table information for the file
    tree
        Parsed AST or None
    file_path
        Source file path

    Returns
    -------
    dict
        Enrichment data (may be empty)
    """
    enrichment: dict = {}

    # Add symtable info for function/class definitions
    if record.kind in {"function_definition", "class_definition"}:
        try:
            matched_lines = source.splitlines()[record.start_line - 1 : record.end_line]
            matched_text = "\n".join(matched_lines)
            name = _extract_definition_name(matched_text, record.kind)
            if name and name in symtable_map:
                enrichment["symtable_info"] = symtable_map[name]
        except (IndexError, ValueError):
            pass

    # Add bytecode info for function definitions
    if tree is not None and record.kind == "function_definition":
        bytecode_info = _enrich_with_bytecode(tree, record, file_path)
        if bytecode_info is not None:
            enrichment["bytecode_info"] = bytecode_info

    return enrichment


def enrich_records(
    records: list[SgRecord],
    root: Path,
    python_path: str | None = None,  # noqa: ARG001
) -> dict[str, dict]:
    """Enrich records with symtable and bytecode analysis.

    Parameters
    ----------
    records
        ast-grep records to enrich
    root
        Repository root
    python_path
        Python interpreter to use (currently unused, reserved for future use)

    Returns
    -------
    dict[str, dict]
        Mapping from record location to enrichment data containing:
        - symtable_info: SymtableInfo if available
        - bytecode_info: BytecodeInfo if available
    """
    enrichment_map: dict[str, dict] = {}

    # Group records by file for efficient processing
    records_by_file: dict[Path, list[SgRecord]] = {}
    for record in records:
        file_path = root / record.file
        if file_path not in records_by_file:
            records_by_file[file_path] = []
        records_by_file[file_path].append(record)

    # Process each file
    for file_path, file_records in records_by_file.items():
        try:
            source = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.warning("Failed to read %s: %s", file_path, exc)
            continue

        # Get symtable information for the entire file
        symtable_map = analyze_symtable(source, str(file_path))

        # Parse AST for bytecode analysis
        try:
            tree = ast.parse(source, str(file_path))
        except SyntaxError as exc:
            logger.debug("Syntax error in %s: %s", file_path, exc)
            tree = None

        # Enrich each record
        for record in file_records:
            location = f"{record.file}:{record.start_line}:{record.start_col}"
            enrichment = _enrich_record(record, source, symtable_map, tree, file_path)
            if enrichment:
                enrichment_map[location] = enrichment

    return enrichment_map


class SymtableEnricher:
    """Enricher that adds symtable-based scope information."""

    def __init__(self, root: Path) -> None:
        """Initialize enricher.

        Parameters
        ----------
        root
            Repository root for resolving file paths.
        """
        self._root = root
        self._cache: dict[Path, ScopeGraph] = {}

    def _get_scope_graph(self, file_path: Path) -> ScopeGraph | None:
        """Get or create scope graph for a file."""
        if file_path in self._cache:
            return self._cache[file_path]

        try:
            source = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.debug("Failed to read %s: %s", file_path, exc)
            return None

        graph = extract_scope_graph(source, str(file_path))
        self._cache[file_path] = graph
        return graph

    def enrich_function_finding(
        self,
        finding: Finding,
        record: SgRecord,
    ) -> dict:
        """Enrich a function finding with scope details.

        Parameters
        ----------
        finding
            The finding to enrich
        record
            The ast-grep record for the function

        Returns
        -------
        dict
            Scope-related enrichment data.
        """
        if finding.anchor is None:
            return {}

        file_path = self._root / finding.anchor.file
        graph = self._get_scope_graph(file_path)
        if graph is None:
            return {}

        # Extract function name from record
        func_name = _extract_def_name_from_record(record)
        if not func_name:
            return {}

        # Find matching scope
        scope = graph.scope_by_name.get(func_name)
        if scope is None:
            return {}

        enrichment: dict = {
            "is_closure": scope.has_free_vars,
            "is_nested": scope.is_nested,
            "free_vars": list(scope.free_vars),
            "cell_vars": list(scope.cell_vars),
        }

        return enrichment


def _extract_def_name_from_record(record: SgRecord) -> str | None:
    """Extract function/class name from an ast-grep record."""
    text = record.text

    # Match def name(...) or class name
    match = re.match(r"(?:async\s+)?(?:def|class)\s+(\w+)", text)
    if match:
        return match.group(1)

    return None


def filter_by_scope(
    findings: list[Finding],
    scope_filter: ScopeFilter,
    enricher: SymtableEnricher,
    records: list[SgRecord],
) -> list[Finding]:
    """Filter findings by scope criteria.

    Parameters
    ----------
    findings
        Findings to filter
    scope_filter
        Scope filter criteria
    enricher
        Symtable enricher for scope analysis
    records
        Original ast-grep records (parallel to findings)

    Returns
    -------
    list[Finding]
        Filtered findings matching scope criteria.
    """
    if not findings or len(findings) != len(records):
        return findings

    filtered: list[Finding] = []

    for finding, record in zip(findings, records, strict=True):
        # Get scope info
        scope_info = enricher.enrich_function_finding(finding, record)
        if not scope_info:
            # No scope info available - include by default unless strict filter
            if scope_filter.scope_type is None:
                filtered.append(finding)
            continue

        # Check scope type filter
        if scope_filter.scope_type:
            scope_type = scope_filter.scope_type.lower()
            if scope_type == "closure" and not scope_info.get("is_closure"):
                continue
            if scope_type == "nested" and not scope_info.get("is_nested"):
                continue
            if scope_type == "toplevel" and scope_info.get("is_nested"):
                continue

        # Check captures filter
        if scope_filter.captures:
            free_vars = scope_info.get("free_vars", [])
            if scope_filter.captures not in free_vars:
                continue

        # Check has_cells filter
        if scope_filter.has_cells is not None:
            cell_vars = scope_info.get("cell_vars", [])
            has_cells = len(cell_vars) > 0
            if scope_filter.has_cells != has_cells:
                continue

        filtered.append(finding)

    return filtered


def extract_decorators_from_function(source: str, lineno: int) -> list[str]:
    """Extract decorator names from a function definition.

    Parameters
    ----------
    source
        Source code
    lineno
        Line number of the function definition (1-indexed)

    Returns
    -------
    list[str]
        List of decorator names.
    """
    decorators: list[str] = []

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return decorators

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or isinstance(node, ast.ClassDef):
            if node.lineno == lineno:
                for decorator in node.decorator_list:
                    dec_name = _extract_decorator_name(decorator)
                    if dec_name:
                        decorators.append(dec_name)
                break

    return decorators


def _extract_decorator_name(decorator: ast.expr) -> str | None:
    """Extract name from a decorator expression."""
    if isinstance(decorator, ast.Name):
        return decorator.id
    if isinstance(decorator, ast.Attribute):
        # Handle dotted decorators like @foo.bar
        parts: list[str] = []
        node = decorator
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            parts.append(node.id)
        return ".".join(reversed(parts))
    if isinstance(decorator, ast.Call):
        # Handle decorator calls like @foo()
        return _extract_decorator_name(decorator.func)
    return None


def enrich_with_decorators(
    finding: Finding,
    source: str,
) -> dict:
    """Enrich a finding with decorator information.

    Parameters
    ----------
    finding
        The finding to enrich
    source
        Source code

    Returns
    -------
    dict
        Decorator enrichment data.
    """
    if finding.anchor is None:
        return {}

    decorators = extract_decorators_from_function(source, finding.anchor.line)

    if not decorators:
        return {}

    return {
        "decorators": decorators,
        "decorator_count": len(decorators),
    }
