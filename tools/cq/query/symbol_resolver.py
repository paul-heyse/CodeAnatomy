"""Symbol resolution for cq queries.

Builds symbol keys and resolves references across files.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from tools.cq.query.sg_parser import SgRecord, filter_records_by_kind


@dataclass
class SymbolKey:
    """Unique identifier for a symbol.

    Format: module_path:qualname
    Example: src/cli/app:meta_launcher
    """

    module_path: str
    qualname: str

    def __str__(self) -> str:
        """Render the symbol key in ``module_path:qualname`` form.

        Returns
        -------
        str
            Serialized symbol key.
        """
        return f"{self.module_path}:{self.qualname}"

    @classmethod
    def from_string(cls, key_str: str) -> SymbolKey:
        """Parse a symbol key from string format.

        Used by symbol resolution utilities when deserializing keys.

        Returns
        -------
        SymbolKey
            Parsed symbol key.
        """
        if ":" not in key_str:
            return cls(module_path="", qualname=key_str)
        parts = key_str.split(":", 1)
        return cls(module_path=parts[0], qualname=parts[1])


@dataclass
class ImportBinding:
    """Represents an import binding.

    Tracks what name is bound and where it comes from.
    """

    local_name: str  # Name as used in this module
    source_module: str  # Module imported from
    source_name: str | None  # Original name if different (for 'as' imports)
    is_from_import: bool  # True for 'from x import y'

    @property
    def original_name(self) -> str:
        """Get the original name in the source module.

        Used by import resolution to map aliases back to source names.

        Returns
        -------
        str
            Original name as defined in the source module.
        """
        return self.source_name or self.local_name


@dataclass
class SymbolTable:
    """Symbol table for a codebase.

    Provides resolution of names to definitions and tracking of imports.
    """

    # Mapping from symbol key to definition record
    definitions: dict[str, SgRecord] = field(default_factory=dict)

    # Mapping from (file, local_name) to import binding
    imports: dict[tuple[str, str], ImportBinding] = field(default_factory=dict)

    # Unresolved references with reasons
    unresolved: list[tuple[str, str]] = field(default_factory=list)

    @classmethod
    def from_records(
        cls,
        records: list[SgRecord],
    ) -> SymbolTable:
        """Build symbol table from ast-grep records.

        Parameters
        ----------
        records
            All ast-grep records

        Returns
        -------
        SymbolTable
            Built symbol table
        """
        table = cls()

        # Process definitions
        def_records = filter_records_by_kind(records, "def")
        for record in def_records:
            key = _build_symbol_key(record)
            if key:
                table.definitions[str(key)] = record

        # Process imports
        import_records = [r for r in records if r.record == "import"]
        for record in import_records:
            bindings = _parse_import(record)
            for binding in bindings:
                table.imports[record.file, binding.local_name] = binding

        return table

    def resolve(self, file: str, name: str) -> SgRecord | None:
        """Resolve a name reference to its definition.

        Parameters
        ----------
        file
            File where the reference appears
        name
            Name being referenced

        Returns
        -------
        SgRecord | None
            Definition record or None if unresolved
        """
        # Check if it's an imported name
        binding = self.imports.get((file, name))
        if binding:
            # Try to resolve via import
            return self._resolve_import(binding)

        # Try local definitions in same module
        module_path = _file_to_module_path(file)
        key = f"{module_path}:{name}"
        if key in self.definitions:
            return self.definitions[key]

        # Track as unresolved
        self.unresolved.append((file, name))
        return None

    def _resolve_import(self, binding: ImportBinding) -> SgRecord | None:
        """Resolve an imported name to its definition.

        Used by ``resolve`` when a name resolves via an import binding.

        Returns
        -------
        SgRecord | None
            Definition record if resolved.
        """
        # Convert module to path pattern
        module_parts = binding.source_module.split(".")

        # Try common path patterns
        for suffix in [".py", "/__init__.py"]:
            path_guess = "/".join(module_parts) + suffix
            key = f"{path_guess}:{binding.original_name}"
            if key in self.definitions:
                return self.definitions[key]

        return None


def _build_symbol_key(record: SgRecord) -> SymbolKey | None:
    """Build a symbol key for a definition record.

    Used by ``SymbolTable.from_records`` to index definition records.

    Returns
    -------
    SymbolKey | None
        Symbol key for the record, or None if no name is found.
    """
    # Extract name from definition
    name = _extract_def_name(record)
    if not name:
        return None

    # Convert file path to module path
    module_path = _file_to_module_path(record.file)

    return SymbolKey(module_path=module_path, qualname=name)


def _extract_def_name(record: SgRecord) -> str | None:
    """Extract the name from a definition record.

    Used by ``_build_symbol_key`` to derive a qualname.

    Returns
    -------
    str | None
        Definition name if one is found.
    """
    text = record.text.lstrip()

    # Match def name(...) or class name
    if record.record == "def":
        match = re.search(r"(?:async\s+)?(?:def|class)\s+(\w+)", text)
        if match:
            return match.group(1)

    return None


def _file_to_module_path(file_path: str) -> str:
    """Convert file path to module-style path.

    Examples
    --------
        src/cli/app.py -> src/cli/app
        src/cli/__init__.py -> src/cli

    Used by symbol resolution to align file paths with import paths.

    Returns
    -------
    str
        Module-style path without ``.py`` or ``/__init__`` suffixes.
    """
    path = file_path

    # Remove .py extension
    if path.endswith(".py"):
        path = path[:-3]

    # Handle __init__.py
    if path.endswith("/__init__"):
        path = path[:-9]

    return path


def _parse_import(record: SgRecord) -> list[ImportBinding]:
    """Parse an import record into import bindings.

    Used by ``SymbolTable.from_records`` to track imports.

    Returns
    -------
    list[ImportBinding]
        Import bindings extracted from the record.
    """
    bindings: list[ImportBinding] = []
    text = record.text

    # Handle "import x" or "import x as y"
    if text.startswith("import "):
        # Remove "import " prefix
        remainder = text[7:].strip()

        # Check for "as" alias
        if " as " in remainder:
            parts = remainder.split(" as ", 1)
            source = parts[0].strip()
            alias = parts[1].strip()
            bindings.append(
                ImportBinding(
                    local_name=alias,
                    source_module=source,
                    source_name=source.split(".")[-1],
                    is_from_import=False,
                )
            )
        else:
            # Plain import
    for module_name in remainder.split(","):
        cleaned_module = module_name.strip()
        if cleaned_module:
            bindings.append(
                ImportBinding(
                    local_name=cleaned_module.split(".")[-1],
                    source_module=cleaned_module,
                    source_name=None,
                    is_from_import=False,
                )
            )

    # Handle "from x import y" or "from x import y as z"
    elif text.startswith("from "):
        # Parse "from module import names"
        match = re.match(r"from\s+([\w.]+)\s+import\s+(.+)", text)
        if match:
            source_module = match.group(1)
            names_part = match.group(2).strip()

            # Remove parentheses if present
            if names_part.startswith("("):
                names_part = names_part[1:]
            if names_part.endswith(")"):
                names_part = names_part[:-1]

            # Parse each imported name
            for name_spec in names_part.split(","):
                cleaned_spec = name_spec.strip()
                if not cleaned_spec:
                    continue

                if " as " in cleaned_spec:
                    parts = cleaned_spec.split(" as ", 1)
                    source_name = parts[0].strip()
                    local_name = parts[1].strip()
                else:
                    source_name = cleaned_spec
                    local_name = cleaned_spec

                bindings.append(
                    ImportBinding(
                        local_name=local_name,
                        source_module=source_module,
                        source_name=source_name if source_name != local_name else None,
                        is_from_import=True,
                    )
                )

    return bindings


def resolve_call_target(
    call: SgRecord,
    symbol_table: SymbolTable,
) -> tuple[SgRecord | None, str]:
    """Resolve a call record to its target definition.

    Parameters
    ----------
    call
        Call record to resolve
    symbol_table
        Symbol table for resolution

    Returns
    -------
    tuple[SgRecord | None, str]
        Tuple of (resolved definition or None, resolution status)
        Status is one of: "resolved", "unresolved", "dynamic", "builtin"
    """
    target_name = _extract_call_target(call)
    if not target_name:
        return None, "unresolved"

    # Check for known builtins
    if _is_builtin(target_name):
        return None, "builtin"

    # Try to resolve
    resolved = symbol_table.resolve(call.file, target_name)
    if resolved:
        return resolved, "resolved"

    return None, "unresolved"


def _extract_call_target(call: SgRecord) -> str:
    """Extract the target name from a call record.

    Used by ``resolve_call_target`` to normalize call expressions.

    Returns
    -------
    str
        Extracted call target name, or empty string if unknown.
    """
    text = call.text.lstrip()

    # For attribute calls (obj.method()), extract the method name
    if call.kind in {"attr_call", "attr"}:
        match = re.search(r"\.(\w+)\s*\(", text)
        if match:
            return match.group(1)

    # For name calls (func()), extract the function name
    match = re.search(r"\b(\w+)\s*\(", text)
    if match:
        return match.group(1)

    return ""


def _is_builtin(name: str) -> bool:
    """Check if a name is a Python builtin.

    Used by ``resolve_call_target`` to short-circuit builtin calls.

    Returns
    -------
    bool
        True if the name is a builtin.
    """
    builtins = {
        "abs",
        "all",
        "any",
        "ascii",
        "bin",
        "bool",
        "breakpoint",
        "bytearray",
        "bytes",
        "callable",
        "chr",
        "classmethod",
        "compile",
        "complex",
        "delattr",
        "dict",
        "dir",
        "divmod",
        "enumerate",
        "eval",
        "exec",
        "filter",
        "float",
        "format",
        "frozenset",
        "getattr",
        "globals",
        "hasattr",
        "hash",
        "help",
        "hex",
        "id",
        "input",
        "int",
        "isinstance",
        "issubclass",
        "iter",
        "len",
        "list",
        "locals",
        "map",
        "max",
        "memoryview",
        "min",
        "next",
        "object",
        "oct",
        "open",
        "ord",
        "pow",
        "print",
        "property",
        "range",
        "repr",
        "reversed",
        "round",
        "set",
        "setattr",
        "slice",
        "sorted",
        "staticmethod",
        "str",
        "sum",
        "super",
        "tuple",
        "type",
        "vars",
        "zip",
    }
    return name in builtins
