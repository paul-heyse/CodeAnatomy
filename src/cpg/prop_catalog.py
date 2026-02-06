"""Define the canonical property catalog for CPG metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from cpg.specs import PropValueType

PropPrimitive = Literal["string", "int", "float", "bool", "json"]


@dataclass(frozen=True)
class PropSpec:
    """Define a property specification with value typing."""

    type: PropPrimitive
    description: str = ""
    enum_values: tuple[str, ...] | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize the property spec to a dictionary.

        Returns:
        -------
        dict[str, object]
            JSON-serializable property spec.
        """
        return {
            "type": self.type,
            "description": self.description,
            "enum_values": list(self.enum_values) if self.enum_values else None,
        }


def p_str(desc: str = "", enum: Sequence[str] | None = None) -> PropSpec:
    """Create a string property spec.

    Returns:
    -------
    PropSpec
        String property specification.
    """
    return PropSpec("string", desc, tuple(enum) if enum else None)


def p_int(desc: str = "") -> PropSpec:
    """Create an integer property spec.

    Returns:
    -------
    PropSpec
        Integer property specification.
    """
    return PropSpec("int", desc)


def p_float(desc: str = "") -> PropSpec:
    """Create a float property spec.

    Returns:
    -------
    PropSpec
        Float property specification.
    """
    return PropSpec("float", desc)


def p_bool(desc: str = "") -> PropSpec:
    """Create a boolean property spec.

    Returns:
    -------
    PropSpec
        Boolean property specification.
    """
    return PropSpec("bool", desc)


def p_json(desc: str = "") -> PropSpec:
    """Create a JSON property spec.

    Returns:
    -------
    PropSpec
        JSON property specification.
    """
    return PropSpec("json", desc)


PROP_ENUMS: dict[str, tuple[str, ...]] = {
    "ast_attr_type": ("Attribute",),
    "ast_call_type": ("Call",),
    "ast_def_type": ("FunctionDef", "AsyncFunctionDef", "ClassDef", "Lambda"),
    "ast_name_type": ("Name",),
    "base_shape": ("NAME", "ATTRIBUTE", "CALL", "SUBSCRIPT", "OTHER"),
    "binding_kind": (
        "local",
        "param",
        "import",
        "global_ref",
        "nonlocal_ref",
        "free_ref",
        "namespace",
        "annot_only",
        "unknown",
    ),
    "callee_shape": ("NAME", "ATTRIBUTE", "SUBSCRIPT", "OTHER"),
    "ctx_basic": ("Load", "Store", "Del"),
    "ctx_full": ("Load", "Store", "Del", "Param"),
    "def_kind": ("function", "class", "lambda"),
    "def_site_kind": ("function", "class", "import", "assign", "param", "other"),
    "expr_context": ("LOAD", "STORE", "DEL", "UNKNOWN"),
    "expr_kind": ("annotation", "type_comment", "typing_alias", "other"),
    "index_format": ("scip",),
    "literal_kind": (
        "string",
        "bytes",
        "int",
        "float",
        "complex",
        "bool",
        "none",
        "ellipsis",
        "fstring",
        "other",
    ),
    "mapping_method": ("line_table", "offset_span", "heuristic"),
    "origin_qname": ("qname",),
    "origin_scip": ("scip",),
    "origin_type": ("inferred", "runtime", "heuristic"),
    "origin_type_expr": ("annotation", "type_comment", "other"),
    "origin_type_node": ("annotation", "inferred", "runtime", "heuristic"),
    "param_kind": ("positional", "vararg", "kwonly", "varkw"),
    "qname_source": ("QualifiedNameProvider", "FullyQualifiedNameProvider", "syntactic", "other"),
    "ref_kind": ("name", "attribute"),
    "resolution_kind": ("GLOBAL", "NONLOCAL", "FREE", "UNKNOWN"),
    "role": ("syntax", "scope", "symbol", "flow", "diag", "runtime", "type"),
    "severity": ("INFO", "WARNING", "ERROR"),
    "use_kind": ("read", "write", "del", "call", "import", "other"),
    "variance": ("invariant", "covariant", "contravariant"),
}


PROP_SPECS: dict[str, PropSpec] = {
    "ambiguity_group_id": p_str(""),
    "anchor_confidence": p_float(""),
    "anchor_kind": p_str(""),
    "anchor_reason": p_str(""),
    "annotated_here": p_bool(""),
    "annotation_json": p_json(""),
    "annotation_repr": p_str(""),
    "arg_count": p_int("Number of args (positional+keyword)"),
    "argrepr": p_str(""),
    "asname": p_str("Alias name"),
    "assigned_here": p_bool(""),
    "ast_type": p_str("ast.AST type name (e.g., Call, Name, FunctionDef)"),
    "attr": p_str("Attribute name"),
    "base_exprs_json": p_json("List of base class expr renderings"),
    "base_shape": p_str("", enum=PROP_ENUMS["base_shape"]),
    "baseopname": p_str("Semantic opcode name (base op)"),
    "binding_id": p_str("binding_id = f'{scope_id}:BIND:{name}'"),
    "binding_kind": p_str("", enum=PROP_ENUMS["binding_kind"]),
    "block_id": p_str(""),
    "callee_dotted": p_str("Best-effort dotted callee path"),
    "callee_qnames": p_json("Candidate qualified names (JSON list)"),
    "callee_fqns": p_json("Fully qualified callee names (JSON list)"),
    "callee_shape": p_str("", enum=PROP_ENUMS["callee_shape"]),
    "callee_text": p_str("Rendered callee text"),
    "class_methods": p_json("Method name partitions derived from symtable"),
    "co_firstlineno": p_int(""),
    "code": p_str(""),
    "code_unit_id": p_str(""),
    "col_end": p_int(""),
    "col_offset": p_int(""),
    "col_start": p_int(""),
    "cond_instr_id": p_str(""),
    "container_def_id": p_str("Owning definition id"),
    "cst_type": p_str("LibCST node type name (e.g., FunctionDef, Name, Call)"),
    "ctx": p_str(""),
    "declared_here": p_bool(""),
    "declaring_type": p_str(""),
    "decorator_count": p_int(""),
    "decorator_text": p_str(""),
    "def_kind": p_str("", enum=PROP_ENUMS["def_kind"]),
    "def_site_kind": p_str("", enum=PROP_ENUMS["def_site_kind"]),
    "default_json": p_json(""),
    "default_repr": p_str(""),
    "depth": p_int(""),
    "details": p_json(""),
    "df_id": p_str(""),
    "diag_source": p_str(""),
    "diagnostic_count": p_int(""),
    "display_name": p_str(""),
    "docstring": p_str("Docstring text"),
    "documentation": p_json(""),
    "enclosing_symbol": p_str(""),
    "encoding": p_str("Encoding guess / decoding policy"),
    "end_col_offset": p_int(""),
    "end_lineno": p_int(""),
    "end_off": p_int(""),
    "engine": p_str(""),
    "exc_entry_index": p_int(""),
    "expr_context": p_str("", enum=PROP_ENUMS["expr_context"]),
    "expr_kind": p_str("", enum=PROP_ENUMS["expr_kind"]),
    "expr_role": p_str(""),
    "ref_kind": p_str("", enum=PROP_ENUMS["ref_kind"]),
    "ref_text": p_str("Reference text"),
    "expr_text": p_str("Rendered annotation/type expression"),
    "field": p_str(""),
    "file": p_str(""),
    "file_sha256": p_str("SHA-256 hash (hex)"),
    "flags_json": p_json(""),
    "function_partitions": p_json("Function scope partitioning from symtable"),
    "git_commit": p_str("Best-effort git commit"),
    "has_default": p_bool(""),
    "has_error": p_bool(""),
    "has_changes": p_bool(""),
    "id": p_str("Identifier string"),
    "import_kind": p_str("Import kind (import/from)"),
    "import_text": p_str(""),
    "imported_name": p_str("Imported name (module or member)"),
    "index": p_int(""),
    "index_format": p_str("", enum=PROP_ENUMS["index_format"]),
    "instr_id": p_str("Stable instruction id"),
    "is_annotated": p_bool(""),
    "is_assigned": p_bool(""),
    "is_async": p_bool(""),
    "is_descriptor": p_bool(""),
    "is_entry": p_bool(""),
    "is_error": p_bool(""),
    "is_extra": p_bool(""),
    "is_exit": p_bool(""),
    "is_free": p_bool(""),
    "is_global": p_bool(""),
    "is_imported": p_bool(""),
    "is_kw_star": p_bool(""),
    "is_local": p_bool(""),
    "is_meta_scope": p_bool("True for ANNOTATION/TYPE_* scopes"),
    "is_missing": p_bool(""),
    "is_module_docstring": p_bool(""),
    "is_named": p_bool(""),
    "is_namespace": p_bool(""),
    "is_nonlocal": p_bool(""),
    "is_parameter": p_bool(""),
    "is_referenced": p_bool(""),
    "is_star": p_bool(""),
    "jump_off": p_int(""),
    "kind": p_str(""),
    "language": p_str(""),
    "lasti": p_int(""),
    "line": p_int(""),
    "line_end": p_int(""),
    "line_start": p_int(""),
    "lineno": p_int(""),
    "literal_kind": p_str("", enum=PROP_ENUMS["literal_kind"]),
    "literal_text": p_str(""),
    "mapping_method": p_str("", enum=PROP_ENUMS["mapping_method"]),
    "member_id": p_str(""),
    "member_kind": p_str(""),
    "message": p_str(""),
    "module": p_str("Module path for import-from"),
    "module_fqn": p_str("Fully qualified module name"),
    "mtime_ns": p_int("Modification time (ns)"),
    "namespace_block_ids": p_json("Symbol namespace block ids (JSON list)"),
    "namespace_count": p_int("Namespace block count"),
    "name": p_str("Definition name"),
    "notes": p_str(""),
    "obj_type": p_str(""),
    "object_type": p_str(""),
    "occurrence_role_names_json": p_json("Human-readable role names"),
    "offset": p_int("Bytecode offset"),
    "opname": p_str("Raw opname"),
    "origin": p_str(""),
    "package_fqn": p_str("Package fully-qualified name"),
    "package_root": p_str("Detected package root"),
    "param_count": p_int(""),
    "param_id": p_str(""),
    "param_name": p_str(""),
    "path": p_str("Repo-relative file path"),
    "path_length": p_int(""),
    "position": p_int(""),
    "project_root": p_str(""),
    "qname": p_str("Qualified name string"),
    "qname_source": p_str("", enum=PROP_ENUMS["qname_source"]),
    "qnames": p_json("Qualified names (JSON list)"),
    "def_fqns": p_json("Fully qualified def names (JSON list)"),
    "qualname": p_str("co_qualname / derived qualname"),
    "reason": p_str(""),
    "referenced_here": p_bool(""),
    "relative_level": p_int("Relative import level"),
    "repo_id": p_str("Stable repo identifier (hash of repo_root + git head)"),
    "repo_root": p_str("Repository root path"),
    "resolution_method": p_str(""),
    "return_annotation": p_str(""),
    "returns_annotation": p_str(""),
    "role": p_str("", enum=PROP_ENUMS["role"]),
    "rt_id": p_str("Runtime object identity"),
    "task_name": p_str(""),
    "task_priority": p_int(""),
    "scope_id": p_str("ScopeProvider scope id (if captured)"),
    "scope_role": p_str("ScopeProvider role"),
    "scope_name": p_str("symtable table.get_name()"),
    "scope_type": p_str("symtable table.get_type().name"),
    "score": p_float(""),
    "severity": p_str("", enum=PROP_ENUMS["severity"]),
    "parent_kind": p_str("Parent CST node kind"),
    "inferred_type": p_str("Inferred type string"),
    "sig_id": p_str(""),
    "signature": p_str(""),
    "signature_language": p_str(""),
    "signature_text": p_str(""),
    "size_bytes": p_int("File size in bytes"),
    "source": p_str(""),
    "source_line": p_int(""),
    "source_path": p_str(""),
    "start_off": p_int(""),
    "starts_line": p_int(""),
    "subscript_text": p_str(""),
    "symbol": p_str("SCIP symbol string (global identity)"),
    "symbol_kind": p_str(""),
    "symbol_roles": p_int("SCIP symbol_roles bitmask"),
    "target_type_repr": p_str(""),
    "text": p_str("Comment text"),
    "tool": p_str(""),
    "ts_type": p_str("tree-sitter node type"),
    "ts_kind_id": p_int("tree-sitter node kind id"),
    "ts_grammar_id": p_int("tree-sitter grammar symbol id"),
    "ts_grammar_name": p_str("tree-sitter grammar symbol name"),
    "ts_node_uid": p_int("tree-sitter node unique id"),
    "type_form": p_str(""),
    "type_repr": p_str("Normalized type representation (string or JSON)"),
    "unwrap_depth": p_int(""),
    "use_kind": p_str("", enum=PROP_ENUMS["use_kind"]),
    "value_module": p_str(""),
    "value_qualname": p_str(""),
    "value_repr": p_str(""),
    "variance": p_str("", enum=PROP_ENUMS["variance"]),
    "version": p_str(""),
}


PROP_BUNDLES: dict[str, tuple[str, ...]] = {
    "anchor_lines": ("line_start", "col_start", "line_end", "col_end"),
    "sym_flags": (
        "is_local",
        "is_global",
        "is_nonlocal",
        "is_free",
        "is_parameter",
        "is_imported",
        "is_assigned",
        "is_referenced",
        "is_annotated",
        "is_namespace",
    ),
}


def prop_value_type(key: str) -> PropValueType:
    """Return the value type for a property key.

    Args:
        key: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    spec = PROP_SPECS.get(key)
    if spec is None:
        msg = f"Unknown prop key {key!r} in catalog."
        raise ValueError(msg)
    return spec.type


def resolve_prop_specs(
    keys: Sequence[str],
    *,
    overrides: Mapping[str, PropSpec] | None = None,
) -> dict[str, PropSpec]:
    """Return prop specs for the requested keys.

    Args:
        keys: Description.
        overrides: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    out: dict[str, PropSpec] = {}
    override_map: dict[str, PropSpec] = dict(overrides) if overrides else {}
    for key in keys:
        if key in override_map:
            out[key] = override_map[key]
            continue
        spec = PROP_SPECS.get(key)
        if spec is None:
            msg = f"Unknown prop key {key!r} in registry catalog."
            raise ValueError(msg)
        out[key] = spec
    return out


__all__ = [
    "PROP_BUNDLES",
    "PROP_ENUMS",
    "PROP_SPECS",
    "PropPrimitive",
    "PropSpec",
    "p_bool",
    "p_float",
    "p_int",
    "p_json",
    "p_str",
    "prop_value_type",
    "resolve_prop_specs",
]
