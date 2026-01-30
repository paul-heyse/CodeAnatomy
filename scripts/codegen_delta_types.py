"""Generate Rust and Python Delta types from JSON schema."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SchemaInfo:
    """Schema metadata loaded from JSON schema files."""

    title: str
    payload: dict[str, Any]


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas" / "delta"
PY_OUT = ROOT / "src" / "datafusion_engine" / "generated" / "delta_types.py"
RS_OUT = ROOT / "rust" / "datafusion_ext" / "src" / "generated" / "delta_types.rs"


def _load_schemas() -> tuple[dict[str, SchemaInfo], dict[str, SchemaInfo]]:
    by_title: dict[str, SchemaInfo] = {}
    by_id: dict[str, SchemaInfo] = {}
    for path in sorted(SCHEMA_DIR.glob("*.schema.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        title = payload.get("title")
        if not isinstance(title, str) or not title:
            msg = f"Schema {path} missing title."
            raise ValueError(msg)
        info = SchemaInfo(title=title, payload=payload)
        by_title[title] = info
        schema_id = payload.get("$id")
        if isinstance(schema_id, str):
            by_id[schema_id] = info
    return by_title, by_id


def _resolve_ref(ref: str, by_id: dict[str, SchemaInfo]) -> SchemaInfo:
    if ref in by_id:
        return by_id[ref]
    msg = f"Unknown schema ref: {ref}"
    raise ValueError(msg)


def _is_optional(schema: dict[str, Any]) -> bool:
    types = schema.get("type")
    if isinstance(types, list):
        return "null" in types
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        return any(isinstance(item, dict) and item.get("type") == "null" for item in any_of)
    return False


def _base_type(schema: dict[str, Any]) -> str | None:
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        for item in any_of:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "null":
                continue
            base = _base_type(item)
            if base is not None:
                return base
    types = schema.get("type")
    if isinstance(types, list):
        filtered = [value for value in types if value != "null"]
        if len(filtered) == 1:
            return filtered[0]
        return None
    if isinstance(types, str):
        return types
    return None


def _python_type(schema: dict[str, Any], by_id: dict[str, SchemaInfo]) -> str:
    resolved = _resolve_composite(schema, by_id, _python_type)
    if resolved is not None:
        return resolved
    base = _base_type(schema)
    return _python_type_from_base(base, schema, by_id)


def _python_type_from_base(
    base: str | None,
    schema: dict[str, Any],
    by_id: dict[str, SchemaInfo],
) -> str:
    if base is None:
        msg = f"Unsupported schema type: {schema}"
        raise TypeError(msg)
    primitives = {"string": "str", "integer": "int", "boolean": "bool"}
    primitive = primitives.get(base)
    if primitive is not None:
        return primitive
    if base == "array":
        items = schema.get("items")
        if not isinstance(items, dict):
            msg = "Array schema missing items."
            raise TypeError(msg)
        return f"tuple[{_python_type(items, by_id)}, ...]"
    if base == "object":
        additional = schema.get("additionalProperties")
        if isinstance(additional, dict):
            value_type = _python_type(additional, by_id)
            return f"dict[str, {value_type}]"
        return "dict[str, object]"
    msg = f"Unsupported schema type: {schema}"
    raise TypeError(msg)


def _rust_int_type(schema: dict[str, Any]) -> str:
    fmt = schema.get("format")
    if fmt == "int32":
        return "i32"
    if fmt == "int64":
        return "i64"
    return "i64"


def _rust_type(schema: dict[str, Any], by_id: dict[str, SchemaInfo]) -> str:
    resolved = _resolve_composite(schema, by_id, _rust_type)
    if resolved is not None:
        return resolved
    base = _base_type(schema)
    return _rust_type_from_base(base, schema, by_id)


def _rust_type_from_base(
    base: str | None,
    schema: dict[str, Any],
    by_id: dict[str, SchemaInfo],
) -> str:
    if base is None:
        msg = f"Unsupported schema type: {schema}"
        raise TypeError(msg)
    primitives = {"string": "String", "boolean": "bool"}
    primitive = primitives.get(base)
    if primitive is not None:
        return primitive
    if base == "integer":
        return _rust_int_type(schema)
    if base == "array":
        items = schema.get("items")
        if not isinstance(items, dict):
            msg = "Array schema missing items."
            raise TypeError(msg)
        return f"Vec<{_rust_type(items, by_id)}>"
    if base == "object":
        additional = schema.get("additionalProperties")
        if isinstance(additional, dict):
            value_type = _rust_type(additional, by_id)
            return f"HashMap<String, {value_type}>"
        return "HashMap<String, serde_json::Value>"
    msg = f"Unsupported schema type: {schema}"
    raise TypeError(msg)


def _resolve_composite(
    schema: dict[str, Any],
    by_id: dict[str, SchemaInfo],
    resolver: Callable[[dict[str, Any], dict[str, SchemaInfo]], str],
) -> str | None:
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        for item in any_of:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "null":
                continue
            return resolver(item, by_id)
    ref = schema.get("$ref")
    if isinstance(ref, str):
        return _resolve_ref(ref, by_id).title
    if ref is not None:
        msg = "Invalid $ref in schema."
        raise TypeError(msg)
    return None


def _python_field_line(name: str, schema: dict[str, Any], by_id: dict[str, SchemaInfo]) -> str:
    typ = _python_type(schema, by_id)
    if _is_optional(schema):
        typ = f"{typ} | None"
    default = " = None" if _is_optional(schema) else ""
    if typ.startswith("dict[") and not _is_optional(schema):
        return f"    {name}: {typ} = field(default_factory=dict)"
    if typ.startswith("tuple[") and not _is_optional(schema):
        return f"    {name}: {typ} = ()"
    if default:
        return f"    {name}: {typ}{default}"
    return f"    {name}: {typ}"


def _rust_field_line(name: str, schema: dict[str, Any], by_id: dict[str, SchemaInfo]) -> str:
    typ = _rust_type(schema, by_id)
    if _is_optional(schema):
        typ = f"Option<{typ}>"
    return f'    #[cfg_attr(feature = "python", pyo3(get, set))]\n    pub {name}: {typ},'


def _generate_python(types: dict[str, SchemaInfo], by_id: dict[str, SchemaInfo]) -> str:
    lines: list[str] = [
        '"""Generated Delta protocol types (auto-generated)."""',
        "",
        "# Auto-generated by scripts/codegen_delta_types.py. Do not edit.",
        "",
        "from __future__ import annotations",
        "",
        "from dataclasses import dataclass, field",
        "",
        "__all__ = [",
    ]
    lines.extend([f'    "{name}",' for name in sorted(types)])
    lines.append("]")
    lines.append("")
    for name in sorted(types):
        schema = types[name].payload
        lines.append("@dataclass(frozen=True)")
        lines.append(f"class {name}:")
        lines.append(f'    """Generated data class for {name}."""')
        properties = schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            lines.append("    ...")
            lines.append("")
            continue
        for prop_name, prop_schema in properties.items():
            if not isinstance(prop_schema, dict):
                msg = f"Invalid property schema for {name}.{prop_name}"
                raise TypeError(msg)
            lines.append(_python_field_line(prop_name, prop_schema, by_id))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _generate_rust(types: dict[str, SchemaInfo], by_id: dict[str, SchemaInfo]) -> str:
    lines: list[str] = [
        "// Auto-generated by scripts/codegen_delta_types.py. Do not edit.",
        "",
        "use std::collections::HashMap;",
        "",
        "use serde::{Deserialize, Serialize};",
        "",
    ]
    for name in sorted(types):
        schema = types[name].payload
        derives = "#[derive(Debug, Clone, Serialize, Deserialize, Default)]"
        if name == "DeltaAppTransaction":
            derives = "#[derive(Debug, Clone, Serialize, Deserialize)]"
        lines.append(derives)
        lines.append('#[cfg_attr(feature = "python", pyo3::pyclass)]')
        lines.append(f"pub struct {name} {{")
        properties = schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            lines.append("}")
            lines.append("")
            continue
        for prop_name, prop_schema in properties.items():
            if not isinstance(prop_schema, dict):
                msg = f"Invalid property schema for {name}.{prop_name}"
                raise TypeError(msg)
            lines.append(_rust_field_line(prop_name, prop_schema, by_id))
        lines.append("}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    """Generate delta types from schemas into Python and Rust outputs."""
    types, by_id = _load_schemas()
    PY_OUT.parent.mkdir(parents=True, exist_ok=True)
    RS_OUT.parent.mkdir(parents=True, exist_ok=True)
    python_payload = _generate_python(types, by_id)
    rust_payload = _generate_rust(types, by_id)
    PY_OUT.write_text(python_payload, encoding="utf-8")
    RS_OUT.write_text(rust_payload, encoding="utf-8")
    init_path = PY_OUT.parent / "__init__.py"
    if not init_path.exists():
        init_path.write_text(
            '"""Generated package for Delta types."""\n',
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
