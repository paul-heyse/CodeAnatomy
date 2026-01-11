"""Extract runtime inspection tables in a sandboxed subprocess."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.empty import empty_table
from arrowdsl.ids import hash64_from_parts

SCHEMA_VERSION = 1

type Row = dict[str, object]


def _hash_id(prefix: str, *parts: str) -> str:
    # Row-wise hash64 IDs are needed while building dependent rows.
    hashed = hash64_from_parts(*parts, prefix=prefix)
    return f"{prefix}:{hashed}"


INVALID_PAYLOAD_TYPE = "Runtime inspect output is not a JSON object."


class RuntimeInspectPayloadTypeError(TypeError):
    """Raised when runtime inspect output has an unexpected type."""

    def __init__(self) -> None:
        super().__init__(INVALID_PAYLOAD_TYPE)


@dataclass(frozen=True)
class RuntimeInspectOptions:
    """Configure runtime inspection extraction."""

    module_allowlist: Sequence[str]
    timeout_s: int = 15


@dataclass(frozen=True)
class RuntimeInspectResult:
    """Runtime inspection tables for objects, signatures, and members."""

    rt_objects: pa.Table
    rt_signatures: pa.Table
    rt_signature_params: pa.Table
    rt_members: pa.Table


RT_OBJECTS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("rt_id", pa.string()),
        ("module", pa.string()),
        ("qualname", pa.string()),
        ("name", pa.string()),
        ("obj_type", pa.string()),
        ("source_path", pa.string()),
        ("source_line", pa.int32()),
    ]
)

RT_SIGNATURES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("sig_id", pa.string()),
        ("rt_id", pa.string()),
        ("signature", pa.string()),
        ("return_annotation", pa.string()),
    ]
)

RT_SIGNATURE_PARAMS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("param_id", pa.string()),
        ("sig_id", pa.string()),
        ("name", pa.string()),
        ("kind", pa.string()),
        ("default_repr", pa.string()),
        ("annotation_repr", pa.string()),
        ("position", pa.int32()),
    ]
)

RT_MEMBERS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("member_id", pa.string()),
        ("rt_id", pa.string()),
        ("name", pa.string()),
        ("member_kind", pa.string()),
        ("value_repr", pa.string()),
        ("value_module", pa.string()),
        ("value_qualname", pa.string()),
    ]
)


def _inspect_script() -> str:
    return textwrap.dedent(
        """
        import importlib
        import inspect
        import json
        import os
        import sys

        allowlist = json.loads(os.environ.get("CODEANATOMY_ALLOWLIST", "[]"))
        repo_root = os.environ.get("CODEANATOMY_REPO_ROOT")
        if repo_root:
            sys.path.insert(0, repo_root)

        def is_allowed(module_name: str) -> bool:
            return any(
                module_name == prefix or module_name.startswith(prefix + ".")
                for prefix in allowlist
            )

        def obj_key(module_name: str, qualname: str) -> str:
            return f"{module_name}:{qualname}"

        def safe_repr(value: object) -> str | None:
            try:
                return repr(value)
            except Exception:
                return None

        def source_path(obj: object) -> str | None:
            try:
                path = inspect.getsourcefile(obj) or inspect.getfile(obj)
                return path if isinstance(path, str) else None
            except Exception:
                return None

        def source_line(obj: object) -> int | None:
            try:
                lines, line_no = inspect.getsourcelines(obj)
                _ = lines
                return int(line_no)
            except Exception:
                return None

        def obj_type(obj: object) -> str:
            if inspect.isclass(obj):
                return "class"
            if inspect.isfunction(obj):
                return "function"
            if inspect.ismethod(obj):
                return "method"
            if inspect.isbuiltin(obj):
                return "builtin"
            if inspect.ismodule(obj):
                return "module"
            return "object"

        def member_kind(obj: object) -> str:
            if inspect.isclass(obj):
                return "class"
            if inspect.isfunction(obj) or inspect.ismethod(obj):
                return "function"
            if inspect.isbuiltin(obj):
                return "builtin"
            if inspect.ismodule(obj):
                return "module"
            return "attribute"

        objects = []
        signatures = []
        members = []
        errors = []
        seen = set()

        for mod_name in allowlist:
            try:
                module = importlib.import_module(mod_name)
            except Exception as exc:
                errors.append({"module": mod_name, "stage": "import", "error": str(exc)})
                continue

            mod_key = obj_key(module.__name__, "<module>")
            if mod_key not in seen:
                seen.add(mod_key)
                objects.append(
                    {
                        "object_key": mod_key,
                        "module": module.__name__,
                        "qualname": "<module>",
                        "name": module.__name__.rsplit(".", 1)[-1],
                        "obj_type": "module",
                        "source_path": getattr(module, "__file__", None),
                        "source_line": None,
                    }
                )

            try:
                mod_members = inspect.getmembers_static(module)
            except Exception as exc:
                errors.append({"module": module.__name__, "stage": "members", "error": str(exc)})
                continue

            for name, value in mod_members:
                mod = getattr(value, "__module__", None)
                if isinstance(mod, str) and not is_allowed(mod):
                    continue
                qualname = getattr(value, "__qualname__", None) or name
                module_name = mod if isinstance(mod, str) else module.__name__
                key = obj_key(module_name, qualname)
                members.append(
                    {
                        "owner_key": mod_key,
                        "name": name,
                        "member_kind": member_kind(value),
                        "value_repr": safe_repr(value),
                        "value_module": module_name,
                        "value_qualname": qualname,
                    }
                )
                if key in seen:
                    continue
                if inspect.isclass(value) or inspect.isfunction(value) or inspect.ismethod(value):
                    seen.add(key)
                    objects.append(
                        {
                            "object_key": key,
                            "module": module_name,
                            "qualname": qualname,
                            "name": name,
                            "obj_type": obj_type(value),
                            "source_path": source_path(value),
                            "source_line": source_line(value),
                        }
                    )
                    try:
                        sig = inspect.signature(value)
                    except Exception:
                        continue
                    params = []
                    for idx, param in enumerate(sig.parameters.values()):
                        params.append(
                            {
                                "name": param.name,
                                "kind": str(param.kind),
                                "default": None
                                if param.default is inspect._empty
                                else safe_repr(param.default),
                                "annotation": None
                                if param.annotation is inspect._empty
                                else safe_repr(param.annotation),
                                "position": idx,
                            }
                        )
                    signatures.append(
                        {
                            "object_key": key,
                            "signature": str(sig),
                            "return_annotation": None
                            if sig.return_annotation is inspect._empty
                            else safe_repr(sig.return_annotation),
                            "parameters": params,
                        }
                    )

        payload = {
            "objects": objects,
            "signatures": signatures,
            "members": members,
            "errors": errors,
        }
        print(json.dumps(payload))
        """
    ).strip()


def _run_inspect_subprocess(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
) -> dict[str, object]:
    env = os.environ.copy()
    env["CODEANATOMY_REPO_ROOT"] = repo_root
    env["CODEANATOMY_ALLOWLIST"] = json.dumps([str(m) for m in module_allowlist])

    result = subprocess.run(
        [sys.executable, "-c", _inspect_script()],
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
        env=env,
        cwd=repo_root,
    )

    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or "runtime inspect failed"
        raise RuntimeError(msg)

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        msg = f"Failed to parse runtime inspect output: {exc}"
        raise RuntimeError(msg) from exc

    if not isinstance(payload, dict):
        raise RuntimeInspectPayloadTypeError
    return payload


def _object_id(module: str, qualname: str) -> str:
    return _hash_id("rt_obj", module, qualname)


def _parse_runtime_objects(objects_raw: object) -> tuple[list[Row], dict[str, str]]:
    obj_rows: list[Row] = []
    object_id_by_key: dict[str, str] = {}
    if not isinstance(objects_raw, list):
        return obj_rows, object_id_by_key
    for obj in objects_raw:
        if not isinstance(obj, dict):
            continue
        module = obj.get("module")
        qualname = obj.get("qualname")
        name = obj.get("name")
        obj_type = obj.get("obj_type")
        if not isinstance(module, str) or not isinstance(qualname, str):
            continue
        key = obj.get("object_key")
        obj_id = _object_id(module, qualname)
        if isinstance(key, str):
            object_id_by_key[key] = obj_id
        obj_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "rt_id": obj_id,
                "module": module,
                "qualname": qualname,
                "name": name if isinstance(name, str) else None,
                "obj_type": obj_type if isinstance(obj_type, str) else None,
                "source_path": obj.get("source_path")
                if isinstance(obj.get("source_path"), str)
                else None,
                "source_line": obj.get("source_line")
                if isinstance(obj.get("source_line"), int)
                else None,
            }
        )
    return obj_rows, object_id_by_key


def _parse_runtime_signatures(
    signatures_raw: object,
    object_id_by_key: dict[str, str],
) -> tuple[list[Row], list[Row]]:
    sig_rows: list[Row] = []
    param_rows: list[Row] = []
    if not isinstance(signatures_raw, list):
        return sig_rows, param_rows
    for sig in signatures_raw:
        if not isinstance(sig, dict):
            continue
        key = sig.get("object_key")
        if not isinstance(key, str):
            continue
        rt_id = object_id_by_key.get(key)
        if rt_id is None:
            continue
        signature_str = sig.get("signature")
        if not isinstance(signature_str, str):
            continue
        sig_id = _hash_id("rt_sig", rt_id, signature_str)
        sig_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "sig_id": sig_id,
                "rt_id": rt_id,
                "signature": signature_str,
                "return_annotation": sig.get("return_annotation")
                if isinstance(sig.get("return_annotation"), str)
                else None,
            }
        )
        params = sig.get("parameters")
        if not isinstance(params, list):
            continue
        param_rows.extend(_parse_runtime_params(params, sig_id))
    return sig_rows, param_rows


def _parse_runtime_params(params: list[object], sig_id: str) -> list[Row]:
    rows: list[Row] = []
    for param in params:
        if not isinstance(param, dict):
            continue
        name = param.get("name")
        if not isinstance(name, str):
            continue
        param_id = _hash_id("rt_param", sig_id, name)
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "param_id": param_id,
                "sig_id": sig_id,
                "name": name,
                "kind": param.get("kind") if isinstance(param.get("kind"), str) else None,
                "default_repr": param.get("default")
                if isinstance(param.get("default"), str)
                else None,
                "annotation_repr": param.get("annotation")
                if isinstance(param.get("annotation"), str)
                else None,
                "position": param.get("position")
                if isinstance(param.get("position"), int)
                else None,
            }
        )
    return rows


def _parse_runtime_members(
    members_raw: object,
    object_id_by_key: dict[str, str],
) -> list[Row]:
    member_rows: list[Row] = []
    if not isinstance(members_raw, list):
        return member_rows
    for member in members_raw:
        if not isinstance(member, dict):
            continue
        owner_key = member.get("owner_key")
        if not isinstance(owner_key, str):
            continue
        rt_id = object_id_by_key.get(owner_key)
        if rt_id is None:
            continue
        name = member.get("name")
        if not isinstance(name, str):
            continue
        member_id = _hash_id("rt_member", rt_id, name)
        member_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "member_id": member_id,
                "rt_id": rt_id,
                "name": name,
                "member_kind": member.get("member_kind")
                if isinstance(member.get("member_kind"), str)
                else None,
                "value_repr": member.get("value_repr")
                if isinstance(member.get("value_repr"), str)
                else None,
                "value_module": member.get("value_module")
                if isinstance(member.get("value_module"), str)
                else None,
                "value_qualname": member.get("value_qualname")
                if isinstance(member.get("value_qualname"), str)
                else None,
            }
        )
    return member_rows


def _runtime_tables_from_rows(
    *,
    obj_rows: list[Row],
    sig_rows: list[Row],
    param_rows: list[Row],
    member_rows: list[Row],
) -> RuntimeInspectResult:
    rt_objects = (
        pa.Table.from_pylist(obj_rows, schema=RT_OBJECTS_SCHEMA)
        if obj_rows
        else empty_table(RT_OBJECTS_SCHEMA)
    )
    rt_signatures = (
        pa.Table.from_pylist(sig_rows, schema=RT_SIGNATURES_SCHEMA)
        if sig_rows
        else empty_table(RT_SIGNATURES_SCHEMA)
    )
    rt_params = (
        pa.Table.from_pylist(param_rows, schema=RT_SIGNATURE_PARAMS_SCHEMA)
        if param_rows
        else empty_table(RT_SIGNATURE_PARAMS_SCHEMA)
    )
    rt_members = (
        pa.Table.from_pylist(member_rows, schema=RT_MEMBERS_SCHEMA)
        if member_rows
        else empty_table(RT_MEMBERS_SCHEMA)
    )
    return RuntimeInspectResult(
        rt_objects=rt_objects,
        rt_signatures=rt_signatures,
        rt_signature_params=rt_params,
        rt_members=rt_members,
    )


def extract_runtime_tables(
    repo_root: str,
    *,
    options: RuntimeInspectOptions,
) -> RuntimeInspectResult:
    """Extract runtime inspection tables via subprocess.

    Parameters
    ----------
    repo_root:
        Repository root path for module imports.
    options:
        Runtime inspect options.

    Returns
    -------
    RuntimeInspectResult
        Extracted runtime inspection tables.
    """
    if not options.module_allowlist:
        return RuntimeInspectResult(
            rt_objects=empty_table(RT_OBJECTS_SCHEMA),
            rt_signatures=empty_table(RT_SIGNATURES_SCHEMA),
            rt_signature_params=empty_table(RT_SIGNATURE_PARAMS_SCHEMA),
            rt_members=empty_table(RT_MEMBERS_SCHEMA),
        )

    payload = _run_inspect_subprocess(
        repo_root,
        module_allowlist=options.module_allowlist,
        timeout_s=options.timeout_s,
    )

    obj_rows, object_id_by_key = _parse_runtime_objects(payload.get("objects"))
    sig_rows, param_rows = _parse_runtime_signatures(
        payload.get("signatures"),
        object_id_by_key,
    )
    member_rows = _parse_runtime_members(payload.get("members"), object_id_by_key)
    return _runtime_tables_from_rows(
        obj_rows=obj_rows,
        sig_rows=sig_rows,
        param_rows=param_rows,
        member_rows=member_rows,
    )


def extract_runtime_objects(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
) -> pa.Table:
    """Extract runtime objects via subprocess.

    Returns
    -------
    pa.Table
        Runtime object table.
    """
    result = extract_runtime_tables(
        repo_root,
        options=RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s),
    )
    return result.rt_objects


def extract_runtime_signatures(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
) -> dict[str, pa.Table]:
    """Extract runtime signatures and parameters via subprocess.

    Returns
    -------
    dict[str, pa.Table]
        Signature bundle with ``rt_signatures`` and ``rt_signature_params``.
    """
    result = extract_runtime_tables(
        repo_root,
        options=RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s),
    )
    return {
        "rt_signatures": result.rt_signatures,
        "rt_signature_params": result.rt_signature_params,
    }


def extract_runtime_members(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
) -> pa.Table:
    """Extract runtime members via subprocess.

    Returns
    -------
    pa.Table
        Runtime member table.
    """
    result = extract_runtime_tables(
        repo_root,
        options=RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s),
    )
    return result.rt_members
