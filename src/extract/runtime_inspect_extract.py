"""Extract runtime inspection tables in a sandboxed subprocess using shared helpers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext, RuntimeProfile
from arrowdsl.core.ids import HashSpec
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike, pc
from arrowdsl.plan.plan import Plan
from arrowdsl.schema.schema import empty_table
from extract.hashing import apply_hash_projection
from extract.join_helpers import JoinConfig, left_join
from extract.spec_helpers import register_dataset
from extract.tables import (
    align_plan,
    apply_query_spec,
    finalize_plan_bundle,
    materialize_plan,
    plan_from_rows,
    query_for_schema,
)
from schema_spec.specs import ArrowFieldSpec

SCHEMA_VERSION = 1

type Row = dict[str, object]


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

    rt_objects: TableLike
    rt_signatures: TableLike
    rt_signature_params: TableLike
    rt_members: TableLike


RT_OBJECTS_SPEC = register_dataset(
    name="rt_objects_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=[
        ArrowFieldSpec(name="rt_id", dtype=pa.string()),
        ArrowFieldSpec(name="module", dtype=pa.string()),
        ArrowFieldSpec(name="qualname", dtype=pa.string()),
        ArrowFieldSpec(name="name", dtype=pa.string()),
        ArrowFieldSpec(name="obj_type", dtype=pa.string()),
        ArrowFieldSpec(name="source_path", dtype=pa.string()),
        ArrowFieldSpec(name="source_line", dtype=pa.int32()),
    ],
)

RT_SIGNATURES_SPEC = register_dataset(
    name="rt_signatures_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=[
        ArrowFieldSpec(name="sig_id", dtype=pa.string()),
        ArrowFieldSpec(name="rt_id", dtype=pa.string()),
        ArrowFieldSpec(name="signature", dtype=pa.string()),
        ArrowFieldSpec(name="return_annotation", dtype=pa.string()),
    ],
)

RT_SIGNATURE_PARAMS_SPEC = register_dataset(
    name="rt_signature_params_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=[
        ArrowFieldSpec(name="param_id", dtype=pa.string()),
        ArrowFieldSpec(name="sig_id", dtype=pa.string()),
        ArrowFieldSpec(name="name", dtype=pa.string()),
        ArrowFieldSpec(name="kind", dtype=pa.string()),
        ArrowFieldSpec(name="default_repr", dtype=pa.string()),
        ArrowFieldSpec(name="annotation_repr", dtype=pa.string()),
        ArrowFieldSpec(name="position", dtype=pa.int32()),
    ],
)

RT_MEMBERS_SPEC = register_dataset(
    name="rt_members_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=[
        ArrowFieldSpec(name="member_id", dtype=pa.string()),
        ArrowFieldSpec(name="rt_id", dtype=pa.string()),
        ArrowFieldSpec(name="name", dtype=pa.string()),
        ArrowFieldSpec(name="member_kind", dtype=pa.string()),
        ArrowFieldSpec(name="value_repr", dtype=pa.string()),
        ArrowFieldSpec(name="value_module", dtype=pa.string()),
        ArrowFieldSpec(name="value_qualname", dtype=pa.string()),
    ],
)

RT_OBJECTS_SCHEMA = RT_OBJECTS_SPEC.table_spec.to_arrow_schema()
RT_SIGNATURES_SCHEMA = RT_SIGNATURES_SPEC.table_spec.to_arrow_schema()
RT_SIGNATURE_PARAMS_SCHEMA = RT_SIGNATURE_PARAMS_SPEC.table_spec.to_arrow_schema()
RT_MEMBERS_SCHEMA = RT_MEMBERS_SPEC.table_spec.to_arrow_schema()

RT_OBJECT_ROWS_SCHEMA = pa.schema(
    [
        pa.field("object_key", pa.string()),
        pa.field("module", pa.string()),
        pa.field("qualname", pa.string()),
        pa.field("name", pa.string()),
        pa.field("obj_type", pa.string()),
        pa.field("source_path", pa.string()),
        pa.field("source_line", pa.int32()),
    ]
)

RT_SIGNATURE_ROWS_SCHEMA = pa.schema(
    [
        pa.field("object_key", pa.string()),
        pa.field("signature", pa.string()),
        pa.field("return_annotation", pa.string()),
    ]
)

RT_PARAM_ROWS_SCHEMA = pa.schema(
    [
        pa.field("object_key", pa.string()),
        pa.field("signature", pa.string()),
        pa.field("name", pa.string()),
        pa.field("kind", pa.string()),
        pa.field("default_repr", pa.string()),
        pa.field("annotation_repr", pa.string()),
        pa.field("position", pa.int32()),
    ]
)

RT_MEMBER_ROWS_SCHEMA = pa.schema(
    [
        pa.field("object_key", pa.string()),
        pa.field("name", pa.string()),
        pa.field("member_kind", pa.string()),
        pa.field("value_repr", pa.string()),
        pa.field("value_module", pa.string()),
        pa.field("value_qualname", pa.string()),
    ]
)

RT_OBJECTS_QUERY = query_for_schema(RT_OBJECTS_SCHEMA)
RT_SIGNATURES_QUERY = query_for_schema(RT_SIGNATURES_SCHEMA)
RT_SIGNATURE_PARAMS_QUERY = query_for_schema(RT_SIGNATURE_PARAMS_SCHEMA)
RT_MEMBERS_QUERY = query_for_schema(RT_MEMBERS_SCHEMA)


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


def _parse_runtime_objects(objects_raw: object) -> list[Row]:
    obj_rows: list[Row] = []
    if not isinstance(objects_raw, list):
        return obj_rows
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
        obj_rows.append(
            {
                "object_key": key if isinstance(key, str) else None,
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
    return obj_rows


def _parse_runtime_signatures(signatures_raw: object) -> tuple[list[Row], list[Row]]:
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
        signature_str = sig.get("signature")
        if not isinstance(signature_str, str):
            continue
        sig_rows.append(
            {
                "object_key": key,
                "signature": signature_str,
                "return_annotation": sig.get("return_annotation")
                if isinstance(sig.get("return_annotation"), str)
                else None,
            }
        )
        params = sig.get("parameters")
        if not isinstance(params, list):
            continue
        param_rows.extend(_parse_runtime_params(params, object_key=key, signature=signature_str))
    return sig_rows, param_rows


def _parse_runtime_params(
    params: list[object],
    *,
    object_key: str,
    signature: str,
) -> list[Row]:
    rows: list[Row] = []
    for param in params:
        if not isinstance(param, dict):
            continue
        name = param.get("name")
        if not isinstance(name, str):
            continue
        rows.append(
            {
                "object_key": object_key,
                "signature": signature,
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


def _parse_runtime_members(members_raw: object) -> list[Row]:
    member_rows: list[Row] = []
    if not isinstance(members_raw, list):
        return member_rows
    for member in members_raw:
        if not isinstance(member, dict):
            continue
        owner_key = member.get("owner_key")
        if not isinstance(owner_key, str):
            continue
        name = member.get("name")
        if not isinstance(name, str):
            continue
        member_rows.append(
            {
                "object_key": owner_key,
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


def _empty_runtime_result() -> RuntimeInspectResult:
    return RuntimeInspectResult(
        rt_objects=empty_table(RT_OBJECTS_SCHEMA),
        rt_signatures=empty_table(RT_SIGNATURES_SCHEMA),
        rt_signature_params=empty_table(RT_SIGNATURE_PARAMS_SCHEMA),
        rt_members=empty_table(RT_MEMBERS_SCHEMA),
    )


def _build_rt_objects(
    obj_rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
) -> tuple[Plan, Plan]:
    rt_objects_plan = plan_from_rows(
        obj_rows,
        schema=RT_OBJECT_ROWS_SCHEMA,
        label="rt_objects_raw",
    )
    rt_objects_plan = apply_hash_projection(
        rt_objects_plan,
        specs=(HashSpec(prefix="rt_obj", cols=("module", "qualname"), out_col="rt_id"),),
        available=RT_OBJECT_ROWS_SCHEMA.names,
        required={"rt_id": ("module", "qualname")},
        ctx=exec_ctx,
    )
    rt_objects_key_plan = rt_objects_plan.project(
        [pc.field("object_key"), pc.field("rt_id")],
        ["object_key", "rt_id"],
        ctx=exec_ctx,
    )
    rt_objects_plan = apply_query_spec(rt_objects_plan, spec=RT_OBJECTS_QUERY, ctx=exec_ctx)
    rt_objects_plan = align_plan(
        rt_objects_plan,
        schema=RT_OBJECTS_SCHEMA,
        available=RT_OBJECTS_SCHEMA.names,
        ctx=exec_ctx,
    )
    return rt_objects_plan, rt_objects_key_plan


def _build_rt_signatures(
    sig_rows: list[Row],
    *,
    rt_objects_key_plan: Plan,
    exec_ctx: ExecutionContext,
) -> tuple[Plan, Plan | None]:
    if not sig_rows:
        empty_plan = Plan.table_source(empty_table(RT_SIGNATURES_SCHEMA))
        return empty_plan, None
    sig_plan = plan_from_rows(sig_rows, schema=RT_SIGNATURE_ROWS_SCHEMA, label="rt_signatures_raw")
    sig_cols = list(RT_SIGNATURE_ROWS_SCHEMA.names)
    if "object_key" in sig_cols:
        join_config = JoinConfig.from_sequences(
            left_keys=("object_key",),
            right_keys=("object_key",),
            left_output=tuple(RT_SIGNATURE_ROWS_SCHEMA.names),
            right_output=("rt_id",),
        )
        sig_plan = left_join(
            sig_plan,
            rt_objects_key_plan,
            config=join_config,
            use_threads=exec_ctx.use_threads,
            ctx=exec_ctx,
        )
        sig_cols = list(join_config.left_output) + list(join_config.right_output)
    sig_plan = apply_hash_projection(
        sig_plan,
        specs=(HashSpec(prefix="rt_sig", cols=("rt_id", "signature"), out_col="sig_id"),),
        available=sig_cols,
        required={"sig_id": ("rt_id", "signature")},
        ctx=exec_ctx,
    )
    sig_meta_plan = None
    if {"object_key", "signature"} <= set(sig_cols):
        sig_meta_plan = sig_plan.project(
            [pc.field("object_key"), pc.field("signature"), pc.field("sig_id")],
            ["object_key", "signature", "sig_id"],
            ctx=exec_ctx,
        )
    sig_plan = apply_query_spec(sig_plan, spec=RT_SIGNATURES_QUERY, ctx=exec_ctx)
    sig_plan = align_plan(
        sig_plan,
        schema=RT_SIGNATURES_SCHEMA,
        available=RT_SIGNATURES_SCHEMA.names,
        ctx=exec_ctx,
    )
    return sig_plan, sig_meta_plan


def _build_rt_params(
    param_rows: list[Row],
    *,
    sig_meta_plan: Plan | None,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not param_rows or sig_meta_plan is None:
        return Plan.table_source(empty_table(RT_SIGNATURE_PARAMS_SCHEMA))
    param_plan = plan_from_rows(param_rows, schema=RT_PARAM_ROWS_SCHEMA, label="rt_params_raw")
    param_cols = list(RT_PARAM_ROWS_SCHEMA.names)
    if {"object_key", "signature"} <= set(param_cols):
        join_config = JoinConfig.from_sequences(
            left_keys=("object_key", "signature"),
            right_keys=("object_key", "signature"),
            left_output=tuple(RT_PARAM_ROWS_SCHEMA.names),
            right_output=("sig_id",),
        )
        param_plan = left_join(
            param_plan,
            sig_meta_plan,
            config=join_config,
            use_threads=exec_ctx.use_threads,
            ctx=exec_ctx,
        )
        param_cols = list(join_config.left_output) + list(join_config.right_output)
    param_plan = apply_hash_projection(
        param_plan,
        specs=(HashSpec(prefix="rt_param", cols=("sig_id", "name"), out_col="param_id"),),
        available=param_cols,
        required={"param_id": ("sig_id", "name")},
        ctx=exec_ctx,
    )
    param_plan = apply_query_spec(param_plan, spec=RT_SIGNATURE_PARAMS_QUERY, ctx=exec_ctx)
    return align_plan(
        param_plan,
        schema=RT_SIGNATURE_PARAMS_SCHEMA,
        available=RT_SIGNATURE_PARAMS_SCHEMA.names,
        ctx=exec_ctx,
    )


def _build_rt_members(
    member_rows: list[Row],
    *,
    rt_objects_key_plan: Plan,
    exec_ctx: ExecutionContext,
) -> Plan:
    if not member_rows:
        return Plan.table_source(empty_table(RT_MEMBERS_SCHEMA))
    member_plan = plan_from_rows(member_rows, schema=RT_MEMBER_ROWS_SCHEMA, label="rt_members_raw")
    member_cols = list(RT_MEMBER_ROWS_SCHEMA.names)
    if "object_key" in member_cols:
        join_config = JoinConfig.from_sequences(
            left_keys=("object_key",),
            right_keys=("object_key",),
            left_output=tuple(RT_MEMBER_ROWS_SCHEMA.names),
            right_output=("rt_id",),
        )
        member_plan = left_join(
            member_plan,
            rt_objects_key_plan,
            config=join_config,
            use_threads=exec_ctx.use_threads,
            ctx=exec_ctx,
        )
        member_cols = list(join_config.left_output) + list(join_config.right_output)
    member_plan = apply_hash_projection(
        member_plan,
        specs=(HashSpec(prefix="rt_member", cols=("rt_id", "name"), out_col="member_id"),),
        available=member_cols,
        required={"member_id": ("rt_id", "name")},
        ctx=exec_ctx,
    )
    member_plan = apply_query_spec(member_plan, spec=RT_MEMBERS_QUERY, ctx=exec_ctx)
    return align_plan(
        member_plan,
        schema=RT_MEMBERS_SCHEMA,
        available=RT_MEMBERS_SCHEMA.names,
        ctx=exec_ctx,
    )


def _runtime_tables_from_rows(
    *,
    obj_rows: list[Row],
    sig_rows: list[Row],
    param_rows: list[Row],
    member_rows: list[Row],
    exec_ctx: ExecutionContext,
) -> RuntimeInspectResult:
    if not obj_rows:
        return _empty_runtime_result()

    rt_objects_plan, rt_objects_key_plan = _build_rt_objects(obj_rows, exec_ctx=exec_ctx)
    rt_signatures_plan, sig_meta_plan = _build_rt_signatures(
        sig_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        exec_ctx=exec_ctx,
    )
    rt_params_plan = _build_rt_params(
        param_rows,
        sig_meta_plan=sig_meta_plan,
        exec_ctx=exec_ctx,
    )
    rt_members_plan = _build_rt_members(
        member_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        exec_ctx=exec_ctx,
    )

    return RuntimeInspectResult(
        rt_objects=materialize_plan(rt_objects_plan, ctx=exec_ctx),
        rt_signatures=materialize_plan(rt_signatures_plan, ctx=exec_ctx),
        rt_signature_params=materialize_plan(rt_params_plan, ctx=exec_ctx),
        rt_members=materialize_plan(rt_members_plan, ctx=exec_ctx),
    )


def _runtime_plans_from_rows(
    *,
    obj_rows: list[Row],
    sig_rows: list[Row],
    param_rows: list[Row],
    member_rows: list[Row],
    exec_ctx: ExecutionContext,
) -> dict[str, Plan]:
    if not obj_rows:
        empty = Plan.table_source(empty_table(RT_OBJECTS_SCHEMA))
        return {
            "rt_objects": empty,
            "rt_signatures": Plan.table_source(empty_table(RT_SIGNATURES_SCHEMA)),
            "rt_signature_params": Plan.table_source(empty_table(RT_SIGNATURE_PARAMS_SCHEMA)),
            "rt_members": Plan.table_source(empty_table(RT_MEMBERS_SCHEMA)),
        }
    rt_objects_plan, rt_objects_key_plan = _build_rt_objects(obj_rows, exec_ctx=exec_ctx)
    rt_signatures_plan, sig_meta_plan = _build_rt_signatures(
        sig_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        exec_ctx=exec_ctx,
    )
    rt_params_plan = _build_rt_params(param_rows, sig_meta_plan=sig_meta_plan, exec_ctx=exec_ctx)
    rt_members_plan = _build_rt_members(
        member_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        exec_ctx=exec_ctx,
    )
    return {
        "rt_objects": rt_objects_plan,
        "rt_signatures": rt_signatures_plan,
        "rt_signature_params": rt_params_plan,
        "rt_members": rt_members_plan,
    }


def extract_runtime_tables(
    repo_root: str,
    *,
    options: RuntimeInspectOptions,
    ctx: ExecutionContext | None = None,
) -> RuntimeInspectResult:
    """Extract runtime inspection tables via subprocess.

    Parameters
    ----------
    repo_root:
        Repository root path for module imports.
    options:
        Runtime inspect options.
    ctx:
        Execution context for plan execution.

    Returns
    -------
    RuntimeInspectResult
        Extracted runtime inspection tables.
    """
    exec_ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    if not options.module_allowlist:
        return _empty_runtime_result()

    payload = _run_inspect_subprocess(
        repo_root,
        module_allowlist=options.module_allowlist,
        timeout_s=options.timeout_s,
    )

    obj_rows = _parse_runtime_objects(payload.get("objects"))
    sig_rows, param_rows = _parse_runtime_signatures(payload.get("signatures"))
    member_rows = _parse_runtime_members(payload.get("members"))
    return _runtime_tables_from_rows(
        obj_rows=obj_rows,
        sig_rows=sig_rows,
        param_rows=param_rows,
        member_rows=member_rows,
        exec_ctx=exec_ctx,
    )


def extract_runtime_plans(
    repo_root: str,
    *,
    options: RuntimeInspectOptions,
    ctx: ExecutionContext | None = None,
) -> dict[str, Plan]:
    """Extract runtime inspection plans via subprocess.

    Returns
    -------
    dict[str, Plan]
        Plan bundle keyed by runtime inspection table name.
    """
    exec_ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    if not options.module_allowlist:
        return {
            "rt_objects": Plan.table_source(empty_table(RT_OBJECTS_SCHEMA)),
            "rt_signatures": Plan.table_source(empty_table(RT_SIGNATURES_SCHEMA)),
            "rt_signature_params": Plan.table_source(empty_table(RT_SIGNATURE_PARAMS_SCHEMA)),
            "rt_members": Plan.table_source(empty_table(RT_MEMBERS_SCHEMA)),
        }

    payload = _run_inspect_subprocess(
        repo_root,
        module_allowlist=options.module_allowlist,
        timeout_s=options.timeout_s,
    )

    obj_rows = _parse_runtime_objects(payload.get("objects"))
    sig_rows, param_rows = _parse_runtime_signatures(payload.get("signatures"))
    member_rows = _parse_runtime_members(payload.get("members"))
    return _runtime_plans_from_rows(
        obj_rows=obj_rows,
        sig_rows=sig_rows,
        param_rows=param_rows,
        member_rows=member_rows,
        exec_ctx=exec_ctx,
    )


def extract_runtime_objects(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
    prefer_reader: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Extract runtime objects via subprocess.

    prefer_reader:
        When True, return a streaming reader when possible.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Runtime object output.
    """
    plans = extract_runtime_plans(
        repo_root,
        options=RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s),
    )
    return finalize_plan_bundle(
        {"rt_objects": plans["rt_objects"]},
        ctx=ExecutionContext(runtime=RuntimeProfile(name="DEFAULT")),
        prefer_reader=prefer_reader,
    )["rt_objects"]


def extract_runtime_signatures(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
    prefer_reader: bool = False,
) -> dict[str, TableLike | RecordBatchReaderLike]:
    """Extract runtime signatures and parameters via subprocess.

    prefer_reader:
        When True, return streaming readers when possible.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Signature bundle with ``rt_signatures`` and ``rt_signature_params``.
    """
    plans = extract_runtime_plans(
        repo_root,
        options=RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s),
    )
    return finalize_plan_bundle(
        {
            "rt_signatures": plans["rt_signatures"],
            "rt_signature_params": plans["rt_signature_params"],
        },
        ctx=ExecutionContext(runtime=RuntimeProfile(name="DEFAULT")),
        prefer_reader=prefer_reader,
    )


def extract_runtime_members(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
    prefer_reader: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Extract runtime members via subprocess.

    prefer_reader:
        When True, return a streaming reader when possible.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Runtime member output.
    """
    plans = extract_runtime_plans(
        repo_root,
        options=RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s),
    )
    return finalize_plan_bundle(
        {"rt_members": plans["rt_members"]},
        ctx=ExecutionContext(runtime=RuntimeProfile(name="DEFAULT")),
        prefer_reader=prefer_reader,
    )["rt_members"]
