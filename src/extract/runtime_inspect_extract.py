"""Extract runtime inspection tables in a sandboxed subprocess using shared helpers."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
from pyarrow import ipc

from arrowdsl.core.context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from extract.helpers import (
    ExtractMaterializeOptions,
    apply_query_and_project,
    ibis_plan_from_rows,
    materialize_extract_plan,
)
from extract.registry_specs import dataset_query, dataset_row_schema, normalize_options
from extract.schema_ops import ExtractNormalizeOptions
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import apply_projection

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable

    from extract.evidence_plan import EvidencePlan

type Row = dict[str, object]


INVALID_PAYLOAD_TYPE = "Runtime inspect output is not a valid IPC payload."


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


@dataclass(frozen=True)
class RuntimeRows:
    """Row buffers for runtime inspection extraction."""

    obj_rows: list[Row]
    sig_rows: list[Row]
    param_rows: list[Row]
    member_rows: list[Row]


RT_OBJECTS_ROW_SCHEMA = dataset_row_schema("rt_objects_v1")
RT_SIGNATURES_ROW_SCHEMA = dataset_row_schema("rt_signatures_v1")
RT_PARAMS_ROW_SCHEMA = dataset_row_schema("rt_signature_params_v1")
RT_MEMBERS_ROW_SCHEMA = dataset_row_schema("rt_members_v1")


def _inspect_script() -> str:
    return textwrap.dedent(
        """
        import importlib
        import inspect
        import os
        import sys
        import pyarrow as pa
        from pyarrow import ipc

        raw_allowlist = os.environ.get("CODEANATOMY_ALLOWLIST", "")
        allowlist = [
            item.strip()
            for line in raw_allowlist.replace(",", "\\n").splitlines()
            for item in [line]
            if item.strip()
        ]
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
            "version": 1,
            "objects": objects,
            "signatures": signatures,
            "members": members,
            "errors": errors,
        }
        param_struct = pa.struct(
            [
                pa.field("name", pa.string()),
                pa.field("kind", pa.string()),
                pa.field("default", pa.string()),
                pa.field("annotation", pa.string()),
                pa.field("position", pa.int64()),
            ]
        )
        signature_struct = pa.struct(
            [
                pa.field("object_key", pa.string()),
                pa.field("signature", pa.string()),
                pa.field("return_annotation", pa.string()),
                pa.field("parameters", pa.list_(param_struct)),
            ]
        )
        object_struct = pa.struct(
            [
                pa.field("object_key", pa.string()),
                pa.field("module", pa.string()),
                pa.field("qualname", pa.string()),
                pa.field("name", pa.string()),
                pa.field("obj_type", pa.string()),
                pa.field("source_path", pa.string()),
                pa.field("source_line", pa.int64()),
            ]
        )
        member_struct = pa.struct(
            [
                pa.field("owner_key", pa.string()),
                pa.field("name", pa.string()),
                pa.field("member_kind", pa.string()),
                pa.field("value_repr", pa.string()),
                pa.field("value_module", pa.string()),
                pa.field("value_qualname", pa.string()),
            ]
        )
        error_struct = pa.struct(
            [
                pa.field("module", pa.string()),
                pa.field("stage", pa.string()),
                pa.field("error", pa.string()),
            ]
        )
        schema = pa.schema(
            [
                pa.field("version", pa.int32()),
                pa.field("objects", pa.list_(object_struct)),
                pa.field("signatures", pa.list_(signature_struct)),
                pa.field("members", pa.list_(member_struct)),
                pa.field("errors", pa.list_(error_struct)),
            ]
        )
        table = pa.Table.from_pylist([payload], schema=schema)
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        sys.stdout.buffer.write(sink.getvalue().to_pybytes())
        """
    ).strip()


def _with_derived_columns(table: IbisTable, *, dataset: str) -> IbisTable:
    spec = dataset_query(dataset)
    if not spec.projection.derived:
        return table
    return apply_projection(
        table,
        base=table.columns,
        derived=spec.projection.derived,
    )


def _run_inspect_subprocess(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
) -> dict[str, object]:
    env = os.environ.copy()
    env["CODEANATOMY_REPO_ROOT"] = repo_root
    env["CODEANATOMY_ALLOWLIST"] = "\n".join(str(m) for m in module_allowlist)

    result = subprocess.run(
        [sys.executable, "-c", _inspect_script()],
        capture_output=True,
        text=False,
        timeout=timeout_s,
        check=False,
        env=env,
        cwd=repo_root,
    )

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        msg = stderr or stdout or "runtime inspect failed"
        raise RuntimeError(msg)

    return _decode_runtime_payload(result.stdout)


def _decode_runtime_payload(payload: bytes) -> dict[str, object]:
    if not payload:
        raise RuntimeInspectPayloadTypeError
    reader = ipc.open_stream(pa.BufferReader(payload))
    table = reader.read_all()
    rows = table.to_pylist()
    if not rows or not isinstance(rows[0], dict):
        raise RuntimeInspectPayloadTypeError
    row = rows[0]
    version = row.get("version")
    if version is not None and int(version) != 1:
        msg = "Runtime inspect payload version mismatch."
        raise ValueError(msg)
    return row


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
        meta = {"object_key": key} if isinstance(key, str) else None
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
                "meta": meta,
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


def _build_rt_objects(
    obj_rows: list[Row],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> tuple[IbisPlan, IbisPlan]:
    raw_plan = ibis_plan_from_rows(
        "rt_objects_v1",
        obj_rows,
        row_schema=RT_OBJECTS_ROW_SCHEMA,
    )
    raw_table = raw_plan.expr
    with_rt_id = _with_derived_columns(raw_table, dataset="rt_objects_v1")
    rt_keys = with_rt_id.select(with_rt_id["object_key"], with_rt_id["rt_id"])
    rt_objects_key_plan = IbisPlan(expr=rt_keys, ordering=raw_plan.ordering)
    rt_objects_plan = apply_query_and_project(
        "rt_objects_v1",
        raw_table,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )
    return rt_objects_plan, rt_objects_key_plan


def _build_rt_signatures(
    sig_rows: list[Row],
    *,
    rt_objects_key_plan: IbisPlan,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> tuple[IbisPlan, IbisPlan | None]:
    raw_plan = ibis_plan_from_rows(
        "rt_signatures_v1",
        sig_rows,
        row_schema=RT_SIGNATURES_ROW_SCHEMA,
    )
    sig_table = raw_plan.expr
    if not sig_rows:
        empty_plan = apply_query_and_project(
            "rt_signatures_v1",
            sig_table,
            normalize=normalize,
            evidence_plan=evidence_plan,
            repo_id=normalize.repo_id,
        )
        return empty_plan, None
    obj_keys = rt_objects_key_plan.expr
    joined = sig_table.left_join(
        obj_keys,
        predicates=[sig_table["object_key"] == obj_keys["object_key"]],
    )
    joined_table = joined.select(
        [sig_table[col] for col in sig_table.columns] + [obj_keys["rt_id"]]
    )
    with_sig_id = _with_derived_columns(joined_table, dataset="rt_signatures_v1")
    sig_meta_table = with_sig_id.select(
        with_sig_id["object_key"],
        with_sig_id["signature"],
        with_sig_id["sig_id"],
    )
    sig_meta_plan = IbisPlan(expr=sig_meta_table, ordering=raw_plan.ordering)
    sig_plan = apply_query_and_project(
        "rt_signatures_v1",
        joined_table,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )
    return sig_plan, sig_meta_plan


def _build_rt_params(
    param_rows: list[Row],
    *,
    sig_meta_plan: IbisPlan | None,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "rt_signature_params_v1",
        param_rows,
        row_schema=RT_PARAMS_ROW_SCHEMA,
    )
    params_table = raw_plan.expr
    if not param_rows or sig_meta_plan is None:
        return apply_query_and_project(
            "rt_signature_params_v1",
            params_table,
            normalize=normalize,
            evidence_plan=evidence_plan,
            repo_id=normalize.repo_id,
        )
    sig_meta = sig_meta_plan.expr
    joined = params_table.left_join(
        sig_meta,
        predicates=[
            params_table["object_key"] == sig_meta["object_key"],
            params_table["signature"] == sig_meta["signature"],
        ],
    )
    selected = joined.select(
        [params_table[col] for col in params_table.columns] + [sig_meta["sig_id"]]
    )
    return apply_query_and_project(
        "rt_signature_params_v1",
        selected,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


def _build_rt_members(
    member_rows: list[Row],
    *,
    rt_objects_key_plan: IbisPlan,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> IbisPlan:
    raw_plan = ibis_plan_from_rows(
        "rt_members_v1",
        member_rows,
        row_schema=RT_MEMBERS_ROW_SCHEMA,
    )
    member_table = raw_plan.expr
    if not member_rows:
        return apply_query_and_project(
            "rt_members_v1",
            member_table,
            normalize=normalize,
            evidence_plan=evidence_plan,
            repo_id=normalize.repo_id,
        )
    obj_keys = rt_objects_key_plan.expr
    joined = member_table.left_join(
        obj_keys,
        predicates=[member_table["object_key"] == obj_keys["object_key"]],
    )
    selected = joined.select(
        [member_table[col] for col in member_table.columns] + [obj_keys["rt_id"]]
    )
    return apply_query_and_project(
        "rt_members_v1",
        selected,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


def _runtime_plans_from_rows(
    *,
    rows: RuntimeRows,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> dict[str, IbisPlan]:
    rt_objects_plan, rt_objects_key_plan = _build_rt_objects(
        rows.obj_rows,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    rt_signatures_plan, sig_meta_plan = _build_rt_signatures(
        rows.sig_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    rt_params_plan = _build_rt_params(
        rows.param_rows,
        sig_meta_plan=sig_meta_plan,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    rt_members_plan = _build_rt_members(
        rows.member_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        normalize=normalize,
        evidence_plan=evidence_plan,
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
    evidence_plan: EvidencePlan | None = None,
    ctx: ExecutionContext | None = None,
    profile: str = "default",
) -> RuntimeInspectResult:
    """Extract runtime inspection tables via subprocess.

    Parameters
    ----------
    repo_root:
        Repository root path for module imports.
    options:
        Runtime inspect options.
    evidence_plan:
        Evidence plan used for early column projection.
    ctx:
        Execution context for plan execution.
    profile:
        Execution profile name used when ``ctx`` is not provided.

    Returns
    -------
    RuntimeInspectResult
        Extracted runtime inspection tables.
    """
    normalized_options = normalize_options("runtime_inspect", options, RuntimeInspectOptions)
    ctx = ctx or execution_context_factory(profile)
    normalize = ExtractNormalizeOptions(options=normalized_options)
    if not normalized_options.module_allowlist:
        empty_rows = RuntimeRows(obj_rows=[], sig_rows=[], param_rows=[], member_rows=[])
        plans = _runtime_plans_from_rows(
            rows=empty_rows,
            normalize=normalize,
            evidence_plan=evidence_plan,
        )
        return RuntimeInspectResult(
            rt_objects=materialize_extract_plan(
                "rt_objects_v1",
                plans["rt_objects"],
                ctx=ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
            rt_signatures=materialize_extract_plan(
                "rt_signatures_v1",
                plans["rt_signatures"],
                ctx=ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
            rt_signature_params=materialize_extract_plan(
                "rt_signature_params_v1",
                plans["rt_signature_params"],
                ctx=ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
            rt_members=materialize_extract_plan(
                "rt_members_v1",
                plans["rt_members"],
                ctx=ctx,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    apply_post_kernels=True,
                ),
            ),
        )

    payload = _run_inspect_subprocess(
        repo_root,
        module_allowlist=normalized_options.module_allowlist,
        timeout_s=normalized_options.timeout_s,
    )

    obj_rows = _parse_runtime_objects(payload.get("objects"))
    sig_rows, param_rows = _parse_runtime_signatures(payload.get("signatures"))
    member_rows = _parse_runtime_members(payload.get("members"))
    rows = RuntimeRows(
        obj_rows=obj_rows,
        sig_rows=sig_rows,
        param_rows=param_rows,
        member_rows=member_rows,
    )
    plans = _runtime_plans_from_rows(
        rows=rows,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    return RuntimeInspectResult(
        rt_objects=materialize_extract_plan(
            "rt_objects_v1",
            plans["rt_objects"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        rt_signatures=materialize_extract_plan(
            "rt_signatures_v1",
            plans["rt_signatures"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        rt_signature_params=materialize_extract_plan(
            "rt_signature_params_v1",
            plans["rt_signature_params"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
        rt_members=materialize_extract_plan(
            "rt_members_v1",
            plans["rt_members"],
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                apply_post_kernels=True,
            ),
        ),
    )


def extract_runtime_plans(
    repo_root: str,
    *,
    options: RuntimeInspectOptions,
    evidence_plan: EvidencePlan | None = None,
) -> dict[str, IbisPlan]:
    """Extract runtime inspection plans via subprocess.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plan bundle keyed by runtime inspection table name.
    """
    normalized_options = normalize_options("runtime_inspect", options, RuntimeInspectOptions)
    normalize = ExtractNormalizeOptions(options=normalized_options)
    if not normalized_options.module_allowlist:
        rows = RuntimeRows(obj_rows=[], sig_rows=[], param_rows=[], member_rows=[])
        return _runtime_plans_from_rows(
            rows=rows,
            normalize=normalize,
            evidence_plan=evidence_plan,
        )

    payload = _run_inspect_subprocess(
        repo_root,
        module_allowlist=normalized_options.module_allowlist,
        timeout_s=normalized_options.timeout_s,
    )

    obj_rows = _parse_runtime_objects(payload.get("objects"))
    sig_rows, param_rows = _parse_runtime_signatures(payload.get("signatures"))
    member_rows = _parse_runtime_members(payload.get("members"))
    rows = RuntimeRows(
        obj_rows=obj_rows,
        sig_rows=sig_rows,
        param_rows=param_rows,
        member_rows=member_rows,
    )
    return _runtime_plans_from_rows(
        rows=rows,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )


def extract_runtime_objects(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
    prefer_reader: bool = False,
    profile: str = "default",
) -> TableLike | RecordBatchReaderLike:
    """Extract runtime objects via subprocess.

    prefer_reader:
        When True, return a streaming reader when possible.
    profile:
        Execution profile name used for the default execution context.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Runtime object output.
    """
    options = RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s)
    plans = extract_runtime_plans(
        repo_root,
        options=options,
    )
    exec_ctx = execution_context_factory(profile)
    normalize = ExtractNormalizeOptions(options=options)
    return materialize_extract_plan(
        "rt_objects_v1",
        plans["rt_objects"],
        ctx=exec_ctx,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=True,
        ),
    )


def extract_runtime_signatures(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
    prefer_reader: bool = False,
    profile: str = "default",
) -> dict[str, TableLike | RecordBatchReaderLike]:
    """Extract runtime signatures and parameters via subprocess.

    prefer_reader:
        When True, return streaming readers when possible.
    profile:
        Execution profile name used for the default execution context.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Signature bundle with ``rt_signatures`` and ``rt_signature_params``.
    """
    options = RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s)
    plans = extract_runtime_plans(
        repo_root,
        options=options,
    )
    exec_ctx = execution_context_factory(profile)
    normalize = ExtractNormalizeOptions(options=options)
    return {
        "rt_signatures": materialize_extract_plan(
            "rt_signatures_v1",
            plans["rt_signatures"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
        "rt_signature_params": materialize_extract_plan(
            "rt_signature_params_v1",
            plans["rt_signature_params"],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        ),
    }


def extract_runtime_members(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
    prefer_reader: bool = False,
    profile: str = "default",
) -> TableLike | RecordBatchReaderLike:
    """Extract runtime members via subprocess.

    prefer_reader:
        When True, return a streaming reader when possible.
    profile:
        Execution profile name used for the default execution context.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Runtime member output.
    """
    options = RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s)
    plans = extract_runtime_plans(
        repo_root,
        options=options,
    )
    exec_ctx = execution_context_factory(profile)
    normalize = ExtractNormalizeOptions(options=options)
    return materialize_extract_plan(
        "rt_members_v1",
        plans["rt_members"],
        ctx=exec_ctx,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=True,
        ),
    )
