"""Extract runtime inspection tables in a sandboxed subprocess using shared helpers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from arrowdsl.compute.expr_core import MaskedHashExprSpec
from arrowdsl.core.context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, SchemaLike, TableLike, pc
from arrowdsl.plan.joins import join_config_for_output, left_join
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.runner import materialize_plan, run_plan_bundle
from arrowdsl.plan.scan_io import record_batches_from_rows
from arrowdsl.schema.ops import unify_tables
from arrowdsl.schema.schema import SchemaMetadataSpec, empty_table
from extract.helpers import project_columns
from extract.plan_helpers import apply_query_and_normalize
from extract.registry_ids import hash_spec
from extract.registry_specs import (
    dataset_row_schema,
    dataset_schema,
    normalize_options,
)
from extract.schema_ops import metadata_spec_for_dataset

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan

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


@dataclass(frozen=True)
class RuntimeRows:
    """Row buffers for runtime inspection extraction."""

    obj_rows: list[Row]
    sig_rows: list[Row]
    param_rows: list[Row]
    member_rows: list[Row]


RT_OBJECT_ID_SPEC = hash_spec("rt_object_id")
RT_SIGNATURE_ID_SPEC = hash_spec("rt_signature_id")
RT_PARAM_ID_SPEC = hash_spec("rt_param_id")
RT_MEMBER_ID_SPEC = hash_spec("rt_member_id")

RT_OBJECTS_SCHEMA = dataset_schema("rt_objects_v1")
RT_SIGNATURES_SCHEMA = dataset_schema("rt_signatures_v1")
RT_SIGNATURE_PARAMS_SCHEMA = dataset_schema("rt_signature_params_v1")
RT_MEMBERS_SCHEMA = dataset_schema("rt_members_v1")

RT_OBJECTS_ROW_SCHEMA = dataset_row_schema("rt_objects_v1")
RT_SIGNATURES_ROW_SCHEMA = dataset_row_schema("rt_signatures_v1")
RT_PARAMS_ROW_SCHEMA = dataset_row_schema("rt_signature_params_v1")
RT_MEMBERS_ROW_SCHEMA = dataset_row_schema("rt_members_v1")


def _runtime_metadata_specs(
    options: RuntimeInspectOptions,
) -> dict[str, SchemaMetadataSpec]:
    return {
        "rt_objects": metadata_spec_for_dataset("rt_objects_v1", options=options),
        "rt_signatures": metadata_spec_for_dataset("rt_signatures_v1", options=options),
        "rt_signature_params": metadata_spec_for_dataset("rt_signature_params_v1", options=options),
        "rt_members": metadata_spec_for_dataset("rt_members_v1", options=options),
    }


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


def _empty_runtime_result(
    metadata_specs: Mapping[str, SchemaMetadataSpec] | None = None,
) -> RuntimeInspectResult:
    def _empty(schema: SchemaLike, key: str) -> TableLike:
        if metadata_specs is None:
            return empty_table(schema)
        return empty_table(metadata_specs[key].apply(schema))

    return RuntimeInspectResult(
        rt_objects=_empty(RT_OBJECTS_SCHEMA, "rt_objects"),
        rt_signatures=_empty(RT_SIGNATURES_SCHEMA, "rt_signatures"),
        rt_signature_params=_empty(RT_SIGNATURE_PARAMS_SCHEMA, "rt_signature_params"),
        rt_members=_empty(RT_MEMBERS_SCHEMA, "rt_members"),
    )


def _plan_from_row_fragments(
    rows: Sequence[Row],
    *,
    schema: SchemaLike,
    label: str,
    batch_size: int = 4096,
) -> Plan:
    if not rows:
        return Plan.table_source(empty_table(schema), label=label)
    tables = [
        pa.Table.from_batches([batch], schema=schema)
        for batch in record_batches_from_rows(rows, schema=schema, batch_size=batch_size)
    ]
    table = unify_tables(tables)
    return Plan.table_source(table, label=label)


def _build_rt_objects(
    obj_rows: list[Row],
    *,
    exec_ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> tuple[Plan, Plan]:
    rt_objects_plan = _plan_from_row_fragments(
        obj_rows,
        schema=RT_OBJECTS_ROW_SCHEMA,
        label="rt_objects_raw",
    )
    rt_objects_plan = project_columns(
        rt_objects_plan,
        base=RT_OBJECTS_ROW_SCHEMA.names,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=RT_OBJECT_ID_SPEC,
                    required=("module", "qualname"),
                ).to_expression(),
                "rt_id",
            )
        ],
        ctx=exec_ctx,
    )
    rt_objects_key_plan = rt_objects_plan.project(
        [pc.field("object_key"), pc.field("rt_id")],
        ["object_key", "rt_id"],
        ctx=exec_ctx,
    )
    rt_objects_plan = apply_query_and_normalize(
        "rt_objects_v1",
        rt_objects_plan,
        ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    return rt_objects_plan, rt_objects_key_plan


def _build_rt_signatures(
    sig_rows: list[Row],
    *,
    rt_objects_key_plan: Plan,
    exec_ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> tuple[Plan, Plan | None]:
    if not sig_rows:
        empty_plan = Plan.table_source(empty_table(RT_SIGNATURES_SCHEMA))
        return empty_plan, None
    sig_plan = _plan_from_row_fragments(
        sig_rows,
        schema=RT_SIGNATURES_ROW_SCHEMA,
        label="rt_signatures_raw",
    )
    sig_cols = list(RT_SIGNATURES_ROW_SCHEMA.names)
    join_config = join_config_for_output(
        left_columns=RT_SIGNATURES_ROW_SCHEMA.names,
        right_columns=rt_objects_key_plan.schema(ctx=exec_ctx).names,
        key_pairs=(("object_key", "object_key"),),
        right_output=("rt_id",),
    )
    if join_config is not None:
        sig_plan = left_join(
            sig_plan,
            rt_objects_key_plan,
            config=join_config,
            use_threads=exec_ctx.use_threads,
            ctx=exec_ctx,
        )
        sig_cols = list(join_config.left_output) + list(join_config.right_output)
    sig_plan = project_columns(
        sig_plan,
        base=sig_cols,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=RT_SIGNATURE_ID_SPEC,
                    required=("rt_id", "signature"),
                ).to_expression(),
                "sig_id",
            )
        ],
        ctx=exec_ctx,
    )
    sig_meta_plan = None
    if {"object_key", "signature"} <= set(sig_cols):
        sig_meta_plan = sig_plan.project(
            [pc.field("object_key"), pc.field("signature"), pc.field("sig_id")],
            ["object_key", "signature", "sig_id"],
            ctx=exec_ctx,
        )
    sig_plan = apply_query_and_normalize(
        "rt_signatures_v1",
        sig_plan,
        ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    return sig_plan, sig_meta_plan


def _build_rt_params(
    param_rows: list[Row],
    *,
    sig_meta_plan: Plan | None,
    exec_ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> Plan:
    if not param_rows or sig_meta_plan is None:
        return Plan.table_source(empty_table(RT_SIGNATURE_PARAMS_SCHEMA))
    param_plan = _plan_from_row_fragments(
        param_rows,
        schema=RT_PARAMS_ROW_SCHEMA,
        label="rt_params_raw",
    )
    param_cols = list(RT_PARAMS_ROW_SCHEMA.names)
    join_config = join_config_for_output(
        left_columns=RT_PARAMS_ROW_SCHEMA.names,
        right_columns=sig_meta_plan.schema(ctx=exec_ctx).names,
        key_pairs=(("object_key", "object_key"), ("signature", "signature")),
        right_output=("sig_id",),
    )
    if join_config is not None:
        param_plan = left_join(
            param_plan,
            sig_meta_plan,
            config=join_config,
            use_threads=exec_ctx.use_threads,
            ctx=exec_ctx,
        )
        param_cols = list(join_config.left_output) + list(join_config.right_output)
    param_plan = project_columns(
        param_plan,
        base=param_cols,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=RT_PARAM_ID_SPEC,
                    required=("sig_id", "name"),
                ).to_expression(),
                "param_id",
            )
        ],
        ctx=exec_ctx,
    )
    return apply_query_and_normalize(
        "rt_signature_params_v1",
        param_plan,
        ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )


def _build_rt_members(
    member_rows: list[Row],
    *,
    rt_objects_key_plan: Plan,
    exec_ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> Plan:
    if not member_rows:
        return Plan.table_source(empty_table(RT_MEMBERS_SCHEMA))
    member_plan = _plan_from_row_fragments(
        member_rows,
        schema=RT_MEMBERS_ROW_SCHEMA,
        label="rt_members_raw",
    )
    member_cols = list(RT_MEMBERS_ROW_SCHEMA.names)
    join_config = join_config_for_output(
        left_columns=RT_MEMBERS_ROW_SCHEMA.names,
        right_columns=rt_objects_key_plan.schema(ctx=exec_ctx).names,
        key_pairs=(("object_key", "object_key"),),
        right_output=("rt_id",),
    )
    if join_config is not None:
        member_plan = left_join(
            member_plan,
            rt_objects_key_plan,
            config=join_config,
            use_threads=exec_ctx.use_threads,
            ctx=exec_ctx,
        )
        member_cols = list(join_config.left_output) + list(join_config.right_output)
    member_plan = project_columns(
        member_plan,
        base=member_cols,
        extras=[
            (
                MaskedHashExprSpec(
                    spec=RT_MEMBER_ID_SPEC,
                    required=("rt_id", "name"),
                ).to_expression(),
                "member_id",
            )
        ],
        ctx=exec_ctx,
    )
    return apply_query_and_normalize(
        "rt_members_v1",
        member_plan,
        ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )


def _runtime_tables_from_rows(
    *,
    rows: RuntimeRows,
    exec_ctx: ExecutionContext,
    metadata_specs: Mapping[str, SchemaMetadataSpec],
    evidence_plan: EvidencePlan | None = None,
) -> RuntimeInspectResult:
    if not rows.obj_rows:
        return _empty_runtime_result(metadata_specs)

    rt_objects_plan, rt_objects_key_plan = _build_rt_objects(
        rows.obj_rows,
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    rt_signatures_plan, sig_meta_plan = _build_rt_signatures(
        rows.sig_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    rt_params_plan = _build_rt_params(
        rows.param_rows,
        sig_meta_plan=sig_meta_plan,
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    rt_members_plan = _build_rt_members(
        rows.member_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )

    return RuntimeInspectResult(
        rt_objects=materialize_plan(
            rt_objects_plan,
            ctx=exec_ctx,
            metadata_spec=metadata_specs["rt_objects"],
            attach_ordering_metadata=True,
        ),
        rt_signatures=materialize_plan(
            rt_signatures_plan,
            ctx=exec_ctx,
            metadata_spec=metadata_specs["rt_signatures"],
            attach_ordering_metadata=True,
        ),
        rt_signature_params=materialize_plan(
            rt_params_plan,
            ctx=exec_ctx,
            metadata_spec=metadata_specs["rt_signature_params"],
            attach_ordering_metadata=True,
        ),
        rt_members=materialize_plan(
            rt_members_plan,
            ctx=exec_ctx,
            metadata_spec=metadata_specs["rt_members"],
            attach_ordering_metadata=True,
        ),
    )


def _runtime_plans_from_rows(
    *,
    rows: RuntimeRows,
    exec_ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> dict[str, Plan]:
    if not rows.obj_rows:
        empty = Plan.table_source(empty_table(RT_OBJECTS_SCHEMA))
        return {
            "rt_objects": empty,
            "rt_signatures": Plan.table_source(empty_table(RT_SIGNATURES_SCHEMA)),
            "rt_signature_params": Plan.table_source(empty_table(RT_SIGNATURE_PARAMS_SCHEMA)),
            "rt_members": Plan.table_source(empty_table(RT_MEMBERS_SCHEMA)),
        }
    rt_objects_plan, rt_objects_key_plan = _build_rt_objects(
        rows.obj_rows,
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    rt_signatures_plan, sig_meta_plan = _build_rt_signatures(
        rows.sig_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    rt_params_plan = _build_rt_params(
        rows.param_rows,
        sig_meta_plan=sig_meta_plan,
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    rt_members_plan = _build_rt_members(
        rows.member_rows,
        rt_objects_key_plan=rt_objects_key_plan,
        exec_ctx=exec_ctx,
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

    Returns
    -------
    RuntimeInspectResult
        Extracted runtime inspection tables.
    """
    normalized_options = normalize_options("runtime_inspect", options, RuntimeInspectOptions)
    exec_ctx = ctx or execution_context_factory("default")
    if not normalized_options.module_allowlist:
        return _empty_runtime_result(_runtime_metadata_specs(normalized_options))
    metadata_specs = _runtime_metadata_specs(normalized_options)

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
    return _runtime_tables_from_rows(
        rows=rows,
        exec_ctx=exec_ctx,
        metadata_specs=metadata_specs,
        evidence_plan=evidence_plan,
    )


def extract_runtime_plans(
    repo_root: str,
    *,
    options: RuntimeInspectOptions,
    evidence_plan: EvidencePlan | None = None,
    ctx: ExecutionContext | None = None,
) -> dict[str, Plan]:
    """Extract runtime inspection plans via subprocess.

    Returns
    -------
    dict[str, Plan]
        Plan bundle keyed by runtime inspection table name.
    """
    normalized_options = normalize_options("runtime_inspect", options, RuntimeInspectOptions)
    exec_ctx = ctx or execution_context_factory("default")
    if not normalized_options.module_allowlist:
        return {
            "rt_objects": Plan.table_source(empty_table(RT_OBJECTS_SCHEMA)),
            "rt_signatures": Plan.table_source(empty_table(RT_SIGNATURES_SCHEMA)),
            "rt_signature_params": Plan.table_source(empty_table(RT_SIGNATURE_PARAMS_SCHEMA)),
            "rt_members": Plan.table_source(empty_table(RT_MEMBERS_SCHEMA)),
        }

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
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
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
    options = RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s)
    plans = extract_runtime_plans(
        repo_root,
        options=options,
    )
    metadata_specs = _runtime_metadata_specs(options)
    return run_plan_bundle(
        {"rt_objects": plans["rt_objects"]},
        ctx=execution_context_factory("default"),
        prefer_reader=prefer_reader,
        metadata_specs={"rt_objects": metadata_specs["rt_objects"]},
        attach_ordering_metadata=True,
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
    options = RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s)
    plans = extract_runtime_plans(
        repo_root,
        options=options,
    )
    metadata_specs = _runtime_metadata_specs(options)
    return run_plan_bundle(
        {
            "rt_signatures": plans["rt_signatures"],
            "rt_signature_params": plans["rt_signature_params"],
        },
        ctx=execution_context_factory("default"),
        prefer_reader=prefer_reader,
        metadata_specs={
            "rt_signatures": metadata_specs["rt_signatures"],
            "rt_signature_params": metadata_specs["rt_signature_params"],
        },
        attach_ordering_metadata=True,
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
    options = RuntimeInspectOptions(module_allowlist=module_allowlist, timeout_s=timeout_s)
    plans = extract_runtime_plans(
        repo_root,
        options=options,
    )
    metadata_specs = _runtime_metadata_specs(options)
    return run_plan_bundle(
        {"rt_members": plans["rt_members"]},
        ctx=execution_context_factory("default"),
        prefer_reader=prefer_reader,
        metadata_specs={"rt_members": metadata_specs["rt_members"]},
        attach_ordering_metadata=True,
    )["rt_members"]
