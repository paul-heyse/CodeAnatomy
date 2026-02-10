"""Extract Python bytecode artifacts into Arrow tables using shared helpers."""

from __future__ import annotations

import contextlib
import dis
import functools
import importlib.util
import inspect
import json
import sys
import types as pytypes
from collections.abc import Iterable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

from core_types import RowRich as Row
from core_types import RowValueRich as RowValue
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from extract.coordination.context import (
    ExtractExecutionContext,
    FileContext,
    SpanSpec,
    attrs_map,
    bytes_from_file_ctx,
    file_identity_row,
    span_dict,
    text_from_file_ctx,
)
from extract.coordination.line_offsets import LineOffsets
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.infrastructure.cache_utils import (
    CacheSetOptions,
    cache_for_extract,
    cache_get,
    cache_lock,
    cache_set,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    stable_cache_key,
)
from extract.infrastructure.options import RepoOptions, WorkerOptions, WorklistQueueOptions
from extract.infrastructure.parallel import resolve_max_workers
from extract.infrastructure.result_types import ExtractResult
from extract.infrastructure.schema_cache import bytecode_files_fingerprint
from extract.infrastructure.worklists import (
    WorklistRequest,
    iter_worklist_contexts,
    worklist_queue_name,
)
from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache

    from extract.coordination.evidence_plan import EvidencePlan
    from extract.scanning.scope_manifest import ScopeManifest
    from extract.session import ExtractSession

type BytecodeCacheKey = tuple[str, str, str, int, bool, bool, bool, tuple[str, ...]]

BC_LINE_BASE = 1
CACHE_ENTRY_FIELDS = 3
PYTHON_VERSION = sys.version.split()[0]
PYTHON_MAGIC = importlib.util.MAGIC_NUMBER.hex()


@dataclass(frozen=True)
class BytecodeExtractOptions(RepoOptions, WorklistQueueOptions, WorkerOptions):
    """Bytecode extraction options.

    - adaptive=False is the canonical stream for stable semantics across runs
      (per your recipe). :contentReference[oaicite:17]{index=17}
    - include_cfg_derivations implements the blocks/CFG edges derivation recipe.
      :contentReference[oaicite:18]{index=18}
    """

    optimize: int = 0
    dont_inherit: bool = True
    adaptive: bool = False
    include_cfg_derivations: bool = True
    parallel_min_files: int = 8
    parallel_max_bytes: int = 50_000_000
    terminator_opnames: Sequence[str] = (
        "RETURN_VALUE",
        "RETURN_CONST",
        "RAISE_VARARGS",
        "RERAISE",
    )


@dataclass(frozen=True)
class BytecodeFileContext:
    """Per-file context for bytecode extraction."""

    file_ctx: FileContext
    options: BytecodeExtractOptions

    @property
    def file_id(self) -> str:
        """Return the file id for this extraction context.

        Returns:
        -------
        str
            File id from the file context.
        """
        return self.file_ctx.file_id

    @property
    def path(self) -> str:
        """Return the file path for this extraction context.

        Returns:
        -------
        str
            File path from the file context.
        """
        return self.file_ctx.path

    @property
    def file_sha256(self) -> str | None:
        """Return the file sha256 for this extraction context.

        Returns:
        -------
        str | None
            File hash from the file context.
        """
        return self.file_ctx.file_sha256

    def identity_row(self) -> Row:
        """Return the identity row for this context.

        Returns:
        -------
        dict[str, object]
            File identity columns for row construction.
        """
        return cast("Row", file_identity_row(self.file_ctx))


@dataclass(frozen=True)
class BytecodeCacheResult:
    """Cached bytecode extraction payload for a single file."""

    code_objects: list[dict[str, object]]
    errors: list[dict[str, object]]


@dataclass(frozen=True)
class InstructionData:
    """Instruction data for a compiled code object."""

    instructions: list[dis.Instruction]
    index_by_offset: dict[int, int]


@dataclass(frozen=True)
class CodeUnitKey:
    """Key fields identifying a code unit."""

    qualpath: str
    co_name: str
    firstlineno: int


@dataclass(frozen=True)
class CodeUnitContext:
    """Per-code-unit extraction context."""

    code_unit_key: CodeUnitKey
    file_ctx: BytecodeFileContext
    code: pytypes.CodeType
    instruction_data: InstructionData
    exc_entries: Sequence[object]
    code_len: int


@dataclass(frozen=True)
class CfgEdgeSpec:
    """CFG edge descriptor."""

    src_block_start: int
    src_block_end: int
    dst_block_start: int
    dst_block_end: int
    kind: str
    edge_key: str
    cond_instr_index: int | None
    cond_instr_offset: int | None
    exc_index: int | None
    jump_label: int | None
    jump_kind: str | None


@dataclass(frozen=True)
class _CfgEdgeContext:
    """Context data for CFG edge derivation."""

    unit_ctx: CodeUnitContext
    instructions: Sequence[dis.Instruction]
    index_by_offset: Mapping[int, int]
    terminators: Sequence[str]
    off_to_block: Mapping[int, tuple[int, int]]
    edge_rows: list[Row]


@dataclass(frozen=True)
class _BlockEdgeContext:
    """Per-block inputs for CFG edge derivation."""

    src_start: int
    src_end: int
    end: int
    last: dis.Instruction
    last_index: int


@dataclass
class BytecodeRowBuffers:
    """Mutable row buffers for bytecode extraction."""

    code_unit_rows: list[Row]
    instruction_rows: list[Row]
    exception_rows: list[Row]
    block_rows: list[Row]
    edge_rows: list[Row]
    line_rows: list[Row]
    dfg_edge_rows: list[Row]
    error_rows: list[Row]


@dataclass(frozen=True)
class DfgValueRef:
    """Stack value reference for DFG scaffolding."""

    instr_index: int
    instr_offset: int


@dataclass(frozen=True)
class DfgState:
    """DFG scaffolding state for one code unit."""

    unit_ctx: CodeUnitContext
    stack: list[DfgValueRef]
    dfg_rows: list[Row]
    error_rows: list[Row]
    code_id: str | None


@dataclass(frozen=True)
class CodeUnitGroups:
    """Grouped buffer rows keyed by code-unit identity."""

    instructions_by_key: dict[tuple[str, str, int], list[Row]]
    exceptions_by_key: dict[tuple[str, str, int], list[Row]]
    blocks_by_key: dict[tuple[str, str, int], list[Row]]
    edges_by_key: dict[tuple[str, str, int], list[Row]]
    line_table_by_key: dict[tuple[str, str, int], list[Row]]
    dfg_edges_by_key: dict[tuple[str, str, int], list[Row]]


def _consts_json(values: Sequence[object]) -> str | None:
    try:
        return json.dumps(values, default=str)
    except (TypeError, ValueError):
        return None


def _code_unit_key(row: Mapping[str, RowValue]) -> tuple[str, str, int]:
    qualpath = row.get("qualpath")
    co_name = row.get("co_name")
    firstlineno = row.get("firstlineno")
    qualpath_str = str(qualpath) if qualpath is not None else ""
    co_name_str = str(co_name) if co_name is not None else ""
    firstlineno_int = int(firstlineno) if isinstance(firstlineno, int) else 0
    return qualpath_str, co_name_str, firstlineno_int


def _instruction_entry(
    row: Mapping[str, RowValue],
    *,
    line_offsets: LineOffsets | None,
) -> dict[str, object]:
    start_line = row.get("pos_start_line")
    end_line = row.get("pos_end_line")
    start_col = row.get("pos_start_col")
    end_col = row.get("pos_end_col")
    start_line0 = int(start_line) - BC_LINE_BASE if isinstance(start_line, int) else None
    end_line0 = int(end_line) - BC_LINE_BASE if isinstance(end_line, int) else None
    byte_start = None
    byte_len = None
    if line_offsets is not None:
        start_offset = line_offsets.byte_offset(
            start_line0,
            int(start_col) if isinstance(start_col, int) else None,
        )
        end_offset = line_offsets.byte_offset(
            end_line0,
            int(end_col) if isinstance(end_col, int) else None,
        )
        if start_offset is not None and end_offset is not None:
            byte_start = start_offset
            byte_len = max(0, end_offset - start_offset)
    return {
        "instr_index": row.get("instr_index"),
        "offset": row.get("offset"),
        "start_offset": row.get("start_offset"),
        "end_offset": row.get("end_offset"),
        "opname": row.get("opname"),
        "baseopname": row.get("baseopname"),
        "opcode": row.get("opcode"),
        "baseopcode": row.get("baseopcode"),
        "arg": row.get("arg"),
        "oparg": row.get("oparg"),
        "argval_kind": row.get("argval_kind"),
        "argval_int": row.get("argval_int"),
        "argval_str": row.get("argval_str"),
        "argrepr": row.get("argrepr"),
        "line_number": row.get("line_number"),
        "starts_line": row.get("starts_line"),
        "label": row.get("label"),
        "is_jump_target": row.get("is_jump_target"),
        "jump_target": row.get("jump_target"),
        "cache_info": row.get("cache_info") or [],
        "span": span_dict(
            SpanSpec(
                start_line0=start_line0,
                start_col=int(start_col) if isinstance(start_col, int) else None,
                end_line0=end_line0,
                end_col=int(end_col) if isinstance(end_col, int) else None,
                end_exclusive=None,
                col_unit="byte",
                byte_start=byte_start,
                byte_len=byte_len,
            )
        ),
        "attrs": attrs_map(
            {
                "stack_depth_before": row.get("stack_depth_before"),
                "stack_depth_after": row.get("stack_depth_after"),
            }
        ),
    }


def _string_list(value: object | None) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return []


def _const_repr(value: object) -> str:
    return repr(value)


def _const_entries(values: Sequence[object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, value in enumerate(values):
        rows.append(
            {
                "const_index": int(index),
                "const_repr": _const_repr(value),
            }
        )
    return rows


def _const_entry_list(value: object | None) -> list[dict[str, object]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _cache_info_rows(cache_info: object | None) -> list[dict[str, object]]:
    if not isinstance(cache_info, (list, tuple)):
        return []
    rows: list[dict[str, object]] = []
    for entry in cache_info:
        if not isinstance(entry, tuple) or len(entry) < CACHE_ENTRY_FIELDS:
            continue
        name, size, data = entry[0], entry[1], entry[2]
        data_hex: str | None
        if isinstance(data, (bytes, bytearray, memoryview)):
            data_hex = bytes(data).hex()
        elif data is None:
            data_hex = None
        else:
            data_hex = str(data)
        rows.append(
            {
                "name": str(name),
                "size": int(size) if isinstance(size, int) else None,
                "data_hex": data_hex,
            }
        )
    return rows


def _argval_kind(ins: dis.Instruction) -> str | None:
    kind = None
    if ins.opcode in dis.hasconst:
        kind = "const"
    elif ins.opcode in dis.hasname:
        kind = "name"
    elif ins.opcode in dis.haslocal:
        kind = "local"
    elif ins.opcode in dis.hasfree:
        kind = "free"
    elif ins.opcode in dis.hascompare:
        kind = "compare"
    elif ins.opcode in dis.hasjump:
        kind = "jump"
    return kind


def _argval_fields(ins: dis.Instruction) -> tuple[str | None, int | None, str | None]:
    argval = ins.argval
    argval_kind = _argval_kind(ins)
    argval_int: int | None = None
    argval_str: str | None = None
    if isinstance(argval, bool):
        argval_int = int(argval)
    elif isinstance(argval, int):
        argval_int = argval
    elif isinstance(argval, str):
        argval_str = argval
    elif argval is not None:
        argval_str = str(argval)
    return argval_kind, argval_int, argval_str


def _co_flag_enabled(flags: int, name: str) -> bool:
    value = getattr(inspect, name, 0)
    return bool(flags & value)


def _flags_detail(flags: int) -> dict[str, bool]:
    return {
        "is_optimized": _co_flag_enabled(flags, "CO_OPTIMIZED"),
        "is_newlocals": _co_flag_enabled(flags, "CO_NEWLOCALS"),
        "has_varargs": _co_flag_enabled(flags, "CO_VARARGS"),
        "has_varkeywords": _co_flag_enabled(flags, "CO_VARKEYWORDS"),
        "is_nested": _co_flag_enabled(flags, "CO_NESTED"),
        "is_generator": _co_flag_enabled(flags, "CO_GENERATOR"),
        "is_nofree": _co_flag_enabled(flags, "CO_NOFREE"),
        "is_coroutine": _co_flag_enabled(flags, "CO_COROUTINE"),
        "is_iterable_coroutine": _co_flag_enabled(flags, "CO_ITERABLE_COROUTINE"),
        "is_async_generator": _co_flag_enabled(flags, "CO_ASYNC_GENERATOR"),
    }


def _exception_entry(row: Mapping[str, RowValue]) -> dict[str, object]:
    return {
        "exc_index": row.get("exc_index"),
        "start_offset": row.get("start_offset"),
        "end_offset": row.get("end_offset"),
        "target_offset": row.get("target_offset"),
        "depth": row.get("depth"),
        "lasti": row.get("lasti"),
        "attrs": attrs_map({}),
    }


def _block_entry(row: Mapping[str, RowValue]) -> dict[str, object]:
    return {
        "start_offset": row.get("start_offset"),
        "end_offset": row.get("end_offset"),
        "kind": row.get("kind"),
        "attrs": attrs_map({}),
    }


def _cfg_edge_entry(row: Mapping[str, RowValue]) -> dict[str, object]:
    return {
        "src_block_start": row.get("src_block_start"),
        "src_block_end": row.get("src_block_end"),
        "dst_block_start": row.get("dst_block_start"),
        "dst_block_end": row.get("dst_block_end"),
        "kind": row.get("kind"),
        "edge_key": row.get("edge_key"),
        "cond_instr_index": row.get("cond_instr_index"),
        "cond_instr_offset": row.get("cond_instr_offset"),
        "exc_index": row.get("exc_index"),
        "attrs": attrs_map(
            {
                "jump_label": row.get("jump_label"),
                "jump_kind": row.get("jump_kind"),
            }
        ),
    }


def _line_entry(row: Mapping[str, RowValue]) -> dict[str, object]:
    return {
        "offset": row.get("offset"),
        "line1": row.get("line1"),
        "line0": row.get("line0"),
        "attrs": attrs_map({}),
    }


def _dfg_edge_entry(row: Mapping[str, RowValue]) -> dict[str, object]:
    return {
        "src_instr_index": row.get("src_instr_index"),
        "dst_instr_index": row.get("dst_instr_index"),
        "kind": row.get("kind"),
        "attrs": attrs_map({}),
    }


def _error_entry(row: Mapping[str, RowValue]) -> dict[str, object]:
    return {
        "error_type": row.get("error_type"),
        "message": row.get("message"),
        "attrs": attrs_map(
            {
                "error_stage": row.get("error_stage"),
                "code_id": row.get("code_id"),
            }
        ),
    }


def _append_error_row(
    ctx: BytecodeFileContext,
    error_rows: list[Row],
    *,
    exc: Exception,
    stage: str,
    code_id: str | None = None,
) -> None:
    error_rows.append(
        {
            **ctx.identity_row(),
            "error_type": type(exc).__name__,
            "message": str(exc),
            "error_stage": stage,
            "code_id": code_id,
        }
    )


def _bytecode_cache_key(
    file_ctx: FileContext,
    *,
    options: BytecodeExtractOptions,
) -> BytecodeCacheKey | None:
    if not file_ctx.file_id or file_ctx.file_sha256 is None:
        return None
    terminators = tuple(str(name) for name in options.terminator_opnames)
    return (
        file_ctx.file_id,
        file_ctx.file_sha256,
        bytecode_files_fingerprint(),
        int(options.optimize),
        bool(options.dont_inherit),
        bool(options.adaptive),
        bool(options.include_cfg_derivations),
        terminators,
    )


def _effective_max_workers(
    options: BytecodeExtractOptions,
) -> int:
    max_workers = resolve_max_workers(options.max_workers, kind="cpu")
    return max(1, min(32, max_workers))


def _estimate_context_bytes(file_ctx: FileContext) -> int | None:
    if file_ctx.data is not None:
        return len(file_ctx.data)
    if file_ctx.text is not None:
        # Approximate bytes using decoded text length to avoid extra I/O.
        return len(file_ctx.text)
    if file_ctx.abs_path:
        try:
            return int(Path(file_ctx.abs_path).stat().st_size)
        except OSError:
            return None
    return None


def _line_offsets(file_ctx: FileContext) -> LineOffsets | None:
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    return LineOffsets.from_bytes(data)


def _should_parallelize(
    contexts: Sequence[FileContext],
    *,
    options: BytecodeExtractOptions,
    max_workers: int,
) -> bool:
    if max_workers <= 1:
        return False
    min_files = max(1, options.parallel_min_files)
    if len(contexts) < min_files:
        return False
    if options.parallel_max_bytes <= 0:
        return False
    total_bytes = 0
    for file_ctx in contexts:
        size = _estimate_context_bytes(file_ctx)
        if size is None:
            return False
        total_bytes += size
        if total_bytes > options.parallel_max_bytes:
            return False
    return True


def _bytecode_row_payload(
    file_ctx: FileContext,
    *,
    options: BytecodeExtractOptions,
    code_objects: list[dict[str, object]],
    errors: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "file_sha256": file_ctx.file_sha256,
        "code_objects": code_objects,
        "errors": errors,
        "attrs": attrs_map(
            {
                "python_version": PYTHON_VERSION,
                "python_magic": PYTHON_MAGIC,
                "optimize": options.optimize,
                "dont_inherit": options.dont_inherit,
                "adaptive": options.adaptive,
                "include_cfg": options.include_cfg_derivations,
            }
        ),
    }


def _context_from_file_ctx(
    file_ctx: FileContext,
    options: BytecodeExtractOptions,
) -> BytecodeFileContext | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    return BytecodeFileContext(file_ctx=file_ctx, options=options)


def _row_context(
    rf: dict[str, object], options: BytecodeExtractOptions
) -> BytecodeFileContext | None:
    file_ctx = FileContext.from_repo_row(rf)
    return _context_from_file_ctx(file_ctx, options)


def _compile_text(
    text: str, ctx: BytecodeFileContext
) -> tuple[pytypes.CodeType | None, Row | None]:
    try:
        top = compile(
            text,
            ctx.path,
            "exec",
            dont_inherit=ctx.options.dont_inherit,
            optimize=ctx.options.optimize,
        )
    except (OverflowError, SyntaxError, ValueError) as exc:
        return (
            None,
            {
                **ctx.identity_row(),
                "error_type": type(exc).__name__,
                "message": str(exc),
                "error_stage": "compile",
            },
        )
    return top, None


def _qual_for(co: pytypes.CodeType, parent_qual: str | None) -> str:
    if parent_qual is None:
        return "<module>"
    return f"{parent_qual}.{co.co_name}@{co.co_firstlineno}"


def _assign_code_units(
    top: pytypes.CodeType,
    ctx: BytecodeFileContext,
    cu_rows: list[Row],
) -> dict[int, CodeUnitKey]:
    co_to_key: dict[int, CodeUnitKey] = {}
    co_to_qual: dict[int, str] = {}

    for co, parent in _iter_code_objects(top):
        parent_key = co_to_key.get(id(parent)) if parent is not None else None
        parent_qual = co_to_qual.get(id(parent)) if parent is not None else None
        qual = _qual_for(co, parent_qual)
        key = CodeUnitKey(
            qualpath=qual,
            co_name=co.co_name,
            firstlineno=int(co.co_firstlineno or 0),
        )
        co_to_key[id(co)] = key
        co_to_qual[id(co)] = qual

        cu_rows.append(
            {
                **ctx.identity_row(),
                "qualpath": qual,
                "co_qualname": getattr(co, "co_qualname", None),
                "co_filename": getattr(co, "co_filename", None),
                "co_name": co.co_name,
                "firstlineno": int(co.co_firstlineno or 0),
                "parent_qualpath": parent_key.qualpath if parent_key is not None else None,
                "parent_co_name": parent_key.co_name if parent_key is not None else None,
                "parent_firstlineno": parent_key.firstlineno if parent_key is not None else None,
                "argcount": int(getattr(co, "co_argcount", 0)),
                "posonlyargcount": int(getattr(co, "co_posonlyargcount", 0)),
                "kwonlyargcount": int(getattr(co, "co_kwonlyargcount", 0)),
                "nlocals": int(getattr(co, "co_nlocals", 0)),
                "flags": int(getattr(co, "co_flags", 0)),
                "stacksize": int(getattr(co, "co_stacksize", 0)),
                "code_len": len(co.co_code),
                "varnames": list(getattr(co, "co_varnames", ())),
                "freevars": list(getattr(co, "co_freevars", ())),
                "cellvars": list(getattr(co, "co_cellvars", ())),
                "names": list(getattr(co, "co_names", ())),
                "consts": _const_entries(getattr(co, "co_consts", ())),
                "consts_json": _consts_json(getattr(co, "co_consts", ())),
            }
        )

    return co_to_key


def _instruction_data(co: pytypes.CodeType, options: BytecodeExtractOptions) -> InstructionData:
    instructions = list(dis.get_instructions(co, adaptive=options.adaptive))
    index_by_offset = {int(ins.offset): idx for idx, ins in enumerate(instructions)}
    return InstructionData(instructions=instructions, index_by_offset=index_by_offset)


def _exception_entries(co: pytypes.CodeType) -> Sequence[object]:
    bc = dis.Bytecode(co)
    return list(getattr(bc, "exception_entries", ()))


def _code_unit_key_columns(unit_ctx: CodeUnitContext) -> dict[str, RowValue]:
    key = unit_ctx.code_unit_key
    return {
        "qualpath": key.qualpath,
        "co_name": key.co_name,
        "firstlineno": key.firstlineno,
    }


def _append_exception_rows(unit_ctx: CodeUnitContext, exc_rows: list[Row]) -> None:
    identity = unit_ctx.file_ctx.identity_row()
    for k, ex in enumerate(unit_ctx.exc_entries):
        exc_rows.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "exc_index": int(k),
                "start_offset": int(getattr(ex, "start", 0)),
                "end_offset": int(getattr(ex, "end", 0)),
                "target_offset": int(getattr(ex, "target", 0)),
                "depth": int(getattr(ex, "depth", 0)),
                "lasti": bool(getattr(ex, "lasti", False)),
            }
        )


def _position_fields(ins: dis.Instruction) -> dict[str, RowValue]:
    pos = getattr(ins, "positions", None)
    pos_start_line = getattr(pos, "lineno", None) if pos else None
    pos_end_line = getattr(pos, "end_lineno", None) if pos else None
    pos_start_col = getattr(pos, "col_offset", None) if pos else None
    pos_end_col = getattr(pos, "end_col_offset", None) if pos else None
    return {
        "pos_start_line": int(pos_start_line) if pos_start_line is not None else None,
        "pos_end_line": int(pos_end_line) if pos_end_line is not None else None,
        "pos_start_col": int(pos_start_col) if pos_start_col is not None else None,
        "pos_end_col": int(pos_end_col) if pos_end_col is not None else None,
    }


def _instruction_core_fields(ins: dis.Instruction) -> dict[str, RowValue]:
    baseopname = getattr(ins, "baseopname", ins.opname)
    baseopcode = getattr(ins, "baseopcode", ins.opcode)
    oparg = getattr(ins, "oparg", ins.arg)
    line_number = getattr(ins, "line_number", None)
    label = getattr(ins, "label", None)
    start_offset = getattr(ins, "start_offset", ins.offset)
    end_offset = getattr(ins, "end_offset", ins.offset)
    cache_info = _cache_info_rows(getattr(ins, "cache_info", None))
    argval_kind, argval_int, argval_str = _argval_fields(ins)
    return {
        "start_offset": int(start_offset) if isinstance(start_offset, int) else None,
        "end_offset": int(end_offset) if isinstance(end_offset, int) else None,
        "baseopname": baseopname,
        "baseopcode": int(baseopcode) if isinstance(baseopcode, int) else None,
        "oparg": int(oparg) if isinstance(oparg, int) else None,
        "argval_kind": argval_kind,
        "argval_int": argval_int,
        "argval_str": argval_str,
        "line_number": int(line_number) if line_number is not None else None,
        "label": int(label) if isinstance(label, int) else None,
        "cache_info": cache_info,
    }


def _stack_effect_delta(ins: dis.Instruction, state: DfgState) -> int:
    arg = int(ins.arg) if ins.arg is not None else 0
    try:
        return dis.stack_effect(ins.opcode, arg, jump=None)
    except ValueError as exc:
        _append_error_row(
            state.unit_ctx.file_ctx,
            state.error_rows,
            exc=exc,
            stage="stack_effect",
            code_id=state.code_id,
        )
    return 0


def _apply_stack_effect(
    *,
    state: DfgState,
    idx: int,
    ins: dis.Instruction,
) -> tuple[int, int]:
    identity = state.unit_ctx.file_ctx.identity_row()
    stack_depth_before = len(state.stack)
    delta = _stack_effect_delta(ins, state)
    stack_depth_after = stack_depth_before + delta
    if stack_depth_after < 0:
        _append_error_row(
            state.unit_ctx.file_ctx,
            state.error_rows,
            exc=ValueError("Stack depth underflow."),
            stage="stack_effect",
            code_id=state.code_id,
        )
        stack_depth_after = 0

    pop_count = max(0, -delta)
    if pop_count > len(state.stack):
        _append_error_row(
            state.unit_ctx.file_ctx,
            state.error_rows,
            exc=ValueError("Stack underflow while building DFG."),
            stage="stack_effect",
            code_id=state.code_id,
        )
    for _ in range(pop_count):
        if not state.stack:
            break
        producer = state.stack.pop()
        state.dfg_rows.append(
            {
                **identity,
                **_code_unit_key_columns(state.unit_ctx),
                "src_instr_index": producer.instr_index,
                "dst_instr_index": int(idx),
                "kind": "USE_STACK",
            }
        )
    if delta > 0:
        for _ in range(delta):
            state.stack.append(DfgValueRef(instr_index=int(idx), instr_offset=int(ins.offset)))
    return stack_depth_before, stack_depth_after


def _append_instruction_rows(
    unit_ctx: CodeUnitContext,
    ins_rows: list[Row],
    dfg_rows: list[Row],
    error_rows: list[Row],
) -> None:
    identity = unit_ctx.file_ctx.identity_row()
    code_id = None
    state = DfgState(
        unit_ctx=unit_ctx,
        stack=[],
        dfg_rows=dfg_rows,
        error_rows=error_rows,
        code_id=code_id,
    )

    for idx, ins in enumerate(unit_ctx.instruction_data.instructions):
        jt = _jump_target(ins)
        core_fields = _instruction_core_fields(ins)
        pos_fields = _position_fields(ins)
        stack_depth_before, stack_depth_after = _apply_stack_effect(
            state=state,
            idx=idx,
            ins=ins,
        )
        ins_rows.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "instr_index": int(idx),
                "offset": int(ins.offset),
                "opname": ins.opname,
                "opcode": int(ins.opcode),
                "arg": int(ins.arg) if ins.arg is not None else None,
                "argrepr": ins.argrepr,
                "starts_line": int(ins.starts_line) if ins.starts_line is not None else None,
                "is_jump_target": bool(ins.is_jump_target),
                "jump_target": int(jt) if jt is not None else None,
                "stack_depth_before": stack_depth_before,
                "stack_depth_after": stack_depth_after,
                **core_fields,
                **pos_fields,
            }
        )


def _append_line_table_rows(
    unit_ctx: CodeUnitContext,
    line_rows: list[Row],
    error_rows: list[Row],
) -> None:
    identity = unit_ctx.file_ctx.identity_row()
    code_id = None
    try:
        line_pairs = dis.findlinestarts(unit_ctx.code)
    except (RuntimeError, TypeError, ValueError) as exc:
        _append_error_row(
            unit_ctx.file_ctx,
            error_rows,
            exc=exc,
            stage="line_table",
            code_id=code_id,
        )
        return
    for offset, lineno in line_pairs:
        if lineno is None:
            line1: int | None = None
            line0: int | None = None
        else:
            line1 = int(lineno)
            line0 = line1 - 1
        line_rows.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "offset": int(offset),
                "line1": line1,
                "line0": line0,
            }
        )


def _is_block_terminator(ins: dis.Instruction, terminator_opnames: Sequence[str]) -> bool:
    return ins.opname in terminator_opnames or _is_unconditional_jump(ins)


def _next_instruction_offset(
    ins: dis.Instruction,
    index_by_offset: Mapping[int, int],
    instructions: Sequence[dis.Instruction],
) -> int | None:
    current_index = index_by_offset.get(int(ins.offset))
    if current_index is None:
        return None
    next_index = current_index + 1
    if next_index < len(instructions):
        return int(instructions[next_index].offset)
    return None


def _boundary_offsets(unit_ctx: CodeUnitContext, ins_offsets: set[int]) -> list[int]:
    boundaries: set[int] = {0, unit_ctx.code_len}
    with contextlib.suppress(TypeError, ValueError):
        boundaries.update(int(label) for label in dis.findlabels(unit_ctx.code))
    instructions = unit_ctx.instruction_data.instructions
    for ins in instructions:
        jt = _jump_target(ins)
        if jt is not None:
            boundaries.add(int(jt))
        if _is_block_terminator(ins, unit_ctx.file_ctx.options.terminator_opnames):
            next_offset = _next_instruction_offset(
                ins, unit_ctx.instruction_data.index_by_offset, instructions
            )
            if next_offset is not None:
                boundaries.add(next_offset)
    for ex in unit_ctx.exc_entries:
        boundaries.update(
            {
                int(getattr(ex, "start", 0)),
                int(getattr(ex, "end", 0)),
                int(getattr(ex, "target", 0)),
            }
        )
    return sorted(
        offset for offset in boundaries if offset in ins_offsets or offset == unit_ctx.code_len
    )


def _build_blocks(boundaries: Sequence[int]) -> list[tuple[int, int]]:
    return [
        (boundaries[index], boundaries[index + 1])
        for index in range(len(boundaries) - 1)
        if boundaries[index] < boundaries[index + 1]
    ]


def _append_block_rows(
    unit_ctx: CodeUnitContext,
    blocks: Sequence[tuple[int, int]],
    ins_offsets: set[int],
    blk_rows: list[Row],
) -> dict[int, tuple[int, int]]:
    identity = unit_ctx.file_ctx.identity_row()
    target_offsets = {int(getattr(ex, "target", 0)) for ex in unit_ctx.exc_entries}
    off_to_block: dict[int, tuple[int, int]] = {}

    for start, end in blocks:
        kind = "entry" if start == 0 else ("handler" if start in target_offsets else "normal")
        blk_rows.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "start_offset": int(start),
                "end_offset": int(end),
                "kind": kind,
            }
        )
        for offset in sorted(offset for offset in ins_offsets if start <= offset < end):
            off_to_block[offset] = (start, end)

    return off_to_block


def _append_cfg_edge(unit_ctx: CodeUnitContext, spec: CfgEdgeSpec, edge_rows: list[Row]) -> None:
    edge_rows.append(
        {
            **unit_ctx.file_ctx.identity_row(),
            **_code_unit_key_columns(unit_ctx),
            "src_block_start": spec.src_block_start,
            "src_block_end": spec.src_block_end,
            "dst_block_start": spec.dst_block_start,
            "dst_block_end": spec.dst_block_end,
            "kind": spec.kind,
            "edge_key": spec.edge_key,
            "cond_instr_index": spec.cond_instr_index,
            "cond_instr_offset": spec.cond_instr_offset,
            "exc_index": spec.exc_index,
            "jump_label": spec.jump_label,
            "jump_kind": spec.jump_kind,
        }
    )


def _append_jump_edges(ctx: _CfgEdgeContext, block: _BlockEdgeContext) -> bool:
    jt = _jump_target(block.last)
    if jt is None:
        return False
    dst_block = ctx.off_to_block.get(int(jt))
    if dst_block is None:
        return False
    dst_start, dst_end = dst_block
    kind = "jump" if _is_unconditional_jump(block.last) else "branch"
    jump_kind = "unconditional" if kind == "jump" else "conditional"
    label = getattr(block.last, "label", None)
    jump_label = int(label) if isinstance(label, int) else None
    _append_cfg_edge(
        ctx.unit_ctx,
        CfgEdgeSpec(
            src_block_start=block.src_start,
            src_block_end=block.src_end,
            dst_block_start=dst_start,
            dst_block_end=dst_end,
            kind=kind,
            edge_key=str(block.last.offset),
            cond_instr_index=block.last_index if kind == "branch" else None,
            cond_instr_offset=int(block.last.offset) if kind == "branch" else None,
            exc_index=None,
            jump_label=jump_label,
            jump_kind=jump_kind,
        ),
        ctx.edge_rows,
    )
    if kind != "branch":
        return True
    next_block = ctx.off_to_block.get(block.end)
    if next_block is None:
        return True
    next_start, next_end = next_block
    _append_cfg_edge(
        ctx.unit_ctx,
        CfgEdgeSpec(
            src_block_start=block.src_start,
            src_block_end=block.src_end,
            dst_block_start=next_start,
            dst_block_end=next_end,
            kind="next",
            edge_key=str(block.end),
            cond_instr_index=block.last_index,
            cond_instr_offset=int(block.last.offset),
            exc_index=None,
            jump_label=None,
            jump_kind=None,
        ),
        ctx.edge_rows,
    )
    return True


def _append_fallthrough_edge(ctx: _CfgEdgeContext, block: _BlockEdgeContext) -> None:
    next_block = ctx.off_to_block.get(block.end)
    if next_block is None:
        return
    next_start, next_end = next_block
    _append_cfg_edge(
        ctx.unit_ctx,
        CfgEdgeSpec(
            src_block_start=block.src_start,
            src_block_end=block.src_end,
            dst_block_start=next_start,
            dst_block_end=next_end,
            kind="next",
            edge_key=str(block.end),
            cond_instr_index=None,
            cond_instr_offset=None,
            exc_index=None,
            jump_label=None,
            jump_kind=None,
        ),
        ctx.edge_rows,
    )


def _append_edges_for_block(ctx: _CfgEdgeContext, start: int, end: int) -> None:
    ins_in_block = [ins for ins in ctx.instructions if start <= int(ins.offset) < end]
    if not ins_in_block:
        return
    first_offset = int(ins_in_block[0].offset)
    src_block = ctx.off_to_block.get(first_offset)
    if src_block is None:
        return
    src_start, src_end = src_block
    last = ins_in_block[-1]
    last_index = ctx.index_by_offset.get(int(last.offset))
    if last_index is None:
        return
    block = _BlockEdgeContext(
        src_start=src_start,
        src_end=src_end,
        end=end,
        last=last,
        last_index=int(last_index),
    )
    if _append_jump_edges(ctx, block):
        return
    if last.opname in ctx.terminators:
        return
    _append_fallthrough_edge(ctx, block)


def _append_normal_edges(
    unit_ctx: CodeUnitContext,
    blocks: Sequence[tuple[int, int]],
    off_to_block: Mapping[int, tuple[int, int]],
    edge_rows: list[Row],
) -> None:
    ctx = _CfgEdgeContext(
        unit_ctx=unit_ctx,
        instructions=unit_ctx.instruction_data.instructions,
        index_by_offset=unit_ctx.instruction_data.index_by_offset,
        terminators=unit_ctx.file_ctx.options.terminator_opnames,
        off_to_block=off_to_block,
        edge_rows=edge_rows,
    )

    for start, end in blocks:
        _append_edges_for_block(ctx, start, end)


def _append_exception_edges(
    unit_ctx: CodeUnitContext,
    blocks: Sequence[tuple[int, int]],
    off_to_block: Mapping[int, tuple[int, int]],
    edge_rows: list[Row],
) -> None:
    for k, ex in enumerate(unit_ctx.exc_entries):
        start = int(getattr(ex, "start", 0))
        end = int(getattr(ex, "end", 0))
        target = int(getattr(ex, "target", 0))
        handler_block = off_to_block.get(target)
        if handler_block is None:
            continue
        handler_start, handler_end = handler_block
        for block_start, block_end in blocks:
            if block_end <= start or block_start >= end:
                continue
            src_block = off_to_block.get(block_start)
            if src_block is None:
                continue
            src_start, src_end = src_block
            _append_cfg_edge(
                unit_ctx,
                CfgEdgeSpec(
                    src_block_start=src_start,
                    src_block_end=src_end,
                    dst_block_start=handler_start,
                    dst_block_end=handler_end,
                    kind="exc",
                    edge_key=str(k),
                    cond_instr_index=None,
                    cond_instr_offset=None,
                    exc_index=int(k),
                    jump_label=None,
                    jump_kind=None,
                ),
                edge_rows,
            )


def _append_cfg_rows(unit_ctx: CodeUnitContext, blk_rows: list[Row], edge_rows: list[Row]) -> None:
    instructions = unit_ctx.instruction_data.instructions
    if not instructions:
        return
    ins_offsets = {int(ins.offset) for ins in instructions}
    boundaries = _boundary_offsets(unit_ctx, ins_offsets)
    blocks = _build_blocks(boundaries)
    if not blocks:
        return
    off_to_block = _append_block_rows(unit_ctx, blocks, ins_offsets, blk_rows)
    _append_normal_edges(unit_ctx, blocks, off_to_block, edge_rows)
    _append_exception_edges(unit_ctx, blocks, off_to_block, edge_rows)


def _extract_code_unit_rows(
    top: pytypes.CodeType,
    ctx: BytecodeFileContext,
    code_unit_keys: Mapping[int, CodeUnitKey],
    buffers: BytecodeRowBuffers,
) -> None:
    include_cfg = ctx.options.include_cfg_derivations
    for co, _parent in _iter_code_objects(top):
        key = code_unit_keys.get(id(co))
        if key is None:
            continue
        code_id = None
        try:
            instruction_data = _instruction_data(co, ctx.options)
        except (RuntimeError, TypeError, ValueError) as exc:
            _append_error_row(
                ctx,
                buffers.error_rows,
                exc=exc,
                stage="disassembly",
                code_id=code_id,
            )
            instruction_data = InstructionData(instructions=[], index_by_offset={})
        exc_entries: Sequence[object]
        try:
            exc_entries = _exception_entries(co)
        except (RuntimeError, TypeError, ValueError) as exc:
            _append_error_row(
                ctx,
                buffers.error_rows,
                exc=exc,
                stage="exception_table",
                code_id=code_id,
            )
            exc_entries = []
        unit_ctx = CodeUnitContext(
            code_unit_key=key,
            file_ctx=ctx,
            code=co,
            instruction_data=instruction_data,
            exc_entries=exc_entries,
            code_len=len(co.co_code),
        )
        _append_exception_rows(unit_ctx, buffers.exception_rows)
        _append_instruction_rows(
            unit_ctx,
            buffers.instruction_rows,
            buffers.dfg_edge_rows,
            buffers.error_rows,
        )
        _append_line_table_rows(unit_ctx, buffers.line_rows, buffers.error_rows)
        if include_cfg:
            _append_cfg_rows(unit_ctx, buffers.block_rows, buffers.edge_rows)


def _sorted_rows(rows: Sequence[Row], *, key: str) -> list[Row]:
    def _key(row: Row) -> int:
        value = row.get(key)
        return int(value) if isinstance(value, int) else 0

    return sorted(rows, key=_key)


def _sorted_rows_by(rows: Sequence[Row], *, keys: Sequence[str]) -> list[Row]:
    def _key(row: Row) -> tuple[int, ...]:
        parts: list[int] = []
        for field in keys:
            value = row.get(field)
            parts.append(int(value) if isinstance(value, int) else 0)
        return tuple(parts)

    return sorted(rows, key=_key)


def _group_code_unit_buffers(buffers: BytecodeRowBuffers) -> CodeUnitGroups:
    instructions_by_key: dict[tuple[str, str, int], list[Row]] = {}
    exceptions_by_key: dict[tuple[str, str, int], list[Row]] = {}
    blocks_by_key: dict[tuple[str, str, int], list[Row]] = {}
    edges_by_key: dict[tuple[str, str, int], list[Row]] = {}
    line_table_by_key: dict[tuple[str, str, int], list[Row]] = {}
    dfg_edges_by_key: dict[tuple[str, str, int], list[Row]] = {}

    for row in buffers.instruction_rows:
        instructions_by_key.setdefault(_code_unit_key(row), []).append(row)
    for row in buffers.exception_rows:
        exceptions_by_key.setdefault(_code_unit_key(row), []).append(row)
    for row in buffers.block_rows:
        blocks_by_key.setdefault(_code_unit_key(row), []).append(row)
    for row in buffers.edge_rows:
        edges_by_key.setdefault(_code_unit_key(row), []).append(row)
    for row in buffers.line_rows:
        line_table_by_key.setdefault(_code_unit_key(row), []).append(row)
    for row in buffers.dfg_edge_rows:
        dfg_edges_by_key.setdefault(_code_unit_key(row), []).append(row)

    return CodeUnitGroups(
        instructions_by_key=instructions_by_key,
        exceptions_by_key=exceptions_by_key,
        blocks_by_key=blocks_by_key,
        edges_by_key=edges_by_key,
        line_table_by_key=line_table_by_key,
        dfg_edges_by_key=dfg_edges_by_key,
    )


def _code_object_entry(
    row: Row,
    groups: CodeUnitGroups,
    *,
    line_offsets: LineOffsets | None,
) -> dict[str, object]:
    qualpath = row.get("qualpath")
    co_qualname = row.get("co_qualname")
    co_filename = row.get("co_filename")
    co_name = row.get("co_name")
    firstlineno = row.get("firstlineno")
    code_id = None
    key = _code_unit_key(row)
    instructions = [
        _instruction_entry(item, line_offsets=line_offsets)
        for item in _sorted_rows(groups.instructions_by_key.get(key, []), key="instr_index")
    ]
    exceptions = [
        _exception_entry(item)
        for item in _sorted_rows(groups.exceptions_by_key.get(key, []), key="exc_index")
    ]
    blocks = [
        _block_entry(item)
        for item in _sorted_rows(groups.blocks_by_key.get(key, []), key="start_offset")
    ]
    edges = [_cfg_edge_entry(item) for item in groups.edges_by_key.get(key, [])]
    line_table = [
        _line_entry(item)
        for item in _sorted_rows(groups.line_table_by_key.get(key, []), key="offset")
    ]
    dfg_edges = [
        _dfg_edge_entry(item)
        for item in _sorted_rows_by(
            groups.dfg_edges_by_key.get(key, []),
            keys=("src_instr_index", "dst_instr_index"),
        )
    ]
    flags = row.get("flags")
    flags_int = int(flags) if isinstance(flags, int) else 0
    return {
        "code_id": code_id,
        "qualname": qualpath,
        "co_qualname": co_qualname,
        "co_filename": co_filename,
        "name": co_name,
        "firstlineno1": firstlineno,
        "argcount": row.get("argcount"),
        "posonlyargcount": row.get("posonlyargcount"),
        "kwonlyargcount": row.get("kwonlyargcount"),
        "nlocals": row.get("nlocals"),
        "flags": flags_int,
        "flags_detail": _flags_detail(flags_int),
        "stacksize": row.get("stacksize"),
        "code_len": row.get("code_len"),
        "varnames": _string_list(row.get("varnames")),
        "freevars": _string_list(row.get("freevars")),
        "cellvars": _string_list(row.get("cellvars")),
        "names": _string_list(row.get("names")),
        "consts": _const_entry_list(row.get("consts")),
        "consts_json": row.get("consts_json"),
        "line_table": line_table,
        "instructions": instructions,
        "exception_table": exceptions,
        "blocks": blocks,
        "cfg_edges": edges,
        "dfg_edges": dfg_edges,
        "attrs": attrs_map(
            {
                "parent_qualpath": row.get("parent_qualpath"),
                "parent_co_name": row.get("parent_co_name"),
                "parent_firstlineno": row.get("parent_firstlineno"),
            }
        ),
    }


def _code_objects_from_buffers(
    buffers: BytecodeRowBuffers,
    *,
    line_offsets: LineOffsets | None,
) -> list[dict[str, object]]:
    groups = _group_code_unit_buffers(buffers)
    return [
        _code_object_entry(row, groups, line_offsets=line_offsets) for row in buffers.code_unit_rows
    ]


def _cached_bytecode_payload(
    *,
    cache: Cache | FanoutCache | None,
    cache_key: str | None,
    file_ctx: FileContext,
    options: BytecodeExtractOptions,
) -> dict[str, object] | None:
    if cache is None or cache_key is None:
        return None
    cached = cache_get(cache, key=cache_key, default=None)
    if not isinstance(cached, BytecodeCacheResult):
        return None
    return _bytecode_row_payload(
        file_ctx,
        options=options,
        code_objects=cached.code_objects,
        errors=cached.errors,
    )


def _bytecode_row_from_context(
    bc_ctx: BytecodeFileContext,
    *,
    options: BytecodeExtractOptions,
    cache: Cache | FanoutCache | None,
    cache_key: str | None,
    cache_ttl: float | None,
) -> dict[str, object] | None:
    text = text_from_file_ctx(bc_ctx.file_ctx)
    if text is None:
        return None
    line_offsets = _line_offsets(bc_ctx.file_ctx)
    buffers = BytecodeRowBuffers(
        code_unit_rows=[],
        instruction_rows=[],
        exception_rows=[],
        block_rows=[],
        edge_rows=[],
        line_rows=[],
        dfg_edge_rows=[],
        error_rows=[],
    )
    top, err = _compile_text(text, bc_ctx)
    if err is not None:
        buffers.error_rows.append(err)
    if top is not None:
        code_unit_keys = _assign_code_units(top, bc_ctx, buffers.code_unit_rows)
        _extract_code_unit_rows(top, bc_ctx, code_unit_keys, buffers)
    code_objects = _code_objects_from_buffers(buffers, line_offsets=line_offsets)
    errors = [_error_entry(row) for row in buffers.error_rows]
    if cache is not None and cache_key is not None:
        cache_set(
            cache,
            key=cache_key,
            value=BytecodeCacheResult(code_objects=code_objects, errors=errors),
            options=CacheSetOptions(
                expire=cache_ttl,
                tag=options.repo_id,
            ),
        )
    return _bytecode_row_payload(
        bc_ctx.file_ctx,
        options=options,
        code_objects=code_objects,
        errors=errors,
    )


def _bytecode_file_row(
    file_ctx: FileContext,
    *,
    options: BytecodeExtractOptions,
    cache: Cache | FanoutCache | None,
    cache_ttl: float | None,
) -> dict[str, object] | None:
    bc_ctx = _context_from_file_ctx(file_ctx, options)
    if bc_ctx is None:
        return None
    cache_key = _bytecode_cache_key(file_ctx, options=options)
    cache_key_str = stable_cache_key("bytecode", {"key": cache_key}) if cache_key else None
    cached = _cached_bytecode_payload(
        cache=cache,
        cache_key=cache_key_str,
        file_ctx=file_ctx,
        options=options,
    )
    if cached is not None:
        return cached
    if cache is not None and cache_key_str is not None:
        with cache_lock(cache, key=cache_key_str):
            cached = _cached_bytecode_payload(
                cache=cache,
                cache_key=cache_key_str,
                file_ctx=file_ctx,
                options=options,
            )
            if cached is not None:
                return cached
            return _bytecode_row_from_context(
                bc_ctx,
                options=options,
                cache=cache,
                cache_key=cache_key_str,
                cache_ttl=cache_ttl,
            )
    return _bytecode_row_from_context(
        bc_ctx,
        options=options,
        cache=cache,
        cache_key=cache_key_str,
        cache_ttl=cache_ttl,
    )


def _collect_bytecode_file_rows(
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None,
    *,
    scope_manifest: ScopeManifest | None,
    options: BytecodeExtractOptions,
    runtime_profile: DataFusionRuntimeProfile | None,
) -> list[dict[str, object]]:
    contexts = list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=repo_files,
                output_table="bytecode_files_v1",
                runtime_profile=runtime_profile,
                file_contexts=file_contexts,
                queue_name=(
                    worklist_queue_name(output_table="bytecode_files_v1", repo_id=options.repo_id)
                    if options.use_worklist_queue
                    else None
                ),
                scope_manifest=scope_manifest,
            )
        )
    )
    if not contexts:
        return []
    cache_profile = diskcache_profile_from_ctx(runtime_profile)
    cache = cache_for_extract(cache_profile)
    cache_ttl = cache_ttl_seconds(cache_profile, "extract")
    max_workers = _effective_max_workers(options)
    if not _should_parallelize(contexts, options=options, max_workers=max_workers):
        rows: list[dict[str, object]] = []
        for file_ctx in contexts:
            row = _bytecode_file_row(
                file_ctx,
                options=options,
                cache=cache,
                cache_ttl=cache_ttl,
            )
            if row is not None:
                rows.append(row)
        return rows
    worker = functools.partial(
        _bytecode_file_row,
        options=options,
        cache=cache,
        cache_ttl=cache_ttl,
    )
    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for row in executor.map(worker, contexts):
            if row is not None:
                rows.append(row)
    return rows


def _build_bytecode_file_plan(
    rows: list[dict[str, object]],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    return extract_plan_from_rows(
        "bytecode_files_v1",
        rows,
        session=session,
        options=ExtractPlanOptions(
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    )


def _jump_target(ins: dis.Instruction) -> int | None:
    jt = getattr(ins, "jump_target", None)
    if isinstance(jt, int):
        return jt
    jt = getattr(ins, "jump_target_offset", None)
    if isinstance(jt, int):
        return jt
    if ins.opcode in dis.hasjump and isinstance(ins.argval, int):
        return int(ins.argval)
    if (ins.opcode in dis.hasjabs or ins.opcode in dis.hasjrel) and isinstance(ins.argval, int):
        return int(ins.argval)
    return None


def _is_unconditional_jump(ins: dis.Instruction) -> bool:
    if ins.opcode not in dis.hasjump:
        return False
    if not ins.opname.startswith("JUMP"):
        return False
    # heuristic: "IF" jumps are conditional, FOR_ITER is conditional-ish
    return ("IF" not in ins.opname) and (ins.opname != "FOR_ITER")


def _iter_code_objects(
    co: pytypes.CodeType, parent: pytypes.CodeType | None = None
) -> Iterator[tuple[pytypes.CodeType, pytypes.CodeType | None]]:
    yield co, parent
    for c in co.co_consts:
        if isinstance(c, pytypes.CodeType):
            yield from _iter_code_objects(c, co)


def extract_bytecode(
    repo_files: TableLike,
    options: BytecodeExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract bytecode tables from repository files.

    Returns:
    -------
    ExtractResult[TableLike]
        Nested bytecode file table.
    """
    normalized_options = normalize_options("bytecode", options, BytecodeExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    rows = _collect_bytecode_file_rows(
        repo_files,
        exec_context.file_contexts,
        scope_manifest=exec_context.scope_manifest,
        options=normalized_options,
        runtime_profile=runtime_profile,
    )
    plan = _build_bytecode_file_plan(
        rows,
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
        session=session,
    )
    materialize_options = ExtractMaterializeOptions(
        normalize=normalize,
        apply_post_kernels=True,
    )
    table = materialize_extract_plan(
        "bytecode_files_v1",
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=materialize_options,
    )
    return ExtractResult(table=table, extractor_name="bytecode")


def extract_bytecode_plans(
    repo_files: TableLike,
    options: BytecodeExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> dict[str, DataFusionPlanArtifact]:
    """Extract bytecode plans from repository files.

    Returns:
    -------
    dict[str, DataFusionPlanArtifact]
        Plan bundle keyed by bytecode output name.
    """
    normalized_options = normalize_options("bytecode", options, BytecodeExtractOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    evidence_plan = exec_context.evidence_plan
    rows = _collect_bytecode_file_rows(
        repo_files,
        exec_context.file_contexts,
        scope_manifest=exec_context.scope_manifest,
        options=normalized_options,
        runtime_profile=runtime_profile,
    )
    return {
        "bytecode_files": _build_bytecode_file_plan(
            rows,
            normalize=normalize,
            evidence_plan=evidence_plan,
            session=session,
        )
    }


class _BytecodeTableKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: BytecodeExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: bool


class _BytecodeTableKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: BytecodeExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: Literal[False]


class _BytecodeTableKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: BytecodeExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    profile: str
    prefer_reader: Required[Literal[True]]


@overload
def extract_bytecode_table(
    **kwargs: Unpack[_BytecodeTableKwargsTable],
) -> TableLike: ...


@overload
def extract_bytecode_table(
    **kwargs: Unpack[_BytecodeTableKwargsReader],
) -> TableLike | RecordBatchReaderLike: ...


def extract_bytecode_table(
    **kwargs: Unpack[_BytecodeTableKwargs],
) -> TableLike | RecordBatchReaderLike:
    """Extract bytecode instruction facts as a single table.

    Parameters
    ----------
    kwargs:
        Keyword-only arguments for extraction (repo_files, options, file_contexts, ctx, profile,
        prefer_reader).

    Returns:
    -------
    TableLike | RecordBatchReaderLike
        Bytecode instruction output.
    """
    with stage_span(
        "extract.bytecode_table",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={"codeanatomy.extractor": "bytecode"},
    ):
        repo_files = kwargs["repo_files"]
        normalized_options = normalize_options(
            "bytecode",
            kwargs.get("options"),
            BytecodeExtractOptions,
        )
        file_contexts = kwargs.get("file_contexts")
        evidence_plan = kwargs.get("evidence_plan")
        profile = kwargs.get("profile", "default")
        prefer_reader = kwargs.get("prefer_reader", False)
        exec_context = ExtractExecutionContext(
            file_contexts=file_contexts,
            evidence_plan=evidence_plan,
            scope_manifest=kwargs.get("scope_manifest"),
            session=kwargs.get("session"),
            profile=profile,
        )
        session = exec_context.ensure_session()
        exec_context = replace(exec_context, session=session)
        runtime_profile = exec_context.ensure_runtime_profile()
        determinism_tier = exec_context.determinism_tier()
        normalize = ExtractNormalizeOptions(options=normalized_options)
        plans = extract_bytecode_plans(
            repo_files,
            options=normalized_options,
            context=exec_context,
        )
        return materialize_extract_plan(
            "bytecode_files_v1",
            plans["bytecode_files"],
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        )
