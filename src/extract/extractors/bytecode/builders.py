"""Extract Python bytecode artifacts into Arrow tables using shared helpers."""

from __future__ import annotations

import dis
import importlib.util
import inspect
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from core_types import RowRich as Row
from core_types import RowValueRich as RowValue
from extract.coordination.context import (
    FileContext,
    SpanSpec,
    attrs_map,
    span_dict,
)
from extract.coordination.line_offsets import LineOffsets
from extract.extractors.bytecode.setup import (
    BytecodeExtractOptions,
    BytecodeFileContext,
)
from extract.extractors.bytecode.visitors import (
    CodeUnitContext,
)
from extract.infrastructure.parallel import resolve_max_workers
from extract.infrastructure.schema_cache import bytecode_files_fingerprint

type BytecodeCacheKey = tuple[str, str, str, int, bool, bool, bool, tuple[str, ...]]

BC_LINE_BASE = 1
CACHE_ENTRY_FIELDS = 3
PYTHON_VERSION = sys.version.split()[0]
PYTHON_MAGIC = importlib.util.MAGIC_NUMBER.hex()


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


from extract.extractors.bytecode.builders_runtime import (
    extract_bytecode,
    extract_bytecode_plans,
    extract_bytecode_table,
)

__all__ = [
    "extract_bytecode",
    "extract_bytecode_plans",
    "extract_bytecode_table",
]
