"""Extract Python bytecode artifacts into Arrow tables using shared helpers."""

from __future__ import annotations

import dis
import json
import types as pytypes
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Required, TypedDict, Unpack, cast, overload

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.ids import stable_id
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    FileContext,
    SpanSpec,
    apply_query_and_project,
    attrs_map,
    file_identity_row,
    ibis_plan_from_rows,
    iter_contexts,
    materialize_extract_plan,
    span_dict,
    text_from_file_ctx,
)
from extract.registry_specs import dataset_schema, normalize_options
from extract.schema_ops import ExtractNormalizeOptions
from ibis_engine.plan import IbisPlan

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan

type RowValue = str | int | bool | list[str] | None
type Row = dict[str, RowValue]

BC_LINE_BASE = 1
BC_COL_UNIT = "utf32"
BC_END_EXCLUSIVE = True


@dataclass(frozen=True)
class BytecodeExtractOptions:
    """
    Bytecode extraction options.

    - adaptive=False is the canonical stream for stable semantics across runs
      (per your recipe). :contentReference[oaicite:17]{index=17}
    - include_cfg_derivations implements the blocks/CFG edges derivation recipe.
      :contentReference[oaicite:18]{index=18}
    """

    optimize: int = 0
    dont_inherit: bool = True
    adaptive: bool = False
    include_cfg_derivations: bool = True
    terminator_opnames: Sequence[str] = (
        "RETURN_VALUE",
        "RETURN_CONST",
        "RAISE_VARARGS",
        "RERAISE",
    )
    repo_id: str | None = None


@dataclass(frozen=True)
class BytecodeExtractResult:
    """Extracted bytecode tables for code units, instructions, and edges."""

    bytecode_files: TableLike


@dataclass(frozen=True)
class BytecodeFileContext:
    """Per-file context for bytecode extraction."""

    file_ctx: FileContext
    options: BytecodeExtractOptions

    @property
    def file_id(self) -> str:
        """Return the file id for this extraction context.

        Returns
        -------
        str
            File id from the file context.
        """
        return self.file_ctx.file_id

    @property
    def path(self) -> str:
        """Return the file path for this extraction context.

        Returns
        -------
        str
            File path from the file context.
        """
        return self.file_ctx.path

    @property
    def file_sha256(self) -> str | None:
        """Return the file sha256 for this extraction context.

        Returns
        -------
        str | None
            File hash from the file context.
        """
        return self.file_ctx.file_sha256

    def identity_row(self) -> Row:
        """Return the identity row for this context.

        Returns
        -------
        dict[str, object]
            File identity columns for row construction.
        """
        return cast("Row", file_identity_row(self.file_ctx))


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
    error_rows: list[Row]


BYTECODE_FILES_SCHEMA = dataset_schema("bytecode_files_v1")


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


def _instruction_entry(row: Mapping[str, RowValue]) -> dict[str, object]:
    line_base = row.get("line_base")
    line_base_int = int(line_base) if isinstance(line_base, int) else BC_LINE_BASE
    start_line = row.get("pos_start_line")
    end_line = row.get("pos_end_line")
    start_col = row.get("pos_start_col")
    end_col = row.get("pos_end_col")
    start_line0 = int(start_line) - line_base_int if isinstance(start_line, int) else None
    end_line0 = int(end_line) - line_base_int if isinstance(end_line, int) else None
    col_unit = str(row.get("col_unit")) if row.get("col_unit") is not None else BC_COL_UNIT
    return {
        "offset": row.get("offset"),
        "opname": row.get("opname"),
        "opcode": row.get("opcode"),
        "arg": row.get("arg"),
        "argrepr": row.get("argrepr"),
        "is_jump_target": row.get("is_jump_target"),
        "jump_target": row.get("jump_target_offset"),
        "span": span_dict(
            SpanSpec(
                start_line0=start_line0,
                start_col=int(start_col) if isinstance(start_col, int) else None,
                end_line0=end_line0,
                end_col=int(end_col) if isinstance(end_col, int) else None,
                end_exclusive=bool(row.get("end_exclusive", BC_END_EXCLUSIVE)),
                col_unit=col_unit,
            )
        ),
        "attrs": attrs_map(
            {
                "argval_str": row.get("argval_str"),
                "starts_line": row.get("starts_line"),
                "instr_index": row.get("instr_index"),
            }
        ),
    }


def _string_list(value: object | None) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return []


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
        "attrs": attrs_map({}),
    }


def _error_entry(row: Mapping[str, RowValue]) -> dict[str, object]:
    return {
        "error_type": row.get("error_type"),
        "message": row.get("message"),
        "attrs": attrs_map({}),
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


def _build_code_unit_context(
    co: pytypes.CodeType,
    ctx: BytecodeFileContext,
    code_unit_key: CodeUnitKey,
) -> CodeUnitContext:
    return CodeUnitContext(
        code_unit_key=code_unit_key,
        file_ctx=ctx,
        instruction_data=_instruction_data(co, ctx.options),
        exc_entries=_exception_entries(co),
        code_len=len(co.co_code),
    )


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


def _append_instruction_rows(unit_ctx: CodeUnitContext, ins_rows: list[Row]) -> None:
    identity = unit_ctx.file_ctx.identity_row()
    for idx, ins in enumerate(unit_ctx.instruction_data.instructions):
        jt = _jump_target(ins)
        pos = getattr(ins, "positions", None)

        pos_start_line = getattr(pos, "lineno", None) if pos else None
        pos_end_line = getattr(pos, "end_lineno", None) if pos else None
        pos_start_col = getattr(pos, "col_offset", None) if pos else None
        pos_end_col = getattr(pos, "end_col_offset", None) if pos else None

        ins_rows.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "instr_index": int(idx),
                "offset": int(ins.offset),
                "opname": ins.opname,
                "opcode": int(ins.opcode),
                "arg": int(ins.arg) if ins.arg is not None else None,
                "argval_str": str(ins.argval) if ins.argval is not None else None,
                "argrepr": ins.argrepr,
                "is_jump_target": bool(ins.is_jump_target),
                "jump_target_offset": int(jt) if jt is not None else None,
                "starts_line": int(ins.starts_line) if ins.starts_line is not None else None,
                "pos_start_line": int(pos_start_line) if pos_start_line is not None else None,
                "pos_end_line": int(pos_end_line) if pos_end_line is not None else None,
                "pos_start_col": int(pos_start_col) if pos_start_col is not None else None,
                "pos_end_col": int(pos_end_col) if pos_end_col is not None else None,
                "line_base": BC_LINE_BASE,
                "col_unit": BC_COL_UNIT,
                "end_exclusive": BC_END_EXCLUSIVE,
            }
        )


def _is_block_terminator(ins: dis.Instruction, terminator_opnames: Sequence[str]) -> bool:
    return ins.opname in terminator_opnames or _is_unconditional_jump(ins.opname)


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
    kind = "jump" if _is_unconditional_jump(block.last.opname) else "branch"
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
        unit_ctx = _build_code_unit_context(co, ctx, key)
        _append_exception_rows(unit_ctx, buffers.exception_rows)
        _append_instruction_rows(unit_ctx, buffers.instruction_rows)
        if include_cfg:
            _append_cfg_rows(unit_ctx, buffers.block_rows, buffers.edge_rows)


def _sorted_rows(rows: Sequence[Row], *, key: str) -> list[Row]:
    def _key(row: Row) -> int:
        value = row.get(key)
        return int(value) if isinstance(value, int) else 0

    return sorted(rows, key=_key)


def _code_objects_from_buffers(buffers: BytecodeRowBuffers) -> list[dict[str, object]]:
    instructions_by_key: dict[tuple[str, str, int], list[Row]] = {}
    exceptions_by_key: dict[tuple[str, str, int], list[Row]] = {}
    blocks_by_key: dict[tuple[str, str, int], list[Row]] = {}
    edges_by_key: dict[tuple[str, str, int], list[Row]] = {}

    for row in buffers.instruction_rows:
        instructions_by_key.setdefault(_code_unit_key(row), []).append(row)
    for row in buffers.exception_rows:
        exceptions_by_key.setdefault(_code_unit_key(row), []).append(row)
    for row in buffers.block_rows:
        blocks_by_key.setdefault(_code_unit_key(row), []).append(row)
    for row in buffers.edge_rows:
        edges_by_key.setdefault(_code_unit_key(row), []).append(row)

    code_objects: list[dict[str, object]] = []
    for row in buffers.code_unit_rows:
        qualpath = row.get("qualpath")
        co_name = row.get("co_name")
        firstlineno = row.get("firstlineno")
        code_id = stable_id(
            "code",
            str(qualpath) if qualpath is not None else None,
            str(co_name) if co_name is not None else None,
            str(firstlineno) if firstlineno is not None else None,
        )
        key = _code_unit_key(row)
        instructions = [
            _instruction_entry(item)
            for item in _sorted_rows(instructions_by_key.get(key, []), key="instr_index")
        ]
        exceptions = [
            _exception_entry(item)
            for item in _sorted_rows(exceptions_by_key.get(key, []), key="exc_index")
        ]
        blocks = [
            _block_entry(item)
            for item in _sorted_rows(blocks_by_key.get(key, []), key="start_offset")
        ]
        edges = [_cfg_edge_entry(item) for item in edges_by_key.get(key, [])]
        code_objects.append(
            {
                "code_id": code_id,
                "qualname": qualpath,
                "name": co_name,
                "firstlineno1": firstlineno,
                "argcount": row.get("argcount"),
                "posonlyargcount": row.get("posonlyargcount"),
                "kwonlyargcount": row.get("kwonlyargcount"),
                "nlocals": row.get("nlocals"),
                "flags": row.get("flags"),
                "stacksize": row.get("stacksize"),
                "code_len": row.get("code_len"),
                "varnames": _string_list(row.get("varnames")),
                "freevars": _string_list(row.get("freevars")),
                "cellvars": _string_list(row.get("cellvars")),
                "names": _string_list(row.get("names")),
                "consts_json": row.get("consts_json"),
                "instructions": instructions,
                "exception_table": exceptions,
                "blocks": blocks,
                "cfg_edges": edges,
                "attrs": attrs_map(
                    {
                        "parent_qualpath": row.get("parent_qualpath"),
                        "parent_co_name": row.get("parent_co_name"),
                        "parent_firstlineno": row.get("parent_firstlineno"),
                    }
                ),
            }
        )
    return code_objects


def _bytecode_file_row(
    file_ctx: FileContext,
    *,
    options: BytecodeExtractOptions,
) -> dict[str, object] | None:
    bc_ctx = _context_from_file_ctx(file_ctx, options)
    if bc_ctx is None:
        return None
    text = text_from_file_ctx(bc_ctx.file_ctx)
    if text is None:
        return None
    buffers = BytecodeRowBuffers(
        code_unit_rows=[],
        instruction_rows=[],
        exception_rows=[],
        block_rows=[],
        edge_rows=[],
        error_rows=[],
    )
    top, err = _compile_text(text, bc_ctx)
    if err is not None:
        buffers.error_rows.append(err)
    if top is not None:
        code_unit_keys = _assign_code_units(top, bc_ctx, buffers.code_unit_rows)
        _extract_code_unit_rows(top, bc_ctx, code_unit_keys, buffers)
    code_objects = _code_objects_from_buffers(buffers)
    errors = [_error_entry(row) for row in buffers.error_rows]
    return {
        "repo": options.repo_id,
        "path": file_ctx.path,
        "file_id": file_ctx.file_id,
        "code_objects": code_objects,
        "errors": errors,
        "attrs": attrs_map({"file_sha256": file_ctx.file_sha256}),
    }


def _collect_bytecode_file_rows(
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None,
    *,
    options: BytecodeExtractOptions,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for file_ctx in iter_contexts(repo_files, file_contexts):
        row = _bytecode_file_row(file_ctx, options=options)
        if row is not None:
            rows.append(row)
    return rows


def _build_bytecode_file_plan(
    rows: list[dict[str, object]],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None = None,
) -> IbisPlan:
    raw_plan = ibis_plan_from_rows("bytecode_files_v1", rows, row_schema=BYTECODE_FILES_SCHEMA)
    return apply_query_and_project(
        "bytecode_files_v1",
        raw_plan.expr,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


def _jump_target(ins: dis.Instruction) -> int | None:
    jt = getattr(ins, "jump_target", None)
    if isinstance(jt, int):
        return jt
    jt = getattr(ins, "jump_target_offset", None)
    if isinstance(jt, int):
        return jt
    if (ins.opcode in dis.hasjabs or ins.opcode in dis.hasjrel) and isinstance(ins.argval, int):
        return int(ins.argval)
    return None


def _is_unconditional_jump(opname: str) -> bool:
    if not opname.startswith("JUMP"):
        return False
    # heuristic: "IF" jumps are conditional, FOR_ITER is conditional-ish
    return ("IF" not in opname) and (opname != "FOR_ITER")


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
) -> BytecodeExtractResult:
    """Extract bytecode tables from repository files.

    Returns
    -------
    BytecodeExtractResult
        Nested bytecode file table.
    """
    normalized_options = normalize_options("bytecode", options, BytecodeExtractOptions)
    exec_context = context or ExtractExecutionContext()
    exec_ctx = exec_context.ensure_ctx()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    rows = _collect_bytecode_file_rows(
        repo_files,
        exec_context.file_contexts,
        options=normalized_options,
    )
    plan = _build_bytecode_file_plan(
        rows,
        normalize=normalize,
        evidence_plan=exec_context.evidence_plan,
    )
    materialize_options = ExtractMaterializeOptions(
        normalize=normalize,
        apply_post_kernels=True,
    )
    return BytecodeExtractResult(
        bytecode_files=materialize_extract_plan(
            "bytecode_files_v1",
            plan,
            ctx=exec_ctx,
            options=materialize_options,
        )
    )


def extract_bytecode_plans(
    repo_files: TableLike,
    options: BytecodeExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> dict[str, IbisPlan]:
    """Extract bytecode plans from repository files.

    Returns
    -------
    dict[str, IbisPlan]
        Ibis plan bundle keyed by bytecode output name.
    """
    normalized_options = normalize_options("bytecode", options, BytecodeExtractOptions)
    exec_context = context or ExtractExecutionContext()
    normalize = ExtractNormalizeOptions(options=normalized_options)
    evidence_plan = exec_context.evidence_plan
    rows = _collect_bytecode_file_rows(
        repo_files,
        exec_context.file_contexts,
        options=normalized_options,
    )
    return {
        "bytecode_files": _build_bytecode_file_plan(
            rows,
            normalize=normalize,
            evidence_plan=evidence_plan,
        )
    }


class _BytecodeTableKwargs(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: BytecodeExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    prefer_reader: bool


class _BytecodeTableKwargsTable(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: BytecodeExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
    profile: str
    prefer_reader: Literal[False]


class _BytecodeTableKwargsReader(TypedDict, total=False):
    repo_files: Required[TableLike]
    options: BytecodeExtractOptions | None
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    ctx: ExecutionContext | None
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

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Bytecode instruction output.
    """
    repo_files = kwargs["repo_files"]
    normalized_options = normalize_options(
        "bytecode",
        kwargs.get("options"),
        BytecodeExtractOptions,
    )
    file_contexts = kwargs.get("file_contexts")
    evidence_plan = kwargs.get("evidence_plan")
    profile = kwargs.get("profile", "default")
    exec_ctx = kwargs.get("ctx") or execution_context_factory(profile)
    prefer_reader = kwargs.get("prefer_reader", False)
    normalize = ExtractNormalizeOptions(options=normalized_options)
    plans = extract_bytecode_plans(
        repo_files,
        options=normalized_options,
        context=ExtractExecutionContext(
            file_contexts=file_contexts,
            evidence_plan=evidence_plan,
            ctx=exec_ctx,
            profile=profile,
        ),
    )
    return materialize_extract_plan(
        "bytecode_files_v1",
        plans["bytecode_files"],
        ctx=exec_ctx,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=True,
        ),
    )
