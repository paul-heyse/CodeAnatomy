"""Extract Python bytecode artifacts into Arrow tables."""

from __future__ import annotations

import dis
import types as pytypes
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.ids import hash64_from_parts
from arrowdsl.iter import iter_table_rows

type RowValue = str | int | bool | None
type Row = dict[str, RowValue]

SCHEMA_VERSION = 1


def _hash_id(prefix: str, *parts: str) -> str:
    # Row-wise hash64 IDs are needed while building dependent rows.
    hashed = hash64_from_parts(*parts, prefix=prefix)
    return f"{prefix}:{hashed}"


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


@dataclass(frozen=True)
class BytecodeExtractResult:
    """Extracted bytecode tables for code units, instructions, and edges."""

    py_bc_code_units: pa.Table
    py_bc_instructions: pa.Table
    py_bc_exception_table: pa.Table
    py_bc_blocks: pa.Table
    py_bc_cfg_edges: pa.Table
    py_bc_errors: pa.Table


@dataclass(frozen=True)
class BytecodeFileContext:
    """Per-file context for bytecode extraction."""

    file_id: str
    path: str
    file_sha256: str | None
    options: BytecodeExtractOptions


@dataclass(frozen=True)
class InstructionData:
    """Instruction data for a compiled code object."""

    instructions: list[dis.Instruction]
    index_by_offset: dict[int, int]


@dataclass(frozen=True)
class CodeUnitContext:
    """Per-code-unit extraction context."""

    cu_id: str
    file_ctx: BytecodeFileContext
    instruction_data: InstructionData
    exc_entries: Sequence[object]
    code_len: int


@dataclass(frozen=True)
class CfgEdgeSpec:
    """CFG edge descriptor."""

    src_block_id: str
    dst_block_id: str
    kind: str
    edge_key: str
    cond_instr_id: str | None
    exc_index: int | None


@dataclass
class BytecodeRowBuffers:
    """Mutable row buffers for bytecode extraction."""

    code_unit_rows: list[Row]
    instruction_rows: list[Row]
    exception_rows: list[Row]
    block_rows: list[Row]
    edge_rows: list[Row]
    error_rows: list[Row]


CODE_UNITS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("code_unit_id", pa.string()),
        ("parent_code_unit_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("qualpath", pa.string()),
        ("co_name", pa.string()),
        ("firstlineno", pa.int32()),
        ("argcount", pa.int32()),
        ("posonlyargcount", pa.int32()),
        ("kwonlyargcount", pa.int32()),
        ("nlocals", pa.int32()),
        ("flags", pa.int32()),
        ("stacksize", pa.int32()),
        ("code_len", pa.int32()),
    ]
)

INSTR_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("instr_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("instr_index", pa.int32()),
        ("offset", pa.int32()),
        ("opname", pa.string()),
        ("opcode", pa.int32()),
        ("arg", pa.int32()),
        ("argval_str", pa.string()),
        ("argrepr", pa.string()),
        ("is_jump_target", pa.bool_()),
        ("jump_target_offset", pa.int32()),
        ("starts_line", pa.int32()),
        ("pos_start_line", pa.int32()),
        ("pos_end_line", pa.int32()),
        ("pos_start_col", pa.int32()),
        ("pos_end_col", pa.int32()),
    ]
)

EXC_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("exc_entry_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("exc_index", pa.int32()),
        ("start_offset", pa.int32()),
        ("end_offset", pa.int32()),
        ("target_offset", pa.int32()),
        ("depth", pa.int32()),
        ("lasti", pa.bool_()),
    ]
)

BLOCKS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("block_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("start_offset", pa.int32()),
        ("end_offset", pa.int32()),
        ("kind", pa.string()),
    ]
)

CFG_EDGES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("edge_id", pa.string()),
        ("code_unit_id", pa.string()),
        ("src_block_id", pa.string()),
        ("dst_block_id", pa.string()),
        ("kind", pa.string()),  # next|jump|branch|exc
        ("cond_instr_id", pa.string()),
        ("exc_index", pa.int32()),
    ]
)

ERRORS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("file_id", pa.string()),
        ("path", pa.string()),
        ("file_sha256", pa.string()),
        ("error_type", pa.string()),
        ("message", pa.string()),
    ]
)


def _row_context(
    rf: dict[str, object], options: BytecodeExtractOptions
) -> BytecodeFileContext | None:
    file_id_value = rf.get("file_id")
    path_value = rf.get("path")
    if file_id_value is None or path_value is None:
        return None
    file_sha256_value = rf.get("file_sha256")
    file_sha256 = file_sha256_value if isinstance(file_sha256_value, str) else None
    return BytecodeFileContext(
        file_id=str(file_id_value),
        path=str(path_value),
        file_sha256=file_sha256,
        options=options,
    )


def _row_text(rf: dict[str, object]) -> str | None:
    text = rf.get("text")
    if isinstance(text, str) and text:
        return text
    raw_bytes = rf.get("bytes")
    if not isinstance(raw_bytes, (bytes, bytearray, memoryview)):
        return None
    encoding_value = rf.get("encoding")
    encoding = encoding_value if isinstance(encoding_value, str) else "utf-8"
    try:
        return bytes(raw_bytes).decode(encoding, errors="replace")
    except LookupError:
        return None


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
                "schema_version": SCHEMA_VERSION,
                "file_id": ctx.file_id,
                "path": ctx.path,
                "file_sha256": ctx.file_sha256,
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
) -> dict[int, str]:
    co_to_id: dict[int, str] = {}
    co_to_qual: dict[int, str] = {}

    for co, parent in _iter_code_objects(top):
        parent_id = co_to_id.get(id(parent)) if parent is not None else None
        parent_qual = co_to_qual.get(id(parent)) if parent is not None else None
        qual = _qual_for(co, parent_qual)
        cu_id = _hash_id("bc_code", ctx.file_id, qual, str(co.co_firstlineno), co.co_name)
        co_to_id[id(co)] = cu_id
        co_to_qual[id(co)] = qual

        cu_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "code_unit_id": cu_id,
                "parent_code_unit_id": parent_id,
                "file_id": ctx.file_id,
                "path": ctx.path,
                "file_sha256": ctx.file_sha256,
                "qualpath": qual,
                "co_name": co.co_name,
                "firstlineno": int(co.co_firstlineno or 0),
                "argcount": int(getattr(co, "co_argcount", 0)),
                "posonlyargcount": int(getattr(co, "co_posonlyargcount", 0)),
                "kwonlyargcount": int(getattr(co, "co_kwonlyargcount", 0)),
                "nlocals": int(getattr(co, "co_nlocals", 0)),
                "flags": int(getattr(co, "co_flags", 0)),
                "stacksize": int(getattr(co, "co_stacksize", 0)),
                "code_len": len(co.co_code),
            }
        )

    return co_to_id


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
    cu_id: str,
) -> CodeUnitContext:
    return CodeUnitContext(
        cu_id=cu_id,
        file_ctx=ctx,
        instruction_data=_instruction_data(co, ctx.options),
        exc_entries=_exception_entries(co),
        code_len=len(co.co_code),
    )


def _append_exception_rows(unit_ctx: CodeUnitContext, exc_rows: list[Row]) -> None:
    for k, ex in enumerate(unit_ctx.exc_entries):
        exc_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "exc_entry_id": _hash_id("bc_exc", unit_ctx.cu_id, str(k)),
                "code_unit_id": unit_ctx.cu_id,
                "file_id": unit_ctx.file_ctx.file_id,
                "path": unit_ctx.file_ctx.path,
                "file_sha256": unit_ctx.file_ctx.file_sha256,
                "exc_index": int(k),
                "start_offset": int(getattr(ex, "start", 0)),
                "end_offset": int(getattr(ex, "end", 0)),
                "target_offset": int(getattr(ex, "target", 0)),
                "depth": int(getattr(ex, "depth", 0)),
                "lasti": bool(getattr(ex, "lasti", False)),
            }
        )


def _append_instruction_rows(unit_ctx: CodeUnitContext, ins_rows: list[Row]) -> None:
    for idx, ins in enumerate(unit_ctx.instruction_data.instructions):
        jt = _jump_target(ins)
        pos = getattr(ins, "positions", None)

        pos_start_line = getattr(pos, "lineno", None) if pos else None
        pos_end_line = getattr(pos, "end_lineno", None) if pos else None
        pos_start_col = getattr(pos, "col_offset", None) if pos else None
        pos_end_col = getattr(pos, "end_col_offset", None) if pos else None

        ins_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "instr_id": _hash_id("bc_insn", unit_ctx.cu_id, str(idx), str(ins.offset)),
                "code_unit_id": unit_ctx.cu_id,
                "file_id": unit_ctx.file_ctx.file_id,
                "path": unit_ctx.file_ctx.path,
                "file_sha256": unit_ctx.file_ctx.file_sha256,
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
) -> dict[int, str]:
    target_offsets = {int(getattr(ex, "target", 0)) for ex in unit_ctx.exc_entries}
    off_to_block: dict[int, str] = {}

    for start, end in blocks:
        kind = "entry" if start == 0 else ("handler" if start in target_offsets else "normal")
        block_id = _hash_id("bc_block", unit_ctx.cu_id, str(start), str(end))
        blk_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "block_id": block_id,
                "code_unit_id": unit_ctx.cu_id,
                "start_offset": int(start),
                "end_offset": int(end),
                "kind": kind,
            }
        )
        for offset in sorted(offset for offset in ins_offsets if start <= offset < end):
            off_to_block[offset] = block_id

    return off_to_block


def _append_cfg_edge(unit_ctx: CodeUnitContext, spec: CfgEdgeSpec, edge_rows: list[Row]) -> None:
    edge_rows.append(
        {
            "schema_version": SCHEMA_VERSION,
            "edge_id": _hash_id(
                "bc_cfg",
                unit_ctx.cu_id,
                spec.src_block_id,
                spec.dst_block_id,
                spec.kind,
                spec.edge_key,
            ),
            "code_unit_id": unit_ctx.cu_id,
            "src_block_id": spec.src_block_id,
            "dst_block_id": spec.dst_block_id,
            "kind": spec.kind,
            "cond_instr_id": spec.cond_instr_id,
            "exc_index": spec.exc_index,
        }
    )


def _append_normal_edges(
    unit_ctx: CodeUnitContext,
    blocks: Sequence[tuple[int, int]],
    off_to_block: Mapping[int, str],
    edge_rows: list[Row],
) -> None:
    instructions = unit_ctx.instruction_data.instructions
    index_by_offset = unit_ctx.instruction_data.index_by_offset
    terminators = unit_ctx.file_ctx.options.terminator_opnames

    for start, end in blocks:
        ins_in_block = [ins for ins in instructions if start <= int(ins.offset) < end]
        if not ins_in_block:
            continue
        first_offset = int(ins_in_block[0].offset)
        src = off_to_block.get(first_offset)
        if src is None:
            continue
        last = ins_in_block[-1]
        last_index = index_by_offset.get(int(last.offset))
        if last_index is None:
            continue
        last_id = _hash_id("bc_insn", unit_ctx.cu_id, str(last_index), str(last.offset))

        jt = _jump_target(last)
        if jt is not None and jt in off_to_block:
            dst = off_to_block[int(jt)]
            kind = "jump" if _is_unconditional_jump(last.opname) else "branch"
            _append_cfg_edge(
                unit_ctx,
                CfgEdgeSpec(
                    src_block_id=src,
                    dst_block_id=dst,
                    kind=kind,
                    edge_key=str(last.offset),
                    cond_instr_id=last_id if kind == "branch" else None,
                    exc_index=None,
                ),
                edge_rows,
            )
            if kind == "branch":
                next_block = off_to_block.get(end)
                if next_block is not None:
                    _append_cfg_edge(
                        unit_ctx,
                        CfgEdgeSpec(
                            src_block_id=src,
                            dst_block_id=next_block,
                            kind="next",
                            edge_key=str(end),
                            cond_instr_id=last_id,
                            exc_index=None,
                        ),
                        edge_rows,
                    )
            continue

        if last.opname in terminators:
            continue

        next_block = off_to_block.get(end)
        if next_block is not None:
            _append_cfg_edge(
                unit_ctx,
                CfgEdgeSpec(
                    src_block_id=src,
                    dst_block_id=next_block,
                    kind="next",
                    edge_key=str(end),
                    cond_instr_id=None,
                    exc_index=None,
                ),
                edge_rows,
            )


def _append_exception_edges(
    unit_ctx: CodeUnitContext,
    blocks: Sequence[tuple[int, int]],
    off_to_block: Mapping[int, str],
    edge_rows: list[Row],
) -> None:
    for k, ex in enumerate(unit_ctx.exc_entries):
        start = int(getattr(ex, "start", 0))
        end = int(getattr(ex, "end", 0))
        target = int(getattr(ex, "target", 0))
        handler = off_to_block.get(target)
        if handler is None:
            continue
        for block_start, block_end in blocks:
            if block_end <= start or block_start >= end:
                continue
            src = off_to_block.get(block_start)
            if src is None:
                continue
            _append_cfg_edge(
                unit_ctx,
                CfgEdgeSpec(
                    src_block_id=src,
                    dst_block_id=handler,
                    kind="exc",
                    edge_key=str(k),
                    cond_instr_id=None,
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
    code_unit_ids: Mapping[int, str],
    buffers: BytecodeRowBuffers,
) -> None:
    for co, _parent in _iter_code_objects(top):
        cu_id = code_unit_ids.get(id(co))
        if cu_id is None:
            continue
        unit_ctx = _build_code_unit_context(co, ctx, cu_id)
        _append_exception_rows(unit_ctx, buffers.exception_rows)
        _append_instruction_rows(unit_ctx, buffers.instruction_rows)
        if ctx.options.include_cfg_derivations:
            _append_cfg_rows(unit_ctx, buffers.block_rows, buffers.edge_rows)


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
    repo_files: pa.Table, options: BytecodeExtractOptions | None = None
) -> BytecodeExtractResult:
    """Extract bytecode tables from repository files.

    Returns
    -------
    BytecodeExtractResult
        Tables for bytecode code units, instructions, exception data, and edges.
    """
    options = options or BytecodeExtractOptions()

    buffers = BytecodeRowBuffers(
        code_unit_rows=[],
        instruction_rows=[],
        exception_rows=[],
        block_rows=[],
        edge_rows=[],
        error_rows=[],
    )

    for rf in iter_table_rows(repo_files):
        ctx = _row_context(rf, options)
        if ctx is None:
            continue
        text = _row_text(rf)
        if text is None:
            continue
        top, err = _compile_text(text, ctx)
        if err is not None:
            buffers.error_rows.append(err)
            continue
        if top is None:
            continue
        code_unit_ids = _assign_code_units(top, ctx, buffers.code_unit_rows)
        _extract_code_unit_rows(top, ctx, code_unit_ids, buffers)

    return BytecodeExtractResult(
        py_bc_code_units=pa.Table.from_pylist(buffers.code_unit_rows, schema=CODE_UNITS_SCHEMA),
        py_bc_instructions=pa.Table.from_pylist(buffers.instruction_rows, schema=INSTR_SCHEMA),
        py_bc_exception_table=pa.Table.from_pylist(buffers.exception_rows, schema=EXC_SCHEMA),
        py_bc_blocks=pa.Table.from_pylist(buffers.block_rows, schema=BLOCKS_SCHEMA),
        py_bc_cfg_edges=pa.Table.from_pylist(buffers.edge_rows, schema=CFG_EDGES_SCHEMA),
        py_bc_errors=pa.Table.from_pylist(buffers.error_rows, schema=ERRORS_SCHEMA),
    )


def extract_bytecode_table(
    *,
    repo_root: str | None,
    repo_files: pa.Table,
    ctx: object | None = None,
) -> pa.Table:
    """Extract bytecode instruction facts as a single table.

    Parameters
    ----------
    repo_root:
        Optional repository root (unused).
    repo_files:
        Repo files table.
    ctx:
        Execution context (unused).

    Returns
    -------
    pyarrow.Table
        Bytecode instruction table.
    """
    _ = repo_root
    _ = ctx
    result = extract_bytecode(repo_files, options=BytecodeExtractOptions())
    return result.py_bc_instructions
